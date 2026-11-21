package main

import (
	"context"
	"testing"

	"github.com/user/opencode-gateway/internal/adapters/base"
	"github.com/user/opencode-gateway/internal/opencode"
)

// routingSender is a no-op MessageSender. The routing bridge never sends
// anything itself, but base.NewBidirectionalAdapter requires a sender and
// SendToUserInChannel errors without one.
type routingSender struct{}

func (routingSender) SendMessage(context.Context, string, string, string) error { return nil }

func newRoutingRegistry(t *testing.T, channels ...string) (*base.AdapterRegistry, map[string]*base.BidirectionalAdapter) {
	t.Helper()
	registry := base.NewAdapterRegistry()
	adapters := make(map[string]*base.BidirectionalAdapter, len(channels))
	for _, channel := range channels {
		adapter := base.NewBidirectionalAdapter(channel, routingSender{})
		registry.Register(adapter)
		adapters[channel] = adapter
	}
	return registry, adapters
}

// TestSnapshotSessionRoutingCapturesEverythingSwitchDestroys pins the three
// things a snapshot has to hold: the user's previous session, that session's
// reverse mapping and delivery metadata, and the target's own metadata.
// SwitchUserSession deletes the first two, so a snapshot that missed them would
// make the compensation as destructive as the failure it undoes.
func TestSnapshotSessionRoutingCapturesEverythingSwitchDestroys(t *testing.T) {
	registry, adapters := newRoutingRegistry(t, "feishu")
	adapter := adapters["feishu"]
	adapter.MapUserToSession("alice", "ses_previous")
	adapter.MapSessionData("ses_previous", "receive_id", "oc_previous")
	adapter.MapSessionData("ses_previous", "receive_id_type", "chat_id")
	adapter.MapSessionData("ses_target", "receive_id", "oc_target")
	adapter.MapSessionData("ses_target", "receive_id_type", "open_id")

	snapshot := registrySessionRouting{registry: registry}.SnapshotSessionRouting("feishu", "alice", "ses_target")

	if snapshot.Channel != "feishu" || snapshot.UserID != "alice" || snapshot.TargetSessionID != "ses_target" {
		t.Fatalf("identity not recorded: %+v", snapshot)
	}
	if snapshot.PreviousSessionID != "ses_previous" {
		t.Fatalf("previous session not recorded: %+v", snapshot)
	}
	if snapshot.PreviousSessionData["receive_id"] != "oc_previous" || snapshot.PreviousSessionData["receive_id_type"] != "chat_id" {
		t.Fatalf("previous delivery context not recorded: %+v", snapshot.PreviousSessionData)
	}
	if snapshot.TargetSessionData["receive_id"] != "oc_target" || snapshot.TargetSessionData["receive_id_type"] != "open_id" {
		t.Fatalf("target delivery context not recorded: %+v", snapshot.TargetSessionData)
	}
	if snapshot.TargetUser != "" {
		t.Fatalf("target had no reverse mapping but snapshot claims %q", snapshot.TargetUser)
	}
}

// TestSnapshotSessionRoutingCopiesMetadata guards against handing out the live
// map: MapSessionData mutates it in place, so a snapshot holding the same map
// would silently change after the switch and restore the wrong webhook.
func TestSnapshotSessionRoutingCopiesMetadata(t *testing.T) {
	registry, adapters := newRoutingRegistry(t, "dingtalk")
	adapter := adapters["dingtalk"]
	adapter.MapUserToSession("alice", "ses_previous")
	adapter.MapSessionData("ses_previous", "channel", "https://oapi.dingtalk.com/robot/send?access_token=old")

	snapshot := registrySessionRouting{registry: registry}.SnapshotSessionRouting("dingtalk", "alice", "ses_target")
	adapter.MapSessionData("ses_previous", "channel", "https://oapi.dingtalk.com/robot/send?access_token=new")

	if got := snapshot.PreviousSessionData["channel"]; got != "https://oapi.dingtalk.com/robot/send?access_token=old" {
		t.Fatalf("snapshot aliased live metadata: %q", got)
	}
}

// TestApplySessionRoutingPreservesPlatformFields is the reason ChannelData is a
// map instead of a single string: Feishu needs receive_id plus receive_id_type,
// DingTalk needs its webhook, and collapsing them into one field would deliver
// to the wrong chat.
func TestApplySessionRoutingPreservesPlatformFields(t *testing.T) {
	registry, adapters := newRoutingRegistry(t, "feishu", "dingtalk")
	routing := registrySessionRouting{registry: registry}

	if err := routing.ApplySessionRouting("feishu", "alice", "ses_target", map[string]string{
		"receive_id":      "oc_target",
		"receive_id_type": "chat_id",
	}); err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	feishu := adapters["feishu"]
	if session, ok := feishu.GetSessionForUser("alice"); !ok || session != "ses_target" {
		t.Fatalf("user not pointed at target: %q %v", session, ok)
	}
	if user, ok := feishu.GetUserForSession("ses_target"); !ok || user != "alice" {
		t.Fatalf("reverse mapping missing: %q %v", user, ok)
	}
	if got, _ := feishu.GetSessionData("ses_target", "receive_id"); got != "oc_target" {
		t.Fatalf("receive_id not stored: %q", got)
	}
	if got, _ := feishu.GetSessionData("ses_target", "receive_id_type"); got != "chat_id" {
		t.Fatalf("receive_id_type not stored: %q", got)
	}

	if err := routing.ApplySessionRouting("dingtalk", "bob", "ses_ding", map[string]string{
		"channel": "https://oapi.dingtalk.com/robot/send?access_token=abc",
	}); err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if got, _ := adapters["dingtalk"].GetSessionData("ses_ding", "channel"); got != "https://oapi.dingtalk.com/robot/send?access_token=abc" {
		t.Fatalf("dingtalk webhook not stored: %q", got)
	}
}

// TestApplySessionRoutingDropsBlankFields keeps empty platform values out of
// the metadata map. A stored empty receive_id is worse than a missing one: the
// send path treats it as present and delivers nowhere.
func TestApplySessionRoutingDropsBlankFields(t *testing.T) {
	registry, adapters := newRoutingRegistry(t, "feishu")
	err := registrySessionRouting{registry: registry}.ApplySessionRouting("feishu", "alice", "ses_target", map[string]string{
		"receive_id":      "  ",
		"receive_id_type": "",
		" ":               "ignored",
	})
	if err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if data := adapters["feishu"].SessionData("ses_target"); len(data) != 0 {
		t.Fatalf("blank fields stored: %+v", data)
	}
}

// TestApplySessionRoutingRequiresAdapter makes a misconfigured channel an
// explicit error instead of a silent no-op that reports a successful switch.
func TestApplySessionRoutingRequiresAdapter(t *testing.T) {
	registry, _ := newRoutingRegistry(t, "feishu")
	routing := registrySessionRouting{registry: registry}
	if err := routing.ApplySessionRouting("wechat", "alice", "ses_target", nil); err == nil {
		t.Fatal("missing adapter accepted")
	}
	if err := routing.RestoreSessionRouting(sessionRoutingSnapshotFor("wechat")); err == nil {
		t.Fatal("missing adapter accepted on restore")
	}
	if err := routing.ValidateSessionDelivery("wechat", "alice", "ses_target", nil); err == nil {
		t.Fatal("missing adapter accepted on validate")
	}
}

// TestRestoreSessionRoutingUndoesApply is the compensation contract: after a
// failed switch the user must be back on the previous session with its webhook
// intact, and the target must not keep the metadata the failed attempt wrote.
func TestRestoreSessionRoutingUndoesApply(t *testing.T) {
	registry, adapters := newRoutingRegistry(t, "feishu")
	adapter := adapters["feishu"]
	adapter.MapUserToSession("alice", "ses_previous")
	adapter.MapSessionData("ses_previous", "receive_id", "oc_previous")
	adapter.MapSessionData("ses_previous", "receive_id_type", "chat_id")
	adapter.MapSessionData("ses_target", "receive_id", "oc_target")
	adapter.MapSessionData("ses_target", "receive_id_type", "open_id")

	routing := registrySessionRouting{registry: registry}
	snapshot := routing.SnapshotSessionRouting("feishu", "alice", "ses_target")
	if err := routing.ApplySessionRouting("feishu", "alice", "ses_target", map[string]string{
		"receive_id":      "oc_new",
		"receive_id_type": "chat_id",
	}); err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if err := routing.RestoreSessionRouting(snapshot); err != nil {
		t.Fatalf("restore failed: %v", err)
	}

	if session, ok := adapter.GetSessionForUser("alice"); !ok || session != "ses_previous" {
		t.Fatalf("user not restored: %q %v", session, ok)
	}
	if user, ok := adapter.GetUserForSession("ses_previous"); !ok || user != "alice" {
		t.Fatalf("previous reverse mapping not restored: %q %v", user, ok)
	}
	if got, _ := adapter.GetSessionData("ses_previous", "receive_id"); got != "oc_previous" {
		t.Fatalf("previous delivery context not restored: %q", got)
	}
	if got, _ := adapter.GetSessionData("ses_target", "receive_id"); got != "oc_target" {
		t.Fatalf("target delivery context not restored: %q", got)
	}
	if got, _ := adapter.GetSessionData("ses_target", "receive_id_type"); got != "open_id" {
		t.Fatalf("target receive_id_type not restored: %q", got)
	}
}

// TestRestoreSessionRoutingClearsMetadataThatDidNotExist covers the cold-start
// shape: the target had no metadata before the switch, so the failed attempt's
// metadata must be removed rather than left behind.
func TestRestoreSessionRoutingClearsMetadataThatDidNotExist(t *testing.T) {
	registry, adapters := newRoutingRegistry(t, "wecom")
	adapter := adapters["wecom"]
	adapter.MapUserToSession("alice", "ses_previous")

	routing := registrySessionRouting{registry: registry}
	snapshot := routing.SnapshotSessionRouting("wecom", "alice", "ses_target")
	if err := routing.ApplySessionRouting("wecom", "alice", "ses_target", map[string]string{"channel": "chat-1"}); err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if err := routing.RestoreSessionRouting(snapshot); err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	if data := adapter.SessionData("ses_target"); len(data) != 0 {
		t.Fatalf("failed switch left metadata behind: %+v", data)
	}
	if user, ok := adapter.GetUserForSession("ses_target"); ok {
		t.Fatalf("failed switch left reverse mapping behind: %q", user)
	}
}

// TestRestoreSessionRoutingClearsUserWithNoPreviousSession covers a user whose
// first switch failed: there was no previous session, so the mapping must be
// removed instead of pointing at the target.
func TestRestoreSessionRoutingClearsUserWithNoPreviousSession(t *testing.T) {
	registry, adapters := newRoutingRegistry(t, "wechat")
	adapter := adapters["wechat"]

	routing := registrySessionRouting{registry: registry}
	snapshot := routing.SnapshotSessionRouting("wechat", "alice", "ses_target")
	if err := routing.ApplySessionRouting("wechat", "alice", "ses_target", nil); err != nil {
		t.Fatalf("apply failed: %v", err)
	}
	if err := routing.RestoreSessionRouting(snapshot); err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	if session, ok := adapter.GetSessionForUser("alice"); ok {
		t.Fatalf("user still mapped after failed first switch: %q", session)
	}
}

// TestValidateSessionDeliveryAcceptsRealTargets covers the three ways a
// restore-claim can legitimately deliver: the inbound message carried platform
// context, the session already has stored context, or the adapter already knows
// the user.
func TestValidateSessionDeliveryAcceptsRealTargets(t *testing.T) {
	registry, adapters := newRoutingRegistry(t, "feishu", "dingtalk", "wechat")
	routing := registrySessionRouting{registry: registry}

	if err := routing.ValidateSessionDelivery("feishu", "alice", "ses_a", map[string]string{"receive_id": "oc_a"}); err != nil {
		t.Fatalf("inbound context rejected: %v", err)
	}
	adapters["dingtalk"].MapSessionData("ses_b", "channel", "https://example.invalid/hook")
	if err := routing.ValidateSessionDelivery("dingtalk", "bob", "ses_b", nil); err != nil {
		t.Fatalf("stored context rejected: %v", err)
	}
	adapters["wechat"].MapUserToSession("carol", "ses_c")
	if err := routing.ValidateSessionDelivery("wechat", "carol", "ses_c", nil); err != nil {
		t.Fatalf("known user rejected: %v", err)
	}
}

// TestValidateSessionDeliveryRejectsSilentDrops is the defect this check
// exists for: DingTalk returns nil when it has no target, so a restore-claim
// with no delivery context would tell the user their TUI replies are mirrored
// while every send was dropped.
func TestValidateSessionDeliveryRejectsSilentDrops(t *testing.T) {
	registry, _ := newRoutingRegistry(t, "dingtalk")
	routing := registrySessionRouting{registry: registry}

	if err := routing.ValidateSessionDelivery("dingtalk", "bob", "ses_b", nil); err == nil {
		t.Fatal("no delivery target accepted")
	}
	if err := routing.ValidateSessionDelivery("dingtalk", "bob", "ses_b", map[string]string{"channel": "   "}); err == nil {
		t.Fatal("blank delivery target accepted")
	}
	if err := routing.ValidateSessionDelivery("dingtalk", "", "ses_b", map[string]string{"channel": "hook"}); err == nil {
		t.Fatal("missing user accepted")
	}
}

func sessionRoutingSnapshotFor(channel string) opencode.SessionRoutingSnapshot {
	return opencode.SessionRoutingSnapshot{Channel: channel, UserID: "alice", TargetSessionID: "ses_target"}
}
