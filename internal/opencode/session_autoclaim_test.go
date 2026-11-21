package opencode

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	sdk "github.com/sst/opencode-sdk-go"
)

// sessionUpdatedEvent builds a session.updated event carrying the full session
// object, which is the shape the opencode server emits after a TUI /rename.
func sessionUpdatedEvent(t *testing.T, sessionID, title string) *sdk.EventListResponse {
	t.Helper()
	raw := `{"type":"session.updated","properties":{"sessionID":` + mustJSON(sessionID) +
		`,"info":{"id":` + mustJSON(sessionID) + `,"title":` + mustJSON(title) +
		`,"directory":"/work","projectID":"proj","version":"1.18.32"}}}`
	event := &sdk.EventListResponse{}
	if err := json.Unmarshal([]byte(raw), event); err != nil {
		t.Fatalf("could not build session.updated event: %v", err)
	}
	return event
}

func TestAutoClaimFromTitleClaimsPrefixedSession(t *testing.T) {
	rec := &claimRecorder{title: "[feishu:ou_alice] my tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	var hookChannel, hookUser string
	client.SetClaimedSessionHook(func(ctx context.Context, sessionID, channel, userID, channelData string) error {
		hookChannel, hookUser = channel, userID
		return nil
	})

	claimed, err := client.AutoClaimFromTitle(context.Background(),
		sessionUpdatedEvent(t, "ses_claim", "[feishu:ou_alice] my tui work"))
	if err != nil {
		t.Fatalf("AutoClaimFromTitle returned error: %v", err)
	}
	if !claimed {
		t.Fatal("AutoClaimFromTitle did not claim a prefixed session")
	}

	if hookChannel != "feishu" || hookUser != "ou_alice" {
		t.Fatalf("hook got (%q, %q), want (feishu, ou_alice)", hookChannel, hookUser)
	}
	if got := client.ClaimedSessions(); len(got) != 1 || got[0] != "ses_claim" {
		t.Fatalf("ClaimedSessions() = %#v, want [ses_claim]", got)
	}
	if _, ok := client.sessionHandlers.Load("ses_claim"); !ok {
		t.Fatal("auto-claim did not register a forwarding handler")
	}

	// The title already carries the prefix, so no PATCH should be issued.
	if patches := rec.patchBodies(); len(patches) != 0 {
		t.Fatalf("auto-claim rewrote an already-prefixed title: %#v", patches)
	}
}

func TestAutoClaimFromTitleIgnoresUnprefixedSession(t *testing.T) {
	rec := &claimRecorder{title: "plain tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	claimed, err := client.AutoClaimFromTitle(context.Background(),
		sessionUpdatedEvent(t, "ses_claim", "plain tui work"))
	if err != nil {
		t.Fatalf("AutoClaimFromTitle returned error: %v", err)
	}
	if claimed {
		t.Fatal("AutoClaimFromTitle claimed a session with no ownership prefix")
	}
	if got := client.ClaimedSessions(); len(got) != 0 {
		t.Fatalf("ClaimedSessions() = %#v, want empty", got)
	}
	if rec.getCount() != 0 {
		t.Fatalf("AutoClaimFromTitle made %d GET requests for an unprefixed title, want 0", rec.getCount())
	}
}

func TestAutoClaimFromTitleIgnoresOtherEventTypes(t *testing.T) {
	rec := &claimRecorder{title: "[feishu:ou_alice] work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	event := sessionUpdatedEvent(t, "ses_claim", "[feishu:ou_alice] work")
	event.Type = "message.updated"

	claimed, err := client.AutoClaimFromTitle(context.Background(), event)
	if err != nil {
		t.Fatalf("AutoClaimFromTitle returned error: %v", err)
	}
	if claimed {
		t.Fatal("AutoClaimFromTitle acted on a non-session.updated event")
	}
}

func TestAutoClaimFromTitleIsIdempotent(t *testing.T) {
	rec := &claimRecorder{title: "[feishu:ou_alice] work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	event := sessionUpdatedEvent(t, "ses_claim", "[feishu:ou_alice] work")
	if claimed, err := client.AutoClaimFromTitle(context.Background(), event); err != nil || !claimed {
		t.Fatalf("first AutoClaimFromTitle = (%v, %v), want (true, nil)", claimed, err)
	}
	getsAfterFirst := rec.getCount()

	claimed, err := client.AutoClaimFromTitle(context.Background(), event)
	if err != nil {
		t.Fatalf("second AutoClaimFromTitle returned error: %v", err)
	}
	if claimed {
		t.Fatal("second AutoClaimFromTitle re-claimed an already claimed session")
	}
	if rec.getCount() != getsAfterFirst {
		t.Fatalf("second claim made %d extra GET requests, want 0", rec.getCount()-getsAfterFirst)
	}
}

func TestAutoClaimFromTitleSkipsSkillgen(t *testing.T) {
	rec := &claimRecorder{title: "[skillgen:draft] work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	claimed, err := client.AutoClaimFromTitle(context.Background(),
		sessionUpdatedEvent(t, "ses_claim", "[skillgen:draft] work"))
	if err != nil {
		t.Fatalf("AutoClaimFromTitle returned error: %v", err)
	}
	if claimed {
		t.Fatal("AutoClaimFromTitle claimed a skillgen drafting session")
	}
	if rec.getCount() != 0 {
		t.Fatalf("skillgen session triggered %d GET requests, want 0", rec.getCount())
	}
}

func TestAutoClaimFromTitleSkipsGatewayOwnedSession(t *testing.T) {
	rec := &claimRecorder{title: "[feishu:ou_alice] work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	// A session this process created is already routed by its own per-turn
	// handler; adopting it again would replace that handler with a long-lived
	// mirror.
	client.sessions.Store("thread-1", "ses_claim")

	claimed, err := client.AutoClaimFromTitle(context.Background(),
		sessionUpdatedEvent(t, "ses_claim", "[feishu:ou_alice] work"))
	if err != nil {
		t.Fatalf("AutoClaimFromTitle returned error: %v", err)
	}
	if claimed {
		t.Fatal("AutoClaimFromTitle claimed a session the gateway created itself")
	}
}

func TestClaimSessionFromTitleClaimsAfterRestart(t *testing.T) {
	rec := &claimRecorder{title: "[dingtalk:manager1234] work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	claimed, err := client.ClaimSessionFromTitle(context.Background(), "ses_claim")
	if err != nil {
		t.Fatalf("ClaimSessionFromTitle returned error: %v", err)
	}
	if !claimed {
		t.Fatal("ClaimSessionFromTitle did not claim a prefixed session")
	}
	if _, ok := client.sessionHandlers.Load("ses_claim"); !ok {
		t.Fatal("ClaimSessionFromTitle did not register a forwarding handler")
	}
}

func TestClaimSessionFromTitleIgnoresUnprefixedSession(t *testing.T) {
	rec := &claimRecorder{title: "plain work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	claimed, err := client.ClaimSessionFromTitle(context.Background(), "ses_claim")
	if err != nil {
		t.Fatalf("ClaimSessionFromTitle returned error: %v", err)
	}
	if claimed {
		t.Fatal("ClaimSessionFromTitle claimed a session with no ownership prefix")
	}
}

// syncEnvelopeEvent builds a /global/event "sync" envelope. This is the shape
// opencode publishes process-wide: it names the session in syncEvent.aggregateID
// but carries no title and no properties object.
func syncEnvelopeEvent(t *testing.T, syncType, sessionID string) *sdk.EventListResponse {
	t.Helper()
	raw := `{"type":"sync","syncEvent":{"id":"evt_1bc1b442b001fOwaXU3yDZXGyi","type":` +
		mustJSON(syncType) + `,"seq":390,"aggregateID":` + mustJSON(sessionID) +
		`,"data":{"sessionID":` + mustJSON(sessionID) + `}}}`
	event := &sdk.EventListResponse{}
	if err := json.Unmarshal([]byte(raw), event); err != nil {
		t.Fatalf("could not build sync envelope: %v", err)
	}
	return event
}

func TestAutoClaimFromSyncEnvelopeClaimsPrefixedSession(t *testing.T) {
	rec := &claimRecorder{title: "[feishu:ou_alice] my tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	var hookChannel, hookUser string
	client.SetClaimedSessionHook(func(ctx context.Context, sessionID, channel, userID, channelData string) error {
		hookChannel, hookUser = channel, userID
		return nil
	})

	claimed, err := client.AutoClaimFromTitle(context.Background(),
		syncEnvelopeEvent(t, "session.updated.1", "ses_claim"))
	if err != nil {
		t.Fatalf("AutoClaimFromTitle returned error: %v", err)
	}
	if !claimed {
		t.Fatal("AutoClaimFromTitle did not claim a prefixed session from a sync envelope")
	}
	if hookChannel != "feishu" || hookUser != "ou_alice" {
		t.Fatalf("hook got (%q, %q), want (feishu, ou_alice)", hookChannel, hookUser)
	}
	if _, ok := client.sessionHandlers.Load("ses_claim"); !ok {
		t.Fatal("sync-envelope claim did not register a forwarding handler")
	}
	// The title already carries the prefix, so no PATCH should be issued.
	if patches := rec.patchBodies(); len(patches) != 0 {
		t.Fatalf("sync-envelope claim rewrote an already-prefixed title: %#v", patches)
	}
}

func TestAutoClaimFromSyncEnvelopeIgnoresOtherSyncTypes(t *testing.T) {
	rec := &claimRecorder{title: "[feishu:ou_alice] work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	for _, syncType := range []string{"message.updated.1", "message.part.updated.1", "session.created.1"} {
		claimed, err := client.AutoClaimFromTitle(context.Background(),
			syncEnvelopeEvent(t, syncType, "ses_claim"))
		if err != nil {
			t.Fatalf("AutoClaimFromTitle(%s) returned error: %v", syncType, err)
		}
		if claimed {
			t.Fatalf("AutoClaimFromTitle claimed on sync type %s", syncType)
		}
	}
	if rec.getCount() != 0 {
		t.Fatalf("non-session.updated sync envelopes made %d GET requests, want 0", rec.getCount())
	}
}

func TestAutoClaimFromSyncEnvelopeThrottlesProbes(t *testing.T) {
	rec := &claimRecorder{title: "plain work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	// session.updated.1 fires on every session record change, so a busy session
	// can emit dozens per minute. Only the first should cost a round-trip.
	for i := 0; i < 5; i++ {
		if _, err := client.AutoClaimFromTitle(context.Background(),
			syncEnvelopeEvent(t, "session.updated.1", "ses_claim")); err != nil {
			t.Fatalf("AutoClaimFromTitle returned error: %v", err)
		}
	}
	if got := rec.getCount(); got != 1 {
		t.Fatalf("5 sync envelopes made %d GET requests, want 1 (throttled)", got)
	}
}

func TestAutoClaimFromSyncEnvelopeSkipsGatewayOwnedSession(t *testing.T) {
	rec := &claimRecorder{title: "[feishu:ou_alice] work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	client.sessions.Store("thread-1", "ses_claim")

	claimed, err := client.AutoClaimFromTitle(context.Background(),
		syncEnvelopeEvent(t, "session.updated.1", "ses_claim"))
	if err != nil {
		t.Fatalf("AutoClaimFromTitle returned error: %v", err)
	}
	if claimed {
		t.Fatal("sync envelope claimed a session the gateway created itself")
	}
	if rec.getCount() != 0 {
		t.Fatalf("gateway-owned session made %d GET requests, want 0", rec.getCount())
	}
}

func TestAutoClaimFromSyncEnvelopeIgnoresUnprefixedTitle(t *testing.T) {
	rec := &claimRecorder{title: "plain tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	claimed, err := client.AutoClaimFromTitle(context.Background(),
		syncEnvelopeEvent(t, "session.updated.1", "ses_claim"))
	if err != nil {
		t.Fatalf("AutoClaimFromTitle returned error: %v", err)
	}
	if claimed {
		t.Fatal("sync envelope claimed a session with no ownership prefix")
	}
	if got := client.ClaimedSessions(); len(got) != 0 {
		t.Fatalf("ClaimedSessions() = %#v, want empty", got)
	}
}

func TestAutoClaimFromSyncEnvelopeIgnoresMissingAggregateID(t *testing.T) {
	rec := &claimRecorder{title: "[feishu:ou_alice] work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	claimed, err := client.AutoClaimFromTitle(context.Background(),
		syncEnvelopeEvent(t, "session.updated.1", ""))
	if err != nil {
		t.Fatalf("AutoClaimFromTitle returned error: %v", err)
	}
	if claimed {
		t.Fatal("sync envelope with no aggregateID was claimed")
	}
	if rec.getCount() != 0 {
		t.Fatalf("empty aggregateID made %d GET requests, want 0", rec.getCount())
	}
}

func TestParseSessionTitlePrefix(t *testing.T) {
	cases := []struct {
		title   string
		channel string
		userID  string
		ok      bool
	}{
		{"[feishu:ou_alice] work", "feishu", "ou_alice", true},
		{"[dingtalk:manager1234]", "dingtalk", "manager1234", true},
		{"  [wechat:alice]  spaced  ", "wechat", "alice", true},
		{"[wecom:corp/user] x", "wecom", "corp/user", true},
		// Channel-only forms parse with an empty userID; the host resolver
		// fills it in. They are valid prefixes, not malformed ones.
		{"[feishu] work", "feishu", "", true},
		{"[feishu:] work", "feishu", "", true},
		{"[feishu: ] work", "feishu", "", true},
		{"plain title", "", "", false},
		{"[]", "", "", false},
		{"[:alice]", "", "", false},
		{"", "", "", false},
	}
	for _, tc := range cases {
		channel, userID, ok := parseSessionTitlePrefix(tc.title)
		if ok != tc.ok || channel != tc.channel || userID != tc.userID {
			t.Errorf("parseSessionTitlePrefix(%q) = (%q, %q, %v), want (%q, %q, %v)",
				tc.title, channel, userID, ok, tc.channel, tc.userID, tc.ok)
		}
	}
}

// TestAutoClaimFromChannelOnlyTitleResolvesSoleUser covers the single-user
// deployment shortcut: "[feishu] my work" claims for whichever user the host
// resolver reports, and the title is rewritten to the full prefix so later
// ownership checks (ListSessionsForUser) still work.
func TestAutoClaimFromChannelOnlyTitleResolvesSoleUser(t *testing.T) {
	rec := &claimRecorder{title: "[feishu] my tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	client.SetClaimUserResolver(func(ctx context.Context, channel string) (string, bool) {
		if channel != "feishu" {
			return "", false
		}
		return "ou_alice", true
	})

	var hookChannel, hookUser string
	client.SetClaimedSessionHook(func(ctx context.Context, sessionID, channel, userID, channelData string) error {
		hookChannel, hookUser = channel, userID
		return nil
	})

	claimed, err := client.AutoClaimFromTitle(context.Background(),
		sessionUpdatedEvent(t, "ses_claim", "[feishu] my tui work"))
	if err != nil {
		t.Fatalf("AutoClaimFromTitle returned error: %v", err)
	}
	if !claimed {
		t.Fatal("a channel-only title was not claimed even though the resolver had a sole user")
	}
	if hookChannel != "feishu" || hookUser != "ou_alice" {
		t.Fatalf("hook got (%q, %q), want (feishu, ou_alice)", hookChannel, hookUser)
	}

	// The stored title must carry the resolved user, otherwise the session
	// would not show up in that user's /sessions.
	bodies := rec.patchBodies()
	if len(bodies) != 1 {
		t.Fatalf("expected exactly 1 PATCH, got %d: %#v", len(bodies), bodies)
	}
	if !strings.Contains(bodies[0], `"[feishu:ou_alice] my tui work"`) {
		t.Fatalf("PATCH body did not carry the resolved prefix: %s", bodies[0])
	}
}

// TestAutoClaimFromChannelOnlyTitleWithoutResolverIsIgnored is the safety
// property: with no resolver installed, a channel-only title must be ignored
// rather than claimed for an arbitrary user.
func TestAutoClaimFromChannelOnlyTitleWithoutResolverIsIgnored(t *testing.T) {
	rec := &claimRecorder{title: "my tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	claimed, err := client.AutoClaimFromTitle(context.Background(),
		sessionUpdatedEvent(t, "ses_claim", "[feishu] my tui work"))
	if err != nil {
		t.Fatalf("AutoClaimFromTitle returned error: %v", err)
	}
	if claimed {
		t.Fatal("a channel-only title was claimed with no resolver installed")
	}
	if got := client.ClaimedSessions(); len(got) != 0 {
		t.Fatalf("ClaimedSessions() = %#v, want empty", got)
	}
}

// TestAutoClaimFromChannelOnlyTitleRefusesAmbiguousChannel is the other half of
// the safety property: a resolver that cannot name a single user must not
// produce a claim.
func TestAutoClaimFromChannelOnlyTitleRefusesAmbiguousChannel(t *testing.T) {
	rec := &claimRecorder{title: "my tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	client.SetClaimUserResolver(func(ctx context.Context, channel string) (string, bool) {
		return "", false
	})

	claimed, err := client.AutoClaimFromTitle(context.Background(),
		sessionUpdatedEvent(t, "ses_claim", "[feishu] my tui work"))
	if err != nil {
		t.Fatalf("AutoClaimFromTitle returned error: %v", err)
	}
	if claimed {
		t.Fatal("an ambiguous channel was claimed anyway")
	}
	if patches := rec.patchBodies(); len(patches) != 0 {
		t.Fatalf("an ambiguous channel still rewrote the title: %#v", patches)
	}
}

// TestUnclaimAllowsReclaimByAnotherChannel is the property /release exists for:
// a session claimed by one channel can be released and then claimed by a
// different one, without restarting the gateway.
func TestUnclaimAllowsReclaimByAnotherChannel(t *testing.T) {
	rec := &claimRecorder{title: "[wechat:carol] my tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	// First claim: wechat.
	if _, err := client.ClaimSession(context.Background(), "ses_claim", "wechat", "carol", ""); err != nil {
		t.Fatalf("first ClaimSession returned error: %v", err)
	}

	// A second claim attempt on the same session is a silent no-op, which is
	// exactly why /release is needed.
	claimed, err := client.AutoClaimFromTitle(context.Background(),
		sessionUpdatedEvent(t, "ses_claim", "[dingtalk:dave] my tui work"))
	if err != nil {
		t.Fatalf("AutoClaimFromTitle returned error: %v", err)
	}
	if claimed {
		t.Fatal("an already-claimed session was re-claimed without a release")
	}

	// Release, then the same rename must now take effect.
	if _, err := client.UnclaimSession(context.Background(), "ses_claim"); err != nil {
		t.Fatalf("UnclaimSession returned error: %v", err)
	}
	rec.mu.Lock()
	rec.title = "[dingtalk:dave] my tui work"
	rec.mu.Unlock()

	claimed, err = client.AutoClaimFromTitle(context.Background(),
		sessionUpdatedEvent(t, "ses_claim", "[dingtalk:dave] my tui work"))
	if err != nil {
		t.Fatalf("AutoClaimFromTitle after release returned error: %v", err)
	}
	if !claimed {
		t.Fatal("a released session could not be claimed by another channel")
	}

	value, ok := client.claimedSessions.Load("ses_claim")
	if !ok {
		t.Fatal("the re-claim did not record the session")
	}
	got := value.(claimedSession)
	if got.Channel != "dingtalk" || got.UserID != "dave" {
		t.Fatalf("re-claim target = %s/%s, want dingtalk/dave", got.Channel, got.UserID)
	}
}
