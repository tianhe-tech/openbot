package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	sdk "github.com/sst/opencode-sdk-go"
)

// switchServer is a fake opencode upstream serving the three endpoints a
// /switch touches: the session listing (also used by the client's background
// health check), a single session read, and a title update.
type switchServer struct {
	mu     sync.Mutex
	titles map[string]string
	server *httptest.Server
}

func newSwitchServer(t *testing.T, titles map[string]string) *switchServer {
	t.Helper()
	fake := &switchServer{titles: make(map[string]string, len(titles))}
	for id, title := range titles {
		fake.titles[id] = title
	}
	fake.server = httptest.NewServer(http.HandlerFunc(fake.handle))
	t.Cleanup(fake.server.Close)
	return fake
}

func (f *switchServer) URL() string { return f.server.URL }

func (f *switchServer) title(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.titles[id]
}

func (f *switchServer) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.URL.Path == "/session" {
		if r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		f.mu.Lock()
		rows := make([]string, 0, len(f.titles))
		for id, title := range f.titles {
			rows = append(rows, sessionJSON(id, title, 1794209871904))
		}
		f.mu.Unlock()
		_, _ = w.Write([]byte("[" + strings.Join(rows, ",") + "]"))
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/session/")
	if id == "" || id == r.URL.Path {
		http.NotFound(w, r)
		return
	}

	switch r.Method {
	case http.MethodGet:
		f.mu.Lock()
		title, ok := f.titles[id]
		f.mu.Unlock()
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
			return
		}
		_, _ = w.Write([]byte(sessionJSON(id, title, 1794209871904)))
	case http.MethodPatch:
		body, _ := io.ReadAll(r.Body)
		var payload struct {
			Title string `json:"title"`
		}
		_ = json.Unmarshal(body, &payload)
		f.mu.Lock()
		f.titles[id] = payload.Title
		f.mu.Unlock()
		_, _ = w.Write([]byte(sessionJSON(id, payload.Title, 1794209871904)))
	default:
		http.NotFound(w, r)
	}
}

type switchApplyCall struct {
	Channel     string
	UserID      string
	SessionID   string
	ChannelData map[string]string
}

type switchValidateCall struct {
	Channel     string
	UserID      string
	SessionID   string
	ChannelData map[string]string
}

// fakeSwitchRouting records what SwitchSession asks the adapter half to do and
// models the two adapter maps so a test can assert that a failed switch left
// them alone.
type fakeSwitchRouting struct {
	mu sync.Mutex

	snapshots   []SessionRoutingSnapshot
	applies     []switchApplyCall
	restores    []SessionRoutingSnapshot
	validations []switchValidateCall

	applyErr    error
	validateErr error

	userSessions map[string]string
	sessionData  map[string]map[string]string
}

func newFakeSwitchRouting() *fakeSwitchRouting {
	return &fakeSwitchRouting{
		userSessions: map[string]string{},
		sessionData:  map[string]map[string]string{},
	}
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func (f *fakeSwitchRouting) SnapshotSessionRouting(channel, userID, sessionID string) SessionRoutingSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	snapshot := SessionRoutingSnapshot{
		Channel:         channel,
		UserID:          userID,
		TargetSessionID: sessionID,
	}
	if previous, ok := f.userSessions[userID]; ok {
		snapshot.PreviousSessionID = previous
		snapshot.PreviousSessionData = cloneStringMap(f.sessionData[previous])
	}
	snapshot.TargetSessionData = cloneStringMap(f.sessionData[sessionID])
	f.snapshots = append(f.snapshots, snapshot)
	return snapshot
}

func (f *fakeSwitchRouting) ApplySessionRouting(channel, userID, sessionID string, channelData map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.applies = append(f.applies, switchApplyCall{channel, userID, sessionID, cloneStringMap(channelData)})

	// Mutate first, then fail. A real adapter can fail after SwitchUserSession
	// has already deleted the previous session's metadata, and that partial
	// apply is exactly what the compensation has to undo.
	if previous, ok := f.userSessions[userID]; ok && previous != sessionID {
		delete(f.sessionData, previous)
	}
	f.userSessions[userID] = sessionID
	for key, value := range channelData {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			continue
		}
		if f.sessionData[sessionID] == nil {
			f.sessionData[sessionID] = map[string]string{}
		}
		f.sessionData[sessionID][key] = value
	}
	return f.applyErr
}

func (f *fakeSwitchRouting) RestoreSessionRouting(snapshot SessionRoutingSnapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restores = append(f.restores, snapshot)
	if snapshot.PreviousSessionID == "" {
		delete(f.userSessions, snapshot.UserID)
	} else {
		f.userSessions[snapshot.UserID] = snapshot.PreviousSessionID
		f.sessionData[snapshot.PreviousSessionID] = cloneStringMap(snapshot.PreviousSessionData)
	}
	if snapshot.TargetSessionID != "" && snapshot.TargetSessionID != snapshot.PreviousSessionID {
		f.sessionData[snapshot.TargetSessionID] = cloneStringMap(snapshot.TargetSessionData)
	}
	return nil
}

func (f *fakeSwitchRouting) ValidateSessionDelivery(channel, userID, sessionID string, channelData map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.validations = append(f.validations, switchValidateCall{channel, userID, sessionID, cloneStringMap(channelData)})
	return f.validateErr
}

func (f *fakeSwitchRouting) applyCalls() []switchApplyCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]switchApplyCall, len(f.applies))
	copy(out, f.applies)
	return out
}

func (f *fakeSwitchRouting) restoreCalls() []SessionRoutingSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]SessionRoutingSnapshot, len(f.restores))
	copy(out, f.restores)
	return out
}

func (f *fakeSwitchRouting) validateCalls() []switchValidateCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]switchValidateCall, len(f.validations))
	copy(out, f.validations)
	return out
}

func (f *fakeSwitchRouting) userSession(userID string) (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	sessionID, ok := f.userSessions[userID]
	return sessionID, ok
}

func (f *fakeSwitchRouting) data(sessionID string) map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return cloneStringMap(f.sessionData[sessionID])
}

// TestSwitchSessionBindsThreadAndAppliesRouting covers the ordinary case: a
// session this gateway created is already delivering through its per-turn
// handler, so a switch only moves the routing.
func TestSwitchSessionBindsThreadAndAppliesRouting(t *testing.T) {
	fake := newSwitchServer(t, map[string]string{
		"ses_target": "[wechat:alice] target work",
	})
	client := NewClient(fake.URL(), "")
	routing := newFakeSwitchRouting()
	client.SetSessionSwitchRouting(routing)

	// A thread already pointing at the target is what marks it as created by
	// this gateway. The switch itself arrives on a different thread.
	client.BindSessionToThread("warmup", "ses_target")

	result, err := client.SwitchSession(context.Background(), SessionSwitchRequest{
		Channel:  "wechat",
		UserID:   "alice",
		ThreadID: "chat-1",
		Selector: "ses_target",
	})
	if err != nil {
		t.Fatalf("SwitchSession returned error: %v", err)
	}
	if result.Session.ID != "ses_target" {
		t.Fatalf("result session = %q, want ses_target", result.Session.ID)
	}
	if result.Claimed || result.Resumed {
		t.Fatalf("a gateway-owned session must not be claimed or resumed: %+v", result)
	}
	if got, ok := client.GetSessionForThread("chat-1"); !ok || got != "ses_target" {
		t.Fatalf("thread mapping = (%q, %t), want (ses_target, true)", got, ok)
	}
	if got, ok := routing.userSession("alice"); !ok || got != "ses_target" {
		t.Fatalf("adapter routing = (%q, %t), want (ses_target, true)", got, ok)
	}
	if _, registered := client.sessionHandlers.Load("ses_target"); registered {
		t.Fatal("a gateway-owned session must keep its per-turn handler, not gain a mirror")
	}
	if calls := routing.applyCalls(); len(calls) != 1 {
		t.Fatalf("expected exactly one apply, got %d", len(calls))
	}
}

// TestSwitchSessionRestoresRoutingWhenApplyFails pins the compensation. The
// previous implementation reported success after a half-applied switch, leaving
// the chat pointing at a session whose delivery metadata had been deleted.
func TestSwitchSessionRestoresRoutingWhenApplyFails(t *testing.T) {
	fake := newSwitchServer(t, map[string]string{
		"ses_old":    "[wechat:alice] old work",
		"ses_target": "[wechat:alice] target work",
	})
	client := NewClient(fake.URL(), "")
	routing := newFakeSwitchRouting()
	client.SetSessionSwitchRouting(routing)

	// The user is currently on ses_old, which carries a webhook. A failed
	// switch must leave both the mapping and that webhook in place.
	routing.userSessions["alice"] = "ses_old"
	routing.sessionData["ses_old"] = map[string]string{"channel": "https://hook/old"}
	client.BindSessionToThread("warmup", "ses_target")

	routing.applyErr = errors.New("adapter exploded")

	_, err := client.SwitchSession(context.Background(), SessionSwitchRequest{
		Channel:  "wechat",
		UserID:   "alice",
		ThreadID: "chat-1",
		Selector: "ses_target",
	})
	if err == nil {
		t.Fatal("expected the failed apply to be reported")
	}
	if !strings.Contains(err.Error(), "adapter exploded") {
		t.Fatalf("error = %v, want it to name the apply failure", err)
	}
	if got, ok := routing.userSession("alice"); !ok || got != "ses_old" {
		t.Fatalf("adapter routing was not restored: (%q, %t)", got, ok)
	}
	if got := routing.data("ses_old")["channel"]; got != "https://hook/old" {
		t.Fatalf("previous session delivery metadata was not restored: %q", got)
	}
	if _, ok := client.GetSessionForThread("chat-1"); ok {
		t.Fatal("a failed switch must not bind the thread")
	}
	if calls := routing.restoreCalls(); len(calls) != 1 {
		t.Fatalf("expected exactly one restore, got %d", len(calls))
	}
}

// TestSwitchSessionRefusesBusySession pins that a turn in flight is not moved.
// Its output is bound to the session, so redirecting the routing mid-answer
// would deliver the rest of it into a different conversation.
func TestSwitchSessionRefusesBusySession(t *testing.T) {
	fake := newSwitchServer(t, map[string]string{
		"ses_target": "[wechat:alice] target work",
	})
	client := NewClient(fake.URL(), "")
	routing := newFakeSwitchRouting()
	client.SetSessionSwitchRouting(routing)
	client.BindSessionToThread("warmup", "ses_target")
	client.runningSessions.Store("ses_target", true)

	_, err := client.SwitchSession(context.Background(), SessionSwitchRequest{
		Channel:  "wechat",
		UserID:   "alice",
		ThreadID: "chat-1",
		Selector: "ses_target",
	})
	if !errors.Is(err, ErrSessionBusy) {
		t.Fatalf("expected ErrSessionBusy, got %v", err)
	}
	if calls := routing.applyCalls(); len(calls) != 0 {
		t.Fatalf("a busy session must not be mutated, got %d applies", len(calls))
	}
	if _, ok := client.GetSessionForThread("chat-1"); ok {
		t.Fatal("a refused switch must not bind the thread")
	}
}

// TestSwitchSessionRefusesCronTarget pins the S1 guard: a scheduled task's
// session is visible in /sessions but must never become a chat's session.
func TestSwitchSessionRefusesCronTarget(t *testing.T) {
	fake := newSwitchServer(t, map[string]string{
		"ses_cron": "[wechat:alice] cron-task-one",
	})
	client := NewClient(fake.URL(), "")
	routing := newFakeSwitchRouting()
	client.SetSessionSwitchRouting(routing)

	_, err := client.SwitchSession(context.Background(), SessionSwitchRequest{
		Channel:  "wechat",
		UserID:   "alice",
		ThreadID: "chat-1",
		Selector: "ses_cron",
	})
	if err == nil {
		t.Fatal("a cron session was accepted as the current chat session")
	}
	if !strings.Contains(err.Error(), "cron") {
		t.Fatalf("error = %v, want it to name the cron guard", err)
	}
	if calls := routing.applyCalls(); len(calls) != 0 {
		t.Fatalf("a refused switch must not touch the adapter, got %d applies", len(calls))
	}
}

// TestSwitchSessionRefusesAnotherUsersSession pins that "not yours" stays
// indistinguishable from "does not exist".
func TestSwitchSessionRefusesAnotherUsersSession(t *testing.T) {
	fake := newSwitchServer(t, map[string]string{
		"ses_theirs": "[wechat:bob] their work",
	})
	client := NewClient(fake.URL(), "")
	client.SetSessionSwitchRouting(newFakeSwitchRouting())

	_, err := client.SwitchSession(context.Background(), SessionSwitchRequest{
		Channel:  "wechat",
		UserID:   "alice",
		ThreadID: "chat-1",
		Selector: "ses_theirs",
	})
	if !errors.Is(err, ErrSessionNotOwned) {
		t.Fatalf("expected ErrSessionNotOwned, got %v", err)
	}
}

// TestSwitchSessionRefusesRestoreClaimOverLiveHandler pins that a restore-claim
// never replaces a registered handler. A handler on an unclaimed session is
// either a per-turn handler whose turn has not been cleaned up yet or a mirror
// this process does not know about; replacing either would drop output.
func TestSwitchSessionRefusesRestoreClaimOverLiveHandler(t *testing.T) {
	fake := newSwitchServer(t, map[string]string{
		"ses_tui": "[wechat:alice] tui work",
	})
	client := NewClient(fake.URL(), "")
	routing := newFakeSwitchRouting()
	client.SetSessionSwitchRouting(routing)
	client.RegisterSessionHandler("ses_tui", func(context.Context, *sdk.EventListResponse) error { return nil })

	_, err := client.SwitchSession(context.Background(), SessionSwitchRequest{
		Channel:  "wechat",
		UserID:   "alice",
		ThreadID: "chat-1",
		Selector: "ses_tui",
	})
	if !errors.Is(err, ErrSessionBusy) {
		t.Fatalf("expected ErrSessionBusy, got %v", err)
	}
	if client.IsClaimedSession("ses_tui") {
		t.Fatal("a refused restore-claim still recorded a claim")
	}
	if calls := routing.applyCalls(); len(calls) != 0 {
		t.Fatalf("a refused restore-claim must not touch the adapter, got %d applies", len(calls))
	}
}

// TestSwitchSessionRefusesRestoreClaimWithoutDeliveryTarget pins the delivery
// pre-check. Some adapters drop a send they cannot route without returning an
// error, so claiming such a session would promise mirroring that never arrives.
func TestSwitchSessionRefusesRestoreClaimWithoutDeliveryTarget(t *testing.T) {
	fake := newSwitchServer(t, map[string]string{
		"ses_tui": "[dingtalk:alice] tui work",
	})
	client := NewClient(fake.URL(), "")
	routing := newFakeSwitchRouting()
	client.SetSessionSwitchRouting(routing)
	routing.validateErr = errors.New("no delivery target")

	_, err := client.SwitchSession(context.Background(), SessionSwitchRequest{
		Channel:  "dingtalk",
		UserID:   "alice",
		ThreadID: "chat-1",
		Selector: "ses_tui",
	})
	if err == nil {
		t.Fatal("a restore-claim without a delivery target was accepted")
	}
	if !strings.Contains(err.Error(), "no delivery target") {
		t.Fatalf("error = %v, want it to name the delivery failure", err)
	}
	if client.IsClaimedSession("ses_tui") {
		t.Fatal("a refused restore-claim still recorded a claim")
	}
	if _, registered := client.sessionHandlers.Load("ses_tui"); registered {
		t.Fatal("a refused restore-claim still registered a handler")
	}
	if calls := routing.applyCalls(); len(calls) != 0 {
		t.Fatalf("validation must run before any mutation, got %d applies", len(calls))
	}
}

// TestSwitchSessionRestoresClaimForTuiSession covers the cold-start case: a
// session adopted from the TUI carries the ownership title but has no local
// claim, so the switch has to restore one for later TUI turns to be mirrored.
func TestSwitchSessionRestoresClaimForTuiSession(t *testing.T) {
	fake := newSwitchServer(t, map[string]string{
		"ses_tui": "[dingtalk:alice] tui work",
	})
	client := NewClient(fake.URL(), "")
	routing := newFakeSwitchRouting()
	client.SetSessionSwitchRouting(routing)

	result, err := client.SwitchSession(context.Background(), SessionSwitchRequest{
		Channel:     "dingtalk",
		UserID:      "alice",
		ThreadID:    "chat-1",
		Selector:    "ses_tui",
		ChannelData: map[string]string{"channel": "https://hook/new"},
	})
	if err != nil {
		t.Fatalf("SwitchSession returned error: %v", err)
	}
	if !result.Claimed {
		t.Fatal("a TUI session with no local claim must be claimed by the switch")
	}
	if !client.IsClaimedSession("ses_tui") {
		t.Fatal("the claim was not recorded")
	}
	if _, registered := client.sessionHandlers.Load("ses_tui"); !registered {
		t.Fatal("no mirroring handler was registered")
	}
	if got := routing.data("ses_tui")["channel"]; got != "https://hook/new" {
		t.Fatalf("delivery context = %q, want the webhook from the inbound message", got)
	}
	calls := routing.validateCalls()
	if len(calls) != 1 {
		t.Fatalf("expected exactly one delivery validation, got %d", len(calls))
	}
	if calls[0].ChannelData["channel"] != "https://hook/new" {
		t.Fatalf("validation did not receive the inbound delivery context: %+v", calls[0])
	}
}

// TestSwitchSessionResumesSuspendedClaim pins that switching back to a session
// an earlier /switch paused resumes it instead of re-claiming it. Re-claiming
// would rewrite the title and replace the long-lived handler for no reason.
func TestSwitchSessionResumesSuspendedClaim(t *testing.T) {
	fake := newSwitchServer(t, map[string]string{
		"ses_tui": "[wechat:alice] tui work",
	})
	client := NewClient(fake.URL(), "")
	routing := newFakeSwitchRouting()
	client.SetSessionSwitchRouting(routing)

	if _, err := client.ClaimSession(context.Background(), "ses_tui", "wechat", "alice", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}
	client.SuspendClaimedSession("ses_tui")
	if !client.IsSuspendedClaimedSession("ses_tui") {
		t.Fatal("setup: the claim was not suspended")
	}

	result, err := client.SwitchSession(context.Background(), SessionSwitchRequest{
		Channel:  "wechat",
		UserID:   "alice",
		ThreadID: "chat-1",
		Selector: "ses_tui",
	})
	if err != nil {
		t.Fatalf("SwitchSession returned error: %v", err)
	}
	if !result.Resumed {
		t.Fatal("switching back to a suspended claim must resume it")
	}
	if result.Claimed {
		t.Fatal("an existing claim must not be re-claimed")
	}
	if client.IsSuspendedClaimedSession("ses_tui") {
		t.Fatal("the claim is still suspended after the switch")
	}
	if _, registered := client.sessionHandlers.Load("ses_tui"); !registered {
		t.Fatal("the mirroring handler was not re-attached")
	}
	if calls := routing.validateCalls(); len(calls) != 0 {
		t.Fatalf("an existing claim needs no delivery validation, got %d", len(calls))
	}
}

// TestSwitchSessionSuspendsOtherClaims pins that one chat cannot have two
// conversations writing into it at once.
func TestSwitchSessionSuspendsOtherClaims(t *testing.T) {
	fake := newSwitchServer(t, map[string]string{
		"ses_a": "[wechat:alice] a",
		"ses_b": "[wechat:alice] b",
	})
	client := NewClient(fake.URL(), "")
	client.SetSessionSwitchRouting(newFakeSwitchRouting())

	for _, id := range []string{"ses_a", "ses_b"} {
		if _, err := client.ClaimSession(context.Background(), id, "wechat", "alice", ""); err != nil {
			t.Fatalf("ClaimSession(%s) returned error: %v", id, err)
		}
	}

	result, err := client.SwitchSession(context.Background(), SessionSwitchRequest{
		Channel:  "wechat",
		UserID:   "alice",
		ThreadID: "chat-1",
		Selector: "ses_a",
	})
	if err != nil {
		t.Fatalf("SwitchSession returned error: %v", err)
	}
	if len(result.Suspended) != 1 || result.Suspended[0] != "ses_b" {
		t.Fatalf("suspended = %#v, want [ses_b]", result.Suspended)
	}
	if !client.IsSuspendedClaimedSession("ses_b") {
		t.Fatal("the other claim is still mirroring into the same chat")
	}
	if client.IsSuspendedClaimedSession("ses_a") {
		t.Fatal("the switch target was suspended")
	}
}

// activeHandler returns the handler currently registered for sessionID.
func activeHandler(t *testing.T, client *Client, sessionID string) *StreamingSessionHandler {
	t.Helper()
	value, ok := client.activeHandlers.Load(sessionID)
	if !ok {
		t.Fatalf("no active handler for %s", sessionID)
	}
	handler, ok := value.(*StreamingSessionHandler)
	if !ok {
		t.Fatalf("activeHandlers holds %T, want *StreamingSessionHandler", value)
	}
	return handler
}

// runTuiTurn drives one complete TUI turn through a mirroring handler.
func runTuiTurn(t *testing.T, client *Client, handler *StreamingSessionHandler, messageID, partID, text string) {
	t.Helper()
	client.messageRoles.Store(messageID, "assistant")
	if err := handler.HandleEvent(context.Background(), deltaEvent(t, messageID, partID, "text", text)); err != nil {
		t.Fatalf("HandleEvent returned error: %v", err)
	}
	if err := handler.HandleEvent(context.Background(), idleEvent(t)); err != nil {
		t.Fatalf("HandleEvent(session.idle) returned error: %v", err)
	}
}

// runChatTurn simulates a chat-initiated turn: SendMessageStreamingWithEvents
// registers its own per-turn handler under the same session ID and unregisters
// it on completion, which is what used to kill the mirror.
func runChatTurn(t *testing.T, client *Client) {
	t.Helper()
	perTurn := NewStreamingSessionHandlerWithModel(
		"ses_claim",
		func(string) error { return nil },
		nil,
		func() {
			client.runningSessions.Delete("ses_claim")
			client.probeSnapshots.Delete("ses_claim")
			client.activeHandlers.Delete("ses_claim")
			client.UnregisterSessionHandler("ses_claim")
			client.restoreClaimedHandler("ses_claim")
		},
		client, client, true, false, nil,
		MessagePayload{Channel: "dingtalk", UserID: "alice", SessionID: "ses_claim"},
	)
	client.RegisterSessionHandler("ses_claim", perTurn.HandleEvent)
	client.activeHandlers.Store("ses_claim", perTurn)
	perTurn.fireOnComplete()
	<-perTurn.cleanupDone
}

// TestSwitchKeepsMirroringAcrossTuiAndChatTurns is the S4 acceptance sequence:
// TUI turn → channel reply → channel turn → TUI turn → channel still receives,
// with a /switch in the middle.
func TestSwitchKeepsMirroringAcrossTuiAndChatTurns(t *testing.T) {
	fake := newSwitchServer(t, map[string]string{
		"ses_claim": "[dingtalk:alice] tui work",
	})
	client := NewClient(fake.URL(), "")
	client.SetSessionSwitchRouting(newFakeSwitchRouting())

	recorder := &claimDeliveryRecorder{}
	client.SetClaimedSessionSender(recorder.send)

	if _, err := client.ClaimSession(context.Background(), "ses_claim", "dingtalk", "alice", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}

	// 1. A TUI turn reaches the chat.
	runTuiTurn(t, client, activeHandler(t, client, "ses_claim"), "msg_1", "part_1", "tui one")

	// 2. The chat replies, replacing and then restoring the mirroring handler.
	runChatTurn(t, client)

	// 3. The next TUI turn still reaches the chat.
	runTuiTurn(t, client, activeHandler(t, client, "ses_claim"), "msg_2", "part_2", "tui two")

	// 4. /switch to the same session must not disturb the mirror.
	result, err := client.SwitchSession(context.Background(), SessionSwitchRequest{
		Channel:  "dingtalk",
		UserID:   "alice",
		ThreadID: "chat-1",
		Selector: "ses_claim",
	})
	if err != nil {
		t.Fatalf("SwitchSession returned error: %v", err)
	}
	if result.Claimed {
		t.Fatal("switching to an already-claimed session must not re-claim it")
	}

	// 5. And the mirror is still live afterwards.
	runTuiTurn(t, client, activeHandler(t, client, "ses_claim"), "msg_3", "part_3", "tui three")

	got := recorder.joined()
	for _, want := range []string{"tui one", "tui two", "tui three"} {
		if !strings.Contains(got, want) {
			t.Fatalf("delivered %q, want it to contain %q", got, want)
		}
	}
}
