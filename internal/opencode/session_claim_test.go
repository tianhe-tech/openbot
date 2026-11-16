package opencode

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// claimRecorder captures the requests a claim makes against the fake upstream.
type claimRecorder struct {
	mu       sync.Mutex
	patches  []string // raw JSON bodies of PATCH /session/{id}
	gets     int
	title    string // title returned by GET /session/{id}
	patchErr int    // when non-zero, PATCH responds with this status
}

func (r *claimRecorder) recordPatch(body string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.patches = append(r.patches, body)
}

func (r *claimRecorder) patchBodies() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.patches))
	copy(out, r.patches)
	return out
}

func (r *claimRecorder) getCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.gets
}

// claimServer serves GET and PATCH /session/{id} for a single session.
func claimServer(t *testing.T, rec *claimRecorder) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if !strings.HasPrefix(r.URL.Path, "/session/") {
			// NewClient starts a background health check that hits GET /session.
			if r.URL.Path == "/session" {
				_, _ = w.Write([]byte("[]"))
				return
			}
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}

		switch r.Method {
		case http.MethodGet:
			rec.mu.Lock()
			rec.gets++
			title := rec.title
			rec.mu.Unlock()
			_, _ = w.Write([]byte(sessionJSON("ses_claim", title, 1794209871904)))
		case http.MethodPatch:
			body, _ := io.ReadAll(r.Body)
			rec.recordPatch(string(body))
			if rec.patchErr != 0 {
				w.WriteHeader(rec.patchErr)
				_, _ = w.Write([]byte(`{"error":"boom"}`))
				return
			}
			var payload struct {
				Title string `json:"title"`
			}
			_ = json.Unmarshal(body, &payload)
			// Persist the new title so a follow-up GET sees it. TransferSession
			// rewrites the title and then claims, and the claim reads the title
			// back — without this the fake would report the pre-transfer title
			// and the claim would stack a second prefix.
			rec.mu.Lock()
			rec.title = payload.Title
			rec.mu.Unlock()
			_, _ = w.Write([]byte(sessionJSON("ses_claim", payload.Title, 1794209871904)))
		default:
			t.Errorf("unexpected method: %s", r.Method)
			http.NotFound(w, r)
		}
	}))
}

func TestClaimSessionRewritesTitle(t *testing.T) {
	rec := &claimRecorder{title: "my tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	title, err := client.ClaimSession(context.Background(), "ses_claim", "dingtalk", "alice", "")
	if err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}

	bodies := rec.patchBodies()
	if len(bodies) != 1 {
		t.Fatalf("expected exactly 1 PATCH, got %d: %#v", len(bodies), bodies)
	}
	if !strings.Contains(bodies[0], `"[dingtalk:alice] my tui work"`) {
		t.Fatalf("PATCH body did not carry the ownership prefix: %s", bodies[0])
	}
	if title != "[dingtalk:alice] my tui work" {
		t.Fatalf("returned title = %q, want the prefixed title", title)
	}
}

func TestClaimRejectsCronSessionBeforeRouting(t *testing.T) {
	rec := &claimRecorder{title: "[feishu:alice] cron-task-one"}
	server := claimServer(t, rec)
	defer server.Close()
	client := NewClient(server.URL, "")
	hooked := false
	client.SetClaimedSessionHook(func(context.Context, string, string, string, string) error {
		hooked = true
		return nil
	})
	if _, err := client.ClaimSession(context.Background(), "ses_claim", "feishu", "alice", ""); err == nil {
		t.Fatal("cron session was accepted as a chat session")
	}
	if hooked || client.IsClaimedSession("ses_claim") || len(rec.patchBodies()) != 0 {
		t.Fatal("rejected cron claim changed routing, title or handlers")
	}
}

func TestTransferFailurePreservesSource(t *testing.T) {
	recorder := &claimRecorder{title: "[wechat:alice] work", patchErr: http.StatusBadRequest}
	server := claimServer(t, recorder)
	defer server.Close()
	client := NewClient(server.URL, "")
	if _, err := client.ClaimSession(context.Background(), "ses_claim", "wechat", "alice", ""); err != nil {
		t.Fatal(err)
	}
	client.BindSessionToThread("source", "ses_claim")
	if _, err := client.TransferSession(context.Background(), "ses_claim", "feishu", "bob"); err == nil {
		t.Fatal("expected PATCH failure")
	}
	if !client.IsClaimedSession("ses_claim") {
		t.Fatal("failed transfer removed source claim")
	}
	if _, ok := client.sessionHandlers.Load("ses_claim"); !ok {
		t.Fatal("failed transfer removed source handler")
	}
}

func TestTransferClearsSourceThread(t *testing.T) {
	recorder := &claimRecorder{title: "[wechat:alice] work"}
	server := claimServer(t, recorder)
	defer server.Close()
	client := NewClient(server.URL, "")
	if _, err := client.ClaimSession(context.Background(), "ses_claim", "wechat", "alice", ""); err != nil {
		t.Fatal(err)
	}
	client.BindSessionToThread("source", "ses_claim")
	if _, err := client.TransferSession(context.Background(), "ses_claim", "feishu", "bob"); err != nil {
		t.Fatal(err)
	}
	if _, ok := client.sessions.Load("source"); ok {
		t.Fatal("transferred session still bound to source thread")
	}
}

// TestTransferRejectsSourceThreadEvenIfBindingSurvives is the second half of the
// S01 fix: clearing the thread binding at transfer time is not enough on its
// own, because a binding can outlive the transfer (a restart mid-transfer, an
// adapter holding its own copy, a re-claim by another chat after /release).
// The send entry must therefore check the CURRENT owner before reusing a
// thread→session fallback.
func TestTransferRejectsSourceThreadEvenIfBindingSurvives(t *testing.T) {
	recorder := &claimRecorder{title: "[wechat:alice] work"}
	server := claimServer(t, recorder)
	defer server.Close()
	client := NewClient(server.URL, "")

	if _, err := client.ClaimSession(context.Background(), "ses_claim", "wechat", "alice", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := client.TransferSession(context.Background(), "ses_claim", "feishu", "bob"); err != nil {
		t.Fatal(err)
	}
	// Simulate the binding surviving the transfer.
	client.BindSessionToThread("source", "ses_claim")

	if got, ok := client.resolveThreadSession("source", "wechat", "alice"); ok {
		t.Fatalf("source chat resolved the transferred session %s", got)
	}
	// The rejected binding must be dropped, not merely ignored, or every later
	// message from this thread re-runs the same rejected lookup.
	if _, ok := client.sessions.Load("source"); ok {
		t.Fatal("a rejected thread binding was left in place")
	}
	// The new owner may use it.
	client.BindSessionToThread("target", "ses_claim")
	if got, ok := client.resolveThreadSession("target", "feishu", "bob"); !ok || got != "ses_claim" {
		t.Fatalf("target chat could not resolve its own session: %q ok=%t", got, ok)
	}
}

// TestResolveThreadSessionAllowsUnclaimedSessions keeps the ownership check from
// breaking ordinary gateway-created sessions, which are bound to exactly one
// thread and never appear in claimedSessions.
func TestResolveThreadSessionAllowsUnclaimedSessions(t *testing.T) {
	client := NewClient("http://127.0.0.1:1", "")
	client.BindSessionToThread("thread", "ses_plain")

	if got, ok := client.resolveThreadSession("thread", "wechat", "alice"); !ok || got != "ses_plain" {
		t.Fatalf("resolveThreadSession() = %q, %t for an unclaimed session", got, ok)
	}
	if _, ok := client.resolveThreadSession("", "wechat", "alice"); ok {
		t.Fatal("resolveThreadSession() resolved an empty thread")
	}
	if _, ok := client.resolveThreadSession("unknown", "wechat", "alice"); ok {
		t.Fatal("resolveThreadSession() resolved an unbound thread")
	}
}

// TestExplicitSessionOwnershipRejectsOtherChat covers the adapter-supplied
// SessionID path. /transfer clears the source chat's adapter map, but a second
// chat that had adopted the same session — or a stale map after a restart —
// would otherwise keep driving a session that no longer belongs to it.
func TestExplicitSessionOwnershipRejectsOtherChat(t *testing.T) {
	rec := &claimRecorder{title: "[wechat:alice] work"}
	server := claimServer(t, rec)
	defer server.Close()
	client := NewClient(server.URL, "")

	if _, err := client.ClaimSession(context.Background(), "ses_claim", "wechat", "alice", ""); err != nil {
		t.Fatal(err)
	}

	// The owner may send.
	if err := client.checkExplicitSessionOwnership(MessagePayload{
		Channel: "wechat", UserID: "alice", SessionID: "ses_claim",
	}); err != nil {
		t.Fatalf("the owning chat was rejected: %v", err)
	}
	// Another chat may not.
	if err := client.checkExplicitSessionOwnership(MessagePayload{
		Channel: "feishu", UserID: "bob", SessionID: "ses_claim",
	}); err == nil {
		t.Fatal("a different chat was allowed to drive a claimed session")
	}
	// An unclaimed session is unrestricted: gateway-created sessions are never
	// in claimedSessions.
	if err := client.checkExplicitSessionOwnership(MessagePayload{
		Channel: "feishu", UserID: "bob", SessionID: "ses_plain",
	}); err != nil {
		t.Fatalf("an unclaimed session was rejected: %v", err)
	}
	// Internal gateway work targets sessions it created itself.
	if err := client.checkExplicitSessionOwnership(MessagePayload{
		Channel: "scheduler", UserID: "system", SessionID: "ses_claim", Internal: true,
	}); err != nil {
		t.Fatalf("an internal payload was rejected: %v", err)
	}
}

func TestTransferIgnoresOldTitleDuringPatch(t *testing.T) {
	recorder := &claimRecorder{title: "[wechat:alice] work"}
	upstream := claimServer(t, recorder)
	defer upstream.Close()
	var client *Client
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method == http.MethodPatch {
			if _, err := client.AutoClaimFromTitle(context.Background(), sessionUpdatedEvent(t, "ses_claim", "[wechat:alice] work")); err != nil {
				t.Error(err)
			}
		}
		upstream.Config.Handler.ServeHTTP(writer, request)
	}))
	defer server.Close()
	client = NewClient(server.URL, "")
	if _, err := client.ClaimSession(context.Background(), "ses_claim", "wechat", "alice", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := client.TransferSession(context.Background(), "ses_claim", "feishu", "bob"); err != nil {
		t.Fatal(err)
	}
	value, _ := client.claimedSessions.Load("ses_claim")
	claimed := value.(claimedSession)
	if claimed.Channel != "feishu" || claimed.UserID != "bob" {
		t.Fatalf("wrong target: %+v", claimed)
	}
}

func TestClaimSessionIsIdempotent(t *testing.T) {
	rec := &claimRecorder{title: "[dingtalk:alice] already mine"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	title, err := client.ClaimSession(context.Background(), "ses_claim", "dingtalk", "alice", "")
	if err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}

	if bodies := rec.patchBodies(); len(bodies) != 0 {
		t.Fatalf("an already-claimed session must not be re-PATCHed, got %#v", bodies)
	}
	if title != "[dingtalk:alice] already mine" {
		t.Fatalf("returned title = %q, want the existing title unchanged", title)
	}
}

func TestClaimSessionRequiresIdentity(t *testing.T) {
	rec := &claimRecorder{title: "x"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	cases := []struct{ name, sessionID, channel, userID string }{
		{"no session", "", "dingtalk", "alice"},
		{"no channel", "ses_claim", "", "alice"},
		{"no user", "ses_claim", "dingtalk", ""},
		{"whitespace only", "  ", "dingtalk", "alice"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := client.ClaimSession(context.Background(), tc.sessionID, tc.channel, tc.userID, ""); err == nil {
				t.Fatalf("expected an error for %s", tc.name)
			}
		})
	}

	if rec.getCount() != 0 {
		t.Fatalf("validation must reject before touching the upstream, got %d GETs", rec.getCount())
	}
}

func TestClaimSessionRegistersForwardingHandler(t *testing.T) {
	rec := &claimRecorder{title: "tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	if _, err := client.ClaimSession(context.Background(), "ses_claim", "feishu", "bob", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}

	// Without a registered handler, eventListenerLoop drops every event for the
	// session with "no session handler found" — which is the whole reason a TUI
	// session never reached a chat adapter.
	if _, ok := client.sessionHandlers.Load("ses_claim"); !ok {
		t.Fatal("claim did not register a session handler; the session's events would be dropped")
	}
	if _, ok := client.activeHandlers.Load("ses_claim"); !ok {
		t.Fatal("claim did not record the handler in activeHandlers")
	}
	if got := client.ClaimedSessions(); len(got) != 1 || got[0] != "ses_claim" {
		t.Fatalf("ClaimedSessions() = %#v, want [ses_claim]", got)
	}
}

func TestClaimSessionRunsHookBeforeHandler(t *testing.T) {
	rec := &claimRecorder{title: "tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	var hookChannel, hookUser, hookData string
	client.SetClaimedSessionHook(func(ctx context.Context, sessionID, channel, userID, channelData string) error {
		hookChannel, hookUser, hookData = channel, userID, channelData
		return nil
	})

	if _, err := client.ClaimSession(context.Background(), "ses_claim", "wechat", "carol", "chat-42"); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}

	if hookChannel != "wechat" || hookUser != "carol" || hookData != "chat-42" {
		t.Fatalf("hook got (%q, %q, %q), want (wechat, carol, chat-42)", hookChannel, hookUser, hookData)
	}
}

func TestClaimSessionDeliversThroughSender(t *testing.T) {
	rec := &claimRecorder{title: "tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	type delivery struct{ channel, userID, content string }
	var got []delivery
	client.SetClaimedSessionSender(func(ctx context.Context, sessionID, channel, userID, content string) error {
		got = append(got, delivery{channel, userID, content})
		return nil
	})

	if _, err := client.ClaimSession(context.Background(), "ses_claim", "dingtalk", "dave", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}

	if err := client.SendMessageToSession(context.Background(), "ses_claim", "hello from the TUI"); err != nil {
		t.Fatalf("SendMessageToSession returned error: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 delivery, got %d: %#v", len(got), got)
	}
	if got[0].channel != "dingtalk" || got[0].userID != "dave" || got[0].content != "hello from the TUI" {
		t.Fatalf("delivery = %#v, want the claim's channel/user and the content", got[0])
	}
}

func TestSendMessageToSessionIgnoresUnclaimedSession(t *testing.T) {
	rec := &claimRecorder{title: "x"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	called := false
	client.SetClaimedSessionSender(func(ctx context.Context, sessionID, channel, userID, content string) error {
		called = true
		return nil
	})

	// A session the gateway created delivers through its own adapter callback,
	// never through this path. Sending here must be a no-op, not a misdelivery.
	if err := client.SendMessageToSession(context.Background(), "ses_unknown", "text"); err != nil {
		t.Fatalf("SendMessageToSession returned error: %v", err)
	}
	if called {
		t.Fatal("an unclaimed session must not be delivered through the claimed-session sender")
	}
}

func TestReleaseClaimedSessionUnregistersHandler(t *testing.T) {
	rec := &claimRecorder{title: "tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	if _, err := client.ClaimSession(context.Background(), "ses_claim", "wecom", "erin", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}

	client.ReleaseClaimedSession("ses_claim")

	if _, ok := client.sessionHandlers.Load("ses_claim"); ok {
		t.Fatal("release left the session handler registered")
	}
	if got := client.ClaimedSessions(); len(got) != 0 {
		t.Fatalf("ClaimedSessions() = %#v after release, want empty", got)
	}
}

func TestClaimSessionPropagatesPatchFailure(t *testing.T) {
	rec := &claimRecorder{title: "tui work", patchErr: http.StatusInternalServerError}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	if _, err := client.ClaimSession(context.Background(), "ses_claim", "dingtalk", "frank", ""); err == nil {
		t.Fatal("expected an error when the title rewrite fails")
	}

	// A failed claim must not leave a handler behind: the session is not owned,
	// so forwarding its output would leak another user's conversation.
	if _, ok := client.sessionHandlers.Load("ses_claim"); ok {
		t.Fatal("a failed claim registered a handler anyway")
	}
}

func TestUnclaimSessionStripsPrefixAndReleases(t *testing.T) {
	rec := &claimRecorder{title: "[feishu:alice] my tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	if _, err := client.ClaimSession(context.Background(), "ses_claim", "feishu", "alice", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}
	if !client.IsClaimedSession("ses_claim") {
		t.Fatal("IsClaimedSession() = false right after a successful claim")
	}

	title, err := client.UnclaimSession(context.Background(), "ses_claim")
	if err != nil {
		t.Fatalf("UnclaimSession returned error: %v", err)
	}

	// The prefix must be gone: AutoClaimFromTitle keys on it, so leaving it in
	// place would let the next session.updated event re-claim the session.
	if title != "my tui work" {
		t.Fatalf("returned title = %q, want the prefix stripped", title)
	}
	bodies := rec.patchBodies()
	if len(bodies) != 1 {
		t.Fatalf("expected exactly 1 PATCH (the strip), got %d: %#v", len(bodies), bodies)
	}
	if !strings.Contains(bodies[0], `"my tui work"`) {
		t.Fatalf("PATCH body did not carry the stripped title: %s", bodies[0])
	}

	if client.IsClaimedSession("ses_claim") {
		t.Fatal("IsClaimedSession() = true after release")
	}
	if _, ok := client.sessionHandlers.Load("ses_claim"); ok {
		t.Fatal("unclaim left the session handler registered")
	}
	if got := client.ClaimedSessions(); len(got) != 0 {
		t.Fatalf("ClaimedSessions() = %#v after unclaim, want empty", got)
	}
}

func TestUnclaimSessionLeavesUnprefixedTitleAlone(t *testing.T) {
	rec := &claimRecorder{title: "plain title"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	title, err := client.UnclaimSession(context.Background(), "ses_claim")
	if err != nil {
		t.Fatalf("UnclaimSession returned error: %v", err)
	}
	if title != "plain title" {
		t.Fatalf("returned title = %q, want it unchanged", title)
	}
	if bodies := rec.patchBodies(); len(bodies) != 0 {
		t.Fatalf("a title with no prefix must not be rewritten, got %#v", bodies)
	}
}

func TestUnclaimSessionFallsBackWhenOnlyPrefix(t *testing.T) {
	rec := &claimRecorder{title: "[wechat:bob]"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	title, err := client.UnclaimSession(context.Background(), "ses_claim")
	if err != nil {
		t.Fatalf("UnclaimSession returned error: %v", err)
	}
	// opencode needs a non-empty title, so the session ID is the fallback name.
	if !strings.HasPrefix(title, "session ses_clai") {
		t.Fatalf("returned title = %q, want a session-ID fallback name", title)
	}
}

func TestIsClaimedSessionRejectsUnknownAndEmpty(t *testing.T) {
	rec := &claimRecorder{title: "x"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	if client.IsClaimedSession("") {
		t.Fatal("IsClaimedSession(\"\") = true")
	}
	if client.IsClaimedSession("ses_never_claimed") {
		t.Fatal("IsClaimedSession() = true for a session that was never claimed")
	}
}

// TestTransferSessionMovesClaimToAnotherChannel is the property /transfer
// exists for: a session mirrored into one chat can be moved to another chat
// without going back to the TUI.
func TestTransferSessionMovesClaimToAnotherChannel(t *testing.T) {
	rec := &claimRecorder{title: "[wechat:carol] my tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	var hookChannel, hookUser string
	client.SetClaimedSessionHook(func(ctx context.Context, sessionID, channel, userID, channelData string) error {
		hookChannel, hookUser = channel, userID
		return nil
	})

	if _, err := client.ClaimSession(context.Background(), "ses_claim", "wechat", "carol", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}

	title, err := client.TransferSession(context.Background(), "ses_claim", "feishu", "ou_alice")
	if err != nil {
		t.Fatalf("TransferSession returned error: %v", err)
	}

	// The original text must survive; only the ownership prefix changes.
	if title != "[feishu:ou_alice] my tui work" {
		t.Fatalf("returned title = %q, want the feishu prefix on the original text", title)
	}
	if hookChannel != "feishu" || hookUser != "ou_alice" {
		t.Fatalf("hook got (%q, %q), want (feishu, ou_alice)", hookChannel, hookUser)
	}

	value, ok := client.claimedSessions.Load("ses_claim")
	if !ok {
		t.Fatal("the transfer did not leave the session claimed")
	}
	if got := value.(claimedSession); got.Channel != "feishu" || got.UserID != "ou_alice" {
		t.Fatalf("claim target = %s/%s, want feishu/ou_alice", got.Channel, got.UserID)
	}
	if _, ok := client.sessionHandlers.Load("ses_claim"); !ok {
		t.Fatal("the transfer left no forwarding handler registered")
	}

	// Exactly one PATCH: the transfer's rewrite. The follow-up claim must see
	// the prefix it just wrote and skip its own rewrite.
	if bodies := rec.patchBodies(); len(bodies) != 1 {
		t.Fatalf("expected exactly 1 PATCH, got %d: %#v", len(bodies), bodies)
	}
}

// TestTransferSessionDoesNotStackPrefixes guards the second-transfer case: the
// title already carries a prefix, and the new one must replace it rather than
// prepend to it.
func TestTransferSessionDoesNotStackPrefixes(t *testing.T) {
	rec := &claimRecorder{title: "[wechat:carol] my tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	if _, err := client.ClaimSession(context.Background(), "ses_claim", "wechat", "carol", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}
	if _, err := client.TransferSession(context.Background(), "ses_claim", "dingtalk", "dave"); err != nil {
		t.Fatalf("first TransferSession returned error: %v", err)
	}
	title, err := client.TransferSession(context.Background(), "ses_claim", "feishu", "ou_alice")
	if err != nil {
		t.Fatalf("second TransferSession returned error: %v", err)
	}
	if title != "[feishu:ou_alice] my tui work" {
		t.Fatalf("returned title = %q, want a single prefix", title)
	}
}

// TestTransferSessionResolvesChannelOnlyTarget covers the single-user form
// "/transfer feishu": the target user comes from the host's resolver.
func TestTransferSessionResolvesChannelOnlyTarget(t *testing.T) {
	rec := &claimRecorder{title: "[wechat:carol] my tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	client.SetClaimUserResolver(func(ctx context.Context, channel string) (string, bool) {
		if channel != "feishu" {
			return "", false
		}
		return "ou_alice", true
	})

	if _, err := client.ClaimSession(context.Background(), "ses_claim", "wechat", "carol", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}

	title, err := client.TransferSession(context.Background(), "ses_claim", "feishu", "")
	if err != nil {
		t.Fatalf("TransferSession returned error: %v", err)
	}
	if title != "[feishu:ou_alice] my tui work" {
		t.Fatalf("returned title = %q, want the resolved user in the prefix", title)
	}
}

// TestTransferSessionRefusesUnknownChannel is the safety property: a typo must
// be rejected BEFORE the title is rewritten. Otherwise the session would end up
// claimed by a channel that has no adapter — invisible in every /sessions list
// and unreachable from the chat it was moved away from.
func TestTransferSessionRefusesUnknownChannel(t *testing.T) {
	rec := &claimRecorder{title: "[wechat:carol] my tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	client.SetClaimChannelChecker(func(channel string) bool {
		return channel == "feishu" || channel == "wechat"
	})

	if _, err := client.ClaimSession(context.Background(), "ses_claim", "wechat", "carol", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}
	patchesBefore := len(rec.patchBodies())

	if _, err := client.TransferSession(context.Background(), "ses_claim", "feshu", "ou_alice"); err == nil {
		t.Fatal("expected an error for an unknown channel")
	}
	if got := len(rec.patchBodies()); got != patchesBefore {
		t.Fatalf("an unknown channel still rewrote the title (%d -> %d PATCHes)", patchesBefore, got)
	}

	// The original claim must be intact: a rejected transfer is a no-op.
	value, ok := client.claimedSessions.Load("ses_claim")
	if !ok {
		t.Fatal("a rejected transfer dropped the existing claim")
	}
	if got := value.(claimedSession); got.Channel != "wechat" || got.UserID != "carol" {
		t.Fatalf("claim target = %s/%s after a rejected transfer, want wechat/carol", got.Channel, got.UserID)
	}
}

// TestTransferSessionRefusesAmbiguousChannelOnlyTarget is the other half of the
// safety property: with no resolvable single user, the transfer must fail
// rather than pick someone.
func TestTransferSessionRefusesAmbiguousChannelOnlyTarget(t *testing.T) {
	rec := &claimRecorder{title: "[wechat:carol] my tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	client.SetClaimUserResolver(func(ctx context.Context, channel string) (string, bool) {
		return "", false
	})

	if _, err := client.ClaimSession(context.Background(), "ses_claim", "wechat", "carol", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}
	patchesBefore := len(rec.patchBodies())

	if _, err := client.TransferSession(context.Background(), "ses_claim", "feishu", ""); err == nil {
		t.Fatal("expected an error when the channel has no single user")
	}
	if got := len(rec.patchBodies()); got != patchesBefore {
		t.Fatalf("an ambiguous target still rewrote the title (%d -> %d PATCHes)", patchesBefore, got)
	}
}

// TestTransferSessionRequiresChannel covers the argument validation.
func TestTransferSessionRequiresChannel(t *testing.T) {
	rec := &claimRecorder{title: "x"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	if _, err := client.TransferSession(context.Background(), "ses_claim", "", "alice"); err == nil {
		t.Fatal("expected an error when the channel is empty")
	}
	if _, err := client.TransferSession(context.Background(), "", "feishu", "alice"); err == nil {
		t.Fatal("expected an error when the session ID is empty")
	}
	if _, err := client.TransferSession(context.Background(), "ses_claim", "skillgen", "alice"); err == nil {
		t.Fatal("expected an error for the non-routable skillgen channel")
	}
}

// TestSuspendClaimedSessionDetachesHandler is the /switch property: moving away
// from a claimed session must stop it writing into the chat, but must NOT give
// up the claim — the title prefix is what keeps it in /sessions.
func TestSuspendClaimedSessionDetachesHandler(t *testing.T) {
	rec := &claimRecorder{title: "tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	if _, err := client.ClaimSession(context.Background(), "ses_claim", "feishu", "alice", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}
	patchesBefore := len(rec.patchBodies())

	if !client.SuspendClaimedSession("ses_claim") {
		t.Fatal("SuspendClaimedSession() = false for a claimed session")
	}

	if _, ok := client.sessionHandlers.Load("ses_claim"); ok {
		t.Fatal("suspend left the forwarding handler registered")
	}
	if _, ok := client.activeHandlers.Load("ses_claim"); ok {
		t.Fatal("suspend left the active handler recorded")
	}
	// The claim itself must survive: /sessions and /switch both depend on it.
	if !client.IsClaimedSession("ses_claim") {
		t.Fatal("suspend dropped the claim, which would remove the session from /sessions")
	}
	if !client.IsSuspendedClaimedSession("ses_claim") {
		t.Fatal("IsSuspendedClaimedSession() = false after a suspend")
	}
	// Suspending must not touch the title: no PATCH at all.
	if got := len(rec.patchBodies()); got != patchesBefore {
		t.Fatalf("suspend rewrote the title (%d -> %d PATCHes)", patchesBefore, got)
	}
}

// TestSuspendClaimedSessionDropsDelivery is the belt to the detached handler's
// braces: a flush that races the suspend must not reach the chat.
func TestSuspendClaimedSessionDropsDelivery(t *testing.T) {
	rec := &claimRecorder{title: "tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	delivered := 0
	client.SetClaimedSessionSender(func(ctx context.Context, sessionID, channel, userID, content string) error {
		delivered++
		return nil
	})

	if _, err := client.ClaimSession(context.Background(), "ses_claim", "feishu", "alice", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}
	client.SuspendClaimedSession("ses_claim")

	if err := client.SendMessageToSession(context.Background(), "ses_claim", "late flush"); err != nil {
		t.Fatalf("SendMessageToSession returned error: %v", err)
	}
	if delivered != 0 {
		t.Fatalf("a suspended session delivered %d messages, want 0", delivered)
	}
}

// TestResumeClaimedSessionReattachesHandler covers switching back.
func TestResumeClaimedSessionReattachesHandler(t *testing.T) {
	rec := &claimRecorder{title: "tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	if _, err := client.ClaimSession(context.Background(), "ses_claim", "feishu", "alice", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}
	client.SuspendClaimedSession("ses_claim")

	if !client.ResumeClaimedSession("ses_claim") {
		t.Fatal("ResumeClaimedSession() = false for a suspended session")
	}
	if _, ok := client.sessionHandlers.Load("ses_claim"); !ok {
		t.Fatal("resume did not re-register the forwarding handler")
	}
	if client.IsSuspendedClaimedSession("ses_claim") {
		t.Fatal("IsSuspendedClaimedSession() = true after a resume")
	}
	// Resuming a session that was never suspended is a no-op, not an error.
	if client.ResumeClaimedSession("ses_claim") {
		t.Fatal("ResumeClaimedSession() = true for a session that was not suspended")
	}
}

// TestSuspendClaimedSessionsForUserSkipsTarget is the multi-claim case: one chat
// can hold several claims, and /switch must pause all of them except the one it
// is switching to.
func TestSuspendClaimedSessionsForUserSkipsTarget(t *testing.T) {
	rec := &claimRecorder{title: "tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")

	// Two claims for the same chat, one for a different user.
	for _, id := range []string{"ses_a", "ses_b"} {
		if _, err := client.ClaimSession(context.Background(), id, "feishu", "alice", ""); err != nil {
			t.Fatalf("ClaimSession(%s) returned error: %v", id, err)
		}
	}
	if _, err := client.ClaimSession(context.Background(), "ses_other", "feishu", "bob", ""); err != nil {
		t.Fatalf("ClaimSession(ses_other) returned error: %v", err)
	}

	suspended := client.SuspendClaimedSessionsForUser("feishu", "alice", "ses_b")
	if len(suspended) != 1 || suspended[0] != "ses_a" {
		t.Fatalf("suspended = %#v, want [ses_a]", suspended)
	}
	if client.IsSuspendedClaimedSession("ses_b") {
		t.Fatal("the switch target was suspended")
	}
	if client.IsSuspendedClaimedSession("ses_other") {
		t.Fatal("another user's claim was suspended")
	}
}

// TestRestoreClaimedHandlerRespectsSuspend guards the interaction with the
// per-turn handler: a turn completing must not silently un-pause a session the
// user switched away from.
func TestRestoreClaimedHandlerRespectsSuspend(t *testing.T) {
	rec := &claimRecorder{title: "tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	if _, err := client.ClaimSession(context.Background(), "ses_claim", "feishu", "alice", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}
	client.SuspendClaimedSession("ses_claim")

	client.restoreClaimedHandler("ses_claim")

	if _, ok := client.sessionHandlers.Load("ses_claim"); ok {
		t.Fatal("restoreClaimedHandler re-attached a suspended session's handler")
	}
}

// TestReleaseClaimedSessionClearsSuspend keeps the two states from leaking into
// each other: a released session must not stay marked as suspended.
func TestReleaseClaimedSessionClearsSuspend(t *testing.T) {
	rec := &claimRecorder{title: "tui work"}
	server := claimServer(t, rec)
	defer server.Close()

	client := NewClient(server.URL, "")
	if _, err := client.ClaimSession(context.Background(), "ses_claim", "feishu", "alice", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}
	client.SuspendClaimedSession("ses_claim")
	client.ReleaseClaimedSession("ses_claim")

	if client.IsSuspendedClaimedSession("ses_claim") {
		t.Fatal("a released session is still marked as suspended")
	}
	if client.IsClaimedSession("ses_claim") {
		t.Fatal("release left the claim in place")
	}
}
