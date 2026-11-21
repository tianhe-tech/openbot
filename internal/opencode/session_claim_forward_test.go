package opencode

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	sdk "github.com/sst/opencode-sdk-go"
)

// claimDeliveryRecorder captures what a claimed session forwards to the chat.
type claimDeliveryRecorder struct {
	mu       sync.Mutex
	messages []string
}

func (r *claimDeliveryRecorder) send(ctx context.Context, sessionID, channel, userID, content string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.messages = append(r.messages, content)
	return nil
}

func (r *claimDeliveryRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.messages))
	copy(out, r.messages)
	return out
}

func (r *claimDeliveryRecorder) joined() string {
	return strings.Join(r.all(), "")
}

// claimedHandlerFor builds a client with one claimed session and returns the
// registered handler, so a test can drive real SSE events through it.
func claimedHandlerFor(t *testing.T, rec *claimDeliveryRecorder) (*Client, *StreamingSessionHandler) {
	t.Helper()

	claimRec := &claimRecorder{title: "tui work"}
	server := claimServer(t, claimRec)
	t.Cleanup(server.Close)

	client := NewClient(server.URL, "")
	client.SetClaimedSessionSender(rec.send)

	if _, err := client.ClaimSession(context.Background(), "ses_claim", "dingtalk", "alice", ""); err != nil {
		t.Fatalf("ClaimSession returned error: %v", err)
	}

	value, ok := client.activeHandlers.Load("ses_claim")
	if !ok {
		t.Fatal("claim did not record an active handler")
	}
	handler, ok := value.(*StreamingSessionHandler)
	if !ok {
		t.Fatalf("activeHandlers holds %T, want *StreamingSessionHandler", value)
	}
	return client, handler
}

// deltaEvent builds a message.part.delta event, the primary source of
// incremental assistant text.
func deltaEvent(t *testing.T, messageID, partID, field, delta string) *sdk.EventListResponse {
	t.Helper()
	raw := `{"type":"message.part.delta","properties":{"sessionID":"ses_claim","messageID":"` +
		messageID + `","partID":"` + partID + `","field":"` + field + `","delta":` + mustJSON(delta) + `}}`
	event := &sdk.EventListResponse{}
	if err := json.Unmarshal([]byte(raw), event); err != nil {
		t.Fatalf("could not build delta event: %v", err)
	}
	return event
}

func mustJSON(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func TestClaimedSessionForwardsAssistantText(t *testing.T) {
	rec := &claimDeliveryRecorder{}
	_, handler := claimedHandlerFor(t, rec)

	// The assistant message must be known as assistant, otherwise the handler
	// filters it as a user echo.
	handler.client.messageRoles.Store("msg_a", "assistant")

	for _, chunk := range []string{"Hello", " from", " the TUI"} {
		if err := handler.HandleEvent(context.Background(), deltaEvent(t, "msg_a", "part_a", "text", chunk)); err != nil {
			t.Fatalf("HandleEvent returned error: %v", err)
		}
	}

	// Text is buffered until the turn flushes, so nothing has been delivered yet.
	if got := rec.all(); len(got) != 0 {
		t.Fatalf("text was delivered before the turn flushed: %#v", got)
	}

	if err := handler.HandleEvent(context.Background(), idleEvent(t)); err != nil {
		t.Fatalf("HandleEvent(session.idle) returned error: %v", err)
	}

	if got := rec.joined(); got != "Hello from the TUI" {
		t.Fatalf("delivered %q, want %q", got, "Hello from the TUI")
	}
}

func TestClaimedSessionDropsUserEcho(t *testing.T) {
	rec := &claimDeliveryRecorder{}
	_, handler := claimedHandlerFor(t, rec)

	// A user message must never be mirrored back into the chat.
	handler.client.messageRoles.Store("msg_u", "user")

	if err := handler.HandleEvent(context.Background(), deltaEvent(t, "msg_u", "part_u", "text", "my own words")); err != nil {
		t.Fatalf("HandleEvent returned error: %v", err)
	}
	if err := handler.HandleEvent(context.Background(), idleEvent(t)); err != nil {
		t.Fatalf("HandleEvent(session.idle) returned error: %v", err)
	}

	if got := rec.joined(); got != "" {
		t.Fatalf("user echo was forwarded to the chat: %q", got)
	}
}

func TestClaimedSessionDropsProgressSignals(t *testing.T) {
	rec := &claimDeliveryRecorder{}
	_, handler := claimedHandlerFor(t, rec)

	handler.client.messageRoles.Store("msg_a", "assistant")

	// Reasoning deltas are progress, not answer text.
	if err := handler.HandleEvent(context.Background(), deltaEvent(t, "msg_a", "part_r", "reasoning", "thinking hard")); err != nil {
		t.Fatalf("HandleEvent returned error: %v", err)
	}
	if err := handler.HandleEvent(context.Background(), idleEvent(t)); err != nil {
		t.Fatalf("HandleEvent(session.idle) returned error: %v", err)
	}

	if got := rec.joined(); strings.Contains(got, "thinking hard") {
		t.Fatalf("reasoning leaked into the chat: %q", got)
	}
}

func TestClaimedSessionSurvivesMultipleTurns(t *testing.T) {
	rec := &claimDeliveryRecorder{}
	client, handler := claimedHandlerFor(t, rec)

	handler.client.messageRoles.Store("msg_1", "assistant")
	if err := handler.HandleEvent(context.Background(), deltaEvent(t, "msg_1", "part_1", "text", "first turn")); err != nil {
		t.Fatalf("HandleEvent returned error: %v", err)
	}
	if err := handler.HandleEvent(context.Background(), idleEvent(t)); err != nil {
		t.Fatalf("HandleEvent(session.idle) returned error: %v", err)
	}

	// A claimed session is a long-lived mirror: the handler must still be
	// registered after the turn ends, or the next TUI prompt would be dropped.
	if _, ok := client.sessionHandlers.Load("ses_claim"); !ok {
		t.Fatal("the handler was unregistered when the turn completed")
	}

	handler.client.messageRoles.Store("msg_2", "assistant")
	if err := handler.HandleEvent(context.Background(), deltaEvent(t, "msg_2", "part_2", "text", "second turn")); err != nil {
		t.Fatalf("HandleEvent returned error: %v", err)
	}
	if err := handler.HandleEvent(context.Background(), idleEvent(t)); err != nil {
		t.Fatalf("HandleEvent(session.idle) returned error: %v", err)
	}

	got := rec.joined()
	if !strings.Contains(got, "first turn") || !strings.Contains(got, "second turn") {
		t.Fatalf("delivered %q, want both turns", got)
	}
}

// idleEvent builds a session.idle event, which is what ends a turn.
func idleEvent(t *testing.T) *sdk.EventListResponse {
	t.Helper()
	event := &sdk.EventListResponse{}
	if err := json.Unmarshal([]byte(`{"type":"session.idle","properties":{"sessionID":"ses_claim"}}`), event); err != nil {
		t.Fatalf("could not build idle event: %v", err)
	}
	return event
}

// TestClaimedSessionIgnoresUserPromptAsTurnStart pins the role check in
// beginLongLivedTurnLocked: a user message arriving after a turn completed must
// not be mistaken for a new turn, or the handler would reset mid-answer and the
// assistant's reply would be dropped.
func TestClaimedSessionIgnoresUserPromptAsTurnStart(t *testing.T) {
	rec := &claimDeliveryRecorder{}
	_, handler := claimedHandlerFor(t, rec)

	handler.client.messageRoles.Store("msg_a1", "assistant")
	if err := handler.HandleEvent(context.Background(), deltaEvent(t, "msg_a1", "part_a1", "text", "the answer")); err != nil {
		t.Fatalf("HandleEvent returned error: %v", err)
	}
	if err := handler.HandleEvent(context.Background(), idleEvent(t)); err != nil {
		t.Fatalf("HandleEvent(session.idle) returned error: %v", err)
	}

	// The user types the next prompt in the TUI. Its deltas carry a user
	// message ID and must be ignored, not treated as a new turn.
	handler.client.messageRoles.Store("msg_u2", "user")
	if err := handler.HandleEvent(context.Background(), deltaEvent(t, "msg_u2", "part_u2", "text", "next question")); err != nil {
		t.Fatalf("HandleEvent returned error: %v", err)
	}

	// The handler must still consider the previous turn finished, so the
	// assistant's reply to the new prompt starts a fresh turn.
	if !handler.IsCompleted() {
		t.Fatal("a user prompt reset the handler as if it were a new turn")
	}

	handler.client.messageRoles.Store("msg_a2", "assistant")
	if err := handler.HandleEvent(context.Background(), deltaEvent(t, "msg_a2", "part_a2", "text", "the second answer")); err != nil {
		t.Fatalf("HandleEvent returned error: %v", err)
	}
	if err := handler.HandleEvent(context.Background(), idleEvent(t)); err != nil {
		t.Fatalf("HandleEvent(session.idle) returned error: %v", err)
	}

	got := rec.joined()
	if !strings.Contains(got, "the answer") || !strings.Contains(got, "the second answer") {
		t.Fatalf("delivered %q, want both answers", got)
	}
	if strings.Contains(got, "next question") {
		t.Fatalf("the user's own prompt was mirrored into the chat: %q", got)
	}
}
