package opencode

import (
	"context"
	"testing"
)

func TestResumeDiscardsUnfinishedTurn(t *testing.T) {
	recorder := &claimDeliveryRecorder{}
	client, previous := claimedHandlerFor(t, recorder)
	client.messageRoles.Store("msg_old", "assistant")
	client.messageRoles.Store("msg_new", "assistant")
	if err := previous.HandleEvent(context.Background(), deltaEvent(t, "msg_old", "part_old", "text", "old turn")); err != nil {
		t.Fatal(err)
	}
	client.SuspendClaimedSession("ses_claim")
	client.ResumeClaimedSession("ses_claim")
	value, _ := client.activeHandlers.Load("ses_claim")
	current := value.(*StreamingSessionHandler)
	if current == previous {
		t.Fatal("resumed unfinished handler")
	}
	if err := current.HandleEvent(context.Background(), deltaEvent(t, "msg_new", "part_new", "text", "new turn")); err != nil {
		t.Fatal(err)
	}
	if err := current.HandleEvent(context.Background(), idleEvent(t)); err != nil {
		t.Fatal(err)
	}
	if err := previous.HandleEvent(context.Background(), idleEvent(t)); err != nil {
		t.Fatal(err)
	}
	if got := recorder.joined(); got != "new turn" {
		t.Fatalf("mixed output: %q", got)
	}
}

// TestClaimedHandlerSurvivesChatInitiatedTurn pins the bidirectional half of the
// TUI→chat handoff.
//
// A claim calls SwitchUserSession, so the user's next chat message resolves to
// the claimed session and goes through SendMessageStreamingWithEvents. That path
// registers its OWN per-turn handler under the same session ID and unregisters
// it on completion — which also removes the claimed handler, because both live
// in the same sessionHandlers map. Without a restore, the mirror dies after the
// first chat reply and every later TUI turn is dropped with
// "no session handler found".
func TestClaimedHandlerSurvivesChatInitiatedTurn(t *testing.T) {
	rec := &claimDeliveryRecorder{}
	client, claimed := claimedHandlerFor(t, rec)

	// Simulate what SendMessageStreamingWithEvents does: replace the claimed
	// handler with a per-turn one, then run its completion callback.
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

	// The per-turn handler finishes and unregisters itself. fireOnComplete runs
	// the callback in a goroutine, so wait for it to signal completion.
	perTurn.fireOnComplete()
	<-perTurn.cleanupDone

	// The claimed handler must be back in both maps. activeHandlers holds the
	// *StreamingSessionHandler, which is a comparable pointer — the func in
	// sessionHandlers is not (method values share one code pointer).
	if _, ok := client.sessionHandlers.Load("ses_claim"); !ok {
		t.Fatal("claimed handler was not restored after the per-turn handler completed")
	}
	active, ok := client.activeHandlers.Load("ses_claim")
	if !ok {
		t.Fatal("activeHandlers lost the claimed handler")
	}
	if active != claimed {
		t.Fatalf("activeHandlers holds %T, want the claimed handler", active)
	}

	// And it must still forward: a TUI turn after the chat reply reaches the chat.
	client.messageRoles.Store("msg_after", "assistant")
	if err := claimed.HandleEvent(context.Background(), deltaEvent(t, "msg_after", "part_after", "text", "still mirrored")); err != nil {
		t.Fatalf("HandleEvent returned error: %v", err)
	}
	if err := claimed.HandleEvent(context.Background(), idleEvent(t)); err != nil {
		t.Fatalf("HandleEvent(session.idle) returned error: %v", err)
	}
	if got := rec.joined(); got != "still mirrored" {
		t.Fatalf("delivered %q, want %q", got, "still mirrored")
	}
}

// TestRestoreClaimedHandlerIgnoresUnclaimedSession makes sure the restore is a
// no-op for ordinary sessions, so it cannot resurrect a handler that was
// deliberately released.
func TestRestoreClaimedHandlerIgnoresUnclaimedSession(t *testing.T) {
	client := NewClient("http://127.0.0.1:1", "")

	client.restoreClaimedHandler("ses_plain")

	if _, ok := client.sessionHandlers.Load("ses_plain"); ok {
		t.Fatal("restoreClaimedHandler registered a handler for an unclaimed session")
	}
	if _, ok := client.activeHandlers.Load("ses_plain"); ok {
		t.Fatal("restoreClaimedHandler recorded an active handler for an unclaimed session")
	}
}

// TestRestoreClaimedHandlerDoesNotClobberLiveHandler pins that the restore only
// fills an empty slot. If another handler is already registered (a newer turn
// started before the old one finished), it must be left alone.
func TestRestoreClaimedHandlerDoesNotClobberLiveHandler(t *testing.T) {
	rec := &claimDeliveryRecorder{}
	client, claimed := claimedHandlerFor(t, rec)

	replacement := NewStreamingSessionHandlerWithModel(
		"ses_claim",
		func(string) error { return nil },
		nil, func() {}, client, client, true, false, nil,
		MessagePayload{Channel: "dingtalk", UserID: "alice", SessionID: "ses_claim"},
	)
	client.RegisterSessionHandler("ses_claim", replacement.HandleEvent)
	client.activeHandlers.Store("ses_claim", replacement)

	client.restoreClaimedHandler("ses_claim")

	active, ok := client.activeHandlers.Load("ses_claim")
	if !ok {
		t.Fatal("activeHandlers lost the live handler")
	}
	if active == claimed {
		t.Fatal("restoreClaimedHandler clobbered a live handler with the claimed one")
	}
	if active != replacement {
		t.Fatalf("activeHandlers holds %T, want the live replacement handler", active)
	}
}
