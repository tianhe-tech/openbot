package wechat

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestOutboundQueueSplitAndDispatchOrder(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "outbound.db")

	var mu sync.Mutex
	sent := make([]string, 0)
	q, err := newOutboundTextQueue(dbPath, 5*time.Millisecond, 1600, func(item *queuedOutboundText) error {
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, item.Content)
		return nil
	}, nil)
	if err != nil {
		t.Fatalf("newOutboundTextQueue failed: %v", err)
	}
	defer q.Stop()
	q.setMinGap(0) // disable pacing for deterministic direct dispatch

	long := repeatRune("你", 3600)
	if err := q.EnqueueText("u1", "s1", "", "final", long, true); err != nil {
		t.Fatalf("enqueue long failed: %v", err)
	}

	for i := 0; i < 3; i++ {
		did, derr := q.dispatchOne()
		if derr != nil {
			t.Fatalf("dispatchOne failed: %v", derr)
		}
		if !did {
			t.Fatalf("expected work at step %d", i)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 3 {
		t.Fatalf("expected 3 chunks sent, got %d", len(sent))
	}
	for i, chunk := range sent {
		if got := len([]rune(chunk)); got > 1600 {
			t.Fatalf("chunk %d len=%d exceeds 1600", i, got)
		}
	}
	if len([]rune(sent[0])) != 1600 || len([]rune(sent[1])) != 1600 || len([]rune(sent[2])) != 400 {
		t.Fatalf("unexpected chunk lengths: [%d,%d,%d]", len([]rune(sent[0])), len([]rune(sent[1])), len([]rune(sent[2])))
	}

	pending, err := q.pendingCount()
	if err != nil {
		t.Fatalf("pendingCount failed: %v", err)
	}
	if pending != 0 {
		t.Fatalf("expected queue empty, pending=%d", pending)
	}
}

func TestOutboundQueueRetryOnRateLimit(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "outbound.db")

	attempts := 0
	q, err := newOutboundTextQueue(dbPath, 5*time.Millisecond, 1600, func(item *queuedOutboundText) error {
		attempts++
		if attempts == 1 {
			return ErrRateLimited
		}
		return nil
	}, nil)
	if err != nil {
		t.Fatalf("newOutboundTextQueue failed: %v", err)
	}
	defer q.Stop()
	q.setMinGap(0) // disable pacing for deterministic direct dispatch

	if err := q.EnqueueText("u1", "s1", "", "final", "hello", true); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	did, err := q.dispatchOne()
	if err != nil {
		t.Fatalf("first dispatch failed: %v", err)
	}
	if !did {
		t.Fatal("first dispatch should do work")
	}

	pending, err := q.pendingCount()
	if err != nil {
		t.Fatalf("pendingCount failed: %v", err)
	}
	if pending != 1 {
		t.Fatalf("expected 1 pending after rate-limit, got %d", pending)
	}

	// Force due immediately for second dispatch in test.
	if _, err := q.db.Exec(`UPDATE wechat_outbound_queue SET next_attempt_at = ?`, time.Now().UnixMilli()); err != nil {
		t.Fatalf("force due failed: %v", err)
	}

	// Clear the rate-limit cooldown so the second dispatch is not skipped.
	q.ClearUserCooldown("u1")

	did, err = q.dispatchOne()
	if err != nil {
		t.Fatalf("second dispatch failed: %v", err)
	}
	if !did {
		t.Fatal("second dispatch should do work")
	}

	pending, err = q.pendingCount()
	if err != nil {
		t.Fatalf("pendingCount failed: %v", err)
	}
	if pending != 0 {
		t.Fatalf("expected queue empty after retry success, got %d", pending)
	}
}

func TestOutboundQueuePerUserHeadLock(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "outbound.db")

	blocked := true
	var mu sync.Mutex
	sentUsers := make([]string, 0)
	q, err := newOutboundTextQueue(dbPath, 5*time.Millisecond, 1600, func(item *queuedOutboundText) error {
		if item.UserID == "u1" && blocked {
			return errors.New("temporary failure")
		}
		mu.Lock()
		sentUsers = append(sentUsers, item.UserID)
		mu.Unlock()
		return nil
	}, nil)
	if err != nil {
		t.Fatalf("newOutboundTextQueue failed: %v", err)
	}
	defer q.Stop()
	q.setMinGap(0) // disable pacing for deterministic direct dispatch

	if err := q.EnqueueText("u1", "s1", "", "final", "first", true); err != nil {
		t.Fatalf("enqueue u1 first failed: %v", err)
	}
	if err := q.EnqueueText("u1", "s1", "", "final", "second", true); err != nil {
		t.Fatalf("enqueue u1 second failed: %v", err)
	}

	// First dispatch fails on u1 head; second u1 item must not leapfrog.
	did, err := q.dispatchOne()
	if err != nil {
		t.Fatalf("dispatch failed: %v", err)
	}
	if !did {
		t.Fatal("dispatch should do work")
	}

	pending, err := q.pendingCount()
	if err != nil {
		t.Fatalf("pendingCount failed: %v", err)
	}
	if pending != 2 {
		t.Fatalf("expected 2 pending when head failed, got %d", pending)
	}

	blocked = false
	if _, err := q.db.Exec(`UPDATE wechat_outbound_queue SET next_attempt_at = ?`, time.Now().UnixMilli()); err != nil {
		t.Fatalf("force due failed: %v", err)
	}

	for i := 0; i < 2; i++ {
		// Clear any cooldown that might have been set by the first failure
		// (non-rate-limit errors don't set cooldown, but be safe).
		q.ClearUserCooldown("u1")
		did, derr := q.dispatchOne()
		if derr != nil {
			t.Fatalf("dispatch %d failed: %v", i, derr)
		}
		if !did {
			t.Fatalf("dispatch %d expected work", i)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(sentUsers) != 2 {
		t.Fatalf("expected 2 successful sends, got %d", len(sentUsers))
	}
	if sentUsers[0] != "u1" || sentUsers[1] != "u1" {
		t.Fatalf("unexpected send order users=%v", sentUsers)
	}
}

func repeatRune(ch string, n int) string {
	if n <= 0 {
		return ""
	}
	buf := make([]rune, 0, n)
	r := []rune(ch)
	if len(r) == 0 {
		return ""
	}
	for i := 0; i < n; i++ {
		buf = append(buf, r[0])
	}
	return string(buf)
}

// TestOutboundQueueBatchOrderingOnNack verifies that when a chunk of a split
// message fails and is deferred to a future next_attempt_at, the later chunks
// of the SAME batch are deferred with it and cannot leapfrog ahead. Without the
// nack batch-sync, chunk seq=2 (still due) would be sent before the retried
// chunk seq=1, delivering the message out of order.
func TestOutboundQueueBatchOrderingOnNack(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "batch_order.db")

	calls := 0
	q, err := newOutboundTextQueue(dbPath, 5*time.Millisecond, 1600, func(item *queuedOutboundText) error {
		calls++
		if calls == 1 {
			// Fail the first chunk (seq=1) once.
			return errors.New("temporary failure")
		}
		return nil
	}, nil)
	if err != nil {
		t.Fatalf("newOutboundTextQueue failed: %v", err)
	}
	defer q.Stop()
	q.setMinGap(0) // disable pacing for deterministic direct dispatch

	// 3600 runes → three chunks (1600/1600/400) sharing one batch_id.
	long := repeatRune("你", 3600)
	if err := q.EnqueueText("u1", "s1", "", "final", long, true); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	// First dispatch picks the head (seq=1) and fails → it is nacked to a
	// future next_attempt_at, and the batch-sync must push seq=2 and seq=3 to
	// the same future time.
	did, derr := q.dispatchOne()
	if derr != nil {
		t.Fatalf("dispatchOne failed: %v", derr)
	}
	if !did {
		t.Fatal("first dispatch should do work")
	}

	// No chunk should be due right now: the failed head and all its successors
	// are deferred together.
	if item, ok, perr := q.pickNextDueHead(time.Now().UnixMilli()); perr != nil {
		t.Fatalf("pickNextDueHead failed: %v", perr)
	} else if ok {
		t.Fatalf("no chunk should be due after batch nack, but seq=%d is due", item.Seq)
	}

	// All three chunks remain queued.
	pending, err := q.pendingCount()
	if err != nil {
		t.Fatalf("pendingCount failed: %v", err)
	}
	if pending != 3 {
		t.Fatalf("expected 3 pending after batch nack, got %d", pending)
	}
}

// TestOutboundQueuePriorityJump verifies that a PriorityHigh message
// (e.g. permission/question confirmation) can jump ahead of a lower-priority
// head that is blocked by a failed retry. This prevents the deadlock where:
//  1. todo (PriorityLow) is enqueued first, fails, nacks with future retry
//  2. question (PriorityHigh) is enqueued second
//  3. question must be dispatchable despite the blocked todo head
func TestOutboundQueuePriorityJump(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "priority_test.db")
	var mu sync.Mutex
	var sentContents []string
	sendFunc := func(item *queuedOutboundText) error {
		// Simulate failure for the first attempt of the todo item only.
		if item.EventType == "todo" && item.Attempts == 0 {
			return errors.New("simulated rate limit")
		}
		mu.Lock()
		sentContents = append(sentContents, item.Content)
		mu.Unlock()
		return nil
	}
	q, err := newOutboundTextQueue(dbPath, 50*time.Millisecond, 1600, sendFunc, nil)
	if err != nil {
		t.Fatalf("newOutboundTextQueue failed: %v", err)
	}
	defer q.Stop()
	q.setMinGap(0) // disable pacing for deterministic direct dispatch
	q.Start()

	// 1. Enqueue a todo (PriorityLow) — will fail on first attempt.
	if err := q.EnqueueText("u1", "s1", "", "todo", "todo-update", false); err != nil {
		t.Fatalf("enqueue todo failed: %v", err)
	}

	// 2. Dispatch: todo fails and nacks with a future next_attempt_at.
	time.Sleep(200 * time.Millisecond)

	// 3. Enqueue a question (PriorityHigh) — should be dispatchable immediately
	//    despite the todo head being blocked.
	if err := q.EnqueueText("u1", "s1", "", "question", "need-confirm", true); err != nil {
		t.Fatalf("enqueue question failed: %v", err)
	}

	// 4. Wait for dispatch to process the question.
	time.Sleep(500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	found := false
	for _, c := range sentContents {
		if c == "need-confirm" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("question (PriorityHigh) was not dispatched despite low-priority head blocked; sent=%v", sentContents)
	}
}
