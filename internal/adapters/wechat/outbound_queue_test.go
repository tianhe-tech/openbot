package wechat

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// A chunk that can never be delivered (stale iLink session, ret=-2 "prepare
// failed") must stop being retried. In production these were critical
// PriorityHigh notices, so retrying them forever kept them at the head of the
// user's queue and starved every real reply (171 rows → 18k failed sends).
func TestOutboundQueueParksAfterSessionExpired(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "expired.db")
	q, err := newOutboundTextQueue(dbPath, 5*time.Millisecond, 1600, func(item *queuedOutboundText) error {
		return fmt.Errorf("sendmessage ret=-2 errcode=0 errmsg=prepare failed: %w", ErrSessionExpired)
	})
	if err != nil {
		t.Fatalf("newOutboundTextQueue failed: %v", err)
	}
	defer q.Stop()
	q.setMinGap(0)

	if err := q.EnqueueText("u1", "s1", "", "final", "hello", true); err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}

	for i := 0; i < maxSessionExpiredAttempts; i++ {
		if _, err := q.db.Exec(`UPDATE wechat_outbound_queue SET next_attempt_at = 0`); err != nil {
			t.Fatalf("force due failed: %v", err)
		}
		did, derr := q.dispatchOne()
		if derr != nil {
			t.Fatalf("dispatch %d failed: %v", i, derr)
		}
		if !did {
			t.Fatalf("dispatch %d should do work", i)
		}
	}

	if parked := q.ListParked("u1"); len(parked) != 1 {
		t.Fatalf("expected session-expired chunk to be parked, got %d parked entries", len(parked))
	}
	if did, derr := q.dispatchOne(); derr != nil || did {
		t.Fatalf("parked chunk must not be dispatched again (did=%v err=%v)", did, derr)
	}
}

// A sender loop that re-enqueues the same notice must not accumulate rows, and
// real conversation content must never be deduplicated.
func TestOutboundQueueDedupsIdenticalNotices(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "dedup.db")
	q, err := newOutboundTextQueue(dbPath, 5*time.Millisecond, 1600, func(item *queuedOutboundText) error {
		return errors.New("no transport in test")
	})
	if err != nil {
		t.Fatalf("newOutboundTextQueue failed: %v", err)
	}
	defer q.Stop()
	q.setMinGap(0)

	notice := "⏳ 检测到模型未进入生成或工具执行阶段，已重建会话并继续原任务。"
	for i := 0; i < 5; i++ {
		if err := q.EnqueueText("u1", "s1", "", "question", notice, true); err != nil {
			t.Fatalf("enqueue notice %d failed: %v", i, err)
		}
	}
	pending, err := q.pendingCount()
	if err != nil {
		t.Fatalf("pendingCount failed: %v", err)
	}
	if pending != 1 {
		t.Fatalf("identical notices must collapse into 1 row, got %d", pending)
	}

	if err := q.EnqueueText("u1", "s1", "", "question", "另一个提示", true); err != nil {
		t.Fatalf("enqueue distinct notice failed: %v", err)
	}
	if pending, err = q.pendingCount(); err != nil || pending != 2 {
		t.Fatalf("distinct notice must be queued (pending=%d err=%v)", pending, err)
	}

	for i := 0; i < 2; i++ {
		if err := q.EnqueueText("u1", "s1", "", "final", "same reply", true); err != nil {
			t.Fatalf("enqueue final %d failed: %v", i, err)
		}
	}
	if pending, err = q.pendingCount(); err != nil || pending != 4 {
		t.Fatalf("final content must never be deduped (pending=%d err=%v, want 4)", pending, err)
	}
}

// A PriorityHigh backlog that keeps failing (iLink rejecting stalled prompt
// notices) must yield to the user's real replies instead of blocking them
// indefinitely.
func TestOutboundQueueHighBacklogDoesNotStarveNormal(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "fairness.db")
	var mu sync.Mutex
	var sent []string
	q, err := newOutboundTextQueue(dbPath, 5*time.Millisecond, 1600, func(item *queuedOutboundText) error {
		if item.Priority >= PriorityHigh {
			return errors.New("prompt rejected by transport")
		}
		mu.Lock()
		sent = append(sent, item.Content)
		mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatalf("newOutboundTextQueue failed: %v", err)
	}
	defer q.Stop()
	q.setMinGap(0)

	for i := 0; i < 10; i++ {
		if err := q.EnqueueTextWithPriority("u1", "s1", "", "question", fmt.Sprintf("prompt-%d", i), true, PriorityHigh); err != nil {
			t.Fatalf("enqueue prompt %d failed: %v", i, err)
		}
	}
	if err := q.EnqueueTextWithPriority("u1", "s1", "", "final", "real-reply", true, PriorityNormal); err != nil {
		t.Fatalf("enqueue final failed: %v", err)
	}

	for i := 0; i < maxHighStreakPerUser+2; i++ {
		if _, err := q.db.Exec(`UPDATE wechat_outbound_queue SET next_attempt_at = 0`); err != nil {
			t.Fatalf("force due failed: %v", err)
		}
		if _, derr := q.dispatchOne(); derr != nil {
			t.Fatalf("dispatch %d failed: %v", i, derr)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(sent) == 0 {
		t.Fatal("PriorityNormal reply was starved by the PriorityHigh backlog")
	}
	if sent[0] != "real-reply" {
		t.Fatalf("unexpected dispatch order: %v", sent)
	}
}

func TestOutboundQueueSplitAndDispatchOrder(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "outbound.db")

	var mu sync.Mutex
	sent := make([]string, 0)
	q, err := newOutboundTextQueue(dbPath, 5*time.Millisecond, 1600, func(item *queuedOutboundText) error {
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, item.Content)
		return nil
	})
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
	})
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
	})
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
	})
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
// The queue is the only durable outbound path: a gateway restart must not lose
// accepted replies. This reopens the same SQLite file with a fresh queue and
// verifies the pending rows (content, order, attempt count, and the stable
// client_id used as the iLink idempotency key) survive and are dispatchable.
func TestOutboundQueueSurvivesRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "restart.db")

	// First process: every send fails, so both chunks stay pending.
	failing, err := newOutboundTextQueue(dbPath, 5*time.Millisecond, 1600, func(item *queuedOutboundText) error {
		return errors.New("transport down")
	})
	if err != nil {
		t.Fatalf("newOutboundTextQueue failed: %v", err)
	}
	failing.setMinGap(0)
	if err := failing.EnqueueText("u1", "s1", "ctx", "final", "first", true); err != nil {
		t.Fatalf("enqueue first failed: %v", err)
	}
	if err := failing.EnqueueText("u1", "s1", "ctx", "final", "second", true); err != nil {
		t.Fatalf("enqueue second failed: %v", err)
	}
	for i := 0; i < 2; i++ {
		if _, derr := failing.dispatchOne(); derr != nil {
			t.Fatalf("dispatch %d failed: %v", i, derr)
		}
	}
	// Capture the persisted identity before shutdown.
	type row struct {
		content  string
		clientID string
		attempts int
	}
	var before []row
	rows, err := failing.db.Query(`SELECT content, client_id, attempts FROM wechat_outbound_queue ORDER BY id ASC`)
	if err != nil {
		t.Fatalf("read pending rows failed: %v", err)
	}
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.content, &r.clientID, &r.attempts); err != nil {
			t.Fatalf("scan pending row failed: %v", err)
		}
		before = append(before, r)
	}
	rows.Close()
	if len(before) != 2 {
		t.Fatalf("expected 2 pending rows before restart, got %d", len(before))
	}
	if before[0].attempts == 0 {
		t.Fatal("failed dispatch did not persist an attempt count")
	}
	if before[0].clientID == "" || before[0].clientID == before[1].clientID {
		t.Fatalf("chunks must carry distinct stable client IDs: %q %q", before[0].clientID, before[1].clientID)
	}
	failing.Stop()

	// Second process: same file, working transport.
	var mu sync.Mutex
	var sent []string
	var sentClientIDs []string
	restarted, err := newOutboundTextQueue(dbPath, 5*time.Millisecond, 1600, func(item *queuedOutboundText) error {
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, item.Content)
		sentClientIDs = append(sentClientIDs, item.ClientID)
		return nil
	})
	if err != nil {
		t.Fatalf("reopen outbound queue failed: %v", err)
	}
	defer restarted.Stop()
	restarted.setMinGap(0)

	// Simulate the backoff window having elapsed while the process was down.
	if _, err := restarted.db.Exec(`UPDATE wechat_outbound_queue SET next_attempt_at = 0`); err != nil {
		t.Fatalf("force due failed: %v", err)
	}
	for i := 0; i < 2; i++ {
		did, derr := restarted.dispatchOne()
		if derr != nil {
			t.Fatalf("restart dispatch %d failed: %v", i, derr)
		}
		if !did {
			t.Fatalf("restart dispatch %d found no work; pending rows were lost", i)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 2 || sent[0] != "first" || sent[1] != "second" {
		t.Fatalf("restart lost order or content: %v", sent)
	}
	// The idempotency key must be the one persisted before the restart, so a
	// replay after a crash mid-send is recognised by iLink instead of
	// duplicating the message.
	for i := range sentClientIDs {
		if sentClientIDs[i] != before[i].clientID {
			t.Fatalf("chunk %d client_id changed across restart: %q != %q", i, sentClientIDs[i], before[i].clientID)
		}
	}
	if pending, err := restarted.pendingCount(); err != nil || pending != 0 {
		t.Fatalf("queue not drained after restart: pending=%d err=%v", pending, err)
	}
}

// Stop must not block for the full drain timeout when the queue was never
// started: there is no drain goroutine, so doneCh never closes.
func TestOutboundQueueStopWithoutStartIsImmediate(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "nostart.db")
	q, err := newOutboundTextQueue(dbPath, 5*time.Millisecond, 1600, func(item *queuedOutboundText) error {
		return nil
	})
	if err != nil {
		t.Fatalf("newOutboundTextQueue failed: %v", err)
	}
	start := time.Now()
	q.Stop()
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("Stop on an unstarted queue blocked for %s", elapsed)
	}
	// Stop must stay idempotent.
	q.Stop()
}

// (e.g. permission/question confirmation) can jump ahead of a lower-priority
// head that is blocked by a failed retry. This prevents the deadlock where:
//  1. todo (PriorityNormal) is enqueued first, fails, nacks with future retry
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
	q, err := newOutboundTextQueue(dbPath, 50*time.Millisecond, 1600, sendFunc)
	if err != nil {
		t.Fatalf("newOutboundTextQueue failed: %v", err)
	}
	defer q.Stop()
	q.setMinGap(0) // disable pacing for deterministic direct dispatch
	q.Start()

	// 1. Enqueue a todo (PriorityNormal) — will fail on first attempt.
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
