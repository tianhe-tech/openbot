package wechat

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestInboundQueue(t *testing.T) *inboundQueue {
	t.Helper()
	q, err := newInboundQueue(filepath.Join(t.TempDir(), "inbound.db"))
	if err != nil {
		t.Fatalf("newInboundQueue failed: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

func TestInboundDeferredDispatchRetainsRowsUntilCapture(t *testing.T) {
	queue := newTestInboundQueue(t)
	if err := queue.Save("owner", "owner", "delivery", "request", "token"); err != nil {
		t.Fatal(err)
	}
	replayInboundQueue(context.Background(), queue, func(context.Context, *WeixinMessage) error {
		return errInboundDeferred
	})
	if count, err := queue.Count(); err != nil || count != 1 {
		t.Fatalf("async acceptance removed inbox: %d %v", count, err)
	}
	handler := &Handler{inbound: queue}
	handler.acknowledgeCapture(wechatCaptureInput{DeliveryIDs: []string{"delivery"}})
	if count, err := queue.Count(); err != nil || count != 0 {
		t.Fatalf("capture did not clear inbox: %d %v", count, err)
	}
}

func TestInboundReplayReadsBeyondFirstPage(t *testing.T) {
	queue := newTestInboundQueue(t)
	for index := 0; index < 205; index++ {
		if err := queue.Save("owner", "owner", fmt.Sprintf("message:%d", index), "input", "token"); err != nil {
			t.Fatal(err)
		}
	}
	seen := 0
	replayInboundQueue(context.Background(), queue, func(_ context.Context, message *WeixinMessage) error {
		seen += len(message.deliveryIDs)
		return errInboundDeferred
	})
	if seen != 205 {
		t.Fatalf("replayed %d of 205 entries", seen)
	}
}

// A duplicate poll delivery or a retried persist must not create a second row:
// the delivery ID is the idempotency key, and a duplicated row would replay the
// same user request twice after a restart.
func TestInboundQueueSaveIsIdempotent(t *testing.T) {
	q := newTestInboundQueue(t)

	for i := 0; i < 3; i++ {
		if err := q.Save("u1", "u1", "message:1", "hello", "tok"); err != nil {
			t.Fatalf("save %d failed: %v", i, err)
		}
	}
	n, err := q.Count()
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if n != 1 {
		t.Fatalf("duplicate delivery ID must collapse into 1 row, got %d", n)
	}

	entries, err := q.List(10)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(entries) != 1 || entries[0].Content != "hello" || entries[0].ContextToken != "tok" {
		t.Fatalf("unexpected entry: %+v", entries)
	}
}

// Empty content is not a user request and must not occupy a row; a missing
// delivery ID is a programming error and must be reported.
func TestInboundQueueSaveRejectsEmptyInput(t *testing.T) {
	q := newTestInboundQueue(t)

	if err := q.Save("u1", "u1", "message:1", "   ", ""); err != nil {
		t.Fatalf("empty content should be a no-op, got %v", err)
	}
	if n, _ := q.Count(); n != 0 {
		t.Fatalf("empty content must not persist, got %d rows", n)
	}
	if err := q.Save("u1", "u1", "", "hello", ""); err == nil {
		t.Fatal("missing delivery ID must be an error")
	}
}

// List must return arrival order so a replayed batch reproduces the order the
// user typed the chunks in.
func TestInboundQueueListPreservesArrivalOrder(t *testing.T) {
	q := newTestInboundQueue(t)

	for _, id := range []string{"message:1", "message:2", "message:3"} {
		if err := q.Save("u1", "u1", id, "chunk-"+id, ""); err != nil {
			t.Fatalf("save %s failed: %v", id, err)
		}
	}
	entries, err := q.List(10)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}
	for i, want := range []string{"message:1", "message:2", "message:3"} {
		if entries[i].DeliveryID != want {
			t.Fatalf("entry %d: want %s, got %s", i, want, entries[i].DeliveryID)
		}
	}
}

func TestInboundQueueDeleteByDeliveryIDs(t *testing.T) {
	q := newTestInboundQueue(t)

	for _, id := range []string{"message:1", "message:2", "message:3"} {
		if err := q.Save("u1", "u1", id, "chunk", ""); err != nil {
			t.Fatalf("save %s failed: %v", id, err)
		}
	}
	if err := q.Delete([]string{"message:1", "message:3", ""}); err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	entries, err := q.List(10)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(entries) != 1 || entries[0].DeliveryID != "message:2" {
		t.Fatalf("expected only message:2 to survive, got %+v", entries)
	}
	if err := q.Delete(nil); err != nil {
		t.Fatalf("empty delete must be a no-op, got %v", err)
	}
}

// A backlog older than maxInboundReplayAge is no longer what the user is
// waiting for; replaying it would inject a stale instruction into a fresh
// session.
func TestInboundQueuePruneDropsStaleRows(t *testing.T) {
	q := newTestInboundQueue(t)

	if err := q.Save("u1", "u1", "message:old", "stale", ""); err != nil {
		t.Fatalf("save failed: %v", err)
	}
	if err := q.Save("u1", "u1", "message:new", "fresh", ""); err != nil {
		t.Fatalf("save failed: %v", err)
	}
	stale := time.Now().Add(-maxInboundReplayAge - time.Hour).UnixMilli()
	if _, err := q.db.Exec(`UPDATE wechat_inbound_queue SET created_at = ? WHERE delivery_id = ?`, stale, "message:old"); err != nil {
		t.Fatalf("backdate failed: %v", err)
	}

	pruned, err := q.Prune(time.Now())
	if err != nil {
		t.Fatalf("prune failed: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("expected 1 pruned row, got %d", pruned)
	}
	entries, err := q.List(10)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(entries) != 1 || entries[0].DeliveryID != "message:new" {
		t.Fatalf("expected only the fresh row to survive, got %+v", entries)
	}
}

// The batcher must persist before arming the debounce timer, so a crash inside
// the window still replays the message.
func TestInboundBatcherPersistsBeforeDispatch(t *testing.T) {
	q := newTestInboundQueue(t)

	var mu sync.Mutex
	var dispatched []string
	b := newInboundBatcher(50*time.Millisecond, 50*time.Millisecond, 1800,
		func(ctx context.Context, msg *WeixinMessage) error {
			mu.Lock()
			dispatched = append(dispatched, firstTextBody(msg))
			mu.Unlock()
			return nil
		})
	b.queue = q

	msg := &WeixinMessage{FromUserID: "u1", MessageID: 7}
	b.enqueueText(context.Background(), msg, "hello")

	// Persisted immediately, before the timer fires.
	if n, err := q.Count(); err != nil || n != 1 {
		t.Fatalf("expected 1 persisted row before dispatch, got %d (err=%v)", n, err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		done := len(dispatched) > 0
		mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("batcher never dispatched")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Successful dispatch clears the row.
	if n, err := q.Count(); err != nil || n != 0 {
		t.Fatalf("expected row cleared after dispatch, got %d (err=%v)", n, err)
	}
}

// A failed dispatch must keep the row: the message was accepted by iLink and
// the user is waiting for it, so it has to be replayable after a restart.
func TestInboundBatcherKeepsRowOnDispatchFailure(t *testing.T) {
	q := newTestInboundQueue(t)

	b := newInboundBatcher(20*time.Millisecond, 20*time.Millisecond, 1800,
		func(ctx context.Context, msg *WeixinMessage) error {
			return errors.New("client unavailable")
		})
	b.queue = q

	b.enqueueText(context.Background(), &WeixinMessage{FromUserID: "u1", MessageID: 9}, "hello")

	deadline := time.Now().Add(2 * time.Second)
	for {
		if n, _ := q.Count(); n == 0 {
			t.Fatal("row must survive a failed dispatch")
		}
		// The row is only cleared on success, so waiting cannot change it; use
		// the pending map to detect that the flush actually ran.
		b.mu.Lock()
		remaining := len(b.pending)
		b.mu.Unlock()
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("batcher never flushed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n, err := q.Count(); err != nil || n != 1 {
		t.Fatalf("expected row kept for replay, got %d (err=%v)", n, err)
	}
}

// Replay groups by sender+chat, concatenates in arrival order, and deletes only
// after a successful dispatch.
func TestReplayInboundQueueGroupsAndClears(t *testing.T) {
	q := newTestInboundQueue(t)

	for _, row := range []struct{ user, chat, id, content string }{
		{"u1", "u1", "message:1", "first"},
		{"u1", "u1", "message:2", "second"},
		{"u2", "g1", "message:3", "group"},
	} {
		if err := q.Save(row.user, row.chat, row.id, row.content, "tok-"+row.user); err != nil {
			t.Fatalf("save failed: %v", err)
		}
	}

	var mu sync.Mutex
	type call struct {
		user, chat, text, token string
		ids                     []string
	}
	var calls []call
	dispatch := func(ctx context.Context, msg *WeixinMessage) error {
		mu.Lock()
		defer mu.Unlock()
		calls = append(calls, call{
			user:  msg.FromUserID,
			chat:  msg.GroupID,
			text:  firstTextBody(msg),
			token: msg.ContextToken,
			ids:   append([]string(nil), msg.deliveryIDs...),
		})
		return nil
	}

	replayInboundQueue(context.Background(), q, dispatch)

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("expected 2 batches (one per sender+chat), got %d: %+v", len(calls), calls)
	}
	if calls[0].user != "u1" || calls[0].chat != "" {
		t.Fatalf("DM batch must clear GroupID, got %+v", calls[0])
	}
	if calls[0].text != "first\nsecond" {
		t.Fatalf("DM batch must concatenate in arrival order, got %q", calls[0].text)
	}
	if calls[0].token != "tok-u1" {
		t.Fatalf("DM batch must carry the context token, got %q", calls[0].token)
	}
	if got := marshalDeliveryIDs(calls[0].ids); got != `["message:1","message:2"]` {
		t.Fatalf("DM batch must carry both delivery IDs, got %s", got)
	}
	if calls[1].user != "u2" || calls[1].chat != "g1" || calls[1].text != "group" {
		t.Fatalf("group batch must keep GroupID, got %+v", calls[1])
	}

	if n, err := q.Count(); err != nil || n != 0 {
		t.Fatalf("successful replay must clear all rows, got %d (err=%v)", n, err)
	}
}

// A failed replay must leave the rows in place so the next restart can retry
// instead of silently dropping an accepted request.
func TestReplayInboundQueueKeepsRowsOnFailure(t *testing.T) {
	q := newTestInboundQueue(t)

	if err := q.Save("u1", "u1", "message:1", "hello", ""); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	replayInboundQueue(context.Background(), q, func(ctx context.Context, msg *WeixinMessage) error {
		return errors.New("client unavailable")
	})

	entries, err := q.List(10)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(entries) != 1 || entries[0].DeliveryID != "message:1" {
		t.Fatalf("failed replay must keep the row, got %+v", entries)
	}
}

// Replay must prune stale rows before dispatching, so a long-dead backlog is
// never injected into a fresh session.
func TestReplayInboundQueuePrunesBeforeDispatch(t *testing.T) {
	q := newTestInboundQueue(t)

	if err := q.Save("u1", "u1", "message:old", "stale", ""); err != nil {
		t.Fatalf("save failed: %v", err)
	}
	stale := time.Now().Add(-maxInboundReplayAge - time.Hour).UnixMilli()
	if _, err := q.db.Exec(`UPDATE wechat_inbound_queue SET created_at = ?`, stale); err != nil {
		t.Fatalf("backdate failed: %v", err)
	}

	var mu sync.Mutex
	dispatched := 0
	replayInboundQueue(context.Background(), q, func(ctx context.Context, msg *WeixinMessage) error {
		mu.Lock()
		dispatched++
		mu.Unlock()
		return nil
	})

	mu.Lock()
	defer mu.Unlock()
	if dispatched != 0 {
		t.Fatalf("stale rows must not be dispatched, got %d dispatch(es)", dispatched)
	}
	if n, _ := q.Count(); n != 0 {
		t.Fatalf("stale rows must be pruned, got %d rows", n)
	}
}

// A nil queue (init failed) must degrade to a no-op rather than panic.
func TestReplayInboundQueueNilQueueIsNoop(t *testing.T) {
	replayInboundQueue(context.Background(), nil, func(ctx context.Context, msg *WeixinMessage) error {
		t.Fatal("nil queue must not dispatch")
		return nil
	})
}

// The queue must survive a process restart: rows written by one instance are
// visible to a fresh instance opened on the same file.
func TestInboundQueueSurvivesRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "inbound.db")

	first, err := newInboundQueue(dbPath)
	if err != nil {
		t.Fatalf("first open failed: %v", err)
	}
	if err := first.Save("u1", "u1", "message:1", "hello", "tok"); err != nil {
		t.Fatalf("save failed: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	second, err := newInboundQueue(dbPath)
	if err != nil {
		t.Fatalf("second open failed: %v", err)
	}
	defer second.Close()

	entries, err := second.List(10)
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if len(entries) != 1 || entries[0].Content != "hello" || entries[0].ContextToken != "tok" {
		t.Fatalf("row must survive restart, got %+v", entries)
	}
	if !strings.HasPrefix(entries[0].DeliveryID, "message:") {
		t.Fatalf("delivery ID must be preserved, got %q", entries[0].DeliveryID)
	}
}
