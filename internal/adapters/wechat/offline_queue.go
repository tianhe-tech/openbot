package wechat

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// offlineQueue persists outbound text messages that failed to deliver (e.g.
// the iLink bot was rate-limited or session-expired with no recoverable
// token) so a background worker can retry them after the per-user cooldown
// has passed. This is a deliberately small JSON-file queue rather than a
// SQLite-backed retry worker because the only thing being recovered is a
// short text payload; durability across process restart is the only
// requirement.
type offlineQueue struct {
	path string

	mu      sync.Mutex
	entries []*offlineEntry
}

type offlineEntry struct {
	ID            string    `json:"id"`
	UserID        string    `json:"user_id"`
	ContextToken  string    `json:"context_token,omitempty"`
	Content       string    `json:"content"`
	Attempts      int       `json:"attempts"`
	CreatedAt     time.Time `json:"created_at"`
	NextAttemptAt time.Time `json:"next_attempt_at"`
	// SessionID tags the entry with the originating opencode session so
	// abandoned messages can be recovered per-session via /pending /recover.
	SessionID string `json:"session_id,omitempty"`
	// Abandoned marks entries parked by /new /reset. The worker skips them
	// (no auto-retry); they are only re-sent when the user explicitly
	// requests recovery via /recover.
	Abandoned bool `json:"abandoned,omitempty"`
}

const (
	// offlineTTL is the only safety valve on parked messages. It is long enough
	// (7 days) that any realistic iLink rate-limit window clears long before it,
	// while still preventing unbounded accumulation for users who are permanently
	// unreachable. There is deliberately NO max-attempt cap — parked messages
	// must be delivered eventually, never abandoned after N failed tries.
	offlineTTL       = 7 * 24 * time.Hour
	offlinePollEvery = 30 * time.Second
)

// offlineBackoff is the schedule between retry attempts. attempts is 1-based
// for the slot lookup (attempts=1 means the entry just had its first failure
// recorded, so the wait is offlineBackoff[0]). Out-of-range attempts use the
// final value.
var offlineBackoff = []time.Duration{
	1 * time.Minute,
	5 * time.Minute,
	15 * time.Minute,
	60 * time.Minute,
	60 * time.Minute,
}

func newOfflineQueue(path string) *offlineQueue {
	q := &offlineQueue{path: path}
	q.load()
	return q
}

func (q *offlineQueue) load() {
	if q.path == "" {
		return
	}
	data, err := os.ReadFile(q.path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Printf("wechat: offline queue load failed path=%s: %v", q.path, err)
		}
		return
	}
	var entries []*offlineEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		log.Printf("wechat: offline queue parse failed path=%s: %v (starting empty)", q.path, err)
		return
	}
	q.entries = entries
	if n := len(entries); n > 0 {
		log.Printf("wechat: offline queue restored %d pending message(s) from %s", n, q.path)
	}
}

// persist writes the queue to disk via atomic rename. Must be called with
// q.mu held.
func (q *offlineQueue) persist() {
	if q.path == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(q.path), 0o755); err != nil {
		log.Printf("wechat: offline queue mkdir failed: %v", err)
		return
	}
	data, err := json.MarshalIndent(q.entries, "", "  ")
	if err != nil {
		log.Printf("wechat: offline queue marshal failed: %v", err)
		return
	}
	tmp := q.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		log.Printf("wechat: offline queue write failed: %v", err)
		return
	}
	if err := os.Rename(tmp, q.path); err != nil {
		log.Printf("wechat: offline queue rename failed: %v", err)
		_ = os.Remove(tmp)
	}
}

func (q *offlineQueue) enqueue(userID, ctxToken, content string) {
	q.enqueueWithSession(userID, "", ctxToken, content)
}

// enqueueWithSession adds an entry tagged with the originating sessionID and
// immediately marks it as Abandoned so the worker does not auto-retry it.
// The entry is recoverable via RecoverSession. Used by /new /reset to park
// old-session replies without auto-resending them.
func (q *offlineQueue) enqueueWithSession(userID, sessionID, ctxToken, content string) {
	q.enqueueEntry(userID, sessionID, ctxToken, content, true)
}

// enqueueForRetry adds an entry tagged with the originating sessionID that is
// NOT abandoned: the background worker will auto-retry it on a slow backoff
// (offlineBackoff). Used by the hot-queue parkFunc when a chunk exhausts
// maxRetryAttempts, so no message is ever silently lost.
func (q *offlineQueue) enqueueForRetry(userID, sessionID, ctxToken, content string) {
	q.enqueueEntry(userID, sessionID, ctxToken, content, false)
}

func (q *offlineQueue) enqueueEntry(userID, sessionID, ctxToken, content string, abandoned bool) {
	if content == "" || userID == "" {
		return
	}
	now := time.Now()
	entry := &offlineEntry{
		ID:            generateClientID(),
		UserID:        userID,
		ContextToken:  ctxToken,
		Content:       content,
		Attempts:      0,
		CreatedAt:     now,
		NextAttemptAt: now.Add(offlineBackoff[0]),
		SessionID:     sessionID,
		Abandoned:     abandoned,
	}
	q.mu.Lock()
	q.entries = append(q.entries, entry)
	q.persist()
	q.mu.Unlock()
	log.Printf("wechat: offline queue parked user=%s session=%s len=%d abandoned=%t", userID, sessionID, len(content), abandoned)
}

// drain runs one pass over the queue, dispatching due entries via send.
// notifyDrop is called once for each entry that is permanently abandoned
// (max attempts or TTL exceeded). All file I/O is serialized on q.mu but the
// network send happens with the lock released to avoid head-of-line blocking.
func (q *offlineQueue) drain(send func(userID, ctxToken, content string) error,
	notifyDrop func(userID string)) {
	now := time.Now()

	q.mu.Lock()
	// Snapshot due entries; modifications to q.entries happen after each
	// send result while holding the lock again.
	due := make([]*offlineEntry, 0, len(q.entries))
	for _, e := range q.entries {
		if e.Abandoned {
			continue // parked by /new; only recovered via /recover
		}
		if !now.Before(e.NextAttemptAt) {
			due = append(due, e)
		}
	}
	q.mu.Unlock()

	for _, entry := range due {
		err := send(entry.UserID, entry.ContextToken, entry.Content)
		q.mu.Lock()
		// Locate the entry (it may have been removed by a concurrent
		// enqueue/drop; identity comparison is safe because we never copy).
		idx := -1
		for i, e := range q.entries {
			if e == entry {
				idx = i
				break
			}
		}
		if idx < 0 {
			q.mu.Unlock()
			continue
		}
		if err == nil {
			q.entries = append(q.entries[:idx], q.entries[idx+1:]...)
			q.persist()
			q.mu.Unlock()
			log.Printf("wechat: offline queue delivered user=%s after %d attempt(s)", entry.UserID, entry.Attempts+1)
			continue
		}
		entry.Attempts++
		expired := time.Since(entry.CreatedAt) >= offlineTTL
		if expired {
			q.entries = append(q.entries[:idx], q.entries[idx+1:]...)
			q.persist()
			q.mu.Unlock()
			log.Printf("wechat: offline queue dropping user=%s reason=TTL age=%s err=%v",
				entry.UserID, time.Since(entry.CreatedAt).Round(time.Minute), err)
			if notifyDrop != nil {
				notifyDrop(entry.UserID)
			}
			continue
		}
		slot := entry.Attempts
		if slot >= len(offlineBackoff) {
			slot = len(offlineBackoff) - 1
		}
		entry.NextAttemptAt = time.Now().Add(offlineBackoff[slot])
		q.persist()
		q.mu.Unlock()
		log.Printf("wechat: offline queue retry-failed user=%s attempt=%d nextIn=%s err=%v",
			entry.UserID, entry.Attempts, offlineBackoff[slot], err)
	}
}

// runWorker polls the queue every offlinePollEvery until ctx is cancelled.
func (q *offlineQueue) runWorker(ctx context.Context,
	send func(userID, ctxToken, content string) error,
	notifyDrop func(userID string)) {
	tick := time.NewTicker(offlinePollEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			q.drain(send, notifyDrop)
		}
	}
}

// ParkForUser moves all non-abandoned entries for a user into "abandoned"
// state, tagged with their SessionID. Abandoned entries are not auto-retried
// by the worker; they can be listed via ListAbandoned and recovered via
// RecoverSession. This is called by /new /reset to preserve AI replies from
// the previous session without blocking the new session's queue.
func (q *offlineQueue) ParkForUser(userID string) int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	for _, e := range q.entries {
		if e.UserID == userID && !e.Abandoned {
			e.Abandoned = true
			n++
		}
	}
	if n > 0 {
		q.persist()
		log.Printf("wechat: offline queue parked %d entries for user %s (recoverable via /recover)", n, userID)
	}
	return n
}

// ListAbandoned returns all abandoned entries for a user, grouped by session.
// Each entry includes the session ID, content preview, and creation time so
// the user can decide which session's messages to recover.
type AbandonedEntry struct {
	SessionID string
	Content   string
	CreatedAt time.Time
}

func (q *offlineQueue) ListAbandoned(userID string) []AbandonedEntry {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	var out []AbandonedEntry
	for _, e := range q.entries {
		if e.UserID == userID && e.Abandoned {
			out = append(out, AbandonedEntry{
				SessionID: e.SessionID,
				Content:   e.Content,
				CreatedAt: e.CreatedAt,
			})
		}
	}
	return out
}

// RecoverSession un-abandons all entries for a user that match the given
// sessionID (or all abandoned entries for the user when sessionID is empty),
// making them eligible for immediate dispatch by the worker. Returns the
// number of entries recovered.
func (q *offlineQueue) RecoverSession(userID, sessionID string) int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	n := 0
	now := time.Now()
	for _, e := range q.entries {
		if e.UserID != userID || !e.Abandoned {
			continue
		}
		if sessionID != "" && e.SessionID != sessionID {
			continue
		}
		e.Abandoned = false
		e.Attempts = 0
		e.NextAttemptAt = now
		n++
	}
	if n > 0 {
		q.persist()
		log.Printf("wechat: offline queue recovered %d entries for user %s (session=%s)", n, userID, sessionID)
	}
	return n
}
