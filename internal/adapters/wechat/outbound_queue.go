package wechat

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

const (
	defaultOutboundTickInterval = 3000 * time.Millisecond
	defaultOutboundMaxLen       = 4000
	defaultNonCriticalTTL       = 10 * time.Minute
	defaultStopDrainTimeout     = 10 * time.Second
	defaultStatsLogInterval     = 60 * time.Second

	// maxRetryAttempts is the maximum number of send attempts a chunk gets in
	// the hot queue before being parked to the offline queue. This prevents
	// infinite retry storms (the old critical=true → never-expire behaviour)
	// while guaranteeing no message is ever lost — parked chunks are
	// recoverable via /pending /recover.
	maxRetryAttempts = 5

	// defaultOutboundMinGap is the minimum interval between two consecutive
	// sends to the SAME user. The drain loop runs once per tick and would
	// otherwise burst-send every due chunk of a long result back-to-back,
	// tripping iLink's rate limit. Pacing to 4s yields 15 msg/min, leaving
	// headroom under iLink's ~20 msg/min limit even with brief jitter.
	defaultOutboundMinGap = 4000 * time.Millisecond

	// cooldownClearStreak is how many consecutive successful sends are
	// required before an active rate-limit cooldown is cleared. A single
	// success (e.g. a wait-hint from the next request) must NOT flush all
	// deferred chunks of the previous task at once — that re-trips the limit.
	cooldownClearStreak = 5
)

// Priority levels for outbound messages. Higher value = higher priority.
// Within the same priority level, strict FIFO is maintained (by id ASC).
// A higher-priority message can jump ahead of a lower-priority head that is
// blocked by a failed retry (nack with future next_attempt_at).
const (
	// PriorityLow is for non-critical, skippable progress updates.
	// Subject to TTL expiry and to DropLowPriorityForUser when a
	// permission/question dialog must jump the queue. NOTE: todo progress is
	// intentionally NOT PriorityLow — it must be delivered in full (see
	// priorityFromEventType).
	PriorityLow = 0
	// PriorityNormal is for final content, flush, error notices — the primary
	// deliverables the user expects to receive.
	PriorityNormal = 1
	// PriorityHigh is for messages requiring user action (permission/question
	// confirmations). These must jump ahead of any queued content to avoid
	// deadlocks where the session waits for user input the user never sees.
	PriorityHigh = 2
)

// priorityFromEventType derives a queue priority from the event type label.
// Callers may also pass an explicit priority; this helper covers the common
// mapping used by enqueueAsyncText.
func priorityFromEventType(eventType string) int {
	switch eventType {
	case "question":
		return PriorityHigh
	case "skippable":
		return PriorityLow
	case "todo":
		// ★ Todo 进度必须完整、按序送达，绝不能作为可丢弃的低优先级：
		// 用 PriorityNormal（配合 critical=true → 无 TTL 过期，且不会被
		// DropLowPriorityForUser 删除）。
		return PriorityNormal
	default:
		// final, flush, error_notice, error_partial, sync_reply, done,
		// deferred_notice, generic, offline_drop_notice → Normal
		return PriorityNormal
	}
}

type outboundQueueStats struct {
	Pending        int
	SentOK         uint64
	SendFailed     uint64
	RetryScheduled uint64
	DroppedTTL     uint64
	ParkedOffline  uint64
}

type outboundQueueConfig struct {
	Interval         time.Duration
	MaxLen           int
	StatsLogInterval time.Duration
	NonCriticalTTL   time.Duration
	StopDrainTimeout time.Duration
}

type queuedOutboundText struct {
	ID           int64
	UserID       string
	SessionID    string
	ContextToken string
	EventType    string
	BatchID      string
	Seq          int
	Total        int
	Content      string
	Attempts     int
	Critical     bool
	Priority     int
	CreatedAt    int64
	ExpiresAt    int64
}

type outboundTextQueue struct {
	mu sync.Mutex

	db               *sql.DB
	maxLen           int
	interval         time.Duration
	minGap           time.Duration
	nonCriticalTTL   time.Duration
	stopDrainTimeout time.Duration
	statsLogInterval time.Duration
	sendFunc         func(item *queuedOutboundText) error

	// parkFunc is called when a chunk exhausts maxRetryAttempts. It transfers
	// the content to the offline queue so the message is never lost.
	parkFunc func(userID, sessionID, ctxToken, content string)

	started bool
	stopped bool
	stopCh  chan struct{}
	doneCh  chan struct{}

	sentOK         atomic.Uint64
	sendFailed     atomic.Uint64
	retryScheduled atomic.Uint64
	droppedTTL     atomic.Uint64
	parkedOffline  atomic.Uint64

	// userCooldowns tracks per-user rate-limit cooldown state. When iLink
	// returns ret=-2, ALL sends for that user are paused — not just the one
	// chunk that failed. This prevents the retry-storm where each chunk
	// independently backs off and hammers iLink in parallel.
	userCooldowns sync.Map // map[userID]*userCooldown
}

// userCooldown tracks the rate-limit cooldown for a single user.
type userCooldown struct {
	mu          sync.Mutex
	pausedUntil time.Time
	extensions  int // number of adaptive extensions (for escalating cooldown)

	// lastSentAt is the wall-clock time of the last successful send to this
	// user. Used to enforce minGap pacing between consecutive sends within a
	// single drain tick (and across ticks).
	lastSentAt time.Time
	// successStreak counts consecutive successful sends since the last
	// rate-limit failure. A cooldown is only cleared once this reaches
	// cooldownClearStreak, preventing a single success from flushing all
	// deferred chunks at once.
	successStreak int
}

func newOutboundTextQueue(dbPath string, interval time.Duration, maxLen int, sendFunc func(item *queuedOutboundText) error, parkFunc func(userID, sessionID, ctxToken, content string)) (*outboundTextQueue, error) {
	if sendFunc == nil {
		return nil, errors.New("wechat: outbound queue requires sendFunc")
	}
	if maxLen <= 0 {
		maxLen = defaultOutboundMaxLen
	}
	if interval <= 0 {
		interval = defaultOutboundTickInterval
	}
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000", filepath.ToSlash(dbPath))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("wechat: open outbound queue db: %w", err)
	}
	db.SetMaxOpenConns(1)

	q := &outboundTextQueue{
		db:               db,
		maxLen:           maxLen,
		interval:         interval,
		minGap:           defaultOutboundMinGap,
		nonCriticalTTL:   defaultNonCriticalTTL,
		stopDrainTimeout: defaultStopDrainTimeout,
		statsLogInterval: defaultStatsLogInterval,
		sendFunc:         sendFunc,
		parkFunc:         parkFunc,
		stopCh:           make(chan struct{}),
		doneCh:           make(chan struct{}),
	}
	if err := q.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return q, nil
}

func (q *outboundTextQueue) setStatsLogInterval(d time.Duration) {
	if d <= 0 {
		return
	}
	q.mu.Lock()
	q.statsLogInterval = d
	q.mu.Unlock()
}

// setMinGap overrides the per-user pacing interval. Used by tests to disable
// pacing (minGap=0) so dispatchOne returns immediately.
func (q *outboundTextQueue) setMinGap(d time.Duration) {
	q.mu.Lock()
	q.minGap = d
	q.mu.Unlock()
}

func (q *outboundTextQueue) config() outboundQueueConfig {
	q.mu.Lock()
	defer q.mu.Unlock()
	return outboundQueueConfig{
		Interval:         q.interval,
		MaxLen:           q.maxLen,
		StatsLogInterval: q.statsLogInterval,
		NonCriticalTTL:   q.nonCriticalTTL,
		StopDrainTimeout: q.stopDrainTimeout,
	}
}

func (q *outboundTextQueue) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS wechat_outbound_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id TEXT NOT NULL,
			session_id TEXT NOT NULL DEFAULT '',
			context_token TEXT NOT NULL DEFAULT '',
			event_type TEXT NOT NULL DEFAULT '',
			batch_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			total INTEGER NOT NULL,
			content TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			next_attempt_at INTEGER NOT NULL,
			critical INTEGER NOT NULL DEFAULT 1,
			expires_at INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_wechat_outbound_due ON wechat_outbound_queue(next_attempt_at, id)`,
		`CREATE INDEX IF NOT EXISTS idx_wechat_outbound_user_head ON wechat_outbound_queue(user_id, id)`,
	}
	for _, stmt := range stmts {
		if _, err := q.db.Exec(stmt); err != nil {
			return fmt.Errorf("wechat: migrate outbound queue: %w", err)
		}
	}
	// Add priority column (idempotent — ignore "duplicate column" error on
	// databases that already have it). priority defaults to 0 (PriorityLow)
	// for legacy rows; the critical flag is still honoured for TTL decisions.
	// New inserts set priority explicitly via EnqueueText.
	if _, err := q.db.Exec(`ALTER TABLE wechat_outbound_queue ADD COLUMN priority INTEGER NOT NULL DEFAULT 0`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column") {
			return fmt.Errorf("wechat: migrate outbound queue (add priority): %w", err)
		}
	}
	// Backfill: legacy critical=1 rows should be at least PriorityNormal so
	// they are not starved by new PriorityHigh inserts.
	if _, err := q.db.Exec(`UPDATE wechat_outbound_queue SET priority = ? WHERE priority = 0 AND critical = 1`, PriorityNormal); err != nil {
		return fmt.Errorf("wechat: migrate outbound queue (backfill priority): %w", err)
	}
	// Index for priority-aware dispatch.
	if _, err := q.db.Exec(`CREATE INDEX IF NOT EXISTS idx_wechat_outbound_priority ON wechat_outbound_queue(user_id, priority DESC, id ASC)`); err != nil {
		return fmt.Errorf("wechat: migrate outbound queue (priority index): %w", err)
	}
	return nil
}

func (q *outboundTextQueue) Start() {
	q.mu.Lock()
	if q.started {
		q.mu.Unlock()
		return
	}
	q.started = true
	q.mu.Unlock()
	go q.run()
}

func (q *outboundTextQueue) Stop() {
	q.mu.Lock()
	if q.stopped {
		q.mu.Unlock()
		return
	}
	q.stopped = true
	close(q.stopCh)
	timeout := q.stopDrainTimeout
	q.mu.Unlock()

	select {
	case <-q.doneCh:
	case <-time.After(timeout):
		log.Printf("wechat: outbound queue stop drain timeout after %s", timeout)
	}

	_ = q.db.Close()
}

func (q *outboundTextQueue) EnqueueText(userID, sessionID, ctxToken, eventType, text string, critical bool) error {
	return q.EnqueueTextWithPriority(userID, sessionID, ctxToken, eventType, text, critical, priorityFromEventType(eventType))
}

// EnqueueTextWithPriority enqueues a message with an explicit priority.
// priority is one of PriorityLow / PriorityNormal / PriorityHigh.
// The critical flag controls TTL expiry (critical=true → no expiry);
// priority controls dispatch ordering relative to other queued messages.
func (q *outboundTextQueue) EnqueueTextWithPriority(userID, sessionID, ctxToken, eventType, text string, critical bool, priority int) error {
	if q == nil {
		return errors.New("wechat: outbound queue not initialized")
	}
	if userID == "" {
		return errors.New("wechat: outbound queue requires userID")
	}
	chunks := splitTextForWeixinDelivery(text, q.maxLen, false)
	if len(chunks) == 0 {
		return nil
	}

	now := time.Now().UnixMilli()
	expiresAt := int64(0)
	if !critical {
		expiresAt = time.Now().Add(q.nonCriticalTTL).UnixMilli()
	}
	batchID := generateClientID()

	tx, err := q.db.Begin()
	if err != nil {
		return fmt.Errorf("wechat: outbound queue begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(`
		INSERT INTO wechat_outbound_queue (
			user_id, session_id, context_token, event_type,
			batch_id, seq, total, content,
			attempts, next_attempt_at, critical, expires_at, created_at, priority
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("wechat: outbound queue prepare insert: %w", err)
	}
	defer stmt.Close()

	total := len(chunks)
	criticalInt := 0
	if critical {
		criticalInt = 1
	}
	for idx, chunk := range chunks {
		if _, err = stmt.Exec(userID, sessionID, ctxToken, eventType, batchID, idx, total, chunk, now, criticalInt, expiresAt, now, priority); err != nil {
			return fmt.Errorf("wechat: outbound queue insert: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("wechat: outbound queue commit: %w", err)
	}
	return nil
}

func (q *outboundTextQueue) run() {
	defer close(q.doneCh)

	ticker := time.NewTicker(q.interval)
	defer ticker.Stop()
	statsTicker := time.NewTicker(q.statsLogInterval)
	defer statsTicker.Stop()

	for {
		select {
		case <-ticker.C:
			// Drain as many items as possible per tick. dispatchOne returns
			// false when the queue is empty OR when the head item was
			// deferred (e.g. user in cooldown). In the deferral case we
			// loop to try the next eligible item instead of waiting for
			// the next tick — otherwise a single paused user's low-priority
			// head would block all other users for 3s per tick.
			for {
				didWork, err := q.dispatchOne()
				if err != nil {
					log.Printf("wechat: outbound queue dispatch failed: %v", err)
					break
				}
				if !didWork {
					break
				}
			}
		case <-statsTicker.C:
			q.logStats()
		case <-q.stopCh:
			q.drainUntil(time.Now().Add(q.stopDrainTimeout))
			q.logStats()
			return
		}
	}
}

func (q *outboundTextQueue) drainUntil(deadline time.Time) {
	for time.Now().Before(deadline) {
		didWork, err := q.dispatchOne()
		if err != nil {
			log.Printf("wechat: outbound queue drain dispatch failed: %v", err)
		}
		if !didWork {
			return
		}
	}
}

func (q *outboundTextQueue) dispatchOne() (bool, error) {
	now := time.Now().UnixMilli()
	item, ok, err := q.pickNextDueHead(now)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	if item.ExpiresAt > 0 && now > item.ExpiresAt {
		if err := q.ackDelete(item.ID); err != nil {
			return true, err
		}
		q.droppedTTL.Add(1)
		log.Printf("wechat: outbound queue dropped expired item id=%d user=%s event=%s", item.ID, item.UserID, item.EventType)
		return true, nil
	}

	// ★ Global per-user cooldown: if this user is in rate-limit cooldown,
	// skip ALL dispatch for them (not just this item). This prevents the
	// retry-storm where each chunk independently backs off and hammers iLink.
	//
	// This now applies to PriorityHigh (permission/question) messages too:
	// during a cooldown iLink is rejecting sends, so hammering a permission
	// prompt only deepens the limit and delays it further. PriorityHigh items
	// are NOT parked and have the highest dispatch priority, so they are sent
	// first the moment the cooldown clears (see pickNextDueHead ordering).
	if q.IsUserPaused(item.UserID) {
		// Defer this item: push its next_attempt_at forward so it doesn't
		// get picked again immediately, and return false so the dispatcher
		// can try other users/items on the next tick.
		deferAt := time.Now().Add(5 * time.Second).UnixMilli()
		_, _ = q.db.Exec(`UPDATE wechat_outbound_queue SET next_attempt_at = ? WHERE id = ?`, deferAt, item.ID)
		return false, nil
	}

	// ★ Per-user pacing: enforce a minimum gap between consecutive sends to
	// the same user. Without this, the drain loop (which runs once per tick
	// and loops over every due chunk) would burst-send all chunks of a long
	// result back-to-back, tripping iLink's ~20 msg/min rate limit. We sleep
	// here so the whole batch is paced, not just spaced across ticks.
	if gap := q.pacingGap(item.UserID); gap > 0 {
		time.Sleep(gap)
	}

	err = q.sendFunc(item)
	if err == nil {
		// Success → record it. The cooldown is only cleared after a streak of
		// consecutive successes (cooldownClearStreak), so a single success
		// (e.g. a wait-hint from the next request) does NOT flush all deferred
		// chunks of the previous task at once and re-trip the limit.
		q.RecordSendSuccess(item.UserID)
		if ackErr := q.ackDelete(item.ID); ackErr != nil {
			return true, ackErr
		}
		q.sentOK.Add(1)
		return true, nil
	}
	q.sendFailed.Add(1)

	// ★ Rate limit → set global per-user cooldown (pause ALL sends for user).
	if errors.Is(err, ErrRateLimited) {
		q.SetUserCooldown(item.UserID)
	}

	// ★ Exhausted retries → park to offline queue (never lose the message).
	if item.Attempts+1 >= maxRetryAttempts {
		// ★ PriorityHigh (permission/question) messages NEVER park. Parking
		// sends them to the offline queue which has no automatic replay, so
		// the user may never see the prompt — deadlocking the session that
		// waits for their input. Instead we keep retrying with the
		// escalating nextAttemptAt backoff (capped at 5min) until delivered.
		// The item stays durable in SQLite across restarts.
		if item.Priority >= PriorityHigh {
			nextAt := q.nextAttemptAt(item.Attempts+1, err)
			if nackErr := q.nack(item.ID, item.Attempts+1, nextAt); nackErr != nil {
				return true, fmt.Errorf("sendErr=%v nackErr=%w", err, nackErr)
			}
			q.retryScheduled.Add(1)
			log.Printf("wechat: outbound queue PriorityHigh continuing retry id=%d user=%s event=%s attempts=%d nextIn=%s err=%v",
				item.ID, item.UserID, item.EventType, item.Attempts+1, time.Until(time.UnixMilli(nextAt)).Round(time.Second), err)
			return true, nil
		}
		log.Printf("wechat: 📦 chunk id=%d user=%s event=%s exhausted %d attempts, parking to offline queue",
			item.ID, item.UserID, item.EventType, item.Attempts+1)
		if q.parkFunc != nil {
			q.parkFunc(item.UserID, item.SessionID, item.ContextToken, item.Content)
		}
		if delErr := q.ackDelete(item.ID); delErr != nil {
			return true, delErr
		}
		q.parkedOffline.Add(1)
		return true, nil
	}

	nextAt := q.nextAttemptAt(item.Attempts+1, err)
	if nackErr := q.nack(item.ID, item.Attempts+1, nextAt); nackErr != nil {
		return true, fmt.Errorf("sendErr=%v nackErr=%w", err, nackErr)
	}
	q.retryScheduled.Add(1)
	log.Printf("wechat: outbound queue send failed id=%d user=%s event=%s attempts=%d nextIn=%s err=%v",
		item.ID, item.UserID, item.EventType, item.Attempts+1, time.Until(time.UnixMilli(nextAt)).Round(time.Second), err)
	return true, nil
}

func (q *outboundTextQueue) pickNextDueHead(nowMillis int64) (*queuedOutboundText, bool, error) {
	// Priority-aware dispatch:
	//   - Within the same priority level, strict FIFO is maintained (by id ASC).
	//   - A higher-priority message can jump ahead of a lower-priority head
	//     that is blocked by a failed retry (nack with future next_attempt_at).
	//   - The NOT EXISTS subquery only blocks q when there is an *earlier*
	//     (p.id < q.id) message with *equal or higher* priority
	//     (p.priority >= q.priority) that is also due. This means a
	//     PriorityHigh message is never blocked by a PriorityLow/Normal head.
	const query = `
		SELECT
			q.id, q.user_id, q.session_id, q.context_token, q.event_type,
			q.batch_id, q.seq, q.total, q.content, q.attempts,
			q.critical, q.created_at, q.expires_at, q.priority
		FROM wechat_outbound_queue q
		WHERE q.next_attempt_at <= ?
		  AND NOT EXISTS (
			SELECT 1
			FROM wechat_outbound_queue p
			WHERE p.user_id = q.user_id
			  AND p.id < q.id
			  AND p.priority >= q.priority
			  AND p.next_attempt_at <= ?
		  )
		ORDER BY q.priority DESC, q.id ASC
		LIMIT 1
	`
	row := q.db.QueryRow(query, nowMillis, nowMillis)
	item := &queuedOutboundText{}
	var criticalInt int
	err := row.Scan(
		&item.ID,
		&item.UserID,
		&item.SessionID,
		&item.ContextToken,
		&item.EventType,
		&item.BatchID,
		&item.Seq,
		&item.Total,
		&item.Content,
		&item.Attempts,
		&criticalInt,
		&item.CreatedAt,
		&item.ExpiresAt,
		&item.Priority,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("wechat: outbound queue query next: %w", err)
	}
	item.Critical = criticalInt == 1
	return item, true, nil
}

func (q *outboundTextQueue) ackDelete(id int64) error {
	_, err := q.db.Exec(`DELETE FROM wechat_outbound_queue WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("wechat: outbound queue ack delete: %w", err)
	}
	return nil
}

func (q *outboundTextQueue) nack(id int64, attempts int, nextAt int64) error {
	_, err := q.db.Exec(`
		UPDATE wechat_outbound_queue
		SET attempts = ?, next_attempt_at = ?
		WHERE id = ?
	`, attempts, nextAt, id)
	if err != nil {
		return fmt.Errorf("wechat: outbound queue nack: %w", err)
	}

	// ★ Ordering guard: when a chunk fails and is deferred to a future
	// next_attempt_at, later chunks of the SAME batch must not overtake it.
	// pickNextDueHead only blocks a chunk when an *earlier* (smaller id) chunk
	// with equal-or-higher priority is also due; a deferred head therefore lets
	// its successors jump ahead, delivering out of order. Push every later chunk
	// (same batch_id, larger seq) to at least this chunk's next_attempt_at so the
	// whole batch stays in seq order. Standalone messages (empty batch_id) and
	// cross-batch messages are unaffected.
	if _, syncErr := q.db.Exec(`
		UPDATE wechat_outbound_queue
		SET next_attempt_at = MAX(next_attempt_at, ?)
		WHERE batch_id != ''
		  AND batch_id = (SELECT batch_id FROM wechat_outbound_queue WHERE id = ?)
		  AND seq > (SELECT seq FROM wechat_outbound_queue WHERE id = ?)
	`, nextAt, id, id); syncErr != nil {
		return fmt.Errorf("wechat: outbound queue nack batch-sync: %w", syncErr)
	}
	return nil
}

func (q *outboundTextQueue) nextAttemptAt(attempt int, sendErr error) int64 {
	now := time.Now()
	// Gentler backoff than the old [3s,6s,12s,30s,60s] sequence. The old
	// schedule was too aggressive — combined with per-chunk-independent
	// retries it caused 1370 failures in ~20 minutes (log evidence). The new
	// schedule spaces retries further apart so iLink's cooldown can clear.
	// Note: the global per-user cooldown (SetUserCooldown) is the primary
	// rate-limit defence; this backoff only governs non-rate-limit errors.
	backoff := []time.Duration{10 * time.Second, 30 * time.Second, 60 * time.Second, 120 * time.Second, 300 * time.Second}
	idx := attempt - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(backoff) {
		idx = len(backoff) - 1
	}
	return now.Add(backoff[idx]).UnixMilli()
}

func (q *outboundTextQueue) pendingCount() (int, error) {
	row := q.db.QueryRow(`SELECT COUNT(1) FROM wechat_outbound_queue`)
	var n int
	if err := row.Scan(&n); err != nil {
		return 0, fmt.Errorf("wechat: outbound queue count failed: %w", err)
	}
	return n, nil
}

// PendingForUser returns the number of pending (not-yet-delivered) chunks
// for a specific user. Used by the inbound gating logic to decide whether
// a new task should wait for prior outbound delivery to complete.
func (q *outboundTextQueue) PendingForUser(userID string) (int, error) {
	row := q.db.QueryRow(`SELECT COUNT(1) FROM wechat_outbound_queue WHERE user_id = ?`, userID)
	var n int
	if err := row.Scan(&n); err != nil {
		return 0, fmt.Errorf("wechat: outbound queue count for user failed: %w", err)
	}
	return n, nil
}

// getUserCooldown returns the per-user cooldown state, creating it if needed.
func (q *outboundTextQueue) getUserCooldown(userID string) *userCooldown {
	v, _ := q.userCooldowns.LoadOrStore(userID, &userCooldown{})
	return v.(*userCooldown)
}

// IsUserPaused returns true if the user is currently in rate-limit cooldown.
// During cooldown, dispatchOne skips all sends for this user.
func (q *outboundTextQueue) IsUserPaused(userID string) bool {
	uc := q.getUserCooldown(userID)
	uc.mu.Lock()
	defer uc.mu.Unlock()
	return time.Now().Before(uc.pausedUntil)
}

// SetUserCooldown activates a rate-limit cooldown for a user. The first
// activation pauses for 120s; if the user is still rate-limited when the
// cooldown expires, the next SetUserCooldown call extends to 240s, then
// 300s (capped). This adaptive escalation avoids hammering iLink while
// allowing faster recovery if the limit was transient.
func (q *outboundTextQueue) SetUserCooldown(userID string) {
	uc := q.getUserCooldown(userID)
	uc.mu.Lock()
	defer uc.mu.Unlock()

	// If already paused, don't reset (avoid shortening an active cooldown).
	if time.Now().Before(uc.pausedUntil) {
		return
	}

	// Adaptive escalation: 30s → 60s → 120s (capped).
	// Shorter cooldowns allow faster recovery after transient iLink rate limits,
	// while still providing escalating backpressure for persistent overload.
	durations := []time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second}
	idx := uc.extensions
	if idx >= len(durations) {
		idx = len(durations) - 1
	}
	uc.pausedUntil = time.Now().Add(durations[idx])
	// ★ Escalate: the next rate-limit failure uses a longer cooldown. This is
	// the adaptive backpressure that prevents a 30s→fail→30s→fail loop.
	uc.extensions++
	// A rate-limit failure resets the success streak so recovery requires a
	// fresh run of consecutive successes before the cooldown is cleared.
	uc.successStreak = 0
	log.Printf("wechat: 🚫 user %s rate-limit cooldown set for %s (extension #%d)",
		userID, durations[idx], idx)
}

// pacingGap returns how long dispatchOne must sleep before sending to userID
// to honour the per-user minGap. Returns 0 when no wait is needed (first send
// or enough time has elapsed since the last send).
func (q *outboundTextQueue) pacingGap(userID string) time.Duration {
	uc := q.getUserCooldown(userID)
	uc.mu.Lock()
	defer uc.mu.Unlock()
	if uc.lastSentAt.IsZero() {
		return 0
	}
	elapsed := time.Since(uc.lastSentAt)
	if elapsed >= q.minGap {
		return 0
	}
	return q.minGap - elapsed
}

// RecordSendSuccess records a successful send to userID. It updates the
// pacing timestamp and, once the user has accumulated cooldownClearStreak
// consecutive successes, clears any active rate-limit cooldown. A single
// success (e.g. a wait-hint from the next request) is deliberately NOT enough
// to clear the cooldown — otherwise all deferred chunks of the previous task
// would flush at once and re-trip iLink's limit.
func (q *outboundTextQueue) RecordSendSuccess(userID string) {
	uc := q.getUserCooldown(userID)
	uc.mu.Lock()
	defer uc.mu.Unlock()

	uc.lastSentAt = time.Now()
	uc.successStreak++

	if uc.pausedUntil.IsZero() {
		return
	}
	if uc.successStreak < cooldownClearStreak {
		log.Printf("wechat: user %s send ok (streak=%d/%d), cooldown held", userID, uc.successStreak, cooldownClearStreak)
		return
	}
	log.Printf("wechat: ✅ user %s rate-limit cooldown cleared after %d consecutive sends", userID, uc.successStreak)
	uc.pausedUntil = time.Time{}
	uc.extensions = 0
	uc.successStreak = 0
}

// ClearUserCooldown forcibly removes the rate-limit cooldown for a user.
// It is used by direct-send paths (sendTextInline / sendWaitHint) that prove
// iLink has recovered. It also resets the success streak and pacing timestamp
// so the next queued send is not delayed by stale pacing state.
func (q *outboundTextQueue) ClearUserCooldown(userID string) {
	uc := q.getUserCooldown(userID)
	uc.mu.Lock()
	defer uc.mu.Unlock()
	if !uc.pausedUntil.IsZero() {
		log.Printf("wechat: ✅ user %s rate-limit cooldown cleared (forced)", userID)
	}
	uc.pausedUntil = time.Time{}
	uc.extensions = 0
	uc.successStreak = 0
	uc.lastSentAt = time.Time{}
}

// CooldownRemaining returns how long until the user's cooldown expires, or 0
// if not paused. Useful for logging and user-facing notices.
func (q *outboundTextQueue) CooldownRemaining(userID string) time.Duration {
	uc := q.getUserCooldown(userID)
	uc.mu.Lock()
	defer uc.mu.Unlock()
	remaining := time.Until(uc.pausedUntil)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// ClearForUser removes all pending queue entries for a specific user.
// This is called when the user starts a new session (/new, /reset).
//
// PriorityHigh (question/permission) and PriorityLow (skippable progress)
// entries are deleted outright — they are session-specific and meaningless in a
// new session. PriorityNormal entries (final AI replies, error notices, todo
// progress) are moved to the offline queue as "abandoned" entries tagged with their
// sessionID, so the user can recover them later via /pending /recover.
//
// parkFunc receives the rescued entries; when nil, Normal entries are also
// deleted. Returns the number of entries deleted.
func (q *outboundTextQueue) ClearForUser(userID string, parkFunc func(userID, sessionID, ctxToken, content string)) (int64, error) {
	if q == nil {
		return 0, nil
	}
	// Rescue PriorityNormal entries before deleting everything.
	if parkFunc != nil {
		rows, err := q.db.Query(
			`SELECT session_id, context_token, content FROM wechat_outbound_queue
			 WHERE user_id = ? AND priority = ?`,
			userID, PriorityNormal)
		if err != nil {
			log.Printf("wechat: outbound queue clear-for-user rescue query failed: %v", err)
		} else {
			for rows.Next() {
				var sid, ctxToken, content string
				if err := rows.Scan(&sid, &ctxToken, &content); err == nil {
					parkFunc(userID, sid, ctxToken, content)
				}
			}
			rows.Close()
		}
	}
	res, err := q.db.Exec(`DELETE FROM wechat_outbound_queue WHERE user_id = ?`, userID)
	if err != nil {
		return 0, fmt.Errorf("wechat: outbound queue clear for user: %w", err)
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		log.Printf("wechat: outbound queue cleared %d pending entries for user %s", n, userID)
	}
	return n, nil
}

// DropLowPriorityForUser deletes all PriorityLow (skippable progress) entries
// for a user. This is called when a permission/question dialog is about to
// be sent to the user, so the dialog isn't drowned by accumulated progress
// messages from earlier in the task. Todo progress is PriorityNormal and is
// deliberately NOT dropped here.
func (q *outboundTextQueue) DropLowPriorityForUser(userID string) (int64, error) {
	if q == nil {
		return 0, nil
	}
	res, err := q.db.Exec(`DELETE FROM wechat_outbound_queue WHERE user_id = ? AND priority = ?`, userID, PriorityLow)
	if err != nil {
		return 0, fmt.Errorf("wechat: outbound queue drop low-priority for user: %w", err)
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		log.Printf("wechat: outbound queue dropped %d low-priority entries for user %s (permission dialog pending)", n, userID)
	}
	return n, nil
}

func (q *outboundTextQueue) snapshotStats() outboundQueueStats {
	pending, err := q.pendingCount()
	if err != nil {
		log.Printf("wechat: outbound queue stats pendingCount failed: %v", err)
	}
	return outboundQueueStats{
		Pending:        pending,
		SentOK:         q.sentOK.Load(),
		SendFailed:     q.sendFailed.Load(),
		RetryScheduled: q.retryScheduled.Load(),
		DroppedTTL:     q.droppedTTL.Load(),
		ParkedOffline:  q.parkedOffline.Load(),
	}
}

func (q *outboundTextQueue) logStats() {
	s := q.snapshotStats()
	log.Printf("wechat: outbound queue stats pending=%d sent_ok=%d send_failed=%d retry_scheduled=%d dropped_ttl=%d parked=%d",
		s.Pending, s.SentOK, s.SendFailed, s.RetryScheduled, s.DroppedTTL, s.ParkedOffline)
}
