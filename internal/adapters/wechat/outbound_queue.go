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
	defaultOutboundTickInterval = 1500 * time.Millisecond
	defaultOutboundMaxLen       = 1600
	defaultNonCriticalTTL       = 10 * time.Minute
	defaultStopDrainTimeout     = 10 * time.Second
	defaultStatsLogInterval     = 60 * time.Second

	// maxRetryAttempts is the maximum number of send attempts a chunk gets in
	// the hot queue before being parked to the offline queue. This prevents
	// infinite retry storms (the old critical=true → never-expire behaviour)
	// while guaranteeing no message is ever lost — parked chunks are
	// recoverable via /pending /recover.
	maxRetryAttempts = 5
)

// Priority levels for outbound messages. Higher value = higher priority.
// Within the same priority level, strict FIFO is maintained (by id ASC).
// A higher-priority message can jump ahead of a lower-priority head that is
// blocked by a failed retry (nack with future next_attempt_at).
const (
	// PriorityLow is for non-critical progress updates (e.g. todo snapshots).
	// Subject to TTL expiry when the queue is congested.
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
	case "todo", "skippable":
		return PriorityLow
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
			if _, err := q.dispatchOne(); err != nil {
				log.Printf("wechat: outbound queue dispatch failed: %v", err)
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
	if q.IsUserPaused(item.UserID) {
		return false, nil
	}

	err = q.sendFunc(item)
	if err == nil {
		// Success → clear any cooldown for this user.
		q.ClearUserCooldown(item.UserID)
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
		log.Printf("wechat: 📦 chunk id=%d user=%s event=%s exhausted %d attempts, parking to offline queue",
			item.ID, item.UserID, item.EventType, item.Attempts+1)
		if q.parkFunc != nil {
			q.parkFunc(item.UserID, item.SessionID, item.ContextToken, item.Content)
		}
		if delErr := q.ackDelete(item.ID); delErr != nil {
			return true, delErr
		}
		q.droppedTTL.Add(1)
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

	// Adaptive escalation: 120s → 240s → 300s (capped).
	durations := []time.Duration{120 * time.Second, 240 * time.Second, 300 * time.Second}
	idx := uc.extensions
	if idx >= len(durations) {
		idx = len(durations) - 1
	}
	uc.pausedUntil = time.Now().Add(durations[idx])
	log.Printf("wechat: 🚫 user %s rate-limit cooldown set for %s (extension #%d)",
		userID, durations[idx], uc.extensions)
}

// ClearUserCooldown removes the rate-limit cooldown for a user. Called when
// a send succeeds, indicating iLink has recovered.
func (q *outboundTextQueue) ClearUserCooldown(userID string) {
	uc := q.getUserCooldown(userID)
	uc.mu.Lock()
	defer uc.mu.Unlock()
	if !uc.pausedUntil.IsZero() {
		log.Printf("wechat: ✅ user %s rate-limit cooldown cleared", userID)
	}
	uc.pausedUntil = time.Time{}
	uc.extensions = 0
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
// PriorityHigh (question/permission) and PriorityLow (todo) entries are
// deleted outright — they are session-specific and meaningless in a new
// session. PriorityNormal entries (final AI replies, error notices) are
// moved to the offline queue as "abandoned" entries tagged with their
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
	}
}

func (q *outboundTextQueue) logStats() {
	s := q.snapshotStats()
	log.Printf("wechat: outbound queue stats pending=%d sent_ok=%d send_failed=%d retry_scheduled=%d dropped_ttl=%d",
		s.Pending, s.SentOK, s.SendFailed, s.RetryScheduled, s.DroppedTTL)
}
