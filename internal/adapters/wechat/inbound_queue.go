package wechat

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// inboundQueue is the durable pre-admission buffer for inbound text.
//
// The debounce batcher holds messages in memory for a few seconds before they
// reach the OpenCode client. That window is the last place where a crash can
// silently lose a user request: the message was accepted by iLink, the user saw
// it delivered, and nothing durable recorded it. This queue closes that gap by
// persisting each buffered message before the debounce timer is armed and
// deleting it only after dispatch succeeds.
//
// Rows are per inbound message (one delivery ID each), not per batch, so a
// merged batch is simply several rows replayed in id order. Media messages
// never enter this queue: they bypass the batcher and dispatch immediately.
type inboundQueue struct {
	db *sql.DB
}

// inboundQueueEntry is one persisted inbound text message.
type inboundQueueEntry struct {
	ID           int64
	UserID       string
	ChatID       string
	DeliveryID   string
	Content      string
	ContextToken string
	CreatedAt    int64
}

// maxInboundReplayAge bounds how old a persisted inbound message may be and
// still be replayed after a restart. A request that has been sitting for a day
// is no longer what the user is waiting for, and replaying it would inject a
// stale instruction into a fresh session.
const maxInboundReplayAge = 24 * time.Hour

func newInboundQueue(dbPath string) (*inboundQueue, error) {
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000", filepath.ToSlash(dbPath))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("wechat: open inbound queue db: %w", err)
	}
	db.SetMaxOpenConns(1)
	q := &inboundQueue{db: db}
	if err := q.migrate(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return q, nil
}

func (q *inboundQueue) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS wechat_inbound_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id TEXT NOT NULL,
			chat_id TEXT NOT NULL DEFAULT '',
			delivery_id TEXT NOT NULL,
			content TEXT NOT NULL,
			context_token TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL
		)`,
		// delivery_id is the idempotency key: re-persisting the same inbound
		// message (retry, duplicate poll delivery) must not create a second row.
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_wechat_inbound_delivery ON wechat_inbound_queue(delivery_id)`,
		`CREATE INDEX IF NOT EXISTS idx_wechat_inbound_order ON wechat_inbound_queue(id)`,
	}
	for _, stmt := range stmts {
		if _, err := q.db.Exec(stmt); err != nil {
			return fmt.Errorf("wechat: migrate inbound queue: %w", err)
		}
	}
	return nil
}

// Save persists one inbound message. It is idempotent on delivery ID, so a
// duplicate poll delivery or a retried persist cannot duplicate the request.
func (q *inboundQueue) Save(userID, chatID, deliveryID, content, contextToken string) error {
	if q == nil {
		return nil
	}
	if strings.TrimSpace(deliveryID) == "" {
		return errors.New("wechat: inbound queue requires a delivery ID")
	}
	if strings.TrimSpace(content) == "" {
		return nil
	}
	_, err := q.db.Exec(`
		INSERT INTO wechat_inbound_queue (user_id, chat_id, delivery_id, content, context_token, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(delivery_id) DO NOTHING
	`, userID, chatID, deliveryID, content, contextToken, time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("wechat: inbound queue save: %w", err)
	}
	return nil
}

// Delete removes the rows for the given delivery IDs. Called only after the
// batch has been dispatched successfully, so a failed dispatch leaves the rows
// in place for replay.
func (q *inboundQueue) Delete(deliveryIDs []string) error {
	if q == nil || len(deliveryIDs) == 0 {
		return nil
	}
	placeholders := make([]string, 0, len(deliveryIDs))
	args := make([]any, 0, len(deliveryIDs))
	for _, id := range deliveryIDs {
		if strings.TrimSpace(id) == "" {
			continue
		}
		placeholders = append(placeholders, "?")
		args = append(args, id)
	}
	if len(args) == 0 {
		return nil
	}
	query := `DELETE FROM wechat_inbound_queue WHERE delivery_id IN (` + strings.Join(placeholders, ",") + `)`
	if _, err := q.db.Exec(query, args...); err != nil {
		return fmt.Errorf("wechat: inbound queue delete: %w", err)
	}
	return nil
}

// List returns persisted messages in arrival order, oldest first.
func (q *inboundQueue) List(limit int) ([]inboundQueueEntry, error) {
	if q == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := q.db.Query(`
		SELECT id, user_id, chat_id, delivery_id, content, context_token, created_at
		FROM wechat_inbound_queue ORDER BY id ASC LIMIT ?
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("wechat: inbound queue list: %w", err)
	}
	defer rows.Close()
	var out []inboundQueueEntry
	for rows.Next() {
		var entry inboundQueueEntry
		if err := rows.Scan(&entry.ID, &entry.UserID, &entry.ChatID, &entry.DeliveryID,
			&entry.Content, &entry.ContextToken, &entry.CreatedAt); err != nil {
			return nil, fmt.Errorf("wechat: inbound queue scan: %w", err)
		}
		out = append(out, entry)
	}
	return out, rows.Err()
}

// Count returns how many messages are currently buffered.
func (q *inboundQueue) Count() (int, error) {
	if q == nil {
		return 0, nil
	}
	var n int
	if err := q.db.QueryRow(`SELECT COUNT(1) FROM wechat_inbound_queue`).Scan(&n); err != nil {
		return 0, fmt.Errorf("wechat: inbound queue count: %w", err)
	}
	return n, nil
}

// Prune drops rows older than maxInboundReplayAge. Called at startup so a
// long-dead backlog cannot be replayed into a fresh session.
func (q *inboundQueue) Prune(now time.Time) (int64, error) {
	if q == nil {
		return 0, nil
	}
	cutoff := now.Add(-maxInboundReplayAge).UnixMilli()
	res, err := q.db.Exec(`DELETE FROM wechat_inbound_queue WHERE created_at < ?`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("wechat: inbound queue prune: %w", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

func (q *inboundQueue) Close() error {
	if q == nil {
		return nil
	}
	return q.db.Close()
}

// persistInbound records a buffered message. Failures are logged, never fatal:
// losing durability is strictly better than dropping the message outright, and
// the in-memory batch still dispatches normally.
func (b *inboundBatcher) persistInbound(msg *WeixinMessage, text string) {
	if b.queue == nil || strings.TrimSpace(text) == "" {
		return
	}
	chatID := msg.GroupID
	if chatID == "" {
		chatID = msg.FromUserID
	}
	for _, deliveryID := range msg.deliveryIDs {
		if err := b.queue.Save(msg.FromUserID, chatID, deliveryID, text, msg.ContextToken); err != nil {
			log.Printf("wechat: inbound queue persist failed user=%s delivery=%s: %v", msg.FromUserID, deliveryID, err)
		}
	}
}

// clearInbound removes the persisted rows for a dispatched batch.
func (b *inboundBatcher) clearInbound(msg *WeixinMessage) {
	if b.queue == nil {
		return
	}
	if err := b.queue.Delete(msg.deliveryIDs); err != nil {
		log.Printf("wechat: inbound queue clear failed user=%s: %v", msg.FromUserID, err)
	}
}

// replayPending re-dispatches messages that were buffered when the process
// stopped. It runs once at startup, before polling begins, so there is no
// concurrent live batch to race with.
func (h *Handler) replayPendingInbound(ctx context.Context) {
	replayInboundQueue(ctx, h.inbound, h.handleBatchedMessage)
}

// replayInboundQueue is the testable core of startup replay.
//
// Replay is deliberately conservative: rows are grouped by sender+chat and
// concatenated in arrival order (matching what the debounce window would have
// produced), stale rows are pruned first, and a row is deleted only after its
// dispatch succeeds. A failed replay leaves the row for the next restart.
func replayInboundQueue(ctx context.Context, q *inboundQueue, dispatch func(context.Context, *WeixinMessage) error) {
	if q == nil || dispatch == nil {
		return
	}
	if pruned, err := q.Prune(time.Now()); err != nil {
		log.Printf("wechat: inbound queue prune failed: %v", err)
	} else if pruned > 0 {
		log.Printf("wechat: inbound queue pruned %d stale message(s) older than %s", pruned, maxInboundReplayAge)
	}
	entries, err := q.List(200)
	if err != nil {
		log.Printf("wechat: inbound queue replay list failed: %v", err)
		return
	}
	if len(entries) == 0 {
		return
	}

	type group struct {
		userID, chatID, contextToken string
		deliveryIDs                  []string
		parts                        []string
	}
	order := make([]string, 0, len(entries))
	groups := make(map[string]*group, len(entries))
	for _, entry := range entries {
		key := entry.UserID + "|" + entry.ChatID
		g, ok := groups[key]
		if !ok {
			g = &group{userID: entry.UserID, chatID: entry.ChatID, contextToken: entry.ContextToken}
			groups[key] = g
			order = append(order, key)
		}
		g.deliveryIDs = append(g.deliveryIDs, entry.DeliveryID)
		g.parts = append(g.parts, entry.Content)
		if entry.ContextToken != "" {
			g.contextToken = entry.ContextToken
		}
	}

	log.Printf("wechat: inbound queue replaying %d buffered message(s) in %d batch(es)", len(entries), len(order))
	for _, key := range order {
		g := groups[key]
		msg := &WeixinMessage{
			FromUserID:   g.userID,
			GroupID:      g.chatID,
			ContextToken: g.contextToken,
			deliveryIDs:  g.deliveryIDs,
		}
		if g.chatID == g.userID {
			msg.GroupID = ""
		}
		setMessageText(msg, strings.Join(g.parts, "\n"))
		if err := dispatch(ctx, msg); err != nil {
			log.Printf("wechat: inbound queue replay dispatch failed user=%s: %v (kept for next restart)", g.userID, err)
			continue
		}
		if err := q.Delete(g.deliveryIDs); err != nil {
			log.Printf("wechat: inbound queue replay clear failed user=%s: %v", g.userID, err)
		}
	}
}

// marshalDeliveryIDs is used by tests and diagnostics to render the persisted
// identity set without depending on the internal field layout.
func marshalDeliveryIDs(ids []string) string {
	encoded, _ := json.Marshal(ids)
	return string(encoded)
}
