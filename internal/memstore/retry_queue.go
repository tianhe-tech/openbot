package memstore

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// RetryStatus values for pending_retry rows.
const (
	RetryStatusPending    = "pending"
	RetryStatusProcessing = "processing"
	RetryStatusDone       = "done"
	RetryStatusFailed     = "failed"
)

// RetryTTL is how long a pending retry record remains eligible (default 7 days).
const RetryTTL = 7 * 24 * time.Hour

// PendingRetry is one failed message request awaiting re-processing.
type PendingRetry struct {
	ID              string
	Adapter         string // "feishu" | "dingtalk" | "wecom"
	UserID          string
	ThreadID        string
	Channel         string // feishu chat_id, dingtalk webhook url, etc.
	Content         string // original message text
	AttachmentsJSON string // JSON-encoded []opencode.Attachment (may be empty)
	Metadata        map[string]string
	FailReason      string
	Status          string
	RetryCount      int
	CreatedAt       time.Time
	LastAttemptAt   time.Time
}

// NewRetryID generates a short random ID for a pending retry record.
func NewRetryID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("rt_%d", time.Now().UnixNano())
	}
	return "rt_" + hex.EncodeToString(b[:])
}

// SavePendingRetry inserts a new pending retry record.
// Dedup: if a row with the same (adapter, user_id, thread_id) and status=pending
// already exists and was created within the last minute, the insert is skipped
// (returns nil, false).
func (s *Store) SavePendingRetry(r PendingRetry) (id string, inserted bool, err error) {
	if strings.TrimSpace(r.UserID) == "" || strings.TrimSpace(r.Adapter) == "" {
		return "", false, fmt.Errorf("memstore: SavePendingRetry: empty adapter or user_id")
	}
	if r.ID == "" {
		r.ID = NewRetryID()
	}
	if r.CreatedAt.IsZero() {
		r.CreatedAt = time.Now()
	}
	if r.Status == "" {
		r.Status = RetryStatusPending
	}

	// Dedup: skip if a pending row for the same thread+content was added <60s ago.
	dedupCutoff := time.Now().Add(-60 * time.Second).Unix()
	var existing string
	dedupErr := s.db.QueryRow(`
		SELECT id FROM pending_retry
		WHERE adapter=? AND user_id=? AND thread_id=? AND content=?
		  AND status=? AND created_at >= ?
		LIMIT 1`,
		r.Adapter, r.UserID, r.ThreadID, r.Content, RetryStatusPending, dedupCutoff,
	).Scan(&existing)
	if dedupErr == nil && existing != "" {
		// Duplicate — skip silently.
		return existing, false, nil
	}

	metaJSON := "{}"
	if len(r.Metadata) > 0 {
		if b, jerr := json.Marshal(r.Metadata); jerr == nil {
			metaJSON = string(b)
		}
	}

	_, execErr := s.db.Exec(`
		INSERT INTO pending_retry
			(id, adapter, user_id, thread_id, channel, content, attachments_json, metadata_json,
			 fail_reason, status, retry_count, created_at, last_attempt_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		r.ID, r.Adapter, r.UserID, r.ThreadID, r.Channel, r.Content,
		r.AttachmentsJSON, metaJSON, r.FailReason, r.Status, r.RetryCount,
		r.CreatedAt.Unix(), r.CreatedAt.Unix(),
	)
	if execErr != nil {
		return "", false, fmt.Errorf("memstore: save pending retry: %w", execErr)
	}
	return r.ID, true, nil
}

// LoadPendingRetries returns up to limit rows with status=pending, ordered by
// created_at ASC (oldest first), skipping rows older than RetryTTL.
func (s *Store) LoadPendingRetries(limit int) ([]PendingRetry, error) {
	if limit <= 0 {
		limit = 20
	}
	cutoff := time.Now().Add(-RetryTTL).Unix()
	rows, err := s.db.Query(`
		SELECT id, adapter, user_id, thread_id, channel, content,
		       attachments_json, metadata_json, fail_reason, status,
		       retry_count, created_at, last_attempt_at
		FROM   pending_retry
		WHERE  status = ? AND created_at >= ?
		ORDER  BY created_at ASC
		LIMIT  ?`,
		RetryStatusPending, cutoff, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("memstore: load pending retries: %w", err)
	}
	defer rows.Close()
	return scanRetries(rows)
}

// CountPendingRetries returns counts by status for a summary view.
func (s *Store) CountPendingRetries() (pending, processing, done, failed int, err error) {
	rows, qerr := s.db.Query(`
		SELECT status, COUNT(*) FROM pending_retry
		WHERE  created_at >= ?
		GROUP  BY status`, time.Now().Add(-RetryTTL).Unix())
	if qerr != nil {
		return 0, 0, 0, 0, fmt.Errorf("memstore: count retries: %w", qerr)
	}
	defer rows.Close()
	for rows.Next() {
		var st string
		var cnt int
		if serr := rows.Scan(&st, &cnt); serr != nil {
			continue
		}
		switch st {
		case RetryStatusPending:
			pending = cnt
		case RetryStatusProcessing:
			processing = cnt
		case RetryStatusDone:
			done = cnt
		case RetryStatusFailed:
			failed = cnt
		}
	}
	return
}

// MarkRetryProcessing marks a row as processing (claimed by a worker).
func (s *Store) MarkRetryProcessing(id string) error {
	_, err := s.db.Exec(`
		UPDATE pending_retry SET status=?, last_attempt_at=?
		WHERE  id=?`,
		RetryStatusProcessing, time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("memstore: mark retry processing: %w", err)
	}
	return nil
}

// MarkRetryDone marks a row as done (successfully retried).
func (s *Store) MarkRetryDone(id string) error {
	_, err := s.db.Exec(`
		UPDATE pending_retry SET status=?, last_attempt_at=?
		WHERE  id=?`,
		RetryStatusDone, time.Now().Unix(), id)
	if err != nil {
		return fmt.Errorf("memstore: mark retry done: %w", err)
	}
	return nil
}

// MarkRetryFailed increments retry_count and, when count >= maxRetries, sets
// status=failed. Otherwise resets to pending so the next run can try again.
func (s *Store) MarkRetryFailed(id string, maxRetries int) (permanent bool, err error) {
	var count int
	if serr := s.db.QueryRow(`SELECT retry_count FROM pending_retry WHERE id=?`, id).Scan(&count); serr != nil {
		return false, fmt.Errorf("memstore: mark retry failed scan: %w", serr)
	}
	count++
	newStatus := RetryStatusPending
	if count >= maxRetries {
		newStatus = RetryStatusFailed
		permanent = true
	}
	_, execErr := s.db.Exec(`
		UPDATE pending_retry SET status=?, retry_count=?, last_attempt_at=?
		WHERE  id=?`,
		newStatus, count, time.Now().Unix(), id)
	if execErr != nil {
		return false, fmt.Errorf("memstore: mark retry failed update: %w", execErr)
	}
	return permanent, nil
}

// ClearRetryQueue deletes all rows for given status (or all when status=="").
// Primarily used by the /retry clear command.
func (s *Store) ClearRetryQueue(status string) (int64, error) {
	var res sql.Result
	var execErr error
	if status == "" {
		res, execErr = s.db.Exec(`DELETE FROM pending_retry`)
	} else {
		res, execErr = s.db.Exec(`DELETE FROM pending_retry WHERE status=?`, status)
	}
	if execErr != nil {
		return 0, fmt.Errorf("memstore: clear retry queue: %w", execErr)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// PurgeExpiredRetries removes rows older than 2*RetryTTL; safe to call periodically.
func (s *Store) PurgeExpiredRetries() error {
	cutoff := time.Now().Add(-2 * RetryTTL).Unix()
	_, err := s.db.Exec(`DELETE FROM pending_retry WHERE created_at < ?`, cutoff)
	return err
}

func scanRetries(rows *sql.Rows) ([]PendingRetry, error) {
	var out []PendingRetry
	for rows.Next() {
		var r PendingRetry
		var createdAt, lastAttemptAt int64
		var metaJSON string
		if err := rows.Scan(
			&r.ID, &r.Adapter, &r.UserID, &r.ThreadID, &r.Channel, &r.Content,
			&r.AttachmentsJSON, &metaJSON, &r.FailReason, &r.Status,
			&r.RetryCount, &createdAt, &lastAttemptAt,
		); err != nil {
			return nil, fmt.Errorf("memstore: scan retry row: %w", err)
		}
		r.CreatedAt = time.Unix(createdAt, 0)
		r.LastAttemptAt = time.Unix(lastAttemptAt, 0)
		if metaJSON != "" && metaJSON != "{}" {
			_ = json.Unmarshal([]byte(metaJSON), &r.Metadata)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
