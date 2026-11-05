package memstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/user/opencode-gateway/internal/opencode"
)

var _ opencode.ConversationRecorder = (*ArchiveWriter)(nil)

func attemptUserMessageID(attemptID string) string {
	if strings.HasPrefix(attemptID, "attempt_msg_") {
		return strings.TrimPrefix(attemptID, "attempt_")
	}
	return "msg_" + archiveID("upstream-user", attemptID)
}

func (writer *ArchiveWriter) BeginTurn(ctx context.Context, input opencode.TurnInput) (opencode.TurnRef, error) {
	for _, value := range []string{input.Adapter, input.UserID, input.DeliveryID} {
		if strings.TrimSpace(value) == "" || !utf8.ValidString(value) {
			return opencode.TurnRef{}, fmt.Errorf("memstore: trusted identity and delivery ID are required")
		}
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return opencode.TurnRef{}, err
	}
	principal := archiveID("principal", input.Adapter, input.UserID)
	turnID := archiveID("turn", principal, input.DeliveryID)
	ref := opencode.TurnRef{ID: turnID}
	err = writer.submit(ctx, len(encoded)+1024, func(ctx context.Context, tx *sql.Tx) error {
		var body []byte
		var thread, directory string
		var codec string
		var codecVersion int
		var rawBytes int64
		err := tx.QueryRowContext(ctx, `SELECT b.codec,b.codec_version,b.raw_bytes,b.payload,s.thread_id,s.work_dir FROM mem_v2_turns t
			JOIN mem_v2_sessions s ON s.id=t.session_id JOIN mem_v2_messages m ON m.turn_id=t.id AND m.role='user'
			JOIN mem_v2_blocks b ON b.id=m.block_id WHERE t.id=?`, turnID).Scan(&codec, &codecVersion, &rawBytes, &body, &thread, &directory)
		if err == nil {
			decoded, decodeErr := decodeBlock(codec, codecVersion, rawBytes, body)
			if decodeErr != nil {
				return fmt.Errorf("memstore: begin turn %s: %w", turnID, decodeErr)
			}
			if string(decoded) != input.Content || thread != input.ThreadID || directory != input.WorkDir {
				return ErrArchiveConflict
			}
			ref.Existing = true
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		threadKey := input.ThreadID
		if threadKey == "" {
			threadKey = "delivery:" + input.DeliveryID
		}
		sessionID := archiveID("conversation", principal, threadKey, input.WorkDir)
		now := time.Now().UnixMilli()
		createdAt := now
		err = tx.QueryRowContext(ctx, `SELECT created_at FROM mem_v2_sessions WHERE id=?`, sessionID).Scan(&createdAt)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var sequence int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq)+1,0) FROM mem_v2_messages WHERE session_id=?`, sessionID).Scan(&sequence); err != nil {
			return err
		}
		message := ArchiveMessage{
			Session: ArchiveSession{ID: sessionID, Adapter: input.Adapter, UserID: input.UserID, ThreadID: input.ThreadID, WorkDir: input.WorkDir, Project: workDirBasename(input.WorkDir), CreatedAt: createdAt},
			Turn:    ArchiveTurn{ID: turnID, DeliveryKey: input.DeliveryID, StartedAt: now},
			ID:      archiveID("input", turnID), Sequence: sequence, Role: "user", State: "completed", Text: input.Content, CreatedAt: now, Fidelity: "captured",
		}
		if err := writeArchiveMessage(ctx, tx, message); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE mem_v2_turns SET dispatch_state='not_dispatched' WHERE id=?`, turnID)
		return err
	})
	if err != nil {
		return opencode.TurnRef{}, err
	}
	return ref, nil
}

func (writer *ArchiveWriter) BindAttempt(ctx context.Context, input opencode.AttemptInput) (opencode.AttemptRef, error) {
	if input.ID == "" || input.SessionID == "" || input.Turn.ID == "" {
		return opencode.AttemptRef{}, fmt.Errorf("memstore: attempt identity is required")
	}
	ref := opencode.AttemptRef{ID: input.ID, TurnID: input.Turn.ID, SessionID: input.SessionID, UserMessageID: attemptUserMessageID(input.ID)}
	err := writer.submit(ctx, 1024+len(input.ID)+len(input.SessionID)+len(input.Turn.ID), func(ctx context.Context, tx *sql.Tx) error {
		var turnID, sessionID string
		err := tx.QueryRowContext(ctx, `SELECT turn_id,upstream_session_id FROM mem_v2_attempts WHERE id=?`, input.ID).Scan(&turnID, &sessionID)
		if err == nil {
			if turnID != ref.TurnID || sessionID != ref.SessionID {
				return ErrArchiveConflict
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM mem_v2_turns WHERE id=?`, ref.TurnID).Scan(&status); err != nil {
			return err
		}
		// A completed turn is normally closed to new attempts: that is what
		// makes duplicate delivery IDs fail closed. A deliberate retry of the
		// same delivery (token-overflow recovery, explicit user retry) reopens
		// the turn with a fresh attempt instead of replaying silently.
		if status == "completed" && !input.Retry {
			return ErrArchiveConflict
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_attempts(id,turn_id,upstream_session_id,attempt_no)
			SELECT ?,?,?,COALESCE(MAX(attempt_no)+1,0) FROM mem_v2_attempts WHERE turn_id=?`, ref.ID, ref.TurnID, ref.SessionID, ref.TurnID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_jobs(id,kind,source_id,source_revision) VALUES(?,'reconcile',?,1)`, archiveID("reconcile", ref.ID), ref.ID); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE mem_v2_turns SET status='active',dispatch_state='uncertain',completed_at=0 WHERE id=?`, ref.TurnID)
		return err
	})
	if err != nil {
		return opencode.AttemptRef{}, err
	}
	return ref, nil
}

func loadAttemptMessage(ctx context.Context, tx *sql.Tx, ref opencode.AttemptRef) (ArchiveMessage, error) {
	var message ArchiveMessage
	var upstream string
	err := tx.QueryRowContext(ctx, `SELECT s.id,s.adapter,s.user_id,s.thread_id,s.upstream_session_id,s.project_key,s.work_dir,s.created_at,
		t.id,t.delivery_key,t.started_at,a.upstream_session_id FROM mem_v2_attempts a
		JOIN mem_v2_turns t ON t.id=a.turn_id JOIN mem_v2_sessions s ON s.id=t.session_id WHERE a.id=? AND a.turn_id=?`, ref.ID, ref.TurnID).Scan(
		&message.Session.ID, &message.Session.Adapter, &message.Session.UserID, &message.Session.ThreadID, &message.Session.UpstreamID,
		&message.Session.Project, &message.Session.WorkDir, &message.Session.CreatedAt, &message.Turn.ID, &message.Turn.DeliveryKey, &message.Turn.StartedAt, &upstream)
	if err != nil {
		return message, err
	}
	if upstream != ref.SessionID || ref.UserMessageID != attemptUserMessageID(ref.ID) {
		return message, ErrArchiveConflict
	}
	return message, nil
}

func (writer *ArchiveWriter) SaveMessage(ctx context.Context, input opencode.MessageInput) error {
	if input.ID == "" {
		return fmt.Errorf("memstore: source message ID is required")
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return writer.submit(ctx, len(encoded)+1024, func(ctx context.Context, tx *sql.Tx) error {
		message, err := loadAttemptMessage(ctx, tx, input.Attempt)
		if err != nil {
			return err
		}
		message.ID = archiveID("assistant", input.Attempt.ID, input.ID)
		message.AttemptID = input.Attempt.ID
		message.UpstreamMessageID = input.ID
		message.Role, message.State, message.Fidelity = "assistant", input.State, "captured"
		message.Text, message.CreatedAt = input.Text, input.CreatedAt
		err = tx.QueryRowContext(ctx, `SELECT seq FROM mem_v2_messages WHERE id=?`, message.ID).Scan(&message.Sequence)
		if errors.Is(err, sql.ErrNoRows) {
			err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq)+1,0) FROM mem_v2_messages WHERE session_id=?`, message.Session.ID).Scan(&message.Sequence)
		}
		if err != nil {
			return err
		}
		return writeArchiveMessage(ctx, tx, message)
	})
}

func (writer *ArchiveWriter) FinishAttempt(ctx context.Context, result opencode.AttemptResult) error {
	switch result.Status {
	case "completed", "failed", "awaiting_reconciliation", "unavailable":
	default:
		return fmt.Errorf("memstore: unsupported attempt outcome")
	}
	return writer.submit(ctx, 1024+len(result.Attempt.ID), func(ctx context.Context, tx *sql.Tx) error {
		if _, err := loadAttemptMessage(ctx, tx, result.Attempt); err != nil {
			return err
		}
		var status string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM mem_v2_attempts WHERE id=?`, result.Attempt.ID).Scan(&status); err != nil {
			return err
		}
		if status == "completed" || status == "failed" {
			return nil
		}
		now := time.Now().UnixMilli()
		completed, jobStatus, nextRun := int64(0), "pending", now+5000
		if result.Status == "completed" || result.Status == "failed" {
			completed, jobStatus = now, "completed"
		}
		if result.Status == "unavailable" {
			nextRun = now + 60000
		}
		if _, err := tx.ExecContext(ctx, `UPDATE mem_v2_attempts SET status=? WHERE id=?`, result.Status, result.Attempt.ID); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE mem_v2_jobs SET status=?,lease_until=0,next_run_at=?,attempts=attempts+1,last_error=? WHERE kind='reconcile' AND source_id=?`, jobStatus, nextRun, result.Status, result.Attempt.ID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE mem_v2_turns SET status=?,completed_at=?,last_reconciled_at=?,dispatch_state=CASE WHEN ? > 0 THEN 'observed' ELSE dispatch_state END
			WHERE id=? AND NOT EXISTS(SELECT 1 FROM mem_v2_attempts newer JOIN mem_v2_attempts current ON current.id=? WHERE newer.turn_id=current.turn_id AND newer.attempt_no>current.attempt_no)`,
			result.Status, completed, now, completed, result.Attempt.TurnID, result.Attempt.ID)
		return err
	})
}

func (writer *ArchiveWriter) PendingAttempts(ctx context.Context, limit int) ([]opencode.AttemptRef, error) {
	if limit < 1 || limit > 100 {
		return nil, fmt.Errorf("memstore: reconciliation limit must be 1-100")
	}
	var attempts []opencode.AttemptRef
	err := writer.submit(ctx, limit*1024, func(ctx context.Context, tx *sql.Tx) error {
		now := time.Now().UnixMilli()
		rows, err := tx.QueryContext(ctx, `SELECT a.id,a.turn_id,a.upstream_session_id FROM mem_v2_attempts a JOIN mem_v2_jobs j ON j.kind='reconcile' AND j.source_id=a.id
			WHERE j.status='pending' AND j.next_run_at<=? AND j.lease_until<=? ORDER BY j.next_run_at,j.id LIMIT ?`, now, now, limit)
		if err != nil {
			return err
		}
		for rows.Next() {
			var ref opencode.AttemptRef
			if err := rows.Scan(&ref.ID, &ref.TurnID, &ref.SessionID); err != nil {
				rows.Close()
				return err
			}
			ref.UserMessageID = attemptUserMessageID(ref.ID)
			attempts = append(attempts, ref)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, ref := range attempts {
			if _, err := tx.ExecContext(ctx, `UPDATE mem_v2_jobs SET lease_until=? WHERE kind='reconcile' AND source_id=?`, now+60000, ref.ID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return attempts, nil
}
