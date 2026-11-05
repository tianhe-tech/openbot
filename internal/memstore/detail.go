package memstore

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Detail and scroll budgets. The budget bounds the response, never the stored
// original: a truncated response always carries continuation metadata so the
// caller can fetch the rest.
const (
	defaultDetailBudget = 32 * 1024
	maxDetailBudget     = 256 * 1024
	defaultScrollLimit  = 20
	maxScrollLimit      = 100
)

// MessageDetail is one full stored message body.
type MessageDetail struct {
	MessageID  string `json:"message_id"`
	SessionID  string `json:"session_id"`
	TurnID     string `json:"turn_id"`
	Role       string `json:"role"`
	State      string `json:"state"`
	Seq        int64  `json:"seq"`
	CreatedAt  int64  `json:"created_at"`
	Project    string `json:"project,omitempty"`
	WorkDir    string `json:"work_dir,omitempty"`
	Fidelity   string `json:"fidelity"`
	Text       string `json:"text"`
	Truncated  bool   `json:"truncated"`
	ByteLength int    `json:"byte_length"`
}

// DetailResult is the bounded answer to a detail request.
type DetailResult struct {
	Messages   []MessageDetail `json:"messages"`
	Truncated  bool            `json:"truncated"`
	NextCursor string          `json:"next_cursor,omitempty"`
	Notice     string          `json:"notice,omitempty"`
}

// ScrollResult is one page of adjacent messages.
type ScrollResult struct {
	Messages   []MessageDetail `json:"messages"`
	NextCursor string          `json:"next_cursor,omitempty"`
	PrevCursor string          `json:"prev_cursor,omitempty"`
	Truncated  bool            `json:"truncated"`
}

type scrollCursor struct {
	SessionID string `json:"s"`
	Seq       int64  `json:"q"`
	Direction string `json:"d"`
}

func encodeScrollCursor(cursor scrollCursor) string {
	encoded, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func decodeScrollCursor(value string) (scrollCursor, error) {
	var cursor scrollCursor
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return cursor, fmt.Errorf("memstore: malformed scroll cursor")
	}
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return cursor, fmt.Errorf("memstore: malformed scroll cursor")
	}
	if cursor.SessionID == "" {
		return cursor, fmt.Errorf("memstore: malformed scroll cursor")
	}
	return cursor, nil
}

// GetMessages returns the original bodies for the requested message IDs. Every
// ID is re-authorized against the scope, so a caller cannot read another
// principal's message by guessing an ID.
func (s *Store) GetMessages(ctx context.Context, scope SearchScope, ids []string, budget int) (DetailResult, error) {
	principal, err := scope.principal()
	if err != nil {
		return DetailResult{}, err
	}
	if len(ids) == 0 {
		return DetailResult{}, fmt.Errorf("memstore: at least one message ID is required")
	}
	if len(ids) > maxScrollLimit {
		return DetailResult{}, fmt.Errorf("memstore: at most %d message IDs per request", maxScrollLimit)
	}
	if budget <= 0 {
		budget = defaultDetailBudget
	}
	if budget > maxDetailBudget {
		budget = maxDetailBudget
	}
	var result DetailResult
	remaining := budget
	for index, id := range ids {
		detail, err := s.loadMessageDetail(ctx, principal, id)
		if errors.Is(err, sql.ErrNoRows) {
			// A missing or unauthorized ID is reported, never silently skipped:
			// silence would let a caller infer existence from ordering.
			result.Notice = fmt.Sprintf("message %s is not available in this scope", id)
			continue
		}
		if err != nil {
			return DetailResult{}, err
		}
		if remaining <= 0 {
			result.Truncated = true
			result.NextCursor = strings.Join(ids[index:], ",")
			break
		}
		if len(detail.Text) > remaining {
			detail.Text = detail.Text[:alignRuneStart(detail.Text, remaining)]
			detail.Truncated = true
			result.Truncated = true
			result.NextCursor = strings.Join(ids[index:], ",")
			result.Messages = append(result.Messages, detail)
			break
		}
		remaining -= len(detail.Text)
		result.Messages = append(result.Messages, detail)
	}
	return result, nil
}

// Scroll returns messages adjacent to a cursor position inside one session.
// Pagination is keyset-based on (created_at, seq), so it stays stable while new
// messages are appended.
func (s *Store) Scroll(ctx context.Context, scope SearchScope, sessionID, cursor string, limit int) (ScrollResult, error) {
	principal, err := scope.principal()
	if err != nil {
		return ScrollResult{}, err
	}
	if strings.TrimSpace(sessionID) == "" {
		return ScrollResult{}, fmt.Errorf("memstore: session ID is required")
	}
	if limit <= 0 {
		limit = defaultScrollLimit
	}
	if limit > maxScrollLimit {
		limit = maxScrollLimit
	}
	// Verify the session belongs to the scope before reading anything from it.
	var exists int
	err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mem_v2_sessions WHERE id=? AND principal_id=?`,
		sessionID, principal).Scan(&exists)
	if err != nil {
		return ScrollResult{}, err
	}
	if exists == 0 {
		return ScrollResult{}, fmt.Errorf("memstore: session %s is not available in this scope", sessionID)
	}
	position := scrollCursor{SessionID: sessionID, Seq: -1, Direction: "forward"}
	if cursor != "" {
		position, err = decodeScrollCursor(cursor)
		if err != nil {
			return ScrollResult{}, err
		}
		if position.SessionID != sessionID {
			return ScrollResult{}, fmt.Errorf("memstore: scroll cursor belongs to a different session")
		}
	}
	query := `SELECT m.id,m.session_id,m.turn_id,m.role,m.state,m.seq,m.created_at,
			s.project_key,s.work_dir,m.fidelity,b.codec,b.codec_version,b.raw_bytes,b.payload
		FROM mem_v2_messages m
		JOIN mem_v2_sessions s ON s.id=m.session_id
		JOIN mem_v2_blocks b ON b.id=m.block_id
		WHERE m.session_id=? AND m.principal_id=?`
	args := []any{sessionID, principal}
	if position.Seq >= 0 {
		if position.Direction == "backward" {
			query += ` AND m.seq < ? ORDER BY m.seq DESC LIMIT ?`
		} else {
			query += ` AND m.seq > ? ORDER BY m.seq ASC LIMIT ?`
		}
		args = append(args, position.Seq, limit+1)
	} else {
		query += ` ORDER BY m.seq ASC LIMIT ?`
		args = append(args, limit+1)
	}
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return ScrollResult{}, fmt.Errorf("memstore: scroll: %w", err)
	}
	defer rows.Close()
	var result ScrollResult
	for rows.Next() {
		var detail MessageDetail
		var payload []byte
		var codec string
		var codecVersion int
		var rawBytes int64
		if err := rows.Scan(&detail.MessageID, &detail.SessionID, &detail.TurnID, &detail.Role,
			&detail.State, &detail.Seq, &detail.CreatedAt, &detail.Project, &detail.WorkDir,
			&detail.Fidelity, &codec, &codecVersion, &rawBytes, &payload); err != nil {
			return ScrollResult{}, err
		}
		decoded, err := decodeBlock(codec, codecVersion, rawBytes, payload)
		if err != nil {
			return ScrollResult{}, fmt.Errorf("memstore: scroll message %s: %w", detail.MessageID, err)
		}
		detail.Text = string(decoded)
		detail.ByteLength = len(decoded)
		result.Messages = append(result.Messages, detail)
	}
	if err := rows.Err(); err != nil {
		return ScrollResult{}, err
	}
	rows.Close()
	if len(result.Messages) > limit {
		result.Messages = result.Messages[:limit]
		result.Truncated = true
	}
	if len(result.Messages) > 0 {
		first, last := result.Messages[0], result.Messages[len(result.Messages)-1]
		if result.Truncated {
			result.NextCursor = encodeScrollCursor(scrollCursor{SessionID: sessionID, Seq: last.Seq, Direction: "forward"})
		}
		if position.Seq >= 0 || first.Seq > 0 {
			result.PrevCursor = encodeScrollCursor(scrollCursor{SessionID: sessionID, Seq: first.Seq, Direction: "backward"})
		}
	}
	return result, nil
}

func (s *Store) loadMessageDetail(ctx context.Context, principal, id string) (MessageDetail, error) {
	var detail MessageDetail
	var payload []byte
	var codec string
	var codecVersion int
	var rawBytes int64
	err := s.db.QueryRowContext(ctx, `SELECT m.id,m.session_id,m.turn_id,m.role,m.state,m.seq,m.created_at,
			s.project_key,s.work_dir,m.fidelity,b.codec,b.codec_version,b.raw_bytes,b.payload
		FROM mem_v2_messages m
		JOIN mem_v2_sessions s ON s.id=m.session_id
		JOIN mem_v2_blocks b ON b.id=m.block_id
		WHERE m.id=? AND m.principal_id=?`, id, principal).Scan(
		&detail.MessageID, &detail.SessionID, &detail.TurnID, &detail.Role, &detail.State,
		&detail.Seq, &detail.CreatedAt, &detail.Project, &detail.WorkDir, &detail.Fidelity,
		&codec, &codecVersion, &rawBytes, &payload)
	if err != nil {
		return detail, err
	}
	decoded, err := decodeBlock(codec, codecVersion, rawBytes, payload)
	if err != nil {
		return detail, fmt.Errorf("memstore: load message %s: %w", id, err)
	}
	detail.Text = string(decoded)
	detail.ByteLength = len(decoded)
	return detail, nil
}
