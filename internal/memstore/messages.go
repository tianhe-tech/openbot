package memstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

type ArchiveSession struct {
	ID         string
	Adapter    string
	UserID     string
	ThreadID   string
	UpstreamID string
	Project    string
	WorkDir    string
	CreatedAt  int64
}

type ArchiveTurn struct {
	ID          string
	DeliveryKey string
	StartedAt   int64
}

type ArchiveMessage struct {
	AttemptID         string `json:",omitempty"`
	UpstreamMessageID string `json:",omitempty"`
	Session           ArchiveSession
	Turn              ArchiveTurn
	ID                string
	Sequence          int64
	Role              string
	State             string
	Text              string
	CreatedAt         int64
	Fidelity          string
}

func archiveID(parts ...string) string {
	encoded, _ := json.Marshal(parts)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func validateArchiveMessage(message ArchiveMessage) error {
	for _, value := range []string{message.Session.ID, message.Session.Adapter, message.Session.UserID, message.Turn.ID, message.Turn.DeliveryKey, message.ID} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("memstore: archive identity fields are required")
		}
		if !utf8.ValidString(value) {
			return fmt.Errorf("memstore: archive identity must be valid UTF-8")
		}
	}
	if (message.Fidelity != "legacy_unknown" && (message.Session.CreatedAt <= 0 || message.Turn.StartedAt <= 0 || message.CreatedAt <= 0)) || message.Sequence < 0 {
		return fmt.Errorf("memstore: stable millisecond timestamps and nonnegative sequence are required")
	}
	if message.Role != "user" && message.Role != "assistant" && message.Role != "tool" {
		return fmt.Errorf("memstore: unsupported archive role")
	}
	if message.State != "completed" && message.State != "failed" && message.State != "interrupted" {
		return fmt.Errorf("memstore: only finalized messages can be archived")
	}
	if message.Fidelity != "captured" && message.Fidelity != "legacy_unknown" {
		return fmt.Errorf("memstore: unsupported archive fidelity")
	}
	if message.Fidelity == "captured" && !utf8.ValidString(message.Text) {
		return fmt.Errorf("memstore: captured text must be valid UTF-8")
	}
	return nil
}

func (writer *ArchiveWriter) RecordMessage(ctx context.Context, message ArchiveMessage) error {
	if err := validateArchiveMessage(message); err != nil {
		return err
	}
	encoded, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return writer.submit(ctx, len(encoded)+256, func(ctx context.Context, tx *sql.Tx) error {
		return writeArchiveMessage(ctx, tx, message)
	})
}

func writeArchiveMessage(ctx context.Context, tx *sql.Tx, message ArchiveMessage) error {
	if err := validateArchiveMessage(message); err != nil {
		return err
	}
	encoded, err := json.Marshal(message)
	if err != nil {
		return err
	}
	checksum := sha256.Sum256([]byte(message.Text))
	fingerprint := archiveID(string(encoded), hex.EncodeToString(checksum[:]))
	var existing string
	err = tx.QueryRowContext(ctx, `SELECT fingerprint FROM mem_v2_messages WHERE id=?`, message.ID).Scan(&existing)
	if err == nil {
		if existing != fingerprint {
			return ErrArchiveConflict
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	session := message.Session
	principal := archiveID("principal", session.Adapter, session.UserID)
	result, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_sessions
		(id,principal_id,adapter,user_id,thread_id,upstream_session_id,project_key,work_dir,created_at)
		VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET id=excluded.id
		WHERE principal_id=excluded.principal_id AND adapter=excluded.adapter AND user_id=excluded.user_id
		AND thread_id=excluded.thread_id AND upstream_session_id=excluded.upstream_session_id
		AND project_key=excluded.project_key AND work_dir=excluded.work_dir AND created_at=excluded.created_at`,
		session.ID, principal, session.Adapter, session.UserID, session.ThreadID, session.UpstreamID, session.Project, session.WorkDir, session.CreatedAt)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrArchiveConflict
	}
	result, err = tx.ExecContext(ctx, `INSERT INTO mem_v2_turns
		(id,principal_id,session_id,delivery_key,started_at) VALUES(?,?,?,?,?)
		ON CONFLICT(id) DO UPDATE SET id=excluded.id
		WHERE principal_id=excluded.principal_id AND session_id=excluded.session_id
		AND delivery_key=excluded.delivery_key AND started_at=excluded.started_at`,
		message.Turn.ID, principal, session.ID, message.Turn.DeliveryKey, message.Turn.StartedAt)
	if err != nil {
		return err
	}
	if affected, err := result.RowsAffected(); err != nil || affected != 1 {
		return ErrArchiveConflict
	}
	blockID := archiveID("block", message.ID)
	codec, payload, rawBytes, err := compressBlock([]byte(message.Text))
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_blocks
		(id,principal_id,codec,codec_version,raw_bytes,stored_bytes,checksum,payload) VALUES(?,?,?,?,?,?,?,?)`,
		blockID, principal, codec, blockCodecVersion, rawBytes, len(payload),
		hex.EncodeToString(checksum[:]), payload); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_messages
		(id,principal_id,session_id,turn_id,seq,role,state,block_id,byte_length,created_at,fidelity,fingerprint,attempt_id,upstream_message_id)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, message.ID, principal, session.ID, message.Turn.ID, message.Sequence,
		message.Role, message.State, blockID, len(message.Text), message.CreatedAt, message.Fidelity, fingerprint,
		nullArchiveID(message.AttemptID), message.UpstreamMessageID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO mem_v2_jobs(id,kind,source_id,source_revision) VALUES(?,'index',?,1)`, archiveID("index", message.ID), message.ID)
	return err
}

func nullArchiveID(value string) any {
	if value == "" {
		return nil
	}
	return value
}
