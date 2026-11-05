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
	"time"
	"unicode/utf8"

	"github.com/user/opencode-gateway/internal/opencode"
)

// maxToolOutputBytes bounds how much tool output is retained inline. Larger
// output is truncated and flagged so readers can distinguish a complete record
// from a retained partial one.
const maxToolOutputBytes = 8 * 1024

// maxAttachmentURLBytes bounds stored attachment URLs. Data URIs carry the
// media bytes themselves, so only their media type is retained.
const maxAttachmentURLBytes = 2048

// partialKey normalizes the upstream message identity used to coalesce rolling
// snapshots. Upstream may not have announced an assistant message ID yet, in
// which case all snapshots for the attempt share one placeholder slot.
func partialKey(upstreamMessageID string) string {
	if strings.TrimSpace(upstreamMessageID) == "" {
		return "pending"
	}
	return upstreamMessageID
}

// SavePartial stores a rolling snapshot of assistant text that is still being
// produced. Snapshots are separate from finalized messages: a newer revision
// replaces the previous snapshot, and the superseded block is dropped so
// partial capture cannot grow without bound.
func (writer *ArchiveWriter) SavePartial(ctx context.Context, input opencode.PartialInput) error {
	if !utf8.ValidString(input.Text) {
		return fmt.Errorf("memstore: partial snapshot must be valid UTF-8")
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
		principal := archiveID("principal", message.Session.Adapter, message.Session.UserID)
		key := partialKey(input.ID)
		partialID := archiveID("partial", input.Attempt.ID, key)
		now := time.Now().UnixMilli()
		createdAt := input.CreatedAt
		if createdAt <= 0 {
			createdAt = now
		}
		var revision int64
		var previousBlock string
		err = tx.QueryRowContext(ctx, `SELECT revision,block_id FROM mem_v2_partials WHERE id=?`, partialID).Scan(&revision, &previousBlock)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		revision++
		blockID := archiveID("partial-block", partialID, fmt.Sprint(revision))
		checksum := sha256.Sum256([]byte(input.Text))
		if _, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_blocks
			(id,principal_id,raw_bytes,stored_bytes,checksum,payload) VALUES(?,?,?,?,?,?)`,
			blockID, principal, len(input.Text), len(input.Text), hex.EncodeToString(checksum[:]), []byte(input.Text)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_partials
			(id,principal_id,session_id,turn_id,attempt_id,upstream_message_id,revision,block_id,byte_length,created_at,updated_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(attempt_id,upstream_message_id) DO UPDATE SET
				revision=excluded.revision, block_id=excluded.block_id,
				byte_length=excluded.byte_length, updated_at=excluded.updated_at
			WHERE excluded.revision > mem_v2_partials.revision`,
			partialID, principal, message.Session.ID, message.Turn.ID, input.Attempt.ID, key,
			revision, blockID, len(input.Text), createdAt, now); err != nil {
			return err
		}
		if previousBlock != "" && previousBlock != blockID {
			if _, err := tx.ExecContext(ctx, `DELETE FROM mem_v2_blocks WHERE id=?`, previousBlock); err != nil {
				return err
			}
		}
		return nil
	})
}

// SaveToolCall records tool-call metadata. Output is retained up to
// maxToolOutputBytes and referenced through artifact_ref; media bytes are never
// stored here.
func (writer *ArchiveWriter) SaveToolCall(ctx context.Context, input opencode.ToolCallInput) error {
	switch input.State {
	case "pending", "running", "completed", "error":
	default:
		return fmt.Errorf("memstore: unsupported tool state")
	}
	if strings.TrimSpace(input.CallID) == "" || strings.TrimSpace(input.Tool) == "" {
		return fmt.Errorf("memstore: tool call identity is required")
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
		principal := archiveID("principal", message.Session.Adapter, message.Session.UserID)
		toolID := archiveID("tool", input.Attempt.ID, input.CallID)
		now := time.Now().UnixMilli()
		output, truncated := input.Output, false
		if len(output) > maxToolOutputBytes {
			output = output[:maxToolOutputBytes]
			for len(output) > 0 && !utf8.ValidString(output) {
				output = output[:len(output)-1]
			}
			truncated = true
		}
		artifactRef, retention := "", ""
		if output != "" {
			// The block is shared with any earlier call by this principal that
			// retained the same bytes, so a tool that prints the same output
			// twice costs one block rather than two. raw_bytes records the
			// untruncated length, which is why it is passed separately from the
			// payload.
			blockID, err := storeArtifactBlock(ctx, tx, principal, []byte(output), len(input.Output))
			if err != nil {
				return err
			}
			artifactRef = blockID
			retention = "retained"
			if truncated {
				retention = "truncated"
			}
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO mem_v2_tool_calls
			(id,principal_id,session_id,turn_id,attempt_id,upstream_message_id,call_id,part_id,tool_name,
			 state,title,error_text,artifact_ref,retention_reason,output_bytes,output_truncated,
			 started_at,ended_at,created_at,updated_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(attempt_id,call_id) DO UPDATE SET
				state=excluded.state, title=excluded.title, error_text=excluded.error_text,
				artifact_ref=excluded.artifact_ref, retention_reason=excluded.retention_reason,
				output_bytes=excluded.output_bytes, output_truncated=excluded.output_truncated,
				ended_at=excluded.ended_at, updated_at=excluded.updated_at`,
			toolID, principal, message.Session.ID, message.Turn.ID, input.Attempt.ID,
			input.MessageID, input.CallID, input.PartID, input.Tool, input.State,
			input.Title, input.ErrorText, artifactRef, retention, len(input.Output), truncated,
			input.StartedAt, input.EndedAt, now, now)
		return err
	})
}

// SaveAttachment records attachment metadata only. Data URIs are reduced to
// their media type so media bytes never enter the archive or its search index.
func (writer *ArchiveWriter) SaveAttachment(ctx context.Context, input opencode.AttachmentInput) error {
	if input.Role != "input" && input.Role != "output" {
		return fmt.Errorf("memstore: unsupported attachment role")
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
		principal := archiveID("principal", message.Session.Adapter, message.Session.UserID)
		url := input.URL
		if strings.HasPrefix(url, "data:") {
			if index := strings.IndexByte(url, ','); index > 0 {
				url = url[:index]
			} else {
				url = "data:"
			}
		}
		if len(url) > maxAttachmentURLBytes {
			url = url[:maxAttachmentURLBytes]
		}
		attachmentID := archiveID("attachment", input.Attempt.ID, input.Role, input.PartID, url)
		now := time.Now().UnixMilli()
		createdAt := input.CreatedAt
		if createdAt <= 0 {
			createdAt = now
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO mem_v2_attachments
			(id,principal_id,session_id,turn_id,attempt_id,upstream_message_id,part_id,role,
			 mime,filename,url,byte_length,created_at)
			VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
			ON CONFLICT(attempt_id,role,part_id,url) DO UPDATE SET
				mime=excluded.mime, filename=excluded.filename, byte_length=excluded.byte_length`,
			attachmentID, principal, message.Session.ID, message.Turn.ID, input.Attempt.ID,
			input.MessageID, input.PartID, input.Role, input.Mime, input.Filename, url,
			input.ByteLength, createdAt)
		return err
	})
}
