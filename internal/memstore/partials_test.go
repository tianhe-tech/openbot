package memstore

import (
	"context"
	"strings"
	"testing"

	"github.com/user/opencode-gateway/internal/opencode"
)

// partialFixture creates a turn and a bound attempt so partial/tool/attachment
// writes have a valid lifecycle to attach to.
func partialFixture(t *testing.T, writer *ArchiveWriter) opencode.AttemptRef {
	t.Helper()
	ctx := context.Background()
	turn, err := writer.BeginTurn(ctx, opencode.TurnInput{
		Adapter: "test", UserID: "owner", ThreadID: "thread", DeliveryID: "delivery", Content: "request", WorkDir: "/work",
	})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := writer.BindAttempt(ctx, opencode.AttemptInput{ID: "attempt_msg_partial", Turn: turn, SessionID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	return attempt
}

func TestSavePartialReplacesSnapshotAndDropsSupersededBlock(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	attempt := partialFixture(t, writer)

	for _, text := range []string{"first", "first and second", "first and second and third"} {
		if err := writer.SavePartial(ctx, opencode.PartialInput{Attempt: attempt, ID: "msg_assistant", Text: text}); err != nil {
			t.Fatal(err)
		}
	}

	var revision int64
	var blockID string
	if err := writer.store.db.QueryRow(`SELECT revision,block_id FROM mem_v2_partials WHERE attempt_id=?`, attempt.ID).Scan(&revision, &blockID); err != nil {
		t.Fatal(err)
	}
	if revision != 3 {
		t.Fatalf("revision=%d, want 3", revision)
	}
	var body []byte
	if err := writer.store.db.QueryRow(`SELECT payload FROM mem_v2_blocks WHERE id=?`, blockID).Scan(&body); err != nil {
		t.Fatal(err)
	}
	if string(body) != "first and second and third" {
		t.Fatalf("snapshot body=%q", body)
	}
	// Only the live snapshot block survives; superseded revisions are dropped.
	// Two blocks remain in total: the original user input and the live snapshot.
	var blocks int
	if err := writer.store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_blocks`).Scan(&blocks); err != nil {
		t.Fatal(err)
	}
	if blocks != 2 {
		t.Fatalf("superseded blocks retained: %d blocks", blocks)
	}
	// Partial snapshots never create finalized messages.
	var messages int
	if err := writer.store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_messages WHERE role='assistant'`).Scan(&messages); err != nil {
		t.Fatal(err)
	}
	if messages != 0 {
		t.Fatalf("partial snapshot created %d assistant messages", messages)
	}
}

func TestSavePartialCoalescesUnannouncedMessage(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	attempt := partialFixture(t, writer)

	if err := writer.SavePartial(ctx, opencode.PartialInput{Attempt: attempt, Text: "no upstream id yet"}); err != nil {
		t.Fatal(err)
	}
	var key string
	if err := writer.store.db.QueryRow(`SELECT upstream_message_id FROM mem_v2_partials WHERE attempt_id=?`, attempt.ID).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if key != "pending" {
		t.Fatalf("placeholder key=%q", key)
	}
}

func TestSavePartialRejectsInvalidUTF8(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	attempt := partialFixture(t, writer)
	err := writer.SavePartial(context.Background(), opencode.PartialInput{Attempt: attempt, ID: "msg", Text: string([]byte{0xff, 0xfe})})
	if err == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
}

func TestSaveToolCallTruncatesOutputAndRecordsRetention(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	attempt := partialFixture(t, writer)

	output := strings.Repeat("x", maxToolOutputBytes+512)
	if err := writer.SaveToolCall(ctx, opencode.ToolCallInput{
		Attempt: attempt, MessageID: "msg_assistant", CallID: "call-1", PartID: "part-1",
		Tool: "bash", State: "completed", Title: "run tests", Output: output,
		StartedAt: 1700000000000, EndedAt: 1700000001000,
	}); err != nil {
		t.Fatal(err)
	}

	var retention, artifactRef string
	var truncated bool
	var outputBytes int
	if err := writer.store.db.QueryRow(`SELECT retention_reason,artifact_ref,output_truncated,output_bytes
		FROM mem_v2_tool_calls WHERE attempt_id=? AND call_id=?`, attempt.ID, "call-1").Scan(&retention, &artifactRef, &truncated, &outputBytes); err != nil {
		t.Fatal(err)
	}
	if retention != "truncated" || !truncated {
		t.Fatalf("retention=%q truncated=%v", retention, truncated)
	}
	if outputBytes != len(output) {
		t.Fatalf("output_bytes=%d, want %d", outputBytes, len(output))
	}
	var stored []byte
	if err := writer.store.db.QueryRow(`SELECT payload FROM mem_v2_blocks WHERE id=?`, artifactRef).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if len(stored) != maxToolOutputBytes {
		t.Fatalf("stored output=%d bytes, want %d", len(stored), maxToolOutputBytes)
	}
}

func TestSaveToolCallIsIdempotentAndTracksState(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	attempt := partialFixture(t, writer)

	base := opencode.ToolCallInput{Attempt: attempt, MessageID: "msg", CallID: "call-2", Tool: "read", State: "running"}
	if err := writer.SaveToolCall(ctx, base); err != nil {
		t.Fatal(err)
	}
	base.State, base.Output, base.EndedAt = "completed", "file contents", 1700000002000
	if err := writer.SaveToolCall(ctx, base); err != nil {
		t.Fatal(err)
	}

	var count int
	var state, retention string
	if err := writer.store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_tool_calls WHERE attempt_id=?`, attempt.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("tool call rows=%d, want 1", count)
	}
	if err := writer.store.db.QueryRow(`SELECT state,retention_reason FROM mem_v2_tool_calls WHERE attempt_id=?`, attempt.ID).Scan(&state, &retention); err != nil {
		t.Fatal(err)
	}
	if state != "completed" || retention != "retained" {
		t.Fatalf("state=%q retention=%q", state, retention)
	}
}

func TestSaveToolCallRejectsUnknownState(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	attempt := partialFixture(t, writer)
	err := writer.SaveToolCall(context.Background(), opencode.ToolCallInput{Attempt: attempt, CallID: "call", Tool: "bash", State: "exploded"})
	if err == nil {
		t.Fatal("unknown tool state accepted")
	}
}

func TestSaveAttachmentReducesDataURI(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	attempt := partialFixture(t, writer)

	if err := writer.SaveAttachment(ctx, opencode.AttachmentInput{
		Attempt: attempt, PartID: "input-0", Role: "input",
		Mime: "image/png", Filename: "shot.png",
		URL: "data:image/png;base64," + strings.Repeat("A", 4096),
	}); err != nil {
		t.Fatal(err)
	}

	var url string
	if err := writer.store.db.QueryRow(`SELECT url FROM mem_v2_attachments WHERE attempt_id=?`, attempt.ID).Scan(&url); err != nil {
		t.Fatal(err)
	}
	if url != "data:image/png;base64" {
		t.Fatalf("data URI not reduced: %q", url)
	}
	if strings.Contains(url, "AAAA") {
		t.Fatal("media bytes leaked into archive")
	}
}

func TestSaveAttachmentIsIdempotent(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	attempt := partialFixture(t, writer)

	input := opencode.AttachmentInput{
		Attempt: attempt, MessageID: "msg", PartID: "part", Role: "output",
		Mime: "image/jpeg", Filename: "out.jpg", URL: "https://example.test/out.jpg",
	}
	for index := 0; index < 2; index++ {
		if err := writer.SaveAttachment(ctx, input); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := writer.store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_attachments WHERE attempt_id=?`, attempt.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("attachment rows=%d, want 1", count)
	}
}

func TestSaveAttachmentRejectsUnknownRole(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	attempt := partialFixture(t, writer)
	err := writer.SaveAttachment(context.Background(), opencode.AttachmentInput{Attempt: attempt, PartID: "p", Role: "sideways", URL: "https://example.test/x"})
	if err == nil {
		t.Fatal("unknown attachment role accepted")
	}
}
