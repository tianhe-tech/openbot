package memstore

import (
	"context"
	"strings"
	"testing"

	"github.com/user/opencode-gateway/internal/opencode"
)

// TestToolOutputIsDeduplicatedWithinAPrincipal proves two tool calls that
// retained the same bytes share one block.
//
// This is the space saving the dedupe exists for: a build tool that prints the
// same warning on every invocation would otherwise store that warning once per
// call, and the archive would grow with the number of times a command ran
// rather than with the amount of distinct information it produced.
func TestToolOutputIsDeduplicatedWithinAPrincipal(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	attempt := partialFixture(t, writer)

	// Retained whole rather than truncated, so the assertion below compares the
	// block against the text that was actually kept. A larger fixture would be
	// truncated at maxToolOutputBytes and the comparison would be against text
	// that was deliberately dropped.
	output := strings.Repeat("warning: unused variable\n", 300)
	if len(output) > maxToolOutputBytes {
		t.Fatalf("fixture is %d bytes, above the %d-byte retention limit this test needs",
			len(output), maxToolOutputBytes)
	}
	for _, callID := range []string{"call-1", "call-2"} {
		if err := writer.SaveToolCall(ctx, opencode.ToolCallInput{
			Attempt: attempt, MessageID: "msg_assistant", CallID: callID, PartID: "part-" + callID,
			Tool: "bash", State: "completed", Title: "build", Output: output,
			StartedAt: 1700000000000, EndedAt: 1700000001000,
		}); err != nil {
			t.Fatal(err)
		}
	}

	refs := toolArtifactRefs(t, writer, attempt.ID)
	if len(refs) != 2 {
		t.Fatalf("got %d tool calls, want 2", len(refs))
	}
	if refs[0] != refs[1] {
		t.Fatalf("identical outputs got different blocks: %q vs %q", refs[0], refs[1])
	}

	// One block, not two. Counting the blocks is the assertion that the saving
	// actually happened; comparing the refs alone would pass on an archive that
	// wrote two identical rows and pointed both calls at one of them.
	var blocks int
	if err := writer.store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM mem_v2_blocks WHERE principal_id=?`,
		archiveID("principal", "test", "owner")).Scan(&blocks); err != nil {
		t.Fatal(err)
	}
	// The fixture's partial snapshot is also a block, so the count is the
	// snapshot plus exactly one shared tool output.
	if blocks != 2 {
		t.Fatalf("got %d blocks, want 2 (one partial snapshot and one shared tool output)", blocks)
	}

	// The shared block must still hold the right bytes. A dedupe that pointed
	// both calls at a block containing something else would be worse than no
	// dedupe at all.
	var stored []byte
	if err := writer.store.db.QueryRowContext(ctx,
		`SELECT payload FROM mem_v2_blocks WHERE id=?`, refs[0]).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if string(stored) != output {
		t.Fatalf("shared block holds %d bytes, want the %d-byte output", len(stored), len(output))
	}
}

// TestToolOutputIsNotSharedAcrossPrincipals proves the dedupe identity is
// scoped, so one principal's write cannot be deduplicated against another's.
//
// A global content index would turn the dedupe into a hash-existence oracle:
// principal B could learn that principal A has stored a particular payload by
// observing that its own write was shared. The assertion is that the two
// principals get different blocks even though the bytes are identical.
func TestToolOutputIsNotSharedAcrossPrincipals(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()

	output := strings.Repeat("shared looking output\n", 1024)
	refs := make(map[string]string)
	for _, spec := range []struct{ session, user, call string }{
		{"session-a", "owner", "call-a"},
		{"session-b", "someone-else", "call-b"},
	} {
		attempt := attemptForPrincipal(t, writer, spec.session, spec.user)
		if err := writer.SaveToolCall(ctx, opencode.ToolCallInput{
			Attempt: attempt, MessageID: "msg_assistant", CallID: spec.call, PartID: "part-" + spec.call,
			Tool: "bash", State: "completed", Title: "build", Output: output,
			StartedAt: 1700000000000, EndedAt: 1700000001000,
		}); err != nil {
			t.Fatal(err)
		}
		refs[spec.user] = toolArtifactRefs(t, writer, attempt.ID)[0]
	}

	if refs["owner"] == refs["someone-else"] {
		t.Fatalf("two principals share block %q; the identity is not scoped", refs["owner"])
	}
	// Both blocks must exist, so the difference is real storage rather than one
	// principal's row having failed to write.
	var blocks int
	if err := writer.store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM mem_v2_blocks WHERE id IN (?,?)`,
		refs["owner"], refs["someone-else"]).Scan(&blocks); err != nil {
		t.Fatal(err)
	}
	if blocks != 2 {
		t.Fatalf("got %d blocks for two principals, want 2", blocks)
	}
}

// TestToolOutputWithDifferentOriginalLengthsIsNotShared proves the untruncated
// length is part of the identity.
//
// Two calls can retain the same truncated payload while their originals were
// different lengths. Sharing one block would give the second call the first
// call's raw_bytes, so a reader asking "how much output was dropped" would get
// a number that describes a different command's output.
func TestToolOutputWithDifferentOriginalLengthsIsNotShared(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	attempt := partialFixture(t, writer)

	// Both outputs truncate to the same retained prefix but differ in length.
	base := strings.Repeat("y", maxToolOutputBytes)
	for i, output := range []string{base + strings.Repeat("a", 100), base + strings.Repeat("b", 5000)} {
		if err := writer.SaveToolCall(ctx, opencode.ToolCallInput{
			Attempt: attempt, MessageID: "msg_assistant", CallID: "call-" + string(rune('1'+i)),
			PartID: "part", Tool: "bash", State: "completed", Title: "build", Output: output,
			StartedAt: 1700000000000, EndedAt: 1700000001000,
		}); err != nil {
			t.Fatal(err)
		}
	}

	refs := toolArtifactRefs(t, writer, attempt.ID)
	if len(refs) != 2 {
		t.Fatalf("got %d tool calls, want 2", len(refs))
	}
	if refs[0] == refs[1] {
		t.Fatalf("outputs with different original lengths share block %q", refs[0])
	}

	// Each block must record its own original length, which is the number the
	// sharing would have corrupted.
	var first, second int
	if err := writer.store.db.QueryRowContext(ctx,
		`SELECT raw_bytes FROM mem_v2_blocks WHERE id=?`, refs[0]).Scan(&first); err != nil {
		t.Fatal(err)
	}
	if err := writer.store.db.QueryRowContext(ctx,
		`SELECT raw_bytes FROM mem_v2_blocks WHERE id=?`, refs[1]).Scan(&second); err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatalf("both blocks record raw_bytes=%d, want the two different original lengths", first)
	}
}

// TestSmallToolOutputIsStoredOnce proves a repeated small output still occupies
// one row.
//
// The size threshold decides whether a payload is worth a content comparison,
// not whether it is worth sharing. A small output repeated verbatim has the
// same derived ID, so the conflict clause collapses it without any extra work.
func TestSmallToolOutputIsStoredOnce(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	attempt := partialFixture(t, writer)

	output := "ok\n"
	for _, callID := range []string{"call-1", "call-2"} {
		if err := writer.SaveToolCall(ctx, opencode.ToolCallInput{
			Attempt: attempt, MessageID: "msg_assistant", CallID: callID, PartID: "part-" + callID,
			Tool: "bash", State: "completed", Title: "build", Output: output,
			StartedAt: 1700000000000, EndedAt: 1700000001000,
		}); err != nil {
			t.Fatal(err)
		}
	}

	refs := toolArtifactRefs(t, writer, attempt.ID)
	if len(refs) != 2 || refs[0] != refs[1] {
		t.Fatalf("small repeated output was not collapsed: %v", refs)
	}
}

// toolArtifactRefs returns the artifact_ref of every tool call for an attempt,
// ordered by call ID so the result is stable.
func toolArtifactRefs(t *testing.T, writer *ArchiveWriter, attemptID string) []string {
	t.Helper()
	rows, err := writer.store.db.Query(`SELECT artifact_ref FROM mem_v2_tool_calls
		WHERE attempt_id=? ORDER BY call_id`, attemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var refs []string
	for rows.Next() {
		var ref string
		if err := rows.Scan(&ref); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return refs
}

// attemptForPrincipal records a turn for one principal and returns its attempt,
// so a test can write tool calls for two principals in one archive.
func attemptForPrincipal(t *testing.T, writer *ArchiveWriter, sessionID, userID string) opencode.AttemptRef {
	t.Helper()
	ctx := context.Background()
	turn, err := writer.BeginTurn(ctx, opencode.TurnInput{
		Adapter: "test", UserID: userID, ThreadID: "thread-" + sessionID,
		DeliveryID: "delivery-" + sessionID, Content: "request", WorkDir: "/work/shared",
	})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := writer.BindAttempt(ctx, opencode.AttemptInput{
		ID: "attempt-" + sessionID, Turn: turn, SessionID: sessionID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return attempt
}
