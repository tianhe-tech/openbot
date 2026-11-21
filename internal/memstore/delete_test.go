package memstore

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/user/opencode-gateway/internal/opencode"
)

func deleteCountForTest(t *testing.T, writer *ArchiveWriter, query string, args ...any) int64 {
	t.Helper()
	var count int64
	if err := writer.store.db.QueryRow(query, args...).Scan(&count); err != nil {
		t.Fatalf("count %q: %v", query, err)
	}
	return count
}

// sessionIDForAttempt resolves the archive's own session ID for an attempt.
//
// The ID a caller passes to BindAttempt is the upstream session ID, which is
// stored on the attempt. The archive derives its own session ID from the
// principal, thread, and work directory, so a test that filters on the value it
// passed in matches nothing and then passes for the wrong reason. Every
// session-scoped assertion below goes through this helper for that reason.
func sessionIDForAttempt(t *testing.T, writer *ArchiveWriter, attemptID string) string {
	t.Helper()
	var sessionID string
	if err := writer.store.db.QueryRow(`SELECT t.session_id FROM mem_v2_attempts a
		JOIN mem_v2_turns t ON t.id=a.turn_id WHERE a.id=?`, attemptID).Scan(&sessionID); err != nil {
		t.Fatalf("resolve session for attempt %s: %v", attemptID, err)
	}
	return sessionID
}

// TestDeleteScopeRequiresAScope is the gate that makes every other deletion
// assertion meaningful.
//
// An unscoped deletion is not a narrower version of a scoped one, it is the
// whole archive. The three variants below are the ways a caller can arrive at
// one by accident: no scope at all, an adapter with no user, and a user with no
// adapter. All three must be refused before any statement runs.
func TestDeleteScopeRequiresAScope(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	for _, scope := range []SearchScope{
		{},
		{Adapter: "test"},
		{UserID: "owner"},
		{Adapter: "   ", UserID: "owner"},
	} {
		if _, err := writer.DeleteScope(ctx, DeleteOptions{Scope: scope}); !errors.Is(err, ErrSearchScopeRequired) {
			t.Fatalf("scope %+v was accepted: %v", scope, err)
		}
	}
	// Nothing may have been removed by the rejected calls.
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_messages`); count != 0 {
		t.Fatalf("rejected deletions removed %d messages", count)
	}
}

// TestDeleteScopeRemovesOnlyTheBoundPrincipal is the isolation assertion.
//
// The fixture gives both principals the same shape and the same searchable
// term, so a deletion that forgot its principal predicate would remove both and
// the surviving-content assertions below would fail rather than pass by finding
// nothing. The second principal's rows are then checked one table at a time,
// because "the other user's messages survived" is not the same claim as "the
// other user's chunks and index entries survived" — a deletion that removed the
// messages but left the chunks would still be a leak, since the chunks carry
// the text.
func TestDeleteScopeRemovesOnlyTheBoundPrincipal(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	for _, spec := range []turnSpec{
		{Delivery: "own", Project: "gateway", WorkDir: "/work/gateway",
			Request: "restartprobe the gateway", Answer: "restartprobe done", Created: 1700000000000},
		{Delivery: "other", Project: "crawler", WorkDir: "/work/crawler",
			Request: "restartprobe the crawler", Answer: "restartprobe done", Created: 1700000100000,
			UserID: "someone-else"},
	} {
		recordTurn(t, writer, spec)
	}
	indexAll(t, writer)

	owner := SearchScope{Adapter: "test", UserID: "owner"}
	other := SearchScope{Adapter: "test", UserID: "someone-else"}
	before, err := writer.store.Search(ctx, SearchOptions{Scope: other, Query: "restartprobe"})
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Hits) == 0 {
		t.Fatal("fixture is not searchable for the second principal; the test would pass vacuously")
	}

	report, err := writer.DeleteScope(ctx, DeleteOptions{Scope: owner})
	if err != nil {
		t.Fatal(err)
	}
	if report.Messages != 2 {
		t.Fatalf("deleted %d messages, want the owner's 2", report.Messages)
	}
	if report.Empty() {
		t.Fatal("report claims nothing was deleted")
	}

	ownerPrincipal := archiveID("principal", "test", "owner")
	otherPrincipal := archiveID("principal", "test", "someone-else")
	// mem_v2_chunks has no principal column of its own; it is scoped through the
	// message it belongs to. The query is written out rather than derived so the
	// join is visible: a chunk whose message is gone is exactly the leak this
	// test is looking for, and a join would hide it.
	for _, table := range []string{"mem_v2_messages", "mem_v2_blocks", "mem_v2_turns", "mem_v2_sessions"} {
		if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM `+table+` WHERE principal_id=?`, ownerPrincipal); count != 0 {
			t.Fatalf("%s still holds %d rows for the deleted principal", table, count)
		}
		if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM `+table+` WHERE principal_id=?`, otherPrincipal); count == 0 {
			t.Fatalf("%s lost every row of the surviving principal", table)
		}
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_chunks c
		JOIN mem_v2_messages m ON m.id=c.message_id WHERE m.principal_id=?`, ownerPrincipal); count != 0 {
		t.Fatalf("%d chunks still belong to the deleted principal", count)
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_chunks c
		JOIN mem_v2_messages m ON m.id=c.message_id WHERE m.principal_id=?`, otherPrincipal); count == 0 {
		t.Fatal("the surviving principal lost every chunk")
	}

	// The surviving principal must still be searchable, which is the assertion
	// that the FTS rows were not collateral damage. A deletion that removed the
	// other principal's index entries would leave its messages readable by ID
	// but invisible to search, and that is a silent data loss.
	after, err := writer.store.Search(ctx, SearchOptions{Scope: other, Query: "restartprobe"})
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Hits) != len(before.Hits) {
		t.Fatalf("surviving principal went from %d hits to %d", len(before.Hits), len(after.Hits))
	}
	// And the deleted principal must find nothing.
	gone, err := writer.store.Search(ctx, SearchOptions{Scope: owner, Query: "restartprobe"})
	if err != nil {
		t.Fatal(err)
	}
	if len(gone.Hits) != 0 {
		t.Fatalf("deleted principal still has %d search hits", len(gone.Hits))
	}
}

// TestDeleteScopeRemovesChunksAndSearchEntries checks the index side of the
// deletion directly rather than through Search.
//
// Search could return nothing for a deleted message for the wrong reason — a
// stale cursor, a scope mismatch, a query that never matched — so the chunk and
// FTS row counts are asserted here. The FTS counts are read with a plain
// COUNT(*) on the virtual table, which is the only way to see rows that no
// longer have a chunk to point at.
func TestDeleteScopeRemovesChunksAndSearchEntries(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	recordTurn(t, writer, turnSpec{
		Delivery: "chunked", Project: "gateway", WorkDir: "/work/gateway",
		Request: "chunkprobe request", Answer: strings.Repeat("chunkprobe answer text. ", 200),
		Created: 1700000000000,
	})
	indexAll(t, writer)

	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_chunks`); count == 0 {
		t.Fatal("fixture produced no chunks; the test would pass vacuously")
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_chunk_fts`); count == 0 {
		t.Fatal("fixture produced no trigram index rows")
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_chunk_word_fts`); count == 0 {
		t.Fatal("fixture produced no word index rows")
	}

	report, err := writer.DeleteScope(ctx, DeleteOptions{Scope: SearchScope{Adapter: "test", UserID: "owner"}})
	if err != nil {
		t.Fatal(err)
	}
	if report.Chunks == 0 {
		t.Fatal("report claims no chunks were deleted")
	}
	for _, table := range []string{"mem_v2_chunks", "mem_v2_chunk_fts", "mem_v2_chunk_word_fts"} {
		if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM `+table); count != 0 {
			t.Fatalf("%s still holds %d rows after deleting every message", table, count)
		}
	}
	// The index jobs for the deleted messages must be gone too. A surviving job
	// would be picked up by the index worker, which would then fail on a missing
	// message and record a permanent failure that no operator can act on.
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_jobs WHERE kind='index'`); count != 0 {
		t.Fatalf("%d index jobs survived the deletion", count)
	}
}

// TestDeleteScopeKeepsABlockAnotherRecordStillUses is the shared-block
// assertion, and it is the reason the block sweep is by reference rather than
// by ownership.
//
// Two tool calls by the same principal that retained identical output share one
// block. Deleting the session that owns one of them must not remove the block,
// because the other call still points at it and its text would become
// unreadable. A sweep written as "delete the blocks of the deleted messages"
// passes every other test in this file and fails this one.
func TestDeleteScopeKeepsABlockAnotherRecordStillUses(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	output := strings.Repeat("shared build warning\n", 200)
	refs := make(map[string]string)
	sessions := make(map[string]string)
	for _, session := range []string{"session-a", "session-b"} {
		attempt := attemptForPrincipal(t, writer, session, "owner")
		if err := writer.SaveToolCall(ctx, opencode.ToolCallInput{
			Attempt: attempt, MessageID: "msg_assistant", CallID: "call-" + session,
			PartID: "part", Tool: "bash", State: "completed", Title: "build", Output: output,
			StartedAt: 1700000000000, EndedAt: 1700000001000,
		}); err != nil {
			t.Fatal(err)
		}
		refs[session] = toolArtifactRefs(t, writer, attempt.ID)[0]
		sessions[session] = sessionIDForAttempt(t, writer, attempt.ID)
	}
	if refs["session-a"] == "" || refs["session-a"] != refs["session-b"] {
		t.Fatalf("fixture did not share one block: %q vs %q", refs["session-a"], refs["session-b"])
	}
	if sessions["session-a"] == sessions["session-b"] {
		t.Fatalf("fixture put both attempts in one archive session %q; the deletion would not be session-scoped", sessions["session-a"])
	}

	if _, err := writer.DeleteScope(ctx, DeleteOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"}, SessionID: sessions["session-a"],
	}); err != nil {
		t.Fatal(err)
	}

	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_blocks WHERE id=?`, refs["session-a"]); count != 1 {
		t.Fatalf("the shared block was removed while another call still referenced it")
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_tool_calls WHERE session_id=?`, sessions["session-b"]); count != 1 {
		t.Fatalf("the surviving session lost its tool call")
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_tool_calls WHERE session_id=?`, sessions["session-a"]); count != 0 {
		t.Fatalf("the deleted session kept its tool call")
	}
	// The surviving call must still resolve to readable text, not just to a
	// block ID that happens to exist.
	text := blockTextForTest(t, writer.store.db,
		`SELECT codec,codec_version,raw_bytes,payload FROM mem_v2_blocks WHERE id=?`, refs["session-b"])
	if text != output {
		t.Fatalf("surviving block holds %d bytes, want the %d-byte output", len(text), len(output))
	}
}

// TestDeleteScopeIsIdempotent proves a repeated deletion is a no-op rather than
// an error or a second round of removals.
//
// This matters because deletion is the one operation a caller is likely to
// retry: a timeout leaves the caller unsure whether the transaction committed,
// and the safe retry has to be harmless. The second call must report nothing
// removed, which is also what makes the report usable as a confirmation.
func TestDeleteScopeIsIdempotent(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	recordTurn(t, writer, turnSpec{
		Delivery: "once", Project: "gateway", WorkDir: "/work/gateway",
		Request: "idempotentprobe", Answer: "idempotentprobe answer", Created: 1700000000000,
	})
	indexAll(t, writer)
	scope := SearchScope{Adapter: "test", UserID: "owner"}

	first, err := writer.DeleteScope(ctx, DeleteOptions{Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	if first.Messages != 2 {
		t.Fatalf("first deletion removed %d messages, want 2", first.Messages)
	}
	second, err := writer.DeleteScope(ctx, DeleteOptions{Scope: scope})
	if err != nil {
		t.Fatalf("repeated deletion failed: %v", err)
	}
	if !second.Empty() {
		t.Fatalf("repeated deletion reported work: %+v", second)
	}
}

// TestDeleteScopeInvalidatesSearchCursors proves a cursor issued before a
// deletion is refused afterwards.
//
// A cursor is an offset into a ranking. Deleting rows renumbers that ranking,
// so continuing from a stale offset silently skips or repeats results — the
// caller gets a page that looks fine and is wrong. The index generation is what
// makes the refusal possible, and this test is what keeps the deletion from
// forgetting to bump it.
func TestDeleteScopeInvalidatesSearchCursors(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	for i := 0; i < 6; i++ {
		recordTurn(t, writer, turnSpec{
			Delivery: "cursor-" + string(rune('a'+i)), Project: "gateway", WorkDir: "/work/gateway",
			Request: "cursorprobe request", Answer: "cursorprobe answer " + string(rune('a'+i)),
			Created: 1700000000000 + int64(i)*1000,
		})
	}
	indexAll(t, writer)
	scope := SearchScope{Adapter: "test", UserID: "owner"}
	first, err := writer.store.Search(ctx, SearchOptions{Scope: scope, Query: "cursorprobe", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if first.NextCursor == "" {
		t.Fatal("fixture did not produce a cursor; the test would pass vacuously")
	}
	if _, err := writer.DeleteScope(ctx, DeleteOptions{Scope: scope}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.store.Search(ctx, SearchOptions{
		Scope: scope, Query: "cursorprobe", Limit: 2, Cursor: first.NextCursor,
	}); !errors.Is(err, ErrSearchRestartRequired) {
		t.Fatalf("a cursor issued before the deletion was accepted: %v", err)
	}
}

// TestDeleteScopeBySessionLeavesOtherSessions proves the session filter narrows
// the deletion instead of being ignored.
//
// A filter that is accepted and then dropped is the worst kind of bug in a
// deletion path: the caller asked to remove one conversation and the archive
// lost all of them. The fixture gives both sessions the same principal and the
// same searchable term so only the session predicate can tell them apart.
func TestDeleteScopeBySessionLeavesOtherSessions(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	sessions := make(map[string]string)
	for _, session := range []string{"keep-me", "drop-me"} {
		attempt := attemptForPrincipal(t, writer, session, "owner")
		if err := writer.SaveMessage(ctx, opencode.MessageInput{
			Attempt: attempt, ID: "msg_" + session, Text: "sessionprobe answer for " + session,
			State: "completed", CreatedAt: 1700000000000,
		}); err != nil {
			t.Fatal(err)
		}
		sessions[session] = sessionIDForAttempt(t, writer, attempt.ID)
	}
	indexAll(t, writer)
	scope := SearchScope{Adapter: "test", UserID: "owner"}
	if sessions["keep-me"] == sessions["drop-me"] {
		t.Fatalf("fixture put both attempts in one archive session %q", sessions["keep-me"])
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_messages WHERE session_id=?`, sessions["drop-me"]); count == 0 {
		t.Fatal("fixture did not write the session to delete")
	}

	report, err := writer.DeleteScope(ctx, DeleteOptions{Scope: scope, SessionID: sessions["drop-me"]})
	if err != nil {
		t.Fatal(err)
	}
	if report.Messages == 0 {
		t.Fatal("session-scoped deletion removed nothing")
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_messages WHERE session_id=?`, sessions["drop-me"]); count != 0 {
		t.Fatalf("the named session kept %d messages", count)
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_messages WHERE session_id=?`, sessions["keep-me"]); count == 0 {
		t.Fatal("the unnamed session lost its messages")
	}
	// The surviving session must still be findable, so the deletion did not
	// strip its index entries along with the other session's.
	result, err := writer.store.Search(ctx, SearchOptions{Scope: scope, Query: "sessionprobe"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) == 0 {
		t.Fatal("the surviving session is no longer searchable")
	}
	for _, hit := range result.Hits {
		if hit.SessionID == sessions["drop-me"] {
			t.Fatalf("deleted session %q still appears in search results", hit.SessionID)
		}
	}
}

// TestDeleteScopeSweepsBlocksOrphanedByAnEarlierRun proves the block sweep is
// resumable.
//
// A crash between deleting a message and deleting its block leaves an orphan.
// The next deletion has to collect it, or the archive accumulates unreachable
// bytes that no later operation will ever look at. The orphan is inserted
// directly because that is the only way to reproduce the state a crash leaves:
// no code path creates one deliberately.
func TestDeleteScopeSweepsBlocksOrphanedByAnEarlierRun(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	recordTurn(t, writer, turnSpec{
		Delivery: "sweep", Project: "gateway", WorkDir: "/work/gateway",
		Request: "sweepprobe", Answer: "sweepprobe answer", Created: 1700000000000,
	})
	indexAll(t, writer)
	principal := archiveID("principal", "test", "owner")
	orphan := archiveID("orphan-block", "fixture")
	if _, err := writer.store.db.ExecContext(ctx, `INSERT INTO mem_v2_blocks
		(id,principal_id,raw_bytes,stored_bytes,checksum,payload) VALUES(?,?,?,?,?,?)`,
		orphan, principal, 4, 4, "deadbeef", []byte("lost")); err != nil {
		t.Fatal(err)
	}

	report, err := writer.DeleteScope(ctx, DeleteOptions{Scope: SearchScope{Adapter: "test", UserID: "owner"}})
	if err != nil {
		t.Fatal(err)
	}
	if report.Blocks == 0 {
		t.Fatal("the sweep removed no blocks")
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_blocks WHERE id=?`, orphan); count != 0 {
		t.Fatal("the orphaned block survived the sweep")
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_blocks`); count != 0 {
		t.Fatalf("%d blocks remain after deleting every message", count)
	}
}
