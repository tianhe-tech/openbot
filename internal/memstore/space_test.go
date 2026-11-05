package memstore

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/user/opencode-gateway/internal/opencode"
)

// TestSpaceReportAccountsForTheFileAndThePrincipal proves the report measures
// the real file and attributes bytes to the right principal.
//
// The two halves are asserted separately on purpose. A report that only checked
// "FileBytes > 0" would pass on a database that contains nothing, and a report
// that only checked the principal's block bytes would pass while the file-level
// numbers were fabricated.
func TestSpaceReportAccountsForTheFileAndThePrincipal(t *testing.T) {
	path, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()

	// A large compressible body, so the block is stored zstd and the stored
	// size differs from the raw size. If the report summed raw_bytes instead of
	// stored_bytes it would overstate the archive, and this fixture is what
	// makes that difference visible.
	message := archiveMessageForTest("space-1")
	message.Text = compressibleText(64 * 1024)
	if err := writer.RecordMessage(ctx, message); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	scope := SearchScope{Adapter: "test", UserID: "owner"}
	report, err := writer.store.SpaceReport(ctx, scope, "")
	if err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if report.FileBytes != info.Size() {
		t.Fatalf("FileBytes = %d, want the file size %d", report.FileBytes, info.Size())
	}
	if report.FileBytes <= 0 {
		t.Fatal("FileBytes must be positive for a database that exists")
	}
	if report.BlockBytes <= 0 {
		t.Fatal("BlockBytes must be positive after recording a message")
	}
	if report.BlockBytes >= int64(len(message.Text)) {
		t.Fatalf("BlockBytes = %d, want less than the %d raw bytes because the block is compressed",
			report.BlockBytes, len(message.Text))
	}
	if report.BackupBytes != 0 {
		t.Fatalf("BackupBytes = %d, want 0 when no backup path is given", report.BackupBytes)
	}
	if report.EmbeddingBytes != 0 {
		t.Fatalf("EmbeddingBytes = %d, want 0 because no embeddings are stored", report.EmbeddingBytes)
	}
}

// TestSpaceReportIsScopedToThePrincipal proves one principal's report does not
// include another principal's blocks.
//
// This is the same class of defect the PR 5 gate exists for: a storage report
// that summed every principal's rows would leak the existence and size of
// another user's archive through a diagnostic that looks harmless.
func TestSpaceReportIsScopedToThePrincipal(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()

	owner := archiveMessageForTest("owner-1")
	owner.Text = compressibleText(32 * 1024)
	if err := writer.RecordMessage(ctx, owner); err != nil {
		t.Fatal(err)
	}

	// The second principal needs its own session. Reusing the session ID with a
	// different owner is rejected as identity reuse, which is the correct
	// behaviour and would otherwise make this test fail for a reason unrelated
	// to what it measures.
	other := archiveMessageForTest("other-1")
	other.Session.ID = "session-other"
	other.Session.UserID = "someone-else"
	other.Text = compressibleText(32 * 1024)
	if err := writer.RecordMessage(ctx, other); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	ownerReport, err := writer.store.SpaceReport(ctx, SearchScope{Adapter: "test", UserID: "owner"}, "")
	if err != nil {
		t.Fatal(err)
	}
	otherReport, err := writer.store.SpaceReport(ctx, SearchScope{Adapter: "test", UserID: "someone-else"}, "")
	if err != nil {
		t.Fatal(err)
	}

	if ownerReport.BlockBytes <= 0 || otherReport.BlockBytes <= 0 {
		t.Fatalf("both principals must report their own blocks: owner=%d other=%d",
			ownerReport.BlockBytes, otherReport.BlockBytes)
	}
	// The exact assertion: the two scoped reports must partition the archive's
	// blocks. If either report included the other principal's rows, the sum
	// would exceed the unscoped total. Comparing against a measured total is
	// stronger than comparing the two reports to each other, because two
	// reports that both leaked everything would still look equal.
	var total int64
	if err := writer.store.db.QueryRowContext(ctx,
		`SELECT COALESCE(SUM(stored_bytes),0) FROM mem_v2_blocks`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if ownerReport.BlockBytes+otherReport.BlockBytes != total {
		t.Fatalf("scoped reports sum to %d, want the archive total %d: owner=%d other=%d",
			ownerReport.BlockBytes+otherReport.BlockBytes, total,
			ownerReport.BlockBytes, otherReport.BlockBytes)
	}
	// The file-level numbers are shared by design, so they must be identical.
	if ownerReport.FileBytes != otherReport.FileBytes {
		t.Fatalf("FileBytes must be the same for both principals: %d vs %d",
			ownerReport.FileBytes, otherReport.FileBytes)
	}
}

// TestSpaceReportRequiresScope proves an unscoped report is refused rather than
// silently returning every principal's bytes.
func TestSpaceReportRequiresScope(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	if _, err := writer.store.SpaceReport(context.Background(), SearchScope{}, ""); err != ErrSearchScopeRequired {
		t.Fatalf("err = %v, want ErrSearchScopeRequired", err)
	}
}

// TestSpaceReportMeasuresBackupSeparately proves a backup file is reported in
// its own field and not folded into the live archive's size.
func TestSpaceReportMeasuresBackupSeparately(t *testing.T) {
	path, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()

	message := archiveMessageForTest("backup-1")
	message.Text = compressibleText(16 * 1024)
	if err := writer.RecordMessage(ctx, message); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	backupPath := filepath.Join(t.TempDir(), "backup.db")
	if _, err := writer.store.db.ExecContext(ctx, `VACUUM INTO ?`, backupPath); err != nil {
		t.Fatal(err)
	}

	scope := SearchScope{Adapter: "test", UserID: "owner"}
	withoutBackup, err := writer.store.SpaceReport(ctx, scope, "")
	if err != nil {
		t.Fatal(err)
	}
	withBackup, err := writer.store.SpaceReport(ctx, scope, backupPath)
	if err != nil {
		t.Fatal(err)
	}

	if withBackup.BackupBytes <= 0 {
		t.Fatal("BackupBytes must be positive for an existing backup file")
	}
	if withBackup.FileBytes != withoutBackup.FileBytes {
		t.Fatalf("naming a backup must not change FileBytes: %d vs %d",
			withBackup.FileBytes, withoutBackup.FileBytes)
	}
	// The backup is a copy of the same database, so it should be close to the
	// live file. This is a sanity check that the path was measured at all,
	// not an assertion about VACUUM's exact output size.
	if withBackup.BackupBytes > withBackup.FileBytes*2 {
		t.Fatalf("BackupBytes = %d is implausibly larger than FileBytes = %d",
			withBackup.BackupBytes, withBackup.FileBytes)
	}
	_ = path
}

// TestSpaceReportCountsFTSBytes proves the full-text index cost is measured
// across all of its shadow tables rather than only the table named in the
// CREATE statement.
func TestSpaceReportCountsFTSBytes(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()

	message := archiveMessageForTest("fts-1")
	message.Text = compressibleText(32 * 1024)
	if err := writer.RecordMessage(ctx, message); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.IndexPending(ctx, maxIndexBatch); err != nil {
		t.Fatal(err)
	}

	report, err := writer.store.SpaceReport(ctx, SearchScope{Adapter: "test", UserID: "owner"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if !report.TableBreakdownAvailable {
		t.Skip("this SQLite build has no dbstat; FTSBytes cannot be measured")
	}
	if report.FTSBytes <= 0 {
		t.Fatal("FTSBytes must be positive after indexing a message")
	}
	if report.FTSBytes > report.FileBytes {
		t.Fatalf("FTSBytes = %d cannot exceed FileBytes = %d", report.FTSBytes, report.FileBytes)
	}
}

// TestSpaceReportCountsArtifactsOnce proves a retained tool output referenced by
// two tool calls is counted once.
//
// The deduplication is the point. A tool output is stored as a block and
// referenced by artifact_ref, and the same block can be referenced more than
// once. Summing the join without DISTINCT would count the block per reference
// and report an archive larger than the disk holds.
func TestSpaceReportCountsArtifactsOnce(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	attempt := partialFixture(t, writer)

	scope := SearchScope{Adapter: "test", UserID: "owner"}
	before, err := writer.store.SpaceReport(ctx, scope, "")
	if err != nil {
		t.Fatal(err)
	}
	if before.ArtifactBytes != 0 {
		t.Fatalf("ArtifactBytes = %d before any tool call, want 0", before.ArtifactBytes)
	}

	output := strings.Repeat("tool output line\n", 4096)
	if err := writer.SaveToolCall(ctx, opencode.ToolCallInput{
		Attempt: attempt, MessageID: "msg_assistant", CallID: "call-1", PartID: "part-1",
		Tool: "bash", State: "completed", Title: "run tests", Output: output,
		StartedAt: 1700000000000, EndedAt: 1700000001000,
	}); err != nil {
		t.Fatal(err)
	}

	after, err := writer.store.SpaceReport(ctx, scope, "")
	if err != nil {
		t.Fatal(err)
	}
	if after.ArtifactBytes <= 0 {
		t.Fatal("ArtifactBytes must be positive after a tool call retains output")
	}
	if after.ArtifactBytes > after.BlockBytes {
		t.Fatalf("ArtifactBytes = %d cannot exceed BlockBytes = %d: artifacts are blocks",
			after.ArtifactBytes, after.BlockBytes)
	}

	// Point a second tool call at the same block and confirm the report does not
	// grow. This is the assertion the DISTINCT exists for.
	if _, err := writer.store.db.ExecContext(ctx,
		`INSERT INTO mem_v2_tool_calls
			(id,principal_id,session_id,turn_id,attempt_id,call_id,tool_name,state,
			 artifact_ref,created_at,updated_at)
			SELECT 'tool-dup',principal_id,session_id,turn_id,attempt_id,'call-2',tool_name,state,
			 artifact_ref,created_at,updated_at
			FROM mem_v2_tool_calls WHERE attempt_id=? AND call_id='call-1'`,
		attempt.ID); err != nil {
		t.Fatal(err)
	}
	duplicated, err := writer.store.SpaceReport(ctx, scope, "")
	if err != nil {
		t.Fatal(err)
	}
	if duplicated.ArtifactBytes != after.ArtifactBytes {
		t.Fatalf("ArtifactBytes changed from %d to %d after a second reference to the same block",
			after.ArtifactBytes, duplicated.ArtifactBytes)
	}
}
