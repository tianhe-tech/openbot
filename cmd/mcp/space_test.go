package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestMemorySpaceReportsFileAndPrincipalSeparately pins the property that makes
// the space report honest: whole-file numbers and per-principal numbers are
// reported in separate groups and never added together.
//
// The distinction is not cosmetic. Every principal shares the same database
// pages, so a per-principal share of the file would be a fiction. A report that
// presented one number would either overstate what one user costs or understate
// what the archive occupies, and either way the reader would believe it.
func TestMemorySpaceReportsFileAndPrincipalSeparately(t *testing.T) {
	server := v2ServerForTest(t)
	output, err := server.toolMemorySpace(map[string]interface{}{})
	if err != nil {
		t.Fatalf("space: %v", err)
	}

	for _, expected := range []string{
		"scope_mode: single-owner",
		"### Database file (whole archive, shared by every principal)",
		"file_bytes:", "wal_bytes:", "shm_bytes:", "free_bytes:", "logical_bytes:",
		"### This principal (logical sizes, subsets of the file above)",
		"block_bytes:", "artifact_bytes:", "embedding_bytes:",
		"file_mib:",
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("space report missing %q: %s", expected, output)
		}
	}

	// The stored-text line is the number the contentless migration was decided
	// on, so it must still be printed. Schema v7 made both indexes contentless,
	// which means the honest value for an indexed archive is now zero: the text
	// lives in the message blocks and nowhere else. Printing the line anyway is
	// what lets an operator confirm the migration took effect on their archive.
	if !strings.Contains(output, "fts_content_bytes:") {
		t.Fatalf("space report did not print fts_content_bytes: %s", output)
	}
	if !strings.Contains(output, "fts_content_bytes: 0 ") {
		t.Fatalf("fts_content_bytes is not zero for a v7 archive: %s", output)
	}

	// The fixture wrote two messages, so the principal's block bytes must be
	// positive. A zero here would mean the report measured nothing while still
	// printing a plausible file size.
	if strings.Contains(output, "block_bytes: 0\n") {
		t.Fatalf("block_bytes is zero for an archive with two messages: %s", output)
	}
	// No backup was named, so the backup section must be absent rather than
	// present with a zero that reads as "the backup is empty".
	if strings.Contains(output, "backup_bytes:") {
		t.Fatalf("space report printed a backup section without a backup path: %s", output)
	}
	// No conversation content and no principal label, for the same reason the
	// health tool is asserted this way.
	for _, forbidden := range []string{"systemctl restart", "session-v2", "user_id:", "adapter:"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("space report leaked %q: %s", forbidden, output)
		}
	}
}

// TestMemorySpaceReportsBackupSeparately proves a named backup is measured in
// its own field and does not change the live archive's reported size.
//
// A backup is a copy, so folding it into the archive's footprint would make the
// archive appear to grow every time it is backed up, and the growth would be
// invisible as a cause because the file on disk did not change.
func TestMemorySpaceReportsBackupSeparately(t *testing.T) {
	server := v2ServerForTest(t)
	without, err := server.toolMemorySpace(map[string]interface{}{})
	if err != nil {
		t.Fatalf("space without backup: %v", err)
	}

	// The fixture's archive is read-only, so the backup is made by copying the
	// file rather than by VACUUM INTO. The report only stats the path, so any
	// existing file exercises the same code.
	backupPath := filepath.Join(t.TempDir(), "backup.db")
	if err := os.WriteFile(backupPath, []byte("not a real database, only a size"), 0o600); err != nil {
		t.Fatalf("create backup: %v", err)
	}

	with, err := server.toolMemorySpace(map[string]interface{}{"backup_path": backupPath})
	if err != nil {
		t.Fatalf("space with backup: %v", err)
	}
	if !strings.Contains(with, "### Backup (not part of the live archive)") {
		t.Fatalf("space report did not name the backup section: %s", with)
	}
	if strings.Contains(with, "backup_bytes: 0\n") {
		t.Fatalf("backup_bytes is zero for an existing backup file: %s", with)
	}

	// The live archive's numbers must be identical with and without a backup.
	// Comparing the whole output would fail on the added section, so the
	// file-level line is compared directly.
	if fileLine(without) != fileLine(with) {
		t.Fatalf("naming a backup changed the live file size: %q vs %q",
			fileLine(without), fileLine(with))
	}
}

// fileLine returns the file_bytes line of a space report, or "" if absent.
func fileLine(report string) string {
	for _, line := range strings.Split(report, "\n") {
		if strings.HasPrefix(line, "file_bytes:") {
			return line
		}
	}
	return ""
}
