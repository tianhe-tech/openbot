package memstore

import (
	"context"
	"path/filepath"
	"testing"
)

func TestArchiveMigrationIsExplicitAndIdempotent(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "memory.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := checkArchiveSchema(context.Background(), store.db); err == nil {
		t.Fatal("ordinary open unexpectedly enabled archive")
	}
	if err := store.Record(MemRecord{ID: "preserve", Request: "request", Response: "answer"}); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		if err := store.MigrateArchive(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_schema`).Scan(&count); err != nil || count != archiveSchemaVersion {
		t.Fatalf("migration count=%d: %v", count, err)
	}
	var version int
	if err := store.db.QueryRow(`SELECT MAX(version) FROM mem_v2_schema`).Scan(&version); err != nil || version != archiveSchemaVersion {
		t.Fatalf("schema version=%d: %v", version, err)
	}
	var response string
	if err := store.db.QueryRow(`SELECT response FROM mem_records WHERE id='preserve'`).Scan(&response); err != nil || response != "answer" {
		t.Fatalf("legacy data changed: %q, %v", response, err)
	}
	if _, err := store.db.Exec(`INSERT INTO mem_v2_schema VALUES(99,0)`); err != nil {
		t.Fatal(err)
	}
	if err := store.MigrateArchive(context.Background()); err == nil {
		t.Fatal("accepted newer schema")
	}
}

func TestArchiveMigrationRollsBackOnFailure(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "memory.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.db.Exec(`CREATE TABLE mem_v2_turns (incompatible TEXT)`); err != nil {
		t.Fatal(err)
	}
	if err := store.MigrateArchive(context.Background()); err == nil {
		t.Fatal("expected incompatible schema error")
	}
	var count int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name IN ('mem_v2_schema','mem_v2_sessions')`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial migration committed: %d, %v", count, err)
	}
}

// TestArchiveMigrationUpgradesVersionOneArchive verifies that an archive
// created by the original schema is upgraded in place: version-1 data survives
// and the version-2 tables appear.
func TestArchiveMigrationUpgradesVersionOneArchive(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "memory.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	// Build a version-1 archive by hand, exactly as the first release did.
	if _, err := store.db.Exec(`CREATE TABLE mem_v2_schema (version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	for _, statement := range archiveMigrationV1 {
		if _, err := store.db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.db.Exec(`INSERT INTO mem_v2_schema(version,applied_at) VALUES(1,0)`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec(`INSERT INTO mem_v2_sessions
		(id,principal_id,adapter,user_id,thread_id,upstream_session_id,project_key,work_dir,created_at)
		VALUES('session','principal','test','owner','thread','','project','/work',1)`); err != nil {
		t.Fatal(err)
	}

	if err := store.MigrateArchive(ctx); err != nil {
		t.Fatal(err)
	}
	var version int
	if err := store.db.QueryRow(`SELECT MAX(version) FROM mem_v2_schema`).Scan(&version); err != nil || version != archiveSchemaVersion {
		t.Fatalf("schema version=%d: %v", version, err)
	}
	for _, table := range []string{"mem_v2_partials", "mem_v2_tool_calls", "mem_v2_attachments"} {
		var name string
		if err := store.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name); err != nil {
			t.Fatalf("missing table %s: %v", table, err)
		}
	}
	// Version 4 adds the word-tokenized index. It is a virtual table, so it
	// appears in sqlite_master with type='table' like any other.
	for _, table := range []string{"mem_v2_chunk_fts", "mem_v2_chunk_word_fts"} {
		var name string
		if err := store.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name); err != nil {
			t.Fatalf("missing index table %s: %v", table, err)
		}
	}
	// The backfill high-water mark must exist and start at zero, because every
	// chunk that predates the upgrade is missing a word entry.
	var highWater int64
	if err := store.db.QueryRow(`SELECT word_backfill_rowid FROM mem_v2_index_state WHERE id=1`).Scan(&highWater); err != nil {
		t.Fatalf("missing word_backfill_rowid: %v", err)
	}
	if highWater != 0 {
		t.Fatalf("word backfill should start at zero, got %d", highWater)
	}
	// Version 5 adds the index the bounded scan needs. Without it the scan
	// sorts every message in the principal before its LIMIT applies, which
	// measured 13.8 s p95 at 100k messages. The index is asserted by name
	// because a missing index is invisible until the archive is large.
	var scanIndex string
	if err := store.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='index' AND name='mem_v2_message_scan'`).Scan(&scanIndex); err != nil {
		t.Fatalf("missing scan index: %v", err)
	}
	var sessions int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_sessions`).Scan(&sessions); err != nil || sessions != 1 {
		t.Fatalf("version-1 data lost: %d, %v", sessions, err)
	}
	if err := checkArchiveSchema(ctx, store.db); err != nil {
		t.Fatalf("upgraded archive rejected: %v", err)
	}
}
