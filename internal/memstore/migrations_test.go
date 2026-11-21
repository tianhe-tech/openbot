package memstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
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
	// Version 6 adds the instance lease. It is asserted by name because an
	// archive without it cannot tell a second gateway apart from the first,
	// and that failure is silent until two processes run at once.
	var leaseTable string
	if err := store.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name='mem_v2_instance_lease'`).Scan(&leaseTable); err != nil {
		t.Fatalf("missing instance lease table: %v", err)
	}
	// Version 7 makes both indexes contentless. The observable difference is
	// the absence of the _content shadow table: a regular FTS5 table keeps the
	// text in one, and a contentless table has none at all. Asserting the
	// shadow table rather than the CREATE statement is deliberate — the
	// statement is what the migration wrote, the shadow table is what SQLite
	// actually built, and only the second one decides whether the text is
	// stored.
	for _, shadow := range []string{"mem_v2_chunk_fts_content", "mem_v2_chunk_word_fts_content"} {
		var name string
		err := store.db.QueryRow(`SELECT name FROM sqlite_master WHERE name=?`, shadow).Scan(&name)
		if err == nil {
			t.Fatalf("%s exists; the index still stores text and schema v7 did not take effect", shadow)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("probing %s: %v", shadow, err)
		}
	}
	// Version 8 adds the durable-knowledge tables. They are asserted by name
	// because a missing table is invisible until extraction runs, and by then
	// the failure looks like a broken feature rather than a skipped migration.
	// The evidence table is asserted separately from the facts table on
	// purpose: provenance is the whole point of the feature, and a schema that
	// stored facts without evidence would still pass a facts-only check.
	for _, table := range []string{"mem_v2_knowledge_facts", "mem_v2_knowledge_evidence", "mem_v2_knowledge_jobs"} {
		var name string
		if err := store.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, table).Scan(&name); err != nil {
			t.Fatalf("missing knowledge table %s: %v", table, err)
		}
	}
	// The evidence foreign key is deliberately not ON DELETE CASCADE: a
	// deletion that forgot to remove evidence must abort loudly rather than
	// silently leave a fact pointing at a message that no longer exists.
	var evidenceSQL string
	if err := store.db.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='mem_v2_knowledge_evidence'`).Scan(&evidenceSQL); err != nil {
		t.Fatalf("reading evidence schema: %v", err)
	}
	if strings.Contains(strings.ToUpper(evidenceSQL), "ON DELETE CASCADE") {
		t.Fatalf("evidence cascades on delete, which hides a deletion that forgot it:\n%s", evidenceSQL)
	}
	var sessions int
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_sessions`).Scan(&sessions); err != nil || sessions != 1 {
		t.Fatalf("version-1 data lost: %d, %v", sessions, err)
	}
	if err := checkArchiveSchema(ctx, store.db); err != nil {
		t.Fatalf("upgraded archive rejected: %v", err)
	}
}

// TestWriterRefusalNamesTheDirectionOfTheSkew pins the writer's startup message.
//
// The writer already refused a mismatched archive before this test existed, so
// the behaviour under test is not the refusal but what it says. The old message
// was "unsupported archive schema 7 (want 6)", which is accurate and leaves the
// operator to work out which side is wrong: a version above the binary's means
// upgrade the binary, a version below means run memory-migrate, and the two
// remedies are opposite. Getting it backwards on a newer archive means
// downgrading the archive, which discards whatever the newer binary wrote.
func TestWriterRefusalNamesTheDirectionOfTheSkew(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MigrateArchive(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Written through database/sql because memstore has no API for recording a
	// version it cannot produce: the only writer that would do this is a later
	// binary, and giving this one a method to fake it would be a method that
	// exists only for tests.
	if _, err := store.db.Exec(`INSERT INTO mem_v2_schema(version, applied_at) VALUES (?, 0)`, archiveSchemaVersion+1); err != nil {
		t.Fatal(err)
	}
	err = checkArchiveSchema(context.Background(), store.db)
	if err == nil {
		t.Fatal("writer accepted an archive newer than itself")
	}
	message := err.Error()
	if !strings.Contains(message, "newer") {
		t.Fatalf("refusal does not say the archive is newer: %q", message)
	}
	if !strings.Contains(message, "upgrade this binary") {
		t.Fatalf("refusal does not name the remedy: %q", message)
	}
	if !strings.Contains(message, fmt.Sprint(archiveSchemaVersion+1)) {
		t.Fatalf("refusal does not name the archive's version: %q", message)
	}
	store.Close()

	// The older direction must not borrow the newer wording: telling an
	// operator to upgrade a binary that is already current would send them
	// looking for a release that does not exist.
	older, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer older.Close()
	// Both the faked version and the real one have to go. Deleting only the
	// maximum would leave the archive at exactly this binary's version, which
	// is the one case that must be accepted, and the test would then fail for
	// a reason that has nothing to do with the message it is checking.
	if _, err := older.db.Exec(`DELETE FROM mem_v2_schema WHERE version >= ?`, archiveSchemaVersion); err != nil {
		t.Fatal(err)
	}
	err = checkArchiveSchema(context.Background(), older.db)
	if err == nil {
		t.Fatal("writer accepted an archive older than itself")
	}
	if strings.Contains(err.Error(), "upgrade this binary") {
		t.Fatalf("older archive told the operator to upgrade the binary: %q", err)
	}
	if !strings.Contains(err.Error(), "memory-migrate") {
		t.Fatalf("older archive does not name the remedy: %q", err)
	}
}
