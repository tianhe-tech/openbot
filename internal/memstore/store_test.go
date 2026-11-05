package memstore

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestRecordSurvivesReopenAndRecall(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	request := "\u4fee\u590d\u4e0a\u6b21\u90e8\u7f72\u7684\u95ee\u9898"
	response := strings.Repeat("\u914d\u7f6e\u5df2\u66f4\u65b0", 200) + " evidence-only-identifier"
	record := MemRecord{
		ID: "preserved", Adapter: "test", UserID: "owner",
		Request: request, Response: response, Strength: 0.001,
		Ts: time.Now().AddDate(0, 0, -90),
	}
	if err := store.Record(record); err != nil {
		store.Close()
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	for _, keyword := range []string{"evidence-only-identifier", "\u914d\u7f6e"} {
		records, err := store.Recall([]string{keyword}, "test", "owner", time.Time{}, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 1 {
			t.Fatalf("recall %q: got %d records, want 1", keyword, len(records))
		}
		if records[0].Request != request || records[0].Response != response || !utf8.ValidString(records[0].Response) {
			t.Fatalf("recall %q did not preserve original text", keyword)
		}
		if records[0].Strength != record.Strength || records[0].RecallCnt != 0 {
			t.Fatalf("recall mutated memory: %+v", records[0])
		}
	}
	ids, err := store.recallBM25([]string{"evidence-only-identifier"}, "test", "owner", time.Time{}, 10)
	if err != nil || len(ids) != 1 || ids[0] != record.ID {
		t.Fatalf("response not indexed: %v, %v", ids, err)
	}
}

func TestOpenReadOnlyPreservesDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "space # query?.db")
	if os.PathSeparator == '\\' {
		path = filepath.Join(t.TempDir(), "space # query.db")
	}
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { writer.Close() })
	if err := writer.Record(MemRecord{ID: "original", Request: "\u4e4b\u524d\u7684\u9879\u76ee", Response: "answer-evidence", Strength: 0.001}); err != nil {
		t.Fatal(err)
	}
	reader, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })
	var mode string
	var timeout, queryOnly int
	if err := writer.db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
		t.Fatalf("journal mode: %q, %v", mode, err)
	}
	if err := reader.db.QueryRow("PRAGMA busy_timeout").Scan(&timeout); err != nil || timeout != 5000 {
		t.Fatalf("busy timeout: %d, %v", timeout, err)
	}
	if err := reader.db.QueryRow("PRAGMA query_only").Scan(&queryOnly); err != nil || queryOnly != 1 {
		t.Fatalf("query_only: %d, %v", queryOnly, err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		records, err := reader.Recall([]string{"answer-evidence"}, "", "", time.Time{}, 10)
		if err != nil || len(records) != 1 || records[0].RecallCnt != 0 || records[0].Strength != 0.001 {
			t.Fatalf("read-only recall: %+v, %v", records, err)
		}
	}
	if err := reader.Record(MemRecord{ID: "forbidden", Request: "write", Response: "no"}); err == nil {
		t.Fatal("read-only connection accepted a write")
	}
	var count int
	if err := writer.db.QueryRow("SELECT COUNT(*) FROM mem_records").Scan(&count); err != nil || count != 1 {
		t.Fatalf("record count changed: %d, %v", count, err)
	}
}

func TestOpenReadOnlyDoesNotCreateOrMigrate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "memory.db")
	if store, err := OpenReadOnly(path); err == nil {
		store.Close()
		t.Fatal("opened missing database")
	}
	if _, err := os.Stat(filepath.Dir(path)); !os.IsNotExist(err) {
		t.Fatalf("read-only open created directory: %v", err)
	}
	writer, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.db.Exec("DROP TABLE mem_search_migrations"); err != nil {
		t.Fatal(err)
	}
	writer.Close()
	if store, err := OpenReadOnly(path); err == nil {
		store.Close()
		t.Fatal("read-only open migrated an incompatible schema")
	}
	database, err := openDatabase(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var count int
	if err := database.db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE name = 'mem_search_migrations'").Scan(&count); err != nil || count != 0 {
		t.Fatalf("schema was modified: %d, %v", count, err)
	}
}

func TestSearchIndexMigrationIncludesExistingAnswers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Record(MemRecord{ID: "legacy", Request: "deploy", Response: "unique-answer-token"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.Exec("UPDATE mem_fts SET content = 'deploy'; DROP TABLE mem_search_migrations"); err != nil {
		t.Fatal(err)
	}
	store.Close()
	for attempt := 0; attempt < 2; attempt++ {
		store, err = Open(path)
		if err != nil {
			t.Fatal(err)
		}
		ids, err := store.recallBM25([]string{"unique-answer-token"}, "", "", time.Time{}, 10)
		if err != nil || len(ids) != 1 || ids[0] != "legacy" {
			t.Fatalf("index migration missed original answer: %v, %v", ids, err)
		}
		var count int
		if err := store.db.QueryRow("SELECT COUNT(*) FROM mem_fts").Scan(&count); err != nil || count != 1 {
			t.Fatalf("index migration duplicated rows: %d, %v", count, err)
		}
		store.Close()
	}
}

func TestRecordRollsBackOnIndexFailure(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "memory.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.db.Exec("DROP TABLE mem_fts"); err != nil {
		t.Fatal(err)
	}
	if err := store.Record(MemRecord{ID: "unindexed", Request: "request", Response: "answer"}); err == nil {
		t.Fatal("index failure was ignored")
	}
	var count int
	if err := store.db.QueryRow("SELECT COUNT(*) FROM mem_records").Scan(&count); err != nil || count != 0 {
		t.Fatalf("partial record committed: %d, %v", count, err)
	}
}

func TestBM25FiltersBeforeCandidateLimit(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "memory.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for _, record := range []MemRecord{
		{ID: "foreign-user", Adapter: "test", UserID: "other", Response: "needle"},
		{ID: "foreign-adapter", Adapter: "other", UserID: "owner", Response: "needle"},
		{ID: "expired", Adapter: "test", UserID: "owner", Response: "needle", Ts: time.Now().AddDate(0, 0, -60)},
		{ID: "eligible", Adapter: "test", UserID: "owner", Response: "needle " + strings.Repeat("long evidence ", 100), Strength: 0.001},
	} {
		if err := store.Record(record); err != nil {
			t.Fatal(err)
		}
	}
	ids, err := store.recallBM25([]string{"needle"}, "test", "owner", time.Now().AddDate(0, 0, -7), 1)
	if err != nil || len(ids) != 1 || ids[0] != "eligible" {
		t.Fatalf("eligible candidate crowded out: %v, %v", ids, err)
	}
	rows, err := store.recallLIKE([]string{"%", "_"}, "test", "owner", time.Time{}, 10)
	if err != nil || len(rows) != 0 {
		t.Fatalf("LIKE wildcard was not treated literally: %v, %v", rows, err)
	}
}
