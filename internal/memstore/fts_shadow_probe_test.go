package memstore

// This file answers one question: which FTS5 shadow table holds the duplicated
// text, and can its size be measured directly?
//
// The contentless probe established that the saving from a contentless index is
// exactly one copy of the chunk text. That means the saving is not an estimate
// at all — it is the size of a specific set of pages, and if those pages can be
// attributed by name then memory_space can report the number instead of leaving
// an operator to reason about it.
//
// A regular FTS5 table stores its text in a shadow table named <table>_content.
// A contentless table has no such table. So the sum of the _content shadow
// tables is precisely what contentless would remove.

import (
	"context"
	"testing"
)

// TestFTSContentShadowTableIsMeasurable pins the shadow table name and shows
// that its size is attributable with dbstat.
//
// The name matters because space.go sums FTS pages by prefix, which cannot
// distinguish the stored text from the postings. If the text lives in a
// separately named table, the split is available for free.
func TestFTSContentShadowTableIsMeasurable(t *testing.T) {
	ctx := context.Background()
	db := probeDB(t)

	if _, err := db.ExecContext(ctx,
		`CREATE VIRTUAL TABLE probe USING fts5(content, tokenize='unicode61')`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i, text := range probeCorpus(200, 5000) {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO probe(rowid, content) VALUES(?, ?)`, i+1, text); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	// Enumerate the shadow tables so the test fails loudly if a future SQLite
	// renames them, rather than silently reporting zero.
	rows, err := db.QueryContext(ctx,
		`SELECT name FROM sqlite_master WHERE name LIKE 'probe%' ORDER BY name`)
	if err != nil {
		t.Fatalf("list shadow tables: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan name: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate names: %v", err)
	}
	t.Logf("shadow tables: %v", names)

	var found bool
	for _, name := range names {
		if name == "probe_content" {
			found = true
		}
	}
	if !found {
		t.Fatalf("no probe_content shadow table in %v; the stored text is not separately attributable", names)
	}

	bytesOf := func(name string) int64 {
		var total int64
		if err := db.QueryRowContext(ctx,
			`SELECT COALESCE(SUM(pgsize),0) FROM dbstat WHERE name=?`, name).Scan(&total); err != nil {
			t.Skipf("dbstat unavailable: %v", err)
		}
		return total
	}
	// dbstat attributes pages to the shadow tables, not to the virtual table
	// name: asking for "probe" returns zero. The whole index is therefore the
	// sum over the prefix, which is what space.go already does.
	var totalBytes int64
	for _, name := range names {
		totalBytes += bytesOf(name)
	}
	contentBytes := bytesOf("probe_content")
	t.Logf("stored text (probe_content): %d bytes", contentBytes)
	t.Logf("whole index (all shadow):    %d bytes", totalBytes)
	t.Logf("text is %.1f%% of the index", 100*float64(contentBytes)/float64(totalBytes))

	if contentBytes == 0 {
		t.Fatalf("probe_content measured zero bytes; the attribution is not working")
	}
	if totalBytes == 0 {
		t.Fatalf("the whole index measured zero bytes; dbstat is not attributing the shadow tables")
	}
	if contentBytes >= totalBytes {
		t.Fatalf("stored text (%d) is not smaller than the whole index (%d); "+
			"the shadow tables are not being attributed separately", contentBytes, totalBytes)
	}
}

// TestContentlessHasNoContentShadowTable is the other half of the pin: it shows
// that a contentless table genuinely lacks the shadow table, so the measurement
// above is measuring the thing contentless removes rather than a coincidence.
func TestContentlessHasNoContentShadowTable(t *testing.T) {
	ctx := context.Background()
	db := probeDB(t)

	if _, err := db.ExecContext(ctx,
		`CREATE VIRTUAL TABLE probe USING fts5(content, content='', contentless_delete=1, tokenize='unicode61')`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO probe(rowid, content) VALUES(1, 'the gateway forwards requests')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM sqlite_master WHERE name='probe_content'`).Scan(&n); err != nil {
		t.Fatalf("count shadow tables: %v", err)
	}
	if n != 0 {
		t.Fatalf("a contentless table has a probe_content shadow table; " +
			"the saving would not be what the measurement claims")
	}
}
