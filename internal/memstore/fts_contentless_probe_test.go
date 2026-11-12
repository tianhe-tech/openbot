package memstore

// This file is a probe, not a feature. It answers the question the plan asks
// before contentless FTS5 can be adopted: does the pinned SQLite library
// support a contentless index that can still be deleted from and updated, and
// what does it cost in space and in query behaviour?
//
// The answer decides whether the duplicated index text is removable. Both FTS
// tables currently store a full copy of every chunk's text, so the archive
// holds each message twice: once in mem_v2_blocks and once in each index. The
// packing measurement showed that 40% of the archive is recoverable from
// somewhere, and this is the somewhere that does not touch the read path.
//
// Nothing here is wired into the production code. If the probe says the
// library supports it, the work is a migration plus a read-path change, and
// both are large enough to deserve their own decision.

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

// probeDB opens a scratch database for the probe. It is deliberately not a
// memstore archive: the question is about the SQLite library, and building a
// full archive would make the probe depend on the schema it is meant to
// inform.
func probeDB(t *testing.T) *sql.DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "probe.db")
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		t.Fatalf("open probe db: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	return db
}

// TestContentlessDeleteIsSupported is the gate. If this fails, the rest of the
// probe is moot and contentless FTS5 is not available on this library.
//
// contentless_delete=1 was added in SQLite 3.43.0. The pinned driver bundles
// 3.51.3, so it should be present, but "should be" is exactly the kind of
// assumption that turns into a migration that cannot be rolled back. The test
// asserts the three operations the archive actually performs: insert, delete
// by rowid, and update by rowid.
func TestContentlessDeleteIsSupported(t *testing.T) {
	ctx := context.Background()
	db := probeDB(t)

	if _, err := db.ExecContext(ctx,
		`CREATE VIRTUAL TABLE probe USING fts5(content, content='', contentless_delete=1, tokenize='unicode61')`); err != nil {
		t.Fatalf("contentless_delete=1 is not supported by this SQLite build: %v", err)
	}

	// Insert. A contentless table still needs the text at insert time; it just
	// does not keep it.
	for i, text := range []string{
		"the gateway forwards requests to the upstream session",
		"a chunk of text about memory and recall",
		"unrelated filler about nothing in particular",
	} {
		if _, err := db.ExecContext(ctx,
			`INSERT INTO probe(rowid, content) VALUES(?, ?)`, i+1, text); err != nil {
			t.Fatalf("insert row %d: %v", i+1, err)
		}
	}

	// Match still works, because the index is what is searched.
	var n int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM probe WHERE probe MATCH 'gateway'`).Scan(&n); err != nil {
		t.Fatalf("match on contentless table: %v", err)
	}
	if n != 1 {
		t.Fatalf("match 'gateway' returned %d rows, want 1", n)
	}

	// bm25 still works: ranking is computed from the index, not the content.
	var rank float64
	if err := db.QueryRowContext(ctx,
		`SELECT bm25(probe) FROM probe WHERE probe MATCH 'gateway'`).Scan(&rank); err != nil {
		t.Fatalf("bm25 on contentless table: %v", err)
	}
	if rank == 0 {
		t.Fatalf("bm25 returned exactly zero, which is not a usable ranking")
	}

	// The content column is gone. This is the cost, and it is the reason the
	// read path has to change: the excerpt can no longer come from the index.
	var content sql.NullString
	if err := db.QueryRowContext(ctx,
		`SELECT content FROM probe WHERE probe MATCH 'gateway'`).Scan(&content); err != nil {
		t.Fatalf("select content from contentless table: %v", err)
	}
	if content.Valid {
		t.Fatalf("contentless table returned content %q; it is supposed to store none", content.String)
	}

	// Delete by rowid. This is the operation the current deletion path relies
	// on, and the reason a plain contentless table (content='' without
	// contentless_delete) cannot be adopted: it rejects DELETE.
	if _, err := db.ExecContext(ctx, `DELETE FROM probe WHERE rowid=1`); err != nil {
		t.Fatalf("delete by rowid on contentless table: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM probe WHERE probe MATCH 'gateway'`).Scan(&n); err != nil {
		t.Fatalf("match after delete: %v", err)
	}
	if n != 0 {
		t.Fatalf("deleted row still matches: %d rows", n)
	}

	// Update by rowid. indexMessage deletes and re-inserts rather than
	// updating, so this is not strictly required, but a table that cannot be
	// updated is a table whose every future caller has to know that.
	if _, err := db.ExecContext(ctx,
		`UPDATE probe SET content='the gateway now mentions a tunnel' WHERE rowid=2`); err != nil {
		t.Fatalf("update by rowid on contentless table: %v", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM probe WHERE probe MATCH 'tunnel'`).Scan(&n); err != nil {
		t.Fatalf("match after update: %v", err)
	}
	if n != 1 {
		t.Fatalf("updated row does not match its new text: %d rows", n)
	}
}

// probeCorpus builds a chunk-shaped corpus with a chosen vocabulary width.
//
// The width matters more than anything else in this measurement. A contentless
// index removes the stored text and keeps the postings, so the saving is
// text/(text+postings) and the postings size is what decides it. A narrow
// vocabulary makes every term appear in almost every chunk, which compresses
// the doclists to almost nothing and makes the saving look enormous. Real
// conversation has a wide vocabulary, so the narrow case is an upper bound and
// the wide case is the honest one.
func probeCorpus(chunks, vocabularyWidth int) []string {
	words := make([]string, vocabularyWidth)
	for i := range words {
		// Deterministic pseudo-words of varying length. They are not English,
		// but they have English's property that matters here: a long tail of
		// terms that appear rarely.
		words[i] = fmt.Sprintf("w%dx%d", i, i*7919%1000)
	}
	corpus := make([]string, chunks)
	for i := 0; i < chunks; i++ {
		var b strings.Builder
		for b.Len() < chunkTargetBytes {
			// A Zipf-ish draw: low indices are common, high indices are rare.
			// Squaring a uniform draw concentrates it near zero, which is the
			// shape natural language has.
			u := float64((i*31+b.Len())*2654435761%1000000) / 1000000
			index := int(u * u * float64(vocabularyWidth))
			if index >= vocabularyWidth {
				index = vocabularyWidth - 1
			}
			b.WriteString(words[index])
			b.WriteByte(' ')
		}
		corpus[i] = b.String()
	}
	return corpus
}

// TestContentlessIndexSpaceSaving measures what the change would buy.
//
// The comparison is between three tables holding the same text: a plain table
// (what the archive has today), a contentless-delete table, and the chunks
// table alone. The third is the floor: the text has to live somewhere, and
// mem_v2_blocks already holds it, so the index's job is to add as little as
// possible on top.
//
// Both tokenizers are measured, because the archive runs both and they behave
// very differently. The trigram index has far more postings per chunk — every
// three-character sequence rather than every word — so its postings are a
// larger share of its total and contentless saves proportionally less there.
// Reporting only the word index would overstate the win.
func TestContentlessIndexSpaceSaving(t *testing.T) {
	ctx := context.Background()

	type tokenizer struct {
		name string
		spec string
	}
	tokenizers := []tokenizer{
		{"unicode61", `tokenize='unicode61'`},
		{"trigram", `tokenize='trigram'`},
	}
	vocabularies := []struct {
		name  string
		width int
	}{
		{"narrow-30", 30},
		{"wide-20000", 20000},
	}

	const chunks = 2000
	for _, vocab := range vocabularies {
		corpus := probeCorpus(chunks, vocab.width)
		for _, tok := range tokenizers {
			t.Run(vocab.name+"/"+tok.name, func(t *testing.T) {
				db := probeDB(t)
				for _, ddl := range []string{
					`CREATE TABLE chunks(id INTEGER PRIMARY KEY, content TEXT NOT NULL)`,
					`CREATE VIRTUAL TABLE plain USING fts5(content, ` + tok.spec + `)`,
					`CREATE VIRTUAL TABLE contentless USING fts5(content, content='', contentless_delete=1, ` + tok.spec + `)`,
				} {
					if _, err := db.ExecContext(ctx, ddl); err != nil {
						t.Fatalf("create: %v", err)
					}
				}
				for i, text := range corpus {
					if _, err := db.ExecContext(ctx, `INSERT INTO chunks(id, content) VALUES(?, ?)`, i+1, text); err != nil {
						t.Fatalf("insert chunk: %v", err)
					}
					if _, err := db.ExecContext(ctx, `INSERT INTO plain(rowid, content) VALUES(?, ?)`, i+1, text); err != nil {
						t.Fatalf("insert plain: %v", err)
					}
					if _, err := db.ExecContext(ctx, `INSERT INTO contentless(rowid, content) VALUES(?, ?)`, i+1, text); err != nil {
						t.Fatalf("insert contentless: %v", err)
					}
				}

				// dbstat attributes pages to tables. It is the only way to see
				// where the bytes went, and it is what space.go already uses.
				bytesOf := func(name string) int64 {
					var total sql.NullInt64
					err := db.QueryRowContext(ctx,
						`SELECT SUM(pgsize) FROM dbstat WHERE name LIKE ?`, name+"%").Scan(&total)
					if err != nil {
						t.Skipf("dbstat unavailable, cannot measure: %v", err)
					}
					return total.Int64
				}

				chunkBytes := bytesOf("chunks")
				plainBytes := bytesOf("plain")
				contentlessBytes := bytesOf("contentless")

				t.Logf("corpus: %d chunks of ~%d bytes, %d-word vocabulary", chunks, chunkTargetBytes, vocab.width)
				t.Logf("chunks table (the text itself): %d bytes", chunkBytes)
				t.Logf("plain fts5 index:               %d bytes (%.2fx the text)", plainBytes, float64(plainBytes)/float64(chunkBytes))
				t.Logf("contentless fts5 index:         %d bytes (%.2fx the text)", contentlessBytes, float64(contentlessBytes)/float64(chunkBytes))
				t.Logf("saving from contentless:        %d bytes (%.1f%% of the plain index)",
					plainBytes-contentlessBytes,
					100*float64(plainBytes-contentlessBytes)/float64(plainBytes))

				// The assertion is that contentless is smaller, not by how
				// much. A threshold here would be a guess about the corpus, and
				// the corpus is synthetic; the number that matters is the one
				// logged above, measured against a real archive.
				if contentlessBytes >= plainBytes {
					t.Fatalf("contentless index (%d bytes) is not smaller than the plain one (%d bytes); "+
						"the change would buy nothing", contentlessBytes, plainBytes)
				}
			})
		}
	}
}

// TestContentlessSnippetIsUnavailable pins the cost that the read path has to
// absorb, so a later implementation cannot discover it by surprise.
//
// The search path currently selects ft.content and truncates it to
// maxExcerptBytes. With a contentless index that column is NULL, so the excerpt
// has to be reconstructed from the chunk's byte range into the message text.
// snippet() and highlight() are the obvious substitutes and both fail, because
// they need the content too.
func TestContentlessSnippetIsUnavailable(t *testing.T) {
	ctx := context.Background()
	db := probeDB(t)

	if _, err := db.ExecContext(ctx,
		`CREATE VIRTUAL TABLE probe USING fts5(content, content='', contentless_delete=1, tokenize='unicode61')`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO probe(rowid, content) VALUES(1, 'the gateway forwards requests to the upstream session')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	for _, fn := range []string{
		`snippet(probe, 0, '[', ']', '...', 8)`,
		`highlight(probe, 0, '[', ']')`,
	} {
		var out sql.NullString
		err := db.QueryRowContext(ctx,
			`SELECT `+fn+` FROM probe WHERE probe MATCH 'gateway'`).Scan(&out)
		if err == nil && out.Valid && out.String != "" {
			t.Fatalf("%s returned %q on a contentless table; the read path could keep using it", fn, out.String)
		}
		t.Logf("%s is unavailable on a contentless table (err=%v, value=%q)", fn, err, out.String)
	}
}
