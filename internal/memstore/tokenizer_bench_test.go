package memstore

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// BenchmarkTokenizerComparison is the decisive experiment for the search
// latency target, and it exists because every cheaper experiment has been run
// and has failed to explain the cost.
//
// The attribution benchmark established that the cost is inside FTS5, that it is
// not the joins, that it is not the candidate limit, that it is not the number
// of matches (cutting matches 12x cuts latency 10%), and that it is not the
// scoring (scoring every match without ordering costs about 5 ms of a 180 ms
// query). What is left is the tokenizer: the trigram tokenizer indexes every
// three-character sequence, so a six-character term is a phrase of four
// overlapping trigrams and matching it means intersecting four posting lists
// whose length depends on how common those trigrams are, not on how many rows
// survive the query.
//
// This benchmark tests that directly. It builds two FTS5 tables over the same
// chunk text, one with the production trigram tokenizer and one with unicode61,
// and runs the same queries against both. The comparison is only meaningful
// because the content, the row count, and the process are identical; the
// tokenizer is the only difference.
//
// The result decides the shape of the fix:
//
//   - If unicode61 is several times faster, the cost is trigram posting-list
//     intersection and the fix is an index redesign: a word-tokenized index for
//     the terms it can handle, with trigram kept for substring and CJK search.
//   - If the two are comparable, the cost is inherent to FTS5 ranking and the
//     fix has to be a different retrieval strategy, not a different index.
//
// The unicode61 table is built here rather than in the schema because it is an
// experiment, not a migration. Nothing in production reads it.
func BenchmarkTokenizerComparison(b *testing.B) {
	count := benchMessageCount(b)
	path := seedBenchArchive(b, count)

	store, err := OpenReadOnly(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })
	ctx := context.Background()

	// The comparison table lives in a separate database so the seeded archive
	// stays read-only and reusable. It is populated from the archive's own
	// chunk text, so both indexes see byte-identical content.
	wordPath := filepath.Join(b.TempDir(), "word_index.db")
	wordDB, err := sql.Open("sqlite", "file:"+filepath.ToSlash(wordPath))
	if err != nil {
		b.Fatal(err)
	}
	wordDB.SetMaxOpenConns(1)
	b.Cleanup(func() { wordDB.Close() })
	if _, err := wordDB.ExecContext(ctx,
		`CREATE VIRTUAL TABLE word_fts USING fts5(content, tokenize='unicode61')`); err != nil {
		b.Fatal(err)
	}

	rows, err := store.db.QueryContext(ctx,
		`SELECT rowid, content FROM mem_v2_chunk_fts`)
	if err != nil {
		b.Fatal(err)
	}
	tx, err := wordDB.BeginTx(ctx, nil)
	if err != nil {
		b.Fatal(err)
	}
	insert, err := tx.PrepareContext(ctx,
		`INSERT INTO word_fts(rowid, content) VALUES (?, ?)`)
	if err != nil {
		b.Fatal(err)
	}
	var copied int
	for rows.Next() {
		var id int64
		var content string
		if err := rows.Scan(&id, &content); err != nil {
			b.Fatal(err)
		}
		if _, err := insert.ExecContext(ctx, id, content); err != nil {
			b.Fatal(err)
		}
		copied++
	}
	if err := rows.Err(); err != nil {
		b.Fatal(err)
	}
	rows.Close()
	insert.Close()
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
	if copied == 0 {
		b.Fatal("tokenizer comparison copied no chunks; the archive is empty")
	}
	b.Logf("tokenizer comparison: %d chunks indexed with both tokenizers", copied)

	// The same three-term query the attribution benchmark uses, so the numbers
	// are comparable to the ones that motivated this experiment.
	terms := searchTerms(benchWord(11) + " " + benchWord(22) + " " + benchWord(33))
	trigramMatch := buildMatchQuery(terms)
	wordMatch := buildMatchQuery(terms)

	// Selectivity is reported for both, because the two tokenizers do not
	// necessarily match the same rows: unicode61 matches whole tokens while
	// trigram matches substrings, so a difference in match count is expected
	// and has to be visible rather than inferred.
	var trigramMatched, wordMatched int64
	if err := store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM mem_v2_chunk_fts WHERE mem_v2_chunk_fts MATCH ?`,
		trigramMatch).Scan(&trigramMatched); err != nil {
		b.Fatal(err)
	}
	if err := wordDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM word_fts WHERE word_fts MATCH ?`,
		wordMatch).Scan(&wordMatched); err != nil {
		b.Fatal(err)
	}
	b.Run("selectivity", func(b *testing.B) {
		b.ReportMetric(float64(trigramMatched), "trigram-matched")
		b.ReportMetric(float64(wordMatched), "word-matched")
	})

	// The production query shape against both indexes: rank with bm25 and take
	// the top candidates. This is the number that has to reach 100 ms.
	run := func(name string, db *sql.DB, table, match string) {
		b.Run(name, func(b *testing.B) {
			query := fmt.Sprintf(
				`SELECT rowid, bm25(%s) FROM %s WHERE %s MATCH ?
				ORDER BY bm25(%s) LIMIT ?`, table, table, table, table)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.QueryContext(ctx, query, match, maxSearchCandidates)
				if err != nil {
					b.Fatal(err)
				}
				for rows.Next() {
					var id int64
					var rank float64
					if err := rows.Scan(&id, &rank); err != nil {
						b.Fatal(err)
					}
				}
				if err := rows.Err(); err != nil {
					b.Fatal(err)
				}
				rows.Close()
			}
		})
	}
	run("trigram_ranked", store.db, "mem_v2_chunk_fts", trigramMatch)
	run("word_ranked", wordDB, "word_fts", wordMatch)

	// Enumeration without ranking, so the comparison separates "how long does
	// it take to find the matches" from "how long does it take to order them".
	runIDs := func(name string, db *sql.DB, table, match string) {
		b.Run(name, func(b *testing.B) {
			query := fmt.Sprintf(
				`SELECT rowid FROM %s WHERE %s MATCH ? LIMIT ?`, table, table)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := db.QueryContext(ctx, query, match, maxSearchCandidates)
				if err != nil {
					b.Fatal(err)
				}
				for rows.Next() {
					var id int64
					if err := rows.Scan(&id); err != nil {
						b.Fatal(err)
					}
				}
				if err := rows.Err(); err != nil {
					b.Fatal(err)
				}
				rows.Close()
			}
		})
	}
	runIDs("trigram_ids", store.db, "mem_v2_chunk_fts", trigramMatch)
	runIDs("word_ids", wordDB, "word_fts", wordMatch)

	// A single term, to remove the phrase-intersection effect. With one term
	// the trigram tokenizer still intersects four trigram lists while
	// unicode61 looks up one token, so the gap here is the purest measure of
	// what the tokenizer costs.
	single := searchTerms(benchWord(11))
	run("trigram_single_ranked", store.db, "mem_v2_chunk_fts", buildMatchQuery(single))
	run("word_single_ranked", wordDB, "word_fts", buildMatchQuery(single))

	// Guard against the comparison silently measuring an empty index, which is
	// the failure mode that makes a fast number meaningless.
	if wordMatched == 0 {
		b.Fatal("unicode61 index matched nothing; the comparison is not measuring the same content")
	}
	if !strings.Contains(wordMatch, "OR") {
		b.Fatalf("expected an OR expression, got %q", wordMatch)
	}
}
