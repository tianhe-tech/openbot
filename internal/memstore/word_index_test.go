package memstore

import (
	"context"
	"strings"
	"testing"
)

// simulateVersionThreeArchive strips the word index from an archive that was
// created at version 4, leaving exactly the state an upgraded version-3 archive
// is in: trigram entries for every chunk, no word entries, and a backfill
// high-water mark of zero.
//
// This is how the upgrade path is tested without keeping a copy of the old
// schema around. The migration itself is covered by
// TestArchiveMigrationUpgradesVersionOneArchive; what this helper isolates is
// the state the backfill has to repair.
func simulateVersionThreeArchive(t *testing.T, writer *ArchiveWriter) {
	t.Helper()
	ctx := context.Background()
	if _, err := writer.store.db.ExecContext(ctx, `DELETE FROM mem_v2_chunk_word_fts`); err != nil {
		t.Fatalf("clear word index: %v", err)
	}
	if _, err := writer.store.db.ExecContext(ctx,
		`UPDATE mem_v2_index_state SET word_backfill_rowid=0 WHERE id=1`); err != nil {
		t.Fatalf("reset backfill mark: %v", err)
	}
}

func TestWordIndexBackfillRepairsUpgradedArchive(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	ctx := context.Background()
	simulateVersionThreeArchive(t, writer)

	// Before the backfill the archive must admit that the word index is
	// incomplete. Reporting full coverage here would let a caller trust a
	// search that cannot see the archive.
	coverage, err := writer.store.IndexCoverage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if coverage.WordIndexComplete {
		t.Fatalf("word index reported complete with an empty index: %+v", coverage)
	}
	if coverage.WordBackfillRowID != 0 {
		t.Fatalf("backfill mark should be zero, got %d", coverage.WordBackfillRowID)
	}
	if coverage.MaxChunkID == 0 {
		t.Fatal("fixture produced no chunks")
	}

	// A word query against the empty word index finds nothing. This is the
	// failure the backfill exists to fix, and asserting it proves the test is
	// exercising the word path rather than the trigram one.
	result, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"},
		Query: "journal",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Filters.Mode != "word" {
		t.Fatalf("expected word mode, got %q", result.Filters.Mode)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("empty word index returned %d hits", len(result.Hits))
	}
	// The empty result must be labelled as incomplete. Without this flag the
	// caller cannot tell "the archive has no such message" from "the index that
	// would have found it is still being built", and the first reading is the
	// one a model will act on.
	if !result.WordIndexPartial {
		t.Fatal("word-routed query did not report partial word coverage")
	}
	// A trigram-routed query is unaffected by the missing word entries, so it
	// must not claim partial coverage. This is what makes the flag meaningful
	// rather than always-on.
	cjk, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"},
		Query: "并发数",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cjk.Filters.Mode != "fts" {
		t.Fatalf("expected fts mode for CJK, got %q", cjk.Filters.Mode)
	}
	if cjk.WordIndexPartial {
		t.Fatal("trigram-routed query reported partial word coverage")
	}
	if len(cjk.Hits) != 1 {
		t.Fatalf("trigram index should still find the CJK term, got %d hits", len(cjk.Hits))
	}

	// The backfill is resumable, so it is driven in small batches here to prove
	// the high-water mark advances and that a partial run leaves the archive
	// usable rather than corrupt.
	copied := 0
	for pass := 0; pass < 100; pass++ {
		count, err := writer.BackfillWordIndex(ctx, 1)
		if err != nil {
			t.Fatalf("backfill pass %d: %v", pass, err)
		}
		if count == 0 {
			break
		}
		copied += count
	}
	if copied != int(coverage.MaxChunkID) {
		t.Fatalf("backfilled %d chunks, expected %d", copied, coverage.MaxChunkID)
	}

	coverage, err = writer.store.IndexCoverage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !coverage.WordIndexComplete {
		t.Fatalf("word index still incomplete after backfill: %+v", coverage)
	}

	result, err = writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"},
		Query: "journal",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 {
		t.Fatalf("expected one hit after backfill, got %d", len(result.Hits))
	}
	if !strings.Contains(result.Hits[0].Excerpt, "journal") {
		t.Fatalf("excerpt missing the term: %q", result.Hits[0].Excerpt)
	}
	// Once the backfill is done the same query must stop claiming partial
	// coverage, or the flag would be noise a caller learns to ignore.
	if result.WordIndexPartial {
		t.Fatal("word index reported partial after a complete backfill")
	}
}

// TestWordIndexBackfillIsIdempotent covers the overlap between the backfill and
// the normal write path. A chunk written after the migration already has a word
// entry, and the backfill still reaches it because the high-water mark starts at
// zero. The copy must replace rather than fail, or the backfill would stall on
// the first such chunk and never finish.
func TestWordIndexBackfillIsIdempotent(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	ctx := context.Background()

	// Reset only the mark, leaving the word entries in place. This is the exact
	// overlap: every chunk already has a word entry and the backfill will copy
	// all of them again.
	if _, err := writer.store.db.ExecContext(ctx,
		`UPDATE mem_v2_index_state SET word_backfill_rowid=0 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	copied, err := writer.BackfillWordIndex(ctx, WordBackfillBatchLimit)
	if err != nil {
		t.Fatalf("backfill over existing entries: %v", err)
	}
	if copied == 0 {
		t.Fatal("backfill copied nothing")
	}
	// Running it again must be a no-op rather than a duplicate or an error.
	again, err := writer.BackfillWordIndex(ctx, WordBackfillBatchLimit)
	if err != nil {
		t.Fatalf("second backfill: %v", err)
	}
	if again != 0 {
		t.Fatalf("second backfill copied %d chunks, expected 0", again)
	}
	result, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"},
		Query: "journal",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 {
		t.Fatalf("expected one hit, got %d", len(result.Hits))
	}
}

func TestWordIndexBackfillRejectsOversizedBatch(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	if _, err := writer.BackfillWordIndex(context.Background(), WordBackfillBatchLimit+1); err == nil {
		t.Fatal("accepted a batch larger than the limit")
	}
	if _, err := writer.BackfillWordIndex(context.Background(), 0); err == nil {
		t.Fatal("accepted a zero batch")
	}
}

// TestSearchRoutesTermsByShape pins the routing rule. The rule is the whole
// reason two indexes exist, so it is asserted directly rather than inferred from
// latency: a plain ASCII word goes to the word index, a CJK phrase goes to the
// trigram index because unicode61 cannot tokenize it, a term containing a
// separator goes to the trigram index because unicode61 would split it, and a
// term too short for trigram falls back to the bounded scan.
func TestSearchRoutesTermsByShape(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	ctx := context.Background()
	scope := SearchScope{Adapter: "test", UserID: "owner"}

	cases := []struct {
		query string
		mode  string
		why   string
	}{
		{"journal", "word", "plain ASCII word"},
		{"JOURNAL", "word", "case does not change the shape"},
		{"config2", "word", "digits are part of a word"},
		{"并发数", "fts", "CJK has no word boundaries for unicode61"},
		{"config.yaml", "fts", "a dot is a separator, so the term is not one token"},
		{"/work/crawler", "fts", "a path is not a single token"},
		{"do", "scan", "two runes cannot be matched by trigram"},
	}
	for _, testCase := range cases {
		result, err := writer.store.Search(ctx, SearchOptions{Scope: scope, Query: testCase.query})
		if err != nil {
			t.Fatalf("%q: %v", testCase.query, err)
		}
		if result.Filters.Mode != testCase.mode {
			t.Fatalf("%q routed to %q, expected %q (%s)",
				testCase.query, result.Filters.Mode, testCase.mode, testCase.why)
		}
	}
}

// TestSearchHybridMergesBothIndexes covers the mixed query, which is the case
// that would silently lose results if the two index paths were not merged. The
// fixture has one message containing an ASCII term and another containing a CJK
// term, so a hybrid query must return both.
func TestSearchHybridMergesBothIndexes(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	ctx := context.Background()
	result, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"},
		Query: "journal 并发数",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Filters.Mode != "hybrid" {
		t.Fatalf("expected hybrid mode, got %q", result.Filters.Mode)
	}
	if len(result.Hits) != 2 {
		t.Fatalf("expected both index paths to contribute, got %d hits", len(result.Hits))
	}
	// The two hits come from different projects, so the merge must not have
	// collapsed them into one message.
	seen := map[string]bool{}
	for _, hit := range result.Hits {
		seen[hit.MessageID] = true
	}
	if len(seen) != 2 {
		t.Fatalf("hybrid merge collapsed distinct messages: %+v", result.Hits)
	}
}
