package memstore

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// BenchmarkSearchCostBreakdown attributes search latency to the parts that can
// be optimized independently, and reports the facts needed to choose between
// them: how large the archive is, how many chunks exist, and how many chunks a
// representative query matches.
//
// This exists because the whole-search benchmark reports a single number. When
// that number misses its target the next question is always "which part", and
// guessing has already produced one wrong answer in this package: a fixture bug
// made the "realistic" benchmark measure the worst case. The same discipline
// applies here, so the breakdown is measured rather than assumed.
//
// The layers are cumulative. count_only isolates FTS5 match enumeration,
// inner_rank adds bm25 scoring and the scope joins, and full_search adds the
// coverage query and the excerpt join. The difference between consecutive rows
// is the cost of that layer.
//
// index_status is measured after full_search on purpose. The coverage query
// scans mem_v2_chunks, and whether those pages are already resident changes its
// cost by an order of magnitude. Measuring it first, on a cold cache, reports a
// number that no real search ever pays.
//
// The per-query rows repeat one query, so they measure a fully warm cache for
// that query's pages. The target benchmark rotates through all four, so each
// iteration touches different pages and misses. Those are different
// measurements, and the rotating row exists to bridge them: it runs the same
// rotation the target benchmark runs, so its number is directly comparable to
// the target's. Comparing a repeated-query row against the target's aggregate
// is what made an earlier version of this breakdown appear to contradict the
// target by a factor of three.
func BenchmarkSearchCostBreakdown(b *testing.B) {
	count := benchMessageCount(b)
	b.Run(fmt.Sprintf("messages=%d", count), func(b *testing.B) {
		path := seedBenchArchive(b, count)
		store, err := OpenReadOnly(path)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { store.Close() })
		ctx := context.Background()

		if info, err := os.Stat(path); err == nil {
			b.ReportMetric(float64(info.Size())/(1024*1024), "db-mb")
		}
		var chunks int64
		if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM mem_v2_chunks`).Scan(&chunks); err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(chunks), "chunks")

		// The same four queries the target benchmark rotates through, so the
		// per-query rows below explain that benchmark's aggregate number.
		queries := []string{
			benchWord(11) + " " + benchWord(22) + " " + benchWord(33),
			benchCJKWord(11) + " " + benchCJKWord(22),
			benchWord(44) + " " + benchWord(55),
			benchCJKWord(33) + " " + benchCJKWord(44),
		}
		inner := `SELECT mem_v2_chunk_fts.rowid, bm25(mem_v2_chunk_fts)
			FROM mem_v2_chunk_fts
			JOIN mem_v2_chunks c ON c.id=mem_v2_chunk_fts.rowid
			JOIN mem_v2_messages m ON m.id=c.message_id
			JOIN mem_v2_sessions s ON s.id=m.session_id
			WHERE mem_v2_chunk_fts MATCH ? AND m.principal_id=?
			ORDER BY bm25(mem_v2_chunk_fts) LIMIT ?`
		principal := archiveID("principal", "bench", "owner")
		scope := SearchScope{Adapter: "bench", UserID: "owner"}

		run := func(name string, fn func() error) {
			b.Run(name, func(b *testing.B) {
				samples := make([]time.Duration, 0, b.N)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					start := time.Now()
					if err := fn(); err != nil {
						b.Fatal(err)
					}
					samples = append(samples, time.Since(start))
				}
				b.StopTimer()
				reportLatencies(b, summarizeLatencies(samples))
			})
		}

		// Warm the archive so the layer timings below are not dominated by the
		// first read of a freshly written file. This uses the same helper the
		// target benchmark uses, so the two numbers are comparable: an earlier
		// version warmed with a single search here while the target benchmark
		// warmed differently, and the two disagreed by a factor of four.
		warmSearchCache(b, store, scope, queries)

		first := buildMatchQuery(searchTerms(queries[0]))
		run("count_only", func() error {
			var n int64
			return store.db.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM mem_v2_chunk_fts WHERE mem_v2_chunk_fts MATCH ?`, first).Scan(&n)
		})
		run("inner_rank", func() error {
			rows, err := store.db.QueryContext(ctx, inner, first, principal, maxSearchCandidates)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var id int64
				var rank float64
				if err := rows.Scan(&id, &rank); err != nil {
					return err
				}
			}
			return rows.Err()
		})
		run("full_search", func() error {
			_, err := store.Search(ctx, SearchOptions{Scope: scope, Query: queries[0], Limit: 10})
			return err
		})
		run("index_status", func() error {
			_, err := store.IndexStatus(ctx)
			return err
		})
		run("index_coverage", func() error {
			_, err := store.IndexCoverage(ctx)
			return err
		})

		// Per-query cost. Each row repeats one query, so it reports the warm cost
		// of that query rather than the cost of the rotation the target
		// benchmark performs.
		for i, query := range queries {
			query := query
			run(fmt.Sprintf("query_%d", i+1), func() error {
				_, err := store.Search(ctx, SearchOptions{Scope: scope, Query: query, Limit: 10})
				return err
			})
		}

		// The rotation the target benchmark performs, so this row is directly
		// comparable to BenchmarkScopedLexicalSearch's aggregate number.
		rotating := 0
		run("rotating", func() error {
			query := queries[rotating%len(queries)]
			rotating++
			_, err := store.Search(ctx, SearchOptions{Scope: scope, Query: query, Limit: 10})
			return err
		})
	})
}
