package memstore

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// BenchmarkSearchAttribution answers the question the cost breakdown leaves
// open: which layer of the search path actually costs the time.
//
// The answer has moved twice, and both moves are recorded because each was a
// measurement that looked conclusive and was not.
//
// The first reading was that the join was negligible, because joining 100 real
// chunk ids with no MATCH in front of it costs about a millisecond. That
// measured the join over the rows that survive the limit, not over the rows the
// query processes, and the real join cost is a fifth to a third of the query.
//
// The second reading was that the cost was proportional to the number of
// matches, because the match count and the latency both grew fivefold between
// 20k and 100k. The AND row below refutes it: requiring all three terms instead
// of any of them cuts the match count by 12x and the latency by 10%. Match
// count is not the driver.
//
// What the rows do establish is that the cost is in FTS5 itself, before any
// join runs, and that it is not the scoring either: scoring every match without
// ordering is close to enumerating every match, while ordering the same matches
// costs several times more. The remaining candidate is the tokenizer, because
// the trigram tokenizer turns every term into a phrase of overlapping trigrams
// and must intersect one posting list per trigram, and those lists are as long
// as the trigrams are common. That is why a query matching 1.4% of the archive
// still costs 155 microseconds per match.
//
// Every row runs on the same seeded archive, after the same warm-up, as the
// target benchmark. The last several wrong answers in this package all came
// from comparing numbers taken under different conditions, so the conditions
// are shared rather than restated.
func BenchmarkSearchAttribution(b *testing.B) {
	// Every size runs in this one process, so the rows are comparable to each
	// other. Comparing sizes across processes compares machine states too.
	for _, count := range benchMessageCounts(b) {
		count := count
		b.Run(fmt.Sprintf("messages=%d", count), func(b *testing.B) {
			benchmarkSearchAttribution(b, count)
		})
	}
}

func benchmarkSearchAttribution(b *testing.B, count int) {
	{
		path := seedBenchArchive(b, count)
		store, err := OpenReadOnly(path)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { store.Close() })
		ctx := context.Background()

		queries := []string{
			benchWord(11) + " " + benchWord(22) + " " + benchWord(33),
			benchCJKWord(11) + " " + benchCJKWord(22),
			benchWord(44) + " " + benchWord(55),
			benchCJKWord(33) + " " + benchCJKWord(44),
		}
		scope := SearchScope{Adapter: "bench", UserID: "owner"}
		principal := archiveID("principal", "bench", "owner")
		match := buildMatchQuery(searchTerms(queries[0]))

		// Every latency below is a function of how selective the query is, so
		// the selectivity is reported rather than assumed. A query that matches
		// most of the archive measures the worst case no matter what the
		// benchmark is called.
		//
		// The metrics are reported inside a sub-benchmark because a metric
		// reported on the parent is dropped once sub-benchmarks exist, which is
		// how the first version of this benchmark lost the one number that
		// decides the fix.
		var totalChunks, matchedChunks int64
		if err := store.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM mem_v2_chunks`).Scan(&totalChunks); err != nil {
			b.Fatal(err)
		}
		if err := store.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM mem_v2_chunk_fts WHERE mem_v2_chunk_fts MATCH ?`,
			match).Scan(&matchedChunks); err != nil {
			b.Fatal(err)
		}
		b.Run("selectivity", func(b *testing.B) {
			b.ReportMetric(float64(matchedChunks), "matched-chunks")
			b.ReportMetric(float64(totalChunks), "total-chunks")
			if totalChunks > 0 {
				b.ReportMetric(float64(matchedChunks)*100/float64(totalChunks), "matched-pct")
			}
		})

		// A fixed set of chunk ids for the join-only row. The join cost has to
		// be measured without a MATCH in front of it, or it is not the join
		// cost; using real ids keeps the row count and the page layout honest.
		ids, err := sampleChunkIDs(ctx, store, maxSearchCandidates)
		if err != nil {
			b.Fatal(err)
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
		joinOnly := `SELECT c.message_id, m.session_id, s.project_key
			FROM mem_v2_chunks c
			JOIN mem_v2_messages m ON m.id=c.message_id
			JOIN mem_v2_sessions s ON s.id=m.session_id
			WHERE c.id IN (` + placeholders + `)`
		joinArgs := make([]any, len(ids))
		for i, id := range ids {
			joinArgs[i] = id
		}

		// CROSS JOIN, matching searchIndex. These are hand-built copies of the
		// production query, so they have to track it: with plain JOIN the
		// planner drove from mem_v2_messages through mem_v2_message_scan and
		// probed the FTS table once per message, which measured 104471 ms at 20k
		// against 75.20 ms for the same rows in the other order. A copy that
		// drifts from production does not measure production.
		inner := `SELECT mem_v2_chunk_fts.rowid, bm25(mem_v2_chunk_fts)
			FROM mem_v2_chunk_fts
			CROSS JOIN mem_v2_chunks c ON c.id=mem_v2_chunk_fts.rowid
			CROSS JOIN mem_v2_messages m ON m.id=c.message_id
			CROSS JOIN mem_v2_sessions s ON s.id=m.session_id
			WHERE mem_v2_chunk_fts MATCH ? AND m.principal_id=?
			ORDER BY bm25(mem_v2_chunk_fts) LIMIT ?`
		innerProject := `SELECT mem_v2_chunk_fts.rowid, bm25(mem_v2_chunk_fts)
			FROM mem_v2_chunk_fts
			CROSS JOIN mem_v2_chunks c ON c.id=mem_v2_chunk_fts.rowid
			CROSS JOIN mem_v2_messages m ON m.id=c.message_id
			CROSS JOIN mem_v2_sessions s ON s.id=m.session_id
			WHERE mem_v2_chunk_fts MATCH ? AND m.principal_id=? AND s.project_key=?
			ORDER BY bm25(mem_v2_chunk_fts) LIMIT ?`

		// The clock on this machine advances in ~1 ms steps, measured by
		// clockQuantum. A single timing of a sub-millisecond operation
		// therefore reports 0 or 1 ms and nothing between, which is why rows
		// like ids_limit_100 reported "0 p50-ms" and why the whole table's
		// small rows were quantized. Each sample below runs the operation
		// enough times to exceed the quantum, and the per-operation cost is
		// the sample divided by the batch size. The batch size is chosen once
		// per row from a calibration run, so a 300 ms row is not made 300x
		// slower by a batch size that a 0.5 ms row needed.
		quantum := clockQuantum()
		batchFor := func(fn func() error) (int, error) {
			// Calibrate: double the batch until one sample clears the quantum
			// by a comfortable margin, so the quantization error is a few
			// percent rather than a factor of two.
			target := 8 * quantum
			batch := 1
			for {
				start := time.Now()
				for i := 0; i < batch; i++ {
					if err := fn(); err != nil {
						return 0, err
					}
				}
				elapsed := time.Since(start)
				if elapsed >= target || batch >= 4096 {
					return batch, nil
				}
				// Jump straight to the estimated batch rather than doubling
				// one step at a time, then let the loop confirm it.
				if elapsed > 0 {
					estimate := int(float64(batch) * float64(target) / float64(elapsed))
					if estimate > batch {
						batch = estimate
					} else {
						batch *= 2
					}
				} else {
					batch *= 2
				}
			}
		}

		run := func(name string, fn func() error) {
			b.Run(name, func(b *testing.B) {
				// Warm this row's own query before measuring it. Without this,
				// rows measured later in the process are systematically faster
				// than rows measured earlier, because each row touches pages the
				// previous ones had not. The size of that bias is visible in the
				// output: full_search and word_full_search are the same call, and
				// they have measured 3% apart in one run and 37% apart in
				// another, purely from where they sit in the sequence. A row that
				// is not warmed is measuring its position, not its cost.
				if err := fn(); err != nil {
					b.Fatal(err)
				}
				batch, err := batchFor(fn)
				if err != nil {
					b.Fatal(err)
				}
				// There is no reference-operation ratio here, and its absence is
				// a measured result rather than an omission.
				//
				// The plan proposed reporting each row as a multiple of a fixed
				// reference operation measured in the same process, on the theory
				// that dividing by it would cancel the machine drift that makes
				// inner_rank at 20k read 75.20, 125.2, 260.6, and 361.1 ms on
				// identical code. That was tested and it does not work. Against a
				// CPU-bound hash the correlation with the row was 0.21-0.49, and
				// the ratio's coefficient of variation was the same as the row's
				// (37.5% vs 37.5%), so dividing removed nothing. Against SQLite
				// references the best case was fts-count-all, which cut the CV
				// from 20.0% to 16.0% — a fifth of the noise, not the drift.
				// Between processes the ratio was worse than the raw number: the
				// row read 16.76, 17.53, 17.36 ms (4.6% spread) while the ratio
				// read 2.10, 1.91, 2.16 (13% spread).
				//
				// The reason is that the drift is not a single machine-wide
				// factor. A CPU-bound hash and a page-cache-bound query do not
				// slow down together, so a reference built from one cannot
				// normalize the other. Reporting the ratio would have made the
				// numbers look rigorous while making them less reproducible.
				samples := make([]time.Duration, 0, b.N)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					start := time.Now()
					for j := 0; j < batch; j++ {
						if err := fn(); err != nil {
							b.Fatal(err)
						}
					}
					samples = append(samples, time.Since(start)/time.Duration(batch))
				}
				b.StopTimer()
				stats := summarizeLatencies(samples)
				reportLatencies(b, stats)
				b.ReportMetric(float64(batch), "batch")
				b.ReportMetric(float64(quantum.Nanoseconds())/1e6, "clock-ms")
			})
		}

		warmSearchCache(b, store, scope, queries)

		// The existing count_only row, repeated here so this benchmark stands on
		// its own and the rows below are directly comparable to it.
		run("count_all", func() error {
			var n int64
			return store.db.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM mem_v2_chunk_fts WHERE mem_v2_chunk_fts MATCH ?`,
				match).Scan(&n)
		})

		// Same enumeration, but the aggregation is bounded. If this is much
		// cheaper than count_all, the cost is in counting every match rather
		// than in finding them.
		run("count_limited", func() error {
			var n int64
			return store.db.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM (
					SELECT rowid FROM mem_v2_chunk_fts
					WHERE mem_v2_chunk_fts MATCH ? LIMIT ?
				)`, match, maxSearchCandidates).Scan(&n)
		})

		// Ids only, no ranking, no join. This is the cheapest query that still
		// has to find matches, so it is the floor for any FTS5-based approach.
		run("ids_limited", func() error {
			rows, err := store.db.QueryContext(ctx,
				`SELECT rowid FROM mem_v2_chunk_fts
				WHERE mem_v2_chunk_fts MATCH ? LIMIT ?`,
				match, maxSearchCandidates)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err != nil {
					return err
				}
			}
			return rows.Err()
		})

		// Ids plus bm25 ordering, still no join. The difference from
		// ids_limited is the cost of scoring and sorting every match.
		run("ids_bm25_limited", func() error {
			rows, err := store.db.QueryContext(ctx,
				`SELECT rowid, bm25(mem_v2_chunk_fts) FROM mem_v2_chunk_fts
				WHERE mem_v2_chunk_fts MATCH ?
				ORDER BY bm25(mem_v2_chunk_fts) LIMIT ?`,
				match, maxSearchCandidates)
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

		// The cost curve of the two shapes that matter. If bm25 is flat in the
		// limit while the plain id scan grows, the cost is scoring every match
		// rather than returning the first few, and no limit tuning can fix it.
		// If both grow together, the cost is enumeration and a smaller index is
		// the only lever.
		for _, limit := range []int{10, 100, 1000, 10000} {
			limit := limit
			run(fmt.Sprintf("ids_limit_%d", limit), func() error {
				rows, err := store.db.QueryContext(ctx,
					`SELECT rowid FROM mem_v2_chunk_fts
					WHERE mem_v2_chunk_fts MATCH ? LIMIT ?`, match, limit)
				if err != nil {
					return err
				}
				defer rows.Close()
				for rows.Next() {
					var id int64
					if err := rows.Scan(&id); err != nil {
						return err
					}
				}
				return rows.Err()
			})
			run(fmt.Sprintf("bm25_limit_%d", limit), func() error {
				rows, err := store.db.QueryContext(ctx,
					`SELECT rowid, bm25(mem_v2_chunk_fts) FROM mem_v2_chunk_fts
					WHERE mem_v2_chunk_fts MATCH ?
					ORDER BY bm25(mem_v2_chunk_fts) LIMIT ?`, match, limit)
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
		}

		// Three ways to ask for the same top-N, to separate the cost of scoring
		// from the cost of how the score is requested.
		//
		// FTS5 documents `rank` as the special column that means "order by
		// bm25", and the query planner has a dedicated path for it. Writing
		// `ORDER BY bm25(tbl)` instead may not take that path, and calling
		// bm25() in both the select list and the ORDER BY may evaluate it
		// twice. Both are cheap to test and expensive to guess wrong: the
		// difference decides whether the fix is a query rewrite or an index
		// redesign.
		run("bm25_orderby_rank", func() error {
			rows, err := store.db.QueryContext(ctx,
				`SELECT rowid FROM mem_v2_chunk_fts
				WHERE mem_v2_chunk_fts MATCH ?
				ORDER BY rank LIMIT ?`, match, maxSearchCandidates)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err != nil {
					return err
				}
			}
			return rows.Err()
		})
		run("bm25_orderby_rank_select", func() error {
			rows, err := store.db.QueryContext(ctx,
				`SELECT rowid, rank FROM mem_v2_chunk_fts
				WHERE mem_v2_chunk_fts MATCH ?
				ORDER BY rank LIMIT ?`, match, maxSearchCandidates)
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
		// Scoring with no ordering at all. If this is close to ids_limited the
		// scoring itself is cheap and the cost is the sort; if it is close to
		// bm25_limit_10 the scoring is the cost and the sort is free.
		run("bm25_select_no_order", func() error {
			rows, err := store.db.QueryContext(ctx,
				`SELECT rowid, bm25(mem_v2_chunk_fts) FROM mem_v2_chunk_fts
				WHERE mem_v2_chunk_fts MATCH ? LIMIT ?`, match, maxSearchCandidates)
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

		// The AND row above shows that cutting the match count by 12x cuts the
		// latency by only 10%, which means the cost is not proportional to the
		// number of matches. The remaining candidate is the posting lists: a
		// term's posting list is as long as the term is common, and FTS5 must
		// walk it whether or not the boolean expression keeps the row. These
		// rows measure that directly by varying how many terms the query has
		// and how common they are, at a fixed corpus size.
		//
		// A single rare term is the floor: one posting list, few matches. A
		// single common term is the same one posting list, many matches. If the
		// two differ by much, match count matters after all; if they are close,
		// the cost is the walk itself.
		rareMatch := buildMatchQuery([]string{benchWord(11)})
		commonMatch := buildMatchQuery([]string{benchWord(1)})
		var rareMatched, commonMatched int64
		if err := store.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM mem_v2_chunk_fts WHERE mem_v2_chunk_fts MATCH ?`,
			rareMatch).Scan(&rareMatched); err != nil {
			b.Fatal(err)
		}
		if err := store.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM mem_v2_chunk_fts WHERE mem_v2_chunk_fts MATCH ?`,
			commonMatch).Scan(&commonMatched); err != nil {
			b.Fatal(err)
		}
		b.Run("term_selectivity", func(b *testing.B) {
			b.ReportMetric(float64(rareMatched), "rare-matched")
			b.ReportMetric(float64(commonMatched), "common-matched")
		})
		run("one_rare_term", func() error {
			rows, err := store.db.QueryContext(ctx,
				`SELECT rowid, bm25(mem_v2_chunk_fts) FROM mem_v2_chunk_fts
				WHERE mem_v2_chunk_fts MATCH ?
				ORDER BY bm25(mem_v2_chunk_fts) LIMIT ?`, rareMatch, maxSearchCandidates)
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
		run("one_common_term", func() error {
			rows, err := store.db.QueryContext(ctx,
				`SELECT rowid, bm25(mem_v2_chunk_fts) FROM mem_v2_chunk_fts
				WHERE mem_v2_chunk_fts MATCH ?
				ORDER BY bm25(mem_v2_chunk_fts) LIMIT ?`, commonMatch, maxSearchCandidates)
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

		// The query shape is the largest lever available, and it is the one the
		// current code does not use. buildMatchQuery joins terms with OR, so a
		// three-term query matches every chunk containing any one of the three.
		// Requiring all terms instead is a different recall trade-off, not a
		// free win, but the size of the difference decides whether it is worth
		// designing a two-phase search around: try AND, fall back to OR when
		// AND returns too few hits.
		//
		// The selectivity of each shape is reported next to its latency, because
		// a faster query that matches nothing is not an improvement.
		andMatch := buildAndMatchQuery(searchTerms(queries[0]))
		var andMatched int64
		if err := store.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM mem_v2_chunk_fts WHERE mem_v2_chunk_fts MATCH ?`,
			andMatch).Scan(&andMatched); err != nil {
			b.Fatal(err)
		}
		b.Run("and_selectivity", func(b *testing.B) {
			b.ReportMetric(float64(andMatched), "matched-chunks")
			if totalChunks > 0 {
				b.ReportMetric(float64(andMatched)*100/float64(totalChunks), "matched-pct")
			}
			if matchedChunks > 0 {
				b.ReportMetric(float64(andMatched)*100/float64(matchedChunks), "pct-of-or")
			}
		})
		run("and_bm25_limited", func() error {
			rows, err := store.db.QueryContext(ctx,
				`SELECT rowid, bm25(mem_v2_chunk_fts) FROM mem_v2_chunk_fts
				WHERE mem_v2_chunk_fts MATCH ?
				ORDER BY bm25(mem_v2_chunk_fts) LIMIT ?`, andMatch, maxSearchCandidates)
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

		// The joins with no MATCH in front of them, over the same number of
		// rows the candidate limit allows. This is the join cost the breakdown
		// attributed 233 ms to, isolated.
		run("join_only", func() error {
			rows, err := store.db.QueryContext(ctx, joinOnly, joinArgs...)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var messageID, sessionID, project string
				if err := rows.Scan(&messageID, &sessionID, &project); err != nil {
					return err
				}
			}
			return rows.Err()
		})

		run("inner_rank", func() error {
			rows, err := store.db.QueryContext(ctx, inner, match, principal, maxSearchCandidates)
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

		// The same inner query with the project predicate added. The fixture
		// splits messages across two projects, so this measures whether pushing
		// scope into the index would pay for itself.
		run("inner_rank_project", func() error {
			rows, err := store.db.QueryContext(ctx, innerProject,
				match, principal, "gateway", maxSearchCandidates)
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

		// The trigram full path, with the same three terms as full_search.
		//
		// full_search cannot serve as the trigram column: queries[0] is three
		// plain ASCII words, so Search routes it to the word index, and
		// full_search and word_full_search are the same call under two names.
		// An earlier version of the plan's comparison table read full_search as
		// the trigram row and concluded the word path carried ~90 ms of
		// post-inner-query overhead; that was arithmetic on a mislabelled row.
		//
		// Calling searchFTS directly is the honest way to get the trigram
		// number. It is the production trigram path — same query, same joins,
		// same excerpt handling — with only the routing decision bypassed, which
		// is exactly the variable being compared. The terms are identical to
		// full_search's, so the two rows differ in the index and nothing else.
		trigramTerms := searchTerms(queries[0])
		run("trigram_full_search", func() error {
			hits, err := store.searchFTS(ctx, principal, scope, trigramTerms, trigramTerms)
			if err != nil {
				return err
			}
			// A row that returns nothing is fast and meaningless. The word
			// row asserts its mode for the same reason: a benchmark has to
			// prove it measured the operation it is named after.
			if len(hits) == 0 {
				return fmt.Errorf("trigram search returned no hits for %q", queries[0])
			}
			return nil
		})

		// The rows above all read the trigram index. The word index is the
		// redesign that was supposed to reach the target, and it was measured
		// at 100k on the production path at 463.0 ms p95 — 4.6x the target and
		// 12.5x the ~37 ms projected from the tokenizer comparison. The
		// projection was wrong because the tokenizer comparison measured a
		// benchmark-local table with a hand-built query, not this query. These
		// rows exist so the same decomposition that was applied to the trigram
		// index is applied to the word index, on the same archive, in the same
		// process, with the same warm-up.
		//
		// The term count is held fixed across the two indexes on purpose. The
		// per-mode benchmark compares a three-term word query against a
		// two-term trigram query and a one-plus-one hybrid query, so its
		// numbers cannot be read as a comparison of the indexes: cost scales
		// with term count, and the modes do not have the same term count. Every
		// row below uses the same three terms as `match`.
		wordMatch := buildMatchQuery(searchTerms(queries[0]))
		var wordMatched int64
		if err := store.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM mem_v2_chunk_word_fts WHERE mem_v2_chunk_word_fts MATCH ?`,
			wordMatch).Scan(&wordMatched); err != nil {
			b.Fatal(err)
		}
		b.Run("word_selectivity", func(b *testing.B) {
			b.ReportMetric(float64(wordMatched), "matched-chunks")
			if totalChunks > 0 {
				b.ReportMetric(float64(wordMatched)*100/float64(totalChunks), "matched-pct")
			}
			// The two indexes must match nearly the same rows or the latency
			// comparison is measuring selectivity instead of cost.
			if matchedChunks > 0 {
				b.ReportMetric(float64(wordMatched)*100/float64(matchedChunks), "pct-of-trigram")
			}
		})

		wordInner := `SELECT mem_v2_chunk_word_fts.rowid, bm25(mem_v2_chunk_word_fts)
			FROM mem_v2_chunk_word_fts
			CROSS JOIN mem_v2_chunks c ON c.id=mem_v2_chunk_word_fts.rowid
			CROSS JOIN mem_v2_messages m ON m.id=c.message_id
			CROSS JOIN mem_v2_sessions s ON s.id=m.session_id
			WHERE mem_v2_chunk_word_fts MATCH ? AND m.principal_id=?
			ORDER BY bm25(mem_v2_chunk_word_fts) LIMIT ?`

		run("word_ids_limit_100", func() error {
			rows, err := store.db.QueryContext(ctx,
				`SELECT rowid FROM mem_v2_chunk_word_fts
				WHERE mem_v2_chunk_word_fts MATCH ? LIMIT ?`,
				wordMatch, maxSearchCandidates)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var id int64
				if err := rows.Scan(&id); err != nil {
					return err
				}
			}
			return rows.Err()
		})
		run("word_bm25_limit_100", func() error {
			rows, err := store.db.QueryContext(ctx,
				`SELECT rowid, bm25(mem_v2_chunk_word_fts) FROM mem_v2_chunk_word_fts
				WHERE mem_v2_chunk_word_fts MATCH ?
				ORDER BY bm25(mem_v2_chunk_word_fts) LIMIT ?`,
				wordMatch, maxSearchCandidates)
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
		run("word_inner_rank", func() error {
			rows, err := store.db.QueryContext(ctx, wordInner, wordMatch, principal, maxSearchCandidates)
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
		// The production word path, end to end, so the rows above can be
		// checked against the number the gate actually reports.
		run("word_full_search", func() error {
			result, err := store.Search(ctx, SearchOptions{Scope: scope, Query: queries[0], Limit: 10})
			if err != nil {
				return err
			}
			if result.Filters.Mode != "word" {
				return fmt.Errorf("query routed to %q, expected word", result.Filters.Mode)
			}
			return nil
		})

		// Term count is the variable the per-mode benchmark confounds, so it is
		// measured directly on the word index: one term, two terms, three terms,
		// all from the same query. If cost is linear in term count here as it is
		// on the trigram index, then the per-mode numbers are explained by their
		// term counts and not by the indexes.
		for _, termCount := range []int{1, 2, 3} {
			termCount := termCount
			terms := searchTerms(queries[0])[:termCount]
			termMatch := buildMatchQuery(terms)
			run(fmt.Sprintf("word_terms_%d", termCount), func() error {
				rows, err := store.db.QueryContext(ctx,
					`SELECT rowid, bm25(mem_v2_chunk_word_fts) FROM mem_v2_chunk_word_fts
					WHERE mem_v2_chunk_word_fts MATCH ?
					ORDER BY bm25(mem_v2_chunk_word_fts) LIMIT ?`,
					termMatch, maxSearchCandidates)
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
		}
	}
}

// sampleChunkIDs returns up to limit chunk ids from the archive. The join-only
// row needs real ids so the row count and page layout match what a search
// actually joins.
func sampleChunkIDs(ctx context.Context, store *Store, limit int) ([]int64, error) {
	rows, err := store.db.QueryContext(ctx,
		`SELECT id FROM mem_v2_chunks ORDER BY id LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := make([]int64, 0, limit)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("memstore: benchmark archive has no chunks")
	}
	return ids, nil
}

// buildAndMatchQuery is the AND counterpart of buildMatchQuery, used only to
// measure what requiring every term would cost. It is not production code: the
// production query is an OR because a user searching three words usually wants
// any of them, and switching to AND would silently drop results. The benchmark
// measures it so the recall trade-off can be priced instead of guessed.
func buildAndMatchQuery(terms []string) string {
	quoted := make([]string, 0, len(terms))
	for _, term := range terms {
		quoted = append(quoted, `"`+strings.ReplaceAll(term, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, " AND ")
}
