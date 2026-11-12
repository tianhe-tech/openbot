package memstore

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkSearchExcerptReconstruction measures the one piece of new work the
// contentless migration added to the read path, and the one ordering decision
// that keeps it cheap.
//
// Before schema v7, searchIndex selected ft.content — the chunk text, about a
// kilobyte — straight out of the FTS index for each candidate row. A contentless
// index has no such column, so the excerpt is now reconstructed from the message
// block: join mem_v2_blocks, decode it (it may be zstd), then slice out
// [start_byte, end_byte). That is a strictly larger read per candidate.
//
// The migration was adopted on a measurement of that cost — 1.13x at 2k messages
// and indistinguishable at 20k — taken by the benchmark this file replaces. That
// benchmark compared the old path against the new one, and the old path no
// longer exists, so it could not be kept. What can still be measured is the
// ordering inside the new path, which is where the remaining risk is.
//
// A message with several matching chunks appears once per chunk in the candidate
// list. Decoding before collapsing therefore decodes the same block repeatedly.
// The two rows below differ in exactly that and nothing else: the same inner
// ranking query, the same outer projection, the same candidate set. Row A
// decodes every candidate and then collapses; row B collapses and then decodes.
// The difference is the waste, and it is the number that justifies the ordering.
//
// Both rows assert they produce identical excerpts. A reconstruction that is
// faster because it returns different text is not a cheaper read path, it is a
// different one, and the assertion is what stops the benchmark from reporting a
// saving that is really a bug.
func BenchmarkSearchExcerptReconstruction(b *testing.B) {
	count := benchMessageCount(b)
	b.Run(fmt.Sprintf("messages=%d", count), func(b *testing.B) {
		path := seedBenchArchive(b, count)
		store, err := OpenReadOnly(path)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { store.Close() })
		ctx := context.Background()

		scope := SearchScope{Adapter: "bench", UserID: "owner"}
		principal := archiveID("principal", "bench", "owner")
		query := benchWord(11) + " " + benchWord(22) + " " + benchWord(33)
		match := buildMatchQuery(searchTerms(query))

		// The query is copied from searchIndex rather than simplified. A
		// hand-built approximation measures the approximation: several wrong
		// numbers in this package came from a benchmark-local query that had
		// drifted from the production one.
		querySQL := `SELECT c.message_id,c.start_byte,c.end_byte,
				m.session_id,m.turn_id,m.role,m.created_at,m.seq,s.project_key,s.work_dir,
				f.rank, b.codec,b.codec_version,b.raw_bytes,b.payload
			FROM (
				SELECT mem_v2_chunk_fts.rowid AS chunk_id, bm25(mem_v2_chunk_fts) AS rank
				FROM mem_v2_chunk_fts
				CROSS JOIN mem_v2_chunks c ON c.id=mem_v2_chunk_fts.rowid
				CROSS JOIN mem_v2_messages m ON m.id=c.message_id
				CROSS JOIN mem_v2_sessions s ON s.id=m.session_id
				WHERE mem_v2_chunk_fts MATCH ?
					AND m.principal_id=?
					AND (?='' OR s.project_key=?)
					AND (?=0 OR m.created_at>=?)
					AND (?=0 OR m.created_at<=?)
				ORDER BY bm25(mem_v2_chunk_fts)
				LIMIT ?
			) AS f
			JOIN mem_v2_chunks c ON c.id=f.chunk_id
			JOIN mem_v2_messages m ON m.id=c.message_id
			JOIN mem_v2_sessions s ON s.id=m.session_id
			JOIN mem_v2_blocks b ON b.id=m.block_id
			ORDER BY f.rank`
		args := []any{
			match, principal,
			scope.Project, scope.Project,
			scope.Since, scope.Since,
			scope.Until, scope.Until,
			maxSearchCandidates,
		}

		type candidate struct {
			messageID string
			start     int
			end       int
			rank      float64
			codec     string
			version   int
			rawBytes  int64
			payload   []byte
		}

		// fetch runs the query and returns the raw candidates. Both rows share
		// it, so the ranking work cancels and only the decode ordering differs.
		fetch := func() ([]candidate, error) {
			rows, err := store.db.QueryContext(ctx, querySQL, args...)
			if err != nil {
				return nil, err
			}
			defer rows.Close()
			var out []candidate
			for rows.Next() {
				var c candidate
				var sessionID, turnID, role, project, workDir string
				var createdAt int64
				var seq int
				if err := rows.Scan(&c.messageID, &c.start, &c.end,
					&sessionID, &turnID, &role, &createdAt, &seq, &project, &workDir,
					&c.rank, &c.codec, &c.version, &c.rawBytes, &c.payload); err != nil {
					return nil, err
				}
				out = append(out, c)
			}
			return out, rows.Err()
		}

		excerptOf := func(c candidate) (string, error) {
			decoded, err := decodeBlock(c.codec, c.version, c.rawBytes, c.payload)
			if err != nil {
				return "", fmt.Errorf("decode %s: %w", c.messageID, err)
			}
			text := string(decoded)
			if c.start < 0 || c.end > len(text) || c.start > c.end {
				return "", fmt.Errorf("chunk range [%d,%d) outside a %d-byte message %s",
					c.start, c.end, len(text), c.messageID)
			}
			return truncateExcerpt(text[c.start:c.end]), nil
		}

		// decodeThenCollapse is the ordering the code does not use: every
		// candidate is decoded, then the best per message is kept.
		decodeThenCollapse := func() ([]string, error) {
			candidates, err := fetch()
			if err != nil {
				return nil, err
			}
			type decoded struct {
				excerpt string
				rank    float64
			}
			best := make(map[string]decoded, len(candidates))
			for _, c := range candidates {
				excerpt, err := excerptOf(c)
				if err != nil {
					return nil, err
				}
				if existing, ok := best[c.messageID]; !ok || c.rank < existing.rank {
					best[c.messageID] = decoded{excerpt, c.rank}
				}
			}
			out := make([]string, 0, len(best))
			for _, d := range best {
				out = append(out, d.excerpt)
			}
			return out, nil
		}

		// collapseThenDecode is the production ordering.
		collapseThenDecode := func() ([]string, error) {
			candidates, err := fetch()
			if err != nil {
				return nil, err
			}
			best := make(map[string]int, len(candidates))
			for i, c := range candidates {
				if existing, ok := best[c.messageID]; !ok || c.rank < candidates[existing].rank {
					best[c.messageID] = i
				}
			}
			out := make([]string, 0, len(best))
			for _, index := range best {
				excerpt, err := excerptOf(candidates[index])
				if err != nil {
					return nil, err
				}
				out = append(out, excerpt)
			}
			return out, nil
		}

		// The two rows must agree before either is timed. The comparison is on
		// the multiset of excerpts, because both rows iterate a map and the
		// order is not part of the answer.
		want, err := decodeThenCollapse()
		if err != nil {
			b.Fatalf("decode-then-collapse: %v", err)
		}
		got, err := collapseThenDecode()
		if err != nil {
			b.Fatalf("collapse-then-decode: %v", err)
		}
		if len(want) == 0 {
			b.Fatalf("query %q matched no chunks; the fixture is not exercising the index", query)
		}
		if len(want) != len(got) {
			b.Fatalf("hit counts differ: decode-first %d, collapse-first %d", len(want), len(got))
		}
		counts := make(map[string]int, len(want))
		for _, excerpt := range want {
			counts[excerpt]++
		}
		for _, excerpt := range got {
			counts[excerpt]--
		}
		for excerpt, delta := range counts {
			if delta != 0 {
				b.Fatalf("excerpt sets differ by %d for %q", delta, excerpt)
			}
		}

		// How many candidates share a message. This ratio is the size of the
		// waste the ordering avoids, and a fixture where every candidate is a
		// distinct message would hide it entirely.
		candidates, err := fetch()
		if err != nil {
			b.Fatal(err)
		}
		distinct := make(map[string]struct{}, len(candidates))
		for _, c := range candidates {
			distinct[c.messageID] = struct{}{}
		}
		b.Logf("%d candidates over %d messages (%.2f candidates per message), %d excerpts identical",
			len(candidates), len(distinct),
			float64(len(candidates))/float64(len(distinct)), len(want))

		// The clock on this machine advances in ~1 ms steps, so a row that
		// costs less than that reports 0 or 1 ms and nothing between. Both
		// rows are timed in batches sized to clear the quantum, and the
		// per-operation cost is the sample divided by the batch.
		quantum := clockQuantum()
		measure := func(name string, fn func() ([]string, error)) {
			b.Run(name, func(b *testing.B) {
				if _, err := fn(); err != nil {
					b.Fatal(err)
				}
				target := 8 * quantum
				batch := 1
				for {
					start := time.Now()
					for i := 0; i < batch; i++ {
						if _, err := fn(); err != nil {
							b.Fatal(err)
						}
					}
					elapsed := time.Since(start)
					if elapsed >= target || batch >= 4096 {
						break
					}
					if elapsed > 0 {
						if estimate := int(float64(batch) * float64(target) / float64(elapsed)); estimate > batch {
							batch = estimate
							continue
						}
					}
					batch *= 2
				}
				samples := make([]time.Duration, 0, b.N)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					start := time.Now()
					for j := 0; j < batch; j++ {
						if _, err := fn(); err != nil {
							b.Fatal(err)
						}
					}
					samples = append(samples, time.Since(start)/time.Duration(batch))
				}
				b.StopTimer()
				reportLatencies(b, summarizeLatencies(samples))
				b.ReportMetric(float64(batch), "batch")
				b.ReportMetric(float64(quantum.Nanoseconds())/1e6, "clock-ms")
			})
		}

		measure("decode_then_collapse", decodeThenCollapse)
		measure("collapse_then_decode", collapseThenDecode)
	})
}

// truncateExcerpt applies the same bound searchIndex applies, so the rows
// compare the excerpt the caller actually receives rather than the raw chunk.
func truncateExcerpt(text string) string {
	if len(text) > maxExcerptBytes {
		return text[:alignRuneStart(text, maxExcerptBytes)]
	}
	return text
}
