package memstore

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkIndexStatusParts times each subquery of IndexStatus separately.
//
// IndexStatus runs on every Search to decide whether coverage is partial, so
// its cost is added to every search latency. The whole-query benchmark shows
// the total but not which term dominates, and the terms have very different
// asymptotics: counting pending jobs reads a bounded index range, while
// counting chunks and distinct indexed messages scan tables that grow with the
// archive. Measuring them apart is what makes the optimization decision
// evidence-based instead of a guess.
func BenchmarkIndexStatusParts(b *testing.B) {
	count := benchMessageCount(b)
	b.Run(fmt.Sprintf("messages=%d", count), func(b *testing.B) {
		path := seedBenchArchive(b, count)
		store, err := OpenReadOnly(path)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { store.Close() })

		parts := []struct {
			name  string
			query string
			// columns is how many values the query returns. Scanning with the
			// wrong count fails, and retrying inside the timed loop would
			// measure two executions instead of one.
			columns int
		}{
			{"pending_jobs", `SELECT COUNT(*) FROM mem_v2_jobs WHERE kind='index' AND status='pending'`, 1},
			{"failed_jobs", `SELECT COUNT(*) FROM mem_v2_jobs WHERE kind='index' AND status='failed'`, 1},
			{"indexed_messages", `SELECT COUNT(DISTINCT message_id) FROM mem_v2_chunks`, 1},
			{"total_messages", `SELECT COUNT(*) FROM mem_v2_messages`, 1},
			{"chunks", `SELECT COUNT(*) FROM mem_v2_chunks`, 1},
			{"index_state", `SELECT tokenizer_version, updated_at FROM mem_v2_index_state WHERE id=1`, 2},
		}
		for _, part := range parts {
			b.Run(part.name, func(b *testing.B) {
				samples := make([]time.Duration, 0, b.N)
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					start := time.Now()
					var first, second int64
					dest := []any{&first}
					if part.columns == 2 {
						dest = []any{&first, &second}
					}
					if err := store.db.QueryRowContext(context.Background(), part.query).Scan(dest...); err != nil {
						b.Fatalf("%s: %v", part.name, err)
					}
					samples = append(samples, time.Since(start))
				}
				b.StopTimer()
				reportLatencies(b, summarizeLatencies(samples))
			})
		}
	})
}
