package memstore

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkIndexStatus isolates the cost of the coverage query that Search runs
// on every request. Search reports Partial coverage from this status, so its
// cost is paid on the read path even though it is diagnostic information.
func BenchmarkIndexStatus(b *testing.B) {
	count := benchMessageCount(b)
	b.Run(fmt.Sprintf("messages=%d", count), func(b *testing.B) {
		path := seedBenchArchive(b, count)
		store, err := OpenReadOnly(path)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { store.Close() })

		samples := make([]time.Duration, 0, b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := time.Now()
			status, err := store.IndexStatus(context.Background())
			if err != nil {
				b.Fatalf("index status: %v", err)
			}
			samples = append(samples, time.Since(start))
			if status.PendingJobs != 0 {
				b.Fatalf("fixture still has %d pending jobs", status.PendingJobs)
			}
		}
		b.StopTimer()
		reportLatencies(b, summarizeLatencies(samples))
	})
}
