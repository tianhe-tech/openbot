package memstore

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestIndexBatchScalesWithArchiveSize measures how long one full index batch
// takes as the archive grows, and reports the slowest batch seen.
//
// This exists because the index worker failed with "context deadline exceeded"
// while seeding a 20k-message benchmark archive, which means a batch can exceed
// the deadline the worker gives it. The first attempt at this measurement seeded
// messages one at a time and drained between sizes, which was both slow and
// misleading: it measured a batch on a quiet queue, while the real failure
// happens while draining a large backlog. This version seeds in parallel (the
// same way the benchmark fixture does) and then times every batch of the drain,
// so the reported worst case is the one the worker actually has to survive.
func TestIndexBatchScalesWithArchiveSize(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping index scaling measurement in short mode")
	}
	ctx := context.Background()

	// The size that broke the deadline. Smaller sizes are covered by
	// TestIndexBatchFitsCommitTimeout, which is the fast regression guard.
	//
	// The size is overridable because the deadline's whole claim is that it
	// scales with the archive, and a claim about scaling that is only ever
	// checked at one size is not checked. At 20k the drain takes about 2.5
	// minutes; at 100k it takes long enough that the default `go test` timeout
	// kills it, so a larger run needs an explicit -timeout.
	target := 20000
	if raw := os.Getenv("MEMSTORE_INDEX_SCALING_MESSAGES"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			t.Fatalf("MEMSTORE_INDEX_SCALING_MESSAGES must be a positive integer, got %q", raw)
		}
		target = parsed
	}
	_, writer := archiveForTest(t, WriterOptions{})
	seedMessages(t, writer, target)

	status, err := writer.store.IndexStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("seeded archive=%d messages chunks=%d pending=%d",
		status.TotalMessages, status.Chunks, status.PendingJobs)

	// The deadline is recomputed for every batch because it grows with the
	// archive, and the batch that matters is the one that ran against the
	// largest archive. Comparing the worst batch against a single budget taken
	// before the drain would compare it against the loosest deadline the run
	// ever used, which is the wrong direction.
	var (
		batches   int
		worst     time.Duration
		worstAt   int
		worstBudg time.Duration
		indexed   int
		totalDur  time.Duration
	)
	for {
		chunks, err := writer.archiveChunkCount(ctx)
		if err != nil {
			t.Fatal(err)
		}
		budget := indexCommitTimeout(maxIndexBatch, chunks)
		start := time.Now()
		processed, err := writer.IndexPending(ctx, maxIndexBatch)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("batch %d failed after %s (worst so far %s at batch %d, budget %s): %v",
				batches, elapsed.Round(time.Millisecond), worst.Round(time.Millisecond), worstAt, budget, err)
		}
		if processed == 0 {
			break
		}
		batches++
		indexed += processed
		totalDur += elapsed
		if elapsed > worst {
			worst, worstAt, worstBudg = elapsed, batches, budget
		}
		if batches%20 == 0 {
			t.Logf("batch %3d: %s (worst %s)", batches, elapsed.Round(time.Millisecond), worst.Round(time.Millisecond))
		}
	}
	t.Logf("drained %d messages in %d batches, total %s, mean %s",
		indexed, batches, totalDur.Round(time.Millisecond), (totalDur / time.Duration(batches)).Round(time.Millisecond))
	t.Logf("worst batch %s at batch %d against the %s deadline it ran under",
		worst.Round(time.Millisecond), worstAt, worstBudg.Round(time.Millisecond))
	if worst > worstBudg {
		t.Fatalf("a full batch took %s at %d messages but the deadline was only %s; "+
			"the index worker would fail and leave the archive under-indexed",
			worst.Round(time.Millisecond), status.TotalMessages, worstBudg.Round(time.Millisecond))
	}
}

// seedMessages records count messages using a worker pool, so the writer batches
// them the way concurrent capture does. A sequential loop would pay a full
// commit per message and make a 20k seed take minutes.
func seedMessages(t *testing.T, writer *ArchiveWriter, count int) {
	t.Helper()
	ctx := context.Background()
	const workers = 32
	var (
		next    atomic.Int64
		group   sync.WaitGroup
		errMu   sync.Mutex
		seedErr error
	)
	record := func(err error) {
		errMu.Lock()
		if seedErr == nil {
			seedErr = err
		}
		errMu.Unlock()
	}
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= count {
					return
				}
				key := fmt.Sprintf("scale-%06d", i)
				message := benchMessage(key, "gateway", benchBody(i), 1700000000000+int64(i)*1000)
				if err := writer.RecordMessage(ctx, message); err != nil {
					record(fmt.Errorf("record %s: %w", key, err))
					return
				}
			}
		}()
	}
	group.Wait()
	if seedErr != nil {
		t.Fatal(seedErr)
	}
}
