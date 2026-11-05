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

// TestCaptureCommitScalesWithArchiveSize measures how long a capture commit
// takes as the archive grows, and reports the worst one in each size band.
//
// This exists because seeding a 100k-message archive failed at message 34,561
// with "record scale-034561: context deadline exceeded". That failure was in
// RecordMessage, not in IndexPending: capture itself timed out. Capture is the
// one path that must not fail, because a failed capture is a lost message, and
// Phase 0 of the plan exists specifically to stop losing messages.
//
// The mechanism is that RecordMessage uses the writer's default commit timeout,
// which is a fixed 2 seconds chosen to bound how long a caller waits for an
// acknowledgment. Unlike the index deadline, it does not scale with anything.
// If a capture batch's cost grows with the archive, then a fixed deadline is
// wrong for the same reason the index deadline was wrong, and the failure is
// worse: an index batch that times out leaves the archive under-indexed and
// retryable, while a capture batch that times out loses the message.
//
// The measurement is banded rather than continuous because the question is not
// "what is the mean" but "at what archive size does the worst batch cross the
// deadline". A mean would hide exactly the tail that causes the failure.
func TestCaptureCommitScalesWithArchiveSize(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping capture scaling measurement in short mode")
	}
	// 20k is enough to expose a superlinear term: the measured curve rises from
	// 80 ms at 2k to 156 ms at 20k, so a term that grew with the archive would
	// be visible well before the deadline. The default is kept at 20k because
	// this runs in the normal suite and 40k doubles the cost for no extra
	// discrimination; raise it with the environment variable when investigating.
	total := 20000
	if raw := os.Getenv("MEMSTORE_CAPTURE_SCALING_MESSAGES"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			t.Fatalf("MEMSTORE_CAPTURE_SCALING_MESSAGES must be a positive integer, got %q", raw)
		}
		total = parsed
	}
	const band = 2000

	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	// The timeout capture actually runs under. Read it from the resolved
	// options rather than hardcoding 2s, so this test keeps measuring the real
	// deadline if the default changes.
	resolved, err := WriterOptions{}.resolved()
	if err != nil {
		t.Fatal(err)
	}
	captureTimeout := resolved.CommitTimeout

	var (
		worstOverall time.Duration
		worstAt      int
		next         atomic.Int64
	)
	for start := 0; start < total; start += band {
		end := start + band
		if end > total {
			end = total
		}
		// Seed this band with a worker pool, the way concurrent capture does,
		// and time every RecordMessage call. The pool is what makes the writer
		// batch, and a batch is what the deadline applies to.
		//
		// The timed span is the whole call, which includes waiting in the queue
		// behind other workers, while the deadline governs only the commit that
		// follows. That makes every figure here an upper bound on the commit
		// cost, which is the safe direction for this test: if the whole call
		// stays well under the deadline, the commit certainly does. It also
		// means a figure close to the deadline would need decomposing before it
		// could be blamed on the commit.
		var (
			group   sync.WaitGroup
			mu      sync.Mutex
			bandMax time.Duration
			bandErr error
		)
		next.Store(int64(start))
		const workers = 32
		for worker := 0; worker < workers; worker++ {
			group.Add(1)
			go func() {
				defer group.Done()
				for {
					i := int(next.Add(1) - 1)
					if i >= end {
						return
					}
					key := fmt.Sprintf("capture-%06d", i)
					message := benchMessage(key, "gateway", benchBody(i), 1700000000000+int64(i)*1000)
					begin := time.Now()
					err := writer.RecordMessage(ctx, message)
					elapsed := time.Since(begin)
					mu.Lock()
					if elapsed > bandMax {
						bandMax = elapsed
					}
					if err != nil && bandErr == nil {
						bandErr = fmt.Errorf("record %s at archive size %d: %w", key, i, err)
					}
					mu.Unlock()
					if err != nil {
						return
					}
				}
			}()
		}
		group.Wait()
		if bandErr != nil {
			t.Fatalf("%v (capture deadline is %s; worst commit before this band was %s at %d messages)",
				bandErr, captureTimeout, worstOverall.Round(time.Millisecond), worstAt)
		}
		if bandMax > worstOverall {
			worstOverall, worstAt = bandMax, end
		}
		t.Logf("archive %6d messages: worst capture commit %s (%.0f%% of the %s deadline)",
			end, bandMax.Round(time.Millisecond),
			100*float64(bandMax)/float64(captureTimeout), captureTimeout)
	}
	t.Logf("worst capture commit overall %s at %d messages, against a %s deadline",
		worstOverall.Round(time.Millisecond), worstAt, captureTimeout)
	if worstOverall > captureTimeout {
		t.Fatalf("a capture commit took %s at %d messages but the deadline is %s; "+
			"capture would fail and the message would be lost",
			worstOverall.Round(time.Millisecond), worstAt, captureTimeout)
	}
}
