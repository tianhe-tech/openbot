package memstore

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// TestIndexBatchFitsCommitTimeout checks that a full-size index batch of
// realistic 4 KiB messages completes inside the deadline the indexer actually
// uses, including when the archive is already large. IndexPending leases and
// writes its whole batch in a single transaction, so a batch that cannot commit
// in time fails permanently rather than making partial progress.
//
// This is a regression guard: the indexer originally inherited the writer's
// 2-second user-facing commit timeout, which a full batch already exceeded at a
// few thousand messages, so the background indexer could never catch up on a
// real archive.
func TestIndexBatchFitsCommitTimeout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MigrateArchive(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.Close()

	writer, err := OpenArchiveWriter(context.Background(), path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(context.Background())

	ctx := context.Background()
	// Enough messages that the FTS index is well past empty when the last
	// batches run; a fresh-archive measurement hides the growth.
	const total = 1200
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("batch-%06d", i)
		if err := writer.RecordMessage(ctx, benchMessage(key, "gateway", benchBody(i), 1700000000000)); err != nil {
			t.Fatalf("record: %v", err)
		}
	}
	budget := indexCommitTimeout(maxIndexBatch, 0)
	var worst time.Duration
	for batch := 0; ; batch++ {
		start := time.Now()
		indexed, err := writer.IndexPending(ctx, maxIndexBatch)
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("batch %d failed after %s: %v", batch, elapsed, err)
		}
		if indexed == 0 {
			break
		}
		if elapsed > worst {
			worst = elapsed
		}
		t.Logf("batch %d: %d messages in %s", batch, indexed, elapsed)
	}
	// The deadline the indexer actually used for the last batch, which is the
	// one that had the whole archive behind it.
	chunks, err := writer.archiveChunkCount(ctx)
	if err != nil {
		t.Fatal(err)
	}
	budget = indexCommitTimeout(maxIndexBatch, chunks)
	t.Logf("worst batch %s against a %s index deadline (%d chunks)", worst, budget, chunks)
	if worst > budget {
		t.Fatalf("an index batch took %s, exceeding the %s index deadline", worst, budget)
	}
}

// TestIndexCommitTimeoutGrowsWithArchiveSize pins the shape of the index
// deadline rather than a particular number. The deadline has to grow with the
// archive because an FTS5 insert costs more as the index grows; a fixed deadline
// is what made seeding a 100k archive fail while the same code passed at 20k.
func TestIndexCommitTimeoutGrowsWithArchiveSize(t *testing.T) {
	small := indexCommitTimeout(maxIndexBatch, 0)
	medium := indexCommitTimeout(maxIndexBatch, 200_000)
	large := indexCommitTimeout(maxIndexBatch, 1_000_000)
	if !(small < medium && medium < large) {
		t.Fatalf("deadline does not grow with archive size: %s, %s, %s", small, medium, large)
	}
	// The floor must still be generous enough that a small archive is never
	// failed by a slow machine rather than by a wedged commit.
	if small < 60*time.Second {
		t.Fatalf("floor is %s, expected at least 60s", small)
	}
	// A larger batch must get a larger deadline than a smaller one at the same
	// archive size, because it does proportionally more work.
	if indexCommitTimeout(50, 200_000) >= indexCommitTimeout(maxIndexBatch, 200_000) {
		t.Fatal("deadline does not grow with batch size")
	}
}
