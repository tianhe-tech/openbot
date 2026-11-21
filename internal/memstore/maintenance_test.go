package memstore

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestCheckpointTruncatesTheWAL is the gate for the maintenance policy: without
// it the -wal file grows until a checkpoint copies its pages back, and a
// continuously busy writer can starve SQLite's own autocheckpoint.
func TestCheckpointTruncatesTheWAL(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{BatchItems: 1})
	ctx := context.Background()
	// Write enough to make the WAL visibly non-empty. Each message is a few
	// hundred bytes, so this is a few hundred KiB of WAL.
	for index := 0; index < 200; index++ {
		message := archiveMessageForTest(fmt.Sprintf("wal-%d", index))
		message.Sequence = int64(index)
		if err := writer.RecordMessage(ctx, message); err != nil {
			t.Fatal(err)
		}
	}
	before, err := writer.walBytes()
	if err != nil {
		t.Fatal(err)
	}
	if before == 0 {
		t.Fatal("WAL is empty after 200 writes; the fixture cannot test a checkpoint")
	}
	report, err := writer.Checkpoint(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Checkpointed {
		t.Fatal("checkpoint skipped with a zero threshold")
	}
	if report.PagesRemaining != 0 {
		t.Fatalf("checkpoint left %d pages in the WAL", report.PagesRemaining)
	}
	if report.WALBytesAfter >= before {
		t.Fatalf("WAL did not shrink: %d -> %d", before, report.WALBytesAfter)
	}
}

// TestCheckpointSkipsASmallWAL pins the threshold behaviour: a pass below the
// threshold must not touch the file, because checkpointing a small WAL only
// adds I/O.
func TestCheckpointSkipsASmallWAL(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	if err := writer.RecordMessage(ctx, archiveMessageForTest("small")); err != nil {
		t.Fatal(err)
	}
	report, err := writer.Checkpoint(ctx, 1<<30)
	if err != nil {
		t.Fatal(err)
	}
	if report.Checkpointed {
		t.Fatal("checkpoint ran below the threshold")
	}
	if report.WALBytesBefore != report.WALBytesAfter {
		t.Fatalf("skipped checkpoint changed the WAL: %d -> %d", report.WALBytesBefore, report.WALBytesAfter)
	}
}

// TestCheckpointPreservesData is the safety property that matters: a
// truncating checkpoint must move pages into the main database, not discard
// them. A checkpoint that lost data would still report success.
func TestCheckpointPreservesData(t *testing.T) {
	path, writer := archiveForTest(t, WriterOptions{BatchItems: 1})
	ctx := context.Background()
	for index := 0; index < 50; index++ {
		message := archiveMessageForTest(fmt.Sprintf("durable-%d", index))
		message.Sequence = int64(index)
		if err := writer.RecordMessage(ctx, message); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := writer.Checkpoint(ctx, 0); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := writer.store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_messages`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 50 {
		t.Fatalf("checkpoint lost rows: %d of 50", count)
	}
	// Reopen from disk to prove the pages reached the main file rather than
	// living only in a WAL that the same connection can still read.
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_messages`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 50 {
		t.Fatalf("reopened archive has %d of 50 rows", count)
	}
}

// TestMaintenanceLoopStopsOnCancel proves the loop is cancellable. A
// maintenance goroutine that outlives shutdown would keep a database handle
// open after the writer closed it.
func TestMaintenanceLoopStopsOnCancel(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		writer.RunMaintenance(ctx, MaintenanceOptions{Interval: time.Millisecond, CheckpointBytes: 1 << 30})
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("maintenance loop ignored cancellation")
	}
}
