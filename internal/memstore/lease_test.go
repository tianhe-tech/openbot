package memstore

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLeaseWriterCloseReleasesBeforeDatabase(t *testing.T) {
	path, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	lease, err := writer.AcquireLease(ctx, LeaseOptions{Holder: "closing"})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	next, err := OpenArchiveWriter(ctx, path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close(ctx)
	if _, err := next.AcquireLease(ctx, LeaseOptions{Holder: "next"}); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseLostWriterCannotCommit(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	if _, err := writer.AcquireLease(ctx, LeaseOptions{Holder: "old", Heartbeat: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.store.db.Exec(`UPDATE mem_v2_instance_lease SET holder='replacement' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if err := writer.Flush(ctx); !errors.Is(err, ErrArchiveLeased) {
		t.Fatalf("unfenced write: %v", err)
	}
}

// TestLeaseRejectsASecondHolder is the gate for the plan's requirement that a
// second gateway targeting the same store is rejected rather than silently
// allowed. Two writers on one archive is exactly what
// TestArchiveConcurrentWritersAndIdentityIsolation does on purpose, so the
// lease has to be opt-in: this test is the opt-in path.
func TestLeaseRejectsASecondHolder(t *testing.T) {
	path, first := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	lease, err := first.AcquireLease(ctx, LeaseOptions{Holder: "first"})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()

	second, err := OpenArchiveWriter(ctx, path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(ctx)
	if _, err := second.AcquireLease(ctx, LeaseOptions{Holder: "second"}); !errors.Is(err, ErrArchiveLeased) {
		t.Fatalf("second holder accepted: %v", err)
	}
	holder, err := second.LeaseHolder(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if holder != "first" {
		t.Fatalf("lease holder reported as %q", holder)
	}
}

// TestLeaseReleaseAllowsTakeover proves the normal handover path: a clean
// shutdown frees the archive for the next process without waiting for the TTL.
func TestLeaseReleaseAllowsTakeover(t *testing.T) {
	path, first := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	lease, err := first.AcquireLease(ctx, LeaseOptions{Holder: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	// Release must be idempotent: a shutdown path that calls it twice must not
	// panic on a closed channel.
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	second, err := OpenArchiveWriter(ctx, path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(ctx)
	next, err := second.AcquireLease(ctx, LeaseOptions{Holder: "second"})
	if err != nil {
		t.Fatalf("takeover after release failed: %v", err)
	}
	defer next.Release()
	if next.Holder() != "second" {
		t.Fatalf("holder is %q", next.Holder())
	}
}

// TestLeaseExpiresAfterACrash is the crash path. A holder that dies without
// releasing must not block the archive forever, which is why the lease carries
// an expiry rather than being a permanent row.
func TestLeaseExpiresAfterACrash(t *testing.T) {
	path, first := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	// A very short TTL stands in for a crashed process: the row is written and
	// then abandoned without a heartbeat.
	if _, err := first.AcquireLease(ctx, LeaseOptions{Holder: "crashed", TTL: 50 * time.Millisecond, Heartbeat: time.Hour}); err != nil {
		t.Fatal(err)
	}
	second, err := OpenArchiveWriter(ctx, path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(ctx)
	if _, err := second.AcquireLease(ctx, LeaseOptions{Holder: "second"}); !errors.Is(err, ErrArchiveLeased) {
		t.Fatalf("live lease was not respected: %v", err)
	}
	time.Sleep(80 * time.Millisecond)
	lease, err := second.AcquireLease(ctx, LeaseOptions{Holder: "second"})
	if err != nil {
		t.Fatalf("expired lease was not reclaimable: %v", err)
	}
	defer lease.Release()
}

// TestLeaseHeartbeatKeepsItAlive proves the heartbeat actually renews. Without
// this the TTL would expire under a healthy process and a second gateway could
// take the archive while the first was still writing.
func TestLeaseHeartbeatKeepsItAlive(t *testing.T) {
	path, first := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	lease, err := first.AcquireLease(ctx, LeaseOptions{Holder: "first", TTL: 120 * time.Millisecond, Heartbeat: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	second, err := OpenArchiveWriter(ctx, path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close(ctx)
	// Wait well past the TTL. A heartbeat that did not renew would let the
	// second writer in here.
	time.Sleep(300 * time.Millisecond)
	if _, err := second.AcquireLease(ctx, LeaseOptions{Holder: "second"}); !errors.Is(err, ErrArchiveLeased) {
		t.Fatalf("heartbeat did not renew the lease: %v", err)
	}
}

// TestLeaseHolderIgnoresAnExpiredRow pins the reporting rule: an expired row is
// not a holder, so a startup error must not name a process that is gone.
func TestLeaseHolderIgnoresAnExpiredRow(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	if _, err := writer.store.db.Exec(`INSERT INTO mem_v2_instance_lease
		(id,holder,acquired_at,heartbeat_at,expires_at) VALUES(1,'stale',0,0,1)`); err != nil {
		t.Fatal(err)
	}
	holder, err := writer.LeaseHolder(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if holder != "" {
		t.Fatalf("expired row reported as holder %q", holder)
	}
}
