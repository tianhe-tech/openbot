package memstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// MaintenanceOptions tunes the background maintenance loop. The zero value is
// usable.
type MaintenanceOptions struct {
	// Interval is how long to wait between passes. Zero uses five minutes.
	Interval time.Duration
	// CheckpointBytes is the WAL size above which a pass runs a checkpoint.
	// Zero uses 64 MiB. A checkpoint below this threshold is skipped because
	// the WAL is already small and the checkpoint would only add I/O.
	CheckpointBytes int64
	// OnError receives each failed pass. The loop keeps running afterwards: a
	// transient database error must not silently stop maintenance forever.
	OnError func(error)
}

// MaintenanceReport describes what one pass did. It exists so the work is
// observable rather than assumed: a checkpoint that never runs and a
// checkpoint that runs every pass look identical from the outside otherwise.
type MaintenanceReport struct {
	// WALBytesBefore and WALBytesAfter bracket the pass.
	WALBytesBefore int64
	WALBytesAfter  int64
	// Checkpointed reports whether a checkpoint was attempted.
	Checkpointed bool
	// Busy reports that the checkpoint could not complete because a reader
	// held the WAL. This is normal under concurrent reads and is not an error.
	Busy bool
	// PagesCheckpointed and PagesRemaining come from PRAGMA wal_checkpoint.
	PagesCheckpointed int64
	PagesRemaining    int64
}

// RunMaintenance checkpoints the WAL until ctx is cancelled.
//
// Why this exists: SQLite in WAL mode grows the -wal file until a checkpoint
// copies its pages back into the main database. The default autocheckpoint
// (1000 pages, about 4 MiB) runs inside a write transaction, so a writer that
// is continuously busy can starve it, and a long-lived gateway then accumulates
// a WAL that is never truncated. The plan requires an explicit maintenance
// policy rather than relying on the default.
//
// The pass uses TRUNCATE rather than PASSIVE: PASSIVE leaves the WAL at its
// high-water mark, so a single burst of writes would keep the file large
// forever. TRUNCATE resets it, and it is safe here because the writer is the
// only writer and the checkpoint runs on the same single connection.
func (writer *ArchiveWriter) RunMaintenance(ctx context.Context, options MaintenanceOptions) {
	interval := options.Interval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	threshold := options.CheckpointBytes
	if threshold <= 0 {
		threshold = 64 * 1024 * 1024
	}
	for {
		if ctx.Err() != nil {
			return
		}
		report, err := writer.Checkpoint(ctx, threshold)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if options.OnError != nil {
				options.OnError(err)
			}
		} else if report.Checkpointed && options.OnError != nil && report.Busy {
			// A busy checkpoint is not a failure, but it is worth reporting:
			// a WAL that is never checkpointed is the condition this loop
			// exists to prevent.
			options.OnError(fmt.Errorf("memstore: WAL checkpoint incomplete: %d pages remain", report.PagesRemaining))
		}
		if !sleepContext(ctx, interval) {
			return
		}
	}
}

// Checkpoint runs one maintenance pass. A WAL smaller than threshold is left
// alone and reported as not checkpointed.
func (writer *ArchiveWriter) Checkpoint(ctx context.Context, threshold int64) (MaintenanceReport, error) {
	var report MaintenanceReport
	before, err := writer.walBytes()
	if err != nil {
		return report, err
	}
	report.WALBytesBefore = before
	if threshold > 0 && before < threshold {
		report.WALBytesAfter = before
		return report, nil
	}
	report.Checkpointed = true
	// PRAGMA wal_checkpoint returns one row: busy, log pages checkpointed,
	// log pages remaining. It must run outside a transaction, so it goes
	// through the writer's own connection rather than a submitted operation.
	var busy, checkpointed, remaining int64
	if err := writer.store.db.QueryRowContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &checkpointed, &remaining); err != nil {
		return report, fmt.Errorf("memstore: wal checkpoint: %w", err)
	}
	report.Busy = busy != 0
	report.PagesCheckpointed = checkpointed
	report.PagesRemaining = remaining
	after, err := writer.walBytes()
	if err != nil {
		return report, err
	}
	report.WALBytesAfter = after
	return report, nil
}

// walBytes reports the size of the -wal file, or zero when it does not exist.
// A missing WAL is not an error: SQLite removes it after a successful
// truncating checkpoint.
func (writer *ArchiveWriter) walBytes() (int64, error) {
	var path string
	if err := writer.store.db.QueryRow(`PRAGMA database_list`).Scan(new(int), new(string), &path); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("memstore: locate database file: %w", err)
	}
	if path == "" {
		return 0, nil
	}
	return fileBytes(path + "-wal"), nil
}
