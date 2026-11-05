package memstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrArchiveClosed        = errors.New("archive writer closed")
	ErrArchiveTooLarge      = errors.New("archive operation exceeds queue byte limit")
	ErrArchiveCommitUnknown = errors.New("archive commit outcome uncertain; retry using the same IDs")
	ErrArchiveConflict      = errors.New("archive identity reused with different content or ownership")
)

type WriterOptions struct {
	QueueItems    int
	QueueBytes    int
	BatchItems    int
	BatchBytes    int
	BatchDelay    time.Duration
	CommitTimeout time.Duration
}

func (options WriterOptions) resolved() (WriterOptions, error) {
	if options.QueueItems < 0 || options.QueueBytes < 0 || options.BatchItems < 0 || options.BatchBytes < 0 || options.BatchDelay < 0 || options.CommitTimeout < 0 {
		return options, fmt.Errorf("memstore: writer settings cannot be negative")
	}
	if options.QueueItems == 0 {
		options.QueueItems = 1024
	}
	if options.QueueBytes == 0 {
		options.QueueBytes = 16 * 1024 * 1024
	}
	if options.BatchItems == 0 {
		options.BatchItems = 64
	}
	if options.BatchBytes == 0 {
		options.BatchBytes = 256 * 1024
	}
	if options.BatchDelay == 0 {
		options.BatchDelay = 10 * time.Millisecond
	}
	if options.CommitTimeout == 0 {
		options.CommitTimeout = 2 * time.Second
	}
	return options, nil
}

type archiveOperation struct {
	ctx      context.Context
	bytes    int
	queuedAt time.Time
	apply    func(context.Context, *sql.Tx) error
	result   chan error
	// commitTimeout overrides WriterOptions.CommitTimeout for this operation.
	// Zero means use the writer default. Background maintenance work sets it
	// because its cost scales with archive size and cannot be bounded by the
	// latency budget that protects user-facing commits.
	commitTimeout time.Duration
}

type WriterStats struct {
	PendingItems        int
	PendingBytes        int
	CommittedOperations uint64
	FailedOperations    uint64
	Batches             uint64
}

type ArchiveWriter struct {
	store   *Store
	options WriterOptions
	mu      sync.Mutex
	queue   []*archiveOperation
	closing bool
	changed chan struct{}
	done    chan struct{}
	stats   WriterStats
}

func OpenArchiveWriter(ctx context.Context, path string, options WriterOptions) (*ArchiveWriter, error) {
	options, err := options.resolved()
	if err != nil {
		return nil, err
	}
	store, err := openDatabaseMode(path, false, false)
	if err != nil {
		return nil, err
	}
	if err := checkArchiveSchema(ctx, store.db); err != nil {
		store.Close()
		return nil, err
	}
	writer := &ArchiveWriter{store: store, options: options, changed: make(chan struct{}), done: make(chan struct{})}
	go writer.run()
	return writer, nil
}

func (writer *ArchiveWriter) submit(ctx context.Context, size int, apply func(context.Context, *sql.Tx) error) error {
	return writer.submitWithTimeout(ctx, size, 0, apply)
}

// submitWithTimeout queues an operation with an explicit commit timeout. A zero
// timeout uses the writer default.
func (writer *ArchiveWriter) submitWithTimeout(ctx context.Context, size int, commitTimeout time.Duration, apply func(context.Context, *sql.Tx) error) error {
	if size < 0 || size > writer.options.QueueBytes {
		return ErrArchiveTooLarge
	}
	operation := &archiveOperation{
		ctx: ctx, bytes: size, apply: apply, result: make(chan error, 1),
		commitTimeout: commitTimeout,
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		writer.mu.Lock()
		if writer.closing {
			writer.mu.Unlock()
			return ErrArchiveClosed
		}
		if writer.stats.PendingItems < writer.options.QueueItems && size <= writer.options.QueueBytes-writer.stats.PendingBytes {
			operation.queuedAt = time.Now()
			writer.queue = append(writer.queue, operation)
			writer.stats.PendingItems++
			writer.stats.PendingBytes += size
			writer.notifyLocked()
			writer.mu.Unlock()
			break
		}
		changed := writer.changed
		writer.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
	select {
	case err := <-operation.result:
		return err
	case <-ctx.Done():
		select {
		case err := <-operation.result:
			return err
		default:
			return fmt.Errorf("%w: %w", ErrArchiveCommitUnknown, ctx.Err())
		}
	}
}

func (writer *ArchiveWriter) Flush(ctx context.Context) error {
	return writer.submit(ctx, 0, nil)
}

func (writer *ArchiveWriter) Close(ctx context.Context) error {
	writer.mu.Lock()
	writer.closing = true
	writer.notifyLocked()
	writer.mu.Unlock()
	select {
	case <-writer.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (writer *ArchiveWriter) Stats() WriterStats {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	return writer.stats
}

func (writer *ArchiveWriter) notifyLocked() {
	close(writer.changed)
	writer.changed = make(chan struct{})
}

func (writer *ArchiveWriter) takeBatch() []*archiveOperation {
	for {
		writer.mu.Lock()
		if len(writer.queue) == 0 {
			if writer.closing {
				writer.mu.Unlock()
				return nil
			}
			changed := writer.changed
			writer.mu.Unlock()
			<-changed
			continue
		}
		count, size := 0, 0
		for _, operation := range writer.queue {
			if count >= writer.options.BatchItems || (count > 0 && operation.bytes > writer.options.BatchBytes-size) {
				break
			}
			count++
			size += operation.bytes
		}
		wait := time.Until(writer.queue[0].queuedAt.Add(writer.options.BatchDelay))
		if writer.closing || wait <= 0 || count < len(writer.queue) || count >= writer.options.BatchItems || size >= writer.options.BatchBytes {
			batch := append([]*archiveOperation(nil), writer.queue[:count]...)
			copy(writer.queue, writer.queue[count:])
			clear(writer.queue[len(writer.queue)-count:])
			writer.queue = writer.queue[:len(writer.queue)-count]
			writer.mu.Unlock()
			return batch
		}
		changed := writer.changed
		writer.mu.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-changed:
		case <-timer.C:
		}
		timer.Stop()
	}
}

func (writer *ArchiveWriter) run() {
	defer close(writer.done)
	defer writer.store.Close()
	for {
		batch := writer.takeBatch()
		if len(batch) == 0 {
			return
		}
		active := make([]*archiveOperation, 0, len(batch))
		for _, operation := range batch {
			if err := operation.ctx.Err(); err != nil {
				writer.complete(operation, err)
			} else {
				active = append(active, operation)
			}
		}
		if len(active) == 0 {
			continue
		}
		err := writer.commit(active)
		for _, operation := range active {
			writer.complete(operation, err)
		}
	}
}

func (writer *ArchiveWriter) commit(batch []*archiveOperation) error {
	// A batch shares one transaction, so it shares one deadline: the longest
	// timeout any operation in it asked for.
	timeout := writer.options.CommitTimeout
	for _, operation := range batch {
		if operation.commitTimeout > timeout {
			timeout = operation.commitTimeout
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	tx, err := writer.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, operation := range batch {
		if operation.apply != nil {
			if err := operation.apply(ctx, tx); err != nil {
				return err
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("%w: %w", ErrArchiveCommitUnknown, err)
	}
	writer.mu.Lock()
	writer.stats.Batches++
	writer.mu.Unlock()
	return nil
}

func (writer *ArchiveWriter) complete(operation *archiveOperation, err error) {
	writer.mu.Lock()
	writer.stats.PendingItems--
	writer.stats.PendingBytes -= operation.bytes
	if err == nil {
		writer.stats.CommittedOperations++
	} else {
		writer.stats.FailedOperations++
	}
	writer.notifyLocked()
	writer.mu.Unlock()
	operation.result <- err
}
