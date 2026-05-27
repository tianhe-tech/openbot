// Package asyncwork provides a tiny in-process job queue so work that used to
// block the user-facing request path (handoff summarization, skill-candidate
// analysis, draft generation) can run on background goroutines without
// dropping jobs during normal load and without pulling in a third-party queue.
//
// Design goals:
//   - Single dispatcher goroutine (simplicity, ordering is per-kind best effort).
//   - Bounded buffered channel: backpressure surfaces as an enqueue-dropped log
//     line instead of OOM or caller block.
//   - Graceful drain on shutdown: callers invoke Stop(ctx) and we try to finish
//     what's already queued within ctx's deadline.
//   - No external dependencies.
package asyncwork

import (
	"context"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// Job is any unit of async work. Implementations must be idempotent enough to
// survive being dropped on shutdown; the queue makes no durability promises.
type Job interface {
	// Name is a short label used only in logs.
	Name() string
	// Run executes the job. Errors are logged and do not stop the worker.
	Run(ctx context.Context) error
}

// TimedJob is an optional extension of Job that allows individual jobs to
// override the queue's default 5-minute timeout.
type TimedJob interface {
	Job
	// Timeout returns the per-job timeout. Zero or negative means use default.
	Timeout() time.Duration
}

// Queue is a single-goroutine dispatcher over a buffered channel.
type Queue struct {
	jobs    chan Job
	wg      sync.WaitGroup
	stopped atomic.Bool
	cancel  context.CancelFunc

	// Stats (best-effort counters; race-tolerant via atomic).
	enqueued atomic.Uint64
	dropped  atomic.Uint64
	ran      atomic.Uint64
	failed   atomic.Uint64
}

// New creates a Queue with the given buffer capacity. capacity <= 0 defaults to 128.
func New(capacity int) *Queue {
	if capacity <= 0 {
		capacity = 128
	}
	return &Queue{jobs: make(chan Job, capacity)}
}

// Start launches the single dispatcher goroutine. Each job gets per-job timeout
// (default 5 minutes, callers can wrap their own deadlines inside Run).
func (q *Queue) Start(parent context.Context) {
	if q == nil {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	q.cancel = cancel
	q.wg.Add(1)
	go q.loop(ctx)
}

func (q *Queue) loop(ctx context.Context) {
	defer q.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case job, ok := <-q.jobs:
			if !ok {
				return
			}
			q.runOne(ctx, job)
		}
	}
}

func (q *Queue) runOne(parent context.Context, job Job) {
	defer func() {
		if r := recover(); r != nil {
			q.failed.Add(1)
			log.Printf("asyncwork: job %q panicked: %v", job.Name(), r)
		}
	}()
	timeout := 5 * time.Minute
	if tj, ok := job.(TimedJob); ok {
		if d := tj.Timeout(); d > 0 {
			timeout = d
		}
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	start := time.Now()
	if err := job.Run(ctx); err != nil {
		q.failed.Add(1)
		log.Printf("asyncwork: job %q failed after %s: %v", job.Name(), time.Since(start), err)
		return
	}
	q.ran.Add(1)
	log.Printf("asyncwork: job %q done in %s", job.Name(), time.Since(start))
}

// JobFunc is a convenience adapter that turns a closure into a Job.
// When Dur > 0, it also implements TimedJob to override the default timeout.
type JobFunc struct {
	Label string
	Fn    func(ctx context.Context) error
	Dur   time.Duration // optional per-job timeout; 0 = use queue default
}

func (j JobFunc) Name() string                  { return j.Label }
func (j JobFunc) Run(ctx context.Context) error { return j.Fn(ctx) }
func (j JobFunc) Timeout() time.Duration        { return j.Dur }

// Enqueue adds a job. Returns false when the queue is full or stopped; the
// caller can treat the job as lost. Never blocks.
func (q *Queue) Enqueue(job Job) bool {
	if q == nil || q.stopped.Load() || job == nil {
		return false
	}
	q.enqueued.Add(1)
	select {
	case q.jobs <- job:
		return true
	default:
		q.dropped.Add(1)
		log.Printf("asyncwork: queue full, dropping job %q (enqueued=%d dropped=%d)",
			job.Name(), q.enqueued.Load(), q.dropped.Load())
		return false
	}
}

// Stop closes the queue and waits up to ctx's deadline for in-flight work.
func (q *Queue) Stop(ctx context.Context) {
	if q == nil || !q.stopped.CompareAndSwap(false, true) {
		return
	}
	close(q.jobs)
	done := make(chan struct{})
	go func() {
		q.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		if q.cancel != nil {
			q.cancel()
		}
		<-done
	}
	log.Printf("asyncwork: stopped (enqueued=%d ran=%d failed=%d dropped=%d)",
		q.enqueued.Load(), q.ran.Load(), q.failed.Load(), q.dropped.Load())
}

// Stats returns a snapshot of counters.
func (q *Queue) Stats() (enqueued, ran, failed, dropped uint64) {
	return q.enqueued.Load(), q.ran.Load(), q.failed.Load(), q.dropped.Load()
}
