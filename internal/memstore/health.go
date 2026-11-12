package memstore

import (
	"context"
	"fmt"
	"time"
)

// CaptureHealth is the part of the capture pipeline's state that is visible
// from the archive itself.
//
// It exists because the health report is served by a separate, read-only
// process. The gateway's writer queue lives in that process's memory, so its
// depth at any instant is not readable here — and it would not be a useful
// number if it were: the queue is a latency buffer measured in milliseconds,
// so a snapshot of it is noise rather than a health signal. What matters is the
// durable backlog, which is what this struct reports: work that has been
// acknowledged to a caller but not yet reconciled, and turns that are still
// open. Those are the numbers that answer "is anything stuck".
//
// Every count is scoped to one principal. An archive-wide count would let one
// principal infer another's activity from a health line, which is the same
// disclosure the search gate forbids — a count is a disclosure even when it
// carries no text. The lease fields are the exception: the lease row is
// archive-wide by construction and holds only "<hostname>:<pid>", which names a
// process rather than a person.
//
// Every field comes from an indexed lookup. A health check that costs a full
// scan is a health check nobody runs.
type CaptureHealth struct {
	// PendingReconcileJobs and FailedReconcileJobs count this principal's
	// attempts whose outcome is not yet known. A pending job is normal for a
	// few seconds after a turn; a failed one means reconciliation gave up.
	PendingReconcileJobs int64
	FailedReconcileJobs  int64
	// ActiveTurns counts turns that have started and not finished.
	ActiveTurns int64
	// AwaitingReconcile counts turns whose attempt ended without a definite
	// outcome. They are not errors, but they are not finished either.
	AwaitingReconcile int64
	// LastCaptureAt is the newest message timestamp for this principal, or
	// zero when nothing has been captured. It answers "is capture still
	// writing", which no counter can.
	LastCaptureAt int64
	// LeaseHolder, LeaseHeartbeatAt and LeaseExpiresAt describe the instance
	// lease row. An empty holder means the archive is unleased, which is
	// normal for a stopped gateway and suspicious for one that should be
	// running.
	LeaseHolder      string
	LeaseHeartbeatAt int64
	LeaseExpiresAt   int64
}

// LeaseLive reports whether the lease row is currently valid.
func (health CaptureHealth) LeaseLive(now time.Time) bool {
	return health.LeaseHolder != "" && health.LeaseExpiresAt > now.UTC().UnixMilli()
}

// CaptureHealth reports the capture pipeline's durable state for one principal.
//
// The principal is required rather than optional. A missing scope would make
// every count archive-wide, which is both a disclosure and a wrong answer: the
// caller asking "is my capture stuck" would be told about somebody else's.
func (s *Store) CaptureHealth(ctx context.Context, scope SearchScope) (CaptureHealth, error) {
	if scope.Adapter == "" || scope.UserID == "" {
		return CaptureHealth{}, ErrSearchScopeRequired
	}
	principal := archiveID("principal", scope.Adapter, scope.UserID)
	var health CaptureHealth
	// The reconcile job counts join through attempts to turns so they are
	// scoped by principal. mem_v2_jobs itself has no principal column: a job is
	// addressed by (kind, source_id), and for reconcile jobs the source is an
	// attempt, which belongs to exactly one turn and therefore one principal.
	err := s.db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM mem_v2_jobs j
			JOIN mem_v2_attempts a ON a.id=j.source_id
			JOIN mem_v2_turns t ON t.id=a.turn_id
			WHERE j.kind='reconcile' AND j.status='pending' AND t.principal_id=?),
		(SELECT COUNT(*) FROM mem_v2_jobs j
			JOIN mem_v2_attempts a ON a.id=j.source_id
			JOIN mem_v2_turns t ON t.id=a.turn_id
			WHERE j.kind='reconcile' AND j.status='failed' AND t.principal_id=?),
		(SELECT COUNT(*) FROM mem_v2_turns WHERE status='active' AND principal_id=?),
		(SELECT COUNT(*) FROM mem_v2_turns WHERE status='awaiting_reconciliation' AND principal_id=?),
		(SELECT COALESCE(MAX(created_at),0) FROM mem_v2_messages WHERE principal_id=?),
		COALESCE((SELECT holder FROM mem_v2_instance_lease WHERE id=1),''),
		COALESCE((SELECT heartbeat_at FROM mem_v2_instance_lease WHERE id=1),0),
		COALESCE((SELECT expires_at FROM mem_v2_instance_lease WHERE id=1),0)`,
		principal, principal, principal, principal, principal).Scan(
		&health.PendingReconcileJobs, &health.FailedReconcileJobs,
		&health.ActiveTurns, &health.AwaitingReconcile,
		&health.LastCaptureAt,
		&health.LeaseHolder, &health.LeaseHeartbeatAt, &health.LeaseExpiresAt)
	if err != nil {
		return health, fmt.Errorf("memstore: capture health: %w", err)
	}
	return health, nil
}
