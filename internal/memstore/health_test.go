package memstore

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/user/opencode-gateway/internal/opencode"
)

// captureHealthFixture builds a migrated archive with one completed turn for
// the owner and one for a second principal, so a scoped count can be told apart
// from an archive-wide one.
//
// The fixture drives the real lifecycle (BeginTurn, BindAttempt, SaveMessage,
// FinishAttempt) rather than RecordMessage. RecordMessage is the import path: it
// creates a turn and leaves it 'active' because an imported turn has no attempt
// to finish it, so a fixture built that way would report open work that a
// settled archive does not have and the assertions below would be measuring the
// fixture instead of the query.
func captureHealthFixture(t *testing.T) (*ArchiveWriter, SearchScope) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "memory.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MigrateArchive(context.Background()); err != nil {
		store.Close()
		t.Fatal(err)
	}
	store.Close()
	writer, err := OpenArchiveWriter(context.Background(), path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { writer.Close(context.Background()) })
	ctx := context.Background()
	created := int64(1700000000000)
	for _, owner := range []struct{ adapter, user string }{
		{"test", "owner"},
		{"test", "other"},
	} {
		turn, err := writer.BeginTurn(ctx, opencode.TurnInput{
			Adapter: owner.adapter, UserID: owner.user, ThreadID: "thread-" + owner.user,
			DeliveryID: "delivery-" + owner.user, Content: "restart the gateway", WorkDir: "/work/gateway",
		})
		if err != nil {
			t.Fatalf("begin turn for %s: %v", owner.user, err)
		}
		attempt, err := writer.BindAttempt(ctx, opencode.AttemptInput{
			Turn: turn, ID: "attempt-" + owner.user, SessionID: "session-" + owner.user,
		})
		if err != nil {
			t.Fatalf("bind attempt for %s: %v", owner.user, err)
		}
		if err := writer.SaveMessage(ctx, opencode.MessageInput{
			Attempt: attempt, ID: "msg-" + owner.user, Text: "Run systemctl restart opencode-gateway.",
			State: "completed", CreatedAt: created,
		}); err != nil {
			t.Fatalf("save message for %s: %v", owner.user, err)
		}
		if err := writer.FinishAttempt(ctx, opencode.AttemptResult{Attempt: attempt, Status: "completed"}); err != nil {
			t.Fatalf("finish attempt for %s: %v", owner.user, err)
		}
	}
	return writer, SearchScope{Adapter: "test", UserID: "owner"}
}

// TestCaptureHealthCountsOnlyTheBoundPrincipal is the disclosure assertion.
//
// A health line is the easiest place in a codebase to leak a count, because a
// count looks harmless. It is not: "3 turns are stuck" tells one principal that
// another principal is active, and the search gate forbids exactly that
// inference. The fixture seeds a second principal with the same shape as the
// first, so a query that forgot its WHERE clause would report double and the
// test would fail rather than pass by finding nothing.
func TestCaptureHealthCountsOnlyTheBoundPrincipal(t *testing.T) {
	writer, scope := captureHealthFixture(t)
	health, err := writer.store.CaptureHealth(context.Background(), scope)
	if err != nil {
		t.Fatalf("capture health: %v", err)
	}
	// One turn per principal, both completed, so the owner sees its own
	// capture time and no open work. An archive-wide count would report two
	// turns, and an unscoped MAX(created_at) would answer for the other
	// principal too.
	if health.LastCaptureAt <= 0 {
		t.Fatalf("last capture = %d, want a positive timestamp", health.LastCaptureAt)
	}
	if health.ActiveTurns != 0 || health.AwaitingReconcile != 0 {
		t.Fatalf("completed turns reported as open: %+v", health)
	}
	if health.PendingReconcileJobs != 0 || health.FailedReconcileJobs != 0 {
		t.Fatalf("completed turns left reconcile jobs: %+v", health)
	}

	// A principal with no messages must report zero rather than borrowing the
	// archive's newest timestamp. This is the assertion that proves the MAX is
	// scoped: the archive does contain messages, just not this principal's.
	empty, err := writer.store.CaptureHealth(context.Background(), SearchScope{Adapter: "test", UserID: "nobody"})
	if err != nil {
		t.Fatalf("capture health for empty principal: %v", err)
	}
	if empty.LastCaptureAt != 0 {
		t.Fatalf("empty principal reported capture at %d", empty.LastCaptureAt)
	}
	if empty.ActiveTurns != 0 || empty.PendingReconcileJobs != 0 {
		t.Fatalf("empty principal reported another principal's work: %+v", empty)
	}
}

// TestCaptureHealthRequiresAScope pins the fail-closed rule. An unscoped call
// must be an error, not an archive-wide answer, because the archive-wide answer
// is the disclosure.
func TestCaptureHealthRequiresAScope(t *testing.T) {
	writer, _ := captureHealthFixture(t)
	for _, scope := range []SearchScope{
		{},
		{Adapter: "test"},
		{UserID: "owner"},
	} {
		if _, err := writer.store.CaptureHealth(context.Background(), scope); !errors.Is(err, ErrSearchScopeRequired) {
			t.Fatalf("scope %+v: err = %v, want ErrSearchScopeRequired", scope, err)
		}
	}
}

// TestCaptureHealthReportsTheLease pins that the lease is visible from a
// read-only handle, which is the only way the MCP process can answer "is a
// gateway actually running".
func TestCaptureHealthReportsTheLease(t *testing.T) {
	writer, scope := captureHealthFixture(t)
	before, err := writer.store.CaptureHealth(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if before.LeaseHolder != "" || before.LeaseLive(time.Now()) {
		t.Fatalf("unleased archive reported a live lease: %+v", before)
	}

	lease, err := writer.AcquireLease(context.Background(), LeaseOptions{Holder: "test-host:1234", TTL: time.Minute})
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	defer lease.Release()

	held, err := writer.store.CaptureHealth(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if held.LeaseHolder != "test-host:1234" {
		t.Fatalf("lease holder = %q, want test-host:1234", held.LeaseHolder)
	}
	if !held.LeaseLive(time.Now()) {
		t.Fatalf("held lease reported as not live: %+v", held)
	}
	if held.LeaseHeartbeatAt == 0 {
		t.Fatalf("held lease reported no heartbeat: %+v", held)
	}

	// An expired row is not a live lease. This is the case that matters after
	// a crash: the row is still there, and reporting it as live would tell an
	// operator that a dead process is running.
	expired := held
	expired.LeaseExpiresAt = time.Now().Add(-time.Minute).UnixMilli()
	if expired.LeaseLive(time.Now()) {
		t.Fatal("an expired lease row reported as live")
	}
}

// TestCaptureHealthCountsOpenWork pins that the counters move. A health report
// whose numbers are always zero is indistinguishable from a healthy archive, so
// the fixture drives a turn into the open state and asserts it is visible.
func TestCaptureHealthCountsOpenWork(t *testing.T) {
	writer, scope := captureHealthFixture(t)
	ctx := context.Background()
	turn, err := writer.BeginTurn(ctx, opencode.TurnInput{
		Adapter: "test", UserID: "owner", ThreadID: "thread-owner",
		DeliveryID: "delivery-open", Content: "a new request", WorkDir: "/work/gateway",
	})
	if err != nil {
		t.Fatalf("begin turn: %v", err)
	}
	attempt, err := writer.BindAttempt(ctx, opencode.AttemptInput{
		Turn: turn, ID: "attempt-open", SessionID: "session-owner",
	})
	if err != nil {
		t.Fatalf("begin attempt: %v", err)
	}
	health, err := writer.store.CaptureHealth(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if health.ActiveTurns != 1 {
		t.Fatalf("active turns = %d, want 1: %+v", health.ActiveTurns, health)
	}
	if health.PendingReconcileJobs != 1 {
		t.Fatalf("pending reconcile jobs = %d, want 1: %+v", health.PendingReconcileJobs, health)
	}

	// The other principal must not see this turn.
	other, err := writer.store.CaptureHealth(ctx, SearchScope{Adapter: "test", UserID: "other"})
	if err != nil {
		t.Fatal(err)
	}
	if other.ActiveTurns != 0 || other.PendingReconcileJobs != 0 {
		t.Fatalf("another principal saw the owner's open turn: %+v", other)
	}

	// Finishing the attempt clears both counters, so the report cannot be
	// stuck on by a turn that completed.
	if err := writer.FinishAttempt(ctx, opencode.AttemptResult{
		Attempt: attempt, Status: "completed",
	}); err != nil {
		t.Fatalf("finish attempt: %v", err)
	}
	done, err := writer.store.CaptureHealth(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	if done.ActiveTurns != 0 {
		t.Fatalf("finished turn still counted as active: %+v", done)
	}
	if done.PendingReconcileJobs != 0 {
		t.Fatalf("finished attempt left a pending reconcile job: %+v", done)
	}
}
