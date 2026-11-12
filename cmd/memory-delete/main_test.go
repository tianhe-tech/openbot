package main

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/user/opencode-gateway/internal/memstore"
	"github.com/user/opencode-gateway/internal/opencode"
)

// deleteFixture builds a migrated archive with one completed turn for the owner
// and one for a second principal, so a scoped deletion can be told apart from an
// archive-wide one.
func deleteFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "memory.db")
	store, err := memstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MigrateArchive(context.Background()); err != nil {
		store.Close()
		t.Fatal(err)
	}
	store.Close()
	writer, err := memstore.OpenArchiveWriter(context.Background(), path, memstore.WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, owner := range []struct{ adapter, user string }{
		{"test", "owner"},
		{"test", "other"},
	} {
		turn, err := writer.BeginTurn(ctx, opencode.TurnInput{
			Adapter: owner.adapter, UserID: owner.user, ThreadID: "thread-" + owner.user,
			DeliveryID: "delivery-" + owner.user, Content: "deleteprobe request", WorkDir: "/work/gateway",
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
			Attempt: attempt, ID: "msg-" + owner.user, Text: "deleteprobe answer",
			State: "completed", CreatedAt: 1700000000000,
		}); err != nil {
			t.Fatalf("save message for %s: %v", owner.user, err)
		}
		if err := writer.FinishAttempt(ctx, opencode.AttemptResult{Attempt: attempt, Status: "completed"}); err != nil {
			t.Fatalf("finish attempt for %s: %v", owner.user, err)
		}
	}
	// Capture only queues index jobs; nothing is searchable until the worker
	// runs. Without this the fixture is invisible to Search and every count
	// below is zero, which makes the assertions pass for the wrong reason.
	for {
		count, err := writer.IndexPending(ctx, 200)
		if err != nil {
			t.Fatalf("index: %v", err)
		}
		if count == 0 {
			break
		}
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
	return path
}

func messageCountForTest(t *testing.T, path, userID string) int {
	t.Helper()
	store, err := memstore.OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	result, err := store.Search(context.Background(), memstore.SearchOptions{
		Scope: memstore.SearchScope{Adapter: "test", UserID: userID}, Query: "deleteprobe",
	})
	if err != nil {
		t.Fatal(err)
	}
	return len(result.Hits)
}

// TestDeleteCommandRemovesOnlyTheNamedPrincipal is the end-to-end assertion.
//
// The library test proves DeleteScope is scoped; this proves the command passes
// the scope through. Those are different failures: a command that ignored
// --user and deleted the whole archive would pass every library test in the
// package, because the library was never called with the wrong scope.
func TestDeleteCommandRemovesOnlyTheNamedPrincipal(t *testing.T) {
	path := deleteFixture(t)
	if count := messageCountForTest(t, path, "owner"); count == 0 {
		t.Fatal("fixture is not searchable for the owner; the test would pass vacuously")
	}
	if count := messageCountForTest(t, path, "other"); count == 0 {
		t.Fatal("fixture is not searchable for the second principal")
	}

	var output bytes.Buffer
	if err := run(context.Background(), []string{
		"--db", path, "--adapter", "test", "--user", "owner", "--confirm",
	}, &output); err != nil {
		t.Fatal(err)
	}
	var report memstore.DeleteReport
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatalf("report is not JSON: %s, %v", output.String(), err)
	}
	if report.Messages != 2 {
		t.Fatalf("report claims %d messages deleted, want the owner's 2", report.Messages)
	}
	if count := messageCountForTest(t, path, "owner"); count != 0 {
		t.Fatalf("the named principal still has %d search hits", count)
	}
	if count := messageCountForTest(t, path, "other"); count == 0 {
		t.Fatal("the unnamed principal lost its messages")
	}
}

// TestDeleteCommandRequiresConfirmation pins the one guard that cannot be
// satisfied by forgetting a filter.
//
// Every other check narrows the selection, and a selection that is accidentally
// empty selects everything: --adapter and --user with no session, project, or
// time bound is the principal's entire history. The confirmation flag is the
// only thing standing between a typo and an irreversible deletion, so its
// absence has to be an error rather than a prompt.
func TestDeleteCommandRequiresConfirmation(t *testing.T) {
	path := deleteFixture(t)
	if err := run(context.Background(), []string{
		"--db", path, "--adapter", "test", "--user", "owner",
	}, &bytes.Buffer{}); err == nil {
		t.Fatal("deletion ran without --confirm")
	}
	if count := messageCountForTest(t, path, "owner"); count == 0 {
		t.Fatal("the refused command deleted data anyway")
	}
}

func TestDeleteCommandRejectsUnsafeArguments(t *testing.T) {
	path := deleteFixture(t)
	for _, args := range [][]string{
		{},
		{"--db", path},
		{"--db", path, "--confirm"},
		{"--db", path, "--adapter", "test", "--confirm"},
		{"--db", path, "--user", "owner", "--confirm"},
		{"--db", path, "--adapter", "test", "--user", "owner", "--confirm", "positional"},
		{"--db", path, "--adapter", "test", "--user", "owner", "--confirm", "--since", "-1"},
		{"--db", path, "--adapter", "test", "--user", "owner", "--confirm", "--until", "-1"},
		{"--db", path, "--adapter", "test", "--user", "owner", "--confirm", "--since", "200", "--until", "100"},
	} {
		if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatalf("accepted unsafe arguments: %v", args)
		}
	}
	if count := messageCountForTest(t, path, "owner"); count == 0 {
		t.Fatal("a rejected invocation deleted data")
	}
}
