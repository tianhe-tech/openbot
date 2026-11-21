package knowledge

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/user/opencode-gateway/internal/memstore"
	"github.com/user/opencode-gateway/internal/opencode"
)

// ---- fixtures ----

// knowledgeArchive is a migrated archive plus the two handles the service needs.
//
// The service takes a *Store for reads and an *ArchiveWriter for writes, and
// they are separate objects because the writer owns the only write path. A test
// that used one handle for both would not exercise the split the production
// wiring has.
type knowledgeArchive struct {
	path   string
	store  *memstore.Store
	writer *memstore.ArchiveWriter
}

func newKnowledgeArchive(t *testing.T) *knowledgeArchive {
	t.Helper()
	path := filepath.Join(t.TempDir(), "memory.db")
	store, err := memstore.Open(path)
	if err != nil {
		t.Fatalf("open archive: %v", err)
	}
	if err := store.MigrateArchive(context.Background()); err != nil {
		t.Fatalf("migrate archive: %v", err)
	}
	store.Close()
	writer, err := memstore.OpenArchiveWriter(context.Background(), path, memstore.WriterOptions{})
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	t.Cleanup(func() { writer.Close(context.Background()) })
	reader, err := memstore.Open(path)
	if err != nil {
		t.Fatalf("reopen archive: %v", err)
	}
	t.Cleanup(func() { reader.Close() })
	return &knowledgeArchive{path: path, store: reader, writer: writer}
}

// recordTurn writes one user/assistant pair and returns the two message IDs.
func (a *knowledgeArchive) recordTurn(t *testing.T, delivery, project, request, answer string, created int64) (userID, answerID string) {
	t.Helper()
	ctx := context.Background()
	session := memstore.ArchiveSession{
		ID: "session-" + delivery, Adapter: "test", UserID: "owner",
		ThreadID: "thread-" + delivery, Project: project,
		WorkDir: "/work/" + project, CreatedAt: created,
	}
	turn := memstore.ArchiveTurn{ID: "turn-" + delivery, DeliveryKey: delivery, StartedAt: created}
	user := memstore.ArchiveMessage{
		Session: session, Turn: turn, ID: "user-" + delivery, Sequence: 0,
		Role: "user", State: "completed", Text: request,
		CreatedAt: created, Fidelity: "captured",
	}
	if err := a.writer.RecordMessage(ctx, user); err != nil {
		t.Fatalf("record request %s: %v", delivery, err)
	}
	reply := user
	reply.ID = "answer-" + delivery
	reply.Sequence = 1
	reply.Role = "assistant"
	reply.Text = answer
	reply.CreatedAt = created + 1000
	if err := a.writer.RecordMessage(ctx, reply); err != nil {
		t.Fatalf("record answer %s: %v", delivery, err)
	}
	return user.ID, reply.ID
}

// fakeDrafter returns canned facts and records how it was called.
type fakeDrafter struct {
	facts []DraftFact
	err   error
	calls []DraftInput
}

func (f *fakeDrafter) Draft(ctx context.Context, in DraftInput) (DraftOutput, error) {
	f.calls = append(f.calls, in)
	if f.err != nil {
		return DraftOutput{}, f.err
	}
	return DraftOutput{Facts: f.facts, ModelID: in.ModelID}, nil
}

func testScope() memstore.SearchScope {
	return memstore.SearchScope{Adapter: "test", UserID: "owner"}
}

func TestKnowledgeWorkerRestoresRealCapturedSource(t *testing.T) {
	archive := newKnowledgeArchive(t)
	ctx := context.Background()
	turn, err := archive.writer.BeginTurn(ctx, opencode.TurnInput{
		Adapter: "test", UserID: "owner", ThreadID: "chat", DeliveryID: "captured", Content: "use Chinese", WorkDir: "/work/gateway",
	})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := archive.writer.BindAttempt(ctx, opencode.AttemptInput{Turn: turn, ID: "attempt", SessionID: "ses_upstream"})
	if err != nil {
		t.Fatal(err)
	}
	if err := archive.writer.SaveMessage(ctx, opencode.MessageInput{Attempt: attempt, ID: "answer", Text: "confirmed", State: "completed", CreatedAt: time.Now().UnixMilli()}); err != nil {
		t.Fatal(err)
	}
	first := newTestService(t, archive, &fakeDrafter{}, Config{})
	first.OnKnowledgeCandidate(opencode.KnowledgeCandidateEvent{
		Trigger: opencode.KnowledgeTriggerHandoff, Adapter: "test", UserID: "owner", ThreadID: "chat", SessionID: "ses_upstream",
	})
	if pending, _, err := archive.store.KnowledgeJobStats(ctx, testScope()); err != nil || pending != 1 {
		t.Fatalf("job not durable: %d %v", pending, err)
	}
	drafter := &fakeDrafter{err: errors.New("temporary model failure")}
	restarted := newTestService(t, archive, drafter, Config{})
	if err := restarted.runPending(ctx); err != nil {
		t.Fatal(err)
	}
	if len(drafter.calls) != 1 || len(drafter.calls[0].Conversation) != 2 {
		t.Fatalf("real lifecycle evidence not recovered: %+v", drafter.calls)
	}
	if pending, _, err := archive.store.KnowledgeJobStats(ctx, testScope()); err != nil || pending != 1 {
		t.Fatalf("failed extraction not retained: %d %v", pending, err)
	}
}

func TestDrafterWaitsForCompletedReply(t *testing.T) {
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path == "/session" {
			_, _ = writer.Write([]byte("[]"))
			return
		}
		completed, text := 0, "["
		if polls.Add(1) > 1 {
			completed, text = 2, "[]"
		}
		_, _ = fmt.Fprintf(writer, `[{"info":{"id":"msg_answer","sessionID":"ses_draft","role":"assistant","time":{"created":1,"completed":%d}},"parts":[{"type":"text","text":%q}]}]`, completed, text)
	}))
	defer server.Close()
	drafter := NewOpencodeDrafter(opencode.NewClient(server.URL, ""))
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	reply, err := drafter.waitForAssistantReply(ctx, "ses_draft")
	if err != nil || reply != "[]" || polls.Load() < 2 {
		t.Fatalf("returned partial draft: %q, polls=%d, error=%v", reply, polls.Load(), err)
	}
}

// newTestService builds a service wired to the archive's real writer, so the
// guards are observed through the durable job table rather than an in-memory
// queue.
func newTestService(t *testing.T, archive *knowledgeArchive, drafter Drafter, cfg Config) *Service {
	t.Helper()
	cfg.Enabled = true
	if cfg.PerModelTimeout == 0 {
		cfg.PerModelTimeout = 5 * time.Second
	}
	return NewService(cfg, archive.store, archive.writer, drafter)
}

// ---- guards ----

// TestOnKnowledgeCandidateRefusesItsOwnDraftingSessions is the recursion guard.
//
// Without it an extraction prompt would itself look like a conversation worth
// extracting, and every extraction would schedule the next one. The plan
// requires that extraction "never schedules itself recursively", so this is the
// test that keeps that true.
func TestOnKnowledgeCandidateRefusesItsOwnDraftingSessions(t *testing.T) {
	archive := newKnowledgeArchive(t)
	drafter := &fakeDrafter{}
	svc := newTestService(t, archive, drafter, Config{MinTurns: 0})

	for _, event := range []opencode.KnowledgeCandidateEvent{
		{Trigger: opencode.KnowledgeTriggerHandoff, Adapter: "knowledge", ThreadID: "thread-a", SessionID: "ses_a", UserID: "owner"},
		{Trigger: opencode.KnowledgeTriggerHandoff, Adapter: "test", ThreadID: "knowledge-draft-thread-a", SessionID: "ses_a", UserID: "owner"},
	} {
		svc.OnKnowledgeCandidate(event)
	}
	if enqueued, _, _ := archive.store.KnowledgeJobStats(context.Background(), testScope()); enqueued != 0 {
		t.Fatalf("the service enqueued %d jobs for its own drafting sessions", enqueued)
	}
}

// TestOnKnowledgeCandidateIgnoresNonHandoffTriggers pins that only a session
// boundary starts extraction. The plan forbids calling a model for every
// message, and a trigger that fired per message would be exactly that.
func TestOnKnowledgeCandidateIgnoresNonHandoffTriggers(t *testing.T) {
	archive := newKnowledgeArchive(t)
	svc := newTestService(t, archive, &fakeDrafter{}, Config{})
	svc.OnKnowledgeCandidate(opencode.KnowledgeCandidateEvent{
		Trigger: opencode.KnowledgeTriggerExplicit, Adapter: "test", ThreadID: "thread-a", SessionID: "ses_a", UserID: "owner",
	})
	if enqueued, _, _ := archive.store.KnowledgeJobStats(context.Background(), testScope()); enqueued != 0 {
		t.Fatalf("a non-handoff trigger enqueued %d jobs", enqueued)
	}
}

// TestOnKnowledgeCandidateRespectsMinTurns pins the cheap gate that avoids a
// model call for a conversation with nothing in it.
func TestOnKnowledgeCandidateRespectsMinTurns(t *testing.T) {
	archive := newKnowledgeArchive(t)
	svc := newTestService(t, archive, &fakeDrafter{}, Config{MinTurns: 4})
	svc.OnKnowledgeCandidate(opencode.KnowledgeCandidateEvent{
		Trigger: opencode.KnowledgeTriggerHandoff, Adapter: "test", ThreadID: "thread-a", SessionID: "ses_a", UserID: "owner", TurnCount: 3,
	})
	if enqueued, _, _ := archive.store.KnowledgeJobStats(context.Background(), testScope()); enqueued != 0 {
		t.Fatalf("a conversation below MinTurns enqueued %d jobs", enqueued)
	}
	svc.OnKnowledgeCandidate(opencode.KnowledgeCandidateEvent{
		Trigger: opencode.KnowledgeTriggerHandoff, Adapter: "test", ThreadID: "thread-b", SessionID: "ses_b", UserID: "owner", TurnCount: 4,
	})
	if enqueued, _, _ := archive.store.KnowledgeJobStats(context.Background(), testScope()); enqueued != 1 {
		t.Fatalf("a conversation at MinTurns enqueued %d jobs, want 1", enqueued)
	}
}

// TestOnKnowledgeCandidateDedupsTheSameThread pins that successive handoffs of
// one thread do not each buy a model call.
func TestOnKnowledgeCandidateDedupsTheSameThread(t *testing.T) {
	archive := newKnowledgeArchive(t)
	svc := newTestService(t, archive, &fakeDrafter{}, Config{})
	event := opencode.KnowledgeCandidateEvent{
		Trigger: opencode.KnowledgeTriggerHandoff, Adapter: "test", ThreadID: "thread-a", SessionID: "ses_a", UserID: "owner",
	}
	svc.OnKnowledgeCandidate(event)
	svc.OnKnowledgeCandidate(event)
	svc.OnKnowledgeCandidate(event)
	if enqueued, _, _ := archive.store.KnowledgeJobStats(context.Background(), testScope()); enqueued != 1 {
		t.Fatalf("three handoffs of one thread enqueued %d jobs, want 1", enqueued)
	}
}

// TestOnKnowledgeCandidateRespectsTheDailyCap pins the cost bound. The cap is
// checked before enqueueing so a capped service does not even pay for a job.
func TestOnKnowledgeCandidateRespectsTheDailyCap(t *testing.T) {
	archive := newKnowledgeArchive(t)
	userMessage, _ := archive.recordTurn(t, "cap", "gateway", "以后都用中文回答我", "好的。", 1700000000000)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := archive.writer.SaveKnowledgeFact(ctx, memstore.KnowledgeFact{
			PrincipalID: memstore.PrincipalFor(testScope()),
			Kind:        memstore.KnowledgeKindPreference,
			Statement:   "prefers Chinese answers " + strings.Repeat("x", i+1),
			Status:      memstore.KnowledgeStatusObserved,
			Scope:       memstore.KnowledgeScopeGlobal,
			Origin:      "extraction",
			Evidence:    []string{userMessage},
		}); err != nil {
			t.Fatalf("seed fact %d: %v", i, err)
		}
	}
	svc := newTestService(t, archive, &fakeDrafter{}, Config{MaxPerDay: 2})
	svc.OnKnowledgeCandidate(opencode.KnowledgeCandidateEvent{
		Trigger: opencode.KnowledgeTriggerHandoff, Adapter: "test", ThreadID: "thread-a", SessionID: "ses_a", UserID: "owner",
	})
	if enqueued, _, _ := archive.store.KnowledgeJobStats(context.Background(), testScope()); enqueued != 0 {
		t.Fatalf("a capped service enqueued %d jobs", enqueued)
	}
}

// ---- pipeline ----

// TestRunStoresFactsWithRealEvidence is the happy path: the drafter cites a
// message that exists, and the fact lands.
func TestRunStoresFactsWithRealEvidence(t *testing.T) {
	archive := newKnowledgeArchive(t)
	userMessage, _ := archive.recordTurn(t, "happy", "gateway", "以后都用中文回答我", "好的，以后都用中文。", 1700000000000)
	drafter := &fakeDrafter{facts: []DraftFact{{
		Kind:       "preference",
		Statement:  "prefers Chinese answers",
		Status:     "observed",
		Scope:      "global",
		Confidence: 0.8,
		Evidence:   []string{userMessage},
	}}}
	svc := newTestService(t, archive, drafter, Config{})
	if err := svc.run(context.Background(), opencode.KnowledgeCandidateEvent{
		Trigger: opencode.KnowledgeTriggerHandoff, Adapter: "test", UserID: "owner",
		ThreadID: "thread-happy", SessionID: "session-happy",
	}); err != nil {
		t.Fatalf("run: %v", err)
	}
	facts, err := archive.store.ListKnowledgeFacts(context.Background(), testScope(), memstore.KnowledgeListOptions{})
	if err != nil {
		t.Fatalf("list facts: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("stored %d facts, want 1", len(facts))
	}
	if facts[0].Statement != "prefers Chinese answers" {
		t.Fatalf("stored statement %q", facts[0].Statement)
	}
	if len(facts[0].Evidence) != 1 || facts[0].Evidence[0] != userMessage {
		t.Fatalf("stored evidence %v, want [%s]", facts[0].Evidence, userMessage)
	}
	if facts[0].Origin != "extraction" {
		t.Fatalf("stored origin %q, want extraction", facts[0].Origin)
	}
}

// TestRunRejectsFabricatedEvidenceWithoutRetrying is the provenance gate at the
// service layer.
//
// The plan requires that unsupported claims are rejected and that "confidence is
// not proof". It also requires that extraction failures do not affect capture,
// which is why a rejection is not an error: the job completes, the fact is
// dropped, and nothing upstream notices.
func TestRunRejectsFabricatedEvidenceWithoutRetrying(t *testing.T) {
	archive := newKnowledgeArchive(t)
	archive.recordTurn(t, "fabricated", "gateway", "以后都用中文回答我", "好的。", 1700000000000)
	drafter := &fakeDrafter{facts: []DraftFact{{
		Kind:       "preference",
		Statement:  "prefers Chinese answers",
		Status:     "observed",
		Scope:      "global",
		Confidence: 1.0, // certainty does not substitute for evidence
		Evidence:   []string{"msg_that_was_never_captured"},
	}}}
	svc := newTestService(t, archive, drafter, Config{})
	err := svc.run(context.Background(), opencode.KnowledgeCandidateEvent{
		Trigger: opencode.KnowledgeTriggerHandoff, Adapter: "test", UserID: "owner",
		ThreadID: "thread-fabricated", SessionID: "session-fabricated",
	})
	if err != nil {
		t.Fatalf("a rejected fact must not fail the job (it would be retried to the same rejection): %v", err)
	}
	facts, err := archive.store.ListKnowledgeFacts(context.Background(), testScope(), memstore.KnowledgeListOptions{})
	if err != nil {
		t.Fatalf("list facts: %v", err)
	}
	if len(facts) != 0 {
		t.Fatalf("a fact with fabricated evidence was stored: %+v", facts)
	}
}

// TestRunDefaultsTheScopeToSession pins the safety default.
//
// A model that omits the scope must not get a global fact: turning a task-local
// instruction into a global preference is the failure the plan names explicitly
// ("task-local instructions do not become global preferences"). The safest
// reading of a missing scope is the narrowest one.
func TestRunDefaultsTheScopeToSession(t *testing.T) {
	archive := newKnowledgeArchive(t)
	userMessage, _ := archive.recordTurn(t, "scoped", "gateway", "这次先用英文写注释", "好的。", 1700000000000)
	drafter := &fakeDrafter{facts: []DraftFact{{
		Kind:      "constraint",
		Statement: "write comments in English",
		Evidence:  []string{userMessage},
		// Scope deliberately empty.
	}}}
	svc := newTestService(t, archive, drafter, Config{})
	if err := svc.run(context.Background(), opencode.KnowledgeCandidateEvent{
		Trigger: opencode.KnowledgeTriggerHandoff, Adapter: "test", UserID: "owner",
		ThreadID: "thread-scoped", SessionID: "session-scoped",
	}); err != nil {
		t.Fatalf("run: %v", err)
	}
	// IncludeSessionFacts because the assertion is about a session-scoped row:
	// the default listing deliberately hides those from the injection path.
	facts, err := archive.store.ListKnowledgeFacts(context.Background(), testScope(),
		memstore.KnowledgeListOptions{IncludeSessionFacts: true})
	if err != nil {
		t.Fatalf("list facts: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("stored %d facts, want 1", len(facts))
	}
	if facts[0].Scope != memstore.KnowledgeScopeSession {
		t.Fatalf("a fact with no declared scope became %q, want session", facts[0].Scope)
	}
	if facts[0].SessionID != "session-scoped" {
		t.Fatalf("session-scoped fact has session id %q", facts[0].SessionID)
	}
	// And the injection path must NOT see it: a session fact belongs to one
	// conversation, so injecting it into the next one would be a leak.
	visible, err := archive.store.ListKnowledgeFacts(context.Background(), testScope(), memstore.KnowledgeListOptions{})
	if err != nil {
		t.Fatalf("list visible: %v", err)
	}
	if len(visible) != 0 {
		t.Fatalf("a session-scoped fact is visible to the injection path: %+v", visible)
	}
}

// TestRunSupersedesWhenTheDrafterSaysSo pins that a correction is stored as a
// supersession rather than an overwrite, so the old fact stays auditable.
func TestRunSupersedesWhenTheDrafterSaysSo(t *testing.T) {
	archive := newKnowledgeArchive(t)
	userMessage, _ := archive.recordTurn(t, "correct", "gateway", "以后用英文回答我", "OK.", 1700000000000)
	ctx := context.Background()
	original, err := archive.writer.SaveKnowledgeFact(ctx, memstore.KnowledgeFact{
		PrincipalID: memstore.PrincipalFor(testScope()),
		Kind:        memstore.KnowledgeKindPreference,
		Statement:   "prefers Chinese answers",
		Status:      memstore.KnowledgeStatusObserved,
		Scope:       memstore.KnowledgeScopeGlobal,
		Origin:      "extraction",
		Evidence:    []string{userMessage},
	})
	if err != nil {
		t.Fatalf("seed original: %v", err)
	}
	drafter := &fakeDrafter{facts: []DraftFact{{
		Kind:       "preference",
		Statement:  "prefers English answers",
		Status:     "verified",
		Scope:      "global",
		Confidence: 0.9,
		Evidence:   []string{userMessage},
		Supersedes: original.ID,
	}}}
	svc := newTestService(t, archive, drafter, Config{})
	if err := svc.run(ctx, opencode.KnowledgeCandidateEvent{
		Trigger: opencode.KnowledgeTriggerHandoff, Adapter: "test", UserID: "owner",
		ThreadID: "thread-correct", SessionID: "session-correct",
	}); err != nil {
		t.Fatalf("run: %v", err)
	}
	current, err := archive.store.ListKnowledgeFacts(ctx, testScope(), memstore.KnowledgeListOptions{})
	if err != nil {
		t.Fatalf("list current: %v", err)
	}
	if len(current) != 1 || current[0].Statement != "prefers English answers" {
		t.Fatalf("current facts = %+v, want only the correction", current)
	}
	all, err := archive.store.ListKnowledgeFacts(ctx, testScope(), memstore.KnowledgeListOptions{IncludeSuperseded: true})
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("history has %d facts, want 2 (the correction must not overwrite the original)", len(all))
	}
}

// TestRunFailureLeavesCaptureAndSearchIntact is the isolation requirement.
//
// The plan's gate says "extraction failures do not affect capture or search".
// The strongest form of that test is not "the error was returned" but "the
// archive still answers the same query afterwards", so this asserts on search.
func TestRunFailureLeavesCaptureAndSearchIntact(t *testing.T) {
	archive := newKnowledgeArchive(t)
	archive.recordTurn(t, "isolation", "gateway",
		"how do I restart the gateway service",
		"Run systemctl restart opencode-gateway and check the journal.", 1700000000000)
	ctx := context.Background()
	if _, err := archive.writer.IndexPending(ctx, 100); err != nil {
		t.Fatalf("index: %v", err)
	}
	before, err := archive.store.Search(ctx, memstore.SearchOptions{Scope: testScope(), Query: "restart gateway"})
	if err != nil {
		t.Fatalf("search before: %v", err)
	}
	if len(before.Hits) == 0 {
		t.Fatalf("fixture is not searchable, so the isolation assertion would be vacuous")
	}

	svc := newTestService(t, archive, &fakeDrafter{err: errors.New("model is down")}, Config{})
	if err := svc.run(ctx, opencode.KnowledgeCandidateEvent{
		Trigger: opencode.KnowledgeTriggerHandoff, Adapter: "test", UserID: "owner",
		ThreadID: "thread-isolation", SessionID: "session-isolation",
	}); err == nil {
		t.Fatalf("a drafter failure should be reported so the job can be retried")
	}

	after, err := archive.store.Search(ctx, memstore.SearchOptions{Scope: testScope(), Query: "restart gateway"})
	if err != nil {
		t.Fatalf("search after: %v", err)
	}
	if len(after.Hits) != len(before.Hits) {
		t.Fatalf("search returned %d hits after a failed extraction, want %d", len(after.Hits), len(before.Hits))
	}
}

// TestRunWithNoEvidenceIsANoOp pins that a session with nothing captured does
// not spend a model call.
func TestRunWithNoEvidenceIsANoOp(t *testing.T) {
	archive := newKnowledgeArchive(t)
	drafter := &fakeDrafter{}
	svc := newTestService(t, archive, drafter, Config{})
	if err := svc.run(context.Background(), opencode.KnowledgeCandidateEvent{
		Trigger: opencode.KnowledgeTriggerHandoff, Adapter: "test", UserID: "owner",
		ThreadID: "thread-empty", SessionID: "session-that-does-not-exist",
	}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(drafter.calls) != 0 {
		t.Fatalf("the drafter was called %d times for a session with no evidence", len(drafter.calls))
	}
}

// TestDraftFallsBackThroughTheModelChain pins that a failing preferred model
// does not lose the extraction.
func TestDraftFallsBackThroughTheModelChain(t *testing.T) {
	archive := newKnowledgeArchive(t)
	drafter := &fakeDrafter{err: errors.New("model unavailable")}
	svc := newTestService(t, archive, drafter, Config{
		DraftModel:      "model-a",
		AlternateModels: []string{"model-b", "model-c"},
	})
	if _, err := svc.draft(context.Background(), DraftInput{ThreadID: "t"}); err == nil {
		t.Fatalf("draft should report failure when every model fails")
	}
	if len(drafter.calls) != 3 {
		t.Fatalf("tried %d models, want 3", len(drafter.calls))
	}
	for i, want := range []string{"model-a", "model-b", "model-c"} {
		if drafter.calls[i].ModelID != want {
			t.Fatalf("attempt %d used model %q, want %q", i, drafter.calls[i].ModelID, want)
		}
	}
}

// TestDraftStopsAtTheFirstSuccess pins that the chain is a fallback, not a
// fan-out: a working model must not be followed by two more calls.
func TestDraftStopsAtTheFirstSuccess(t *testing.T) {
	archive := newKnowledgeArchive(t)
	drafter := &fakeDrafter{facts: []DraftFact{{Statement: "x", Evidence: []string{"m"}}}}
	svc := newTestService(t, archive, drafter, Config{
		DraftModel:      "model-a",
		AlternateModels: []string{"model-b"},
	})
	out, err := svc.draft(context.Background(), DraftInput{ThreadID: "t"})
	if err != nil {
		t.Fatalf("draft: %v", err)
	}
	if len(drafter.calls) != 1 {
		t.Fatalf("tried %d models after a success, want 1", len(drafter.calls))
	}
	if out.ModelID != "model-a" {
		t.Fatalf("output model %q, want model-a", out.ModelID)
	}
}

// ---- index ----

// TestLoadKnowledgeIndexIsScopedAndBounded pins the session-start injection: it
// returns the rendered block for the scope, and nothing for a scope with no
// facts.
func TestLoadKnowledgeIndexIsScopedAndBounded(t *testing.T) {
	archive := newKnowledgeArchive(t)
	userMessage, _ := archive.recordTurn(t, "index", "gateway", "以后都用中文回答我", "好的。", 1700000000000)
	ctx := context.Background()
	if _, err := archive.writer.SaveKnowledgeFact(ctx, memstore.KnowledgeFact{
		PrincipalID: memstore.PrincipalFor(testScope()),
		Kind:        memstore.KnowledgeKindPreference,
		Statement:   "prefers Chinese answers",
		Status:      memstore.KnowledgeStatusVerified,
		Scope:       memstore.KnowledgeScopeGlobal,
		Origin:      "user",
		Evidence:    []string{userMessage},
	}); err != nil {
		t.Fatalf("seed fact: %v", err)
	}
	svc := newTestService(t, archive, &fakeDrafter{}, Config{})
	block := svc.LoadKnowledgeIndex("test", "owner", "gateway")
	if !strings.Contains(block, "prefers Chinese answers") {
		t.Fatalf("index block does not contain the fact: %q", block)
	}
	if other := svc.LoadKnowledgeIndex("test", "someone-else", "gateway"); other != "" {
		t.Fatalf("another principal's index is not empty: %q", other)
	}
}

// TestLoadKnowledgeIndexSurvivesANilStore pins that a service without a store
// degrades to no injection rather than panicking on the request path.
func TestLoadKnowledgeIndexSurvivesANilStore(t *testing.T) {
	svc := NewService(Config{}, nil, nil, nil)
	if block := svc.LoadKnowledgeIndex("test", "owner", "gateway"); block != "" {
		t.Fatalf("a storeless service returned %q", block)
	}
}

// ---- commands ----

// TestRememberProducesARealWriteReceipt is the plan's explicit requirement:
// "An assistant statement that it remembered something is not a successful
// write receipt." The reply must name a fact ID that actually exists.
func TestRememberProducesARealWriteReceipt(t *testing.T) {
	archive := newKnowledgeArchive(t)
	archive.recordTurn(t, "remember", "gateway", "以后都用中文回答我", "好的。", 1700000000000)
	svc := newTestService(t, archive, &fakeDrafter{}, Config{})

	reply, handled := svc.HandleCommand("test", "owner", "/remember 以后都用中文回答我")
	if !handled {
		t.Fatalf("/remember was not handled")
	}
	if !strings.Contains(reply, "✅") {
		t.Fatalf("reply does not report success: %q", reply)
	}
	facts, err := archive.store.ListKnowledgeFacts(context.Background(), testScope(), memstore.KnowledgeListOptions{})
	if err != nil {
		t.Fatalf("list facts: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("stored %d facts, want 1", len(facts))
	}
	// The receipt must name the row that exists, not a plausible-looking string.
	if !strings.Contains(reply, short(facts[0].ID)) {
		t.Fatalf("reply %q does not name the stored fact %s", reply, facts[0].ID)
	}
	if facts[0].Origin != "user" {
		t.Fatalf("a user-requested fact has origin %q, want user", facts[0].Origin)
	}
}

func TestRememberArchivesItsOwnEvidence(t *testing.T) {
	archive := newKnowledgeArchive(t)
	svc := newTestService(t, archive, &fakeDrafter{}, Config{})
	reply, handled := svc.HandleCommand("test", "owner", "/remember 以后都用中文回答我")
	if !handled {
		t.Fatalf("/remember was not handled")
	}
	if !strings.Contains(reply, "✅") {
		t.Fatalf("remember failed: %q", reply)
	}
	facts, err := archive.store.ListKnowledgeFacts(context.Background(), testScope(), memstore.KnowledgeListOptions{})
	if err != nil {
		t.Fatalf("list facts: %v", err)
	}
	if len(facts) != 1 {
		t.Fatalf("expected one fact: %+v", facts)
	}
	detail, err := archive.store.GetMessages(context.Background(), testScope(), facts[0].Evidence, 1024)
	if err != nil || len(detail.Messages) != 1 || detail.Messages[0].Text != "/remember 以后都用中文回答我" {
		t.Fatalf("incorrect command evidence: %+v, %v", detail, err)
	}
	if !strings.Contains(svc.cmdList("test", "owner"), facts[0].ID) {
		t.Fatal("list must show usable full ID")
	}
	if result := svc.cmdForget("test", "owner", facts[0].ID); !strings.Contains(result, "✅") {
		t.Fatalf("displayed ID cannot be deleted: %s", result)
	}
}

// TestForgetRemovesTheFactAndRestoresTheSupersededOne pins the deletion
// semantics the plan asks for: forgetting a correction brings the original back
// rather than leaving a hole.
func TestForgetRemovesTheFactAndRestoresTheSupersededOne(t *testing.T) {
	archive := newKnowledgeArchive(t)
	userMessage, _ := archive.recordTurn(t, "forget", "gateway", "以后用英文回答我", "OK.", 1700000000000)
	ctx := context.Background()
	original, err := archive.writer.SaveKnowledgeFact(ctx, memstore.KnowledgeFact{
		PrincipalID: memstore.PrincipalFor(testScope()),
		Kind:        memstore.KnowledgeKindPreference,
		Statement:   "prefers Chinese answers",
		Status:      memstore.KnowledgeStatusObserved,
		Scope:       memstore.KnowledgeScopeGlobal,
		Origin:      "extraction",
		Evidence:    []string{userMessage},
	})
	if err != nil {
		t.Fatalf("seed original: %v", err)
	}
	correction, err := archive.writer.SupersedeKnowledgeFact(ctx, original.ID, memstore.KnowledgeFact{
		PrincipalID: memstore.PrincipalFor(testScope()),
		Kind:        memstore.KnowledgeKindPreference,
		Statement:   "prefers English answers",
		Status:      memstore.KnowledgeStatusVerified,
		Scope:       memstore.KnowledgeScopeGlobal,
		Origin:      "user",
		Evidence:    []string{userMessage},
	})
	if err != nil {
		t.Fatalf("supersede: %v", err)
	}
	svc := newTestService(t, archive, &fakeDrafter{}, Config{})
	reply, handled := svc.HandleCommand("test", "owner", "/forget "+correction.ID)
	if !handled {
		t.Fatalf("/forget was not handled")
	}
	if !strings.Contains(reply, "✅") {
		t.Fatalf("reply does not report success: %q", reply)
	}
	current, err := archive.store.ListKnowledgeFacts(ctx, testScope(), memstore.KnowledgeListOptions{})
	if err != nil {
		t.Fatalf("list current: %v", err)
	}
	if len(current) != 1 || current[0].Statement != "prefers Chinese answers" {
		t.Fatalf("after forgetting the correction, current facts = %+v, want the original", current)
	}
}

// TestForgetResolvesAStatementToAnID pins that the user does not have to copy a
// hash to undo a fact.
func TestForgetResolvesAStatementToAnID(t *testing.T) {
	archive := newKnowledgeArchive(t)
	userMessage, _ := archive.recordTurn(t, "forget-stmt", "gateway", "以后都用中文回答我", "好的。", 1700000000000)
	ctx := context.Background()
	if _, err := archive.writer.SaveKnowledgeFact(ctx, memstore.KnowledgeFact{
		PrincipalID: memstore.PrincipalFor(testScope()),
		Kind:        memstore.KnowledgeKindPreference,
		Statement:   "prefers Chinese answers",
		Status:      memstore.KnowledgeStatusVerified,
		Scope:       memstore.KnowledgeScopeGlobal,
		Origin:      "user",
		Evidence:    []string{userMessage},
	}); err != nil {
		t.Fatalf("seed fact: %v", err)
	}
	svc := newTestService(t, archive, &fakeDrafter{}, Config{})
	reply, _ := svc.HandleCommand("test", "owner", "/forget prefers Chinese answers")
	if !strings.Contains(reply, "✅") {
		t.Fatalf("reply does not report success: %q", reply)
	}
	facts, err := archive.store.ListKnowledgeFacts(ctx, testScope(), memstore.KnowledgeListOptions{})
	if err != nil {
		t.Fatalf("list facts: %v", err)
	}
	if len(facts) != 0 {
		t.Fatalf("statement-based forget left %d facts", len(facts))
	}
}

// TestHandleCommandIgnoresUnrelatedMessages pins that the interceptor does not
// swallow ordinary conversation.
func TestHandleCommandIgnoresUnrelatedMessages(t *testing.T) {
	archive := newKnowledgeArchive(t)
	svc := newTestService(t, archive, &fakeDrafter{}, Config{})
	for _, message := range []string{"hello", "/skill-list", "/new", ""} {
		if reply, handled := svc.HandleCommand("test", "owner", message); handled {
			t.Fatalf("%q was handled as a knowledge command (reply %q)", message, reply)
		}
	}
}

// TestKnowledgeHelpListsEveryCommand pins that the help text does not drift from
// the dispatch switch.
func TestKnowledgeHelpListsEveryCommand(t *testing.T) {
	archive := newKnowledgeArchive(t)
	svc := newTestService(t, archive, &fakeDrafter{}, Config{})
	reply, handled := svc.HandleCommand("test", "owner", "/knowledge-help")
	if !handled {
		t.Fatalf("/knowledge-help was not handled")
	}
	for _, command := range []string{"/remember", "/forget", "/knowledge-list", "/knowledge-help"} {
		if !strings.Contains(reply, command) {
			t.Fatalf("help text does not mention %s:\n%s", command, reply)
		}
	}
}

// ---- parser ----

// TestParseExtractionReplyToleratesProseAndFences pins that the parser finds the
// array even when the model wraps it, because models do that regardless of the
// instruction not to.
func TestParseExtractionReplyToleratesProseAndFences(t *testing.T) {
	reply := "好的，这是抽取结果：\n```json\n[{\"kind\":\"preference\",\"statement\":\"prefers Chinese\",\"evidence\":[\"m1\"]}]\n```\n希望有帮助。"
	facts, err := parseExtractionReply(reply)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(facts) != 1 || facts[0].Statement != "prefers Chinese" {
		t.Fatalf("parsed %+v", facts)
	}
	if len(facts[0].Evidence) != 1 || facts[0].Evidence[0] != "m1" {
		t.Fatalf("parsed evidence %v", facts[0].Evidence)
	}
}

// TestParseExtractionReplyRejectsAReplyWithNoArray pins that a non-conforming
// reply is an error rather than an empty result. Returning zero facts would look
// like "nothing durable here" when the truth is "the extraction did not run".
func TestParseExtractionReplyRejectsAReplyWithNoArray(t *testing.T) {
	if _, err := parseExtractionReply("I could not find anything worth remembering."); err == nil {
		t.Fatalf("a reply with no JSON array was accepted")
	}
}

// TestParseExtractionReplyDropsStatementsWithoutText pins that an entry with no
// statement is skipped rather than stored as an empty fact.
func TestParseExtractionReplyDropsStatementsWithoutText(t *testing.T) {
	facts, err := parseExtractionReply(`[{"kind":"fact","statement":"  ","evidence":["m1"]},{"kind":"fact","statement":"real","evidence":["m1"]}]`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(facts) != 1 || facts[0].Statement != "real" {
		t.Fatalf("parsed %+v", facts)
	}
}

// TestBuildExtractionPromptCarriesTheEvidenceIDs pins the contract the whole
// design rests on: the prompt must show the model the message IDs it is allowed
// to cite, or every fact it produces will be rejected.
func TestBuildExtractionPromptCarriesTheEvidenceIDs(t *testing.T) {
	prompt := buildExtractionPrompt(DraftInput{
		Project:   "gateway",
		SessionID: "session-a",
		Conversation: []Turn{
			{Role: "user", Text: "以后都用中文回答我", MessageID: "msg-user-1"},
			{Role: "assistant", Text: "好的。", MessageID: "msg-answer-1"},
		},
		ExistingFacts: []string{"prefers Chinese answers"},
	})
	for _, want := range []string{"msg-user-1", "msg-answer-1", "gateway", "session-a", "prefers Chinese answers", "supersedes"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt does not mention %q", want)
		}
	}
}
