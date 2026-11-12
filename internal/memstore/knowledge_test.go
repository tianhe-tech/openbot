package memstore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// knowledgeFixture writes one turn for the owner and returns the two message
// IDs it created, so a fact can cite real evidence.
//
// The IDs are read back from the archive rather than reconstructed, because the
// archive derives them and a test that guessed would be asserting its own
// arithmetic instead of the store's.
func knowledgeFixture(t *testing.T, writer *ArchiveWriter) (userMessageID, answerMessageID string) {
	t.Helper()
	recordTurn(t, writer, turnSpec{
		Delivery: "knowledge", Project: "gateway", WorkDir: "/work/gateway",
		Request: "以后都用中文回答我", Answer: "好的，以后都用中文。",
		Created: 1700000000000,
	})
	key := archiveID("fixture", "knowledge")
	return "user-" + key, "answer-" + key
}

func ownerScope() SearchScope { return SearchScope{Adapter: "test", UserID: "owner"} }

func ownerPrincipal() string { return archiveID("principal", "test", "owner") }

func TestKnowledgeIdentityIncludesScope(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	evidence, _ := knowledgeFixture(t, writer)
	seen := make(map[string]bool)
	for _, project := range []string{"alpha", "beta"} {
		fact, err := writer.SaveKnowledgeFact(context.Background(), KnowledgeFact{
			PrincipalID: ownerPrincipal(), Kind: KnowledgeKindPreference, Statement: "use PostgreSQL",
			Status: KnowledgeStatusVerified, Scope: KnowledgeScopeProject, ProjectKey: project,
			Origin: "user", Evidence: []string{evidence},
		})
		if err != nil {
			t.Fatal(err)
		}
		if seen[fact.ID] {
			t.Fatal("different projects collided")
		}
		seen[fact.ID] = true
	}
}

func TestKnowledgeReusesLegacyScopedIdentity(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	evidence, _ := knowledgeFixture(t, writer)
	fact := KnowledgeFact{
		ID: "legacy-id", PrincipalID: ownerPrincipal(), Kind: KnowledgeKindPreference,
		Statement: "use PostgreSQL", Status: KnowledgeStatusVerified, Scope: KnowledgeScopeProject,
		ProjectKey: "alpha", Origin: "user", Evidence: []string{evidence},
	}
	if _, err := writer.SaveKnowledgeFact(context.Background(), fact); err != nil {
		t.Fatal(err)
	}
	fact.ID = ""
	saved, err := writer.SaveKnowledgeFact(context.Background(), fact)
	if err != nil || saved.ID != "legacy-id" {
		t.Fatalf("legacy identity duplicated: %+v %v", saved, err)
	}
}

// TestSaveKnowledgeFactRejectsForeignEvidence is the provenance gate.
//
// The plan requires that "evidence IDs belong to the source principal and
// actually exist" and that "unsupported claims are rejected; confidence is not
// proof". Both halves are tested here: a message that exists under another
// principal, and a message that does not exist at all. The confidence is set to
// 1.0 in both cases to make the point that certainty does not substitute for
// evidence.
func TestSaveKnowledgeFactRejectsForeignEvidence(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	ownUser, _ := knowledgeFixture(t, writer)
	recordTurn(t, writer, turnSpec{
		Delivery: "other", Project: "crawler", WorkDir: "/work/crawler",
		Request: "别人的对话", Answer: "别人的回答", Created: 1700000100000, UserID: "someone-else",
	})
	otherKey := archiveID("fixture", "other")
	otherUser := "user-" + otherKey

	for _, testCase := range []struct {
		name     string
		evidence []string
	}{
		{"another principal's message", []string{otherUser}},
		{"a message that does not exist", []string{"msg_does_not_exist"}},
		{"one valid and one foreign", []string{ownUser, otherUser}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := writer.SaveKnowledgeFact(ctx, KnowledgeFact{
				PrincipalID: ownerPrincipal(),
				Kind:        KnowledgeKindPreference,
				Statement:   "prefers Chinese answers",
				Status:      KnowledgeStatusObserved,
				Scope:       KnowledgeScopeGlobal,
				Confidence:  1.0,
				Origin:      "extraction",
				Evidence:    testCase.evidence,
			})
			if !errors.Is(err, ErrKnowledgeEvidenceInvalid) {
				t.Fatalf("evidence %v was accepted: %v", testCase.evidence, err)
			}
		})
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_knowledge_facts`); count != 0 {
		t.Fatalf("rejected facts left %d rows behind", count)
	}
}

// TestSaveKnowledgeFactRequiresEvidence pins that a fact with no evidence is
// refused before any SQL runs. A fact with an empty evidence list would pass
// the per-message check vacuously, which is the shape of a gate that looks
// present and is not.
func TestSaveKnowledgeFactRequiresEvidence(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	_, err := writer.SaveKnowledgeFact(context.Background(), KnowledgeFact{
		PrincipalID: ownerPrincipal(),
		Kind:        KnowledgeKindFact,
		Statement:   "the gateway runs on port 8080",
		Status:      KnowledgeStatusObserved,
		Scope:       KnowledgeScopeGlobal,
		Origin:      "extraction",
	})
	if !errors.Is(err, ErrKnowledgeFactInvalid) {
		t.Fatalf("a fact with no evidence was accepted: %v", err)
	}
}

// TestSaveKnowledgeFactIsIdempotent pins that re-extracting the same statement
// lands on the same row. Extraction is retried, so a non-idempotent write would
// turn one retry into two facts.
func TestSaveKnowledgeFactIsIdempotent(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	userMessage, _ := knowledgeFixture(t, writer)
	fact := KnowledgeFact{
		PrincipalID: ownerPrincipal(),
		Kind:        KnowledgeKindPreference,
		Statement:   "prefers Chinese answers",
		Status:      KnowledgeStatusObserved,
		Scope:       KnowledgeScopeGlobal,
		Confidence:  0.6,
		Origin:      "extraction",
		Evidence:    []string{userMessage},
	}
	first, err := writer.SaveKnowledgeFact(ctx, fact)
	if err != nil {
		t.Fatal(err)
	}
	// Different whitespace, same statement: the identity is normalized.
	fact.Statement = "  prefers   Chinese\nanswers "
	fact.Confidence = 0.9
	second, err := writer.SaveKnowledgeFact(ctx, fact)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("re-saving produced a second fact: %s vs %s", first.ID, second.ID)
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_knowledge_facts`); count != 1 {
		t.Fatalf("expected 1 fact, got %d", count)
	}
	facts, err := writer.store.ListKnowledgeFacts(ctx, ownerScope(), KnowledgeListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].Confidence != 0.9 {
		t.Fatalf("re-save did not refresh confidence: %+v", facts)
	}
}

// TestSupersedeKnowledgeFactKeepsHistory is the correction gate.
//
// The plan requires that "user corrections supersede old facts" and that
// corrections are stored "as supersession rather than overwriting historical
// evidence". So after a correction there must be two rows: the old one, still
// carrying its original statement and evidence, and the new one, current.
func TestSupersedeKnowledgeFactKeepsHistory(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	userMessage, answerMessage := knowledgeFixture(t, writer)
	original, err := writer.SaveKnowledgeFact(ctx, KnowledgeFact{
		PrincipalID: ownerPrincipal(),
		Kind:        KnowledgeKindPreference,
		Statement:   "prefers English answers",
		Status:      KnowledgeStatusObserved,
		Scope:       KnowledgeScopeGlobal,
		Confidence:  0.5,
		Origin:      "extraction",
		Evidence:    []string{userMessage},
	})
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := writer.SupersedeKnowledgeFact(ctx, original.ID, KnowledgeFact{
		PrincipalID: ownerPrincipal(),
		Kind:        KnowledgeKindPreference,
		Statement:   "prefers Chinese answers",
		Status:      KnowledgeStatusVerified,
		Scope:       KnowledgeScopeGlobal,
		Confidence:  1.0,
		Origin:      "user",
		Evidence:    []string{answerMessage},
	})
	if err != nil {
		t.Fatal(err)
	}

	current, err := writer.store.ListKnowledgeFacts(ctx, ownerScope(), KnowledgeListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(current) != 1 || current[0].ID != replacement.ID {
		t.Fatalf("current facts should be only the replacement, got %+v", current)
	}
	history, err := writer.store.ListKnowledgeFacts(ctx, ownerScope(), KnowledgeListOptions{IncludeSuperseded: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 {
		t.Fatalf("history should hold both facts, got %d", len(history))
	}
	var foundOriginal bool
	for _, fact := range history {
		if fact.ID != original.ID {
			continue
		}
		foundOriginal = true
		if fact.Statement != "prefers English answers" {
			t.Fatalf("the superseded fact's statement was overwritten: %q", fact.Statement)
		}
		if fact.SupersededBy != replacement.ID {
			t.Fatalf("superseded_by is %q, want %q", fact.SupersededBy, replacement.ID)
		}
		if len(fact.Evidence) != 1 || fact.Evidence[0] != userMessage {
			t.Fatalf("the superseded fact lost its evidence: %v", fact.Evidence)
		}
	}
	if !foundOriginal {
		t.Fatal("the superseded fact is gone; corrections must not overwrite history")
	}
}

// TestSupersedeKnowledgeFactRefusesToFork pins that a second, different
// correction of the same fact is refused. Allowing it would leave two facts
// claiming to be the successor of one, and a reader following the chain would
// have to pick.
func TestSupersedeKnowledgeFactRefusesToFork(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	userMessage, _ := knowledgeFixture(t, writer)
	base := KnowledgeFact{
		PrincipalID: ownerPrincipal(), Kind: KnowledgeKindFact,
		Statement: "the gateway runs on port 8080", Status: KnowledgeStatusObserved,
		Scope: KnowledgeScopeGlobal, Origin: "extraction", Evidence: []string{userMessage},
	}
	original, err := writer.SaveKnowledgeFact(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	first := base
	first.Statement = "the gateway runs on port 9090"
	if _, err := writer.SupersedeKnowledgeFact(ctx, original.ID, first); err != nil {
		t.Fatal(err)
	}
	second := base
	second.Statement = "the gateway runs on port 7070"
	if _, err := writer.SupersedeKnowledgeFact(ctx, original.ID, second); !errors.Is(err, ErrKnowledgeAlreadySuperseded) {
		t.Fatalf("a forked correction was accepted: %v", err)
	}
	// Retrying the same correction is idempotent, not a fork.
	if _, err := writer.SupersedeKnowledgeFact(ctx, original.ID, first); err != nil {
		t.Fatalf("retrying the same correction failed: %v", err)
	}
}

// TestKnowledgeScopeKeepsTaskLocalFactsLocal is the gate the plan names
// explicitly: "task-local instructions do not become global preferences".
//
// A project-scoped fact must be invisible from a scope that names no project,
// and invisible from a different project. The global fact in the same fixture
// proves the filter is a filter and not a blanket exclusion.
func TestKnowledgeScopeKeepsTaskLocalFactsLocal(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	userMessage, _ := knowledgeFixture(t, writer)
	base := KnowledgeFact{
		PrincipalID: ownerPrincipal(), Kind: KnowledgeKindConstraint,
		Status: KnowledgeStatusObserved, Origin: "extraction", Evidence: []string{userMessage},
	}
	global := base
	global.Statement = "always answer in Chinese"
	global.Scope = KnowledgeScopeGlobal
	if _, err := writer.SaveKnowledgeFact(ctx, global); err != nil {
		t.Fatal(err)
	}
	project := base
	project.Statement = "never touch the production database in this project"
	project.Scope = KnowledgeScopeProject
	project.ProjectKey = "gateway"
	if _, err := writer.SaveKnowledgeFact(ctx, project); err != nil {
		t.Fatal(err)
	}

	for _, testCase := range []struct {
		name      string
		scope     SearchScope
		wantCount int
		wantLocal bool
	}{
		{"no project named", ownerScope(), 1, false},
		{"the owning project", SearchScope{Adapter: "test", UserID: "owner", Project: "gateway"}, 2, true},
		{"a different project", SearchScope{Adapter: "test", UserID: "owner", Project: "crawler"}, 1, false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			facts, err := writer.store.ListKnowledgeFacts(ctx, testCase.scope, KnowledgeListOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if len(facts) != testCase.wantCount {
				t.Fatalf("got %d facts, want %d: %+v", len(facts), testCase.wantCount, facts)
			}
			var sawLocal bool
			for _, fact := range facts {
				if fact.Scope == KnowledgeScopeProject {
					sawLocal = true
				}
			}
			if sawLocal != testCase.wantLocal {
				t.Fatalf("project fact visibility is %v, want %v", sawLocal, testCase.wantLocal)
			}
		})
	}
}

// TestKnowledgeIndexRespectsTheByteBudget pins the session-start budget.
//
// The plan asks for "up to 1,500 tokens using an available tokenizer or
// conservative byte budget". The budget is bytes here, so the assertion is that
// the rendered text never exceeds it and that truncation is reported rather
// than silent.
func TestKnowledgeIndexRespectsTheByteBudget(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	userMessage, _ := knowledgeFixture(t, writer)
	for index := 0; index < 20; index++ {
		if _, err := writer.SaveKnowledgeFact(ctx, KnowledgeFact{
			PrincipalID: ownerPrincipal(),
			Kind:        KnowledgeKindFact,
			Statement:   "fact number " + strings.Repeat("x", 40) + " " + string(rune('a'+index)),
			Status:      KnowledgeStatusObserved,
			Scope:       KnowledgeScopeGlobal,
			Origin:      "extraction",
			Evidence:    []string{userMessage},
		}); err != nil {
			t.Fatal(err)
		}
	}
	const budget = 400
	index, err := writer.store.KnowledgeIndex(ctx, ownerScope(), budget)
	if err != nil {
		t.Fatal(err)
	}
	if index.Bytes > budget {
		t.Fatalf("index is %d bytes, over the %d budget", index.Bytes, budget)
	}
	if !index.Truncated {
		t.Fatal("20 facts of 50 bytes cannot fit in 400 bytes; truncation was not reported")
	}
	if len(index.Facts) == 0 {
		t.Fatal("the budget was spent on nothing")
	}
	if index.Text == "" {
		t.Fatal("a non-empty index rendered no text")
	}
}

// TestKnowledgeIndexIsEmptyWithoutFacts pins that an archive with no knowledge
// produces no preamble at all. A header with no facts would tell the model
// there is knowledge when there is none, and it would be injected on every
// turn.
func TestKnowledgeIndexIsEmptyWithoutFacts(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	index, err := writer.store.KnowledgeIndex(context.Background(), ownerScope(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if index.Text != "" || index.Bytes != 0 || len(index.Facts) != 0 {
		t.Fatalf("empty archive produced an index: %+v", index)
	}
	if index.ByteBudget != knowledgeIndexDefaultBytes {
		t.Fatalf("default budget is %d, want %d", index.ByteBudget, knowledgeIndexDefaultBytes)
	}
}

// TestKnowledgeIndexPrefersTheMostSpecificFacts pins the ordering that makes
// truncation safe. When the budget runs out, what survives must be the
// knowledge that applies to the conversation actually starting.
func TestKnowledgeIndexPrefersTheMostSpecificFacts(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	userMessage, _ := knowledgeFixture(t, writer)
	base := KnowledgeFact{
		PrincipalID: ownerPrincipal(), Kind: KnowledgeKindFact,
		Status: KnowledgeStatusObserved, Origin: "extraction", Evidence: []string{userMessage},
	}
	global := base
	global.Statement = "global fact " + strings.Repeat("g", 60)
	global.Scope = KnowledgeScopeGlobal
	if _, err := writer.SaveKnowledgeFact(ctx, global); err != nil {
		t.Fatal(err)
	}
	project := base
	project.Statement = "project fact " + strings.Repeat("p", 60)
	project.Scope = KnowledgeScopeProject
	project.ProjectKey = "gateway"
	if _, err := writer.SaveKnowledgeFact(ctx, project); err != nil {
		t.Fatal(err)
	}
	// A budget that fits the header plus one fact, so exactly one survives.
	// The header is charged against the budget, so a budget that ignores it
	// would fit nothing and the test would fail for a reason unrelated to the
	// ordering it is checking.
	budget := len(knowledgeIndexHeader) + 120
	index, err := writer.store.KnowledgeIndex(ctx,
		SearchScope{Adapter: "test", UserID: "owner", Project: "gateway"}, budget)
	if err != nil {
		t.Fatal(err)
	}
	if len(index.Facts) != 1 {
		t.Fatalf("expected exactly one fact to fit in %d bytes, got %d", budget, len(index.Facts))
	}
	if index.Facts[0].Scope != KnowledgeScopeProject {
		t.Fatalf("the surviving fact is %q, want the project-scoped one", index.Facts[0].Scope)
	}
}

// TestForgetKnowledgeFactRestoresTheSupersededFact pins the interaction between
// forgetting and supersession.
//
// If the user forgets a correction, the statement it replaced becomes current
// again. Leaving the pointer in place would leave a current-looking fact
// pointing at a row that no longer exists, which the self-referencing foreign
// key would reject anyway.
func TestForgetKnowledgeFactRestoresTheSupersededFact(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	userMessage, _ := knowledgeFixture(t, writer)
	base := KnowledgeFact{
		PrincipalID: ownerPrincipal(), Kind: KnowledgeKindPreference,
		Status: KnowledgeStatusObserved, Scope: KnowledgeScopeGlobal,
		Origin: "extraction", Evidence: []string{userMessage},
	}
	original := base
	original.Statement = "prefers English answers"
	saved, err := writer.SaveKnowledgeFact(ctx, original)
	if err != nil {
		t.Fatal(err)
	}
	replacement := base
	replacement.Statement = "prefers Chinese answers"
	correction, err := writer.SupersedeKnowledgeFact(ctx, saved.ID, replacement)
	if err != nil {
		t.Fatal(err)
	}

	removed, err := writer.ForgetKnowledgeFact(ctx, ownerScope(), correction.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatal("forget reported nothing removed")
	}
	facts, err := writer.store.ListKnowledgeFacts(ctx, ownerScope(), KnowledgeListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 || facts[0].ID != saved.ID {
		t.Fatalf("the original fact should be current again, got %+v", facts)
	}
	if facts[0].SupersededBy != "" {
		t.Fatalf("the original fact still points at a deleted correction: %q", facts[0].SupersededBy)
	}
}

// TestForgetKnowledgeFactIsScoped pins that forgetting cannot reach another
// principal's fact by ID. The ID is a content hash, so guessing one is not
// far-fetched; the principal predicate is what makes it useless.
func TestForgetKnowledgeFactIsScoped(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	userMessage, _ := knowledgeFixture(t, writer)
	saved, err := writer.SaveKnowledgeFact(ctx, KnowledgeFact{
		PrincipalID: ownerPrincipal(), Kind: KnowledgeKindFact,
		Statement: "the gateway runs on port 8080", Status: KnowledgeStatusObserved,
		Scope: KnowledgeScopeGlobal, Origin: "extraction", Evidence: []string{userMessage},
	})
	if err != nil {
		t.Fatal(err)
	}
	removed, err := writer.ForgetKnowledgeFact(ctx,
		SearchScope{Adapter: "test", UserID: "someone-else"}, saved.ID)
	if err != nil {
		t.Fatal(err)
	}
	if removed {
		t.Fatal("another principal's fact was removed by ID")
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_knowledge_facts`); count != 1 {
		t.Fatalf("the fact was removed anyway: %d rows left", count)
	}
}

// TestDeleteScopeRemovesKnowledgeWithItsEvidence is the §13 obligation.
//
// The plan recorded that derived facts "must be added to the same transaction
// when PR 7 lands, or a deletion will leave knowledge rows pointing at messages
// that no longer exist". This asserts both halves: the fact is gone, and the
// evidence rows are gone with it, so no row references a deleted message.
func TestDeleteScopeRemovesKnowledgeWithItsEvidence(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	userMessage, answerMessage := knowledgeFixture(t, writer)
	recordTurn(t, writer, turnSpec{
		Delivery: "other", Project: "crawler", WorkDir: "/work/crawler",
		Request: "别人的对话", Answer: "别人的回答", Created: 1700000100000, UserID: "someone-else",
	})
	otherKey := archiveID("fixture", "other")
	otherUser := "user-" + otherKey

	if _, err := writer.SaveKnowledgeFact(ctx, KnowledgeFact{
		PrincipalID: ownerPrincipal(), Kind: KnowledgeKindPreference,
		Statement: "prefers Chinese answers", Status: KnowledgeStatusObserved,
		Scope: KnowledgeScopeGlobal, Origin: "extraction",
		Evidence: []string{userMessage, answerMessage},
	}); err != nil {
		t.Fatal(err)
	}
	otherPrincipal := archiveID("principal", "test", "someone-else")
	if _, err := writer.SaveKnowledgeFact(ctx, KnowledgeFact{
		PrincipalID: otherPrincipal, Kind: KnowledgeKindFact,
		Statement: "the crawler uses 8 workers", Status: KnowledgeStatusObserved,
		Scope: KnowledgeScopeGlobal, Origin: "extraction", Evidence: []string{otherUser},
	}); err != nil {
		t.Fatal(err)
	}

	report, err := writer.DeleteScope(ctx, DeleteOptions{Scope: ownerScope()})
	if err != nil {
		t.Fatal(err)
	}
	if report.Knowledge != 1 {
		t.Fatalf("report counted %d knowledge facts, want 1", report.Knowledge)
	}
	if count := deleteCountForTest(t, writer,
		`SELECT COUNT(*) FROM mem_v2_knowledge_facts WHERE principal_id=?`, ownerPrincipal()); count != 0 {
		t.Fatalf("%d facts survived the deletion", count)
	}
	if count := deleteCountForTest(t, writer,
		`SELECT COUNT(*) FROM mem_v2_knowledge_evidence WHERE principal_id=?`, ownerPrincipal()); count != 0 {
		t.Fatalf("%d evidence rows survived the deletion", count)
	}
	// The other principal's fact must be untouched, and its evidence must still
	// resolve to a message that exists.
	if count := deleteCountForTest(t, writer,
		`SELECT COUNT(*) FROM mem_v2_knowledge_facts WHERE principal_id=?`, otherPrincipal); count != 1 {
		t.Fatalf("the surviving principal lost its fact: %d rows", count)
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_knowledge_evidence e
		WHERE NOT EXISTS (SELECT 1 FROM mem_v2_messages m WHERE m.id=e.message_id)`); count != 0 {
		t.Fatalf("%d evidence rows point at messages that no longer exist", count)
	}
}

// TestDeleteScopeRemovesKnowledgeJobsForItsSource pins that a queued extraction
// does not outlive the thing it would have extracted from. A surviving job
// would call the model against a deleted session and fail on every retry.
func TestDeleteScopeRemovesKnowledgeJobsForItsSource(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	knowledgeFixture(t, writer)
	sessionID := sessionIDForAttemptForKnowledge(t, writer)
	if err := writer.EnqueueKnowledgeJob(ctx, KnowledgeJob{
		PrincipalID: ownerPrincipal(), SourceKind: KnowledgeSourceSession, SourceID: sessionID,
	}); err != nil {
		t.Fatal(err)
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_knowledge_jobs`); count != 1 {
		t.Fatalf("fixture did not enqueue a job: %d rows", count)
	}
	report, err := writer.DeleteScope(ctx, DeleteOptions{Scope: ownerScope()})
	if err != nil {
		t.Fatal(err)
	}
	if report.Jobs == 0 {
		t.Fatal("the deletion did not count the knowledge job it removed")
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_knowledge_jobs`); count != 0 {
		t.Fatalf("%d knowledge jobs survived their source", count)
	}
}

// sessionIDForAttemptForKnowledge resolves the archive's session ID for the
// fixture's only turn.
func sessionIDForAttemptForKnowledge(t *testing.T, writer *ArchiveWriter) string {
	t.Helper()
	var sessionID string
	if err := writer.store.db.QueryRow(`SELECT session_id FROM mem_v2_messages
		WHERE principal_id=? LIMIT 1`, ownerPrincipal()).Scan(&sessionID); err != nil {
		t.Fatalf("resolve fixture session: %v", err)
	}
	return sessionID
}

// TestEnqueueKnowledgeJobCollapsesRepeats pins the mechanism behind "do not
// call an LLM for every message": the same source enqueued twice is one job.
func TestEnqueueKnowledgeJobCollapsesRepeats(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	knowledgeFixture(t, writer)
	sessionID := sessionIDForAttemptForKnowledge(t, writer)
	for attempt := 0; attempt < 3; attempt++ {
		if err := writer.EnqueueKnowledgeJob(ctx, KnowledgeJob{
			PrincipalID: ownerPrincipal(), SourceKind: KnowledgeSourceSession, SourceID: sessionID,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if count := deleteCountForTest(t, writer, `SELECT COUNT(*) FROM mem_v2_knowledge_jobs`); count != 1 {
		t.Fatalf("three enqueues produced %d jobs, want 1", count)
	}
}

// TestClaimKnowledgeJobsLeasesOnce pins that two claims cannot return the same
// job. The lease is written in the same transaction that selects, so the second
// claim sees a job that is not yet due.
func TestClaimKnowledgeJobsLeasesOnce(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	knowledgeFixture(t, writer)
	sessionID := sessionIDForAttemptForKnowledge(t, writer)
	if err := writer.EnqueueKnowledgeJob(ctx, KnowledgeJob{
		PrincipalID: ownerPrincipal(), SourceKind: KnowledgeSourceSession, SourceID: sessionID,
	}); err != nil {
		t.Fatal(err)
	}
	first, err := writer.ClaimKnowledgeJobs(ctx, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 {
		t.Fatalf("first claim returned %d jobs, want 1", len(first))
	}
	second, err := writer.ClaimKnowledgeJobs(ctx, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 0 {
		t.Fatalf("the same job was claimed twice: %+v", second)
	}
	if err := writer.CompleteKnowledgeJob(ctx, first[0].ID); err != nil {
		t.Fatal(err)
	}
	third, err := writer.ClaimKnowledgeJobs(ctx, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(third) != 0 {
		t.Fatalf("a completed job was claimed again: %+v", third)
	}
}

// TestFailKnowledgeJobParksAfterMaxAttempts pins that a permanently broken
// source stops consuming the model budget.
func TestFailKnowledgeJobParksAfterMaxAttempts(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	knowledgeFixture(t, writer)
	sessionID := sessionIDForAttemptForKnowledge(t, writer)
	if err := writer.EnqueueKnowledgeJob(ctx, KnowledgeJob{
		PrincipalID: ownerPrincipal(), SourceKind: KnowledgeSourceSession, SourceID: sessionID,
	}); err != nil {
		t.Fatal(err)
	}
	claimed, err := writer.ClaimKnowledgeJobs(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claim returned %d jobs", len(claimed))
	}
	jobID := claimed[0].ID
	for attempt := 0; attempt < knowledgeJobMaxAttempts; attempt++ {
		if err := writer.FailKnowledgeJob(ctx, jobID, errors.New("drafter unavailable")); err != nil {
			t.Fatal(err)
		}
	}
	var status string
	var attempts int
	if err := writer.store.db.QueryRow(`SELECT status,attempts FROM mem_v2_knowledge_jobs WHERE id=?`, jobID).
		Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != KnowledgeJobFailed {
		t.Fatalf("job status is %q after %d failures, want %q", status, attempts, KnowledgeJobFailed)
	}
	if attempts != knowledgeJobMaxAttempts {
		t.Fatalf("attempts is %d, want %d", attempts, knowledgeJobMaxAttempts)
	}
}

// TestKnowledgeFactValidationRejectsMalformedFacts covers the field checks that
// would otherwise reach SQL and fail there with a less useful message.
func TestKnowledgeFactValidationRejectsMalformedFacts(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	userMessage, _ := knowledgeFixture(t, writer)
	valid := KnowledgeFact{
		PrincipalID: ownerPrincipal(), Kind: KnowledgeKindFact,
		Statement: "the gateway runs on port 8080", Status: KnowledgeStatusObserved,
		Scope: KnowledgeScopeGlobal, Origin: "extraction", Evidence: []string{userMessage},
	}
	for _, testCase := range []struct {
		name   string
		mutate func(*KnowledgeFact)
	}{
		{"no principal", func(f *KnowledgeFact) { f.PrincipalID = "" }},
		{"unknown kind", func(f *KnowledgeFact) { f.Kind = "rumour" }},
		{"unknown status", func(f *KnowledgeFact) { f.Status = "maybe" }},
		{"unknown scope", func(f *KnowledgeFact) { f.Scope = "galaxy" }},
		{"empty statement", func(f *KnowledgeFact) { f.Statement = "   " }},
		{"statement too long", func(f *KnowledgeFact) { f.Statement = strings.Repeat("x", knowledgeStatementMaxBytes+1) }},
		{"confidence above one", func(f *KnowledgeFact) { f.Confidence = 1.5 }},
		{"confidence below zero", func(f *KnowledgeFact) { f.Confidence = -0.1 }},
		{"no origin", func(f *KnowledgeFact) { f.Origin = "" }},
		{"project scope without a key", func(f *KnowledgeFact) { f.Scope = KnowledgeScopeProject }},
		{"session scope without an id", func(f *KnowledgeFact) { f.Scope = KnowledgeScopeSession }},
		{"empty evidence entry", func(f *KnowledgeFact) { f.Evidence = []string{"  "} }},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			fact := valid
			fact.Evidence = append([]string{}, valid.Evidence...)
			testCase.mutate(&fact)
			if _, err := writer.SaveKnowledgeFact(ctx, fact); !errors.Is(err, ErrKnowledgeFactInvalid) {
				t.Fatalf("malformed fact was accepted: %v", err)
			}
		})
	}
}

// TestCountKnowledgeFactsSinceIsScoped pins that the daily cap cannot be
// influenced by another principal's extraction volume.
func TestCountKnowledgeFactsSinceIsScoped(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	userMessage, _ := knowledgeFixture(t, writer)
	recordTurn(t, writer, turnSpec{
		Delivery: "other", Project: "crawler", WorkDir: "/work/crawler",
		Request: "别人的对话", Answer: "别人的回答", Created: 1700000100000, UserID: "someone-else",
	})
	otherKey := archiveID("fixture", "other")
	otherPrincipal := archiveID("principal", "test", "someone-else")
	if _, err := writer.SaveKnowledgeFact(ctx, KnowledgeFact{
		PrincipalID: otherPrincipal, Kind: KnowledgeKindFact,
		Statement: "the crawler uses 8 workers", Status: KnowledgeStatusObserved,
		Scope: KnowledgeScopeGlobal, Origin: "extraction", Evidence: []string{"user-" + otherKey},
	}); err != nil {
		t.Fatal(err)
	}
	count, err := writer.store.CountKnowledgeFactsSince(ctx, ownerScope(), time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("the owner's count includes another principal's fact: %d", count)
	}
	if _, err := writer.SaveKnowledgeFact(ctx, KnowledgeFact{
		PrincipalID: ownerPrincipal(), Kind: KnowledgeKindPreference,
		Statement: "prefers Chinese answers", Status: KnowledgeStatusObserved,
		Scope: KnowledgeScopeGlobal, Origin: "extraction", Evidence: []string{userMessage},
	}); err != nil {
		t.Fatal(err)
	}
	count, err = writer.store.CountKnowledgeFactsSince(ctx, ownerScope(), time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("the owner's count is %d after one fact, want 1", count)
	}
}

// TestSessionFactsAreReviewableButNotInjectable pins the one place the review
// path and the injection path are allowed to disagree.
//
// A session-scoped fact belongs to the principal, so the user must be able to
// list and forget it — otherwise the row is written, never shown, and never
// deletable, which is the opposite of the plan's "user-requested forgetting
// removes derived facts". But it belongs to one conversation, so injecting it
// into the next one would leak a task-local instruction into a different task.
// The two behaviours are one option apart, and this test pins both.
func TestSessionFactsAreReviewableButNotInjectable(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	userMessage, _ := knowledgeFixture(t, writer)
	sessionID := sessionIDForAttemptForKnowledge(t, writer)
	if _, err := writer.SaveKnowledgeFact(ctx, KnowledgeFact{
		PrincipalID: ownerPrincipal(), Kind: KnowledgeKindConstraint,
		Statement: "write comments in English for this task",
		Status:    KnowledgeStatusObserved, Scope: KnowledgeScopeSession,
		SessionID: sessionID, Origin: "extraction", Evidence: []string{userMessage},
	}); err != nil {
		t.Fatal(err)
	}

	injectable, err := writer.store.ListKnowledgeFacts(ctx, ownerScope(), KnowledgeListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(injectable) != 0 {
		t.Fatalf("a session fact is visible to the injection path: %+v", injectable)
	}
	reviewable, err := writer.store.ListKnowledgeFacts(ctx, ownerScope(), KnowledgeListOptions{IncludeSessionFacts: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(reviewable) != 1 {
		t.Fatalf("the review path sees %d session facts, want 1", len(reviewable))
	}
	// The index is the injection path, so it must agree with the default list.
	index, err := writer.store.KnowledgeIndex(ctx, ownerScope(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(index.Text, "write comments in English") {
		t.Fatalf("the session-start index injected a session fact:\n%s", index.Text)
	}
	// And the fact must still be forgettable, which is the whole reason the
	// review path includes it.
	removed, err := writer.ForgetKnowledgeFact(ctx, ownerScope(), reviewable[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatalf("a session fact could not be forgotten")
	}
}

// TestKnowledgeJobScopeReadsTheScopeBackFromTheSession pins that a job row does
// not carry a copy of the scope.
//
// The job stores only (principal, source_kind, source_id) because that triple is
// what makes it unique. The adapter, user, and project are properties of the
// session, so they are read back — a copy would be a second place for them to be
// wrong, and a job that outlived a rename would extract under a stale scope.
func TestKnowledgeJobScopeReadsTheScopeBackFromTheSession(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	knowledgeFixture(t, writer)
	sessionID := sessionIDForAttemptForKnowledge(t, writer)
	scope, err := writer.store.KnowledgeJobScope(ctx, KnowledgeJob{
		PrincipalID: ownerPrincipal(), SourceKind: KnowledgeSourceSession, SourceID: sessionID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if scope.Adapter != "test" || scope.UserID != "owner" || scope.Project != "gateway" {
		t.Fatalf("resolved scope %+v, want test/owner/gateway", scope)
	}
	// A source that no longer exists is not a transient failure: retrying cannot
	// bring the session back, so the caller must be able to tell the difference.
	if _, err := writer.store.KnowledgeJobScope(ctx, KnowledgeJob{
		PrincipalID: ownerPrincipal(), SourceKind: KnowledgeSourceSession, SourceID: "session-that-is-gone",
	}); !errors.Is(err, ErrKnowledgeEvidenceInvalid) {
		t.Fatalf("a missing source returned %v, want ErrKnowledgeEvidenceInvalid", err)
	}
}

// TestLatestMessageIDIsScopedAndHonest pins the evidence /remember cites.
//
// The command interceptor runs before the inbound message is archived, so the
// newest message is the previous turn rather than the command itself. That is
// still real evidence, and the point of the test is that it is real: the ID must
// resolve to a row under the same principal, and a scope with nothing captured
// must report that rather than inventing an ID.
func TestLatestMessageIDIsScopedAndHonest(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	_, answerMessage := knowledgeFixture(t, writer)
	latest, err := writer.store.LatestMessageID(ctx, ownerScope())
	if err != nil {
		t.Fatal(err)
	}
	if latest != answerMessage {
		t.Fatalf("latest message is %s, want the newest one %s", latest, answerMessage)
	}
	// The ID must be usable as evidence, which is the only thing that makes the
	// receipt honest.
	if _, err := writer.SaveKnowledgeFact(ctx, KnowledgeFact{
		PrincipalID: ownerPrincipal(), Kind: KnowledgeKindPreference,
		Statement: "prefers Chinese answers", Status: KnowledgeStatusVerified,
		Scope: KnowledgeScopeGlobal, Origin: "user", Evidence: []string{latest},
	}); err != nil {
		t.Fatalf("the cited message is not usable as evidence: %v", err)
	}
	// Another principal has captured nothing, so there is nothing to cite.
	if _, err := writer.store.LatestMessageID(ctx, SearchScope{Adapter: "test", UserID: "someone-else"}); !errors.Is(err, ErrKnowledgeEvidenceInvalid) {
		t.Fatalf("an empty scope returned %v, want ErrKnowledgeEvidenceInvalid", err)
	}
}

// TestRecentEvidenceReturnsTheTailInChronologicalOrder pins the extraction
// input.
//
// The prompt must read like the conversation, so the rows come back oldest-first
// even though the query selects newest-first. A test that only checked the count
// would pass against a reversed prompt, which is the failure that matters: a
// model reading the conversation backwards will attribute the answer to the
// question.
func TestRecentEvidenceReturnsTheTailInChronologicalOrder(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	knowledgeFixture(t, writer)
	sessionID := sessionIDForAttemptForKnowledge(t, writer)
	evidence, err := writer.store.RecentEvidence(ctx, ownerScope(), sessionID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence) != 2 {
		t.Fatalf("read %d evidence messages, want 2", len(evidence))
	}
	if evidence[0].Role != "user" || evidence[1].Role != "assistant" {
		t.Fatalf("evidence order is %s then %s, want user then assistant", evidence[0].Role, evidence[1].Role)
	}
	if evidence[0].Text != "以后都用中文回答我" {
		t.Fatalf("first evidence text is %q", evidence[0].Text)
	}
	if evidence[0].MessageID == "" {
		t.Fatalf("evidence has no message id, so the drafter could not cite it")
	}
	// The limit selects the tail, not the head: asking for one message must
	// return the newest one.
	tail, err := writer.store.RecentEvidence(ctx, ownerScope(), sessionID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(tail) != 1 || tail[0].Role != "assistant" {
		t.Fatalf("limit 1 returned %+v, want only the newest message", tail)
	}
	// Another principal must not be able to read this session's evidence.
	if _, err := writer.store.RecentEvidence(ctx, SearchScope{Adapter: "test", UserID: "someone-else"}, sessionID, 10); err != nil {
		t.Fatalf("a foreign scope errored instead of returning nothing: %v", err)
	} else {
		foreign, _ := writer.store.RecentEvidence(ctx, SearchScope{Adapter: "test", UserID: "someone-else"}, sessionID, 10)
		if len(foreign) != 0 {
			t.Fatalf("a foreign scope read %d messages from another principal's session", len(foreign))
		}
	}
}
