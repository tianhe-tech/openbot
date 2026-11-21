package skillgen

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeDrafter fails for models in failFor and succeeds otherwise.
type fakeDrafter struct {
	failFor map[string]int // modelID → how many times to fail before succeeding
	calls   []string
}

func (f *fakeDrafter) Draft(ctx context.Context, in DraftInput) (DraftOutput, error) {
	f.calls = append(f.calls, in.ModelID)
	if n, ok := f.failFor[in.ModelID]; ok && n > 0 {
		f.failFor[in.ModelID]--
		return DraftOutput{}, errors.New("simulated draft failure")
	}
	return DraftOutput{
		Title:   "test-skill",
		SkillMD: "---\nname: test-skill\ndescription: test\n---\nbody",
		Score:   0.9,
		ModelID: in.ModelID,
		Action:  "create",
	}, nil
}

func newTestService(t *testing.T, drafter Drafter) *Service {
	t.Helper()
	cfg := Config{
		Enabled:             true,
		DraftModel:          "model-a",
		AlternateModels:     []string{"model-a", "model-b"},
		ModelSelfSelect:     false, // deterministic: always DraftModel first
		PerModelTimeout:     50 * time.Millisecond,
		MaxConsecutiveFails: 2,
		Cooldown:            10 * time.Second, // long enough to avoid flaky expiry under load
		MinConfidence:       0.4,
	}
	return NewService(cfg, nil, nil, nil, drafter, nil)
}

// TestPickModelSkipsDemotedModel verifies pickModel falls back to a healthy
// alternate when the DraftModel is demoted.
func TestPickModelSkipsDemotedModel(t *testing.T) {
	drafter := &fakeDrafter{}
	s := newTestService(t, drafter)

	s.mu.Lock()
	s.modelFails["model-a"] = 2
	s.demotedAt["model-a"] = time.Now()
	s.mu.Unlock()

	picked := s.pickModel()
	if picked != "model-b" {
		t.Fatalf("pickModel should skip demoted model-a, got %q", picked)
	}
}

// TestPickModelSkipsDemotedSelfSelectPick verifies the self-select path also
// moves off a demoted pick instead of returning it unchanged.
func TestPickModelSkipsDemotedSelfSelectPick(t *testing.T) {
	drafter := &fakeDrafter{}
	s := newTestService(t, drafter)
	s.cfg.ModelSelfSelect = true

	s.mu.Lock()
	s.modelFails["model-a"] = 2
	s.demotedAt["model-a"] = time.Now()
	s.mu.Unlock()

	picked := s.pickModel()
	if picked == "model-a" {
		t.Fatal("pickModel returned the demoted model it just logged as unhealthy")
	}
	if picked != "model-b" {
		t.Fatalf("pickModel should select the healthy alternate, got %q", picked)
	}
}

// TestSlugifyFallsBackForNonASCIITitles verifies a Chinese-only title still
// produces a usable, deterministic directory name.
func TestSlugifyFallsBackForNonASCIITitles(t *testing.T) {
	const title = "从对话中提取可复用流程"
	first := slugify(title)
	if first == "" {
		t.Fatal("slugify returned an empty slug for a non-ASCII title")
	}
	if !strings.HasPrefix(first, "skill-") {
		t.Fatalf("expected a hash-based fallback slug, got %q", first)
	}
	if second := slugify(title); second != first {
		t.Fatalf("slugify is not deterministic: %q != %q", first, second)
	}
	if other := slugify("另一个中文标题"); other == first {
		t.Fatalf("distinct titles collided on slug %q", first)
	}
	if got := slugify("PDF to Excel"); got != "pdf-to-excel" {
		t.Fatalf("ASCII slug changed: %q", got)
	}
	if got := slugify("从 PDF 提取表格到 Excel"); got != "pdf-excel" {
		t.Fatalf("mixed title should keep its ASCII parts, got %q", got)
	}
	if got := slugify("   "); got != "" {
		t.Fatalf("blank title should stay empty, got %q", got)
	}
}

// TestJobBudgetCoversFullFallbackChain verifies the async job timeout is large
// enough for every model in the chain to get its full per-model budget.
func TestJobBudgetCoversFullFallbackChain(t *testing.T) {
	drafter := &fakeDrafter{}
	s := newTestService(t, drafter)
	s.cfg.PerModelTimeout = 8 * time.Minute

	chain := s.buildFallbackChain(s.cfg.DraftModel)
	if got, want := s.jobBudget(), s.cfg.PerModelTimeout*time.Duration(len(chain)); got < want {
		t.Fatalf("job budget %v cannot cover %d models at %v each", got, len(chain), s.cfg.PerModelTimeout)
	}
}

// TestPickModelRestoresAfterCooldown verifies a demoted model returns to
// rotation once its cooldown elapses.
func TestPickModelRestoresAfterCooldown(t *testing.T) {
	drafter := &fakeDrafter{}
	s := newTestService(t, drafter)

	s.mu.Lock()
	s.modelFails["model-a"] = 2
	s.demotedAt["model-a"] = time.Now().Add(-11 * time.Second) // already expired (cooldown=10s)
	s.mu.Unlock()

	picked := s.pickModel()
	if picked != "model-a" {
		t.Fatalf("pickModel should restore model-a after cooldown, got %q", picked)
	}
}

// TestRecordDraftFailureDemotesAfterThreshold verifies the demotion threshold.
func TestRecordDraftFailureDemotesAfterThreshold(t *testing.T) {
	drafter := &fakeDrafter{}
	s := newTestService(t, drafter)

	s.recordDraftFailure("model-a")
	if s.isDemoted("model-a") {
		t.Fatalf("model-a should not be demoted after 1 failure (threshold=2)")
	}
	s.recordDraftFailure("model-a")
	if !s.isDemoted("model-a") {
		t.Fatalf("model-a should be demoted after 2 failures")
	}
	// A different model is unaffected.
	s.recordDraftFailure("model-b")
	if s.isDemoted("model-b") {
		t.Fatalf("model-b should not be demoted after 1 failure")
	}
}

// TestRecordDraftSuccessClearsFailures verifies success resets the counter.
func TestRecordDraftSuccessClearsFailures(t *testing.T) {
	drafter := &fakeDrafter{}
	s := newTestService(t, drafter)

	s.recordDraftFailure("model-a")
	s.recordDraftSuccess("model-a")
	s.recordDraftFailure("model-a")
	if s.isDemoted("model-a") {
		t.Fatalf("model-a should not be demoted: success reset the counter")
	}
}

// TestCapConversationKeepsRecentTurns verifies the conversation cap keeps the
// most recent turns and prepends a marker.
func TestCapConversationKeepsRecentTurns(t *testing.T) {
	turns := make([]Turn, 0, maxDraftTurns+10)
	for i := 0; i < maxDraftTurns+10; i++ {
		turns = append(turns, Turn{Role: "user", Text: strings.Repeat("x", 10)})
	}
	capped := capConversation(turns)
	if len(capped) != maxDraftTurns+1 {
		t.Fatalf("capped conversation should have %d turns (marker + %d), got %d",
			maxDraftTurns+1, maxDraftTurns, len(capped))
	}
	if !strings.Contains(capped[0].Text, "已省略") {
		t.Fatalf("first capped turn should be a drop marker, got %q", capped[0].Text)
	}
	// Under the cap: unchanged.
	small := []Turn{{Role: "user", Text: "hi"}, {Role: "assistant", Text: "hello"}}
	if got := capConversation(small); len(got) != 2 {
		t.Fatalf("small conversation should be unchanged, got %d turns", len(got))
	}
}

// TestTruncatePromptCapsLength verifies the hard prompt cap.
func TestTruncatePromptCapsLength(t *testing.T) {
	big := strings.Repeat("a", maxDraftPromptChars+5000)
	got := truncatePrompt(big)
	if len(got) > maxDraftPromptChars+200 { // cap + truncation marker
		t.Fatalf("prompt should be capped near %d, got %d", maxDraftPromptChars, len(got))
	}
	if !strings.Contains(got, "prompt truncated") {
		t.Fatalf("truncated prompt should carry a marker")
	}
	if got := truncatePrompt("short"); got != "short" {
		t.Fatalf("short prompt should be unchanged, got %q", got)
	}
}
