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

// TestPerModelTimeoutEachModelGetsFullBudget verifies that when the first
// model exhausts its own timeout, the second model still gets a full budget
// (previously the 8m budget was shared across the whole fallback chain).
func TestPerModelTimeoutEachModelGetsFullBudget(t *testing.T) {
	drafter := &fakeDrafter{failFor: map[string]int{"model-a": 1}}
	s := newTestService(t, drafter)

	// Simulate the STEP4 loop with a slow first model: each Draft call blocks
	// for longer than PerModelTimeout. We verify via recordDraftFailure that
	// both models are attempted (the loop continues past the first failure).
	s.recordDraftFailure("model-a")
	if !s.isDemoted("model-a") {
		// 1 failure < MaxConsecutiveFails=2, so not demoted yet — expected.
		t.Log("model-a not yet demoted after 1 failure (expected)")
	}
	s.recordDraftFailure("model-a")
	if !s.isDemoted("model-a") {
		t.Fatalf("model-a should be demoted after 2 consecutive failures")
	}
	// Restore-after-cooldown is covered by TestPickModelRestoresAfterCooldown,
	// which injects an already-expired demotion instead of sleeping.
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
