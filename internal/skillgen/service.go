// Package skillgen implements Hermes-style automatic skill generation: after a
// completed conversation (long session or post-handoff), it mines the
// exchange for a reusable procedure and drafts a SKILL.md — always off the
// hot path, idle-gated so it never interferes with active user Q&A.
//
// High level:
//
//	opencode.Client fires SkillCandidateEvent
//	                        │
//	     ┌──────────────────┴──────────────────┐
//	     │ skillgen.Service.OnSkillCandidate() │   (non-blocking, enqueue)
//	     └──────────────────┬──────────────────┘
//	                        │
//	         asyncwork.Queue job (single goroutine)
//	                        │
//	     ┌──────────────────┴──────────────────┐
//	     │ 1. dedup + daily cap                │
//	     │ 2. pick model (epsilon-greedy)      │
//	     │ 3. drafter.Draft → SKILL.md text    │
//	     │    (dedicated session, no idle-wait) │
//	     │ 4. persist as pending_review        │
//	     │ 5. installer.WriteDraft → file      │
//	     │ 6. notifier.Notify → adapter        │
//	     └─────────────────────────────────────┘
//
// Review happens via slash commands intercepted in opencode.Client.SendMessage
// (see commands.go): /skill-list /skill-view /skill-approve /skill-reject.
package skillgen

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/user/opencode-gateway/internal/asyncwork"
	"github.com/user/opencode-gateway/internal/memstore"
	"github.com/user/opencode-gateway/internal/opencode"
)

// Config controls skill-autogen behavior. Zero values mean "disabled" / safe default.
type Config struct {
	Enabled bool
	// DraftModel is the preferred model id (e.g. "anthropic/claude-sonnet-4-5"),
	// ignored when AlternateModels list is non-empty and ModelSelfSelect is true.
	DraftModel string
	// AlternateModels is a pool from which the Service may explore via
	// epsilon-greedy when ModelSelfSelect is on.
	AlternateModels []string
	// Epsilon in [0, 1]: random-explore probability for model selection.
	Epsilon float64
	// ModelSelfSelect: if false, always use DraftModel.
	ModelSelfSelect bool
	// MaxPerDay caps total candidates created in any rolling 24h window. 0 = disabled.
	MaxPerDay int
	// OnHandoff enables mining from stuck-session handoffs.
	OnHandoff bool
	// OnLongSession enables mining from long successful sessions.
	OnLongSession bool
	// LongSessionMinTurns gates the long-session trigger.
	LongSessionMinTurns int
	// MinToolCalls gates the long-session trigger: the session must have
	// completed at least this many tool calls before it is worth mining.
	// This is more reliable than LongSessionMinTurns because a single-turn
	// PDF→Excel task with 5 tool calls is valuable, while a 10-message
	// pure-chat thread is not. 0 disables; default 3.
	MinToolCalls int
	// CandidateDir is where SKILL.md drafts are written for review.
	// Defaults to "skills-candidates" under CWD.
	CandidateDir string
	// InstallDir is where approved skills are moved to.
	// Defaults to "skills" under CWD.
	InstallDir string
	// ApprovalRequired: if false, drafts are auto-approved and installed directly.
	ApprovalRequired bool
	// MinConfidence gates drafts below this heuristic score (0..1). 0 accepts all.
	MinConfidence float64
}

// DefaultConfig returns a conservative, disabled-by-default config.
func DefaultConfig() Config {
	return Config{
		Enabled:             false,
		DraftModel:          "",
		Epsilon:             0.15,
		ModelSelfSelect:     true,
		MaxPerDay:           5,
		OnHandoff:           true,
		OnLongSession:       true,
		LongSessionMinTurns: 0, // disabled in favour of MinToolCalls
		MinToolCalls:        3, // >=3 completed tool calls required
		CandidateDir:        "skills-candidates",
		InstallDir:          "skills",
		ApprovalRequired:    true,
		MinConfidence:       0.4,
	}
}

// Drafter generates a SKILL.md draft from a captured conversation.
type Drafter interface {
	Draft(ctx context.Context, in DraftInput) (DraftOutput, error)
}

// DraftInput is the evidence a Drafter works with.
type DraftInput struct {
	Trigger  string
	Adapter  string
	UserID   string
	ThreadID string
	// Conversation is a chronological list of {role, text} turns.
	Conversation []Turn
	// ModelID selected by the Service (may be "" when Drafter decides).
	ModelID string
	// ExistingSkillTitles is the list of already-installed skill directory names
	// (read from InstallDir before calling Draft). The drafter uses this to
	// decide whether to PATCH an existing skill rather than create a duplicate.
	ExistingSkillTitles []string
}

// Turn is one role-tagged utterance.
type Turn struct {
	Role string // "user" or "assistant"
	Text string
}

// DraftOutput is what a Drafter produces.
type DraftOutput struct {
	Title       string  // short slug, used for directory name (kebab-case)
	SkillMD     string  // full SKILL.md content including frontmatter
	Score       float64 // self-confidence [0, 1]
	ModelID     string  // model actually used
	Action      string  // "create" or "patch"
	PatchTarget string  // slug of the existing skill to update (when Action=="patch")
}

// Notifier pushes a notification back to the originating adapter that a
// skill candidate is awaiting review.
type Notifier interface {
	NotifyCandidate(adapter, userID, candidateID, title string, approvalRequired bool) error
}

// Service orchestrates candidate mining on a background queue.
type Service struct {
	cfg      Config
	store    *memstore.Store
	client   *opencode.Client
	queue    *asyncwork.Queue
	drafter  Drafter
	notifier Notifier
	// dedup: best-effort per-thread recent-fire cache to avoid mining the same
	// thread repeatedly on successive long-session ticks.
	recent sync.Map // map[threadID]time.Time
}

// NewService wires dependencies. queue and drafter MUST be non-nil when cfg.Enabled is true.
func NewService(cfg Config, store *memstore.Store, client *opencode.Client, queue *asyncwork.Queue, drafter Drafter, notifier Notifier) *Service {
	if cfg.CandidateDir == "" {
		cfg.CandidateDir = "skills-candidates"
	}
	if cfg.InstallDir == "" {
		cfg.InstallDir = "skills"
	}
	return &Service{cfg: cfg, store: store, client: client, queue: queue, drafter: drafter, notifier: notifier}
}

// Config returns the effective config (used by command handlers to branch on settings).
func (s *Service) Config() Config {
	if s == nil {
		return Config{}
	}
	return s.cfg
}

// OnSkillCandidate is the opencode.SkillCandidateHook entry point.
// Never blocks: it validates, dedups, and enqueues the mining job.
func (s *Service) OnSkillCandidate(event opencode.SkillCandidateEvent) {
	if s == nil || !s.cfg.Enabled || s.queue == nil || s.drafter == nil {
		return
	}
	// Never mine candidates from skillgen's own drafting sessions.
	if event.Adapter == "skillgen" || strings.HasPrefix(event.ThreadID, "skillgen-draft-") {
		return
	}
	// Per-trigger gating.
	switch event.Trigger {
	case opencode.SkillTriggerHandoff:
		if !s.cfg.OnHandoff {
			return
		}
	case opencode.SkillTriggerLongSession:
		if !s.cfg.OnLongSession {
			return
		}
		// MinToolCalls gate (primary): session must have completed enough tool calls
		// to represent a real procedural task, not just Q&A or file browsing.
		if s.cfg.MinToolCalls > 0 && event.ToolCallCount < s.cfg.MinToolCalls {
			return
		}
		// LongSessionMinTurns gate (secondary, backward-compat): only applied when
		// MinToolCalls is disabled (0) and caller still relies on turn count.
		if s.cfg.MinToolCalls == 0 && s.cfg.LongSessionMinTurns > 0 && event.TurnCount < s.cfg.LongSessionMinTurns {
			return
		}
	default:
		return
	}
	// Dedup: same thread, same tool-call decile (every 10 completions creates a
	// new segment) within 30 min → skip. This avoids re-mining the same task on
	// each tick while still allowing re-trigger when the session moves into a
	// meaningfully different phase (10+ more tool calls completed).
	dedupKey := fmt.Sprintf("%s:%d", event.ThreadID, event.ToolCallCount/10)
	if prev, ok := s.recent.Load(dedupKey); ok {
		if t, ok2 := prev.(time.Time); ok2 && time.Since(t) < 30*time.Minute {
			return
		}
	}
	s.recent.Store(dedupKey, time.Now())

	// Daily cap: check cheaply before we enqueue.
	if s.cfg.MaxPerDay > 0 && s.store != nil {
		if n, err := s.store.CountSkillCandidatesSince(time.Now().Add(-24 * time.Hour)); err == nil && n >= s.cfg.MaxPerDay {
			log.Printf("skillgen: daily cap reached (%d), skipping %s for thread %s", n, event.Trigger, event.ThreadID)
			return
		}
	}

	ev := event // capture
	s.queue.Enqueue(asyncwork.JobFunc{
		Label: fmt.Sprintf("skillgen:%s:%s", ev.Trigger, ev.ThreadID),
		Fn: func(ctx context.Context) error {
			return s.run(ctx, ev)
		},
		Dur: 10 * time.Minute, // drafter uses a dedicated session; give it more headroom
	})
}

// run is the full mining pipeline: fetch turns → draft → persist → install → notify.
func (s *Service) run(ctx context.Context, ev opencode.SkillCandidateEvent) error {
	// Drafter uses a dedicated session (thread "skillgen-draft-<threadID>"),
	// so it does not interfere with the user's active Q&A. No idle-wait needed.
	log.Printf("skillgen: pipeline start — trigger=%s thread=%s session=%s adapter=%s user=%s",
		ev.Trigger, ev.ThreadID, ev.SessionID, ev.Adapter, ev.UserID)

	// 1. Fetch conversation turns.
	turns, err := s.fetchTurns(ctx, ev.SessionID)
	if err != nil || len(turns) == 0 {
		if err != nil {
			log.Printf("skillgen: STEP1 fetch turns failed for %s: %v", ev.SessionID, err)
		} else {
			log.Printf("skillgen: STEP1 fetch turns returned 0 turns for %s (nothing to mine)", ev.SessionID)
		}
		return err
	}
	log.Printf("skillgen: STEP1 fetched %d turns from session %s", len(turns), ev.SessionID)

	// 2. Pick model.
	model := s.pickModel()
	log.Printf("skillgen: STEP2 picked model=%q (selfSelect=%t draftModel=%s alternates=%v)", model, s.cfg.ModelSelfSelect, s.cfg.DraftModel, s.cfg.AlternateModels)
	if s.store != nil && model != "" {
		_ = s.store.RecordModelAttempt(model)
	}

	// Build the fallback chain: start with the picked model, then all other
	// models from the pool that haven't been tried yet.
	fallbackModels := s.buildFallbackChain(model)

	// 3. Read existing installed skills from disk (zero LLM cost) so the drafter
	// can decide to PATCH an existing skill instead of creating a duplicate.
	var existingSkills []string
	if s.cfg.InstallDir != "" {
		if entries, err := os.ReadDir(s.cfg.InstallDir); err == nil {
			for _, e := range entries {
				if e.IsDir() {
					existingSkills = append(existingSkills, e.Name())
				}
			}
			log.Printf("skillgen: STEP3 read %d existing skills from %s", len(existingSkills), s.cfg.InstallDir)
		} else {
			log.Printf("skillgen: STEP3 read existing skills failed from %s: %v (proceeding without patch context)", s.cfg.InstallDir, err)
		}
	}

	// 4. Draft — try the picked model first, then fall back to alternates
	// if the send fails (e.g. model unavailable, 500 from server).
	draftCtx, cancel := context.WithTimeout(ctx, 8*time.Minute)
	defer cancel()

	var out DraftOutput
	var draftErr error
	triedModels := map[string]bool{}

	for _, tryModel := range fallbackModels {
		if triedModels[tryModel] {
			continue
		}
		triedModels[tryModel] = true
		log.Printf("skillgen: STEP4 drafting (model=%s, timeout=8m)...", tryModel)
		out, draftErr = s.drafter.Draft(draftCtx, DraftInput{
			Trigger:             string(ev.Trigger),
			Adapter:             ev.Adapter,
			UserID:              ev.UserID,
			ThreadID:            ev.ThreadID,
			Conversation:        turns,
			ModelID:             tryModel,
			ExistingSkillTitles: existingSkills,
		})
		if draftErr == nil {
			break
		}
		log.Printf("skillgen: STEP4 draft failed (model=%s thread=%s): %v", tryModel, ev.ThreadID, draftErr)
		// If there are more models to try, continue; otherwise give up.
		if len(fallbackModels) > 1 {
			log.Printf("skillgen: STEP4 falling back to next model...")
		}
	}
	if draftErr != nil {
		return draftErr
	}
	log.Printf("skillgen: STEP4 draft completed (title=%s score=%.2f action=%s)", out.Title, out.Score, out.Action)
	if strings.TrimSpace(out.SkillMD) == "" || strings.TrimSpace(out.Title) == "" {
		log.Printf("skillgen: STEP4 drafter returned empty output for thread %s (title=%q skillMD_len=%d)", ev.ThreadID, out.Title, len(out.SkillMD))
		return nil
	}
	if s.cfg.MinConfidence > 0 && out.Score < s.cfg.MinConfidence {
		log.Printf("skillgen: STEP4 draft score %.2f < minConfidence %.2f, dropping (title=%s)", out.Score, s.cfg.MinConfidence, out.Title)
		return nil
	}

	// 5. Persist candidate row.
	status := memstore.SkillStatusPendingReview
	if !s.cfg.ApprovalRequired {
		status = memstore.SkillStatusApproved
	}
	c := memstore.SkillCandidate{
		ID:        memstore.NewSkillCandidateID(),
		Trigger:   string(ev.Trigger),
		Adapter:   ev.Adapter,
		UserID:    ev.UserID,
		ThreadID:  ev.ThreadID,
		SessionID: ev.SessionID,
		Status:    status,
		Score:     out.Score,
		ModelID:   out.ModelID,
		Title:     out.Title,
		SkillMD:   out.SkillMD,
	}

	// 6. Write to disk: when ApprovalRequired is true, always write to
	// CandidateDir for review (even for patch actions). When auto-approving,
	// write directly to InstallDir.
	targetDir := s.cfg.CandidateDir
	if !s.cfg.ApprovalRequired {
		targetDir = s.cfg.InstallDir
	}
	log.Printf("skillgen: STEP5 writing skill file — targetDir=%s title=%s action=%s", targetDir, out.Title, out.Action)
	path, werr := writeSkillFile(targetDir, out.Title, out.SkillMD)
	if werr != nil {
		log.Printf("skillgen: STEP5 write skill file failed: %v", werr)
		// Still persist the candidate so user can retrieve via /skill-view.
	} else {
		c.DraftPath = path
		log.Printf("skillgen: STEP5 skill file written to %s", path)
	}

	if s.store != nil {
		if err := s.store.SaveSkillCandidate(c); err != nil {
			log.Printf("skillgen: STEP5 save candidate failed: %v", err)
			return err
		}
		log.Printf("skillgen: STEP5 candidate persisted id=%s status=%s", c.ID, c.Status)
	}

	// When auto-approving, record a positive outcome so model stats accumulate.
	if !s.cfg.ApprovalRequired && s.store != nil && out.ModelID != "" {
		_ = s.store.RecordModelOutcome(out.ModelID, true, out.Score)
	}

	// 7. Notify.
	if s.notifier != nil && ev.Adapter != "" && ev.UserID != "" {
		log.Printf("skillgen: STEP6 notifying user (adapter=%s user=%s candidate=%s)", ev.Adapter, ev.UserID, c.ID)
		if err := s.notifier.NotifyCandidate(ev.Adapter, ev.UserID, c.ID, c.Title, s.cfg.ApprovalRequired); err != nil {
			log.Printf("skillgen: STEP6 notifier failed (adapter=%s user=%s): %v", ev.Adapter, ev.UserID, err)
		}
	}

	log.Printf("skillgen: ✅ pipeline done — candidate %s created (title=%q status=%s score=%.2f model=%s path=%s)",
		c.ID, c.Title, c.Status, c.Score, c.ModelID, c.DraftPath)
	return nil
}

// fetchTurns retrieves the session message history via the opencode client.
// Uses the already-exposed client helper (added below via export).
func (s *Service) fetchTurns(ctx context.Context, sessionID string) ([]Turn, error) {
	if s.client == nil || sessionID == "" {
		return nil, nil
	}
	raw, err := s.client.FetchSessionTurns(ctx, sessionID)
	if err != nil {
		return nil, err
	}
	turns := make([]Turn, 0, len(raw))
	for _, t := range raw {
		turns = append(turns, Turn{Role: t.Role, Text: t.Text})
	}
	return turns, nil
}

// pickModel selects a model using memstore's epsilon-greedy helper when
// ModelSelfSelect is on and stats are available; falls back to DraftModel.
func (s *Service) pickModel() string {
	if !s.cfg.ModelSelfSelect || s.store == nil {
		return s.cfg.DraftModel
	}
	return s.store.PickSkillGenModel(s.cfg.AlternateModels, s.cfg.DraftModel, s.cfg.Epsilon)
}

// buildFallbackChain returns an ordered list of models to try for drafting.
// The first element is the picked model; subsequent elements are the other
// models from the pool (DraftModel + AlternateModels), deduplicated. This
// allows the pipeline to fall back to an alternate model when the primary
// pick fails (e.g. model unavailable, 500 error).
func (s *Service) buildFallbackChain(picked string) []string {
	seen := map[string]bool{}
	chain := []string{}
	add := func(m string) {
		m = strings.TrimSpace(m)
		if m == "" || seen[m] {
			return
		}
		seen[m] = true
		chain = append(chain, m)
	}
	add(picked)
	add(s.cfg.DraftModel)
	for _, m := range s.cfg.AlternateModels {
		add(m)
	}
	return chain
}

// writeSkillFile writes SKILL.md under <root>/<slug>/SKILL.md and returns the path.
func writeSkillFile(root, title, content string) (string, error) {
	slug := slugify(title)
	if slug == "" {
		return "", fmt.Errorf("skillgen: empty title")
	}
	dir := filepath.Join(root, slug)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "SKILL.md")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return "", err
	}
	return path, nil
}

// slugify converts a title to a kebab-case directory name.
func slugify(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	var b strings.Builder
	prevDash := false
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevDash = false
		case r == ' ', r == '-', r == '_':
			if !prevDash && b.Len() > 0 {
				b.WriteByte('-')
				prevDash = true
			}
		}
	}
	out := strings.Trim(b.String(), "-")
	if len(out) > 60 {
		out = out[:60]
	}
	return out
}
