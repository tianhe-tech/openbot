// Package knowledge implements source-backed durable knowledge: after a
// completed unit of work, it asks a model to extract durable statements from
// the captured conversation, and stores only the ones whose claimed evidence
// actually exists in the archive under the same principal.
//
// High level:
//
//	opencode.Client fires KnowledgeCandidateEvent
//	                        │
//	     ┌──────────────────┴──────────────────┐
//	     │ knowledge.Service.OnKnowledgeCandidate() │  (non-blocking, enqueue)
//	     └──────────────────┬──────────────────┘
//	                        │
//	     mem_v2_knowledge_jobs (durable, survives restart)
//	                        │
//	     ┌──────────────────┴──────────────────┐
//	     │ Service.Run: claim → lease → run    │
//	     │ 1. dedup + daily cap                │
//	     │ 2. read the tail of the session     │
//	     │ 3. drafter.Draft → structured facts │
//	     │    (dedicated session, no idle-wait) │
//	     │ 4. validate each fact's evidence    │
//	     │ 5. SaveKnowledgeFact / Supersede    │
//	     └─────────────────────────────────────┘
//
// The design constraint that shapes everything here is that a fact is not
// evidence. A model can assert anything; the archive can only vouch for what it
// captured. So the drafter is asked for message IDs alongside every statement,
// and a statement whose IDs do not check out is dropped rather than stored with
// a low confidence. "Confidence is not proof" — a number between 0 and 1 does
// not make an unsupported claim supported, it only makes it look measured.
//
// Two consequences follow, and both are deliberate:
//
//   - A rejected fact is not a retry. The evidence will not appear on the next
//     attempt, so retrying spends the model budget to reach the same rejection.
//     The job completes and the rejection is logged.
//   - Extraction failure never affects capture or search. The service runs on
//     its own durable job queue, writes through the same durable writer, and
//     returns errors to nobody who is waiting. A model outage costs facts, not
//     messages.
//
// Review and control happen via slash commands intercepted in
// opencode.Client.SendMessage (see commands.go): /remember /forget
// /knowledge-list /knowledge-help.
package knowledge

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/user/opencode-gateway/internal/memstore"
	"github.com/user/opencode-gateway/internal/opencode"
)

// Config controls knowledge extraction. Zero values mean "disabled" / safe
// default.
type Config struct {
	// Enabled turns automatic extraction on. Explicit /remember works even when
	// this is false: the user asking for a fact to be stored is not the same
	// feature as the gateway deciding to extract one.
	Enabled bool
	// DraftModel is the preferred model id for extraction.
	DraftModel string
	// AlternateModels is the fallback pool tried in order when DraftModel fails.
	AlternateModels []string
	// MaxPerDay caps facts created in any rolling 24h window. 0 disables.
	MaxPerDay int
	// MinTurns gates extraction: a conversation shorter than this is not worth
	// a model call. 0 disables.
	MinTurns int
	// MaxEvidenceMessages bounds how much of the session tail is sent to the
	// model. Extraction reads the end of the conversation, not all of it.
	MaxEvidenceMessages int
	// PerModelTimeout is the extraction timeout budget for EACH model in the
	// fallback chain (not shared), so the second model is not left with the
	// first model's leftovers.
	PerModelTimeout time.Duration
	// IndexByteBudget bounds the session-start injection. 0 uses the store's
	// default.
	IndexByteBudget int
}

// DefaultConfig returns a conservative, disabled-by-default config.
func DefaultConfig() Config {
	return Config{
		Enabled:             false,
		MaxPerDay:           20,
		MinTurns:            4,
		MaxEvidenceMessages: 20,
		PerModelTimeout:     3 * time.Minute,
		IndexByteBudget:     0,
	}
}

// Drafter turns a conversation into candidate facts.
type Drafter interface {
	Draft(ctx context.Context, in DraftInput) (DraftOutput, error)
}

// DraftInput is the evidence a Drafter works with.
type DraftInput struct {
	Adapter  string
	UserID   string
	ThreadID string
	// SessionID is the session being mined. It is passed so the drafter can
	// scope session-local facts to it.
	SessionID string
	// Project is the project key the session belongs to, used for
	// project-scoped facts.
	Project string
	// Conversation is a chronological list of {role, text} turns.
	Conversation []Turn
	// ModelID selected by the Service (may be "" when the Drafter decides).
	ModelID string
	// ExistingFacts are the current statements, so the drafter can propose a
	// supersession instead of a duplicate.
	ExistingFacts []string
}

// Turn is one role-tagged utterance plus the message ID it came from.
//
// The ID is the whole point: it is what the drafter cites as evidence, and what
// the store re-checks before accepting a fact. A turn without an ID cannot be
// cited, so the drafter is told to ignore it.
type Turn struct {
	Role      string // "user" or "assistant"
	Text      string
	MessageID string
}

// DraftOutput is what a Drafter produces.
type DraftOutput struct {
	Facts   []DraftFact
	ModelID string
}

// DraftFact is one candidate fact as the model reported it.
//
// Every field is a string or a number the model can produce; nothing here is
// trusted. The Service validates each one against the store's rules and drops
// what does not pass.
type DraftFact struct {
	Kind       string
	Statement  string
	Status     string
	Scope      string
	ProjectKey string
	SessionID  string
	Confidence float64
	// Evidence is the message IDs the statement rests on. Empty means the model
	// did not cite anything, which is a rejection, not a low-confidence accept.
	Evidence []string
	// Supersedes is the ID of a fact this one corrects. Empty means it is new.
	Supersedes string
}

// Service orchestrates extraction on the durable job queue.
type Service struct {
	cfg     Config
	store   *memstore.Store
	writer  *memstore.ArchiveWriter
	drafter Drafter
	// recent is a best-effort per-thread dedup cache so the same thread is not
	// mined repeatedly on successive handoffs.
	recent sync.Map // map[string]time.Time
}

// NewService wires dependencies. writer and drafter MUST be non-nil when
// cfg.Enabled is true.
func NewService(cfg Config, store *memstore.Store, writer *memstore.ArchiveWriter, drafter Drafter) *Service {
	if cfg.MaxEvidenceMessages <= 0 {
		cfg.MaxEvidenceMessages = 20
	}
	if cfg.PerModelTimeout <= 0 {
		cfg.PerModelTimeout = 3 * time.Minute
	}
	return &Service{
		cfg:     cfg,
		store:   store,
		writer:  writer,
		drafter: drafter,
	}
}

// Config returns the effective config (used by command handlers to branch on
// settings).
func (s *Service) Config() Config {
	if s == nil {
		return Config{}
	}
	return s.cfg
}

// OnKnowledgeCandidate is the opencode.KnowledgeCandidateHook entry point.
// Never blocks: it validates, dedups, and enqueues the extraction job.
func (s *Service) OnKnowledgeCandidate(event opencode.KnowledgeCandidateEvent) {
	if s == nil || !s.cfg.Enabled || s.drafter == nil || s.writer == nil || event.SessionID == "" {
		return
	}
	// Never extract from the service's own drafting sessions. This is the
	// recursion guard: without it, an extraction prompt would itself look like
	// a conversation worth extracting, and each extraction would schedule the
	// next one.
	if event.Adapter == "knowledge" || strings.HasPrefix(event.ThreadID, "knowledge-draft-") {
		return
	}
	if event.Trigger != opencode.KnowledgeTriggerHandoff {
		return
	}
	if s.cfg.MinTurns > 0 && event.TurnCount < s.cfg.MinTurns {
		return
	}
	// Dedup: same thread within 30 minutes is the same unit of work.
	dedupKey := event.Adapter + ":" + event.UserID + ":" + event.SessionID
	if prev, ok := s.recent.Load(dedupKey); ok {
		if t, ok2 := prev.(time.Time); ok2 && time.Since(t) < 30*time.Minute {
			return
		}
	}

	// Daily cap: check cheaply before enqueueing.
	if s.cfg.MaxPerDay > 0 && s.store != nil {
		scope := memstore.SearchScope{Adapter: event.Adapter, UserID: event.UserID}
		if n, err := s.store.CountKnowledgeFactsSince(context.Background(), scope, time.Now().Add(-24*time.Hour)); err == nil && n >= s.cfg.MaxPerDay {
			log.Printf("knowledge: daily cap reached (%d), skipping extraction for thread %s", n, event.ThreadID)
			return
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.writer.EnqueueKnowledgeJob(ctx, memstore.KnowledgeJob{
		PrincipalID: memstore.PrincipalFor(memstore.SearchScope{Adapter: event.Adapter, UserID: event.UserID}),
		SourceKind:  memstore.KnowledgeSourceSession, SourceID: event.SessionID,
	}); err != nil {
		log.Printf("knowledge: durable enqueue failed: %v", err)
		return
	}
	s.recent.Store(dedupKey, time.Now())
}

func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for ctx.Err() == nil {
		if err := s.runPending(ctx); err != nil && ctx.Err() == nil {
			log.Printf("knowledge: worker failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) runPending(ctx context.Context) error {
	jobs, err := s.writer.ClaimKnowledgeJobs(ctx, 1)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		scope, runErr := s.store.KnowledgeJobScope(ctx, job)
		if runErr == nil {
			jobCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
			runErr = s.run(jobCtx, opencode.KnowledgeCandidateEvent{
				Adapter: scope.Adapter, UserID: scope.UserID, SessionID: job.SourceID,
				ThreadID: job.ID, Trigger: opencode.KnowledgeTriggerHandoff,
			})
			cancel()
		}
		finishCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if runErr != nil {
			err = s.writer.FailKnowledgeJob(finishCtx, job.ID, runErr)
		} else {
			err = s.writer.CompleteKnowledgeJob(finishCtx, job.ID)
		}
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// run is the extraction pipeline: read evidence → draft → validate → persist.
//
// It returns an error only for failures worth retrying. A fact rejected for bad
// evidence is not one of them: the evidence will not appear on a second
// attempt, so the job completes and the rejection is logged.
func (s *Service) run(ctx context.Context, ev opencode.KnowledgeCandidateEvent) error {
	log.Printf("knowledge: pipeline start — trigger=%s thread=%s session=%s adapter=%s user=%s",
		ev.Trigger, ev.ThreadID, ev.SessionID, ev.Adapter, ev.UserID)

	scope := memstore.SearchScope{Adapter: ev.Adapter, UserID: ev.UserID}
	evidence, err := s.store.RecentEvidence(ctx, scope, ev.SessionID, s.cfg.MaxEvidenceMessages)
	if err != nil {
		log.Printf("knowledge: STEP1 read evidence failed for session %s: %v", ev.SessionID, err)
		return err
	}
	if len(evidence) == 0 {
		log.Printf("knowledge: STEP1 no captured evidence for session %s (nothing to extract)", ev.SessionID)
		return nil
	}
	turns := make([]Turn, 0, len(evidence))
	project := ""
	for _, detail := range evidence {
		if strings.TrimSpace(detail.Text) == "" {
			continue
		}
		turns = append(turns, Turn{Role: detail.Role, Text: detail.Text, MessageID: detail.MessageID})
		if project == "" {
			project = detail.Project
		}
	}
	if len(turns) == 0 {
		log.Printf("knowledge: STEP1 evidence for session %s was all empty text", ev.SessionID)
		return nil
	}
	log.Printf("knowledge: STEP1 read %d evidence messages from session %s (project=%q)", len(turns), ev.SessionID, project)

	// Existing facts are passed so the drafter can propose a supersession
	// instead of re-asserting something already known.
	var existing []string
	if facts, err := s.store.ListKnowledgeFacts(ctx, memstore.SearchScope{Adapter: ev.Adapter, UserID: ev.UserID, Project: project}, memstore.KnowledgeListOptions{Limit: 40}); err == nil {
		for _, fact := range facts {
			existing = append(existing, fmt.Sprintf("id=%s: %s", fact.ID, fact.Statement))
		}
	}

	out, err := s.draft(ctx, DraftInput{
		Adapter:       ev.Adapter,
		UserID:        ev.UserID,
		ThreadID:      ev.ThreadID,
		SessionID:     ev.SessionID,
		Project:       project,
		Conversation:  turns,
		ExistingFacts: existing,
	})
	if err != nil {
		log.Printf("knowledge: STEP2 draft failed for thread %s: %v", ev.ThreadID, err)
		return err
	}
	if len(out.Facts) == 0 {
		log.Printf("knowledge: STEP2 drafter returned no facts for thread %s (nothing durable in this conversation)", ev.ThreadID)
		return nil
	}
	log.Printf("knowledge: STEP2 drafter returned %d candidate facts (model=%s)", len(out.Facts), out.ModelID)

	accepted, rejected, err := s.persist(ctx, scope, project, evidence[0].SessionID, out.Facts)
	log.Printf("knowledge: pipeline done — %d accepted, %d rejected (thread=%s)", accepted, rejected, ev.ThreadID)
	return err
}

// draft tries the preferred model, then each alternate, giving every model its
// own timeout budget.
func (s *Service) draft(ctx context.Context, in DraftInput) (DraftOutput, error) {
	models := make([]string, 0, len(s.cfg.AlternateModels)+1)
	if s.cfg.DraftModel != "" {
		models = append(models, s.cfg.DraftModel)
	}
	for _, model := range s.cfg.AlternateModels {
		model = strings.TrimSpace(model)
		if model == "" {
			continue
		}
		duplicate := false
		for _, seen := range models {
			if seen == model {
				duplicate = true
				break
			}
		}
		if !duplicate {
			models = append(models, model)
		}
	}
	if len(models) == 0 {
		models = append(models, "")
	}
	var lastErr error
	for _, model := range models {
		attempt := in
		attempt.ModelID = model
		draftCtx, cancel := context.WithTimeout(ctx, s.cfg.PerModelTimeout)
		out, err := s.drafter.Draft(draftCtx, attempt)
		cancel()
		if err == nil {
			if out.ModelID == "" {
				out.ModelID = model
			}
			return out, nil
		}
		lastErr = err
		log.Printf("knowledge: drafter model %q failed: %v", model, err)
	}
	return DraftOutput{}, lastErr
}

// persist validates and stores each candidate fact, returning how many were
// accepted and how many were rejected.
//
// A rejection is logged with its reason and never retried. The distinction
// matters: a retry would re-ask the model for the same statement against the
// same evidence and reach the same rejection, so treating it as a transient
// failure would burn the budget to no effect.
func (s *Service) persist(ctx context.Context, scope memstore.SearchScope, project, sessionID string, facts []DraftFact) (accepted, rejected int, failure error) {
	principal := memstore.PrincipalFor(scope)
	for _, candidate := range facts {
		fact := memstore.KnowledgeFact{
			PrincipalID: principal,
			Kind:        memstore.KnowledgeKind(strings.TrimSpace(candidate.Kind)),
			Statement:   candidate.Statement,
			Status:      memstore.KnowledgeStatus(strings.TrimSpace(candidate.Status)),
			Scope:       memstore.KnowledgeScope(strings.TrimSpace(candidate.Scope)),
			ProjectKey:  candidate.ProjectKey,
			SessionID:   candidate.SessionID,
			Confidence:  candidate.Confidence,
			Origin:      "extraction",
			Evidence:    candidate.Evidence,
		}
		// Fill in the scope the model left implicit. A model that says "global"
		// gets a global fact; one that says "project" without naming the project
		// gets the session's project, because that is the only project the
		// evidence came from. A model that says nothing at all gets the safest
		// scope, which is session: it cannot leak into another conversation.
		if fact.Scope == "" {
			fact.Scope = memstore.KnowledgeScopeSession
		}
		if fact.Scope == memstore.KnowledgeScopeProject && strings.TrimSpace(fact.ProjectKey) == "" {
			fact.ProjectKey = project
		}
		if fact.Scope == memstore.KnowledgeScopeSession && strings.TrimSpace(fact.SessionID) == "" {
			fact.SessionID = sessionID
		}
		if fact.Status == "" {
			fact.Status = memstore.KnowledgeStatusObserved
		}

		var err error
		if strings.TrimSpace(candidate.Supersedes) != "" {
			_, err = s.writer.SupersedeKnowledgeFact(ctx, candidate.Supersedes, fact)
		} else {
			_, err = s.writer.SaveKnowledgeFact(ctx, fact)
		}
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, memstore.ErrKnowledgeEvidenceInvalid):
			// The model cited a message that is not in the archive under this
			// principal. That is the failure the whole design exists to catch.
			rejected++
			log.Printf("knowledge: rejected fact (evidence not in archive): %.120s", fact.Statement)
		case errors.Is(err, memstore.ErrKnowledgeFactInvalid):
			rejected++
			log.Printf("knowledge: rejected fact (malformed: %v): %.120s", err, fact.Statement)
		case errors.Is(err, memstore.ErrKnowledgeAlreadySuperseded):
			rejected++
			log.Printf("knowledge: rejected fact (supersession would fork): %.120s", fact.Statement)
		default:
			return accepted, rejected, err
		}
	}
	return accepted, rejected, nil
}

// LoadKnowledgeIndex implements opencode.KnowledgeIndexStore. It returns the
// bounded block for the scope, or "" when there is nothing to inject.
//
// It is on the request path, so it is bounded twice: the store caps the byte
// budget, and the query is a single indexed read. A failure returns "" rather
// than an error — a knowledge lookup that fails must not stop a conversation.
func (s *Service) LoadKnowledgeIndex(adapter, userID, project string) string {
	if s == nil || s.store == nil {
		return ""
	}
	scope := memstore.SearchScope{Adapter: adapter, UserID: userID, Project: project}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	index, err := s.store.KnowledgeIndex(ctx, scope, s.cfg.IndexByteBudget)
	if err != nil {
		log.Printf("knowledge: index lookup failed (adapter=%s user=%s): %v", adapter, userID, err)
		return ""
	}
	return index.Text
}
