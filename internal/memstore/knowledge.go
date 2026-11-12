package memstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// Durable knowledge is the archive's derived layer: statements extracted from
// captured messages, each one traceable back to the messages that support it.
//
// The design constraint that shapes everything here is that a fact is not
// evidence. A model can assert anything; the archive can only vouch for what it
// captured. So a fact is stored with the message IDs it claims to rest on, and
// every write path re-checks that those messages exist and belong to the same
// principal before the fact is accepted. A fact whose evidence does not check
// out is rejected outright rather than stored with a low confidence, because
// "confidence is not proof" — a number between 0 and 1 does not make an
// unsupported claim supported, it only makes it look measured.
var (
	// ErrKnowledgeEvidenceInvalid means a claimed supporting message does not
	// exist, or exists under a different principal. The whole fact is rejected:
	// partial provenance would let a caller cite evidence it cannot read.
	ErrKnowledgeEvidenceInvalid = errors.New("memstore: knowledge evidence does not belong to this principal or does not exist")
	// ErrKnowledgeFactInvalid means the fact itself is malformed.
	ErrKnowledgeFactInvalid = errors.New("memstore: knowledge fact is malformed")
	// ErrKnowledgeAlreadySuperseded means the fact being corrected has already
	// been replaced by a different fact. Superseding it again would fork the
	// history, so the caller has to read the current fact first.
	ErrKnowledgeAlreadySuperseded = errors.New("memstore: knowledge fact was already superseded by a different fact")
	// ErrKnowledgeScopeRequired means no principal was supplied. Knowledge is
	// never read or written unscoped.
	ErrKnowledgeScopeRequired = errors.New("memstore: knowledge requires an adapter and user scope")
)

// KnowledgeKind classifies what a fact asserts. The distinction matters because
// the plan's gate requires that "task-local instructions do not become global
// preferences": a constraint observed inside one project must not be injected
// as a preference for every project, and the kind is what a reader uses to tell
// them apart.
type KnowledgeKind string

const (
	// KnowledgeKindPreference is a durable like or dislike ("prefers Chinese").
	KnowledgeKindPreference KnowledgeKind = "preference"
	// KnowledgeKindFact is a stable statement about the user or their setup.
	KnowledgeKindFact KnowledgeKind = "fact"
	// KnowledgeKindProcedure is a repeatable how-to the user has confirmed.
	KnowledgeKindProcedure KnowledgeKind = "procedure"
	// KnowledgeKindConstraint is a rule that limits what may be done.
	KnowledgeKindConstraint KnowledgeKind = "constraint"
	// KnowledgeKindDecision is a choice that was made and should not be
	// silently revisited.
	KnowledgeKindDecision KnowledgeKind = "decision"
)

var knowledgeKinds = map[KnowledgeKind]bool{
	KnowledgeKindPreference: true,
	KnowledgeKindFact:       true,
	KnowledgeKindProcedure:  true,
	KnowledgeKindConstraint: true,
	KnowledgeKindDecision:   true,
}

// KnowledgeStatus is the lifecycle position of a fact. The four values are the
// ones the plan names; they are ordered from weakest to strongest evidence, and
// nothing in this package promotes a fact between them automatically. A
// promotion is a write, and a write needs a caller that decided to make it.
type KnowledgeStatus string

const (
	// KnowledgeStatusObserved means the fact was read out of captured text.
	KnowledgeStatusObserved KnowledgeStatus = "observed"
	// KnowledgeStatusProposed means the fact is a suggestion awaiting a human.
	KnowledgeStatusProposed KnowledgeStatus = "proposed"
	// KnowledgeStatusCompleted means the work the fact describes was finished.
	KnowledgeStatusCompleted KnowledgeStatus = "completed"
	// KnowledgeStatusVerified means a human confirmed the fact.
	KnowledgeStatusVerified KnowledgeStatus = "verified"
)

var knowledgeStatuses = map[KnowledgeStatus]bool{
	KnowledgeStatusObserved:  true,
	KnowledgeStatusProposed:  true,
	KnowledgeStatusCompleted: true,
	KnowledgeStatusVerified:  true,
}

// KnowledgeScope is how far a fact reaches. It is the field that keeps a
// task-local instruction from becoming a global preference, so it is required
// rather than defaulted: a caller that does not know the scope does not know
// what it is asserting.
type KnowledgeScope string

const (
	// KnowledgeScopeGlobal applies to every conversation with the principal.
	KnowledgeScopeGlobal KnowledgeScope = "global"
	// KnowledgeScopeProject applies only within one project key.
	KnowledgeScopeProject KnowledgeScope = "project"
	// KnowledgeScopeSession applies only within one session.
	KnowledgeScopeSession KnowledgeScope = "session"
)

var knowledgeScopes = map[KnowledgeScope]bool{
	KnowledgeScopeGlobal:  true,
	KnowledgeScopeProject: true,
	KnowledgeScopeSession: true,
}

// Knowledge tuning.
const (
	// knowledgeStatementMaxBytes bounds one statement. A fact that needs more
	// than this is not a fact, it is a document, and injecting it at session
	// start would spend the whole budget on one line.
	knowledgeStatementMaxBytes = 512
	// knowledgeIndexDefaultBytes bounds the session-start injection.
	//
	// The plan asks for "up to 1,500 tokens using an available tokenizer or
	// conservative byte budget". This package has no tokenizer — chunking is
	// byte-based — so the budget is bytes, and it is chosen to be conservative
	// in the direction that matters. 3,000 bytes is at most 1,000 CJK
	// characters (3 bytes each), which a per-character tokenizer turns into at
	// most ~1,500 tokens; for Latin text it is ~750 tokens. So 3,000 bytes
	// cannot exceed the token target for either script, at the cost of leaving
	// budget unused for Latin text. That trade is deliberate: overshooting the
	// budget costs prompt space on every turn, undershooting costs a few facts
	// that retrieval can still find on demand.
	knowledgeIndexDefaultBytes = 3000
	// knowledgeIndexMaxBytes is the ceiling a caller may raise the budget to.
	knowledgeIndexMaxBytes = 12000
	// knowledgeIndexMaxFacts bounds the row count independently of bytes, so a
	// corpus of very short facts cannot produce a thousand-line preamble.
	knowledgeIndexMaxFacts = 40
	// knowledgeListDefaultLimit and knowledgeListMaxLimit bound a listing.
	knowledgeListDefaultLimit = 50
	knowledgeListMaxLimit     = 500
	// knowledgeJobLeaseMillis is how long a claimed extraction job stays
	// claimed. A crashed worker's job becomes claimable again after this.
	knowledgeJobLeaseMillis = 600000
	// knowledgeJobMaxAttempts is how many times a job is retried before it is
	// parked as failed. Extraction is best-effort: a job that keeps failing
	// must stop consuming the model budget.
	knowledgeJobMaxAttempts = 3
)

// KnowledgeFact is one durable statement plus its provenance.
type KnowledgeFact struct {
	ID          string          `json:"id"`
	PrincipalID string          `json:"-"`
	Kind        KnowledgeKind   `json:"kind"`
	Statement   string          `json:"statement"`
	Status      KnowledgeStatus `json:"status"`
	Scope       KnowledgeScope  `json:"scope"`
	// ProjectKey narrows a project-scoped fact. Empty for global facts.
	ProjectKey string `json:"project_key,omitempty"`
	// SessionID narrows a session-scoped fact. Empty otherwise.
	SessionID string `json:"session_id,omitempty"`
	// Confidence is the drafter's self-reported certainty. It is recorded for
	// ranking and for auditing the drafter, and it is never used as a
	// substitute for evidence: a fact with no valid evidence is rejected
	// regardless of how confident the drafter was.
	Confidence float64 `json:"confidence"`
	// Origin records what produced the fact ("extraction", "user", ...). It is
	// required, and the extraction service refuses to run on a fact whose
	// origin is its own, which is how extraction is prevented from scheduling
	// itself recursively.
	Origin string `json:"origin"`
	// RedactionPolicy records what was removed from the statement before it was
	// stored. "none" means nothing was removed. It is stored rather than
	// assumed so an auditor can tell a fact that never contained a secret from
	// one that had a secret stripped out of it.
	RedactionPolicy string `json:"redaction_policy"`
	// SupersededBy is the ID of the fact that replaced this one. Empty means
	// the fact is current. A superseded fact keeps its row, its statement, and
	// its evidence: corrections are recorded as history, not as overwrites.
	SupersededBy string `json:"superseded_by,omitempty"`
	// Evidence is the set of message IDs this fact rests on.
	Evidence  []string  `json:"evidence"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Current reports whether the fact is still in force.
func (fact KnowledgeFact) Current() bool { return fact.SupersededBy == "" }

// KnowledgeListOptions narrows a knowledge listing.
type KnowledgeListOptions struct {
	// IncludeSuperseded returns replaced facts as well as current ones.
	IncludeSuperseded bool
	// IncludeSessionFacts returns session-scoped facts too.
	//
	// This is the difference between a review surface and an injection surface.
	// A session-scoped fact belongs to the principal — the principal owns every
	// session it ever had — so a listing that is meant for the user to review
	// and delete their own knowledge must see them, or the rows become
	// unreachable: written, never shown, never deletable.
	//
	// The session-start index leaves this false, because a session that is
	// starting has no facts of its own yet, and a session fact from a different
	// conversation must never be injected into this one.
	IncludeSessionFacts bool
	// Kinds restricts the result to these kinds. Empty means all kinds.
	Kinds []KnowledgeKind
	// Limit bounds the result. Zero uses knowledgeListDefaultLimit.
	Limit int
}

// KnowledgeIndex is the bounded block injected at session start.
type KnowledgeIndex struct {
	Facts []KnowledgeFact `json:"facts"`
	// Text is the rendered block. Empty when there is nothing to inject, which
	// is the common case and must not produce a header with no content.
	Text string `json:"text"`
	// Bytes is len(Text).
	Bytes int `json:"bytes"`
	// ByteBudget is the budget that was applied, echoed so a caller can tell a
	// short index from a truncated one.
	ByteBudget int `json:"byte_budget"`
	// Truncated reports that facts were dropped to fit the budget.
	Truncated bool `json:"truncated"`
}

// KnowledgeJob is one queued extraction.
type KnowledgeJob struct {
	ID          string `json:"id"`
	PrincipalID string `json:"-"`
	// SourceKind is "session", "turn", or "explicit". "explicit" is a
	// user-requested remember, which is allowed to run even when automatic
	// extraction is disabled.
	SourceKind string `json:"source_kind"`
	SourceID   string `json:"source_id"`
	Status     string `json:"status"`
	Attempts   int    `json:"attempts"`
	LeaseUntil int64  `json:"lease_until"`
	NextRunAt  int64  `json:"next_run_at"`
	LastError  string `json:"last_error,omitempty"`
	CreatedAt  int64  `json:"created_at"`
}

// KnowledgeJobSourceKind values.
const (
	KnowledgeSourceSession  = "session"
	KnowledgeSourceTurn     = "turn"
	KnowledgeSourceExplicit = "explicit"
)

// KnowledgeJobStatus values.
const (
	KnowledgeJobPending   = "pending"
	KnowledgeJobCompleted = "completed"
	KnowledgeJobFailed    = "failed"
)

// knowledgeFactID derives a fact's identity from what it asserts.
//
// Deterministic rather than random, because extraction is retried: the same
// statement read out of the same conversation twice must land on the same row
// instead of producing a duplicate. A correction changes the statement, which
// changes the ID, which is exactly the case supersession exists for.
func knowledgeFactID(fact KnowledgeFact) string {
	return archiveID("knowledge", fact.PrincipalID, string(fact.Kind), normalizeKnowledgeStatement(fact.Statement), string(fact.Scope), fact.ProjectKey, fact.SessionID)
}

func resolveKnowledgeIdentity(ctx context.Context, tx *sql.Tx, fact *KnowledgeFact) error {
	if fact.ID != "" {
		return nil
	}
	err := tx.QueryRowContext(ctx, `SELECT id FROM mem_v2_knowledge_facts
		WHERE principal_id=? AND kind=? AND statement=? AND scope=? AND project_key=? AND session_id=?
		ORDER BY created_at,id LIMIT 1`, fact.PrincipalID, fact.Kind, fact.Statement, fact.Scope, fact.ProjectKey, fact.SessionID).Scan(&fact.ID)
	if errors.Is(err, sql.ErrNoRows) {
		fact.ID = knowledgeFactID(*fact)
		return nil
	}
	return err
}

// normalizeKnowledgeStatement collapses whitespace so that a statement
// re-extracted with different line wrapping keeps its identity.
func normalizeKnowledgeStatement(statement string) string {
	return strings.Join(strings.Fields(statement), " ")
}

func validateKnowledgeFact(fact KnowledgeFact) error {
	if strings.TrimSpace(fact.PrincipalID) == "" {
		return fmt.Errorf("%w: principal is required", ErrKnowledgeFactInvalid)
	}
	if !knowledgeKinds[fact.Kind] {
		return fmt.Errorf("%w: unsupported kind %q", ErrKnowledgeFactInvalid, fact.Kind)
	}
	if !knowledgeStatuses[fact.Status] {
		return fmt.Errorf("%w: unsupported status %q", ErrKnowledgeFactInvalid, fact.Status)
	}
	if !knowledgeScopes[fact.Scope] {
		return fmt.Errorf("%w: unsupported scope %q", ErrKnowledgeFactInvalid, fact.Scope)
	}
	statement := normalizeKnowledgeStatement(fact.Statement)
	if statement == "" {
		return fmt.Errorf("%w: statement is required", ErrKnowledgeFactInvalid)
	}
	if !utf8.ValidString(statement) {
		return fmt.Errorf("%w: statement must be valid UTF-8", ErrKnowledgeFactInvalid)
	}
	if len(statement) > knowledgeStatementMaxBytes {
		return fmt.Errorf("%w: statement exceeds %d bytes", ErrKnowledgeFactInvalid, knowledgeStatementMaxBytes)
	}
	if fact.Confidence < 0 || fact.Confidence > 1 {
		return fmt.Errorf("%w: confidence must be within 0..1", ErrKnowledgeFactInvalid)
	}
	if strings.TrimSpace(fact.Origin) == "" {
		return fmt.Errorf("%w: origin is required", ErrKnowledgeFactInvalid)
	}
	if fact.Scope == KnowledgeScopeProject && strings.TrimSpace(fact.ProjectKey) == "" {
		return fmt.Errorf("%w: a project-scoped fact needs a project key", ErrKnowledgeFactInvalid)
	}
	if fact.Scope == KnowledgeScopeSession && strings.TrimSpace(fact.SessionID) == "" {
		return fmt.Errorf("%w: a session-scoped fact needs a session id", ErrKnowledgeFactInvalid)
	}
	if len(fact.Evidence) == 0 {
		return fmt.Errorf("%w: at least one supporting message is required", ErrKnowledgeFactInvalid)
	}
	for _, messageID := range fact.Evidence {
		if strings.TrimSpace(messageID) == "" {
			return fmt.Errorf("%w: evidence contains an empty message id", ErrKnowledgeFactInvalid)
		}
	}
	return nil
}

// verifyKnowledgeEvidence checks that every claimed message exists and belongs
// to the fact's principal. It runs inside the writing transaction so a message
// deleted concurrently cannot slip between the check and the insert.
func verifyKnowledgeEvidence(ctx context.Context, tx *sql.Tx, principal string, messageIDs []string) error {
	for _, messageID := range messageIDs {
		var found int
		err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM mem_v2_messages WHERE id=? AND principal_id=?`,
			messageID, principal).Scan(&found)
		if err != nil {
			return err
		}
		if found == 0 {
			return fmt.Errorf("%w: message %s", ErrKnowledgeEvidenceInvalid, messageID)
		}
	}
	return nil
}

// SaveKnowledgeFact stores a fact and its provenance.
//
// The fact is rejected unless every claimed message exists under the same
// principal. Re-saving an identical statement refreshes its status and
// confidence but never clears a supersession: a re-extraction of an old
// statement must not resurrect a fact the user already corrected.
func (writer *ArchiveWriter) SaveKnowledgeFact(ctx context.Context, fact KnowledgeFact) (KnowledgeFact, error) {
	if err := validateKnowledgeFact(fact); err != nil {
		return KnowledgeFact{}, err
	}
	fact.Statement = normalizeKnowledgeStatement(fact.Statement)
	if fact.RedactionPolicy == "" {
		fact.RedactionPolicy = "none"
	}
	now := time.Now().UTC()
	if fact.CreatedAt.IsZero() {
		fact.CreatedAt = now
	}
	fact.UpdatedAt = now
	encoded := len(fact.Statement) + len(fact.Evidence)*64 + 256
	err := writer.submit(ctx, encoded, func(ctx context.Context, tx *sql.Tx) error {
		if err := resolveKnowledgeIdentity(ctx, tx, &fact); err != nil {
			return err
		}
		return saveKnowledgeFactTx(ctx, tx, fact)
	})
	if err != nil {
		return KnowledgeFact{}, err
	}
	return fact, nil
}

func saveKnowledgeFactTx(ctx context.Context, tx *sql.Tx, fact KnowledgeFact) error {
	var mismatched int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM mem_v2_knowledge_facts WHERE id=?
		AND NOT (principal_id=? AND kind=? AND statement=? AND scope=? AND project_key=? AND session_id=?)`,
		fact.ID, fact.PrincipalID, fact.Kind, fact.Statement, fact.Scope, fact.ProjectKey, fact.SessionID).Scan(&mismatched); err != nil {
		return err
	}
	if mismatched != 0 {
		return ErrKnowledgeFactInvalid
	}
	if err := verifyKnowledgeEvidence(ctx, tx, fact.PrincipalID, fact.Evidence); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_knowledge_facts
		(id,principal_id,kind,statement,status,scope,project_key,session_id,confidence,origin,redaction_policy,superseded_by,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,NULL,?,?)
		ON CONFLICT(id) DO UPDATE SET
			status=excluded.status,
			confidence=excluded.confidence,
			redaction_policy=excluded.redaction_policy,
			updated_at=excluded.updated_at`,
		fact.ID, fact.PrincipalID, string(fact.Kind), fact.Statement, string(fact.Status),
		string(fact.Scope), fact.ProjectKey, fact.SessionID, fact.Confidence, fact.Origin,
		fact.RedactionPolicy, fact.CreatedAt.UnixMilli(), fact.UpdatedAt.UnixMilli())
	if err != nil {
		return err
	}
	for ordinal, messageID := range fact.Evidence {
		if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO mem_v2_knowledge_evidence
			(fact_id,principal_id,message_id,ordinal) VALUES(?,?,?,?)`,
			fact.ID, fact.PrincipalID, messageID, ordinal); err != nil {
			return err
		}
	}
	return nil
}

// SupersedeKnowledgeFact stores replacement as the current fact and marks
// previousID as replaced by it, in one transaction.
//
// The old fact keeps its row, its statement, and its evidence. That is the
// plan's requirement that corrections are "stored as supersession rather than
// overwriting historical evidence": a reader can still see what was believed
// before, and by which messages it was supported.
//
// Superseding an already-superseded fact is an error unless it is already
// superseded by this same replacement, which makes a retried correction
// idempotent without allowing a fork.
func (writer *ArchiveWriter) SupersedeKnowledgeFact(ctx context.Context, previousID string, replacement KnowledgeFact) (KnowledgeFact, error) {
	if strings.TrimSpace(previousID) == "" {
		return KnowledgeFact{}, fmt.Errorf("%w: previous fact id is required", ErrKnowledgeFactInvalid)
	}
	if err := validateKnowledgeFact(replacement); err != nil {
		return KnowledgeFact{}, err
	}
	replacement.Statement = normalizeKnowledgeStatement(replacement.Statement)
	if replacement.ID == previousID {
		return KnowledgeFact{}, fmt.Errorf("%w: a fact cannot supersede itself", ErrKnowledgeFactInvalid)
	}
	if replacement.RedactionPolicy == "" {
		replacement.RedactionPolicy = "none"
	}
	now := time.Now().UTC()
	if replacement.CreatedAt.IsZero() {
		replacement.CreatedAt = now
	}
	replacement.UpdatedAt = now
	encoded := len(replacement.Statement) + len(replacement.Evidence)*64 + 256
	err := writer.submit(ctx, encoded, func(ctx context.Context, tx *sql.Tx) error {
		if err := resolveKnowledgeIdentity(ctx, tx, &replacement); err != nil {
			return err
		}
		if replacement.ID == previousID {
			return ErrKnowledgeFactInvalid
		}
		var existing string
		err := tx.QueryRowContext(ctx,
			`SELECT COALESCE(superseded_by,'') FROM mem_v2_knowledge_facts WHERE id=? AND principal_id=?`,
			previousID, replacement.PrincipalID).Scan(&existing)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: fact %s", ErrKnowledgeFactInvalid, previousID)
		}
		if err != nil {
			return err
		}
		if existing != "" && existing != replacement.ID {
			return fmt.Errorf("%w: fact %s is superseded by %s", ErrKnowledgeAlreadySuperseded, previousID, existing)
		}
		if err := saveKnowledgeFactTx(ctx, tx, replacement); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx,
			`UPDATE mem_v2_knowledge_facts SET superseded_by=?, updated_at=? WHERE id=? AND principal_id=?`,
			replacement.ID, now.UnixMilli(), previousID, replacement.PrincipalID)
		return err
	})
	if err != nil {
		return KnowledgeFact{}, err
	}
	return replacement, nil
}

// ForgetKnowledgeFact removes one fact and its provenance.
//
// Facts that this one superseded are un-superseded rather than deleted: if the
// user forgets a correction, the statement it replaced becomes current again.
// Leaving the pointer in place would violate the self-reference and, worse,
// would leave a current-looking fact pointing at a row that no longer exists.
func (writer *ArchiveWriter) ForgetKnowledgeFact(ctx context.Context, scope SearchScope, factID string) (bool, error) {
	principal, err := scope.principal()
	if err != nil {
		return false, err
	}
	if strings.TrimSpace(factID) == "" {
		return false, fmt.Errorf("%w: fact id is required", ErrKnowledgeFactInvalid)
	}
	removed := false
	err = writer.submit(ctx, 0, func(ctx context.Context, tx *sql.Tx) error {
		removed = false
		if _, err := tx.ExecContext(ctx,
			`UPDATE mem_v2_knowledge_facts SET superseded_by=NULL WHERE superseded_by=? AND principal_id=?`,
			factID, principal); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM mem_v2_knowledge_evidence WHERE fact_id=? AND principal_id=?`,
			factID, principal); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx,
			`DELETE FROM mem_v2_knowledge_facts WHERE id=? AND principal_id=?`, factID, principal)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		removed = affected > 0
		return nil
	})
	if err != nil {
		return false, err
	}
	return removed, nil
}

// ListKnowledgeFacts returns facts visible to the scope, newest first.
//
// Visibility is the same rule the index uses: a global fact is visible
// everywhere, a project fact only inside its project, a session fact only
// inside its session. The scope's Project field selects the project, so a
// caller that does not set it sees global facts only — which is the safe
// default, because the alternative would leak one project's constraints into
// another's context.
//
// Session facts are the exception, and they are opt-in through
// KnowledgeListOptions.IncludeSessionFacts. The index leaves them out because
// injecting another conversation's session facts would be a leak; the review
// and forget paths include them because a fact the user cannot see is a fact
// the user cannot delete, and the plan requires forgetting to remove derived
// facts.
func (s *Store) ListKnowledgeFacts(ctx context.Context, scope SearchScope, options KnowledgeListOptions) ([]KnowledgeFact, error) {
	principal, err := scope.principal()
	if err != nil {
		return nil, err
	}
	limit := options.Limit
	if limit <= 0 {
		limit = knowledgeListDefaultLimit
	}
	if limit > knowledgeListMaxLimit {
		limit = knowledgeListMaxLimit
	}
	clause, args := knowledgeVisibilityClause(scope, principal, options.IncludeSessionFacts)
	if !options.IncludeSuperseded {
		clause += " AND f.superseded_by IS NULL"
	}
	if len(options.Kinds) > 0 {
		placeholders := make([]string, 0, len(options.Kinds))
		for _, kind := range options.Kinds {
			if !knowledgeKinds[kind] {
				return nil, fmt.Errorf("%w: unsupported kind %q", ErrKnowledgeFactInvalid, kind)
			}
			placeholders = append(placeholders, "?")
			args = append(args, string(kind))
		}
		clause += " AND f.kind IN (" + strings.Join(placeholders, ",") + ")"
	}
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, `SELECT f.id,f.principal_id,f.kind,f.statement,f.status,f.scope,
		f.project_key,f.session_id,f.confidence,f.origin,f.redaction_policy,
		COALESCE(f.superseded_by,''),f.created_at,f.updated_at
		FROM mem_v2_knowledge_facts f WHERE `+clause+`
		ORDER BY f.created_at DESC, f.id LIMIT ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("memstore: list knowledge: %w", err)
	}
	defer rows.Close()
	facts := make([]KnowledgeFact, 0, limit)
	for rows.Next() {
		fact, err := scanKnowledgeFact(rows)
		if err != nil {
			return nil, err
		}
		facts = append(facts, fact)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := s.attachKnowledgeEvidence(ctx, facts); err != nil {
		return nil, err
	}
	return facts, nil
}

// knowledgeVisibilityClause builds the scope predicate shared by listing and
// indexing, so the two can never disagree about what is visible.
//
// includeSession is the one thing the two callers are allowed to disagree
// about, and it is a parameter rather than a second clause so the disagreement
// is visible at the call site instead of hidden in a copy of this function.
func knowledgeVisibilityClause(scope SearchScope, principal string, includeSession bool) (string, []any) {
	clause := `f.principal_id=? AND (f.scope='global'`
	args := []any{principal}
	if project := strings.TrimSpace(scope.Project); project != "" {
		clause += ` OR (f.scope='project' AND f.project_key=?)`
		args = append(args, project)
	}
	if includeSession {
		clause += ` OR f.scope='session'`
	}
	clause += `)`
	return clause, args
}

func scanKnowledgeFact(rows *sql.Rows) (KnowledgeFact, error) {
	var fact KnowledgeFact
	var kind, status, scope string
	var createdAt, updatedAt int64
	if err := rows.Scan(&fact.ID, &fact.PrincipalID, &kind, &fact.Statement, &status, &scope,
		&fact.ProjectKey, &fact.SessionID, &fact.Confidence, &fact.Origin, &fact.RedactionPolicy,
		&fact.SupersededBy, &createdAt, &updatedAt); err != nil {
		return KnowledgeFact{}, err
	}
	fact.Kind = KnowledgeKind(kind)
	fact.Status = KnowledgeStatus(status)
	fact.Scope = KnowledgeScope(scope)
	fact.CreatedAt = time.UnixMilli(createdAt).UTC()
	fact.UpdatedAt = time.UnixMilli(updatedAt).UTC()
	return fact, nil
}

// attachKnowledgeEvidence fills in the Evidence field for a batch of facts with
// one query. Doing it per fact would be a query per row on a path that runs at
// session start.
func (s *Store) attachKnowledgeEvidence(ctx context.Context, facts []KnowledgeFact) error {
	if len(facts) == 0 {
		return nil
	}
	index := make(map[string]int, len(facts))
	placeholders := make([]string, 0, len(facts))
	args := make([]any, 0, len(facts))
	for position := range facts {
		index[facts[position].ID] = position
		placeholders = append(placeholders, "?")
		args = append(args, facts[position].ID)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT fact_id,message_id FROM mem_v2_knowledge_evidence
		WHERE fact_id IN (`+strings.Join(placeholders, ",")+`) ORDER BY fact_id, ordinal`, args...)
	if err != nil {
		return fmt.Errorf("memstore: knowledge evidence: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var factID, messageID string
		if err := rows.Scan(&factID, &messageID); err != nil {
			return err
		}
		if position, ok := index[factID]; ok {
			facts[position].Evidence = append(facts[position].Evidence, messageID)
		}
	}
	return rows.Err()
}

// knowledgeIndexHeader is the preamble of the session-start knowledge block.
//
// It is a constant rather than a literal inside KnowledgeIndex because the
// budget is measured against the rendered text, and a caller that wants to size
// a budget needs to know what the header costs before any fact fits. The
// wording is deliberately cautious: extracted facts can be stale, and the
// current instruction must win when the two disagree.
const knowledgeIndexHeader = "## 已知事实（从历史对话中提取，可能已过时；与当前指令冲突时以当前指令为准）\n"

// KnowledgeIndex renders the bounded block injected at session start.
//
// The budget is applied to the rendered text, not to the fact count, because
// the cost that matters is prompt bytes. Facts are ordered most-specific first
// (session, then project, then global) so that when the budget truncates, what
// survives is the knowledge that applies to the conversation actually starting.
// Truncated is reported rather than silent: a caller that sees it knows the
// model is working from a partial index and can retrieve the rest on demand.
func (s *Store) KnowledgeIndex(ctx context.Context, scope SearchScope, byteBudget int) (KnowledgeIndex, error) {
	if byteBudget <= 0 {
		byteBudget = knowledgeIndexDefaultBytes
	}
	if byteBudget > knowledgeIndexMaxBytes {
		byteBudget = knowledgeIndexMaxBytes
	}
	index := KnowledgeIndex{ByteBudget: byteBudget}
	// Session facts are deliberately excluded: a session that is starting has
	// none of its own, and another conversation's session facts must not be
	// injected into this one.
	facts, err := s.ListKnowledgeFacts(ctx, scope, KnowledgeListOptions{Limit: knowledgeIndexMaxFacts})
	if err != nil {
		return index, err
	}
	if len(facts) == 0 {
		return index, nil
	}
	// Most specific first. A stable sort keeps the newest-first order that
	// ListKnowledgeFacts already applied inside each scope.
	rank := map[KnowledgeScope]int{
		KnowledgeScopeSession: 0,
		KnowledgeScopeProject: 1,
		KnowledgeScopeGlobal:  2,
	}
	sortKnowledgeFactsByScope(facts, rank)
	var builder strings.Builder
	builder.WriteString(knowledgeIndexHeader)
	for _, fact := range facts {
		line := renderKnowledgeLine(fact)
		if builder.Len()+len(line) > byteBudget {
			index.Truncated = true
			break
		}
		builder.WriteString(line)
		index.Facts = append(index.Facts, fact)
	}
	if len(index.Facts) == 0 {
		// The header alone is not worth injecting, and a header with no facts
		// would tell the model there is knowledge when there is none.
		return KnowledgeIndex{ByteBudget: byteBudget}, nil
	}
	index.Text = builder.String()
	index.Bytes = len(index.Text)
	return index, nil
}

func sortKnowledgeFactsByScope(facts []KnowledgeFact, rank map[KnowledgeScope]int) {
	// Insertion sort: the slice is bounded by knowledgeIndexMaxFacts and this
	// keeps the ordering stable without pulling in a comparator type.
	for i := 1; i < len(facts); i++ {
		current := facts[i]
		j := i - 1
		for j >= 0 && rank[facts[j].Scope] > rank[current.Scope] {
			facts[j+1] = facts[j]
			j--
		}
		facts[j+1] = current
	}
}

func renderKnowledgeLine(fact KnowledgeFact) string {
	var builder strings.Builder
	builder.WriteString("- [")
	builder.WriteString(string(fact.Kind))
	builder.WriteString("] ")
	builder.WriteString(fact.Statement)
	builder.WriteString(" (")
	builder.WriteString(string(fact.Status))
	if fact.Scope != KnowledgeScopeGlobal {
		builder.WriteString(", ")
		builder.WriteString(string(fact.Scope))
	}
	builder.WriteString(")\n")
	return builder.String()
}

// CountKnowledgeFactsSince counts facts created at or after since, for the
// extraction service's daily cap. It is scoped so one principal's extraction
// volume cannot be inferred from another's.
func (s *Store) CountKnowledgeFactsSince(ctx context.Context, scope SearchScope, since time.Time) (int, error) {
	principal, err := scope.principal()
	if err != nil {
		return 0, err
	}
	var count int
	err = s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM mem_v2_knowledge_facts WHERE principal_id=? AND created_at>=?`,
		principal, since.UnixMilli()).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("memstore: count knowledge: %w", err)
	}
	return count, nil
}

// EnqueueKnowledgeJob records a durable extraction request.
//
// The unique key is (principal, source_kind, source_id), so enqueueing the same
// source twice is a no-op rather than a second model call. That is the
// mechanism behind the plan's "do not call an LLM for every message": the
// trigger fires per completed unit of work, and the queue collapses repeats.
func (writer *ArchiveWriter) EnqueueKnowledgeJob(ctx context.Context, job KnowledgeJob) error {
	if strings.TrimSpace(job.PrincipalID) == "" {
		return fmt.Errorf("%w: principal is required", ErrKnowledgeFactInvalid)
	}
	switch job.SourceKind {
	case KnowledgeSourceSession, KnowledgeSourceTurn, KnowledgeSourceExplicit:
	default:
		return fmt.Errorf("%w: unsupported source kind %q", ErrKnowledgeFactInvalid, job.SourceKind)
	}
	if strings.TrimSpace(job.SourceID) == "" {
		return fmt.Errorf("%w: source id is required", ErrKnowledgeFactInvalid)
	}
	if job.ID == "" {
		job.ID = archiveID("knowledge-job", job.PrincipalID, job.SourceKind, job.SourceID)
	}
	now := time.Now().UTC().UnixMilli()
	if job.CreatedAt == 0 {
		job.CreatedAt = now
	}
	return writer.submit(ctx, 0, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_knowledge_jobs
			(id,principal_id,source_kind,source_id,status,attempts,lease_until,next_run_at,last_error,created_at)
			VALUES(?,?,?,?,'pending',0,0,?,'',?)
			ON CONFLICT(principal_id,source_kind,source_id) DO UPDATE SET
				status='pending', next_run_at=excluded.next_run_at, last_error=''
			WHERE mem_v2_knowledge_jobs.status='failed'`,
			job.ID, job.PrincipalID, job.SourceKind, job.SourceID, now, job.CreatedAt)
		return err
	})
}

// ClaimKnowledgeJobs leases up to limit due jobs and returns them.
//
// The lease is written in the same transaction that selects the rows, so two
// workers cannot claim the same job. A worker that dies mid-extraction leaves
// the lease to expire, and the job becomes claimable again.
func (writer *ArchiveWriter) ClaimKnowledgeJobs(ctx context.Context, limit int) ([]KnowledgeJob, error) {
	if limit < 1 {
		limit = 1
	}
	if limit > 32 {
		limit = 32
	}
	var claimed []KnowledgeJob
	err := writer.submit(ctx, 0, func(ctx context.Context, tx *sql.Tx) error {
		claimed = nil
		now := time.Now().UTC().UnixMilli()
		rows, err := tx.QueryContext(ctx, `SELECT id,principal_id,source_kind,source_id,status,attempts,created_at
			FROM mem_v2_knowledge_jobs
			WHERE status='pending' AND next_run_at<=? AND lease_until<=?
			ORDER BY next_run_at,id LIMIT ?`, now, now, limit)
		if err != nil {
			return err
		}
		for rows.Next() {
			var job KnowledgeJob
			if err := rows.Scan(&job.ID, &job.PrincipalID, &job.SourceKind, &job.SourceID,
				&job.Status, &job.Attempts, &job.CreatedAt); err != nil {
				rows.Close()
				return err
			}
			claimed = append(claimed, job)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for position := range claimed {
			claimed[position].LeaseUntil = now + knowledgeJobLeaseMillis
			if _, err := tx.ExecContext(ctx,
				`UPDATE mem_v2_knowledge_jobs SET lease_until=? WHERE id=?`,
				claimed[position].LeaseUntil, claimed[position].ID); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

// CompleteKnowledgeJob marks a job done.
func (writer *ArchiveWriter) CompleteKnowledgeJob(ctx context.Context, jobID string) error {
	return writer.submit(ctx, 0, func(ctx context.Context, tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE mem_v2_knowledge_jobs
			SET status='completed', lease_until=0, attempts=attempts+1, last_error='' WHERE id=?`, jobID)
		return err
	})
}

// FailKnowledgeJob records a failed attempt. The job is retried with a growing
// delay until knowledgeJobMaxAttempts, then parked as failed so a permanently
// broken source stops consuming the model budget.
func (writer *ArchiveWriter) FailKnowledgeJob(ctx context.Context, jobID string, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	if len(message) > 512 {
		message = message[:512]
	}
	return writer.submit(ctx, 0, func(ctx context.Context, tx *sql.Tx) error {
		var attempts int
		err := tx.QueryRowContext(ctx, `SELECT attempts FROM mem_v2_knowledge_jobs WHERE id=?`, jobID).Scan(&attempts)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		attempts++
		status := KnowledgeJobPending
		nextRunAt := time.Now().UTC().UnixMilli() + int64(attempts)*60000
		if attempts >= knowledgeJobMaxAttempts {
			status = KnowledgeJobFailed
			nextRunAt = 0
		}
		_, err = tx.ExecContext(ctx, `UPDATE mem_v2_knowledge_jobs
			SET status=?, attempts=?, lease_until=0, next_run_at=?, last_error=? WHERE id=?`,
			status, attempts, nextRunAt, message, jobID)
		return err
	})
}

// KnowledgeJobScope resolves the scope a queued job's source belongs to.
//
// A job row stores only the principal and the source id, because that pair is
// what makes it unique. The adapter, user, and project are properties of the
// session the source lives in, so they are read back from the archive instead
// of being copied into the job row: a copy would be a second place for them to
// be wrong, and a job that outlived a rename would then extract under a scope
// that no longer matches its evidence.
func (s *Store) KnowledgeJobScope(ctx context.Context, job KnowledgeJob) (SearchScope, error) {
	if strings.TrimSpace(job.SourceID) == "" {
		return SearchScope{}, fmt.Errorf("%w: source id is required", ErrKnowledgeFactInvalid)
	}
	var adapter, userID, project string
	err := s.db.QueryRowContext(ctx, `SELECT s.adapter,s.user_id,s.project_key FROM mem_v2_sessions s
		WHERE s.principal_id=? AND (s.id=? OR EXISTS (
			SELECT 1 FROM mem_v2_turns t JOIN mem_v2_attempts a ON a.turn_id=t.id
			WHERE t.session_id=s.id AND a.upstream_session_id=?)) LIMIT 1`, job.PrincipalID, job.SourceID, job.SourceID).Scan(&adapter, &userID, &project)
	if errors.Is(err, sql.ErrNoRows) {
		// The source was deleted between enqueue and claim. That is not a
		// transient failure: retrying cannot bring the session back, so the
		// caller parks the job instead of burning the model budget on it.
		return SearchScope{}, fmt.Errorf("%w: session %s", ErrKnowledgeEvidenceInvalid, job.SourceID)
	}
	if err != nil {
		return SearchScope{}, fmt.Errorf("memstore: knowledge job scope: %w", err)
	}
	return SearchScope{Adapter: adapter, UserID: userID, Project: project}, nil
}

// LatestMessageID returns the newest captured message for the scope.
//
// This is the evidence an explicit remember cites. It exists so that
// "/remember X" can produce a real write receipt: the fact is stored against a
// message that is actually in the archive, rather than against the assistant's
// claim that it remembered something. The caller is told which message was
// cited so the receipt can name it.
//
// The limitation is worth stating: the command interceptor runs before the
// inbound message is archived, so the newest message is the previous turn, not
// the request that carried the command. That is still real, verifiable
// evidence — it is just not the command itself.
func (s *Store) LatestMessageID(ctx context.Context, scope SearchScope) (string, error) {
	principal, err := scope.principal()
	if err != nil {
		return "", err
	}
	var id string
	err = s.db.QueryRowContext(ctx, `SELECT id FROM mem_v2_messages
		WHERE principal_id=? ORDER BY created_at DESC, seq DESC LIMIT 1`, principal).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: this scope has no captured message to cite yet", ErrKnowledgeEvidenceInvalid)
	}
	if err != nil {
		return "", fmt.Errorf("memstore: latest message: %w", err)
	}
	return id, nil
}

// RecentEvidence returns the newest limit messages of a session in
// chronological order, decoded, for use as extraction input.
//
// It is not Scroll with a reversed cursor: extraction wants the tail of the
// conversation, and Scroll's keyset pagination is built to walk forward from a
// known position. Selecting DESC and reversing is the direct way to ask for
// "the last N", and it keeps the decode path identical to every other reader.
func (s *Store) RecentEvidence(ctx context.Context, scope SearchScope, sessionID string, limit int) ([]MessageDetail, error) {
	principal, err := scope.principal()
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(sessionID) == "" {
		return nil, fmt.Errorf("%w: session id is required", ErrKnowledgeFactInvalid)
	}
	if limit <= 0 {
		limit = defaultScrollLimit
	}
	if limit > maxScrollLimit {
		limit = maxScrollLimit
	}
	rows, err := s.db.QueryContext(ctx, `SELECT m.id,m.session_id,m.turn_id,m.role,m.state,m.seq,m.created_at,
			s.project_key,s.work_dir,m.fidelity,b.codec,b.codec_version,b.raw_bytes,b.payload
		FROM mem_v2_messages m
		JOIN mem_v2_sessions s ON s.id=m.session_id
		JOIN mem_v2_blocks b ON b.id=m.block_id
		WHERE m.principal_id=? AND (m.session_id=? OR EXISTS (
			SELECT 1 FROM mem_v2_attempts a WHERE a.turn_id=m.turn_id AND a.upstream_session_id=?))
		ORDER BY m.created_at DESC,m.seq DESC LIMIT ?`, principal, sessionID, sessionID, limit)
	if err != nil {
		return nil, fmt.Errorf("memstore: recent evidence: %w", err)
	}
	defer rows.Close()
	var out []MessageDetail
	for rows.Next() {
		var detail MessageDetail
		var payload []byte
		var codec string
		var codecVersion int
		var rawBytes int64
		if err := rows.Scan(&detail.MessageID, &detail.SessionID, &detail.TurnID, &detail.Role,
			&detail.State, &detail.Seq, &detail.CreatedAt, &detail.Project, &detail.WorkDir,
			&detail.Fidelity, &codec, &codecVersion, &rawBytes, &payload); err != nil {
			return nil, err
		}
		decoded, err := decodeBlock(codec, codecVersion, rawBytes, payload)
		if err != nil {
			return nil, fmt.Errorf("memstore: recent evidence message %s: %w", detail.MessageID, err)
		}
		detail.Text = string(decoded)
		detail.ByteLength = len(decoded)
		out = append(out, detail)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Reverse into chronological order so the prompt reads like the conversation.
	for left, right := 0, len(out)-1; left < right; left, right = left+1, right-1 {
		out[left], out[right] = out[right], out[left]
	}
	return out, nil
}

// KnowledgeJobStats reports the queue depth. It is a diagnostic, not a hot
// path, and it is scoped so one principal cannot read another's backlog.
func (s *Store) KnowledgeJobStats(ctx context.Context, scope SearchScope) (pending, failed int, err error) {
	principal, err := scope.principal()
	if err != nil {
		return 0, 0, err
	}
	err = s.db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM mem_v2_knowledge_jobs WHERE principal_id=? AND status='pending'),
		(SELECT COUNT(*) FROM mem_v2_knowledge_jobs WHERE principal_id=? AND status='failed')`,
		principal, principal).Scan(&pending, &failed)
	if err != nil {
		return 0, 0, fmt.Errorf("memstore: knowledge job stats: %w", err)
	}
	return pending, failed, nil
}
