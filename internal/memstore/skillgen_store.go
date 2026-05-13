package memstore

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	mrand "math/rand"
	"sort"
	"strings"
	"time"
)

// SkillCandidate statuses.
const (
	SkillStatusDraft         = "draft"
	SkillStatusPendingReview = "pending_review"
	SkillStatusApproved      = "approved"
	SkillStatusRejected      = "rejected"
)

// SkillCandidate is one auto-generated skill draft awaiting review.
type SkillCandidate struct {
	ID         string
	Trigger    string
	Adapter    string
	UserID     string
	ThreadID   string
	SessionID  string
	Status     string
	Score      float64
	ModelID    string
	Title      string
	SkillMD    string
	DraftPath  string
	Notes      string
	CreatedAt  time.Time
	ReviewedAt time.Time
}

// ModelStats tracks per-model skill-draft outcomes for epsilon-greedy selection.
type ModelStats struct {
	ModelID    string
	Attempts   int
	Approved   int
	Rejected   int
	AvgScore   float64
	LastUsedAt time.Time
}

// NewSkillCandidateID generates a short random id for a candidate.
func NewSkillCandidateID() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("sc_%d", time.Now().UnixNano())
	}
	return "sc_" + hex.EncodeToString(b[:])
}

// SaveSkillCandidate upserts (insert-or-replace by id) a candidate row.
func (s *Store) SaveSkillCandidate(c SkillCandidate) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("memstore: nil store")
	}
	if c.ID == "" {
		c.ID = NewSkillCandidateID()
	}
	if c.Status == "" {
		c.Status = SkillStatusDraft
	}
	if c.CreatedAt.IsZero() {
		c.CreatedAt = time.Now()
	}
	_, err := s.db.Exec(`INSERT OR REPLACE INTO skill_candidates
		(id, trigger, adapter, user_id, thread_id, session_id, status, score, model_id, title, skill_md, draft_path, notes, created_at, reviewed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		c.ID, c.Trigger, c.Adapter, c.UserID, c.ThreadID, c.SessionID, c.Status, c.Score, c.ModelID,
		c.Title, c.SkillMD, c.DraftPath, c.Notes, c.CreatedAt.Unix(), c.ReviewedAt.Unix())
	return err
}

// GetSkillCandidate returns one candidate by id.
func (s *Store) GetSkillCandidate(id string) (*SkillCandidate, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("memstore: nil store")
	}
	row := s.db.QueryRow(`SELECT id, trigger, adapter, user_id, thread_id, session_id, status, score, model_id, title, skill_md, draft_path, notes, created_at, reviewed_at
		FROM skill_candidates WHERE id = ?`, id)
	c := &SkillCandidate{}
	var created, reviewed int64
	if err := row.Scan(&c.ID, &c.Trigger, &c.Adapter, &c.UserID, &c.ThreadID, &c.SessionID, &c.Status, &c.Score, &c.ModelID,
		&c.Title, &c.SkillMD, &c.DraftPath, &c.Notes, &created, &reviewed); err != nil {
		return nil, err
	}
	c.CreatedAt = time.Unix(created, 0)
	if reviewed > 0 {
		c.ReviewedAt = time.Unix(reviewed, 0)
	}
	return c, nil
}

// ListSkillCandidatesByStatus lists candidates filtered by status ("" = any).
// Newest first, limited to n rows (n <= 0 defaults to 50).
func (s *Store) ListSkillCandidatesByStatus(status string, n int) ([]SkillCandidate, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("memstore: nil store")
	}
	if n <= 0 {
		n = 50
	}
	var (
		rowsErr error
		out     []SkillCandidate
	)
	if status == "" {
		rowsPtr, err := s.db.Query(`SELECT id, trigger, adapter, user_id, thread_id, session_id, status, score, model_id, title, skill_md, draft_path, notes, created_at, reviewed_at
			FROM skill_candidates ORDER BY created_at DESC LIMIT ?`, n)
		if err != nil {
			return nil, err
		}
		defer rowsPtr.Close()
		out, rowsErr = scanSkillCandidates(rowsPtr)
	} else {
		rowsPtr, err := s.db.Query(`SELECT id, trigger, adapter, user_id, thread_id, session_id, status, score, model_id, title, skill_md, draft_path, notes, created_at, reviewed_at
			FROM skill_candidates WHERE status = ? ORDER BY created_at DESC LIMIT ?`, status, n)
		if err != nil {
			return nil, err
		}
		defer rowsPtr.Close()
		out, rowsErr = scanSkillCandidates(rowsPtr)
	}
	return out, rowsErr
}

// scanSkillCandidates is a shared row-scanner.
func scanSkillCandidates(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]SkillCandidate, error) {
	var out []SkillCandidate
	for rows.Next() {
		c := SkillCandidate{}
		var created, reviewed int64
		if err := rows.Scan(&c.ID, &c.Trigger, &c.Adapter, &c.UserID, &c.ThreadID, &c.SessionID, &c.Status, &c.Score, &c.ModelID,
			&c.Title, &c.SkillMD, &c.DraftPath, &c.Notes, &created, &reviewed); err != nil {
			return nil, err
		}
		c.CreatedAt = time.Unix(created, 0)
		if reviewed > 0 {
			c.ReviewedAt = time.Unix(reviewed, 0)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// CountSkillCandidatesSince returns the number of candidates created since t.
// Used for daily-cap gating.
func (s *Store) CountSkillCandidatesSince(t time.Time) (int, error) {
	if s == nil || s.db == nil {
		return 0, fmt.Errorf("memstore: nil store")
	}
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM skill_candidates WHERE created_at >= ?`, t.Unix()).Scan(&n)
	return n, err
}

// UpdateSkillCandidateStatus sets status + reviewed_at.
func (s *Store) UpdateSkillCandidateStatus(id, status string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("memstore: nil store")
	}
	status = strings.ToLower(status)
	_, err := s.db.Exec(`UPDATE skill_candidates SET status = ?, reviewed_at = ? WHERE id = ?`,
		status, time.Now().Unix(), id)
	return err
}

// RecordModelAttempt increments the attempt counter before a draft is generated.
func (s *Store) RecordModelAttempt(modelID string) error {
	if s == nil || s.db == nil || modelID == "" {
		return nil
	}
	_, err := s.db.Exec(`INSERT INTO skill_gen_model_stats (model_id, attempts, last_used_at) VALUES (?, 1, ?)
		ON CONFLICT(model_id) DO UPDATE SET attempts = attempts + 1, last_used_at = excluded.last_used_at`,
		modelID, time.Now().Unix())
	return err
}

// RecordModelOutcome updates approved/rejected + avg_score (running mean over approved).
// score is ignored when approved==false.
func (s *Store) RecordModelOutcome(modelID string, approved bool, score float64) error {
	if s == nil || s.db == nil || modelID == "" {
		return nil
	}
	stats, err := s.GetModelStats(modelID)
	if err != nil {
		return err
	}
	if stats == nil {
		stats = &ModelStats{ModelID: modelID}
	}
	if approved {
		// Running mean over approved-only count
		newApproved := stats.Approved + 1
		stats.AvgScore = (stats.AvgScore*float64(stats.Approved) + score) / float64(newApproved)
		stats.Approved = newApproved
	} else {
		stats.Rejected++
	}
	stats.LastUsedAt = time.Now()
	_, err = s.db.Exec(`INSERT INTO skill_gen_model_stats (model_id, attempts, approved, rejected, avg_score, last_used_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(model_id) DO UPDATE SET approved = excluded.approved, rejected = excluded.rejected, avg_score = excluded.avg_score, last_used_at = excluded.last_used_at`,
		modelID, stats.Attempts, stats.Approved, stats.Rejected, stats.AvgScore, stats.LastUsedAt.Unix())
	return err
}

// GetModelStats returns stats for modelID, or nil when absent.
func (s *Store) GetModelStats(modelID string) (*ModelStats, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	row := s.db.QueryRow(`SELECT model_id, attempts, approved, rejected, avg_score, last_used_at FROM skill_gen_model_stats WHERE model_id = ?`, modelID)
	m := &ModelStats{}
	var last int64
	err := row.Scan(&m.ModelID, &m.Attempts, &m.Approved, &m.Rejected, &m.AvgScore, &last)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, err
	}
	if last > 0 {
		m.LastUsedAt = time.Unix(last, 0)
	}
	return m, nil
}

// ListModelStats returns all model stat rows.
func (s *Store) ListModelStats() ([]ModelStats, error) {
	if s == nil || s.db == nil {
		return nil, nil
	}
	rows, err := s.db.Query(`SELECT model_id, attempts, approved, rejected, avg_score, last_used_at FROM skill_gen_model_stats`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ModelStats
	for rows.Next() {
		m := ModelStats{}
		var last int64
		if err := rows.Scan(&m.ModelID, &m.Attempts, &m.Approved, &m.Rejected, &m.AvgScore, &last); err != nil {
			return nil, err
		}
		if last > 0 {
			m.LastUsedAt = time.Unix(last, 0)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// PickSkillGenModel returns the model to use for a draft using an
// epsilon-greedy policy over approval-rate × avg_score. When no history
// exists for a model in the candidate pool, it is forced (explored) first.
// defaultModel is always included in the pool. epsilon in [0, 1].
func (s *Store) PickSkillGenModel(candidates []string, defaultModel string, epsilon float64) string {
	pool := make([]string, 0, len(candidates)+1)
	seen := map[string]bool{}
	if defaultModel != "" {
		pool = append(pool, defaultModel)
		seen[defaultModel] = true
	}
	for _, m := range candidates {
		m = strings.TrimSpace(m)
		if m == "" || seen[m] {
			continue
		}
		pool = append(pool, m)
		seen[m] = true
	}
	if len(pool) == 0 {
		return defaultModel
	}
	if len(pool) == 1 {
		return pool[0]
	}
	// Explore unknowns first: any model with zero attempts gets forced.
	statsMap := map[string]*ModelStats{}
	if all, err := s.ListModelStats(); err == nil {
		for i := range all {
			statsMap[all[i].ModelID] = &all[i]
		}
	}
	var unknown []string
	for _, m := range pool {
		if _, ok := statsMap[m]; !ok {
			unknown = append(unknown, m)
		}
	}
	if len(unknown) > 0 {
		return unknown[mrand.Intn(len(unknown))]
	}
	// Epsilon: random explore.
	if epsilon > 0 && mrand.Float64() < epsilon {
		return pool[mrand.Intn(len(pool))]
	}
	// Exploit: score = approval_rate * (1 + avg_score).
	type scored struct {
		id string
		v  float64
	}
	scoredPool := make([]scored, 0, len(pool))
	for _, m := range pool {
		st := statsMap[m]
		total := float64(st.Approved + st.Rejected)
		ar := 0.0
		if total > 0 {
			ar = float64(st.Approved) / total
		}
		// Wilson-lower-ish confidence adjustment: shrink toward 0 when total is small.
		shrink := total / (total + 3.0)
		score := math.Max(0, ar*(1.0+st.AvgScore)*shrink)
		scoredPool = append(scoredPool, scored{id: m, v: score})
	}
	sort.Slice(scoredPool, func(i, j int) bool { return scoredPool[i].v > scoredPool[j].v })
	return scoredPool[0].id
}
