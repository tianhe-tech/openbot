package memstore

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// MemRecord is one conversation turn persisted to the store.
type MemRecord struct {
	ID         string
	Adapter    string
	UserID     string
	Ts         time.Time
	Request    string
	Response   string // first 600 chars of response
	Summary    string // one-line human-readable summary
	Project    string // coarse project label (e.g. "爬虫项目")
	WorkDir    string // absolute working directory of the opencode session (e.g. "/root/openbot")
	Action     string // "创建" / "修改" / "调试" / "部署" / "查询" / "other"
	Tags       string // comma-separated keywords
	Strength   float64
	RecallCnt  int
	NextReview time.Time
}

// ProjectSummary aggregates memory records per project.
type ProjectSummary struct {
	Project string
	Adapter string
	UserID  string
	Count   int
	First   time.Time
	Last    time.Time
	Actions []string
	Records []MemRecord
}

// Store wraps a SQLite database.
type Store struct {
	db *sql.DB
}

// Open opens (or creates) the SQLite database at path and ensures the schema exists.
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_journal_mode=WAL&_busy_timeout=5000", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("memstore: open %s: %w", path, err)
	}
	db.SetMaxOpenConns(1) // SQLite WAL works fine with single writer
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("memstore: migrate: %w", err)
	}
	return s, nil
}

// Close releases the underlying database connection.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS mem_records (
			id          TEXT PRIMARY KEY,
			adapter     TEXT NOT NULL,
			user_id     TEXT NOT NULL,
			ts          INTEGER NOT NULL,
			request     TEXT NOT NULL,
			response    TEXT NOT NULL,
			summary     TEXT NOT NULL DEFAULT '',
			project     TEXT NOT NULL DEFAULT '',
			action      TEXT NOT NULL DEFAULT 'other',
			tags        TEXT NOT NULL DEFAULT '',
			strength    REAL NOT NULL DEFAULT 1.0,
			recall_cnt  INTEGER NOT NULL DEFAULT 0,
			next_review INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_mem_adapter_user ON mem_records(adapter, user_id)`,
		`CREATE INDEX IF NOT EXISTS idx_mem_ts           ON mem_records(ts)`,
		`CREATE INDEX IF NOT EXISTS idx_mem_project      ON mem_records(project)`,
		// FTS5 virtual table for BM25 full-text search.
		// trigram tokenizer: indexes every 3-char window, supports CJK without extra config.
		// record_id is UNINDEXED so it's stored but not tokenized.
		`CREATE VIRTUAL TABLE IF NOT EXISTS mem_fts USING fts5(
			record_id UNINDEXED,
			content,
			tokenize='trigram'
		)`,
		// session_handoff: when an opencode session gets stuck (scheduler deadlock,
		// see sst/opencode#21173), we save a compressed summary of the session so
		// the next user turn on the same thread can auto-create a new session and
		// carry the context over. One pending record per thread; consumed flag
		// prevents re-injection; 24h TTL via created_at filter at read time.
		`CREATE TABLE IF NOT EXISTS session_handoff (
			thread_id       TEXT PRIMARY KEY,
			adapter         TEXT NOT NULL,
			user_id         TEXT NOT NULL,
			old_session_id  TEXT NOT NULL,
			created_at      INTEGER NOT NULL,
			summary         TEXT NOT NULL,
			last_user_msg   TEXT NOT NULL DEFAULT '',
			consumed        INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_handoff_created ON session_handoff(created_at)`,
		// skill_candidates: one row per auto-generated skill draft. status evolves
		// draft → pending_review → approved|rejected. When approved, the installer
		// writes skill_md to skills/<title>/SKILL.md on disk.
		`CREATE TABLE IF NOT EXISTS skill_candidates (
			id              TEXT PRIMARY KEY,
			trigger         TEXT NOT NULL,
			adapter         TEXT NOT NULL,
			user_id         TEXT NOT NULL,
			thread_id       TEXT NOT NULL,
			session_id      TEXT NOT NULL,
			status          TEXT NOT NULL,
			score           REAL NOT NULL DEFAULT 0,
			model_id        TEXT NOT NULL DEFAULT '',
			title           TEXT NOT NULL,
			skill_md        TEXT NOT NULL,
			draft_path      TEXT NOT NULL DEFAULT '',
			notes           TEXT NOT NULL DEFAULT '',
			created_at      INTEGER NOT NULL,
			reviewed_at     INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_skill_status ON skill_candidates(status, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_skill_user ON skill_candidates(adapter, user_id, created_at)`,
		// skill_gen_model_stats: per-model outcome counters driving epsilon-greedy
		// self-selection. avg_score is a running mean over approved candidates.
		`CREATE TABLE IF NOT EXISTS skill_gen_model_stats (
			model_id        TEXT PRIMARY KEY,
			attempts        INTEGER NOT NULL DEFAULT 0,
			approved        INTEGER NOT NULL DEFAULT 0,
			rejected        INTEGER NOT NULL DEFAULT 0,
			avg_score       REAL NOT NULL DEFAULT 0,
			last_used_at    INTEGER NOT NULL DEFAULT 0
		)`,
		// pending_retry: messages that timed out (context deadline exceeded with
		// zero accumulated reply) are queued here for automatic off-peak re-processing.
		// status: pending → processing → done|failed.
		// retry_count drives exponential backoff and max-retries gate.
		`CREATE TABLE IF NOT EXISTS pending_retry (
			id                TEXT PRIMARY KEY,
			adapter           TEXT NOT NULL,
			user_id           TEXT NOT NULL,
			thread_id         TEXT NOT NULL DEFAULT '',
			channel           TEXT NOT NULL DEFAULT '',
			content           TEXT NOT NULL,
			attachments_json  TEXT NOT NULL DEFAULT '',
			metadata_json     TEXT NOT NULL DEFAULT '{}',
			fail_reason       TEXT NOT NULL DEFAULT '',
			status            TEXT NOT NULL DEFAULT 'pending',
			retry_count       INTEGER NOT NULL DEFAULT 0,
			created_at        INTEGER NOT NULL,
			last_attempt_at   INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_retry_status    ON pending_retry(status, created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_retry_user      ON pending_retry(adapter, user_id)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("exec %q: %w", stmt[:min(40, len(stmt))], err)
		}
	}
	// Add work_dir column to existing databases (ALTER TABLE is idempotent via error ignore).
	if _, err := s.db.Exec(`ALTER TABLE mem_records ADD COLUMN work_dir TEXT NOT NULL DEFAULT ''`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column") {
			log.Printf("memstore: add work_dir column: %v", err)
		}
	}
	// Backfill any existing records that are not yet in mem_fts.
	// Uses a LEFT JOIN so only missing entries are inserted, safe to run on every startup.
	_, err := s.db.Exec(`
		INSERT INTO mem_fts(record_id, content)
		SELECT r.id, r.summary || ' ' || r.project || ' ' || r.request
		FROM   mem_records r
		LEFT JOIN mem_fts f ON f.record_id = r.id
		WHERE  f.record_id IS NULL`)
	if err != nil {
		log.Printf("memstore: fts backfill: %v", err)
	}
	s.purgeRecallNoise()
	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Record writes a new MemRecord.  Call asynchronously from the send path.
func (s *Store) Record(rec MemRecord) error {
	if rec.ID == "" {
		rec.ID = newID()
	}
	if rec.Ts.IsZero() {
		rec.Ts = time.Now()
	}
	if rec.Strength == 0 {
		rec.Strength = 1.0
	}
	if rec.NextReview.IsZero() {
		rec.NextReview = time.Now().Add(NextReviewAfter(0))
	}

	// Truncate response to keep DB lean
	resp := rec.Response
	if len(resp) > 600 {
		resp = resp[:600]
	}

	_, err := s.db.Exec(`
		INSERT INTO mem_records
			(id, adapter, user_id, ts, request, response, summary, project, work_dir, action, tags, strength, recall_cnt, next_review)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		rec.ID, rec.Adapter, rec.UserID, rec.Ts.Unix(),
		rec.Request, resp, rec.Summary, rec.Project, rec.WorkDir, rec.Action, rec.Tags,
		rec.Strength, rec.RecallCnt, rec.NextReview.Unix(),
	)
	if err != nil {
		return fmt.Errorf("memstore: record insert: %w", err)
	}
	// Index into FTS5: concatenate summary + project + request for richer coverage.
	ftsContent := strings.TrimSpace(rec.Summary + " " + rec.Project + " " + rec.Request)
	if _, ftsErr := s.db.Exec(`INSERT INTO mem_fts(record_id, content) VALUES(?,?)`,
		rec.ID, ftsContent); ftsErr != nil {
		log.Printf("memstore: fts insert %s: %v", rec.ID, ftsErr)
	}
	return nil
}

// Recall performs hybrid BM25 (FTS5 倒排索引) + LIKE retrieval fused via RRF.
// adapter == "" means cross-adapter; userID == "" means all users.
// since filters to records newer than that time (zero = no limit).
func (s *Store) Recall(keywords []string, adapter, userID string, since time.Time, limit int) ([]MemRecord, error) {
	if len(keywords) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = 10
	}
	pool := limit * 3 // over-fetch candidates before fusion

	// 1. BM25 via FTS5 倒排索引 (3+ rune terms only; trigram tokenizer requirement)
	bm25IDs := s.recallBM25(keywords, pool)

	// 2. LIKE fallback (covers 2-char CJK terms that trigram misses)
	likeRows, err := s.recallLIKE(keywords, adapter, userID, since, pool)
	if err != nil {
		return nil, err
	}

	// 3. RRF fusion
	likeIDs := make([]string, len(likeRows))
	for i, r := range likeRows {
		likeIDs[i] = r.ID
	}
	mergedIDs := rrfMerge(bm25IDs, likeIDs)

	// 4. Build record map; fetch BM25-only hits from DB with filters applied
	recordMap := make(map[string]MemRecord, len(likeRows))
	for _, r := range likeRows {
		recordMap[r.ID] = r
	}
	var missingIDs []string
	for _, id := range bm25IDs {
		if _, ok := recordMap[id]; !ok {
			missingIDs = append(missingIDs, id)
		}
	}
	if len(missingIDs) > 0 {
		extra, ferr := s.fetchByIDs(missingIDs, adapter, userID, since)
		if ferr == nil {
			for _, r := range extra {
				recordMap[r.ID] = r
			}
		}
	}

	// 5. Build final ordered result up to limit
	var results []MemRecord
	seen := make(map[string]struct{})
	for _, id := range mergedIDs {
		if len(results) >= limit {
			break
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		if rec, ok := recordMap[id]; ok {
			results = append(results, rec)
		}
	}

	// 6. Strengthen recalled records asynchronously (SM-2)
	go func() {
		for _, r := range results {
			if err := s.strengthen(r.ID); err != nil {
				log.Printf("memstore: strengthen %s: %v", r.ID, err)
			}
		}
	}()

	return results, nil
}

// recallBM25 performs FTS5 BM25 full-text search via the inverted index.
// Only keywords with >= 3 Unicode runes are used (trigram tokenizer requirement).
func (s *Store) recallBM25(keywords []string, limit int) []string {
	var terms []string
	for _, kw := range keywords {
		if len([]rune(strings.TrimSpace(kw))) < 3 {
			continue
		}
		// Escape double-quotes for FTS5 quoted-term syntax
		escaped := strings.ReplaceAll(kw, `"`, `""`)
		terms = append(terms, `"`+escaped+`"`)
	}
	if len(terms) == 0 {
		return nil
	}

	rows, err := s.db.Query(`
		SELECT record_id FROM mem_fts
		WHERE  mem_fts MATCH ?
		ORDER  BY bm25(mem_fts)
		LIMIT  ?`,
		strings.Join(terms, " OR "), limit)
	if err != nil {
		log.Printf("memstore: bm25 query: %v", err)
		return nil
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	return ids
}

// recallLIKE returns records matching keywords via SQL LIKE with all filters applied.
func (s *Store) recallLIKE(keywords []string, adapter, userID string, since time.Time, limit int) ([]MemRecord, error) {
	var conds []string
	var args []interface{}
	for _, kw := range keywords {
		if strings.TrimSpace(kw) == "" {
			continue
		}
		like := "%" + strings.ToLower(kw) + "%"
		conds = append(conds, `(LOWER(tags) LIKE ? OR LOWER(summary) LIKE ? OR LOWER(request) LIKE ?)`)
		args = append(args, like, like, like)
	}
	if len(conds) == 0 {
		return nil, nil
	}
	where := "(" + strings.Join(conds, " OR ") + ")"
	if adapter != "" {
		where += " AND adapter = ?"
		args = append(args, adapter)
	}
	if userID != "" {
		where += " AND user_id = ?"
		args = append(args, userID)
	}
	if !since.IsZero() {
		where += " AND ts >= ?"
		args = append(args, since.Unix())
	}
	where += " AND strength > 0.05"
	args = append(args, limit)

	rows, err := s.db.Query(`
		SELECT id, adapter, user_id, ts, request, response, summary, project, work_dir, action, tags,
		       strength, recall_cnt, next_review
		FROM   mem_records
		WHERE  `+where+`
		ORDER  BY strength DESC, ts DESC
		LIMIT  ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("memstore: like query: %w", err)
	}
	defer rows.Close()
	return scanRecords(rows)
}

// fetchByIDs fetches records by ID slice with standard filters applied.
// Used to retrieve BM25 hits that were not returned by the LIKE query.
func (s *Store) fetchByIDs(ids []string, adapter, userID string, since time.Time) ([]MemRecord, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	var args []interface{}
	for _, id := range ids {
		args = append(args, id)
	}
	where := "id IN (" + placeholders + ") AND strength > 0.05"
	if adapter != "" {
		where += " AND adapter = ?"
		args = append(args, adapter)
	}
	if userID != "" {
		where += " AND user_id = ?"
		args = append(args, userID)
	}
	if !since.IsZero() {
		where += " AND ts >= ?"
		args = append(args, since.Unix())
	}

	rows, err := s.db.Query(`
		SELECT id, adapter, user_id, ts, request, response, summary, project, work_dir, action, tags,
		       strength, recall_cnt, next_review
		FROM   mem_records
		WHERE  `+where, args...)
	if err != nil {
		return nil, fmt.Errorf("memstore: fetchByIDs: %w", err)
	}
	defer rows.Close()
	return scanRecords(rows)
}

// rrfMerge fuses multiple ranked ID lists via Reciprocal Rank Fusion (k=60).
// Returns a deduplicated ID list sorted by descending RRF score.
func rrfMerge(lists ...[]string) []string {
	const k = 60.0
	scores := make(map[string]float64)
	var order []string
	for _, list := range lists {
		for rank, id := range list {
			if _, seen := scores[id]; !seen {
				order = append(order, id)
			}
			scores[id] += 1.0 / (k + float64(rank+1))
		}
	}
	sort.SliceStable(order, func(i, j int) bool {
		return scores[order[i]] > scores[order[j]]
	})
	return order
}

// RecallByProject returns all records for a given project name (fuzzy), ordered by time.
func (s *Store) RecallByProject(project, adapter string, limit int) ([]MemRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	like := "%" + strings.ToLower(strings.TrimSpace(project)) + "%"
	var args []interface{}
	where := "LOWER(project) LIKE ?"
	args = append(args, like)
	if adapter != "" {
		where += " AND adapter = ?"
		args = append(args, adapter)
	}
	args = append(args, limit)

	rows, err := s.db.Query(`
		SELECT id, adapter, user_id, ts, request, response, summary, project, work_dir, action, tags,
		       strength, recall_cnt, next_review
		FROM   mem_records
		WHERE  `+where+`
		ORDER  BY ts ASC
		LIMIT  ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("memstore: recall_by_project: %w", err)
	}
	defer rows.Close()
	return scanRecords(rows)
}

// ProjectSummaries returns aggregated project info for a user across adapters.
// adapter == "" means all adapters. since filters to records newer than that time (zero = no limit).
// minCount excludes projects with fewer records (use 2 to filter one-off noise).
func (s *Store) ProjectSummaries(userID, adapter string, since time.Time, minCount int) ([]ProjectSummary, error) {
	var args []interface{}
	where := "1=1"
	if userID != "" {
		where += " AND user_id = ?"
		args = append(args, userID)
	}
	if adapter != "" {
		where += " AND adapter = ?"
		args = append(args, adapter)
	}
	if !since.IsZero() {
		where += " AND ts >= ?"
		args = append(args, since.Unix())
	}
	if minCount <= 0 {
		minCount = 1
	}

	rows, err := s.db.Query(`
		SELECT project, adapter, user_id,
		       COUNT(*) as cnt,
		       MIN(ts)  as first_ts,
		       MAX(ts)  as last_ts,
		       GROUP_CONCAT(DISTINCT action) as actions
		FROM   mem_records
		WHERE  `+where+` AND project != ''
		GROUP  BY project, adapter, user_id
		HAVING COUNT(*) >= ?
		ORDER  BY last_ts DESC`, append(args, minCount)...)
	if err != nil {
		return nil, fmt.Errorf("memstore: project_summaries: %w", err)
	}
	defer rows.Close()

	var out []ProjectSummary
	for rows.Next() {
		var ps ProjectSummary
		var firstUnix, lastUnix int64
		var actionsStr string
		if err := rows.Scan(
			&ps.Project, &ps.Adapter, &ps.UserID,
			&ps.Count, &firstUnix, &lastUnix, &actionsStr,
		); err != nil {
			return nil, err
		}
		ps.First = time.Unix(firstUnix, 0)
		ps.Last = time.Unix(lastUnix, 0)
		ps.Actions = strings.Split(actionsStr, ",")
		out = append(out, ps)
	}
	return out, rows.Err()
}

// Recent returns the most recent N records across adapters (or filtered by adapter).
func (s *Store) Recent(adapter, userID string, days, limit int) ([]MemRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	since := time.Now().AddDate(0, 0, -days).Unix()
	var args []interface{}
	where := "ts >= ?"
	args = append(args, since)
	if adapter != "" {
		where += " AND adapter = ?"
		args = append(args, adapter)
	}
	if userID != "" {
		where += " AND user_id = ?"
		args = append(args, userID)
	}
	args = append(args, limit)

	rows, err := s.db.Query(`
		SELECT id, adapter, user_id, ts, request, response, summary, project, work_dir, action, tags,
		       strength, recall_cnt, next_review
		FROM   mem_records
		WHERE  `+where+`
		ORDER  BY ts DESC
		LIMIT  ?`, args...)
	if err != nil {
		return nil, fmt.Errorf("memstore: recent: %w", err)
	}
	defer rows.Close()
	return scanRecords(rows)
}

// DecayAll applies time-based strength decay to all records.
// Safe to call periodically (e.g., once per hour) in a goroutine.
func (s *Store) DecayAll() error {
	rows, err := s.db.Query(`SELECT id, strength, ts FROM mem_records WHERE strength > 0.01`)
	if err != nil {
		return fmt.Errorf("memstore: decay query: %w", err)
	}
	defer rows.Close()

	now := time.Now()
	type update struct {
		id       string
		strength float64
	}
	var updates []update
	for rows.Next() {
		var id string
		var strength float64
		var ts int64
		if err := rows.Scan(&id, &strength, &ts); err != nil {
			continue
		}
		daysSince := now.Sub(time.Unix(ts, 0)).Hours() / 24
		newStrength := EffectiveStrength(strength, daysSince)
		if newStrength < strength-0.001 { // only update if meaningfully changed
			updates = append(updates, update{id, newStrength})
		}
	}
	if rows.Err() != nil {
		return rows.Err()
	}

	for _, u := range updates {
		if _, err := s.db.Exec(`UPDATE mem_records SET strength=? WHERE id=?`, u.strength, u.id); err != nil {
			log.Printf("memstore: decay update %s: %v", u.id, err)
		}
	}
	return nil
}

func (s *Store) strengthen(id string) error {
	var recallCnt int
	_ = s.db.QueryRow(`SELECT recall_cnt FROM mem_records WHERE id = ?`, id).Scan(&recallCnt)
	_, err := s.db.Exec(`
		UPDATE mem_records
		SET    strength   = MIN(1.0, strength + 0.15),
		       recall_cnt = recall_cnt + 1,
		       next_review = ?
		WHERE  id = ?`,
		time.Now().Add(NextReviewAfter(recallCnt)).Unix(), id)
	return err
}

// ---- helpers ----

// purgeRecallNoise deletes records where the stored request is itself a recall/history query.
// These are meta-questions ("我最近开发了啥") that were mistakenly stored as work records
// before the RecordConversation guard was in place. Safe to call on every startup.
func (s *Store) purgeRecallNoise() {
	// Build OR conditions for each recall trigger keyword.
	noisePatterns := []string{
		// Original trigger set
		"%之前%", "%以前%", "%上次%", "%曾经%",
		"%开发了%", "%做过%", "%写过%", "%建过%", "%实现过%",
		"%我做过%", "%我开发过%",
		// Question-form triggers (new)
		"%是啊%", "%是啥%",
		"%啥项目%", "%啥程序%", "%啥软件%",
		"%什么项目%", "%什么程序%",
		"%哪些项目%", "%哪个项目%",
		"%最近开发%", "%近期开发%", "%近来开发%",
	}

	var conds []string
	var args []interface{}
	for _, p := range noisePatterns {
		conds = append(conds, "LOWER(request) LIKE ?")
		args = append(args, p)
	}
	where := strings.Join(conds, " OR ")

	// Collect IDs to delete so we can also remove from FTS.
	rows, err := s.db.Query("SELECT id FROM mem_records WHERE "+where, args...)
	if err != nil {
		log.Printf("memstore: purge query: %v", err)
		return
	}
	var ids []string
	for rows.Next() {
		var id string
		if rows.Scan(&id) == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()

	if len(ids) == 0 {
		return
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(ids)), ",")
	idArgs := make([]interface{}, len(ids))
	for i, id := range ids {
		idArgs[i] = id
	}

	if _, err := s.db.Exec("DELETE FROM mem_records WHERE id IN ("+placeholders+")", idArgs...); err != nil {
		log.Printf("memstore: purge delete records: %v", err)
	}
	if _, err := s.db.Exec("DELETE FROM mem_fts WHERE record_id IN ("+placeholders+")", idArgs...); err != nil {
		log.Printf("memstore: purge delete fts: %v", err)
	}
	log.Printf("memstore: purged %d recall-noise records from DB", len(ids))
}

func scanRecords(rows *sql.Rows) ([]MemRecord, error) {
	var out []MemRecord
	for rows.Next() {
		var r MemRecord
		var ts, nextReview int64
		if err := rows.Scan(
			&r.ID, &r.Adapter, &r.UserID, &ts,
			&r.Request, &r.Response, &r.Summary, &r.Project, &r.WorkDir, &r.Action,
			&r.Tags, &r.Strength, &r.RecallCnt, &nextReview,
		); err != nil {
			return nil, fmt.Errorf("memstore: scan: %w", err)
		}
		r.Ts = time.Unix(ts, 0)
		r.NextReview = time.Unix(nextReview, 0)
		out = append(out, r)
	}
	return out, rows.Err()
}

func newID() string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 16)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return "mem_" + string(b)
}
