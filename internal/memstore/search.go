package memstore

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Search tuning. The candidate pool is bounded so a broad query cannot turn
// into an unbounded scan; when the pool is exhausted the result is reported as
// too broad instead of silently returning an arbitrary subset.
const (
	defaultSearchLimit  = 10
	maxSearchLimit      = 50
	maxSearchCandidates = 100
	maxSearchTerms      = 12
	maxFallbackScan     = 2000
	maxExcerptBytes     = 512
	minTrigramRunes     = 3
)

var (
	// ErrSearchRestartRequired means the ranked dataset changed since the
	// cursor was issued (new messages indexed, or a different query/scope).
	// The caller must restart the search rather than continue a stale ranking.
	ErrSearchRestartRequired = errors.New("memstore: search cursor is stale; restart the search")
	// ErrSearchScopeRequired means no principal was supplied. Unscoped search
	// is never allowed: it would expose every user's archive.
	ErrSearchScopeRequired = errors.New("memstore: search requires an adapter and user scope")
)

// SearchScope identifies whose archive is being read. Adapter and UserID are
// mandatory; Project, Since, and Until narrow the scope further.
type SearchScope struct {
	Adapter string
	UserID  string
	Project string
	Since   int64 // inclusive millisecond bound, 0 means unbounded
	Until   int64 // inclusive millisecond bound, 0 means unbounded
}

func (scope SearchScope) principal() (string, error) {
	if strings.TrimSpace(scope.Adapter) == "" || strings.TrimSpace(scope.UserID) == "" {
		return "", ErrSearchScopeRequired
	}
	return archiveID("principal", scope.Adapter, scope.UserID), nil
}

// PrincipalFor returns the principal a scope resolves to, or "" when the scope
// is incomplete.
//
// It exists for callers outside this package that need to build a row carrying
// a principal — the knowledge service, for instance, which constructs facts and
// must stamp them with the same principal the store will check them against.
// Returning "" rather than an error is deliberate: the caller's next write will
// fail validation with a message that names the missing field, which is a
// better diagnostic than a second error path here.
func PrincipalFor(scope SearchScope) string {
	principal, err := scope.principal()
	if err != nil {
		return ""
	}
	return principal
}

// SearchFilters echoes the effective filters so a caller can tell what was
// actually applied rather than what it hoped was applied.
type SearchFilters struct {
	Adapter string   `json:"adapter"`
	UserID  string   `json:"user_id"`
	Project string   `json:"project,omitempty"`
	Since   int64    `json:"since,omitempty"`
	Until   int64    `json:"until,omitempty"`
	Terms   []string `json:"terms"`
	Mode    string   `json:"mode"`
}

// SearchHit is one message-level result. Excerpt is a bounded slice of the
// stored body and StartByte/EndByte locate it inside the original message text,
// so the caller can always fetch the full evidence with GetMessages.
type SearchHit struct {
	MessageID string  `json:"message_id"`
	SessionID string  `json:"session_id"`
	TurnID    string  `json:"turn_id"`
	Role      string  `json:"role"`
	Seq       int64   `json:"seq"`
	CreatedAt int64   `json:"created_at"`
	Project   string  `json:"project,omitempty"`
	WorkDir   string  `json:"work_dir,omitempty"`
	Excerpt   string  `json:"excerpt"`
	StartByte int     `json:"start_byte"`
	EndByte   int     `json:"end_byte"`
	Score     float64 `json:"score"`
	Exact     bool    `json:"exact_identifier"`
}

// SearchResult is the bounded answer to one search page.
type SearchResult struct {
	Hits       []SearchHit   `json:"hits"`
	Filters    SearchFilters `json:"filters"`
	Truncated  bool          `json:"truncated"`
	TooBroad   bool          `json:"too_broad"`
	Partial    bool          `json:"partial_coverage"`
	IndexLag   IndexCoverage `json:"index_status"`
	NextCursor string        `json:"next_cursor,omitempty"`
	Notice     string        `json:"notice,omitempty"`
	// WordIndexPartial reports that this query was answered by the word index
	// while that index was still being backfilled. It is separate from Partial
	// because the two mean different things: Partial says messages are not
	// indexed at all, while this says the messages are indexed but only in the
	// trigram index, so a word-routed query can miss older ones. A caller that
	// sees this should retry the same query with the trigram path (a CJK or
	// punctuated term forces it) or wait for the backfill.
	WordIndexPartial bool `json:"word_index_partial"`
}

// SearchOptions is one search request.
type SearchOptions struct {
	Scope  SearchScope
	Query  string
	Limit  int
	Cursor string
}

type searchCursor struct {
	Offset     int    `json:"o"`
	Query      string `json:"q"`
	Scope      string `json:"s"`
	Generation int64  `json:"g"`
}

func encodeSearchCursor(cursor searchCursor) string {
	encoded, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func decodeSearchCursor(value string) (searchCursor, error) {
	var cursor searchCursor
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return cursor, fmt.Errorf("memstore: malformed search cursor")
	}
	if err := json.Unmarshal(decoded, &cursor); err != nil {
		return cursor, fmt.Errorf("memstore: malformed search cursor")
	}
	if cursor.Offset < 0 || cursor.Offset > maxSearchCandidates {
		return cursor, fmt.Errorf("memstore: invalid search cursor offset")
	}
	return cursor, nil
}

// Search performs scoped lexical search over the archive. Scope constraints are
// applied inside the SQL before the candidate limit, so a narrow scope can never
// be widened by a broad query.
func (s *Store) Search(ctx context.Context, options SearchOptions) (SearchResult, error) {
	principal, err := options.Scope.principal()
	if err != nil {
		return SearchResult{}, err
	}
	limit := options.Limit
	if limit <= 0 {
		limit = defaultSearchLimit
	}
	if limit > maxSearchLimit {
		limit = maxSearchLimit
	}
	terms := searchTerms(options.Query)
	result := SearchResult{
		Filters: SearchFilters{
			Adapter: options.Scope.Adapter,
			UserID:  options.Scope.UserID,
			Project: options.Scope.Project,
			Since:   options.Scope.Since,
			Until:   options.Scope.Until,
			Terms:   terms,
		},
	}
	if len(terms) == 0 {
		result.Notice = "query produced no searchable terms; supply at least one word or identifier"
		return result, nil
	}
	status, err := s.IndexCoverage(ctx)
	if err != nil {
		return SearchResult{}, err
	}
	result.IndexLag = status
	result.Partial = status.PendingJobs > 0

	offset := 0
	if options.Cursor != "" {
		cursor, err := decodeSearchCursor(options.Cursor)
		if err != nil {
			return SearchResult{}, err
		}
		if cursor.Query != options.Query || cursor.Scope != scopeFingerprint(options.Scope) || cursor.Generation != status.UpdatedAt {
			return SearchResult{}, ErrSearchRestartRequired
		}
		offset = cursor.Offset
	}

	// Terms are routed to the index that can answer them. The word index is
	// 15.9x faster on the same rows, but it splits on word boundaries and so
	// cannot find a substring inside a token, and it cannot tokenize a CJK
	// phrase at all because there are no spaces to split on. The trigram index
	// finds any substring, which is what makes CJK search work. So a term that
	// is a plain ASCII word goes to the word index, and anything else goes to
	// the trigram index.
	//
	// Trigram MATCH cannot resolve one- and two-character terms either, so
	// those queries take an explicitly bounded scoped scan instead of returning
	// a misleading empty result.
	var wordTerms, trigramTerms []string
	for _, term := range terms {
		switch {
		case utf8.RuneCountInString(term) < minTrigramRunes:
			// Too short for either index; handled by the scan fallback.
		case isWordIndexableTerm(term):
			wordTerms = append(wordTerms, term)
		default:
			trigramTerms = append(trigramTerms, term)
		}
	}
	var hits []SearchHit
	switch {
	case len(wordTerms) > 0 && len(trigramTerms) == 0:
		result.Filters.Mode = "word"
		hits, err = s.searchWordFTS(ctx, principal, options.Scope, wordTerms, terms)
	case len(wordTerms) > 0:
		// A mixed query needs both indexes. Running them separately and merging
		// keeps each index on the terms it can answer, which is the whole point
		// of having two; a single query against either index would have to
		// route the other index's terms through a tokenizer that cannot match
		// them.
		result.Filters.Mode = "hybrid"
		hits, err = s.searchHybrid(ctx, principal, options.Scope, wordTerms, trigramTerms, terms)
	case len(trigramTerms) > 0:
		result.Filters.Mode = "fts"
		hits, err = s.searchFTS(ctx, principal, options.Scope, trigramTerms, terms)
	default:
		result.Filters.Mode = "scan"
		result.Notice = "query terms are shorter than three characters; used a bounded scoped scan"
		var capped bool
		hits, capped, err = s.searchScan(ctx, principal, options.Scope, terms)
		if capped {
			// The scan stopped at its cap, so the answer is a sample of the
			// newest messages rather than a count of all matches. Saying so is
			// the difference between "no more matches" and "not searched".
			result.TooBroad = true
			result.Notice = fmt.Sprintf(
				"query terms are shorter than three characters; scanned the newest %d messages only, "+
					"so older matches were not examined", maxFallbackScan)
		}
	}
	if err != nil {
		return SearchResult{}, err
	}
	// A word-routed query can only see chunks that have a word entry. On an
	// archive that was just upgraded, the backfill has not reached the older
	// chunks yet, so the answer is incomplete in a way that Partial does not
	// describe: the messages are indexed, just not in the index this query
	// used. Saying so is the difference between "no match" and "not searched".
	if result.Filters.Mode == "word" || result.Filters.Mode == "hybrid" {
		result.WordIndexPartial = !status.WordIndexComplete
	}
	if len(hits) >= maxSearchCandidates {
		result.TooBroad = true
	}
	if offset >= len(hits) {
		return result, nil
	}
	end := offset + limit
	if end > len(hits) {
		end = len(hits)
	}
	result.Hits = hits[offset:end]
	if end < len(hits) {
		result.Truncated = true
		result.NextCursor = encodeSearchCursor(searchCursor{
			Offset:     end,
			Query:      options.Query,
			Scope:      scopeFingerprint(options.Scope),
			Generation: status.UpdatedAt,
		})
	}
	return result, nil
}

func scopeFingerprint(scope SearchScope) string {
	return archiveID("scope", scope.Adapter, scope.UserID, scope.Project,
		fmt.Sprint(scope.Since), fmt.Sprint(scope.Until))
}

// isWordIndexableTerm reports whether the word-tokenized index can answer a
// term. The word index splits on word boundaries, so it can only match a term
// that is itself a whole token: ASCII letters, digits, and the punctuation that
// appears inside identifiers and paths. A term containing a CJK rune, a space,
// or a character the tokenizer treats as a separator cannot be matched as a
// whole token, so it has to go to the trigram index, which matches substrings.
//
// The check is deliberately conservative. Routing a term to the word index that
// the word index cannot match would silently drop results, which is worse than
// paying the trigram cost for a term that might have been word-indexable.
func isWordIndexableTerm(term string) bool {
	if term == "" {
		return false
	}
	for _, r := range term {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
		case r == '_' || r == '-' || r == '.' || r == '/' || r == ':' || r == '@' || r == '\\':
			// Characters that appear inside identifiers and paths. unicode61
			// treats these as separators, so a term containing one is not a
			// single token and must not be routed here.
			return false
		default:
			return false
		}
	}
	return true
}

// searchWordFTS runs the word-tokenized query. It is the fast path: the same
// rows cost 15.9x less than the trigram index because a word term is one token
// with one posting list instead of a phrase of overlapping trigrams.
func (s *Store) searchWordFTS(ctx context.Context, principal string, scope SearchScope, wordTerms, allTerms []string) ([]SearchHit, error) {
	return s.searchIndex(ctx, principal, scope, "mem_v2_chunk_word_fts", wordTerms, allTerms)
}

// searchHybrid answers a query whose terms split across the two indexes. Each
// index runs on the terms it can match, and the results are merged by message,
// keeping the best score. Merging is necessary rather than convenient: a term
// routed to the wrong index returns nothing, so a single-index query would
// silently drop half of a mixed query.
func (s *Store) searchHybrid(ctx context.Context, principal string, scope SearchScope, wordTerms, trigramTerms, allTerms []string) ([]SearchHit, error) {
	wordHits, err := s.searchWordFTS(ctx, principal, scope, wordTerms, allTerms)
	if err != nil {
		return nil, err
	}
	trigramHits, err := s.searchFTS(ctx, principal, scope, trigramTerms, allTerms)
	if err != nil {
		return nil, err
	}
	merged := make(map[string]SearchHit, len(wordHits)+len(trigramHits))
	for _, hit := range wordHits {
		merged[hit.MessageID] = hit
	}
	for _, hit := range trigramHits {
		if existing, ok := merged[hit.MessageID]; ok {
			// The same message found by both indexes is stronger evidence than
			// either alone, so the scores add rather than one replacing the
			// other. The excerpt is kept from whichever index scored higher,
			// because that is the chunk the ranking actually preferred.
			if hit.Score > existing.Score {
				existing.Excerpt = hit.Excerpt
				existing.StartByte = hit.StartByte
				existing.EndByte = hit.EndByte
			}
			existing.Score += hit.Score
			existing.Exact = existing.Exact || hit.Exact
			merged[hit.MessageID] = existing
			continue
		}
		merged[hit.MessageID] = hit
	}
	hits := make([]SearchHit, 0, len(merged))
	for _, hit := range merged {
		hits = append(hits, hit)
	}
	sortHits(hits)
	return hits, nil
}

// searchFTS runs the trigram query and collapses overlapping chunks to their
// owning message, keeping the best-scoring chunk as the evidence excerpt.
//
// The excerpt is reconstructed from the message block, because schema v7 made
// the index contentless: the text lives in mem_v2_blocks and nowhere else. It
// is fetched only for the bounded candidate set, though: FTS5 must score every
// matching chunk before the candidate limit applies, so selecting the block in
// the ranked query would read the full text of every match. On a broad query
// that is the entire archive, and the cost grows faster than the archive does.
// The inner query therefore ranks chunk ids alone, and the outer query joins
// back for the text of at most maxSearchCandidates rows.
//
// Scope predicates stay inside the inner query on purpose: applying them after
// the limit would let a broad query starve a narrow scope.
func (s *Store) searchFTS(ctx context.Context, principal string, scope SearchScope, ftsTerms, allTerms []string) ([]SearchHit, error) {
	return s.searchIndex(ctx, principal, scope, "mem_v2_chunk_fts", ftsTerms, allTerms)
}

// searchIndex is the shared body of both index queries. The index table name is
// a parameter rather than a second copy of the query because the two indexes
// differ only in which table is read: the ranking, the scope predicates, the
// candidate limit, and the collapse to messages are identical, and duplicating
// them would let the two paths drift apart.
//
// The table name is interpolated into the SQL, so it must never come from a
// caller. Both call sites pass a constant.
func (s *Store) searchIndex(ctx context.Context, principal string, scope SearchScope, indexTable string, ftsTerms, allTerms []string) ([]SearchHit, error) {
	match := buildMatchQuery(ftsTerms)
	// CROSS JOIN, not JOIN, on the FTS table. This is not a style choice.
	//
	// SQLite is free to reorder an inner join, and it chooses the order by
	// estimated cost. When schema v5 added mem_v2_message_scan, the planner
	// started driving this query from mem_v2_messages — it could now find the
	// principal's messages through an index instead of scanning them — and
	// probed the FTS table once per message. The plan went from
	//
	//	SCAN mem_v2_chunk_fts VIRTUAL TABLE INDEX 0:M1
	//	SEARCH c USING INTEGER PRIMARY KEY (rowid=?)
	//
	// to
	//
	//	SEARCH m USING INDEX mem_v2_message_scan (principal_id=?)
	//	SEARCH c USING COVERING INDEX ... (message_id=?)
	//	SCAN mem_v2_chunk_fts VIRTUAL TABLE INDEX 0:=M1
	//
	// which is the same rows in the worst possible order: every message in the
	// principal, each one probing the index for a match. Measured at 20k
	// messages the inner query went from 75.20 ms to 104471 ms, a factor of
	// 1388, and the full suite did not notice because no test asserted this
	// plan.
	//
	// CROSS JOIN is the documented way to tell SQLite not to reorder a join.
	// The FTS table must drive: MATCH is the only selective predicate here, and
	// the joins exist to filter and decorate its output, not the other way
	// round. The ON clauses are still honoured; only the ordering is pinned.
	//
	// The excerpt is reconstructed from the message block rather than read out
	// of the index, because schema v7 made both indexes contentless. The text
	// is still there — in mem_v2_blocks, which is where it always was — so the
	// outer query joins the block and the caller decodes it. That is a strictly
	// larger read per candidate than selecting ft.content was, and it was
	// measured before the migration was adopted: 1.13x at 2k messages and
	// indistinguishable at 20k, because the reconstruction is bounded by
	// maxSearchCandidates while the ranking work grows with the archive.
	//
	// The payload is selected here rather than fetched per hit afterwards so
	// the whole search stays one round trip. That does mean up to
	// maxSearchCandidates message bodies are held at once, which is the price
	// of the single query; the alternative is a second query per hit, and the
	// candidate cap is what keeps the first bounded.
	query := fmt.Sprintf(`SELECT c.message_id,c.start_byte,c.end_byte,
			m.session_id,m.turn_id,m.role,m.created_at,m.seq,s.project_key,s.work_dir,
			f.rank, b.codec,b.codec_version,b.raw_bytes,b.payload
		FROM (
			SELECT %[1]s.rowid AS chunk_id, bm25(%[1]s) AS rank
			FROM %[1]s
			CROSS JOIN mem_v2_chunks c ON c.id=%[1]s.rowid
			CROSS JOIN mem_v2_messages m ON m.id=c.message_id
			CROSS JOIN mem_v2_sessions s ON s.id=m.session_id
			WHERE %[1]s MATCH ?
				AND m.principal_id=?
				AND (?='' OR s.project_key=?)
				AND (?=0 OR m.created_at>=?)
				AND (?=0 OR m.created_at<=?)
			ORDER BY bm25(%[1]s)
			LIMIT ?
		) AS f
		JOIN mem_v2_chunks c ON c.id=f.chunk_id
		JOIN mem_v2_messages m ON m.id=c.message_id
		JOIN mem_v2_sessions s ON s.id=m.session_id
		JOIN mem_v2_blocks b ON b.id=m.block_id
		ORDER BY f.rank`, indexTable)
	rows, err := s.db.QueryContext(ctx, query,
		match, principal,
		scope.Project, scope.Project,
		scope.Since, scope.Since,
		scope.Until, scope.Until,
		maxSearchCandidates)
	if err != nil {
		return nil, fmt.Errorf("memstore: search: %w", err)
	}
	defer rows.Close()
	type candidate struct {
		messageID string
		start     int
		end       int
		hit       SearchHit
		rank      float64
		codec     string
		version   int
		rawBytes  int64
		payload   []byte
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.messageID, &c.start, &c.end,
			&c.hit.SessionID, &c.hit.TurnID, &c.hit.Role, &c.hit.CreatedAt, &c.hit.Seq,
			&c.hit.Project, &c.hit.WorkDir, &c.rank,
			&c.codec, &c.version, &c.rawBytes, &c.payload); err != nil {
			return nil, err
		}
		c.hit.MessageID = c.messageID
		c.hit.StartByte = c.start
		c.hit.EndByte = c.end
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	if len(candidates) == 0 {
		return nil, nil
	}
	// Collapse to one hit per message, keeping the best-ranked chunk. This runs
	// before any block is decoded, and the ordering matters: a message with
	// several matching chunks appears once per chunk in the candidate list, so
	// decoding first would decode the same block repeatedly. Measured at 2k
	// messages the candidate list holds 1.27 chunks per message, so decoding
	// first would do 27% more work than the answer needs.
	best := make(map[string]int, len(candidates))
	for i, c := range candidates {
		if existing, ok := best[c.messageID]; !ok || c.rank < candidates[existing].rank {
			best[c.messageID] = i
		}
	}
	hits := make([]SearchHit, 0, len(best))
	for _, index := range best {
		c := candidates[index]
		decoded, err := decodeBlock(c.codec, c.version, c.rawBytes, c.payload)
		if err != nil {
			return nil, fmt.Errorf("memstore: search message %s: %w", c.messageID, err)
		}
		text := string(decoded)
		// The chunk range was written against this exact text, so a range that
		// does not fit means the block and the chunk disagree. Reporting it is
		// better than slicing out of bounds or silently returning a shorter
		// excerpt that looks like a match.
		if c.start < 0 || c.end > len(text) || c.start > c.end {
			return nil, fmt.Errorf("memstore: search message %s: chunk range [%d,%d) is outside a %d-byte body",
				c.messageID, c.start, c.end, len(text))
		}
		excerpt := text[c.start:c.end]
		if len(excerpt) > maxExcerptBytes {
			excerpt = excerpt[:alignRuneStart(excerpt, maxExcerptBytes)]
		}
		c.hit.Excerpt = excerpt
		// bm25 returns negative values where more negative is better; flip the
		// sign so a larger score means a better match.
		c.hit.Score = -c.rank
		c.hit.Exact = containsExactIdentifier(excerpt, allTerms)
		if c.hit.Exact {
			c.hit.Score += 1
		}
		hits = append(hits, c.hit)
	}
	sortHits(hits)
	return hits, nil
}

// searchScan is the bounded fallback for queries neither FTS5 tokenizer can
// index. It examines at most maxFallbackScan messages inside the scope, newest
// first, and reports through capped whether that limit stopped it before it
// reached the end of the scope.
//
// The cap has to be reported, not just applied. A scan that examined the newest
// 2000 messages and found three matches has not answered "how many matches are
// there"; it has answered "how many are in the newest 2000". Returning those
// three without saying so is the same failure as reporting an unindexed message
// as a confident miss, and it is worse here because the query is the one a user
// reaches for when they are already unsure what they are looking for.
func (s *Store) searchScan(ctx context.Context, principal string, scope SearchScope, terms []string) ([]SearchHit, bool, error) {
	// One row beyond the cap is requested so the caller can tell the difference
	// between "exactly maxFallbackScan messages exist" and "there are more".
	// Without the extra row the two cases are indistinguishable and the honest
	// answer would have to be a guess.
	rows, err := s.db.QueryContext(ctx, `SELECT m.id,m.session_id,m.turn_id,m.role,m.created_at,m.seq,
			s.project_key,s.work_dir,b.codec,b.codec_version,b.raw_bytes,b.payload
		FROM mem_v2_messages m
		JOIN mem_v2_sessions s ON s.id=m.session_id
		JOIN mem_v2_blocks b ON b.id=m.block_id
		WHERE m.principal_id=?
			AND (?='' OR s.project_key=?)
			AND (?=0 OR m.created_at>=?)
			AND (?=0 OR m.created_at<=?)
		ORDER BY m.created_at DESC, m.seq DESC
		LIMIT ?`,
		principal,
		scope.Project, scope.Project,
		scope.Since, scope.Since,
		scope.Until, scope.Until,
		maxFallbackScan+1)
	if err != nil {
		return nil, false, fmt.Errorf("memstore: search scan: %w", err)
	}
	defer rows.Close()
	var hits []SearchHit
	examined := 0
	for rows.Next() {
		examined++
		if examined > maxFallbackScan {
			break
		}
		var hit SearchHit
		var payload []byte
		var codec string
		var codecVersion int
		var rawBytes int64
		if err := rows.Scan(&hit.MessageID, &hit.SessionID, &hit.TurnID, &hit.Role,
			&hit.CreatedAt, &hit.Seq, &hit.Project, &hit.WorkDir,
			&codec, &codecVersion, &rawBytes, &payload); err != nil {
			return nil, false, err
		}
		decoded, err := decodeBlock(codec, codecVersion, rawBytes, payload)
		if err != nil {
			return nil, false, fmt.Errorf("memstore: search scan message %s: %w", hit.MessageID, err)
		}
		text := string(decoded)
		lower := strings.ToLower(text)
		matched := 0
		firstOffset := -1
		for _, term := range terms {
			idx := strings.Index(lower, strings.ToLower(term))
			if idx < 0 {
				continue
			}
			matched++
			if firstOffset < 0 || idx < firstOffset {
				firstOffset = idx
			}
		}
		if matched == 0 {
			continue
		}
		hit.Score = float64(matched)
		hit.Exact = containsExactIdentifier(text, terms)
		if hit.Exact {
			hit.Score += 1
		}
		start, end := excerptWindow(text, firstOffset, maxExcerptBytes)
		hit.StartByte, hit.EndByte = start, end
		hit.Excerpt = text[start:end]
		hits = append(hits, hit)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	sortHits(hits)
	return hits, examined > maxFallbackScan, nil
}

// excerptWindow returns a bounded window around offset, aligned to rune
// boundaries so the excerpt is always valid UTF-8.
func excerptWindow(text string, offset, budget int) (int, int) {
	if offset < 0 {
		offset = 0
	}
	if offset > len(text) {
		offset = len(text)
	}
	start := offset - budget/4
	if start < 0 {
		start = 0
	}
	start = alignRuneStart(text, start)
	end := start + budget
	if end > len(text) {
		end = len(text)
	}
	end = alignRuneStart(text, end)
	if end < start {
		end = start
	}
	return start, end
}

// sortHits orders by score, then exact identifier match, then recency as a
// modest tie-breaker. Recency never outranks relevance.
func sortHits(hits []SearchHit) {
	sortSliceStable(hits, func(a, b SearchHit) bool {
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		if a.Exact != b.Exact {
			return a.Exact
		}
		if a.CreatedAt != b.CreatedAt {
			return a.CreatedAt > b.CreatedAt
		}
		return a.MessageID < b.MessageID
	})
}

// sortSliceStable is a small insertion sort: hit lists are bounded to a few
// hundred entries, so this avoids pulling in a comparator adapter.
func sortSliceStable(hits []SearchHit, less func(a, b SearchHit) bool) {
	for i := 1; i < len(hits); i++ {
		for j := i; j > 0 && less(hits[j], hits[j-1]); j-- {
			hits[j], hits[j-1] = hits[j-1], hits[j]
		}
	}
}

// buildMatchQuery turns terms into a quoted FTS5 OR expression. Every term is
// quoted with internal quotes doubled, so FTS operators in user input are
// treated as literal text instead of syntax.
func buildMatchQuery(terms []string) string {
	quoted := make([]string, 0, len(terms))
	for _, term := range terms {
		quoted = append(quoted, `"`+strings.ReplaceAll(term, `"`, `""`)+`"`)
	}
	return strings.Join(quoted, " OR ")
}

// searchTerms extracts bounded, deduplicated search terms from a query. Whole
// technical identifiers and paths are preserved so exact-match boosting can see
// them.
func searchTerms(query string) []string {
	var terms []string
	seen := make(map[string]struct{})
	add := func(term string) {
		term = strings.TrimSpace(term)
		if term == "" || len(terms) >= maxSearchTerms {
			return
		}
		key := strings.ToLower(term)
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		terms = append(terms, term)
	}
	for _, field := range strings.FieldsFunc(query, func(r rune) bool {
		return unicode.IsSpace(r) || strings.ContainsRune(",;()[]{}<>\"'`|&", r)
	}) {
		field = strings.Trim(field, ".,:!?。，：！？、")
		if field == "" {
			continue
		}
		add(field)
	}
	if len(terms) == 0 {
		// Fall back to the keyword extractor for CJK queries that contain no
		// whitespace at all.
		for _, keyword := range ExtractKeywords(query) {
			add(keyword)
		}
	}
	return terms
}

// containsExactIdentifier reports whether any term appears literally in text.
// This is what makes an exact command or path outrank a topical match.
func containsExactIdentifier(text string, terms []string) bool {
	lower := strings.ToLower(text)
	for _, term := range terms {
		if len(term) < 2 {
			continue
		}
		if strings.Contains(lower, strings.ToLower(term)) {
			return true
		}
	}
	return false
}
