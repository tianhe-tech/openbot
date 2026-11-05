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

// Chunking parameters. These are initial tuning values from the memory plan,
// not promises: the target keeps a chunk small enough to be a useful excerpt,
// the cap bounds the worst case for a single paragraph, and the overlap keeps a
// phrase that straddles a boundary findable from either side.
const (
	chunkTargetBytes      = 1024
	chunkMaxBytes         = 2048
	chunkOverlapBytes     = 128
	chunkTokenizerVersion = 1
	maxIndexBatch         = 200
)

// indexCommitTimeout returns the deadline for one index batch.
//
// The deadline exists to catch a wedged commit, not to bound normal work.
// Indexing is background maintenance with no caller waiting on a latency
// budget, and the caller's context already bounds how long it will wait. A
// deadline that is too tight is actively harmful, because a batch that times
// out fails permanently and leaves the archive under-indexed.
//
// The cost of a batch has two terms and neither can be dropped. It grows with
// the number of messages in the batch, and it grows with the number of chunks
// already in the index, because an FTS5 insert touches more B-tree pages as the
// index grows. A fixed deadline therefore cannot be right at every archive
// size: it either fires on a large archive or is absurdly loose on a small one.
// The previous version of this function used a fixed 30-second floor, which was
// measured at 4.61 s for the worst batch at 20k messages and is therefore
// marginal by 100k, where the same batch has five times as many chunks to
// insert into.
//
// An earlier version of this comment claimed that this was why seeding a 100k
// archive failed intermittently with "context deadline exceeded". That claim was
// wrong and is recorded here so it is not re-derived. The failure was in
// RecordMessage, not in IndexPending, and it was caused by contention: the
// seeding run shared the disk with another test process. Re-running the same
// seeding alone succeeded, and TestCaptureCommitScalesWithArchiveSize measures
// the worst capture commit at 329 ms in 40k messages, 16% of the capture
// deadline, so capture cost is flat in archive size. The scaling below is
// justified by the two cost terms, not by that incident.
//
// So the deadline is computed from both terms. The archive size is passed in
// rather than read here so the caller can fetch it once per batch with an O(1)
// MAX(id) lookup.
func indexCommitTimeout(limit int, archiveChunks int64) time.Duration {
	// Per-message cost of chunking and writing both indexes.
	perMessage := time.Duration(limit) * 100 * time.Millisecond * indexWritesPerChunk
	// Per-existing-chunk cost of inserting into an index that already holds
	// them. The coefficient is set well above the measured cost so this stays a
	// wedge detector rather than a performance assertion: at 20k messages the
	// worst batch measured 4.61 s against a computed 72 s, and at 100k the
	// computed budget is 200 s against an extrapolated 46 s.
	perExistingChunk := time.Duration(archiveChunks) * 200 * time.Microsecond * indexWritesPerChunk
	timeout := perMessage + perExistingChunk
	if timeout < 60*time.Second {
		timeout = 60 * time.Second
	}
	return timeout
}

// indexWritesPerChunk is the number of FTS5 rows written for each chunk. It is
// the trigram entry plus the word entry.
const indexWritesPerChunk = 2

// archiveChunkCount returns the largest chunk id, which is an upper bound on
// the number of indexed chunks and therefore on how expensive an insert into
// the existing index is. MAX(id) is O(1) because id is the primary key, so this
// is cheap enough to call once per batch.
func (writer *ArchiveWriter) archiveChunkCount(ctx context.Context) (int64, error) {
	var maxID int64
	if err := writer.store.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(id),0) FROM mem_v2_chunks`).Scan(&maxID); err != nil {
		return 0, fmt.Errorf("memstore: archive chunk count: %w", err)
	}
	return maxID, nil
}

// textChunk is one slice of a message body. Offsets are byte offsets into the
// original stored text, so a search hit can always be resolved back to the
// exact evidence it came from.
type textChunk struct {
	Ordinal   int
	StartByte int
	EndByte   int
}

// chunkText splits text into overlapping chunks at paragraph, line, sentence,
// and whitespace boundaries, in that order of preference. It never splits a
// UTF-8 rune and never returns a chunk longer than chunkMaxBytes.
func chunkText(text string) []textChunk {
	if text == "" {
		return nil
	}
	var chunks []textChunk
	start := 0
	for start < len(text) {
		end := chunkEnd(text, start)
		if end <= start {
			// Defensive: never emit an empty chunk or loop forever.
			end = alignRuneStart(text, start+chunkMaxBytes)
			if end <= start {
				end = len(text)
			}
		}
		chunks = append(chunks, textChunk{Ordinal: len(chunks), StartByte: start, EndByte: end})
		if end >= len(text) {
			break
		}
		next := end
		if candidate := alignRuneStart(text, end-chunkOverlapBytes); candidate > start {
			next = candidate
		}
		start = next
	}
	return chunks
}

// chunkEnd returns the exclusive end offset of the chunk that starts at start.
func chunkEnd(text string, start int) int {
	limit := start + chunkMaxBytes
	if limit >= len(text) {
		return len(text)
	}
	target := start + chunkTargetBytes
	if target > limit {
		target = limit
	}
	// Paragraph boundaries are the strongest signal: they separate prose from
	// code blocks and keep a fenced snippet intact.
	if idx := lastIndexInRange(text, "\n\n", start+1, target); idx >= 0 {
		return idx + 2
	}
	if idx := indexInRange(text, "\n\n", target, limit); idx >= 0 {
		return idx + 2
	}
	// Then line boundaries, which is what an exceptionally long line falls back
	// to before we resort to a hard cut.
	if idx := lastIndexByteInRange(text, '\n', start+1, target); idx >= 0 {
		return idx + 1
	}
	if idx := indexByteInRange(text, '\n', target, limit); idx >= 0 {
		return idx + 1
	}
	// Then sentence boundaries, so a CJK paragraph without newlines still splits
	// at a readable place.
	if idx := lastSentenceBoundary(text, start+1, target); idx >= 0 {
		return idx
	}
	if idx := lastSpaceInRange(text, start+1, target); idx >= 0 {
		return idx + 1
	}
	return alignRuneStart(text, limit)
}

func lastIndexInRange(text, sep string, lo, hi int) int {
	if hi > len(text) {
		hi = len(text)
	}
	if lo >= hi {
		return -1
	}
	idx := strings.LastIndex(text[:hi], sep)
	if idx < lo {
		return -1
	}
	return idx
}

func indexInRange(text, sep string, lo, hi int) int {
	if hi > len(text) {
		hi = len(text)
	}
	if lo >= hi {
		return -1
	}
	idx := strings.Index(text[lo:hi], sep)
	if idx < 0 {
		return -1
	}
	return lo + idx
}

func lastIndexByteInRange(text string, b byte, lo, hi int) int {
	if hi > len(text) {
		hi = len(text)
	}
	if lo >= hi {
		return -1
	}
	idx := strings.LastIndexByte(text[:hi], b)
	if idx < lo {
		return -1
	}
	return idx
}

func indexByteInRange(text string, b byte, lo, hi int) int {
	if hi > len(text) {
		hi = len(text)
	}
	if lo >= hi {
		return -1
	}
	idx := strings.IndexByte(text[lo:hi], b)
	if idx < 0 {
		return -1
	}
	return lo + idx
}

// lastSentenceBoundary returns the offset just past the last sentence-ending
// punctuation in [lo, hi). ASCII '.' is deliberately excluded so version
// numbers, file names, and dotted identifiers are not split.
func lastSentenceBoundary(text string, lo, hi int) int {
	if hi > len(text) {
		hi = len(text)
	}
	best := -1
	for i := lo; i < hi; {
		r, size := utf8.DecodeRuneInString(text[i:])
		i += size
		switch r {
		case '。', '！', '？', '；', '…', '!', '?', ';':
			if i <= hi {
				best = i
			}
		}
	}
	return best
}

func lastSpaceInRange(text string, lo, hi int) int {
	if hi > len(text) {
		hi = len(text)
	}
	for i := hi - 1; i >= lo; i-- {
		switch text[i] {
		case ' ', '\t', '\r':
			return i
		}
	}
	return -1
}

// alignRuneStart moves idx back to the start of a UTF-8 rune so a chunk never
// begins mid-character.
func alignRuneStart(text string, idx int) int {
	if idx <= 0 {
		return 0
	}
	if idx >= len(text) {
		return len(text)
	}
	for idx > 0 && !utf8.RuneStart(text[idx]) {
		idx--
	}
	return idx
}

// IndexCoverage is the part of the index state that can be read without
// touching the chunk table. Every field comes from a small indexed lookup, so
// this is cheap enough to run on the search path.
//
// Lag is always observable so a caller can tell "no match" apart from "not
// indexed yet".
type IndexCoverage struct {
	PendingJobs      int64 `json:"pending_jobs"`
	FailedJobs       int64 `json:"failed_jobs"`
	TokenizerVersion int   `json:"tokenizer_version"`
	UpdatedAt        int64 `json:"updated_at"`
	// WordBackfillRowID is the largest chunk id copied into the word index.
	// MaxChunkID is the largest chunk id that exists. The word index is
	// complete when the first is at least the second.
	WordBackfillRowID int64 `json:"word_backfill_rowid"`
	MaxChunkID        int64 `json:"max_chunk_id"`
	WordIndexComplete bool  `json:"word_index_complete"`
}

// IndexStatus reports how much of the archive is indexed. The counts beyond
// IndexCoverage are full scans of mem_v2_chunks, so they are reported only by
// diagnostics that actually need them; search uses IndexCoverage instead.
type IndexStatus struct {
	IndexCoverage
	IndexedMessages int64 `json:"indexed_messages"`
	TotalMessages   int64 `json:"total_messages"`
	Chunks          int64 `json:"chunks"`
}

// IndexPending consumes up to limit pending index jobs and returns how many
// messages were indexed. Jobs are leased inside the same transaction that
// writes the chunks, so a crash either leaves the job pending (retried) or
// leaves it completed with its chunks present.
//
// Indexing is background maintenance, not a user-facing commit, so it does not
// use the writer's commit timeout. That timeout exists to bound how long a
// caller waits for an acknowledgment, and it is far too short for a batch whose
// cost grows with the size of the FTS index: a full batch of 4 KiB messages
// already exceeds it at a few thousand messages. The deadline here scales with
// the batch instead, and the caller's context still bounds the wait.
func (writer *ArchiveWriter) IndexPending(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > maxIndexBatch {
		return 0, fmt.Errorf("memstore: index batch size must be 1-%d", maxIndexBatch)
	}
	indexed := 0
	chunks, err := writer.archiveChunkCount(ctx)
	if err != nil {
		return 0, err
	}
	timeout := indexCommitTimeout(limit, chunks)
	err = writer.submitWithTimeout(ctx, limit*chunkMaxBytes, timeout, func(ctx context.Context, tx *sql.Tx) error {
		indexed = 0
		now := time.Now().UnixMilli()
		rows, err := tx.QueryContext(ctx, `SELECT id,source_id,source_revision FROM mem_v2_jobs
			WHERE kind='index' AND status='pending' AND next_run_at<=? AND lease_until<=?
			ORDER BY next_run_at,id LIMIT ?`, now, now, limit)
		if err != nil {
			return err
		}
		type indexJob struct {
			id       string
			sourceID string
			revision int64
		}
		var jobs []indexJob
		for rows.Next() {
			var job indexJob
			if err := rows.Scan(&job.id, &job.sourceID, &job.revision); err != nil {
				rows.Close()
				return err
			}
			jobs = append(jobs, job)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, job := range jobs {
			if _, err := tx.ExecContext(ctx, `UPDATE mem_v2_jobs SET lease_until=? WHERE id=?`, now+60000, job.id); err != nil {
				return err
			}
			if err := indexMessage(ctx, tx, job.sourceID, job.revision); err != nil {
				// A single unindexable message must not stall the queue: record
				// the failure on the job and keep going.
				if _, updateErr := tx.ExecContext(ctx, `UPDATE mem_v2_jobs
					SET status='failed',lease_until=0,attempts=attempts+1,last_error=? WHERE id=?`, err.Error(), job.id); updateErr != nil {
					return updateErr
				}
				continue
			}
			if _, err := tx.ExecContext(ctx, `UPDATE mem_v2_jobs
				SET status='completed',lease_until=0,attempts=attempts+1,last_error='' WHERE id=?`, job.id); err != nil {
				return err
			}
			indexed++
		}
		if len(jobs) > 0 {
			if _, err := tx.ExecContext(ctx, `UPDATE mem_v2_index_state SET updated_at=? WHERE id=1`, now); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return indexed, nil
}

// indexMessage writes the chunk rows and their FTS entries for one message
// revision. Re-indexing the same revision is idempotent: the previous rows are
// removed first.
func indexMessage(ctx context.Context, tx *sql.Tx, messageID string, revision int64) error {
	var payload []byte
	var codec string
	var codecVersion int
	var rawBytes int64
	err := tx.QueryRowContext(ctx, `SELECT b.codec,b.codec_version,b.raw_bytes,b.payload FROM mem_v2_messages m
		JOIN mem_v2_blocks b ON b.id=m.block_id WHERE m.id=?`, messageID).Scan(&codec, &codecVersion, &rawBytes, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("memstore: message %s is missing", messageID)
	}
	if err != nil {
		return err
	}
	decoded, err := decodeBlock(codec, codecVersion, rawBytes, payload)
	if err != nil {
		return fmt.Errorf("memstore: index message %s: %w", messageID, err)
	}
	payload = decoded
	if _, err := tx.ExecContext(ctx, `DELETE FROM mem_v2_chunk_fts WHERE rowid IN
		(SELECT id FROM mem_v2_chunks WHERE message_id=? AND message_revision=?)`, messageID, revision); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM mem_v2_chunk_word_fts WHERE rowid IN
		(SELECT id FROM mem_v2_chunks WHERE message_id=? AND message_revision=?)`, messageID, revision); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM mem_v2_chunks WHERE message_id=? AND message_revision=?`, messageID, revision); err != nil {
		return err
	}
	text := string(payload)
	for _, chunk := range chunkText(text) {
		result, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_chunks
			(message_id,message_revision,ordinal,start_byte,end_byte,tokenizer_version) VALUES(?,?,?,?,?,?)`,
			messageID, revision, chunk.Ordinal, chunk.StartByte, chunk.EndByte, chunkTokenizerVersion)
		if err != nil {
			return err
		}
		chunkID, err := result.LastInsertId()
		if err != nil {
			return err
		}
		content := text[chunk.StartByte:chunk.EndByte]
		if _, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_chunk_fts(rowid,content) VALUES(?,?)`,
			chunkID, content); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_chunk_word_fts(rowid,content) VALUES(?,?)`,
			chunkID, content); err != nil {
			return err
		}
	}
	return nil
}

// IndexBatchLimit is the largest number of messages one index pass processes.
// It is exported so a caller can size its worker loop without duplicating the
// internal constant.
const IndexBatchLimit = maxIndexBatch

// WordBackfillBatchLimit is the largest number of chunks one backfill pass
// copies into the word index. It is larger than IndexBatchLimit because a
// backfill pass does no chunking and no job bookkeeping: it copies text that is
// already chunked, so the per-row cost is one FTS5 insert.
const WordBackfillBatchLimit = 2000

// BackfillWordIndex copies up to limit chunks into the word-tokenized index and
// returns how many it copied. It is resumable: the high-water mark is persisted
// in mem_v2_index_state, so a restart continues where the last pass stopped
// instead of rescanning the archive.
//
// The backfill exists because the word index is added by a migration to an
// archive that already has chunks. Those chunks have trigram entries but no
// word entries, and a search routed to the word index would silently miss them.
// Copying inside the migration was rejected: on a large archive that turns an
// upgrade into a full reindex, and a migration that can time out halfway leaves
// the archive in a state nobody can reason about. A resumable background pass
// reports its progress instead, and search reports partial coverage until it
// finishes.
func (writer *ArchiveWriter) BackfillWordIndex(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > WordBackfillBatchLimit {
		return 0, fmt.Errorf("memstore: word backfill batch size must be 1-%d", WordBackfillBatchLimit)
	}
	copied := 0
	chunks, err := writer.archiveChunkCount(ctx)
	if err != nil {
		return 0, err
	}
	timeout := indexCommitTimeout(limit, chunks)
	err = writer.submitWithTimeout(ctx, limit*chunkMaxBytes, timeout, func(ctx context.Context, tx *sql.Tx) error {
		copied = 0
		var highWater int64
		if err := tx.QueryRowContext(ctx,
			`SELECT word_backfill_rowid FROM mem_v2_index_state WHERE id=1`).Scan(&highWater); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT c.id, ft.content
			FROM mem_v2_chunks c
			JOIN mem_v2_chunk_fts ft ON ft.rowid=c.id
			WHERE c.id>?
			ORDER BY c.id
			LIMIT ?`, highWater, limit)
		if err != nil {
			return err
		}
		type chunkRow struct {
			id      int64
			content string
		}
		var batch []chunkRow
		for rows.Next() {
			var row chunkRow
			if err := rows.Scan(&row.id, &row.content); err != nil {
				rows.Close()
				return err
			}
			batch = append(batch, row)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, row := range batch {
			// INSERT OR REPLACE, not INSERT: a chunk created after the
			// migration already has a word entry from indexMessage, and the
			// backfill will still reach it because the high-water mark starts
			// at zero. Replacing makes the overlap harmless instead of a
			// constraint violation that would stall the backfill forever.
			if _, err := tx.ExecContext(ctx,
				`INSERT OR REPLACE INTO mem_v2_chunk_word_fts(rowid,content) VALUES(?,?)`, row.id, row.content); err != nil {
				return err
			}
			highWater = row.id
			copied++
		}
		if copied > 0 {
			if _, err := tx.ExecContext(ctx,
				`UPDATE mem_v2_index_state SET word_backfill_rowid=? WHERE id=1`, highWater); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return copied, nil
}

// IndexWorkerOptions tunes the background indexer. The zero value is usable.
type IndexWorkerOptions struct {
	// Batch is the number of messages indexed per pass. Values outside
	// 1..IndexBatchLimit fall back to IndexBatchLimit.
	Batch int
	// Idle is how long the worker waits after a pass that drained the queue.
	// Zero uses one second.
	Idle time.Duration
	// OnError receives each failed pass. The worker keeps running afterwards:
	// a transient database error must not silently stop indexing forever.
	OnError func(error)
}

// RunIndexWorker indexes pending messages until ctx is cancelled. It is the
// production driver for the lexical index: without it mem_v2_chunks stays
// empty and every search reports partial coverage.
//
// The worker also drives the word-index backfill, and it does so only when the
// message queue is empty. That ordering is deliberate: a newly captured message
// is what a user is most likely to search for, so it must reach both indexes
// before the worker spends time copying old chunks that already have trigram
// entries. The backfill is resumable, so being starved by steady capture is
// correct behaviour rather than a stall.
func (writer *ArchiveWriter) RunIndexWorker(ctx context.Context, options IndexWorkerOptions) {
	batch := options.Batch
	if batch < 1 || batch > maxIndexBatch {
		batch = maxIndexBatch
	}
	idle := options.Idle
	if idle <= 0 {
		idle = time.Second
	}
	for {
		if ctx.Err() != nil {
			return
		}
		count, err := writer.IndexPending(ctx, batch)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if options.OnError != nil {
				options.OnError(err)
			}
			if !sleepContext(ctx, idle) {
				return
			}
			continue
		}
		if count >= batch {
			// The queue is still full; keep draining without waiting.
			continue
		}
		// The message queue is drained, so spend the idle time on the word
		// index backfill. A full backfill batch means there is more to do, so
		// loop again instead of sleeping.
		backfilled, backfillErr := writer.BackfillWordIndex(ctx, WordBackfillBatchLimit)
		if backfillErr != nil {
			if ctx.Err() != nil {
				return
			}
			if options.OnError != nil {
				options.OnError(backfillErr)
			}
		} else if backfilled >= WordBackfillBatchLimit {
			continue
		}
		if !sleepContext(ctx, idle) {
			return
		}
	}
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// IndexCoverage reports the cheap part of the index state: how many jobs are
// waiting, how many failed, and which tokenizer generation the index is at.
//
// This exists because Search needs exactly one bit of index state - whether
// anything is still pending - and the full IndexStatus answers it with two full
// scans of mem_v2_chunks. Those scans cost tens of milliseconds on a large
// archive and grow linearly with it, which made every search pay for
// diagnostics it never read. It is safe to call on a read-only handle.
func (s *Store) IndexCoverage(ctx context.Context) (IndexCoverage, error) {
	var coverage IndexCoverage
	err := s.db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM mem_v2_jobs WHERE kind='index' AND status='pending'),
		(SELECT COUNT(*) FROM mem_v2_jobs WHERE kind='index' AND status='failed'),
		(SELECT tokenizer_version FROM mem_v2_index_state WHERE id=1),
		(SELECT updated_at FROM mem_v2_index_state WHERE id=1),
		(SELECT word_backfill_rowid FROM mem_v2_index_state WHERE id=1),
		(SELECT COALESCE(MAX(id),0) FROM mem_v2_chunks)`).Scan(
		&coverage.PendingJobs, &coverage.FailedJobs,
		&coverage.TokenizerVersion, &coverage.UpdatedAt,
		&coverage.WordBackfillRowID, &coverage.MaxChunkID)
	if err != nil {
		return coverage, fmt.Errorf("memstore: index coverage: %w", err)
	}
	coverage.WordIndexComplete = coverage.WordBackfillRowID >= coverage.MaxChunkID
	return coverage, nil
}

// IndexStatus reports index coverage for the archive, including the message and
// chunk counts. The counts are full scans of mem_v2_chunks, so this is a
// diagnostic call, not a per-request one; use IndexCoverage on hot paths. It is
// safe to call on a read-only handle.
func (s *Store) IndexStatus(ctx context.Context) (IndexStatus, error) {
	var status IndexStatus
	err := s.db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM mem_v2_jobs WHERE kind='index' AND status='pending'),
		(SELECT COUNT(*) FROM mem_v2_jobs WHERE kind='index' AND status='failed'),
		(SELECT COUNT(DISTINCT message_id) FROM mem_v2_chunks),
		(SELECT COUNT(*) FROM mem_v2_messages),
		(SELECT COUNT(*) FROM mem_v2_chunks),
		(SELECT tokenizer_version FROM mem_v2_index_state WHERE id=1),
		(SELECT updated_at FROM mem_v2_index_state WHERE id=1)`).Scan(
		&status.PendingJobs, &status.FailedJobs, &status.IndexedMessages,
		&status.TotalMessages, &status.Chunks, &status.TokenizerVersion, &status.UpdatedAt)
	if err != nil {
		return status, fmt.Errorf("memstore: index status: %w", err)
	}
	return status, nil
}
