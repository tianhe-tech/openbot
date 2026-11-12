package memstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const archiveSchemaVersion = 8

// archiveMigrationSteps maps a target schema version to the statements that
// upgrade the archive from version-1 to that version. Steps are applied in
// ascending order inside one transaction, so a partially applied upgrade never
// becomes visible.
var archiveMigrationSteps = map[int][]string{
	1: archiveMigrationV1,
	2: archiveMigrationV2,
	3: archiveMigrationV3,
	4: archiveMigrationV4,
	5: archiveMigrationV5,
	6: archiveMigrationV6,
	7: archiveMigrationV7,
	8: archiveMigrationV8,
}

// UpgradeArchive applies any pending archive migrations to an existing
// database. It is the supported path for an operator whose archive was created
// by an older binary: the gateway refuses to open an archive whose schema does
// not match exactly, so the upgrade must be explicit.
func UpgradeArchive(ctx context.Context, path string) error {
	store, err := openDatabaseMode(path, false, false)
	if err != nil {
		return err
	}
	defer store.Close()
	return store.MigrateArchive(ctx)
}

func (s *Store) MigrateArchive(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS mem_v2_schema (
		version INTEGER PRIMARY KEY, applied_at INTEGER NOT NULL)`); err != nil {
		return err
	}
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM mem_v2_schema`).Scan(&version); err != nil {
		return err
	}
	if version > archiveSchemaVersion {
		return fmt.Errorf("memstore: archive schema %d is newer than supported version %d", version, archiveSchemaVersion)
	}
	for version < archiveSchemaVersion {
		next := version + 1
		statements, ok := archiveMigrationSteps[next]
		if !ok {
			return fmt.Errorf("memstore: archive schema %d has no upgrade path to %d", version, next)
		}
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("memstore: archive migration to %d: %w", next, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO mem_v2_schema(version, applied_at) VALUES (?,?)`, next, time.Now().UTC().UnixMilli()); err != nil {
			return err
		}
		version = next
	}
	return tx.Commit()
}

var archiveMigrationV1 = []string{
	`CREATE TABLE mem_v2_sessions (
			id TEXT PRIMARY KEY, principal_id TEXT NOT NULL,
			adapter TEXT NOT NULL, user_id TEXT NOT NULL, thread_id TEXT NOT NULL,
			upstream_session_id TEXT NOT NULL DEFAULT '', project_key TEXT NOT NULL,
			work_dir TEXT NOT NULL, created_at INTEGER NOT NULL,
			UNIQUE(id, principal_id))`,
	`CREATE UNIQUE INDEX mem_v2_session_upstream ON mem_v2_sessions(principal_id, upstream_session_id)
			WHERE upstream_session_id != ''`,
	`CREATE INDEX mem_v2_session_scope ON mem_v2_sessions(principal_id, project_key, created_at)`,
	`CREATE TABLE mem_v2_turns (
			id TEXT PRIMARY KEY, principal_id TEXT NOT NULL, session_id TEXT NOT NULL,
			delivery_key TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
			dispatch_state TEXT NOT NULL DEFAULT 'unknown', started_at INTEGER NOT NULL,
			completed_at INTEGER NOT NULL DEFAULT 0, last_reconciled_at INTEGER NOT NULL DEFAULT 0,
			UNIQUE(principal_id, delivery_key), UNIQUE(id, session_id),
			FOREIGN KEY(session_id, principal_id) REFERENCES mem_v2_sessions(id, principal_id))`,
	`CREATE INDEX mem_v2_turn_pending ON mem_v2_turns(status, started_at)`,
	`CREATE TABLE mem_v2_attempts (
			id TEXT PRIMARY KEY, turn_id TEXT NOT NULL REFERENCES mem_v2_turns(id),
			upstream_session_id TEXT NOT NULL DEFAULT '', attempt_no INTEGER NOT NULL CHECK(attempt_no >= 0),
			status TEXT NOT NULL DEFAULT 'active', last_error_code TEXT NOT NULL DEFAULT '',
			UNIQUE(turn_id, attempt_no), UNIQUE(id, turn_id))`,
	`CREATE TABLE mem_v2_blocks (
			id TEXT PRIMARY KEY, principal_id TEXT NOT NULL, codec TEXT NOT NULL DEFAULT 'raw',
			codec_version INTEGER NOT NULL DEFAULT 1, raw_bytes INTEGER NOT NULL CHECK(raw_bytes >= 0),
			stored_bytes INTEGER NOT NULL CHECK(stored_bytes >= 0), checksum TEXT NOT NULL,
			payload BLOB NOT NULL, UNIQUE(id, principal_id))`,
	`CREATE TABLE mem_v2_messages (
			id TEXT PRIMARY KEY, principal_id TEXT NOT NULL, session_id TEXT NOT NULL,
			turn_id TEXT NOT NULL, attempt_id TEXT,
			upstream_message_id TEXT NOT NULL DEFAULT '', seq INTEGER NOT NULL CHECK(seq >= 0),
			role TEXT NOT NULL CHECK(role IN ('user','assistant','tool')),
			state TEXT NOT NULL CHECK(state IN ('completed','failed','interrupted')),
			revision INTEGER NOT NULL DEFAULT 1 CHECK(revision > 0), block_id TEXT NOT NULL,
			byte_offset INTEGER NOT NULL DEFAULT 0 CHECK(byte_offset >= 0),
			byte_length INTEGER NOT NULL CHECK(byte_length >= 0), created_at INTEGER NOT NULL,
			fingerprint TEXT NOT NULL,
			fidelity TEXT NOT NULL CHECK(fidelity IN ('captured','legacy_unknown')),
			UNIQUE(session_id, seq),
			FOREIGN KEY(session_id, principal_id) REFERENCES mem_v2_sessions(id, principal_id),
			FOREIGN KEY(turn_id, session_id) REFERENCES mem_v2_turns(id, session_id),
			FOREIGN KEY(attempt_id, turn_id) REFERENCES mem_v2_attempts(id, turn_id),
			FOREIGN KEY(block_id, principal_id) REFERENCES mem_v2_blocks(id, principal_id))`,
	`CREATE UNIQUE INDEX mem_v2_message_upstream ON mem_v2_messages(attempt_id, upstream_message_id)
			WHERE attempt_id IS NOT NULL AND upstream_message_id != ''`,
	`CREATE INDEX mem_v2_message_turn ON mem_v2_messages(turn_id, seq)`,
	`CREATE TABLE mem_v2_chunks (
			id INTEGER PRIMARY KEY, message_id TEXT NOT NULL REFERENCES mem_v2_messages(id),
			message_revision INTEGER NOT NULL, ordinal INTEGER NOT NULL,
			start_byte INTEGER NOT NULL, end_byte INTEGER NOT NULL, tokenizer_version INTEGER NOT NULL,
			UNIQUE(message_id, message_revision, ordinal))`,
	`CREATE TABLE mem_v2_jobs (
			id TEXT PRIMARY KEY, kind TEXT NOT NULL, source_id TEXT NOT NULL,
			source_revision INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
			attempts INTEGER NOT NULL DEFAULT 0, lease_until INTEGER NOT NULL DEFAULT 0,
			next_run_at INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
			UNIQUE(kind, source_id, source_revision))`,
	`CREATE INDEX mem_v2_jobs_due ON mem_v2_jobs(status, next_run_at, lease_until)`,
	`CREATE TABLE mem_v2_legacy_map (
			legacy_id TEXT PRIMARY KEY, turn_id TEXT NOT NULL REFERENCES mem_v2_turns(id),
			import_version INTEGER NOT NULL)`,
}

// archiveMigrationV2 adds rolling partial snapshots and tool/attachment
// metadata. Partial snapshots live in their own table so immutable finalized
// messages are never rewritten; a snapshot row is replaced only by a newer
// revision of the same attempt + upstream message.
var archiveMigrationV2 = []string{
	`CREATE TABLE mem_v2_partials (
		id TEXT PRIMARY KEY, principal_id TEXT NOT NULL, session_id TEXT NOT NULL,
		turn_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
		upstream_message_id TEXT NOT NULL DEFAULT '', revision INTEGER NOT NULL CHECK(revision > 0),
		block_id TEXT NOT NULL, byte_length INTEGER NOT NULL CHECK(byte_length >= 0),
		created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
		UNIQUE(attempt_id, upstream_message_id),
		FOREIGN KEY(session_id, principal_id) REFERENCES mem_v2_sessions(id, principal_id),
		FOREIGN KEY(turn_id, session_id) REFERENCES mem_v2_turns(id, session_id),
		FOREIGN KEY(attempt_id, turn_id) REFERENCES mem_v2_attempts(id, turn_id),
		FOREIGN KEY(block_id, principal_id) REFERENCES mem_v2_blocks(id, principal_id))`,
	`CREATE INDEX mem_v2_partial_turn ON mem_v2_partials(turn_id, updated_at)`,
	`CREATE TABLE mem_v2_tool_calls (
		id TEXT PRIMARY KEY, principal_id TEXT NOT NULL, session_id TEXT NOT NULL,
		turn_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
		upstream_message_id TEXT NOT NULL DEFAULT '', call_id TEXT NOT NULL,
		part_id TEXT NOT NULL DEFAULT '', tool_name TEXT NOT NULL,
		state TEXT NOT NULL CHECK(state IN ('pending','running','completed','error')),
		title TEXT NOT NULL DEFAULT '', error_text TEXT NOT NULL DEFAULT '',
		artifact_ref TEXT NOT NULL DEFAULT '', retention_reason TEXT NOT NULL DEFAULT '',
		output_bytes INTEGER NOT NULL DEFAULT 0, output_truncated INTEGER NOT NULL DEFAULT 0,
		started_at INTEGER NOT NULL DEFAULT 0, ended_at INTEGER NOT NULL DEFAULT 0,
		created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
		UNIQUE(attempt_id, call_id),
		FOREIGN KEY(session_id, principal_id) REFERENCES mem_v2_sessions(id, principal_id),
		FOREIGN KEY(turn_id, session_id) REFERENCES mem_v2_turns(id, session_id),
		FOREIGN KEY(attempt_id, turn_id) REFERENCES mem_v2_attempts(id, turn_id))`,
	`CREATE INDEX mem_v2_tool_call_turn ON mem_v2_tool_calls(turn_id, created_at)`,
	`CREATE TABLE mem_v2_attachments (
		id TEXT PRIMARY KEY, principal_id TEXT NOT NULL, session_id TEXT NOT NULL,
		turn_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
		upstream_message_id TEXT NOT NULL DEFAULT '', part_id TEXT NOT NULL DEFAULT '',
		role TEXT NOT NULL CHECK(role IN ('input','output')),
		mime TEXT NOT NULL DEFAULT '', filename TEXT NOT NULL DEFAULT '',
		url TEXT NOT NULL DEFAULT '', byte_length INTEGER NOT NULL DEFAULT 0,
		created_at INTEGER NOT NULL,
		UNIQUE(attempt_id, role, part_id, url),
		FOREIGN KEY(session_id, principal_id) REFERENCES mem_v2_sessions(id, principal_id),
		FOREIGN KEY(turn_id, session_id) REFERENCES mem_v2_turns(id, session_id),
		FOREIGN KEY(attempt_id, turn_id) REFERENCES mem_v2_attempts(id, turn_id))`,
	`CREATE INDEX mem_v2_attachment_turn ON mem_v2_attachments(turn_id, created_at)`,
}

// archiveMigrationV3 adds the lexical index used by scoped search. Chunk rows
// live in mem_v2_chunks (created by v1) and their text is mirrored into a
// standalone FTS5 table keyed by the chunk rowid, so a chunk and its index
// entry are always addressed by the same integer.
//
// The layout originally duplicated chunk text, on the reasoning that contentless
// FTS was a space optimization to be taken later and correctness came first.
// That deferral was paid off in v7, which converts this table and the word index
// to contentless. The CREATE below is left exactly as it shipped, because a
// migration step is a historical record: an archive that stopped at v6 must look
// the way v3 through v6 built it, and only v7 is allowed to change it.
var archiveMigrationV3 = []string{
	`CREATE VIRTUAL TABLE mem_v2_chunk_fts USING fts5(content, tokenize='trigram')`,
	`CREATE TABLE mem_v2_index_state (
		id INTEGER PRIMARY KEY CHECK(id = 1),
		tokenizer_version INTEGER NOT NULL,
		updated_at INTEGER NOT NULL)`,
	`INSERT INTO mem_v2_index_state(id, tokenizer_version, updated_at) VALUES(1, 1, 0)`,
}

// archiveMigrationV4 adds a word-tokenized index alongside the trigram one.
//
// The trigram index is correct but slow: it indexes every three-character
// sequence, so a six-character term is a phrase of four overlapping trigrams
// and matching it means intersecting four posting lists whose length depends on
// how common those trigrams are. Measured on the same chunk text, the same
// three-term query costs 117.4 ms against the trigram index and 7.38 ms against
// a unicode61 index, a factor of 15.9, while matching 1157 and 1142 chunks
// respectively. The cost is the tokenizer, not the query and not the joins.
//
// The word index is added rather than substituted because the two tokenizers
// answer different questions. unicode61 splits on word boundaries, so it finds
// whole tokens and cannot find a substring inside one; trigram finds any
// substring, which is what makes CJK search work at all, since a CJK phrase has
// no spaces to split on. Keeping both means a query can be routed to the index
// that can answer it, and the routing rule is the term's shape.
//
// The word index is a separate FTS5 table keyed by the same chunk rowid, so a
// chunk and both of its index entries are addressed by one integer and the
// search query can join either one without a mapping table.
var archiveMigrationV4 = []string{
	`CREATE VIRTUAL TABLE mem_v2_chunk_word_fts USING fts5(content, tokenize='unicode61')`,
	// The word index starts empty and is filled by a background backfill, so
	// the archive reports partial coverage until it finishes. Backfilling
	// inside the migration would make upgrading a large archive take as long as
	// a full reindex, and a migration that can time out halfway is worse than
	// one that finishes immediately and reports what is left to do.
	//
	// The high-water mark is the largest chunk rowid already copied. The
	// backfill walks chunk ids in order from there, so it is resumable across
	// restarts and never rescans what it has already done.
	//
	// It starts at zero because every chunk that exists at migration time is
	// missing a word entry and has to be copied. The backfill and the normal
	// write path therefore overlap: a chunk created after the migration gets a
	// word entry from indexMessage, and the backfill will reach it later and
	// copy it again. That is why the backfill writes with INSERT OR REPLACE
	// rather than INSERT — the overlap is expected, and making the copy
	// idempotent is cheaper and more obviously correct than trying to prove the
	// two paths never touch the same row.
	`ALTER TABLE mem_v2_index_state ADD COLUMN word_backfill_rowid INTEGER NOT NULL DEFAULT 0`,
}

// archiveMigrationV5 adds the index the bounded scan needs.
//
// searchScan is the fallback for one- and two-character terms, which neither
// FTS5 tokenizer can match. It reads at most maxFallbackScan messages newest
// first, and it was written as if that cap bounded the work. It did not. The
// query orders by created_at and seq, and mem_v2_messages had no index on
// either, so SQLite scanned the whole table, materialized every message in the
// principal including its payload, and sorted all of it before the LIMIT could
// apply. EXPLAIN QUERY PLAN said so plainly: "SCAN m" plus "USE TEMP B-TREE FOR
// ORDER BY".
//
// The cost of that is not a constant on a rarely used path. Measured at 100k
// messages the scan took 13.8 s p95, against 463 ms for the word path and 8.4 ms
// for the same scan at 2k messages. A fallback that takes thirteen seconds is
// not a fallback; it is a hang with a notice attached.
//
// The index is on (principal_id, created_at DESC, seq DESC) because that is the
// scan's WHERE and ORDER BY in order: principal_id is an equality predicate, so
// it goes first, and the two sort columns follow in the direction the query
// asks for so the rows come out already ordered and the LIMIT stops the walk.
// SQLite can use an index for a descending ORDER BY when the index declares the
// same direction, which is why the directions are written out rather than left
// to the default ascending.
//
// This is the only index on principal_id. The other indexes on mem_v2_messages
// are UNIQUE(session_id, seq), mem_v2_message_turn(turn_id, seq), and the
// partial unique mem_v2_message_upstream, none of which can serve a query that
// filters by principal and orders by time.
//
// The index has a cost that the scan measurement could not see, and it is worth
// recording here because it is the kind of thing an index does quietly. Adding
// it changed the plan of the search query, which joins the FTS table to
// mem_v2_messages. SQLite reorders inner joins by estimated cost, and the new
// index gave it a cheap way to reach a principal's messages, so it started
// driving the search from mem_v2_messages and probing the FTS index once per
// message instead of once per match. The inner query went from 75.20 ms to
// 104471 ms at 20k messages, a factor of 1388. The fix is in searchIndex, not
// here: CROSS JOIN on the FTS table forbids the reordering, and
// TestSearchIndexQueryPlanPinsFTSDriver fails if the FTS table stops driving.
// The index stays because the scan needs it and the search no longer depends on
// the planner's choice.
var archiveMigrationV5 = []string{
	`CREATE INDEX mem_v2_message_scan ON mem_v2_messages(principal_id, created_at DESC, seq DESC)`,
}

// archiveMigrationV6 adds the instance lease.
//
// The plan requires that "a second gateway targeting the same store must be
// rejected by an instance lease or supported by tested coordination; do not
// rely on single-process assumptions silently". Until this table existed the
// gateway relied on that assumption silently: two processes could open the same
// archive, each with its own writer queue, and the only thing keeping them
// apart was that nobody had tried it. SQLite serializes the writes, so the
// damage would not be corruption; it would be two independent capture
// pipelines racing to record the same turn, each seeing the other's rows as
// conflicts, and a WAL checkpointed by whichever process happened to run one.
//
// The lease is a single row. A row rather than a lock file because the archive
// is already the shared resource: a lock file next to it can be deleted, copied
// with the database, or left behind by a crash, and then the operator has to
// reason about two sources of truth. A row expires, so a crashed holder does
// not require manual cleanup.
var archiveMigrationV6 = []string{
	`CREATE TABLE mem_v2_instance_lease (
		id INTEGER PRIMARY KEY CHECK(id = 1),
		holder TEXT NOT NULL,
		acquired_at INTEGER NOT NULL,
		heartbeat_at INTEGER NOT NULL,
		expires_at INTEGER NOT NULL)`,
}

// archiveMigrationV7 makes both full-text indexes contentless.
//
// Until this migration the archive stored every message three times: once in
// mem_v2_blocks, and once in each of the two FTS5 indexes. The v3 comment
// called that duplication a deliberate deferral — "contentless FTS is a space
// optimization deferred to the compression phase, and correctness comes first"
// — and this is that phase.
//
// The saving was measured before it was adopted, because the plan asserted for
// a while that contentless "costs nothing on the read path" and that was never
// checked. `fts_contentless_ratio_test.go` puts the stored text at 2.24-2.26
// copies of the chunk text on a conversation-shaped archive, so removing it
// takes 35.9-37.4% of the archive file. `BenchmarkSearchExcerptReconstruction`
// puts the read-path cost at 1.13x at 2k messages (8.96 -> 10.16 ms p50) and
// indistinguishable at 20k (76.9 vs 76.2-78.2 ms), because the reconstruction
// is bounded by maxSearchCandidates while the ranking work grows with the
// archive. A fixed ~1.2 ms against a 100 ms target is worth 36% of the file.
//
// The rows are copied rather than rebuilt, and that is the decision this
// migration turns on. Rebuilding would mean dropping both indexes and letting
// the background backfill refill them, which works for the word index — it
// already has a resumable backfill and a high-water mark — but not for the
// trigram index, which has no backfill because until now it was always written
// synchronously by indexMessage. Dropping it would leave search returning
// nothing until some new mechanism refilled it, and a migration that silently
// disables search is worse than one that takes a while. Copying is possible
// because a contentless table still accepts text at INSERT time; it just does
// not keep it. So the text is read out of the old table and written into the
// new one, and the index is complete the moment the transaction commits.
//
// The cost is that the transaction holds both copies at once, so upgrading a
// large archive needs roughly one extra index's worth of free space and WAL.
// That is a one-time upgrade cost, and it is the price of not having a window
// where search is broken.
//
// The word index is copied as-is rather than reset. It may be partially
// backfilled, and word_backfill_rowid records how far that got; copying the
// rows that exist and leaving the mark alone means the backfill resumes exactly
// where it stopped instead of redoing work it had already finished.
//
// What this breaks, and where: `snippet()` and `highlight()` return empty on a
// contentless table, so the excerpt is reconstructed from the chunk's byte
// range into the decoded message block. `searchIndex` and `BackfillWordIndex`
// both used to read `ft.content` and now decode instead. `SpaceReport`'s
// FTSContentBytes drops to zero, which is the measurement of this migration
// rather than a regression.
var archiveMigrationV7 = []string{
	`CREATE VIRTUAL TABLE mem_v2_chunk_fts_v7 USING fts5(content, content='', contentless_delete=1, tokenize='trigram')`,
	`INSERT INTO mem_v2_chunk_fts_v7(rowid, content) SELECT rowid, content FROM mem_v2_chunk_fts`,
	`DROP TABLE mem_v2_chunk_fts`,
	`ALTER TABLE mem_v2_chunk_fts_v7 RENAME TO mem_v2_chunk_fts`,
	`CREATE VIRTUAL TABLE mem_v2_chunk_word_fts_v7 USING fts5(content, content='', contentless_delete=1, tokenize='unicode61')`,
	`INSERT INTO mem_v2_chunk_word_fts_v7(rowid, content) SELECT rowid, content FROM mem_v2_chunk_word_fts`,
	`DROP TABLE mem_v2_chunk_word_fts`,
	`ALTER TABLE mem_v2_chunk_word_fts_v7 RENAME TO mem_v2_chunk_word_fts`,
}

// archiveMigrationV8 adds source-backed durable knowledge.
//
// The plan's Phase 5 asks for facts that are derived from captured messages and
// that can be corrected, forgotten, and audited. Three tables, because the
// three questions are different and answering them from one table would mean
// either duplicating evidence or losing it:
//
//   - mem_v2_knowledge_facts holds the claim: what kind of statement it is,
//     the statement itself, its lifecycle status, and which fact superseded it.
//   - mem_v2_knowledge_evidence holds the provenance: one row per supporting
//     message. It is a separate table rather than a JSON column on the fact
//     because deletion has to find facts by message, and a JSON array cannot be
//     indexed or joined. It also carries principal_id so the "evidence belongs
//     to the source principal" check is a single-table predicate instead of a
//     join that a future caller could forget.
//   - mem_v2_knowledge_jobs holds the extraction queue. It is a separate table
//     from mem_v2_jobs because that table's uniqueness is (kind, source_id,
//     source_revision) and its source is a message; a knowledge job's source is
//     a session or a turn, and reusing the table would mean inventing a kind
//     whose source_id namespace collides with the index jobs'.
//
// Supersession is a column, not a delete. The plan requires that "user
// corrections supersede old facts" and that corrections are stored "as
// supersession rather than overwriting historical evidence", so a corrected
// fact keeps its row, its evidence, and its original statement, and gains a
// superseded_by pointer. A reader that wants current knowledge filters on
// superseded_by IS NULL; a reader that wants history does not.
//
// The evidence foreign key is deliberately not ON DELETE CASCADE. Scoped
// deletion removes knowledge rows inside the same transaction that removes the
// messages (see deleteScopeTx), and a cascade would make that step invisible:
// the deletion report would count messages and silently drop facts, so an
// operator could not tell whether the knowledge rows were removed by the
// deletion or by a cascade they did not know about. An explicit delete also
// fails loudly if a future code path forgets the ordering, because the writer's
// connection enables foreign_keys and a violation aborts the transaction
// instead of quietly succeeding.
var archiveMigrationV8 = []string{
	`CREATE TABLE mem_v2_knowledge_facts (
			id TEXT PRIMARY KEY, principal_id TEXT NOT NULL,
			kind TEXT NOT NULL CHECK(kind IN ('preference','fact','procedure','constraint','decision')),
			statement TEXT NOT NULL,
			status TEXT NOT NULL CHECK(status IN ('observed','proposed','completed','verified')),
			scope TEXT NOT NULL CHECK(scope IN ('global','project','session')),
			project_key TEXT NOT NULL DEFAULT '', session_id TEXT NOT NULL DEFAULT '',
			confidence REAL NOT NULL DEFAULT 0 CHECK(confidence >= 0 AND confidence <= 1),
			origin TEXT NOT NULL DEFAULT '',
			redaction_policy TEXT NOT NULL DEFAULT 'none',
			superseded_by TEXT,
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
			UNIQUE(id, principal_id),
			FOREIGN KEY(superseded_by, principal_id) REFERENCES mem_v2_knowledge_facts(id, principal_id))`,
	`CREATE INDEX mem_v2_knowledge_scope ON mem_v2_knowledge_facts(principal_id, scope, project_key, created_at DESC)`,
	`CREATE INDEX mem_v2_knowledge_current ON mem_v2_knowledge_facts(principal_id, superseded_by, created_at DESC)`,
	`CREATE TABLE mem_v2_knowledge_evidence (
			fact_id TEXT NOT NULL, principal_id TEXT NOT NULL, message_id TEXT NOT NULL,
			ordinal INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(fact_id, message_id),
			FOREIGN KEY(fact_id, principal_id) REFERENCES mem_v2_knowledge_facts(id, principal_id),
			FOREIGN KEY(message_id) REFERENCES mem_v2_messages(id))`,
	`CREATE INDEX mem_v2_knowledge_evidence_message ON mem_v2_knowledge_evidence(message_id)`,
	`CREATE TABLE mem_v2_knowledge_jobs (
			id TEXT PRIMARY KEY, principal_id TEXT NOT NULL,
			source_kind TEXT NOT NULL CHECK(source_kind IN ('session','turn','explicit')),
			source_id TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
			attempts INTEGER NOT NULL DEFAULT 0, lease_until INTEGER NOT NULL DEFAULT 0,
			next_run_at INTEGER NOT NULL DEFAULT 0, last_error TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL,
			UNIQUE(principal_id, source_kind, source_id))`,
	`CREATE INDEX mem_v2_knowledge_jobs_due ON mem_v2_knowledge_jobs(status, next_run_at, lease_until)`,
}

func checkArchiveSchema(ctx context.Context, db *sql.DB) error {
	version, err := archiveSchemaVersionOf(ctx, db)
	if err != nil {
		return fmt.Errorf("memstore: archive not initialized; run memory-migrate first: %w", err)
	}
	if version == 0 {
		// Version 0 is the never-migrated archive, which is a different problem
		// from a version mismatch and has a different remedy, so it keeps its
		// own message rather than being folded into the skew report below.
		return fmt.Errorf("memstore: archive not initialized; run memory-migrate first")
	}
	if version != archiveSchemaVersion {
		// The message names both numbers and the direction, because the two
		// directions need opposite remedies and the old message ("unsupported
		// archive schema 7 (want 6)") left the operator to work out which one
		// they were looking at. A newer archive is the case worth spelling out:
		// the fix is to upgrade this binary, and downgrading the archive to
		// satisfy it would discard data the newer binary wrote.
		status := SchemaStatus{Version: version, Supported: archiveSchemaVersion}
		if status.Newer() {
			return fmt.Errorf("memstore: archive schema %s; upgrade this binary instead of downgrading the archive", status)
		}
		return fmt.Errorf("memstore: archive schema %s", status)
	}
	return nil
}

// archiveSchemaVersionOf reads the recorded schema version from a raw
// connection. It exists so the writer's pre-open check and the Store method
// below cannot drift apart: both need the same "a missing table means never
// migrated" rule, and a second copy of that rule is a second place to get it
// wrong.
func archiveSchemaVersionOf(ctx context.Context, db *sql.DB) (int, error) {
	var version int
	err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM mem_v2_schema`).Scan(&version)
	if err != nil {
		// A missing table means the archive was never migrated, which is a
		// normal state for a legacy-only store, not an error.
		if strings.Contains(err.Error(), "no such table") {
			return 0, nil
		}
		return 0, fmt.Errorf("memstore: archive schema version: %w", err)
	}
	return version, nil
}

// ArchiveSchemaVersion reports the archive schema version of the open database,
// or 0 when the v2 archive has never been initialized. Callers use it to decide
// whether v2 retrieval is available before falling back to legacy recall.
func (s *Store) ArchiveSchemaVersion(ctx context.Context) (int, error) {
	return archiveSchemaVersionOf(ctx, s.db)
}

// SupportsV2Retrieval reports whether the open archive has the tables that
// scoped search, detail, and scroll depend on.
//
// It answers "do the tables exist", not "does this binary understand them". A
// newer archive still has the tables, so this returns true for it; callers that
// need to know whether the rows can be trusted must consult SchemaStatus, which
// is what the MCP server does at startup. Keeping the two questions separate
// matters because the answer to the first is a fact about the file and the
// answer to the second is a fact about the binary.
func (s *Store) SupportsV2Retrieval(ctx context.Context) bool {
	version, err := s.ArchiveSchemaVersion(ctx)
	return err == nil && version >= archiveSchemaVersion
}

// SchemaStatus describes how the open archive's schema relates to the version
// this binary was built against.
//
// It exists because the two can disagree in both directions and the directions
// are not equally dangerous. An older archive is missing tables and columns, so
// a query fails loudly and the operator is told to run memory-migrate. A newer
// archive is the quiet case: every table this binary knows about is still
// there, so every query succeeds, and the binary then reports a schema it does
// not actually understand. That is the skew the plan asks for a handshake to
// catch, and it cannot be caught by a query failing.
type SchemaStatus struct {
	// Version is the version recorded in the archive, or 0 when the v2 archive
	// has never been initialized.
	Version int
	// Supported is the version this binary writes and reads.
	Supported int
}

// Newer reports that the archive was written by a later binary. Reading it is
// not safe: a column this binary ignores may have changed meaning, and a
// constraint it does not know about may already have been violated.
func (status SchemaStatus) Newer() bool { return status.Version > status.Supported }

// Older reports that the archive predates this binary and needs an explicit
// upgrade before the writer will open it.
func (status SchemaStatus) Older() bool {
	return status.Version > 0 && status.Version < status.Supported
}

// Exact reports that the archive matches this binary.
func (status SchemaStatus) Exact() bool { return status.Version == status.Supported }

// String renders the status for a log line or a health report. It names both
// numbers because "schema: v2" is true of every version from 2 upward and
// therefore tells an operator nothing about skew.
func (status SchemaStatus) String() string {
	switch {
	case status.Version == 0:
		return fmt.Sprintf("not migrated (this binary supports version %d)", status.Supported)
	case status.Newer():
		return fmt.Sprintf("version %d, newer than this binary's %d", status.Version, status.Supported)
	case status.Older():
		return fmt.Sprintf("version %d, older than this binary's %d (run memory-migrate)", status.Version, status.Supported)
	default:
		return fmt.Sprintf("version %d", status.Version)
	}
}

// SchemaStatus reports the archive's schema version alongside the version this
// binary supports.
func (s *Store) SchemaStatus(ctx context.Context) (SchemaStatus, error) {
	version, err := s.ArchiveSchemaVersion(ctx)
	if err != nil {
		return SchemaStatus{}, err
	}
	return SchemaStatus{Version: version, Supported: archiveSchemaVersion}, nil
}
