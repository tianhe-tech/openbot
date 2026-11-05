package memstore

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const archiveSchemaVersion = 5

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
// entry are always addressed by the same integer. The layout deliberately
// duplicates chunk text: contentless FTS is a space optimization deferred to
// the compression phase, and correctness comes first.
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

func checkArchiveSchema(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM mem_v2_schema`).Scan(&version); err != nil {
		return fmt.Errorf("memstore: archive not initialized; run memory-migrate first: %w", err)
	}
	if version != archiveSchemaVersion {
		return fmt.Errorf("memstore: unsupported archive schema %d (want %d)", version, archiveSchemaVersion)
	}
	return nil
}

// ArchiveSchemaVersion reports the archive schema version of the open database,
// or 0 when the v2 archive has never been initialized. Callers use it to decide
// whether v2 retrieval is available before falling back to legacy recall.
func (s *Store) ArchiveSchemaVersion(ctx context.Context) (int, error) {
	var version int
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM mem_v2_schema`).Scan(&version)
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

// SupportsV2Retrieval reports whether the open archive has the tables that
// scoped search, detail, and scroll depend on.
func (s *Store) SupportsV2Retrieval(ctx context.Context) bool {
	version, err := s.ArchiveSchemaVersion(ctx)
	return err == nil && version >= archiveSchemaVersion
}
