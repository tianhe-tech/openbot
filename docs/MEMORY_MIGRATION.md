# Memory Archive Migration

## Current scope

PR 2 adds an opt-in v2 archive schema, a commit-acknowledged writer, and a resumable
legacy importer. The first PR 3 increment adds experimental strict live capture and
restart recovery; the second adds archive schema version 2 with rolling partial
snapshots and tool/attachment metadata. PR 4 adds archive schema version 3 with the
chunked lexical index (`mem_v2_chunks`, `mem_v2_chunk_fts`, `mem_v2_index_state`),
and version 4 with a word-tokenized index (`mem_v2_chunk_word_fts`) alongside it,
version 5 with the index the bounded scan needs (`mem_v2_message_scan`),
a background index worker in the gateway, and v2-backed `memory_search`,
`memory_get`, and `memory_scroll` in the MCP server. Normal gateway startup does not
initialize the archive; the default capture mode and MCP retrieval still use legacy
tables until the archive is migrated and strict capture is enabled.

Migration is additive: legacy records, handoffs, retry work, and skill tables are
not deleted or rewritten. Archive bodies are currently uncompressed, so importing
increases disk usage. Previously truncated or deleted content cannot be recovered.

## Run the migration

Use the actual configured database path; the gateway default is `mem/memory.db`.
The database must already exist and have been opened by the Phase 0 gateway.
Create the backup's parent directory first. Use a new backup filename.

```powershell
go run ./cmd/memory-migrate --db mem/memory.db --backup mem/memory.before-v2.db
```

The command creates a consistent SQLite backup using `VACUUM INTO`, including
committed WAL data, syncs the backup file, checks its integrity, then applies the
schema transactionally. An existing backup destination is refused. Backup failure
prevents schema migration; a partially created backup is retained for inspection.
Ensure enough space for the backup, new archive, WAL, and SQLite temporary files.
Restrict access to the database and backup, including Windows directory ACLs;
portable Unix permission bits do not configure Windows ACLs.

After successful schema initialization, resume an interrupted import or import
new legacy records without duplicating previously imported rows:

```powershell
go run ./cmd/memory-migrate --db mem/memory.db --resume --batch-size 64
```

`--resume` requires the current archive schema and does not create a backup or run
migrations. If interruption occurred before schema initialization, inspect the
previous backup and retry initialization with a different backup destination.
Do not delete your only backup simply to reuse its filename.

Stop gateway writes for the final catch-up pass. A running gateway may append
records after a batch reports zero pending rows. A backup is a snapshot, not a
backup of future writes. This command does not switch live capture to v2.

## Experimental strict capture

After migration and a final catch-up import, opt in explicitly:

```powershell
$env:MEMORY_CAPTURE_MODE = "strict"
go run ./cmd/gateway
```

The default is `legacy`. Invalid modes and missing/unmigrated strict archives fail
startup. Strict mode does not perform automatic schema migration. No deployed
database or environment setting was changed as part of implementation.

In strict mode, the client commits the adapter's original/normalized input before
handoff and skill wrappers, then commits an attempt and recovery job before
dispatch. The main chat entry points in DingTalk, Feishu, WeCom, and WeChat carry
transport delivery IDs. WeChat batches retain their component IDs. Other callers
without transport IDs receive a synthetic ID, carried through client fallback
attempts; this does not deduplicate independent calls across restarts.

Duplicate delivery IDs fail closed rather than re-execute a possibly completed
task. Distinct IDs with identical text remain distinct turns. Input or attempt
commit failures prevent prompt dispatch. Transport-level automatic prompt retries
are disabled; existing live model fallback creates another attempt on the same
turn. Background recovery never resends prompts. The legacy offline retry worker
and WeChat's automatic overflow resend are disabled in strict mode; queued legacy
retries remain stored. Explicit new requests still work.

A deliberate re-dispatch of an already archived delivery is the one exception to
fail-closed replay: token-overflow recovery and explicit retries set
`RetryOfDelivery`, which continues the existing turn with a new attempt. The
exception is safe because `BeginTurn` has already verified that the stored input,
thread, and working directory match the retry, so it cannot substitute a different
request. Unmarked duplicates still fail closed.

Each attempt carries a caller-supplied upstream user-message ID. IDs follow the
ascending timestamp/counter plus random-suffix format verified against OpenCode
v1.0.0; deployed upstream versions must support `messageID` and assistant `parentID`.
Synchronous responses and recovered history use the same byte-preserving save path.
The synchronous final response commits immediately; its recovery job stays pending
until a history scan also saves intermediate assistant messages for that request.
Only matching-parent finalized assistant text is archived; unrelated turns,
compaction summaries, reasoning, and UI progress notices are excluded. Provider
failures preserve available assistant text with failed state. Missing terminal
`finish` metadata keeps recovery pending instead of assuming success.

Recovery runs on startup and approximately every five seconds, claiming durable
60-second leases. Expired leases are reclaimable; saves are idempotent if workers
overlap. Explicit upstream 404s mark an attempt `unavailable` and retry after a
minute. Other unresolved attempts remain pending. A request timeout or an outbound
callback failure does not complete the archived turn. Restart recovery retrieves
evidence only; it does not restore messaging callbacks or guarantee late delivery
to the user's chat after a gateway restart.

Archive sessions currently group a principal's logical thread and working directory;
actual upstream session IDs belong to attempts. Sequence reflects durable capture
order, while original upstream timestamps remain available. Recovery reads upstream
session history and can be expensive for large sessions; bounded pagination and
benchmarking remain follow-up work.

Strict mode stops legacy conversation dual writes. MCP retrieval reads the v2
archive instead: `memory_search` searches the chunked lexical index, and
`memory_get` / `memory_scroll` return original bodies and session pages. Keep
`legacy` in production until the remaining capture gates have shipped, unless
testing this limitation intentionally. Returning to `legacy` leaves existing
archive data intact but does not backfill it into legacy search. Handoff and skill
tables remain operational.

PR 3 is not fully complete and strict mode remains experimental. The open gates
are listed in [Gate status](MEMORY_IMPLEMENTATION_PLAN.md#gate-status).

Inbound text is now durable before client admission. The WeChat debounce batcher
persists each buffered message to `wechat_inbound_queue` (SQLite, WAL) before arming
its timer and deletes the row only after dispatch succeeds, so a crash inside the
debounce window no longer loses an accepted request. On startup the gateway prunes
rows older than 24 hours and replays the rest, grouped by sender and chat and
concatenated in arrival order, before polling begins. A failed replay keeps its rows
for the next restart. Media messages bypass the batcher and are not buffered here.
Outbound delivery is likewise durable: `wechat_outbound_queue` survives a restart
with its pacing, retry, and park state intact, and the same client IDs are reused so
a redelivered chunk is not duplicated upstream.

Specialized adapter entry points are covered. Internal bootstrap prompts — the
synthetic "initialize session" requests that only materialize an upstream session
for shell execution — are marked `Internal` and never become archived turns.
Token-overflow recovery and explicit retries re-dispatch the original delivery with
`RetryOfDelivery`, which continues the existing turn with a new attempt instead of
failing closed as a replay or creating a duplicate user request. DingTalk, Feishu,
WeCom, and WeChat carry the original transport delivery ID through their
pending-overflow state so the retry keeps the same turn identity. A retry is only
accepted after `BeginTurn` has verified that the stored input, thread, and working
directory match, so it cannot substitute a different request.

Rolling partial snapshots and tool/attachment metadata are implemented as archive
schema version 2. `memory-migrate` upgrades a version-1 archive in place inside one
transaction; version-1 data is preserved and the new tables appear alongside it.
Snapshots are coalesced to at most one write per 5 seconds or 16 KiB of additional
text and live in `mem_v2_partials`, so they never rewrite a finalized message; a
newer revision replaces the previous snapshot and its superseded block is deleted.
Tool calls record name, call ID, parent message ID, state, timing, and an artifact
reference with a `retained`/`truncated` reason; inline output is bounded to 8 KiB.
Attachment metadata is bounded to 2 KiB of URL, and `data:` URIs are reduced to
their media type, so media bytes never enter the archive or its search index. These
writes are best-effort and asynchronous: a failure is logged and never interrupts
delivery. Partial snapshots bound only locally observed output; they do not promise
that upstream retains its own transcript.

For local diagnostics, inspect `mem_v2_turns.status/dispatch_state`,
`mem_v2_attempts.status`, and `mem_v2_jobs` rows with `kind='reconcile'` using a
read-only SQLite client. `not_dispatched` means no attempt was bound; `uncertain`
means dispatch could have happened. Never interpret either as permission to replay.

## Lexical index (schema versions 3, 4, and 5)

Archive schema version 3 adds the retrieval index. `memory-migrate` upgrades a
version-2 archive in place inside the same transaction mechanism; existing data is
preserved. Version 4 adds a second index over the same chunks and version 5 adds
one index on `mem_v2_messages`; both are applied by the same command.

- `mem_v2_chunks` stores one row per chunk: `message_id`, `message_revision`,
  `ordinal`, `start_byte`, `end_byte`, and `tokenizer_version`, unique on
  `(message_id, message_revision, ordinal)`.
- `mem_v2_chunk_fts` is a standalone FTS5 trigram table whose rowid is
  `mem_v2_chunks.id`. Text is duplicated rather than contentless; contentless FTS
  is deferred to the space-optimization phase.
- `mem_v2_chunk_word_fts` (version 4) is a second FTS5 table over the same chunk
  text with the `unicode61` tokenizer, keyed by the same chunk rowid. It exists
  because the trigram tokenizer is correct but slow: measured on identical chunk
  text, the same three-term query costs 117.4 ms against the trigram index and
  7.38 ms against a `unicode61` index, a factor of 15.9, while matching 1157 and
  1142 chunks respectively. The cost is the tokenizer, not the query and not the
  joins.
- `mem_v2_index_state` is a single row holding `tokenizer_version`,
  `updated_at`, and (version 4) `word_backfill_rowid`, so a tokenizer change can
  be detected and reindexed and the word backfill can resume after a restart.
- `mem_v2_message_scan` (version 5) is an index on
  `mem_v2_messages(principal_id, created_at DESC, seq DESC)`. It exists for the
  bounded scan, which is the fallback for one- and two-character terms that
  neither tokenizer can index. That scan reads at most `maxFallbackScan` messages
  newest first, and it was written as if the cap bounded the work. It did not:
  the query orders by `created_at` and `seq`, and `mem_v2_messages` had no index
  on either, so SQLite scanned the whole table, materialized every message in the
  principal including its payload, and sorted all of it before the `LIMIT` could
  apply. Measured at 100k messages the scan took 13.8 s p95, against 463 ms for
  the word path and 8.4 ms for the same scan at 2k. `EXPLAIN QUERY PLAN` reported
  `SCAN m` plus `USE TEMP B-TREE FOR ORDER BY`; with this index it reports
  `SEARCH m USING INDEX mem_v2_message_scan (principal_id=?)` and no temporary
  B-tree. The same scan at 100k now measures 347.1 ms p95, and the pair of
  measurements is the real evidence: 280.3 ms at 2k against 347.1 ms at 100k is
  1.24x for a fiftyfold larger corpus, where before the index the same pair was
  533.8 ms against 13781 ms, 25.8x. A bounded scan whose cost tracks the corpus
  is not bounded; one whose cost tracks the cap is.
  `TestSearchScanQueryPlan` pins the plan so the regression cannot return
  silently. The index is on `principal_id` first because that is the scan's only
  equality predicate, and the two sort columns follow in the direction the query
  asks for so the rows come out already ordered and the `LIMIT` stops the walk.

  The index also had a cost the scan measurement could not see, and it is worth
  recording because it is what an index does quietly. Adding it changed the plan
  of the search query, which joins the FTS table to `mem_v2_messages`. SQLite
  reorders inner joins by estimated cost, and the new index gave it a cheap way
  to reach a principal's messages, so it began driving the search from
  `mem_v2_messages` and probing the FTS index once per message instead of once
  per match. The inner query went from 75.20 ms to 104471 ms at 20k messages, a
  factor of 1388, and the full test suite passed throughout because no test
  asserted a query plan. The fix is in `searchIndex`, not in the schema: `CROSS JOIN` on the
  FTS table forbids the reordering, and `TestSearchIndexQueryPlanPinsFTSDriver`
  fails if the FTS table stops driving. The index stays, because the scan needs
  it and the search no longer depends on the planner's choice.

Chunking targets 1 KiB, caps at 2 KiB, and overlaps 128 bytes. Boundaries prefer
paragraph, then line, then sentence punctuation, then space, then a rune-aligned
hard cut. Chunks always cover the whole body with no gaps, so every stored byte is
reachable through an evidence offset.

The gateway runs the index worker when strict capture is enabled. It consumes the
`kind='index'` jobs written with each archived message, leases them for 60 seconds,
and marks each `completed` or `failed`. A single unindexable message records
`last_error` and does not stall the queue. Re-indexing a revision deletes the prior
chunks and FTS rows from both indexes first, so the operation is idempotent. The
worker stops before the archive writer drains on shutdown.

### Word index backfill

Version 4 adds the word index to an archive that already has chunks. Those chunks
have trigram entries but no word entries, so a word-routed query would silently
miss them. Copying inside the migration was rejected: on a large archive that
turns an upgrade into a full reindex, and a migration that can time out halfway
leaves the archive in a state nobody can reason about.

Instead the index worker fills the word index in the background, and only when
the message queue is drained, so a newly captured message always reaches both
indexes before old chunks are copied. The pass is resumable: `word_backfill_rowid`
is the largest chunk id already copied, and a restart continues from there rather
than rescanning. The copy uses `INSERT OR REPLACE` because the backfill and the
normal write path overlap by design — a chunk created after the migration already
has a word entry and the backfill will still reach it.

Until the backfill finishes, `memory_search` reports `Word index: PARTIAL` for
`word` and `hybrid` queries. That is a different statement from `Coverage:
PARTIAL`: the messages are indexed, just not in the index that query used. `fts`
queries are unaffected because the trigram index is complete.

Diagnose coverage with a read-only query:

```sql
SELECT status, COUNT(*) FROM mem_v2_jobs WHERE kind='index' GROUP BY status;
SELECT COUNT(*) FROM mem_v2_chunks;
SELECT * FROM mem_v2_index_state;
```

`memory_search` reports `Coverage: PARTIAL` whenever index jobs are pending, so a
missing match is never presented as a confident miss. One- and two-character terms
cannot match either index; those queries fall back to a bounded scoped scan and
say so. The scan reports `TooBroad` and a notice naming the cap when it stopped at
`maxFallbackScan` messages before reaching the end of the scope, because "these
are the matches in the newest 2000 messages" and "these are the matches" are
different answers and only one of them is true. The rendered advice differs by
mode for the same reason: a capped candidate pool is fixed by narrowing the query,
while a capped scan is not, because its terms are already too short to index and
no rewording of them will help. Cursors are bound to the query, scope, and index
generation, so a reindex invalidates an outstanding cursor and the call must be
restarted.

Each committed batch emits a JSON line:

```json
{"imported":64,"pending":128,"unmapped":0}
```

`imported` counts that batch; `pending` counts remaining rows with adapter/user
identity; `unmapped` counts rows without that identity. Unmapped originals are
retained and the command exits nonzero after importing eligible rows. It never
guesses ownership. Resolve identity only from trusted evidence before retrying.

Batch size is 1-1000 records and is also bounded by the writer's estimated byte
budget. The default budget is 16 MiB. A single oversized row fails explicitly;
increase the budget only after checking available memory:

```powershell
go run ./cmd/memory-migrate --db mem/memory.db --resume --queue-bytes 33554432
```

## Evidence and retry semantics

Each legacy record becomes one synthetic session and turn with separate user and
assistant messages. Original stored body bytes are preserved, including malformed
UTF-8 from older byte truncation. Fidelity is `legacy_unknown`; no real upstream
session, message identity, or cross-record chronology is fabricated. The turn's
status is also `legacy_unknown`, not proof of a successful historical execution.
Legacy tables retain metadata not represented in v2, such as old summaries/tags.

Messages, raw blocks, durable index jobs, and the legacy mapping commit together.
Restarting after a failed or uncertain batch is idempotent. Stable message IDs with
different content or ownership are rejected, including differences in invalid
legacy text bytes. Do not mutate already imported legacy rows expecting `--resume`
to refresh their archive copies; mapping tracks completed imports, not updates.

## Writer contract

`OpenArchiveWriter` requires explicit schema initialization. Its dedicated SQLite
connection uses WAL, `synchronous=FULL`, foreign-key enforcement, a 5-second busy
timeout, and immediate write transactions. SQLite serializes competing writers;
there is no process lease or application-level automatic retry loop yet. Default
transaction context timeout is 2 seconds, but driver lock waits may affect observed
latency. No hardware-independent latency guarantee is made.

Admission counts queued and in-flight work by items and estimated bytes. Default
limits are 1024 operations / 16 MiB; batches target 64 operations / 256 KiB / 10 ms.
An operation larger than the batch byte target runs alone if it fits admission.
A legacy import batch is one atomic operation. These are queue limits, not a total
process-memory cap: callers retain bodies, encoding creates temporary allocations,
and reading a legacy row allocates it before checking its size.

Successful `RecordMessage` returns only after commit. Queue saturation waits for
capacity or caller cancellation; it does not silently drop an accepted operation.
Cancellation before admission means no write was accepted. Cancellation afterward,
or a commit error, can return `ErrArchiveCommitUnknown`; retry with exactly the same
identities, timestamps, and content. Caller cancellation can skip queued work but
does not abort another caller's already active batch. Any operation failure rolls
back the entire batch and is returned to all participating callers.

`Flush` is an ordering barrier, not a summary of earlier operation errors. Callers
must check their own receipts. `Close` stops admission and drains accepted work.
If its context expires, the worker continues draining and owns the connection until
it finishes; call `Close` again to wait. Queue and outcome counters are available
through `Stats`. The lifecycle interface builds attempt binding, restart recovery,
rolling partial snapshots, and tool/attachment metadata on these primitives.
`IndexPending` and `RunIndexWorker` build the lexical index on the same writer, so
indexing shares the writer's admission control and never opens a second connection.

## Verification and rollback

```powershell
go test ./internal/memstore ./cmd/memory-migrate
```

Tests cover WAL-inclusive backups, resumable imports, unmapped owners, exact bytes,
byte-bounded batches, atomic rollback, queue pressure, shutdown, competing writers,
database capacity failure, and killing a process after commit acknowledgment.
Capture tests also cover pre-dispatch storage failure, duplicate deliveries,
single-shot HTTP dispatch, late results after restart, failed callbacks, intermediate
assistant messages, missing sources, and terminal-state classification. Retrieval
tests cover chunk boundary and offset coverage, rune-safe splitting, response-only
terms, CJK terms, short-query scan fallback, scope isolation, project filtering
before the candidate limit, missing terms, cursor binding and restart, index-lag
reporting, idempotent reindexing, original-body detail with budget truncation,
session scroll pagination, identical text in different turns, exact-identifier
boosting, literal punctuation, old-message recall, time bounds, and the background
index worker draining and stopping. The affected memory, OpenCode, adapter, gateway,
MCP, and migration-command test suites pass. This is not a power-loss or
storage-device durability certification. Run race tests on a CGO-enabled runner
with a supported C compiler before release.

Abandoning this experiment does not require restoring the database. Set capture
mode back to `legacy` and leave the additive tables in place; strict-only records
remain in v2, not legacy search. If disaster
recovery requires restoring a backup, stop every database user and use a consistent
SQLite restore procedure; do not mix a restored database with stale WAL/SHM files.
Restoring the snapshot discards all changes made after it was taken.

See [the implementation plan](MEMORY_IMPLEMENTATION_PLAN.md) for subsequent phases.