# Memory Archive Migration

## Current scope

PR 2 adds an opt-in v2 archive schema, a commit-acknowledged writer, and a resumable
legacy importer. The first PR 3 increment adds experimental strict live capture and
restart recovery; the second adds archive schema version 2 with rolling partial
snapshots and tool/attachment metadata. PR 4 adds archive schema version 3 with the
chunked lexical index (`mem_v2_chunks`, `mem_v2_chunk_fts`, `mem_v2_index_state`),
and version 4 with a word-tokenized index (`mem_v2_chunk_word_fts`) alongside it,
version 5 with the index the bounded scan needs (`mem_v2_message_scan`),
version 6 with the instance lease (`mem_v2_instance_lease`),
version 7 with both full-text indexes converted to contentless,
version 8 with the durable-knowledge tables (`mem_v2_knowledge_facts`,
`mem_v2_knowledge_evidence`, `mem_v2_knowledge_jobs`),
a background index worker in the gateway, and v2-backed `memory_search`,
`memory_get`, and `memory_scroll` in the MCP server. Normal gateway startup does not
initialize the archive; the default capture mode and MCP retrieval still use legacy
tables until the archive is migrated and strict capture is enabled.

Migration is additive: legacy records, handoffs, retry work, and skill tables are
not deleted or rewritten. Archive bodies are compressed with zstd when that is
smaller than the raw bytes, so importing no longer increases disk usage by the
full size of the content. Previously truncated or deleted content cannot be
recovered.

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
timeout, and immediate write transactions. SQLite serializes competing writers, so
two processes cannot corrupt the archive, but serialization is not coordination:
each process would still run its own capture queue and index worker against the
same rows. The gateway therefore takes the instance lease described below before
it starts capturing. Default transaction context timeout is 2 seconds, but driver
lock waits may affect observed latency. No hardware-independent latency guarantee
is made.

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

## Instance lease and WAL maintenance (schema version 6)

Version 6 adds `mem_v2_instance_lease`, a single row that records which process is
capturing into the archive. The gateway acquires it before it starts capturing and
releases it after the writer drains on shutdown.

The lease exists because SQLite's serialization is not coordination. Two gateways
on one archive cannot corrupt it, but each would run its own capture queue, its own
index worker, and its own WAL checkpoint against the same rows, and the only thing
preventing that was that nobody had tried it. The plan requires the second instance
to be rejected rather than silently tolerated.

- The row is `(id=1, holder, acquired_at, heartbeat_at, expires_at)`. `holder` is
  `<hostname>:<pid>` by default, so a startup failure can name the process to stop
  instead of only reporting that the archive is busy.
- Acquisition is one statement: `INSERT … ON CONFLICT(id) DO UPDATE … WHERE
  expires_at <= now RETURNING holder`. A read followed by a write would let two
  processes both observe an expired lease and both claim it, which is the exact
  race the lease exists to prevent. No row returned means the lease is held.
- The holder renews every `TTL/3` (default TTL 90 s). A crashed holder therefore
  blocks a restart for at most 90 seconds, and a clean shutdown blocks it for zero.
- The lease is opt-in at the API level: `AcquireLease` is a separate call, so
  `TestArchiveConcurrentWritersAndIdentityIsolation` can still open two writers on
  one archive to prove the writer's own conflict handling. The gateway always takes
  it.

WAL maintenance is the other half of the same policy. SQLite's autocheckpoint runs
inside a write transaction, so a continuously busy writer can starve it and the
`-wal` file then grows without bound. `RunMaintenance` checkpoints on an interval
(default 5 minutes) but only when the WAL exceeds a threshold (default 64 MiB), and
it uses `wal_checkpoint(TRUNCATE)` rather than `PASSIVE` because `PASSIVE` leaves
the file at its high-water mark, so one burst of writes would keep it large
forever. A checkpoint that cannot complete because a reader holds the WAL is
reported as busy, not as an error: that is normal under concurrent reads.

## Scoped deletion

`cmd/memory-delete` removes one principal's archived conversation. It is a
separate binary because the MCP server opens the archive read-only on purpose:
it is the process a model talks to, and a model must not be able to delete a
user's history because a prompt asked it to.

```bash
memory-delete --db memory.db --adapter dingtalk --user 0123456789 --confirm
```

`--adapter`, `--user`, and `--confirm` are all required. `--session`,
`--project`, `--since`, and `--until` narrow the selection further. The command
prints the `DeleteReport` as JSON so an operator can record what was removed.

The deletion is one transaction on the writer's queue, so it cannot interleave
with capture, and every statement carries the principal predicate — there is no
code path where an ID alone is enough. It removes messages, chunks, both FTS
tables, index and reconcile jobs, partials, tool calls, attachments, attempts,
turns, sessions, the legacy import map, and blocks that nothing references any
more. A block shared by two tool calls survives the deletion of one of them,
because the sweep is by reference rather than ownership; that also makes the
deletion resumable, since a crash mid-transaction leaves the archive consistent
and a later run collects whatever became unreferenced.

It is logical deletion, not a secure erase. The bytes can survive in free pages,
in the WAL until a checkpoint truncates it, and in any backup taken beforehand.
`docs/MEMORY_DELETION.md` states that plainly, because an operator who believes
otherwise will make a promise to a user that the software cannot keep.

A deletion changes the ranked dataset, so it bumps
`mem_v2_index_state.updated_at` and any search cursor issued before it is
refused with `ErrSearchRestartRequired`. The bump is monotonic
(`MAX(updated_at+1, now)`) rather than a plain clock assignment: a generation
that can repeat cannot invalidate a cursor, and `IndexPending` uses the same
rule so both writers agree.

## Version handshake

Both binaries check the archive's schema version before they touch it, and both
describe a mismatch the same way, through `memstore.SchemaStatus`. The check
exists because the two directions of skew are not equally dangerous:

- An **older** archive is missing tables and columns, so every v2 query fails on
  its own. The operator is told to run `memory-migrate`.
- A **newer** archive is the quiet case. Every table this binary knows about
  still exists, so every query succeeds, and the process answers from rows whose
  meaning it does not know while reporting a version it cannot interpret.
  Nothing in the read path can detect that, which is why the check runs before
  the first request rather than on the first failure.

The message names both numbers and the direction, because the remedies are
opposite: a version above the binary's means upgrade the binary, a version below
means run `memory-migrate`. Guessing wrong on a newer archive means downgrading
it, which discards whatever the newer binary wrote. A never-migrated archive
keeps its own message, since there is no version to compare and the fix is to
migrate rather than to change binaries.

`memory_health` prints the exact version and both numbers when they differ. It
does not print the word `v2`, which is true of every version from 2 upward and
therefore cannot show that a migration is pending.

## Block packing was measured and declined

The plan asks to benchmark per-message compression against session-local 32–128
KiB blocks before building packing. `packing_bench_test.go` does that, and the
answer is to not build it yet.

Packing saves space and the saving is large: on a 4 MiB conversation of 1,317
messages, the current per-message format stores 2,234,727 bytes and packing into
64 KiB blocks stores 1,342,727 — 40% less. Most of those messages are below the
4 KiB compression threshold and are stored raw today, which is exactly the
population packing helps: a 200-byte reply has no internal redundancy, but twenty
of them share a vocabulary.

It costs read latency, and the cost is sized by the block rather than by the
request. A 20-message page fetch goes from 83 µs to 155 µs and from 43.5 KB to
131 KB allocated, because the whole block is decompressed to return a page of it.
A single 200-byte message read out of a 64 KiB block costs 92 µs against 3 ns,
since the current format returns a slice of the stored bytes and never enters the
decoder.

Both read numbers are three orders of magnitude below the latency budget, so the
gate as written would pass. The reason to decline is the ratio and the
alternative: the same 40% is recoverable from contentless FTS5 without touching
the read path at all, and the index is the larger of the two consumers. Packing
should be revisited after that work, when the remaining space is message payloads
rather than duplicated index text.

The consequence for the plan is that crash-safe repacking has no caller. It
exists to make asynchronous compression and packing safe, and with packing
declined there is nothing to repack. Two properties are pinned now so a later
implementation inherits them: packing must be whole-message rather than
fixed-size chunks, so every recorded `byte_offset` lands on a rune boundary, and
the space test asserts only that the measurement is self-consistent rather than
that packing wins.

## Contentless FTS5 was measured, then adopted in schema v7

The archive stored each chunk's text three times: once in `mem_v2_blocks` and
once in each of the two FTS5 indexes. Removing the index copies was the obvious
space win, and `fts_contentless_probe_test.go` measured whether it was available.

It is. The pinned driver bundles SQLite 3.51.3, and `contentless_delete=1`
accepts every operation the archive performs — insert, `MATCH`, `bm25()`, DELETE
by rowid, and UPDATE by rowid. A plain contentless table (`content=''` without
the option) rejects DELETE, which is why the option matters; the comment in
`delete.go` that said a contentless index "would not" support deletion was wrong
and has been corrected.

The cost is the excerpt. `snippet()` and `highlight()` both return empty on a
contentless table, so the excerpt has to be reconstructed from the chunk's byte
range into the message text. `searchScan` already did exactly that with
`excerptWindow`, so the machinery existed, but `searchIndex` selected
`ft.content` and had to be changed to join the block and decode it instead.

The saving is a constant per index rather than a percentage, and that is the
finding that matters. Contentless removes the stored text and keeps the
postings, so every configuration saves exactly one copy of the chunk text —
2,736,128 bytes on a 2,000-chunk corpus. The percentage is a statement about how
large the postings are, and they are large for trigram:

| Vocabulary | Tokenizer | Plain index | Contentless | Saving |
| --- | --- | --- | --- | --- |
| 30 words | unicode61 | 3,162,112 B | 425,984 B | 86.5% |
| 30 words | trigram | 5,373,952 B | 2,637,824 B | 50.9% |
| 20,000 words | unicode61 | 3,567,616 B | 827,392 B | 76.8% |
| 20,000 words | trigram | 8,740,864 B | 6,004,736 B | 31.3% |

**The archive runs both indexes and each stores its own copy, so the saving is
the sum of two rows, not one.** `fts_contentless_ratio_test.go` measured that on
a conversation-shaped archive written through the real write path and indexed by
the real index worker, and now asserts the outcome on the same fixture:

| Corpus | Archive file | Full-text index | Stored text | Copies of chunk text | Share of file |
| --- | --- | --- | --- | --- | --- |
| random words | 14,008,320 B | 12,734,464 B | 5,242,880 B | 2.24× | **37.4%** |
| prose (Zipf) | 14,786,560 B | 11,325,440 B | 5,308,416 B | 2.26× | **35.9%** |

The "copies of chunk text" column is the check that catches the error. An earlier
revision of this section took a single row's saving and divided it by the
combined index size, which halved the answer and reported 15% of the archive.
The stored text is 2.24–2.26 times the chunk text — two indexes plus page
rounding — so the saving is about **36% of the archive**, within a few points of
packing's measured 40%.

The read-path cost was then measured rather than assumed, because a space saving
and a latency regression are not comparable numbers. `BenchmarkSearchExcerptReconstruction`
found the reconstruction costs **1.13× at 2,000 messages** (8.96 → 10.16 ms p50)
and is indistinguishable at 20,000 (76.9 vs 76.2–78.2 ms), where the excerpt is
a rounding error against the scan. That was cheap enough to adopt, so schema v7
does.

### What the migration does

`archiveMigrationV7` **copies the rows into a new contentless table and swaps**,
rather than dropping the index and rebuilding it:

```sql
CREATE VIRTUAL TABLE mem_v2_chunk_fts_v7 USING fts5(content, content='', contentless_delete=1, tokenize='trigram');
INSERT INTO mem_v2_chunk_fts_v7(rowid, content) SELECT rowid, content FROM mem_v2_chunk_fts;
DROP TABLE mem_v2_chunk_fts;
ALTER TABLE mem_v2_chunk_fts_v7 RENAME TO mem_v2_chunk_fts;
```

The copy is not laziness. The trigram index has **no backfill mechanism** — the
word index has `word_backfill_rowid` and a resumable pass, but nothing rebuilds
trigram postings from the blocks. Dropping the table would therefore leave every
existing archive with a permanently empty trigram index and search silently
degraded to the bounded scan, which is the failure mode that looks like working
software. Copying preserves the postings and only discards the text.

The word index is copied the same way, and its `word_backfill_rowid` high-water
mark is left untouched so a partially backfilled archive resumes where it was.

The cost is transient: until the swap, the archive holds the old table and the
new one at once, so the migration needs roughly one extra copy of the index in
free space. `memory-migrate` takes a backup first for that reason.

### What changed on the read path

A contentless table still accepts text at INSERT time — it just does not keep it
— which is what makes the copy possible. It also means every reader that
selected `ft.content` had to be found and fixed. There were three:

- **`searchIndex`** now joins `mem_v2_blocks`, decodes the block, and slices the
  chunk's `[start_byte, end_byte)` range. The collapse to one hit per message
  moved **before** the decode, because the benchmark showed 1.27 chunks match per
  message at 2,000 messages and decoding each of them would have paid the
  reconstruction cost more than once per result.
- **`BackfillWordIndex`** was the most dangerous of the three. It read
  `ft.content` to populate the word index; on a contentless table that returns
  NULL, so it would have inserted empty strings and **silently emptied the word
  index on every archive upgraded to v7**. It now decodes the block too.
- **`tokenizer_bench_test.go`** copied chunk text out of the index to build its
  comparison corpus, and would have failed its own "the archive is empty"
  precondition.

`delete.go` needed no change: `contentless_delete=1` accepts DELETE by rowid,
which the probe had already proven.

### What an operator will see

- `snippet()` and `highlight()` return empty on these tables. Nothing calls them
  — excerpts are reconstructed — but a future caller must not expect them.
- `memory_space` reports **`fts_content_bytes: 0`**. That zero is the migration's
  measurement, not a regression, and `fts_contentless_ratio_test.go` now asserts
  it: a content column reintroduced by a later schema change would keep search
  working and would only show up as a larger archive, so nothing else in the
  suite would notice.
- `fts_bytes` is a subset of `logical_bytes`, **not of `file_bytes`**. `dbstat`
  reads through the pager and counts pages still in the WAL, while `file_bytes`
  is `os.Stat` on the main file alone. On a freshly written archive the index can
  legitimately appear larger than the file. `logical_bytes` is reported so the
  two can be compared correctly.

## Durable knowledge (schema version 8)

Version 8 adds three tables for source-backed durable knowledge:

| Table | Purpose |
| --- | --- |
| `mem_v2_knowledge_facts` | the statements, their scope, status, confidence, origin, redaction policy, and supersession pointer |
| `mem_v2_knowledge_evidence` | which messages support each fact |
| `mem_v2_knowledge_jobs` | extraction work that is still pending |

The migration is additive: it creates tables and indexes and touches no existing
row. An archive that never enables `MEMORY_KNOWLEDGE_ENABLED` carries three empty
tables and nothing else changes.

Three decisions are worth recording because each one is a place a simpler design
would have been wrong:

- **Evidence is a table, not a JSON column.** Deletion has to find facts *by
  message*. A JSON array cannot be indexed or joined, so a deletion would have to
  scan and parse every fact — and the one thing a deletion must not do is miss a
  row.
- **Supersession is a column, not a delete.** A correction adds a row and points
  the old one at it, so the history of what was believed, and the evidence for
  it, survives. Forgetting the correction un-supersedes the original rather than
  leaving a hole.
- **The evidence foreign key is not `ON DELETE CASCADE`.** A cascade would make a
  deletion that forgot to remove evidence succeed silently. Without one, it
  aborts loudly, which is the behaviour a deletion report needs in order to stay
  honest. `TestArchiveMigrationUpgradesVersionOneArchive` asserts the absence of
  the cascade.

`mem_v2_knowledge_jobs` is separate from `mem_v2_jobs` because that table's
uniqueness is `(kind, source_id, source_revision)` and its source is a message,
while a knowledge job's source is a session, a turn, or an explicit request.

See [durable knowledge](MEMORY_KNOWLEDGE.md) for the extraction pipeline, the
injection budget, and the commands.

## Verification and rollback

```powershell
go test ./internal/memstore ./cmd/memory-migrate ./cmd/memory-delete -timeout 40m
```

`-timeout` is required, not a convenience. `go test` defaults to ten minutes per
package and `internal/memstore` now exceeds it: the suite builds real archives,
runs the index worker over them, and measures FTS5 index sizes, so a full run
takes well over ten minutes. Without the flag the package does not fail an
assertion — it panics with `test timed out after 10m0s` while a test is still
legitimately running, which reads like a hang and is not one. The ceiling is set
at 40 minutes so a genuine hang still terminates.

Tests cover WAL-inclusive backups, resumable imports, unmapped owners, exact bytes,
byte-bounded batches, atomic rollback, queue pressure, shutdown, competing writers,
database capacity failure, and killing a process after commit acknowledgment.
Maintenance tests cover WAL truncation, the below-threshold skip, data survival
across a truncating checkpoint, and loop cancellation; lease tests cover rejection
of a second holder, clean handover, expiry after a crash, heartbeat renewal, and
expired-row reporting. Deletion tests cover the scope requirement, principal
isolation across every table, chunk and FTS removal, a block shared by two
records, idempotence, cursor invalidation, session-scoped deletion, and the
orphan sweep; the command tests cover the end-to-end scope pass-through, the
confirmation requirement, and argument rejection. The handshake tests cover a
newer archive, an older one, a never-migrated one, and the gateway's refusal
message in both directions — including that the older case does not borrow the
newer wording, since telling an operator to upgrade a current binary sends them
looking for a release that does not exist. The v7 test asserts the migration by
probing for the `_content` shadow tables rather than by reading the CREATE
statement back: the statement is what the migration wrote, the shadow table is
what SQLite actually built, and only the second one proves the index is
contentless.
Capture tests also cover pre-dispatch storage failure, duplicate deliveries,
single-shot HTTP dispatch, late results after restart, failed callbacks, intermediate
assistant messages, missing sources, and terminal-state classification. The
real-upstream compatibility test asserts the wire format against a live
`opencode serve` rather than a fake server: session creation, the message and part
JSON shape including a caller-supplied `messageID` echoed back, the `/event`
envelope, the `/global/event` envelope with its `payload` wrapper and `sync`
variant, and the gateway's own parsers fed real captured events. It skips unless
`OPENCODE_ENDPOINT` is set, and every prompt uses `noReply: true` so no provider is
invoked:

```powershell
opencode serve --port 4096 --hostname 127.0.0.1
$env:OPENCODE_ENDPOINT = "http://127.0.0.1:4096"
go test ./internal/opencode -run TestRealUpstream -v
```

Retrieval
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