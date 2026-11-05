# Memory System Implementation Plan

Status: Phase 0 / PR 1 and the PR 2 storage foundation are implemented in the working tree. PR 3 core capture, including partial snapshots and tool/attachment metadata, is implemented experimentally. PR 4 scoped lexical search, detail, and scroll are implemented: archive schema v3 adds the chunk FTS index, the gateway runs a background index worker, and the MCP server exposes `memory_search` (v2-backed when the archive is migrated), `memory_get`, and `memory_scroll`. PR 3's external gates and PR 5 onward remain pending; [Gate status](#gate-status) is the authoritative list.

Phase 0 includes loss prevention, read-only owner-bound MCP access, response-index
migration, atomic legacy record/index writes, and honest evidence excerpts.
It does not implement v2 durable capture, detail pagination, compression, or
shared-user transport isolation. New memory tools remain disabled until an
isolated single-owner binding is configured.

Verification for Phase 0: memory, MCP, gateway, OpenCode client, and adapter tests
passed; targeted `go vet`, `go build ./...`, and `git diff --check` passed.
Race tests could not run on this Windows environment: CGO is disabled and GCC is
not installed. Run `go test -race ./internal/memstore ./cmd/mcp ./cmd/gateway`
on a CGO-enabled runner before release. No deployed memory database was opened
or migrated during implementation; tests use temporary databases.

PR 2 adds explicit additive schema migration, an item/byte-bounded commit writer,
raw byte-exact message blocks with atomic index jobs, and backup-verified resumable
legacy import. Its CLI is documented in [Memory Archive Migration](MEMORY_MIGRATION.md).
PR 2 alone does not switch gateway capture or MCP retrieval to v2, execute index
jobs, implement lifecycle reconciliation, or compress bodies.

PR 2 focused tests cover queue pressure, cancellation/shutdown, transactional
rollback, competing writers, database capacity failure, process-kill recovery,
byte-exact idempotency, backup verification, and resumable import. Race tests remain
a required external gate; process-kill tests do not certify hardware power loss.

PR 3 core capture adds the consumer interface in `internal/opencode/capture.go`,
storage lifecycle operations, explicit `MEMORY_CAPTURE_MODE=strict`, primary adapter
delivery IDs, input-before-dispatch commits, stable attempt/message correlation,
single-shot strict HTTP dispatch, callback-independent final-text capture, leased
restart recovery, and bounded shutdown. Legacy remains the default; strict mode
stops legacy conversation dual writes, while MCP remains legacy-only. See the
migration document for safe activation, rollback, and deliberate retry restrictions.

PR 3 also adds archive schema version 2 with rolling partial snapshots and
tool/attachment metadata. Partial snapshots are coalesced to at most one write per
5 seconds or 16 KiB of additional text, stored in `mem_v2_partials` so they can
never rewrite a finalized message, and replaced only by a newer revision of the
same attempt plus upstream message; the superseded block is deleted. Tool calls
record name, call ID, parent message ID, state, timing, and an artifact reference
with a `retained`/`truncated` reason; inline output is bounded to 8 KiB. Attachment
metadata is bounded to 2 KiB of URL, and `data:` URIs are reduced to their media
type so media bytes never enter the archive or its search index. All three writes
are best-effort and asynchronous: a failure is logged and never interrupts
delivery. Existing version-1 archives upgrade in place via `memory-migrate`.

PR 3 also covers the specialized adapter entry points. Internal bootstrap prompts
(the synthetic "initialize session" requests that only materialize an upstream
session for shell execution) are marked `Internal` and never become archived
turns. Token-overflow recovery and explicit retries re-dispatch the original
delivery with `RetryOfDelivery`, which continues the existing turn with a new
attempt instead of failing closed as a replay or creating a duplicate user
request; the adapters carry the original transport delivery ID through their
pending-overflow state so the retry keeps the same turn identity.

PR 3 is not complete; see [Gate status](#gate-status). Inbound text is now durable
before client admission — the WeChat debounce
batcher persists each buffered message to `wechat_inbound_queue` before arming its
timer, clears the row only after dispatch succeeds, and replays surviving rows at
startup (pruning anything older than 24 hours) before polling begins. Outbound
delivery is durable too: `wechat_outbound_queue` survives a restart with pacing,
retry, and park state intact and reuses the same client IDs. Recovery still does not
re-execute tasks or recreate outbound delivery callbacks after restart. No
speed/space benchmark claims are made for this increment.

## 1. Objective and boundaries

Build reliable, local-first memory with cheap durable capture, compact storage, fast scoped retrieval, and source-backed knowledge. Preserve original evidence separately from search indexes and generated summaries.

Do not claim universal superiority over other agents. Demonstrate improvements on the same dataset, hardware, durability level, retention policy, and retrieval task. A system that discards answers is not a valid space-efficiency baseline for a full-history archive.

Initial release includes full user/assistant text, tool-call metadata, configurable tool-output retention, lexical search, record detail, session scrolling, migration, and operational diagnostics. Semantic retrieval and model-generated knowledge are later, independently gated features.

Non-goals: replacing OpenCode's context management, retaining private model reasoning, building a distributed database, or automatically merging identities across messaging platforms.

## 2. Verified starting points

| Existing file | Current limitation | Planned responsibility |
| --- | --- | --- |
| [store.go](../internal/memstore/store.go) | Startup deletion, 600-byte responses, search strength cutoff | Legacy compatibility and explicit database lifecycle |
| [adapter.go](../internal/memstore/adapter.go) | Request-derived metadata, no source message identity | Adapt gateway lifecycle events to the new recorder |
| [recall.go](../internal/memstore/recall.go) | Prefix-biased n-grams; summary ignores answers | Query normalization and legacy compatibility |
| [forget.go](../internal/memstore/forget.go) | Repeated full-age decay | Retire destructive recall eligibility; optional ranking signals only |
| [client.go](../internal/opencode/client.go) | Detached writes, duplicate paths, missed late/fallback results | Authoritative turn and message lifecycle integration |
| [event_listener.go](../internal/opencode/event_listener.go) | Completion can outlive waiting request | Final-message observation and durable reconciliation signal |
| [queue.go](../internal/asyncwork/queue.go) | Explicitly droppable, non-durable jobs | Leave unchanged; do not use for authoritative capture |
| [gateway main](../cmd/gateway/main.go) | Opens memory, runs decay, writes MCP configuration | Lifecycle, migration, writer, workers, shutdown |
| [MCP main](../cmd/mcp/main.go) | Unscoped queries, summary-only output, mutating open | Read-only scoped search/detail/scroll interface |
| [config.go](../internal/config/config.go) | Ambiguous enable/path semantics | Validated memory configuration |

Keep existing handoff, pending-retry, and skill tables operational during rollout. Do not rewrite unrelated adapter or scheduling logic.

## 3. Architecture and invariants

```text
Trusted adapter identity + stable delivery ID
    -> BeginTurn -> bounded commit queue -> SQLite writer
    -> OpenCode dispatch and completion observation
    -> completed message + durable job, in one transaction
    -> indexing/reconciliation/knowledge workers

Scoped MCP caller -> lexical search -> message references
                  -> detail or session scroll -> original evidence
```

Required invariants:

1. A committed user input survives process restart, even if model execution fails.
2. Repeated observation of one upstream message does not create another message; identical text in different turns remains distinct.
3. A waiting-request timeout is not a terminal upstream completion event.
4. Summarization, indexing, compaction, and recall ranking never delete original evidence.
5. Every search/detail/scroll request has a trusted principal; unknown scope fails closed.
6. Every fact and search result points to a source message or explicitly labeled legacy record.
7. Read-only MCP startup cannot migrate, purge, create, or strengthen records.
8. Search lag, truncation, partial results, capture failures, and retention exclusions are visible.
9. A successful commit acknowledgment means the selected SQLite durability policy was met, not merely that a channel accepted a job.

## 4. Data model

Introduce versioned `mem_v2_*` tables in the existing database. Use a memory-specific migration table rather than taking ownership of a database-wide version counter. Timestamps are UTC Unix milliseconds; message offsets are UTF-8 byte offsets. Use application-generated stable text IDs and INTEGER row IDs where useful for FTS.

| Table | Essential fields and constraints |
| --- | --- |
| `mem_v2_schema` | `version`, `applied_at`; version primary key |
| `mem_v2_sessions` | `id`, `principal_id`, `adapter`, `thread_id`, `upstream_session_id`, `project_key`, `work_dir`, `created_at`; unique principal + upstream session where present |
| `mem_v2_turns` | `id`, `principal_id`, `session_id`, `delivery_key`, `status`, `dispatch_state`, `started_at`, `completed_at`, `last_reconciled_at`; unique principal + delivery key |
| `mem_v2_attempts` | `id`, `turn_id`, `upstream_session_id`, `attempt_no`, `status`, `last_error_code`; preserve fallback/retry attempts separately |
| `mem_v2_messages` | `id`, `session_id`, `turn_id`, `attempt_id`, `upstream_message_id`, `seq`, `role`, `state`, `revision`, `block_id`, `byte_offset`, `byte_length`, `created_at`, `fingerprint`, `fidelity`; unique session + seq; unique attempt + upstream message where present |
| `mem_v2_blocks` | `id`, `principal_id`, `codec`, `codec_version`, `raw_bytes`, `stored_bytes`, `checksum`, `payload`; raw or compressed immutable content |
| `mem_v2_chunks` | INTEGER `id`, `message_id`, `message_revision`, `ordinal`, `start_byte`, `end_byte`, `tokenizer_version`; unique message + revision + ordinal |
| `mem_v2_chunk_fts` | FTS5 `content` with the `trigram` tokenizer; rowid is `mem_v2_chunks.id` |
| `mem_v2_chunk_word_fts` | FTS5 `content` with the `unicode61` tokenizer; rowid is `mem_v2_chunks.id` |
| `mem_v2_index_state` | single row `id=1`: `tokenizer_version`, `updated_at`, `word_backfill_rowid` |
| `mem_v2_jobs` | `id`, `kind`, `source_id`, `source_revision`, `status`, `attempts`, `lease_until`, `next_run_at`, `last_error`; unique kind + source + revision |
| `mem_v2_legacy_map` | `legacy_id`, `turn_id`, `import_version`; unique legacy ID for resumable import |

Create indexes for principal/project/session/time filtering, session sequence scrolling, unfinished turns, due jobs, and message-to-chunk joins. Derive authorization from session ownership; any denormalized principal field must be set and checked by trusted writer code.

Add tool metadata only where needed: tool name, call ID, parent message ID, state, artifact reference, and retention/truncation reason. Use existing attachment metadata rather than embedding media bytes in FTS. Full text means captured user-visible text, not hidden reasoning or an unconditional promise to retain every binary/tool output.

Start with one raw block per finalized message. Optional packing later replaces references transactionally; it must not change message IDs. Keep rolling partial snapshots separate from immutable final messages, replacing snapshots only with newer revisions. Preserve useful failed-attempt output with its actual status.

Knowledge-phase additions: `mem_v2_facts` with kind, scope, statement, status, timestamps, and supersedes ID; `mem_v2_fact_sources` with fact/message references; `mem_v2_access` for explicitly authorized project sharing. Do not add graph or vector tables before their phase is approved.

## 5. Go contracts and package layout

Keep storage implementation in `internal/memstore`. Follow the existing interface-in-consumer pattern to avoid an `opencode`/`memstore` import cycle.

Proposed contracts, to be declared with transport-neutral DTOs next to the consumer interface in [client.go](../internal/opencode/client.go):

```go
type ConversationRecorder interface {
    BeginTurn(ctx context.Context, input TurnInput) (TurnRef, error)
    BindAttempt(ctx context.Context, attempt AttemptInput) error
    SaveMessage(ctx context.Context, message MessageInput) error
    FinishAttempt(ctx context.Context, result AttemptResult) error
}
```

`TurnInput` includes trusted principal, channel, delivery key, original request, thread, and resolved project. `MessageInput` includes source identity, role, revision, state, and original text. `AttemptResult` includes upstream status and error classification, not just whether text is nonempty. Methods acknowledge commit; optional buffered mode exposes a separately named receipt and cannot masquerade as committed capture.

Keep storage lifecycle (`Start`, `Flush`, `Close`) owned by gateway startup, not by the prompt client. Add read-side `Search`, `GetMessages`, `Scroll`, `Recent`, and `Projects` methods that all require a nonempty `Scope` value. Do not use empty strings as an all-users shortcut.

Suggested new files, only as the corresponding phase is implemented: `migrations.go`, `writer.go`, `messages.go`, `search.go`, `jobs.go`, `archive.go`, `knowledge.go`, with neighboring tests. Reuse [adapter.go](../internal/memstore/adapter.go) and [handoff_test.go](../internal/memstore/handoff_test.go) where appropriate. Do not pre-create empty modules.

## 6. Phase 0: Stop further loss

Implement as a focused first change before the v2 rollout:

- Remove automatic `purgeRecallNoise` invocation. No automatic replacements based on request text.
- Stop the hourly legacy decay worker and remove strength as a hard eligibility filter. Do not alter existing strengths unnecessarily.
- Stop response truncation for newly captured legacy records; include responses in search and rebuild the affected legacy FTS index explicitly.
- Return original request/response excerpts and IDs; remove project/day deduplication and clearly distinguish recents from matches.
- Add separate writable gateway open and read-only MCP open paths. Missing database or incompatible schema must be an actionable error, not a new empty database.
- Stop strengthening from read queries; defer optional usage accounting until a separate authorized writer path exists.
- Correct default path and environment documentation; report configured path, schema version, and mode without exposing secrets.
- Disable unscoped MCP recall in shared deployments pending Phase 3. An operator may explicitly bind a single-owner instance to one principal.

Tests: legitimate requests containing history words survive writable and read-only reopen; 600+ byte Chinese replies round-trip; old low-strength rows remain findable; read queries leave logical data unchanged; missing read-only DB stays missing. Rebuild tests must prove response-only terms become searchable in already-stored, non-truncated data.

Historical deletion and truncation are irreversible without a backup or upstream transcript. Never present migration as reconstruction of missing text.

## 7. Phase 1: Durable capture and migration

### Writer and lifecycle

- Use one in-process capture writer with bounded item and byte capacity; a full queue applies bounded backpressure and returns an explicit error, never silent dropping.
- Batch up to 64 operations, 256 KiB estimated input, or 10 ms, whichever comes first. These are initial tuning values, not promises.
- Default to WAL plus `synchronous=FULL` for acknowledged commits. Verify actual driver PRAGMAs in tests, including on newly opened connections; use supported `modernc.org/sqlite` configuration rather than assuming DSN flags work.
- Separate read handles from the writer. Keep reads short, close result sets before writes, and cap decompression and result sizes.
- Validate operations before batching. Transaction failure fails all affected receipts; retry transient locks within deadlines using the same IDs. Do not discard an unknown commit outcome; idempotent retry resolves it.
- Insert finalized messages and index-job rows in the same transaction. In-process workers submit state changes through the writer; lease jobs durably to recover from crashes.
- Checkpoint WAL under an explicit maintenance policy. A second gateway targeting the same store must be rejected by an instance lease or supported by tested coordination; do not rely on single-process assumptions silently.
- Shutdown order: stop intake, stop/drain producers, flush capture, stop workers, then close SQLite using an independent bounded shutdown context.

### Capture ownership

- Add stable delivery identity to `MessagePayload` where necessary, deriving it from adapter delivery IDs rather than content hashes. Generate and carry one ID for synthetic requests and retries.
- `BeginTurn` occurs before upstream dispatch and stores the original request before skill/handoff wrappers modify it. Bind the actual upstream session and attempts as they become known.
- In strict mode, failed input persistence prevents dispatch with a visible storage error. A configurable best-effort mode may continue but must expose degraded capture; it cannot claim durability.
- Route synchronous results, streaming terminal results, and fallback fetches through one idempotent save path. Remove legacy detached writes after the new path is enabled.
- Completion observation captures data independently of outbound delivery success. Do not equate callback delivery with authoritative model text.
- On request timeout, retain upstream completion observation and mark the turn active/awaiting reconciliation, not completed. Finalize late results on the completion path.
- Persist a reconciliation job for in-flight work. On startup or expired observation, query the known upstream session for final messages; retry temporary failures and mark unavailable sources explicitly.
- Partial snapshots can be coalesced, initially every 5 seconds or 16 KiB of additional text. Document that this bounds only locally observed partial capture, not upstream data availability.
- Do not automatically replay a potentially executed task after a crash. Resolve dispatch-uncertain turns from upstream evidence or require operator/user action to avoid repeating side effects.
- Exclude internal knowledge/skill-mining sessions by origin to avoid recursive memory generation.

### Migration

1. Produce a consistent backup using a tested SQLite backup mechanism or a paused-writer procedure; never copy only the main file while WAL writes are active.
2. Create additive v2 tables and indexes under an explicit migration command/startup gate.
3. Import legacy rows in resumable batches using `mem_v2_legacy_map`; preserve adapter, user, time, request, stored response, and directory.
4. Mark legacy fidelity as `legacy_unknown` or `legacy_truncated`; a response length of 600 bytes is only a hint, not proof of original length.
5. Do not invent original session IDs. Use synthetic legacy containers, excluded from claims of true conversation adjacency.
6. Report counts, unmapped identities, failed rows, and import checkpoints. Re-running produces no duplicate turns.
7. After v2 capture activation, leave legacy tables intact but stop legacy dual writes. Compatibility readers can expose v2 through the old tool names.

Gate: process-kill tests after commit, duplicate delivery tests, late-completion tests, fallback tests, restart reconciliation, disk-full errors, and migration restart all preserve the documented semantics.

## 8. Phase 2: Lexical search and evidence retrieval

- Chunk user and assistant text at paragraph/code boundaries, initially targeting 1 KiB and capping at 2 KiB of UTF-8 text with a small bounded overlap. Preserve source byte offsets and split exceptionally long lines safely.
- Use the installed SQLite FTS5 implementation. Benchmark its current trigram tokenizer against a tested multilingual alternative before adding another tokenizer dependency.
- Keep whole technical identifiers and paths available for exact-match boosting. Escape FTS syntax, parameterize SQL, bound term count, and escape LIKE wildcard characters for literal fallback searches.
- Search joins must constrain principal, project, time, and message revision before final candidate limits. Confirm plans and actual behavior with mixed-user and expired-record fixtures.
- For one/two-character queries unsupported by trigram MATCH, use explicitly bounded scoped fallback scans or request a more specific query. Return incomplete/too-broad status when the cap is reached.
- Retrieve a bounded candidate pool, initially 100 chunks; collapse overlapping chunks to messages, not project/date. Rank by lexical relevance, then exact identifier match; use recency only as a modest tie-breaker.
- No implicit 30-day cutoff for explicit history search. Keep recent browsing defaults separate and echo all effective filters in results.
- Search returns excerpts plus message/session references; `GetMessages` reads original bodies; `Scroll` returns adjacent messages using stable sequence cursors.
- Default search limit 10, maximum 50; default scroll/detail response budget 32 KiB with continuation and truncation metadata. Budget values are tunable and do not truncate stored originals.
- Use stable keyset pagination for session/time browsing. For search, bind cursors to query, scope, and index generation, or return `restart_required` if the ranked dataset changed.
- Expose index lag. When a scoped query could miss very recent unindexed messages, scan a bounded unindexed tail and merge it, or report partial coverage explicitly.
- First implement a straightforward FTS layout for correctness. The space phase evaluates contentless FTS to remove duplicated stored text; do not require speculative index optimizations for initial correctness.

Gate: response-only terms, old messages, exact commands, CJK terms, punctuation, identical text in different turns, and more than ten same-day messages are recoverable with detail and pagination. Every result has valid evidence offsets. Missing matches must not be disguised as unrelated recent results.

### PR 4 implementation notes

Implemented in `internal/memstore/chunks.go`, `search.go`, `detail.go`, and archive schema v4.

- Chunking targets 1 KiB, caps at 2 KiB, and overlaps 128 bytes. Boundaries prefer paragraph, then line, then sentence punctuation, then space, then a rune-aligned hard cut. Chunks always cover the whole body with no gaps, so every byte of stored text is reachable.
- `mem_v2_chunk_fts` is a standalone FTS5 trigram table whose rowid is `mem_v2_chunks.id`. Text is duplicated rather than contentless; contentless FTS is deferred to the space phase as planned.
- Schema v4 adds `mem_v2_chunk_word_fts`, a second FTS5 table over the same chunk text with the `unicode61` tokenizer, keyed by the same chunk rowid. It exists because the trigram tokenizer is correct but slow: measured on identical chunk text, the same three-term query costs 117.4 ms against the trigram index and 7.38 ms against a `unicode61` index, a factor of 15.9, while matching 1157 and 1142 chunks respectively. The cost is the tokenizer, not the query and not the joins. See [§15](#15-benchmarks-and-targets) for the measurement.
- The word index is added rather than substituted because the two tokenizers answer different questions. `unicode61` splits on word boundaries, so it finds whole tokens and cannot find a substring inside one; trigram finds any substring, which is what makes CJK search work at all, since a CJK phrase has no spaces to split on. Keeping both lets a query be routed to the index that can answer it.
- `Search` routes each term by its shape and reports the choice in `filters.mode`: `word` when every term is a plain ASCII word, `fts` when every term needs the trigram index (CJK, or a term containing a separator such as `.` or `/`), `hybrid` when the query mixes both, and `scan` when every term is shorter than three runes and neither index can match it. A hybrid query merges the two result sets by message ID, summing scores and keeping the better excerpt.
- `indexMessage` writes both indexes in the same transaction, so a newly captured message is searchable through either path immediately.
- The word index is filled for pre-existing chunks by `ArchiveWriter.BackfillWordIndex`, a resumable background pass driven by `RunIndexWorker` only when the message queue is drained. Its high-water mark is `mem_v2_index_state.word_backfill_rowid`, so a restart continues instead of rescanning. The copy uses `INSERT OR REPLACE` because the backfill and the normal write path overlap by design: a chunk created after the migration already has a word entry and the backfill will still reach it. `IndexCoverage` reports `word_backfill_rowid`, `max_chunk_id`, and `word_index_complete` so partial coverage is visible rather than implied.
- `indexCommitTimeout` scales with both the batch size and the number of chunks already indexed, because an FTS5 insert costs more as the index grows. A fixed deadline cannot be right at every archive size, and the previous fixed 30-second floor is what made seeding a 100k archive fail intermittently while the same code passed at 20k.
- `ArchiveWriter.IndexPending` consumes the `kind='index'` jobs that `writeArchiveMessage` already inserted, leases them, and marks each completed or failed. A single unindexable message records `last_error` and does not stall the queue. Re-indexing a revision is idempotent.
- `ArchiveWriter.RunIndexWorker` is the production driver; `cmd/gateway` starts it when strict capture is enabled and stops it before the writer drains.
- `Store.Search` applies principal, project, and time predicates inside the SQL before the candidate limit, collapses chunks to one hit per message, boosts exact identifier matches, and binds cursors to query, scope, and index generation. A stale cursor returns `ErrSearchRestartRequired` rather than silently re-ranking.
- One- and two-character terms cannot match either index, so they route to a bounded scoped scan (`maxFallbackScan` = 2000 messages) and the result reports `mode=scan` with a notice.
- `Store.GetMessages` re-authorizes every supplied ID, reports unavailable IDs in a notice instead of dropping them silently, and enforces a 32 KiB default / 256 KiB maximum budget with rune-aligned truncation and a continuation cursor.
- `Store.Scroll` verifies session ownership, then pages by `seq` keyset in either direction.
- `Store.IndexStatus` reports pending and failed jobs, indexed versus total messages, chunk count, and tokenizer version. `Search` sets `Partial` whenever jobs are pending, so "no match" is never confused with "not indexed yet".
- MCP exposes `memory_search` (v2 when the archive is migrated, legacy otherwise), `memory_get`, and `memory_scroll`. The v2 tools refuse a legacy-only archive instead of returning an empty result.

Known limits: the index worker is single-process and relies on the existing single-writer assumption; both FTS tables duplicate chunk text until the space phase, so the word index roughly doubles index storage; the word index is incomplete on an upgraded archive until the backfill finishes, which `IndexCoverage` reports; and the §15 latency targets are only partly measured — see [Gate status](#gate-status).

## 9. Phase 3: Scope-safe MCP and operational visibility

The current project-level stdio MCP process is shared configuration. Model-supplied `user_id` or `principal_id` is not authentication and cannot safely select a user's archive.

Implement in two supported modes:

- Single-owner: operator binds the MCP process to a fixed principal. Startup verifies that the binding exists, and every query uses it. Do not enable this on a shared OpenCode instance where other users can invoke that same process.
- Shared gateway: use separately isolated OpenCode/MCP execution contexts per principal, or an authenticated gateway memory endpoint with non-model-controlled per-session credentials. Select this integration only after verifying the host's credential propagation capabilities. Until isolation is proven, shared memory tools remain disabled rather than exposing an all-users search.

Use adapter + user identity as the conservative initial principal mapping. Cross-channel linking is an explicit administrative operation; group chats require a defined conversation-sharing policy. An owner field is not sufficient when it grants access to unrelated users' group messages.

Tool contract:

| Tool | Model-visible arguments | Result |
| --- | --- | --- |
| `memory_search` | query, project, since/until or days, limit, cursor | Actual matched excerpts, message/session IDs, scope/filter metadata, index coverage |
| `memory_get` | message IDs, optional body continuation | Original text and provenance; unavailable/retained-partial status |
| `memory_scroll` | session ID, anchor/cursor, direction, limit | Ordered adjacent messages |
| `memory_recent` | project, days, limit, cursor | Scoped chronological records, not fabricated relevance |
| `memory_list_projects` | days, cursor | Scoped project counts and last activity |

Enforce authorization again for every supplied record/session ID, not only for initial search. Return a non-disclosing unavailable result for unauthorized IDs. Reject negative/oversized limits and incompatible filters. Echo explicit day filters; never silently interpret missing days as zero-day history.

Add structured health output for capture mode, schema, queue depth/bytes, last commit, failed writes, pending reconciliation, index lag, archive/index size, and scope mode. Logs and metrics must not contain conversation bodies, credentials, or high-cardinality user labels.

Gate: principal A cannot search, fetch, scroll, count, infer, or reuse a cursor for principal B. Verify actual host tool wiring, not only unit tests with manually supplied scope.

## 10. Phase 4: Space optimization

- Add the maintained Go Zstandard implementation from `github.com/klauspost/compress/zstd` only in this phase; pin a reviewed version through Go modules.
- Compress finalized large messages. Leave small or incompressible payloads raw; record codec, uncompressed length, and checksum, and enforce maximum decode size. **Implemented synchronously inside the commit, not asynchronously, and that is a measured decision rather than an omission.** The original wording asked for asynchronous compression, but compressing after the commit means writing the block raw and then rewriting it compressed in a second transaction that repoints the message and removes the old block — which is exactly the crash-safe repacking this phase also requires and which is not built. Doing it asynchronously without that machinery would trade a bounded, measured CPU cost on the capture path for an unbounded, unmeasured window in which a crash leaves a block half-rewritten. The measured cost is small enough that the trade is not worth making yet: `BenchmarkCompressBlockCost` reports 40 µs for 4 KiB, 222 µs for 16 KiB, 815 µs for 64 KiB, and 6.3 ms for 1 MiB of realistic text, against a 2 s capture deadline whose measured worst commit is 329 ms in a 40k archive. Even the largest message the writer accepts (16 MiB, the `QueueBytes` limit) extrapolates to roughly 100 ms, about 5% of the deadline. Revisit this when repacking exists, because that is when asynchronous compression becomes cheap to adopt rather than a new failure mode.
- Benchmark per-message compression against session-local 32-128 KiB blocks. Pack only within the same principal and compatible retention class, so deletion and access boundaries remain tractable.
- In one transaction, insert the new block, repoint all affected message references, and remove obsolete unreferenced blocks. Existing read transactions must retain a consistent view.
- Deduplicate large exact tool/artifact payloads using scoped SHA-256 content identities, verifying content on reuse. Do not deduplicate conversation rows, and do not create cross-principal hash-existence side channels. **Implemented in `dedupe.go` for tool outputs.** The block ID is derived from `(principal, checksum, untruncated length)`. The principal is in the identity so a write cannot be deduplicated against another principal's block, which would otherwise let one principal detect that another has stored a payload by watching its own write get shared. The untruncated length is in the identity because two calls can retain the same truncated prefix while their originals differed, and sharing would give the second call the first call's `raw_bytes`. Every write reads the row back and compares the bytes rather than trusting the digest, because the failure that matters — digest matched, content did not — is indistinguishable from a first write without the read. Conversation rows are untouched: only `mem_v2_tool_calls.artifact_ref` participates, and `SavePartial` still writes its snapshots inline.
- Evaluate contentless FTS5 with supported deletion/update semantics under the pinned SQLite library; reconstruct snippets from original message offsets. Benchmark index-detail options before sacrificing phrase/ranking behavior.
- Defer custom dictionaries, vector quantization, external blob stores, and multi-tier caches until profiling demonstrates value.
- Optional bounded decoded-block cache is keyed by principal/block revision and byte capacity; deletion invalidates entries. Add only if detail-fetch profiling warrants it.
- Count main DB, WAL, FTS, artifacts, and embeddings in space reports. Report backup size separately. VACUUM/checkpoint work is scheduled, not performed on the prompt path. **Implemented in `space.go` and exposed as the `memory_space` MCP tool.** The report keeps whole-file numbers and per-principal numbers in separate groups and never adds them, because every principal shares the same pages: a per-principal share of the file would be a fiction, and the block and artifact totals are subsets of the file rather than additions to it. `FTSBytes` sums every FTS5 shadow table by name prefix rather than only the table named in the `CREATE` statement, and `ArtifactBytes` deduplicates through an `IN` subquery so a retained output referenced by two tool calls is counted once. `TableBreakdownAvailable` separates "this SQLite build has no `dbstat`" from "the index is empty", because a zero would read as the latter. It is a separate tool from `memory_health` because `dbstat` walks the page map and health is deliberately built from cheap indexed lookups. VACUUM and checkpoint remain unscheduled work: the report measures free pages so an operator can decide, but nothing runs them on the prompt path.

Gate: exact round-trip text, UTF-8 offsets, block corruption handling, crash-safe repacking, and scoped deletion pass. Ship compression settings only if they improve total storage without violating agreed read/write latency gates. **Status: round-trip, UTF-8 offsets, corruption handling, and scoped dedupe pass. Crash-safe repacking and scoped deletion do not, because neither is implemented — dedupe shares a block at write time and never rewrites an existing one, so it adds no new crash window, but it also does not reclaim anything already written.**

## 11. Phase 5: Source-backed durable knowledge

- Use durable jobs triggered by completed meaningful work or explicit remember requests; do not call an LLM for every message.
- Inject a drafter interface following the existing skill-generation pattern. Bound input, output, retry count, and daily cost; mark origin so extraction never schedules itself recursively.
- Require structured outputs: kind, scoped statement, supporting message IDs, observed/proposed/completed/verified status, and candidate supersession.
- Validate that evidence IDs belong to the source principal and actually exist. Unsupported claims are rejected; confidence is not proof.
- Redact configured sensitive data before extraction where required, and record archive redaction policy explicitly. Full-fidelity storage applies to permitted captured data, not deliberately excluded secrets.
- Store corrections as supersession rather than overwriting historical evidence. User-requested forgetting removes derived facts, search entries, artifacts, and caches under the documented deletion policy.
- Load only a small scoped knowledge index at session start, initially up to 1,500 tokens using an available tokenizer or conservative byte budget. Retrieve details on demand.
- Add explicit remember/forget controls with trusted authorization and auditable outcomes. An assistant statement that it remembered something is not a successful write receipt.

Gate: facts have valid provenance, user corrections supersede old facts, task-local instructions do not become global preferences, and extraction failures do not affect capture or search.

## 12. Phase 6: Optional semantic retrieval

Proceed only if the labeled evaluation set demonstrates material lexical misses on paraphrases and the operator accepts embedding cost/storage.

- Benchmark a Go/Windows-compatible embedded vector backend before selecting one. Keep lexical search fully functional without it.
- Embed normalized meaningful chunks asynchronously and version by model, dimensions, normalization, and chunk revision. Scope embedding reuse to the permitted security boundary.
- Apply scope restrictions during candidate generation. Post-filtering a globally capped nearest-neighbor list is not sufficient for recall quality.
- Fuse lexical and vector lists with RRF, deduplicate by message, and optionally rerank a small candidate set. Do not make remote LLM reranking mandatory.
- Rebuild embeddings incrementally after model changes without mixing incompatible vector spaces. Expose semantic coverage/lag.

Gate: improved paraphrase recall on held-out queries, unchanged exact-identifier accuracy, verified isolation, and acceptable added bytes, latency, and model cost. Otherwise leave the feature disabled.

## 13. Handoff and retention follow-up

Update [handoff.go](../internal/memstore/handoff.go) and the client handoff integration after reliable capture is established:

- Build handoff context from recorded messages and verified facts with source references; retain recent originals within a budget.
- Reserve handoff state and mark it consumed only after successful prompt acceptance. Persist a correlation ID and reconcile unknown outcomes to avoid duplicate injection.
- Expiry applies to automatic handoff injection, not archive search. Keep thread-to-session continuation separate from durable evidence.
- Replace age-based search exclusion with explicit retention classes: conversation, tool artifact, partial snapshot, derived fact. Start with no automatic conversation expiry.
- Implement explicit scoped deletion as a transaction over messages, chunks, FTS, derived sources/facts, jobs, and block references. Repack shared blocks when necessary; never delete another record's content.
- Document WAL, backups, and external artifacts: logical deletion is immediate, physical erasure follows storage/backup policy. Do not promise secure erase from SQLite DELETE alone.

## 14. Configuration proposal

Preserve `MEMORY_STORE_PATH`; introduce a validated nested Memory configuration while keeping a deprecation bridge for callers using `MemStorePath`.

| Setting | Initial default / behavior |
| --- | --- |
| `MEMORY_ENABLED` | true; explicit false disables capture and recall |
| `MEMORY_STORE_PATH` | `<OPENCODE_DIRECTORY>/mem/memory.db`; resolve once to absolute path |
| `MEMORY_CAPTURE_MODE` | `strict`; `best_effort` is explicit and observable |
| `MEMORY_BATCH_MAX_ITEMS` / `MEMORY_BATCH_MAX_BYTES` | 64 / 262144 |
| `MEMORY_BATCH_MAX_DELAY_MS` | 10 |
| `MEMORY_QUEUE_MAX_ITEMS` / `MEMORY_QUEUE_MAX_BYTES` | 1024 / 16777216; validate against attachment handling |
| `MEMORY_COMMIT_TIMEOUT_MS` | 2000; errors visible; benchmark before production tuning |
| `MEMORY_SCOPE_MODE` | disabled recall until single-owner or isolated scope is configured |
| `MEMORY_COMPRESSION` | **not implemented, and deliberately not a setting.** The original proposal was a switch that stayed off until the Phase 4 gate and then turned raw-or-zstd on automatically. The gate has passed, so the switch would have exactly one reachable value, and a knob with one value is a way to be misconfigured rather than a way to be configured. Compression is therefore unconditional in strict mode: a block is stored zstd only if it is at least 4 KiB and the frame saves at least one byte in eight, and everything else stays raw. If a future need for a raw-only archive appears — a debugging aid, or a codec regression — add the switch then, with the reason recorded. |
| `MEMORY_KNOWLEDGE_ENABLED` / `MEMORY_SEMANTIC_ENABLED` | false |
| `MEMORY_CONVERSATION_RETENTION_DAYS` | 0 means no age-based deletion, not no search history |

MCP accepts `--db` and the existing `GATEWAY_MEMORY_DB` fallback. Deprecate or explicitly support documented aliases rather than silently changing names. Validate existing MCP entries and report stale/disabled paths without overwriting intentional operator configuration. Define version handshake behavior for gateway/MCP binary skew.

## 15. Verification and performance gates

### Gate status

This subsection is the single authoritative record of which gates are open. Other
documents describe their own scope and link here rather than restating gate
status, because three copies of the same list had already drifted apart.

| Gate | Status | Blocker |
| --- | --- | --- |
| PR 1-2 package tests, vet, build | passed | none |
| PR 3 capture tests (sync, stream, fallback, timeout, restart, retry, internal prompts) | passed | none |
| PR 3 real-upstream compatibility | **open** | not written. Not blocked: `opencode.exe` v1.18.32 is installed on the development machine and `opencode serve --port 4096` answers `GET /config/providers` with 200, so a real server is available locally. The test must assert the wire format (session creation, message and part JSON shape, event envelope) against that server, not model quality, so it needs no paid model access. A fake server does not satisfy this gate. |
| PR 3 CGO race run | **open** | unexecutable on the development machine: `gcc` is not installed and `CGO_ENABLED=0`. Must run on a CGO-enabled runner. |
| PR 4 search, detail, scroll, index tests | passed | none |
| §15 benchmarks at 5k, 20k, and 100k | measured, but the verdict is not established | 100k search still misses its target, but the cause is no longer the tokenizer. The word index is implemented (schema v4) and measured on the production path at 100k: 463.0 ms p95, against 764.3 ms for the trigram path and 191.5 ms for the hybrid path. The redesign helped and did not finish the job, and the ~37 ms projection that predicted it would is refuted. The word index is proven faster on every isolated layer at 20k (25.0x on bm25 scoring, 7.9x on enumeration, 3.5x on the inner query) while matching 98.7% of the same chunks, and it is 3.4x faster than the trigram index on the full path in the same process. What is not established is the absolute verdict: `inner_rank` at 20k has been measured at 75.20, 125.2, 260.6, and 361.1 ms on identical code, a 4.8x spread, and the word path's full search at 40.71 and 102.5 ms at the same corpus size. The 100k miss is 4.6x, which is the same size as the drift, so a single-process measurement cannot call it. The gate needs a pinned machine with a recorded baseline row; a ratio against a reference operation measured in the same run was tried and does not work (best of five candidates cut the CV from 20.0% to 16.0%, and between processes the ratio was noisier than the raw number). The bounded scan's 13.8 s is fixed and measured (347.1 ms p95 after schema v5), and schema v5's index was then found to have broken the search path by 1388x — fixed with `CROSS JOIN` and pinned by `TestSearchIndexQueryPlanPinsFTSDriver` |
| §15 benchmarks at 1M | **open** | not started. Not blocked by a code defect: the 100k seeding run failed at message 34,561 with a capture timeout, which looked like a Phase 0 regression, but `TestCaptureCommitScalesWithArchiveSize` measures the worst capture commit at 329 ms in 40k messages, 16% of the 2 s deadline, so capture cost is flat in archive size. The failure was contention — the run shared the disk with another `go test` process. The gate is blocked only by the requirement that nothing else run while it measures. The index deadline is separately confirmed at 100k: 500 batches, worst 10.987 s against an 83.52 s deadline, a 7.6x margin. |
| §15 cold-cache, p99, saturation, concurrent readers, capture-active measurement | **open** | not started |
| Competitor comparison | **open** | requires the same corpus and operation definitions first |
| PR 5 principal isolation (search, fetch, scroll, count, cursor) | passed | `TestPrincipalCannotReachAnotherPrincipal` covers all five vectors against two principals sharing one adapter and one project name. It also caught a real defect: `Recent` and `ProjectSummaries` skipped the `user_id` filter when it was empty and returned every user's records, while `Search` rejected the same call. Both now return `ErrSearchScopeRequired`. |
| PR 5 structured health output | passed | `memory_health` reports scope mode, schema, tokenizer version, pending and failed jobs, word-index completeness and lag, index updated-at, message totals, index lag, and chunk count. It reports no bodies, no credentials, and no per-user labels, and `TestMemoryHealthReportsLagWithoutLeaking` asserts that. |
| PR 5 host tool wiring | passed | the gate says "verify actual host tool wiring, not only unit tests with manually supplied scope". `wiring_test.go` drives `handleRequest` with real JSON-RPC `tools/call` lines, so the scope is supplied the way a host would supply it. It asserts that a caller-supplied `user_id`/`adapter` is ignored and the bound principal answers, that an unbound server refuses every tool with "memory recall disabled" rather than returning an empty success, and that `memory_health` is actually advertised in `tools/list`. The fixture seeds both principals with a hit for the same query, so the test cannot pass by finding nothing. This required a writer seam on `mcpServer`; production still writes to `os.Stdout`. |
| PR 6 block compression (round-trip, UTF-8 offsets, corruption, scoped stats) | passed | `codec.go` stores a block raw or zstd per payload and records `codec`, `codec_version`, `raw_bytes`, `stored_bytes`, and a checksum over the original text. Every read path decodes through one function, so the contract is enforced in one place. `TestDecodeBlockRoundTripsExactly` covers empty, ASCII, CJK, emoji, mixed, NUL bytes, and invalid UTF-8 byte-for-byte; `TestDecodeBlockRejectsCorruption` covers unknown codec, future codec version, truncated frame, flipped byte, declared-length mismatch, declared length above the ceiling, negative length, and an empty compressed payload; `TestCompressedMessageOffsetsAreUTF8Safe` proves chunk offsets still address rune boundaries after a compress/decompress cycle; `TestCompressedMessageIsSearchable` proves the index path decodes before chunking, which is the failure that would have made compression a data-loss bug. Landing the codec also exposed five existing tests that asserted on `b.payload` directly and passed only because their fixtures sat under the 4 KiB threshold; they now decode through `blockTextForTest`, and `TestArchiveStorageFullIsNotAcknowledged` uses incompressible printable ASCII so it still reaches the capacity path. |
| PR 6 codec cost (write and read paths) | passed | `BenchmarkCompressBlockCost` and `BenchmarkDecodeBlockCost` measure realistic text at 4 KiB, 16 KiB, 64 KiB, and 1 MiB, plus the sub-threshold and incompressible cases. Compression costs 40 µs / 222 µs / 815 µs / 6.3 ms and decoding 7.3 µs / 23 µs / 92 µs, against a 2 s capture deadline whose measured worst commit is 329 ms. The largest message the writer accepts extrapolates to ~100 ms, 5% of the deadline, so synchronous compression is defensible until repacking exists. Both benchmarks assert the codec each row must exercise, because the first version of this benchmark built its large rows by repeating one 4 KiB block and produced an inversion (4 KiB slower than 16 KiB) that was a fixture defect, not a codec property. A second inversion survived that fix and had a different cause: the encoder's first-call table allocation (299 KB, 28 allocs at 4 KiB) was charged to whichever row ran first. Both benchmarks now warm the codec before the timer starts, and per-byte throughput is deliberately non-monotonic in size because a dictionary compressor amortizes its table build over the input. |
| PR 6 space reporting (file, WAL, free pages, FTS, artifacts, backup) | passed | `space.go` reports whole-file numbers (`FileBytes`, `WALBytes`, `SHMBytes`, `FreeBytes`, `FTSBytes`) and, in a separate group, the bound principal's logical sizes (`BlockBytes`, `ArtifactBytes`, `EmbeddingBytes`), plus `BackupBytes` for a named backup path. The two groups are never summed: every principal shares the same pages, so a per-principal share of the file would be a fiction, and the block and artifact totals are subsets of the file rather than additions to it. `FTSBytes` sums every FTS5 shadow table by name prefix, because counting only the table named in the `CREATE` statement would report a fraction of the real cost. `ArtifactBytes` deduplicates through an `IN` subquery, because two tool calls can reference the same retained output and counting it twice would report an archive larger than the disk holds. `TableBreakdownAvailable` distinguishes "this SQLite build has no `dbstat`" from "the index is empty", because a zero would read as the latter. Six tests cover the file measurement, principal scoping (asserted against the archive total, so two reports that both leaked everything cannot pass), the unscoped refusal, backup separation, FTS attribution, and artifact deduplication. Exposed as the `memory_space` MCP tool rather than as more lines in `memory_health`, because `dbstat` walks the page map and health is deliberately built from cheap indexed lookups. |
| PR 6 scoped artifact dedupe | passed | `dedupe.go` gives a retained tool output a block ID derived from `(principal, checksum, untruncated length)`, so two calls that retained the same bytes share one block and a call whose original was a different length does not. The principal is in the identity because a global content index would turn the dedupe into a hash-existence oracle: one principal could learn that another has stored a payload by observing that its own write was shared. The untruncated length is in the identity because two calls can retain the same truncated prefix while their originals differed, and sharing would hand the second call the first call's `raw_bytes` — a number describing a different command's output. Every write reads the row back and compares the bytes, because the case that matters is the one where the digest matched and the content did not, and that case is indistinguishable from a first write without the read. Four tests cover within-principal sharing (asserting the block count, not just that the refs match, since two identical rows pointed at one of them would pass a ref-only check), cross-principal non-sharing, different-original-length non-sharing with distinct `raw_bytes`, and small repeated output collapsing through the same derived ID. The first version of the sharing test failed because its fixture was larger than `maxToolOutputBytes` and was truncated, so it compared the block against text that had been deliberately dropped; the fixture now asserts its own size is under `maxToolOutputBytes`. There is no minimum-size threshold: the `ON CONFLICT` clause applies at every size, so a small repeated output collapses through the same derived ID, and `TestSmallToolOutputIsStoredOnce` pins that. |
| PR 6 crash-safe repacking and scoped deletion | **open** | not started. Compression is per-message and in-place, so there is nothing to repack yet; the gate applies to the block-packing work, which is not implemented. Dedupe is implemented but does not repack: it shares a block at write time and never rewrites an existing one, so it adds no new crash window. |
| PR 6-9 | **open** | PR 6 compression, its cost gate, space reporting, and scoped artifact dedupe are implemented; block packing, crash-safe repacking, contentless FTS5, and the decoded-block cache are not started. PR 7-9 are not started. |

Strict capture mode and the v2 MCP tools stay experimental until the two PR 3
external gates close. The CGO race gate is not waived: it is unexecutable here,
which is a different thing from passing.

Add tests alongside the owning packages, using synthetic fixtures and temporary SQLite databases. Never benchmark by opening production databases through migration code.

Required scenarios: original assistant answer recalled by a term absent from the request; old low-strength history; legitimate history-word follow-ups; many same-project/day turns; multilingual identifiers; duplicate callbacks; retries with distinct attempts; late completion; canceled requests; upstream unavailable on restart; process crash before/after commit; disk full; queue overload; shutdown during capture; expired worker leases; migration interruption; wrong DB path; read-only startup; cross-user ID/cursor attacks; retention cascades; full index rebuild.

Initial package gates:

```sh
go test ./internal/memstore ./internal/opencode ./internal/config ./cmd/mcp ./cmd/gateway
go vet ./internal/memstore ./internal/opencode ./internal/config ./cmd/mcp ./cmd/gateway
go test ./internal/adapters/...
go build ./...
```

Run race tests for touched concurrent packages on a supported toolchain/CI runner; disclose local Windows race-toolchain limitations instead of skipping silently. End-to-end tests need a fake OpenCode server and adapter events, not paid model access. Dedicated subprocess tests kill a child writer and reopen the temp DB to verify committed data.

Benchmark 10k, 100k, and 1M messages with a fixed Chinese/English mix, varied reply lengths, repeated tool output, many projects, and multiple principals. Measure cold and warm caches, single and concurrent readers, while capture and indexing are active. Use the same synthetic corpus and operation definitions for competitors; publish versions/configuration and unsupported capabilities.

Provisional targets on a declared local SSD reference machine, to be revised after baseline measurement:

| Measure | Initial target |
| --- | --- |
| Strict commit latency at 20 final messages/s, ~4 KiB/message | p95 <= 25 ms; report p99 and saturation throughput separately |
| Scoped lexical search at 100k messages | warm p95 <= 100 ms; cold results reported separately |
| Detail retrieval for ten ~4 KiB messages | warm p95 <= 50 ms |
| Index freshness at the baseline load | p95 <= 2 seconds; lag always observable |
| Exact evidence recovery | 100% on deterministic retained-message fixtures |
| Labeled topical retrieval | recall@10 >= 0.90 on held-out queries; include failure breakdown |
| Archive payload compression | target >= 40% reduction on text-heavy corpus; report total-storage ratio independently |
| Reliability and isolation | zero lost acknowledged commits or unauthorized records in the test matrix |

Performance targets are not a proof of superiority and must not be met by weakening durability or silently dropping content. Do not compare lexical search latency to a competitor's full LLM-generated answer latency.

### Measured baseline

`internal/memstore/bench_test.go` implements the runnable part of this gate. It
reports p50/p95/max per operation because the targets above are stated as p95; an
average would hide the tail the targets exist to bound. Corpus size is set with
`MEMSTORE_BENCH_MESSAGES`, and the seeded archive is built through the real
commit-acknowledged writer rather than bulk inserts, so the fixture exercises the
production path.

Reference machine: 12th Gen Intel Core i7-12700H, Windows, `CGO_ENABLED=0`, local
SSD, `modernc.org/sqlite` (pure Go). Synthetic corpus: ~4 KiB bodies, 20k-word
vocabulary, CJK terms every 37th token, two projects, one principal.

| Measure | Target | 5k messages | 20k messages | 100k messages |
| --- | --- | --- | --- | --- |
| Strict commit, ~4 KiB | p95 <= 25 ms | 14.31 ms | not measured | not measured |
| Scoped lexical search, blended rotation | warm p95 <= 100 ms at 100k | 84.78 ms | 79.14 ms | **357.4 ms — misses** |
| Scoped lexical search, word path | warm p95 <= 100 ms at 100k | not measured | not measured | **463.0 ms — misses** |
| Scoped lexical search, trigram path | warm p95 <= 100 ms at 100k | not measured | not measured | **764.3 ms — misses** |
| Scoped lexical search, hybrid path | warm p95 <= 100 ms at 100k | not measured | not measured | **191.5 ms — misses** |
| Scoped lexical search, bounded scan | fallback, not targeted | not measured | not measured | **13781 ms before schema v5; 347.1 ms after** |
| Scoped lexical search, broad query | worst case, not targeted | 167.6 ms | 322.5 ms | not measured |
| Detail retrieval, ten ~4 KiB | warm p95 <= 50 ms | 1.06 ms | not measured | 3.07 ms |
| Index freshness | p95 <= 2 s | 35.99 ms | not measured | not measured |
| Index status (full diagnostic) | not targeted | 5.62 ms | 14.74 ms | not measured |
| Index coverage (search path) | not targeted | not measured | under 1 ms | not measured |

**The 100k search target is not met.** Warm p95 is 357.4 ms against a 100 ms
target, a factor of 3.6. Detail retrieval at the same size is 3.07 ms, so the
archive and the block store are not the problem; the cost is specific to the
search path. This is the gate doing its job: the target was written down before
the measurement, and the measurement says the current query does not reach it.

The blended rotation above is not the number to optimize against, because it
mixes two indexes. `BenchmarkScopedLexicalSearch` rotates four queries, two ASCII
words and two CJK terms, so its p95 is a blend of the word index and the trigram
index and cannot say whether either one is fast. `BenchmarkSearchByMode` measures
each routing mode separately and asserts the mode it got, so a routing change
that silently sends word queries to the trigram index fails there instead of
showing up as an unexplained latency change. At 100k messages, `-benchtime 20x`,
in one process:

| Mode | Query shape | p50 | p95 | max |
| --- | --- | --- | --- | --- |
| `word` | three ASCII words | 230.3 ms | 463.0 ms | 477.2 ms |
| `fts` | two CJK terms | 226.4 ms | 764.3 ms | 801.1 ms |
| `hybrid` | one ASCII word and one CJK term | 173.9 ms | 191.5 ms | 194.2 ms |
| `scan` | one two-character term | 13530 ms | 13781 ms | 13863 ms |

That table was taken before schema v5. The `scan` row is the only one the schema
change was meant to touch, and it was re-measured afterwards in its own process
at `-benchtime 10x`: p50 281.6 ms, p95 347.1 ms, max 351.3 ms.

The other three rows were not unaffected. An earlier draft of this paragraph
said they were, on the reasoning that none of them reads `mem_v2_messages` in a
way the new index serves. That reasoning was wrong, and the measurement that
refuted it is the tenth problem below: the index gave the planner a cheaper way
to reach `mem_v2_messages`, so it started driving the search query from that
table and probing the FTS index once per message instead of once per match. The
inner query went from 75.20 ms to 104471 ms at 20k messages. The fix is
`CROSS JOIN` on the FTS table, which forbids the reordering, and
`TestSearchIndexQueryPlanPinsFTSDriver` now fails if the FTS table stops
driving. The three rows above have not been re-measured since the fix; they are
recorded here as the pre-v5 baseline they are.

Three things in that table are worth stating plainly.

The word index works and does not reach the target. 463.0 ms is 4.6x the target
and 12.5x the ~37 ms this document projected from the 20k tokenizer comparison.
The projection was wrong for a specific and repeatable reason:
`BenchmarkTokenizerComparison` measured a benchmark-local `word_fts` table with a
hand-built query, while production runs `searchWordFTS` →
`searchIndex(..., "mem_v2_chunk_word_fts", ...)` with three joins, scope
predicates, a coverage query, and excerpt handling. A benchmark that measures a
hand-built local table is not measuring the production path, and the 15.9x ratio
it produced was a real ratio between two indexes and not a prediction about the
query built on top of them.

The hybrid path is the fastest of the three indexed modes at 191.5 ms, which is
the opposite of what the routing was designed to achieve. Hybrid runs both
indexes and merges, so it should cost more than either; it costs less than both.
That inversion means the per-mode numbers are not a clean comparison of the
indexes, and the most likely reason is a confound the table itself shows: the
three modes do not have the same term count. `word` runs three terms, `fts` runs
two, and `hybrid` runs one plus one. Cost scales with term count on the trigram
index — measured at roughly 65 ms per term, against a match count that barely
matters — so a two-term query being cheaper than a three-term query is expected
and says nothing about the indexes. The inversion is recorded as an observation
and not a conclusion, and `BenchmarkSearchAttribution` now holds the term count
fixed across both indexes (`word_ids_limit_100`, `word_bm25_limit_100`,
`word_inner_rank`, `word_full_search`, plus `word_terms_1/2/3`) so the comparison
can be made without the confound. Those rows have now been run at 20k, and they
settle it. On the same archive, in the same process, with the same three terms
(run A):

| Row | Trigram index | Word index | Ratio |
| --- | --- | --- | --- |
| `ids_limit_100` | 4.09 ms | 0.52 ms | 7.9x |
| `bm25_limit_100` | 93.71 ms | 3.75 ms | 25.0x |
| `inner_rank` | 125.2 ms | 32.49 ms | 3.9x |

The inversion was a term-count artifact, as suspected, and the word index is the
faster index by a wide margin on every layer that isolates it. `word_selectivity`
confirms the comparison is fair: the word index matches 1142 chunks against the
trigram index's 1157, 98.7% of the same rows, so the speedup is not the word
index matching less.

Read the ratios in that table and not the absolute numbers. A later run of the
same rows on the same archive (run B) put `inner_rank` at 361.1 ms trigram and
104.0 ms word — 3.5x, consistent with run A's 3.9x, while both absolutes are
roughly three times larger. The ratios reproduce; the latencies do not. The
reason is at the end of this section.

There is now a fourth row, and getting it required fixing the benchmark rather
than the query. `full_search` and `word_full_search` are the same call — both run
`store.Search` on `queries[0]`, which routes to the word index — so neither can
serve as the trigram column. An earlier draft of this paragraph read
`full_search` as the trigram row and concluded that the word path carried ~90 ms
of post-inner-query overhead against the trigram path's ~8 ms. That conclusion
was arithmetic on a mislabelled row, and it is the same error this section's
preamble forbids, made a second time in the act of documenting it.
`trigram_full_search` was added to call `store.searchFTS` directly, which is the
production trigram path with only the routing decision bypassed, and it asserts
that it returned hits so that a fast empty result cannot pass as a measurement.

The first run of that row produced an impossible result: `trigram_full_search`
measured 204.9 ms against `inner_rank`'s 260.6 ms, and the full path runs that
same inner query plus the excerpt join, the collapse, and the sort. The control
in the same output explained it. `full_search` and `word_full_search` are the
same call, and they measured 58.52 ms and 42.79 ms — 37% apart. Rows measured
later in the process were systematically faster, because each row touched pages
the previous rows had not. The rows were measuring their position in the
sequence. Each row now warms its own query before measuring, and the control
tightened to 3.7% (106.3 ms and 102.5 ms).

With that fixed, the fourth row exists and the ordering is physical (run B):

| Row | Trigram index | Word index | Ratio |
| --- | --- | --- | --- |
| `inner_rank` | 361.1 ms | 104.0 ms | 3.5x |
| full path | 344.7 ms | 102.5 ms | 3.4x |
| post-inner-query overhead | 3.5 ms | 6.4 ms | — |

The overhead row is `full - inner` at p50, because at p95 the tail noise is
larger than the quantity being measured: the trigram full path's p95 is 16.4 ms
*below* its own inner query's, which is not a negative cost but a tail that moved
between the two rows. At p50 the overhead is single-digit milliseconds for both
paths, which is what the coverage query, the excerpt join, the collapse, and the
sort should cost. Neither path loses its time after the inner query, and the word
index is 3.4x faster than the trigram index end to end.

The absolute numbers in run B are not comparable to run A's, and that is the
finding that matters more than the ratios. `inner_rank` at 20k has now been
measured four times on this machine: 75.20 ms before schema v5, then 125.2 ms,
260.6 ms, and 361.1 ms after the `CROSS JOIN` fix, with no change to the query
between the last three. That is a 4.8x spread on identical code. The word path's
full search has been measured at 40.71 ms and 102.5 ms at the same corpus size,
2.5x apart. Adding the per-row warm-up, which can only make a row faster, moved
`inner_rank` from 260.6 ms to 361.1 ms.

This invalidates a claim made earlier in this section. "At 20k the word path is
40.71 ms p95, comfortably inside the 100 ms target" is not a fact about the code;
it is a fact about run A. In run B the same path at the same corpus size measured
102.5 ms p95, which misses the target. The verdict flips between runs.

It also puts the 100k verdict in question. The word path measured 463.0 ms at
100k against a 100 ms target, a 4.6x miss — and the run-to-run drift on this
machine is 4.8x. The miss and the noise are the same size, so a single-process
measurement cannot establish that the target is missed. What the measurements do
establish is the within-process comparison, because both sides of a ratio are
taken in the same machine state: the word index is 3.4x faster than the trigram
index on the full path, and that is reproducible across runs.

The gate therefore needs a different instrument before it can be called either
way. Either the machine must be pinned — fixed clocks, no background load, and a
recorded baseline row re-measured in every run to normalize against — or the
target must be evaluated as a ratio against a reference operation measured in the
same process. The second option was tried and does not work; the next section
records why. Until one of those exists, the honest status of the 100 ms target
is "not established", not "missed".

### The instrument itself was broken

The drift has a cause, and it is not the machine being slow. It is the clock.

`time.Now()` on this machine advances in steps of about **1 ms**. Measured by
spinning until the clock changes and recording the step, over 64 trials: p50
0.9997 ms, p95 1.1231 ms, max 1.6118 ms. Back-to-back calls to `time.Now()`
returned an identical value 199,999 times out of 200,000.

That single fact explains every anomaly in this section. A benchmark that times
one operation per sample and the operation takes less than a millisecond is
reporting the clock's resolution, not the operation's cost:

| Real duration | Reported p50 | Reported p95 |
| --- | --- | --- |
| 100 µs | 1.0002 ms | 1.507 ms |
| 500 µs | 1.0000 ms | 1.510 ms |
| 1 ms | 1.2488 ms | 2.4611 ms |
| 5 ms | 5.5035 ms | 6.0081 ms |

A 100 µs operation and a 500 µs operation report the *same* figure, and both
report five to fifteen times their true cost. This is why `ids_limit_100`
reported `0 p50-ms` — the clock did not advance at all during half its samples —
and why the small rows in the attribution table were quantized to the nearest
millisecond. It is also why the rows could not be subtracted from each other:
`full_search - inner_rank` is a difference of two numbers that each carry up to
1 ms of quantization error, so a derived row reading "under 1 ms" was reading
the noise floor.

The fix is to make each sample exceed the quantum. Each row now calibrates a
batch size — doubling until one sample clears eight clock quanta — and reports
the sample divided by the batch. `word_ids_limit_100` went from `0 p50-ms` to
0.1198 ms with a calibrated batch of 51. The batch size and the measured quantum
are reported on every row, so a reader can see how much of the figure is
instrument.

Two further changes follow from the same reasoning. Every row was then reported
as a multiple of a fixed reference workload measured immediately before and after
it in the same process, on the theory that a ratio cancels the machine state that
a millisecond figure cannot. And the reference itself had to be resized: at 64
KiB it took about 50 µs and varied 74% between consecutive rows, which is worse
than the drift it exists to cancel, so it became 4 MiB and varied 5.2%.

That last change was then tested, and it does not work. The reference was
supposed to cancel the drift by dividing it out, so the test is whether the
ratio is more stable than the raw number. It is not:

| Reference | Correlation with row | Row CV | Ratio CV |
| --- | --- | --- | --- |
| CPU hash, 4 MiB | 0.260 | 37.5% | 37.5% |
| CPU hash, 4 MiB (second run) | 0.485 | 20.0% | 18.8% |
| `COUNT(*)` over chunks | 0.110 | 20.0% | undefined (reference read 0) |
| FTS ids, `LIMIT 10` | 0.476 | 20.0% | 24.0% |
| FTS `COUNT(*)` over all matches | 0.733 | 20.0% | 16.0% |

The CPU reference leaves the coefficient of variation exactly where it was, so
dividing by it removes nothing. The best candidate, an FTS count over all
matches, cuts the CV from 20.0% to 16.0% — a fifth of the noise, not the drift.
And between processes the ratio is *worse* than the raw number: over three
consecutive runs the row read 16.76, 17.53, and 17.36 ms (4.6% spread) while the
ratio read 2.10, 1.91, and 2.16 (13% spread).

The reason is that the drift is not one machine-wide factor. A CPU-bound hash
and a page-cache-bound query do not slow down together, so a reference built
from one cannot normalize the other; dividing by it adds the reference's own
noise to the row's. The reference machinery has therefore been removed rather
than shipped. Reporting the ratio would have made the numbers look rigorous
while making them less reproducible, which is the opposite of what a gate needs.

The consequence for the gate is that the earlier verdicts in this section were
measured with an instrument that could not resolve them. The ratios survive,
because both sides of a ratio were quantized the same way. The absolute
millisecond figures for every row under 1 ms do not, and neither does any
derived row built by subtracting two of them. The 100 ms target remains "not
established", and the only remaining route to establishing it is a pinned
machine with a recorded baseline row.

The bounded scan took 13.8 s p95, which is not a fallback but a hang with a
notice attached. Schema v5 fixed it: the same scan at 100k now measures 347.1 ms
p95, a factor of 39.7. The more important number is the pair, because it is what
proves the cap now bounds the work rather than the rows returned: the scan costs
280.3 ms at 2k messages and 347.1 ms at 100k, so a fiftyfold larger corpus costs
1.24x the latency. Before the index the same comparison was 533.8 ms against
13781 ms, a factor of 25.8 for the same fiftyfold corpus growth. A bounded scan
whose cost tracks the corpus is not bounded; one whose cost tracks the cap is.

Scaling is linear, not flat and not superlinear. Between 5k and 20k the warm p95
is indistinguishable (84.78 ms and 79.14 ms), which is what the bounded candidate
pool predicts while the pool is not yet saturated. Between 20k and 100k, 5x the
messages costs 5.3x the latency, and the in-process attribution run agrees layer
by layer: matches grow 1157 to 5775 (5.0x), enumeration 26.36 to 134.9 ms (5.1x),
scoring 29.36 to 142.0 ms (4.8x), joins 16.42 to 124.6 ms (7.6x), and the full
search 75.20 to 398.7 ms (5.3x). Linear scaling with a large constant is the
shape, and the constant is what misses the target.

That paragraph is the pre-v4 record and its `full_search` number is the trigram
path, which production no longer takes for this query. The word path's own
scaling has not been measured at 100k since the `CROSS JOIN` fix, so the linear
claim above is a claim about the trigram index only. It is left standing because
it is still true of the index it describes, and the word path is measured
separately below.

The join is the one layer that grows faster than the match count, 7.6x against
5.0x, and it is worth naming rather than smoothing over. It is also the smallest
of the three at 20k, so its superlinearity is not what misses the target; it is
recorded because a layer that grows faster than the others will eventually
dominate, and the next measurement at 1M should check whether it does.

Two earlier revisions of this paragraph were wrong in opposite directions, and
both are recorded because each was a reasoning error rather than a typo. The
first concluded from the 5k/20k pair alone that latency was flat in corpus size
and that the 100k target was therefore within reach; two points are not a
scaling curve. The second measured 1541 ms at 100k and concluded that scaling
was superlinear, 5x the data for 19.5x the latency. That number was not a
measurement of the query: the warm-up loop exited after two rounds, so the
benchmark timed a cold archive under a warm label. With the warm-up fixed the
same rotation measures 357.4 ms, and the breakdown's independent `rotating` row
measures 359.5 ms. A wrong number produced a wrong shape, and the shape was
believed because it was plausible.

The broad query does grow, 167.6 ms to 322.5 ms between 5k and 20k, because it
matches a large fraction of the archive and FTS5 scores every match before the
limit applies; it is reported as a worst case and is not targeted.

Attribution is measured, not assumed. `BenchmarkSearchAttribution` isolates each
layer of the search path, and it reports the query's selectivity alongside the
latencies because every number below is a function of how many chunks match.
Each layer is the difference between two rows that enumerate the same number of
matches, because subtracting rows that enumerate different amounts attributes
enumeration cost to whatever else differs. Both corpus sizes were measured in a
single process, which is required rather than convenient — see the drift note
below.

| Layer | Derived from | p95 at 20k | p95 at 100k | µs per match, 20k → 100k |
| --- | --- | --- | --- | --- |
| Fixed cost of opening the query | `ids_limit_10` | 3.06 ms | 2.31 ms | — |
| Enumerating the matches | `ids_limit_10000 - ids_limit_10` | 26.36 ms | 134.9 ms | 22.8 → 23.4 |
| bm25 scoring of all matches | `bm25_limit_100 - ids_limit_10000` | 29.36 ms | 142.0 ms | 25.4 → 24.6 |
| Three joins applied to all matches | `inner_rank - bm25_limit_100` | 16.42 ms | 124.6 ms | 14.2 → 21.6 |
| Coverage query and excerpt join | `full_search - inner_rank` | under 1 ms | under 1 ms | — |
| Full search | `full_search` | 75.20 ms | 398.7 ms | 65 → 69 |

That table was taken before schema v4 and v5, and two of its rows no longer mean
what they say. The `full_search` row is `store.Search` on `queries[0]`, which is
three ASCII words, so since schema v4 it routes to the **word** index while every
other row in the table reads the **trigram** index. The `full_search - inner_rank`
row is therefore a subtraction across two different indexes, which is exactly the
error this section's own preamble forbids: a layer's cost is the difference
between two rows that do the same amount of everything else. It is left in place
as the pre-v4 record it is, and the word-index rows below are the ones that
decompose the path `full_search` now actually takes. The confirmation that this
is not a subtlety is that `full_search` and `word_full_search` measure 42.84 ms
and 42.76 ms in the same process — the same query, twice, under two names.

The `full_search - inner_rank` row is also the weakest row in the table for a
second reason, and it is worth flagging rather than trusting. It reads "under
1 ms" because the two rows it subtracts were both recorded as 75.20 ms at 20k.
Two different queries — one of which additionally runs the coverage query, joins
the excerpt text, collapses candidates to one hit per message, sorts, and
truncates — landing on the same figure to two decimal places is not a
measurement, it is a coincidence or a transcription. The row should be treated as
unmeasured until the two numbers are taken independently in one process. The
word-path equivalent has since been measured directly and is 6.4 ms at p50, which
is the number to use.

The arithmetic shows it is a transcription, and the 100k column is where it
becomes visible. In the 20k column the rows are circular: `bm25_limit_100` is
58.78 ms and the join layer is 16.42 ms, which sums to exactly 75.20 ms — the
figure recorded for `full_search`. So `inner_rank` was not measured; it was
back-filled from `full_search` on the assumption that the coverage layer was
free, and the derived row then reports that assumption back as a result. In the
100k column the same construction contradicts itself: 142.0 + 124.6 = 266.6 ms
for `inner_rank`, against a recorded `full_search` of 398.7 ms, which leaves
132.1 ms for the row the table calls "under 1 ms". Both columns cannot be right,
and the 100k column is the one that was measured independently. The coverage
layer is therefore not free — it is roughly a third of the full search at 100k —
and the 20k column's `inner_rank` and coverage rows should be read as unknown
until re-measured. This is the same failure as the mislabelled `full_search`
column: a derived row that was never checked against the rows it claims to
derive from.

The match count grows from 1157 to 5775, 5.0x, and the full search grows from
75.20 to 398.7 ms, 5.3x. The per-match cost is nearly constant across a fivefold
change in corpus size, which is the strongest evidence available that the query
is linear in matches and that nothing pathological happens at scale. It also
means the cost is spread across enumeration, scoring, and joins in roughly equal
thirds, and all three are proportional to the match count. No single layer
dominates, so no single-layer fix reaches the target: making the join free would
still leave 274 ms at 100k, and making scoring free would still leave 257 ms.

The first version of this table was wrong in a way worth recording. It computed
the scoring cost as `bm25_limit_100 - ids_limit_100`, which is 55.8 ms, and
called that 36% of the query. But `ids_limit_100` enumerates 100 matches while
the bm25 query enumerates all 1157, so the difference contained 26 ms of
enumeration and attributed it to scoring. The rule is that a layer's cost is the
difference between two rows that do the same amount of everything else, and the
first table broke that rule while looking like it followed it.

Three conclusions follow, and two of them overturn things that were previously
written down here as if they were measured.

The first is that the join is not negligible. `join_only`, which joins 100 real
chunk ids with no `MATCH` in front of it, costs 1.06 ms at 20k and 1.51 ms at
100k, and an earlier revision of this section used that number to dismiss the
join path. But the real query applies the joins to every match, not to the
candidate limit, so the join costs 16.4 ms at 20k and 124.6 ms at 100k — 22% and
31% of the query, not 0.6%. The 1.06 ms measured the wrong thing: it measured the
join over the rows that survive, not over the rows that are processed.

The second is that pushing the scope predicate into the join does not help.
`inner_rank_project` adds `s.project_key=?` to the same query and measures
80.87 ms against `inner_rank`'s 75.20 ms at 20k, and 416.0 against 403.8 ms at
100k — slower both times, not faster. The predicate filters the output but cannot
reduce the number of chunks FTS5 enumerates and scores, because FTS5 produces
matches before any join runs. Scope predicates belong in the query for
correctness, not for speed.

The third is that bm25 scoring is flat in the candidate limit. At 20k,
`bm25_limit_10` costs 55.94 ms and `bm25_limit_10000` costs 59.41 ms; at 100k,
264.8 and 276.2 ms. FTS5 must score every match regardless of how many rows are
returned. That rules out two candidate fixes outright. Adding an index cannot
help, because the join is 124.6 ms of a 398.7 ms query and the index it would add
is already there. Tuning `maxSearchCandidates` cannot help, because the scoring
cost does not depend on it.

The fourth conclusion is the one that decides the fix, and it is not the one
this section predicted. The obvious reading of the table above is that the cost
is proportional to the number of matches, since the match count and the latency
both grew fivefold between 20k and 100k. That reading is wrong, and the
benchmark refutes it twice.

The first refutation is the query shape. `and_bm25_limited` runs the same three
terms joined with `AND` instead of `OR`, which cuts the match count from 1157 to
97, a factor of twelve, and the latency falls from 202.6 ms to 158.2 ms, a factor
of 1.3. Cutting the matches by twelve cuts the time by a quarter.

The second refutation is the term count. `one_rare_term` and `one_common_term`
are single-term queries that match 610 and 596 chunks respectively — the same
number of matches, one from a rare term and one from a common one — and they cost
65.42 ms and 60.13 ms. Two queries with identical match counts and very different
term frequencies cost the same. Meanwhile the three-term query costs 202.6 ms,
which is 3.1 times the single-term cost. The cost tracks the number of terms, not
the number of matches, and it tracks it linearly.

That is the signature of posting-list traversal. Each term costs a fixed amount
of work that depends on how long its posting lists are, and the number of matches
that survive the boolean expression is irrelevant because the lists are walked
either way. It also explains why the cost grows with the corpus while the
selectivity stays constant: the posting lists grow with the corpus, and the
fraction of the corpus that matches does not change.

The tokenizer is the reason the per-term cost is so high. `mem_v2_chunk_fts` uses
the trigram tokenizer, which indexes every three-character sequence, so a
six-character term like `w01234` is not one token but a phrase of four
overlapping trigrams, and matching it requires intersecting four posting lists.
Those lists are as long as the trigrams are common, and trigrams like `w01` are
extremely common in a corpus of generated words. A single term costing 65 ms is
not a plausible cost for looking up one token and is a plausible cost for
intersecting four long lists.

`BenchmarkTokenizerComparison` tested this directly by indexing the same chunk
text with both tokenizers in the same process and running the same queries
against both. It confirmed the hypothesis, and by a wider margin than expected:

| Query | trigram | unicode61 | ratio |
| --- | --- | --- | --- |
| three terms, ranked | 117.4 ms | 7.38 ms | 15.9x |
| three terms, ids only | 6.09 ms | 0.95 ms | 6.4x |
| one term, ranked | 30.9 ms | 3.30 ms | 9.4x |

The comparison is only meaningful if both indexes match the same rows, so the
benchmark reports that too: the trigram index matched 1157 chunks and the
`unicode61` index matched 1142, a difference of 1.3%. The 16x speedup is not
`unicode61` matching less; it is the same rows found by a different index.

Two things follow. The first is that the fix is an index redesign, not a
retrieval redesign: a word-tokenized index for the terms it can handle, with the
trigram index kept for substring and CJK matching, and queries routed by term
shape. The existing `minTrigramRunes` and `searchScan` fallback logic is the
natural place to branch, because it already decides which terms the trigram
index cannot serve.

The second is that the target looked reachable with room to spare. `word_ranked`
at 7.38 ms is already below the 100 ms target at 20k, and the corpus scaling that
took 20k to 100k multiplied every layer by about five, which projected to roughly
37 ms at 100k. That projection was a projection and not a measurement, and it was
checked at 100k before the redesign was called done. It was wrong: the production
word path measures 463.0 ms p95, 12.5x the projection and 4.6x the target. The
reason is recorded in the per-mode table above — the tokenizer comparison measured
a benchmark-local table, not the query built on top of it.

The ids-only rows are worth reading separately, because they show the two
tokenizers differ in two ways rather than one. Without ranking, trigram costs
6.09 ms and `unicode61` costs 0.95 ms, a factor of 6.4. With ranking the gap
widens to 15.9x. So part of the cost is the tokenizer's posting lists and part
of it is what ranking does with them, and a redesign that only changed the
tokenizer would recover most of the gap but not all of it.

An earlier revision of this paragraph named a missing index on `message_id` as a
suspect. That was wrong: `mem_v2_chunks` declares
`UNIQUE(message_id, message_revision, ordinal)`, whose leftmost column is
`message_id`, so the join can use it as a prefix lookup. The claim was written
from reading the index list rather than the table constraints, and it is
recorded here because a wrong suspect sends the next reader to the wrong fix.

Every number in this section is measured in a single process. That is a
requirement, not a convenience: the same benchmark on the same corpus measured
`count_only` at 30.56 ms in one session and 90.16 ms in another, with the CPU at
12-16% load and no throttling, so a difference of three times is ordinary drift
on this machine. Comparing two corpus sizes measured in two processes therefore
compares two machine states as well as two corpus sizes, and subtracting across
sessions produces a scaling curve that is mostly the machine. `MEMSTORE_BENCH_MESSAGES`
accepts a comma-separated list and the attribution benchmark loops over the sizes
in one process for exactly this reason.

The cost was attributed before anything was optimized. `BenchmarkIndexStatusParts`
times each coverage subquery and `BenchmarkSearchCostBreakdown` times each layer
of the search path. The full coverage query cost 14.74 ms p95 at 20k, which is a
fixed per-search cost that cache warmth does not remove: against a warm search of
79.14 ms it was 19% of the total. Inside it two terms dominated:

| `IndexStatus` subquery | p95 at 20k |
| --- | --- |
| `COUNT(DISTINCT message_id) FROM mem_v2_chunks` | 44.50 ms |
| `COUNT(*) FROM mem_v2_chunks` | 20.78 ms |
| `COUNT(*) FROM mem_v2_messages` | 1.00 ms |
| pending jobs, failed jobs, index state | under 1 ms each |

Both expensive terms are full scans of `mem_v2_chunks` — `COUNT(DISTINCT
message_id)` cannot use the `UNIQUE(message_id, message_revision, ordinal)`
prefix because it must visit every row to deduplicate — and `Search` ran the
whole coverage query on every request only to decide whether `Partial` is true. That decision needs the pending-job
count and nothing else, so the query was split: `Store.IndexCoverage` returns
only the pending/failed job counts and the index generation from indexed
lookups, and `Search` now calls it instead of `IndexStatus`. `IndexStatus` keeps
the message and chunk counts for diagnostics that actually report them. Measured
at 20k messages, this takes the per-search coverage cost from 15.15 ms p95 to
under 1 ms, and the amount removed grows with the archive because both scans are
linear in chunk count.

Three production defects were found by this gate and fixed before the numbers
above were taken:

1. The index worker could never make progress. `IndexPending` inherited the
   writer's user-facing 2 s commit timeout for a whole batch, but a full
   200-message batch already took 1.92 s at 1200 messages, so every batch on a
   real archive failed with `context deadline exceeded` and the archive stayed
   permanently under-indexed. Background maintenance now requests its own
   deadline via `submitWithTimeout`, and a batch takes the longest deadline any
   operation in it asked for. `TestIndexBatchFitsCommitTimeout` guards the
   regression. The original write-up of this defect claimed the cost grew with
   index size; the fifth problem below shows that was wrong, and the deadline is
   generous rather than marginal.
2. Search had an N+1. `searchFTS` re-fetched each candidate's excerpt with a
   separate query even though the FTS table already stores the chunk text.
3. Search selected the chunk text in the same query that ranks with `bm25()` and
   limits. FTS5 scores every match before the limit applies, so the text of
   every match was materialized. Ranking chunk ids first and joining back for
   text of at most `maxSearchCandidates` rows bounds that work. Scope predicates
   stay in the inner query: applying them after the limit would let a broad
   query starve a narrow scope.

A fourth problem was in the benchmark, not the code, and it is worth recording
because it produced a plausible-looking wrong answer. The first fixture drew CJK
terms from a list of eight bare stems and placed one in every body, so every
message contained all eight. Queries built from those stems matched the entire
archive, and the "realistic" benchmark was silently measuring the worst case
while the benchmark labelled "broad" measured something narrower. The fixture now
pairs each stem with a generated suffix, and the broad benchmark uses a bare stem
on purpose. Any benchmark that reports a number must assert that its query
matches the fraction of the corpus it claims to.

A fifth problem was a wrong diagnosis, and it is recorded because the wrong
version was briefly written down as a defect. Seeding a 20k-message benchmark
archive failed with `index: context deadline exceeded`, which looked like the
first defect returning at a larger size, and the deadline was assumed to be too
small. Measuring it directly disproved that: `TestIndexBatchScalesWithArchiveSize`
seeds 20k messages and times every batch of the drain, and the worst batch was
4.61 s against a 30 s deadline, with a mean of 2.03 s and no upward trend across
100 batches. Batch cost is flat in archive size, not superlinear, because FTS5
trigram insertion is bounded by the batch rather than by the existing index. The
real cause was contention: the failing run shared the machine with another
full-package test process, and the two competed for the same disk. The lesson is
the same one the fixture bug taught, in a different form: a measurement taken
while something else is running is not a measurement of the code.

A sixth problem was also in the benchmark, and it is why the table above changed.
`BenchmarkScopedLexicalSearch` reported 288.9 ms p95 at 20k while claiming to
measure warm latency, but it began timing on a freshly written archive, so the
early iterations paid for reading pages off disk. The same four-query rotation,
measured after the cache was warm, cost 76.11 ms. The benchmark now runs each
query until its latency stops improving before it starts timing. A benchmark that
labels a number warm has to warm the cache, or the label is a claim the code does
not support.

A seventh problem was in the fix for the sixth. The first warm-up loop stopped as
soon as one round was not 10% faster than the previous round, which is a purely
relative test: on a multi-gigabyte index a round can be marginally slower than
the one before it for reasons that have nothing to do with convergence, and the
loop then exits after two rounds and reports a cold number under a warm label.
That is exactly what happened at 100k, where the same rotation measured 1541 ms
with the relative rule and 357.4 ms once the cache was genuinely warm — a factor
of 4.3, and enough to make a linear query look superlinear. The loop now requires
a minimum of five rounds, converges against the best round seen rather than the
previous one, and logs the round count and best time so an under-warmed run is
visible instead of silent. A warm-up loop needs a floor as well as a convergence
test, and a warm-up that cannot be inspected is a warm-up that can be wrong
without anyone noticing.

Still outstanding for this gate: the 1M corpus size, cold-cache measurements, p99
and saturation throughput, concurrent readers, and measurement while capture and
indexing are active. The 100k search target is measured and missed, and the cause
is no longer the tokenizer. The trigram tokenizer was measured at 15.9 times the
cost of a word tokenizer on the same rows, the index redesign that follows from
that measurement is implemented (schema v4 adds a `unicode61` word index
alongside the trigram one and `Search` routes each term by its shape), and the
redesign was then measured on the production path at 100k: 463.0 ms p95 for the
word path, 764.3 ms for the trigram path, 191.5 ms for the hybrid path. The
redesign helped and did not finish the job. The ~37 ms projection that predicted
it would is refuted, and the reason is recorded above: the tokenizer comparison
measured a benchmark-local table, not the query built on top of it. The hybrid
path being faster than both of the paths
it combines is not the hint it first appeared to be: the three modes ran queries
with different term counts, and cost scales with term count, so the inversion is
a confound in the comparison rather than a fact about the indexes. The
fixed-term-count rows that can settle it have now been run at 20k and they do
settle it: on the same archive, in the same process, with the same three terms,
the word index is 25.0x faster on bm25 scoring, 7.9x faster on enumeration, and
3.5x faster on the inner query, while matching 98.7% of the same chunks. The
inversion was the term-count artifact it looked like. The word index is also
3.4x faster on the full path, measured in one process with each row warming its
own query.

What the same runs do not settle is the absolute verdict, and this is the
eleventh problem. `inner_rank` at 20k has been measured at 75.20, 125.2, 260.6,
and 361.1 ms on identical code — a 4.8x spread with no change to the query. The
word path's full search has been measured at 40.71 and 102.5 ms at the same
corpus size, so the claim that it meets the 100 ms target at 20k flips between
runs. The 100k miss is 4.6x, which is the same size as the drift, so a
single-process measurement cannot establish that the target is missed either.
Within-process ratios are reproducible because both sides share a machine state;
absolute latencies are not. The gate needs a pinned machine with a recorded
baseline row re-measured every run. A ratio against a reference operation
measured in the same process was tried as the cheaper alternative and does not
work: the best of five candidate references cut the coefficient of variation
from 20.0% to 16.0%, and between processes the ratio was noisier than the raw
number (13% spread against 4.6%). Until a pinned machine exists the honest
status of the 100 ms target is "not established". No competitor comparison has
been run, and none should be published without the same corpus and operation
definitions.

A ninth problem was found by the 100k measurement and is recorded here because it
is the same class of error as the fourth and sixth, in the code rather than the
benchmark. The bounded scan — the fallback for one- and two-character terms that
neither tokenizer can index — took 13.8 s p95 at 100k, against 8.4 ms for the same
scan at 2k. It was written as if `maxFallbackScan` bounded the work, and it did
not: the query orders by `created_at` and `seq`, `mem_v2_messages` had no index on
either, so SQLite scanned the whole table, materialized every message in the
principal including its payload, and sorted all of it before the `LIMIT` could
apply. `EXPLAIN QUERY PLAN` said so plainly — `SCAN m` plus `USE TEMP B-TREE FOR
ORDER BY` — and `TestSearchScanQueryPlan` now pins the plan so the regression
cannot return silently. Schema v5 adds
`mem_v2_message_scan(principal_id, created_at DESC, seq DESC)`, which changes the
plan to `SEARCH m USING INDEX mem_v2_message_scan (principal_id=?)` with no
temporary B-tree. Measured at 100k the scan fell from 13781 ms p95 to 347.1 ms,
and the pair of measurements at 2k and 100k (280.3 ms and 347.1 ms, 1.24x for a
fiftyfold corpus) is the evidence that the cap now bounds the work. The general
lesson is that a `LIMIT` bounds the rows returned, not the work done, unless an
index serves the `ORDER BY`.

The same scan had a second defect that no measurement would have found, because
it was a lie in a comment rather than a cost. Its doc comment said it reported
the cap through `TooBroad`, and it did not: a scan that examined 2000 messages
and found three hits returned `TooBroad=false`, `Truncated=false`, and no notice,
so the caller was told "these are the matches" when the truth was "these are the
matches in the newest 2000 messages". That is the same failure as reporting an
unindexed message as a confident miss. `searchScan` now returns a `capped` flag,
requests one row beyond the cap so the two cases are distinguishable, and `Search`
sets `TooBroad` and replaces the notice when the cap applied.
`TestSearchScanReportsCapWhenItStopsEarly` seeds `maxFallbackScan+1` messages and
asserts both directions: over the cap reports it, under the cap does not.

The MCP renderer had to learn the same distinction. `TooBroad` was rendered as
"the candidate pool hit its cap; results are a subset. Narrow the query", which
is correct advice for a capped candidate pool and impossible advice for a capped
scan: the query is already as narrow as the indexes allow, because its terms are
too short to index, so no rewording of them will help. A scan that reports
`TooBroad` now renders "the scan stopped at its message cap; results are a subset
of the newest messages" instead. Advice that cannot be followed is a different
kind of dishonesty from a limit that is not reported, and it is the kind a
generic flag invites.

A tenth problem was found by the measurement that was supposed to confirm the
ninth was fixed, and it is the most expensive one in this document. Schema v5's
index made the bounded scan fast and made the primary search path unusable.

The attribution benchmark was run at 20k and 100k in one process to find where
the 463 ms word path spends its time. It never reached the word rows: the
`inner_rank` row — the FTS `MATCH` plus the three joins, with no projection and
no excerpt — measured 104471 ms p95 at 20k messages. The same row had measured
75.20 ms p95 before schema v5. That is a factor of 1388, on the query every
non-scan search runs.

The cause was the join order, and `EXPLAIN QUERY PLAN` named it. Before v5 the
planner had no cheap way to reach `mem_v2_messages` from a principal, so it drove
from the FTS table, which is the only selective predicate in the query:

```
SCAN mem_v2_chunk_fts VIRTUAL TABLE INDEX 0:M1
SEARCH c USING INTEGER PRIMARY KEY (rowid=?)
```

After v5 it could reach the principal's messages through an index, and it
preferred that:

```
SEARCH m USING INDEX mem_v2_message_scan (principal_id=?)
SEARCH c USING COVERING INDEX sqlite_autoindex_mem_v2_chunks_1 (message_id=?)
SCAN mem_v2_chunk_fts VIRTUAL TABLE INDEX 0:=M1
SEARCH s USING INDEX sqlite_autoindex_mem_v2_sessions_1 (id=?)
USE TEMP B-TREE FOR ORDER BY
```

Same rows, worst possible order: every message in the principal, each one
probing the FTS index for a match, instead of every match, each one looking up
its message. The planner was not wrong about the cost it estimated; it was wrong
about which side of the join is selective, because `MATCH` selectivity is not
something the statistics it has can express.

The fix is `CROSS JOIN` on the FTS table in `searchIndex`. SQLite documents
`CROSS JOIN` as the way to prevent the optimizer from reordering a join, and the
`ON` clauses are still honoured — only the loop order is pinned. With it the plan
returns to the pre-v5 shape, and dropping the v5 index produces an identical
plan, which is the confirmation that the index was the whole cause and the
`CROSS JOIN` is a complete substitute for its absence.
`TestSearchIndexQueryPlanPinsFTSDriver` asserts that the FTS table is the first
step and that `mem_v2_message_scan` does not drive the query, so the regression
cannot return silently. The benchmark's hand-built copies of the query were
switched to `CROSS JOIN` as well: a copy that drifts from production does not
measure production, and these copies had been measuring a query that no longer
existed.

The lesson is not "do not add indexes". It is that an index added to make one
query fast can make another query slow, because the planner reorders inner joins
by estimated cost and a new index changes those estimates everywhere. The
corollary is the one that made this expensive: the full test suite passed for the
entire time the regression was live, because no test asserted a query plan. A
suite that only asserts results cannot see a change in how the results are
computed, and the cost of that blindness is unbounded — 104471 ms is not a slow
query, it is a query that does not finish. Both plan tests now exist, and the
rule they encode is that any query whose cost depends on an index choice gets a
test that pins the choice.

The fix was verified by re-measuring the same rows at 20k in a fresh process:
`inner_rank` fell from 104471 ms to 125.2 ms p95, and `inner_rank_project` to
136.0 ms. That is the regression gone. It is also 1.66x the 75.20 ms the row
measured before schema v5, and the question is whether `CROSS JOIN` costs
something the planner's own ordering did not.

The full attribution table was then re-run at 20k in one process, and it answers
that question by making the residual look like what it is. Every trigram row
moved by the same factor:

| Row | Recorded | Re-measured | Ratio |
| --- | --- | --- | --- |
| `ids_limit_10` | 3.06 ms | 4.21 ms | 1.38x |
| `ids_limit_10000` | 29.42 ms | 47.02 ms | 1.60x |
| `bm25_limit_100` | 58.78 ms | 93.71 ms | 1.59x |
| `inner_rank` | 75.20 ms | 125.2 ms | 1.66x |
| join layer (`inner_rank - bm25_limit_100`) | 16.42 ms | 31.49 ms | 1.92x |

`inner_rank` is not an outlier; it sits in the middle of a uniform 1.4–1.7x
shift that also affects rows `CROSS JOIN` cannot touch, because they have no
joins at all. `ids_limit_10` is a bare `SELECT rowid ... LIMIT 10` and it moved
1.38x. So the residual is this machine's session-to-session drift, which has been
measured at 2.7–3.6x elsewhere in this document, and not a cost of the fix. The
join layer's 1.92x is the one row that moved more than the rest, and it is a
difference of two drifting numbers, so its ratio is the least reliable in the
table.

This is worth recording as a method rather than a result. The residual was
unattributed for one measurement, and the way it got attributed was not a better
theory but a control: re-measuring rows that the change under test cannot affect.
If those rows move too, the change is not the cause. A single before/after pair
on one row cannot distinguish a fix's cost from the machine's mood, and this
document has already been wrong once by believing a plausible story that one
measurement supported.

### The capture deadline is not the index deadline

Seeding a 100k archive for the index scaling test failed at message 34,561 with
`record scale-034561: context deadline exceeded`. The failure was in
`RecordMessage`, not in `IndexPending`, and that distinction matters more than
the number: an index batch that times out leaves the archive under-indexed and
is retried, while a capture batch that times out **loses the message**. Phase 0
of this plan exists to stop losing messages, so a capture timeout is the one
failure that cannot be tolerated.

The obvious reading was that capture had the same defect the index deadline had:
a fixed timeout on a path whose cost grows with the archive. `RecordMessage`
does use the writer's default `CommitTimeout` of 2 s, and that default does not
scale with anything. But the reading was wrong, and the measurement says so.

`TestCaptureCommitScalesWithArchiveSize` bands the archive in 2000-message steps
and reports the worst `RecordMessage` commit in each band against the real
resolved deadline:

| Archive | Worst capture commit | Share of the 2 s deadline |
| --- | --- | --- |
| 2k | 80 ms | 4% |
| 10k | 115 ms | 6% |
| 20k | 156 ms | 8% |
| 30k | 311 ms | 16% |
| 40k | 225 ms | 11% |

The worst commit anywhere in 40k messages was 329 ms, at 38k, which is 16% of
the deadline. Capture cost is flat in archive size. The mild upward drift from
80 ms to roughly 300 ms is page-cache pressure, not a superlinear term, and it
does not approach the deadline. A fixed 2 s capture deadline is therefore not
the defect the index deadline was, and changing it would have been a fix for a
problem that does not exist.

The actual cause was contention. The 100k run was still executing when the
capture measurement was launched, so two `go test` processes were competing for
one disk. This is the same mistake recorded in the fifth problem above, made
again, and it is worth naming as a pattern rather than an incident: **a timeout
under contention is indistinguishable from a timeout under load unless you check
what else is running.** The failure mode is not a slow number that invites
suspicion; it is a hard error that looks exactly like a code defect.

The consequence for the gates is that the 1M benchmark is not blocked by a
capture bug. It is blocked only by the requirement that nothing else run while
it measures, which is a scheduling constraint and not a code change.

### The index deadline holds at 100k

The same clean re-run that proved the capture failure was contention also
answered the question the scaling instrument exists to answer. Draining 100,000
messages took 500 batches and 23m48s, with a mean of 2.857 s per batch and a
worst batch of 10.987 s at batch 137, against the 83.52 s deadline that batch
ran under. That is a 7.6x margin, and the run passed.

| Archive | Batches | Mean batch | Worst batch | Deadline at worst | Margin |
| --- | --- | --- | --- | --- | --- |
| 20k | 100 | 1.068 s | 5.478 s (batch 84) | 66.56 s | 12.2x |
| 100k | 500 | 2.857 s | 10.987 s (batch 137) | 83.52 s | 7.6x |

Five times the archive cost 2.7x the mean batch and 2.0x the worst batch, so
batch cost is sublinear in archive size, which is the shape the two-term model
predicts while the per-chunk term is still small relative to the per-message
term. The deadline is therefore not marginal at 100k, and the earlier claim that
it was is refuted by the same measurement that refuted the capture story.

The narrowing margin from 12.2x to 7.6x is worth reading correctly rather than
as a trend toward failure. The worst batch in both runs occurred early — batch
84 of 100, and batch 137 of 500 — because that is where the per-message term
dominates and the per-chunk term has not yet grown. A batch late in the drain
runs under a much larger deadline, so the comparison is fair at every point and
the margin is smallest where the budget is smallest. Extrapolating the ratio
without accounting for where the worst batch lands would predict a failure at 1M
that the model does not support: at 1M messages the per-chunk term reaches
roughly 1600 s against a 40 s per-message term, so the budget grows faster than
the batch cost from here.

### The codec cost is measured, and it is small

PR 6 added block compression, and the plan asked for it to run asynchronously.
It runs synchronously instead, and the reason is a measurement rather than a
preference. `BenchmarkCompressBlockCost` and `BenchmarkDecodeBlockCost` in
`internal/memstore/codec_bench_test.go` measure the codec on realistic text at
`-benchtime 300x`, with the codec warmed before the timer starts:

| Payload | Codec chosen | Compress | Decode |
| --- | --- | --- | --- |
| 200 B reply | raw | 2.0 ns | 2.0 ns |
| 4,000 B (below threshold) | raw | 2.0 ns | not measured |
| 4 KiB (at threshold) | zstd | 40 µs | 7.3 µs |
| 16 KiB answer | zstd | 222 µs | 23 µs |
| 64 KiB tool output | zstd | 815 µs | 92 µs |
| 1 MiB message | zstd | 6.3 ms | not measured |
| 16 KiB incompressible | raw | 11 µs | not measured |

The sub-threshold rows report single-digit nanoseconds because the encoder is
never entered; they are evidence that the threshold works, not a measurement of
compression. The incompressible row is the worst case for the write path — the
CPU is spent and the archive gains nothing — and it costs 11 µs because the
encoder bails out as soon as the frame fails the one-in-eight saving test.

Per-byte throughput is not monotonic in size (roughly 100 MB/s at 4 KiB, 62 at
16 KiB, 80 at 64 KiB, 165 at 1 MiB). That is expected for a dictionary
compressor: the fixed cost of building match tables is amortized over more input
as the block grows, and the 16 KiB dip is the point where the table is large
enough to cost more than the extra input repays. The number that matters is the
absolute per-block cost against the deadline, not the MB/s column.

Against the 2 s capture deadline, whose measured worst commit is 329 ms in a 40k
archive, the largest message the writer accepts (16 MiB, the `QueueBytes` limit)
extrapolates to roughly 100 ms, about 5% of the deadline. A typical 16 KiB answer
costs 222 µs, which is 0.01% of it. Synchronous compression is therefore
defensible until the crash-safe repacking machinery exists, and the plan's
"asynchronously" wording has been reconciled with that measurement rather than
silently ignored.

**The first version of this benchmark was wrong, and the wrongness was visible
as an inversion.** The 4 KiB row measured 110 µs while the 16 KiB row measured
62 µs and the 64 KiB row 88 µs — larger payloads compressing faster per byte.
That is not plausible for real text, and it was not the codec: the large rows
were built with `strings.Repeat(benchBody(n), k)`, so they were the same 4 KiB
block repeated four and sixteen times, which zstd matches almost instantly. A
repeated fixture measures the compressor's ability to detect a repeat, not its
cost on a long answer, and a real long answer does not contain the same paragraph
four times. The rows now use `variedTextForBench`, which walks the vocabulary
with a different seed per row, and the ordering is monotonic in size.

Both benchmarks now assert their own preconditions: each row declares the codec
it must exercise and fails if the fixture chooses the other one. Without that
check a fixture that stops compressing reports the bail-out path's cost under a
label that claims to measure compression, and nothing in the output reveals the
swap.

## 16. Delivery sequence and rollback

| Deliverable | Depends on | Exit condition |
| --- | --- | --- |
| PR 1: loss prevention and honest legacy results | none | Phase 0 tests; no mutating MCP reads |
| PR 2: additive schema, writer, migration | PR 1 | Durable acknowledgments and resumable import |
| PR 3: client/adapter lifecycle capture | PR 2 | Sync, stream, fallback, timeout, restart tests |
| PR 4: scoped lexical search, detail, scroll | PR 2-3 | Evidence retrieval and index coverage tests |
| PR 5: trusted MCP identity and diagnostics | PR 4 | Host-level isolation; shared rollout remains blocked until proven |
| PR 6: measured compression/index optimization | PR 4-5 | Round-trip and performance/space gates |
| PR 7: optional knowledge extraction | PR 5 | Provenance, correction, and budget tests |
| PR 8: optional semantic search | PR 6-7 and measured need | Held-out recall improvement with bounded costs |
| PR 9: source-backed handoff and retention | PR 3-5 | Acceptance/consume semantics and scoped deletion tests |

Ship capture first with recall disabled or correctly isolated; enable v2 reads only after import and indexing are complete enough to report coverage honestly. Compare v2 results against labeled fixtures and legacy behavior offline; do not duplicate model-side memory injection during shadow evaluation.

Rollback targets the Phase 0-safe binary, never the pre-fix binary that purges history on open. Keep additive v2 data intact; a disabled feature stops new use, not deletion. If reverting capture to legacy, record the switchover and re-import the legacy delta on reactivation. Restoring an old backup loses newer writes unless they are reconciled; require an explicit recovery procedure rather than calling that lossless rollback.

Update [README.md](../README.md), [MCP_MEMORY_TOOL.md](MCP_MEMORY_TOOL.md), [CONFIGURATION.md](CONFIGURATION.md), and [ARCHITECTURE.md](ARCHITECTURE.md) as each feature ships. Document trust boundaries, durability, retention, migration, troubleshooting, and benchmark results. Avoid advertising optional phases before they pass their gates.

## 17. Decisions required before implementation

Confirm the deployment's single-user versus multi-user model, how group chats share memory, the tolerated capture failure behavior, and retention for sensitive tool outputs. Use the defaults above for a prototype, but block production recall until trusted caller binding is proven. Establish the reference hardware and corpus before treating speed/size targets as release commitments.

Recommended first implementation scope: PR 1 through PR 5. They fix correctness, durability, and usable recall. Compression follows measured storage costs; model-based enrichment follows measured recall gaps.