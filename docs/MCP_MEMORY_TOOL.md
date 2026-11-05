# MCP Memory Tool

The gateway ships a read-only MCP stdio server for searching saved requests and
responses. The model chooses when to call it. This release supports only an
explicitly configured, isolated single-owner OpenCode/MCP instance.

## Upgrade and Safety

- Back up the database consistently before upgrading. Stop writers before copying
  SQLite files, or use a WAL-aware SQLite backup tool; do not copy only a live main DB.
- Upgrade both gateway and MCP binaries, then start the gateway first. It adds a
  search migration marker and transactionally rebuilds the legacy FTS index once
  to include stored responses and tags. Records themselves are retained.
- Newly stored responses are not truncated to 600 bytes. Startup keyword purging,
  hourly strength decay, and strength-based search exclusion are removed.
- Deleted history and already-truncated responses cannot be restored without an
  independent backup or upstream conversation archive.
- MCP opens with SQLite `mode=ro` and `query_only=1`: it never creates, migrates,
  purges, or strengthens records. Missing/old databases produce an actionable error.
  SQLite may still require access to WAL/shared-memory sidecars for a live database.
- New automatic registrations are disabled. Existing entries are not overwritten;
  verify their binary and database paths and configure the binding below.

**Do not enable this MCP process on a shared-user OpenCode instance.** A fixed
owner flag limits the database rows, but cannot authenticate different users
calling the same tool process. Shared-user transport isolation is a later phase.

## Build

```bash
go build -o bin/openbot-mcp ./cmd/mcp
```

On Windows, build `bin/openbot-mcp.exe` and use that executable in the configuration.

## Configure opencode.json

Add the following to your project's `opencode.json` (typically at
`$OPENCODE_DIRECTORY/opencode.json`):

```jsonc
{
  "mcp": {
    "gateway-memory": {
      "type": "local",
      "command": ["./bin/openbot-mcp", "--db", "/absolute/opencode-directory/mem/memory.db", "--single-owner", "--adapter", "wechat", "--user", "YOUR_USER_ID"],
      "enabled": true
    }
  }
}
```

If the binary is installed globally or in a different path, adjust `command`
accordingly. The `--db` flag must point to the same SQLite memory database used
by the gateway (default: `$OPENCODE_DIRECTORY/mem/memory.db`, overridden by
`MEMORY_STORE_PATH`). Use an absolute path and replace the adapter/user with the
stored channel and user ID. Cross-channel identity linking is not automatic.

`--single-owner`, a nonempty `--adapter`, and a nonempty `--user` are required.
These are operator-controlled process arguments, never model-supplied tool arguments.
Missing binding fails closed before opening the database. The example enables the
tool only after the operator has ensured instance isolation.

## Tools Exposed

| Tool | Description |
|------|-------------|
| `memory_search` | Scoped lexical search. On a migrated v2 archive it searches original message bodies with evidence offsets, project/time filters, cursor pagination, and index-coverage reporting. On a legacy-only archive it falls back to the Phase 0 FTS5/BM25 recall. `query` required (1-1024 characters); optional `days` (default 0: all history), `limit` (default 10), `project`, `cursor`. |
| `memory_get` | Returns the original stored bodies for up to 100 message IDs. Optional `budget_bytes` (default 32768, max 262144). Requires a migrated v2 archive. |
| `memory_scroll` | Pages one session chronologically by sequence. `session_id` required; optional `cursor`, `direction` (`forward`/`backward`, default `forward`), `limit` (default 20, max 100). Requires a migrated v2 archive. |
| `memory_list_projects` | Scoped project counts and last activity. Optional `days` (default 90). |
| `memory_recent` | Scoped chronological records. Optional `days` (default 7), `limit` (default 10). |
| `memory_health` | Operational status for the bound principal: scope mode, archive schema, tokenizer version, pending and failed index jobs, word-index completeness and lag, index updated-at, message totals, index lag, chunk count, and storage totals (blocks, compressed blocks, raw bytes, stored bytes, bytes saved). Takes no arguments. Reports no message bodies, no credentials, and no per-user labels, so it is safe to expose to a model. On a legacy-only archive it reports `schema: legacy` and states that indexing is not applicable until `memory-migrate` runs, rather than reporting zeros that would look like a healthy empty index. The storage lines report raw and stored bytes separately so the effect of compression is visible rather than assumed: a report that showed only stored bytes would look identical whether compression worked or was silently disabled. |
| `memory_space` | Where the archive's bytes go. Reports whole-file numbers (database file, WAL, SHM, free pages, full-text index pages) and, separately, the bound principal's logical sizes (block bytes, artifact bytes, embedding bytes). Optional `backup_path` reports a backup file's size in its own field. The two groups are never added together: every principal shares the same pages, so a per-principal share of the file would be a fiction, and the block and artifact totals are subsets of the file rather than additions to it. Slower than `memory_health` because the full-text index cost is measured with `dbstat`, which walks the page map, so it is a separate tool rather than more lines in the health report. When the SQLite build has no `dbstat`, `fts_bytes` is reported as unknown rather than as zero, because zero would read as "the index is empty". Reports no message bodies and no per-user labels. |

Days must be integers from 0 to 36500; 0 means all history. Limits must be integers
from 1 to 50. Invalid values are errors.

Search and recent results include record IDs, UTC timestamps, project/directory,
and request/response excerpts. Different records on the same project/day remain
distinct. Text excerpts share a 24,000-byte source-text budget per response,
excluding metadata and explanatory markers; UTF-8 boundaries are preserved.
Truncation and hitting the requested result limit are labeled. Excerpts are not
a complete archive export. A missing search match is not replaced with
unrelated recent records or project lists.

### v2 search, detail, and scroll

When the archive has been migrated to schema v4, `memory_search` searches the
chunked lexical index instead of the legacy recall tables. Results carry
`message_id`, `session_id`, `bytes=start-end` evidence offsets, a score, and a
`mode` that says which index answered the query:

| `mode` | Meaning |
| --- | --- |
| `word` | Every term is a plain ASCII word, answered by the word-tokenized index. This is the fast path. |
| `fts` | At least one term needs the trigram index: a CJK phrase, or a term containing a separator such as `.` or `/`. |
| `hybrid` | The query mixed both kinds of term, so both indexes were queried and the results merged. |
| `scan` | Every term was shorter than three characters, so neither index could match it and a bounded scoped scan was used. |

The response also reports coverage:

- `Coverage: PARTIAL` means index jobs are still pending, so a missing match may
  simply not be indexed yet. It is never reported as a confident "no match".
- `More results available. Repeat the call with cursor=...` means the page was
  truncated; the cursor is bound to the query, scope, and index generation, so a
  reindex invalidates it and the call must be restarted.
- One- and two-character terms cannot match either index. Those queries fall
  back to a bounded scoped scan and say so in the notice.
- `Word index: PARTIAL` means the word index is still being backfilled after a
  schema upgrade. A `word` or `hybrid` query can miss older messages until it
  finishes; `fts` queries are unaffected because the trigram index is complete.

`memory_get` returns the original body, not an excerpt, and re-authorizes every
requested ID against the caller's scope. IDs that are missing or belong to
another user are reported in a notice rather than silently dropped. The byte
budget is enforced across the whole response with rune-aligned truncation and a
continuation cursor.

`memory_scroll` walks a single session in `seq` order. It verifies that the
session belongs to the caller before reading anything.

Both v2 tools refuse a legacy-only archive with an explicit error instead of
returning an empty result, so an unmigrated deployment cannot look like an empty
memory.

## How It Works

```
OpenCode LLM  ──stdin/stdout──▶  openbot-mcp  ──SQLite──▶  memory.db
```

The server implements JSON-RPC 2.0 over stdio (the MCP "local" transport).
When the LLM calls a tool, the server queries the same SQLite database that the
gateway writes to, returning formatted results. The MCP process opens the
database read-only, so it can never mutate or purge history.

The gateway owns indexing. When strict v2 capture is enabled it runs a
background index worker that turns archived messages into chunks and FTS rows in
both the trigram index and the word index. When the message queue is drained the
same worker spends its idle time backfilling the word index for chunks that
predate the schema-v4 upgrade; that backfill is resumable, so a restart continues
where it stopped. The MCP server only reads those indexes; it never builds them.
If the gateway is not running, `memory_search` still works but reports
`Coverage: PARTIAL` until the backlog drains.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_MEMORY_DB` | (none) | MCP alternative to `--db`; explicit flag takes precedence |
| `MEMORY_STORE_PATH` | `<OPENCODE_DIRECTORY>/mem/memory.db` | Gateway database path; empty selects the default, not disabled capture |

## Tips

- The LLM will see these tools in its tool list and can invoke them whenever
  it judges a memory lookup is relevant—no keyword trigger needed.
- OpenCode must actually invoke a memory tool; this release does not implement
  the previously documented `DetectRecallIntent` passive injection.
- Durable v2 capture, scoped lexical search, detail retrieval, and scroll are
  implemented. Compression, knowledge extraction, semantic search, and
  source-backed handoff are subsequent phases of
  [the implementation plan](MEMORY_IMPLEMENTATION_PLAN.md).
- Search/database errors are reported as tool errors rather than disguised as
  no-match responses. Check gateway startup logs for the actual path and any
  migration failure; an unavailable memory store does not stop the legacy gateway.
