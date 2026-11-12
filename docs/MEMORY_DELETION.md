# Memory deletion

This document describes what `memory-delete` removes, what it does not, and why
the two are different. It exists because the honest answer to "is my data gone?"
is more specific than "yes".

## What runs the deletion

`cmd/memory-delete` is the only caller of `memstore.DeleteScope`. It is a
separate binary on purpose:

- The MCP server opens the archive **read-only**. It is the process a model
  talks to, and a model must not be able to delete a user's history because a
  prompt asked it to. Adding deletion there would make the read-only guarantee
  conditional on a tool handler behaving, which is not a guarantee.
- The gateway is a long-running capture process holding the writer lease.
  Deletion is a rare, deliberate, operator-initiated act. Putting it behind an
  environment variable on a process that restarts automatically would make it
  possible to trigger by restarting with the wrong configuration.

```bash
memory-delete --db /var/lib/opencode-gateway/memory.db \
              --adapter dingtalk --user 0123456789 --confirm
```

| Flag | Meaning |
| --- | --- |
| `--db` | Archive path. Required. |
| `--adapter` | Adapter that owns the data. Required. |
| `--user` | User ID whose archive is deleted. Required. |
| `--session` | Restrict to one archive session ID. |
| `--project` | Restrict to sessions archived under one project key. |
| `--since` / `--until` | Restrict to messages by creation time (Unix ms). |
| `--confirm` | Required. Acknowledges the deletion cannot be undone. |

The command prints the `DeleteReport` as JSON, so an operator can record what
was removed:

```json
{
  "messages": 412,
  "chunks": 1877,
  "blocks": 398,
  "partials": 0,
  "tool_calls": 96,
  "attachments": 12,
  "attempts": 118,
  "turns": 118,
  "sessions": 7,
  "jobs": 530
}
```

The counts are reported rather than collapsed into a bare success because a
deletion that silently removed nothing looks exactly like one that worked.

## Scope is not optional

`--adapter` and `--user` are required, and `DeleteScope` refuses a scope with
either one empty or blank. An unscoped deletion is not a narrower version of a
scoped one — it is the whole archive. Every statement in the transaction carries
the principal predicate, so a caller cannot delete another principal's content
by supplying an ID it guessed: there is no code path where an ID alone is
enough.

`--confirm` is the one guard that cannot be satisfied by forgetting a filter.
Every other check narrows the selection, and a selection that is accidentally
empty selects everything: `--adapter` and `--user` with no session, project, or
time bound is the principal's entire history.

## What is removed, and in what order

The deletion runs as a single transaction on the writer's queue, so it cannot
interleave with capture. The order is forced by foreign keys, which are enforced
on the writer connection (`foreign_keys(1)`):

1. The selection is materialized into temporary tables. It is read many times
   and the rows are deleted partway through, so re-running the subquery after
   deleting from it would silently do no work.
2. FTS rows, by chunk rowid, from both `mem_v2_chunk_fts` and
   `mem_v2_chunk_word_fts`. They are addressed by rowid, so they must go before
   the chunk rows they point at.
3. Chunks, index jobs, messages.
4. Reconcile jobs, partials, tool calls, attachments, attempts.
5. The legacy import map, then turns, then sessions.
6. A sweep of blocks that no surviving message, partial, or tool call references.

Two details are worth stating because they are deliberate rather than
incidental:

- **Attempts are selected per turn, not per attempt.** A time-bounded deletion
  can remove some of a turn's messages and leave the rest. Requiring the *turn*
  to have no surviving message is the conservative reading, and the only one
  that cannot over-delete or break the foreign key from a surviving message.
- **The block sweep is by reference, not ownership.** A block can be shared: two
  tool calls with identical output resolve to the same content-addressed block.
  Deleting one principal's session must not remove a block another record still
  uses, so the sweep removes only blocks nothing points at. This also makes
  deletion resumable — a crash mid-transaction leaves the archive consistent,
  and a later run collects any block that became unreferenced.

## What is not removed

**This is logical deletion, not secure erasure.** SQLite `DELETE` removes rows
from the b-tree, but the bytes can survive in:

- **Free pages** inside the database file. The pages are marked reusable, not
  overwritten. `VACUUM` rewrites the file and is worth running afterwards, but
  it is not a guarantee either: it can leave remnants in the rollback journal or
  in the filesystem's own free space.
- **The write-ahead log.** Frames written before the deletion remain in the WAL
  until a checkpoint truncates it. `RunMaintenance` checkpoints periodically, but
  a deletion does not force one.
- **Backups.** Any copy taken before the deletion still contains the data. This
  includes the `--backup` file `memory-migrate` writes.
- **External artifacts.** Tool output and attachments are stored in the archive
  as blocks, so they are covered. Anything the gateway wrote outside the archive
  is not.

Physical erasure follows the storage and backup policy, not this program. An
operator who tells a user "your data has been securely erased" on the strength
of a successful `memory-delete` run is making a promise the software cannot
keep. The accurate statement is: the data is no longer readable through the
gateway, the MCP tools, or search, and it will be gone from the database file
once the WAL is checkpointed and the file is vacuumed, subject to whatever
backups exist.

## Search cursors are invalidated

A deletion changes the ranked dataset, so a search cursor issued before it must
be refused rather than silently applied to a different ranking. The transaction
bumps `mem_v2_index_state.updated_at`, and `Search` compares the cursor's
generation against it, returning `ErrSearchRestartRequired` on a mismatch.

The bump is monotonic (`MAX(updated_at+1, now)`) rather than a plain assignment
to the clock. A generation that can repeat is not a generation: an index write
and a deletion in the same millisecond would otherwise leave the value unchanged,
and a cursor issued between them would be accepted against a ranking that had
already moved. `IndexPending` uses the same rule, so both writers agree.

## Verifying a deletion

```bash
# The report says what was removed.
memory-delete --db memory.db --adapter dingtalk --user 0123456789 --confirm

# A second run reports nothing, which is the idempotence check.
memory-delete --db memory.db --adapter dingtalk --user 0123456789 --confirm
```

The second run should report every content count as zero. `blocks` may still be
non-zero if the first run left orphans behind, which is the sweep doing its job
rather than a failure.

The behaviour is pinned by `internal/memstore/delete_test.go` (8 tests) and
`cmd/memory-delete/main_test.go` (3 tests). The library tests prove the deletion
is scoped; the command tests prove the command passes the scope through. Those
are different failures — a command that ignored `--user` and deleted the whole
archive would pass every library test in the package, because the library was
never called with the wrong scope.
