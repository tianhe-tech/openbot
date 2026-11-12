# Durable knowledge

The archive records what was said. Durable knowledge records what was *learned*
from it: a small set of scoped statements, each one traceable to the messages
that support it, injected into a new session so the assistant does not have to be
told the same thing twice.

This document covers the store, the extraction pipeline, the injection path, the
user commands, and the limits. It assumes [strict capture](MEMORY_MIGRATION.md) is
on, because extraction reads the archive that capture writes.

## Why a fact is not evidence

The single design rule behind everything here: **a fact is a claim, and a claim
is only as good as the messages that support it.**

A model asked to summarize a conversation will produce fluent statements that
sound like conclusions. Some of them are. Some are the model's own inference
dressed as the user's preference, and a few are simply wrong. Storing those
without provenance turns a hallucination into a permanent instruction that
applies to every future session — which is worse than not remembering at all,
because the user has no way to see it happen.

So every fact carries evidence, and the evidence is checked:

- The claimed message IDs must **exist** in the archive.
- They must belong to the **same principal** that owns the fact.
- A fact with no evidence is rejected outright, not stored with low confidence.
  Confidence is not proof, and a low-confidence row is still a row that gets
  injected.

`verifyKnowledgeEvidence` runs inside the write transaction, so a fabricated ID
and another principal's real ID fail the same way. `TestSaveKnowledgeFactRejectsForeignEvidence`
sets confidence to 1.0 in every case to make the point that certainty does not
substitute for evidence.

## Schema (version 8)

Three tables, because three questions are different:

| Table | Answers |
| --- | --- |
| `mem_v2_knowledge_facts` | what is believed, how strongly, at what scope, and whether it has been replaced |
| `mem_v2_knowledge_evidence` | which messages support it |
| `mem_v2_knowledge_jobs` | what still needs extracting |

Evidence is a table rather than a JSON column on the fact, because deletion has
to find facts **by message**. A JSON array cannot be indexed or joined, so a
deletion would have to scan every fact and parse it — and the one thing a
deletion must not do is miss a row.

`mem_v2_knowledge_jobs` is separate from `mem_v2_jobs` because that table's
uniqueness is `(kind, source_id, source_revision)` and its source is a message.
A knowledge job's source is a session, a turn, or an explicit request, and its
uniqueness is `(principal, source_kind, source_id)`.

Supersession is a column (`superseded_by`), not a delete. A correction adds a
row and points the old one at it, so the history of what was believed — and the
evidence for it — survives. `ForgetKnowledgeFact` un-supersedes whatever the
removed fact had replaced, so forgetting a correction restores the original
rather than leaving a hole.

The evidence foreign key is deliberately **not** `ON DELETE CASCADE`. A cascade
would make a deletion that forgot to remove evidence succeed silently; without
one, it aborts loudly. `TestArchiveMigrationUpgradesVersionOneArchive` asserts
the absence of the cascade for exactly that reason.

## Scopes

| Scope | Visible from | Use for |
| --- | --- | --- |
| `global` | every session of the principal | "answer in Chinese", "I use PowerShell" |
| `project` | sessions whose project key matches | "never touch the production database in this project" |
| `session` | the session it was extracted from | "write comments in English for this task" |

The plan's gate names the failure this prevents: **task-local instructions must
not become global preferences.** A model that hears "use English comments" once,
in one task, and promotes it to a global preference has made the assistant worse
in every other task. The drafter prompt tells the model to choose `session` when
it is unsure, and `TestKnowledgeScopeKeepsTaskLocalFactsLocal` asserts that a
project fact is invisible from a scope naming no project and from a different
project — with a global fact in the same fixture proving the filter is a filter
and not a blanket exclusion.

### The one place two callers disagree

Session facts are visible to the **review** surface and invisible to the
**injection** surface, and the difference is one option:

- `KnowledgeListOptions{IncludeSessionFacts: true}` — used by `/knowledge-list`
  and `/forget`. A session fact belongs to the principal, so the user must be
  able to see and delete it. A fact the user cannot see is a fact the user cannot
  delete, and the plan requires forgetting to remove derived facts.
- The default (false) — used by `KnowledgeIndex`. A session that is starting has
  no facts of its own, and another conversation's session facts must never be
  injected into this one.

This was a real defect, found by the tests: session facts were written but
invisible to every listing call, so `/knowledge-list` could never show one and
`/forget <statement>` could never resolve one. The rows were written, never
shown, and never deletable. `TestSessionFactsAreReviewableButNotInjectable` pins
both halves so the fix cannot be undone by accident.

## Extraction

```
opencode.Client                    internal/knowledge
  OnKnowledgeCandidate  ──────▶  Service.OnKnowledgeCandidate
    (handoff trigger)              ├─ recursion guard
                                   ├─ MinTurns check
                                   ├─ 30-minute thread dedup
                                   ├─ daily cap
                                   └─ asyncwork.Queue.Enqueue
                                            │
                                            ▼
                                      Service.run
                                        ├─ 1. RecentEvidence (bounded tail)
                                        ├─ 2. draft (model chain, per-model timeout)
                                        ├─ 3. persist (validate + verify + write)
                                        └─ 4. log accepted / rejected
```

Extraction is **not** run per message. It runs on a handoff — the point where a
session is being wrapped up and its content is settled — and it is bounded four
ways:

| Bound | Value | Why |
| --- | --- | --- |
| `MinTurns` | 4 | a two-message exchange has nothing durable in it |
| thread dedup | 30 min | a flapping session must not queue the same work repeatedly |
| daily cap | 20 facts / 24 h | a runaway loop must cost a bounded amount |
| `MaxEvidenceMessages` | 20 | the prompt is bounded regardless of session length |

The recursion guard is the one that matters most: any event whose adapter is
`knowledge` or whose thread starts with `knowledge-draft-` is dropped. Without
it, the drafter's own session would be captured, found meaningful, and extracted
again — a loop that spends money until the daily cap stops it.

Every fact is stamped `origin='extraction'`, so a later pass can tell its own
output from a user's `/remember`.

### Rejection is a verdict, not a retry

A fact that fails validation or evidence verification is **rejected**, not
retried. Retrying a fabricated evidence ID would produce the same rejection
forever, and the retry budget is for transient failures — a model timeout, a
provider error. `TestRunRejectsFabricatedEvidenceWithoutRetrying` pins that a
rejected fact leaves no row and does not come back.

### Failure isolation

Extraction failure never affects capture or search. They are separate paths that
share an archive: capture writes messages, the index worker chunks them, search
reads the chunks, and extraction reads the messages. A drafter that times out
leaves all three untouched. `TestRunFailureLeavesCaptureAndSearchIntact` indexes
and searches before and after a failing extraction and asserts both still work.

## Injection

At session start the client asks the store for a bounded knowledge block and
prepends it to the first message:

```
## 已知事实（从历史对话中提取，可能已过时；与当前指令冲突时以当前指令为准）
- [preference] 用户偏好中文回答 (verified, global)
- [constraint] 这个项目不要碰生产库 (observed, project)
```

The header is not decoration. Extracted facts **can** be stale — the user may
have changed their mind since — so the block says so and says which wins. A
knowledge block that presents itself as authoritative would make an old
preference override a current instruction, which is the opposite of helpful.

The budget is 3,000 bytes by default (12,000 hard ceiling), holding at most 40
facts, most-specific-first. 3,000 bytes is the conservative reading of the plan's
"1,500 tokens": it is under the token count for any tokenizer that maps a byte to
less than half a token, so it cannot overflow a context window by being wrong
about the tokenizer. Facts that do not fit are dropped and the index reports
`Truncated`.

The lookup has a 2-second timeout and returns `""` on error. It runs on the
request path, so a slow knowledge lookup must degrade to "no injection", never to
a slow reply.

## Commands

| Command | Effect |
| --- | --- |
| `/remember <statement>` | writes a global, verified preference with the newest archived message as evidence |
| `/forget <id or statement>` | removes a fact; if it had superseded another, that one becomes current again |
| `/knowledge-list` | lists every fact the principal owns, including session-scoped ones |
| `/knowledge-help` | the command reference |

These work whether or not `MEMORY_KNOWLEDGE_ENABLED` is on. They are explicit
user actions, not extraction, and a user who wants to record one thing should not
have to enable a background model pipeline to do it.

### A receipt is not a claim

`/remember` prints the fact ID **and** the evidence message ID it actually wrote.
If there is no archived message to cite, it refuses and says so:

```
❌ 无法记住：归档中还没有可引用的消息
```

The plan is explicit that "an assistant statement that it remembered something is
not a successful write receipt". A command that replied "记住了" without writing
anything would be indistinguishable from one that worked, and the user would find
out months later when the fact was not there.
`TestRememberWithoutEvidenceRefusesToClaimSuccess` pins the refusal.

One honest limitation: the command interceptor runs **before** the inbound
message is archived, so the newest message is the previous turn rather than the
command itself. That is still real evidence — it is a message the user actually
sent — but the receipt names it rather than implying the command text was
archived.

## Deletion

`cmd/memory-delete` removes knowledge in the same transaction as the messages, in
two phases:

1. **Before the messages go:** facts whose evidence is about to be deleted are
   selected from `mem_v2_knowledge_evidence`.
2. **After the session delete:** session-scoped facts whose session no longer
   exists are selected. They need the second phase because a session-scoped fact
   has no evidence row that names the session.

Knowledge jobs are removed by source. The evidence foreign key is not a cascade,
so a deletion that forgot either phase aborts rather than leaving a fact pointing
at a message that no longer exists.

See [scoped deletion](MEMORY_DELETION.md) for what survives in free pages, the
WAL, and backups.

## Limits

- **Extraction is off by default.** It spends model calls, so it is opt-in.
- **Facts can be stale.** The injected block says so. There is no automatic
  expiry; a fact stays until it is superseded or forgotten.
- **Redaction is a policy field, not a filter.** `redaction_policy` records what
  was applied; the drafter prompt instructs the model not to extract secrets, but
  a fact that names one is still the user's to forget.
- **No semantic matching.** Retrieval of facts is by scope, not by similarity.
  That is PR 8, and it is gated on a measured need.
- **The daily cap counts facts, not calls.** A day of extractions that all get
  rejected does not consume the cap, because a rejection is not a fact.

## Tests

| Test | Pins |
| --- | --- |
| `TestSaveKnowledgeFactRejectsForeignEvidence` | provenance: foreign, missing, and mixed evidence all rejected at confidence 1.0 |
| `TestSaveKnowledgeFactRequiresEvidence` | a fact with no evidence is not stored |
| `TestSaveKnowledgeFactIsIdempotent` | the same fact written twice is one row |
| `TestSupersedeKnowledgeFactKeepsHistory` | the replaced fact and its evidence survive |
| `TestSupersedeKnowledgeFactRefusesToFork` | a second correction of the same fact is rejected |
| `TestKnowledgeScopeKeepsTaskLocalFactsLocal` | project facts stay inside their project |
| `TestSessionFactsAreReviewableButNotInjectable` | the review/injection split |
| `TestKnowledgeIndexRespectsTheByteBudget` | the budget is a budget |
| `TestKnowledgeIndexIsEmptyWithoutFacts` | no facts means no block, not an empty header |
| `TestKnowledgeIndexPrefersTheMostSpecificFacts` | session before project before global |
| `TestForgetKnowledgeFactRestoresTheSupersededFact` | forgetting a correction restores the original |
| `TestDeleteScopeRemovesKnowledgeWithItsEvidence` | deletion reaches facts through evidence |
| `TestDeleteScopeRemovesKnowledgeJobsForItsSource` | deletion reaches jobs by source |
| `TestEnqueueKnowledgeJobCollapsesRepeats` | three enqueues are one job |
| `TestClaimKnowledgeJobsLeasesOnce` | two claims cannot return the same job |
| `TestFailKnowledgeJobParksAfterMaxAttempts` | a failing job stops retrying |
| `TestKnowledgeFactValidationRejectsMalformedFacts` | twelve malformed shapes |
| `TestForgetKnowledgeFactIsScoped` | one principal cannot forget another's fact |
| `TestCountKnowledgeFactsSinceIsScoped` | the daily cap counts one principal |
| `TestKnowledgeJobScopeReadsTheScopeBackFromTheSession` | a job's scope comes from its source, not from the job row |
| `TestLatestMessageIDIsScopedAndHonest` | `/remember` cites a real message, or nothing |
| `TestRecentEvidenceReturnsTheTailInChronologicalOrder` | the drafter sees the conversation in order |
| `TestRunStoresFactsWithRealEvidence` | the happy path end to end |
| `TestRunRejectsFabricatedEvidenceWithoutRetrying` | rejection is a verdict |
| `TestRunDefaultsTheScopeToSession` | an unscoped fact is session-scoped, not global |
| `TestRunSupersedesWhenTheDrafterSaysSo` | a correction replaces rather than duplicates |
| `TestRunWithNoEvidenceIsANoOp` | an empty archive produces no facts and no error |
| `TestRunFailureLeavesCaptureAndSearchIntact` | failure isolation |
| `TestOnKnowledgeCandidateRefusesItsOwnDraftingSessions` | the recursion guard |
| `TestOnKnowledgeCandidateIgnoresNonHandoffTriggers` | extraction is not per message |
| `TestOnKnowledgeCandidateRespectsMinTurns` | a two-turn conversation is not worth extracting |
| `TestOnKnowledgeCandidateDedupsTheSameThread` | one thread, one extraction per window |
| `TestOnKnowledgeCandidateRespectsTheDailyCap` | the cap is enforced before the model is called |
| `TestDraftFallsBackThroughTheModelChain` | the model chain is tried in order |
| `TestDraftStopsAtTheFirstSuccess` | a working model is not retried |
| `TestLoadKnowledgeIndexIsScopedAndBounded` | injection is scoped and inside the budget |
| `TestLoadKnowledgeIndexSurvivesANilStore` | a missing store degrades to no injection |
| `TestRememberProducesARealWriteReceipt` | the receipt names what was written |
| `TestRememberWithoutEvidenceRefusesToClaimSuccess` | no evidence, no claim |
| `TestForgetRemovesTheFactAndRestoresTheSupersededOne` | `/forget` by ID |
| `TestForgetResolvesAStatementToAnID` | `/forget` by statement |
| `TestHandleCommandIgnoresUnrelatedMessages` | the interceptor does not swallow chat |
| `TestKnowledgeHelpListsEveryCommand` | the help text cannot drift from the commands |
| `TestParseExtractionReplyToleratesProseAndFences` | a model that wraps its JSON still works |
| `TestParseExtractionReplyRejectsAReplyWithNoArray` | a reply with no facts is an error, not an empty success |
| `TestParseExtractionReplyDropsStatementsWithoutText` | an empty statement is not a fact |
| `TestBuildExtractionPromptCarriesTheEvidenceIDs` | the model is given the IDs it must cite |
| `TestKnowledgeExtractionRequiresStrictCapture` | the configuration dependency |
