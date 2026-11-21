package memstore

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// DeleteOptions selects what one scoped deletion removes.
//
// Scope is mandatory and is the only thing that decides whose rows may be
// touched. Every statement below carries the principal predicate, so a caller
// cannot delete another principal's content by supplying an ID it guessed:
// there is no code path where an ID alone is enough. SessionID, Project, Since,
// and Until narrow the selection further; with none of them set, the whole
// principal's conversation archive is removed.
type DeleteOptions struct {
	Scope     SearchScope
	SessionID string
}

// DeleteReport counts what one scoped deletion removed.
//
// The counts are reported rather than collapsed into a bare error because a
// deletion that silently removed nothing looks exactly like one that worked,
// and the difference matters to whoever asked for it.
//
// Blocks is the one count that is not attributable to this call. It is every
// unreferenced block of the principal that was swept, which may include blocks
// orphaned by an earlier interrupted deletion. That is deliberate: it makes
// deletion resumable, because a crash between deleting a message and deleting
// its block leaves an orphan that the next call collects. Presenting it as a
// per-call total would be a lie about where those bytes came from.
type DeleteReport struct {
	Messages    int64 `json:"messages"`
	Chunks      int64 `json:"chunks"`
	Blocks      int64 `json:"blocks"`
	Partials    int64 `json:"partials"`
	ToolCalls   int64 `json:"tool_calls"`
	Attachments int64 `json:"attachments"`
	Attempts    int64 `json:"attempts"`
	Turns       int64 `json:"turns"`
	Sessions    int64 `json:"sessions"`
	Jobs        int64 `json:"jobs"`
	// Knowledge counts derived facts removed because their evidence was
	// deleted. It is reported separately from Messages because the two are
	// different kinds of loss: a message is the record, a fact is a conclusion
	// drawn from it, and an operator who deletes a session should be able to
	// see that conclusions went with it.
	Knowledge int64 `json:"knowledge"`
}

// Empty reports whether the deletion found nothing to remove.
func (report DeleteReport) Empty() bool {
	return report.Messages == 0 && report.Chunks == 0 && report.Partials == 0 &&
		report.ToolCalls == 0 && report.Attachments == 0 && report.Attempts == 0 &&
		report.Turns == 0 && report.Sessions == 0 && report.Jobs == 0 &&
		report.Knowledge == 0
}

// deleteCommitTimeout returns the deadline for one scoped deletion.
//
// Deletion is a maintenance operation with no caller waiting on a latency
// budget, so this deadline exists only to catch a wedged commit rather than to
// bound normal work. It scales with the number of messages because each one
// costs a chunk delete, two FTS deletes, and a row delete, and it has a floor
// because the block sweep is a full scan of the principal's blocks no matter
// how few messages are being removed.
func deleteCommitTimeout(messages int64) time.Duration {
	timeout := 30*time.Second + time.Duration(messages)*time.Millisecond
	if timeout > 10*time.Minute {
		return 10 * time.Minute
	}
	return timeout
}

// DeleteScope removes one principal's archived conversation data in a single
// transaction.
//
// The plan requires "explicit scoped deletion as a transaction over messages,
// chunks, FTS, derived sources/facts, jobs, and block references. Repack shared
// blocks when necessary; never delete another record's content." This is that
// transaction. It runs on the writer's queue rather than directly on the
// database so it cannot interleave with capture: a deletion that raced a
// concurrent SaveMessage would either delete a message the writer was about to
// reference or leave a chunk behind, and both are silent.
//
// The order below is forced by the foreign keys, which are enabled on the
// writer connection. Children go before parents: FTS and chunks before
// messages, messages before attempts, attempts before turns, turns before
// sessions, and everything before blocks. Getting the order wrong does not
// corrupt anything — SQLite refuses the statement — but it does mean a
// reordering that looks harmless will fail at runtime rather than at review.
//
// Blocks are swept last and by reference rather than by ownership. A block can
// be shared: two tool calls that retained the same output point at one block,
// and a partial snapshot's block is replaced rather than reused. Deleting a
// block because the message that happened to be deleted referenced it would
// destroy the surviving message's text, so the sweep removes only blocks that
// nothing references any more.
func (writer *ArchiveWriter) DeleteScope(ctx context.Context, options DeleteOptions) (DeleteReport, error) {
	principal, err := options.Scope.principal()
	if err != nil {
		return DeleteReport{}, err
	}
	clause, args := deleteMessageFilter(options, principal)

	// Count before submitting so the commit deadline can scale with the work.
	// This read is outside the transaction and could in principle disagree with
	// the count inside it; that is acceptable because the deadline is a safety
	// net, not a budget the operation is expected to approach.
	var selected int64
	if err := writer.store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM mem_v2_messages m WHERE `+clause, args...).Scan(&selected); err != nil {
		return DeleteReport{}, fmt.Errorf("memstore: count messages to delete: %w", err)
	}

	var report DeleteReport
	err = writer.submitWithTimeout(ctx, 0, deleteCommitTimeout(selected), func(ctx context.Context, tx *sql.Tx) error {
		return deleteScopeTx(ctx, tx, options, principal, clause, args, &report)
	})
	if err != nil {
		return DeleteReport{}, err
	}
	return report, nil
}

// deleteMessageFilter builds the WHERE clause that selects the messages to
// remove. It always begins with the principal predicate, so no caller-supplied
// value can widen the selection beyond one principal.
func deleteMessageFilter(options DeleteOptions, principal string) (string, []any) {
	clause := "m.principal_id=?"
	args := []any{principal}
	if options.SessionID != "" {
		clause += " AND m.session_id=?"
		args = append(args, options.SessionID)
	}
	if options.Scope.Project != "" {
		clause += " AND m.session_id IN (SELECT id FROM mem_v2_sessions WHERE principal_id=? AND project_key=?)"
		args = append(args, principal, options.Scope.Project)
	}
	if options.Scope.Since > 0 {
		clause += " AND m.created_at>=?"
		args = append(args, options.Scope.Since)
	}
	if options.Scope.Until > 0 {
		clause += " AND m.created_at<=?"
		args = append(args, options.Scope.Until)
	}
	return clause, args
}

func deleteScopeTx(ctx context.Context, tx *sql.Tx, options DeleteOptions, principal, clause string, args []any, report *DeleteReport) error {
	// The selection is materialized into temp tables because it is read many
	// times and the rows it names are deleted partway through. Re-running the
	// subquery after the messages are gone would return nothing, so every later
	// statement would silently do no work.
	//
	// Temp tables are per-connection and the writer holds exactly one
	// connection, so these are private to this transaction. They are created
	// with IF NOT EXISTS and cleared first because a rolled-back transaction
	// removes the table but a committed one leaves it behind for the next call.
	for _, statement := range []string{
		`CREATE TEMP TABLE IF NOT EXISTS mem_delete_messages(id TEXT PRIMARY KEY)`,
		`CREATE TEMP TABLE IF NOT EXISTS mem_delete_turns(id TEXT PRIMARY KEY)`,
		`CREATE TEMP TABLE IF NOT EXISTS mem_delete_attempts(id TEXT PRIMARY KEY)`,
		`CREATE TEMP TABLE IF NOT EXISTS mem_delete_sessions(id TEXT PRIMARY KEY)`,
		`DELETE FROM mem_delete_messages`,
		`DELETE FROM mem_delete_turns`,
		`DELETE FROM mem_delete_attempts`,
		`DELETE FROM mem_delete_sessions`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("memstore: prepare deletion: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO mem_delete_messages(id) SELECT m.id FROM mem_v2_messages m WHERE `+clause, args...); err != nil {
		return fmt.Errorf("memstore: select messages to delete: %w", err)
	}

	// Turns are derived from the selected messages, plus every turn of the
	// session when the deletion is session-scoped. The second half matters
	// because a turn whose messages were never archived — a dispatch that
	// failed before the first message was saved — would otherwise survive its
	// own session and keep the session row alive with it.
	turnArgs := append(append([]any{}, args...), principal)
	turnSQL := `INSERT INTO mem_delete_turns(id) SELECT DISTINCT m.turn_id FROM mem_v2_messages m WHERE ` + clause
	if options.SessionID != "" {
		turnSQL += ` UNION SELECT id FROM mem_v2_turns WHERE principal_id=? AND session_id=?`
		turnArgs = append(turnArgs, options.SessionID)
	}
	if _, err := tx.ExecContext(ctx, turnSQL, turnArgs...); err != nil {
		return fmt.Errorf("memstore: select turns to delete: %w", err)
	}

	// An attempt is removed only when its whole turn is being emptied. The
	// condition is written against the turn rather than the attempt because a
	// time-bounded deletion can remove some of a turn's messages and leave the
	// rest; in that case the turn survives, and deleting an attempt that still
	// has a surviving message would break the foreign key from that message.
	// Requiring the turn to have no surviving message at all is the
	// conservative reading, and it is also the only one that cannot over-delete:
	// an attempt whose turn is only partly selected keeps its partial snapshot
	// and its tool calls, which is what "delete these messages" should mean.
	if _, err := tx.ExecContext(ctx, `INSERT INTO mem_delete_attempts(id)
		SELECT a.id FROM mem_v2_attempts a
		WHERE a.turn_id IN (SELECT id FROM mem_delete_turns)
		AND NOT EXISTS (SELECT 1 FROM mem_v2_messages m
			WHERE m.turn_id=a.turn_id AND m.id NOT IN (SELECT id FROM mem_delete_messages))`); err != nil {
		return fmt.Errorf("memstore: select attempts to delete: %w", err)
	}

	// Sessions are collected from the selected messages and from the selected
	// turns, so a session whose only content was a turn with no archived
	// message is still a candidate. They are only removed below if nothing
	// survives in them.
	sessionArgs := append(append([]any{}, args...), principal)
	sessionSQL := `INSERT INTO mem_delete_sessions(id)
		SELECT DISTINCT m.session_id FROM mem_v2_messages m WHERE ` + clause + `
		UNION SELECT t.session_id FROM mem_v2_turns t WHERE t.id IN (SELECT id FROM mem_delete_turns)`
	if options.SessionID != "" {
		sessionSQL += ` UNION SELECT id FROM mem_v2_sessions WHERE principal_id=? AND id=?`
		sessionArgs = append(sessionArgs, options.SessionID)
	}
	if _, err := tx.ExecContext(ctx, sessionSQL, sessionArgs...); err != nil {
		return fmt.Errorf("memstore: select sessions to delete: %w", err)
	}

	// FTS rows are addressed by chunk rowid, so they must go before the chunk
	// rows that supply those rowids.
	//
	// This works on a contentless index too, provided it was created with
	// contentless_delete=1. A plain contentless table (content='' alone)
	// rejects DELETE, which is why the option matters; the probe in
	// fts_contentless_probe_test.go pins that the pinned SQLite build supports
	// it. So the deletion path is not what blocks adopting a contentless
	// index — the excerpt reconstruction is.
	for _, table := range []string{"mem_v2_chunk_fts", "mem_v2_chunk_word_fts"} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE rowid IN
			(SELECT c.id FROM mem_v2_chunks c
			 JOIN mem_delete_messages d ON d.id=c.message_id)`); err != nil {
			return fmt.Errorf("memstore: delete %s: %w", table, err)
		}
	}

	result, err := tx.ExecContext(ctx, `DELETE FROM mem_v2_chunks
		WHERE message_id IN (SELECT id FROM mem_delete_messages)`)
	if err != nil {
		return fmt.Errorf("memstore: delete chunks: %w", err)
	}
	report.Chunks, _ = result.RowsAffected()

	// Derived knowledge goes before the messages it cites, because
	// mem_v2_knowledge_evidence has a foreign key onto mem_v2_messages and the
	// writer connection enables foreign_keys. This is the obligation the plan
	// recorded when deletion was implemented without knowledge tables: "they
	// must be added to the same transaction when PR 7 lands, or a deletion will
	// leave knowledge rows pointing at messages that no longer exist."
	//
	// A fact is removed when any of its evidence is being removed, not only
	// when all of it is. A fact that cited three messages and lost one is no
	// longer supported by the archive, and keeping it would mean keeping a
	// conclusion whose provenance is partly gone — which is exactly the state
	// the provenance requirement exists to prevent. The alternative, deleting
	// only the evidence row and keeping the fact, would leave a fact that
	// claims support it cannot show.
	//
	// Session-scoped facts are handled separately, after the sessions are
	// actually removed. They cannot be selected here: mem_delete_sessions is a
	// candidate list, and a time-bounded deletion can name a session without
	// emptying it, so selecting on it would delete a fact whose session
	// survives.
	if _, err := tx.ExecContext(ctx, `CREATE TEMP TABLE IF NOT EXISTS mem_delete_knowledge(id TEXT PRIMARY KEY)`); err != nil {
		return fmt.Errorf("memstore: prepare knowledge deletion: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM mem_delete_knowledge`); err != nil {
		return fmt.Errorf("memstore: prepare knowledge deletion: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO mem_delete_knowledge(id)
		SELECT DISTINCT e.fact_id FROM mem_v2_knowledge_evidence e
		WHERE e.message_id IN (SELECT id FROM mem_delete_messages)`); err != nil {
		return fmt.Errorf("memstore: select knowledge to delete: %w", err)
	}
	if err := deleteSelectedKnowledge(ctx, tx, principal, report); err != nil {
		return err
	}

	// Index jobs are keyed by message ID and reconcile jobs by attempt ID.
	result, err = tx.ExecContext(ctx, `DELETE FROM mem_v2_knowledge_jobs
		WHERE principal_id=? AND source_kind='session' AND source_id IN (
			SELECT a.upstream_session_id FROM mem_v2_attempts a WHERE a.turn_id IN (SELECT id FROM mem_delete_turns))`, principal)
	if err != nil {
		return fmt.Errorf("memstore: delete upstream knowledge jobs: %w", err)
	}
	if affected, _ := result.RowsAffected(); affected > 0 {
		report.Jobs += affected
	}
	// mem_v2_jobs has no principal column, which is safe here because both IDs
	// are content hashes that already include the principal: a job can only be
	// selected by an ID that belongs to the principal being deleted.
	result, err = tx.ExecContext(ctx, `DELETE FROM mem_v2_jobs
		WHERE kind='index' AND source_id IN (SELECT id FROM mem_delete_messages)`)
	if err != nil {
		return fmt.Errorf("memstore: delete index jobs: %w", err)
	}
	if affected, _ := result.RowsAffected(); affected > 0 {
		report.Jobs += affected
	}

	result, err = tx.ExecContext(ctx, `DELETE FROM mem_v2_messages
		WHERE id IN (SELECT id FROM mem_delete_messages)`)
	if err != nil {
		return fmt.Errorf("memstore: delete messages: %w", err)
	}
	report.Messages, _ = result.RowsAffected()

	result, err = tx.ExecContext(ctx, `DELETE FROM mem_v2_jobs
		WHERE kind='reconcile' AND source_id IN (SELECT id FROM mem_delete_attempts)`)
	if err != nil {
		return fmt.Errorf("memstore: delete reconcile jobs: %w", err)
	}
	if affected, _ := result.RowsAffected(); affected > 0 {
		report.Jobs += affected
	}

	for _, target := range []struct {
		table string
		count *int64
	}{
		{"mem_v2_partials", &report.Partials},
		{"mem_v2_tool_calls", &report.ToolCalls},
		{"mem_v2_attachments", &report.Attachments},
	} {
		result, err := tx.ExecContext(ctx, `DELETE FROM `+target.table+`
			WHERE attempt_id IN (SELECT id FROM mem_delete_attempts)`)
		if err != nil {
			return fmt.Errorf("memstore: delete %s: %w", target.table, err)
		}
		*target.count, _ = result.RowsAffected()
	}

	result, err = tx.ExecContext(ctx, `DELETE FROM mem_v2_attempts
		WHERE id IN (SELECT id FROM mem_delete_attempts)`)
	if err != nil {
		return fmt.Errorf("memstore: delete attempts: %w", err)
	}
	report.Attempts, _ = result.RowsAffected()

	// The legacy map is a foreign key onto turns, so it has to go before them
	// even though it holds no conversation content of its own.
	if _, err := tx.ExecContext(ctx, `DELETE FROM mem_v2_legacy_map
		WHERE turn_id IN (SELECT id FROM mem_delete_turns)`); err != nil {
		return fmt.Errorf("memstore: delete legacy map: %w", err)
	}

	// A turn is removed only when nothing still points at it. The checks are
	// written out rather than relying on the foreign keys to fail, because a
	// failure would abort the whole deletion over one surviving partial, and
	// the correct behaviour is to keep the turn and finish the rest.
	result, err = tx.ExecContext(ctx, `DELETE FROM mem_v2_turns
		WHERE id IN (SELECT id FROM mem_delete_turns)
		AND NOT EXISTS (SELECT 1 FROM mem_v2_messages m WHERE m.turn_id=mem_v2_turns.id)
		AND NOT EXISTS (SELECT 1 FROM mem_v2_attempts a WHERE a.turn_id=mem_v2_turns.id)
		AND NOT EXISTS (SELECT 1 FROM mem_v2_partials p WHERE p.turn_id=mem_v2_turns.id)
		AND NOT EXISTS (SELECT 1 FROM mem_v2_tool_calls c WHERE c.turn_id=mem_v2_turns.id)
		AND NOT EXISTS (SELECT 1 FROM mem_v2_attachments t WHERE t.turn_id=mem_v2_turns.id)`)
	if err != nil {
		return fmt.Errorf("memstore: delete turns: %w", err)
	}
	report.Turns, _ = result.RowsAffected()

	result, err = tx.ExecContext(ctx, `DELETE FROM mem_v2_sessions
		WHERE principal_id=?
		AND id IN (SELECT id FROM mem_delete_sessions)
		AND NOT EXISTS (SELECT 1 FROM mem_v2_turns t WHERE t.session_id=mem_v2_sessions.id)
		AND NOT EXISTS (SELECT 1 FROM mem_v2_messages m WHERE m.session_id=mem_v2_sessions.id)
		AND NOT EXISTS (SELECT 1 FROM mem_v2_partials p WHERE p.session_id=mem_v2_sessions.id)
		AND NOT EXISTS (SELECT 1 FROM mem_v2_tool_calls c WHERE c.session_id=mem_v2_sessions.id)
		AND NOT EXISTS (SELECT 1 FROM mem_v2_attachments a WHERE a.session_id=mem_v2_sessions.id)`,
		principal)
	if err != nil {
		return fmt.Errorf("memstore: delete sessions: %w", err)
	}
	report.Sessions, _ = result.RowsAffected()

	// Session-scoped facts whose session is now gone can never be read again:
	// the visibility predicate requires the session to match, and nothing will
	// ever match a session row that no longer exists. Keeping them would leave
	// the statement in the archive as an unreachable row, which is a leak of
	// exactly the text the user asked to forget.
	//
	// This runs after the session delete rather than before it because the
	// candidate list is not the same as the removed list: a time-bounded
	// deletion names a session without necessarily emptying it, and a fact
	// scoped to a surviving session must survive with it.
	if _, err := tx.ExecContext(ctx, `DELETE FROM mem_delete_knowledge`); err != nil {
		return fmt.Errorf("memstore: prepare knowledge deletion: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO mem_delete_knowledge(id)
		SELECT f.id FROM mem_v2_knowledge_facts f
		WHERE f.principal_id=? AND f.scope='session'
		AND f.session_id != ''
		AND NOT EXISTS (SELECT 1 FROM mem_v2_sessions s WHERE s.id=f.session_id AND s.principal_id=f.principal_id)`,
		principal); err != nil {
		return fmt.Errorf("memstore: select orphaned knowledge: %w", err)
	}
	if err := deleteSelectedKnowledge(ctx, tx, principal, report); err != nil {
		return err
	}

	// Extraction jobs are keyed by session or turn, so they are removed with
	// the thing they would have extracted from. Leaving them would queue a
	// model call against a source that no longer exists, and the extraction
	// would fail on every retry until it exhausted its attempts.
	result, err = tx.ExecContext(ctx, `DELETE FROM mem_v2_knowledge_jobs
		WHERE principal_id=? AND (
			(source_kind='session' AND source_id IN (SELECT id FROM mem_delete_sessions))
			OR (source_kind='turn' AND source_id IN (SELECT id FROM mem_delete_turns)))`,
		principal)
	if err != nil {
		return fmt.Errorf("memstore: delete knowledge jobs: %w", err)
	}
	if affected, _ := result.RowsAffected(); affected > 0 {
		report.Jobs += affected
	}

	// The block sweep is by reference, not by ownership. A block survives if
	// any message, partial, or retained tool output still points at it, which
	// is what keeps a shared artifact block alive when only one of the two
	// calls that share it is deleted.
	result, err = tx.ExecContext(ctx, `DELETE FROM mem_v2_blocks
		WHERE principal_id=?
		AND id NOT IN (SELECT block_id FROM mem_v2_messages WHERE principal_id=?)
		AND id NOT IN (SELECT block_id FROM mem_v2_partials WHERE principal_id=?)
		AND id NOT IN (SELECT artifact_ref FROM mem_v2_tool_calls
			WHERE principal_id=? AND artifact_ref != '')`,
		principal, principal, principal, principal)
	if err != nil {
		return fmt.Errorf("memstore: sweep blocks: %w", err)
	}
	report.Blocks, _ = result.RowsAffected()

	// A deletion changes the ranked dataset, so any search cursor issued before
	// it now points into a ranking that no longer exists. The cursor carries the
	// index generation and Search refuses a cursor whose generation is stale, so
	// bumping updated_at here is what turns "the page after this one" into an
	// explicit restart instead of a silently skipped or duplicated page.
	//
	// The bump is monotonic rather than a plain assignment to the clock. A
	// generation that can repeat is not a generation: an index write and a
	// deletion in the same millisecond would leave updated_at unchanged, and a
	// cursor issued between them would be accepted against a ranking that had
	// already changed. MAX(updated_at+1, now) makes every change strictly
	// greater than the last one while still tracking wall time.
	//
	// This is done unconditionally, even when nothing was deleted, because the
	// cost is one row update and the alternative is deciding whether a
	// zero-row deletion still invalidated a cursor. It did not, but the
	// reasoning that says so is more fragile than the write.
	if _, err := tx.ExecContext(ctx,
		`UPDATE mem_v2_index_state SET updated_at=MAX(updated_at+1, ?) WHERE id=1`,
		time.Now().UTC().UnixMilli()); err != nil {
		return fmt.Errorf("memstore: invalidate search cursors: %w", err)
	}

	return nil
}

// deleteSelectedKnowledge removes the facts named in mem_delete_knowledge and
// their evidence, and adds the count to the report.
//
// It is a helper rather than inline code because the deletion runs it twice:
// once for facts whose evidence is being deleted, and once for session-scoped
// facts whose session is gone. Both passes need the same three statements in
// the same order, and duplicating them is how the second pass would drift from
// the first.
//
// The supersession pointer is cleared on surviving facts before the delete.
// mem_v2_knowledge_facts.superseded_by is a self-reference with a foreign key,
// so a fact that survives cannot be left pointing at one that is about to be
// removed. Clearing it makes the surviving fact current again, which is the
// honest reading: the correction it recorded is gone, so what it replaced is
// what is believed.
func deleteSelectedKnowledge(ctx context.Context, tx *sql.Tx, principal string, report *DeleteReport) error {
	if _, err := tx.ExecContext(ctx, `UPDATE mem_v2_knowledge_facts SET superseded_by=NULL
		WHERE principal_id=? AND superseded_by IN (SELECT id FROM mem_delete_knowledge)`,
		principal); err != nil {
		return fmt.Errorf("memstore: clear knowledge supersession: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM mem_v2_knowledge_evidence
		WHERE fact_id IN (SELECT id FROM mem_delete_knowledge)`); err != nil {
		return fmt.Errorf("memstore: delete knowledge evidence: %w", err)
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM mem_v2_knowledge_facts
		WHERE principal_id=? AND id IN (SELECT id FROM mem_delete_knowledge)`, principal)
	if err != nil {
		return fmt.Errorf("memstore: delete knowledge facts: %w", err)
	}
	affected, _ := result.RowsAffected()
	report.Knowledge += affected
	return nil
}
