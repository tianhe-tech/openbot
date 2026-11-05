package memstore

import (
	"context"
	"strings"
	"testing"
)

// TestSearchIndexQueryPlanPinsFTSDriver pins the join order of the production
// search query, which is the one thing about it that is not obvious from
// reading it.
//
// The query joins the FTS table to mem_v2_chunks, mem_v2_messages, and
// mem_v2_sessions. Only the FTS MATCH is selective; the joins filter and
// decorate its output. SQLite is nevertheless free to reorder an inner join by
// estimated cost, and schema v5 gave it a reason to: mem_v2_message_scan lets
// the planner find a principal's messages through an index, so it started
// driving from mem_v2_messages and probing the FTS table once per message. The
// plan changed from
//
//	SCAN mem_v2_chunk_fts VIRTUAL TABLE INDEX 0:M1
//	SEARCH c USING INTEGER PRIMARY KEY (rowid=?)
//
// to
//
//	SEARCH m USING INDEX mem_v2_message_scan (principal_id=?)
//	SEARCH c USING COVERING INDEX ... (message_id=?)
//	SCAN mem_v2_chunk_fts VIRTUAL TABLE INDEX 0:=M1
//
// which is the same rows in the worst possible order. Measured at 20k messages
// the inner query went from 75.20 ms to 104471 ms, a factor of 1388. Nothing
// caught it because no test asserted this plan, and the full suite passed
// throughout.
//
// The fix is CROSS JOIN on the FTS table, which SQLite documents as the way to
// prevent reordering. This test is what makes that fix stick: it fails if the
// FTS table stops driving, whatever the reason.
func TestSearchIndexQueryPlanPinsFTSDriver(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)

	principal, err := SearchScope{Adapter: "test", UserID: "owner"}.principal()
	if err != nil {
		t.Fatal(err)
	}
	// The inner query from searchIndex, with the scope parameters inlined as
	// literals so the planner sees the shape it sees at runtime.
	query := `EXPLAIN QUERY PLAN
		SELECT mem_v2_chunk_fts.rowid, bm25(mem_v2_chunk_fts)
		FROM mem_v2_chunk_fts
		CROSS JOIN mem_v2_chunks c ON c.id=mem_v2_chunk_fts.rowid
		CROSS JOIN mem_v2_messages m ON m.id=c.message_id
		CROSS JOIN mem_v2_sessions s ON s.id=m.session_id
		WHERE mem_v2_chunk_fts MATCH 'gateway'
			AND m.principal_id='` + principal + `'
			AND (''='' OR s.project_key='')
			AND (0=0 OR m.created_at>=0)
			AND (0=0 OR m.created_at<=0)
		ORDER BY bm25(mem_v2_chunk_fts)
		LIMIT ` + itoa(maxSearchCandidates)

	rows, err := writer.store.db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var id, parent, notUsed int
		var detail string
		if err := rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
			t.Fatalf("plan row: %v", err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n")
	t.Logf("search query plan:\n%s", joined)

	// The FTS table must be the first step. If it is not, the query is probing
	// the index once per message in the principal instead of once per match.
	if len(plan) == 0 || !strings.Contains(plan[0], "mem_v2_chunk_fts") {
		t.Fatalf("the FTS table does not drive the search query, so the join "+
			"probes the index once per message instead of once per match:\n%s", joined)
	}
	// Driving from mem_v2_messages is the specific regression this pins. It is
	// named separately because it is the one the planner actually chose, and a
	// future index could make it choose it again.
	if strings.Contains(joined, "SEARCH m USING INDEX mem_v2_message_scan") {
		t.Fatalf("the search query is driven by mem_v2_message_scan, which "+
			"probes the FTS index once per message in the principal:\n%s", joined)
	}
}
