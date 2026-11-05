package memstore

import (
	"context"
	"strings"
	"testing"
)

// TestSearchScanQueryPlan pins the query plan of the bounded scan.
//
// The scan is the fallback for one- and two-character terms, and it is the
// slowest path in the package by a wide margin: measured at 100k messages it
// costs 13.8 s p95 against 463 ms for the word path. That is not a small
// constant on a rarely used path, it is a query that cannot finish, so the plan
// has to be asserted rather than assumed.
//
// The specific thing being pinned is that the ORDER BY is served by an index.
// Without one, SQLite must materialize every message in the principal, payload
// included, and sort it before the LIMIT applies, so maxFallbackScan bounds the
// rows returned and not the work done. That is the difference between a bounded
// scan and an unbounded one wearing a bound.
func TestSearchScanQueryPlan(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)

	// The exact query searchScan runs, with the scope parameters inlined as
	// literals so the planner sees the same shape it sees at runtime. The
	// principal is a real one from the fixture.
	principal, err := SearchScope{Adapter: "test", UserID: "owner"}.principal()
	if err != nil {
		t.Fatal(err)
	}
	query := `EXPLAIN QUERY PLAN
		SELECT m.id,m.session_id,m.turn_id,m.role,m.created_at,m.seq,
			s.project_key,s.work_dir,b.codec,b.codec_version,b.raw_bytes,b.payload
		FROM mem_v2_messages m
		JOIN mem_v2_sessions s ON s.id=m.session_id
		JOIN mem_v2_blocks b ON b.id=m.block_id
		WHERE m.principal_id='` + principal + `'
			AND (''='' OR s.project_key='')
			AND (0=0 OR m.created_at>=0)
			AND (0=0 OR m.created_at<=0)
		ORDER BY m.created_at DESC, m.seq DESC
		LIMIT ` + itoa(maxFallbackScan)

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
			t.Fatalf("scan plan row: %v", err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(plan, "\n")
	t.Logf("scan query plan:\n%s", joined)

	// A temporary B-tree sort means the ORDER BY was not served by an index,
	// so the LIMIT did not bound the work.
	if strings.Contains(joined, "USE TEMP B-TREE FOR ORDER BY") {
		t.Fatalf("the bounded scan sorts every message before the LIMIT applies, "+
			"so maxFallbackScan bounds the rows returned and not the work done:\n%s", joined)
	}
	// The scan must walk mem_v2_messages through an index, not a full table
	// scan, or it reads every payload in the archive.
	if !strings.Contains(joined, "USING INDEX") {
		t.Fatalf("the bounded scan does not use an index on mem_v2_messages:\n%s", joined)
	}
}

// itoa is a tiny helper so the query above reads as one literal without
// pulling strconv into the import list for a single call.
func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var digits []byte
	for value > 0 {
		digits = append([]byte{byte('0' + value%10)}, digits...)
		value /= 10
	}
	return string(digits)
}
