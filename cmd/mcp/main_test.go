package main

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/user/opencode-gateway/internal/memstore"
)

func memoryServerForTest(t *testing.T, records []memstore.MemRecord) *mcpServer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "memory.db")
	writer, err := memstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range records {
		if err := writer.Record(record); err != nil {
			writer.Close()
			t.Fatal(err)
		}
	}
	writer.Close()
	reader, err := memstore.OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })
	return &mcpServer{store: reader, singleOwner: true, adapter: "test", userID: "owner"}
}

func TestMemoryToolsScopeAndEvidence(t *testing.T) {
	now := time.Now()
	var records []memstore.MemRecord
	for index := 0; index < 12; index++ {
		records = append(records, memstore.MemRecord{
			ID: fmt.Sprintf("owner-%02d", index), Adapter: "test", UserID: "owner", Project: "same-project",
			Request: "original request", Response: fmt.Sprintf("needle answer-%02d", index), Ts: now,
		})
	}
	records = append(records,
		memstore.MemRecord{ID: "old", Adapter: "test", UserID: "owner", Request: "old request", Response: "needle historic evidence", Ts: now.AddDate(0, 0, -120), Strength: 0.001},
		memstore.MemRecord{ID: "foreign-user", Adapter: "test", UserID: "other", Project: "foreign-project", Response: "needle foreign-evidence"},
		memstore.MemRecord{ID: "foreign-adapter", Adapter: "other", UserID: "owner", Project: "foreign-project", Response: "needle foreign-evidence"},
	)
	server := memoryServerForTest(t, records)
	search, err := server.toolMemorySearch(map[string]interface{}{"query": "needle", "limit": 50, "user_id": "other"})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"original request", "historic evidence", "Record owner-00", "Record owner-11", "days=0"} {
		if !strings.Contains(search, expected) {
			t.Fatalf("search missing %q: %s", expected, search)
		}
	}
	recent, err := server.toolRecent(map[string]interface{}{"days": 0, "limit": 50})
	if err != nil {
		t.Fatal(err)
	}
	projects, err := server.toolListProjects(map[string]interface{}{"days": 0})
	if err != nil {
		t.Fatal(err)
	}
	for _, output := range []string{search, recent, projects} {
		if strings.Contains(output, "foreign-") {
			t.Fatalf("scope leaked: %s", output)
		}
	}
	if !strings.Contains(recent, "historic evidence") {
		t.Fatal("days=0 recent excluded old history")
	}
	filtered, err := server.toolMemorySearch(map[string]interface{}{"query": "needle", "days": 7, "limit": 50})
	if err != nil || strings.Contains(filtered, "historic evidence") {
		t.Fatalf("explicit time filter failed: %s, %v", filtered, err)
	}
	missing, err := server.toolMemorySearch(map[string]interface{}{"query": "zzzzzzzzzzzz"})
	if err != nil || !strings.Contains(missing, "No matching memory records") || strings.Contains(missing, "original request") {
		t.Fatalf("unrelated records substituted for a miss: %s, %v", missing, err)
	}
}

func TestMemoryToolsFailClosed(t *testing.T) {
	for _, server := range []*mcpServer{
		{},
		{singleOwner: true, adapter: "test"},
		{singleOwner: true, userID: "owner"},
		{adapter: "test", userID: "owner"},
	} {
		for _, call := range []func(map[string]interface{}) (string, error){server.toolMemorySearch, server.toolRecent, server.toolListProjects, server.toolMemoryHealth} {
			if _, err := call(map[string]interface{}{"query": "needle", "adapter": "test", "user_id": "owner"}); err == nil {
				t.Fatal("unbound memory tool accepted caller-supplied scope")
			}
		}
	}
}

func TestMemoryToolsRejectInvalidArguments(t *testing.T) {
	server := &mcpServer{singleOwner: true, adapter: "test", userID: "owner"}
	for _, invalid := range []interface{}{-1, 36501, 1.5, "7", nil, math.Inf(1), math.NaN()} {
		args := map[string]interface{}{"query": "needle", "days": invalid}
		for _, call := range []func(map[string]interface{}) (string, error){server.toolMemorySearch, server.toolRecent, server.toolListProjects} {
			if _, err := call(args); err == nil {
				t.Fatalf("accepted invalid days %v", invalid)
			}
		}
	}
	for _, invalid := range []interface{}{0, -1, 51, 2.5, "10"} {
		args := map[string]interface{}{"query": "needle", "limit": invalid}
		if _, err := server.toolMemorySearch(args); err == nil {
			t.Fatalf("accepted invalid search limit %v", invalid)
		}
		if _, err := server.toolRecent(args); err == nil {
			t.Fatalf("accepted invalid recent limit %v", invalid)
		}
	}
}

func TestEvidenceExcerptsAreBoundedAndUTF8(t *testing.T) {
	text := strings.Repeat("\u914d\u7f6e", 20000)
	output := formatSearchResult([]memstore.MemRecord{{ID: "long", Request: text, Response: text}}, 1)
	if !utf8.ValidString(output) || !strings.Contains(output, "excerpt truncated") || !strings.Contains(output, "Result limit reached") || len(output) > 26000 {
		t.Fatalf("invalid excerpt: bytes=%d, valid=%v", len(output), utf8.ValidString(output))
	}
	if got := excerpt("\u914d\u7f6e", 4); !utf8.ValidString(got) || !strings.HasPrefix(got, "\u914d\n") {
		t.Fatalf("split a UTF-8 character: %q", got)
	}
}

// v2ServerForTest builds a migrated v2 archive with one indexed turn and
// returns a server bound to its owner.
func v2ServerForTest(t *testing.T) *mcpServer {
	t.Helper()
	path := filepath.Join(t.TempDir(), "memory.db")
	store, err := memstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MigrateArchive(context.Background()); err != nil {
		store.Close()
		t.Fatal(err)
	}
	store.Close()
	writer, err := memstore.OpenArchiveWriter(context.Background(), path, memstore.WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	created := int64(1700000000000)
	session := memstore.ArchiveSession{
		ID: "session-v2", Adapter: "test", UserID: "owner",
		ThreadID: "thread-v2", Project: "gateway", WorkDir: "/work/gateway", CreatedAt: created,
	}
	turn := memstore.ArchiveTurn{ID: "turn-v2", DeliveryKey: "delivery-v2", StartedAt: created}
	for _, message := range []memstore.ArchiveMessage{
		{
			Session: session, Turn: turn, ID: "user-v2", Sequence: 0, Role: "user",
			State: "completed", Text: "how do I restart the gateway", CreatedAt: created, Fidelity: "captured",
		},
		{
			Session: session, Turn: turn, ID: "answer-v2", Sequence: 1, Role: "assistant",
			State: "completed", Text: "Run systemctl restart opencode-gateway and check the journal.",
			CreatedAt: created + 1000, Fidelity: "captured",
		},
	} {
		if err := writer.RecordMessage(context.Background(), message); err != nil {
			writer.Close(context.Background())
			t.Fatal(err)
		}
	}
	for {
		count, err := writer.IndexPending(context.Background(), memstore.IndexBatchLimit)
		if err != nil {
			writer.Close(context.Background())
			t.Fatal(err)
		}
		if count == 0 {
			break
		}
	}
	// Live indexing writes both FTS tables, but it does not advance the word
	// backfill high-water mark, so coverage still reports the word index as
	// partial until a backfill pass runs. Driving it here makes the fixture
	// represent a fully indexed archive, which is what these tests are about;
	// leaving it out would have every word-routed search print a PARTIAL
	// warning that is not true of the data.
	for {
		copied, err := writer.BackfillWordIndex(context.Background(), memstore.WordBackfillBatchLimit)
		if err != nil {
			writer.Close(context.Background())
			t.Fatal(err)
		}
		if copied == 0 {
			break
		}
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	reader, err := memstore.OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })
	return &mcpServer{store: reader, singleOwner: true, adapter: "test", userID: "owner"}
}

func TestV2SearchReturnsEvidenceReferences(t *testing.T) {
	server := v2ServerForTest(t)
	output, err := server.toolMemorySearch(map[string]interface{}{"query": "journal"})
	if err != nil {
		t.Fatal(err)
	}
	// "journal" is a plain ASCII word, so schema v4 routes it to the word index.
	// The assertion names the mode because a routing change that silently sent
	// word queries to the trigram index would otherwise only show up as an
	// unexplained latency change.
	for _, expected := range []string{"message_id=answer-v2", "session_id=session-v2", "mode=word", "systemctl restart opencode-gateway"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("v2 search missing %q: %s", expected, output)
		}
	}
	// A term containing a separator is not a word-index token, so the same
	// archive must answer it through the trigram index. Both paths have to work
	// through the MCP layer, not just the one the fixture happens to exercise.
	trigram, err := server.toolMemorySearch(map[string]interface{}{"query": "opencode-gateway"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(trigram, "mode=fts") {
		t.Fatalf("separator term did not route to the trigram index: %s", trigram)
	}
	if !strings.Contains(trigram, "message_id=answer-v2") {
		t.Fatalf("trigram search lost the match: %s", trigram)
	}
	// A miss must not be disguised as unrelated recent results.
	miss, err := server.toolMemorySearch(map[string]interface{}{"query": "zzzzzzzzzzzz"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(miss, "No matching messages found") {
		t.Fatalf("v2 miss was disguised: %s", miss)
	}
}

func TestV2GetReturnsOriginalBody(t *testing.T) {
	server := v2ServerForTest(t)
	output, err := server.toolMemoryGet(map[string]interface{}{"message_ids": []interface{}{"answer-v2"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output, "Run systemctl restart opencode-gateway and check the journal.") {
		t.Fatalf("v2 get did not return the original body: %s", output)
	}
	if !strings.Contains(output, "fidelity=captured") {
		t.Fatalf("v2 get did not report provenance: %s", output)
	}
	// A single string is accepted as a one-element list.
	if _, err := server.toolMemoryGet(map[string]interface{}{"message_ids": "answer-v2"}); err != nil {
		t.Fatalf("single message id was rejected: %v", err)
	}
	// An unauthorized ID must not be readable.
	foreign := &mcpServer{store: server.store, singleOwner: true, adapter: "test", userID: "intruder"}
	denied, err := foreign.toolMemoryGet(map[string]interface{}{"message_ids": []interface{}{"answer-v2"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(denied, "systemctl restart") {
		t.Fatalf("cross-user get leaked a body: %s", denied)
	}
}

func TestV2ScrollPaginatesSession(t *testing.T) {
	server := v2ServerForTest(t)
	first, err := server.toolMemoryScroll(map[string]interface{}{"session_id": "session-v2", "limit": 1})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(first, "message_id=user-v2") {
		t.Fatalf("scroll did not start at the first message: %s", first)
	}
	if !strings.Contains(first, "Next page") {
		t.Fatalf("scroll did not report a continuation: %s", first)
	}
	// A session outside the scope must be rejected.
	foreign := &mcpServer{store: server.store, singleOwner: true, adapter: "test", userID: "intruder"}
	if _, err := foreign.toolMemoryScroll(map[string]interface{}{"session_id": "session-v2"}); err == nil {
		t.Fatal("cross-user scroll was accepted")
	}
	if _, err := server.toolMemoryScroll(map[string]interface{}{"session_id": "session-v2", "direction": "sideways"}); err == nil {
		t.Fatal("invalid direction was accepted")
	}
}

func TestV2ToolsRequireMigratedArchive(t *testing.T) {
	// A legacy-only store has no v2 tables, so the v2 tools must refuse rather
	// than silently returning nothing.
	server := memoryServerForTest(t, nil)
	if _, err := server.toolMemoryGet(map[string]interface{}{"message_ids": []interface{}{"x"}}); err == nil {
		t.Fatal("memory_get accepted a legacy-only archive")
	}
	if _, err := server.toolMemoryScroll(map[string]interface{}{"session_id": "x"}); err == nil {
		t.Fatal("memory_scroll accepted a legacy-only archive")
	}
	// memory_search must still work through the legacy path.
	if _, err := server.toolMemorySearch(map[string]interface{}{"query": "needle"}); err != nil {
		t.Fatalf("legacy search broke: %v", err)
	}
}

// TestV2SearchTooBroadAdviceMatchesTheMode pins the two different things
// TooBroad can mean. A capped candidate pool is fixed by narrowing the query; a
// capped scan is not, because the query is already as narrow as the indexes
// allow — its terms are too short to index, so no rewording of them will help.
// Rendering both as "narrow the query" gives the scan caller advice that cannot
// be followed, which is its own kind of dishonesty.
func TestV2SearchTooBroadAdviceMatchesTheMode(t *testing.T) {
	scan := formatV2SearchResult(memstore.SearchResult{
		Filters:  memstore.SearchFilters{Mode: "scan"},
		TooBroad: true,
		Notice:   "query terms are shorter than three characters; scanned the newest 2000 messages only",
	})
	if !strings.Contains(scan, "scan stopped at its message cap") {
		t.Fatalf("scan TooBroad did not explain the cap: %s", scan)
	}
	if strings.Contains(scan, "Narrow the query") {
		t.Fatalf("scan TooBroad told the caller to narrow a query that cannot be narrowed: %s", scan)
	}
	if !strings.Contains(scan, "Notice: ") {
		t.Fatalf("scan TooBroad dropped the notice naming the cap: %s", scan)
	}

	word := formatV2SearchResult(memstore.SearchResult{
		Filters:  memstore.SearchFilters{Mode: "word"},
		TooBroad: true,
	})
	if !strings.Contains(word, "Narrow the query") {
		t.Fatalf("word TooBroad lost its narrowing advice: %s", word)
	}
	if strings.Contains(word, "scan stopped") {
		t.Fatalf("word TooBroad claimed a scan cap: %s", word)
	}
}

// TestMemoryHealthReportsLagWithoutLeaking pins the two properties that make a
// health tool safe to expose to a model: it must report the numbers that answer
// "why can't I find what I just said", and it must not report conversation
// bodies or per-user labels.
//
// The second property is the one that decays. A health line is the easiest
// place in a codebase to accidentally print a principal, because the person
// writing it is debugging and wants to know whose archive it is. That is
// exactly what turns a log scrape into a user enumeration, so it is asserted.
func TestMemoryHealthReportsLagWithoutLeaking(t *testing.T) {
	// The v2 archive is the one that has an index to report on. A legacy-only
	// store answers with the migration notice instead, which is asserted
	// separately below so neither branch is left untested.
	server := v2ServerForTest(t)
	health, err := server.toolMemoryHealth(map[string]interface{}{})
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	for _, expected := range []string{
		"scope_mode: single-owner", "schema: v2", "pending_index_jobs:",
		"failed_index_jobs:", "messages_total:", "messages_indexed:",
		"index_lag_messages:", "chunks:", "word_index_complete:",
		"blocks:", "blocks_compressed:", "storage_raw_bytes:",
		"storage_stored_bytes:", "storage_saved_bytes:",
	} {
		if !strings.Contains(health, expected) {
			t.Fatalf("health missing %q: %s", expected, health)
		}
	}
	// A fully indexed archive must report zero lag, or the number is noise.
	if !strings.Contains(health, "index_lag_messages: 0") {
		t.Fatalf("fully indexed archive reported lag: %s", health)
	}
	// No conversation content, no project names, no principal labels. The mode
	// line names the mode ("single-owner ... one principal") and that is not a
	// label, so the assertion targets the keys a label would appear under.
	for _, forbidden := range []string{"systemctl restart", "opencode-gateway", "session-v2", "user_id:", "adapter:"} {
		if strings.Contains(health, forbidden) {
			t.Fatalf("health leaked %q: %s", forbidden, health)
		}
	}

	// A legacy-only archive has no index, and the tool must say so rather than
	// reporting zeros that look like a healthy empty index.
	legacy := memoryServerForTest(t, []memstore.MemRecord{
		{ID: "h1", Adapter: "test", UserID: "owner", Request: "secret request body", Ts: time.Now()},
	})
	legacyHealth, err := legacy.toolMemoryHealth(map[string]interface{}{})
	if err != nil {
		t.Fatalf("legacy health: %v", err)
	}
	if !strings.Contains(legacyHealth, "schema: legacy") {
		t.Fatalf("legacy archive did not report its schema: %s", legacyHealth)
	}
	if strings.Contains(legacyHealth, "secret request body") {
		t.Fatalf("legacy health leaked a body: %s", legacyHealth)
	}
}
