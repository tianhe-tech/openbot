package memstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// turnSpec describes one archived turn for the search fixtures.
type turnSpec struct {
	Delivery string
	Project  string
	WorkDir  string
	Request  string
	Answer   string
	Created  int64
	// UserID defaults to "owner". The cross-principal gate needs a second
	// principal in the same archive, and a fixture that can only write one
	// user cannot express the thing being tested.
	UserID string
}

// recordTurn writes a user message and an assistant answer for one turn using
// the low-level archive API, so the fixture controls timestamps and project
// keys exactly. The lifecycle path is covered separately by
// TestSearchReportsIndexLag.
func recordTurn(t *testing.T, writer *ArchiveWriter, spec turnSpec) {
	t.Helper()
	ctx := context.Background()
	key := archiveID("fixture", spec.Delivery)
	userID := spec.UserID
	if userID == "" {
		userID = "owner"
	}
	session := ArchiveSession{
		ID: "session-" + key, Adapter: "test", UserID: userID,
		ThreadID: "thread-" + spec.Delivery, Project: spec.Project,
		WorkDir: spec.WorkDir, CreatedAt: spec.Created,
	}
	turn := ArchiveTurn{ID: "turn-" + key, DeliveryKey: spec.Delivery, StartedAt: spec.Created}
	user := ArchiveMessage{
		Session: session, Turn: turn, ID: "user-" + key, Sequence: 0,
		Role: "user", State: "completed", Text: spec.Request,
		CreatedAt: spec.Created, Fidelity: "captured",
	}
	if err := writer.RecordMessage(ctx, user); err != nil {
		t.Fatalf("record request %s: %v", spec.Delivery, err)
	}
	answer := user
	answer.ID = "answer-" + key
	answer.Sequence = 1
	answer.Role = "assistant"
	answer.Text = spec.Answer
	answer.CreatedAt = spec.Created + 1000
	if err := writer.RecordMessage(ctx, answer); err != nil {
		t.Fatalf("record answer %s: %v", spec.Delivery, err)
	}
}

// seedArchive writes a small deterministic archive and indexes everything.
func seedArchive(t *testing.T, writer *ArchiveWriter) {
	t.Helper()
	for _, spec := range []turnSpec{
		{
			Delivery: "d1", Project: "gateway", WorkDir: "/work/gateway",
			Request: "how do I restart the gateway service",
			Answer:  "Run systemctl restart opencode-gateway and then check the journal for the startup banner.",
			Created: 1700000000000,
		},
		{
			Delivery: "d2", Project: "gateway", WorkDir: "/work/gateway",
			Request: "what does the circuit breaker do",
			Answer:  "The circuit breaker trips after repeated upstream failures and cools down before retrying.",
			Created: 1700000100000,
		},
		{
			Delivery: "d3", Project: "crawler", WorkDir: "/work/crawler",
			Request: "帮我看看爬虫的配置",
			Answer:  "配置文件在 /work/crawler/config.yaml，里面定义了并发数和超时时间。",
			Created: 1700000200000,
		},
	} {
		recordTurn(t, writer, spec)
	}
	indexAll(t, writer)
}

func indexAll(t *testing.T, writer *ArchiveWriter) {
	t.Helper()
	for {
		count, err := writer.IndexPending(context.Background(), maxIndexBatch)
		if err != nil {
			t.Fatalf("index: %v", err)
		}
		if count == 0 {
			return
		}
	}
}

func TestChunkTextRespectsBoundariesAndOffsets(t *testing.T) {
	paragraph := strings.Repeat("alpha beta gamma delta. ", 200)
	text := paragraph + "\n\n" + strings.Repeat("配置说明。", 400) + "\n\n" + "tail"
	chunks := chunkText(text)
	if len(chunks) < 3 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		if chunk.Ordinal != i {
			t.Fatalf("chunk %d has ordinal %d", i, chunk.Ordinal)
		}
		if chunk.StartByte < 0 || chunk.EndByte > len(text) || chunk.StartByte >= chunk.EndByte {
			t.Fatalf("chunk %d has invalid offsets %d-%d", i, chunk.StartByte, chunk.EndByte)
		}
		if chunk.EndByte-chunk.StartByte > chunkMaxBytes {
			t.Fatalf("chunk %d is %d bytes, over the cap", i, chunk.EndByte-chunk.StartByte)
		}
		if !utf8.ValidString(text[chunk.StartByte:chunk.EndByte]) {
			t.Fatalf("chunk %d is not valid UTF-8", i)
		}
	}
	if chunks[0].StartByte != 0 {
		t.Fatalf("first chunk does not start at zero: %d", chunks[0].StartByte)
	}
	if last := chunks[len(chunks)-1]; last.EndByte != len(text) {
		t.Fatalf("last chunk does not reach the end: %d != %d", last.EndByte, len(text))
	}
	// Chunks must cover the whole text so no evidence is unreachable.
	covered := 0
	for _, chunk := range chunks {
		if chunk.StartByte > covered {
			t.Fatalf("gap between chunks at %d", covered)
		}
		covered = chunk.EndByte
	}
	if covered != len(text) {
		t.Fatalf("coverage ended at %d of %d", covered, len(text))
	}
}

func TestChunkTextSplitsLongLineWithoutBreakingRunes(t *testing.T) {
	text := strings.Repeat("配置", 5000)
	chunks := chunkText(text)
	if len(chunks) < 2 {
		t.Fatalf("expected the long line to split, got %d chunks", len(chunks))
	}
	for _, chunk := range chunks {
		if !utf8.ValidString(text[chunk.StartByte:chunk.EndByte]) {
			t.Fatalf("chunk %d-%d split a rune", chunk.StartByte, chunk.EndByte)
		}
	}
}

func TestSearchFindsResponseOnlyTerm(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	ctx := context.Background()
	result, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"},
		Query: "journal",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 {
		t.Fatalf("expected one hit, got %d", len(result.Hits))
	}
	hit := result.Hits[0]
	if hit.Role != "assistant" {
		t.Fatalf("expected the assistant answer, got role %q", hit.Role)
	}
	if !strings.Contains(hit.Excerpt, "journal") {
		t.Fatalf("excerpt does not contain the term: %q", hit.Excerpt)
	}
	if hit.StartByte < 0 || hit.EndByte <= hit.StartByte {
		t.Fatalf("invalid evidence offsets %d-%d", hit.StartByte, hit.EndByte)
	}
	if result.Filters.Adapter != "test" || result.Filters.UserID != "owner" {
		t.Fatalf("filters not echoed: %+v", result.Filters)
	}
	// A plain ASCII word is answered by the word-tokenized index, which is the
	// fast path. The mode is asserted so a routing regression that silently
	// sends every query to the trigram index is visible as a test failure
	// rather than as a latency regression nobody notices.
	if result.Filters.Mode != "word" {
		t.Fatalf("expected word mode for an ASCII term, got %q", result.Filters.Mode)
	}
}

func TestSearchFindsCJKTerm(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	result, err := writer.store.Search(context.Background(), SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"},
		Query: "并发数",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 1 {
		t.Fatalf("expected one CJK hit, got %d", len(result.Hits))
	}
	if !strings.Contains(result.Hits[0].Excerpt, "并发数") {
		t.Fatalf("excerpt missing CJK term: %q", result.Hits[0].Excerpt)
	}
}

func TestSearchShortQueryUsesBoundedScan(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	result, err := writer.store.Search(context.Background(), SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"},
		Query: "do",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Filters.Mode != "scan" {
		t.Fatalf("expected scan mode for a two-character query, got %q", result.Filters.Mode)
	}
	if len(result.Hits) == 0 {
		t.Fatal("bounded scan found nothing for a term that exists")
	}
	if result.Notice == "" {
		t.Fatal("scan mode must explain itself")
	}
	// The fixture is far smaller than the cap, so the scan reached the end of
	// the scope and the answer is complete. Reporting TooBroad here would be
	// the opposite error: claiming a limit that never applied.
	if result.TooBroad {
		t.Fatalf("scan reported TooBroad on an archive smaller than the cap: %q", result.Notice)
	}
}

// TestSearchScanReportsCapWhenItStopsEarly is the honesty test for the bounded
// scan. The scan examines at most maxFallbackScan messages, and before this test
// existed it applied that cap silently: a query that stopped at the cap returned
// its hits with TooBroad false and no notice, so the caller was told "these are
// the matches" when the truth was "these are the matches in the newest N
// messages". That is the same failure as reporting an unindexed message as a
// confident miss.
//
// The fixture writes maxFallbackScan+1 messages so the scan has exactly one
// message it cannot reach, which is the smallest case where the cap applies.
func TestSearchScanReportsCapWhenItStopsEarly(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	// Every message contains the two-character term, so the scan finds a match
	// in every message it examines and the cap is what stops it, not a lack of
	// matches.
	for i := 0; i <= maxFallbackScan; i++ {
		recordTurn(t, writer, turnSpec{
			Delivery: fmt.Sprintf("cap-%05d", i),
			Project:  "gateway",
			WorkDir:  "/work/gateway",
			Request:  "do the thing",
			Answer:   "done",
			Created:  1700000000000 + int64(i)*1000,
		})
	}
	indexAll(t, writer)

	result, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"},
		Query: "do",
		Limit: maxSearchLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Filters.Mode != "scan" {
		t.Fatalf("expected scan mode, got %q", result.Filters.Mode)
	}
	if !result.TooBroad {
		t.Fatal("scan stopped at its cap but did not report TooBroad")
	}
	if !strings.Contains(result.Notice, "newest") {
		t.Fatalf("scan notice does not say the answer is bounded: %q", result.Notice)
	}
	// The cap must actually bound the work: no more than maxFallbackScan
	// messages may be examined, so no more than that many hits can come back.
	if len(result.Hits) > maxFallbackScan {
		t.Fatalf("scan returned %d hits, more than the %d it is allowed to examine",
			len(result.Hits), maxFallbackScan)
	}
}

func TestSearchScopeIsolation(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	ctx := context.Background()
	// A different user in the same adapter must see nothing.
	result, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "intruder"},
		Query: "journal",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("cross-user search leaked %d hits", len(result.Hits))
	}
	// A different adapter must see nothing either.
	result, err = writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "other", UserID: "owner"},
		Query: "journal",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("cross-adapter search leaked %d hits", len(result.Hits))
	}
	// An empty scope is rejected outright.
	if _, err := writer.store.Search(ctx, SearchOptions{Query: "journal"}); !errors.Is(err, ErrSearchScopeRequired) {
		t.Fatalf("unscoped search was not rejected: %v", err)
	}
}

func TestSearchProjectFilterNarrowsBeforeLimit(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	ctx := context.Background()
	result, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner", Project: "crawler"},
		Query: "配置",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) == 0 {
		t.Fatal("project-scoped search found nothing")
	}
	for _, hit := range result.Hits {
		if hit.Project != "crawler" {
			t.Fatalf("project filter leaked %q", hit.Project)
		}
	}
}

func TestSearchMissingTermReturnsNoHits(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	result, err := writer.store.Search(context.Background(), SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"},
		Query: "nonexistentterm",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) != 0 {
		t.Fatalf("missing term returned %d hits; unrelated recent results must not be substituted", len(result.Hits))
	}
}

func TestSearchPaginationAndCursorBinding(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	// Twelve same-day messages that all contain the same term.
	for i := 0; i < 12; i++ {
		recordTurn(t, writer, turnSpec{
			Delivery: "page-" + string(rune('a'+i)),
			Project:  "page", WorkDir: "/work/page",
			Request: "paginationprobe request",
			Answer:  "paginationprobe answer number " + string(rune('a'+i)),
			Created: 1700000000000 + int64(i)*1000,
		})
	}
	indexAll(t, writer)
	scope := SearchScope{Adapter: "test", UserID: "owner"}
	first, err := writer.store.Search(ctx, SearchOptions{Scope: scope, Query: "paginationprobe", Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Hits) != 5 || !first.Truncated || first.NextCursor == "" {
		t.Fatalf("first page: %d hits truncated=%v cursor=%q", len(first.Hits), first.Truncated, first.NextCursor)
	}
	second, err := writer.store.Search(ctx, SearchOptions{Scope: scope, Query: "paginationprobe", Limit: 5, Cursor: first.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Hits) != 5 {
		t.Fatalf("second page returned %d hits", len(second.Hits))
	}
	seen := make(map[string]struct{})
	for _, hit := range append(append([]SearchHit{}, first.Hits...), second.Hits...) {
		if _, dup := seen[hit.MessageID]; dup {
			t.Fatalf("message %s appeared on two pages", hit.MessageID)
		}
		seen[hit.MessageID] = struct{}{}
	}
	// A cursor bound to a different query must be rejected.
	if _, err := writer.store.Search(ctx, SearchOptions{Scope: scope, Query: "other", Cursor: first.NextCursor}); !errors.Is(err, ErrSearchRestartRequired) {
		t.Fatalf("cursor reuse across queries was accepted: %v", err)
	}
	// A cursor bound to a different scope must be rejected.
	if _, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "intruder"}, Query: "paginationprobe", Cursor: first.NextCursor,
	}); !errors.Is(err, ErrSearchRestartRequired) {
		t.Fatalf("cursor reuse across scopes was accepted: %v", err)
	}
}

func TestSearchReportsIndexLag(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	recordTurn(t, writer, turnSpec{
		Delivery: "lag", Project: "lag", WorkDir: "/work/lag",
		Request: "lagprobe request", Answer: "lagprobe answer", Created: 1700000000000,
	})
	// Before indexing, the search must report partial coverage rather than
	// pretending the archive is complete.
	result, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"}, Query: "lagprobe",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Partial {
		t.Fatal("unindexed messages were not reported as partial coverage")
	}
	if result.IndexLag.PendingJobs == 0 {
		t.Fatal("index lag did not report pending jobs")
	}
	indexAll(t, writer)
	result, err = writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"}, Query: "lagprobe",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Partial {
		t.Fatal("indexed archive still reported partial coverage")
	}
	if len(result.Hits) == 0 {
		t.Fatal("indexed message was not searchable")
	}
}

func TestIndexPendingIsIdempotent(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	ctx := context.Background()
	status, err := writer.store.IndexStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.PendingJobs != 0 {
		t.Fatalf("jobs still pending after indexing: %d", status.PendingJobs)
	}
	if status.IndexedMessages != status.TotalMessages {
		t.Fatalf("indexed %d of %d messages", status.IndexedMessages, status.TotalMessages)
	}
	chunks := status.Chunks
	// Re-running the indexer must not duplicate chunks.
	if _, err := writer.IndexPending(ctx, maxIndexBatch); err != nil {
		t.Fatal(err)
	}
	status, err = writer.store.IndexStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Chunks != chunks {
		t.Fatalf("re-indexing changed chunk count %d -> %d", chunks, status.Chunks)
	}
}

func TestGetMessagesReturnsOriginalBodies(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	ctx := context.Background()
	scope := SearchScope{Adapter: "test", UserID: "owner"}
	result, err := writer.store.Search(ctx, SearchOptions{Scope: scope, Query: "journal"})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) == 0 {
		t.Fatal("no hit to fetch")
	}
	detail, err := writer.store.GetMessages(ctx, scope, []string{result.Hits[0].MessageID}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(detail.Messages) != 1 {
		t.Fatalf("expected one message, got %d", len(detail.Messages))
	}
	if !strings.Contains(detail.Messages[0].Text, "systemctl restart opencode-gateway") {
		t.Fatalf("detail did not return the original body: %q", detail.Messages[0].Text)
	}
	if detail.Messages[0].Truncated {
		t.Fatal("a small message was truncated")
	}
	// A message from another scope must not be readable.
	other, err := writer.store.GetMessages(ctx, SearchScope{Adapter: "test", UserID: "intruder"}, []string{result.Hits[0].MessageID}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(other.Messages) != 0 {
		t.Fatal("cross-user detail read leaked a message")
	}
	if other.Notice == "" {
		t.Fatal("unavailable message was not reported")
	}
}

func TestGetMessagesRespectsBudget(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	big := strings.Repeat("budgetprobe ", 2000)
	recordTurn(t, writer, turnSpec{
		Delivery: "budget", Project: "budget", WorkDir: "/work/budget",
		Request: "budget request", Answer: big, Created: 1700000000000,
	})
	scope := SearchScope{Adapter: "test", UserID: "owner"}
	detail, err := writer.store.GetMessages(ctx, scope, []string{"answer-" + archiveID("fixture", "budget")}, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if len(detail.Messages) != 1 {
		t.Fatalf("expected one message, got %d", len(detail.Messages))
	}
	if !detail.Messages[0].Truncated || !detail.Truncated {
		t.Fatal("budget overflow was not reported as truncation")
	}
	if len(detail.Messages[0].Text) > 1024 {
		t.Fatalf("budget exceeded: %d bytes", len(detail.Messages[0].Text))
	}
	if detail.Messages[0].ByteLength != len(big) {
		t.Fatalf("stored length was not reported: %d", detail.Messages[0].ByteLength)
	}
}

func TestScrollPaginatesSession(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	ctx := context.Background()
	scope := SearchScope{Adapter: "test", UserID: "owner"}
	var sessionID string
	if err := writer.store.db.QueryRow(`SELECT id FROM mem_v2_sessions LIMIT 1`).Scan(&sessionID); err != nil {
		t.Fatal(err)
	}
	first, err := writer.store.Scroll(ctx, scope, sessionID, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Messages) != 1 {
		t.Fatalf("expected one message, got %d", len(first.Messages))
	}
	if !first.Truncated || first.NextCursor == "" {
		t.Fatal("scroll did not report a continuation")
	}
	second, err := writer.store.Scroll(ctx, scope, sessionID, first.NextCursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Messages) != 1 {
		t.Fatalf("expected one message on page two, got %d", len(second.Messages))
	}
	if second.Messages[0].Seq <= first.Messages[0].Seq {
		t.Fatalf("scroll did not advance: %d then %d", first.Messages[0].Seq, second.Messages[0].Seq)
	}
	// A cursor from another session must be rejected.
	if _, err := writer.store.Scroll(ctx, scope, "other-session", first.NextCursor, 1); err == nil {
		t.Fatal("cross-session cursor was accepted")
	}
	// A session outside the scope must be rejected.
	if _, err := writer.store.Scroll(ctx, SearchScope{Adapter: "test", UserID: "intruder"}, sessionID, "", 1); err == nil {
		t.Fatal("cross-user scroll was accepted")
	}
}

func TestSearchIdenticalTextInDifferentTurns(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		recordTurn(t, writer, turnSpec{
			Delivery: "dup-" + string(rune('a'+i)),
			Project:  "dup", WorkDir: "/work/dup",
			Request: "duplicateprobe request",
			Answer:  "duplicateprobe identical answer text",
			Created: 1700000000000 + int64(i)*1000,
		})
	}
	indexAll(t, writer)
	result, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"}, Query: "duplicateprobe",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Both the two requests and the two identical answers must be distinct hits.
	ids := make(map[string]struct{})
	for _, hit := range result.Hits {
		ids[hit.MessageID] = struct{}{}
	}
	if len(ids) != len(result.Hits) {
		t.Fatal("identical text in different turns collapsed into one hit")
	}
	if len(result.Hits) < 4 {
		t.Fatalf("expected at least four hits, got %d", len(result.Hits))
	}
}

func TestSearchExactIdentifierOutranksTopical(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	entries := []struct {
		delivery string
		text     string
	}{
		{"topical", "the deploy script mentions opencode-gateway somewhere in a long paragraph about deployment"},
		{"exact", "opencode-gateway"},
	}
	for i, entry := range entries {
		recordTurn(t, writer, turnSpec{
			Delivery: entry.delivery, Project: "id", WorkDir: "/work/id",
			Request: "identifier probe", Answer: entry.text,
			Created: 1700000000000 + int64(i)*1000,
		})
	}
	indexAll(t, writer)
	result, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"}, Query: "opencode-gateway",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) < 2 {
		t.Fatalf("expected both messages, got %d", len(result.Hits))
	}
	if !result.Hits[0].Exact {
		t.Fatalf("top hit was not an exact identifier match: %+v", result.Hits[0])
	}
}

func TestSearchPunctuationQueryIsLiteral(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	ctx := context.Background()
	// FTS operators in user input must be treated as text, not syntax.
	for _, query := range []string{`"journal"`, "journal OR", "journal*", "NEAR(journal)"} {
		if _, err := writer.store.Search(ctx, SearchOptions{
			Scope: SearchScope{Adapter: "test", UserID: "owner"}, Query: query,
		}); err != nil {
			t.Fatalf("query %q failed: %v", query, err)
		}
	}
}

func TestSearchOldMessageIsFindable(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	// A message far outside any recent window must still be found: explicit
	// history search has no implicit recency cutoff.
	recordTurn(t, writer, turnSpec{
		Delivery: "old", Project: "old", WorkDir: "/work/old",
		Request: "oldprobe request", Answer: "oldprobe answer from years ago",
		Created: 1500000000000,
	})
	indexAll(t, writer)
	result, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"}, Query: "oldprobe",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) == 0 {
		t.Fatal("old message was not findable")
	}
}

func TestSearchTimeBoundsAreApplied(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	seedArchive(t, writer)
	ctx := context.Background()
	// Only the first turn falls inside this window.
	result, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner", Since: 1699999999000, Until: 1700000050000},
		Query: "gateway",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, hit := range result.Hits {
		if hit.CreatedAt < 1699999999000 || hit.CreatedAt > 1700000050000 {
			t.Fatalf("time bound leaked a hit at %d", hit.CreatedAt)
		}
	}
	if result.Filters.Since != 1699999999000 || result.Filters.Until != 1700000050000 {
		t.Fatalf("time filters not echoed: %+v", result.Filters)
	}
}

func TestIndexWorkerDrainsQueueAndStops(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	recordTurn(t, writer, turnSpec{
		Delivery: "worker", Project: "worker", WorkDir: "/work/worker",
		Request: "workerprobe request", Answer: "workerprobe answer", Created: 1700000000000,
	})
	workerCtx, stopWorker := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		writer.RunIndexWorker(workerCtx, IndexWorkerOptions{Batch: 8, Idle: 10 * time.Millisecond})
	}()
	// The worker must index without any explicit IndexPending call.
	deadline := time.Now().Add(5 * time.Second)
	for {
		status, err := writer.store.IndexStatus(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if status.PendingJobs == 0 && status.IndexedMessages == status.TotalMessages {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("worker did not drain the queue: %+v", status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	result, err := writer.store.Search(ctx, SearchOptions{
		Scope: SearchScope{Adapter: "test", UserID: "owner"}, Query: "workerprobe",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hits) == 0 {
		t.Fatal("worker-indexed message was not searchable")
	}
	// Cancelling the context must stop the worker promptly.
	stopWorker()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("index worker did not stop after cancellation")
	}
}
