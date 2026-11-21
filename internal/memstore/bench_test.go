package memstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The plan's §15 targets are stated as p95 latencies, not averages, so these
// benchmarks collect per-operation samples and report percentiles. An average
// would hide exactly the tail behavior the targets are meant to bound.
//
// Corpus size is controlled by MEMSTORE_BENCH_MESSAGES. The default is small
// enough to iterate on; the release gate requires the documented size:
//
//	$env:MEMSTORE_BENCH_MESSAGES=100000
//	go test ./internal/memstore -run '^$' -bench 'BenchmarkScopedLexicalSearch|BenchmarkDetailRetrieval' -benchtime 200x
//
// Seeding dominates wall time because every message goes through the real
// commit-acknowledged writer. That cost is deliberate: a benchmark that bulk
// inserted rows would not measure the code that actually runs in production.
// The seeded archive is built once per message count and shared by every
// benchmark that needs it.
//
// Seeding submits from a small worker pool rather than a sequential loop. The
// writer batches concurrent operations into one transaction, which is how
// capture behaves in production; a sequential loop would pay a full commit per
// message (~13 ms on the reference machine) and make a 100k seed take over
// twenty minutes. The rows written are identical either way, so this changes
// only how long the fixture takes to build, not what it contains.

// defaultBenchMessages keeps a plain `go test -bench .` run to a few seconds.
const defaultBenchMessages = 2000

// benchVocabularySize controls term diversity. It must be large enough that a
// three-term query matches a small fraction of chunks, the way real prose
// behaves. A tiny vocabulary makes every chunk match every query, which
// measures the worst case rather than the expected case; that worst case has
// its own benchmark below.
const benchVocabularySize = 20000

// benchCJKStems are three or more runes on purpose: a trigram index cannot
// match one- or two-character terms, and those queries take the bounded scan
// path instead.
//
// A stem alone is not a usable term. There are only a handful of stems, and
// every body contains all of them, so a query for a bare stem matches the whole
// archive. That is the worst case, not the expected case, and measuring it as
// if it were typical is how a fixture silently reports the wrong number. Real
// CJK terms carry a distinguishing suffix, so benchCJKWord pairs a stem with a
// generated word to spread terms across the same space as the ASCII vocabulary.
var benchCJKStems = []string{
	"配置文件", "并发数", "超时时间", "会话管理", "索引状态", "检索结果",
	"归档数据", "重试次数",
}

// benchWord returns a deterministic pseudo-word. The generator is a cheap
// multiplicative hash so the corpus is reproducible without allocating a
// vocabulary slice.
func benchWord(index int) string {
	mixed := uint32(index)*2654435761 + 1013904223
	return fmt.Sprintf("w%05d", mixed%benchVocabularySize)
}

// benchCJKWord returns a deterministic CJK term: a stem plus a generated
// suffix, so the term is distinct across the corpus while still exercising
// multi-byte trigram tokenization.
func benchCJKWord(index int) string {
	return benchCJKStems[index%len(benchCJKStems)] + benchWord(index)
}

// latencyStats summarizes per-operation latencies.
type latencyStats struct {
	P50 time.Duration
	P95 time.Duration
	Max time.Duration
}

func summarizeLatencies(samples []time.Duration) latencyStats {
	if len(samples) == 0 {
		return latencyStats{}
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	pick := func(quantile float64) time.Duration {
		index := int(quantile * float64(len(sorted)-1))
		return sorted[index]
	}
	return latencyStats{
		P50: pick(0.50),
		P95: pick(0.95),
		Max: sorted[len(sorted)-1],
	}
}

func reportLatencies(b *testing.B, stats latencyStats) {
	b.Helper()
	// Nanoseconds divided by 1e6, not Microseconds() divided by 1000. The
	// latter truncates to whole microseconds, so a row that takes 900 ns
	// reports 0 ms and looks free. A fast row is not a free row.
	b.ReportMetric(float64(stats.P50.Nanoseconds())/1e6, "p50-ms")
	b.ReportMetric(float64(stats.P95.Nanoseconds())/1e6, "p95-ms")
	b.ReportMetric(float64(stats.Max.Nanoseconds())/1e6, "max-ms")
}

// referenceWorkBytes and referenceMinDuration are gone with the reference
// operation they configured. The operation was measured and did not work: see
// the note in search_attribution_bench_test.go where it used to be called.

// clockQuantum measures how far time.Now() advances in one step on this
// machine, by spinning until the clock changes and recording the step.
//
// This is not a curiosity. On this machine the quantum is about 1 ms, which
// means a single timing of a sub-millisecond operation reports either 0 or
// 1 ms and nothing in between. Every row in the attribution table that costs
// less than a millisecond was therefore quantized to the nearest millisecond,
// and a row that reported "0 p50-ms" reported that the clock did not advance
// at all during half its samples. A benchmark that does not know its own
// resolution cannot tell a fast operation from a free one.
func clockQuantum() time.Duration {
	const trials = 64
	steps := make([]time.Duration, 0, trials)
	for i := 0; i < trials; i++ {
		prev := time.Now()
		for {
			now := time.Now()
			if now != prev {
				steps = append(steps, now.Sub(prev))
				break
			}
		}
	}
	sort.Slice(steps, func(i, j int) bool { return steps[i] < steps[j] })
	return steps[len(steps)/2]
}

// benchBody builds a deterministic body of roughly 4 KiB from the shared
// vocabulary, with occasional CJK terms and sentence breaks so chunking has
// realistic boundaries to find.
func benchBody(seed int) string {
	var builder strings.Builder
	builder.Grow(4096)
	for i := 0; builder.Len() < 4096; i++ {
		if i%37 == 36 {
			builder.WriteString(benchCJKWord(seed*131 + i))
		} else {
			builder.WriteString(benchWord(seed*131 + i))
		}
		if i%12 == 11 {
			builder.WriteString(".\n")
		} else {
			builder.WriteByte(' ')
		}
	}
	return builder.String()
}

func benchMessageCount(b *testing.B) int {
	b.Helper()
	counts := benchMessageCounts(b)
	if len(counts) != 1 {
		b.Fatalf("this benchmark takes one corpus size, got %v", counts)
	}
	return counts[0]
}

// benchMessageCounts parses MEMSTORE_BENCH_MESSAGES as a comma-separated list
// of corpus sizes.
//
// A list exists because comparing two sizes measured in two processes compares
// two machine states as well as two corpus sizes. That is not hypothetical: a
// 20k run and a 100k run taken hours apart differed by a factor of three on
// every row, which is enough to turn a linear query into an apparent
// superlinear one. Measuring the sizes in one process removes the machine from
// the comparison, and the seeded archives are cached per size so the second
// size costs only its own seed.
func benchMessageCounts(b *testing.B) []int {
	b.Helper()
	raw := os.Getenv("MEMSTORE_BENCH_MESSAGES")
	if raw == "" {
		return []int{defaultBenchMessages}
	}
	parts := strings.Split(raw, ",")
	counts := make([]int, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		count, err := strconv.Atoi(part)
		if err != nil || count < 1 {
			b.Fatalf("MEMSTORE_BENCH_MESSAGES must be a comma-separated list of positive integers, got %q", raw)
		}
		counts = append(counts, count)
	}
	if len(counts) == 0 {
		b.Fatalf("MEMSTORE_BENCH_MESSAGES must name at least one corpus size, got %q", raw)
	}
	return counts
}

// benchMessage builds one archived message with a caller-chosen body.
func benchMessage(key, project, text string, createdAt int64) ArchiveMessage {
	return ArchiveMessage{
		Session: ArchiveSession{
			ID: "session-" + key, Adapter: "bench", UserID: "owner",
			ThreadID: "thread-" + key, Project: project,
			WorkDir: "/work/" + project, CreatedAt: createdAt,
		},
		Turn:      ArchiveTurn{ID: "turn-" + key, DeliveryKey: key, StartedAt: createdAt},
		ID:        "message-" + key,
		Role:      "assistant",
		State:     "completed",
		Text:      text,
		CreatedAt: createdAt,
		Fidelity:  "captured",
	}
}

// seededArchives caches one fully indexed archive per message count so several
// benchmarks can share the expensive seed instead of rebuilding it each time.
var seededArchives = struct {
	sync.Mutex
	paths map[int]string
	dirs  []string
}{paths: map[int]string{}}

// TestMain removes the seeded archives after the benchmark process exits.
func TestMain(m *testing.M) {
	code := m.Run()
	seededArchives.Lock()
	for _, dir := range seededArchives.dirs {
		os.RemoveAll(dir)
	}
	seededArchives.Unlock()
	os.Exit(code)
}

// seedBenchArchive writes count messages of ~4 KiB each, indexes them, and
// returns the database path. The result is cached per count.
func seedBenchArchive(b *testing.B, count int) string {
	b.Helper()
	seededArchives.Lock()
	defer seededArchives.Unlock()
	if path, ok := seededArchives.paths[count]; ok {
		return path
	}

	dir, err := os.MkdirTemp("", "memstore-bench-")
	if err != nil {
		b.Fatal(err)
	}
	seededArchives.dirs = append(seededArchives.dirs, dir)
	path := filepath.Join(dir, "memory.db")

	store, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	if err := store.MigrateArchive(context.Background()); err != nil {
		b.Fatal(err)
	}
	store.Close()

	writer, err := OpenArchiveWriter(context.Background(), path, WriterOptions{})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	const base = int64(1700000000000)
	// seedWorkers bounds how many messages are in flight at once. The writer
	// batches up to BatchItems (64) operations per transaction, so a pool this
	// size keeps batches full without exceeding the queue.
	const seedWorkers = 32
	var (
		next    atomic.Int64
		group   sync.WaitGroup
		errMu   sync.Mutex
		seedErr error
	)
	record := func(err error) {
		errMu.Lock()
		if seedErr == nil {
			seedErr = err
		}
		errMu.Unlock()
	}
	for worker := 0; worker < seedWorkers; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= count {
					return
				}
				project := "gateway"
				if i%3 == 0 {
					project = "crawler"
				}
				key := fmt.Sprintf("bench-%06d", i)
				if err := writer.RecordMessage(ctx, benchMessage(key, project, benchBody(i), base+int64(i)*1000)); err != nil {
					record(fmt.Errorf("record %s: %w", key, err))
					return
				}
			}
		}()
	}
	group.Wait()
	if seedErr != nil {
		b.Fatal(seedErr)
	}
	for {
		processed, err := writer.IndexPending(ctx, maxIndexBatch)
		if err != nil {
			b.Fatalf("index: %v", err)
		}
		if processed == 0 {
			break
		}
	}
	if err := writer.Close(ctx); err != nil {
		b.Fatalf("close writer: %v", err)
	}
	seededArchives.paths[count] = path
	return path
}

// benchArchive opens a migrated archive and its writer for a benchmark.
func benchArchive(b *testing.B, options WriterOptions) (string, *ArchiveWriter) {
	b.Helper()
	path := filepath.Join(b.TempDir(), "memory.db")
	store, err := Open(path)
	if err != nil {
		b.Fatal(err)
	}
	if err := store.MigrateArchive(context.Background()); err != nil {
		b.Fatal(err)
	}
	store.Close()
	writer, err := OpenArchiveWriter(context.Background(), path, options)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { writer.Close(context.Background()) })
	return path, writer
}

// BenchmarkStrictCommit measures the commit-acknowledged write path with ~4 KiB
// bodies. The plan's target is p95 <= 25 ms at 20 messages/second. This loop is
// single-threaded, so it bounds per-commit latency rather than throughput; the
// archive grows as it runs, which is the realistic direction of drift.
func BenchmarkStrictCommit(b *testing.B) {
	_, writer := benchArchive(b, WriterOptions{})
	ctx := context.Background()
	body := benchBody(1)
	samples := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("commit-%06d", i)
		message := benchMessage(key, "gateway", body, 1700000000000)
		start := time.Now()
		if err := writer.RecordMessage(ctx, message); err != nil {
			b.Fatalf("record: %v", err)
		}
		samples = append(samples, time.Since(start))
	}
	b.StopTimer()
	reportLatencies(b, summarizeLatencies(samples))
}

// warmSearchCache runs each query until its latency stops improving, so a
// benchmark that reports warm latency actually measures warm latency.
//
// The stopping rule needs a floor as well as a convergence test. An earlier
// version stopped as soon as one run was not 10% faster than the previous one,
// which on a multi-gigabyte index can happen on the second round purely because
// that round was marginally slower. It then reported a cold number under a warm
// label: at 100k the same rotation measured 1541 ms with that rule and 359 ms
// after the cache was genuinely warm. A minimum round count is what makes the
// label honest, and the log line is what makes an under-warmed run visible
// instead of silent.
func warmSearchCache(b *testing.B, store *Store, scope SearchScope, queries []string) {
	b.Helper()
	ctx := context.Background()
	const (
		minRounds = 5
		maxRounds = 12
	)
	for _, query := range queries {
		best := time.Duration(0)
		rounds := 0
		for round := 0; round < maxRounds; round++ {
			start := time.Now()
			if _, err := store.Search(ctx, SearchOptions{Scope: scope, Query: query, Limit: 10}); err != nil {
				b.Fatalf("warm-up search %q: %v", query, err)
			}
			elapsed := time.Since(start)
			rounds++
			if best == 0 || elapsed < best {
				best = elapsed
			}
			// Converged once this round is within 10% of the best seen and the
			// floor has been met.
			if rounds >= minRounds && elapsed <= best*11/10 {
				break
			}
		}
		b.Logf("warm-up %q: %d rounds, best %s", query, rounds, best.Round(time.Millisecond))
	}
}

// BenchmarkScopedLexicalSearch measures warm search latency for queries that
// match a realistic fraction of the corpus. The plan's target is p95 <= 100 ms
// at 100k messages.
func BenchmarkScopedLexicalSearch(b *testing.B) {
	count := benchMessageCount(b)
	b.Run(fmt.Sprintf("messages=%d", count), func(b *testing.B) {
		path := seedBenchArchive(b, count)
		store, err := OpenReadOnly(path)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { store.Close() })

		scope := SearchScope{Adapter: "bench", UserID: "owner"}
		// Terms drawn from the same generator the corpus uses, so each query
		// matches the chunks that happen to contain those words. The CJK terms
		// are stem+suffix pairs for the same reason: a bare stem appears in
		// every body and would turn this into the broad benchmark.
		queries := []string{
			benchWord(11) + " " + benchWord(22) + " " + benchWord(33),
			benchCJKWord(11) + " " + benchCJKWord(22),
			benchWord(44) + " " + benchWord(55),
			benchCJKWord(33) + " " + benchCJKWord(44),
		}
		// The target is stated as warm p95, so the cache must be warm before
		// timing starts. Without this the first iterations pay for reading a
		// freshly written archive off disk, and the reported p95 is a cold
		// number wearing a warm label: measured at 20k, the same rotation costs
		// 288.9 ms cold and 76.1 ms warm.
		warmSearchCache(b, store, scope, queries)
		samples := make([]time.Duration, 0, b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			options := SearchOptions{Scope: scope, Query: queries[i%len(queries)], Limit: 10}
			start := time.Now()
			result, err := store.Search(context.Background(), options)
			if err != nil {
				b.Fatalf("search %q: %v", options.Query, err)
			}
			samples = append(samples, time.Since(start))
			if len(result.Hits) == 0 {
				b.Fatalf("query %q returned no hits; the fixture is not exercising the index", options.Query)
			}
			if result.Partial {
				b.Fatalf("query %q reported partial coverage; the fixture did not finish indexing", options.Query)
			}
		}
		b.StopTimer()
		reportLatencies(b, summarizeLatencies(samples))
	})
}

// BenchmarkScopedLexicalSearchBroad documents the worst case: a query whose
// terms appear in nearly every chunk. FTS5 must score every match before the
// candidate limit applies, so this is the upper bound on search cost and is
// reported separately rather than folded into the target measurement.
func BenchmarkScopedLexicalSearchBroad(b *testing.B) {
	count := benchMessageCount(b)
	b.Run(fmt.Sprintf("messages=%d", count), func(b *testing.B) {
		path := seedBenchArchive(b, count)
		store, err := OpenReadOnly(path)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { store.Close() })

		scope := SearchScope{Adapter: "bench", UserID: "owner"}
		// A bare stem appears in every body, so this matches every chunk in the
		// archive. That is the genuine worst case: FTS5 scores all of them
		// before the candidate limit applies.
		query := benchCJKStems[0]
		// Warm the cache for the same reason as the target benchmark: this
		// number is quoted next to a warm one and must be measured the same way.
		warmSearchCache(b, store, scope, []string{query})
		samples := make([]time.Duration, 0, b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := time.Now()
			result, err := store.Search(context.Background(), SearchOptions{Scope: scope, Query: query, Limit: 10})
			if err != nil {
				b.Fatalf("search: %v", err)
			}
			samples = append(samples, time.Since(start))
			if !result.TooBroad {
				b.Fatalf("expected the broad query to report TooBroad")
			}
		}
		b.StopTimer()
		reportLatencies(b, summarizeLatencies(samples))
	})
}

// BenchmarkSearchByMode measures each routing mode separately.
//
// BenchmarkScopedLexicalSearch rotates four queries, two of which are ASCII
// words and two of which are CJK, so its p95 is a blend of the word index and
// the trigram index and cannot say whether the fast path is actually fast. The
// whole point of the word index is that one mode is much cheaper than the
// other, and a blended number would hide a regression in either. Each
// sub-benchmark therefore asserts the mode it got, so a routing change that
// silently sends word queries to the trigram index fails here instead of
// showing up as an unexplained latency increase.
func BenchmarkSearchByMode(b *testing.B) {
	count := benchMessageCount(b)
	cases := []struct {
		name  string
		query string
		mode  string
	}{
		{"word", benchWord(11) + " " + benchWord(22) + " " + benchWord(33), "word"},
		{"fts", benchCJKWord(11) + " " + benchCJKWord(22), "fts"},
		{"hybrid", benchWord(44) + " " + benchCJKWord(33), "hybrid"},
		{"scan", "w1", "scan"},
	}
	for _, testCase := range cases {
		b.Run(fmt.Sprintf("messages=%d/%s", count, testCase.name), func(b *testing.B) {
			path := seedBenchArchive(b, count)
			store, err := OpenReadOnly(path)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { store.Close() })

			scope := SearchScope{Adapter: "bench", UserID: "owner"}
			warmSearchCache(b, store, scope, []string{testCase.query})
			samples := make([]time.Duration, 0, b.N)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				result, err := store.Search(context.Background(),
					SearchOptions{Scope: scope, Query: testCase.query, Limit: 10})
				samples = append(samples, time.Since(start))
				if err != nil {
					b.Fatalf("search %q: %v", testCase.query, err)
				}
				if result.Filters.Mode != testCase.mode {
					b.Fatalf("query %q routed to %q, expected %q",
						testCase.query, result.Filters.Mode, testCase.mode)
				}
				if result.Partial {
					b.Fatalf("query %q reported partial coverage; the fixture did not finish indexing", testCase.query)
				}
				// The scan mode is a bounded fallback over a two-character term
				// and is not expected to match anything in this corpus; every
				// indexed mode must actually return hits or it is not measuring
				// the index.
				if testCase.mode != "scan" && len(result.Hits) == 0 {
					b.Fatalf("query %q returned no hits; the fixture is not exercising the index", testCase.query)
				}
			}
			b.StopTimer()
			reportLatencies(b, summarizeLatencies(samples))
		})
	}
}

// BenchmarkDetailRetrieval measures fetching ten ~4 KiB bodies by ID. The plan's
// target is p95 <= 50 ms. The budget is raised above the 32 KiB default because
// ten 4 KiB bodies plus metadata exceed it; the default is exercised by
// TestGetMessagesRespectsBudget.
func BenchmarkDetailRetrieval(b *testing.B) {
	count := benchMessageCount(b)
	b.Run(fmt.Sprintf("messages=%d", count), func(b *testing.B) {
		path := seedBenchArchive(b, count)
		store, err := OpenReadOnly(path)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { store.Close() })

		ids := make([]string, 10)
		for i := range ids {
			ids[i] = fmt.Sprintf("message-bench-%06d", i*7)
		}
		scope := SearchScope{Adapter: "bench", UserID: "owner"}
		// The target is stated as warm p95, so the pages must be resident
		// before timing starts. Same reason as the search benchmarks.
		for round := 0; round < 3; round++ {
			if _, err := store.GetMessages(context.Background(), scope, ids, maxDetailBudget); err != nil {
				b.Fatalf("warm-up get messages: %v", err)
			}
		}
		samples := make([]time.Duration, 0, b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			start := time.Now()
			result, err := store.GetMessages(context.Background(), scope, ids, maxDetailBudget)
			if err != nil {
				b.Fatalf("get messages: %v", err)
			}
			samples = append(samples, time.Since(start))
			if len(result.Messages) != len(ids) {
				b.Fatalf("got %d of %d messages: %s", len(result.Messages), len(ids), result.Notice)
			}
		}
		b.StopTimer()
		reportLatencies(b, summarizeLatencies(samples))
	})
}

// BenchmarkIndexFreshness measures the delay between a committed message and it
// becoming searchable. The plan's target is p95 <= 2 s.
func BenchmarkIndexFreshness(b *testing.B) {
	path, writer := benchArchive(b, WriterOptions{})
	store, err := OpenReadOnly(path)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })

	ctx := context.Background()
	scope := SearchScope{Adapter: "bench", UserID: "owner"}
	samples := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("fresh-%06d", i)
		// A term unique to this message, so a hit can only mean this message
		// was indexed rather than some earlier one matching.
		term := fmt.Sprintf("freshness%06d", i)
		message := benchMessage(key, "gateway", term+" "+benchBody(i), 1700000000000)

		start := time.Now()
		if err := writer.RecordMessage(ctx, message); err != nil {
			b.Fatalf("record: %v", err)
		}
		deadline := start.Add(30 * time.Second)
		for {
			if _, err := writer.IndexPending(ctx, maxIndexBatch); err != nil {
				b.Fatalf("index: %v", err)
			}
			result, err := store.Search(ctx, SearchOptions{Scope: scope, Query: term, Limit: 1})
			if err != nil {
				b.Fatalf("search: %v", err)
			}
			if len(result.Hits) > 0 {
				break
			}
			if time.Now().After(deadline) {
				b.Fatalf("message %s never became searchable", key)
			}
			time.Sleep(5 * time.Millisecond)
		}
		samples = append(samples, time.Since(start))
	}
	b.StopTimer()
	reportLatencies(b, summarizeLatencies(samples))
}
