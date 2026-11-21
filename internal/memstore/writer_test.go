package memstore

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func archiveForTest(t *testing.T, options WriterOptions) (string, *ArchiveWriter) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "memory.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MigrateArchive(context.Background()); err != nil {
		t.Fatal(err)
	}
	store.Close()
	writer, err := OpenArchiveWriter(context.Background(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { writer.Close(context.Background()) })
	return path, writer
}

func archiveMessageForTest(id string) ArchiveMessage {
	return ArchiveMessage{
		Session: ArchiveSession{ID: "session", Adapter: "test", UserID: "owner", CreatedAt: 1700000000000},
		Turn:    ArchiveTurn{ID: "turn-" + id, DeliveryKey: "delivery-" + id, StartedAt: 1700000000000},
		ID:      id, Role: "assistant", State: "completed", Fidelity: "captured", CreatedAt: 1700000000000,
		Text: "full response " + strings.Repeat("\u914d\u7f6e", 500),
	}
}

// blockTextForTest reads a stored block and returns its decoded text.
//
// Tests must not compare b.payload to the original text directly. A block is
// stored raw or zstd depending on its size and how well it compresses, so a
// direct comparison passes only while the fixture happens to stay under the
// compression threshold. That is a test that silently stops covering the code
// it names the moment a fixture grows, which is exactly how the storage-full
// test above broke when compression landed.
func blockTextForTest(t *testing.T, db *sql.DB, query string, args ...any) string {
	t.Helper()
	var codec string
	var codecVersion int
	var rawBytes int64
	var payload []byte
	if err := db.QueryRow(query, args...).Scan(&codec, &codecVersion, &rawBytes, &payload); err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeBlock(codec, codecVersion, rawBytes, payload)
	if err != nil {
		t.Fatalf("decode block: %v", err)
	}
	return string(decoded)
}

func TestArchiveWriterCommitsIdempotently(t *testing.T) {
	path, writer := archiveForTest(t, WriterOptions{})
	message := archiveMessageForTest("message")
	for attempt := 0; attempt < 2; attempt++ {
		if err := writer.RecordMessage(context.Background(), message); err != nil {
			t.Fatal(err)
		}
	}
	conflict := message
	conflict.Text = "changed"
	if err := writer.RecordMessage(context.Background(), conflict); !errors.Is(err, ErrArchiveConflict) {
		t.Fatalf("conflicting replay: %v", err)
	}
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	reader, err := openDatabaseMode(path, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if body := blockTextForTest(t, reader.db, `SELECT codec,codec_version,raw_bytes,payload FROM mem_v2_blocks`); body != message.Text {
		t.Fatalf("body not durable: %q", body)
	}
	for _, table := range []string{"mem_v2_messages", "mem_v2_jobs", "mem_v2_blocks"} {
		var count int
		if err := reader.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 1 {
			t.Fatalf("%s count=%d: %v", table, count, err)
		}
	}
	if err := writer.RecordMessage(context.Background(), message); !errors.Is(err, ErrArchiveClosed) {
		t.Fatalf("write after close: %v", err)
	}
}

func TestArchiveWriterDurabilityPragmas(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	writer.store.db.SetConnMaxLifetime(time.Nanosecond)
	for attempt := 0; attempt < 2; attempt++ {
		var synchronous, foreignKeys int
		if err := writer.store.db.QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil || synchronous != 2 {
			t.Fatalf("synchronous=%d: %v", synchronous, err)
		}
		if err := writer.store.db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeys); err != nil || foreignKeys != 1 {
			t.Fatalf("foreign_keys=%d: %v", foreignKeys, err)
		}
	}
}

func TestArchiveWriterRejectsOversizeAndCanceledCalls(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{QueueBytes: 100})
	if err := writer.RecordMessage(context.Background(), archiveMessageForTest("large")); !errors.Is(err, ErrArchiveTooLarge) {
		t.Fatalf("oversize accepted: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := writer.Flush(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled call: %v", err)
	}
	if stats := writer.Stats(); stats.PendingItems != 0 || stats.PendingBytes != 0 {
		t.Fatalf("capacity leaked: %+v", stats)
	}
}

func waitForArchivePending(t *testing.T, writer *ArchiveWriter, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		writer.mu.Lock()
		pending := writer.stats.PendingItems
		changed := writer.changed
		writer.mu.Unlock()
		if pending == count {
			return
		}
		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatalf("pending=%d, want %d", pending, count)
		}
	}
}

func TestArchiveBatchFailureRollsBackEveryReceipt(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{BatchItems: 2, BatchDelay: time.Hour})
	results := make(chan error, 2)
	go func() { results <- writer.RecordMessage(context.Background(), archiveMessageForTest("rolled-back")) }()
	waitForArchivePending(t, writer, 1)
	go func() {
		results <- writer.submit(context.Background(), 1, func(context.Context, *sql.Tx) error { return errors.New("injected failure") })
	}()
	for operation := 0; operation < 2; operation++ {
		if err := <-results; err == nil {
			t.Fatal("failed batch acknowledged success")
		}
	}
	for _, table := range []string{"mem_v2_sessions", "mem_v2_turns", "mem_v2_messages", "mem_v2_blocks", "mem_v2_jobs"} {
		var count int
		if err := writer.store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("%s partially committed: %d %v", table, count, err)
		}
	}
	go func() { results <- writer.RecordMessage(context.Background(), archiveMessageForTest("recovered")) }()
	waitForArchivePending(t, writer, 1)
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-results; err != nil {
		t.Fatal(err)
	}
}

func TestArchiveQueueBackpressureAndClose(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{QueueItems: 2, QueueBytes: 100, BatchItems: 1})
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	first := make(chan error, 1)
	go func() {
		first <- writer.submit(context.Background(), 60, func(context.Context, *sql.Tx) error { close(entered); <-release; return nil })
	}()
	<-entered
	capacityContext, cancelCapacity := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelCapacity()
	if err := writer.submit(capacityContext, 50, nil); !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrArchiveCommitUnknown) {
		t.Fatalf("unaccepted operation outcome: %v", err)
	}
	acceptedContext, cancelAccepted := context.WithCancel(context.Background())
	second := make(chan error, 1)
	go func() { second <- writer.submit(acceptedContext, 40, nil) }()
	waitForArchivePending(t, writer, 2)
	cancelAccepted()
	if err := <-second; !errors.Is(err, ErrArchiveCommitUnknown) {
		t.Fatalf("accepted cancellation was not uncertain: %v", err)
	}
	closeContext, cancelClose := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelClose()
	if err := writer.Close(closeContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close ignored in-flight work: %v", err)
	}
	releaseOnce.Do(func() { close(release) })
	if err := writer.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if stats := writer.Stats(); stats.PendingItems != 0 || stats.PendingBytes != 0 {
		t.Fatalf("capacity leaked: %+v", stats)
	}
}

func TestArchiveConcurrentWritersAndIdentityIsolation(t *testing.T) {
	path, first := archiveForTest(t, WriterOptions{BatchItems: 8})
	second, err := OpenArchiveWriter(context.Background(), path, WriterOptions{BatchItems: 8})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.Close(context.Background()) })
	results := make(chan error, 32)
	for index := 0; index < 16; index++ {
		message := archiveMessageForTest(fmt.Sprintf("concurrent-%d", index))
		message.Sequence = int64(index)
		for _, writer := range []*ArchiveWriter{first, second} {
			go func(writer *ArchiveWriter, message ArchiveMessage) {
				results <- writer.RecordMessage(context.Background(), message)
			}(writer, message)
		}
	}
	for index := 0; index < 32; index++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := first.store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_messages`).Scan(&count); err != nil || count != 16 {
		t.Fatalf("concurrent duplicates: %d %v", count, err)
	}
	foreign := archiveMessageForTest("foreign")
	foreign.Session.UserID = "other"
	foreign.Sequence = 99
	if err := first.RecordMessage(context.Background(), foreign); !errors.Is(err, ErrArchiveConflict) {
		t.Fatalf("session ownership crossed: %v", err)
	}
	var violations int
	rows, err := first.store.db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		violations++
	}
	rows.Close()
	if violations != 0 {
		t.Fatalf("foreign-key violations: %d", violations)
	}
}

func TestArchiveJobFailureDoesNotLeaveMessage(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	if _, err := writer.store.db.Exec(`CREATE TRIGGER reject_archive_job BEFORE INSERT ON mem_v2_jobs BEGIN SELECT RAISE(ABORT,'job failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := writer.RecordMessage(context.Background(), archiveMessageForTest("unindexed")); err == nil {
		t.Fatal("job failure ignored")
	}
	var count int
	if err := writer.store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_messages`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("message without job: %d %v", count, err)
	}
}

func TestArchiveWriterCrashRecovery(t *testing.T) {
	if path := os.Getenv("MEMSTORE_TEST_CRASH_DB"); path != "" {
		writer, err := OpenArchiveWriter(context.Background(), path, WriterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := writer.RecordMessage(context.Background(), archiveMessageForTest("crash-proof")); err != nil {
			t.Fatal(err)
		}
		fmt.Fprintln(os.Stdout, "COMMITTED")
		_, _ = io.ReadFull(os.Stdin, make([]byte, 1))
		return
	}
	path, writer := archiveForTest(t, WriterOptions{})
	writer.Close(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestArchiveWriterCrashRecovery$")
	command.Env = append(os.Environ(), "MEMSTORE_TEST_CRASH_DB="+path)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "COMMITTED" {
		command.Process.Kill()
		command.Wait()
		t.Fatalf("child did not acknowledge commit: %s", stderr.String())
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("child unexpectedly exited normally")
	}
	reader, err := openDatabaseMode(path, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	body := blockTextForTest(t, reader.db, `SELECT b.codec,b.codec_version,b.raw_bytes,b.payload FROM mem_v2_messages m JOIN mem_v2_blocks b ON b.id=m.block_id WHERE m.id='crash-proof'`)
	if body != archiveMessageForTest("crash-proof").Text {
		t.Fatalf("acknowledged data lost on process kill: %q", body)
	}
}

func TestArchiveStorageFullIsNotAcknowledged(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	var pages int
	if err := writer.store.db.QueryRow(`PRAGMA page_count`).Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.store.db.Exec(fmt.Sprintf("PRAGMA max_page_count=%d", pages)); err != nil {
		t.Fatal(err)
	}
	message := archiveMessageForTest("full")
	// The payload must be incompressible. A repeated byte compresses to a few
	// hundred bytes, so the write would fit in the capped database and the test
	// would pass without ever reaching the capacity path it exists to cover.
	// This is the same trap as a fixture that sits below a threshold: the test
	// keeps passing while the branch it names stops running.
	message.Text = incompressibleTextForTest(1024 * 1024)
	if err := writer.RecordMessage(context.Background(), message); err == nil {
		t.Fatal("database capacity failure acknowledged success")
	}
	var count int
	if err := writer.store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_messages`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("failed write partially committed: %d %v", count, err)
	}
}

// incompressibleTextForTest returns size bytes of pseudo-random printable ASCII.
// It uses a linear congruential generator rather than math/rand so the bytes are
// identical on every run and every platform, which keeps a failure reproducible.
//
// The alphabet is printable ASCII on purpose. Random bytes would be rejected by
// the UTF-8 check before the write ever reached the capacity path, so the test
// would pass while proving nothing. Printable ASCII is valid UTF-8 and carries
// about 6.6 bits of entropy per byte, so no codec can shrink it enough to fit in
// the capped database: the fixture reaches the capacity path whether the block
// is stored raw or compressed, which is what this test is about.
func incompressibleTextForTest(size int) string {
	buffer := make([]byte, size)
	state := uint32(0x9e3779b9)
	for index := range buffer {
		state = state*1664525 + 1013904223
		buffer[index] = byte(0x20 + (state>>24)%95)
	}
	return string(buffer)
}

func TestArchiveLegacyFingerprintPreservesInvalidBytes(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	message := archiveMessageForTest("legacy-bytes")
	message.Fidelity = "legacy_unknown"
	message.Text = string([]byte{0xff})
	if err := writer.RecordMessage(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	message.Text = string([]byte{0xfe})
	if err := writer.RecordMessage(context.Background(), message); !errors.Is(err, ErrArchiveConflict) {
		t.Fatalf("different legacy bytes treated as equal: %v", err)
	}
}
