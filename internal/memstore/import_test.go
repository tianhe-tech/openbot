package memstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLegacyImportBacksUpAndResumes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		if err := store.Record(MemRecord{ID: fmt.Sprint(index), Adapter: "test", UserID: "owner", Request: "request", Response: string([]byte{0xff, 0xfe}) + " original bytes", Project: "project", WorkDir: "/work"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Record(MemRecord{ID: "unmapped", Request: "unknown owner", Response: "retained"}); err != nil {
		t.Fatal(err)
	}
	backupPath := filepath.Join(filepath.Dir(path), "before.db")
	if err := PrepareArchive(context.Background(), path, backupPath); err != nil {
		t.Fatal(err)
	}
	store.Close()
	backup, err := openDatabaseMode(backupPath, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkArchiveSchema(context.Background(), backup.db); err == nil {
		t.Fatal("backup was created after migration")
	}
	var count int
	if err := backup.db.QueryRow(`SELECT COUNT(*) FROM mem_records`).Scan(&count); err != nil || count != 4 {
		t.Fatalf("backup lost WAL records: %d %v", count, err)
	}
	backup.Close()
	for batch := 0; batch < 4; batch++ {
		writer, err := OpenArchiveWriter(context.Background(), path, WriterOptions{})
		if err != nil {
			t.Fatal(err)
		}
		progress, err := writer.ImportLegacyBatch(context.Background(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if progress.Unmapped != 1 {
			t.Fatalf("unmapped count: %+v", progress)
		}
		if batch < 3 && (progress.Imported != 1 || progress.Pending != int64(2-batch)) {
			t.Fatalf("bad progress: %+v", progress)
		}
		if batch == 3 && (progress.Imported != 0 || progress.Pending != 0) {
			t.Fatalf("duplicate import: %+v", progress)
		}
		writer.Close(context.Background())
	}
	reader, err := openDatabaseMode(path, true, false)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for table, expected := range map[string]int{"mem_records": 4, "mem_v2_legacy_map": 3, "mem_v2_messages": 6, "mem_v2_jobs": 6} {
		if err := reader.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != expected {
			t.Fatalf("%s count=%d: %v", table, count, err)
		}
	}
	var body []byte
	var codec string
	var codecVersion int
	var rawBytes int64
	var fidelity, upstream string
	if err := reader.db.QueryRow(`SELECT b.codec,b.codec_version,b.raw_bytes,b.payload,m.fidelity,s.upstream_session_id FROM mem_v2_messages m JOIN mem_v2_blocks b ON b.id=m.block_id JOIN mem_v2_sessions s ON s.id=m.session_id WHERE m.role='assistant' LIMIT 1`).Scan(&codec, &codecVersion, &rawBytes, &body, &fidelity, &upstream); err != nil {
		t.Fatal(err)
	}
	// Decode rather than compare the stored payload: a block is raw or zstd
	// depending on its size, so a direct comparison would only hold while this
	// fixture stays under the compression threshold.
	decoded, err := decodeBlock(codec, codecVersion, rawBytes, body)
	if err != nil {
		t.Fatalf("decode block: %v", err)
	}
	if string(decoded) != string([]byte{0xff, 0xfe})+" original bytes" || fidelity != "legacy_unknown" || upstream != "" {
		t.Fatalf("invented legacy fidelity: %q %s %s", decoded, fidelity, upstream)
	}
}

func TestPrepareArchiveRefusesUnsafePaths(t *testing.T) {
	directory := t.TempDir()
	missing := filepath.Join(directory, "missing.db")
	backup := filepath.Join(directory, "backup.db")
	if err := PrepareArchive(context.Background(), missing, backup); err == nil {
		t.Fatal("created missing source")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("missing source created: %v", err)
	}
	store, err := Open(filepath.Join(directory, "memory.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := os.WriteFile(backup, []byte("do not overwrite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := PrepareArchive(context.Background(), filepath.Join(directory, "memory.db"), backup); err == nil {
		t.Fatal("overwrote existing backup")
	}
	if err := checkArchiveSchema(context.Background(), store.db); err == nil {
		t.Fatal("migrated despite backup failure")
	}
}

func TestLegacyImportUsesByteBoundedBatches(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{QueueBytes: 2500})
	for index := 0; index < 3; index++ {
		if err := writer.store.Record(MemRecord{ID: fmt.Sprint(index), Adapter: "test", UserID: "owner", Request: "request", Response: strings.Repeat("x", 1200)}); err != nil {
			t.Fatal(err)
		}
	}
	progress, err := writer.ImportLegacyBatch(context.Background(), 1000)
	if err != nil || progress.Imported != 1 || progress.Pending != 2 {
		t.Fatalf("byte budget not respected: %+v %v", progress, err)
	}
}

func TestLegacyImportMappingFailureRollsBack(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	if err := writer.store.Record(MemRecord{ID: "legacy", Adapter: "test", UserID: "owner", Request: "request", Response: "answer"}); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.store.db.Exec(`CREATE TRIGGER reject_import BEFORE INSERT ON mem_v2_legacy_map BEGIN SELECT RAISE(ABORT,'mapping failed'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.ImportLegacyBatch(context.Background(), 10); err == nil {
		t.Fatal("mapping failure ignored")
	}
	var count int
	for _, table := range []string{"mem_v2_messages", "mem_v2_jobs", "mem_v2_blocks", "mem_v2_legacy_map"} {
		if err := writer.store.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("partial import in %s: %d %v", table, count, err)
		}
	}
	if _, err := writer.store.db.Exec(`DROP TRIGGER reject_import`); err != nil {
		t.Fatal(err)
	}
	if progress, err := writer.ImportLegacyBatch(context.Background(), 10); err != nil || progress.Imported != 1 {
		t.Fatalf("retry failed: %+v %v", progress, err)
	}
}
