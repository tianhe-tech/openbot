package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/user/opencode-gateway/internal/memstore"
)

func TestMigrationCommandInitializeAndResume(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "memory.db")
	store, err := memstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Record(memstore.MemRecord{ID: "legacy", Adapter: "test", UserID: "owner", Request: "request", Response: "answer"}); err != nil {
		t.Fatal(err)
	}
	store.Close()
	var output bytes.Buffer
	if err := run(context.Background(), []string{"--db", path, "--backup", filepath.Join(directory, "before.db"), "--batch-size", "1"}, &output); err != nil {
		t.Fatal(err)
	}
	var progress memstore.ImportProgress
	if err := json.Unmarshal(output.Bytes(), &progress); err != nil || progress.Imported != 1 || progress.Pending != 0 {
		t.Fatalf("initial progress: %s, %v", output.String(), err)
	}
	output.Reset()
	if err := run(context.Background(), []string{"--db", path, "--resume"}, &output); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(output.Bytes(), &progress); err != nil || progress.Imported != 0 {
		t.Fatalf("resume duplicated data: %s, %v", output.String(), err)
	}
	store, err = memstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Record(memstore.MemRecord{ID: "unmapped", Request: "request", Response: "answer"}); err != nil {
		t.Fatal(err)
	}
	store.Close()
	output.Reset()
	if err := run(context.Background(), []string{"--db", path, "--resume"}, &output); err == nil {
		t.Fatal("unmapped rows reported as complete")
	}
	if err := json.Unmarshal(output.Bytes(), &progress); err != nil || progress.Unmapped != 1 {
		t.Fatalf("missing unmapped report: %s, %v", output.String(), err)
	}
}

func TestMigrationCommandRejectsUnsafeArguments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.db")
	for _, args := range [][]string{
		{}, {"--db", path}, {"--db", path, "--resume"},
		{"--db", path, "--resume", "--backup", "unused.db"},
		{"--db", path, "--batch-size", "0"}, {"--db", path, "--queue-bytes", "-1"},
	} {
		if err := run(context.Background(), args, &bytes.Buffer{}); err == nil {
			t.Fatalf("accepted unsafe arguments: %v", args)
		}
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("command created a missing source: %v", err)
	}
}
