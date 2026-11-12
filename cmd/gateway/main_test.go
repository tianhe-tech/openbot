package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/user/opencode-gateway/internal/config"
	"github.com/user/opencode-gateway/internal/memstore"
)

func TestMemoryMCPRegistrationDefaultsToDisabled(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	name := "openbot-mcp"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	if err := os.WriteFile(filepath.Join(directory, name), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(directory, "mem", "memory.db")
	ensureMCPMemoryTool(directory, database)
	configPath := filepath.Join(directory, "opencode.json")
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		MCP map[string]struct {
			Enabled bool     `json:"enabled"`
			Command []string `json:"command"`
		} `json:"mcp"`
	}
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	entry, exists := config.MCP["gateway-memory"]
	if !exists || entry.Enabled || len(entry.Command) != 3 || entry.Command[2] != database {
		t.Fatalf("unsafe or incorrect registration: %+v", entry)
	}
	existing := []byte(`{"mcp":{"gateway-memory":{"enabled":false,"command":["custom-mcp","--db","custom.db"]}}}`)
	if err := os.WriteFile(configPath, existing, 0o600); err != nil {
		t.Fatal(err)
	}
	ensureMCPMemoryTool(directory, database)
	after, err := os.ReadFile(configPath)
	if err != nil || !bytes.Equal(existing, after) {
		t.Fatalf("operator configuration overwritten: %s, %v", after, err)
	}
}

func TestMemoryCaptureRequiresExplicitMigratedArchive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.db")
	ctx := context.Background()
	legacy := config.MemoryConfig{Enabled: true, CaptureMode: "legacy"}
	if writer, err := openCaptureArchive(ctx, path, legacy); err != nil || writer != nil {
		t.Fatalf("legacy changed: %v", err)
	}
	if _, err := openCaptureArchive(ctx, path, config.MemoryConfig{Enabled: true, CaptureMode: "typo"}); err == nil {
		t.Fatal("invalid mode silently disabled capture")
	}
	strict := config.MemoryConfig{Enabled: true, CaptureMode: "strict"}
	if _, err := openCaptureArchive(ctx, path, strict); err == nil {
		t.Fatal("missing archive accepted")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("missing archive created: %v", err)
	}
	store, err := memstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := openCaptureArchive(ctx, path, strict); err == nil {
		t.Fatal("unmigrated archive accepted")
	}
	if err := store.MigrateArchive(ctx); err != nil {
		t.Fatal(err)
	}
	writer, err := openCaptureArchive(ctx, path, strict)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

// TestMemoryDisabledOverridesStrictCapture pins the precedence rule: an
// operator who sets MEMORY_ENABLED=false must not get capture just because a
// stale MEMORY_CAPTURE_MODE=strict is still in the environment.
func TestMemoryDisabledOverridesStrictCapture(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.db")
	ctx := context.Background()
	writer, err := openCaptureArchive(ctx, path, config.MemoryConfig{Enabled: false, CaptureMode: "strict"})
	if err != nil {
		t.Fatalf("disabled memory failed to start: %v", err)
	}
	if writer != nil {
		t.Fatal("MEMORY_ENABLED=false still opened a capture writer")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("disabled memory created an archive: %v", err)
	}
}

// TestWriterOptionsComeFromConfig proves the tuning settings reach the writer
// rather than being silently dropped on the way from the environment.
func TestWriterOptionsComeFromConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.db")
	ctx := context.Background()
	store, err := memstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MigrateArchive(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	memory := config.MemoryConfig{
		Enabled:       true,
		CaptureMode:   "strict",
		QueueMaxItems: 32,
		QueueMaxBytes: 1 << 20,
		BatchMaxItems: 8,
		BatchMaxBytes: 1 << 16,
		BatchMaxDelay: 3 * time.Millisecond,
		CommitTimeout: 5 * time.Second,
	}
	writer, err := openCaptureArchive(ctx, path, memory)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(ctx)
	got := writer.Options()
	if got.QueueItems != 32 || got.QueueBytes != 1<<20 || got.BatchItems != 8 || got.BatchBytes != 1<<16 {
		t.Fatalf("queue/batch settings not applied: %+v", got)
	}
	if got.BatchDelay != 3*time.Millisecond || got.CommitTimeout != 5*time.Second {
		t.Fatalf("timing settings not applied: %+v", got)
	}
}
