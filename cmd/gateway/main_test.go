package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

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
	if writer, err := openCaptureArchive(ctx, path, "legacy"); err != nil || writer != nil {
		t.Fatalf("legacy changed: %v", err)
	}
	if _, err := openCaptureArchive(ctx, path, "typo"); err == nil {
		t.Fatal("invalid mode silently disabled capture")
	}
	if _, err := openCaptureArchive(ctx, path, "strict"); err == nil {
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
	if _, err := openCaptureArchive(ctx, path, "strict"); err == nil {
		t.Fatal("unmigrated archive accepted")
	}
	if err := store.MigrateArchive(ctx); err != nil {
		t.Fatal(err)
	}
	writer, err := openCaptureArchive(ctx, path, "strict")
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
