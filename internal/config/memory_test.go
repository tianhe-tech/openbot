package config

import (
	"strings"
	"testing"
	"time"
)

// TestMemoryDefaultsMatchThePlan pins the documented defaults. If a default
// changes, the plan's configuration table must change with it, so this test is
// the place where that disagreement becomes visible.
func TestMemoryDefaultsMatchThePlan(t *testing.T) {
	t.Setenv("OPENCODE_ENDPOINT", "http://localhost:4096")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	m := cfg.Memory
	if !m.Enabled {
		t.Error("MEMORY_ENABLED should default to true")
	}
	if m.CaptureMode != "legacy" {
		t.Errorf("MEMORY_CAPTURE_MODE default = %q, want legacy", m.CaptureMode)
	}
	if m.ScopeMode != "disabled" {
		t.Errorf("MEMORY_SCOPE_MODE default = %q, want disabled", m.ScopeMode)
	}
	if m.QueueMaxItems != 1024 || m.QueueMaxBytes != 16*1024*1024 {
		t.Errorf("queue defaults = %d/%d, want 1024/16777216", m.QueueMaxItems, m.QueueMaxBytes)
	}
	if m.BatchMaxItems != 64 || m.BatchMaxBytes != 256*1024 {
		t.Errorf("batch defaults = %d/%d, want 64/262144", m.BatchMaxItems, m.BatchMaxBytes)
	}
	if m.BatchMaxDelay != 10*time.Millisecond {
		t.Errorf("MEMORY_BATCH_MAX_DELAY_MS default = %s, want 10ms", m.BatchMaxDelay)
	}
	if m.CommitTimeout != 2*time.Second {
		t.Errorf("MEMORY_COMMIT_TIMEOUT_MS default = %s, want 2s", m.CommitTimeout)
	}
}

// TestMemoryMillisecondSettingsAreMilliseconds is the regression test for the
// unit bug this setting family invites: getDuration reads a bare "10" as ten
// seconds, which would turn a 10 ms batch delay into a 10 s one and make every
// capture commit wait for a batch that never fills.
func TestMemoryMillisecondSettingsAreMilliseconds(t *testing.T) {
	t.Setenv("OPENCODE_ENDPOINT", "http://localhost:4096")
	t.Setenv("MEMORY_BATCH_MAX_DELAY_MS", "10")
	t.Setenv("MEMORY_COMMIT_TIMEOUT_MS", "250")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Memory.BatchMaxDelay != 10*time.Millisecond {
		t.Fatalf("bare 10 parsed as %s, want 10ms", cfg.Memory.BatchMaxDelay)
	}
	if cfg.Memory.CommitTimeout != 250*time.Millisecond {
		t.Fatalf("bare 250 parsed as %s, want 250ms", cfg.Memory.CommitTimeout)
	}
}

// TestMemoryMillisecondSettingsAcceptExplicitUnits keeps the escape hatch
// usable: an operator who writes "1s" means one second, not one millisecond.
func TestMemoryMillisecondSettingsAcceptExplicitUnits(t *testing.T) {
	t.Setenv("OPENCODE_ENDPOINT", "http://localhost:4096")
	t.Setenv("MEMORY_COMMIT_TIMEOUT_MS", "1s")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Memory.CommitTimeout != time.Second {
		t.Fatalf("explicit unit parsed as %s, want 1s", cfg.Memory.CommitTimeout)
	}
}

// TestMemoryRejectsImpossibleBatch covers the settings that would make the
// writer wait forever: a batch larger than the queue can never be filled.
func TestMemoryRejectsImpossibleBatch(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{
			name: "batch items exceed queue items",
			env:  map[string]string{"MEMORY_QUEUE_MAX_ITEMS": "8", "MEMORY_BATCH_MAX_ITEMS": "64"},
			want: "MEMORY_BATCH_MAX_ITEMS",
		},
		{
			name: "batch bytes exceed queue bytes",
			env:  map[string]string{"MEMORY_QUEUE_MAX_BYTES": "1024", "MEMORY_BATCH_MAX_BYTES": "262144"},
			want: "MEMORY_BATCH_MAX_BYTES",
		},
		{
			name: "negative queue items",
			env:  map[string]string{"MEMORY_QUEUE_MAX_ITEMS": "-1"},
			want: "MEMORY_QUEUE_MAX_ITEMS",
		},
		{
			name: "negative commit timeout",
			env:  map[string]string{"MEMORY_COMMIT_TIMEOUT_MS": "-5"},
			want: "MEMORY_COMMIT_TIMEOUT_MS",
		},
		{
			name: "unknown capture mode",
			env:  map[string]string{"MEMORY_CAPTURE_MODE": "best_effort"},
			want: "MEMORY_CAPTURE_MODE",
		},
		{
			name: "unknown scope mode",
			env:  map[string]string{"MEMORY_SCOPE_MODE": "shared"},
			want: "MEMORY_SCOPE_MODE",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OPENCODE_ENDPOINT", "http://localhost:4096")
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			_, err := Load()
			if err == nil {
				t.Fatalf("invalid configuration accepted")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name %s", err, tc.want)
			}
		})
	}
}

// TestMemoryDeprecatedFieldsTrackValidatedValues guards the compatibility
// bridge: a caller still reading cfg.MemStorePath must see the same value the
// new nested config holds, including the normalisation Load applies.
func TestMemoryDeprecatedFieldsTrackValidatedValues(t *testing.T) {
	t.Setenv("OPENCODE_ENDPOINT", "http://localhost:4096")
	t.Setenv("MEMORY_STORE_PATH", "/var/lib/memory.db")
	t.Setenv("MEMORY_CAPTURE_MODE", "  STRICT  ")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.MemStorePath != cfg.Memory.StorePath {
		t.Fatalf("deprecated path %q != nested %q", cfg.MemStorePath, cfg.Memory.StorePath)
	}
	if cfg.MemoryCaptureMode != cfg.Memory.CaptureMode {
		t.Fatalf("deprecated mode %q != nested %q", cfg.MemoryCaptureMode, cfg.Memory.CaptureMode)
	}
	if cfg.Memory.CaptureMode != "strict" {
		t.Fatalf("mode not normalised: %q", cfg.Memory.CaptureMode)
	}
}

// TestMemoryCaptureEnabledRequiresBothSwitches documents the precedence rule
// the gateway relies on: strict capture needs MEMORY_ENABLED and
// MEMORY_CAPTURE_MODE=strict, and either one alone is not enough.
func TestMemoryCaptureEnabledRequiresBothSwitches(t *testing.T) {
	cases := []struct {
		enabled bool
		mode    string
		want    bool
	}{
		{true, "strict", true},
		{true, "legacy", false},
		{false, "strict", false},
		{false, "legacy", false},
	}
	for _, tc := range cases {
		got := MemoryConfig{Enabled: tc.enabled, CaptureMode: tc.mode}.MemoryCaptureEnabled()
		if got != tc.want {
			t.Errorf("Enabled=%v CaptureMode=%q => %v, want %v", tc.enabled, tc.mode, got, tc.want)
		}
	}
}

// TestKnowledgeExtractionDefaultsToOff pins that the expensive optional feature
// is opt-in. Extraction spends model calls, so a default of true would make
// every deployment pay for a feature it never asked for.
func TestKnowledgeExtractionDefaultsToOff(t *testing.T) {
	t.Setenv("OPENCODE_ENDPOINT", "http://localhost:4096")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Memory.KnowledgeEnabled {
		t.Fatal("MEMORY_KNOWLEDGE_ENABLED should default to false")
	}
}

// TestKnowledgeExtractionRequiresStrictCapture pins the dependency.
//
// Extraction reads the archive that capture writes. Enabling it without capture
// would produce a service that can never find evidence, so the configuration is
// rejected rather than silently doing nothing: the operator asked for a feature
// whose input does not exist, and a no-op would look like a bug in the feature.
func TestKnowledgeExtractionRequiresStrictCapture(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
	}{
		{"capture disabled", map[string]string{"MEMORY_ENABLED": "false", "MEMORY_CAPTURE_MODE": "strict"}},
		{"capture in legacy mode", map[string]string{"MEMORY_ENABLED": "true", "MEMORY_CAPTURE_MODE": "legacy"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("OPENCODE_ENDPOINT", "http://localhost:4096")
			t.Setenv("MEMORY_KNOWLEDGE_ENABLED", "true")
			for key, value := range tc.env {
				t.Setenv(key, value)
			}
			_, err := Load()
			if err == nil {
				t.Fatalf("knowledge extraction was accepted without strict capture")
			}
			if !strings.Contains(err.Error(), "MEMORY_KNOWLEDGE_ENABLED") {
				t.Fatalf("error %q does not name MEMORY_KNOWLEDGE_ENABLED", err)
			}
		})
	}
}

// TestKnowledgeExtractionIsAcceptedWithStrictCapture is the positive half: the
// rule must reject the impossible combination without rejecting the possible
// one.
func TestKnowledgeExtractionIsAcceptedWithStrictCapture(t *testing.T) {
	t.Setenv("OPENCODE_ENDPOINT", "http://localhost:4096")
	t.Setenv("MEMORY_ENABLED", "true")
	t.Setenv("MEMORY_CAPTURE_MODE", "strict")
	t.Setenv("MEMORY_KNOWLEDGE_ENABLED", "true")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("a valid knowledge configuration was rejected: %v", err)
	}
	if !cfg.Memory.KnowledgeEnabled {
		t.Fatal("MEMORY_KNOWLEDGE_ENABLED=true was not carried into the config")
	}
}
