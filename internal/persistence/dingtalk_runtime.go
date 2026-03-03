package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const defaultDingTalkRuntimeConfigFile = ".opencode-gateway-dingtalk.json"

var runtimeFileMu sync.Mutex

type DingTalkRuntimeConfig struct {
	UserWhitelist []string `json:"userWhitelist"`
	OwnerUserID   string   `json:"ownerUserId,omitempty"`
}

func LoadDingTalkRuntimeConfig() (DingTalkRuntimeConfig, bool, error) {
	path := dingtalkRuntimeConfigPath()
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return DingTalkRuntimeConfig{}, false, nil
		}
		return DingTalkRuntimeConfig{}, false, fmt.Errorf("read dingtalk runtime config: %w", err)
	}

	var cfg DingTalkRuntimeConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return DingTalkRuntimeConfig{}, false, fmt.Errorf("decode dingtalk runtime config: %w", err)
	}

	cfg.OwnerUserID = strings.TrimSpace(cfg.OwnerUserID)
	cfg.UserWhitelist = normalizeWhitelist(cfg.UserWhitelist)
	return cfg, true, nil
}

func SaveDingTalkRuntimeConfig(cfg DingTalkRuntimeConfig) error {
	runtimeFileMu.Lock()
	defer runtimeFileMu.Unlock()

	path := dingtalkRuntimeConfigPath()
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create runtime config dir: %w", err)
	}

	cfg.OwnerUserID = strings.TrimSpace(cfg.OwnerUserID)
	cfg.UserWhitelist = normalizeWhitelist(cfg.UserWhitelist)

	payload, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal dingtalk runtime config: %w", err)
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, payload, 0o644); err != nil {
		return fmt.Errorf("write temp runtime config: %w", err)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove old runtime config: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace runtime config: %w", err)
	}

	return nil
}

func dingtalkRuntimeConfigPath() string {
	if raw := strings.TrimSpace(os.Getenv("DINGTALK_RUNTIME_CONFIG_FILE")); raw != "" {
		return raw
	}
	return defaultDingTalkRuntimeConfigFile
}

func normalizeWhitelist(input []string) []string {
	set := make(map[string]struct{}, len(input))
	for _, v := range input {
		item := strings.TrimSpace(v)
		if item == "" {
			continue
		}
		set[item] = struct{}{}
	}

	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	return out
}
