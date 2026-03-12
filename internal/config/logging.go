package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// LogConfig captures log-related startup settings from file.
type LogConfig struct {
	LogLevel string `json:"log_level"`
	Logging  struct {
		Level string `json:"level"`
	} `json:"logging"`
}

// LoadLogConfigFile reads log settings from a JSON config file.
func LoadLogConfigFile(path string) (LogConfig, error) {
	var cfg LogConfig
	if strings.TrimSpace(path) == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config file %s: %w", path, err)
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config file %s: %w", path, err)
	}
	return cfg, nil
}

// ResolveLogLevel returns the final log level and source by precedence.
// Priority: CLI --log-level > config file > LOG_LEVEL env > default(info).
func ResolveLogLevel(cliLevel string, fileCfg LogConfig) (string, string) {
	if v := strings.TrimSpace(cliLevel); v != "" {
		return v, "cli"
	}
	if v := strings.TrimSpace(fileCfg.LogLevel); v != "" {
		return v, "config"
	}
	if v := strings.TrimSpace(fileCfg.Logging.Level); v != "" {
		return v, "config"
	}
	if v := strings.TrimSpace(os.Getenv("LOG_LEVEL")); v != "" {
		return v, "env"
	}
	return "info", "default"
}
