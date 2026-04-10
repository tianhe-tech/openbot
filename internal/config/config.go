package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/user/opencode-gateway/internal/adapters/dingtalk"
	"github.com/user/opencode-gateway/internal/adapters/feishu"
	"github.com/user/opencode-gateway/internal/adapters/wecom"
)

// Config captures all runtime configuration knobs for the gateway.
type Config struct {
	HTTPEnabled            bool
	ServerAddr             string
	ReadTimeout            time.Duration
	WriteTimeout           time.Duration
	ShutdownGrace          time.Duration
	OpenCodeEndpoint       string
	OpenCodeAPIKey         string
	OpenCodeDirectory      string
	OpenCodeDevCoreEnabled bool
	OpenCodeDevCorePrompt  string
	OpenCodeManageServe    bool
	OpenCodeServeCommand   string
	OpenCodeServeArgs      []string
	ProxyHubWSURL          string
	ProxyKeyFile           string
	ProxyLocalAddr         string
	ProxyReconnect         time.Duration
	MemStorePath           string // path to the SQLite memory database; "" disables memory
	WeCom                  wecom.Config
	FeiShu                 feishu.Config
	DingTalk               dingtalk.Config
}

// Load reads configuration from environment variables with sensible defaults.
func Load() (Config, error) {
	cfg := Config{
		HTTPEnabled:            getBool("HTTP_ENABLED", false),
		ServerAddr:             getEnv("SERVER_ADDR", ":8080"),
		ReadTimeout:            getDuration("SERVER_READ_TIMEOUT", 30*time.Second),   // 增加读取超时
		WriteTimeout:           getDuration("SERVER_WRITE_TIMEOUT", 300*time.Second), // 增加到5分钟，AI响应可能很慢
		ShutdownGrace:          getDuration("SERVER_SHUTDOWN_GRACE", 30*time.Second),
		OpenCodeEndpoint:       getEnv("OPENCODE_ENDPOINT", "http://localhost:4096"),
		OpenCodeAPIKey:         getOpenCodeAPIKey(),
		OpenCodeDirectory:      getEnv("OPENCODE_DIRECTORY", "."),
		OpenCodeDevCoreEnabled: getBool("OPENCODE_DEV_CORE_ENABLED", false),
		OpenCodeDevCorePrompt:  strings.TrimSpace(os.Getenv("OPENCODE_DEV_CORE_PROMPT")),
		OpenCodeManageServe:    getBool("OPENCODE_MANAGE_SERVE", true),
		OpenCodeServeCommand:   strings.TrimSpace(getEnv("OPENCODE_SERVE_COMMAND", "opencode")),
		OpenCodeServeArgs:      splitServeArgs(getEnv("OPENCODE_SERVE_ARGS", "serve")),
		ProxyHubWSURL:          strings.TrimSpace(os.Getenv("PROXY_HUB_WS_URL")),
		ProxyKeyFile:           getEnv("PROXY_KEY_FILE", ".opencode-gateway-proxy.json"),
		ProxyLocalAddr:         getEnv("PROXY_LOCAL_OPENCODE_ADDR", "127.0.0.1:4096"),
		ProxyReconnect:         getDuration("PROXY_RECONNECT_DELAY", 5*time.Second),
		MemStorePath:           getEnv("MEMORY_STORE_PATH", ""),
		WeCom: wecom.Config{
			Token:          os.Getenv("WECOM_TOKEN"),
			EncodingAESKey: os.Getenv("WECOM_AES_KEY"),
			CorpID:         os.Getenv("WECOM_CORP_ID"),
			CorpSecret:     os.Getenv("WECOM_CORP_SECRET"),
			AgentID:        os.Getenv("WECOM_AGENT_ID"),
		},
		FeiShu: feishu.Config{
			AppID:             os.Getenv("FEISHU_APP_ID"),
			AppSecret:         os.Getenv("FEISHU_APP_SECRET"),
			VerificationToken: os.Getenv("FEISHU_VERIFICATION_TOKEN"),
			EncryptKey:        os.Getenv("FEISHU_ENCRYPT_KEY"),
			UseWebSocket:      getBool("FEISHU_USE_WEBSOCKET", true), // 默认启用WebSocket模式
			AliyunNLSAkID:     os.Getenv("ALIYUN_NLS_AKID"),
			AliyunNLSAkKey:    os.Getenv("ALIYUN_NLS_AKKEY"),
			AliyunNLSAppKey:   os.Getenv("ALIYUN_NLS_APPKEY"),
		},
		DingTalk: dingtalk.Config{
			// Stream mode (preferred)
			ClientID:     os.Getenv("DINGTALK_CLIENT_ID"),
			ClientSecret: os.Getenv("DINGTALK_CLIENT_SECRET"),
			UseStream:    true, // 启用Stream模式
			// Webhook mode (legacy)
			AppKey:            os.Getenv("DINGTALK_APP_KEY"),
			AppSecret:         os.Getenv("DINGTALK_APP_SECRET"),
			VerificationToken: os.Getenv("DINGTALK_VERIFICATION_TOKEN"),
			EncryptKey:        os.Getenv("DINGTALK_ENCRYPT_KEY"),
			SigningSecret:     os.Getenv("DINGTALK_SIGNING_SECRET"),
			UserWhitelist:     nil,
			OwnerUserID:       strings.TrimSpace(os.Getenv("DINGTALK_OWNER_USERID")),
			NonOwnerPlanMode:  getBool("DINGTALK_NON_OWNER_PLAN_MODE", false),
			// 阿里云 NLS 语音识别（可选）
			AliyunNLSAkID:   os.Getenv("ALIYUN_NLS_AKID"),
			AliyunNLSAkKey:  os.Getenv("ALIYUN_NLS_AKKEY"),
			AliyunNLSAppKey: os.Getenv("ALIYUN_NLS_APPKEY"),
		},
	}

	if cfg.OpenCodeEndpoint == "" {
		return cfg, fmt.Errorf("missing OPENCODE_ENDPOINT")
	}

	if cfg.DingTalk.OwnerUserID != "" {
		cfg.DingTalk.UserWhitelist = []string{cfg.DingTalk.OwnerUserID}
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getOpenCodeAPIKey() string {
	if val := strings.TrimSpace(os.Getenv("OPENCODE_API_KEY")); val != "" {
		return val
	}
	return strings.TrimSpace(os.Getenv("OPENCODE_SERVER_PASSWORD"))
}

func getDuration(key string, fallback time.Duration) time.Duration {
	if raw := os.Getenv(key); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil {
			return d
		}
		if seconds, err := strconv.Atoi(raw); err == nil {
			return time.Duration(seconds) * time.Second
		}
	}
	return fallback
}

func getBool(key string, fallback bool) bool {
	if raw := os.Getenv(key); raw != "" {
		if val, err := strconv.ParseBool(raw); err == nil {
			return val
		}
	}
	return fallback
}

func splitAndTrim(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		v := strings.TrimSpace(p)
		if v != "" {
			out = append(out, v)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func splitServeArgs(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []string{"serve"}
	}
	return strings.Fields(raw)
}
