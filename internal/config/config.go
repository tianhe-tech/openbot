package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/user/opencode-gateway/internal/adapters/dingtalk"
	"github.com/user/opencode-gateway/internal/adapters/feishu"
	"github.com/user/opencode-gateway/internal/adapters/wechat"
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
	WeChat                 wechat.Config
	FeiShu                 feishu.Config
	DingTalk               dingtalk.Config

	// Skill autogen (Hermes-style): mine reusable SKILL.md drafts from
	// completed or stuck sessions, idle-gated so it never interferes with
	// active Q&A. Disabled by default.
	SkillAutogen SkillAutogenConfig

	// RetryQueue: offline retry for messages that timed out (context deadline
	// exceeded with zero accumulated reply). Disabled by default.
	RetryQueue RetryQueueConfig

	// CircuitBreaker: per provider/model failure tracking. When a provider
	// fails with a provider-level error (e.g. "No available client"), the
	// breaker opens and selectModelOverride skips it until a half-open probe
	// succeeds. Enabled by default.
	CircuitBreaker CircuitBreakerConfig
}

// CircuitBreakerConfig is the env-driven sub-config for the opencode client
// per-model circuit breaker.
type CircuitBreakerConfig struct {
	// Enabled: set OPENCODE_CIRCUIT_BREAKER_ENABLED=false to disable (default true).
	Enabled bool
	// FailureThreshold: consecutive provider-level failures before opening (default 2).
	FailureThreshold int
	// Cooldown: initial open duration before a half-open probe (default 60s).
	Cooldown time.Duration
	// MaxCooldown: cap for exponential backoff on repeated probe failures (default 30m).
	MaxCooldown time.Duration
	// TripKeywords: comma-separated error substrings that trip the breaker.
	// Empty means all session.error events trip. Default covers common
	// provider-outage messages.
	TripKeywords []string
}

// SkillAutogenConfig is the env-driven sub-config for internal/skillgen.
type SkillAutogenConfig struct {
	Enabled             bool
	DraftModel          string
	AlternateModels     []string
	Epsilon             float64
	ModelSelfSelect     bool
	MaxPerDay           int
	OnHandoff           bool
	OnLongSession       bool
	LongSessionMinTurns int
	CandidateDir        string
	InstallDir          string
	ApprovalRequired    bool
	MinConfidence       float64
	MinToolCalls        int
	QueueCapacity       int
	ReferenceSkillPath  string
}

// RetryQueueConfig is the env-driven sub-config for internal/retryworker.
type RetryQueueConfig struct {
	// Enabled: set RETRY_QUEUE_ENABLED=true to activate.
	Enabled bool
	// CronExpr: when to auto-run the retry worker (default "0 22 * * *").
	CronExpr string
	// MaxRetries: per-message retry limit before marking permanently failed (default 3).
	MaxRetries int
	// BatchSize: messages per run (default 20).
	BatchSize int
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
		ProxyReconnect:         getDuration("PROXY_RECONNECT_DELAY", 180*time.Second),
		MemStorePath:           getEnv("MEMORY_STORE_PATH", ""),
		WeCom: wecom.Config{
			Token:          os.Getenv("WECOM_TOKEN"),
			EncodingAESKey: os.Getenv("WECOM_AES_KEY"),
			CorpID:         os.Getenv("WECOM_CORP_ID"),
			CorpSecret:     os.Getenv("WECOM_CORP_SECRET"),
			AgentID:        os.Getenv("WECOM_AGENT_ID"),
		},
		WeChat: wechat.Config{
			BotToken:   os.Getenv("WECHAT_BOT_TOKEN"),
			BaseURL:    getEnv("WECHAT_BASE_URL", "https://ilinkai.weixin.qq.com"),
			AccountID:  os.Getenv("WECHAT_ACCOUNT_ID"),
			StateDir:   os.Getenv("WECHAT_STATE_DIR"),
			CDNBaseURL: getEnv("WECHAT_CDN_BASE_URL", "https://ilinkai.weixin.qq.com"),
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
		SkillAutogen: SkillAutogenConfig{
			Enabled:             getBool("SKILLGEN_ENABLED", false),
			DraftModel:          strings.TrimSpace(os.Getenv("SKILLGEN_DRAFT_MODEL")),
			AlternateModels:     splitAndTrim(os.Getenv("SKILLGEN_ALTERNATE_MODELS")),
			Epsilon:             getFloat("SKILLGEN_EPSILON", 0.15),
			ModelSelfSelect:     getBool("SKILLGEN_MODEL_SELF_SELECT", true),
			MaxPerDay:           getInt("SKILLGEN_MAX_PER_DAY", 5),
			OnHandoff:           getBool("SKILLGEN_ON_HANDOFF", true),
			OnLongSession:       getBool("SKILLGEN_ON_LONG_SESSION", true),
			LongSessionMinTurns: getInt("SKILLGEN_LONG_SESSION_MIN_TURNS", 8),
			MinToolCalls:        getInt("SKILLGEN_MIN_TOOL_CALLS", 3),
			CandidateDir:        getEnv("SKILLGEN_CANDIDATE_DIR", "skills-candidates"),
			InstallDir:          getEnv("SKILLGEN_INSTALL_DIR", "skills"),
			ApprovalRequired:    getBool("SKILLGEN_APPROVAL_REQUIRED", true),
			MinConfidence:       getFloat("SKILLGEN_MIN_CONFIDENCE", 0.4),
			QueueCapacity:       getInt("SKILLGEN_QUEUE_CAPACITY", 128),
			ReferenceSkillPath:  getEnv("SKILLGEN_REFERENCE_SKILL", "skills/skill-creator/SKILL.md"),
		},
		RetryQueue: RetryQueueConfig{
			Enabled:    getBool("RETRY_QUEUE_ENABLED", false),
			CronExpr:   getEnv("RETRY_QUEUE_CRON", "0 22 * * *"),
			MaxRetries: getInt("RETRY_QUEUE_MAX_RETRIES", 3),
			BatchSize:  getInt("RETRY_QUEUE_BATCH_SIZE", 20),
		},
		CircuitBreaker: CircuitBreakerConfig{
			Enabled:          getBool("OPENCODE_CIRCUIT_BREAKER_ENABLED", true),
			FailureThreshold: getInt("OPENCODE_CB_FAILURE_THRESHOLD", 2),
			Cooldown:         getDuration("OPENCODE_CB_COOLDOWN_SECONDS", 60*time.Second),
			MaxCooldown:      getDuration("OPENCODE_CB_MAX_COOLDOWN_SECONDS", 30*time.Minute),
			TripKeywords: splitAndTrim(getEnv("OPENCODE_CB_TRIP_KEYWORDS",
				"No available client,No available channel,Insufficient Balance,provider unavailable")),
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

func getInt(key string, fallback int) int {
	if raw := os.Getenv(key); raw != "" {
		if v, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil {
			return v
		}
	}
	return fallback
}

func getFloat(key string, fallback float64) float64 {
	if raw := os.Getenv(key); raw != "" {
		if v, err := strconv.ParseFloat(strings.TrimSpace(raw), 64); err == nil {
			return v
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
