package config

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/user/opencode-gateway/internal/adapters/dingtalk"
	"github.com/user/opencode-gateway/internal/adapters/feishu"
	"github.com/user/opencode-gateway/internal/adapters/wecom"
)

// Config captures all runtime configuration knobs for the gateway.
type Config struct {
	HTTPEnabled      bool
	ServerAddr       string
	ReadTimeout      time.Duration
	WriteTimeout     time.Duration
	ShutdownGrace    time.Duration
	OpenCodeEndpoint string
	OpenCodeAPIKey   string
	WeCom            wecom.Config
	FeiShu           feishu.Config
	DingTalk         dingtalk.Config
}

// Load reads configuration from environment variables with sensible defaults.
func Load() (Config, error) {
	cfg := Config{
		HTTPEnabled:      getBool("HTTP_ENABLED", false),
		ServerAddr:       getEnv("SERVER_ADDR", ":8080"),
		ReadTimeout:      getDuration("SERVER_READ_TIMEOUT", 30*time.Second),   // 增加读取超时
		WriteTimeout:     getDuration("SERVER_WRITE_TIMEOUT", 300*time.Second), // 增加到5分钟，AI响应可能很慢
		ShutdownGrace:    getDuration("SERVER_SHUTDOWN_GRACE", 30*time.Second),
		OpenCodeEndpoint: getEnv("OPENCODE_ENDPOINT", "http://localhost:4096"),
		OpenCodeAPIKey:   getEnv("OPENCODE_API_KEY", "123"),
		WeCom: wecom.Config{
			Token:          os.Getenv("WECOM_TOKEN"),
			EncodingAESKey: os.Getenv("WECOM_AES_KEY"),
			CorpID:         os.Getenv("WECOM_CORP_ID"),
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
			// 阿里云 NLS 语音识别（可选）
			AliyunNLSAkID:   os.Getenv("ALIYUN_NLS_AKID"),
			AliyunNLSAkKey:  os.Getenv("ALIYUN_NLS_AKKEY"),
			AliyunNLSAppKey: os.Getenv("ALIYUN_NLS_APPKEY"),
		},
	}

	if cfg.OpenCodeEndpoint == "" {
		return cfg, fmt.Errorf("missing OPENCODE_ENDPOINT")
	}

	if cfg.OpenCodeAPIKey == "" {
		return cfg, fmt.Errorf("missing OPENCODE_API_KEY")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
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
