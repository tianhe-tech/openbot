package opencode

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
	"github.com/user/opencode-gateway/internal/bootstrap"
)

// ErrEmptyPayload indicates the caller attempted to send an empty message.
var ErrEmptyPayload = errors.New("opencode: empty payload")

// ErrDuplicateRequest indicates a duplicate request was detected.
var ErrDuplicateRequest = errors.New("opencode: duplicate request detected")

// ErrMaxRetriesExceeded indicates all retry attempts failed.
var ErrMaxRetriesExceeded = errors.New("opencode: max retries exceeded")

const (
	// DefaultMaxTokens 默认最大token数（当无法获取模型配置时使用）
	DefaultMaxTokens = 4096
	// EstimatedTokensPerMessage 估算每条消息的平均token数
	EstimatedTokensPerMessage = 200
	// FallbackMaxMessages 降级方案：按消息数判断（当token估算不可用时）
	FallbackMaxMessages = 50

	// MaxRetries 最大重试次数
	MaxRetries = 3
	// InitialRetryDelay 初始重试延迟
	InitialRetryDelay = 2 * time.Second
	// MaxRetryDelay 最大重试延迟
	MaxRetryDelay = 30 * time.Second
	// RequestDeduplicationWindow 请求去重时间窗口（只防止快速重复点击）
	RequestDeduplicationWindow = 30 * time.Second
)

// Response represents the minimal data we expect back from OpenCode.
type Response struct {
	Reply     string                 `json:"reply"`
	Trace     string                 `json:"trace_id"`
	SessionID string                 `json:"session_id"`
	MessageID string                 `json:"message_id"`
	Raw       map[string]interface{} `json:"raw,omitempty"`
}

// Attachment 表示附件（图片、语音、视频等媒体文件）
// URL 必须是 data URI 格式：data:<mime>;base64,<base64data>
type Attachment struct {
	Mime     string `json:"mime"`               // MIME 类型，如 image/jpeg、image/png
	URL      string `json:"url"`                // data URI: data:<mime>;base64,<base64>
	Filename string `json:"filename,omitempty"` // 可选文件名
}

// MessagePayload collects the metadata adapters send to OpenCode.
type MessagePayload struct {
	Channel     string            `json:"channel"`
	UserID      string            `json:"user_id"`
	ThreadID    string            `json:"thread_id,omitempty"`
	SessionID   string            `json:"session_id,omitempty"`
	Content     string            `json:"content"`
	Agent       string            `json:"agent,omitempty"`     // 可选：指定使用的agent/skill名称
	Streaming   bool              `json:"streaming,omitempty"` // 是否使用流式返回
	Metadata    map[string]string `json:"metadata,omitempty"`
	Attachments []Attachment      `json:"attachments,omitempty"` // 附件（图片/语音/视频）

	// slotPreAcquired is an internal flag set by SendMessageStreaming.
	// When true, SendMessage skips per-session slot acquisition because
	// SendMessageStreaming already holds the slot and its doneFn releases it.
	slotPreAcquired bool
}

// StreamCallback defines a callback for streaming responses.
type StreamCallback func(chunk string) error

// EventHandler defines a callback for incoming OpenCode events.
type EventHandler func(ctx context.Context, event *opencode.EventListResponse) error

// ServerUnavailableHandler is invoked when OpenCode server is unreachable.
// Typical use is to ensure `opencode serve` is running.
type ServerUnavailableHandler func(ctx context.Context, reason string) (string, error)

// ModelConfig 存储模型配置信息
type ModelConfig struct {
	ProviderID    string // 提供商ID (如 "anthropic", "openai")
	ModelID       string // 模型ID (如 "claude-3-opus", "gpt-4")
	ContextLength int    // 模型上下文长度
	LastUpdated   time.Time
}

// ModelCapability stores model input/output modality capabilities.
type ModelCapability struct {
	ProviderID       string
	ModelID          string
	InputModalities  map[string]bool
	OutputModalities map[string]bool
}

// RequestRecord 记录已处理的请求用于去重
type RequestRecord struct {
	Hash      string
	Response  Response
	Timestamp time.Time
	InFlight  bool // 是否正在处理中
}

// RetryConfig 重试配置
type RetryConfig struct {
	MaxRetries      int
	InitialDelay    time.Duration
	MaxDelay        time.Duration
	RetryableErrors []string // 可重试的错误类型关键字
}

// QuestionOption 表示问题选项
type QuestionOption struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

// QuestionItem 表示单个子问题
type QuestionItem struct {
	Header   string           `json:"header"`
	Question string           `json:"question"`
	Multiple bool             `json:"multiple"`
	Options  []QuestionOption `json:"options"`
}

// TodoItem represents a single TODO item tracked by OpenCode during a session.
// Mirrors the "todo.updated" SSE event structure from the OpenCode server.
type TodoItem struct {
	ID       string `json:"id"`
	Task     string `json:"task"`     // 兼容旧字段
	Content  string `json:"content"`  // OpenCode SDK todo.updated 当前字段
	Status   string `json:"status"`   // "pending", "in_progress", "completed", "cancelled"
	Priority string `json:"priority"` // "high", "medium", "low"
}

// Text returns the human-readable todo text with backward/forward compatibility.
func (t TodoItem) Text() string {
	if strings.TrimSpace(t.Content) != "" {
		return t.Content
	}
	return t.Task
}

// PriorityLabel returns a user-friendly priority label.
func (t TodoItem) PriorityLabel() string {
	switch strings.ToLower(strings.TrimSpace(t.Priority)) {
	case "high":
		return "高"
	case "medium":
		return "中"
	case "low":
		return "低"
	default:
		if strings.TrimSpace(t.Priority) == "" {
			return "未设置"
		}
		return t.Priority
	}
}

// FileDiff represents changes to a single file within a session.
// Mirrors the "session.diff" SSE event structure from the OpenCode server.
type FileDiff struct {
	Path    string `json:"path"`
	Added   int    `json:"added"`
	Removed int    `json:"removed"`
}

// Question represents a pending question that needs user confirmation
type Question struct {
	ID           string         `json:"id"`
	SessionID    string         `json:"session_id"`
	MessageID    string         `json:"message_id"`
	Text         string         `json:"text"`          // 简化的问题文本（向后兼容）
	Options      []string       `json:"options"`       // 简化的选项列表（向后兼容）
	Questions    []QuestionItem `json:"questions"`     // 详细的子问题列表（新版）
	IsPermission bool           `json:"is_permission"` // 是否是权限请求
	Directory    string         `json:"directory"`     // 权限请求的工作目录
	CreatedAt    time.Time      `json:"created_at"`
}

// Client knows how to talk to the remote OpenCode service using the official SDK.
type Client struct {
	sdk                        *opencode.Client
	endpoint                   string
	apiKey                     string
	basicAuthHeader            string
	httpClient                 *http.Client
	eventHandlers              []EventHandler
	eventListenerMu            sync.RWMutex
	sessionHandlers            sync.Map     // map[sessionID]EventHandler for fast lookup
	activeHandlers             sync.Map     // map[sessionID]*StreamingSessionHandler for todo/diff access
	messageToSession           sync.Map     // map[messageID]sessionID for events with only messageID
	partToSession              sync.Map     // map[partID]sessionID for message.part.delta fallback routing
	messageRoles               sync.Map     // map[messageID]role ("user"/"assistant") for filtering user message delta events
	lastDiffSummary            sync.Map     // map[sessionID]string dedupe auto-pushed session.diff summaries across turns
	sessionMu                  sync.RWMutex // 用于保护 session 相关操作
	sessions                   sync.Map     // map[sessionMapKey]sessionID, sessionMapKey=channel:threadID (legacy fallback: threadID)
	sessionLocks               sync.Map     // map[threadID]*sync.Mutex for preventing concurrent session operations
	sessionsMu                 sync.RWMutex // 保护 sessions 的读写
	messageCount               sync.Map     // map[sessionID]int tracks messages per session
	tokenCount                 sync.Map     // map[sessionID]int tracks estimated tokens per session
	sessionSummary             sync.Map     // map[sessionID]string stores session summaries
	modelConfig                sync.Map     // map[sessionID]*ModelConfig caches model config per session
	sessionModel               sync.Map     // map[sessionID]*ModelConfig tracks latest provider/model seen in assistant replies
	sessionOverride            sync.Map     // map[sessionID]*ModelConfig stores user-selected model via /model
	requestCache               sync.Map     // map[requestHash]*RequestRecord 请求去重缓存
	sessionQueues              sync.Map     // map[sessionID]chan struct{} (cap=1) per-session semaphore; held ⟺ running
	pendingQuestions           sync.Map     // map[questionID]*Question 待回答的问题
	modelCatalogMu             sync.RWMutex
	modelCatalog               map[string]*ModelCapability // key: providerID/modelID
	defaultModelMu             sync.RWMutex
	defaultModel               *ModelConfig
	directory                  string
	timeout                    time.Duration // 默认超时时间
	retryConfig                RetryConfig   // 重试配置
	debugMediaRouting          bool          // 是否启用多模态路由调试日志
	enableSkillHint            bool          // 是否在消息中添加skill提示
	skillHintCache             []string      // 缓存的可用skill列表
	skillCacheMu               sync.RWMutex
	personaSetupPrompt         string
	personaPromptSent          sync.Map // map[channel:userID]struct{}
	personaSetupPending        sync.Map // map[channel:userID]struct{} answers to startup persona questions are expected
	memoryEnabled              bool
	memoryDir                  string
	memoryMaxChars             int
	memoryMaxFacts             int
	memoryInjectFacts          int
	memoryShortTermFacts       int
	memoryRecentWindow         time.Duration
	memoryDecayEnabled         bool
	memoryDecayHalfLife        time.Duration
	memoryDebugRecall          bool
	memoryCategoryQuotaEnabled bool
	memoryCategoryQuota        map[string]int
	memoryAutoCompactInterval  time.Duration
	memoryDefaults             MemoryRuntimeConfig
	memoryMu                   sync.Mutex
	lastHealthCheck            time.Time    // 最后一次健康检查时间
	isHealthy                  bool         // OpenCode server是否健康
	healthCheckMu              sync.RWMutex // 保护健康状态
	thinkingEnabled            atomic.Bool  // 是否输出 reasoning/thinking 内容
	finalOnlyEnabled           atomic.Bool  // 是否仅在结束时发送最终回复
	stepEnabled                atomic.Bool  // 是否显示 step-start/step-finish 中间步骤
	serverUnavailable          ServerUnavailableHandler
	unavailableMu              sync.Mutex
	lastUnavailableAt          time.Time
	unavailableDelay           time.Duration

	// dispatcher is the SSE event routing hub.  It replaces the old
	// eventHandlers/eventListenerMu/sessionHandlers trio with a single
	// component that guarantees per-session ordered delivery and exposes
	// a clean API for registering global and session-specific handlers.
	dispatcher *SSEDispatcher
}

type memoryFact struct {
	Text       string    `json:"text"`
	Category   string    `json:"category"`
	Importance int       `json:"importance"`
	LastSeen   time.Time `json:"last_seen"`
	Count      int       `json:"count"`
}

type userMemoryStore struct {
	Version int          `json:"version"`
	Facts   []memoryFact `json:"facts"`
}

// MemoryFactView is a safe exported view used by adapters.
type MemoryFactView struct {
	Text       string
	Category   string
	Importance int
	LastSeen   time.Time
	Count      int
}

// MemoryRecallResult exposes scored recall candidates for debugging/tuning.
type MemoryRecallResult struct {
	Text       string    `json:"text"`
	Category   string    `json:"category"`
	Importance int       `json:"importance"`
	Count      int       `json:"count"`
	LastSeen   time.Time `json:"last_seen"`
	Score      float64   `json:"score"`
	Layer      string    `json:"layer"`
	Reasons    []string  `json:"reasons"`
}

// MemoryRuntimeConfig is a snapshot of memory tuning knobs that can be changed at runtime.
type MemoryRuntimeConfig struct {
	Enabled              bool           `json:"enabled"`
	MaxChars             int            `json:"max_chars"`
	MaxFacts             int            `json:"max_facts"`
	InjectFacts          int            `json:"inject_facts"`
	ShortTermFacts       int            `json:"short_term_facts"`
	RecentWindow         string         `json:"recent_window"`
	DecayEnabled         bool           `json:"decay_enabled"`
	DecayHalfLife        string         `json:"decay_half_life"`
	CategoryQuotaEnabled bool           `json:"category_quota_enabled"`
	CategoryQuota        map[string]int `json:"category_quota"`
	AutoCompactInterval  string         `json:"auto_compact_interval"`
	DebugRecall          bool           `json:"debug_recall"`
}

// Option mutates a client during construction.
type Option func(*Client)

// WithDirectory sets the working directory for sessions.
func WithDirectory(dir string) Option {
	return func(c *Client) {
		c.directory = dir
	}
}

// Directory returns the current working directory context used by OpenCode APIs.
func (c *Client) Directory() string {
	return strings.TrimSpace(c.directory)
}

// WithTimeout sets the default timeout for OpenCode operations.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		c.timeout = timeout
	}
}

// WithEventHandler registers a handler for incoming events.
func WithEventHandler(handler EventHandler) Option {
	return func(c *Client) {
		c.eventHandlers = append(c.eventHandlers, handler)
	}
}

// WithRetryConfig sets custom retry configuration.
func WithRetryConfig(cfg RetryConfig) Option {
	return func(c *Client) {
		c.retryConfig = cfg
	}
}

// WithSkillHint enables automatic skill hint injection.
func WithSkillHint(enable bool) Option {
	return func(c *Client) {
		c.enableSkillHint = enable
	}
}

// WithDebugMediaRouting enables detailed media routing debug logs.
func WithDebugMediaRouting(enable bool) Option {
	return func(c *Client) {
		c.debugMediaRouting = enable
	}
}

// WithServerUnavailableHandler installs a self-healing hook used when the
// OpenCode server is unreachable (for example connection refused on /event).
func WithServerUnavailableHandler(handler ServerUnavailableHandler) Option {
	return func(c *Client) {
		c.serverUnavailable = handler
	}
}

func parseEnvBool(key string) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return false
	}
	b, err := strconv.ParseBool(v)
	if err == nil {
		return b
	}
	switch strings.ToLower(v) {
	case "1", "y", "yes", "on", "enable", "enabled":
		return true
	default:
		return false
	}
}

func parseEnvBoolDefault(key string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err == nil {
		return b
	}
	switch strings.ToLower(v) {
	case "1", "y", "yes", "on", "enable", "enabled":
		return true
	case "0", "n", "no", "off", "disable", "disabled":
		return false
	default:
		return fallback
	}
}

// NewClient builds a Client instance using the official OpenCode SDK.
func NewClient(endpoint, apiKey string, opts ...Option) *Client {
	sdkOptions := []option.RequestOption{
		option.WithBaseURL(endpoint),
	}
	trimmedAPIKey := strings.TrimSpace(apiKey)
	serverPassword := strings.TrimSpace(os.Getenv("OPENCODE_SERVER_PASSWORD"))
	basicAuthHeader := ""
	if serverPassword != "" {
		username := strings.TrimSpace(os.Getenv("OPENCODE_SERVER_USERNAME"))
		if username == "" {
			username = "opencode"
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(username + ":" + serverPassword))
		basicAuthHeader = "Basic " + encoded
		sdkOptions = append(sdkOptions, option.WithHeader("Authorization", basicAuthHeader))
	} else if trimmedAPIKey != "" {
		sdkOptions = append(sdkOptions,
			option.WithHeader("Authorization", "Bearer "+trimmedAPIKey),
			option.WithHeader("X-API-Key", trimmedAPIKey),
		)
	}

	client := &Client{
		sdk:             opencode.NewClient(sdkOptions...),
		endpoint:        strings.TrimRight(endpoint, "/"),
		apiKey:          trimmedAPIKey,
		basicAuthHeader: basicAuthHeader,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
		eventHandlers: make([]EventHandler, 0),
		directory:     ".",
		timeout:       1200 * time.Second, // 20分钟超时，给复杂任务（如模型微调、大规模代码生成）足够时间
		retryConfig: RetryConfig{
			MaxRetries:   MaxRetries,
			InitialDelay: InitialRetryDelay,
			MaxDelay:     MaxRetryDelay,
			RetryableErrors: []string{
				// 注意：不包括 "context deadline exceeded"，因为超时意味着任务需要更长时间
				// 重试会导致重复发送请求到OpenCode
				"connection refused",
				"connection reset",
				"temporarily unavailable",
				"503",
				"502",
				"500",
			},
		},
		enableSkillHint:            false, // 默认禁用skill提示
		debugMediaRouting:          parseEnvBool("OPENBOT_DEBUG_MEDIA_ROUTING"),
		modelCatalog:               make(map[string]*ModelCapability),
		memoryEnabled:              parseEnvBoolDefault("OPENCODE_GATEWAY_MEMORY_ENABLED", true),
		memoryDir:                  strings.TrimSpace(os.Getenv("OPENCODE_GATEWAY_MEMORY_DIR")),
		memoryMaxChars:             4000,
		memoryMaxFacts:             40,
		memoryInjectFacts:          8,
		memoryShortTermFacts:       3,
		memoryRecentWindow:         48 * time.Hour,
		memoryDecayEnabled:         true,
		memoryDecayHalfLife:        7 * 24 * time.Hour,
		memoryDebugRecall:          parseEnvBool("OPENCODE_GATEWAY_MEMORY_DEBUG_RECALL"),
		memoryCategoryQuotaEnabled: parseEnvBoolDefault("OPENCODE_GATEWAY_MEMORY_CATEGORY_QUOTA_ENABLED", true),
		memoryCategoryQuota:        map[string]int{},
		memoryAutoCompactInterval:  6 * time.Hour,
		isHealthy:                  false,       // 初始状态未知
		lastHealthCheck:            time.Time{}, // 未检查过
		unavailableDelay:           20 * time.Second,
		dispatcher:                 NewSSEDispatcher(),
	}
	log.Printf("opencode: initializing client endpoint=%s memory_enabled=%t memory_dir_env=%s", endpoint, client.memoryEnabled, client.memoryDir)
	if rawMax := strings.TrimSpace(os.Getenv("OPENCODE_GATEWAY_MEMORY_MAX_CHARS")); rawMax != "" {
		if parsed, err := strconv.Atoi(rawMax); err == nil && parsed > 0 {
			client.memoryMaxChars = parsed
		}
	}
	if rawMaxFacts := strings.TrimSpace(os.Getenv("OPENCODE_GATEWAY_MEMORY_MAX_FACTS")); rawMaxFacts != "" {
		if parsed, err := strconv.Atoi(rawMaxFacts); err == nil && parsed > 0 {
			client.memoryMaxFacts = parsed
		}
	}
	if rawInjectFacts := strings.TrimSpace(os.Getenv("OPENCODE_GATEWAY_MEMORY_INJECT_FACTS")); rawInjectFacts != "" {
		if parsed, err := strconv.Atoi(rawInjectFacts); err == nil && parsed > 0 {
			client.memoryInjectFacts = parsed
		}
	}
	if rawShort := strings.TrimSpace(os.Getenv("OPENCODE_GATEWAY_MEMORY_SHORT_TERM_FACTS")); rawShort != "" {
		if parsed, err := strconv.Atoi(rawShort); err == nil && parsed >= 0 {
			client.memoryShortTermFacts = parsed
		}
	}
	if rawRecent := strings.TrimSpace(os.Getenv("OPENCODE_GATEWAY_MEMORY_RECENT_WINDOW")); rawRecent != "" {
		if parsed, err := time.ParseDuration(rawRecent); err == nil && parsed > 0 {
			client.memoryRecentWindow = parsed
		}
	}
	client.memoryDecayEnabled = parseEnvBoolDefault("OPENCODE_GATEWAY_MEMORY_DECAY_ENABLED", true)
	if rawHalf := strings.TrimSpace(os.Getenv("OPENCODE_GATEWAY_MEMORY_DECAY_HALFLIFE")); rawHalf != "" {
		if parsed, err := time.ParseDuration(rawHalf); err == nil && parsed > 0 {
			client.memoryDecayHalfLife = parsed
		}
	}
	if rawCompact := strings.TrimSpace(os.Getenv("OPENCODE_GATEWAY_MEMORY_AUTO_COMPACT_INTERVAL")); rawCompact != "" {
		if strings.EqualFold(rawCompact, "off") || rawCompact == "0" {
			client.memoryAutoCompactInterval = 0
		} else if parsed, err := time.ParseDuration(rawCompact); err == nil && parsed > 0 {
			client.memoryAutoCompactInterval = parsed
		}
	}
	client.memoryCategoryQuotaEnabled = parseEnvBoolDefault("OPENCODE_GATEWAY_MEMORY_CATEGORY_QUOTA_ENABLED", true)
	if rawQuota := strings.TrimSpace(os.Getenv("OPENCODE_GATEWAY_MEMORY_CATEGORY_QUOTA")); rawQuota != "" {
		client.memoryCategoryQuota = parseMemoryCategoryQuota(rawQuota)
	}

	for _, opt := range opts {
		opt(client)
	}

	if client.memoryDir == "" {
		base := bootstrap.ResolveBootstrapDir(strings.TrimSpace(client.directory))
		client.memoryDir = filepath.Join(base, ".opencode-gateway-memory")
	}
	log.Printf("opencode: memory configured enabled=%t dir=%s maxChars=%d maxFacts=%d injectFacts=%d",
		client.memoryEnabled, client.memoryDir, client.memoryMaxChars, client.memoryMaxFacts, client.memoryInjectFacts)
	log.Printf("opencode: memory retrieval shortTerm=%d recentWindow=%s decayEnabled=%t halfLife=%s autoCompact=%s debugRecall=%t",
		client.memoryShortTermFacts, client.memoryRecentWindow, client.memoryDecayEnabled, client.memoryDecayHalfLife, client.memoryAutoCompactInterval, client.memoryDebugRecall)
	log.Printf("opencode: memory category quota enabled=%t", client.memoryCategoryQuotaEnabled)
	if len(client.memoryCategoryQuota) > 0 {
		log.Printf("opencode: memory category quota config=%v", client.memoryCategoryQuota)
	}
	client.memoryDefaults = client.GetMemoryRuntimeConfig()

	client.thinkingEnabled.Store(parseEnvBool("OPENBOT_SHOW_THINKING"))
	log.Printf("opencode: thinking output enabled=%t (env OPENBOT_SHOW_THINKING)", client.thinkingEnabled.Load())
	client.finalOnlyEnabled.Store(parseEnvBool("OPENBOT_FINAL_ONLY"))
	log.Printf("opencode: final-only output enabled=%t (env OPENBOT_FINAL_ONLY)", client.finalOnlyEnabled.Load())
	client.stepEnabled.Store(parseEnvBool("OPENBOT_SHOW_STEPS"))
	log.Printf("opencode: step output enabled=%t (env OPENBOT_SHOW_STEPS)", client.stepEnabled.Load())

	// 启动后台清理协程
	go client.cleanupRequestCache()
	go client.autoCompactMemoryLoop()

	// 启动后进行首次健康检查
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := client.CheckHealth(ctx); err != nil {
			log.Printf("opencode: initial health check failed: %v", err)
		} else {
			log.Printf("opencode: initial health check succeeded")
			if err := client.refreshModelCatalog(ctx); err != nil {
				log.Printf("opencode: initial model catalog fetch failed: %v", err)
			} else {
				log.Printf("opencode: initial model catalog loaded")
			}
		}
	}()

	return client
}

// WithPersonaSetupPrompt pushes a one-time startup prompt to each user/channel
// when persona files are still in default template state.
func WithPersonaSetupPrompt(prompt string) Option {
	return func(c *Client) {
		c.personaSetupPrompt = strings.TrimSpace(prompt)
	}
}

func (c *Client) applyAuthHeaders(header http.Header) {
	if c.basicAuthHeader != "" {
		header.Set("Authorization", c.basicAuthHeader)
		header.Del("X-API-Key")
		return
	}
	if c.apiKey == "" {
		return
	}
	header.Set("Authorization", "Bearer "+c.apiKey)
	header.Set("X-API-Key", c.apiKey)
}

// Ready reports if the client has enough data to operate.
func (c *Client) Ready() bool {
	return c.sdk != nil
}

// SetThinkingEnabled toggles whether reasoning/thinking stream is emitted to adapters.
func (c *Client) SetThinkingEnabled(enabled bool) {
	c.thinkingEnabled.Store(enabled)
	log.Printf("opencode: thinking output toggled to %t", enabled)
}

// IsThinkingEnabled reports whether reasoning/thinking stream emission is enabled.
func (c *Client) IsThinkingEnabled() bool {
	return c.thinkingEnabled.Load()
}

// SetFinalOnlyEnabled toggles whether adapters should buffer final output until completion.
func (c *Client) SetFinalOnlyEnabled(enabled bool) {
	c.finalOnlyEnabled.Store(enabled)
	log.Printf("opencode: final-only output toggled to %t", enabled)
}

// IsFinalOnlyEnabled reports whether final-only output mode is enabled.
func (c *Client) IsFinalOnlyEnabled() bool {
	return c.finalOnlyEnabled.Load()
}

// SetStepEnabled toggles whether step lifecycle messages are emitted to adapters.
func (c *Client) SetStepEnabled(enabled bool) {
	c.stepEnabled.Store(enabled)
	log.Printf("opencode: step output toggled to %t", enabled)
}

// IsStepEnabled reports whether step lifecycle messages are enabled.
func (c *Client) IsStepEnabled() bool {
	return c.stepEnabled.Load()
}

// CheckHealth checks if the OpenCode server is running and accessible.
// It caches the result for 10 seconds to avoid excessive health checks.
func (c *Client) CheckHealth(ctx context.Context) error {
	// 检查缓存的健康状态（10秒内）
	c.healthCheckMu.RLock()
	if time.Since(c.lastHealthCheck) < 10*time.Second && c.isHealthy {
		c.healthCheckMu.RUnlock()
		return nil
	}
	c.healthCheckMu.RUnlock()

	// 执行健康检查：尝试列出sessions
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := c.sdk.Session.List(ctx, opencode.SessionListParams{})

	c.healthCheckMu.Lock()
	c.lastHealthCheck = time.Now()
	if err != nil {
		c.isHealthy = false
		c.healthCheckMu.Unlock()
		c.maybeHandleServerUnavailable(context.Background(), err, "health-check")
		return c.formatHealthCheckError(err)
	}
	c.isHealthy = true
	c.healthCheckMu.Unlock()

	return nil
}

func (c *Client) formatHealthCheckError(err error) error {
	var apiErr *opencode.Error
	if errors.As(err, &apiErr) {
		switch apiErr.StatusCode {
		case http.StatusUnauthorized:
			return fmt.Errorf("opencode server认证失败(401): %w\n\n💡 请检查：\n1. OPENCODE_ENDPOINT 是否指向正确服务 (%s)\n2. 若 OpenCode 开启了 OPENCODE_SERVER_PASSWORD，请在 gateway 进程设置 OPENCODE_SERVER_PASSWORD（可选 OPENCODE_SERVER_USERNAME，默认 opencode）\n3. 若使用 API key 认证，请改为设置 OPENCODE_API_KEY\n4. 修改环境变量后请重启 gateway", err, c.endpoint)
		case http.StatusForbidden:
			return fmt.Errorf("opencode server拒绝访问(403): %w\n\n💡 请检查账号/权限配置以及服务端认证策略", err)
		case http.StatusNotFound:
			return fmt.Errorf("opencode server接口不存在(404): %w\n\n💡 请检查 OPENCODE_ENDPOINT 是否正确，当前: %s", err, c.endpoint)
		default:
			return fmt.Errorf("opencode server返回异常状态(%d): %w\n\n💡 请检查服务日志与 endpoint 配置 (%s)", apiErr.StatusCode, err, c.endpoint)
		}
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return fmt.Errorf("opencode server不可达: %w\n\n💡 请确保：\n1. OpenCode server 已启动\n2. OPENCODE_ENDPOINT 配置正确 (%s)\n3. 网络连通且端口可访问", err, c.endpoint)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("opencode server健康检查超时: %w\n\n💡 请检查服务负载或网络延迟，并确认 endpoint 可达 (%s)", err, c.endpoint)
	}

	return fmt.Errorf("opencode server不可用: %w\n\n💡 请确保：\n1. OpenCode server已启动\n2. 服务地址配置正确 (%s)\n3. 网络连接正常", err, c.endpoint)
}

// IsHealthy returns the cached health status.
func (c *Client) IsHealthy() bool {
	c.healthCheckMu.RLock()
	defer c.healthCheckMu.RUnlock()
	return c.isHealthy
}

// SendMessage forwards an adapter payload to OpenCode and returns its response.
// 注意：OpenCode支持两种模式：
// 1. POST /session/:id/message - 同步模式，等待响应后返回
// 2. POST /session/:id/prompt_async - 异步模式，立即返回204，通过事件流获取结果
// 对于长时间任务，应该使用异步模式+事件监听
func (c *Client) SendMessage(ctx context.Context, payload MessagePayload) (Response, error) {
	if !c.Ready() {
		return Response{}, fmt.Errorf("opencode: client not configured")
	}

	if strings.TrimSpace(payload.Content) == "" {
		return Response{}, ErrEmptyPayload
	}

	startupNotice := ""
	if !payload.Streaming {
		startupNotice = c.consumePersonaSetupPrompt(payload.Channel, payload.UserID)
	}

	if response, handled, err := c.handlePersonaSetupCommand(payload.Content); handled {
		if err != nil {
			return Response{}, err
		}
		c.clearPersonaSetupPending(payload.Channel, payload.UserID)
		response.Reply = prependNotice(response.Reply, startupNotice)
		response.SessionID = payload.SessionID
		response.Trace = payload.SessionID
		return response, nil
	}
	if response, handled, err := c.handlePersonaSetupPromptReply(payload.Channel, payload.UserID, payload.Content); handled {
		if err != nil {
			return Response{}, err
		}
		response.Reply = prependNotice(response.Reply, startupNotice)
		response.SessionID = payload.SessionID
		response.Trace = payload.SessionID
		return response, nil
	}
	if response, handled, err := c.handleBotNamingCommand(payload.Content); handled {
		if err != nil {
			return Response{}, err
		}
		response.Reply = prependNotice(response.Reply, startupNotice)
		response.SessionID = payload.SessionID
		response.Trace = payload.SessionID
		return response, nil
	}
	originalContent := payload.Content
	payload.Content = c.injectSoulIntoPrompt(payload.Content)

	if c.memoryEnabled {
		if memory := c.renderUserMemoryForPrompt(payload.Channel, payload.UserID, originalContent); memory != "" {
			payload.Content = fmt.Sprintf("[用户长期记忆]\n%s\n\n[用户当前消息]\n%s", memory, payload.Content)
			log.Printf("opencode: injected user memory for %s/%s (%d chars)", payload.Channel, payload.UserID, len(memory))
		}
	}

	// ========== 健康检查：确保OpenCode server已启动 ==========
	if err := c.CheckHealth(ctx); err != nil {
		return Response{}, err
	}

	// ========== 请求去重检查（仅防止快速重复点击）==========
	requestHash := generateRequestHash(payload)
	if record, isDuplicate := c.checkAndMarkRequest(requestHash); isDuplicate {
		if !record.InFlight {
			// 已完成的请求，返回缓存的响应（快速响应）
			// 这不是真正的重复，只是缓存命中
			return record.Response, nil
		}
		// 请求正在处理中（30秒内的快速重复点击）
		log.Printf("opencode: duplicate request detected, request is still processing (in-flight)")
		return Response{}, ErrDuplicateRequest
	}

	// 确保请求完成时更新状态
	defer func() {
		// 如果发生panic，清理请求状态
		if r := recover(); r != nil {
			c.failRequest(requestHash)
			panic(r)
		}
	}()

	// 使用独立的context用于session创建，避免被外部context取消影响
	sessionCtx, sessionCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer sessionCancel()

	// Get or create session with lock to prevent concurrent session creation
	threadLock := c.getThreadLock(sessionMapKey(payload.Channel, payload.ThreadID))
	threadLock.Lock()
	sessionID := payload.SessionID

	// 🔍 诊断日志：记录 session 查找请求
	log.Printf("opencode: session lookup - channel=%s, userID=%s, threadID=%s, requestingSessionID=%s",
		payload.Channel, payload.UserID, payload.ThreadID, sessionID)

	if sessionID == "" && payload.ThreadID != "" {
		if foundSessionID, ok := c.loadSessionForThread(payload.Channel, payload.ThreadID); ok {

			// 🔍 诊断日志：检查 session 是否属于当前用户
			// 通过查询 adapter 的映射来验证（如果可用）
			log.Printf("opencode: found cached session %s for threadID %s (requested by %s user %s)",
				foundSessionID[:8], payload.ThreadID, payload.Channel, payload.UserID)

			// 警告：可能存在 session 混用
			log.Printf("opencode: ⚠️ WARNING - ThreadID %s is mapped to session %s, but cannot verify ownership!",
				payload.ThreadID, foundSessionID[:8])

			sessionID = foundSessionID
		} else {
			log.Printf("opencode: no cached session for threadID %s, will create new", payload.ThreadID)
		}
	}

	// Create new session if needed
	if sessionID == "" {
		// 🔍 诊断日志：创建新 session
		log.Printf("opencode: creating new session - channel=%s, userID=%s, threadID=%s",
			payload.Channel, payload.UserID, payload.ThreadID)

		// 将 adapter 和 user 信息编码到 Title 中，格式: [adapter:userId] threadId
		sessionTitle := fmt.Sprintf("[%s:%s] %s", payload.Channel, payload.UserID, payload.ThreadID)

		session, err := c.sdk.Session.New(sessionCtx, opencode.SessionNewParams{
			Title: opencode.F(sessionTitle),
		})
		if err != nil {
			threadLock.Unlock()
			c.failRequest(requestHash)
			return Response{}, fmt.Errorf("opencode: create session: %w", err)
		}
		sessionID = session.ID
		if payload.ThreadID != "" {
			c.storeSessionForThread(payload.Channel, payload.ThreadID, sessionID)
			// 🔍 诊断日志：记录 session 映射
			log.Printf("opencode: mapped threadID %s -> sessionID %s (for %s user %s)",
				payload.ThreadID, sessionID[:8], payload.Channel, payload.UserID)
		}
		c.messageCount.Store(sessionID, 0)
		c.tokenCount.Store(sessionID, 0)

		// 获取模型配置
		go c.fetchAndCacheModelConfig(context.Background(), sessionID)

		log.Printf("opencode: created new session %s for thread %s", sessionID[:8], payload.ThreadID)
	} else {
		// 🔍 诊断日志：复用现有 session
		log.Printf("opencode: reusing existing sessionID %s for %s user %s (threadID %s)",
			sessionID[:8], payload.Channel, payload.UserID, payload.ThreadID)

		// 验证 session 是否仍然有效（OpenCode Server 重启后 session 可能失效）
		checkCtx, checkCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, checkErr := c.GetSession(checkCtx, sessionID)
		checkCancel()
		if checkErr != nil {
			log.Printf("opencode: ⚠️ session %s is stale (err: %v), creating new session",
				sessionID[:8], checkErr)

			// 清除旧映射
			if payload.ThreadID != "" {
				c.deleteSessionForThread(payload.Channel, payload.ThreadID)
			}
			c.messageCount.Delete(sessionID)
			c.tokenCount.Delete(sessionID)
			c.modelConfig.Delete(sessionID)

			// 创建新 session
			sessionTitle := fmt.Sprintf("[%s:%s] %s", payload.Channel, payload.UserID, payload.ThreadID)
			newSession, err := c.sdk.Session.New(sessionCtx, opencode.SessionNewParams{
				Title: opencode.F(sessionTitle),
			})
			if err != nil {
				threadLock.Unlock()
				c.failRequest(requestHash)
				return Response{}, fmt.Errorf("opencode: create replacement session: %w", err)
			}
			sessionID = newSession.ID
			if payload.ThreadID != "" {
				c.storeSessionForThread(payload.Channel, payload.ThreadID, sessionID)
				log.Printf("opencode: 🔄 remapped threadID %s -> new sessionID %s (replaced stale session)",
					payload.ThreadID, sessionID[:8])
			}
			c.messageCount.Store(sessionID, 0)
			c.tokenCount.Store(sessionID, 0)
			go c.fetchAndCacheModelConfig(context.Background(), sessionID)
			// 跳过后续的 token 检查，直接使用新 session
			threadLock.Unlock()
			goto sendMessage
		}

		// 记录当前会话的大致上下文占用，仅用于状态展示与日志。
		msgCount := c.loadCounter(&c.messageCount, sessionID)
		currentTokens := c.loadCounter(&c.tokenCount, sessionID)

		// 估算当前消息的token数
		estimatedMsgTokens := estimateTokens(payload.Content)
		projectedTokens := currentTokens + estimatedMsgTokens

		// 获取模型上下文长度
		maxContextTokens := c.getMaxContextLength(sessionID)
		contextUsage := float64(projectedTokens) / float64(maxContextTokens)

		log.Printf("opencode: session %s - messages: %d, tokens: %d/%d (%.1f%%), estimated msg tokens: %d",
			sessionID[:8], msgCount, currentTokens, maxContextTokens, contextUsage*100, estimatedMsgTokens)
	}
	threadLock.Unlock()

sendMessage:
	// ========== 增强消息内容 ==========
	// 添加skill提示（仅在session开始时）
	enhancedContent := c.enhanceContentWithSkillHint(payload.Content, sessionID)
	effectiveContent := enhancedContent

	// ========== 多模态兼容处理 ==========
	// 若当前会话模型不支持图片/视频，则使用支持模型先识别媒体，再将识别结果转为文本发给当前会话模型。
	if processed, err := c.preprocessAttachmentsForSession(ctx, sessionID, &payload, &effectiveContent); err != nil {
		log.Printf("opencode: media preprocessing failed for session %s: %v", sessionID[:8], err)
		c.failRequest(requestHash)
		return Response{}, fmt.Errorf("opencode: media preprocessing: %w", err)
	} else if processed {
		log.Printf("opencode: media preprocessing applied for session %s", sessionID[:8])
	}

	mainModelOverride := c.getSessionModelOverride(sessionID)
	if mainModelOverride != nil {
		log.Printf("opencode: request route - session=%s mainModel=%s/%s attachments=%d",
			sessionID[:8], mainModelOverride.ProviderID.Value, mainModelOverride.ModelID.Value, len(payload.Attachments))
	} else {
		log.Printf("opencode: request route - session=%s mainModel=<default/session> attachments=%d",
			sessionID[:8], len(payload.Attachments))
	}

	// Build message parts
	parts := []opencode.SessionPromptParamsPartUnion{}

	// Add agent part if specified
	// Note: OpenCode支持多种模式：
	// - chat: 普通对话模式，无需确认
	// - plan: 规划模式，会生成计划
	// - build: 构建模式，需要用户确认才执行（可能导致等待）
	if payload.Agent != "" {
		parts = append(parts, opencode.AgentPartInputParam{
			Name: opencode.F(payload.Agent),
			Type: opencode.F(opencode.AgentPartInputTypeAgent),
		})
		log.Printf("opencode: using agent '%s' for session %s", payload.Agent, sessionID[:8])
	}

	// Add text content (使用增强后的内容)
	parts = append(parts, opencode.TextPartInputParam{
		Text: opencode.F(effectiveContent),
		Type: opencode.F(opencode.TextPartInputTypeText),
	})

	// Add file attachments (images, audio, video as data URIs)
	for _, att := range payload.Attachments {
		if att.URL == "" || att.Mime == "" {
			continue
		}
		fp := opencode.FilePartInputParam{
			Type: opencode.F(opencode.FilePartInputTypeFile),
			Mime: opencode.F(att.Mime),
			URL:  opencode.F(att.URL),
		}
		if att.Filename != "" {
			fp.Filename = opencode.F(att.Filename)
		}
		parts = append(parts, fp)
		log.Printf("opencode: added %s attachment (%s, %d bytes data URI) to session %s",
			att.Mime, att.Filename, len(att.URL), sessionID[:8])
	}

	// 流式模式下改用异步 prompt_async，避免长任务导致 context deadline
	if payload.Streaming {
		// Slot is pre-acquired by SendMessageStreaming before handler registration.
		// On error, release it so the next queued message can proceed.
		if err := c.sendPromptAsync(ctx, sessionID, parts, mainModelOverride); err != nil {
			c.releaseSessionSlot(sessionID)
			c.failRequest(requestHash)
			return Response{}, fmt.Errorf("opencode: prompt_async: %w", err)
		}

		// 仅统计用户消息本身的tokens，回复在事件流中获取
		c.incrementCounter(&c.messageCount, sessionID, 1)
		estimatedMsgTokens := estimateTokens(effectiveContent)
		c.incrementCounter(&c.tokenCount, sessionID, estimatedMsgTokens)

		response := Response{
			Reply:     "",
			SessionID: sessionID,
			MessageID: "",
			Trace:     sessionID,
		}

		// Streaming channels (dingtalk/feishu/wecom) also need memory persistence.
		// We at least persist user-side candidates here; assistant reply is delivered by SSE.
		if c.memoryEnabled {
			c.updateUserMemory(payload.Channel, payload.UserID, originalContent, "")
		}

		c.completeRequest(requestHash, response)
		return response, nil
	}

	// ========== 使用重试机制发送消息 ==========
	// 仅对 webui 启用 per-session 排队，其他 adapter 保持原行为。
	if shouldQueueByChannel(payload.Channel) {
		slotRelease, slotErr := c.acquireSessionSlot(ctx, sessionID)
		if slotErr != nil {
			c.failRequest(requestHash)
			return Response{}, fmt.Errorf("opencode: wait for session slot: %w", slotErr)
		}
		defer slotRelease()
	}

	result, err := c.sendPromptWithRetry(ctx, sessionID, parts, mainModelOverride)

	if err != nil {
		c.failRequest(requestHash)
		return Response{}, fmt.Errorf("opencode: send prompt: %w", err)
	}

	// Extract reply from assistant message
	reply := extractReplyFromMessage(result)

	// Increment message count and token count for this session
	c.incrementCounter(&c.messageCount, sessionID, 1)

	// 更新token计数（估算用户消息 + AI回复）
	estimatedMsgTokens := estimateTokens(effectiveContent)
	estimatedReplyTokens := estimateTokens(reply)
	c.incrementCounter(&c.tokenCount, sessionID, estimatedMsgTokens+estimatedReplyTokens)

	// 缓存本次实际使用的模型信息（若SDK返回）
	c.updateSessionModel(sessionID, result.Info.ProviderID, result.Info.ModelID)

	response := Response{
		Reply:     reply,
		SessionID: sessionID,
		MessageID: result.Info.ID,
		Trace:     sessionID,
	}
	response.Reply = prependNotice(response.Reply, startupNotice)

	if c.memoryEnabled {
		c.updateUserMemory(payload.Channel, payload.UserID, originalContent, reply)
	}

	// ========== 缓存成功响应用于去重 ==========
	c.completeRequest(requestHash, response)

	return response, nil
}

func prependNotice(reply, notice string) string {
	notice = strings.TrimSpace(notice)
	if notice == "" {
		return reply
	}
	reply = strings.TrimSpace(reply)
	if reply == "" {
		return notice
	}
	return notice + "\n\n" + reply
}

func (c *Client) consumePersonaSetupPrompt(channel, userID string) string {
	prompt := strings.TrimSpace(c.personaSetupPrompt)
	if prompt == "" {
		return ""
	}
	key := strings.ToLower(strings.TrimSpace(channel)) + ":" + strings.TrimSpace(userID)
	if strings.TrimSpace(userID) == "" {
		return ""
	}
	if _, exists := c.personaPromptSent.Load(key); exists {
		return ""
	}
	c.personaPromptSent.Store(key, struct{}{})
	c.personaSetupPending.Store(key, struct{}{})
	return prompt
}

func personaSetupKey(channel, userID string) string {
	return strings.ToLower(strings.TrimSpace(channel)) + ":" + strings.TrimSpace(userID)
}

func (c *Client) clearPersonaSetupPending(channel, userID string) {
	key := personaSetupKey(channel, userID)
	if strings.TrimSpace(userID) == "" {
		return
	}
	c.personaSetupPending.Delete(key)
}

func (c *Client) handlePersonaSetupPromptReply(channel, userID, content string) (Response, bool, error) {
	if strings.TrimSpace(userID) == "" {
		return Response{}, false, nil
	}
	key := personaSetupKey(channel, userID)
	if _, ok := c.personaSetupPending.Load(key); !ok {
		return Response{}, false, nil
	}

	trimmed := strings.TrimSpace(content)
	if trimmed == "" || strings.HasPrefix(trimmed, "/") {
		return Response{}, false, nil
	}

	botName, preferredName, style, goals, ok := parsePersonaSetupBatch(trimmed)
	if !ok {
		return Response{}, false, nil
	}

	if err := writeBotNameToSoul(c.soulFilePaths(), botName); err != nil {
		return Response{}, true, err
	}
	if err := upsertUserProfileInUserFiles(c.userFilePaths(), preferredName, style, goals); err != nil {
		return Response{}, true, err
	}
	c.clearPersonaSetupPending(channel, userID)

	reply := "✅ 已根据你的回复完成设定并写入文件：\n- SOUL: 机器人名称\n- USER: 称呼、回答风格、长期目标\n\n后续不会再重复询问；你也可随时用 /setup 或 /name 更新。"
	return Response{Reply: reply}, true, nil
}

func parsePersonaSetupBatch(content string) (string, string, string, string, bool) {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return "", "", "", "", false
	}

	// Format A: name|preferredName|style|goals
	if strings.Contains(trimmed, "|") {
		parts := strings.Split(trimmed, "|")
		if len(parts) == 4 {
			v0 := strings.TrimSpace(parts[0])
			v1 := strings.TrimSpace(parts[1])
			v2 := strings.TrimSpace(parts[2])
			v3 := strings.TrimSpace(parts[3])
			if v0 != "" && v1 != "" && v2 != "" && v3 != "" {
				return v0, v1, v2, v3, true
			}
		}
	}

	// Format B: numbered lines, e.g. 1.xxx 2.xxx 3.xxx 4.xxx
	values := map[string]string{}
	for _, line := range strings.Split(trimmed, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		line = strings.TrimLeft(line, "-•")
		line = strings.TrimSpace(line)

		if idx := firstColonIndex(line); idx > 0 {
			left := strings.TrimSpace(line[:idx])
			right := strings.TrimSpace(line[idx+1:])
			switch normalizeSetupKey(left) {
			case "1", "name":
				values["1"] = right
			case "2", "preferred":
				values["2"] = right
			case "3", "style":
				values["3"] = right
			case "4", "goals":
				values["4"] = right
			}
			continue
		}

		for _, k := range []string{"1.", "1 ", "2.", "2 ", "3.", "3 ", "4.", "4 ", "1、", "2、", "3、", "4、"} {
			if strings.HasPrefix(line, k) {
				key := string(k[0])
				values[key] = strings.TrimSpace(line[len(k):])
				break
			}
		}
	}

	v0 := strings.TrimSpace(values["1"])
	v1 := strings.TrimSpace(values["2"])
	v2 := strings.TrimSpace(values["3"])
	v3 := strings.TrimSpace(values["4"])
	if v0 != "" && v1 != "" && v2 != "" && v3 != "" {
		return v0, v1, v2, v3, true
	}

	return "", "", "", "", false
}

func firstColonIndex(s string) int {
	for i, r := range s {
		if r == ':' || r == '：' {
			return i
		}
	}
	return -1
}

func normalizeSetupKey(s string) string {
	v := strings.ToLower(strings.TrimSpace(s))
	v = strings.TrimRight(v, ".、")
	switch v {
	case "1", "机器人名字", "机器人名称", "名字", "name", "bot", "botname":
		return "name"
	case "2", "如何称呼", "称呼", "叫我", "preferred", "preferredname":
		return "preferred"
	case "3", "回答风格", "风格", "style":
		return "style"
	case "4", "长期目标", "目标", "项目背景", "goals", "goal":
		return "goals"
	default:
		return v
	}
}

func (c *Client) handlePersonaSetupCommand(content string) (Response, bool, error) {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" || !strings.HasPrefix(trimmed, "/") {
		return Response{}, false, nil
	}

	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return Response{}, false, nil
	}
	if strings.ToLower(fields[0]) != "/setup" {
		return Response{}, false, nil
	}

	if len(fields) == 1 {
		return Response{Reply: "用法：/setup 机器人名称|如何称呼你|回答风格|长期目标\n\n示例：/setup 小码|老王|中文+简洁|维护 opencode-gateway 并提升稳定性"}, true, nil
	}

	raw := strings.TrimSpace(strings.TrimPrefix(trimmed, fields[0]))
	parts := strings.Split(raw, "|")
	if len(parts) != 4 {
		return Response{}, true, fmt.Errorf("opencode: /setup 格式错误，应为 /setup 机器人名称|如何称呼你|回答风格|长期目标")
	}

	botName := strings.TrimSpace(parts[0])
	preferredName := strings.TrimSpace(parts[1])
	style := strings.TrimSpace(parts[2])
	goals := strings.TrimSpace(parts[3])
	if botName == "" || preferredName == "" || style == "" || goals == "" {
		return Response{}, true, fmt.Errorf("opencode: /setup 每个字段都不能为空")
	}

	if err := writeBotNameToSoul(c.soulFilePaths(), botName); err != nil {
		return Response{}, true, err
	}
	if err := upsertUserProfileInUserFiles(c.userFilePaths(), preferredName, style, goals); err != nil {
		return Response{}, true, err
	}

	return Response{Reply: "✅ 人格设定已写入：SOUL 名称 + USER 档案。你也可以继续手动补充 IDENTITY/BOOTSTRAP。"}, true, nil
}

func (c *Client) handleBotNamingCommand(content string) (Response, bool, error) {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" || !strings.HasPrefix(trimmed, "/") {
		return Response{}, false, nil
	}

	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return Response{}, false, nil
	}
	cmd := strings.ToLower(fields[0])
	if cmd != "/name" && cmd != "/botname" {
		return Response{}, false, nil
	}

	paths := c.soulFilePaths()
	if len(fields) == 1 {
		name, err := readBotNameFromSoul(paths)
		if err != nil {
			return Response{}, true, err
		}
		if strings.TrimSpace(name) == "" {
			name = "OpenBot"
		}
		return Response{Reply: fmt.Sprintf("🤖 当前名字：%s\n\n使用 /name <新名字> 进行修改。", name)}, true, nil
	}

	newName := strings.TrimSpace(strings.TrimPrefix(trimmed, fields[0]))
	if newName == "" {
		return Response{}, true, fmt.Errorf("opencode: name cannot be empty")
	}
	if len([]rune(newName)) > 40 {
		return Response{}, true, fmt.Errorf("opencode: name too long (max 40 chars)")
	}

	if err := writeBotNameToSoul(paths, newName); err != nil {
		return Response{}, true, err
	}
	return Response{Reply: fmt.Sprintf("✅ 好的，以后我就叫「%s」。", newName)}, true, nil
}

func (c *Client) injectSoulIntoPrompt(content string) string {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" {
		return content
	}

	// Already injected by upstream caller.
	if strings.Contains(trimmed, "[灵魂设定]") || strings.Contains(trimmed, "[人格设定]") {
		return content
	}

	persona, err := c.readPersonaPromptContent()
	if err != nil {
		log.Printf("opencode: read persona files failed: %v", err)
		return content
	}
	if persona == "" {
		return content
	}

	return "[人格设定]\n" + persona + "\n\n[用户当前消息]\n" + content
}

func (c *Client) readSoulPromptContent() (string, error) {
	return c.readFirstPersonaFile(c.soulFilePaths(), 4000)
}

func (c *Client) readPersonaPromptContent() (string, error) {
	sections := []struct {
		title    string
		paths    []string
		maxRunes int
	}{
		{title: "SOUL", paths: c.soulFilePaths(), maxRunes: 3200},
		{title: "IDENTITY", paths: c.identityFilePaths(), maxRunes: 1800},
		{title: "USER", paths: c.userFilePaths(), maxRunes: 1600},
		{title: "BOOTSTRAP", paths: c.bootstrapFilePaths(), maxRunes: 1400},
	}

	blocks := make([]string, 0, len(sections))
	for _, sec := range sections {
		text, err := c.readFirstPersonaFile(sec.paths, sec.maxRunes)
		if err != nil {
			return "", err
		}
		if text == "" {
			continue
		}
		blocks = append(blocks, "### "+sec.title+"\n"+text)
	}

	if len(blocks) == 0 {
		return "", nil
	}

	joined := strings.Join(blocks, "\n\n")
	if len([]rune(joined)) > 7000 {
		joined = string([]rune(joined)[:7000])
	}
	return joined, nil
}

func (c *Client) readFirstPersonaFile(paths []string, maxRunes int) (string, error) {
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", fmt.Errorf("read %s: %w", p, err)
		}
		s := strings.TrimSpace(string(b))
		if s == "" {
			continue
		}
		if maxRunes > 0 && len([]rune(s)) > maxRunes {
			s = string([]rune(s)[:maxRunes])
		}
		return s, nil
	}
	return "", nil
}

func (c *Client) soulFilePaths() []string {
	base := c.personaBaseDir()
	return []string{
		filepath.Join(base, "soul.md"),
		filepath.Join(base, "SOUL.md"),
	}
}

func (c *Client) identityFilePaths() []string {
	base := c.personaBaseDir()
	return []string{
		filepath.Join(base, "identity.md"),
		filepath.Join(base, "IDENTITY.md"),
	}
}

func (c *Client) userFilePaths() []string {
	base := c.personaBaseDir()
	return []string{
		filepath.Join(base, "user.md"),
		filepath.Join(base, "USER.md"),
	}
}

func (c *Client) bootstrapFilePaths() []string {
	base := c.personaBaseDir()
	return []string{
		filepath.Join(base, "bootstrap.md"),
		filepath.Join(base, "BOOTSTRAP.md"),
	}
}

func (c *Client) personaBaseDir() string {
	return bootstrap.ResolveBootstrapDir(strings.TrimSpace(c.directory))
}

func readBotNameFromSoul(paths []string) (string, error) {
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", fmt.Errorf("read %s: %w", p, err)
		}
		for _, line := range strings.Split(string(b), "\n") {
			t := strings.TrimSpace(line)
			if strings.HasPrefix(strings.ToLower(t), "- **name:**") {
				return strings.TrimSpace(strings.TrimPrefix(t, "- **Name:**")), nil
			}
			if strings.HasPrefix(t, "- **名称：**") {
				return strings.TrimSpace(strings.TrimPrefix(t, "- **名称：**")), nil
			}
		}
	}
	return "", nil
}

func writeBotNameToSoul(paths []string, name string) error {
	for _, p := range paths {
		if err := upsertBotNameInSoulFile(p, name); err != nil {
			return err
		}
	}
	return nil
}

func upsertBotNameInSoulFile(path, name string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("read %s: %w", path, err)
		}
		content := "# SOUL.md - Who You Are\n\n- **Name:** " + name + "\n"
		if writeErr := os.WriteFile(path, []byte(content), 0o644); writeErr != nil {
			return fmt.Errorf("write %s: %w", path, writeErr)
		}
		return nil
	}

	lines := strings.Split(string(b), "\n")
	replaced := false
	for i, line := range lines {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToLower(t), "- **name:**") || strings.HasPrefix(t, "- **名称：**") {
			lines[i] = "- **Name:** " + name
			replaced = true
			break
		}
	}
	if !replaced {
		lines = append(lines, "", "- **Name:** "+name)
	}
	out := strings.Join(lines, "\n")
	if err := os.WriteFile(path, []byte(out), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

func upsertUserProfileInUserFiles(paths []string, preferredName, style, goals string) error {
	if len(paths) == 0 {
		return fmt.Errorf("no USER.md path configured")
	}

	target := strings.TrimSpace(paths[0])
	for _, p := range paths {
		if strings.TrimSpace(p) == "" {
			continue
		}
		if _, err := os.Stat(p); err == nil {
			target = p
			break
		}
	}

	content := "# USER.md\n\n## User Profile\n\n- Preferred name: " + preferredName + "\n- Communication style: " + style + "\n- Primary goals: " + goals + "\n\n## Working Preferences\n\n- Default response length: concise\n- Ask before destructive actions: yes\n- Show intermediate progress on long tasks: yes\n\n## Notes\n\nUpdate this file whenever the user gives stable preferences.\n"

	if b, err := os.ReadFile(target); err == nil {
		content = mergeUserProfileContent(string(b), preferredName, style, goals)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("read %s: %w", target, err)
	}

	for _, p := range paths {
		if strings.TrimSpace(p) == "" {
			continue
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", p, err)
		}
	}
	return nil
}

func mergeUserProfileContent(existing, preferredName, style, goals string) string {
	lines := strings.Split(existing, "\n")
	replacedPreferred := false
	replacedStyle := false
	replacedGoals := false

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(strings.ToLower(trimmed), "- preferred name:"):
			lines[i] = "- Preferred name: " + preferredName
			replacedPreferred = true
		case strings.HasPrefix(strings.ToLower(trimmed), "- communication style:"):
			lines[i] = "- Communication style: " + style
			replacedStyle = true
		case strings.HasPrefix(strings.ToLower(trimmed), "- primary goals:"):
			lines[i] = "- Primary goals: " + goals
			replacedGoals = true
		}
	}

	if !replacedPreferred || !replacedStyle || !replacedGoals {
		lines = append(lines,
			"",
			"## User Profile",
			"",
			"- Preferred name: "+preferredName,
			"- Communication style: "+style,
			"- Primary goals: "+goals,
		)
	}

	return strings.TrimRight(strings.Join(lines, "\n"), "\n") + "\n"
}

func (c *Client) memoryFilePath(channel, userID string) string {
	channel = sanitizeMemoryKey(channel)
	userID = sanitizeMemoryKey(userID)
	if channel == "" {
		channel = "unknown"
	}
	if userID == "" {
		userID = "unknown"
	}
	return filepath.Join(c.memoryDir, channel+"__"+userID+".json")
}

func sanitizeMemoryKey(input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range input {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

func (c *Client) loadUserMemoryStore(channel, userID string) userMemoryStore {
	path := c.memoryFilePath(channel, userID)
	b, err := os.ReadFile(path)
	if err != nil {
		return userMemoryStore{Version: 1, Facts: []memoryFact{}}
	}
	raw := strings.TrimSpace(string(b))
	if raw == "" {
		return userMemoryStore{Version: 1, Facts: []memoryFact{}}
	}

	var store userMemoryStore
	if err := json.Unmarshal([]byte(raw), &store); err == nil {
		if store.Version == 0 {
			store.Version = 1
		}
		return store
	}

	// Backward compatibility: old plain text memory file.
	return userMemoryStore{
		Version: 1,
		Facts: []memoryFact{{
			Text:       raw,
			Category:   "conversation",
			Importance: 1,
			LastSeen:   time.Now(),
			Count:      1,
		}},
	}
}

type memoryRecallCandidate struct {
	Fact    memoryFact
	Score   float64
	Layer   string
	Reasons []string
}

func (c *Client) renderUserMemoryForPrompt(channel, userID, query string) string {
	store := c.loadUserMemoryStore(channel, userID)
	if len(store.Facts) == 0 {
		return ""
	}
	selected := c.selectMemoryForQuery(store.Facts, query)
	if len(selected) == 0 {
		return ""
	}
	limit := c.memoryInjectFacts
	if limit <= 0 {
		limit = 8
	}
	if limit > len(selected) {
		limit = len(selected)
	}

	items := make([]string, 0, limit)
	for i := 0; i < limit; i++ {
		items = append(items, fmt.Sprintf("- [%s] %s", selected[i].Fact.Category, selected[i].Fact.Text))
	}
	if c.memoryDebugRecall {
		for i := 0; i < limit; i++ {
			cand := selected[i]
			log.Printf("opencode: memory recall[%d] layer=%s score=%.3f category=%s count=%d lastSeen=%s reasons=%s text=%.80s",
				i+1,
				cand.Layer,
				cand.Score,
				cand.Fact.Category,
				cand.Fact.Count,
				cand.Fact.LastSeen.Format(time.RFC3339),
				strings.Join(cand.Reasons, ","),
				cand.Fact.Text,
			)
		}
	}
	prompt := strings.Join(items, "\n")
	runes := []rune(prompt)
	if len(runes) > c.memoryMaxChars {
		prompt = string(runes[:c.memoryMaxChars])
	}
	return prompt
}

func (c *Client) selectMemoryForQuery(facts []memoryFact, query string) []memoryRecallCandidate {
	now := time.Now()
	query = strings.TrimSpace(query)
	short := make([]memoryRecallCandidate, 0)
	long := make([]memoryRecallCandidate, 0)

	for _, f := range facts {
		score, reasons := c.memoryRecallScore(f, query, now)
		cand := memoryRecallCandidate{Fact: f, Score: score, Reasons: reasons, Layer: "long"}

		age := now.Sub(f.LastSeen)
		if c.memoryRecentWindow > 0 && age <= c.memoryRecentWindow {
			cand.Layer = "short"
			short = append(short, cand)
		} else {
			long = append(long, cand)
		}
	}

	sort.Slice(short, func(i, j int) bool {
		if short[i].Score != short[j].Score {
			return short[i].Score > short[j].Score
		}
		return short[i].Fact.LastSeen.After(short[j].Fact.LastSeen)
	})
	sort.Slice(long, func(i, j int) bool {
		if long[i].Score != long[j].Score {
			return long[i].Score > long[j].Score
		}
		return long[i].Fact.LastSeen.After(long[j].Fact.LastSeen)
	})

	shortLimit := c.memoryShortTermFacts
	if shortLimit < 0 {
		shortLimit = 0
	}
	if shortLimit > c.memoryInjectFacts {
		shortLimit = c.memoryInjectFacts
	}
	out := make([]memoryRecallCandidate, 0, c.memoryInjectFacts)
	seen := make(map[string]struct{}, c.memoryInjectFacts)
	appendCandidate := func(cand memoryRecallCandidate) bool {
		if len(out) >= c.memoryInjectFacts {
			return false
		}
		key := normalizeMemoryText(cand.Fact.Text)
		if _, ok := seen[key]; ok {
			return false
		}
		seen[key] = struct{}{}
		out = append(out, cand)
		return true
	}

	for i := 0; i < len(short) && i < shortLimit; i++ {
		appendCandidate(short[i])
	}

	if c.memoryCategoryQuotaEnabled {
		quota := c.recallCategoryQuota(c.memoryInjectFacts)
		for cat, minNeed := range quota {
			if minNeed <= 0 {
				continue
			}
			have := 0
			for _, item := range out {
				if strings.EqualFold(item.Fact.Category, cat) {
					have++
				}
			}
			if have >= minNeed {
				continue
			}
			need := minNeed - have
			for _, group := range [][]memoryRecallCandidate{short, long} {
				for _, cand := range group {
					if need <= 0 {
						break
					}
					if !strings.EqualFold(cand.Fact.Category, cat) {
						continue
					}
					if appendCandidate(cand) {
						need--
					}
				}
				if need <= 0 || len(out) >= c.memoryInjectFacts {
					break
				}
			}
			if len(out) >= c.memoryInjectFacts {
				break
			}
		}
	}

	for _, group := range [][]memoryRecallCandidate{long, short} {
		for _, cand := range group {
			if !appendCandidate(cand) && len(out) >= c.memoryInjectFacts {
				break
			}
		}
		if len(out) >= c.memoryInjectFacts {
			break
		}
	}

	return out
}

func (c *Client) recallCategoryQuota(injectLimit int) map[string]int {
	if injectLimit <= 0 {
		return map[string]int{}
	}

	if len(c.memoryCategoryQuota) > 0 {
		custom := make(map[string]int, len(c.memoryCategoryQuota))
		sum := 0
		for k, v := range c.memoryCategoryQuota {
			if v <= 0 {
				continue
			}
			custom[k] = v
			sum += v
		}
		if sum <= injectLimit {
			return custom
		}

		scaled := make(map[string]int, len(custom))
		remaining := injectLimit
		for k, v := range custom {
			sv := int(math.Floor(float64(v) * float64(injectLimit) / float64(sum)))
			if sv < 1 {
				sv = 1
			}
			scaled[k] = sv
			remaining -= sv
		}

		if remaining < 0 {
			type pair struct {
				k string
				v int
			}
			pairs := make([]pair, 0, len(scaled))
			for k, v := range scaled {
				pairs = append(pairs, pair{k: k, v: v})
			}
			sort.Slice(pairs, func(i, j int) bool {
				return pairs[i].v > pairs[j].v
			})
			for remaining < 0 {
				adjusted := false
				for _, p := range pairs {
					if remaining == 0 {
						break
					}
					if scaled[p.k] > 1 {
						scaled[p.k]--
						remaining++
						adjusted = true
					}
				}
				if !adjusted {
					break
				}
			}
		} else if remaining > 0 {
			for remaining > 0 {
				for k := range scaled {
					if remaining == 0 {
						break
					}
					scaled[k]++
					remaining--
				}
			}
		}

		log.Printf("opencode: memory quota scaled raw=%v scaled=%v injectLimit=%d", custom, scaled, injectLimit)
		return scaled
	}

	if injectLimit < 4 {
		return map[string]int{}
	}
	quota := map[string]int{"preference": 1, "project": 1}
	if injectLimit >= 6 {
		quota["profile"] = 1
	}
	if injectLimit >= 8 {
		quota["environment"] = 1
	}
	return quota
}

func parseMemoryCategoryQuota(raw string) map[string]int {
	out := map[string]int{}
	allowed := map[string]struct{}{
		"profile":      {},
		"preference":   {},
		"project":      {},
		"environment":  {},
		"model":        {},
		"conversation": {},
	}
	for _, item := range strings.Split(raw, ",") {
		part := strings.TrimSpace(item)
		if part == "" {
			continue
		}
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			log.Printf("opencode: ignore invalid memory category quota segment %q (expected category=value)", part)
			continue
		}
		rawKey := strings.TrimSpace(kv[0])
		key := normalizeMemoryCategory(rawKey)
		if _, ok := allowed[key]; !ok {
			log.Printf("opencode: ignore unsupported memory category quota %q (supported: profile, preference, project, environment, model, conversation)", rawKey)
			continue
		}
		val, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil || val <= 0 {
			log.Printf("opencode: ignore invalid memory category quota value %q for %s", strings.TrimSpace(kv[1]), key)
			continue
		}
		out[key] = val
	}
	return out
}

// GetMemoryRuntimeConfig returns a snapshot of current memory runtime settings.
func (c *Client) GetMemoryRuntimeConfig() MemoryRuntimeConfig {
	quota := map[string]int{}
	for k, v := range c.memoryCategoryQuota {
		quota[k] = v
	}
	return MemoryRuntimeConfig{
		Enabled:              c.memoryEnabled,
		MaxChars:             c.memoryMaxChars,
		MaxFacts:             c.memoryMaxFacts,
		InjectFacts:          c.memoryInjectFacts,
		ShortTermFacts:       c.memoryShortTermFacts,
		RecentWindow:         c.memoryRecentWindow.String(),
		DecayEnabled:         c.memoryDecayEnabled,
		DecayHalfLife:        c.memoryDecayHalfLife.String(),
		CategoryQuotaEnabled: c.memoryCategoryQuotaEnabled,
		CategoryQuota:        quota,
		AutoCompactInterval:  c.memoryAutoCompactInterval.String(),
		DebugRecall:          c.memoryDebugRecall,
	}
}

// ResetMemoryRuntimeConfigToDefault resets memory runtime knobs to startup values.
func (c *Client) ResetMemoryRuntimeConfigToDefault() MemoryRuntimeConfig {
	defaults := c.memoryDefaults
	quota := map[string]int{}
	for k, v := range defaults.CategoryQuota {
		quota[k] = v
	}
	c.memoryEnabled = defaults.Enabled
	c.memoryMaxChars = defaults.MaxChars
	c.memoryMaxFacts = defaults.MaxFacts
	c.memoryInjectFacts = defaults.InjectFacts
	c.memoryShortTermFacts = defaults.ShortTermFacts
	if d, err := time.ParseDuration(defaults.RecentWindow); err == nil {
		c.memoryRecentWindow = d
	}
	c.memoryDecayEnabled = defaults.DecayEnabled
	if d, err := time.ParseDuration(defaults.DecayHalfLife); err == nil {
		c.memoryDecayHalfLife = d
	}
	c.memoryCategoryQuotaEnabled = defaults.CategoryQuotaEnabled
	c.memoryCategoryQuota = quota
	if d, err := time.ParseDuration(defaults.AutoCompactInterval); err == nil {
		c.memoryAutoCompactInterval = d
	}
	c.memoryDebugRecall = defaults.DebugRecall
	return c.GetMemoryRuntimeConfig()
}

// UpdateMemoryRuntimeConfig updates one memory runtime key and returns the latest snapshot.
func (c *Client) UpdateMemoryRuntimeConfig(key, value string) (MemoryRuntimeConfig, error) {
	key = strings.ToLower(strings.TrimSpace(key))
	value = strings.TrimSpace(value)
	if key == "" {
		return c.GetMemoryRuntimeConfig(), fmt.Errorf("memory config key is empty")
	}

	parseOnOff := func(v string) (bool, error) {
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "on", "true", "1", "yes", "y", "enable", "enabled":
			return true, nil
		case "off", "false", "0", "no", "n", "disable", "disabled":
			return false, nil
		default:
			return false, fmt.Errorf("invalid bool value %q", v)
		}
	}

	switch key {
	case "enabled":
		b, err := parseOnOff(value)
		if err != nil {
			return c.GetMemoryRuntimeConfig(), err
		}
		c.memoryEnabled = b
	case "max_chars":
		n, err := strconv.Atoi(value)
		if err != nil || n <= 0 {
			return c.GetMemoryRuntimeConfig(), fmt.Errorf("max_chars must be positive int")
		}
		c.memoryMaxChars = n
	case "max_facts":
		n, err := strconv.Atoi(value)
		if err != nil || n <= 0 {
			return c.GetMemoryRuntimeConfig(), fmt.Errorf("max_facts must be positive int")
		}
		c.memoryMaxFacts = n
	case "inject_facts":
		n, err := strconv.Atoi(value)
		if err != nil || n <= 0 {
			return c.GetMemoryRuntimeConfig(), fmt.Errorf("inject_facts must be positive int")
		}
		c.memoryInjectFacts = n
	case "short_term_facts":
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 {
			return c.GetMemoryRuntimeConfig(), fmt.Errorf("short_term_facts must be >= 0")
		}
		c.memoryShortTermFacts = n
	case "recent_window":
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return c.GetMemoryRuntimeConfig(), fmt.Errorf("recent_window must be duration, e.g. 24h")
		}
		c.memoryRecentWindow = d
	case "decay_enabled":
		b, err := parseOnOff(value)
		if err != nil {
			return c.GetMemoryRuntimeConfig(), err
		}
		c.memoryDecayEnabled = b
	case "decay_half_life":
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return c.GetMemoryRuntimeConfig(), fmt.Errorf("decay_half_life must be duration, e.g. 168h")
		}
		c.memoryDecayHalfLife = d
	case "category_quota_enabled":
		b, err := parseOnOff(value)
		if err != nil {
			return c.GetMemoryRuntimeConfig(), err
		}
		c.memoryCategoryQuotaEnabled = b
	case "category_quota":
		if value == "" {
			c.memoryCategoryQuota = map[string]int{}
			break
		}
		parsed := parseMemoryCategoryQuota(value)
		if len(parsed) == 0 {
			return c.GetMemoryRuntimeConfig(), fmt.Errorf("category_quota parsed empty, expected format preference=2,project=1")
		}
		c.memoryCategoryQuota = parsed
	case "auto_compact_interval":
		if strings.EqualFold(value, "off") || value == "0" {
			c.memoryAutoCompactInterval = 0
			break
		}
		d, err := time.ParseDuration(value)
		if err != nil || d <= 0 {
			return c.GetMemoryRuntimeConfig(), fmt.Errorf("auto_compact_interval must be duration or off")
		}
		c.memoryAutoCompactInterval = d
	case "debug_recall":
		b, err := parseOnOff(value)
		if err != nil {
			return c.GetMemoryRuntimeConfig(), err
		}
		c.memoryDebugRecall = b
	default:
		return c.GetMemoryRuntimeConfig(), fmt.Errorf("unsupported memory config key: %s", key)
	}

	return c.GetMemoryRuntimeConfig(), nil
}

// PreviewMemoryRecall returns top recall results with score breakdown for diagnostics.
func (c *Client) PreviewMemoryRecall(channel, userID, query string, limit int) []MemoryRecallResult {
	store := c.loadUserMemoryStore(channel, userID)
	if len(store.Facts) == 0 {
		return nil
	}
	selected := c.selectMemoryForQuery(store.Facts, query)
	if len(selected) == 0 {
		return nil
	}
	if limit <= 0 || limit > len(selected) {
		limit = len(selected)
	}
	out := make([]MemoryRecallResult, 0, limit)
	for i := 0; i < limit; i++ {
		cand := selected[i]
		out = append(out, MemoryRecallResult{
			Text:       cand.Fact.Text,
			Category:   cand.Fact.Category,
			Importance: cand.Fact.Importance,
			Count:      cand.Fact.Count,
			LastSeen:   cand.Fact.LastSeen,
			Score:      cand.Score,
			Layer:      cand.Layer,
			Reasons:    append([]string(nil), cand.Reasons...),
		})
	}
	return out
}

func (c *Client) memoryRecallScore(f memoryFact, query string, now time.Time) (float64, []string) {
	reasons := make([]string, 0, 6)
	base := float64(f.Importance) * 2.0
	reasons = append(reasons, fmt.Sprintf("importance=%.2f", base))

	countBoost := math.Log1p(float64(max(1, f.Count)))
	reasons = append(reasons, fmt.Sprintf("count=%.2f", countBoost))

	decay := 1.0
	if c.memoryDecayEnabled && c.memoryDecayHalfLife > 0 && !f.LastSeen.IsZero() {
		ageHours := now.Sub(f.LastSeen).Hours()
		if ageHours < 0 {
			ageHours = 0
		}
		halfHours := c.memoryDecayHalfLife.Hours()
		decay = math.Exp(-math.Ln2 * ageHours / halfHours)
		reasons = append(reasons, fmt.Sprintf("decay=%.3f", decay))
	}

	queryScore := memoryQueryRelevanceScore(f.Text, query)
	if queryScore > 0 {
		reasons = append(reasons, fmt.Sprintf("query=%.2f", queryScore))
	}

	// Apply forgetting curve on base knowledge while keeping query hit additive.
	total := (base+countBoost)*decay + queryScore*3.0
	if f.Importance >= 5 {
		total += 1.5
		reasons = append(reasons, "pinned=1.50")
	}
	return total, reasons
}

func memoryQueryRelevanceScore(text, query string) float64 {
	query = strings.TrimSpace(query)
	if query == "" {
		return 0
	}
	textNorm := normalizeMemoryText(text)
	queryNorm := normalizeMemoryText(query)
	if textNorm == "" || queryNorm == "" {
		return 0
	}

	if strings.Contains(queryNorm, textNorm) || strings.Contains(textNorm, queryNorm) {
		return 2.0
	}

	textTokens := tokenizeMemoryText(text)
	queryTokens := tokenizeMemoryText(query)
	if len(textTokens) == 0 || len(queryTokens) == 0 {
		return 0
	}

	querySet := make(map[string]struct{}, len(queryTokens))
	for _, t := range queryTokens {
		querySet[t] = struct{}{}
	}
	overlap := 0
	for _, t := range textTokens {
		if _, ok := querySet[t]; ok {
			overlap++
		}
	}
	if overlap == 0 {
		return 0
	}
	return float64(overlap) / float64(len(querySet))
}

func tokenizeMemoryText(s string) []string {
	lower := strings.ToLower(strings.TrimSpace(s))
	if lower == "" {
		return nil
	}
	tokens := strings.FieldsFunc(lower, func(r rune) bool {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return false
		}
		if r >= 0x4e00 && r <= 0x9fff {
			return false
		}
		return true
	})
	if len(tokens) == 0 {
		return nil
	}
	uniq := make(map[string]struct{}, len(tokens))
	out := make([]string, 0, len(tokens))
	for _, t := range tokens {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if _, ok := uniq[t]; ok {
			continue
		}
		uniq[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

func (c *Client) updateUserMemory(channel, userID, userMessage, assistantReply string) {
	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()

	if err := os.MkdirAll(c.memoryDir, 0o755); err != nil {
		log.Printf("opencode: create memory dir failed: %v", err)
		return
	}

	path := c.memoryFilePath(channel, userID)
	store := c.loadUserMemoryStore(channel, userID)
	candidates := extractMemoryCandidates(userMessage, assistantReply)
	if len(candidates) == 0 {
		return
	}

	for _, cand := range candidates {
		merged := false
		for i := range store.Facts {
			if normalizeMemoryText(store.Facts[i].Text) == normalizeMemoryText(cand.Text) {
				store.Facts[i].LastSeen = time.Now()
				store.Facts[i].Count++
				if cand.Importance > store.Facts[i].Importance {
					store.Facts[i].Importance = cand.Importance
				}
				merged = true
				break
			}
		}
		if !merged {
			cand.LastSeen = time.Now()
			cand.Count = 1
			store.Facts = append(store.Facts, cand)
		}
	}

	sort.Slice(store.Facts, func(i, j int) bool {
		if store.Facts[i].Importance != store.Facts[j].Importance {
			return store.Facts[i].Importance > store.Facts[j].Importance
		}
		if !store.Facts[i].LastSeen.Equal(store.Facts[j].LastSeen) {
			return store.Facts[i].LastSeen.After(store.Facts[j].LastSeen)
		}
		return store.Facts[i].Count > store.Facts[j].Count
	})

	if c.memoryMaxFacts > 0 && len(store.Facts) > c.memoryMaxFacts {
		store.Facts = store.Facts[:c.memoryMaxFacts]
	}

	encoded, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		log.Printf("opencode: marshal memory failed: %v", err)
		return
	}

	if err := os.WriteFile(path, encoded, 0o644); err != nil {
		log.Printf("opencode: write memory failed: %v", err)
	}
}

// ListUserMemory returns top memory facts sorted by priority.
func (c *Client) ListUserMemory(channel, userID string, limit int) []MemoryFactView {
	store := c.loadUserMemoryStore(channel, userID)
	if len(store.Facts) == 0 {
		return nil
	}

	facts := c.sortedMemoryFacts(store.Facts)

	if limit <= 0 || limit > len(facts) {
		limit = len(facts)
	}
	out := make([]MemoryFactView, 0, limit)
	for i := 0; i < limit; i++ {
		out = append(out, MemoryFactView{
			Text:       facts[i].Text,
			Category:   facts[i].Category,
			Importance: facts[i].Importance,
			LastSeen:   facts[i].LastSeen,
			Count:      facts[i].Count,
		})
	}
	return out
}

// ExportUserMemory returns base64-encoded JSON snapshot for backup/migration.
func (c *Client) ExportUserMemory(channel, userID string) (string, error) {
	store := c.loadUserMemoryStore(channel, userID)
	encoded, err := json.Marshal(store)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(encoded), nil
}

// ImportUserMemory imports a base64 JSON snapshot and replaces existing memory.
func (c *Client) ImportUserMemory(channel, userID, payload string) (int, error) {
	store, err := parseImportMemoryStore(payload)
	if err != nil {
		return 0, err
	}

	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()
	if err := os.MkdirAll(c.memoryDir, 0o755); err != nil {
		return 0, err
	}
	if err := c.saveMemoryStore(channel, userID, store); err != nil {
		return 0, err
	}
	return len(store.Facts), nil
}

// MergeImportUserMemory imports snapshot and merges with existing facts (dedupe + priority keep).
func (c *Client) MergeImportUserMemory(channel, userID, payload string) (int, error) {
	incoming, err := parseImportMemoryStore(payload)
	if err != nil {
		return 0, err
	}

	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()
	if err := os.MkdirAll(c.memoryDir, 0o755); err != nil {
		return 0, err
	}

	existing := c.loadUserMemoryStore(channel, userID)
	merged := map[string]memoryFact{}
	for _, f := range existing.Facts {
		key := normalizeMemoryText(f.Text)
		merged[key] = f
	}
	for _, f := range incoming.Facts {
		key := normalizeMemoryText(f.Text)
		if cur, ok := merged[key]; ok {
			if f.Importance > cur.Importance {
				cur.Importance = f.Importance
				cur.Category = f.Category
			}
			if f.LastSeen.After(cur.LastSeen) {
				cur.LastSeen = f.LastSeen
			}
			cur.Count += f.Count
			merged[key] = cur
			continue
		}
		merged[key] = f
	}

	facts := make([]memoryFact, 0, len(merged))
	for _, f := range merged {
		facts = append(facts, f)
	}
	existing.Facts = facts
	if err := c.saveMemoryStore(channel, userID, existing); err != nil {
		return 0, err
	}
	return len(existing.Facts), nil
}

// ClearUserMemory deletes persisted memory for the user.
func (c *Client) ClearUserMemory(channel, userID string) error {
	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()
	path := c.memoryFilePath(channel, userID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// PinUserMemory stores a high-priority explicit memory fact.
func (c *Client) PinUserMemory(channel, userID, text, category string) error {
	text = strings.TrimSpace(text)
	if text == "" {
		return fmt.Errorf("memory text is empty")
	}
	category = normalizeMemoryCategory(category)

	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()
	if err := os.MkdirAll(c.memoryDir, 0o755); err != nil {
		return err
	}

	store := c.loadUserMemoryStore(channel, userID)
	norm := normalizeMemoryText(text)
	for i := range store.Facts {
		if normalizeMemoryText(store.Facts[i].Text) == norm {
			store.Facts[i].Category = category
			store.Facts[i].Importance = 5
			store.Facts[i].LastSeen = time.Now()
			store.Facts[i].Count++
			return c.saveMemoryStore(channel, userID, store)
		}
	}

	store.Facts = append(store.Facts, memoryFact{
		Text:       text,
		Category:   category,
		Importance: 5,
		LastSeen:   time.Now(),
		Count:      1,
	})
	return c.saveMemoryStore(channel, userID, store)
}

// UnpinUserMemory removes memory facts containing the keyword.
func (c *Client) UnpinUserMemory(channel, userID, keyword string) (int, error) {
	keyword = strings.TrimSpace(keyword)
	if keyword == "" {
		return 0, fmt.Errorf("memory keyword is empty")
	}

	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()
	store := c.loadUserMemoryStore(channel, userID)
	if len(store.Facts) == 0 {
		return 0, nil
	}

	norm := normalizeMemoryText(keyword)
	filtered := make([]memoryFact, 0, len(store.Facts))
	removed := 0
	for _, f := range store.Facts {
		if strings.Contains(normalizeMemoryText(f.Text), norm) {
			removed++
			continue
		}
		filtered = append(filtered, f)
	}

	if removed == 0 {
		return 0, nil
	}
	store.Facts = filtered
	if err := c.saveMemoryStore(channel, userID, store); err != nil {
		return 0, err
	}
	return removed, nil
}

// RemoveUserMemoryByRank removes an entry by its display rank (1-based from /memory show order).
func (c *Client) RemoveUserMemoryByRank(channel, userID string, rank int) (bool, error) {
	if rank <= 0 {
		return false, fmt.Errorf("rank must be >= 1")
	}

	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()
	store := c.loadUserMemoryStore(channel, userID)
	if len(store.Facts) == 0 {
		return false, nil
	}

	sorted := c.sortedMemoryFacts(store.Facts)
	if rank > len(sorted) {
		return false, nil
	}
	target := normalizeMemoryText(sorted[rank-1].Text)

	filtered := make([]memoryFact, 0, len(store.Facts))
	removed := false
	for _, f := range store.Facts {
		if !removed && normalizeMemoryText(f.Text) == target {
			removed = true
			continue
		}
		filtered = append(filtered, f)
	}
	if !removed {
		return false, nil
	}
	store.Facts = filtered
	if err := c.saveMemoryStore(channel, userID, store); err != nil {
		return false, err
	}
	return true, nil
}

// CompactUserMemory deduplicates and trims lower-value memory entries.
func (c *Client) CompactUserMemory(channel, userID string) (int, error) {
	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()
	store := c.loadUserMemoryStore(channel, userID)
	if len(store.Facts) == 0 {
		return 0, nil
	}

	merged := map[string]memoryFact{}
	for _, f := range store.Facts {
		key := normalizeMemoryText(f.Text)
		existing, ok := merged[key]
		if !ok {
			merged[key] = f
			continue
		}
		if f.Importance > existing.Importance {
			existing.Importance = f.Importance
			existing.Category = f.Category
		}
		if f.LastSeen.After(existing.LastSeen) {
			existing.LastSeen = f.LastSeen
		}
		existing.Count += f.Count
		merged[key] = existing
	}

	facts := make([]memoryFact, 0, len(merged))
	for _, v := range merged {
		facts = append(facts, v)
	}

	sort.Slice(facts, func(i, j int) bool {
		if facts[i].Importance != facts[j].Importance {
			return facts[i].Importance > facts[j].Importance
		}
		if !facts[i].LastSeen.Equal(facts[j].LastSeen) {
			return facts[i].LastSeen.After(facts[j].LastSeen)
		}
		return facts[i].Count > facts[j].Count
	})

	if c.memoryMaxFacts > 0 && len(facts) > c.memoryMaxFacts {
		facts = facts[:c.memoryMaxFacts]
	}
	removed := len(store.Facts) - len(facts)
	store.Facts = facts
	if err := c.saveMemoryStore(channel, userID, store); err != nil {
		return 0, err
	}
	return removed, nil
}

func (c *Client) saveMemoryStore(channel, userID string, store userMemoryStore) error {
	path := c.memoryFilePath(channel, userID)
	sort.Slice(store.Facts, func(i, j int) bool {
		if store.Facts[i].Importance != store.Facts[j].Importance {
			return store.Facts[i].Importance > store.Facts[j].Importance
		}
		if !store.Facts[i].LastSeen.Equal(store.Facts[j].LastSeen) {
			return store.Facts[i].LastSeen.After(store.Facts[j].LastSeen)
		}
		return store.Facts[i].Count > store.Facts[j].Count
	})
	if c.memoryMaxFacts > 0 && len(store.Facts) > c.memoryMaxFacts {
		store.Facts = store.Facts[:c.memoryMaxFacts]
	}
	encoded, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, encoded, 0o644)
}

func (c *Client) sortedMemoryFacts(facts []memoryFact) []memoryFact {
	out := append([]memoryFact(nil), facts...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Importance != out[j].Importance {
			return out[i].Importance > out[j].Importance
		}
		if !out[i].LastSeen.Equal(out[j].LastSeen) {
			return out[i].LastSeen.After(out[j].LastSeen)
		}
		return out[i].Count > out[j].Count
	})
	return out
}

func normalizeMemoryText(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, " ", "")
	return s
}

func parseImportMemoryStore(payload string) (userMemoryStore, error) {
	payload = strings.TrimSpace(payload)
	if payload == "" {
		return userMemoryStore{}, fmt.Errorf("memory import payload is empty")
	}

	raw, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		// Allow raw JSON payload as fallback.
		raw = []byte(payload)
	}

	var store userMemoryStore
	if err := json.Unmarshal(raw, &store); err != nil {
		return userMemoryStore{}, fmt.Errorf("invalid memory payload: %w", err)
	}
	if store.Version == 0 {
		store.Version = 1
	}

	now := time.Now()
	for i := range store.Facts {
		store.Facts[i].Category = normalizeMemoryCategory(store.Facts[i].Category)
		if store.Facts[i].Importance <= 0 {
			store.Facts[i].Importance = 1
		}
		if store.Facts[i].LastSeen.IsZero() {
			store.Facts[i].LastSeen = now
		}
		if store.Facts[i].Count <= 0 {
			store.Facts[i].Count = 1
		}
		store.Facts[i].Text = strings.TrimSpace(store.Facts[i].Text)
	}
	return store, nil
}

func normalizeMemoryCategory(category string) string {
	v := strings.ToLower(strings.TrimSpace(category))
	switch v {
	case "profile", "preference", "project", "environment", "model", "conversation":
		return v
	default:
		return "preference"
	}
}

func extractMemoryCandidates(userMessage, assistantReply string) []memoryFact {
	_ = assistantReply
	msg := strings.TrimSpace(userMessage)
	if msg == "" {
		return nil
	}

	separators := []string{"\n", "。", ".", "；", ";", "!", "！", "?", "？"}
	parts := []string{msg}
	for _, sep := range separators {
		next := make([]string, 0, len(parts))
		for _, p := range parts {
			next = append(next, strings.Split(p, sep)...)
		}
		parts = next
	}

	facts := make([]memoryFact, 0)
	for _, p := range parts {
		text := strings.TrimSpace(p)
		if text == "" {
			continue
		}
		runes := []rune(text)
		if len(runes) < 4 || len(runes) > 140 {
			continue
		}

		cat, importance := classifyMemoryFact(text)
		if importance <= 0 {
			continue
		}
		facts = append(facts, memoryFact{
			Text:       text,
			Category:   cat,
			Importance: importance,
		})
	}

	if len(facts) > 12 {
		facts = facts[:12]
	}
	return facts
}

func classifyMemoryFact(text string) (string, int) {
	lower := strings.ToLower(strings.TrimSpace(text))

	if strings.HasPrefix(lower, "/model ") {
		return "model", 5
	}
	if strings.HasPrefix(lower, "/provider ") {
		return "environment", 5
	}
	if strings.Contains(lower, "记住") || strings.Contains(lower, "以后") || strings.Contains(lower, "请用") || strings.Contains(lower, "必须") || strings.Contains(lower, "不要") {
		return "preference", 4
	}
	if strings.Contains(lower, "我叫") || strings.Contains(lower, "叫我") || strings.Contains(lower, "我的") || strings.Contains(lower, "我是") {
		return "profile", 3
	}
	if strings.Contains(lower, "项目") || strings.Contains(lower, "仓库") || strings.Contains(lower, "服务") || strings.Contains(lower, "端口") || strings.Contains(lower, "环境") {
		return "project", 3
	}
	if strings.Contains(lower, "喜欢") || strings.Contains(lower, "不喜欢") || strings.Contains(lower, "偏好") || strings.Contains(lower, "习惯") {
		return "preference", 3
	}

	return "conversation", 1
}

// GetSession retrieves session details.
func (c *Client) GetSession(ctx context.Context, sessionID string) (*opencode.Session, error) {
	return c.sdk.Session.Get(ctx, sessionID, opencode.SessionGetParams{})
}

// GetSessionStatus retrieves the status of a session.
// 根据OpenCode文档，GET /session/status 返回所有session的状态
// 状态包括：idle, running, error等
func (c *Client) GetSessionStatus(ctx context.Context, sessionID string) (string, error) {
	// TODO: SDK可能需要添加SessionStatus方法
	// 目前可以通过GetSession来获取状态
	_, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return "", err
	}
	// 根据session对象推断状态
	return "unknown", nil
}

// ListAgents retrieves all available agents/skills.
func (c *Client) ListAgents(ctx context.Context) ([]opencode.Agent, error) {
	result, err := c.sdk.Agent.List(ctx, opencode.AgentListParams{
		Directory: opencode.F(c.directory),
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return []opencode.Agent{}, nil
	}
	return *result, nil
}

// ExecuteCommand executes a command in the session context.
// This can be used to directly invoke skill scripts like websearch.py
func (c *Client) ExecuteCommand(ctx context.Context, sessionID, command string) (*opencode.SessionCommandResponse, error) {
	return c.sdk.Session.Command(ctx, sessionID, opencode.SessionCommandParams{
		Command:   opencode.F(command),
		Directory: opencode.F(c.directory),
	})
}

// ExecuteShell executes a shell command in the session context.
func (c *Client) ExecuteShell(ctx context.Context, sessionID, command string) (*opencode.AssistantMessage, error) {
	return c.sdk.Session.Shell(ctx, sessionID, opencode.SessionShellParams{
		Command:   opencode.F(command),
		Directory: opencode.F(c.directory),
	})
}

// ListSessions retrieves all sessions.
func (c *Client) ListSessions(ctx context.Context) ([]opencode.Session, error) {
	result, err := c.sdk.Session.List(ctx, opencode.SessionListParams{})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return []opencode.Session{}, nil
	}
	return *result, nil
}

// AbortSession aborts a running session.
// 根据OpenCode文档，POST /session/:id/abort 可以中止正在运行的session
func (c *Client) AbortSession(ctx context.Context, sessionID string) error {
	log.Printf("opencode: aborting session %s", sessionID[:8])
	_, err := c.sdk.Session.Abort(ctx, sessionID, opencode.SessionAbortParams{})
	if err != nil {
		return fmt.Errorf("opencode: abort session: %w", err)
	}
	c.releaseSessionSlot(sessionID)
	return nil
}

// ForkSession forks an existing session, creating a new independent session
// branching from the forked session's history. Uses the HTTP API directly
// since the Go SDK may not expose this endpoint.
func (c *Client) ForkSession(ctx context.Context, sessionID string) (string, error) {
	if c.endpoint == "" {
		return "", fmt.Errorf("opencode: fork session unavailable: missing endpoint")
	}
	log.Printf("opencode: forking session %s", sessionID[:min(8, len(sessionID))])

	forkURL := fmt.Sprintf("%s/session/%s/fork", strings.TrimRight(c.endpoint, "/"), sessionID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, forkURL, bytes.NewBufferString("{}"))
	if err != nil {
		return "", fmt.Errorf("opencode: fork session request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuthHeaders(req.Header)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("opencode: fork session do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("opencode: fork session status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var result struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("opencode: fork session decode response: %w", err)
	}
	if result.ID == "" {
		return "", fmt.Errorf("opencode: fork session returned empty ID")
	}

	log.Printf("opencode: forked session %s -> %s", sessionID[:min(8, len(sessionID))], result.ID[:min(8, len(result.ID))])
	return result.ID, nil
}

// CompactAndForkSession compacts the current session via the official summarize API
// and then forks it into a new session for continued work.
func (c *Client) CompactAndForkSession(ctx context.Context, sessionID string) (string, error) {
	if err := c.SummarizeSession(ctx, sessionID); err != nil {
		return "", err
	}

	newSessionID, err := c.ForkSession(ctx, sessionID)
	if err != nil {
		return "", err
	}

	if cfg, ok := c.getCurrentSessionModel(ctx, sessionID); ok {
		copied := &ModelConfig{
			ProviderID:  cfg.ProviderID,
			ModelID:     cfg.ModelID,
			LastUpdated: time.Now(),
		}
		c.sessionModel.Store(newSessionID, copied)
	}
	if override, ok := c.sessionOverride.Load(sessionID); ok {
		cfg := override.(*ModelConfig)
		c.sessionOverride.Store(newSessionID, &ModelConfig{
			ProviderID:  cfg.ProviderID,
			ModelID:     cfg.ModelID,
			LastUpdated: time.Now(),
		})
	}
	go c.fetchAndCacheModelConfig(context.Background(), newSessionID)

	return newSessionID, nil
}

// DeleteSession deletes a session.
func (c *Client) DeleteSession(ctx context.Context, sessionID string) error {
	log.Printf("opencode: deleting session %s", sessionID[:8])
	_, err := c.sdk.Session.Delete(ctx, sessionID, opencode.SessionDeleteParams{})
	if err != nil {
		return fmt.Errorf("opencode: delete session: %w", err)
	}
	// Clean up local caches
	c.messageCount.Delete(sessionID)
	c.tokenCount.Delete(sessionID)
	c.sessionSummary.Delete(sessionID)
	c.modelConfig.Delete(sessionID)
	c.lastDiffSummary.Delete(sessionID)
	c.releaseSessionSlot(sessionID)
	c.sessionQueues.Delete(sessionID)
	return nil
}

// GetProviders retrieves the list of available providers.
// Uses SDK App.Providers endpoint (/config/providers).
func (c *Client) GetProviders(ctx context.Context) ([]Provider, error) {
	if !c.Ready() {
		return nil, fmt.Errorf("opencode: client not configured")
	}

	params := opencode.AppProvidersParams{}
	if c.directory != "" {
		params.Directory = opencode.F(c.directory)
	}

	result, err := c.sdk.App.Providers(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("opencode: list providers: %w", err)
	}
	if result == nil || len(result.Providers) == 0 {
		c.modelCatalogMu.Lock()
		c.modelCatalog = map[string]*ModelCapability{}
		c.modelCatalogMu.Unlock()
		return []Provider{}, nil
	}

	for _, v := range result.Default {
		parts := strings.SplitN(strings.TrimSpace(v), "/", 2)
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			c.defaultModelMu.Lock()
			c.defaultModel = &ModelConfig{ProviderID: parts[0], ModelID: parts[1], LastUpdated: time.Now()}
			c.defaultModelMu.Unlock()
			break
		}
	}

	providers := make([]Provider, 0, len(result.Providers))
	newCatalog := make(map[string]*ModelCapability)
	for _, p := range result.Providers {
		models := make([]Model, 0, len(p.Models))
		for modelKey, m := range p.Models {
			modelID := m.ID
			if modelID == "" {
				modelID = modelKey
			}

			inputModalities := make([]string, 0, len(m.Modalities.Input))
			inputMap := make(map[string]bool)
			for _, in := range m.Modalities.Input {
				s := strings.ToLower(strings.TrimSpace(string(in)))
				if s == "" {
					continue
				}
				inputModalities = append(inputModalities, s)
				inputMap[s] = true
			}

			outputModalities := make([]string, 0, len(m.Modalities.Output))
			outputMap := make(map[string]bool)
			for _, out := range m.Modalities.Output {
				s := strings.ToLower(strings.TrimSpace(string(out)))
				if s == "" {
					continue
				}
				outputModalities = append(outputModalities, s)
				outputMap[s] = true
			}

			models = append(models, Model{
				ID:               modelID,
				Name:             m.Name,
				InputModalities:  inputModalities,
				OutputModalities: outputModalities,
			})

			newCatalog[modelCatalogKey(p.ID, modelID)] = &ModelCapability{
				ProviderID:       p.ID,
				ModelID:          modelID,
				InputModalities:  inputMap,
				OutputModalities: outputMap,
			}
		}

		sort.Slice(models, func(i, j int) bool {
			return models[i].ID < models[j].ID
		})

		providers = append(providers, Provider{
			ID:     p.ID,
			Name:   p.Name,
			Models: models,
		})
	}

	c.modelCatalogMu.Lock()
	c.modelCatalog = newCatalog
	c.modelCatalogMu.Unlock()

	return providers, nil
}

// Provider represents a model provider
type Provider struct {
	ID     string  `json:"id"`
	Name   string  `json:"name"`
	Models []Model `json:"models"`
}

// Model represents an AI model
type Model struct {
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	InputModalities  []string `json:"input_modalities,omitempty"`
	OutputModalities []string `json:"output_modalities,omitempty"`
}

// GetCurrentProvider retrieves the current provider and model for a session.
// Note: Session may not expose provider/model info directly
func (c *Client) GetCurrentProvider(ctx context.Context, sessionID string) (*Provider, string, error) {
	_, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return nil, "", fmt.Errorf("opencode: get session: %w", err)
	}

	// SDK Session doesn't have ProviderID/ModelID fields
	// Return empty for now
	log.Printf("opencode: session %s - provider info not available in SDK", sessionID[:8])
	return nil, "", nil
}

// UpdateSessionProvider updates the provider and model for a session.
// Note: SDK may not support this directly
func (c *Client) UpdateSessionProvider(ctx context.Context, sessionID, providerID, modelID string) error {
	log.Printf("opencode: updating session %s provider to %s/%s", sessionID[:8], providerID, modelID)

	providerID = strings.TrimSpace(providerID)
	modelID = strings.TrimSpace(modelID)
	if providerID == "" || modelID == "" {
		return fmt.Errorf("opencode: provider/model cannot be empty")
	}

	override := &ModelConfig{
		ProviderID:  providerID,
		ModelID:     modelID,
		LastUpdated: time.Now(),
	}
	c.sessionOverride.Store(sessionID, override)
	c.sessionModel.Store(sessionID, override)
	log.Printf("opencode: session %s model override set to %s/%s", sessionID[:8], providerID, modelID)

	// SDK SessionUpdateParams may not have ProviderID/ModelID fields
	// Try using Update with available params
	_, err := c.sdk.Session.Update(ctx, sessionID, opencode.SessionUpdateParams{
		// Use available fields only
	})
	if err != nil {
		log.Printf("opencode: session update API ignored (local override remains active): %v", err)
	}

	// Invalidate cached model config
	c.modelConfig.Delete(sessionID)
	// Fetch new config
	go c.fetchAndCacheModelConfig(context.Background(), sessionID)

	log.Printf("opencode: note - provider/model update may not be fully supported by SDK, using gateway override")
	return nil
}

func (c *Client) getSessionModelOverride(sessionID string) *opencode.SessionPromptParamsModel {
	if strings.TrimSpace(sessionID) == "" {
		return nil
	}
	if v, ok := c.sessionOverride.Load(sessionID); ok {
		cfg := v.(*ModelConfig)
		if strings.TrimSpace(cfg.ProviderID) != "" && strings.TrimSpace(cfg.ModelID) != "" {
			return &opencode.SessionPromptParamsModel{
				ProviderID: opencode.F(cfg.ProviderID),
				ModelID:    opencode.F(cfg.ModelID),
			}
		}
	}
	return nil
}

// IsSessionRunning checks if a session is currently running.
// A session is considered running when its semaphore slot is held.
func (c *Client) IsSessionRunning(sessionID string) bool {
	raw, ok := c.sessionQueues.Load(sessionID)
	if !ok {
		return false
	}
	return len(raw.(chan struct{})) > 0
}

func shouldQueueByChannel(_ string) bool {
	// All channels queue messages: only one message per session is sent to
	// OpenCode at a time.  Subsequent messages from the same user are held in
	// the per-session semaphore and the user sees an "⏳ 已入队" notification.
	// This matches TUI behaviour and prevents interleaved responses under
	// concurrent sends from DingTalk, Feishu, WeChat, and WebUI.
	return true
}

func sessionMapKey(channel, threadID string) string {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return ""
	}
	channel = strings.ToLower(strings.TrimSpace(channel))
	if channel == "" {
		return threadID
	}
	return channel + ":" + threadID
}

func (c *Client) loadSessionForThread(channel, threadID string) (string, bool) {
	key := sessionMapKey(channel, threadID)
	if key != "" {
		if sid, ok := c.sessions.Load(key); ok {
			return sid.(string), true
		}
	}
	legacyKey := strings.TrimSpace(threadID)
	if legacyKey != "" && legacyKey != key {
		if sid, ok := c.sessions.Load(legacyKey); ok {
			return sid.(string), true
		}
	}
	return "", false
}

func (c *Client) storeSessionForThread(channel, threadID, sessionID string) {
	key := sessionMapKey(channel, threadID)
	if key == "" || strings.TrimSpace(sessionID) == "" {
		return
	}
	c.sessions.Store(key, sessionID)
}

func (c *Client) deleteSessionForThread(channel, threadID string) {
	key := sessionMapKey(channel, threadID)
	if key != "" {
		c.sessions.Delete(key)
	}
	legacyKey := strings.TrimSpace(threadID)
	if legacyKey != "" && legacyKey != key {
		c.sessions.Delete(legacyKey)
	}
}

// acquireSessionSlot blocks until a slot is free for the given session,
// serialising concurrent messages to the same opencode session.
// Returns a release function that MUST be called when the send completes.
func (c *Client) acquireSessionSlot(ctx context.Context, sessionID string) (func(), error) {
	raw, _ := c.sessionQueues.LoadOrStore(sessionID, make(chan struct{}, 1))
	sem := raw.(chan struct{})
	// Fast path: slot is free
	select {
	case sem <- struct{}{}:
		return func() { c.releaseSessionSlot(sessionID) }, nil
	default:
	}
	// Slow path: session busy, queue up
	log.Printf("opencode: session %s busy, queuing new message", sessionID[:8])
	select {
	case sem <- struct{}{}:
		log.Printf("opencode: session %s slot acquired after queuing", sessionID[:8])
		return func() { c.releaseSessionSlot(sessionID) }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// releaseSessionSlot releases the current slot for a session. Idempotent and safe to call multiple times.
func (c *Client) releaseSessionSlot(sessionID string) {
	raw, ok := c.sessionQueues.Load(sessionID)
	if !ok {
		return
	}
	select {
	case <-raw.(chan struct{}):
	default:
	}
}

// StartEventListener begins listening for OpenCode events via SSE.
// 支持自动重连：当 OpenCode Server 断开或重启后，会自动重新连接事件流。
func (c *Client) StartEventListener(ctx context.Context) error {
	log.Printf("opencode: starting event listener...")

	go c.eventListenerLoop(ctx)

	log.Printf("opencode: event listener started successfully")
	return nil
}

// eventListenerLoop 事件监听主循环，支持自动重连
func (c *Client) eventListenerLoop(ctx context.Context) {
	reconnectDelay := 2 * time.Second
	maxReconnectDelay := 60 * time.Second
	consecutiveFailures := 0

	for {
		select {
		case <-ctx.Done():
			log.Printf("opencode: event listener context cancelled, stopping")
			return
		default:
		}

		if consecutiveFailures > 0 {
			// 指数退避重连
			delay := reconnectDelay * time.Duration(1<<uint(min(consecutiveFailures-1, 5)))
			if delay > maxReconnectDelay {
				delay = maxReconnectDelay
			}
			log.Printf("opencode: 🔄 reconnecting event listener in %v (attempt #%d)...", delay, consecutiveFailures)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return
			}
		}

		log.Printf("opencode: creating event stream...")
		stream := c.sdk.Event.ListStreaming(ctx, opencode.EventListParams{})

		eventCount := 0
		connected := false

		for stream.Next() {
			event := stream.Current()
			eventCount++

			eventType := string(event.Type)

			// 首次收到事件，标记连接成功
			if !connected {
				connected = true
				if consecutiveFailures > 0 {
					log.Printf("opencode: 🔄 server reconnected after %d failures, invalidating stale session cache", consecutiveFailures)
					c.invalidateStaleSessions(ctx)
					log.Printf("opencode: ✅ event listener reconnected successfully after %d failures", consecutiveFailures)
				}
				consecutiveFailures = 0
			}

			// Log every event for debugging
			if eventCount <= 10 || eventType != "server.heartbeat" {
				log.Printf("opencode: [event #%d] type=%s", eventCount, eventType)
			}

			// Extract session ID using shared helper
			sessionID := extractSessionIDFromEvent(&event)

			// Populate messageRoles for message.part.updated events (regardless of sessionID path)
			// so that message.part.delta events for user messages can be filtered before broadcast.
			if eventType == "message.part.updated" {
				if raw := event.JSON.RawJSON(); raw != "" {
					// Dump first 15 events for debugging
					if eventCount <= 15 {
						log.Printf("opencode: 🔍🔍 [message.part.updated #%d RAW] %.1000s", eventCount, raw)
					}
					if meta, ok := parseMessagePartUpdatedMeta(raw); ok {
						if meta.MessageID != "" && meta.Role != "" {
							c.messageRoles.Store(meta.MessageID, meta.Role)
						}
						if meta.MessageID != "" && meta.SessionID != "" {
							c.messageToSession.Store(meta.MessageID, meta.SessionID)
						}
						if meta.PartID != "" && meta.SessionID != "" {
							c.partToSession.Store(meta.PartID, meta.SessionID)
						}
					}
				}
			}

			// ALSO populate messageRoles from message.updated events (which DO contain role)
			// NOTE: message.updated structure is properties.info.{id,role} NOT properties.message.{id,role}
			if eventType == "message.updated" {
				if raw := event.JSON.RawJSON(); raw != "" {
					var msgInfo struct {
						Properties struct {
							Info struct {
								ID   string `json:"id"`
								Role string `json:"role"`
							} `json:"info"`
						} `json:"properties"`
					}
					if json.Unmarshal([]byte(raw), &msgInfo) == nil {
						if msgID := msgInfo.Properties.Info.ID; msgID != "" && msgInfo.Properties.Info.Role != "" {
							c.messageRoles.Store(msgID, msgInfo.Properties.Info.Role)
							log.Printf("opencode: ✅ stored messageRole: msgID=%s, role=%s",
								msgID[:min(8, len(msgID))], msgInfo.Properties.Info.Role)
						}
					}
				}
			}

			// For message.part.updated / message.part.delta events the base extractor
			// may return "" because the sessionID is nested in properties.message.
			// Re-parse those two types and also build the messageID→sessionID reverse map
			// so that subsequent delta events can be dispatched correctly.
			if sessionID == "" && (eventType == "message.part.delta" || eventType == "message.part.updated") {
				if raw := event.JSON.RawJSON(); raw != "" {
					var probe struct {
						Properties struct {
							MessageID string `json:"messageID"` // present in message.part.delta
							PartID    string `json:"partID"`    // present in message.part.delta
							Message   struct {
								ID        string `json:"id"`
								SessionID string `json:"sessionID"`
								Role      string `json:"role"` // "user" or "assistant"
							} `json:"message"` // present in message.part.updated
						} `json:"properties"`
					}
					if json.Unmarshal([]byte(raw), &probe) == nil {
						if probe.Properties.Message.SessionID != "" {
							// message.part.updated — record messageID→sessionID and messageID→role
							sessionID = probe.Properties.Message.SessionID
							if msgID := probe.Properties.Message.ID; msgID != "" {
								c.messageToSession.Store(msgID, sessionID)
								if role := probe.Properties.Message.Role; role != "" {
									c.messageRoles.Store(msgID, role)
								}
							}
							if partID := probe.Properties.PartID; partID != "" {
								c.partToSession.Store(partID, sessionID)
							}
						} else if probe.Properties.MessageID != "" {
							// message.part.delta — resolve via messageID, fallback to partID.
							sessionID = resolveSessionIDForPartDelta(probe.Properties.MessageID, probe.Properties.PartID, &c.messageToSession, &c.partToSession)
							// If still no sessionID, drop delta instead of broadcasting to all sessions.
							// Broadcasting causes cross-session content mixing under high concurrency.
							if sessionID == "" {
								role, roleKnown := c.messageRoles.Load(probe.Properties.MessageID)
								if !roleKnown {
									log.Printf("opencode: dropping message.part.delta with unknown messageID (likely old session replay)")
								} else if role.(string) == "user" {
									log.Printf("opencode: skipping user message.part.delta broadcast (msgID=%s)", probe.Properties.MessageID[:min(8, len(probe.Properties.MessageID))])
								} else {
									log.Printf("opencode: dropping assistant message.part.delta without resolvable session (msgID=%s, partID=%s)", probe.Properties.MessageID[:min(8, len(probe.Properties.MessageID))], probe.Properties.PartID[:min(8, len(probe.Properties.PartID))])
								}
							}
						}
					}
					// Dump first few events for structure verification
					if eventCount <= 5 {
						log.Printf("opencode: [raw %s #%d] %.300s", eventType, eventCount, raw)
					}
				}
			}

			if sessionID != "" && len(sessionID) > 8 {
				log.Printf("opencode: processing event type=%s, sessionID=%s", eventType, sessionID[:8])
			} else if eventType != "server.heartbeat" && eventType != "server.connected" && sessionID == "" {
				log.Printf("opencode: processing event type=%s (no sessionID)", eventType)
			}

			// Dispatch: per-session streaming handler first, then global handlers.
			// SSEDispatcher guarantees in-order delivery per session.
			c.dispatcher.Dispatch(ctx, sessionID, &event)
		}

		stream.Close()

		log.Printf("opencode: event stream ended, total events processed: %d", eventCount)

		if err := stream.Err(); err != nil {
			log.Printf("opencode: event stream error: %v", err)
			c.maybeHandleServerUnavailable(ctx, err, "event-stream")
		}

		// 检查是否是主动退出
		select {
		case <-ctx.Done():
			log.Printf("opencode: event listener stopped (context cancelled)")
			return
		default:
			// 非主动退出，准备重连
			consecutiveFailures++
			log.Printf("opencode: ⚠️ event stream disconnected unexpectedly, will reconnect...")
		}
	}
}

func (c *Client) maybeHandleServerUnavailable(ctx context.Context, err error, source string) {
	if c.serverUnavailable == nil || err == nil {
		return
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "connection refused") &&
		!strings.Contains(msg, "no such host") &&
		!strings.Contains(msg, "dial tcp") &&
		!strings.Contains(msg, "connect:") {
		return
	}

	c.unavailableMu.Lock()
	if !c.lastUnavailableAt.IsZero() && time.Since(c.lastUnavailableAt) < c.unavailableDelay {
		c.unavailableMu.Unlock()
		return
	}
	c.lastUnavailableAt = time.Now()
	c.unavailableMu.Unlock()

	reason := fmt.Sprintf("%s unavailable: %v", source, err)
	go func() {
		actionCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		msg, actionErr := c.serverUnavailable(actionCtx, reason)
		if actionErr != nil {
			log.Printf("opencode: self-heal action failed (%s): %v", source, actionErr)
			return
		}
		if strings.TrimSpace(msg) != "" {
			log.Printf("opencode: self-heal action: %s", msg)
		}
	}()
}

// invalidateStaleSessions 清除可能失效的 session 缓存
// 当 OpenCode Server 重启后调用，让下次消息时创建新 session
func (c *Client) invalidateStaleSessions(ctx context.Context) {
	var staleThreads []string

	c.sessions.Range(func(key, value interface{}) bool {
		threadID := key.(string)
		sessionID := value.(string)

		// 尝试验证 session 是否仍然有效
		checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := c.GetSession(checkCtx, sessionID)
		cancel()

		if err != nil {
			log.Printf("opencode: 🗑️ session %s for thread %s is stale (err: %v), removing",
				sessionID[:min(8, len(sessionID))], threadID, err)
			staleThreads = append(staleThreads, threadID)
		} else {
			log.Printf("opencode: ✅ session %s for thread %s is still valid",
				sessionID[:min(8, len(sessionID))], threadID)
		}

		return true
	})

	// 删除失效的映射
	for _, threadID := range staleThreads {
		c.sessions.Delete(threadID)
		log.Printf("opencode: removed stale session mapping for thread %s", threadID)
	}

	if len(staleThreads) > 0 {
		log.Printf("opencode: invalidated %d stale session mappings", len(staleThreads))
	}
}

// RegisterEventHandler adds a global event handler that receives every SSE
// event regardless of sessionID.  Delegates to the SSEDispatcher.
func (c *Client) RegisterEventHandler(handler EventHandler) {
	c.dispatcher.AddGlobalHandler(handler)
}

// RegisterSessionHandler registers a per-session event handler.
// The handler is stored both in the SSEDispatcher (for routing) and in
// activeHandlers (for TODO/diff introspection by other callers).
func (c *Client) RegisterSessionHandler(sessionID string, handler EventHandler) {
	c.sessionHandlers.Store(sessionID, handler) // legacy: kept for direct Load calls in tests
	c.dispatcher.SetSessionHandler(sessionID, handler)
}

// UnregisterSessionHandler removes per-session routing for sessionID.
func (c *Client) UnregisterSessionHandler(sessionID string) {
	c.sessionHandlers.Delete(sessionID)
	c.activeHandlers.Delete(sessionID)
	c.dispatcher.RemoveSessionHandler(sessionID)
}

// GetTodosForSession returns the current todo list for a running session.
// Returns nil if the session has no active handler or no todos.
func (c *Client) GetTodosForSession(sessionID string) []TodoItem {
	if h, ok := c.activeHandlers.Load(sessionID); ok {
		return h.(*StreamingSessionHandler).GetTodos()
	}
	return nil
}

// GetDiffForSession returns the accumulated file-change diff for a session.
// Returns nil if the session has no active handler or no diff yet.
func (c *Client) GetDiffForSession(sessionID string) []FileDiff {
	if h, ok := c.activeHandlers.Load(sessionID); ok {
		return h.(*StreamingSessionHandler).GetDiff()
	}
	return nil
}

var _ MessageSender = (*Client)(nil)

// SendMessageToSession 通过adapter主动推送消息给会话关联的用户
// 注意：这个方法目前只是记录日志，实际的消息推送通过streaming callback完成
// 未来可以扩展支持通过adapter的双向通信机制主动推送
func (c *Client) SendMessageToSession(ctx context.Context, sessionID, content string) error {
	log.Printf("opencode: SendMessageToSession for session %s (len=%d chars)", sessionID[:8], len(content))
	// 当前实现：依赖streaming callback机制，这里只是接口占位
	// 未来可以添加通过adapter反向推送的逻辑
	return nil
}

// extractReplyFromMessage extracts text content from a prompt response.
// OpenCode返回的message包含多个part，每个part可以是：
// - TextPart: 普通文本
// - 其他类型: 工具调用、思考过程等
func extractReplyFromMessage(msg *opencode.SessionPromptResponse) string {
	if msg == nil || len(msg.Parts) == 0 {
		log.Printf("opencode: WARNING - no response parts to extract")
		return "(正在处理中，请稍后查看 OpenCode 界面获取结果）"
	}

	var textParts []string

	for i, part := range msg.Parts {
		switch p := part.AsUnion().(type) {
		case opencode.TextPart:
			textParts = append(textParts, p.Text)
			log.Printf("opencode: extracted text part %d: %d chars", i, len(p.Text))
		default:
			// 其他类型的part，记录但不提取
			log.Printf("opencode: skipped non-text part %d (type: %T)", i, p)
		}
	}

	if len(textParts) == 0 {
		log.Printf("opencode: WARNING - no text parts found in %d parts", len(msg.Parts))
		return "(响应已收到但无文本内容，请查看 OpenCode 界面 - message ID: " + msg.Info.ID + ")"
	}

	fullReply := strings.Join(textParts, "\n")
	log.Printf("opencode: extracted reply: %d chars from %d text parts", len(fullReply), len(textParts))
	return fullReply
}

type messagePartUpdatedMeta struct {
	SessionID string
	MessageID string
	PartID    string
	Role      string
}

func parseMessagePartUpdatedMeta(raw string) (messagePartUpdatedMeta, bool) {
	if strings.TrimSpace(raw) == "" {
		return messagePartUpdatedMeta{}, false
	}

	// message.part.updated structure (from SDK EventListResponseEventMessagePartUpdatedProperties):
	//   properties.part  → the full Part object (id, messageID, sessionID, type, text, ...)
	//   properties.delta → optional incremental delta string
	//
	// The session ID and message ID are in properties.part.*, NOT in properties.message.*
	// (there is no "message" field in message.part.updated properties).
	var payload struct {
		Properties struct {
			Part struct {
				ID        string `json:"id"`
				MessageID string `json:"messageID"`
				SessionID string `json:"sessionID"`
			} `json:"part"`
		} `json:"properties"`
	}

	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return messagePartUpdatedMeta{}, false
	}

	meta := messagePartUpdatedMeta{
		SessionID: payload.Properties.Part.SessionID,
		MessageID: payload.Properties.Part.MessageID,
		PartID:    payload.Properties.Part.ID,
		// Role is not available in message.part.updated; it comes from message.updated events.
	}
	if meta.SessionID == "" && meta.MessageID == "" && meta.PartID == "" {
		return messagePartUpdatedMeta{}, false
	}
	return meta, true
}

func resolveSessionIDForPartDelta(messageID, partID string, messageToSession, partToSession *sync.Map) string {
	if messageToSession != nil && strings.TrimSpace(messageID) != "" {
		if sid, ok := messageToSession.Load(messageID); ok {
			if resolved, ok := sid.(string); ok && strings.TrimSpace(resolved) != "" {
				return resolved
			}
		}
	}
	if partToSession != nil && strings.TrimSpace(partID) != "" {
		if sid, ok := partToSession.Load(partID); ok {
			if resolved, ok := sid.(string); ok && strings.TrimSpace(resolved) != "" {
				return resolved
			}
		}
	}
	return ""
}

// getThreadLock gets or creates a lock for a specific thread to prevent concurrent operations.
func (c *Client) getThreadLock(threadID string) *sync.Mutex {
	if threadID == "" {
		return &sync.Mutex{} // Return a new mutex for single-use
	}

	lock, _ := c.sessionLocks.LoadOrStore(threadID, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

// SummarizeSession 使用官方 summarize API 压缩当前 session。
func (c *Client) SummarizeSession(ctx context.Context, sessionID string) error {
	if !c.Ready() {
		return fmt.Errorf("opencode: client not configured")
	}

	log.Printf("opencode: summarizing session %s", sessionID)
	modelCfg, ok := c.getCurrentSessionModel(ctx, sessionID)
	if !ok || strings.TrimSpace(modelCfg.ProviderID) == "" || strings.TrimSpace(modelCfg.ModelID) == "" {
		return fmt.Errorf("opencode: summarize session: unable to determine model for session %s", sessionID)
	}

	_, err := c.sdk.Session.Summarize(ctx, sessionID, opencode.SessionSummarizeParams{
		ProviderID: opencode.F(modelCfg.ProviderID),
		ModelID:    opencode.F(modelCfg.ModelID),
	})
	if err != nil {
		return fmt.Errorf("opencode: summarize session: %w", err)
	}
	log.Printf("opencode: session %s summarized successfully with %s/%s", sessionID, modelCfg.ProviderID, modelCfg.ModelID)

	return nil
}

// extractSummaryFromSession 从session中提取总结信息
func extractSummaryFromSession(session *opencode.Session) string {
	if session == nil {
		return ""
	}
	// TODO: 根据实际的session结构提取总结
	// 可能需要获取messages并查找summary类型的消息
	return "" // 暂时返回空，需要根据SDK实际结构实现
}

// truncateString 截断字符串到指定长度
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	// 处理UTF-8字符
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "..."
}

// GetMessageCount 获取指定session的消息数量
func (c *Client) GetMessageCount(sessionID string) int {
	return c.loadCounter(&c.messageCount, sessionID)
}

// ResetSession 重置thread的session映射，强制创建新session
func (c *Client) ResetSession(threadID string) {
	if threadID != "" {
		c.sessions.Delete(threadID)
		log.Printf("opencode: reset session mapping for thread %s", threadID)
	}
}

// SetSessionForThread binds a thread to a specific session.
func (c *Client) SetSessionForThread(threadID, sessionID string) {
	if strings.TrimSpace(threadID) == "" {
		return
	}
	if strings.TrimSpace(sessionID) == "" {
		c.sessions.Delete(threadID)
		return
	}
	c.sessions.Store(threadID, sessionID)
	log.Printf("opencode: set session mapping for thread %s -> %s", threadID, sessionID[:min(8, len(sessionID))])
}

// GetSessionForThread retrieves the session ID associated with a thread.
func (c *Client) GetSessionForThread(threadID string) (string, bool) {
	val, ok := c.sessions.Load(threadID)
	if !ok {
		return "", false
	}
	return val.(string), true
}

// GetSessionInfo retrieves detailed information about a session.
type SessionInfo struct {
	SessionID     string
	Title         string
	Directory     string
	MessageCount  int
	TokenCount    int
	ContextUsage  float64
	ContextLength int
	Created       string // Use string since SDK doesn't have CreatedAt
}

func (c *Client) GetSessionInfo(ctx context.Context, sessionID string) (*SessionInfo, error) {
	session, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return nil, err
	}

	// Convert Unix timestamp to time string
	createdTime := time.Unix(int64(session.Time.Updated), 0).Format("2006-01-02 15:04:05")

	info := &SessionInfo{
		SessionID:    sessionID,
		Title:        session.Title,
		Directory:    session.Directory,
		MessageCount: c.GetMessageCount(sessionID),
		TokenCount:   c.GetTokenCount(sessionID),
		Created:      createdTime,
	}

	// Get context length
	info.ContextLength = c.getMaxContextLength(sessionID)
	if info.ContextLength > 0 && info.TokenCount > 0 {
		info.ContextUsage = float64(info.TokenCount) / float64(info.ContextLength)
	}

	return info, nil
}

// SendMessageStreaming sends a message and calls the callback for each chunk of the response.
// 真正的流式实现：注册StreamingSessionHandler监听实时事件
func (c *Client) SendMessageStreaming(ctx context.Context, payload MessagePayload, callback StreamCallback) (Response, error) {
	//fmt.Println("payload is______________________________:", payload.)
	if callback == nil {
		// 如果没有回调，直接使用普通模式
		return c.SendMessage(ctx, payload)
	}

	if notice := c.consumePersonaSetupPrompt(payload.Channel, payload.UserID); notice != "" {
		if err := callback(notice + "\n\n"); err != nil {
			log.Printf("opencode: persona setup notice callback failed: %v", err)
		}
	}

	// 1. 先确定sessionID（可能需要创建新session）
	threadLock := c.getThreadLock(sessionMapKey(payload.Channel, payload.ThreadID))
	threadLock.Lock()
	sessionID := payload.SessionID
	if sessionID == "" && payload.ThreadID != "" {
		if sid, ok := c.loadSessionForThread(payload.Channel, payload.ThreadID); ok {
			sessionID = sid

			// 验证 session 是否仍然有效
			checkCtx, checkCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, checkErr := c.GetSession(checkCtx, sessionID)
			checkCancel()
			if checkErr != nil {
				log.Printf("opencode: ⚠️ streaming session %s is stale (err: %v), will create new",
					sessionID[:min(8, len(sessionID))], checkErr)
				c.deleteSessionForThread(payload.Channel, payload.ThreadID)
				c.messageCount.Delete(sessionID)
				c.tokenCount.Delete(sessionID)
				sessionID = "" // 强制创建新 session
			}
		}
	}

	// 如果还是没有sessionID，我们需要先创建session
	if sessionID == "" {
		sessionCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		// 将 adapter 和 user 信息编码到 Title 中，格式: [adapter:userId] threadId
		sessionTitle := fmt.Sprintf("[%s:%s] %s", payload.Channel, payload.UserID, payload.ThreadID)
		session, err := c.sdk.Session.New(sessionCtx, opencode.SessionNewParams{
			Title: opencode.F(sessionTitle),
		})
		cancel()
		if err != nil {
			threadLock.Unlock()
			return Response{}, fmt.Errorf("opencode: create session for streaming: %w", err)
		}
		sessionID = session.ID
		if payload.ThreadID != "" {
			c.storeSessionForThread(payload.Channel, payload.ThreadID, sessionID)
		}
		c.messageCount.Store(sessionID, 0)
		c.tokenCount.Store(sessionID, 0)
		log.Printf("opencode: created new session %s for streaming", sessionID)
	}
	threadLock.Unlock()

	// 2. 立即通过callback通知sessionID（供adapter建立user映射）
	log.Printf("opencode: notifying sessionID %s via callback", sessionID)
	if err := callback(sessionID); err != nil {
		log.Printf("opencode: failed to notify sessionID via callback: %v", err)
	} else {
		log.Printf("opencode: sessionID notification sent successfully")
	}

	var handler *StreamingSessionHandler
	unregisterIfCurrent := func() {
		if current, ok := c.activeHandlers.Load(sessionID); ok {
			if current == handler {
				c.UnregisterSessionHandler(sessionID)
			}
		}
	}

	// 3. 仅对 webui 开启排队：钉钉/飞书/企微不在 gateway 侧排队。
	queueEnabled := shouldQueueByChannel(payload.Channel)
	if queueEnabled {
		if c.IsSessionRunning(sessionID) {
			log.Printf("opencode: session %s busy, notifying user of queued message", sessionID[:8])
			_ = callback("⏳ 上条消息处理中，已排队等待...\n")
		}
		slotRelease, slotErr := c.acquireSessionSlot(ctx, sessionID)
		if slotErr != nil {
			return Response{}, fmt.Errorf("opencode: wait for session slot: %w", slotErr)
		}
		// defer 保证无论 SendMessageStreaming 经哪条路径退出（doneCh / 超时 / error / ctx），
		// slot 一定被释放；handler 仅在仍是当前 active handler 时才会被注销。
		defer func() {
			unregisterIfCurrent()
			slotRelease()
		}()
	} else {
		defer unregisterIfCurrent()
	}

	// 4. 创建StreamingSessionHandler并注册（slot 已持有，handler 正式接管本次请求）
	// onComplete=nil：清理统一由上方 defer 完成，handler 内部只需关闭 doneCh。
	handler = NewStreamingSessionHandler(sessionID, callback, nil, c, c, c.IsThinkingEnabled(), c.IsStepEnabled())
	c.RegisterSessionHandler(sessionID, handler.HandleEvent)
	c.activeHandlers.Store(sessionID, handler)
	log.Printf("opencode: registered streaming handler for session %s", sessionID[:8])

	// 5. 使用goroutine异步发送消息（slot 已预占，SendMessage 中跳过再次获取）
	responseChan := make(chan Response, 1)
	errorChan := make(chan error, 1)

	payloadWithSlot := payload
	payloadWithSlot.slotPreAcquired = true
	go func(p MessagePayload) {
		response, err := c.SendMessage(ctx, p)
		if err != nil {
			errorChan <- err
			return
		}
		responseChan <- response
	}(payloadWithSlot)

	// ticker 仅作超时兜底，正常完成路径由 handler.Done() channel 驱动，避免 5s 轮询延迟。
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	isAsyncMode := false
	var asyncResponse Response
	// sessionIdleFired: doneCh 已关闭，但 isAsyncMode 还未设置时的标记
	sessionIdleFired := false
	idleCheckCount := 0

	for {
		select {
		case <-ctx.Done():
			return Response{}, ctx.Err()

		case err := <-errorChan:
			return Response{}, err

		case response := <-responseChan:
			if response.Reply == "" {
				// Async 模式：prompt_async 立即返回空 reply，等待 SSE 事件
				log.Printf("opencode: streaming async mode for session %s, waiting for SSE events", sessionID[:8])
				isAsyncMode = true
				asyncResponse = response
				// doneCh 可能已经关闭（session.idle 早于 prompt_async 响应极少数情况）
				if sessionIdleFired {
					log.Printf("opencode: ✅ session already idle before responseChan, returning immediately for %s", sessionID[:8])
					return asyncResponse, nil
				}
				continue
			}
			// 同步模式
			sentContent := handler.GetLastContent()
			log.Printf("opencode: streaming completed, handler sent: %d chars, final response: %d chars",
				len(sentContent), len(response.Reply))
			if sentContent == "" || sentContent != response.Reply {
				if response.Reply != "" {
					log.Printf("opencode: sending final response via callback")
					if err := callback(response.Reply); err != nil {
						log.Printf("opencode: final callback error: %v", err)
					}
				}
			}
			return response, nil

		case <-handler.Done():
			// session.idle / error / deleted 事件关闭 doneCh，立即响应，无需等下一个 tick。
			//
			// FlushSignal is sent HERE, in the adapter's message-handler goroutine
			// (NOT in the SSE reader goroutine while s.mu is held), so the SSE reader
			// is never blocked by a potentially long network I/O call.
			log.Printf("opencode: 📤 doneCh fired for session %s — sending FlushSignal", sessionID[:8])
			_ = callback(FlushSignal)

			if isAsyncMode {
				log.Printf("opencode: ✅ async streaming completed via doneCh for session %s (contentSent=%t)",
					sessionID[:8], handler.HasSentContent())
				return asyncResponse, nil
			}
			// responseChan 尚未投递（极少数情况），先记录，等 responseChan case 处理
			sessionIdleFired = true

		case <-ticker.C:
			// 兜底超时逻辑（doneCh 未能触发时的安全网）
			lastEventTime, lastEventType := handler.GetLastEventInfo()
			timeSinceLastEvent := time.Since(lastEventTime)
			hasSentContent := handler.HasSentContent()
			hasStepFinish := handler.HasReceivedStepFinish()
			stepFinishTime := handler.GetStepFinishTime()
			hasPendingQ := handler.HasPendingQuestion()

			log.Printf("opencode: 🔍 ticker check - session=%s, isAsync=%t, hasSent=%t, hasStepFinish=%t, lastEvent=%v ago (type=%s), pendingQ=%t, idleCount=%d",
				sessionID[:8], isAsyncMode, hasSentContent, hasStepFinish, timeSinceLastEvent, lastEventType, hasPendingQ, idleCheckCount)

			// 会话正在等待用户回答问题/权限确认时，跳过短超时，使用更长的超时（5分钟）。
			// 这防止了用户在思考答案时 SendMessageStreaming 提前返回并注销 handler，
			// 导致 OpenCode 继续执行后发出的事件找不到 handler 的问题。
			if hasPendingQ {
				if timeSinceLastEvent > 5*time.Minute {
					log.Printf("opencode: ⏱️ question-answer timeout (5m) for session %s, giving up", sessionID[:8])
					_ = callback(FlushSignal)
					return asyncResponse, nil
				}
				idleCheckCount++
				continue
			}

			// step-finish 后 5s 无新事件 → 认为完成
			if isAsyncMode && hasStepFinish && hasSentContent && !stepFinishTime.IsZero() {
				if time.Since(stepFinishTime) > 5*time.Second {
					log.Printf("opencode: 🏁 step-finish timeout fallback for session %s", sessionID[:8])
					_ = callback(FlushSignal)
					return asyncResponse, nil
				}
			}
			// 有内容且 10s 无事件
			if isAsyncMode && hasSentContent && timeSinceLastEvent > 10*time.Second {
				log.Printf("opencode: ⏱️ idle fallback (10s, has content) for session %s", sessionID[:8])
				_ = callback(FlushSignal)
				return asyncResponse, nil
			}
			// 20s 无任何事件
			if isAsyncMode && timeSinceLastEvent > 20*time.Second {
				log.Printf("opencode: ⏱️ idle fallback (20s, no content) for session %s", sessionID[:8])
				_ = callback(FlushSignal)
				return asyncResponse, nil
			}
			idleCheckCount++
		}
	}
}

// estimateTokens 估算文本的token数量
// 简单实现：中文字符按1.5倍计算，英文单词按1个token计算
func estimateTokens(text string) int {
	if text == "" {
		return 0
	}

	runes := []rune(text)
	tokens := 0
	inWord := false

	for _, r := range runes {
		// 中文字符（CJK统一表意文字）
		if r >= 0x4E00 && r <= 0x9FFF {
			tokens += 2 // 中文字符通常占用更多token
		} else if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			// 英文字母和数字，按单词计数
			if !inWord {
				tokens++
				inWord = true
			}
		} else {
			inWord = false
			// 标点符号等
			if r != ' ' && r != '\t' && r != '\n' {
				tokens++
			}
		}
	}

	// 添加一些开销（系统提示词、格式化等）
	return int(float64(tokens) * 1.3)
}

// getMaxContextLength 获取session的最大上下文长度
func (c *Client) getMaxContextLength(sessionID string) int {
	// 尝试从缓存获取模型配置
	if cfg, ok := c.modelConfig.Load(sessionID); ok {
		modelCfg := cfg.(*ModelConfig)
		if modelCfg.ContextLength > 0 {
			return modelCfg.ContextLength
		}
	}

	// 返回默认值
	return DefaultMaxTokens
}

// fetchAndCacheModelConfig 获取并缓存session的模型配置
func (c *Client) fetchAndCacheModelConfig(ctx context.Context, sessionID string) {
	// 创建一个带超时的context
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// 获取session详情
	session, err := c.GetSession(ctx, sessionID)
	if err != nil {
		log.Printf("opencode: failed to get session %s for model config: %v", sessionID[:8], err)
		return
	}

	// 提取模型信息（需要根据实际SDK结构调整）
	// 这里假设session中包含模型信息，实际可能需要调用其他API
	config := &ModelConfig{
		LastUpdated:   time.Now(),
		ContextLength: guessContextLengthFromSession(session),
	}

	c.modelConfig.Store(sessionID, config)
	log.Printf("opencode: cached model config for session %s, context length: %d",
		sessionID[:8], config.ContextLength)
}

// guessContextLengthFromSession 根据session信息猜测上下文长度
func guessContextLengthFromSession(session *opencode.Session) int {
	// TODO: 根据实际SDK结构提取模型信息
	// 可能需要调用 /config/providers API 获取模型列表和配置

	// 常见模型的上下文长度
	// GPT-4: 8k, 32k, 128k
	// Claude: 100k, 200k
	// 其他模型: 4k-8k

	// 目前返回一个保守的默认值
	return 8192 // 8k tokens，适用于大多数模型
}

// GetTokenCount 获取指定session的token使用量
func (c *Client) GetTokenCount(sessionID string) int {
	return c.loadCounter(&c.tokenCount, sessionID)
}

// loadCounter safely loads an integer counter from sync.Map.
// It auto-recovers missing or unexpected value types by normalizing to 0.
func (c *Client) loadCounter(counterMap *sync.Map, key string) int {
	v, ok := counterMap.Load(key)
	if !ok || v == nil {
		counterMap.Store(key, 0)
		return 0
	}
	if n, ok := v.(int); ok {
		return n
	}

	// 兼容历史/异常值，避免 interface conversion panic
	switch n := v.(type) {
	case int8:
		counterMap.Store(key, int(n))
		return int(n)
	case int16:
		counterMap.Store(key, int(n))
		return int(n)
	case int32:
		counterMap.Store(key, int(n))
		return int(n)
	case int64:
		counterMap.Store(key, int(n))
		return int(n)
	case uint:
		counterMap.Store(key, int(n))
		return int(n)
	case uint8:
		counterMap.Store(key, int(n))
		return int(n)
	case uint16:
		counterMap.Store(key, int(n))
		return int(n)
	case uint32:
		counterMap.Store(key, int(n))
		return int(n)
	case uint64:
		counterMap.Store(key, int(n))
		return int(n)
	case float32:
		counterMap.Store(key, int(n))
		return int(n)
	case float64:
		counterMap.Store(key, int(n))
		return int(n)
	default:
		log.Printf("opencode: counter type mismatch for key %s, got %T, reset to 0", key, v)
		counterMap.Store(key, 0)
		return 0
	}
}

// incrementCounter adds delta to a counter with safe initialization.
func (c *Client) incrementCounter(counterMap *sync.Map, key string, delta int) {
	current := c.loadCounter(counterMap, key)
	counterMap.Store(key, current+delta)
}

// GetContextUsage 获取session的上下文使用率
func (c *Client) GetContextUsage(sessionID string) float64 {
	tokens := c.GetTokenCount(sessionID)
	maxTokens := c.getMaxContextLength(sessionID)
	if maxTokens == 0 {
		return 0
	}
	return float64(tokens) / float64(maxTokens)
}

// ========== 重试机制 ==========

// isRetryableError 判断错误是否可以重试
func (c *Client) isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	for _, keyword := range c.retryConfig.RetryableErrors {
		if strings.Contains(strings.ToLower(errStr), strings.ToLower(keyword)) {
			return true
		}
	}
	return false
}

// calculateBackoff 计算指数退避延迟
func (c *Client) calculateBackoff(attempt int) time.Duration {
	delay := c.retryConfig.InitialDelay * time.Duration(1<<uint(attempt))
	if delay > c.retryConfig.MaxDelay {
		delay = c.retryConfig.MaxDelay
	}
	// 添加随机抖动防止惊群效应
	jitter := time.Duration(rand.Int63n(int64(delay / 4)))
	return delay + jitter
}

// sendPromptWithRetry 带重试的发送消息
func (c *Client) sendPromptWithRetry(ctx context.Context, sessionID string, parts []opencode.SessionPromptParamsPartUnion, model *opencode.SessionPromptParamsModel) (*opencode.SessionPromptResponse, error) {
	var lastErr error

	for attempt := 0; attempt <= c.retryConfig.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := c.calculateBackoff(attempt - 1)
			log.Printf("opencode: retry attempt %d/%d for session %s after %v delay",
				attempt, c.retryConfig.MaxRetries, sessionID[:8], delay)

			// 等待重试延迟，不检查外部context（它可能已超时）
			time.Sleep(delay)
		}

		// 为每次尝试创建独立的context，避免前一次超时影响下一次
		attemptCtx, cancel := context.WithTimeout(context.Background(), c.timeout)

		params := opencode.SessionPromptParams{
			Parts: opencode.F(parts),
		}
		if model != nil {
			params.Model = opencode.F(*model)
		}

		result, err := c.sdk.Session.Prompt(attemptCtx, sessionID, params)
		cancel()

		if err == nil {
			// 记录响应详情
			if result != nil {
				log.Printf("opencode: received response for session %s - parts: %d, message_id: %s",
					sessionID[:8], len(result.Parts), result.Info.ID)
				// 检查响应是否为空
				if len(result.Parts) == 0 {
					log.Printf("opencode: WARNING - empty response parts for session %s", sessionID[:8])
				}
			} else {
				log.Printf("opencode: WARNING - nil result for session %s", sessionID[:8])
			}
			if attempt > 0 {
				log.Printf("opencode: retry succeeded on attempt %d for session %s", attempt, sessionID[:8])
			}
			return result, nil
		}

		lastErr = err

		// 判断是否可重试
		if !c.isRetryableError(err) {
			log.Printf("opencode: non-retryable error for session %s: %v", sessionID[:8], err)
			return nil, err
		}

		log.Printf("opencode: retryable error on attempt %d for session %s: %v", attempt, sessionID[:8], err)

		// 如果是最后一次尝试前，给出特别提示
		if attempt == c.retryConfig.MaxRetries {
			log.Printf("opencode: FINAL retry attempt failed for session %s. Task may require user interaction in OpenCode UI.", sessionID[:8])
		}
	}

	return nil, fmt.Errorf("%w: %v", ErrMaxRetriesExceeded, lastErr)
}

func modelCatalogKey(providerID, modelID string) string {
	return strings.ToLower(strings.TrimSpace(providerID)) + "/" + strings.ToLower(strings.TrimSpace(modelID))
}

func (c *Client) mediaDebugf(format string, args ...interface{}) {
	if c == nil || !c.debugMediaRouting {
		return
	}
	log.Printf("opencode[media]: "+format, args...)
}

func (c *Client) refreshModelCatalog(ctx context.Context) error {
	providers, err := c.GetProviders(ctx)
	if err != nil {
		return err
	}
	log.Printf("opencode: model catalog refreshed, providers=%d", len(providers))
	return nil
}

func (c *Client) ensureModelCatalog(ctx context.Context) {
	c.modelCatalogMu.RLock()
	empty := len(c.modelCatalog) == 0
	c.modelCatalogMu.RUnlock()
	c.mediaDebugf("ensure catalog: empty=%t", empty)
	if !empty {
		return
	}
	if err := c.refreshModelCatalog(ctx); err != nil {
		log.Printf("opencode: ensure model catalog failed: %v", err)
		c.mediaDebugf("catalog refresh failed: %v", err)
	}
}

func (c *Client) updateSessionModel(sessionID, providerID, modelID string) {
	if strings.TrimSpace(sessionID) == "" || strings.TrimSpace(providerID) == "" || strings.TrimSpace(modelID) == "" {
		return
	}

	cfg := &ModelConfig{
		ProviderID:  providerID,
		ModelID:     modelID,
		LastUpdated: time.Now(),
	}
	c.sessionModel.Store(sessionID, cfg)

	if v, ok := c.modelConfig.Load(sessionID); ok {
		existing := v.(*ModelConfig)
		existing.ProviderID = providerID
		existing.ModelID = modelID
		existing.LastUpdated = time.Now()
		c.modelConfig.Store(sessionID, existing)
	}
}

func (c *Client) getCurrentSessionModel(ctx context.Context, sessionID string) (*ModelConfig, bool) {
	if v, ok := c.sessionModel.Load(sessionID); ok {
		cfg := v.(*ModelConfig)
		if cfg.ProviderID != "" && cfg.ModelID != "" {
			return cfg, true
		}
	}

	lookupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	messages, err := c.sdk.Session.Messages(lookupCtx, sessionID, opencode.SessionMessagesParams{})
	if err != nil || messages == nil || len(*messages) == 0 {
		c.defaultModelMu.RLock()
		defer c.defaultModelMu.RUnlock()
		if c.defaultModel != nil {
			return c.defaultModel, true
		}
		return nil, false
	}

	for i := len(*messages) - 1; i >= 0; i-- {
		msg := (*messages)[i].Info
		if msg.ProviderID == "" || msg.ModelID == "" {
			continue
		}
		cfg := &ModelConfig{
			ProviderID:  msg.ProviderID,
			ModelID:     msg.ModelID,
			LastUpdated: time.Now(),
		}
		c.sessionModel.Store(sessionID, cfg)
		return cfg, true
	}

	c.defaultModelMu.RLock()
	defer c.defaultModelMu.RUnlock()
	if c.defaultModel != nil {
		return c.defaultModel, true
	}
	return nil, false
}

func hasAttachmentType(att Attachment, prefix string) bool {
	m := strings.ToLower(strings.TrimSpace(att.Mime))
	return strings.HasPrefix(m, prefix)
}

func capabilitySupportsModality(cap *ModelCapability, modality string) bool {
	if cap == nil {
		return false
	}
	modality = strings.ToLower(strings.TrimSpace(modality))
	if modality == "" {
		return false
	}
	if cap.InputModalities[modality] {
		return true
	}
	if modality == "text" {
		return cap.InputModalities["input_text"] || cap.InputModalities["prompt"]
	}
	if modality == "image" {
		return cap.InputModalities["vision"] || cap.InputModalities["input_image"]
	}
	if modality == "video" {
		return cap.InputModalities["input_video"]
	}
	if modality == "audio" {
		return cap.InputModalities["voice"] || cap.InputModalities["speech"] || cap.InputModalities["input_audio"]
	}
	return false
}

func maybeVisionCapableByModelID(modelID string) bool {
	id := strings.ToLower(strings.TrimSpace(modelID))
	if id == "" {
		return false
	}
	for _, hint := range []string{"kimi", "gpt-4o", "gemini", "qwen-vl", "qvq", "claude-3-5-sonnet", "claude-3-7-sonnet", "vision", "vl"} {
		if strings.Contains(id, hint) {
			return true
		}
	}
	return false
}

func (c *Client) preprocessAttachmentsForSession(ctx context.Context, sessionID string, payload *MessagePayload, effectiveContent *string) (bool, error) {
	if payload == nil || len(payload.Attachments) == 0 {
		return false, nil
	}
	c.mediaDebugf("preprocess start: session=%s attachments=%d", sessionID[:min(8, len(sessionID))], len(payload.Attachments))

	needImage := false
	needVideo := false
	needAudio := false
	mediaAttachments := make([]Attachment, 0)
	for _, att := range payload.Attachments {
		if hasAttachmentType(att, "image/") {
			needImage = true
			mediaAttachments = append(mediaAttachments, att)
			continue
		}
		if hasAttachmentType(att, "video/") {
			needVideo = true
			mediaAttachments = append(mediaAttachments, att)
			continue
		}
		if hasAttachmentType(att, "audio/") {
			needAudio = true
			mediaAttachments = append(mediaAttachments, att)
		}
	}

	if !needImage && !needVideo && !needAudio {
		c.mediaDebugf("no image/video/audio attachments, skip preprocess")
		return false, nil
	}
	c.mediaDebugf("media detected: needImage=%t needVideo=%t needAudio=%t mediaCount=%d", needImage, needVideo, needAudio, len(mediaAttachments))

	c.ensureModelCatalog(ctx)
	recognizerModel, ok := c.selectFallbackMediaModel(needImage, needVideo, needAudio)
	if !ok {
		c.mediaDebugf("no matched recognizer model for media, skip preprocessing")
		return false, nil
	}
	c.mediaDebugf("matched recognizer model: %s/%s", recognizerModel.ProviderID, recognizerModel.ModelID)

	recognized, err := c.recognizeMediaWithModel(ctx, mediaAttachments, recognizerModel)
	if err != nil {
		c.mediaDebugf("media recognizer failed: %v", err)
		return false, err
	}
	if strings.TrimSpace(recognized) == "" {
		c.mediaDebugf("media recognizer returned empty text, keep original flow")
		return false, nil
	}

	*effectiveContent = fmt.Sprintf("[多模态预处理结果]\n%s\n\n[用户请求]\n%s", recognized, *effectiveContent)

	filtered := make([]Attachment, 0, len(payload.Attachments))
	for _, att := range payload.Attachments {
		if hasAttachmentType(att, "image/") || hasAttachmentType(att, "video/") || hasAttachmentType(att, "audio/") {
			continue
		}
		filtered = append(filtered, att)
	}
	payload.Attachments = filtered
	c.mediaDebugf("preprocess done: converted media to text, remaining attachments=%d", len(filtered))

	return true, nil
}

func (c *Client) selectFallbackMediaModel(needImage, needVideo, needAudio bool) (*ModelConfig, bool) {
	c.modelCatalogMu.RLock()
	keys := make([]string, 0, len(c.modelCatalog))
	for k := range c.modelCatalog {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Pass 1: strict capability matching based on provider metadata.
	for _, k := range keys {
		cap := c.modelCatalog[k]
		if cap == nil {
			continue
		}
		if !capabilitySupportsModality(cap, "text") {
			continue
		}
		if needImage && !capabilitySupportsModality(cap, "image") {
			continue
		}
		if needVideo && !capabilitySupportsModality(cap, "video") {
			continue
		}
		if needAudio && !capabilitySupportsModality(cap, "audio") {
			continue
		}
		cfg := &ModelConfig{ProviderID: cap.ProviderID, ModelID: cap.ModelID}
		c.mediaDebugf("selected media recognizer model: %s/%s", cfg.ProviderID, cfg.ModelID)
		c.modelCatalogMu.RUnlock()
		return cfg, true
	}

	// Pass 2: metadata-missing fallback (common in some OpenAI-compatible providers).
	// For image/video recognition, prefer known vision-capable model IDs (e.g., Kimi).
	if (needImage || needVideo) && !needAudio {
		for _, k := range keys {
			cap := c.modelCatalog[k]
			if cap == nil {
				continue
			}
			if maybeVisionCapableByModelID(cap.ModelID) {
				cfg := &ModelConfig{ProviderID: cap.ProviderID, ModelID: cap.ModelID}
				c.mediaDebugf("selected media recognizer model by heuristic: %s/%s", cfg.ProviderID, cfg.ModelID)
				c.modelCatalogMu.RUnlock()
				return cfg, true
			}
		}
	}

	c.modelCatalogMu.RUnlock()
	c.mediaDebugf("no media recognizer model found for needImage=%t needVideo=%t needAudio=%t", needImage, needVideo, needAudio)
	return nil, false
}

func (c *Client) recognizeMediaWithModel(ctx context.Context, attachments []Attachment, visionModel *ModelConfig) (string, error) {
	if visionModel == nil {
		return "", fmt.Errorf("media recognizer model is nil")
	}
	c.mediaDebugf("recognize with fallback model: %s/%s attachments=%d", visionModel.ProviderID, visionModel.ModelID, len(attachments))

	prepCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	tmpSession, err := c.sdk.Session.New(prepCtx, opencode.SessionNewParams{
		Title: opencode.F("[media-preprocess]"),
	})
	if err != nil {
		return "", fmt.Errorf("create media preprocess session: %w", err)
	}
	c.mediaDebugf("media preprocess temp session created: %s", tmpSession.ID[:min(8, len(tmpSession.ID))])
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, delErr := c.sdk.Session.Delete(cleanupCtx, tmpSession.ID, opencode.SessionDeleteParams{}); delErr != nil {
			log.Printf("opencode: cleanup media preprocess session %s failed: %v", tmpSession.ID[:min(8, len(tmpSession.ID))], delErr)
		}
	}()

	parts := []opencode.SessionPromptParamsPartUnion{
		opencode.TextPartInputParam{
			Type: opencode.F(opencode.TextPartInputTypeText),
			Text: opencode.F("请识别以下媒体内容（图片/视频/语音），其中语音请转写关键内容；输出简洁中文摘要，重点提取可用于回答用户问题的关键信息。"),
		},
	}

	for _, att := range attachments {
		if att.URL == "" || att.Mime == "" {
			continue
		}
		parts = append(parts, opencode.FilePartInputParam{
			Type: opencode.F(opencode.FilePartInputTypeFile),
			Mime: opencode.F(att.Mime),
			URL:  opencode.F(att.URL),
		})
	}

	modelOverride := &opencode.SessionPromptParamsModel{
		ProviderID: opencode.F(visionModel.ProviderID),
		ModelID:    opencode.F(visionModel.ModelID),
	}

	resp, err := c.sendPromptWithRetry(prepCtx, tmpSession.ID, parts, modelOverride)
	if err != nil {
		return "", fmt.Errorf("media recognize with %s/%s: %w", visionModel.ProviderID, visionModel.ModelID, err)
	}
	recognized := strings.TrimSpace(extractReplyFromMessage(resp))
	c.mediaDebugf("media recognize completed: textLen=%d", len(recognized))

	return recognized, nil
}

// sendPromptAsync 调用 OpenCode 的 prompt_async 接口，立即返回，由事件流提供结果
func (c *Client) sendPromptAsync(ctx context.Context, sessionID string, parts []opencode.SessionPromptParamsPartUnion, model *opencode.SessionPromptParamsModel) error {
	if c.endpoint == "" {
		return fmt.Errorf("opencode: prompt_async unavailable: missing endpoint")
	}

	params := opencode.SessionPromptParams{
		Parts: opencode.F(parts),
	}
	if model != nil {
		params.Model = opencode.F(*model)
	}
	if c.directory != "" {
		params.Directory = opencode.F(c.directory)
	}

	body, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("opencode: prompt_async marshal: %w", err)
	}

	promptURL, err := url.JoinPath(c.endpoint, "session", sessionID, "prompt_async")
	if err != nil {
		promptURL = fmt.Sprintf("%s/session/%s/prompt_async", strings.TrimRight(c.endpoint, "/"), sessionID)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, promptURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("opencode: prompt_async request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuthHeaders(req.Header)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("opencode: prompt_async do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusAccepted {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("opencode: prompt_async unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	log.Printf("opencode: prompt_async accepted for session %s (status=%d)", sessionID[:8], resp.StatusCode)
	return nil
}

// ========== 请求去重 ==========

// generateRequestHash 生成请求的唯一hash
func generateRequestHash(payload MessagePayload) string {
	data := fmt.Sprintf("%s|%s|%s|%s", payload.Channel, payload.UserID, payload.ThreadID, payload.Content)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:16]) // 使用前16字节
}

// checkAndMarkRequest 检查请求是否重复，如果不重复则标记为处理中
func (c *Client) checkAndMarkRequest(hash string) (*RequestRecord, bool) {
	now := time.Now()

	// 检查是否存在
	if val, ok := c.requestCache.Load(hash); ok {
		record := val.(*RequestRecord)
		// 检查是否在时间窗口内
		if now.Sub(record.Timestamp) < RequestDeduplicationWindow {
			if record.InFlight {
				// 正在处理中，返回重复（真正的重复请求）
				log.Printf("opencode: duplicate request detected (in-flight), age: %v", now.Sub(record.Timestamp))
				return record, true
			}
			// 已完成的请求，不认为是重复（允许用户再次发送相同消息）
			// 只返回缓存的响应以加快响应速度
			log.Printf("opencode: returning cached response (request completed %v ago)", now.Sub(record.Timestamp))
			return record, true
		}
		// 超出时间窗口，可以重新处理
	}

	// 标记为处理中
	record := &RequestRecord{
		Hash:      hash,
		Timestamp: now,
		InFlight:  true,
	}
	c.requestCache.Store(hash, record)
	return record, false
}

// completeRequest 完成请求并缓存结果
func (c *Client) completeRequest(hash string, response Response) {
	if val, ok := c.requestCache.Load(hash); ok {
		record := val.(*RequestRecord)
		record.Response = response
		record.InFlight = false
		record.Timestamp = time.Now() // 更新时间戳
		c.requestCache.Store(hash, record)
	}
}

// failRequest 标记请求失败
func (c *Client) failRequest(hash string) {
	c.requestCache.Delete(hash)
}

// StorePendingQuestion stores a question for later answering
func (c *Client) StorePendingQuestion(q *Question) {
	c.pendingQuestions.Store(q.ID, q)
}

// GetPendingQuestion retrieves a pending question
func (c *Client) GetPendingQuestion(questionID string) (*Question, bool) {
	val, ok := c.pendingQuestions.Load(questionID)
	if !ok {
		return nil, false
	}
	return val.(*Question), true
}

// GetLatestPendingPermission 获取指定 session 最近的待处理权限请求
// 如果 sessionID 为空，返回任意最近的权限请求
func (c *Client) GetLatestPendingPermission(sessionID string) (*Question, bool) {
	var latest *Question
	c.pendingQuestions.Range(func(key, value interface{}) bool {
		q := value.(*Question)
		// 只返回权限请求（以 per_ 开头）
		if !strings.HasPrefix(q.ID, "per_") {
			return true
		}
		// 如果指定了 sessionID，只返回该 session 的
		if sessionID != "" && q.SessionID != sessionID {
			return true
		}
		// 找最近创建的
		if latest == nil || q.CreatedAt.After(latest.CreatedAt) {
			latest = q
		}
		return true
	})
	if latest != nil {
		return latest, true
	}
	return nil, false
}

// GetLatestPendingQuestion 获取指定 session 最近的待处理问题（非权限请求）
// 如果 sessionID 为空，返回任意最近的问题
func (c *Client) GetLatestPendingQuestion(sessionID string) (*Question, bool) {
	var latest *Question
	c.pendingQuestions.Range(func(key, value interface{}) bool {
		q := value.(*Question)
		// 排除权限请求（以 per_ 开头）
		if strings.HasPrefix(q.ID, "per_") {
			return true
		}
		// 如果指定了 sessionID，只返回该 session 的
		if sessionID != "" && q.SessionID != sessionID {
			return true
		}
		// 找最近创建的
		if latest == nil || q.CreatedAt.After(latest.CreatedAt) {
			latest = q
		}
		return true
	})
	if latest != nil {
		return latest, true
	}
	return nil, false
}

// DeletePendingQuestion removes a pending question
func (c *Client) DeletePendingQuestion(questionID string) {
	c.pendingQuestions.Delete(questionID)
}

// AnswerQuestion submits an answer to a pending question or permission request
func (c *Client) AnswerQuestion(ctx context.Context, questionID string, answer string) error {
	q, ok := c.GetPendingQuestion(questionID)
	if !ok {
		return fmt.Errorf("question not found: %s", questionID)
	}

	if c.endpoint == "" {
		return fmt.Errorf("opencode: answer question unavailable: missing endpoint")
	}

	// 判断是权限请求还是普通问题
	if strings.HasPrefix(questionID, "per_") {
		return c.answerPermission(ctx, q, answer)
	}

	return c.answerNormalQuestion(ctx, q, answer)
}

// answerPermission 回答权限请求
func (c *Client) answerPermission(ctx context.Context, q *Question, answer string) error {
	// 解析用户选择：允许、拒绝、始终允许（支持去标点与模糊匹配）
	response, responseStr, ok := parsePermissionAnswer(answer)
	if !ok {
		return fmt.Errorf("无效的回复: %s (回复 '允许'、'拒绝' 或 '始终允许')", answer)
	}

	log.Printf("opencode: answering permission - ID=%s, sessionID=%s, directory=%s (from question), responseStr=%s",
		q.ID, q.SessionID, q.Directory, responseStr)

	// 优先使用 HTTP API（与 Python 一致），SDK 作为备选
	// 原因：SDK 可能存在问题导致 OpenCode 不继续处理
	directory := q.Directory
	if directory == "" {
		directory = c.directory // 回退到 client 的默认 directory
	}

	log.Printf("opencode: trying HTTP API first for permission respond")
	if err := c.answerPermissionViaHTTP(ctx, q, responseStr); err != nil {
		log.Printf("opencode: HTTP API failed: %v, trying SDK", err)

		// 回退到 SDK
		result, err := c.sdk.Session.Permissions.Respond(ctx, q.SessionID, q.ID, opencode.SessionPermissionRespondParams{
			Response:  opencode.F(response),
			Directory: opencode.F(directory),
		})
		if err != nil {
			return fmt.Errorf("opencode: both HTTP and SDK permission respond failed: %w", err)
		}

		if result != nil {
			log.Printf("opencode: SDK permission respond succeeded, result=%v", *result)
		} else {
			log.Printf("opencode: SDK permission respond succeeded, result=nil")
		}
	} else {
		log.Printf("opencode: HTTP API permission respond succeeded")
	}

	c.DeletePendingQuestion(q.ID)
	log.Printf("opencode: answered permission %s for session %s (response=%s)", q.ID, q.SessionID[:8], response)
	return nil
}

// parsePermissionAnswer 解析权限请求回复，容错处理语音识别中的标点/空格噪音
func parsePermissionAnswer(answer string) (opencode.SessionPermissionRespondParamsResponse, string, bool) {
	normalized := normalizePermissionAnswer(answer)
	if normalized == "" {
		return "", "", false
	}

	allowTokens := []string{"1", "allow", "yes", "允许", "同意", "确认", "ok", "okay", "y", "可以", "行"}
	rejectTokens := []string{"2", "deny", "no", "拒绝", "不同意", "取消", "n"}
	alwaysTokens := []string{"3", "always", "始终允许", "始终", "一直允许", "总是允许"}

	if containsAnyToken(normalized, alwaysTokens) {
		return opencode.SessionPermissionRespondParamsResponseAlways, "always", true
	}
	if containsAnyToken(normalized, rejectTokens) {
		return opencode.SessionPermissionRespondParamsResponseReject, "reject", true
	}
	if containsAnyToken(normalized, allowTokens) {
		return opencode.SessionPermissionRespondParamsResponseOnce, "once", true
	}

	// 兜底：先判断明确否定，再判断允许，避免“不允许”被误判为允许
	if strings.Contains(normalized, "不允许") || strings.Contains(normalized, "拒绝") || strings.Contains(normalized, "不同意") {
		return opencode.SessionPermissionRespondParamsResponseReject, "reject", true
	}
	if strings.Contains(normalized, "始终") || strings.Contains(normalized, "always") {
		return opencode.SessionPermissionRespondParamsResponseAlways, "always", true
	}
	if strings.Contains(normalized, "允许") || strings.Contains(normalized, "同意") || strings.Contains(normalized, "确认") {
		return opencode.SessionPermissionRespondParamsResponseOnce, "once", true
	}

	return "", "", false
}

// normalizePermissionAnswer 标准化回复文本：转小写并移除空格、标点、符号
func normalizePermissionAnswer(answer string) string {
	answerLower := strings.TrimSpace(strings.ToLower(answer))
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r) {
			return -1
		}
		return r
	}, answerLower)
}

func containsAnyToken(text string, tokens []string) bool {
	for _, token := range tokens {
		if text == token || strings.Contains(text, token) {
			return true
		}
	}
	return false
}

// answerPermissionViaHTTP 直接调用 HTTP API（与 Python 版本一致）
func (c *Client) answerPermissionViaHTTP(ctx context.Context, q *Question, response string) error {
	if c.endpoint == "" {
		return fmt.Errorf("opencode: answer permission via HTTP unavailable: missing endpoint")
	}

	// 构造 URL：POST /session/{sessionID}/permissions/{permissionID}
	permissionURL := fmt.Sprintf("%s/session/%s/permissions/%s", c.endpoint, q.SessionID, q.ID)

	payload := map[string]interface{}{
		"response": response,
	}
	// 使用 Question 中保存的 directory，如果为空则使用 client 的默认 directory
	directory := q.Directory
	if directory == "" {
		directory = c.directory
	}
	// ⚠️ 注意：只在 payload 中发送 directory，不要在 query string 中重复
	if directory != "" {
		payload["directory"] = directory
	}

	log.Printf("opencode: HTTP permission API - URL=%s, payload=%+v", permissionURL, payload)

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("opencode: permission HTTP marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, permissionURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("opencode: permission HTTP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuthHeaders(req.Header)
	// 不在 query string 中发送 directory，Python 也没有这样做

	log.Printf("opencode: HTTP permission request - URL=%s, method=POST, body=%s",
		req.URL.String(), string(body))

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("opencode: permission HTTP do: %w", err)
	}
	defer resp.Body.Close()

	log.Printf("opencode: HTTP permission response - status=%d", resp.StatusCode)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("opencode: permission HTTP unexpected status %d: %s",
			resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	c.DeletePendingQuestion(q.ID)
	log.Printf("opencode: answered permission %s for session %s via HTTP API", q.ID, q.SessionID[:8])
	return nil
}

// answerNormalQuestion 回答普通问题
// API 端点: POST /question/:requestID/reply with body {"answers": [[答案1], [答案2], ...]}
// answers 是一个二维数组：每个子数组对应一个 question 的答案（多选可以有多个元素）
//
// 答案格式支持:
// - 简单格式: "1" - 为第一个问题选择选项1
// - 多问题格式: "1;2,3;1" - 用分号分隔不同问题的答案，用逗号分隔多选答案
// - 标签格式: "纯HTML页面;API接口请求;GPU利用率,显存使用情况"
func (c *Client) answerNormalQuestion(ctx context.Context, q *Question, answer string) error {
	// 根据问题 ID 类型选择不同的回答方式
	// que_xxx: 使用 /question/:id/reply 端点
	// 其他: 使用 /session/:id/message/:messageID/answer 端点

	var answerURL string
	var payload map[string]interface{}

	if strings.HasPrefix(q.ID, "que_") {
		// 新版问题格式，使用 /question/:id/reply 端点
		answerURL = fmt.Sprintf("%s/question/%s/reply", c.endpoint, q.ID)

		// 解析答案
		var allAnswers [][]string

		// 检查是否使用分号分隔多个问题的答案
		if strings.Contains(answer, ";") {
			// 多问题格式: "1;2,3;1"
			questionAnswers := strings.Split(answer, ";")
			for idx, qa := range questionAnswers {
				var answerItems []string
				if strings.Contains(qa, ",") {
					// 多选答案
					for _, item := range strings.Split(qa, ",") {
						if trimmed := strings.TrimSpace(item); trimmed != "" {
							answerItems = append(answerItems, c.resolveAnswerOption(q, idx, trimmed))
						}
					}
				} else {
					// 单选答案
					if trimmed := strings.TrimSpace(qa); trimmed != "" {
						answerItems = []string{c.resolveAnswerOption(q, idx, trimmed)}
					}
				}
				if len(answerItems) > 0 {
					allAnswers = append(allAnswers, answerItems)
				}
			}
		} else if strings.Contains(answer, ",") {
			// 单问题多选格式: "选项1,选项2"
			var answerItems []string
			for _, item := range strings.Split(answer, ",") {
				if trimmed := strings.TrimSpace(item); trimmed != "" {
					answerItems = append(answerItems, c.resolveAnswerOption(q, 0, trimmed))
				}
			}
			allAnswers = [][]string{answerItems}
		} else {
			// 单问题单选格式: "1" 或 "选项1"
			resolved := c.resolveAnswerOption(q, 0, strings.TrimSpace(answer))
			allAnswers = [][]string{{resolved}}
		}

		payload = map[string]interface{}{
			"answers": allAnswers,
		}
		log.Printf("opencode: answering question %s via /question/reply endpoint, answers=%v", q.ID, allAnswers)
	} else {
		// 旧版格式，使用 /session/:id/message/:messageID/answer 端点
		answerURL = fmt.Sprintf("%s/session/%s/message/%s/answer", c.endpoint, q.SessionID, q.MessageID)
		payload = map[string]interface{}{
			"answer": answer,
		}
		log.Printf("opencode: answering question %s via /session/message/answer endpoint", q.ID)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("opencode: answer marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, answerURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("opencode: answer request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuthHeaders(req.Header)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("opencode: answer do: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("opencode: answer unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	c.DeletePendingQuestion(q.ID)
	log.Printf("opencode: answered question %s for session %s", q.ID, q.SessionID[:8])
	return nil
}

// resolveAnswerOption 将用户输入的答案（可能是数字索引）解析为实际选项标签
// questionIndex: 第几个子问题 (0-based)
// input: 用户输入，可能是 "1" 或 "纯HTML页面" 等
func (c *Client) resolveAnswerOption(q *Question, questionIndex int, input string) string {
	input = strings.TrimSpace(input)
	if input == "" {
		return ""
	}

	log.Printf("opencode: resolveAnswerOption - qID: %s, questionIndex: %d, input: '%s', hasQuestions: %d, hasSimpleOptions: %d",
		q.ID, questionIndex, input, len(q.Questions), len(q.Options))

	// 优先使用新版 Questions 数组
	if len(q.Questions) > 0 {
		if questionIndex >= len(q.Questions) {
			log.Printf("opencode: questionIndex %d out of range (has %d questions), returning original input", questionIndex, len(q.Questions))
			return input
		}

		qi := q.Questions[questionIndex]

		// 尝试将输入解析为数字
		if idx, err := strconv.Atoi(input); err == nil {
			// 数字索引是 1-based
			if idx >= 1 && idx <= len(qi.Options) {
				result := qi.Options[idx-1].Label
				log.Printf("opencode: converted number %d -> '%s'", idx, result)
				return result
			} else {
				log.Printf("opencode: number %d out of range (1-%d)", idx, len(qi.Options))
			}
		}

		// 如果不是有效的数字，检查是否是有效的选项标签
		for _, opt := range qi.Options {
			if strings.EqualFold(opt.Label, input) {
				log.Printf("opencode: matched option label '%s'", opt.Label)
				return opt.Label
			}
		}
	}

	// 回退到简化 Options 数组（旧格式兼容）
	if len(q.Options) > 0 {
		// 如果 questionIndex 为 0，尝试使用简化选项
		if questionIndex == 0 {
			if idx, err := strconv.Atoi(input); err == nil {
				if idx >= 1 && idx <= len(q.Options) {
					result := q.Options[idx-1]
					log.Printf("opencode: converted number %d -> '%s' (simple options)", idx, result)
					return result
				}
			}
			for _, opt := range q.Options {
				if strings.EqualFold(opt, input) {
					log.Printf("opencode: matched simple option '%s'", opt)
					return opt
				}
			}
		}
	}

	// 无法解析，返回原始输入
	log.Printf("opencode: could not resolve input '%s', returning original", input)
	return input
}

// cleanupRequestCache 定期清理过期的请求缓存
func (c *Client) cleanupRequestCache() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		c.requestCache.Range(func(key, value interface{}) bool {
			record := value.(*RequestRecord)
			if now.Sub(record.Timestamp) > RequestDeduplicationWindow {
				c.requestCache.Delete(key)
			}
			return true
		})
	}
}

func (c *Client) autoCompactMemoryLoop() {
	if !c.memoryEnabled || c.memoryAutoCompactInterval <= 0 {
		return
	}
	ticker := time.NewTicker(c.memoryAutoCompactInterval)
	defer ticker.Stop()

	for range ticker.C {
		removed, files, err := c.compactAllMemoryFiles()
		if err != nil {
			log.Printf("opencode: auto compact memory failed: %v", err)
			continue
		}
		if removed > 0 || c.memoryDebugRecall {
			log.Printf("opencode: auto compact memory done files=%d removed=%d", files, removed)
		}
	}
}

func (c *Client) compactAllMemoryFiles() (int, int, error) {
	c.memoryMu.Lock()
	defer c.memoryMu.Unlock()

	if err := os.MkdirAll(c.memoryDir, 0o755); err != nil {
		return 0, 0, err
	}

	entries, err := os.ReadDir(c.memoryDir)
	if err != nil {
		return 0, 0, err
	}

	removedTotal := 0
	fileCount := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
			continue
		}
		path := filepath.Join(c.memoryDir, entry.Name())
		store, removed, changed, compactErr := c.compactMemoryStoreByPath(path)
		if compactErr != nil {
			log.Printf("opencode: auto compact skip %s: %v", entry.Name(), compactErr)
			continue
		}
		fileCount++
		if !changed {
			continue
		}
		encoded, marshalErr := json.MarshalIndent(store, "", "  ")
		if marshalErr != nil {
			log.Printf("opencode: auto compact marshal failed %s: %v", entry.Name(), marshalErr)
			continue
		}
		if writeErr := os.WriteFile(path, encoded, 0o644); writeErr != nil {
			log.Printf("opencode: auto compact write failed %s: %v", entry.Name(), writeErr)
			continue
		}
		removedTotal += removed
	}

	return removedTotal, fileCount, nil
}

func (c *Client) compactMemoryStoreByPath(path string) (userMemoryStore, int, bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return userMemoryStore{}, 0, false, err
	}
	if strings.TrimSpace(string(b)) == "" {
		return userMemoryStore{Version: 1, Facts: []memoryFact{}}, 0, false, nil
	}

	var store userMemoryStore
	if err := json.Unmarshal(b, &store); err != nil {
		return userMemoryStore{}, 0, false, err
	}
	if store.Version == 0 {
		store.Version = 1
	}

	original := len(store.Facts)
	merged := map[string]memoryFact{}
	for _, f := range store.Facts {
		if strings.TrimSpace(f.Text) == "" {
			continue
		}
		if f.Importance <= 0 {
			f.Importance = 1
		}
		if f.Count <= 0 {
			f.Count = 1
		}
		if f.LastSeen.IsZero() {
			f.LastSeen = time.Now()
		}
		key := normalizeMemoryText(f.Text)
		if cur, ok := merged[key]; ok {
			if f.Importance > cur.Importance {
				cur.Importance = f.Importance
				cur.Category = f.Category
			}
			if f.LastSeen.After(cur.LastSeen) {
				cur.LastSeen = f.LastSeen
			}
			cur.Count += f.Count
			merged[key] = cur
			continue
		}
		merged[key] = f
	}

	facts := make([]memoryFact, 0, len(merged))
	for _, v := range merged {
		facts = append(facts, v)
	}
	sort.Slice(facts, func(i, j int) bool {
		if facts[i].Importance != facts[j].Importance {
			return facts[i].Importance > facts[j].Importance
		}
		if !facts[i].LastSeen.Equal(facts[j].LastSeen) {
			return facts[i].LastSeen.After(facts[j].LastSeen)
		}
		return facts[i].Count > facts[j].Count
	})
	if c.memoryMaxFacts > 0 && len(facts) > c.memoryMaxFacts {
		facts = facts[:c.memoryMaxFacts]
	}

	store.Facts = facts
	removed := original - len(facts)
	changed := removed > 0 || original != len(merged)
	return store, max(0, removed), changed, nil
}

// ========== Skill提示 ==========

// refreshSkillCache 刷新可用skill缓存
func (c *Client) refreshSkillCache(ctx context.Context) {
	agents, err := c.ListAgents(ctx)
	if err != nil {
		log.Printf("opencode: failed to refresh skill cache: %v", err)
		return
	}

	c.skillCacheMu.Lock()
	defer c.skillCacheMu.Unlock()

	c.skillHintCache = make([]string, 0, len(agents))
	for _, agent := range agents {
		hint := agent.Name
		if agent.Description != "" {
			hint = fmt.Sprintf("%s (%s)", agent.Name, agent.Description)
		}
		c.skillHintCache = append(c.skillHintCache, hint)
	}

	log.Printf("opencode: refreshed skill cache with %d skills", len(c.skillHintCache))
}

// getSkillHint 获取skill提示文本
func (c *Client) getSkillHint() string {
	c.skillCacheMu.RLock()
	defer c.skillCacheMu.RUnlock()

	if len(c.skillHintCache) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("\n\n[可用技能提示] 如果需要，你可以使用以下技能：")
	for _, skill := range c.skillHintCache {
		sb.WriteString("\n- ")
		sb.WriteString(skill)
	}
	sb.WriteString("\n请在需要时主动调用这些技能。")

	return sb.String()
}

// enhanceContentWithSkillHint 在消息内容中添加skill提示
func (c *Client) enhanceContentWithSkillHint(content string, sessionID string) string {
	if !c.enableSkillHint {
		return content
	}

	// 检查是否需要刷新缓存
	c.skillCacheMu.RLock()
	needRefresh := len(c.skillHintCache) == 0
	c.skillCacheMu.RUnlock()

	if needRefresh {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		c.refreshSkillCache(ctx)
		cancel()
	}

	hint := c.getSkillHint()
	if hint == "" {
		return content
	}

	// 只在session的前几条消息添加提示，避免冗余
	msgCount := c.GetMessageCount(sessionID)
	if msgCount > 3 {
		return content
	}

	return content + hint
}

// RefreshSkills 手动刷新skill缓存
func (c *Client) RefreshSkills(ctx context.Context) error {
	c.refreshSkillCache(ctx)
	return nil
}

// SetSkillHintEnabled 设置是否启用skill提示
func (c *Client) SetSkillHintEnabled(enabled bool) {
	c.enableSkillHint = enabled
}
