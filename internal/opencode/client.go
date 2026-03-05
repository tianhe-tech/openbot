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
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
)

// ErrEmptyPayload indicates the caller attempted to send an empty message.
var ErrEmptyPayload = errors.New("opencode: empty payload")

// ErrDuplicateRequest indicates a duplicate request was detected.
var ErrDuplicateRequest = errors.New("opencode: duplicate request detected")

// ErrMaxRetriesExceeded indicates all retry attempts failed.
var ErrMaxRetriesExceeded = errors.New("opencode: max retries exceeded")

const (
	// ContextUsageThreshold 上下文使用率达到此阈值时创建新session (默认80%)
	ContextUsageThreshold = 0.8
	// SummaryThreshold 上下文使用率达到此阈值时开始总结 (默认60%)
	SummaryThreshold = 0.6
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
}

// StreamCallback defines a callback for streaming responses.
type StreamCallback func(chunk string) error

// EventHandler defines a callback for incoming OpenCode events.
type EventHandler func(ctx context.Context, event *opencode.EventListResponse) error

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
	sdk               *opencode.Client
	endpoint          string
	apiKey            string
	basicAuthHeader   string
	httpClient        *http.Client
	eventHandlers     []EventHandler
	eventListenerMu   sync.RWMutex
	sessionHandlers   sync.Map     // map[sessionID]EventHandler for fast lookup
	activeHandlers    sync.Map     // map[sessionID]*StreamingSessionHandler for todo/diff access
	messageToSession  sync.Map     // map[messageID]sessionID for events with only messageID
	messageRoles      sync.Map     // map[messageID]role ("user"/"assistant") for filtering user message delta events
	sessionMu         sync.RWMutex // 用于保护 session 相关操作
	sessions          sync.Map     // map[threadID]sessionID
	sessionLocks      sync.Map     // map[threadID]*sync.Mutex for preventing concurrent session operations
	sessionsMu        sync.RWMutex // 保护 sessions 的读写
	messageCount      sync.Map     // map[sessionID]int tracks messages per session
	tokenCount        sync.Map     // map[sessionID]int tracks estimated tokens per session
	sessionSummary    sync.Map     // map[sessionID]string stores session summaries
	modelConfig       sync.Map     // map[sessionID]*ModelConfig caches model config per session
	sessionModel      sync.Map     // map[sessionID]*ModelConfig tracks latest provider/model seen in assistant replies
	sessionOverride   sync.Map     // map[sessionID]*ModelConfig stores user-selected model via /model
	requestCache      sync.Map     // map[requestHash]*RequestRecord 请求去重缓存
	runningSessions   sync.Map     // map[sessionID]bool 跟踪正在运行的session
	pendingQuestions  sync.Map     // map[questionID]*Question 待回答的问题
	modelCatalogMu    sync.RWMutex
	modelCatalog      map[string]*ModelCapability // key: providerID/modelID
	defaultModelMu    sync.RWMutex
	defaultModel      *ModelConfig
	directory         string
	timeout           time.Duration // 默认超时时间
	retryConfig       RetryConfig   // 重试配置
	debugMediaRouting bool          // 是否启用多模态路由调试日志
	enableSkillHint   bool          // 是否在消息中添加skill提示
	skillHintCache    []string      // 缓存的可用skill列表
	skillCacheMu      sync.RWMutex
	lastHealthCheck   time.Time    // 最后一次健康检查时间
	isHealthy         bool         // OpenCode server是否健康
	healthCheckMu     sync.RWMutex // 保护健康状态
	thinkingEnabled   atomic.Bool  // 是否输出 reasoning/thinking 内容
	finalOnlyEnabled  atomic.Bool  // 是否仅在结束时发送最终回复
	stepEnabled       atomic.Bool  // 是否显示 step-start/step-finish 中间步骤

}

// Option mutates a client during construction.
type Option func(*Client)

// WithDirectory sets the working directory for sessions.
func WithDirectory(dir string) Option {
	return func(c *Client) {
		c.directory = dir
	}
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
		enableSkillHint:   false, // 默认禁用skill提示
		debugMediaRouting: parseEnvBool("OPENBOT_DEBUG_MEDIA_ROUTING"),
		modelCatalog:      make(map[string]*ModelCapability),
		isHealthy:         false,       // 初始状态未知
		lastHealthCheck:   time.Time{}, // 未检查过
	}

	for _, opt := range opts {
		opt(client)
	}

	client.thinkingEnabled.Store(parseEnvBool("OPENBOT_SHOW_THINKING"))
	log.Printf("opencode: thinking output enabled=%t (env OPENBOT_SHOW_THINKING)", client.thinkingEnabled.Load())
	client.finalOnlyEnabled.Store(parseEnvBool("OPENBOT_FINAL_ONLY"))
	log.Printf("opencode: final-only output enabled=%t (env OPENBOT_FINAL_ONLY)", client.finalOnlyEnabled.Load())
	client.stepEnabled.Store(parseEnvBool("OPENBOT_SHOW_STEPS"))
	log.Printf("opencode: step output enabled=%t (env OPENBOT_SHOW_STEPS)", client.stepEnabled.Load())

	// 启动后台清理协程
	go client.cleanupRequestCache()

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
	threadLock := c.getThreadLock(payload.ThreadID)
	threadLock.Lock()
	sessionID := payload.SessionID

	// 🔍 诊断日志：记录 session 查找请求
	log.Printf("opencode: session lookup - channel=%s, userID=%s, threadID=%s, requestingSessionID=%s",
		payload.Channel, payload.UserID, payload.ThreadID, sessionID)

	if sessionID == "" && payload.ThreadID != "" {
		if sid, ok := c.sessions.Load(payload.ThreadID); ok {
			foundSessionID := sid.(string)

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
			c.sessions.Store(payload.ThreadID, sessionID)
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
				c.sessions.Delete(payload.ThreadID)
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
				c.sessions.Store(payload.ThreadID, sessionID)
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

		// 检查是否需要总结或创建新session
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

		// 如果上下文使用率超过阈值，创建新session
		if contextUsage >= ContextUsageThreshold {
			log.Printf("opencode: session %s context usage %.1f%% >= threshold %.1f%%, creating new session",
				sessionID[:8], contextUsage*100, ContextUsageThreshold*100)

			// 总结旧session
			if err := c.SummarizeSession(ctx, sessionID); err != nil {
				log.Printf("opencode: failed to summarize session %s: %v", sessionID, err)
			}

			// 获取总结内容
			summary := ""
			if sum, ok := c.sessionSummary.Load(sessionID); ok {
				summary = sum.(string)
			}

			// 创建新session，标题包含历史信息
			title := fmt.Sprintf("%s-%s-续", payload.Channel, payload.UserID)
			if summary != "" {
				title = fmt.Sprintf("%s-%s (之前讨论: %s)", payload.Channel, payload.UserID, truncateString(summary, 50))
			}

			newSession, err := c.sdk.Session.New(ctx, opencode.SessionNewParams{
				Title: opencode.F(title),
			})
			if err != nil {
				log.Printf("opencode: failed to create new session: %v, continuing with current", err)
			} else {
				sessionID = newSession.ID
				if payload.ThreadID != "" {
					c.sessions.Store(payload.ThreadID, sessionID)
				}
				c.messageCount.Store(sessionID, 0)
				c.tokenCount.Store(sessionID, 0)

				// 获取新session的模型配置
				go c.fetchAndCacheModelConfig(context.Background(), sessionID)

				// 如果有总结，将总结作为系统消息添加到新session的上下文
				if summary != "" {
					contextMsg := fmt.Sprintf("[上一轮对话总结]: %s\n\n[用户新消息]: %s", summary, payload.Content)
					payload.Content = contextMsg
					estimatedMsgTokens = estimateTokens(contextMsg) // 重新估算
					log.Printf("opencode: created new session %s with context from previous session", sessionID)
				} else {
					log.Printf("opencode: created new session %s for thread %s", sessionID, payload.ThreadID)
				}
			}
		} else if contextUsage >= SummaryThreshold && msgCount%5 == 0 {
			// 在达到总结阈值后，每5条消息尝试总结一次（后台异步）
			log.Printf("opencode: session %s context usage %.1f%% >= summary threshold %.1f%%, scheduling background summary",
				sessionID[:8], contextUsage*100, SummaryThreshold*100)
			go func(sid string) {
				sumCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := c.SummarizeSession(sumCtx, sid); err != nil {
					log.Printf("opencode: background summarization failed for session %s: %v", sid, err)
				}
			}(sessionID)
		}
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
		c.runningSessions.Store(sessionID, true)
		if err := c.sendPromptAsync(ctx, sessionID, parts, mainModelOverride); err != nil {
			c.runningSessions.Delete(sessionID)
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

		c.completeRequest(requestHash, response)
		return response, nil
	}

	// ========== 使用重试机制发送消息 ==========
	// 标记session为运行状态
	c.runningSessions.Store(sessionID, true)

	result, err := c.sendPromptWithRetry(ctx, sessionID, parts, mainModelOverride)

	// 清除运行状态
	c.runningSessions.Delete(sessionID)

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

	// ========== 缓存成功响应用于去重 ==========
	c.completeRequest(requestHash, response)

	return response, nil
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
	c.runningSessions.Delete(sessionID)
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, forkURL, http.NoBody)
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
	c.runningSessions.Delete(sessionID)
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

// IsSessionRunning checks if a session is currently running
func (c *Client) IsSessionRunning(sessionID string) bool {
	val, ok := c.runningSessions.Load(sessionID)
	if !ok {
		return false
	}
	return val.(bool)
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

		// 清除旧的 session 缓存（server 重启后 session 可能失效）
		if consecutiveFailures > 0 {
			log.Printf("opencode: 🔄 server reconnected after %d failures, invalidating stale session cache", consecutiveFailures)
			c.invalidateStaleSessions(ctx)
		}

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
					var msgInfo struct {
						Properties struct {
							Message struct {
								ID   string `json:"id"`
								Role string `json:"role"`
							} `json:"message"`
						} `json:"properties"`
					}
					if json.Unmarshal([]byte(raw), &msgInfo) == nil {
						if msgID := msgInfo.Properties.Message.ID; msgID != "" && msgInfo.Properties.Message.Role != "" {
							c.messageRoles.Store(msgID, msgInfo.Properties.Message.Role)
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
						} else if probe.Properties.MessageID != "" {
							// message.part.delta — look up sessionID via reverse map
							if sid, ok := c.messageToSession.Load(probe.Properties.MessageID); ok {
								sessionID = sid.(string)
							}
							// If still no sessionID, broadcast — but only for assistant messages
							// with a KNOWN role. Unknown-role deltas are likely from old/other
							// sessions being replayed; broadcasting them would echo stale content.
							if sessionID == "" {
								role, roleKnown := c.messageRoles.Load(probe.Properties.MessageID)
								if !roleKnown {
									log.Printf("opencode: dropping message.part.delta with unknown messageID (likely old session replay)")
								} else if role.(string) == "user" {
									log.Printf("opencode: skipping user message.part.delta broadcast (msgID=%s)", probe.Properties.MessageID[:min(8, len(probe.Properties.MessageID))])
								} else {
									// Known assistant message — broadcast to all session handlers
									dispatched := 0
									c.sessionHandlers.Range(func(k, v interface{}) bool {
										if err := v.(EventHandler)(ctx, &event); err != nil {
											log.Printf("opencode: message.part.delta broadcast error (session %v): %v", k, err)
										}
										dispatched++
										return true
									})
									if dispatched == 0 {
										log.Printf("opencode: message.part.delta broadcast - no session handlers registered")
									}
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

			// Dispatch to the specific session handler (when sessionID is known)
			if sessionID != "" {
				if handler, ok := c.sessionHandlers.Load(sessionID); ok {
					if err := handler.(EventHandler)(ctx, &event); err != nil {
						log.Printf("opencode: session handler error for %s: %v", sessionID[:8], err)
					}
				} else {
					log.Printf("opencode: no session handler found for %s", sessionID[:8])
					c.sessionHandlers.Range(func(k, v interface{}) bool {
						log.Printf("opencode:   registered handler: %v", k)
						return true
					})
				}
			}

			// Always dispatch to global handlers
			c.eventListenerMu.RLock()
			handlers := c.eventHandlers
			c.eventListenerMu.RUnlock()

			if len(handlers) > 0 {
				log.Printf("opencode: dispatching to %d global handlers", len(handlers))
			}

			for _, handler := range handlers {
				if err := handler(ctx, &event); err != nil {
					log.Printf("opencode: global event handler error: %v", err)
				}
			}
		}

		stream.Close()

		log.Printf("opencode: event stream ended, total events processed: %d", eventCount)

		if err := stream.Err(); err != nil {
			log.Printf("opencode: event stream error: %v", err)
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

// RegisterEventHandler adds a new event handler dynamically.
func (c *Client) RegisterEventHandler(handler EventHandler) {
	c.eventListenerMu.Lock()
	defer c.eventListenerMu.Unlock()
	c.eventHandlers = append(c.eventHandlers, handler)
}

// RegisterSessionHandler registers an event handler tied to a specific session.
// This allows fast lookup and prevents unnecessary processing of unrelated events.
func (c *Client) RegisterSessionHandler(sessionID string, handler EventHandler) {
	c.sessionHandlers.Store(sessionID, handler)
}

// UnregisterSessionHandler removes an event handler for a specific session.
func (c *Client) UnregisterSessionHandler(sessionID string) {
	c.sessionHandlers.Delete(sessionID)
	c.activeHandlers.Delete(sessionID)
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

// getThreadLock gets or creates a lock for a specific thread to prevent concurrent operations.
func (c *Client) getThreadLock(threadID string) *sync.Mutex {
	if threadID == "" {
		return &sync.Mutex{} // Return a new mutex for single-use
	}

	lock, _ := c.sessionLocks.LoadOrStore(threadID, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

// SummarizeSession 总结一个session的对话内容
func (c *Client) SummarizeSession(ctx context.Context, sessionID string) error {
	if !c.Ready() {
		return fmt.Errorf("opencode: client not configured")
	}

	// 检查是否已有总结
	if _, exists := c.sessionSummary.Load(sessionID); exists {
		return nil // 已经总结过了
	}

	log.Printf("opencode: summarizing session %s", sessionID)

	// 调用OpenCode的summarize API
	_, err := c.sdk.Session.Summarize(ctx, sessionID, opencode.SessionSummarizeParams{})
	if err != nil {
		return fmt.Errorf("opencode: summarize session: %w", err)
	}

	// 获取session详情以获取总结
	session, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return fmt.Errorf("opencode: get session after summarize: %w", err)
	}

	// 提取总结内容（从session的messages中查找summary类型的消息）
	summary := extractSummaryFromSession(session)
	if summary != "" {
		c.sessionSummary.Store(sessionID, summary)
		log.Printf("opencode: session %s summarized successfully", sessionID)
	}

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

	// 1. 先确定sessionID（可能需要创建新session）
	threadLock := c.getThreadLock(payload.ThreadID)
	threadLock.Lock()
	sessionID := payload.SessionID
	if sessionID == "" && payload.ThreadID != "" {
		if sid, ok := c.sessions.Load(payload.ThreadID); ok {
			sessionID = sid.(string)

			// 验证 session 是否仍然有效
			checkCtx, checkCancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, checkErr := c.GetSession(checkCtx, sessionID)
			checkCancel()
			if checkErr != nil {
				log.Printf("opencode: ⚠️ streaming session %s is stale (err: %v), will create new",
					sessionID[:min(8, len(sessionID))], checkErr)
				c.sessions.Delete(payload.ThreadID)
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
			c.sessions.Store(payload.ThreadID, sessionID)
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

	// 3. 创建StreamingSessionHandler并注册
	handler := NewStreamingSessionHandler(sessionID, callback, func() {
		c.runningSessions.Delete(sessionID)
		c.UnregisterSessionHandler(sessionID)
	}, c, c, c.IsThinkingEnabled(), c.IsStepEnabled())
	c.RegisterSessionHandler(sessionID, handler.HandleEvent)
	c.activeHandlers.Store(sessionID, handler)
	log.Printf("opencode: registered streaming handler for session %s", sessionID[:8])

	// 4. 使用goroutine异步发送消息
	responseChan := make(chan Response, 1)
	errorChan := make(chan error, 1)

	go func() {
		response, err := c.SendMessage(ctx, payload)
		if err != nil {
			errorChan <- err
			return
		}
		responseChan <- response
	}()

	// 4. 定时检查完成状态（不再发送进度消息）
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	isAsyncMode := false
	var asyncResponse Response
	idleCheckCount := 0 // 空闲检测计数器

	for {
		select {
		case <-ctx.Done():
			return Response{}, ctx.Err()

		case err := <-errorChan:
			return Response{}, err

		case response := <-responseChan:
			// 如果是Async模式（reply为空），标记并继续等待SSE事件
			if response.Reply == "" {
				log.Printf("opencode: streaming async mode for session %s, waiting for SSE events", sessionID[:8])
				isAsyncMode = true
				asyncResponse = response
				continue
			}

			// 同步模式：检查是否通过streaming handler已经发送了内容
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

		case <-ticker.C:
			// 如果在async模式且handler已完成，返回结果
			if isAsyncMode && handler.IsCompleted() {
				log.Printf("opencode: ✅ async streaming completed via SSE for session %s (contentSent=%t, lastContentLen=%d)",
					sessionID[:8], handler.HasSentContent(), len(handler.GetLastContent()))
				// 注意：不填充 asyncResponse.Reply，让调用者从 fullReply 获取内容
				return asyncResponse, nil
			}

			// 检查最后一次事件时间
			lastEventTime, lastEventType := handler.GetLastEventInfo()
			timeSinceLastEvent := time.Since(lastEventTime)
			hasSentContent := handler.HasSentContent()
			hasStepFinish := handler.HasReceivedStepFinish()
			stepFinishTime := handler.GetStepFinishTime()
			isCompleted := handler.IsCompleted()

			log.Printf("opencode: 🔍 ticker check - session=%s, isAsync=%t, isCompleted=%t, hasSent=%t, hasStepFinish=%t, lastEvent=%v ago (type=%s), idleCount=%d",
				sessionID[:8], isAsyncMode, isCompleted, hasSentContent, hasStepFinish, timeSinceLastEvent, lastEventType, idleCheckCount)

			// 如果收到了 step-finish 事件且已发送内容，5秒后没有新事件就认为完成
			// (step-finish 通常标志着模型输出完成，后续应该很快有 session.idle)
			if isAsyncMode && hasStepFinish && hasSentContent && !stepFinishTime.IsZero() {
				timeSinceStepFinish := time.Since(stepFinishTime)
				if timeSinceStepFinish > 5*time.Second {
					log.Printf("opencode: 🏁 received step-finish %v ago (has sent content), treating as completed for session %s",
						timeSinceStepFinish, sessionID[:8])
					return asyncResponse, nil
				}
			}

			// 如果已发送内容且超过30秒无新事件，认为可能完成
			// （从2分钟缩短到30秒，更快响应）
			if isAsyncMode && hasSentContent && timeSinceLastEvent > 30*time.Second {
				log.Printf("opencode: ⏱️ streaming idle for %v (has sent content), treating as completed for session %s",
					timeSinceLastEvent, sessionID[:8])
				return asyncResponse, nil
			}

			// 如果超过1分钟无任何事件（即使没发送内容），也认为完成
			// 这处理 OpenCode 不发送完成事件的情况
			if isAsyncMode && timeSinceLastEvent > 1*time.Minute {
				log.Printf("opencode: ⏱️ streaming timeout (no events for %v, hasSent=%t), treating as completed for session %s",
					timeSinceLastEvent, hasSentContent, sessionID[:8])
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
