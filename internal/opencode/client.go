package opencode

import (
	"bufio"
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
	"sort"
	"strconv"
	"strings"
	"sync"
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
	// ProviderCatalogCacheTTL provider/model 目录缓存时长
	ProviderCatalogCacheTTL = 30 * time.Second
)

// Response represents the minimal data we expect back from OpenCode.
type Response struct {
	Reply     string                 `json:"reply"`
	Trace     string                 `json:"trace_id"`
	SessionID string                 `json:"session_id"`
	MessageID string                 `json:"message_id"`
	Raw       map[string]interface{} `json:"raw,omitempty"`
}

// Attachment represents a media attachment encoded as data URI.
type Attachment struct {
	Mime     string `json:"mime"`
	URL      string `json:"url"`
	Filename string `json:"filename,omitempty"`
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
	Attachments []Attachment      `json:"attachments,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
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

// TodoItem mirrors the todo.updated payload.
type TodoItem struct {
	ID       string `json:"id"`
	Task     string `json:"task"`
	Content  string `json:"content"`
	Status   string `json:"status"`
	Priority string `json:"priority"`
}

func (t TodoItem) Text() string {
	if strings.TrimSpace(t.Content) != "" {
		return t.Content
	}
	return t.Task
}

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

// FileDiff mirrors the session.diff payload.
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
	sdk              *opencode.Client
	endpoint         string
	apiKey           string
	httpClient       *http.Client
	eventHandlers    []EventHandler
	eventListenerMu  sync.RWMutex
	sessionHandlers  sync.Map     // map[sessionID]EventHandler for fast lookup
	messageToSession sync.Map     // map[messageID]sessionID for events with only messageID
	messageRoles     sync.Map     // map[messageID]role("user"/"assistant")
	sessionMu        sync.RWMutex // 用于保护 session 相关操作
	sessions         sync.Map     // map[threadID]sessionID
	sessionLocks     sync.Map     // map[threadID]*sync.Mutex for preventing concurrent session operations
	sessionsMu       sync.RWMutex // 保护 sessions 的读写
	messageCount     sync.Map     // map[sessionID]int tracks messages per session
	tokenCount       sync.Map     // map[sessionID]int tracks estimated tokens per session
	sessionSummary   sync.Map     // map[sessionID]string stores session summaries
	modelConfig      sync.Map     // map[sessionID]*ModelConfig caches model config per session
	requestCache     sync.Map     // map[requestHash]*RequestRecord 请求去重缓存
	runningSessions  sync.Map     // map[sessionID]bool 跟踪正在运行的session
	pendingQuestions sync.Map     // map[questionID]*Question 待回答的问题
	todoCache        sync.Map     // map[sessionID][]TodoItem
	diffCache        sync.Map     // map[sessionID][]FileDiff
	userMemory       sync.Map     // map[channel:userID][]UserMemoryFact
	directory        string
	timeout          time.Duration // 默认超时时间
	retryConfig      RetryConfig   // 重试配置
	devCoreEnabled   bool          // 是否启用开发助手内核提示词注入
	devCorePrompt    string        // 开发助手内核提示词
	enableSkillHint  bool          // 是否在消息中添加skill提示
	skillHintCache   []string      // 缓存的可用skill列表
	skillCacheMu     sync.RWMutex
	showThinking     bool
	finalOnly        bool
	showSteps        bool
	modeMu           sync.RWMutex
	lastHealthCheck  time.Time    // 最后一次健康检查时间
	isHealthy        bool         // OpenCode server是否健康
	healthCheckMu    sync.RWMutex // 保护健康状态
	modelOverride    sync.Map     // map[sessionID]opencode.SessionPromptParamsModel
	providerCacheMu  sync.RWMutex
	providerCacheAt  time.Time
	providerCache    []Provider
	capabilityCache  map[string]modelCapability // key: lower(provider/model)
	defaultModelHint *opencode.SessionPromptParamsModel
	memStore         MemStoreRecorder // optional memory store (set via WithMemStore)
}

// Option mutates a client during construction.
type Option func(*Client)

const defaultDevCorePrompt = "你是 Dev Core。请按用户偏好输出，回答清晰、可执行、可验证。"

// UserMemoryFact is the adapter-facing memory record format.
type UserMemoryFact struct {
	Text       string `json:"text"`
	Category   string `json:"category"`
	Importance int    `json:"importance"`
}

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

// MemStoreRecorder is the minimal interface the Client calls to record and recall conversations.
// Implemented by *memstore.Store via an adapter wrapper in main.go.
type MemStoreRecorder interface {
	// RecordConversation persists one completed request/response turn.
	RecordConversation(adapter, userID, request, response string)
	// InjectRecallContext returns a context preamble to prepend to the user message when a
	// recall intent is detected; returns "" when no injection is needed.
	InjectRecallContext(request, adapter, userID string) string
}

// WithMemStore attaches an optional memory store that records every successful
// conversation turn (request → response) for later recall.
func WithMemStore(ms MemStoreRecorder) Option {
	return func(c *Client) {
		c.memStore = ms
	}
}

// WithSkillHint enables automatic skill hint injection.
func WithSkillHint(enable bool) Option {
	return func(c *Client) {
		c.enableSkillHint = enable
	}
}

// WithDevCoreProfile configures the always-on developer profile prompt that is
// injected before each user message.
func WithDevCoreProfile(enabled bool, prompt string) Option {
	return func(c *Client) {
		c.devCoreEnabled = enabled
		c.devCorePrompt = strings.TrimSpace(prompt)
	}
}

// WithServerUnavailableHandler keeps backward compatibility with previous options API.
func WithServerUnavailableHandler(handler func(ctx context.Context, reason string) (string, error)) Option {
	return func(c *Client) {
		if handler == nil {
			return
		}
		c.eventHandlers = append(c.eventHandlers, func(ctx context.Context, event *opencode.EventListResponse) error {
			return nil
		})
	}
}

// NewClient builds a Client instance using the official OpenCode SDK.
func NewClient(endpoint, apiKey string, opts ...Option) *Client {
	client := &Client{
		sdk: opencode.NewClient(
			option.WithBaseURL(endpoint),
			// option.WithAPIKey(apiKey), // If SDK supports API key authentication
		),
		endpoint: strings.TrimRight(endpoint, "/"),
		apiKey:   apiKey,
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
		devCoreEnabled:  false,
		enableSkillHint: false,       // 默认禁用skill提示
		isHealthy:       false,       // 初始状态未知
		lastHealthCheck: time.Time{}, // 未检查过
	}

	for _, opt := range opts {
		opt(client)
	}

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
		}
	}()

	return client
}

// Ready reports if the client has enough data to operate.
func (c *Client) Ready() bool {
	return c.sdk != nil
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
		return fmt.Errorf("opencode server不可用: %w\n\n💡 请确保：\n1. OpenCode server已启动\n2. 服务地址配置正确 (%s)\n3. 网络连接正常", err, c.endpoint)
	}
	c.isHealthy = true
	c.healthCheckMu.Unlock()

	return nil
}

// IsHealthy returns the cached health status.
func (c *Client) IsHealthy() bool {
	c.healthCheckMu.RLock()
	defer c.healthCheckMu.RUnlock()
	return c.isHealthy
}

func (c *Client) applyAuthHeaders(header http.Header) {
	if header == nil {
		return
	}
	if c.apiKey != "" {
		header.Set("Authorization", "Bearer "+c.apiKey)
	}
}

func (c *Client) maybeHandleServerUnavailable(ctx context.Context, err error, source string) {
	if err == nil {
		return
	}
	log.Printf("opencode: %s unavailable: %v", source, err)

	// Best-effort health refresh for better user-facing diagnostics.
	checkCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if healthErr := c.CheckHealth(checkCtx); healthErr != nil {
		log.Printf("opencode: health check failed after %s error: %v", source, healthErr)
	}
}

// SendMessage forwards an adapter payload to OpenCode and returns its response.
// 注意：OpenCode支持两种模式：
// 1. POST /session/:id/message - 同步模式，等待响应后返回
// 2. POST /session/:id/prompt_async - 异步模式，立即返回204，通过事件流获取结果
// 对于长时间任务，应该使用异步模式+事件监听
func (c *Client) SendMessage(ctx context.Context, payload MessagePayload) (Response, error) {
	log.Printf("opencode: TRACE ENTER_SENDMESSAGE channel=%s user=%s thread=%s streaming=%t attachments=%d",
		payload.Channel,
		payload.UserID,
		payload.ThreadID,
		payload.Streaming,
		len(payload.Attachments),
	)
	log.Printf("opencode: SendMessage called (channel=%s, user=%s, thread=%s, streaming=%t, attachments=%d)",
		payload.Channel,
		payload.UserID,
		payload.ThreadID,
		payload.Streaming,
		len(payload.Attachments),
	)
	if !c.Ready() {
		return Response{}, fmt.Errorf("opencode: client not configured")
	}

	if strings.TrimSpace(payload.Content) == "" {
		return Response{}, ErrEmptyPayload
	}

	// ========== 记忆召回注入（如果用户在询问历史工作）==========
	// Save original content BEFORE injection so recording always stores the clean user message.
	originalContent := payload.Content
	if c.memStore != nil {
		if injected := c.memStore.InjectRecallContext(payload.Content, payload.Channel, payload.UserID); injected != "" {
			payload.Content = injected + payload.Content
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
			c.modelOverride.Delete(sessionID)

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
			log.Printf("opencode: stale session replaced, continue to send message with new session %s", sessionID[:min(8, len(sessionID))])
			goto sendMessage
		}

		// 检查是否需要总结或创建新session
		count, _ := c.messageCount.Load(sessionID)
		msgCount := count.(int)
		tokens, _ := c.tokenCount.Load(sessionID)
		currentTokens := tokens.(int)

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

			// 保存旧session的model override，以便迁移到新session
			oldSessionID := sessionID
			oldModelOverride, hasModelOverride := c.getSessionModelOverride(oldSessionID)

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

				// 迁移旧session的model override到新session，避免切换过model的用户压缩后回退到默认模型
				if hasModelOverride {
					c.modelOverride.Store(sessionID, oldModelOverride)
					log.Printf("opencode: carried model override %s/%s from old session %s to new session %s",
						oldModelOverride.ProviderID.Value, oldModelOverride.ModelID.Value,
						oldSessionID[:min(8, len(oldSessionID))], sessionID[:min(8, len(sessionID))])
				}

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
		Text: opencode.F(enhancedContent),
		Type: opencode.F(opencode.TextPartInputTypeText),
	})

	log.Printf("opencode: payload attachments count=%d", len(payload.Attachments))
	// Add file parts from adapter attachments so OpenCode can perform multimodal understanding.
	for idx, attachment := range payload.Attachments {
		urlValue := strings.TrimSpace(attachment.URL)
		if urlValue == "" {
			continue
		}

		mimeValue := strings.TrimSpace(attachment.Mime)
		if mimeValue == "" {
			if inferredMime := inferMimeFromDataURI(urlValue); inferredMime != "" {
				mimeValue = inferredMime
			}
		}
		if mimeValue == "" {
			mimeValue = "application/octet-stream"
		}

		filePart := opencode.FilePartInputParam{
			Type: opencode.F(opencode.FilePartInputTypeFile),
			Mime: opencode.F(mimeValue),
			URL:  opencode.F(urlValue),
		}
		if name := strings.TrimSpace(attachment.Filename); name != "" {
			filePart.Filename = opencode.F(name)
		}

		parts = append(parts, filePart)
		log.Printf("opencode: attached file part #%d for session %s (mime=%s, url_len=%d)", idx+1, sessionID[:8], mimeValue, len(urlValue))
	}

	modelOverride, modelReason := c.selectModelOverride(ctx, sessionID, payload)
	log.Printf("opencode: modelOverride selected=%t reason=%s", modelOverride != nil, modelReason)
	if modelOverride != nil {
		log.Printf("opencode: selected model %s/%s for session %s (%s)", modelOverride.ProviderID.Value, modelOverride.ModelID.Value, sessionID[:8], modelReason)
	}

	// 流式模式下默认使用异步 prompt_async；但当已选定模型覆盖时，改走同步 prompt
	// 以确保 provider/model 绑定在服务端稳定生效。
	if payload.Streaming && modelOverride == nil {
		c.runningSessions.Store(sessionID, true)
		if err := c.sendPromptAsync(ctx, sessionID, parts, modelOverride); err != nil {
			c.runningSessions.Delete(sessionID)
			c.failRequest(requestHash)
			return Response{}, fmt.Errorf("opencode: prompt_async: %w", err)
		}

		// 仅统计用户消息本身的tokens，回复在事件流中获取
		count, _ := c.messageCount.LoadOrStore(sessionID, 0)
		c.messageCount.Store(sessionID, count.(int)+1)
		estimatedMsgTokens := estimateTokens(payload.Content)
		tokens, _ := c.tokenCount.LoadOrStore(sessionID, 0)
		c.tokenCount.Store(sessionID, tokens.(int)+estimatedMsgTokens)

		response := Response{
			Reply:     "",
			SessionID: sessionID,
			MessageID: "",
			Trace:     sessionID,
		}

		c.completeRequest(requestHash, response)
		return response, nil
	}

	if payload.Streaming && modelOverride != nil {
		log.Printf("opencode: streaming request for session %s will use sync prompt to enforce model override %s/%s",
			sessionID[:min(8, len(sessionID))], modelOverride.ProviderID.Value, modelOverride.ModelID.Value)
	}

	// ========== 使用重试机制发送消息 ==========
	// 标记session为运行状态
	c.runningSessions.Store(sessionID, true)

	result, err := c.sendPromptWithRetry(ctx, sessionID, parts, modelOverride)

	// 清除运行状态
	c.runningSessions.Delete(sessionID)

	if err != nil {
		c.failRequest(requestHash)
		return Response{}, fmt.Errorf("opencode: send prompt: %w", err)
	}

	// Extract reply from assistant message
	reply := extractReplyFromMessage(result)

	// Increment message count and token count for this session
	count, _ := c.messageCount.LoadOrStore(sessionID, 0)
	c.messageCount.Store(sessionID, count.(int)+1)

	// 更新token计数（估算用户消息 + AI回复）
	estimatedMsgTokens := estimateTokens(payload.Content)
	estimatedReplyTokens := estimateTokens(reply)
	tokens, _ := c.tokenCount.LoadOrStore(sessionID, 0)
	c.tokenCount.Store(sessionID, tokens.(int)+estimatedMsgTokens+estimatedReplyTokens)

	response := Response{
		Reply:     reply,
		SessionID: sessionID,
		MessageID: result.Info.ID,
		Trace:     sessionID,
	}

	// ========== 缓存成功响应用于去重 ==========
	c.completeRequest(requestHash, response)

	// ========== 记忆存储（异步，不阻塞主流程）==========
	if c.memStore != nil && strings.TrimSpace(reply) != "" {
		ms := c.memStore
		ch, uid, req, rep := payload.Channel, payload.UserID, originalContent, reply
		go ms.RecordConversation(ch, uid, req, rep)
	}

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

// FindVideoSkill picks the most likely video-capable skill/agent name.
func (c *Client) FindVideoSkill(ctx context.Context) string {
	agents, err := c.ListAgents(ctx)
	if err != nil || len(agents) == 0 {
		return ""
	}

	bestName := ""
	bestScore := -1
	for _, agent := range agents {
		name := strings.TrimSpace(agent.Name)
		if name == "" {
			continue
		}
		text := strings.ToLower(strings.TrimSpace(agent.Name + " " + agent.Description + " " + agent.Prompt + " " + string(agent.Mode)))
		score := 0
		switch {
		case strings.Contains(text, "video-analyzer") || strings.Contains(text, "video analyzer"):
			score += 12
		case strings.Contains(text, "video_understanding"):
			score += 11
		}
		for _, token := range []string{"video", "vision", "frame", "media", "analy", "clip", "movie"} {
			if strings.Contains(text, token) {
				score += 2
			}
		}
		if score > bestScore {
			bestScore = score
			bestName = name
		}
	}

	if bestScore <= 0 {
		return ""
	}
	return bestName
}

// RevertSession reverts session state to a previous point.
func (c *Client) RevertSession(ctx context.Context, sessionID, messageID string) (*opencode.Session, error) {
	if strings.TrimSpace(sessionID) == "" {
		return nil, fmt.Errorf("opencode: revert session: empty session id")
	}
	if strings.TrimSpace(c.endpoint) == "" {
		return nil, fmt.Errorf("opencode: revert session unavailable: missing endpoint")
	}

	apiURL, err := url.JoinPath(c.endpoint, "session", sessionID, "revert")
	if err != nil {
		apiURL = fmt.Sprintf("%s/session/%s/revert", strings.TrimRight(c.endpoint, "/"), sessionID)
	}
	if strings.TrimSpace(messageID) != "" {
		u, parseErr := url.Parse(apiURL)
		if parseErr == nil {
			q := u.Query()
			q.Set("messageID", strings.TrimSpace(messageID))
			u.RawQuery = q.Encode()
			apiURL = u.String()
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("opencode: revert session request: %w", err)
	}
	c.applyAuthHeaders(req.Header)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("opencode: revert session do: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 65536))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("opencode: revert session status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return c.GetSession(ctx, sessionID)
	}

	var session opencode.Session
	if err := json.Unmarshal(trimmed, &session); err == nil && strings.TrimSpace(session.ID) != "" {
		return &session, nil
	}

	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(trimmed, &envelope); err == nil && len(bytes.TrimSpace(envelope.Data)) > 0 {
		if decodeErr := json.Unmarshal(envelope.Data, &session); decodeErr == nil {
			return &session, nil
		}
	}

	return c.GetSession(ctx, sessionID)
}

// UnrevertSession restores the latest reverted session state.
func (c *Client) UnrevertSession(ctx context.Context, sessionID string) (*opencode.Session, error) {
	if strings.TrimSpace(sessionID) == "" {
		return nil, fmt.Errorf("opencode: unrevert session: empty session id")
	}
	if strings.TrimSpace(c.endpoint) == "" {
		return nil, fmt.Errorf("opencode: unrevert session unavailable: missing endpoint")
	}

	apiURL, err := url.JoinPath(c.endpoint, "session", sessionID, "unrevert")
	if err != nil {
		apiURL = fmt.Sprintf("%s/session/%s/unrevert", strings.TrimRight(c.endpoint, "/"), sessionID)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("opencode: unrevert session request: %w", err)
	}
	c.applyAuthHeaders(req.Header)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("opencode: unrevert session do: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 65536))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("opencode: unrevert session status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return c.GetSession(ctx, sessionID)
	}

	var session opencode.Session
	if err := json.Unmarshal(trimmed, &session); err == nil && strings.TrimSpace(session.ID) != "" {
		return &session, nil
	}

	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(trimmed, &envelope); err == nil && len(bytes.TrimSpace(envelope.Data)) > 0 {
		if decodeErr := json.Unmarshal(envelope.Data, &session); decodeErr == nil {
			return &session, nil
		}
	}

	return c.GetSession(ctx, sessionID)
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
	result, err := c.sdk.Session.Shell(ctx, sessionID, opencode.SessionShellParams{
		Agent:     opencode.F("build"),
		Command:   opencode.F(command),
		Directory: opencode.F(c.directory),
	})
	if err == nil {
		return result, nil
	}

	if !isMissingShellAgentError(err) {
		return nil, err
	}

	log.Printf("opencode: shell SDK call failed with missing-agent validation, retrying via HTTP fallback (session=%s): %v",
		sessionID[:min(8, len(sessionID))], err)

	fallbackResult, fallbackErr := c.executeShellViaHTTP(ctx, sessionID, command, "build")
	if fallbackErr != nil {
		return nil, fmt.Errorf("opencode: shell failed (sdk=%v, fallback=%w)", err, fallbackErr)
	}

	return fallbackResult, nil
}

// ExecuteShellOutput executes a shell command and returns human-readable output text.
func (c *Client) ExecuteShellOutput(ctx context.Context, sessionID, command string) (string, error) {
	msg, err := c.ExecuteShell(ctx, sessionID, command)
	if err != nil {
		return "", err
	}

	if text := extractTextFromAssistantMessage(msg); strings.TrimSpace(text) != "" {
		log.Printf("opencode: shell output extracted from assistant message (session=%s, cmd=%q, chars=%d)",
			sessionID[:min(8, len(sessionID))], command, len(text))
		return text, nil
	}

	if strings.TrimSpace(sessionID) != "" {
		fallbackCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if reply, _, fetchErr := c.fetchLatestAssistantReply(fallbackCtx, sessionID); fetchErr == nil {
			if strings.TrimSpace(reply) != "" {
				log.Printf("opencode: shell output extracted from latest session message (session=%s, cmd=%q, chars=%d)",
					sessionID[:min(8, len(sessionID))], command, len(reply))
				return reply, nil
			}
		}
	}

	if msg != nil && strings.TrimSpace(msg.ID) != "" {
		return fmt.Sprintf("(命令已执行，但未返回可解析文本。message_id=%s)", msg.ID), nil
	}

	return "", nil
}

func extractTextFromAssistantMessage(msg *opencode.AssistantMessage) string {
	if msg == nil {
		return ""
	}

	rawJSON := strings.TrimSpace(msg.JSON.RawJSON())
	if rawJSON != "" {
		if text := extractTextFromAssistantMessageJSON([]byte(rawJSON)); strings.TrimSpace(text) != "" {
			return text
		}
	}

	raw, err := json.Marshal(msg)
	if err != nil {
		return ""
	}

	return extractTextFromAssistantMessageJSON(raw)
}

func extractTextFromAssistantMessageJSON(raw []byte) string {
	if len(bytes.TrimSpace(raw)) == 0 {
		return ""
	}

	var obj map[string]interface{}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}

	collectString := func(v interface{}, keys ...string) string {
		m, ok := v.(map[string]interface{})
		if !ok {
			return ""
		}
		cur := interface{}(m)
		for _, k := range keys {
			next, ok := cur.(map[string]interface{})
			if !ok {
				return ""
			}
			cur, ok = next[k]
			if !ok {
				return ""
			}
		}
		s, _ := cur.(string)
		return strings.TrimSpace(s)
	}

	partsValue, ok := obj["parts"].([]interface{})
	if !ok || len(partsValue) == 0 {
		if content, ok := obj["content"].(string); ok {
			return strings.TrimSpace(content)
		}
		return ""
	}

	textParts := make([]string, 0, len(partsValue))
	seen := make(map[string]struct{}, len(partsValue))
	for _, part := range partsValue {
		partMap, ok := part.(map[string]interface{})
		if !ok {
			continue
		}

		candidates := []string{
			collectString(partMap, "text"),
			collectString(partMap, "content"),
			collectString(partMap, "stdout"),
			collectString(partMap, "output"),
			collectString(partMap, "result"),
			collectString(partMap, "text", "value"),
			collectString(partMap, "content", "text"),
			collectString(partMap, "content", "value"),
		}

		for _, c := range candidates {
			if c != "" {
				if _, exists := seen[c]; !exists {
					seen[c] = struct{}{}
					textParts = append(textParts, c)
				}
				break
			}
		}
	}

	return strings.TrimSpace(strings.Join(textParts, "\n"))
}

func isMissingShellAgentError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	if !strings.Contains(msg, "/shell") {
		return false
	}
	if strings.Contains(msg, "\"path\":[\"agent\"]") {
		return true
	}
	return strings.Contains(msg, "expected string") && strings.Contains(msg, "received undefined") && strings.Contains(msg, "agent")
}

func (c *Client) executeShellViaHTTP(ctx context.Context, sessionID, command, agent string) (*opencode.AssistantMessage, error) {
	if strings.TrimSpace(c.endpoint) == "" {
		return nil, fmt.Errorf("opencode: shell HTTP fallback unavailable: missing endpoint")
	}

	shellURL, err := url.JoinPath(c.endpoint, "session", sessionID, "shell")
	if err != nil {
		shellURL = fmt.Sprintf("%s/session/%s/shell", strings.TrimRight(c.endpoint, "/"), sessionID)
	}

	if dir := strings.TrimSpace(c.directory); dir != "" {
		u, parseErr := url.Parse(shellURL)
		if parseErr == nil {
			q := u.Query()
			q.Set("directory", dir)
			u.RawQuery = q.Encode()
			shellURL = u.String()
		}
	}

	payload := map[string]string{
		"agent":   strings.TrimSpace(agent),
		"command": command,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("opencode: shell HTTP fallback marshal: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, shellURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("opencode: shell HTTP fallback request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.applyAuthHeaders(req.Header)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("opencode: shell HTTP fallback do: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 65536))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("opencode: shell HTTP fallback unexpected status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	trimmed := bytes.TrimSpace(respBody)
	if len(trimmed) == 0 {
		return nil, nil
	}

	var message opencode.AssistantMessage
	if err := json.Unmarshal(trimmed, &message); err == nil {
		return &message, nil
	}

	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(trimmed, &envelope); err == nil && len(bytes.TrimSpace(envelope.Data)) > 0 {
		if unmarshalErr := json.Unmarshal(envelope.Data, &message); unmarshalErr == nil {
			return &message, nil
		}
	}

	log.Printf("opencode: shell HTTP fallback succeeded but response format was unexpected, returning empty assistant message")
	return &opencode.AssistantMessage{}, nil
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
	c.modelOverride.Delete(sessionID)
	c.runningSessions.Delete(sessionID)
	return nil
}

// GetProviders retrieves the list of available providers.
func (c *Client) GetProviders(ctx context.Context) ([]Provider, error) {
	if !c.Ready() {
		return nil, fmt.Errorf("opencode: client not configured")
	}

	providers, _, _, err := c.loadProviderCatalog(ctx, false)
	if err != nil {
		return nil, err
	}
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
	Attachment       bool     `json:"attachment,omitempty"`
	InputModalities  []string `json:"input_modalities,omitempty"`
	OutputModalities []string `json:"output_modalities,omitempty"`
}

type modelCapability struct {
	ProviderID       string
	ModelID          string
	Attachment       bool
	InputModalities  map[string]struct{}
	OutputModalities map[string]struct{}
}

// GetCurrentProvider retrieves the current provider and model for a session.
func (c *Client) GetCurrentProvider(ctx context.Context, sessionID string) (*Provider, string, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil, "", fmt.Errorf("opencode: get current provider: empty session id")
	}

	_, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return nil, "", fmt.Errorf("opencode: get session: %w", err)
	}

	providers, _, defaultModel, err := c.loadProviderCatalog(ctx, false)
	if err != nil {
		return nil, "", err
	}

	if override, ok := c.getSessionModelOverride(sessionID); ok {
		overrideProviderID := strings.TrimSpace(override.ProviderID.Value)
		overrideModelID := strings.TrimSpace(override.ModelID.Value)
		if overrideProviderID == "" || overrideModelID == "" {
			return nil, "", nil
		}

		for _, provider := range providers {
			if provider.ID != overrideProviderID {
				continue
			}
			providerCopy := provider
			return &providerCopy, overrideModelID, nil
		}
		return &Provider{ID: overrideProviderID, Name: overrideProviderID}, overrideModelID, nil
	}

	if defaultModel != nil {
		defaultProviderID := strings.TrimSpace(defaultModel.ProviderID.Value)
		defaultModelID := strings.TrimSpace(defaultModel.ModelID.Value)
		if defaultProviderID == "" || defaultModelID == "" {
			return nil, "", nil
		}

		for _, provider := range providers {
			if provider.ID != defaultProviderID {
				continue
			}
			providerCopy := provider
			return &providerCopy, defaultModelID, nil
		}
		return &Provider{ID: defaultProviderID, Name: defaultProviderID}, defaultModelID, nil
	}

	return nil, "", nil
}

// UpdateSessionProvider updates the provider and model for a session.
func (c *Client) UpdateSessionProvider(ctx context.Context, sessionID, providerID, modelID string) error {
	if strings.TrimSpace(sessionID) == "" {
		return fmt.Errorf("opencode: update session provider: empty session id")
	}
	providerID = strings.TrimSpace(providerID)
	modelID = strings.TrimSpace(modelID)
	if providerID == "" || modelID == "" {
		return fmt.Errorf("opencode: update session provider: empty provider/model")
	}

	if _, err := c.GetSession(ctx, sessionID); err != nil {
		return fmt.Errorf("opencode: update session provider: %w", err)
	}

	override := opencode.SessionPromptParamsModel{
		ProviderID: opencode.F(providerID),
		ModelID:    opencode.F(modelID),
	}
	c.modelOverride.Store(sessionID, override)

	// Keep model config cache fresh after model override changes.
	c.modelConfig.Delete(sessionID)
	go c.fetchAndCacheModelConfig(context.Background(), sessionID)

	log.Printf("opencode: session %s model override set to %s/%s (applied via prompt model param)", sessionID[:min(8, len(sessionID))], providerID, modelID)
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
	go c.globalEventListenerLoop(ctx)

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
				previousFailures := consecutiveFailures
				connected = true
				if previousFailures > 0 {
					log.Printf("opencode: ✅ event listener reconnected successfully after %d failures", previousFailures)
					go c.syncRegisteredSessionSnapshots(ctx)
				}
				consecutiveFailures = 0
			}

			// Log every event for debugging
			if eventCount <= 10 || eventType != "server.heartbeat" {
				log.Printf("opencode: [event #%d] type=%s", eventCount, eventType)
			}

			// Extract session ID using shared helper
			sessionID := extractSessionIDFromEvent(&event)

			if sessionID != "" && len(sessionID) > 8 {
				log.Printf("opencode: processing event type=%s, sessionID=%s", eventType, sessionID[:8])
			} else if eventType != "server.heartbeat" && eventType != "server.connected" {
				log.Printf("opencode: processing event type=%s (no sessionID)", eventType)
			}

			// Fast path: if session ID found, call the specific session handler
			if sessionID != "" {
				if handler, ok := c.sessionHandlers.Load(sessionID); ok {
					if err := handler.(EventHandler)(ctx, &event); err != nil {
						log.Printf("opencode: session handler error for %s: %v", sessionID[:8], err)
					}
				} else {
					log.Printf("opencode: no session handler found for %s", sessionID[:8])
				}
			}

			c.dispatchGlobalEventHandlers(ctx, &event)
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

func (c *Client) dispatchGlobalEventHandlers(ctx context.Context, event *opencode.EventListResponse) {
	c.eventListenerMu.RLock()
	handlers := c.eventHandlers
	c.eventListenerMu.RUnlock()

	if len(handlers) > 0 {
		log.Printf("opencode: dispatching to %d global handlers", len(handlers))
	}

	for _, handler := range handlers {
		if err := handler(ctx, event); err != nil {
			log.Printf("opencode: global event handler error: %v", err)
		}
	}
}

// globalEventListenerLoop subscribes to /global/event to align with official OpenCode
// global SSE semantics (server.connected + heartbeat + process-wide events).
func (c *Client) globalEventListenerLoop(ctx context.Context) {
	if c.endpoint == "" {
		log.Printf("opencode: global event listener skipped: empty endpoint")
		return
	}

	baseDelay := 2 * time.Second
	maxDelay := 60 * time.Second
	consecutiveFailures := 0
	lastEventID := ""

	for {
		select {
		case <-ctx.Done():
			log.Printf("opencode: global event listener stopped (context cancelled)")
			return
		default:
		}

		if consecutiveFailures > 0 {
			delay := baseDelay * time.Duration(1<<uint(min(consecutiveFailures-1, 5)))
			if delay > maxDelay {
				delay = maxDelay
			}
			log.Printf("opencode: reconnecting global SSE in %v (attempt #%d)", delay, consecutiveFailures)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return
			}
		}

		nextEventID, eventCount, err := c.readGlobalEventStreamOnce(ctx, lastEventID)
		if nextEventID != "" {
			lastEventID = nextEventID
		}

		if err != nil {
			consecutiveFailures++
			log.Printf("opencode: global SSE stream ended with error: %v", err)
			c.maybeHandleServerUnavailable(ctx, err, "global-event-stream")
			continue
		}

		if eventCount > 0 {
			log.Printf("opencode: global SSE stream ended (events=%d), reconnecting", eventCount)
		} else {
			log.Printf("opencode: global SSE stream ended without events, reconnecting")
		}
		consecutiveFailures++
	}
}

type sseFrame struct {
	ID    string
	Event string
	Retry int
	Data  string
}

func (c *Client) readGlobalEventStreamOnce(ctx context.Context, lastEventID string) (string, int, error) {
	globalURL, err := url.JoinPath(c.endpoint, "global", "event")
	if err != nil {
		globalURL = strings.TrimRight(c.endpoint, "/") + "/global/event"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, globalURL, nil)
	if err != nil {
		return lastEventID, 0, err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	if lastEventID != "" {
		req.Header.Set("Last-Event-ID", lastEventID)
	}
	c.applyAuthHeaders(req.Header)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return lastEventID, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return lastEventID, 0, fmt.Errorf("global event stream status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	eventCount := 0
	parseErr := parseSSE(resp.Body, func(frame sseFrame) error {
		if frame.ID != "" {
			lastEventID = frame.ID
		}
		raw := strings.TrimSpace(frame.Data)
		if raw == "" {
			return nil
		}

		var evt opencode.EventListResponse
		if err := json.Unmarshal([]byte(raw), &evt); err != nil {
			// Keep parity with OpenCode parser behavior: ignore non-JSON payloads.
			return nil
		}

		if c.isSessionScopedEvent(&evt) {
			return nil
		}

		eventCount++
		c.dispatchGlobalEventHandlers(ctx, &evt)
		return nil
	})

	return lastEventID, eventCount, parseErr
}

func (c *Client) isSessionScopedEvent(event *opencode.EventListResponse) bool {
	if event == nil {
		return false
	}
	if extractSessionIDFromEvent(event) != "" {
		return true
	}

	eventType := string(event.Type)
	if strings.HasPrefix(eventType, "session.") ||
		strings.HasPrefix(eventType, "message.") ||
		strings.HasPrefix(eventType, "todo.") ||
		strings.HasPrefix(eventType, "permission.") ||
		strings.HasPrefix(eventType, "question.") {
		return true
	}

	raw := event.JSON.RawJSON()
	if raw == "" {
		return false
	}

	var probe struct {
		Properties struct {
			MessageID string `json:"messageID"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(raw), &probe); err == nil && probe.Properties.MessageID != "" {
		if _, ok := c.messageToSession.Load(probe.Properties.MessageID); ok {
			return true
		}
	}

	return false
}

func parseSSE(reader io.Reader, onFrame func(frame sseFrame) error) error {
	br := bufio.NewReader(reader)
	var id string
	var eventName string
	retry := 0
	dataLines := make([]string, 0, 4)

	flush := func() error {
		if len(dataLines) == 0 && id == "" && eventName == "" && retry == 0 {
			return nil
		}
		frame := sseFrame{
			ID:    id,
			Event: eventName,
			Retry: retry,
			Data:  strings.Join(dataLines, "\n"),
		}
		id = ""
		eventName = ""
		retry = 0
		dataLines = dataLines[:0]
		return onFrame(frame)
	}

	for {
		line, err := br.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		line = strings.TrimRight(line, "\r\n")

		if line == "" {
			if flushErr := flush(); flushErr != nil {
				return flushErr
			}
		} else if !strings.HasPrefix(line, ":") {
			field := line
			value := ""
			if idx := strings.IndexByte(line, ':'); idx >= 0 {
				field = line[:idx]
				value = line[idx+1:]
				if strings.HasPrefix(value, " ") {
					value = value[1:]
				}
			}

			switch field {
			case "data":
				dataLines = append(dataLines, value)
			case "id":
				id = value
			case "event":
				eventName = value
			case "retry":
				if n, convErr := strconv.Atoi(value); convErr == nil {
					retry = n
				}
			}
		}

		if errors.Is(err, io.EOF) {
			return flush()
		}
	}
}

func inferMimeFromDataURI(raw string) string {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "data:") {
		return ""
	}
	semi := strings.Index(raw, ";")
	comma := strings.Index(raw, ",")
	end := semi
	if end < 0 || (comma >= 0 && comma < end) {
		end = comma
	}
	if end <= len("data:") {
		return ""
	}
	return strings.TrimSpace(raw[len("data:"):end])
}

func normalizeModelKey(providerID, modelID string) string {
	return strings.ToLower(strings.TrimSpace(providerID)) + "/" + strings.ToLower(strings.TrimSpace(modelID))
}

func parseProviderModelRef(ref string) (string, string, bool) {
	parts := strings.SplitN(strings.TrimSpace(ref), "/", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	providerID := strings.TrimSpace(parts[0])
	modelID := strings.TrimSpace(parts[1])
	if providerID == "" || modelID == "" {
		return "", "", false
	}
	return providerID, modelID, true
}

func resolveDefaultModelHint(defaultMap map[string]string, capabilityMap map[string]modelCapability) *opencode.SessionPromptParamsModel {
	if len(defaultMap) == 0 {
		return nil
	}

	keys := make([]string, 0, len(defaultMap))
	for k := range defaultMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		value := strings.TrimSpace(defaultMap[key])
		if value == "" {
			continue
		}

		if providerID, modelID, ok := parseProviderModelRef(value); ok {
			if _, exists := capabilityMap[normalizeModelKey(providerID, modelID)]; exists {
				return modelFromRef(providerID, modelID)
			}
		}

		providerID := strings.TrimSpace(key)
		if providerID == "" {
			continue
		}
		if _, exists := capabilityMap[normalizeModelKey(providerID, value)]; exists {
			return modelFromRef(providerID, value)
		}
	}

	return nil
}

func containsAttachmentModalities(attachments []Attachment) map[string]struct{} {
	required := make(map[string]struct{})
	for _, attachment := range attachments {
		mimeValue := strings.ToLower(strings.TrimSpace(attachment.Mime))
		if mimeValue == "" {
			mimeValue = strings.ToLower(inferMimeFromDataURI(attachment.URL))
		}
		if strings.HasPrefix(mimeValue, "image/") {
			required["image"] = struct{}{}
			continue
		}
		if strings.HasPrefix(mimeValue, "video/") {
			required["video"] = struct{}{}
			continue
		}
		if strings.HasPrefix(mimeValue, "audio/") {
			required["audio"] = struct{}{}
			continue
		}
		if mimeValue == "application/pdf" || strings.HasSuffix(mimeValue, "/pdf") {
			required["pdf"] = struct{}{}
		}
	}
	return required
}

func requiredModalitiesFromPayload(payload MessagePayload) map[string]struct{} {
	required := containsAttachmentModalities(payload.Attachments)

	messageType := detectPayloadMessageType(payload)

	switch messageType {
	case "image", "picture":
		required["image"] = struct{}{}
	case "video":
		required["video"] = struct{}{}
	case "audio", "voice":
		required["audio"] = struct{}{}
	case "pdf":
		required["pdf"] = struct{}{}
	}

	return required
}

func detectPayloadMessageType(payload MessagePayload) string {
	messageType := strings.ToLower(strings.TrimSpace(payload.Metadata["message_type"]))
	if messageType != "" {
		return messageType
	}
	return strings.ToLower(strings.TrimSpace(payload.Metadata["media_message_type"]))
}

func payloadNeedsAttachmentModel(payload MessagePayload) bool {
	if len(payload.Attachments) > 0 {
		return true
	}

	messageType := detectPayloadMessageType(payload)
	if messageType == "" {
		return false
	}

	switch messageType {
	case "text":
		return false
	case "image", "picture", "video", "audio", "voice", "file", "pdf", "media", "sticker", "emotion", "image_message", "audio_message", "video_message", "file_message":
		return true
	default:
		// Conservative fallback for common non-text aliases from adapters.
		return strings.Contains(messageType, "image") ||
			strings.Contains(messageType, "video") ||
			strings.Contains(messageType, "audio") ||
			strings.Contains(messageType, "voice") ||
			strings.Contains(messageType, "file") ||
			strings.Contains(messageType, "media") ||
			strings.Contains(messageType, "pdf")
	}
}

func capabilityLikelySupportsNonText(capability modelCapability) bool {
	if capability.Attachment {
		return true
	}

	for modality := range capability.InputModalities {
		normalized := strings.ToLower(strings.TrimSpace(modality))
		switch normalized {
		case "", "text", "input_text", "prompt":
			continue
		default:
			return true
		}
	}

	return false
}

func (c *Client) getSessionModelOverride(sessionID string) (opencode.SessionPromptParamsModel, bool) {
	v, ok := c.modelOverride.Load(sessionID)
	if !ok {
		return opencode.SessionPromptParamsModel{}, false
	}
	model, ok := v.(opencode.SessionPromptParamsModel)
	if !ok {
		return opencode.SessionPromptParamsModel{}, false
	}
	return model, true
}

func modelFromRef(providerID, modelID string) *opencode.SessionPromptParamsModel {
	providerID = strings.TrimSpace(providerID)
	modelID = strings.TrimSpace(modelID)
	if providerID == "" || modelID == "" {
		return nil
	}
	return &opencode.SessionPromptParamsModel{
		ProviderID: opencode.F(providerID),
		ModelID:    opencode.F(modelID),
	}
}

func (c *Client) capabilitySupports(required map[string]struct{}, capability modelCapability) bool {
	if len(required) == 0 {
		return true
	}
	if len(capability.InputModalities) == 0 {
		// For multimodal routing we only trust explicit modality declarations.
		return false
	}
	for modality := range required {
		if _, ok := capability.InputModalities[modality]; !ok {
			return false
		}
	}
	return true
}

func (c *Client) hasCapableModelForModalities(required map[string]struct{}) bool {
	if len(required) == 0 {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, capabilityMap, defaultModel, err := c.loadProviderCatalog(ctx, false)
	if err != nil {
		log.Printf("opencode: failed to load provider catalog for modality check: %v", err)
	}

	if defaultModel != nil {
		providerID := strings.TrimSpace(defaultModel.ProviderID.Value)
		modelID := strings.TrimSpace(defaultModel.ModelID.Value)
		if providerID != "" && modelID != "" {
			if capability, ok := capabilityMap[normalizeModelKey(providerID, modelID)]; ok {
				if c.capabilitySupports(required, capability) {
					return true
				}
			}
		}
	}

	for _, capability := range capabilityMap {
		if c.capabilitySupports(required, capability) {
			return true
		}
	}

	return false
}

// HasImageCapableModel reports whether any configured model can accept image input.
func (c *Client) HasImageCapableModel() bool {
	return c.hasCapableModelForModalities(map[string]struct{}{"image": {}})
}

// HasVideoCapableModel reports whether any configured model can accept video input.
func (c *Client) HasVideoCapableModel() bool {
	return c.hasCapableModelForModalities(map[string]struct{}{"video": {}})
}

func (c *Client) loadProviderCatalogFromHTTP(ctx context.Context) ([]Provider, map[string]modelCapability, *opencode.SessionPromptParamsModel, error) {
	if strings.TrimSpace(c.endpoint) == "" {
		return nil, nil, nil, fmt.Errorf("opencode: providers HTTP unavailable: missing endpoint")
	}

	providersURL, err := url.JoinPath(c.endpoint, "config", "providers")
	if err != nil {
		providersURL = strings.TrimRight(c.endpoint, "/") + "/config/providers"
	}

	if dir := strings.TrimSpace(c.directory); dir != "" {
		u, parseErr := url.Parse(providersURL)
		if parseErr == nil {
			q := u.Query()
			q.Set("directory", dir)
			u.RawQuery = q.Encode()
			providersURL = u.String()
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, providersURL, nil)
	if err != nil {
		return nil, nil, nil, err
	}
	c.applyAuthHeaders(req.Header)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, nil, nil, fmt.Errorf("providers status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var payload struct {
		Default   map[string]string `json:"default"`
		Providers []struct {
			ID     string `json:"id"`
			Name   string `json:"name"`
			Models map[string]struct {
				ID         string `json:"id"`
				Name       string `json:"name"`
				Attachment bool   `json:"attachment"`
				Modalities struct {
					Input  []string `json:"input"`
					Output []string `json:"output"`
				} `json:"modalities"`
				Capabilities struct {
					Attachment bool            `json:"attachment"`
					Input      map[string]bool `json:"input"`
					Output     map[string]bool `json:"output"`
				} `json:"capabilities"`
			} `json:"models"`
		} `json:"providers"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, nil, nil, err
	}

	providerItems := payload.Providers
	sort.Slice(providerItems, func(i, j int) bool {
		return strings.ToLower(strings.TrimSpace(providerItems[i].ID)) < strings.ToLower(strings.TrimSpace(providerItems[j].ID))
	})

	providers := make([]Provider, 0, len(providerItems))
	capabilityMap := make(map[string]modelCapability)

	for _, providerCfg := range providerItems {
		provider := Provider{
			ID:   strings.TrimSpace(providerCfg.ID),
			Name: strings.TrimSpace(providerCfg.Name),
		}
		if provider.ID == "" {
			continue
		}
		if provider.Name == "" {
			provider.Name = provider.ID
		}

		modelKeys := make([]string, 0, len(providerCfg.Models))
		for modelID := range providerCfg.Models {
			modelKeys = append(modelKeys, modelID)
		}
		sort.Strings(modelKeys)

		provider.Models = make([]Model, 0, len(modelKeys))
		for _, modelKey := range modelKeys {
			modelCfg := providerCfg.Models[modelKey]
			normalizedModelID := strings.TrimSpace(modelCfg.ID)
			if normalizedModelID == "" {
				normalizedModelID = strings.TrimSpace(modelKey)
			}
			if normalizedModelID == "" {
				continue
			}

			inputModalities := make([]string, 0, 4)
			inputSet := make(map[string]struct{}, 4)
			for modality, enabled := range modelCfg.Capabilities.Input {
				if !enabled {
					continue
				}
				value := strings.ToLower(strings.TrimSpace(modality))
				if value == "" {
					continue
				}
				inputModalities = append(inputModalities, value)
				inputSet[value] = struct{}{}
			}
			if len(inputSet) == 0 {
				for _, modality := range modelCfg.Modalities.Input {
					value := strings.ToLower(strings.TrimSpace(modality))
					if value == "" {
						continue
					}
					inputModalities = append(inputModalities, value)
					inputSet[value] = struct{}{}
				}
			}
			sort.Strings(inputModalities)

			outputModalities := make([]string, 0, 4)
			outputSet := make(map[string]struct{}, 4)
			for modality, enabled := range modelCfg.Capabilities.Output {
				if !enabled {
					continue
				}
				value := strings.ToLower(strings.TrimSpace(modality))
				if value == "" {
					continue
				}
				outputModalities = append(outputModalities, value)
				outputSet[value] = struct{}{}
			}
			if len(outputSet) == 0 {
				for _, modality := range modelCfg.Modalities.Output {
					value := strings.ToLower(strings.TrimSpace(modality))
					if value == "" {
						continue
					}
					outputModalities = append(outputModalities, value)
					outputSet[value] = struct{}{}
				}
			}
			sort.Strings(outputModalities)

			attachmentCap := modelCfg.Attachment || modelCfg.Capabilities.Attachment

			provider.Models = append(provider.Models, Model{
				ID:               normalizedModelID,
				Name:             strings.TrimSpace(modelCfg.Name),
				Attachment:       attachmentCap,
				InputModalities:  inputModalities,
				OutputModalities: outputModalities,
			})

			capabilityMap[normalizeModelKey(provider.ID, normalizedModelID)] = modelCapability{
				ProviderID:       provider.ID,
				ModelID:          normalizedModelID,
				Attachment:       attachmentCap,
				InputModalities:  inputSet,
				OutputModalities: outputSet,
			}
		}

		providers = append(providers, provider)
	}

	defaultModel := resolveDefaultModelHint(payload.Default, capabilityMap)
	return providers, capabilityMap, defaultModel, nil
}

func (c *Client) loadProviderCatalog(ctx context.Context, forceRefresh bool) ([]Provider, map[string]modelCapability, *opencode.SessionPromptParamsModel, error) {
	c.providerCacheMu.RLock()
	if !forceRefresh && time.Since(c.providerCacheAt) < ProviderCatalogCacheTTL && len(c.providerCache) > 0 {
		providers := append([]Provider(nil), c.providerCache...)
		capabilityCopy := make(map[string]modelCapability, len(c.capabilityCache))
		for k, v := range c.capabilityCache {
			capabilityCopy[k] = v
		}
		var defaultCopy *opencode.SessionPromptParamsModel
		if c.defaultModelHint != nil {
			v := *c.defaultModelHint
			defaultCopy = &v
		}
		c.providerCacheMu.RUnlock()
		return providers, capabilityCopy, defaultCopy, nil
	}
	c.providerCacheMu.RUnlock()

	if providers, capabilityMap, defaultModel, httpErr := c.loadProviderCatalogFromHTTP(ctx); httpErr == nil {
		c.providerCacheMu.Lock()
		c.providerCache = append([]Provider(nil), providers...)
		c.capabilityCache = capabilityMap
		c.providerCacheAt = time.Now()
		if defaultModel != nil {
			copyModel := *defaultModel
			c.defaultModelHint = &copyModel
		} else {
			c.defaultModelHint = nil
		}
		c.providerCacheMu.Unlock()

		return providers, capabilityMap, defaultModel, nil
	} else {
		log.Printf("opencode: providers HTTP catalog unavailable, falling back to SDK: %v", httpErr)
	}

	appParams := opencode.AppProvidersParams{}
	if strings.TrimSpace(c.directory) != "" {
		appParams.Directory = opencode.F(c.directory)
	}

	if appProviders, appErr := c.sdk.App.Providers(ctx, appParams); appErr == nil && appProviders != nil && len(appProviders.Providers) > 0 {
		providerItems := append([]opencode.Provider(nil), appProviders.Providers...)
		sort.Slice(providerItems, func(i, j int) bool {
			return strings.ToLower(strings.TrimSpace(providerItems[i].ID)) < strings.ToLower(strings.TrimSpace(providerItems[j].ID))
		})

		providers := make([]Provider, 0, len(providerItems))
		capabilityMap := make(map[string]modelCapability)

		for _, providerCfg := range providerItems {
			provider := Provider{
				ID:   strings.TrimSpace(providerCfg.ID),
				Name: strings.TrimSpace(providerCfg.Name),
			}
			if provider.ID == "" {
				continue
			}
			if provider.Name == "" {
				provider.Name = provider.ID
			}

			modelKeys := make([]string, 0, len(providerCfg.Models))
			for modelID := range providerCfg.Models {
				modelKeys = append(modelKeys, modelID)
			}
			sort.Strings(modelKeys)

			provider.Models = make([]Model, 0, len(modelKeys))
			for _, modelKey := range modelKeys {
				modelCfg := providerCfg.Models[modelKey]
				normalizedModelID := strings.TrimSpace(modelCfg.ID)
				if normalizedModelID == "" {
					normalizedModelID = strings.TrimSpace(modelKey)
				}
				if normalizedModelID == "" {
					continue
				}

				inputModalities := make([]string, 0, len(modelCfg.Modalities.Input))
				inputSet := make(map[string]struct{}, len(modelCfg.Modalities.Input))
				for _, modality := range modelCfg.Modalities.Input {
					value := strings.ToLower(strings.TrimSpace(string(modality)))
					if value == "" {
						continue
					}
					inputModalities = append(inputModalities, value)
					inputSet[value] = struct{}{}
				}

				outputModalities := make([]string, 0, len(modelCfg.Modalities.Output))
				outputSet := make(map[string]struct{}, len(modelCfg.Modalities.Output))
				for _, modality := range modelCfg.Modalities.Output {
					value := strings.ToLower(strings.TrimSpace(string(modality)))
					if value == "" {
						continue
					}
					outputModalities = append(outputModalities, value)
					outputSet[value] = struct{}{}
				}

				provider.Models = append(provider.Models, Model{
					ID:               normalizedModelID,
					Name:             strings.TrimSpace(modelCfg.Name),
					Attachment:       modelCfg.Attachment,
					InputModalities:  inputModalities,
					OutputModalities: outputModalities,
				})

				capabilityMap[normalizeModelKey(provider.ID, normalizedModelID)] = modelCapability{
					ProviderID:       provider.ID,
					ModelID:          normalizedModelID,
					Attachment:       modelCfg.Attachment,
					InputModalities:  inputSet,
					OutputModalities: outputSet,
				}
			}

			providers = append(providers, provider)
		}

		defaultModel := resolveDefaultModelHint(appProviders.Default, capabilityMap)

		c.providerCacheMu.Lock()
		c.providerCache = append([]Provider(nil), providers...)
		c.capabilityCache = capabilityMap
		c.providerCacheAt = time.Now()
		if defaultModel != nil {
			copyModel := *defaultModel
			c.defaultModelHint = &copyModel
		} else {
			c.defaultModelHint = nil
		}
		c.providerCacheMu.Unlock()

		return providers, capabilityMap, defaultModel, nil
	}

	params := opencode.ConfigGetParams{}
	if strings.TrimSpace(c.directory) != "" {
		params.Directory = opencode.F(c.directory)
	}

	cfg, err := c.sdk.Config.Get(ctx, params)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("opencode: get providers from config: %w", err)
	}
	if cfg == nil {
		return []Provider{}, map[string]modelCapability{}, nil, nil
	}

	providerKeys := make([]string, 0, len(cfg.Provider))
	for providerID := range cfg.Provider {
		providerKeys = append(providerKeys, providerID)
	}
	sort.Strings(providerKeys)

	providers := make([]Provider, 0, len(providerKeys))
	capabilityMap := make(map[string]modelCapability)

	for _, providerID := range providerKeys {
		providerCfg := cfg.Provider[providerID]
		provider := Provider{
			ID:   providerCfg.ID,
			Name: providerCfg.Name,
		}
		if strings.TrimSpace(provider.ID) == "" {
			provider.ID = providerID
		}
		if strings.TrimSpace(provider.Name) == "" {
			provider.Name = provider.ID
		}

		modelKeys := make([]string, 0, len(providerCfg.Models))
		for modelID := range providerCfg.Models {
			modelKeys = append(modelKeys, modelID)
		}
		sort.Strings(modelKeys)

		provider.Models = make([]Model, 0, len(modelKeys))
		for _, modelID := range modelKeys {
			modelCfg := providerCfg.Models[modelID]
			normalizedModelID := strings.TrimSpace(modelCfg.ID)
			if normalizedModelID == "" {
				normalizedModelID = modelID
			}

			inputModalities := make([]string, 0, len(modelCfg.Modalities.Input))
			inputSet := make(map[string]struct{}, len(modelCfg.Modalities.Input))
			for _, modality := range modelCfg.Modalities.Input {
				value := strings.ToLower(strings.TrimSpace(string(modality)))
				if value == "" {
					continue
				}
				inputModalities = append(inputModalities, value)
				inputSet[value] = struct{}{}
			}

			outputModalities := make([]string, 0, len(modelCfg.Modalities.Output))
			outputSet := make(map[string]struct{}, len(modelCfg.Modalities.Output))
			for _, modality := range modelCfg.Modalities.Output {
				value := strings.ToLower(strings.TrimSpace(string(modality)))
				if value == "" {
					continue
				}
				outputModalities = append(outputModalities, value)
				outputSet[value] = struct{}{}
			}

			provider.Models = append(provider.Models, Model{
				ID:               normalizedModelID,
				Name:             modelCfg.Name,
				Attachment:       modelCfg.Attachment,
				InputModalities:  inputModalities,
				OutputModalities: outputModalities,
			})

			capabilityMap[normalizeModelKey(provider.ID, normalizedModelID)] = modelCapability{
				ProviderID:       provider.ID,
				ModelID:          normalizedModelID,
				Attachment:       modelCfg.Attachment,
				InputModalities:  inputSet,
				OutputModalities: outputSet,
			}
		}

		providers = append(providers, provider)
	}

	var defaultModel *opencode.SessionPromptParamsModel
	if providerID, modelID, ok := parseProviderModelRef(cfg.Model); ok {
		defaultModel = modelFromRef(providerID, modelID)
	}

	c.providerCacheMu.Lock()
	c.providerCache = append([]Provider(nil), providers...)
	c.capabilityCache = capabilityMap
	c.providerCacheAt = time.Now()
	if defaultModel != nil {
		copyModel := *defaultModel
		c.defaultModelHint = &copyModel
	} else {
		c.defaultModelHint = nil
	}
	c.providerCacheMu.Unlock()

	return providers, capabilityMap, defaultModel, nil
}

func (c *Client) selectModelOverride(ctx context.Context, sessionID string, payload MessagePayload) (*opencode.SessionPromptParamsModel, string) {
	requiredModalities := requiredModalitiesFromPayload(payload)
	nonTextHint := payloadNeedsAttachmentModel(payload)
	if len(requiredModalities) > 0 {
		mods := make([]string, 0, len(requiredModalities))
		for modality := range requiredModalities {
			mods = append(mods, modality)
		}
		sort.Strings(mods)
		log.Printf("opencode: multimodal requirements detected for session %s: %s", sessionID[:min(8, len(sessionID))], strings.Join(mods, ","))
	} else if nonTextHint {
		log.Printf("opencode: non-text message detected for session %s by adapter metadata, selecting attachment-capable model", sessionID[:min(8, len(sessionID))])
	}

	if override, ok := c.getSessionModelOverride(sessionID); ok {
		model := override
		if len(requiredModalities) == 0 && !nonTextHint {
			return &model, "session override"
		}

		_, capabilityMap, _, err := c.loadProviderCatalog(ctx, false)
		if err != nil {
			log.Printf("opencode: failed to load provider catalog for model override (%v), using session override", err)
			return &model, "session override (catalog unavailable)"
		}

		if cap, ok := capabilityMap[normalizeModelKey(model.ProviderID.Value, model.ModelID.Value)]; ok {
			if len(requiredModalities) > 0 && c.capabilitySupports(requiredModalities, cap) {
				return &model, "session override supports attachments"
			}
			if len(requiredModalities) == 0 && nonTextHint && capabilityLikelySupportsNonText(cap) {
				return &model, "session override supports non-text input"
			}
		}
	}

	if len(requiredModalities) == 0 && !nonTextHint {
		return nil, ""
	}

	providers, capabilityMap, defaultModel, err := c.loadProviderCatalog(ctx, false)
	if err != nil {
		log.Printf("opencode: failed to load provider catalog for auto model selection: %v", err)
		return nil, ""
	}

	if defaultModel != nil {
		if cap, ok := capabilityMap[normalizeModelKey(defaultModel.ProviderID.Value, defaultModel.ModelID.Value)]; ok {
			if len(requiredModalities) > 0 && c.capabilitySupports(requiredModalities, cap) {
				model := *defaultModel
				return &model, "default model supports attachments"
			}
			if len(requiredModalities) == 0 && nonTextHint && capabilityLikelySupportsNonText(cap) {
				model := *defaultModel
				return &model, "default model supports non-text input"
			}
		}
		log.Printf("opencode: default model %s/%s does not satisfy required modalities", defaultModel.ProviderID.Value, defaultModel.ModelID.Value)
	}

	for _, provider := range providers {
		for _, model := range provider.Models {
			cap, ok := capabilityMap[normalizeModelKey(provider.ID, model.ID)]
			if !ok {
				continue
			}
			if len(requiredModalities) > 0 {
				if !c.capabilitySupports(requiredModalities, cap) {
					continue
				}
			} else if nonTextHint {
				if !capabilityLikelySupportsNonText(cap) {
					continue
				}
			}
			override := modelFromRef(provider.ID, model.ID)
			if override != nil {
				if len(requiredModalities) > 0 {
					return override, "auto-selected by attachment modalities"
				}
				return override, "auto-selected by non-text message type"
			}
		}
	}

	// Fallback for providers that only expose coarse attachment capability
	// without explicit per-modality declarations.
	for _, provider := range providers {
		for _, model := range provider.Models {
			cap, ok := capabilityMap[normalizeModelKey(provider.ID, model.ID)]
			if !ok {
				continue
			}
			if !cap.Attachment {
				continue
			}
			override := modelFromRef(provider.ID, model.ID)
			if override != nil {
				return override, "fallback: attachment-capable model (modalities unavailable)"
			}
		}
	}

	log.Printf("opencode: no model matched attachment modalities for session %s (attachments=%d)", sessionID[:min(8, len(sessionID))], len(payload.Attachments))
	return nil, ""
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

func (c *Client) syncRegisteredSessionSnapshots(ctx context.Context) {
	var sessionIDs []string
	c.sessionHandlers.Range(func(key, value interface{}) bool {
		sessionID, _ := key.(string)
		if strings.TrimSpace(sessionID) != "" {
			sessionIDs = append(sessionIDs, sessionID)
		}
		return true
	})

	if len(sessionIDs) == 0 {
		return
	}

	for _, sessionID := range sessionIDs {
		syncCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		_, err := c.SyncSessionSnapshot(syncCtx, sessionID)
		cancel()
		if err != nil {
			log.Printf("opencode: snapshot sync failed after reconnect (session=%s): %v", sessionID[:min(8, len(sessionID))], err)
			continue
		}
		log.Printf("opencode: snapshot sync completed after reconnect (session=%s)", sessionID[:min(8, len(sessionID))])
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

	// 解析当前session使用的 providerID / modelID（session override 优先，次选默认模型）
	var summarizeProviderID, summarizeModelID string
	if override, ok := c.getSessionModelOverride(sessionID); ok {
		summarizeProviderID = strings.TrimSpace(override.ProviderID.Value)
		summarizeModelID = strings.TrimSpace(override.ModelID.Value)
	}
	if summarizeProviderID == "" || summarizeModelID == "" {
		_, _, defaultModel, _ := c.loadProviderCatalog(ctx, false)
		if defaultModel != nil {
			summarizeProviderID = strings.TrimSpace(defaultModel.ProviderID.Value)
			summarizeModelID = strings.TrimSpace(defaultModel.ModelID.Value)
		}
	}
	if summarizeProviderID == "" || summarizeModelID == "" {
		return fmt.Errorf("opencode: summarize session: no model configured for session %s", sessionID)
	}

	// 调用OpenCode的summarize API
	_, err := c.sdk.Session.Summarize(ctx, sessionID, opencode.SessionSummarizeParams{
		ProviderID: opencode.F(summarizeProviderID),
		ModelID:    opencode.F(summarizeModelID),
	})
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
	count, ok := c.messageCount.Load(sessionID)
	if !ok {
		return 0
	}
	return count.(int)
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

// Directory returns current working directory configured for session actions.
func (c *Client) Directory() string {
	return c.directory
}

func (c *Client) IsThinkingEnabled() bool {
	c.modeMu.RLock()
	defer c.modeMu.RUnlock()
	return c.showThinking
}

func (c *Client) SetThinkingEnabled(enabled bool) {
	c.modeMu.Lock()
	c.showThinking = enabled
	c.modeMu.Unlock()
}

func (c *Client) IsFinalOnlyEnabled() bool {
	c.modeMu.RLock()
	defer c.modeMu.RUnlock()
	return c.finalOnly
}

func (c *Client) SetFinalOnlyEnabled(enabled bool) {
	c.modeMu.Lock()
	c.finalOnly = enabled
	c.modeMu.Unlock()
}

func (c *Client) IsStepEnabled() bool {
	c.modeMu.RLock()
	defer c.modeMu.RUnlock()
	return c.showSteps
}

func (c *Client) SetStepEnabled(enabled bool) {
	c.modeMu.Lock()
	c.showSteps = enabled
	c.modeMu.Unlock()
}

func (c *Client) IsDevCoreEnabled() bool {
	c.modeMu.RLock()
	defer c.modeMu.RUnlock()
	return c.devCoreEnabled
}

func (c *Client) SetDevCoreEnabled(enabled bool) {
	c.modeMu.Lock()
	c.devCoreEnabled = enabled
	c.modeMu.Unlock()
}

func (c *Client) GetDevCorePrompt() string {
	c.modeMu.RLock()
	defer c.modeMu.RUnlock()
	return c.devCorePrompt
}

func (c *Client) SetDevCorePrompt(prompt string) {
	c.modeMu.Lock()
	c.devCorePrompt = strings.TrimSpace(prompt)
	c.modeMu.Unlock()
}

func (c *Client) ResetDevCorePrompt() {
	c.modeMu.Lock()
	c.devCorePrompt = ""
	c.modeMu.Unlock()
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

// SessionSnapshot is a best-effort state snapshot for a session.
type SessionSnapshot struct {
	Session  *opencode.Session                  `json:"session,omitempty"`
	Messages []opencode.SessionMessagesResponse `json:"messages,omitempty"`
	Todos    []TodoItem                         `json:"todos,omitempty"`
	Diff     []FileDiff                         `json:"diff,omitempty"`
	SyncedAt time.Time                          `json:"synced_at"`
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

// SyncSessionSnapshot performs a best-effort session state sync used after reconnect
// or when rehydrating adapter-side state.
func (c *Client) SyncSessionSnapshot(ctx context.Context, sessionID string) (*SessionSnapshot, error) {
	if strings.TrimSpace(sessionID) == "" {
		return nil, fmt.Errorf("opencode: sync session snapshot: empty session id")
	}

	session, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return nil, fmt.Errorf("opencode: sync session snapshot get session: %w", err)
	}

	messages, err := c.sdk.Session.Messages(ctx, sessionID, opencode.SessionMessagesParams{})
	if err != nil {
		return nil, fmt.Errorf("opencode: sync session snapshot messages: %w", err)
	}

	todos, todoErr := c.fetchSessionTodos(ctx, sessionID)
	if todoErr != nil {
		log.Printf("opencode: sync session snapshot todo fetch failed for %s: %v", sessionID[:min(8, len(sessionID))], todoErr)
	}

	diff, diffErr := c.fetchSessionDiff(ctx, sessionID)
	if diffErr != nil {
		log.Printf("opencode: sync session snapshot diff fetch failed for %s: %v", sessionID[:min(8, len(sessionID))], diffErr)
	}

	snapshot := &SessionSnapshot{
		Session:  session,
		Todos:    todos,
		Diff:     diff,
		SyncedAt: time.Now(),
	}
	c.todoCache.Store(sessionID, append([]TodoItem(nil), todos...))
	c.diffCache.Store(sessionID, append([]FileDiff(nil), diff...))
	if messages != nil {
		snapshot.Messages = append(snapshot.Messages, (*messages)...)
	}

	return snapshot, nil
}

// ForkSession creates a new session using current session title as base.
func (c *Client) ForkSession(ctx context.Context, sessionID string) (string, error) {
	session, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return "", err
	}
	title := strings.TrimSpace(session.Title)
	if title == "" {
		title = "fork"
	}
	newSession, err := c.sdk.Session.New(ctx, opencode.SessionNewParams{Title: opencode.F(title + " (fork)")})
	if err != nil {
		return "", err
	}
	return newSession.ID, nil
}

func (c *Client) GetTodosForSession(sessionID string) []TodoItem {
	if v, ok := c.todoCache.Load(sessionID); ok {
		items := v.([]TodoItem)
		return append([]TodoItem(nil), items...)
	}
	return nil
}

func (c *Client) GetDiffForSession(sessionID string) []FileDiff {
	if v, ok := c.diffCache.Load(sessionID); ok {
		items := v.([]FileDiff)
		return append([]FileDiff(nil), items...)
	}
	return nil
}

func (c *Client) fetchSessionTodos(ctx context.Context, sessionID string) ([]TodoItem, error) {
	var todos []TodoItem
	if err := c.fetchSessionJSON(ctx, sessionID, "todo", &todos); err != nil {
		return nil, err
	}
	return todos, nil
}

func (c *Client) fetchSessionDiff(ctx context.Context, sessionID string) ([]FileDiff, error) {
	var diff []FileDiff
	if err := c.fetchSessionJSON(ctx, sessionID, "diff", &diff); err != nil {
		return nil, err
	}
	return diff, nil
}

func (c *Client) fetchSessionJSON(ctx context.Context, sessionID, suffix string, out interface{}) error {
	if c.endpoint == "" {
		return fmt.Errorf("opencode: fetch session %s unavailable: missing endpoint", suffix)
	}

	endpoint, err := url.JoinPath(c.endpoint, "session", sessionID, suffix)
	if err != nil {
		endpoint = fmt.Sprintf("%s/session/%s/%s", strings.TrimRight(c.endpoint, "/"), sessionID, suffix)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	c.applyAuthHeaders(req.Header)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}

	if err := json.Unmarshal(body, out); err == nil {
		return nil
	}

	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return err
	}
	if len(envelope.Data) == 0 {
		return nil
	}
	return json.Unmarshal(envelope.Data, out)
}

// SendMessageStreaming sends a message and calls the callback for each chunk of the response.
// 真正的流式实现：注册StreamingSessionHandler监听实时事件
func (c *Client) SendMessageStreaming(ctx context.Context, payload MessagePayload, callback StreamCallback) (Response, error) {
	return c.SendMessageStreamingWithEvents(ctx, payload, callback, nil)
}

// SendMessageStreamingWithEvents sends a streaming message with both legacy chunk callback
// and structured event callback.
func (c *Client) SendMessageStreamingWithEvents(ctx context.Context, payload MessagePayload, callback StreamCallback, eventCallback StreamEventCallback) (Response, error) {
	fmt.Println("payload is ", payload)
	if callback == nil {
		if eventCallback == nil {
			// 如果没有回调，直接使用普通模式
			return c.SendMessage(ctx, payload)
		}
		callback = func(string) error { return nil }
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
	if eventCallback != nil {
		if err := eventCallback(StreamEvent{Kind: StreamEventSessionReady, SessionID: sessionID, Content: sessionID, RawType: "session"}); err != nil {
			log.Printf("opencode: stream event callback error (session_ready): %v", err)
		}
	}
	if eventCallback != nil {
		if err := eventCallback(StreamEvent{Kind: StreamEventSessionReady, SessionID: sessionID, Content: sessionID, RawType: "session"}); err != nil {
			log.Printf("opencode: stream event callback error (session_ready): %v", err)
		}
	}

	// 3. 创建StreamingSessionHandler并注册
	handler := NewStreamingSessionHandler(sessionID, callback, eventCallback, func() {
		c.runningSessions.Delete(sessionID)
		c.UnregisterSessionHandler(sessionID)
	}, c, c, true, false)
	c.RegisterSessionHandler(sessionID, handler.HandleEvent)
	log.Printf("opencode: registered streaming handler for session %s", sessionID[:8])

	// 4. 使用goroutine异步发送消息
	responseChan := make(chan Response, 1)
	errorChan := make(chan error, 1)
	log.Printf("opencode: TRACE STREAM_WRAPPER dispatch SendMessage session=%s thread=%s channel=%s",
		sessionID,
		payload.ThreadID,
		payload.Channel,
	)

	go func() {
		log.Printf("opencode: TRACE STREAM_WRAPPER goroutine invoking SendMessage session=%s thread=%s channel=%s",
			sessionID,
			payload.ThreadID,
			payload.Channel,
		)
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
			c.recordMemAsync(payload, response.Reply)
			return response, nil

		case <-ticker.C:
			// 如果在async模式且handler已完成，返回结果
			if isAsyncMode && handler.IsCompleted() {
				log.Printf("opencode: ✅ async streaming completed via SSE for session %s (contentSent=%t, lastContentLen=%d)",
					sessionID[:8], handler.HasSentContent(), len(handler.GetLastContent()))
				c.recordMemAsync(payload, handler.GetLastContent())
				return c.finalizeAsyncStreamingResponse(sessionID, handler, callback, asyncResponse), nil
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
					c.recordMemAsync(payload, handler.GetLastContent())
					return c.finalizeAsyncStreamingResponse(sessionID, handler, callback, asyncResponse), nil
				}
			}

			// 如果已发送内容且超过30秒无新事件，认为可能完成
			// （从2分钟缩短到30秒，更快响应）
			if isAsyncMode && hasSentContent && timeSinceLastEvent > 30*time.Second {
				log.Printf("opencode: ⏱️ streaming idle for %v (has sent content), treating as completed for session %s",
					timeSinceLastEvent, sessionID[:8])
				c.recordMemAsync(payload, handler.GetLastContent())
				return c.finalizeAsyncStreamingResponse(sessionID, handler, callback, asyncResponse), nil
			}

			// 如果超过1分钟无任何事件（即使没发送内容），也认为完成
			// 这处理 OpenCode 不发送完成事件的情况
			if isAsyncMode && timeSinceLastEvent > 1*time.Minute {
				log.Printf("opencode: ⏱️ streaming timeout (no events for %v, hasSent=%t), treating as completed for session %s",
					timeSinceLastEvent, hasSentContent, sessionID[:8])
				c.recordMemAsync(payload, handler.GetLastContent())
				return c.finalizeAsyncStreamingResponse(sessionID, handler, callback, asyncResponse), nil
			}

			idleCheckCount++
		}
	}
}

// recordMemAsync fires a goroutine to record a conversation turn in the memory store.
// It is a no-op when no store is configured.
func (c *Client) recordMemAsync(payload MessagePayload, reply string) {
	if c.memStore == nil || strings.TrimSpace(reply) == "" {
		return
	}
	ms := c.memStore
	ch, uid, req, rep := payload.Channel, payload.UserID, payload.Content, reply
	go ms.RecordConversation(ch, uid, req, rep)
}

func (c *Client) finalizeAsyncStreamingResponse(sessionID string, handler *StreamingSessionHandler, callback StreamCallback, response Response) Response {
	if strings.TrimSpace(response.Reply) != "" {
		return response
	}
	if handler != nil && (handler.HasSentContent() || strings.TrimSpace(handler.GetLastContent()) != "") {
		return response
	}

	fallbackCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	reply, messageID, err := c.fetchLatestAssistantReply(fallbackCtx, sessionID)
	if err != nil {
		log.Printf("opencode: async fallback fetch failed for session %s: %v", sessionID[:min(8, len(sessionID))], err)
		return response
	}
	if strings.TrimSpace(reply) == "" {
		return response
	}

	if callback != nil {
		if err := callback(reply); err != nil {
			log.Printf("opencode: async fallback callback failed for session %s: %v", sessionID[:min(8, len(sessionID))], err)
		}
	}

	response.Reply = reply
	if response.MessageID == "" {
		response.MessageID = messageID
	}
	if response.Trace == "" {
		response.Trace = sessionID
	}

	log.Printf("opencode: async fallback delivered %d chars for session %s", len(reply), sessionID[:min(8, len(sessionID))])
	return response
}

func (c *Client) fetchLatestAssistantReply(ctx context.Context, sessionID string) (string, string, error) {
	var lastErr error

	for attempt := 0; attempt < 3; attempt++ {
		messages, err := c.sdk.Session.Messages(ctx, sessionID, opencode.SessionMessagesParams{})
		if err != nil {
			lastErr = err
		} else if messages != nil {
			for i := len(*messages) - 1; i >= 0; i-- {
				msg := (*messages)[i]
				if msg.Info.Role != opencode.MessageRoleAssistant {
					continue
				}

				text := extractTextFromSessionParts(msg.Parts)
				if strings.TrimSpace(text) == "" {
					continue
				}
				return text, msg.Info.ID, nil
			}
			lastErr = fmt.Errorf("assistant message not found in session %s", sessionID)
		}

		if attempt < 2 {
			select {
			case <-time.After(800 * time.Millisecond):
			case <-ctx.Done():
				return "", "", ctx.Err()
			}
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("assistant reply unavailable for session %s", sessionID)
	}
	return "", "", lastErr
}

func extractTextFromSessionParts(parts []opencode.Part) string {
	if len(parts) == 0 {
		return ""
	}

	textParts := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts)*2)
	appendUnique := func(text string) {
		text = strings.TrimSpace(text)
		if text == "" {
			return
		}
		if _, exists := seen[text]; exists {
			return
		}
		seen[text] = struct{}{}
		textParts = append(textParts, text)
	}
	for _, part := range parts {
		appendUnique(part.Text)

		appendUnique(extractTextFromPartState(part.State))

		switch p := part.AsUnion().(type) {
		case opencode.TextPart:
			appendUnique(p.Text)
		case opencode.ToolPart:
			appendUnique(p.State.Output)
			appendUnique(p.State.Error)
		}
	}

	return strings.TrimSpace(strings.Join(textParts, "\n"))
}

func extractTextFromPartState(state interface{}) string {
	if state == nil {
		return ""
	}

	collectFromMap := func(m map[string]interface{}) string {
		keys := []string{"output", "stdout", "stderr", "result", "error", "text", "content", "message"}
		chunks := make([]string, 0, len(keys))
		for _, k := range keys {
			if v, ok := m[k]; ok {
				switch typed := v.(type) {
				case string:
					if strings.TrimSpace(typed) != "" {
						chunks = append(chunks, strings.TrimSpace(typed))
					}
				case []interface{}:
					for _, item := range typed {
						if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
							chunks = append(chunks, strings.TrimSpace(s))
						}
					}
				}
			}
		}
		return strings.TrimSpace(strings.Join(chunks, "\n"))
	}

	switch typed := state.(type) {
	case opencode.ToolPartState:
		chunks := make([]string, 0, 2)
		if strings.TrimSpace(typed.Output) != "" {
			chunks = append(chunks, strings.TrimSpace(typed.Output))
		}
		if strings.TrimSpace(typed.Error) != "" {
			chunks = append(chunks, strings.TrimSpace(typed.Error))
		}
		if len(chunks) > 0 {
			return strings.Join(chunks, "\n")
		}
		if raw := strings.TrimSpace(typed.JSON.RawJSON()); raw != "" {
			var m map[string]interface{}
			if err := json.Unmarshal([]byte(raw), &m); err == nil {
				return collectFromMap(m)
			}
		}
	case map[string]interface{}:
		return collectFromMap(typed)
	case string:
		return strings.TrimSpace(typed)
	}

	return ""
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
	tokens, ok := c.tokenCount.Load(sessionID)
	if !ok {
		return 0
	}
	return tokens.(int)
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
func (c *Client) sendPromptWithRetry(ctx context.Context, sessionID string, parts []opencode.SessionPromptParamsPartUnion, modelOverride *opencode.SessionPromptParamsModel) (*opencode.SessionPromptResponse, error) {
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
		if modelOverride != nil {
			params.Model = opencode.F(*modelOverride)
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

// sendPromptAsync 调用 OpenCode 的 prompt_async 接口，立即返回，由事件流提供结果
func (c *Client) sendPromptAsync(ctx context.Context, sessionID string, parts []opencode.SessionPromptParamsPartUnion, modelOverride *opencode.SessionPromptParamsModel) error {
	if c.endpoint == "" {
		return fmt.Errorf("opencode: prompt_async unavailable: missing endpoint")
	}

	params := opencode.SessionPromptParams{
		Parts: opencode.F(parts),
	}
	if modelOverride != nil {
		params.Model = opencode.F(*modelOverride)
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
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

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

func userMemoryKey(channel, userID string) string {
	return strings.TrimSpace(channel) + ":" + strings.TrimSpace(userID)
}

func (c *Client) ListUserMemory(channel, userID string, limit int) []UserMemoryFact {
	key := userMemoryKey(channel, userID)
	v, ok := c.userMemory.Load(key)
	if !ok {
		return nil
	}
	facts := append([]UserMemoryFact(nil), v.([]UserMemoryFact)...)
	sort.SliceStable(facts, func(i, j int) bool {
		if facts[i].Importance == facts[j].Importance {
			return i < j
		}
		return facts[i].Importance > facts[j].Importance
	})
	if limit > 0 && len(facts) > limit {
		facts = facts[:limit]
	}
	return facts
}

func (c *Client) ClearUserMemory(channel, userID string) error {
	c.userMemory.Delete(userMemoryKey(channel, userID))
	return nil
}

func (c *Client) CompactUserMemory(channel, userID string) (int, error) {
	key := userMemoryKey(channel, userID)
	v, ok := c.userMemory.Load(key)
	if !ok {
		return 0, nil
	}
	items := v.([]UserMemoryFact)
	seen := make(map[string]struct{}, len(items))
	compacted := make([]UserMemoryFact, 0, len(items))
	for _, it := range items {
		sig := strings.ToLower(strings.TrimSpace(it.Category)) + "|" + strings.TrimSpace(it.Text)
		if _, exists := seen[sig]; exists {
			continue
		}
		seen[sig] = struct{}{}
		compacted = append(compacted, it)
	}
	removed := len(items) - len(compacted)
	c.userMemory.Store(key, compacted)
	return removed, nil
}

func (c *Client) PinUserMemory(channel, userID, text, category string) error {
	text = strings.TrimSpace(text)
	if text == "" {
		return fmt.Errorf("empty memory text")
	}
	if strings.TrimSpace(category) == "" {
		category = "preference"
	}
	key := userMemoryKey(channel, userID)
	var items []UserMemoryFact
	if v, ok := c.userMemory.Load(key); ok {
		items = append(items, v.([]UserMemoryFact)...)
	}
	items = append(items, UserMemoryFact{Text: text, Category: strings.ToLower(strings.TrimSpace(category)), Importance: 5})
	c.userMemory.Store(key, items)
	return nil
}

func (c *Client) RemoveUserMemoryByRank(channel, userID string, rank int) (bool, error) {
	if rank <= 0 {
		return false, nil
	}
	key := userMemoryKey(channel, userID)
	v, ok := c.userMemory.Load(key)
	if !ok {
		return false, nil
	}
	items := append([]UserMemoryFact(nil), v.([]UserMemoryFact)...)
	if rank > len(items) {
		return false, nil
	}
	idx := rank - 1
	items = append(items[:idx], items[idx+1:]...)
	c.userMemory.Store(key, items)
	return true, nil
}

func (c *Client) UnpinUserMemory(channel, userID, keyword string) (int, error) {
	key := userMemoryKey(channel, userID)
	v, ok := c.userMemory.Load(key)
	if !ok {
		return 0, nil
	}
	kw := strings.ToLower(strings.TrimSpace(keyword))
	items := v.([]UserMemoryFact)
	filtered := make([]UserMemoryFact, 0, len(items))
	removed := 0
	for _, it := range items {
		if kw != "" && strings.Contains(strings.ToLower(it.Text), kw) {
			removed++
			continue
		}
		filtered = append(filtered, it)
	}
	c.userMemory.Store(key, filtered)
	return removed, nil
}

func (c *Client) ExportUserMemory(channel, userID string) (string, error) {
	facts := c.ListUserMemory(channel, userID, 0)
	b, err := json.Marshal(facts)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func (c *Client) ImportUserMemory(channel, userID, payload string) (int, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(payload))
	if err != nil {
		return 0, err
	}
	var facts []UserMemoryFact
	if err := json.Unmarshal(raw, &facts); err != nil {
		return 0, err
	}
	c.userMemory.Store(userMemoryKey(channel, userID), facts)
	return len(facts), nil
}

func (c *Client) MergeImportUserMemory(channel, userID, payload string) (int, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(payload))
	if err != nil {
		return 0, err
	}
	var incoming []UserMemoryFact
	if err := json.Unmarshal(raw, &incoming); err != nil {
		return 0, err
	}
	key := userMemoryKey(channel, userID)
	merged := c.ListUserMemory(channel, userID, 0)
	merged = append(merged, incoming...)
	c.userMemory.Store(key, merged)
	_, _ = c.CompactUserMemory(channel, userID)
	final := c.ListUserMemory(channel, userID, 0)
	return len(final), nil
}

// RespondToPermission answers a permission request using the canonical English response.
// response must be one of: "once" (allow this time), "reject" (deny), "always" (always allow).
// This bypasses all Chinese text parsing — adapters should resolve their own locale first.
func (c *Client) RespondToPermission(ctx context.Context, permissionID, response string) error {
	switch response {
	case "once", "reject", "always":
	default:
		return fmt.Errorf("invalid permission response %q: must be once/reject/always", response)
	}

	q, ok := c.GetPendingQuestion(permissionID)
	if !ok {
		return fmt.Errorf("permission not found: %s", permissionID)
	}

	directory := q.Directory
	if directory == "" {
		directory = c.directory
	}

	log.Printf("opencode: RespondToPermission - ID=%s, sessionID=%s, response=%s, directory=%s",
		q.ID, q.SessionID, response, directory)

	// HTTP API first (more reliable than SDK)
	if err := c.answerPermissionViaHTTP(ctx, q, response); err != nil {
		log.Printf("opencode: HTTP permission API failed: %v, falling back to SDK", err)

		var responseParam opencode.SessionPermissionRespondParamsResponse
		switch response {
		case "always":
			responseParam = opencode.SessionPermissionRespondParamsResponseAlways
		case "reject":
			responseParam = opencode.SessionPermissionRespondParamsResponseReject
		default:
			responseParam = opencode.SessionPermissionRespondParamsResponseOnce
		}
		result, sdkErr := c.sdk.Session.Permissions.Respond(ctx, q.SessionID, permissionID,
			opencode.SessionPermissionRespondParams{
				Response:  opencode.F(responseParam),
				Directory: opencode.F(directory),
			})
		if sdkErr != nil {
			return fmt.Errorf("permission respond failed (HTTP: %v, SDK: %w)", err, sdkErr)
		}
		if result != nil {
			log.Printf("opencode: SDK permission respond succeeded, result=%v", *result)
		}
	}

	c.DeletePendingQuestion(permissionID)
	log.Printf("opencode: permission %s answered (%s) for session %s", permissionID, response, q.SessionID[:8])
	return nil
}

// answerPermission 回答权限请求（内部用，by AnswerQuestion path）
func (c *Client) answerPermission(ctx context.Context, q *Question, answer string) error {
	_, responseStr, ok := parsePermissionAnswer(answer)
	if !ok {
		return fmt.Errorf("invalid permission answer (use allow/reject/always), raw=%q bytes=% X", answer, []byte(answer))
	}
	log.Printf("opencode: answerPermission via parsePermissionAnswer - ID=%s, responseStr=%s", q.ID, responseStr)
	return c.RespondToPermission(ctx, q.ID, responseStr)
}

func parsePermissionAnswer(answer string) (opencode.SessionPermissionRespondParamsResponse, string, bool) {
	if strings.Contains(answer, "始终允许") {
		return opencode.SessionPermissionRespondParamsResponseAlways, "always", true
	}
	if strings.Contains(answer, "拒绝") || strings.Contains(answer, "不同意") {
		return opencode.SessionPermissionRespondParamsResponseReject, "reject", true
	}
	if strings.Contains(answer, "允许") || strings.Contains(answer, "同意") || strings.Contains(answer, "确认") {
		return opencode.SessionPermissionRespondParamsResponseOnce, "once", true
	}

	normalized := normalizePermissionAnswer(answer)
	if normalized == "" {
		log.Printf("opencode: permission answer parse failed: empty normalized answer, raw=%q, bytes=% X", answer, []byte(answer))
		return "", "", false
	}

	allowTokens := []string{"1", "allow", "yes", "允许", "同意", "确认", "ok", "okay", "y", "可以", "行", "鍏佽", "鍚屾剰", "纭", "鍙互"}
	rejectTokens := []string{"2", "deny", "no", "拒绝", "不同意", "取消", "n", "鎷掔粷", "涓嶅悓鎰", "鍙栨秷"}
	alwaysTokens := []string{"3", "always", "始终允许", "始终", "一直允许", "总是允许", "濮嬬粓鍏佽", "濮嬬粓", "涓€鐩村厑璁", "鎬绘槸鍏佽"}

	if containsAnyToken(normalized, alwaysTokens) {
		return opencode.SessionPermissionRespondParamsResponseAlways, "always", true
	}
	if containsAnyToken(normalized, rejectTokens) {
		return opencode.SessionPermissionRespondParamsResponseReject, "reject", true
	}
	if containsAnyToken(normalized, allowTokens) {
		return opencode.SessionPermissionRespondParamsResponseOnce, "once", true
	}

	// 兜底：先判定否定，再判定允许，避免“不允许”被误判为允许。
	if strings.Contains(normalized, "不允许") || strings.Contains(normalized, "拒绝") || strings.Contains(normalized, "不同意") || strings.Contains(normalized, "涓嶅厑璁") || strings.Contains(normalized, "鎷掔粷") {
		return opencode.SessionPermissionRespondParamsResponseReject, "reject", true
	}
	if strings.Contains(normalized, "始终") || strings.Contains(normalized, "always") || strings.Contains(normalized, "濮嬬粓") {
		return opencode.SessionPermissionRespondParamsResponseAlways, "always", true
	}
	if strings.Contains(normalized, "允许") || strings.Contains(normalized, "同意") || strings.Contains(normalized, "确认") || strings.Contains(normalized, "鍏佽") || strings.Contains(normalized, "鍚屾剰") {
		return opencode.SessionPermissionRespondParamsResponseOnce, "once", true
	}

	log.Printf("opencode: permission answer parse failed: raw=%q normalized=%q bytes=% X", answer, normalized, []byte(answer))

	return "", "", false
}

func normalizePermissionAnswer(answer string) string {
	answerLower := strings.TrimSpace(strings.ToLower(answer))
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) || unicode.IsPunct(r) || unicode.IsSymbol(r) || unicode.IsControl(r) {
			return -1
		}
		switch r {
		case ' ', '\t', '\n', '\r', '，', ',', '。', '.', '！', '!', '？', '?', '：', ':', ';', '；', '（', '）', '(', ')', '“', '”', '"', '\'', '、':
			return -1
		case '\u200b', '\u200c', '\u200d', '\ufeff':
			return -1
		default:
			return r
		}
	}, answerLower)
}

func containsAnyToken(text string, tokens []string) bool {
	for _, token := range tokens {
		t := normalizePermissionAnswer(token)
		if t != "" && strings.Contains(text, t) {
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
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
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
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

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
	enhanced := content

	if c.devCoreEnabled {
		msgCount := c.GetMessageCount(sessionID)
		if msgCount == 0 {
			if profile := strings.TrimSpace(c.GetDevCorePrompt()); profile != "" {
				enhanced = "[DEV_CORE_PROFILE]\n" + profile + "\n[/DEV_CORE_PROFILE]\n\n[USER_INPUT]\n" + content
			}
		}
	}

	if !c.enableSkillHint {
		return enhanced
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
		return enhanced
	}

	// 只在session的前几条消息添加提示，避免冗余
	msgCount := c.GetMessageCount(sessionID)
	if msgCount > 3 {
		return enhanced
	}

	return enhanced + hint
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
