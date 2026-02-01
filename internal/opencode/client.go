package opencode

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

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

// MessagePayload collects the metadata adapters send to OpenCode.
type MessagePayload struct {
	Channel   string            `json:"channel"`
	UserID    string            `json:"user_id"`
	ThreadID  string            `json:"thread_id,omitempty"`
	SessionID string            `json:"session_id,omitempty"`
	Content   string            `json:"content"`
	Agent     string            `json:"agent,omitempty"`     // 可选：指定使用的agent/skill名称
	Streaming bool              `json:"streaming,omitempty"` // 是否使用流式返回
	Metadata  map[string]string `json:"metadata,omitempty"`
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

// Client knows how to talk to the remote OpenCode service using the official SDK.
type Client struct {
	sdk             *opencode.Client
	eventHandlers   []EventHandler
	eventListenerMu sync.RWMutex
	sessions        sync.Map // map[threadID]sessionID
	sessionLocks    sync.Map // map[threadID]*sync.Mutex for preventing concurrent session operations
	messageCount    sync.Map // map[sessionID]int tracks messages per session
	tokenCount      sync.Map // map[sessionID]int tracks estimated tokens per session
	sessionSummary  sync.Map // map[sessionID]string stores session summaries
	modelConfig     sync.Map // map[sessionID]*ModelConfig caches model config per session
	requestCache    sync.Map // map[requestHash]*RequestRecord 请求去重缓存
	runningSessions sync.Map // map[sessionID]bool 跟踪正在运行的session
	directory       string
	timeout         time.Duration // 默认超时时间
	retryConfig     RetryConfig   // 重试配置
	enableSkillHint bool          // 是否在消息中添加skill提示
	skillHintCache  []string      // 缓存的可用skill列表
	skillCacheMu    sync.RWMutex
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

// NewClient builds a Client instance using the official OpenCode SDK.
func NewClient(endpoint, apiKey string, opts ...Option) *Client {
	client := &Client{
		sdk: opencode.NewClient(
			option.WithBaseURL(endpoint),
			// option.WithAPIKey(apiKey), // If SDK supports API key authentication
		),
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
		enableSkillHint: true, // 默认启用skill提示
	}

	for _, opt := range opts {
		opt(client)
	}

	// 启动后台清理协程
	go client.cleanupRequestCache()

	return client
}

// Ready reports if the client has enough data to operate.
func (c *Client) Ready() bool {
	return c.sdk != nil
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
	if sessionID == "" && payload.ThreadID != "" {
		if sid, ok := c.sessions.Load(payload.ThreadID); ok {
			sessionID = sid.(string)
		}
	}

	// Create new session if needed
	if sessionID == "" {
		session, err := c.sdk.Session.New(sessionCtx, opencode.SessionNewParams{
			Title: opencode.F(fmt.Sprintf("%s-%s", payload.Channel, payload.UserID)),
		})
		if err != nil {
			threadLock.Unlock()
			c.failRequest(requestHash)
			return Response{}, fmt.Errorf("opencode: create session: %w", err)
		}
		sessionID = session.ID
		if payload.ThreadID != "" {
			c.sessions.Store(payload.ThreadID, sessionID)
		}
		c.messageCount.Store(sessionID, 0)
		c.tokenCount.Store(sessionID, 0)

		// 获取模型配置
		go c.fetchAndCacheModelConfig(context.Background(), sessionID)

		log.Printf("opencode: created new session %s for thread %s", sessionID, payload.ThreadID)
	} else {
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

	// ========== 使用重试机制发送消息 ==========
	// 标记session为运行状态
	c.runningSessions.Store(sessionID, true)

	result, err := c.sendPromptWithRetry(ctx, sessionID, parts)

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

// IsSessionRunning checks if a session is currently running
func (c *Client) IsSessionRunning(sessionID string) bool {
	val, ok := c.runningSessions.Load(sessionID)
	if !ok {
		return false
	}
	return val.(bool)
}

// StartEventListener begins listening for OpenCode events via SSE.
func (c *Client) StartEventListener(ctx context.Context) error {
	stream := c.sdk.Event.ListStreaming(ctx, opencode.EventListParams{})

	go func() {
		defer stream.Close()

		for stream.Next() {
			event := stream.Current()

			// Dispatch to all registered handlers
			c.eventListenerMu.RLock()
			handlers := c.eventHandlers
			c.eventListenerMu.RUnlock()

			for _, handler := range handlers {
				if err := handler(ctx, &event); err != nil {
					log.Printf("opencode: event handler error: %v", err)
				}
			}
		}

		if err := stream.Err(); err != nil {
			log.Printf("opencode: event stream error: %v", err)
		}
	}()

	return nil
}

// RegisterEventHandler adds a new event handler dynamically.
func (c *Client) RegisterEventHandler(handler EventHandler) {
	c.eventListenerMu.Lock()
	defer c.eventListenerMu.Unlock()
	c.eventHandlers = append(c.eventHandlers, handler)
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

// SendMessageStreaming sends a message and calls the callback for each chunk of the response.
// 改进版本：使用定时器定期检查并发送进度更新
func (c *Client) SendMessageStreaming(ctx context.Context, payload MessagePayload, callback StreamCallback) (Response, error) {
	if callback == nil {
		// 如果没有回调，直接使用普通模式
		return c.SendMessage(ctx, payload)
	}

	// 使用goroutine异步发送消息，同时定期发送进度更新
	responseChan := make(chan Response, 1)
	errorChan := make(chan error, 1)

	// 启动消息发送
	go func() {
		response, err := c.SendMessage(ctx, payload)
		if err != nil {
			errorChan <- err
			return
		}
		responseChan <- response
	}()

	// 定时发送进度更新
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	progressMessages := []string{
		"⏳ 正在处理中...",
		"⏳ 仍在执行中，请稍候...",
		"⏳ 任务进行中，可能需要一些时间...",
		"⏳ 继续处理中...",
		"⏳ 快完成了，请耐心等待...",
	}
	progressIndex := 0

	for {
		select {
		case <-ctx.Done():
			return Response{}, ctx.Err()

		case err := <-errorChan:
			return Response{}, err

		case response := <-responseChan:
			// 发送最终结果
			if response.Reply != "" {
				if err := callback(response.Reply); err != nil {
					log.Printf("opencode: stream callback error: %v", err)
				}
			}
			return response, nil

		case <-ticker.C:
			// 定期发送进度提示（仅当还在等待时）
			if progressIndex < len(progressMessages) {
				if err := callback(progressMessages[progressIndex]); err != nil {
					log.Printf("opencode: progress callback error: %v", err)
				}
				progressIndex++
			} else {
				// 超过预设的进度消息数量，发送持续等待提示
				minutes := (progressIndex - len(progressMessages) + 1) * 10 / 60
				if minutes > 0 {
					msg := fmt.Sprintf("⏱️ 已等待 %d 分钟，任务仍在执行中...", minutes)
					if err := callback(msg); err != nil {
						log.Printf("opencode: progress callback error: %v", err)
					}
				}
				progressIndex++
			}
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
func (c *Client) sendPromptWithRetry(ctx context.Context, sessionID string, parts []opencode.SessionPromptParamsPartUnion) (*opencode.SessionPromptResponse, error) {
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

		result, err := c.sdk.Session.Prompt(attemptCtx, sessionID, opencode.SessionPromptParams{
			Parts: opencode.F(parts),
		})
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
