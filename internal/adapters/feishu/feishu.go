package feishu

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	nls "github.com/aliyun/alibabacloud-nls-go-sdk"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"

	"github.com/user/opencode-gateway/internal/adapters/base"
	"github.com/user/opencode-gateway/internal/opencode"
	"github.com/user/opencode-gateway/internal/scheduler"
)

const (
	MessageDeduplicationWindow          = 5 * time.Minute
	MessageDeduplicationCleanupInterval = 10 * time.Minute
	FeiShuAPIEndpoint                   = "https://open.feishu.cn/open-apis"
	maxFeishuTextLength                 = 1800
)

type Config struct {
	AppID             string
	AppSecret         string
	VerificationToken string
	EncryptKey        string
	UseWebSocket      bool
	AliyunNLSAkID     string
	AliyunNLSAkKey    string
	AliyunNLSAppKey   string
}

type Handler struct {
	client          *opencode.Client
	cfg             Config
	adapter         *base.BidirectionalAdapter
	wsClient        *larkws.Client
	cronScheduler   *scheduler.CronScheduler
	processedMsgIDs sync.Map
	cleanupOnce     sync.Once
	httpClient      *http.Client
	userTargets     sync.Map
	tokenMu         sync.Mutex
	accessToken     string
	tokenExpiry     time.Time
	debugMode       bool // Enable detailed event logging
	overflowPolicy  sync.Map
	overflowPending sync.Map
}

const (
	feishuOverflowPolicyAsk     = "ask"
	feishuOverflowPolicySummary = "summary"
	feishuOverflowPolicyNew     = "new"
)

type feishuTokenOverflowPendingState struct {
	UserID      string
	SessionID   string
	ThreadID    string
	Content     string
	Agent       string
	Attachments []opencode.Attachment
	Metadata    map[string]string
	Target      chatTarget
	CreatedAt   time.Time
}

func NewHandler(client *opencode.Client, cfg Config) *Handler {
	h := &Handler{
		client:    client,
		cfg:       cfg,
		debugMode: true, // Enable debug logging by default
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
	h.adapter = base.NewBidirectionalAdapter("feishu", h)
	return h
}

func (h *Handler) SetCronScheduler(cronScheduler *scheduler.CronScheduler) {
	h.cronScheduler = cronScheduler
}

// RegisterCronSession 注册定时任务session到adapter，使SSE事件能正确路由
// 实现 scheduler.SessionRegistrar 接口
func (h *Handler) RegisterCronSession(sessionID string, metadata map[string]interface{}) {
	cronUserID := fmt.Sprintf("cron:%s", sessionID[:min(12, len(sessionID))])
	h.adapter.MapUserToSession(cronUserID, sessionID)

	// 存储receive_id用于发送消息
	if receiveID, ok := metadata["receive_id"].(string); ok && receiveID != "" {
		h.adapter.MapSessionData(sessionID, "receive_id", receiveID)
	}
	if receiveIDType, ok := metadata["receive_id_type"].(string); ok && receiveIDType != "" {
		h.adapter.MapSessionData(sessionID, "receive_id_type", receiveIDType)
	}

	log.Printf("feishu: registered cron session %s (cronUser=%s)", sessionID[:min(8, len(sessionID))], cronUserID)
}

func (h *Handler) Start(ctx context.Context) error {
	if !h.cfg.UseWebSocket {
		log.Println("feishu: using traditional webhook mode")
		return nil
	}

	if h.cfg.AppID == "" || h.cfg.AppSecret == "" {
		return fmt.Errorf("feishu: AppID and AppSecret are required for WebSocket mode")
	}

	log.Println("==================================================")
	log.Println("feishu: 正在建立飞书长连接...")
	log.Printf("feishu: AppID: %s", h.cfg.AppID)
	if h.debugMode {
		log.Println("feishu: Debug模式已启用，将显示详细事件日志")
	}
	log.Println("==================================================")

	// Create event dispatcher with message and custom event handlers
	eventHandler := dispatcher.NewEventDispatcher("", "").
		OnP2MessageReceiveV1(h.onMessageReceived).
		OnCustomizedEvent("message", func(ctx context.Context, event *larkevent.EventReq) error {
			if h.debugMode {
				log.Printf("[飞书自定义事件] type: message, data: %s\n", string(event.Body))
			}
			return nil
		}).
		OnCustomizedEvent("im.message.message_read_v1", func(ctx context.Context, event *larkevent.EventReq) error {
			// 静默处理消息已读事件
			if h.debugMode {
				log.Printf("📖 [飞书消息已读事件] - 已忽略")
			}
			return nil
		}).
		OnCustomizedEvent("im.message.reaction.created_v1", func(ctx context.Context, event *larkevent.EventReq) error {
			// 静默处理消息表情回应事件
			if h.debugMode {
				log.Printf("👍 [飞书消息表情事件] - 已忽略")
			}
			return nil
		}).
		OnCustomizedEvent("im.message.reaction.deleted_v1", func(ctx context.Context, event *larkevent.EventReq) error {
			// 静默处理表情移除事件
			if h.debugMode {
				log.Printf("👎 [飞书表情移除事件] - 已忽略")
			}
			return nil
		})

	// Set log level based on debug mode
	logLevel := larkcore.LogLevelInfo
	if h.debugMode {
		logLevel = larkcore.LogLevelDebug
	}

	h.wsClient = larkws.NewClient(h.cfg.AppID, h.cfg.AppSecret,
		larkws.WithEventHandler(eventHandler),
		larkws.WithLogLevel(logLevel),
	)

	go func() {
		log.Println("feishu: 启动WebSocket客户端连接...")
		if err := h.wsClient.Start(ctx); err != nil {
			log.Printf("❌ feishu websocket连接错误: %v", err)
		} else {
			log.Println("✅ feishu: WebSocket连接成功！")
			log.Println("📱 向机器人发送消息即可开始测试交互")
		}
	}()

	log.Println("feishu: WebSocket客户端已启动（后台连接中）")
	return nil
}

func (h *Handler) Stop() {
	if h.wsClient != nil {
		log.Println("feishu: closing WebSocket client...")
		time.Sleep(100 * time.Millisecond)
		log.Println("feishu: WebSocket client closed")
	}
}

func (h *Handler) cleanupProcessedMessages() {
	ticker := time.NewTicker(MessageDeduplicationCleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		cleanedCount := 0

		h.processedMsgIDs.Range(func(key, value interface{}) bool {
			if ts, ok := value.(time.Time); ok {
				if now.Sub(ts) > MessageDeduplicationWindow {
					h.processedMsgIDs.Delete(key)
					cleanedCount++
				}
			}
			return true
		})

		if cleanedCount > 0 {
			log.Printf("feishu: cleaned up %d expired message IDs", cleanedCount)
		}
	}
}

func (h *Handler) handleIncomingMessage(ctx context.Context, msg incomingMessage) (string, error) {
	if strings.TrimSpace(msg.Content) == "" {
		return "", fmt.Errorf("feishu: empty message content")
	}

	target := h.rememberChatTarget(msg)
	userLabel := msg.UserID
	if len(userLabel) > 8 {
		userLabel = userLabel[:8]
	}
	log.Printf("feishu: routing message from user %s (chat=%s)", userLabel, msg.ChatType)

	// ========== 检查是否是快速回复（权限或问题回答）==========
	content := strings.TrimSpace(msg.Content)
	if handled, err := h.handleTokenOverflowQuickReply(ctx, target, msg, content); handled || err != nil {
		if handled {
			return "handled", nil
		}
		return "", err
	}
	sessionID, hasSession := h.adapter.GetSessionForUser(msg.UserID)

	if hasSession {
		log.Printf("feishu: checking quick reply '%s' for user %s (session: %s)", content, userLabel, sessionID[:min(8, len(sessionID))])

		// 先尝试查找待处理的权限请求
		permission, ok := h.client.GetLatestPendingPermission(sessionID)
		if ok {
			log.Printf("feishu: user %s replied '%s' (bytes=% X) to permission %s (session: %s)",
				userLabel, content, []byte(content), permission.ID, sessionID[:min(8, len(sessionID))])

			englishResponse := parsePermissionReply(content)
			if englishResponse == "" {
				log.Printf("feishu: unrecognized permission reply from %s: raw=%q bytes=% X", userLabel, content, []byte(content))
				_ = h.sendTextChunks(ctx, target, "❌ 未能识别权限回复，请回复：允许 / 拒绝 / 始终允许")
				return "handled", nil
			}

			log.Printf("feishu: resolved permission reply '%s' -> %s for %s", content, englishResponse, permission.ID)

			if err := h.client.RespondToPermission(ctx, permission.ID, englishResponse); err != nil {
				log.Printf("feishu: RespondToPermission failed for %s: %v", permission.ID, err)
				_ = h.sendTextChunks(ctx, target, "❌ 权限回复失败，请重试")
				return "", err
			}

			displayMap := map[string]string{"once": "允许", "reject": "拒绝", "always": "始终允许"}
			msg := fmt.Sprintf("✅ 已回复: %s\n\n⏳ 等待 OpenCode 继续执行...", displayMap[englishResponse])
			_ = h.sendTextChunks(ctx, target, msg)
			log.Printf("feishu: successfully answered permission %s (%s)", permission.ID, englishResponse)
			return "handled", nil
		}

		// 再尝试查找待处理的普通问题
		question, ok := h.client.GetLatestPendingQuestion(sessionID)
		if ok {
			log.Printf("feishu: user %s replied '%s' to question %s", userLabel, content, question.ID)
			log.Printf("feishu: question details - ID: %s, Options count: %d, Questions count: %d",
				question.ID, len(question.Options), len(question.Questions))

			// 解析答案 - 支持多种格式
			answer := content
			if strings.Contains(content, ";") {
				log.Printf("feishu: using multi-question answer format: %s", content)
			} else if strings.Contains(content, ",") {
				log.Printf("feishu: using multi-select answer format: %s", content)
			} else if idx, err := strconv.Atoi(strings.TrimSpace(content)); err == nil {
				log.Printf("feishu: numeric input '%s', converting to option", content)

				if len(question.Questions) > 0 && len(question.Questions[0].Options) > 0 {
					qi := question.Questions[0]
					if idx >= 1 && idx <= len(qi.Options) {
						answer = qi.Options[idx-1].Label
						log.Printf("feishu: converted %d -> %s (from Questions array)", idx, answer)
					} else {
						log.Printf("feishu: index %d out of range (1-%d), using original", idx, len(qi.Options))
					}
				} else if len(question.Options) > 0 {
					if idx >= 1 && idx <= len(question.Options) {
						answer = question.Options[idx-1]
						log.Printf("feishu: converted %d -> %s (from Options array)", idx, answer)
					} else {
						log.Printf("feishu: index %d out of range (1-%d), using original", idx, len(question.Options))
					}
				}
			} else {
				log.Printf("feishu: using text input as answer: %s", content)
			}

			log.Printf("feishu: submitting answer '%s' for question %s (original input: %s)", answer, question.ID, content)

			if err := h.client.AnswerQuestion(ctx, question.ID, answer); err != nil {
				msg := fmt.Sprintf("❌ 回复失败: %v\n\n问题ID: %s\n答案: %s", err, question.ID, answer)
				_ = h.sendTextChunks(ctx, target, msg)
				return "", err
			}

			msg := fmt.Sprintf("✅ 已回复: %s\n\n⏳ 等待 OpenCode 继续处理...", answer)
			_ = h.sendTextChunks(ctx, target, msg)
			log.Printf("feishu: successfully answered question %s", question.ID)
			return "handled", nil
		}

		log.Printf("feishu: no pending question/permission for session %s, treating as new message", sessionID[:min(8, len(sessionID))])
	}
	// ========== 快速回复检查结束 ==========

	// ========== 处理特殊命令 ==========
	// Handle special commands
	if content == "/skills" || content == "/agents" {
		return h.handleListSkills(ctx, target)
	}

	if content == "/help" || content == "帮助" {
		return h.handleHelp(ctx, target)
	}

	// Handle /abort command to abort running session
	if content == "/abort" || content == "/stop" || content == "停止" {
		return h.handleAbort(ctx, target, msg.UserID)
	}

	// Handle /new or /reset command to create new session
	if content == "/new" || content == "/reset" || content == "新会话" {
		return h.handleNewSession(ctx, target, msg.UserID, msg.ChatID)
	}

	// Handle /sessions or /list command to list sessions
	if content == "/sessions" || content == "/list" {
		return h.handleListSessions(ctx, target)
	}

	// Handle /status command to check session status
	if content == "/status" || content == "状态" {
		return h.handleStatus(ctx, target, msg.UserID)
	}

	// Handle /summary command to trigger context compression
	if content == "/summary" || content == "压缩" || content == "总结" {
		return h.handleSummary(ctx, target, msg.UserID)
	}

	// Handle /clear command to clear/delete current session
	if content == "/clear" || content == "清除" {
		return h.handleClear(ctx, target, msg.UserID, msg.ChatID)
	}

	// Handle /model command to get/set model
	if strings.HasPrefix(content, "/model") || strings.HasPrefix(content, "/provider") {
		return h.handleModel(ctx, target, msg.UserID, content)
	}

	// Handle /thinking command to toggle reasoning output
	if strings.HasPrefix(content, "/thinking") {
		return h.handleThinking(ctx, target, content)
	}

	// Handle /final command to toggle final-only output mode
	if strings.HasPrefix(content, "/final") {
		return h.handleFinal(ctx, target, content)
	}

	// Handle /steps command to toggle step visibility
	if strings.HasPrefix(content, "/steps") || strings.HasPrefix(content, "/step") {
		return h.handleSteps(ctx, target, content)
	}

	// Handle /config command to view configuration
	if content == "/config" || content == "配置" {
		return h.handleConfig(ctx, target, msg.UserID)
	}

	// Handle /cmd command to execute skill scripts directly
	if strings.HasPrefix(content, "/cmd ") {
		command := strings.TrimPrefix(content, "/cmd ")
		return h.handleExecuteCommand(ctx, target, msg.UserID, command)
	}

	// Handle /refresh command to refresh skill cache
	if content == "/refresh" {
		if err := h.client.RefreshSkills(ctx); err != nil {
			_ = h.sendTextChunks(ctx, target, "❌ 刷新技能缓存失败: "+err.Error())
		} else {
			_ = h.sendTextChunks(ctx, target, "✅ 技能缓存已刷新")
		}
		return "handled", nil
	}

	// Handle /fork command to fork the current session
	if content == "/fork" {
		return h.handleFork(ctx, target, msg.UserID)
	}

	// Handle /undo command to revert last message
	if content == "/undo" || content == "/revert" || content == "撤销" {
		return h.handleUndo(ctx, target, msg.UserID)
	}

	// Handle /redo command to unrevert (redo) last undone message
	if content == "/redo" || content == "/unrevert" || content == "重做" {
		return h.handleRedo(ctx, target, msg.UserID)
	}

	// Handle /todo command to show current todo list
	if content == "/todo" || content == "/todos" || content == "任务" {
		return h.handleTodo(ctx, target, msg.UserID)
	}

	// Handle /diff command to show current file changes
	if content == "/diff" || content == "/changes" || content == "变更" {
		return h.handleDiff(ctx, target, msg.UserID)
	}

	// Handle /crontask command for scheduled tasks
	if strings.HasPrefix(content, "/crontask") {
		return h.handleCronTask(ctx, target, msg.UserID, content)
	}

	// Handle /answer command to answer pending questions
	if strings.HasPrefix(content, "/answer ") {
		return h.handleAnswer(ctx, target, msg.UserID, content)
	}
	// ========== 特殊命令处理结束 ==========

	// Parse agent specification: @agent_name message content
	var agentName string
	if strings.HasPrefix(content, "@") {
		parts := strings.SplitN(content[1:], " ", 2)
		if len(parts) == 2 {
			agentName = parts[0]
			content = parts[1]
			log.Printf("feishu: using agent '%s' for message", agentName)
		}
	}

	// 如果有视频 skill 且用户没有指定 agent，使用视频 skill
	if msg.VideoSkillName != "" && agentName == "" {
		agentName = msg.VideoSkillName
		log.Printf("feishu: auto-using video skill '%s' for video message", agentName)
	}

	sessionID, _ = h.adapter.GetSessionForUser(msg.UserID)
	var fullReply strings.Builder
	var fullReplyMu sync.Mutex // 保护 fullReply / lastSentLength / lastUpdateTime 的并发访问

	// 使用独立的 context，避免被飞书 SDK 的事件处理超时影响
	sendCtx, sendCancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer sendCancel()

	// 中间更新追踪（与 DingTalk 适配器保持一致，流式发送避免最终消息丢失）
	var lastSentLength int
	lastUpdateTime := time.Now()
	const minUpdateInterval = 5 * time.Second
	const minUpdateChars = 300
	var thinkingBuffer strings.Builder
	thinkingSent := false
	bufferFinalUntilFlush := h.client.IsFinalOnlyEnabled() || h.client.IsThinkingEnabled()

	formatThinkingBlock := func(content string) string {
		trimmed := strings.TrimSpace(content)
		if trimmed == "" {
			return ""
		}
		return "思考过程：\n" + trimmed + "\n思考结束"
	}

	threadID := msg.ChatID
	if threadID == "" {
		threadID = msg.MessageID
	}
	retryContent := content
	retryMetadata := map[string]string{}

	callback := func(chunk string) error {
		raw := chunk
		trimmed := strings.TrimSpace(chunk)
		if h.isTokenOverflowErrorText(trimmed) {
			state := &feishuTokenOverflowPendingState{
				UserID:      msg.UserID,
				SessionID:   sessionID,
				ThreadID:    threadID,
				Content:     retryContent,
				Agent:       agentName,
				Attachments: append([]opencode.Attachment(nil), msg.Attachments...),
				Metadata:    cloneStringMap(retryMetadata),
				Target:      target,
				CreatedAt:   time.Now(),
			}
			if state.SessionID == "" {
				if sid, ok := h.adapter.GetSessionForUser(msg.UserID); ok {
					state.SessionID = sid
				}
			}

			switch h.getTokenOverflowPolicy(msg.UserID) {
			case feishuOverflowPolicySummary:
				h.storeTokenOverflowPending(msg.UserID, state)
				_ = h.sendTextChunks(sendCtx, target, "⚠️ 检测到上下文超限，已按偏好自动执行“压缩并继续”，请稍候...")
				go h.executeTokenOverflowDecision(context.Background(), msg.UserID, "summary")
			case feishuOverflowPolicyNew:
				h.storeTokenOverflowPending(msg.UserID, state)
				_ = h.sendTextChunks(sendCtx, target, "⚠️ 检测到上下文超限，已按偏好自动执行“新会话并继续”，请稍候...")
				go h.executeTokenOverflowDecision(context.Background(), msg.UserID, "new")
			default:
				h.storeTokenOverflowPending(msg.UserID, state)
				_ = h.sendTextChunks(sendCtx, target, h.buildTokenOverflowPrompt())
			}
			return nil
		}

		if strings.HasPrefix(chunk, opencode.ThinkingSignalPrefix) {
			thinkingDelta := strings.TrimPrefix(chunk, opencode.ThinkingSignalPrefix)
			if strings.TrimSpace(thinkingDelta) == "" {
				return nil
			}
			fullReplyMu.Lock()
			thinkingBuffer.WriteString(thinkingDelta)
			fullReplyMu.Unlock()
			log.Printf("feishu: 🧠 buffered thinking chunk (len=%d)", len(thinkingDelta))
			return nil
		}

		if strings.HasPrefix(chunk, opencode.ToolSignalPrefix) {
			toolMsg := strings.TrimSpace(strings.TrimPrefix(chunk, opencode.ToolSignalPrefix))
			if toolMsg == "" {
				return nil
			}
			if err := h.sendTextChunks(sendCtx, target, toolMsg); err != nil {
				log.Printf("feishu: ⚠️ failed to send tool message: %v", err)
				return err
			}
			return nil
		}

		if strings.HasPrefix(chunk, opencode.StepSignalPrefix) {
			stepMsg := strings.TrimSpace(strings.TrimPrefix(chunk, opencode.StepSignalPrefix))
			if stepMsg == "" {
				return nil
			}
			if err := h.sendTextChunks(sendCtx, target, stepMsg); err != nil {
				log.Printf("feishu: ⚠️ failed to send step message: %v", err)
				return err
			}
			return nil
		}

		if strings.HasPrefix(chunk, opencode.TodoSignalPrefix) {
			todoMsg := strings.TrimSpace(strings.TrimPrefix(chunk, opencode.TodoSignalPrefix))
			if todoMsg == "" {
				return nil
			}
			if err := h.sendTextChunks(sendCtx, target, todoMsg); err != nil {
				log.Printf("feishu: ⚠️ failed to send todo progress: %v", err)
				return err
			}
			return nil
		}

		// FlushSignal: session 结束，立即发送所有尚未发送的内容
		if chunk == opencode.FlushSignal {
			fullReplyMu.Lock()
			thinkingMsg := ""
			if !thinkingSent {
				thinkingMsg = formatThinkingBlock(thinkingBuffer.String())
			}
			fullReplyMu.Unlock()

			if thinkingMsg != "" {
				log.Printf("feishu: 📤 flush signal: sending thinking block (%d bytes)", len(thinkingMsg))
				if err := h.sendTextChunks(sendCtx, target, thinkingMsg); err != nil {
					log.Printf("feishu: ⚠️ flush thinking send failed: %v", err)
				} else {
					fullReplyMu.Lock()
					thinkingSent = true
					fullReplyMu.Unlock()
				}
			}

			fullReplyMu.Lock()
			toSend := fullReply.String()[lastSentLength:]
			sentUpTo := fullReply.Len()
			fullReplyMu.Unlock()
			if len(toSend) > 0 {
				log.Printf("feishu: 📤 flush signal: sending final %d bytes", len(toSend))
				if err := h.sendTextChunks(sendCtx, target, toSend); err != nil {
					log.Printf("feishu: ⚠️ flush send failed: %v", err)
					// don't update lastSentLength; safety-net final send will retry
				} else {
					fullReplyMu.Lock()
					lastSentLength = sentUpTo
					fullReplyMu.Unlock()
					log.Printf("feishu: ✅ flush send done (%d bytes)", sentUpTo)
				}
			}
			return nil
		}

		if trimmed == "" {
			return nil
		}
		if strings.HasPrefix(trimmed, "ses_") && len(trimmed) < 100 {
			h.adapter.MapUserToSession(msg.UserID, trimmed)
			return nil
		}
		if isImmediateChunk(trimmed) {
			log.Printf("feishu: 📤 sending immediate message: %s", trimmed[:min(50, len(trimmed))])
			if err := h.sendTextChunks(sendCtx, target, raw); err != nil {
				log.Printf("feishu: ⚠️ failed to send immediate message: %v", err)
				// 发送失败时的处理：
				// - 对于"正在处理中"提示：只返回error保持waiting timer活跃，不加入fullReply（避免污染最终内容）
				// - 对于其他重要消息（权限、问题等）：加入fullReply确保不丢失
				fullReplyMu.Lock()
				if !strings.Contains(trimmed, "正在努力处理中") && !strings.Contains(trimmed, "正在处理") {
					fullReply.WriteString(raw)
				}
				fullReplyMu.Unlock()
				return err
			}
			log.Printf("feishu: ✅ immediate message sent successfully")
			return nil
		}

		fullReplyMu.Lock()
		fullReply.WriteString(raw)

		// 流式中间更新：每 5 秒且累积 300+ 新字符时发送一次，避免最终消息丢失
		timeSinceLastUpdate := time.Since(lastUpdateTime)
		currentLen := fullReply.Len()
		newContentLen := currentLen - lastSentLength
		var toSend string
		var prevLastSent int
		shouldSend := !bufferFinalUntilFlush && newContentLen >= minUpdateChars && timeSinceLastUpdate >= minUpdateInterval
		if shouldSend {
			toSend = fullReply.String()[lastSentLength:]
			prevLastSent = lastSentLength
			_ = prevLastSent
		}
		fullReplyMu.Unlock()

		if shouldSend {
			log.Printf("feishu: 📤 sending intermediate update (%d new chars, interval: %s)", newContentLen, timeSinceLastUpdate)
			if err := h.sendTextChunks(sendCtx, target, toSend); err != nil {
				log.Printf("feishu: ⚠️ failed to send intermediate update: %v", err)
				// 发送失败不中断流，下次会重试（包含这部分内容）
			} else {
				fullReplyMu.Lock()
				lastSentLength = currentLen
				lastUpdateTime = time.Now()
				fullReplyMu.Unlock()
			}
		}
		return nil
	}

	sendContent := msg.Content
	metadata := map[string]string{
		"message_id":      msg.MessageID,
		"message_type":    msg.MessageType,
		"chat_type":       msg.ChatType,
		"chat_id":         msg.ChatID,
		"receive_id":      target.receiveID,
		"receive_id_type": target.receiveIDType,
	}
	if len(msg.MediaFiles) > 0 && (msg.MessageType == "file" || msg.MessageType == "video") {
		taskSessionID := sessionID
		if strings.TrimSpace(taskSessionID) == "" {
			taskSessionID = "new"
		}
		mediaCtx := base.MediaTaskContext{
			Platform:    "feishu",
			MessageType: msg.MessageType,
			UserID:      msg.UserID,
			SessionID:   taskSessionID,
			MessageID:   msg.MessageID,
			Files:       msg.MediaFiles,
		}
		if mediaMD, mdErr := base.BuildMediaMetadata(mediaCtx); mdErr != nil {
			log.Printf("feishu: ⚠️ failed to build media metadata: %v", mdErr)
		} else {
			for k, v := range mediaMD {
				metadata[k] = v
			}
			sendContent = base.BuildMediaPromptPrefix(mediaCtx) + sendContent
		}
	}
	retryContent = sendContent
	retryMetadata = cloneStringMap(metadata)

	response, err := h.client.SendMessageStreaming(sendCtx, opencode.MessagePayload{
		Channel:     "feishu",
		UserID:      msg.UserID,
		ThreadID:    threadID,
		SessionID:   sessionID,
		Content:     sendContent,
		Agent:       agentName,
		Streaming:   true,
		Attachments: msg.Attachments,
		Metadata:    metadata,
	}, callback)

	log.Printf("feishu: 🔍 SendMessageStreaming returned - user=%s, err=%v, reply_len=%d, accumulated_len=%d",
		userLabel, err, len(response.Reply), fullReply.Len())

	if err != nil {
		errMsg := fmt.Sprintf("❌ 处理失败: %v", err)
		_ = h.sendTextChunks(sendCtx, target, errMsg)
		return "", err
	}

	if response.SessionID != "" {
		h.adapter.MapUserToSession(msg.UserID, response.SessionID)
		h.adapter.MapSessionData(response.SessionID, "receive_id", target.receiveID)
		h.adapter.MapSessionData(response.SessionID, "receive_id_type", target.receiveIDType)
	}

	// Send final reply - handle both sync and async modes
	if response.Reply != "" {
		// Sync mode - response.Reply contains the full response
		fullReplyMu.Lock()
		thinkingMsg := ""
		if !thinkingSent {
			thinkingMsg = formatThinkingBlock(thinkingBuffer.String())
			thinkingSent = thinkingMsg != ""
		}
		fullReplyMu.Unlock()

		if thinkingMsg != "" {
			if err := h.sendTextChunks(sendCtx, target, thinkingMsg); err != nil {
				log.Printf("feishu: failed to send thinking block: %v", err)
			}
		}

		if err := h.sendTextChunks(sendCtx, target, response.Reply); err != nil {
			log.Printf("feishu: failed to send sync reply: %v", err)
			return response.Reply, err
		}
		log.Printf("feishu: sent sync reply to user %s (%d chars)", userLabel, len(response.Reply))
	} else {
		// Async mode - send only the remaining unsent content
		// 用 mutex 确保读到 callback goroutine 写入的完整内容
		fullReplyMu.Lock()
		thinkingMsg := ""
		if !thinkingSent {
			thinkingMsg = formatThinkingBlock(thinkingBuffer.String())
			thinkingSent = thinkingMsg != ""
		}
		accumulatedContent := fullReply.String()
		sentSoFar := lastSentLength
		fullReplyMu.Unlock()

		if thinkingMsg != "" {
			if err := h.sendTextChunks(sendCtx, target, thinkingMsg); err != nil {
				log.Printf("feishu: failed to send thinking block: %v", err)
			}
		}

		unsentContent := accumulatedContent[sentSoFar:]
		if len(unsentContent) > 0 {
			log.Printf("feishu: 📤 sending final message (%d total bytes, %d unsent bytes)",
				len(accumulatedContent), len(unsentContent))
			if err := h.sendTextChunks(sendCtx, target, unsentContent); err != nil {
				log.Printf("feishu: ❌ failed to send final message: %v", err)
				return accumulatedContent, err
			}
			log.Printf("feishu: ✅ sent final message to user %s (%d bytes total, %d new)",
				userLabel, len(accumulatedContent), len(unsentContent))
		} else if len(accumulatedContent) == 0 {
			// No accumulated content (might be interactive task like permission request)
			log.Printf("feishu: async mode completed for user %s with no accumulated content", userLabel)
		} else {
			log.Printf("feishu: async mode completed for user %s, all content already sent (%d bytes)",
				userLabel, len(accumulatedContent))
		}
	}

	// Return the content that was sent (for logging purposes)
	if response.Reply != "" {
		return response.Reply, nil
	}
	return fullReply.String(), nil
}

func (h *Handler) onMessageReceived(ctx context.Context, event *larkim.P2MessageReceiveV1) error {
	h.cleanupOnce.Do(func() {
		go h.cleanupProcessedMessages()
	})

	if event == nil || event.Event == nil || event.Event.Message == nil || event.Event.Message.Content == nil {
		return fmt.Errorf("feishu: invalid event payload")
	}

	// Display detailed event log in debug mode
	if h.debugMode {
		log.Println("==================================================")
		log.Println("📩 [收到飞书消息事件]")
		log.Printf("完整事件数据:\n%s\n", larkcore.Prettify(event))
		log.Println("==================================================")
	}

	messageID := ""
	if event.Event.Message.MessageId != nil {
		messageID = *event.Event.Message.MessageId
	}
	if messageID != "" && !h.shouldProcessMessage(messageID) {
		log.Printf("⚠️  feishu: 重复消息 msgId=%s, 已忽略", messageID)
		return nil
	}

	messageType := "text"
	if event.Event.Message.MessageType != nil && *event.Event.Message.MessageType != "" {
		messageType = *event.Event.Message.MessageType
	}

	if event.Event.Sender == nil || event.Event.Sender.SenderId == nil || event.Event.Sender.SenderId.OpenId == nil {
		log.Printf("❌ feishu: 缺少发送者信息")
		return fmt.Errorf("feishu: missing sender info")
	}
	userID := *event.Event.Sender.SenderId.OpenId

	mediaSessionID := "new"
	if existingSessionID, ok := h.adapter.GetSessionForUser(userID); ok && strings.TrimSpace(existingSessionID) != "" {
		mediaSessionID = strings.TrimSpace(existingSessionID)
	}

	content, attachments, mediaFiles, videoSkillName, err := h.parseFeishuMessageContent(ctx, messageType, *event.Event.Message.Content, messageID, userID, mediaSessionID)
	if err != nil {
		log.Printf("❌ feishu: 解析消息内容失败: %v", err)
		return fmt.Errorf("feishu: parse content: %w", err)
	}
	if content == "" {
		log.Printf("⚠️  feishu: 消息内容为空 (type=%s)", messageType)
		return nil
	}

	if h.debugMode {
		log.Printf("📝 消息内容: %s", content)
	}

	chatID := ""
	if event.Event.Message.ChatId != nil {
		chatID = *event.Event.Message.ChatId
	}
	chatType := "p2p"
	if event.Event.Message.ChatType != nil && *event.Event.Message.ChatType != "" {
		chatType = *event.Event.Message.ChatType
	}

	if h.debugMode {
		log.Printf("👤 发送者: %s", userID[:min(12, len(userID))])
		log.Printf("💬 会话类型: %s, 会话ID: %s", chatType, chatID[:min(12, len(chatID))])
		log.Printf("📋 消息类型: %s, 消息ID: %s", messageType, messageID[:min(12, len(messageID))])
	}

	_, err = h.handleIncomingMessage(ctx, incomingMessage{
		UserID:         userID,
		ChatID:         chatID,
		ChatType:       chatType,
		MessageID:      messageID,
		Content:        content,
		MessageType:    messageType,
		Attachments:    attachments,
		MediaFiles:     mediaFiles,
		VideoSkillName: videoSkillName,
	})

	if err != nil {
		log.Printf("❌ feishu: 处理消息失败: %v", err)
	} else if h.debugMode {
		log.Println("✅ feishu: 消息处理完成")
		log.Println("==================================================")
	}

	return err
}

func (h *Handler) GetAdapter() *base.BidirectionalAdapter {
	return h.adapter
}

func (h *Handler) SendMessage(ctx context.Context, channel, userID, content string) error {
	if channel != "" && userID != "" {
		receiveIDType := channel
		if receiveIDType == "" {
			receiveIDType = "open_id"
		}
		target := chatTarget{
			receiveID:     userID,
			receiveIDType: receiveIDType,
		}
		return h.sendTextChunks(ctx, target, content)
	}
	target, err := h.resolveChatTarget(userID, channel)
	if err != nil {
		return err
	}
	return h.sendTextChunks(ctx, target, content)
}

func (h *Handler) sendTextChunks(ctx context.Context, target chatTarget, content string) error {
	chunks := chunkText(content, maxFeishuTextLength)
	if len(chunks) == 0 {
		return nil
	}
	for _, chunk := range chunks {
		if err := h.sendTextMessage(ctx, target, chunk); err != nil {
			return err
		}
		// Small delay helps avoid hitting strict rate limits in group chats
		time.Sleep(150 * time.Millisecond)
	}
	return nil
}

func (h *Handler) sendTextMessage(ctx context.Context, target chatTarget, content string) error {
	if target.receiveID == "" {
		return fmt.Errorf("feishu: missing receive_id for message send")
	}
	if target.receiveIDType == "" {
		target.receiveIDType = "open_id"
	}

	// 使用独立 context，避免父 context 取消时中断消息发送
	httpCtx, httpCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer httpCancel()

	token, err := h.getAccessToken(httpCtx)
	if err != nil {
		return fmt.Errorf("feishu: get access token: %w", err)
	}

	contentBody, err := json.Marshal(map[string]string{"text": content})
	if err != nil {
		return fmt.Errorf("feishu: marshal content: %w", err)
	}

	payload := map[string]interface{}{
		"receive_id": target.receiveID,
		"msg_type":   "text",
		"content":    string(contentBody),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("feishu: marshal payload: %w", err)
	}

	apiURL := fmt.Sprintf("%s/im/v1/messages?receive_id_type=%s", FeiShuAPIEndpoint, target.receiveIDType)
	req, err := http.NewRequestWithContext(httpCtx, http.MethodPost, apiURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("feishu: create message request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	receiveLabel := target.receiveID
	if len(receiveLabel) > 12 {
		receiveLabel = receiveLabel[:12]
	}
	log.Printf("feishu: sending message via API - receive_id=%s type=%s url=%s",
		receiveLabel, target.receiveIDType, apiURL)

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("feishu: send message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		errMsg := fmt.Sprintf("feishu: send status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
		log.Printf("feishu: API request failed - URL: %s, Response: %s", apiURL, errMsg)
		return fmt.Errorf("%s", errMsg)
	}

	var body struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			MessageID string `json:"message_id"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return fmt.Errorf("feishu: decode send response: %w", err)
	}
	if body.Code != 0 {
		errMsg := fmt.Sprintf("feishu: send failed code=%d msg=%s", body.Code, body.Msg)
		log.Printf("feishu: API returned error - code=%d msg=%s", body.Code, body.Msg)
		return fmt.Errorf("%s", errMsg)
	}

	log.Printf("feishu: message sent successfully - message_id=%s, chars=%d, target=%s(%s)",
		body.Data.MessageID, len(content), target.receiveIDType, receiveLabel)
	return nil
}

// downloadFeishuMediaAsDataURI \u4e0b\u8f7d\u98de\u4e66\u5a92\u4f53\u8d44\u6e90\uff0c\u8fd4\u56de base64 data URI\u3002
// fileType \u5e38\u89c1\u503c: "image"\u3001"audio"\u3001"video"\u3001"file"
func (h *Handler) downloadFeishuMediaAsDataURI(ctx context.Context, messageID, fileKey, fileType string) (string, string, error) {
	token, err := h.getAccessToken(ctx)
	if err != nil {
		return "", "", fmt.Errorf("feishu: get token for download: %w", err)
	}
	apiURL := fmt.Sprintf("%s/im/v1/messages/%s/resources/%s?type=%s", FeiShuAPIEndpoint, messageID, fileKey, fileType)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return "", "", fmt.Errorf("feishu: create media request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("feishu: download media: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("feishu: download media status=%d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("feishu: read media body: %w", err)
	}

	mime := resp.Header.Get("Content-Type")
	if idx := strings.Index(mime, ";"); idx != -1 {
		mime = strings.TrimSpace(mime[:idx])
	}
	if mime == "" {
		mime = "image/jpeg"
	}
	dataURI := fmt.Sprintf("data:%s;base64,%s", mime, base64.StdEncoding.EncodeToString(data))
	return dataURI, mime, nil
}

func (h *Handler) downloadFeishuMediaBytes(ctx context.Context, messageID, fileKey, fileType string) ([]byte, string, error) {
	token, err := h.getAccessToken(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("feishu: get token for download: %w", err)
	}

	apiURL := fmt.Sprintf("%s/im/v1/messages/%s/resources/%s?type=%s", FeiShuAPIEndpoint, messageID, fileKey, fileType)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("feishu: create media request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("feishu: download media: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("feishu: download media status=%d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("feishu: read media body: %w", err)
	}

	mime := resp.Header.Get("Content-Type")
	if idx := strings.Index(mime, ";"); idx != -1 {
		mime = strings.TrimSpace(mime[:idx])
	}
	return data, strings.ToLower(strings.TrimSpace(mime)), nil
}

func detectAliyunAudioFormat(audioBytes []byte, mime string) (string, int, []byte) {
	mime = strings.ToLower(strings.TrimSpace(mime))
	switch {
	case len(audioBytes) >= 4 && string(audioBytes[:4]) == "OggS":
		return "opu", 16000, audioBytes
	case strings.HasPrefix(string(audioBytes), "#!AMR-WB\n"):
		return "amr-wb", 16000, audioBytes[len("#!AMR-WB\n"):]
	case strings.HasPrefix(string(audioBytes), "#!AMR\n"):
		return "amr", 8000, audioBytes[len("#!AMR\n"):]
	case strings.Contains(mime, "wav"):
		return "wav", 16000, audioBytes
	case strings.Contains(mime, "amr"):
		return "amr", 8000, audioBytes
	default:
		return "pcm", 16000, audioBytes
	}
}

func (h *Handler) transcribeAudioBytes(ctx context.Context, audioBytes []byte, format string, sampleRate int) (string, error) {
	config, err := nls.NewConnectionConfigWithAKInfoDefault(
		nls.DEFAULT_URL,
		h.cfg.AliyunNLSAppKey,
		h.cfg.AliyunNLSAkID,
		h.cfg.AliyunNLSAkKey,
	)
	if err != nil {
		return "", fmt.Errorf("NLS connection config error: %w", err)
	}

	type nlsCbParam struct {
		resultCh chan string
		errCh    chan error
		latest   string
		mu       sync.Mutex
	}
	cbp := &nlsCbParam{resultCh: make(chan string, 1), errCh: make(chan error, 1)}

	nlsLogger := nls.NewNlsLogger(io.Discard, "nls", log.LstdFlags)
	nlsLogger.SetLogSil(true)

	sr, err := nls.NewSpeechRecognition(config, nlsLogger,
		func(text string, p interface{}) { // taskFailed
			cp := p.(*nlsCbParam)
			select {
			case cp.errCh <- fmt.Errorf("NLS task failed: %s", text):
			default:
			}
		},
		nil,
		func(text string, p interface{}) { // resultChanged
			cp := p.(*nlsCbParam)
			if recognized := extractNLSRecognizedText(text); recognized != "" {
				cp.mu.Lock()
				cp.latest = recognized
				cp.mu.Unlock()
			}
		},
		func(text string, p interface{}) { // completed
			cp := p.(*nlsCbParam)
			log.Printf("feishu: NLS completed raw JSON: %.800s", text)
			recognized := extractNLSRecognizedText(text)
			if recognized == "" {
				cp.mu.Lock()
				recognized = cp.latest
				cp.mu.Unlock()
			}
			log.Printf("feishu: NLS recognized text: %q", recognized)
			select {
			case cp.resultCh <- recognized:
			default:
			}
		},
		func(p interface{}) {},
		cbp,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create NLS SpeechRecognition: %w", err)
	}
	defer sr.Shutdown()

	srParam := nls.DefaultSpeechRecognitionParam()
	srParam.Format = format
	srParam.SampleRate = sampleRate

	ready, err := sr.Start(srParam, nil)
	if err != nil {
		return "", fmt.Errorf("NLS SR Start error: %w", err)
	}
	select {
	case ok := <-ready:
		if !ok {
			return "", fmt.Errorf("NLS SR Start failed")
		}
	case <-time.After(15 * time.Second):
		return "", fmt.Errorf("NLS SR Start timeout")
	case <-ctx.Done():
		return "", ctx.Err()
	}

	const chunkSize = 3200
	for i := 0; i < len(audioBytes); i += chunkSize {
		select {
		case ferr := <-cbp.errCh:
			return "", ferr
		default:
		}
		end := i + chunkSize
		if end > len(audioBytes) {
			end = len(audioBytes)
		}
		if err := sr.SendAudioData(audioBytes[i:end]); err != nil {
			return "", fmt.Errorf("NLS SendAudioData error: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if _, err = sr.Stop(); err != nil {
		return "", fmt.Errorf("NLS SR Stop error: %w", err)
	}

	select {
	case text := <-cbp.resultCh:
		return text, nil
	case ferr := <-cbp.errCh:
		return "", ferr
	case <-time.After(30 * time.Second):
		return "", fmt.Errorf("NLS result timeout")
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func extractNLSRecognizedText(raw string) string {
	text := strings.TrimSpace(raw)
	if text == "" {
		return ""
	}

	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(text), &obj); err != nil {
		return ""
	}

	lookup := func(m map[string]interface{}, keys ...string) string {
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

	candidates := []string{
		lookup(obj, "payload", "result"),
		lookup(obj, "payload", "text"),
		lookup(obj, "result"),
		lookup(obj, "text"),
		lookup(obj, "payload", "output", "text"),
		lookup(obj, "payload", "output", "sentence"),
	}
	for _, v := range candidates {
		if v != "" {
			return v
		}
	}
	return ""
}

// parseFeishuMessageContent 根据消息类型解析飞书消息，返回文字内容、附件列表和视频 skill 名称。
func (h *Handler) parseFeishuMessageContent(ctx context.Context, msgType, rawContent, messageID, userID, sessionID string) (string, []opencode.Attachment, []base.MediaFileRecord, string, error) {
	mediaSessionID := strings.TrimSpace(sessionID)
	if mediaSessionID == "" {
		mediaSessionID = "new"
	}
	saveMediaRecord := func(kind, filename, mime string, data []byte) (*base.MediaFileRecord, error) {
		now := time.Now().UTC()
		relDir := base.BuildMediaRelativeDir("feishu", userID, mediaSessionID, now)
		// Use OpenCode working directory for media storage so skills can access the files
		mediaRoot := base.MediaRootDirForOpenCode(h.client.Directory())
		saved, err := base.SaveTempMedia(
			mediaRoot,
			relDir,
			kind,
			messageID,
			filename,
			mime,
			data,
			base.MediaTTLFromEnv(),
			base.MediaMaxBytesFromEnv(),
		)
		if err != nil {
			return nil, err
		}
		record := &base.MediaFileRecord{
			MessageID:    messageID,
			UserID:       userID,
			SessionID:    mediaSessionID,
			Platform:     "feishu",
			MsgType:      kind,
			Filename:     saved.Filename,
			Mime:         saved.Mime,
			Size:         saved.Size,
			SHA256:       saved.SHA256,
			LocalPath:    saved.LocalPath,
			RelativePath: saved.RelativePath,
			CreatedAt:    saved.CreatedAt,
			ExpireAt:     saved.ExpireAt,
		}
		return record, nil
	}

	switch msgType {
	case "text":
		text, err := parseFeishuText(rawContent)
		return text, nil, nil, "", err

	case "image":
		var img feishuImageContent
		if err := json.Unmarshal([]byte(rawContent), &img); err != nil || img.ImageKey == "" {
			return "请分析这张图片的内容。", nil, nil, "", nil
		}
		dataURI, mime, err := h.downloadFeishuMediaAsDataURI(ctx, messageID, img.ImageKey, "image")
		if err != nil {
			log.Printf("feishu: ⚠️ image download failed: %v", err)
			return "请分析这张图片的内容。", nil, nil, "", nil
		}
		log.Printf("feishu: ✅ image downloaded (mime=%s, len=%d)", mime, len(dataURI))
		return "请分析这张图片的内容。", []opencode.Attachment{{Mime: mime, URL: dataURI}}, nil, "", nil

	case "audio", "voice":
		var aud feishuAudioContent
		asrMime := ""
		asrFormat := ""
		asrRate := 0
		asrTextLen := 0
		asrStatus := "init"
		if err := json.Unmarshal([]byte(rawContent), &aud); err != nil {
			asrStatus = "parse_failed"
			log.Printf("asr-summary platform=feishu mime=%s format=%s sampleRate=%d textLen=%d status=%s", asrMime, asrFormat, asrRate, asrTextLen, asrStatus)
			return "[语音消息]", nil, nil, "", nil
		}
		durMs, _ := strconv.Atoi(aud.Duration)
		durSec := durMs / 1000
		if durSec == 0 && durMs > 0 {
			durSec = 1
		}

		if aud.FileKey != "" && h.cfg.AliyunNLSAkID != "" && h.cfg.AliyunNLSAkKey != "" && h.cfg.AliyunNLSAppKey != "" {
			audioBytes, mime, err := h.downloadFeishuMediaBytes(ctx, messageID, aud.FileKey, "audio")
			asrMime = mime
			if err != nil {
				log.Printf("feishu: ⚠️ audio download failed: %v", err)
				asrStatus = "download_failed"
			} else {
				format, sampleRate, normalized := detectAliyunAudioFormat(audioBytes, mime)
				asrFormat = format
				asrRate = sampleRate
				log.Printf("feishu: 🎤 audio downloaded for ASR (mime=%s, bytes=%d, format=%s, rate=%d)", mime, len(normalized), format, sampleRate)
				text, srErr := h.transcribeAudioBytes(ctx, normalized, format, sampleRate)
				if srErr != nil {
					log.Printf("feishu: ⚠️ NLS transcription failed: %v", srErr)
					asrStatus = "nls_failed"
				} else if strings.TrimSpace(text) != "" {
					log.Printf("feishu: ✅ NLS result: %s", text)
					asrTextLen = len(text)
					asrStatus = "ok"
					log.Printf("asr-summary platform=feishu mime=%s format=%s sampleRate=%d textLen=%d status=%s", asrMime, asrFormat, asrRate, asrTextLen, asrStatus)
					return fmt.Sprintf("[语音转文字] %s", text), nil, nil, "", nil
				} else {
					log.Printf("feishu: ⚠️ NLS returned empty text")
					asrStatus = "empty"
				}
			}
		} else {
			asrStatus = "nls_not_configured"
		}

		log.Printf("asr-summary platform=feishu mime=%s format=%s sampleRate=%d textLen=%d status=%s", asrMime, asrFormat, asrRate, asrTextLen, asrStatus)

		return fmt.Sprintf("[语音消息，时长: %d秒]", durSec), nil, nil, "", nil

	case "video", "media":
		// 飞书的视频消息类型是 "media" 而非 "video"
		// 尝试解析两种格式
		var fileKey, duration string
		var fileName string

		// 先尝试解析 media 格式（飞书实际发送的格式）
		var mediaContent feishuMediaContent
		if err := json.Unmarshal([]byte(rawContent), &mediaContent); err == nil && mediaContent.FileKey != "" {
			fileKey = mediaContent.FileKey
			duration = mediaContent.Duration
			fileName = mediaContent.FileName
			log.Printf("feishu: parsed as media format - fileKey=%s, fileName=%s, duration=%s", fileKey, fileName, duration)
		} else {
			// 回退到 video 格式
			var vid feishuVideoContent
			if err := json.Unmarshal([]byte(rawContent), &vid); err != nil {
				log.Printf("feishu: ⚠️ parse video/media content failed: %v", err)
				return "请分析这个视频的内容。", nil, nil, "", nil
			}
			fileKey = vid.FileKey
			duration = vid.Duration
			log.Printf("feishu: parsed as video format - fileKey=%s, duration=%s", fileKey, duration)
		}

		durMs, _ := strconv.Atoi(strings.TrimSpace(duration))
		durSec := durMs / 1000
		if durSec == 0 && durMs > 0 {
			durSec = 1
		}

		if fileKey == "" {
			videoPrompt := "请分析这个视频的内容。"
			if durSec > 0 {
				videoPrompt = fmt.Sprintf("请分析这个视频的内容（时长: %d秒）。", durSec)
			}
			return videoPrompt, nil, nil, "", nil
		}

		// 下载视频到本地
		videoBytes, videoMime, bytesErr := h.downloadFeishuMediaBytes(ctx, messageID, fileKey, "video")
		if bytesErr != nil {
			log.Printf("feishu: ⚠️ video bytes download with type=video failed: %v, retry with type=file", bytesErr)
			videoBytes, videoMime, bytesErr = h.downloadFeishuMediaBytes(ctx, messageID, fileKey, "file")
		}
		var videoRecord *base.MediaFileRecord
		if bytesErr != nil {
			log.Printf("feishu: ⚠️ video temp save skipped, download bytes failed: %v", bytesErr)
		} else {
			// 使用文件名或默认名称
			videoFileName := fileName
			if videoFileName == "" {
				videoFileName = "feishu_video.mp4"
			}
			record, saveErr := saveMediaRecord("video", videoFileName, videoMime, videoBytes)
			if saveErr != nil {
				log.Printf("feishu: ⚠️ failed to save temp video file: %v", saveErr)
			} else {
				videoRecord = record
				log.Printf("feishu: 🗂️ video temp saved: %s (size: %d bytes)", record.LocalPath, record.Size)
			}
		}

		// 视频消息处理流程（和图片一样自动选择模型）：
		// 1. 有明确支持视频的模型 → 直接发送视频
		// 2. 有图片模型但无视频模型 → Gateway 提取帧图片 → 发送帧图片
		// 3. 都没有 → 返回错误

		// 1. 检查是否有明确支持视频的模型
		if h.client.HasVideoCapableModel() {
			var mediaFiles []base.MediaFileRecord
			if videoRecord != nil {
				mediaFiles = append(mediaFiles, *videoRecord)
			}
			log.Printf("feishu: ✅ Using video-capable model to process video directly")
			dataURI, mime, err := h.downloadFeishuMediaAsDataURI(ctx, messageID, fileKey, "video")
			if err != nil {
				log.Printf("feishu: ⚠️ video download with type=video failed: %v, retry with type=file", err)
				dataURI, mime, err = h.downloadFeishuMediaAsDataURI(ctx, messageID, fileKey, "file")
			}
			if err != nil {
				log.Printf("feishu: ⚠️ video download failed: %v", err)
				videoPrompt := "请分析这个视频的内容。"
				if durSec > 0 {
					videoPrompt = fmt.Sprintf("请分析这个视频的内容（时长: %d秒）。", durSec)
				}
				if videoRecord != nil {
					videoPrompt = fmt.Sprintf("视频文件已保存到: %s\n文件大小: %d bytes\n请分析这个视频。", videoRecord.LocalPath, videoRecord.Size)
				}
				return videoPrompt, nil, mediaFiles, "", nil
			}

			log.Printf("feishu: ✅ video downloaded (mime=%s, len=%d)", mime, len(dataURI))
			videoPrompt := "请分析这个视频的内容。"
			if durSec > 0 {
				videoPrompt = fmt.Sprintf("请分析这个视频的内容（时长: %d秒）。", durSec)
			}
			return videoPrompt, []opencode.Attachment{{Mime: mime, URL: dataURI, Filename: "feishu_video.mp4"}}, mediaFiles, "", nil
		}

		// 2. 有图片模型 → Gateway 提取帧图片，然后发送帧图片（和图片一样处理）
		if h.client.HasImageCapableModel() {
			if videoRecord != nil {
				log.Printf("feishu: 🔄 No video-capable model, extracting frames from video...")
				frames, extractErr := base.ExtractVideoFrames(ctx, videoRecord.LocalPath, h.client.Directory(), 10)
				if extractErr != nil {
					log.Printf("feishu: ⚠️ Failed to extract frames: %v", extractErr)
					videoPrompt := fmt.Sprintf("⚠️ 视频帧提取失败: %v\n\n视频已保存到: %s", extractErr, videoRecord.LocalPath)
					return videoPrompt, nil, nil, "", nil
				}
				log.Printf("feishu: ✅ Extracted %d frames from video", len(frames))
				// 将帧图片作为附件发送（和图片一样）
				var frameAttachments []opencode.Attachment
				for i, frame := range frames {
					frameDataURI, err := base.ReadFileAsDataURI(frame.FramePath)
					if err != nil {
						log.Printf("feishu: ⚠️ Failed to read frame %d: %v", i, err)
						continue
					}
					frameAttachments = append(frameAttachments, opencode.Attachment{
						Mime:     "image/jpeg",
						URL:      frameDataURI,
						Filename: fmt.Sprintf("frame_%d.jpg", frame.FrameNumber),
					})
				}
				if len(frameAttachments) > 0 {
					log.Printf("feishu: 📎 Sending %d frame images as attachments", len(frameAttachments))
					videoPrompt := "这是一个视频的关键帧截图，请分析视频内容。"
					if durSec > 0 {
						videoPrompt = fmt.Sprintf("这是一个视频的关键帧截图（视频时长: %d秒），请分析视频内容。", durSec)
					}
					return videoPrompt, frameAttachments, nil, "", nil
				}
			}
			videoPrompt := "⚠️ 视频帧提取失败。"
			if videoRecord != nil {
				videoPrompt = fmt.Sprintf("⚠️ 视频帧提取失败。\n\n视频已保存到: %s", videoRecord.LocalPath)
			}
			return videoPrompt, nil, nil, "", nil
		}

		// 3. 既没有视频模型也没有图片模型
		log.Printf("feishu: ⚠️ No video-capable model and no image-capable model found")
		videoPrompt := "⚠️ 视频处理暂不可用。"
		if videoRecord != nil {
			videoPrompt = fmt.Sprintf("⚠️ 视频处理暂不可用。\n\n视频已保存到: %s\n大小: %d bytes\n时长: %d秒\n\n请配置支持视频或图片的模型。", videoRecord.LocalPath, videoRecord.Size, durSec)
		}
		return videoPrompt, nil, nil, "", nil

	case "file":
		var fileContent feishuFileContent
		if err := json.Unmarshal([]byte(rawContent), &fileContent); err != nil {
			log.Printf("feishu: ⚠️ parse file content failed: %v", err)
			return "[文件消息]", nil, nil, "", nil
		}
		if strings.TrimSpace(fileContent.FileKey) == "" {
			return "[文件消息]", nil, nil, "", nil
		}

		fileBytes, fileMime, err := h.downloadFeishuMediaBytes(ctx, messageID, fileContent.FileKey, "file")
		if err != nil {
			log.Printf("feishu: ⚠️ file download failed: %v", err)
			if strings.TrimSpace(fileContent.FileName) != "" {
				return fmt.Sprintf("[文件消息: %s]", strings.TrimSpace(fileContent.FileName)), nil, nil, "", nil
			}
			return "[文件消息]", nil, nil, "", nil
		}

		fileName := strings.TrimSpace(fileContent.FileName)
		if fileName == "" {
			fileName = "feishu_file.bin"
		}

		record, saveErr := saveMediaRecord("file", fileName, fileMime, fileBytes)
		if saveErr != nil {
			log.Printf("feishu: ⚠️ failed to save temp file: %v", saveErr)
			if fileName != "" {
				return fmt.Sprintf("[文件消息: %s]", fileName), nil, nil, "", nil
			}
			return "[文件消息]", nil, nil, "", nil
		}
		log.Printf("feishu: 🗂️ file temp saved: %s", record.LocalPath)
		return fmt.Sprintf("[文件消息: %s]", fileName), nil, []base.MediaFileRecord{*record}, "", nil

	case "post":
		var post feishuPostContent
		if err := json.Unmarshal([]byte(rawContent), &post); err != nil {
			return "请分析这些内容。", nil, nil, "", nil
		}
		lang := post.ZhCN
		if lang == nil {
			lang = post.EnUS
		}
		if lang == nil {
			return "请分析这些内容。", nil, nil, "", nil
		}
		var textParts []string
		var attachments []opencode.Attachment
		if lang.Title != "" {
			textParts = append(textParts, lang.Title)
		}
		imgIdx := 0
		for _, row := range lang.Content {
			for _, elem := range row {
				switch elem.Tag {
				case "text", "a":
					if t := strings.TrimSpace(elem.Text); t != "" {
						textParts = append(textParts, t)
					}
				case "img":
					if elem.ImageKey == "" {
						continue
					}
					imgIdx++
					dataURI, mime, err := h.downloadFeishuMediaAsDataURI(ctx, messageID, elem.ImageKey, "image")
					if err != nil {
						log.Printf("feishu: ⚠️ post image #%d download failed: %v", imgIdx, err)
						continue
					}
					attachments = append(attachments, opencode.Attachment{Mime: mime, URL: dataURI, Filename: fmt.Sprintf("feishu_image_%d.jpg", imgIdx)})
				}
			}
		}
		text := strings.Join(textParts, "\n")
		// 如果用户没有提供文字，且只有图片，给默认提示
		if text == "" && len(attachments) > 0 {
			text = "请分析这张图片的内容。"
		} else if text == "" {
			text = "[图文消息]"
		}
		return text, attachments, nil, "", nil

	default:
		return fmt.Sprintf("[%s消息]", msgType), nil, nil, "", nil
	}
}

func (h *Handler) getAccessToken(ctx context.Context) (string, error) {
	h.tokenMu.Lock()
	defer h.tokenMu.Unlock()

	if h.accessToken != "" && time.Now().Before(h.tokenExpiry) {
		return h.accessToken, nil
	}
	if h.cfg.AppID == "" || h.cfg.AppSecret == "" {
		return "", fmt.Errorf("feishu: app credentials required for sending messages")
	}

	apiURL := fmt.Sprintf("%s/auth/v3/tenant_access_token/internal", FeiShuAPIEndpoint)
	payload := map[string]string{
		"app_id":     h.cfg.AppID,
		"app_secret": h.cfg.AppSecret,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("feishu: marshal token payload: %w", err)
	}

	// 使用独立 context，避免父 context 取消时影响 token 获取
	reqCtx, reqCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer reqCancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, apiURL, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("feishu: create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("feishu: fetch token: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Code        int    `json:"code"`
		Msg         string `json:"msg"`
		TenantToken string `json:"tenant_access_token"`
		Expire      int    `json:"expire"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("feishu: decode token response: %w", err)
	}
	if result.Code != 0 {
		return "", fmt.Errorf("feishu: token error code=%d msg=%s", result.Code, result.Msg)
	}
	if result.TenantToken == "" {
		return "", fmt.Errorf("feishu: empty tenant token")
	}

	h.accessToken = result.TenantToken
	validFor := time.Duration(result.Expire) * time.Second
	if validFor <= 0 {
		validFor = time.Hour
	}
	refreshBefore := time.Minute
	if validFor > 2*time.Minute {
		refreshBefore = 2 * time.Minute
	}
	if validFor <= refreshBefore {
		h.tokenExpiry = time.Now().Add(validFor / 2)
	} else {
		h.tokenExpiry = time.Now().Add(validFor - refreshBefore)
	}

	return h.accessToken, nil
}

func (h *Handler) Mount(mux *http.ServeMux) {
	if h.cfg.UseWebSocket {
		log.Println("feishu: WebSocket mode enabled, webhook not registered")
		return
	}
	mux.Handle("/feishu/callback", h)
	log.Println("feishu: webhook endpoint registered at /feishu/callback")
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.cfg.UseWebSocket {
		http.Error(w, "webhook disabled", http.StatusNotImplemented)
		return
	}

	h.cleanupOnce.Do(func() {
		go h.cleanupProcessedMessages()
	})

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	var envelope callbackEnvelope
	if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if envelope.Type == "url_verification" || envelope.Challenge != "" {
		h.handleVerification(w, envelope)
		return
	}

	if h.cfg.VerificationToken != "" && envelope.Token != "" && envelope.Token != h.cfg.VerificationToken {
		http.Error(w, "invalid token", http.StatusForbidden)
		return
	}

	msgType := "text"
	if envelope.Event.Message.MessageType != nil && *envelope.Event.Message.MessageType != "" {
		msgType = *envelope.Event.Message.MessageType
	}

	messageID := ""
	if envelope.Event.Message.MessageID != nil {
		messageID = *envelope.Event.Message.MessageID
	}

	userID := strings.TrimSpace(envelope.Event.Sender.SenderID.OpenID)
	if userID == "" {
		http.Error(w, "missing sender", http.StatusBadRequest)
		return
	}

	mediaSessionID := "new"
	if existingSessionID, ok := h.adapter.GetSessionForUser(userID); ok && strings.TrimSpace(existingSessionID) != "" {
		mediaSessionID = strings.TrimSpace(existingSessionID)
	}

	content, attachments, mediaFiles, videoSkillName, parseErr := h.parseFeishuMessageContent(r.Context(), msgType, envelope.Event.Message.Content,
		func() string {
			return messageID
		}(), userID, mediaSessionID)
	if parseErr != nil {
		http.Error(w, "invalid content", http.StatusBadRequest)
		return
	}
	if content == "" {
		http.Error(w, "empty message", http.StatusBadRequest)
		return
	}

	if messageID != "" && !h.shouldProcessMessage(messageID) {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	chatType := "p2p"
	if envelope.Event.Message.ChatType != nil && *envelope.Event.Message.ChatType != "" {
		chatType = *envelope.Event.Message.ChatType
	}

	reply, err := h.handleIncomingMessage(r.Context(), incomingMessage{
		UserID:         userID,
		ChatID:         envelope.Event.Message.ChatID,
		ChatType:       chatType,
		MessageID:      messageID,
		Content:        content,
		MessageType:    msgType,
		Attachments:    attachments,
		MediaFiles:     mediaFiles,
		VideoSkillName: videoSkillName,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("forward failed: %v", err), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{"msg": "ok"}
	if reply != "" {
		response["content"] = map[string]string{"text": reply}
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (h *Handler) handleVerification(w http.ResponseWriter, env callbackEnvelope) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{"challenge": env.Challenge})
}

type callbackEnvelope struct {
	Schema    string       `json:"schema"`
	Token     string       `json:"token"`
	Type      string       `json:"type"`
	Challenge string       `json:"challenge"`
	Event     eventWrapper `json:"event"`
}

type eventWrapper struct {
	Sender struct {
		SenderID struct {
			UnionID string `json:"union_id"`
			OpenID  string `json:"open_id"`
			UserID  string `json:"user_id"`
		} `json:"sender_id"`
	} `json:"sender"`
	Message struct {
		MessageID   *string `json:"message_id"`
		MessageType *string `json:"message_type"`
		ChatID      string  `json:"chat_id"`
		Content     string  `json:"content"`
		ChatType    *string `json:"chat_type"`
	} `json:"message"`
}

type messageTextBlock struct {
	Text string `json:"text"`
}

// feishuImageContent 图片消息
type feishuImageContent struct {
	ImageKey string `json:"image_key"`
}

// feishuAudioContent 语音消息
type feishuAudioContent struct {
	FileKey  string `json:"file_key"`
	Duration string `json:"duration"` // 毫秒
}

// feishuVideoContent 视频消息
type feishuVideoContent struct {
	FileKey  string `json:"file_key"`
	Duration string `json:"duration"` // 毫秒
}

// feishuMediaContent 媒体消息（飞书视频消息类型是 media 而非 video）
type feishuMediaContent struct {
	FileKey  string `json:"file_key"`
	FileName string `json:"file_name"`
	ImageKey string `json:"image_key"` // 视频封面图
	Duration string `json:"duration"`  // 毫秒
}

// feishuFileContent 文件消息
type feishuFileContent struct {
	FileKey  string `json:"file_key"`
	FileName string `json:"file_name"`
	FileType string `json:"file_type"`
	FileSize string `json:"file_size"`
}

// feishuPostContent 富文本消息（图文混排）
type feishuPostContent struct {
	ZhCN *feishuPostLang `json:"zh_cn"`
	EnUS *feishuPostLang `json:"en_us"`
}

type feishuPostLang struct {
	Title   string             `json:"title"`
	Content [][]feishuPostElem `json:"content"`
}

type feishuPostElem struct {
	Tag      string `json:"tag"`
	Text     string `json:"text,omitempty"`
	ImageKey string `json:"image_key,omitempty"`
	Href     string `json:"href,omitempty"`
}

type chatTarget struct {
	receiveID     string
	receiveIDType string
}

type incomingMessage struct {
	UserID         string
	ChatID         string
	ChatType       string
	MessageID      string
	Content        string
	MessageType    string
	Attachments    []opencode.Attachment // 图片/音频等附件
	MediaFiles     []base.MediaFileRecord
	VideoSkillName string // 视频处理 skill 名称（如果有）
}

func parseFeishuText(raw string) (string, error) {
	var block messageTextBlock
	if err := json.Unmarshal([]byte(raw), &block); err != nil {
		return "", err
	}
	return strings.TrimSpace(block.Text), nil
}

func isImmediateChunk(text string) bool {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return false
	}
	// 立即发送的消息类型：
	// - 状态提示（⏳⏱️🔐❓⚠️）
	// - 工具调用实时反馈（🔧 正在执行... / ✅ 完成 / ❌ 失败）
	for _, prefix := range []string{"⏳", "⏱️", "🔐", "❓", "⚠️", "🔧", "✅ [", "❌ ["} {
		if strings.HasPrefix(trimmed, prefix) {
			return true
		}
	}
	return false
}

func chunkText(content string, limit int) []string {
	if strings.TrimSpace(content) == "" {
		return nil
	}
	if limit <= 0 || len([]rune(content)) <= limit {
		return []string{content}
	}
	runes := []rune(content)
	var chunks []string
	for start := 0; start < len(runes); start += limit {
		end := start + limit
		if end > len(runes) {
			end = len(runes)
		}
		chunks = append(chunks, string(runes[start:end]))
	}
	return chunks
}

func (h *Handler) rememberChatTarget(msg incomingMessage) chatTarget {
	target := chatTarget{
		receiveID:     msg.UserID,
		receiveIDType: "open_id",
	}
	switch strings.ToLower(msg.ChatType) {
	case "group", "supergroup", "chat":
		if msg.ChatID != "" {
			target.receiveID = msg.ChatID
			target.receiveIDType = "chat_id"
		}
	}
	h.userTargets.Store(msg.UserID, target)
	return target
}

func (h *Handler) resolveChatTarget(userID, channel string) (chatTarget, error) {
	if userID == "" {
		return chatTarget{}, fmt.Errorf("feishu: unable to resolve chat target - userID is empty")
	}
	if channel != "" {
		receiveIDType := channel
		if receiveIDType == "" {
			receiveIDType = "open_id"
		}
		return chatTarget{
			receiveID:     userID,
			receiveIDType: receiveIDType,
		}, nil
	}
	if entry, ok := h.userTargets.Load(userID); ok {
		return entry.(chatTarget), nil
	}
	return chatTarget{receiveID: userID, receiveIDType: "open_id"}, nil
}

func (h *Handler) shouldProcessMessage(msgID string) bool {
	if msgID == "" {
		return true
	}
	if _, exists := h.processedMsgIDs.Load(msgID); exists {
		return false
	}
	h.processedMsgIDs.Store(msgID, time.Now())
	return true
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// handleListSkills handles the /skills command to list available agents.
func (h *Handler) handleListSkills(ctx context.Context, target chatTarget) (string, error) {
	agents, err := h.client.ListAgents(ctx)
	if err != nil {
		log.Printf("feishu: failed to list agents: %v", err)
		return "", err
	}

	customSkills := listCustomSkillsForDisplay(h.client.Directory())

	// Build response message
	var reply strings.Builder
	reply.WriteString("📋 可用的 Skills:\n\n")

	if len(customSkills) > 0 {
		reply.WriteString("🧩 自定义 Skills（与 TUI /skills 一致）：\n")
		for i, item := range customSkills {
			reply.WriteString(fmt.Sprintf("%d. %s (%s)\n", i+1, item.Name, item.Source))
		}
		reply.WriteString("\n")
	}

	if len(agents) == 0 {
		if len(customSkills) == 0 {
			reply.WriteString("暂无可用 skills。\n")
		}
	} else {
		reply.WriteString("🤖 内置 Agents：\n")
		for i, agent := range agents {
			reply.WriteString(fmt.Sprintf("%d. **%s**", i+1, agent.Name))
			if agent.Description != "" {
				reply.WriteString(fmt.Sprintf("\n   描述: %s", agent.Description))
			}
			reply.WriteString(fmt.Sprintf("\n   模式: %s", agent.Mode))
			if agent.Prompt != "" {
				reply.WriteString(fmt.Sprintf("\n   提示词: %s", agent.Prompt))
			}
			reply.WriteString("\n\n")
		}
	}

	reply.WriteString("💡 使用方法: @agent_name 你的消息\n")
	reply.WriteString("例如: @build 帮我编译项目")
	if len(customSkills) > 0 {
		reply.WriteString("\n\n📁 自定义 skills 目录: ~/.config/opencode/skills")
	}

	// Reply to user
	_ = h.sendTextChunks(ctx, target, reply.String())
	return "handled", nil
}

type customSkillItem struct {
	Name   string
	Source string
}

func listCustomSkillsForDisplay(baseDir string) []customSkillItem {
	seen := map[string]bool{}
	out := []customSkillItem{}
	dirs := []string{
		filepath.Join(resolveOpenCodeDirectory(baseDir), ".opencode", "skills"),
		filepath.Join(getHomeDirSafe(), ".config", "opencode", "skills"),
	}
	for i, dir := range dirs {
		source := "project"
		if i == 1 {
			source = "global"
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			if seen[name] {
				continue
			}
			seen[name] = true
			out = append(out, customSkillItem{Name: name, Source: source})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func resolveOpenCodeDirectory(baseDir string) string {
	d := strings.TrimSpace(baseDir)
	if d == "" {
		d = "."
	}
	if abs, err := filepath.Abs(d); err == nil {
		return abs
	}
	return d
}

func getHomeDirSafe() string {
	h, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return h
}

// handleHelp handles help command
func (h *Handler) handleHelp(ctx context.Context, target chatTarget) (string, error) {
	helpText := `📖 OpenCode Gateway 使用指南

🤖 基本对话：
直接发送消息即可与AI对话

🔧 基本命令：
/help 或 帮助 - 显示此帮助信息
/skills 或 /agents - 查看可用的技能列表
/abort 或 /stop - 中止正在运行的任务
/refresh - 刷新技能缓存

📊 会话管理：
/status 或 状态 - 查看当前会话状态
/new 或 /reset - 创建新会话
/clear 或 清除 - 删除当前会话
/fork - 派生(fork)当前会话（保留历史，创建新分支）
/undo 或 撤销 - 撤销上一次操作
/redo 或 重做 - 重做已撤销的操作
/sessions 或 /list - 列出所有会话
/summary 或 压缩 - 压缩会话上下文（释放token空间）

📋 任务追踪（对应 TUI 实时看板）：
/todo 或 任务 - 查看 AI 当前的任务进度
/diff 或 变更 - 查看本次会话的文件变更摘要

🤖 模型配置：
/model - 查看可用模型（含当前会话信息）
/model <provider>/<model> - 设置模型
/thinking - 查看 thinking 开关状态
/thinking on|off - 开关 thinking 返回
/final - 查看最终返回模式
/final on|off - 开关仅结束时返回最终结果
/steps - 查看步骤显示状态
/steps on|off - 开关步骤显示
/config 或 配置 - 查看完整配置

📋 OpenCode 模式说明：

1️⃣ Chat模式（默认）
   - 直接对话，立即响应

2️⃣ Plan模式
   - AI先制定计划再执行

3️⃣ Build模式（需要确认）
   - AI生成操作计划并等待确认
   - 回复 '允许' 或序号确认授权

💡 使用技巧：
• @agent_name 消息 - 调用特定技能
• 任务进行中可发 /todo 查看进度
• 完成后自动显示文件变更摘要
• /fork 创建当前上下文的副本继续探索

🛠️ 高级命令：
/cmd <command> - 执行技能脚本
/answer <answer> - 回答最近的待确认问题（可选：/answer <question_id> <answer>）
/crontask - 管理定时任务`

	_ = h.sendTextChunks(ctx, target, helpText)
	return "handled", nil
}

// handleThinking 处理 thinking 输出开关命令（全局）
func (h *Handler) handleThinking(ctx context.Context, target chatTarget, content string) (string, error) {
	parts := strings.Fields(strings.TrimSpace(content))

	if len(parts) == 1 {
		status := "off"
		if h.client.IsThinkingEnabled() {
			status = "on"
		}
		msg := fmt.Sprintf("🧠 Thinking 返回状态: %s\n\n使用方法:\n/thinking on  - 开启\n/thinking off - 关闭", status)
		_ = h.sendTextChunks(ctx, target, msg)
		return "handled", nil
	}

	arg := strings.ToLower(parts[1])
	switch arg {
	case "on", "true", "1":
		h.client.SetThinkingEnabled(true)
		_ = h.sendTextChunks(ctx, target, "✅ 已开启 thinking 返回（将按 'Thinking:' 分段输出）")
		return "handled", nil
	case "off", "false", "0":
		h.client.SetThinkingEnabled(false)
		_ = h.sendTextChunks(ctx, target, "✅ 已关闭 thinking 返回（仅返回最终正文）")
		return "handled", nil
	default:
		msg := "❌ 命令格式错误\n\n使用方法:\n/thinking - 查看状态\n/thinking on - 开启\n/thinking off - 关闭"
		_ = h.sendTextChunks(ctx, target, msg)
		return "handled", nil
	}
}

// handleFinal 处理最终输出模式开关命令（全局）
func (h *Handler) handleFinal(ctx context.Context, target chatTarget, content string) (string, error) {
	parts := strings.Fields(strings.TrimSpace(content))

	if len(parts) == 1 {
		status := "off"
		if h.client.IsFinalOnlyEnabled() {
			status = "on"
		}
		msg := fmt.Sprintf("📦 Final-only 模式: %s\n\n使用方法:\n/final on  - 开启（仅结束时返回最终结果）\n/final off - 关闭（允许中间增量）", status)
		_ = h.sendTextChunks(ctx, target, msg)
		return "handled", nil
	}

	arg := strings.ToLower(parts[1])
	switch arg {
	case "on", "true", "1":
		h.client.SetFinalOnlyEnabled(true)
		_ = h.sendTextChunks(ctx, target, "✅ 已开启 final-only 模式（正文与thinking均在结束后分段返回）")
		return "handled", nil
	case "off", "false", "0":
		h.client.SetFinalOnlyEnabled(false)
		_ = h.sendTextChunks(ctx, target, "✅ 已关闭 final-only 模式（允许中间增量返回）")
		return "handled", nil
	default:
		msg := "❌ 命令格式错误\n\n使用方法:\n/final - 查看状态\n/final on - 开启\n/final off - 关闭"
		_ = h.sendTextChunks(ctx, target, msg)
		return "handled", nil
	}
}

// handleSteps 处理步骤显示开关命令（全局）
func (h *Handler) handleSteps(ctx context.Context, target chatTarget, content string) (string, error) {
	parts := strings.Fields(strings.TrimSpace(content))

	if len(parts) == 1 {
		status := "off"
		if h.client.IsStepEnabled() {
			status = "on"
		}
		msg := fmt.Sprintf("🪜 步骤显示状态: %s\n\n使用方法:\n/steps on  - 开启\n/steps off - 关闭", status)
		_ = h.sendTextChunks(ctx, target, msg)
		return "handled", nil
	}

	arg := strings.ToLower(parts[1])
	switch arg {
	case "on", "true", "1":
		h.client.SetStepEnabled(true)
		_ = h.sendTextChunks(ctx, target, "✅ 已开启步骤显示（会显示步骤开始/完成）")
		return "handled", nil
	case "off", "false", "0":
		h.client.SetStepEnabled(false)
		_ = h.sendTextChunks(ctx, target, "✅ 已关闭步骤显示")
		return "handled", nil
	default:
		msg := "❌ 命令格式错误\n\n使用方法:\n/steps - 查看状态\n/steps on - 开启\n/steps off - 关闭"
		_ = h.sendTextChunks(ctx, target, msg)
		return "handled", nil
	}
}

// handleAbort handles abort command to abort running session
func (h *Handler) handleAbort(ctx context.Context, target chatTarget, userID string) (string, error) {
	// 获取用户的session
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		_ = h.sendTextChunks(ctx, target, "❌ 未找到活动的会话")
		return "handled", nil
	}

	// 检查session是否正在运行
	if !h.client.IsSessionRunning(sessionID) {
		_ = h.sendTextChunks(ctx, target, "ℹ️ 当前没有正在运行的任务")
		return "handled", nil
	}

	// 中止session
	log.Printf("feishu: aborting session %s for user %s", sessionID[:8], userID)
	if err := h.client.AbortSession(ctx, sessionID); err != nil {
		errMsg := fmt.Sprintf("❌ 中止任务失败: %v", err)
		_ = h.sendTextChunks(ctx, target, errMsg)
		log.Printf("feishu: failed to abort session %s: %v", sessionID, err)
		return "", err
	}

	_ = h.sendTextChunks(ctx, target, "✅ 任务已中止")
	return "handled", nil
}

// handleNewSession 处理创建新会话命令
func (h *Handler) handleNewSession(ctx context.Context, target chatTarget, userID, threadID string) (string, error) {
	var oldSessionID string
	if sid, ok := h.adapter.GetSessionForUser(userID); ok {
		oldSessionID = sid
	}

	if threadID != "" {
		h.client.ResetSession(threadID)
	}
	h.adapter.ClearSessionForUser(userID)

	msg := "✅ 已重置会话\n\n"
	if oldSessionID != "" {
		msg += fmt.Sprintf("旧会话: %s\n", oldSessionID[:min(8, len(oldSessionID))])
	}
	msg += "下次发送消息将创建新会话"

	_ = h.sendTextChunks(ctx, target, msg)
	return "handled", nil
}

// handleListSessions 处理列出所有会话命令
func (h *Handler) handleListSessions(ctx context.Context, target chatTarget) (string, error) {
	sessions, err := h.client.ListSessions(ctx)
	if err != nil {
		_ = h.sendTextChunks(ctx, target, fmt.Sprintf("❌ 获取会话列表失败: %v", err))
		return "", err
	}

	if len(sessions) == 0 {
		_ = h.sendTextChunks(ctx, target, "📝 当前没有活跃的会话")
		return "handled", nil
	}

	var msgBuilder strings.Builder
	msgBuilder.WriteString(fmt.Sprintf("📝 会话列表 (%d个):\n\n", len(sessions)))

	maxShow := 10
	if len(sessions) > maxShow {
		sessions = sessions[:maxShow]
	}

	for i, session := range sessions {
		msgBuilder.WriteString(fmt.Sprintf("%d. %s\n", i+1, session.Title))
		msgBuilder.WriteString(fmt.Sprintf("   ID: %s\n", session.ID[:min(8, len(session.ID))]))
		msgBuilder.WriteString(fmt.Sprintf("   目录: %s\n", session.Directory))
		updatedTime := time.Unix(int64(session.Time.Updated), 0).Format("2006-01-02 15:04")
		msgBuilder.WriteString(fmt.Sprintf("   更新: %s\n", updatedTime))
		msgBuilder.WriteString("\n")
	}

	if len(sessions) == maxShow {
		msgBuilder.WriteString(fmt.Sprintf("\n💡 只显示最近%d个会话", maxShow))
	}

	_ = h.sendTextChunks(ctx, target, msgBuilder.String())
	return "handled", nil
}

// handleStatus 处理查看会话状态命令
func (h *Handler) handleStatus(ctx context.Context, target chatTarget, userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		_ = h.sendTextChunks(ctx, target, "ℹ️ 当前没有活跃的会话\n\n发送消息将自动创建新会话")
		return "handled", nil
	}

	info, err := h.client.GetSessionInfo(ctx, sessionID)
	if err != nil {
		_ = h.sendTextChunks(ctx, target, fmt.Sprintf("❌ 获取会话信息失败: %v", err))
		return "", err
	}

	var msgBuilder strings.Builder
	msgBuilder.WriteString("📊 当前会话状态:\n\n")
	msgBuilder.WriteString(fmt.Sprintf("会话ID: %s\n", info.SessionID[:min(8, len(info.SessionID))]))
	msgBuilder.WriteString(fmt.Sprintf("标题: %s\n", info.Title))
	msgBuilder.WriteString(fmt.Sprintf("目录: %s\n", info.Directory))
	msgBuilder.WriteString(fmt.Sprintf("消息数: %d\n", info.MessageCount))
	msgBuilder.WriteString(fmt.Sprintf("Token数: %d\n", info.TokenCount))
	if info.ContextLength > 0 {
		msgBuilder.WriteString(fmt.Sprintf("上下文: %d/%d (%.1f%%)\n",
			info.TokenCount, info.ContextLength, info.ContextUsage*100))
	}
	msgBuilder.WriteString(fmt.Sprintf("创建时间: %s", info.Created))

	_ = h.sendTextChunks(ctx, target, msgBuilder.String())
	return "handled", nil
}

// handleSummary 处理上下文压缩命令
func (h *Handler) handleSummary(ctx context.Context, target chatTarget, userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		_ = h.sendTextChunks(ctx, target, "ℹ️ 当前没有活跃的会话\n\n发送消息将自动创建新会话")
		return "handled", nil
	}

	// 发送提示消息
	_ = h.sendTextChunks(ctx, target, "⏳ 正在进行上下文压缩...")

	// 调用 summary API
	if err := h.client.SummarizeSession(ctx, sessionID); err != nil {
		_ = h.sendTextChunks(ctx, target, fmt.Sprintf("❌ 上下文压缩失败: %v", err))
		return "", err
	}

	_ = h.sendTextChunks(ctx, target, fmt.Sprintf("✅ 上下文压缩完成\n\n会话 %s 的历史消息已被总结压缩，上下文空间已释放。", sessionID[:min(8, len(sessionID))]))
	return "handled", nil
}

// handleClear 处理清除会话命令
func (h *Handler) handleClear(ctx context.Context, target chatTarget, userID, threadID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		_ = h.sendTextChunks(ctx, target, "ℹ️ 当前没有活跃的会话")
		return "handled", nil
	}

	if err := h.client.DeleteSession(ctx, sessionID); err != nil {
		_ = h.sendTextChunks(ctx, target, fmt.Sprintf("❌ 删除会话失败: %v", err))
		return "", err
	}

	h.adapter.ClearSessionForUser(userID)
	if threadID != "" {
		h.client.ResetSession(threadID)
	}

	_ = h.sendTextChunks(ctx, target, fmt.Sprintf("✅ 已删除会话 %s\n\n下次发送消息将创建新会话", sessionID[:min(8, len(sessionID))]))
	return "handled", nil
}

// handleModel 处理模型配置命令
func (h *Handler) handleModel(ctx context.Context, target chatTarget, userID, content string) (string, error) {
	parts := strings.Fields(content)

	if len(parts) == 1 {
		return h.handleModelQuery(ctx, target, userID)
	}

	if len(parts) >= 2 {
		return h.handleModelSet(ctx, target, userID, parts[1:])
	}

	msg := "❌ 命令格式错误\n\n使用方法:\n/model - 查看可用模型\n/model <provider>/<model> - 设置模型\n/model <provider> <model> - 设置模型\n\n例如:\n/model anthropic/claude-3-opus\n/model openai gpt-4"
	_ = h.sendTextChunks(ctx, target, msg)
	return "handled", nil
}

// handleModelQuery 查询当前模型
func (h *Handler) handleModelQuery(ctx context.Context, target chatTarget, userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		var msgBuilder strings.Builder
		msgBuilder.WriteString("ℹ️ 当前没有活跃的会话\n")

		providers, err := h.client.GetProviders(ctx)
		if err != nil {
			msgBuilder.WriteString("\n💡 可用模型列表获取失败，请稍后重试或在 OpenCode Web 界面查看\n")
			msgBuilder.WriteString(fmt.Sprintf("错误: %v", err))
			_ = h.sendTextChunks(ctx, target, msgBuilder.String())
			return "handled", nil
		}

		if len(providers) == 0 {
			msgBuilder.WriteString("\n💡 未获取到可用模型，请在 OpenCode Web 界面查看")
			_ = h.sendTextChunks(ctx, target, msgBuilder.String())
			return "handled", nil
		}

		msgBuilder.WriteString("\n\n📚 可用模型（示例）:\n")
		for _, p := range providers {
			msgBuilder.WriteString(fmt.Sprintf("\n【%s】\n", p.ID))
			if len(p.Models) == 0 {
				msgBuilder.WriteString("  (无模型)\n")
				continue
			}
			maxShow := min(8, len(p.Models))
			for i := 0; i < maxShow; i++ {
				msgBuilder.WriteString(fmt.Sprintf("  /model %s/%s\n", p.ID, p.Models[i].ID))
			}
			if len(p.Models) > maxShow {
				msgBuilder.WriteString(fmt.Sprintf("  ... 还有 %d 个\n", len(p.Models)-maxShow))
			}
		}

		_ = h.sendTextChunks(ctx, target, msgBuilder.String())
		return "handled", nil
	}

	_, _, err := h.client.GetCurrentProvider(ctx, sessionID)
	if err != nil {
		msg := fmt.Sprintf("❌ 获取当前模型失败: %v\n\n"+
			"💡 模型配置功能需要OpenCode SDK的支持\n"+
			"目前的SDK版本可能不包含此API", err)
		_ = h.sendTextChunks(ctx, target, msg)
		return "", err
	}

	var msgBuilder strings.Builder
	msgBuilder.WriteString("🤖 当前会话配置:\n\n")
	msgBuilder.WriteString(fmt.Sprintf("会话: %s\n", sessionID[:min(8, len(sessionID))]))
	msgBuilder.WriteString("\n💡 当前会话的默认模型信息在SDK中不可直接读取\n")

	providers, err := h.client.GetProviders(ctx)
	if err != nil {
		msgBuilder.WriteString("\n可用模型列表获取失败，请稍后重试或在 OpenCode Web 界面查看\n")
		msgBuilder.WriteString(fmt.Sprintf("错误: %v", err))
		_ = h.sendTextChunks(ctx, target, msgBuilder.String())
		return "handled", nil
	}

	if len(providers) == 0 {
		msgBuilder.WriteString("\n未获取到可用模型，请在 OpenCode Web 界面查看")
		_ = h.sendTextChunks(ctx, target, msgBuilder.String())
		return "handled", nil
	}

	msgBuilder.WriteString("\n📚 可用模型（示例）:\n")
	for _, p := range providers {
		msgBuilder.WriteString(fmt.Sprintf("\n【%s】\n", p.ID))
		if len(p.Models) == 0 {
			msgBuilder.WriteString("  (无模型)\n")
			continue
		}
		maxShow := min(8, len(p.Models))
		for i := 0; i < maxShow; i++ {
			msgBuilder.WriteString(fmt.Sprintf("  /model %s/%s\n", p.ID, p.Models[i].ID))
		}
		if len(p.Models) > maxShow {
			msgBuilder.WriteString(fmt.Sprintf("  ... 还有 %d 个\n", len(p.Models)-maxShow))
		}
	}

	_ = h.sendTextChunks(ctx, target, msgBuilder.String())
	return "handled", nil
}

// handleModelSet 设置模型
func (h *Handler) handleModelSet(ctx context.Context, target chatTarget, userID string, args []string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		_ = h.sendTextChunks(ctx, target, "❌ 当前没有活跃的会话\n\n请先发送消息创建会话，然后再设置模型")
		return "handled", nil
	}

	var providerID, modelID string

	if strings.Contains(args[0], "/") {
		parts := strings.SplitN(args[0], "/", 2)
		providerID = parts[0]
		if len(parts) > 1 {
			modelID = parts[1]
		}
	} else {
		providerID = args[0]
		if len(args) > 1 {
			modelID = args[1]
		}
	}

	if providerID == "" {
		_ = h.sendTextChunks(ctx, target, "❌ 提供商ID不能为空")
		return "handled", nil
	}
	if modelID == "" {
		_ = h.sendTextChunks(ctx, target, "❌ 模型ID不能为空\n\n使用方法:\n/model <provider>/<model>\n例如:\n/model TH-AI/Kimi-K2.5")
		return "handled", nil
	}

	providers, err := h.client.GetProviders(ctx)
	if err != nil {
		log.Printf("feishu: failed to fetch providers for case-insensitive model resolve: %v", err)
	} else if len(providers) > 0 {
		providerMatched := false
		modelMatched := false
		for _, p := range providers {
			if !strings.EqualFold(strings.TrimSpace(p.ID), strings.TrimSpace(providerID)) {
				continue
			}
			providerMatched = true
			providerID = p.ID
			for _, m := range p.Models {
				if strings.EqualFold(strings.TrimSpace(m.ID), strings.TrimSpace(modelID)) {
					modelMatched = true
					modelID = m.ID
					break
				}
			}
			break
		}

		if !providerMatched {
			_ = h.sendTextChunks(ctx, target, fmt.Sprintf("❌ 未找到提供商: %s\n\n请先执行 /model 查看可用 provider/model", providerID))
			return "handled", nil
		}
		if !modelMatched {
			_ = h.sendTextChunks(ctx, target, fmt.Sprintf("❌ 提供商 %s 下未找到模型: %s\n\n请先执行 /model 查看可用模型", providerID, modelID))
			return "handled", nil
		}
	}

	if err := h.client.UpdateSessionProvider(ctx, sessionID, providerID, modelID); err != nil {
		msg := fmt.Sprintf("❌ 更新模型失败: %v", err)
		_ = h.sendTextChunks(ctx, target, msg)
		return "", err
	}

	msg := fmt.Sprintf("✅ 已设置会话模型\n\n提供商: %s\n模型: %s\n会话: %s\n\n"+
		"该设置会由 gateway 在后续请求中强制携带。",
		providerID, modelID, sessionID[:min(8, len(sessionID))])
	_ = h.sendTextChunks(ctx, target, msg)
	return "handled", nil
}

// handleConfig 处理配置查看命令
func (h *Handler) handleConfig(ctx context.Context, target chatTarget, userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)

	var msgBuilder strings.Builder
	msgBuilder.WriteString("⚙️ 当前配置:\n\n")

	if ok {
		info, err := h.client.GetSessionInfo(ctx, sessionID)
		if err == nil {
			msgBuilder.WriteString("📊 会话信息:\n")
			msgBuilder.WriteString(fmt.Sprintf("  ID: %s\n", info.SessionID[:min(8, len(info.SessionID))]))
			msgBuilder.WriteString(fmt.Sprintf("  标题: %s\n", info.Title))
			msgBuilder.WriteString(fmt.Sprintf("  目录: %s\n", info.Directory))
			msgBuilder.WriteString(fmt.Sprintf("  消息数: %d\n", info.MessageCount))
			msgBuilder.WriteString(fmt.Sprintf("  Token: %d/%d\n", info.TokenCount, info.ContextLength))
		}
	} else {
		msgBuilder.WriteString("📊 会话信息: 无活跃会话\n")
	}

	msgBuilder.WriteString("\n🔧 可用命令:\n")
	msgBuilder.WriteString("  /model - 查看/设置模型\n")
	msgBuilder.WriteString("  /thinking - 查看/设置 thinking 返回\n")
	msgBuilder.WriteString("  /final - 查看/设置 final-only 输出\n")
	msgBuilder.WriteString("  /steps - 查看/设置步骤显示\n")
	msgBuilder.WriteString("  /status - 查看会话状态\n")
	msgBuilder.WriteString("  /new - 创建新会话\n")
	msgBuilder.WriteString("  /clear - 清除当前会话\n")
	msgBuilder.WriteString("  /fork - 派生(fork)当前会话\n")
	msgBuilder.WriteString("  /todo - 查看当前任务进度\n")
	msgBuilder.WriteString("  /diff - 查看文件变更\n")
	msgBuilder.WriteString("  /sessions - 列出所有会话\n")
	msgBuilder.WriteString("  /skills - 查看可用技能\n")
	msgBuilder.WriteString("  /help - 查看帮助")

	_ = h.sendTextChunks(ctx, target, msgBuilder.String())
	return "handled", nil
}

// handleExecuteCommand handles direct command execution like skill scripts
func (h *Handler) handleExecuteCommand(ctx context.Context, target chatTarget, userID, command string) (string, error) {
	// Get or create session for user
	var sessionID string
	if sid, ok := h.adapter.GetSessionForUser(userID); ok {
		sessionID = sid
	} else {
		// Create new session if needed
		response, err := h.client.SendMessage(ctx, opencode.MessagePayload{
			Channel: "feishu",
			UserID:  userID,
			Content: "Initialize session",
		})
		if err != nil {
			log.Printf("feishu: failed to create session: %v", err)
			return "", err
		}
		sessionID = response.SessionID
		h.adapter.MapUserToSession(userID, sessionID)
	}

	log.Printf("feishu: executing command in session %s: %s", sessionID, command)

	// Execute command
	output, err := h.client.ExecuteShellOutput(ctx, sessionID, command)
	if err != nil {
		log.Printf("feishu: command execution failed: %v", err)
		errMsg := fmt.Sprintf("❌ 命令执行失败: %v", err)
		_ = h.sendTextChunks(ctx, target, errMsg)
		return "", err
	}

	// Build response message
	var reply string
	if strings.TrimSpace(output) != "" {
		reply = fmt.Sprintf("🖥️ 命令执行结果:\n\n```\n%s\n```", output)
	} else {
		reply = "🖥️ 命令已执行，但没有可显示的输出"
	}

	_ = h.sendTextChunks(ctx, target, reply)
	return "handled", nil
}

// handleAnswer handles the /answer command to answer pending questions
func (h *Handler) handleAnswer(ctx context.Context, target chatTarget, userID, content string) (string, error) {
	// 解析命令:
	// 1) /answer <answer>
	// 2) /answer <questionID> <answer>
	parts := strings.Fields(content)
	if len(parts) < 2 {
		msg := h.buildPendingRequirementHint(userID)
		if msg == "" {
			msg = "❌ 当前没有待确认问题。请先等待 OpenCode 提问后直接回复选项内容（如：1、允许、yes）。"
		}
		_ = h.sendTextChunks(ctx, target, msg)
		return "handled", nil
	}

	var questionID, answer string
	if len(parts) >= 3 && (strings.HasPrefix(parts[1], "q_") || strings.HasPrefix(parts[1], "que_") || strings.HasPrefix(parts[1], "per_")) {
		questionID = parts[1]
		answer = strings.Join(parts[2:], " ")
	} else {
		sessionID, ok := h.adapter.GetSessionForUser(userID)
		if !ok {
			_ = h.sendTextChunks(ctx, target, "❌ 当前没有活跃会话，无法定位待确认问题")
			return "handled", nil
		}

		if permission, ok := h.client.GetLatestPendingPermission(sessionID); ok {
			questionID = permission.ID
		} else if question, ok := h.client.GetLatestPendingQuestion(sessionID); ok {
			questionID = question.ID
		} else {
			_ = h.sendTextChunks(ctx, target, "❌ 当前会话没有待确认问题")
			return "handled", nil
		}
		answer = strings.Join(parts[1:], " ")
	}

	// 获取问题
	question, ok := h.client.GetPendingQuestion(questionID)
	if !ok {
		msg := fmt.Sprintf("❌ 找不到问题 %s\n\n可能原因:\n• 问题已被回答\n• 问题ID不正确\n• 问题已过期", questionID)
		_ = h.sendTextChunks(ctx, target, msg)
		return "handled", nil
	}

	// 如果有选项，验证答案
	if len(question.Options) > 0 {
		// 尝试解析为数字索引
		if idx, err := strconv.Atoi(answer); err == nil {
			if idx < 1 || idx > len(question.Options) {
				msg := fmt.Sprintf("❌ 选项序号无效：%d\n\n请选择 1-%d 之间的序号", idx, len(question.Options))
				_ = h.sendTextChunks(ctx, target, msg)
				return "handled", nil
			}
			// 使用实际选项文本
			answer = question.Options[idx-1]
		}
	}

	// 提交答案
	log.Printf("feishu: submitting answer '%s' for question %s (isPermission=%v)", answer, questionID, question.IsPermission)

	if question.IsPermission {
		englishResponse := parsePermissionReply(answer)
		if englishResponse == "" {
			_ = h.sendTextChunks(ctx, target, "❌ 未能识别权限回复，请回复：允许 / 拒绝 / 始终允许")
			return "handled", nil
		}
		if err := h.client.RespondToPermission(ctx, questionID, englishResponse); err != nil {
			_ = h.sendTextChunks(ctx, target, "❌ 权限回复失败，请重试")
			log.Printf("feishu: RespondToPermission failed for %s: %v", questionID, err)
			return "", err
		}
		displayMap := map[string]string{"once": "允许", "reject": "拒绝", "always": "始终允许"}
		msg := fmt.Sprintf("✅ 已回复: %s\n\n⏳ 等待 OpenCode 继续执行...", displayMap[englishResponse])
		_ = h.sendTextChunks(ctx, target, msg)
		log.Printf("feishu: answered permission %s (%s) successfully", questionID, englishResponse)
		return "handled", nil
	}

	if err := h.client.AnswerQuestion(ctx, questionID, answer); err != nil {
		msg := fmt.Sprintf("❌ 提交答案失败: %v", err)
		_ = h.sendTextChunks(ctx, target, msg)
		log.Printf("feishu: failed to answer question %s: %v", questionID, err)
		return "", err
	}

	msg := fmt.Sprintf("✅ 已提交答案: %s\n\n⏳ 等待 OpenCode 继续执行...", answer)
	_ = h.sendTextChunks(ctx, target, msg)
	log.Printf("feishu: answered question %s successfully", questionID)

	return "handled", nil
}

// parsePermissionReply maps user input (any locale/encoding) to a canonical English
// API value: "once" (allow once), "reject" (deny), "always" (always allow), or "" (unrecognized).
func parsePermissionReply(content string) string {
	norm := strings.ToLower(strings.TrimSpace(content))
	// Strip common punctuation and zero-width chars
	norm = strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', ',', '，', '.', '。', '!', '！', '?', '？', '\u200b', '\u200c', '\u200d', '\ufeff':
			return -1
		}
		return r
	}, norm)

	alwaysTokens := []string{"3", "always", "始终允许", "始终", "一直允许", "总是允许", "濮嬬粓鍏佽", "濮嬬粓", "涓€鐩村厑璁", "鎬绘槸鍏佽"}
	rejectTokens := []string{"2", "deny", "reject", "no", "n", "拒绝", "不同意", "不允许", "取消", "鎷掔粷", "涓嶅悓鎰", "鍙栨秷", "涓嶅厑璁"}
	allowTokens := []string{"1", "allow", "yes", "y", "ok", "okay", "允许", "同意", "确认", "可以", "行", "鍏佽", "鍚屾剰", "纭", "鍙互"}

	for _, t := range alwaysTokens {
		if strings.Contains(norm, strings.ToLower(t)) {
			return "always"
		}
	}
	for _, t := range rejectTokens {
		if strings.Contains(norm, strings.ToLower(t)) {
			return "reject"
		}
	}
	for _, t := range allowTokens {
		if strings.Contains(norm, strings.ToLower(t)) {
			return "once"
		}
	}
	return ""
}

func (h *Handler) isTokenOverflowErrorText(text string) bool {
	msg := strings.ToLower(strings.TrimSpace(text))
	if msg == "" {
		return false
	}
	if !strings.Contains(msg, "opencode 会话出错") && !strings.Contains(msg, "session error") {
		return false
	}
	return strings.Contains(msg, "parameter=input_tokens") ||
		strings.Contains(msg, "maximum input length") ||
		strings.Contains(msg, "context length") ||
		strings.Contains(msg, "input tokens")
}

func (h *Handler) buildTokenOverflowPrompt() string {
	return "⚠️ 当前会话上下文已超出模型上限，导致本次请求失败。\n\n" +
		"请选择后续操作（直接回复数字或文字即可）：\n" +
		"1. 压缩并继续（调用 summary 后重试本条消息）\n" +
		"2. 新会话并继续（重置会话后重试本条消息）\n" +
		"3. 取消\n" +
		"4. 总是压缩并继续\n" +
		"5. 总是新会话并继续"
}

func (h *Handler) getTokenOverflowPolicy(userID string) string {
	if value, ok := h.overflowPolicy.Load(strings.TrimSpace(userID)); ok {
		if policy, ok := value.(string); ok {
			policy = strings.TrimSpace(policy)
			if policy == feishuOverflowPolicySummary || policy == feishuOverflowPolicyNew {
				return policy
			}
		}
	}
	return feishuOverflowPolicyAsk
}

func (h *Handler) setTokenOverflowPolicy(userID, policy string) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return
	}
	if policy != feishuOverflowPolicySummary && policy != feishuOverflowPolicyNew {
		h.overflowPolicy.Delete(userID)
		return
	}
	h.overflowPolicy.Store(userID, policy)
}

func (h *Handler) storeTokenOverflowPending(userID string, state *feishuTokenOverflowPendingState) {
	if state == nil {
		return
	}
	h.overflowPending.Store(strings.TrimSpace(userID), state)
}

func (h *Handler) getTokenOverflowPending(userID string) (*feishuTokenOverflowPendingState, bool) {
	value, ok := h.overflowPending.Load(strings.TrimSpace(userID))
	if !ok {
		return nil, false
	}
	state, ok := value.(*feishuTokenOverflowPendingState)
	if !ok || state == nil {
		return nil, false
	}
	if time.Since(state.CreatedAt) > 30*time.Minute {
		h.overflowPending.Delete(strings.TrimSpace(userID))
		return nil, false
	}
	return state, true
}

func (h *Handler) clearTokenOverflowPending(userID string) {
	h.overflowPending.Delete(strings.TrimSpace(userID))
}

func (h *Handler) handleTokenOverflowQuickReply(ctx context.Context, target chatTarget, msg incomingMessage, content string) (bool, error) {
	state, ok := h.getTokenOverflowPending(msg.UserID)
	if !ok {
		return false, nil
	}

	decision, setAlways, recognized := parseTokenOverflowDecision(content)
	if !recognized {
		_ = h.sendTextChunks(ctx, target, "请回复 1/2/3/4/5（或对应中文选项）来处理上下文超限问题。")
		return true, nil
	}

	if decision == "cancel" {
		h.clearTokenOverflowPending(msg.UserID)
		_ = h.sendTextChunks(ctx, target, "✅ 已取消本次继续处理。你可以手动发送 /summary、/new 或重发消息。")
		return true, nil
	}

	if setAlways {
		if decision == "summary" {
			h.setTokenOverflowPolicy(msg.UserID, feishuOverflowPolicySummary)
			_ = h.sendTextChunks(ctx, target, "✅ 已设置为：总是压缩并继续。正在处理本条消息...")
		} else if decision == "new" {
			h.setTokenOverflowPolicy(msg.UserID, feishuOverflowPolicyNew)
			_ = h.sendTextChunks(ctx, target, "✅ 已设置为：总是新会话并继续。正在处理本条消息...")
		}
	}

	h.storeTokenOverflowPending(msg.UserID, state)
	go h.executeTokenOverflowDecision(context.Background(), msg.UserID, decision)
	return true, nil
}

func parseTokenOverflowDecision(content string) (decision string, setAlways bool, recognized bool) {
	normalized := normalizeDecisionText(content)
	if normalized == "" {
		return "", false, false
	}

	if normalized == "3" || strings.Contains(normalized, "取消") || normalized == "no" {
		return "cancel", false, true
	}

	alwaysSummaryTokens := []string{"4", "总是压缩", "总是总结", "总是这样压缩", "alwayssummary"}
	alwaysNewTokens := []string{"5", "总是新会话", "总是重开", "总是这样新会话", "alwaysnew"}
	summaryTokens := []string{"1", "压缩", "总结", "summary", "压缩继续", "继续压缩", "继续"}
	newTokens := []string{"2", "新会话", "重开", "new", "reset", "新会话继续", "继续新会话"}

	if containsDecisionToken(normalized, alwaysSummaryTokens) {
		return "summary", true, true
	}
	if containsDecisionToken(normalized, alwaysNewTokens) {
		return "new", true, true
	}
	if containsDecisionToken(normalized, summaryTokens) {
		return "summary", false, true
	}
	if containsDecisionToken(normalized, newTokens) {
		return "new", false, true
	}

	return "", false, false
}

func normalizeDecisionText(content string) string {
	raw := strings.TrimSpace(strings.ToLower(content))
	return strings.Map(func(r rune) rune {
		switch r {
		case ' ', '\t', '\n', '\r', '，', ',', '。', '.', '！', '!', '？', '?', '：', ':', ';', '；', '（', '）', '(', ')', '“', '”', '"', '\'', '、', '\u200b', '\u200c', '\u200d', '\ufeff':
			return -1
		default:
			return r
		}
	}, raw)
}

func containsDecisionToken(text string, tokens []string) bool {
	for _, token := range tokens {
		t := normalizeDecisionText(token)
		if t != "" && strings.Contains(text, t) {
			return true
		}
	}
	return false
}

func (h *Handler) executeTokenOverflowDecision(ctx context.Context, userID, decision string) {
	state, ok := h.getTokenOverflowPending(userID)
	if !ok {
		return
	}

	decision = strings.TrimSpace(decision)
	if decision == "" {
		decision = "summary"
	}

	timeout := 20 * time.Minute
	if strings.TrimSpace(state.Agent) != "" {
		timeout = 30 * time.Minute
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if decision == "summary" {
		sessionID := strings.TrimSpace(state.SessionID)
		if sessionID == "" {
			if sid, exists := h.adapter.GetSessionForUser(state.UserID); exists {
				sessionID = sid
			}
		}
		if sessionID == "" {
			_ = h.sendTextChunks(runCtx, state.Target, "❌ 无法定位当前会话，已改为新会话继续。")
			decision = "new"
		} else {
			if err := h.client.SummarizeSession(runCtx, sessionID); err != nil {
				_ = h.sendTextChunks(runCtx, state.Target, fmt.Sprintf("❌ 自动压缩失败：%v", err))
				h.clearTokenOverflowPending(userID)
				return
			}
			state.SessionID = sessionID
		}
	}

	if decision == "new" {
		if strings.TrimSpace(state.ThreadID) != "" {
			h.client.ResetSession(state.ThreadID)
		}
		h.adapter.ClearSessionForUser(state.UserID)
		state.SessionID = ""
	}

	response, err := h.client.SendMessage(runCtx, opencode.MessagePayload{
		Channel:     "feishu",
		UserID:      state.UserID,
		ThreadID:    state.ThreadID,
		SessionID:   state.SessionID,
		Content:     state.Content,
		Agent:       state.Agent,
		Streaming:   false,
		Attachments: append([]opencode.Attachment(nil), state.Attachments...),
		Metadata:    cloneStringMap(state.Metadata),
	})
	if err != nil {
		_ = h.sendTextChunks(runCtx, state.Target, fmt.Sprintf("❌ 已尝试%s后重试，但仍失败：%v", tokenOverflowDecisionLabel(decision), err))
		h.clearTokenOverflowPending(userID)
		return
	}

	if response.SessionID != "" {
		h.adapter.MapUserToSession(state.UserID, response.SessionID)
		h.adapter.MapSessionData(response.SessionID, "receive_id", state.Target.receiveID)
		h.adapter.MapSessionData(response.SessionID, "receive_id_type", state.Target.receiveIDType)
	}

	finalReply := strings.TrimSpace(response.Reply)
	if finalReply == "" {
		finalReply = "✅ 已完成重试，本次没有可直接返回的文本内容。"
	}
	if err := h.sendTextChunks(runCtx, state.Target, finalReply); err != nil {
		log.Printf("feishu: failed to send token-overflow retry reply: %v", err)
	}

	h.clearTokenOverflowPending(userID)
}

func tokenOverflowDecisionLabel(decision string) string {
	switch decision {
	case "summary":
		return "压缩"
	case "new":
		return "新会话"
	default:
		return "处理"
	}
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]string, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func (h *Handler) buildPendingRequirementHint(userID string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return ""
	}

	if permission, ok := h.client.GetLatestPendingPermission(sessionID); ok {
		return fmt.Sprintf("OpenCode 需要确认：\n%s\n\n请直接回复：允许 / 拒绝 / 始终允许", permission.Text)
	}
	if question, ok := h.client.GetLatestPendingQuestion(sessionID); ok {
		var b strings.Builder
		b.WriteString("OpenCode 需要选择：\n")
		if question.Text != "" {
			b.WriteString(question.Text)
			b.WriteString("\n")
		}
		if len(question.Questions) > 0 {
			for _, q := range question.Questions {
				if q.Header != "" {
					b.WriteString("\n")
					b.WriteString(q.Header)
					b.WriteString("\n")
				}
				if q.Question != "" {
					b.WriteString(q.Question)
					b.WriteString("\n")
				}
				for i, opt := range q.Options {
					b.WriteString(fmt.Sprintf("%d. %s\n", i+1, opt.Label))
				}
			}
		} else if len(question.Options) > 0 {
			for i, opt := range question.Options {
				b.WriteString(fmt.Sprintf("%d. %s\n", i+1, opt))
			}
		}
		b.WriteString("\n请直接回复选项内容（无需输入 /answer）")
		return b.String()
	}

	return ""
}

// handleCronTask handles the /crontask command for scheduled tasks
func (h *Handler) handleCronTask(ctx context.Context, target chatTarget, userID, content string) (string, error) {
	// 检查是否设置了cronScheduler
	if h.cronScheduler == nil {
		_ = h.sendTextChunks(ctx, target, "❌ 定时任务功能未启用")
		return "handled", nil
	}

	// 解析命令
	parts := strings.Fields(content)
	if len(parts) < 2 {
		return h.sendCronTaskHelp(ctx, target)
	}

	subCommand := parts[1]

	switch subCommand {
	case "add", "create", "新增":
		return h.handleCronTaskAdd(ctx, target, userID, parts[2:])
	case "list", "ls", "列表":
		return h.handleCronTaskList(ctx, target)
	case "delete", "del", "rm", "删除":
		return h.handleCronTaskDelete(ctx, target, parts[2:])
	case "enable", "启用":
		return h.handleCronTaskEnable(ctx, target, parts[2:])
	case "disable", "禁用":
		return h.handleCronTaskDisable(ctx, target, parts[2:])
	case "info", "详情":
		return h.handleCronTaskInfo(ctx, target, parts[2:])
	case "help", "帮助":
		return h.sendCronTaskHelp(ctx, target)
	default:
		_ = h.sendTextChunks(ctx, target, "❌ 未知的子命令，使用 /crontask help 查看帮助")
		return "handled", nil
	}
}

// handleCronTaskAdd adds a cron task
func (h *Handler) handleCronTaskAdd(ctx context.Context, target chatTarget, userID string, args []string) (string, error) {
	// 解析带引号的参数: "0 */30 * * * *" "测试任务" "查看系统负载"
	parsedArgs := parseQuotedArgs(strings.Join(args, " "))

	if len(parsedArgs) < 3 {
		_ = h.sendTextChunks(ctx, target,
			"❌ 参数不足\n\n"+
				"格式: /crontask add \"cron表达式\" \"任务名称\" \"任务内容\" [agent]\n"+
				"示例:\n"+
				"/crontask add \"0 0 9 * * *\" \"每日检查\" \"查看系统负载\"\n"+
				"/crontask add \"0 */30 * * * *\" \"半小时监控\" \"检查服务状态\" system_monitor",
		)
		return "handled", nil
	}

	cronExpr := parsedArgs[0]
	taskName := parsedArgs[1]
	taskContent := parsedArgs[2]
	agent := ""
	if len(parsedArgs) > 3 {
		agent = parsedArgs[3]
	}

	// 创建定时任务
	now := time.Now()
	task := &scheduler.ScheduledTask{
		Name:        taskName,
		Description: fmt.Sprintf("通过飞书创建 (用户: %s)", userID),
		Type:        scheduler.TaskTypeAgent,
		CronExpr:    cronExpr,
		Enabled:     true,
		AdapterType: "feishu",
		Content:     taskContent,
		Agent:       agent,
		Metadata: map[string]interface{}{
			"receive_id":      target.receiveID,
			"receive_id_type": target.receiveIDType,
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	// 添加到调度器
	if err := h.cronScheduler.AddScheduledTask(task); err != nil {
		errMsg := fmt.Sprintf("❌ 创建定时任务失败: %v", err)
		_ = h.sendTextChunks(ctx, target, errMsg)
		return "", err
	}

	msg := fmt.Sprintf(
		"✅ 定时任务创建成功！\n\n"+
			"📋 任务ID: %s\n"+
			"📝 名称: %s\n"+
			"⏰ Cron: %s\n"+
			"📄 内容: %s\n"+
			"🤖 Agent: %s\n"+
			"⏱️ 下次运行: %s\n\n"+
			"使用 /crontask list 查看所有任务",
		task.ID,
		task.Name,
		task.CronExpr,
		task.Content,
		func() string {
			if task.Agent != "" {
				return task.Agent
			}
			return "(默认)"
		}(),
		func() string {
			if task.NextRunTime != nil {
				return task.NextRunTime.Format("2006-01-02 15:04:05")
			}
			return "未知"
		}(),
	)

	_ = h.sendTextChunks(ctx, target, msg)
	return "handled", nil
}

// handleCronTaskList lists all cron tasks
func (h *Handler) handleCronTaskList(ctx context.Context, target chatTarget) (string, error) {
	tasks := h.cronScheduler.GetScheduledTasksByAdapter("feishu")
	if len(tasks) == 0 {
		_ = h.sendTextChunks(ctx, target, "📋 暂无定时任务\n\n使用 /crontask help 查看如何创建")
		return "handled", nil
	}

	var msg strings.Builder
	msg.WriteString(fmt.Sprintf("📋 定时任务列表 (共 %d 个)\n\n", len(tasks)))

	for i, task := range tasks {
		status := "✅"
		if !task.Enabled {
			status = "⏸️"
		}

		msg.WriteString(fmt.Sprintf(
			"%d. %s %s\n"+
				"   ID: %s\n"+
				"   Cron: %s\n"+
				"   Agent: %s\n"+
				"   运行次数: %d\n",
			i+1,
			status,
			task.Name,
			task.ID,
			task.CronExpr,
			func() string {
				if task.Agent != "" {
					return task.Agent
				}
				return "(默认)"
			}(),
			task.RunCount,
		))

		if task.NextRunTime != nil {
			msg.WriteString(fmt.Sprintf("   下次运行: %s\n", task.NextRunTime.Format("2006-01-02 15:04:05")))
		}

		if task.LastRunTime != nil {
			msg.WriteString(fmt.Sprintf("   上次运行: %s (%s)\n",
				task.LastRunTime.Format("2006-01-02 15:04:05"),
				task.LastRunStatus,
			))
		}

		msg.WriteString("\n")
	}

	msg.WriteString("💡 使用 /crontask info <ID> 查看详情")

	_ = h.sendTextChunks(ctx, target, msg.String())
	return "handled", nil
}

// handleCronTaskDelete deletes a cron task
func (h *Handler) handleCronTaskDelete(ctx context.Context, target chatTarget, args []string) (string, error) {
	if len(args) < 1 {
		_ = h.sendTextChunks(ctx, target, "❌ 请指定任务ID\n\n格式: /crontask delete <任务ID>")
		return "handled", nil
	}

	taskID := args[0]

	if err := h.cronScheduler.RemoveScheduledTask(taskID); err != nil {
		errMsg := fmt.Sprintf("❌ 删除任务失败: %v", err)
		_ = h.sendTextChunks(ctx, target, errMsg)
		return "", err
	}

	msg := fmt.Sprintf("✅ 任务 %s 已删除", taskID)
	_ = h.sendTextChunks(ctx, target, msg)
	return "handled", nil
}

// handleCronTaskEnable enables a cron task
func (h *Handler) handleCronTaskEnable(ctx context.Context, target chatTarget, args []string) (string, error) {
	if len(args) < 1 {
		_ = h.sendTextChunks(ctx, target, "❌ 请指定任务ID\n\n格式: /crontask enable <任务ID>")
		return "handled", nil
	}

	taskID := args[0]

	if err := h.cronScheduler.EnableTask(taskID); err != nil {
		errMsg := fmt.Sprintf("❌ 启用任务失败: %v", err)
		_ = h.sendTextChunks(ctx, target, errMsg)
		return "", err
	}

	msg := fmt.Sprintf("✅ 任务 %s 已启用", taskID)
	_ = h.sendTextChunks(ctx, target, msg)
	return "handled", nil
}

// handleCronTaskDisable disables a cron task
func (h *Handler) handleCronTaskDisable(ctx context.Context, target chatTarget, args []string) (string, error) {
	if len(args) < 1 {
		_ = h.sendTextChunks(ctx, target, "❌ 请指定任务ID\n\n格式: /crontask disable <任务ID>")
		return "handled", nil
	}

	taskID := args[0]

	if err := h.cronScheduler.DisableTask(taskID); err != nil {
		errMsg := fmt.Sprintf("❌ 禁用任务失败: %v", err)
		_ = h.sendTextChunks(ctx, target, errMsg)
		return "", err
	}

	msg := fmt.Sprintf("⏸️ 任务 %s 已禁用", taskID)
	_ = h.sendTextChunks(ctx, target, msg)
	return "handled", nil
}

// handleCronTaskInfo shows cron task details
func (h *Handler) handleCronTaskInfo(ctx context.Context, target chatTarget, args []string) (string, error) {
	if len(args) < 1 {
		_ = h.sendTextChunks(ctx, target, "❌ 请指定任务ID\n\n格式: /crontask info <任务ID>")
		return "handled", nil
	}

	taskID := args[0]

	task, err := h.cronScheduler.GetScheduledTask(taskID)
	if err != nil {
		errMsg := fmt.Sprintf("❌ 获取任务信息失败: %v", err)
		_ = h.sendTextChunks(ctx, target, errMsg)
		return "", err
	}

	status := "✅ 启用"
	if !task.Enabled {
		status = "⏸️ 禁用"
	}

	var msg strings.Builder
	msg.WriteString(fmt.Sprintf(
		"📋 定时任务详情\n\n"+
			"📝 名称: %s\n"+
			"🆔 ID: %s\n"+
			"📄 描述: %s\n"+
			"⏰ Cron: %s\n"+
			"📊 状态: %s\n"+
			"🤖 Agent: %s\n"+
			"📄 内容: %s\n"+
			"🔢 运行次数: %d\n",
		task.Name,
		task.ID,
		task.Description,
		task.CronExpr,
		status,
		func() string {
			if task.Agent != "" {
				return task.Agent
			}
			return "(默认)"
		}(),
		task.Content,
		task.RunCount,
	))

	if task.NextRunTime != nil {
		msg.WriteString(fmt.Sprintf("⏱️ 下次运行: %s\n", task.NextRunTime.Format("2006-01-02 15:04:05")))
	}

	if task.LastRunTime != nil {
		msg.WriteString(fmt.Sprintf(
			"📅 上次运行: %s\n"+
				"📊 运行状态: %s\n",
			task.LastRunTime.Format("2006-01-02 15:04:05"),
			task.LastRunStatus,
		))

		if task.LastRunResult != "" {
			msg.WriteString(fmt.Sprintf("📝 运行结果: %s\n", task.LastRunResult))
		}
	}

	msg.WriteString(fmt.Sprintf(
		"\n⏰ 创建时间: %s\n"+
			"🔄 更新时间: %s",
		task.CreatedAt.Format("2006-01-02 15:04:05"),
		task.UpdatedAt.Format("2006-01-02 15:04:05"),
	))

	_ = h.sendTextChunks(ctx, target, msg.String())
	return "handled", nil
}

// sendCronTaskHelp sends cron task help message
func (h *Handler) sendCronTaskHelp(ctx context.Context, target chatTarget) (string, error) {
	helpMsg := `📋 定时任务命令帮助

🔹 创建任务
/crontask add "cron表达式" "任务名称" "任务内容" [agent]

示例:
• /crontask add "0 0 9 * * *" "每日检查" "查看系统负载"
• /crontask add "0 */30 * * * *" "半小时监控" "检查服务" monitor

🔹 列出任务
/crontask list

🔹 查看详情
/crontask info <任务ID>

🔹 启用/禁用
/crontask enable <任务ID>
/crontask disable <任务ID>

🔹 删除任务
/crontask delete <任务ID>

⏰ Cron表达式格式 (秒 分 时 日 月 周):
• "0 0 9 * * *" - 每天9点
• "0 */30 * * * *" - 每30分钟
• "0 0 12 * * 1-5" - 工作日中午12点
• "0 0 0 1 * *" - 每月1号零点

💡 提示: 任务会在指定时间自动执行，结果会发送到当前会话`

	_ = h.sendTextChunks(ctx, target, helpMsg)
	return "handled", nil
}

// handleUndo 处理撤销命令（对应 TUI /undo 操作）
func (h *Handler) handleUndo(ctx context.Context, target chatTarget, userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		_ = h.sendTextChunks(ctx, target, "ℹ️ 当前没有活跃的会话\n\n发送消息将自动创建新会话")
		return "handled", nil
	}

	session, err := h.client.RevertSession(ctx, sessionID, "")
	if err != nil {
		_ = h.sendTextChunks(ctx, target, fmt.Sprintf("❌ 撤销失败: %v", err))
		return "", err
	}

	msg := fmt.Sprintf("↩️ 已撤销上一次操作\n\n会话: %s\n版本: %s\n\n可以使用 /redo 恢复",
		sessionID[:min(8, len(sessionID))], session.Version)
	_ = h.sendTextChunks(ctx, target, msg)
	log.Printf("feishu: reverted session %s to version %s for user %s", sessionID[:8], session.Version, userID)
	return "handled", nil
}

// handleRedo 处理重做命令（对应 TUI /redo 操作）
func (h *Handler) handleRedo(ctx context.Context, target chatTarget, userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		_ = h.sendTextChunks(ctx, target, "ℹ️ 当前没有活跃的会话\n\n发送消息将自动创建新会话")
		return "handled", nil
	}

	session, err := h.client.UnrevertSession(ctx, sessionID)
	if err != nil {
		_ = h.sendTextChunks(ctx, target, fmt.Sprintf("❌ 重做失败: %v", err))
		return "", err
	}

	msg := fmt.Sprintf("↪️ 已重做操作\n\n会话: %s\n版本: %s",
		sessionID[:min(8, len(sessionID))], session.Version)
	_ = h.sendTextChunks(ctx, target, msg)
	log.Printf("feishu: unreverted session %s to version %s for user %s", sessionID[:8], session.Version, userID)
	return "handled", nil
}

// handleFork 处理派生会话命令（对应 TUI session.fork）
func (h *Handler) handleFork(ctx context.Context, target chatTarget, userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		_ = h.sendTextChunks(ctx, target, "ℹ️ 当前没有活跃的会话\n\n发送消息将自动创建新会话")
		return "handled", nil
	}

	newSessionID, err := h.client.ForkSession(ctx, sessionID)
	if err != nil {
		_ = h.sendTextChunks(ctx, target, fmt.Sprintf("❌ 派生会话失败: %v", err))
		return "", err
	}

	h.adapter.MapUserToSession(userID, newSessionID)
	h.adapter.MapSessionData(newSessionID, "receive_id", target.receiveID)
	h.adapter.MapSessionData(newSessionID, "receive_id_type", target.receiveIDType)

	msg := fmt.Sprintf("🔀 已派生新会话\n\n原会话: %s\n新会话: %s\n\n继续对话将使用新的派生会话（与原会话历史相同）",
		sessionID[:min(8, len(sessionID))], newSessionID[:min(8, len(newSessionID))])
	_ = h.sendTextChunks(ctx, target, msg)
	log.Printf("feishu: forked session %s -> %s for user %s", sessionID[:8], newSessionID[:8], userID)
	return "handled", nil
}

// handleCompact 处理压缩/总结会话命令（对应 TUI session.compact）
// handleTodo 处理查看任务进度命令（对应 TUI todo.updated 事件展示）
func (h *Handler) handleTodo(ctx context.Context, target chatTarget, userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		_ = h.sendTextChunks(ctx, target, "ℹ️ 当前没有活跃的会话")
		return "handled", nil
	}

	todos := h.client.GetTodosForSession(sessionID)
	if len(todos) == 0 {
		_ = h.sendTextChunks(ctx, target, "📋 当前没有进行中的任务\n\n当 AI 处理复杂请求时，这里会显示任务进度。")
		return "handled", nil
	}

	var sb strings.Builder
	sb.WriteString("📋 当前任务进度:\n\n")
	pending, inProgress, completed := 0, 0, 0
	for _, todo := range todos {
		var icon string
		switch todo.Status {
		case "completed":
			icon = "✅"
			completed++
		case "in_progress":
			icon = "🔄"
			inProgress++
		case "cancelled":
			icon = "❌"
		default:
			icon = "⬜"
			pending++
		}
		sb.WriteString(fmt.Sprintf("%s [优先级:%s] %s\n", icon, todo.PriorityLabel(), todo.Text()))
	}
	sb.WriteString(fmt.Sprintf("\n进度: %d 完成, %d 进行中, %d 待处理", completed, inProgress, pending))

	_ = h.sendTextChunks(ctx, target, sb.String())
	return "handled", nil
}

// handleDiff 处理查看文件变更命令（对应 TUI session.diff 事件展示）
func (h *Handler) handleDiff(ctx context.Context, target chatTarget, userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		_ = h.sendTextChunks(ctx, target, "ℹ️ 当前没有活跃的会话")
		return "handled", nil
	}

	diff := h.client.GetDiffForSession(sessionID)
	if len(diff) == 0 {
		_ = h.sendTextChunks(ctx, target, "📁 本次会话暂无文件变更\n\n当 AI 修改文件时，这里会显示变更摘要。")
		return "handled", nil
	}

	var sb strings.Builder
	sb.WriteString("📁 文件变更摘要:\n\n")
	totalAdded, totalRemoved := 0, 0
	for _, f := range diff {
		icon := "📝"
		if f.Added > 0 && f.Removed == 0 {
			icon = "🆕"
		} else if f.Added == 0 && f.Removed > 0 {
			icon = "🗑️"
		}
		sb.WriteString(fmt.Sprintf("%s %s (+%d/-%d)\n", icon, f.Path, f.Added, f.Removed))
		totalAdded += f.Added
		totalRemoved += f.Removed
	}
	sb.WriteString(fmt.Sprintf("\n共 %d 个文件，+%d/-%d 行", len(diff), totalAdded, totalRemoved))

	_ = h.sendTextChunks(ctx, target, sb.String())
	return "handled", nil
}

// parseQuotedArgs parses quoted arguments
// Example: "cron" "name" "content" -> ["cron", "name", "content"]
func parseQuotedArgs(input string) []string {
	var result []string
	var inQuote bool
	var current strings.Builder

	for _, r := range input {
		switch r {
		case '"':
			if inQuote {
				// End quote
				inQuote = false
				result = append(result, current.String())
				current.Reset()
			} else {
				// Start quote
				inQuote = true
			}
		case ' ', '\t':
			if inQuote {
				// Preserve spaces inside quotes
				current.WriteRune(r)
			} else if current.Len() > 0 {
				// Space outside quotes, end current word
				result = append(result, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}

	// Handle last word
	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}
