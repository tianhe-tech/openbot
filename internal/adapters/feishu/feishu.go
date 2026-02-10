package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

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
	sessionID, hasSession := h.adapter.GetSessionForUser(msg.UserID)

	if hasSession {
		log.Printf("feishu: checking quick reply '%s' for user %s (session: %s)", content, userLabel, sessionID[:min(8, len(sessionID))])

		// 先尝试查找待处理的权限请求
		permission, ok := h.client.GetLatestPendingPermission(sessionID)
		if ok {
			log.Printf("feishu: user %s replied '%s' to permission %s (session: %s)",
				userLabel, content, permission.ID, sessionID[:min(8, len(sessionID))])

			if err := h.client.AnswerQuestion(ctx, permission.ID, content); err != nil {
				msg := fmt.Sprintf("❌ 权限回复失败: %v", err)
				_ = h.sendTextChunks(ctx, target, msg)
				return "", err
			}

			msg := fmt.Sprintf("✅ 已回复: %s\n\n⏳ 等待 OpenCode 继续执行...", content)
			_ = h.sendTextChunks(ctx, target, msg)
			log.Printf("feishu: successfully answered permission %s, continuing with original SSE listener", permission.ID)
			// 返回空字符串表示已处理，不需要作为新消息发送给 OpenCode
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

	// Handle /crontask command for scheduled tasks
	if strings.HasPrefix(content, "/crontask") {
		return h.handleCronTask(ctx, target, msg.UserID, content)
	}

	// Handle /answer command to answer pending questions
	if strings.HasPrefix(content, "/answer ") {
		return h.handleAnswer(ctx, target, msg.UserID, content)
	}
	// ========== 特殊命令处理结束 ==========

	sessionID, _ = h.adapter.GetSessionForUser(msg.UserID)
	var fullReply strings.Builder

	callback := func(chunk string) error {
		raw := chunk
		trimmed := strings.TrimSpace(chunk)
		if trimmed == "" {
			return nil
		}
		if strings.HasPrefix(trimmed, "ses_") && len(trimmed) < 100 {
			h.adapter.MapUserToSession(msg.UserID, trimmed)
			return nil
		}
		if isImmediateChunk(trimmed) {
			return h.sendTextChunks(ctx, target, raw)
		}
		fullReply.WriteString(raw)
		return nil
	}

	threadID := msg.ChatID
	if threadID == "" {
		threadID = msg.MessageID
	}

	response, err := h.client.SendMessageStreaming(ctx, opencode.MessagePayload{
		Channel:   "feishu",
		UserID:    msg.UserID,
		ThreadID:  threadID,
		SessionID: sessionID,
		Content:   msg.Content,
		Streaming: true,
		Metadata: map[string]string{
			"message_id":      msg.MessageID,
			"message_type":    msg.MessageType,
			"chat_type":       msg.ChatType,
			"chat_id":         msg.ChatID,
			"receive_id":      target.receiveID,
			"receive_id_type": target.receiveIDType,
		},
	}, callback)
	if err != nil {
		errMsg := fmt.Sprintf("❌ 处理失败: %v", err)
		_ = h.sendTextChunks(ctx, target, errMsg)
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
		if err := h.sendTextChunks(ctx, target, response.Reply); err != nil {
			log.Printf("feishu: failed to send sync reply: %v", err)
			return response.Reply, err
		}
		log.Printf("feishu: sent sync reply to user %s (%d chars)", userLabel, len(response.Reply))
	} else {
		// Async mode - check accumulated content from streaming
		accumulatedContent := fullReply.String()
		if len(accumulatedContent) > 0 {
			// We have accumulated content, send it as final message
			finalMsg := fmt.Sprintf("✅ 处理完成\n\n%s", accumulatedContent)
			if err := h.sendTextChunks(ctx, target, finalMsg); err != nil {
				log.Printf("feishu: failed to send accumulated reply: %v", err)
				return accumulatedContent, err
			}
			log.Printf("feishu: sent accumulated reply to user %s (%d chars)", userLabel, len(accumulatedContent))
		} else {
			// No accumulated content (might be interactive task like permission request)
			log.Printf("feishu: async mode completed for user %s with no accumulated content", userLabel)
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

	content, err := parseFeishuText(*event.Event.Message.Content)
	if err != nil {
		log.Printf("❌ feishu: 解析消息内容失败: %v", err)
		return fmt.Errorf("feishu: parse content: %w", err)
	}
	if content == "" {
		log.Printf("⚠️  feishu: 消息内容为空")
		return nil
	}

	if h.debugMode {
		log.Printf("📝 消息内容: %s", content)
	}

	if event.Event.Sender == nil || event.Event.Sender.SenderId == nil || event.Event.Sender.SenderId.OpenId == nil {
		log.Printf("❌ feishu: 缺少发送者信息")
		return fmt.Errorf("feishu: missing sender info")
	}
	userID := *event.Event.Sender.SenderId.OpenId

	chatID := ""
	if event.Event.Message.ChatId != nil {
		chatID = *event.Event.Message.ChatId
	}
	chatType := "p2p"
	if event.Event.Message.ChatType != nil && *event.Event.Message.ChatType != "" {
		chatType = *event.Event.Message.ChatType
	}
	messageType := "text"
	if event.Event.Message.MessageType != nil && *event.Event.Message.MessageType != "" {
		messageType = *event.Event.Message.MessageType
	}

	if h.debugMode {
		log.Printf("👤 发送者: %s", userID[:min(12, len(userID))])
		log.Printf("💬 会话类型: %s, 会话ID: %s", chatType, chatID[:min(12, len(chatID))])
		log.Printf("📋 消息类型: %s, 消息ID: %s", messageType, messageID[:min(12, len(messageID))])
	}

	_, err = h.handleIncomingMessage(ctx, incomingMessage{
		UserID:      userID,
		ChatID:      chatID,
		ChatType:    chatType,
		MessageID:   messageID,
		Content:     content,
		MessageType: messageType,
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

	token, err := h.getAccessToken(ctx)
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(data))
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
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(data))
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
	if msgType != "text" {
		http.Error(w, "unsupported type", http.StatusNotImplemented)
		return
	}

	content, err := parseFeishuText(envelope.Event.Message.Content)
	if err != nil {
		http.Error(w, "invalid content", http.StatusBadRequest)
		return
	}
	if content == "" {
		http.Error(w, "empty message", http.StatusBadRequest)
		return
	}

	messageID := ""
	if envelope.Event.Message.MessageID != nil {
		messageID = *envelope.Event.Message.MessageID
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
		UserID:      envelope.Event.Sender.SenderID.OpenID,
		ChatID:      envelope.Event.Message.ChatID,
		ChatType:    chatType,
		MessageID:   messageID,
		Content:     content,
		MessageType: msgType,
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

type chatTarget struct {
	receiveID     string
	receiveIDType string
}

type incomingMessage struct {
	UserID      string
	ChatID      string
	ChatType    string
	MessageID   string
	Content     string
	MessageType string
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
	for _, prefix := range []string{"⏳", "⏱️", "🔐", "❓", "⚠️"} {
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

	// Build response message
	var reply strings.Builder
	reply.WriteString("📋 可用的 Skills/Agents:\n\n")

	if len(agents) == 0 {
		reply.WriteString("暂无可用的 agent。\n")
		reply.WriteString("请在 OpenCode 工作目录创建 AGENTS.md 文件定义 agents。")
	} else {
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
		reply.WriteString("💡 使用方法: @agent_name 你的消息\n")
		reply.WriteString("例如: @build 帮我编译项目")
	}

	// Reply to user
	_ = h.sendTextChunks(ctx, target, reply.String())
	return "handled", nil
}

// handleHelp handles help command
func (h *Handler) handleHelp(ctx context.Context, target chatTarget) (string, error) {
	helpText := `📖 OpenCode Gateway 使用指南

🤖 基本对话：
直接发送消息即可与AI对话

🔧 可用命令：
/help 或 帮助 - 显示此帮助信息
/skills 或 /agents - 查看可用的技能列表

📋 OpenCode 模式说明：

1️⃣ Chat模式（默认）
   - 直接对话，立即响应
   - 适合：日常问答、代码解释

2️⃣ Plan模式
   - AI会先制定计划再执行
   - 适合：复杂任务规划

3️⃣ Build模式（需要确认）
   - AI会生成操作计划并等待您确认
   - ⚠️ 需要在OpenCode界面手动确认
   - 确认后结果会自动回复到飞书
   - 适合：文件修改、代码生成等

💡 使用技巧：
• 使用 @agent_name 调用特定技能
  例如：@build 帮我创建一个Python脚本
• Build模式请求会提示您去OpenCode确认
• 请勿在短时间内重复发送相同消息
• 发送 /abort 或 /stop 可以中止正在运行的任务

🛠️ 可用命令：
/help 或 帮助 - 显示帮助
/skills - 查看可用技能
/abort 或 /stop - 中止当前任务
/refresh - 刷新技能缓存
/answer <question_id> <answer> - 回答待确认的问题

❓ 问题排查：
• 如果提示"请求处理中"：请等待当前请求完成
• 如果收到确认请求：使用 /answer 命令回复
• 如果超时：可能是build模式等待确认
• 如果失败：稍后重试或简化问题`

	_ = h.sendTextChunks(ctx, target, helpText)
	return "handled", nil
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
	result, err := h.client.ExecuteShell(ctx, sessionID, command)
	if err != nil {
		log.Printf("feishu: command execution failed: %v", err)
		errMsg := fmt.Sprintf("❌ 命令执行失败: %v", err)
		_ = h.sendTextChunks(ctx, target, errMsg)
		return "", err
	}

	// Build response message
	var reply string
	if result != nil {
		reply = fmt.Sprintf("🖥️ 命令执行结果:\n\n```\n%s\n```", result.ID)
	} else {
		reply = "🖥️ 命令执行完成"
	}

	_ = h.sendTextChunks(ctx, target, reply)
	return "handled", nil
}

// handleAnswer handles the /answer command to answer pending questions
func (h *Handler) handleAnswer(ctx context.Context, target chatTarget, userID, content string) (string, error) {
	// 解析命令: /answer <questionID> <answer>
	parts := strings.Fields(content)
	if len(parts) < 3 {
		msg := "❌ 命令格式错误\n\n使用方法:\n/answer <question_id> <answer>\n\n例如:\n/answer q_123456 1\n/answer q_123456 yes"
		_ = h.sendTextChunks(ctx, target, msg)
		return "handled", nil
	}

	questionID := parts[1]
	answer := strings.Join(parts[2:], " ")

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
	log.Printf("feishu: submitting answer '%s' for question %s", answer, questionID)

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
