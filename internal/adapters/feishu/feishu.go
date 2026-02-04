package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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

	sessionID, _ := h.adapter.GetSessionForUser(msg.UserID)
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
			"message_id":   msg.MessageID,
			"message_type": msg.MessageType,
			"chat_type":    msg.ChatType,
			"chat_id":      msg.ChatID,
		},
	}, callback)
	if err != nil {
		errMsg := fmt.Sprintf("❌ 处理失败: %v", err)
		_ = h.sendTextChunks(ctx, target, errMsg)
		return "", err
	}

	if response.SessionID != "" {
		h.adapter.MapUserToSession(msg.UserID, response.SessionID)
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
	token, err := h.getAccessToken(ctx)
	if err != nil {
		return err
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

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("feishu: send message: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("feishu: send status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}

	var body struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return fmt.Errorf("feishu: decode send response: %w", err)
	}
	if body.Code != 0 {
		return fmt.Errorf("feishu: send failed code=%d msg=%s", body.Code, body.Msg)
	}

	receiveLabel := target.receiveID
	if len(receiveLabel) > 8 {
		receiveLabel = receiveLabel[:8]
	}
	log.Printf("feishu: sent %d chars to %s(%s)", len(content), target.receiveIDType, receiveLabel)
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
	if channel != "" {
		return chatTarget{receiveID: channel, receiveIDType: "chat_id"}, nil
	}
	if entry, ok := h.userTargets.Load(userID); ok {
		return entry.(chatTarget), nil
	}
	if userID == "" {
		return chatTarget{}, fmt.Errorf("feishu: unable to resolve chat target")
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
