package dingtalk

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/client"
	"github.com/user/opencode-gateway/internal/adapters/base"
	"github.com/user/opencode-gateway/internal/opencode"
)

// Config stores DingTalk adapter settings.
type Config struct {
	ClientID          string // Stream 模式使用 Client ID
	ClientSecret      string // Stream 模式使用 Client Secret
	AppKey            string // 传统 Webhook 模式（保留兼容）
	AppSecret         string
	VerificationToken string
	EncryptKey        string
	SigningSecret     string
	UseStream         bool // 是否使用 Stream 模式
}

// Handler processes DingTalk callbacks and proxies them to OpenCode.
// Supports both Stream mode and traditional Webhook mode.
type Handler struct {
	client       *opencode.Client
	cfg          Config
	adapter      *base.BidirectionalAdapter
	streamClient *client.StreamClient
}

// NewHandler wires the adapter with an OpenCode client.
func NewHandler(client *opencode.Client, cfg Config) *Handler {
	h := &Handler{
		client: client,
		cfg:    cfg,
	}

	h.adapter = base.NewBidirectionalAdapter("dingtalk", h)

	return h
}

// Start initializes the DingTalk adapter.
// If Stream mode is enabled, it starts the Stream client.
func (h *Handler) Start(ctx context.Context) error {
	if !h.cfg.UseStream {
		log.Println("dingtalk: using traditional webhook mode")
		return nil
	}

	if h.cfg.ClientID == "" || h.cfg.ClientSecret == "" {
		return fmt.Errorf("dingtalk: ClientID and ClientSecret are required for Stream mode")
	}

	log.Println("dingtalk: starting Stream mode connection...")
	log.Printf("dingtalk: using ClientID: %s...", h.cfg.ClientID[:20])

	// Create Stream client
	h.streamClient = client.NewStreamClient(
		client.WithAppCredential(
			client.NewAppCredentialConfig(h.cfg.ClientID, h.cfg.ClientSecret),
		),
	)

	// Register callback for chat bot messages
	h.streamClient.RegisterChatBotCallbackRouter(h.onChatBotMessageReceived)

	// Start in background
	go func() {
		log.Println("dingtalk: starting Stream client connection...")
		if err := h.streamClient.Start(ctx); err != nil {
			log.Printf("dingtalk stream error: %v", err)
		} else {
			log.Println("dingtalk: Stream client connected successfully")
		}
	}()

	log.Println("dingtalk: Stream mode client started (connecting in background)")
	return nil
}

// Stop closes the Stream client if running.
func (h *Handler) Stop() {
	if h.streamClient != nil {
		h.streamClient.Close()
		log.Println("dingtalk: Stream client closed")
	}
}

// onChatBotMessageReceived handles incoming messages from DingTalk Stream.
func (h *Handler) onChatBotMessageReceived(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
	content := strings.TrimSpace(data.Text.Content)
	if content == "" {
		return nil, fmt.Errorf("empty message")
	}

	userID := data.SenderStaffId
	conversationID := data.ConversationId

	log.Printf("dingtalk stream: received message from %s: %s", userID, content)

	// Handle special commands
	if content == "/skills" || content == "/agents" {
		return h.handleListSkills(ctx, data)
	}

	if content == "/help" || content == "帮助" {
		return h.handleHelp(ctx, data)
	}

	// Handle /abort command to abort running session
	if content == "/abort" || content == "/stop" || content == "停止" {
		return h.handleAbort(ctx, data, userID)
	}

	// Handle /cmd command to execute skill scripts directly
	if strings.HasPrefix(content, "/cmd ") {
		command := strings.TrimPrefix(content, "/cmd ")
		return h.handleExecuteCommand(ctx, data, userID, command)
	}

	// Handle /refresh command to refresh skill cache
	if content == "/refresh" {
		replier := chatbot.NewChatbotReplier()
		if err := h.client.RefreshSkills(ctx); err != nil {
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 刷新技能缓存失败: "+err.Error()))
		} else {
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ 技能缓存已刷新"))
		}
		return nil, nil
	}

	// Parse agent specification: @agent_name message content
	var agentName string
	if strings.HasPrefix(content, "@") {
		parts := strings.SplitN(content[1:], " ", 2)
		if len(parts) == 2 {
			agentName = parts[0]
			content = parts[1]
			log.Printf("dingtalk stream: using agent '%s' for message", agentName)
		}
	}

	// Send to OpenCode with streaming
	replier := chatbot.NewChatbotReplier()

	// Send initial "thinking" message with estimated wait time
	thinkingMsg := "⏳ 正在处理中，请稍候...\n💡 提示: 复杂问题可能需要几分钟时间"
	// 如果指定了agent，添加特定提示
	if agentName != "" {
		thinkingMsg = fmt.Sprintf("⏳ 正在使用 @%s 处理中...", agentName)
		// 如果是build相关的agent，特别提醒
		if strings.Contains(strings.ToLower(agentName), "build") {
			thinkingMsg += "\n\n⚠️ 重要提示：build模式需要在OpenCode界面确认操作！"
			thinkingMsg += "\n请在OpenCode中确认后，结果会自动回复到这里。"
		}
	}
	if err := replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(thinkingMsg)); err != nil {
		log.Printf("dingtalk stream: failed to send thinking message: %v", err)
	}

	// 使用独立的context，避免被钉钉SDK的context超时影响
	// 给予充足的超时时间，让OpenCode能够完成复杂任务
	timeout := 20 * time.Minute // 默认20分钟
	if agentName != "" {
		timeout = 30 * time.Minute // agent模式给30分钟
	}
	sendCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	log.Printf("dingtalk stream: sending to OpenCode (timeout: %v, agent: %s, content_len: %d)",
		timeout, agentName, len(content))

	// Use streaming to get response with progress updates
	var fullReply strings.Builder
	var lastSentLength int
	var updateCount int
	const maxUpdates = 5 // 最多发送5次中间更新，避免过于频繁

	response, err := h.client.SendMessageStreaming(sendCtx, opencode.MessagePayload{
		Channel:   "dingtalk",
		UserID:    userID,
		ThreadID:  conversationID,
		Content:   content,
		Agent:     agentName,
		Streaming: true,
		Metadata: map[string]string{
			"conversation_type": data.ConversationType,
			"sender_nick":       data.SenderNick,
		},
	}, func(chunk string) error {
		// 处理进度更新
		if strings.HasPrefix(chunk, "⏳") || strings.HasPrefix(chunk, "⏱️") {
			// 这是进度提示消息，直接发送
			log.Printf("dingtalk stream: sending progress update: %s", chunk)
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(chunk))
			return nil
		}

		// 累积实际内容
		fullReply.WriteString(chunk)
		currentLength := fullReply.Len()

		// 如果累积了足够多的新内容（至少500字符）且还没超过最大更新次数，发送中间结果
		if updateCount < maxUpdates && currentLength-lastSentLength >= 500 {
			log.Printf("dingtalk stream: sending intermediate update (update %d/%d, new content: %d chars)",
				updateCount+1, maxUpdates, currentLength-lastSentLength)

			// 发送中间结果（带有标记表明这不是最终结果）
			intermediateMsg := fmt.Sprintf("📝 中间结果 (%d/%d):\n\n%s\n\n⏳ 继续处理中...",
				updateCount+1, maxUpdates, fullReply.String())
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(intermediateMsg))

			lastSentLength = currentLength
			updateCount++
		}

		return nil
	})

	if err != nil {
		var errMsg string
		if strings.Contains(err.Error(), "duplicate request") {
			errMsg = "⚠️ 您的请求正在处理中，请勿重复发送\n" +
				"💡 这通常是因为您在30秒内发送了相同的消息"
			log.Printf("dingtalk stream: duplicate request from user %s", userID)
		} else if strings.Contains(err.Error(), "max retries exceeded") {
			errMsg = "❌ 服务暂时不可用（已重试多次失败）\n" +
				"💡 建议：请稍等片刻后重试"
			log.Printf("dingtalk stream: max retries for user %s", userID)
		} else if strings.Contains(err.Error(), "context deadline") || strings.Contains(err.Error(), "timeout") {
			// 区分是否使用了build模式
			if agentName != "" && strings.Contains(strings.ToLower(agentName), "build") {
				errMsg = "⏱️ 等待超时\n\n" +
					"⚠️ 您正在使用build模式，这需要在OpenCode界面手动确认！\n" +
					"请检查：\n" +
					"1. 在OpenCode中是否有待确认的操作\n" +
					"2. 确认后结果会自动回复\n" +
					"3. 或者使用chat模式进行普通对话"
			} else {
				// 非build模式的超时，可能是任务太复杂
				errMsg = "⏱️ 处理超时（已等待20-30分钟）\n\n" +
					"这可能是因为：\n" +
					"1. 任务非常复杂（如模型微调、大规模代码生成）\n" +
					"2. OpenCode需要更多时间处理\n" +
					"3. OpenCode可能在等待外部资源\n\n" +
					"建议：\n" +
					"• 请在OpenCode界面查看任务进度\n" +
					"• 简化您的请求后重试\n" +
					"• 或将任务拆分成多个小步骤"
			}
			log.Printf("dingtalk stream: timeout for user %s, agent=%s", userID, agentName)
		} else {
			errMsg = fmt.Sprintf("❌ 处理失败: %v", err)
			log.Printf("dingtalk stream: error for user %s: %v", userID, err)
		}
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(errMsg))
		return nil, err
	}

	// Map user to session
	h.adapter.MapUserToSession(userID, response.SessionID)
	log.Printf("dingtalk stream: mapped user %s to session %s", userID, response.SessionID)

	// Send final complete reply
	if err := replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(response.Reply)); err != nil {
		log.Printf("dingtalk stream: failed to reply: %v", err)
		return nil, err
	}

	log.Printf("dingtalk stream: replied to user %s", userID)
	return nil, nil
}

// handleListSkills handles the /skills command to list available agents.
func (h *Handler) handleListSkills(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
	agents, err := h.client.ListAgents(ctx)
	if err != nil {
		log.Printf("dingtalk: failed to list agents: %v", err)
		return nil, err
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
	replier := chatbot.NewChatbotReplier()
	replyText := []byte(reply.String())
	if err := replier.SimpleReplyText(ctx, data.SessionWebhook, replyText); err != nil {
		log.Printf("dingtalk: failed to reply: %v", err)
		return nil, err
	}

	return nil, nil
}

// handleExecuteCommand handles direct command execution like skill scripts
func (h *Handler) handleExecuteCommand(ctx context.Context, data *chatbot.BotCallbackDataModel, userID, command string) ([]byte, error) {
	// Get or create session for user
	var sessionID string
	if sid, ok := h.adapter.GetSessionForUser(userID); ok {
		sessionID = sid
	} else {
		// Create new session if needed
		response, err := h.client.SendMessage(ctx, opencode.MessagePayload{
			Channel:  "dingtalk",
			UserID:   userID,
			ThreadID: data.ConversationId,
			Content:  "Initialize session",
		})
		if err != nil {
			log.Printf("dingtalk: failed to create session: %v", err)
			return nil, err
		}
		sessionID = response.SessionID
		h.adapter.MapUserToSession(userID, sessionID)
	}

	log.Printf("dingtalk: executing command in session %s: %s", sessionID, command)

	// Execute command
	result, err := h.client.ExecuteShell(ctx, sessionID, command)
	if err != nil {
		log.Printf("dingtalk: command execution failed: %v", err)
		errMsg := fmt.Sprintf("❌ 命令执行失败: %v", err)
		replier := chatbot.NewChatbotReplier()
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(errMsg))
		return nil, err
	}

	// Build response message
	var reply string
	if result != nil {
		reply = fmt.Sprintf("🖥️ 命令执行结果:\n\n```\n%s\n```", result.ID)
	} else {
		reply = "🖥️ 命令执行完成"
	}

	// Send reply
	replier := chatbot.NewChatbotReplier()
	if err := replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(reply)); err != nil {
		log.Printf("dingtalk: failed to reply: %v", err)
		return nil, err
	}

	return nil, nil
}

// GetAdapter returns the bidirectional adapter for event routing.
func (h *Handler) GetAdapter() *base.BidirectionalAdapter {
	return h.adapter
}

// SendMessage implements the MessageSender interface.
// Used for bidirectional communication to push messages to users.
func (h *Handler) SendMessage(ctx context.Context, userID, content string) error {
	// TODO: Implement DingTalk active message sending
	// For Stream mode, this would use the robot API to send messages
	// For Webhook mode, this would use the webhook URL
	log.Printf("dingtalk: would send message to user %s: %s", userID, content)
	return nil
}

// Mount registers the DingTalk webhook callback endpoint.
// This is used for traditional webhook mode, not Stream mode.
func (h *Handler) Mount(mux *http.ServeMux) {
	if h.cfg.UseStream {
		log.Println("dingtalk: Stream mode enabled, webhook endpoint not registered")
		return
	}
	mux.Handle("/dingtalk/callback", h)
	log.Println("dingtalk: webhook endpoint registered at /dingtalk/callback")
}

// ServeHTTP handles traditional webhook callbacks (non-Stream mode).
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.cfg.UseStream {
		http.Error(w, "webhook mode disabled, using Stream mode", http.StatusNotImplemented)
		return
	}

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
		http.Error(w, "invalid verification token", http.StatusForbidden)
		return
	}

	if envelope.MsgType != "text" {
		http.Error(w, "unsupported message type", http.StatusNotImplemented)
		return
	}

	content := strings.TrimSpace(envelope.Text.Content)
	if content == "" {
		http.Error(w, "empty message", http.StatusBadRequest)
		return
	}

	response, err := h.client.SendMessage(r.Context(), opencode.MessagePayload{
		Channel:  "dingtalk",
		UserID:   envelope.SenderStaffID,
		ThreadID: envelope.ConversationID,
		Content:  content,
		Metadata: map[string]string{
			"conversation_type": envelope.ConversationType,
			"robot_code":        envelope.RobotCode,
		},
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("forward failed: %v", err), http.StatusBadGateway)
		return
	}

	// Map user to session for bidirectional communication
	h.adapter.MapUserToSession(envelope.SenderStaffID, response.SessionID)
	log.Printf("dingtalk webhook: mapped user %s to session %s", envelope.SenderStaffID, response.SessionID)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"msgtype": "text",
		"text": map[string]string{
			"content": response.Reply,
		},
		"trace":      response.Trace,
		"session_id": response.SessionID,
	})
}

func (h *Handler) handleVerification(w http.ResponseWriter, env callbackEnvelope) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"challenge": env.Challenge,
	})
}

// callbackEnvelope covers the subset of DingTalk robot event fields we rely on.
type callbackEnvelope struct {
	Type             string       `json:"type"`
	Token            string       `json:"token"`
	Challenge        string       `json:"challenge"`
	MsgType          string       `json:"msgtype"`
	ConversationID   string       `json:"conversationId"`
	ConversationType string       `json:"conversationType"`
	SenderStaffID    string       `json:"senderStaffId"`
	RobotCode        string       `json:"robotCode"`
	Text             textEnvelope `json:"text"`
}

type textEnvelope struct {
	Content string `json:"content"`
}

// handleHelp 处理帮助命令
func (h *Handler) handleHelp(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
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
   - 确认后结果会自动回复到钉钉
   - 适合：文件修改、代码生成等

💡 使用技巧：
• 使用 @agent_name 调用特定技能
  例如：@build 帮我创建一个Python脚本
• Build模式请求会提示您去OpenCode确认
• 请勿在30秒内重复发送相同消息
• 发送 /abort 或 /stop 可以中止正在运行的任务

🛠️ 可用命令：
/help 或 帮助 - 显示帮助
/skills - 查看可用技能
/abort 或 /stop - 中止当前任务
/refresh - 刷新技能缓存

❓ 问题排查：
• 如果提示"请求处理中"：请等待当前请求完成
• 如果超时：可能是build模式等待确认
• 如果失败：稍后重试或简化问题`

	replier := chatbot.NewChatbotReplier()
	if err := replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(helpText)); err != nil {
		log.Printf("dingtalk: failed to send help: %v", err)
		return nil, err
	}

	return nil, nil
}

// handleAbort 处理中止命令
func (h *Handler) handleAbort(ctx context.Context, data *chatbot.BotCallbackDataModel, userID string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	// 获取用户的session
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 未找到活动的会话"))
		return nil, nil
	}

	// 检查session是否正在运行
	if !h.client.IsSessionRunning(sessionID) {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("ℹ️ 当前没有正在运行的任务"))
		return nil, nil
	}

	// 中止session
	log.Printf("dingtalk: aborting session %s for user %s", sessionID[:8], userID)
	if err := h.client.AbortSession(ctx, sessionID); err != nil {
		errMsg := fmt.Sprintf("❌ 中止任务失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(errMsg))
		log.Printf("dingtalk: failed to abort session %s: %v", sessionID, err)
		return nil, err
	}

	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ 任务已中止"))
	return nil, nil
}
