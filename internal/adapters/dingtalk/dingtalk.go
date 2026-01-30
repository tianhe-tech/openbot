package dingtalk

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

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

	// Handle /cmd command to execute skill scripts directly
	if strings.HasPrefix(content, "/cmd ") {
		command := strings.TrimPrefix(content, "/cmd ")
		return h.handleExecuteCommand(ctx, data, userID, command)
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

	// Send initial "thinking" message
	thinkingMsg := "⏳ 正在思考中..."
	if err := replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(thinkingMsg)); err != nil {
		log.Printf("dingtalk stream: failed to send thinking message: %v", err)
	}

	// Use streaming to get response
	var fullReply strings.Builder
	response, err := h.client.SendMessageStreaming(ctx, opencode.MessagePayload{
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
		// Accumulate chunks and send updates
		fullReply.WriteString(chunk)
		// Send update every few chunks to avoid too many updates
		return nil
	})

	if err != nil {
		errMsg := fmt.Sprintf("❌ 处理失败: %v", err)
		replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(errMsg))
		log.Printf("dingtalk stream: failed to send to OpenCode: %v", err)
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
