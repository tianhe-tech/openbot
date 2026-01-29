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

	// Send to OpenCode
	response, err := h.client.SendMessage(ctx, opencode.MessagePayload{
		Channel:  "dingtalk",
		UserID:   userID,
		ThreadID: conversationID,
		Content:  content,
		Metadata: map[string]string{
			"conversation_type": data.ConversationType,
			"sender_nick":       data.SenderNick,
		},
	})
	if err != nil {
		log.Printf("dingtalk stream: failed to send to OpenCode: %v", err)
		return nil, err
	}

	// Map user to session
	h.adapter.MapUserToSession(userID, response.SessionID)
	log.Printf("dingtalk stream: mapped user %s to session %s", userID, response.SessionID)

	// Reply to user via Stream
	replier := chatbot.NewChatbotReplier()
	replyText := []byte(response.Reply)
	if err := replier.SimpleReplyText(ctx, data.SessionWebhook, replyText); err != nil {
		log.Printf("dingtalk stream: failed to reply: %v", err)
		return nil, err
	}

	log.Printf("dingtalk stream: replied to user %s", userID)
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
