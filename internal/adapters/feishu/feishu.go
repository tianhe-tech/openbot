package feishu

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/user/opencode-gateway/internal/adapters/base"
	"github.com/user/opencode-gateway/internal/opencode"
)

// Config stores FeiShu required secrets.
type Config struct {
	AppID             string
	AppSecret         string
	VerificationToken string
	EncryptKey        string
}

// Handler reacts to FeiShu event callbacks and supports bidirectional communication.
type Handler struct {
	client  *opencode.Client
	cfg     Config
	adapter *base.BidirectionalAdapter
}

// NewHandler creates an adapter.
func NewHandler(client *opencode.Client, cfg Config) *Handler {
	h := &Handler{
		client: client,
		cfg:    cfg,
	}

	// Create bidirectional adapter with message sender
	h.adapter = base.NewBidirectionalAdapter("feishu", h)

	return h
}

// GetAdapter returns the bidirectional adapter for event routing.
func (h *Handler) GetAdapter() *base.BidirectionalAdapter {
	return h.adapter
}

// SendMessage implements the MessageSender interface to send messages to FeiShu users.
func (h *Handler) SendMessage(ctx context.Context, userID, content string) error {
	// TODO: Implement FeiShu message sending API
	// This would use FeiShu's API to send a message to the user
	log.Printf("feishu: would send message to user %s: %s", userID, content)

	// Example implementation:
	// 1. Get access token
	// 2. Call FeiShu message API
	// 3. POST to https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id

	return nil
}

// Mount registers FeiShu routes.
func (h *Handler) Mount(mux *http.ServeMux) {
	mux.Handle("/feishu/callback", h)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var envelope callbackEnvelope
	if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if envelope.Type == "url_verification" {
		h.handleVerification(w, envelope)
		return
	}

	if envelope.Token != "" && h.cfg.VerificationToken != "" && envelope.Token != h.cfg.VerificationToken {
		http.Error(w, "invalid verification token", http.StatusForbidden)
		return
	}

	if envelope.Event.Message.MessageType != "text" {
		http.Error(w, "unsupported message type", http.StatusNotImplemented)
		return
	}

	// Parse content JSON string
	var contentData messageTextBlock
	if err := json.Unmarshal([]byte(envelope.Event.Message.Content), &contentData); err != nil {
		http.Error(w, "invalid content format", http.StatusBadRequest)
		return
	}

	content := strings.TrimSpace(contentData.Text)
	if content == "" {
		http.Error(w, "empty message", http.StatusBadRequest)
		return
	}

	userID := envelope.Event.Sender.SenderID.OpenID
	threadID := envelope.Event.Message.MessageID

	response, err := h.client.SendMessage(r.Context(), opencode.MessagePayload{
		Channel:  "feishu",
		UserID:   userID,
		ThreadID: threadID,
		Content:  content,
		Metadata: map[string]string{
			"conversation_id": envelope.Event.Message.ChatID,
		},
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("forward failed: %v", err), http.StatusBadGateway)
		return
	}

	// Map user to session for bidirectional communication
	h.adapter.MapUserToSession(userID, response.SessionID)

	log.Printf("feishu: mapped user %s to session %s", userID, response.SessionID)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"msg_type": "text",
		"content": map[string]string{
			"text": response.Reply,
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

// callbackEnvelope follows FeiShu event schema (trimmed down for text messages).
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
		MessageID   string `json:"message_id"`
		MessageType string `json:"message_type"`
		ChatID      string `json:"chat_id"`
		Content     string `json:"content"` // JSON string that needs to be parsed
	} `json:"message"`
}

type messageTextBlock struct {
	Text string `json:"text"`
}
