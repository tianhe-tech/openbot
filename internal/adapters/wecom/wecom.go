package wecom

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/user/opencode-gateway/internal/adapters/base"
	"github.com/user/opencode-gateway/internal/opencode"
)

// Config captures credentials required by Enterprise WeChat.
type Config struct {
	Token          string
	EncodingAESKey string
	CorpID         string
	AgentID        string
}

// Handler processes WeCom callbacks and forwards them to OpenCode.
type Handler struct {
	client  *opencode.Client
	cfg     Config
	adapter *base.BidirectionalAdapter
}

// NewHandler wires the adapter with an OpenCode client instance.
func NewHandler(client *opencode.Client, cfg Config) *Handler {
	h := &Handler{
		client: client,
		cfg:    cfg,
	}

	h.adapter = base.NewBidirectionalAdapter("wecom", h)

	return h
}

// GetAdapter returns the bidirectional adapter for event routing.
func (h *Handler) GetAdapter() *base.BidirectionalAdapter {
	return h.adapter
}

// RegisterCronSession 注册定时任务session到adapter
// 实现 scheduler.SessionRegistrar 接口
func (h *Handler) RegisterCronSession(sessionID string, metadata map[string]interface{}) {
	cronUserID := fmt.Sprintf("cron:%s", sessionID[:min(12, len(sessionID))])
	h.adapter.MapUserToSession(cronUserID, sessionID)
	log.Printf("wecom: registered cron session %s (cronUser=%s)", sessionID[:min(8, len(sessionID))], cronUserID)
}

// SendMessage implements the MessageSender interface.
func (h *Handler) SendMessage(ctx context.Context, channel, userID, content string) error {
	log.Printf("wecom: would send message to channel %s, user %s: %s", channel, userID, content)
	// TODO: Implement WeCom message sending API
	return nil
}

// Mount registers the handler on the given mux.
func (h *Handler) Mount(mux *http.ServeMux) {
	mux.Handle("/wecom/callback", h)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleChallenge(w, r)
	case http.MethodPost:
		h.handleEvent(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handleChallenge(w http.ResponseWriter, r *http.Request) {
	echostr := r.URL.Query().Get("echostr")
	if echostr == "" {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("missing echostr"))
		return
	}
	_, _ = w.Write([]byte(echostr))
}

func (h *Handler) handleEvent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var envelope callbackEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	content := strings.TrimSpace(envelope.Text.Content)
	if content == "" {
		http.Error(w, "empty message", http.StatusBadRequest)
		return
	}

	response, err := h.client.SendMessage(r.Context(), opencode.MessagePayload{
		Channel:  "wecom",
		UserID:   envelope.FromUserID,
		ThreadID: envelope.RoomID,
		Content:  content,
		Metadata: map[string]string{
			"event":    envelope.Event,
			"msg_type": envelope.MsgType,
		},
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("forward failed: %v", err), http.StatusBadGateway)
		return
	}

	// Map user to session for bidirectional communication
	h.adapter.MapUserToSession(envelope.FromUserID, response.SessionID)
	log.Printf("wecom: mapped user %s to session %s", envelope.FromUserID, response.SessionID)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"reply":      response.Reply,
		"trace":      response.Trace,
		"session_id": response.SessionID,
	})
}

// callbackEnvelope only models the subset of fields we currently need.
type callbackEnvelope struct {
	MsgType    string       `json:"msgtype"`
	Event      string       `json:"event"`
	FromUserID string       `json:"from_userid"`
	RoomID     string       `json:"roomid"`
	Text       textEnvelope `json:"text"`
}

// textEnvelope contains the user provided text.
type textEnvelope struct {
	Content string `json:"content"`
}
