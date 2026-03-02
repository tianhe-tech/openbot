package wecom

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

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

// RegisterCronSession registers a cron session into the adapter.
// Implements scheduler.SessionRegistrar interface.
func (h *Handler) RegisterCronSession(sessionID string, metadata map[string]interface{}) {
	cronUserID := fmt.Sprintf("cron:%s", sessionID[:min(12, len(sessionID))])
	h.adapter.MapUserToSession(cronUserID, sessionID)
	log.Printf("wecom: registered cron session %s (cronUser=%s)", sessionID[:min(8, len(sessionID))], cronUserID)
}

// SendMessage implements the MessageSender interface used by the base adapter
// for routing unsolicited events (e.g. permission requests) back to a user.
func (h *Handler) SendMessage(ctx context.Context, channel, userID, content string) error {
	// TODO: send proactive message via WeCom active message API
	log.Printf("wecom[send] -> user=%s channel=%s content=%s", userID, channel, content)
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

	reply, err := h.dispatch(r.Context(), envelope, content)
	if err != nil {
		http.Error(w, fmt.Sprintf("forward failed: %v", err), http.StatusBadGateway)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"reply": reply,
	})
}

// dispatch routes the message: commands are handled inline; plain messages use
// streaming so that incremental deltas are collected and returned as a reply.
func (h *Handler) dispatch(ctx context.Context, env callbackEnvelope, content string) (string, error) {
	userID := env.FromUserID

	// command routing
	if content == "/help" || content == "帮助" {
		return h.handleHelp()
	}
	if content == "/fork" || content == "派生" {
		return h.handleFork(ctx, userID)
	}
	if content == "/compact" || content == "/summarize" || content == "总结" {
		return h.handleCompact(ctx, userID)
	}
	if content == "/todo" || content == "/todos" || content == "任务" {
		return h.handleTodo(userID)
	}
	if content == "/diff" || content == "/changes" || content == "变更" {
		return h.handleDiff(userID)
	}
	if content == "/abort" || content == "/stop" || content == "停止" {
		return h.handleAbort(ctx, userID)
	}
	if content == "/status" || content == "状态" {
		return h.handleStatus(userID)
	}

	// normal message  streaming session
	sessionID, _ := h.adapter.GetSessionForUser(userID)

	// Collect streamed chunks and return the combined reply.
	var mu sync.Mutex
	var chunks []string
	sessionMapped := false

	sendCtx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	response, err := h.client.SendMessageStreaming(sendCtx, opencode.MessagePayload{
		Channel:   "wecom",
		UserID:    userID,
		ThreadID:  env.RoomID,
		SessionID: sessionID,
		Content:   content,
		Streaming: true,
		Metadata: map[string]string{
			"msg_type": env.MsgType,
		},
	}, func(chunk string) error {
		// First callback with a session-ID-like value is a mapping signal.
		if !sessionMapped && strings.HasPrefix(chunk, "ses_") && len(chunk) < 100 {
			h.adapter.MapUserToSession(userID, chunk)
			log.Printf("wecom: mapped user %s to session %s", userID, chunk[:min(8, len(chunk))])
			sessionMapped = true
			return nil
		}
		if chunk == "" {
			return nil
		}
		mu.Lock()
		chunks = append(chunks, chunk)
		mu.Unlock()
		return nil
	})
	if err != nil {
		log.Printf("wecom: streaming error for user %s: %v", userID, err)
		return "", fmt.Errorf("streaming: %w", err)
	}

	mu.Lock()
	reply := strings.Join(chunks, "")
	mu.Unlock()

	// Fall back to the synchronous reply field if no chunks were streamed.
	if reply == "" {
		reply = response.Reply
	}
	if reply == "" {
		reply = " 处理完成"
	}
	_ = h.SendMessage(ctx, "wecom", userID, reply)
	return reply, nil
}

//  command handlers

func (h *Handler) handleHelp() (string, error) {
	helpText := ` OpenCode Gateway (企业微信)

 可用命令：
/help 或 帮助      - 显示此帮助
/fork 或 派生      - 派生当前会话（保留历史，开启新分支）
/compact 或 总结   - 压缩会话历史，减少上下文占用
/todo 或 任务      - 查看当前任务进度列表
/diff 或 变更      - 查看本次会话的文件变更摘要
/abort 或 停止     - 中止正在运行的任务
/status 或 状态    - 查看当前会话状态

 直接发送消息即可与 AI 对话`
	return helpText, nil
}

func (h *Handler) handleFork(ctx context.Context, userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "ℹ 当前没有活跃的会话，发送消息将自动创建新会话", nil
	}
	newSessionID, err := h.client.ForkSession(ctx, sessionID)
	if err != nil {
		return "", fmt.Errorf("fork session: %w", err)
	}
	h.adapter.MapUserToSession(userID, newSessionID)
	return fmt.Sprintf(" 已派生新会话\n原: %s  新: %s\n继续对话将使用新的派生会话",
		sessionID[:min(8, len(sessionID))], newSessionID[:min(8, len(newSessionID))]), nil
}

func (h *Handler) handleCompact(ctx context.Context, userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "ℹ 当前没有活跃的会话", nil
	}
	if err := h.client.SummarizeSession(ctx, sessionID); err != nil {
		return "", fmt.Errorf("compact session: %w", err)
	}
	return fmt.Sprintf(" 会话 %s 已压缩总结\n上下文占用将减少，后续对话继续本次会话。",
		sessionID[:min(8, len(sessionID))]), nil
}

func (h *Handler) handleTodo(userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "ℹ 当前没有活跃的会话", nil
	}
	todos := h.client.GetTodosForSession(sessionID)
	if len(todos) == 0 {
		return " 当前没有进行中的任务", nil
	}
	var sb strings.Builder
	sb.WriteString(" 当前任务进度:\n\n")
	pending, inProgress, completed := 0, 0, 0
	for _, todo := range todos {
		icon := ""
		switch todo.Status {
		case "completed":
			icon = ""
			completed++
		case "in_progress":
			icon = ""
			inProgress++
		case "cancelled":
			icon = ""
		default:
			pending++
		}
		sb.WriteString(fmt.Sprintf("%s [优先级:%s] %s\n", icon, todo.PriorityLabel(), todo.Text()))
	}
	sb.WriteString(fmt.Sprintf("\n进度: %d 完成, %d 进行中, %d 待处理", completed, inProgress, pending))
	return sb.String(), nil
}

func (h *Handler) handleDiff(userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "ℹ 当前没有活跃的会话", nil
	}
	diff := h.client.GetDiffForSession(sessionID)
	if len(diff) == 0 {
		return " 本次会话暂无文件变更", nil
	}
	var sb strings.Builder
	sb.WriteString(" 文件变更摘要:\n\n")
	totalAdded, totalRemoved := 0, 0
	for _, f := range diff {
		icon := ""
		if f.Added > 0 && f.Removed == 0 {
			icon = ""
		} else if f.Added == 0 && f.Removed > 0 {
			icon = ""
		}
		sb.WriteString(fmt.Sprintf("%s %s (+%d/-%d)\n", icon, f.Path, f.Added, f.Removed))
		totalAdded += f.Added
		totalRemoved += f.Removed
	}
	sb.WriteString(fmt.Sprintf("\n共 %d 个文件，+%d/-%d 行", len(diff), totalAdded, totalRemoved))
	return sb.String(), nil
}

func (h *Handler) handleAbort(ctx context.Context, userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "ℹ 当前没有活跃的会话", nil
	}
	if err := h.client.AbortSession(ctx, sessionID); err != nil {
		return "", fmt.Errorf("abort session: %w", err)
	}
	return fmt.Sprintf(" 已中止会话 %s", sessionID[:min(8, len(sessionID))]), nil
}

func (h *Handler) handleStatus(userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "ℹ 无活跃会话", nil
	}
	return fmt.Sprintf(" 当前会话: %s", sessionID[:min(8, len(sessionID))]), nil
}

//  envelope types

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
