package wecom

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/user/opencode-gateway/internal/adapters/base"
	"github.com/user/opencode-gateway/internal/opencode"
	"github.com/user/opencode-gateway/internal/scheduler"
)

const (
	wecomAPIEndpoint = "https://qyapi.weixin.qq.com/cgi-bin"
)

// Config captures credentials required by Enterprise WeChat.
type Config struct {
	Token          string
	EncodingAESKey string
	CorpID         string
	CorpSecret     string
	AgentID        string
}

// Handler processes WeCom callbacks and forwards them to OpenCode.
type Handler struct {
	client          *opencode.Client
	cfg             Config
	adapter         *base.BidirectionalAdapter
	cronScheduler   *scheduler.CronScheduler
	nlScheduleSvc   *scheduler.NLScheduleService
	httpClient      *http.Client
	tokenMu         sync.Mutex
	accessToken     string
	tokenExpiry     time.Time
	overflowPolicy  sync.Map
	overflowPending sync.Map
}

const (
	wecomOverflowPolicyAsk     = "ask"
	wecomOverflowPolicySummary = "summary"
	wecomOverflowPolicyNew     = "new"
)

type wecomTokenOverflowPendingState struct {
	UserID      string
	SessionID   string
	ThreadID    string
	Content     string
	Attachments []opencode.Attachment
	Metadata    map[string]string
	CreatedAt   time.Time
	Executing   bool // 标记 executeTokenOverflowDecision 是否已在执行中
}

// NewHandler wires the adapter with an OpenCode client instance.
func NewHandler(client *opencode.Client, cfg Config) *Handler {
	h := &Handler{
		client: client,
		cfg:    cfg,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
	h.adapter = base.NewBidirectionalAdapter("wecom", h)
	return h
}

// GetAdapter returns the bidirectional adapter for event routing.
func (h *Handler) GetAdapter() *base.BidirectionalAdapter {
	return h.adapter
}

// SetCronScheduler sets cron scheduler for scheduled task management.
func (h *Handler) SetCronScheduler(cronScheduler *scheduler.CronScheduler) {
	h.cronScheduler = cronScheduler
}

// SetNLScheduleService sets natural-language scheduling service.
func (h *Handler) SetNLScheduleService(svc *scheduler.NLScheduleService) {
	h.nlScheduleSvc = svc
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
	text := strings.TrimSpace(content)
	if text == "" {
		return nil
	}
	log.Printf("wecom: proactive send start channel=%q user=%q len=%d", strings.TrimSpace(channel), strings.TrimSpace(userID), len(text))

	// Prefer group chat when channel carries room/chat id; fallback to direct user.
	if ch := strings.TrimSpace(channel); ch != "" && ch != "wecom" {
		if err := h.sendAppChatText(ctx, ch, text); err == nil {
			log.Printf("wecom: proactive send succeeded via appchat channel=%q", ch)
			return nil
		} else {
			log.Printf("wecom: proactive send via appchat failed channel=%q err=%v", ch, err)
		}
		log.Printf("wecom: proactive send fallback to direct user channel=%q user=%q", ch, strings.TrimSpace(userID))
	}

	uid := strings.TrimSpace(userID)
	if uid == "" || strings.HasPrefix(uid, "cron:") {
		return fmt.Errorf("wecom proactive send requires a concrete user id when chat channel is unavailable")
	}
	if err := h.sendTextToUser(ctx, uid, text); err != nil {
		log.Printf("wecom: proactive send direct user failed user=%q err=%v", uid, err)
		return err
	}
	log.Printf("wecom: proactive send succeeded via direct user user=%q", uid)
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
	if err := h.parseCallbackEnvelope(body, &envelope); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}

	msg, parseErr := h.parseIncomingMessage(r.Context(), envelope)
	if parseErr != nil {
		http.Error(w, fmt.Sprintf("invalid message: %v", parseErr), http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(msg.Content) == "" {
		http.Error(w, "empty message", http.StatusBadRequest)
		return
	}

	reply, err := h.dispatch(r.Context(), envelope, msg)
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
func (h *Handler) dispatch(ctx context.Context, env callbackEnvelope, msg wecomIncomingMessage) (string, error) {
	userID := env.FromUserID
	content := msg.Content
	threadID := strings.TrimSpace(env.RoomID)
	if threadID == "" {
		threadID = userID
	}

	if msg.MsgType == "text" {
		if handled, reply, err := h.handleTokenOverflowQuickReply(ctx, userID, content); handled || err != nil {
			return reply, err
		}
	}

	// command routing
	if msg.MsgType == "text" && (content == "/help" || content == "帮助") {
		return h.handleHelp()
	}
	if msg.MsgType == "text" && (content == "/fork" || content == "派生") {
		return h.handleFork(ctx, userID)
	}
	if msg.MsgType == "text" && (content == "/todo" || content == "/todos" || content == "任务") {
		return h.handleTodo(userID)
	}
	if msg.MsgType == "text" && (content == "/diff" || content == "/changes" || content == "变更") {
		return h.handleDiff(userID)
	}
	if msg.MsgType == "text" && (content == "/abort" || content == "/stop" || content == "停止") {
		return h.handleAbort(ctx, userID)
	}
	if msg.MsgType == "text" && (content == "/status" || content == "状态") {
		return h.handleStatus(userID)
	}
	if msg.MsgType == "text" && (content == "/summary" || content == "压缩" || content == "总结") {
		return h.handleSummary(userID)
	}
	if msg.MsgType == "text" && strings.HasPrefix(content, "/devcore") {
		return h.handleDevCore(content)
	}
	if msg.MsgType == "text" && strings.HasPrefix(content, "/crontask") {
		if handled, reply, err := h.handleCronTask(ctx, env, msg, content); handled || err != nil {
			return reply, err
		}
	}

	if msg.MsgType == "text" {
		if handled, reply, err := h.tryHandleNLSchedule(ctx, env, msg, content); handled || err != nil {
			return reply, err
		}
	}

	// normal message  streaming session
	sessionID, _ := h.adapter.GetSessionForUser(userID)

	// Collect streamed chunks and return the combined reply.
	var mu sync.Mutex
	var chunks []string
	var thinking strings.Builder
	var meta []string
	sessionMapped := false

	sendCtx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	metadata := map[string]string{
		"msg_type": env.MsgType,
	}
	sendContent := content
	if len(msg.MediaFiles) > 0 && (msg.MsgType == "file" || msg.MsgType == "video") {
		taskSessionID := sessionID
		if strings.TrimSpace(taskSessionID) == "" {
			taskSessionID = "new"
		}
		mediaCtx := base.MediaTaskContext{
			Platform:    "wecom",
			MessageType: msg.MsgType,
			UserID:      userID,
			SessionID:   taskSessionID,
			MessageID:   env.MsgID,
			Files:       msg.MediaFiles,
		}
		if mediaMD, mdErr := base.BuildMediaMetadata(mediaCtx); mdErr != nil {
			log.Printf("wecom: failed to build media metadata: %v", mdErr)
		} else {
			for k, v := range mediaMD {
				metadata[k] = v
			}
			sendContent = base.BuildMediaPromptPrefix(mediaCtx) + sendContent
		}
	}

	response, err := h.client.SendMessageStreaming(sendCtx, opencode.MessagePayload{
		Channel:     "wecom",
		UserID:      userID,
		ThreadID:    threadID,
		SessionID:   sessionID,
		Content:     sendContent,
		Streaming:   true,
		Attachments: msg.Attachments,
		Metadata:    metadata,
	}, func(chunk string) error {
		if h.isTokenOverflowErrorText(chunk) {
			state := &wecomTokenOverflowPendingState{
				UserID:      userID,
				SessionID:   sessionID,
				ThreadID:    threadID,
				Content:     sendContent,
				Attachments: append([]opencode.Attachment(nil), msg.Attachments...),
				Metadata:    cloneStringMap(metadata),
				CreatedAt:   time.Now(),
			}
			if state.SessionID == "" {
				if sid, ok := h.adapter.GetSessionForUser(userID); ok {
					state.SessionID = sid
				}
			}
			h.storeTokenOverflowPending(userID, state)
			return nil
		}

		// First callback with a session-ID-like value is a mapping signal.
		if !sessionMapped && strings.HasPrefix(chunk, "ses_") && len(chunk) < 100 {
			h.adapter.MapUserToSession(userID, chunk)
			log.Printf("wecom: mapped user %s to session %s", userID, chunk[:min(8, len(chunk))])
			sessionMapped = true
			return nil
		}
		if chunk == opencode.FlushSignal {
			return nil
		}
		if strings.HasPrefix(chunk, opencode.ThinkingSignalPrefix) {
			delta := strings.TrimPrefix(chunk, opencode.ThinkingSignalPrefix)
			if strings.TrimSpace(delta) == "" {
				return nil
			}
			mu.Lock()
			thinking.WriteString(delta)
			mu.Unlock()
			return nil
		}
		if strings.HasPrefix(chunk, opencode.ToolSignalPrefix) {
			msg := strings.TrimSpace(strings.TrimPrefix(chunk, opencode.ToolSignalPrefix))
			if msg != "" {
				mu.Lock()
				meta = append(meta, msg)
				mu.Unlock()
			}
			return nil
		}
		if strings.HasPrefix(chunk, opencode.StepSignalPrefix) {
			msg := strings.TrimSpace(strings.TrimPrefix(chunk, opencode.StepSignalPrefix))
			if msg != "" {
				mu.Lock()
				meta = append(meta, msg)
				mu.Unlock()
			}
			return nil
		}
		if strings.HasPrefix(chunk, opencode.TodoSignalPrefix) {
			msg := strings.TrimSpace(strings.TrimPrefix(chunk, opencode.TodoSignalPrefix))
			if msg != "" {
				mu.Lock()
				meta = append(meta, msg)
				mu.Unlock()
			}
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

	if pending, ok := h.getTokenOverflowPending(userID); ok {
		if pending.ThreadID == threadID && pending.Content == sendContent {
			switch h.getTokenOverflowPolicy(userID) {
			case wecomOverflowPolicySummary:
				return h.executeTokenOverflowDecision(ctx, userID, "summary")
			case wecomOverflowPolicyNew:
				return h.executeTokenOverflowDecision(ctx, userID, "new")
			default:
				return h.buildTokenOverflowPrompt(), nil
			}
		}
	}

	mu.Lock()
	reply := strings.Join(chunks, "")
	thinkingText := strings.TrimSpace(thinking.String())
	metaText := strings.TrimSpace(strings.Join(meta, "\n"))
	mu.Unlock()

	if reply != "" {
		if thinkingText != "" {
			reply = "思考过程:\n" + thinkingText + "\n\n" + reply
		}
		if metaText != "" {
			reply = reply + "\n\n中间过程:\n" + metaText
		}
	}

	// Fall back to the synchronous reply field if no chunks were streamed.
	if reply == "" {
		reply = response.Reply
	}
	if reply == "" && thinkingText != "" {
		reply = "思考过程:\n" + thinkingText
	}
	if reply == "" && metaText != "" {
		reply = "中间过程:\n" + metaText
	}
	if reply == "" {
		reply = " 处理完成"
	}
	_ = h.SendMessage(ctx, "wecom", userID, reply)
	return reply, nil
}

type wecomIncomingMessage struct {
	MsgType     string
	Content     string
	Attachments []opencode.Attachment
	MediaFiles  []base.MediaFileRecord
}

func (h *Handler) parseIncomingMessage(ctx context.Context, env callbackEnvelope) (wecomIncomingMessage, error) {
	msgType := strings.ToLower(strings.TrimSpace(env.MsgType))
	if msgType == "" {
		msgType = "text"
	}

	content := strings.TrimSpace(env.Text.Content)
	mediaSessionID := "new"
	if existingSessionID, ok := h.adapter.GetSessionForUser(env.FromUserID); ok && strings.TrimSpace(existingSessionID) != "" {
		mediaSessionID = strings.TrimSpace(existingSessionID)
	}

	saveMediaRecord := func(kind, filename, mediaID, mimeType string, data []byte) (*base.MediaFileRecord, error) {
		now := time.Now().UTC()
		relDir := base.BuildMediaRelativeDir("wecom", env.FromUserID, mediaSessionID, now)
		// Use OpenCode working directory for media storage so skills can access the files
		mediaRoot := base.MediaRootDirForOpenCode(h.client.Directory())
		saved, err := base.SaveTempMedia(
			mediaRoot,
			relDir,
			kind,
			env.MsgID,
			filename,
			mimeType,
			data,
			base.MediaTTLFromEnv(),
			base.MediaMaxBytesFromEnv(),
		)
		if err != nil {
			return nil, err
		}
		return &base.MediaFileRecord{
			MessageID:    env.MsgID,
			UserID:       env.FromUserID,
			SessionID:    mediaSessionID,
			Platform:     "wecom",
			MsgType:      kind,
			Filename:     saved.Filename,
			Mime:         saved.Mime,
			Size:         saved.Size,
			SHA256:       saved.SHA256,
			LocalPath:    saved.LocalPath,
			RelativePath: saved.RelativePath,
			CreatedAt:    saved.CreatedAt,
			ExpireAt:     saved.ExpireAt,
		}, nil
	}

	buildAttachment := func(mimeType string, data []byte, filename string) opencode.Attachment {
		mimeType = strings.TrimSpace(mimeType)
		if mimeType == "" {
			mimeType = detectMimeByFilename(filename)
		}
		if mimeType == "" {
			mimeType = "application/octet-stream"
		}
		return opencode.Attachment{
			Mime:     mimeType,
			URL:      "data:" + mimeType + ";base64," + base64.StdEncoding.EncodeToString(data),
			Filename: filename,
		}
	}

	switch msgType {
	case "text":
		if content == "" {
			return wecomIncomingMessage{}, fmt.Errorf("empty text content")
		}
		return wecomIncomingMessage{MsgType: "text", Content: content}, nil
	case "image", "pic":
		if env.PicURL != "" {
			dataURI, mimeType, err := h.downloadURLAsDataURI(ctx, env.PicURL, "image/jpeg")
			if err != nil {
				log.Printf("wecom: image url download failed: %v", err)
			} else {
				return wecomIncomingMessage{
					MsgType: "image",
					Content: fallbackContent(content, "[图片消息]"),
					Attachments: []opencode.Attachment{{
						Mime:     mimeType,
						URL:      dataURI,
						Filename: "wecom_image.jpg",
					}},
				}, nil
			}
		}
		if env.MediaID != "" {
			mediaData, mediaMime, fileName, err := h.downloadMediaBytes(ctx, env.MediaID)
			if err != nil {
				log.Printf("wecom: image media_id download failed: %v", err)
			} else {
				if fileName == "" {
					fileName = "wecom_image.jpg"
				}
				att := buildAttachment(mediaMime, mediaData, fileName)
				return wecomIncomingMessage{
					MsgType:     "image",
					Content:     fallbackContent(content, "[图片消息]"),
					Attachments: []opencode.Attachment{att},
				}, nil
			}
		}
		return wecomIncomingMessage{MsgType: "image", Content: fallbackContent(content, "[图片消息]")}, nil
	case "voice", "audio":
		if env.MediaID != "" {
			mediaData, mediaMime, fileName, err := h.downloadMediaBytes(ctx, env.MediaID)
			if err != nil {
				log.Printf("wecom: voice media_id download failed: %v", err)
			} else {
				if fileName == "" {
					fileName = "wecom_voice.amr"
				}
				att := buildAttachment(mediaMime, mediaData, fileName)
				return wecomIncomingMessage{
					MsgType:     "voice",
					Content:     fallbackContent(content, "[语音消息]"),
					Attachments: []opencode.Attachment{att},
				}, nil
			}
		}
		return wecomIncomingMessage{MsgType: "voice", Content: fallbackContent(content, "[语音消息]")}, nil
	case "video":
		var attachments []opencode.Attachment
		var mediaFiles []base.MediaFileRecord
		if env.MediaID != "" {
			mediaData, mediaMime, fileName, err := h.downloadMediaBytes(ctx, env.MediaID)
			if err != nil {
				log.Printf("wecom: video media_id download failed: %v", err)
			} else {
				if fileName == "" {
					fileName = "wecom_video.mp4"
				}
				att := buildAttachment(mediaMime, mediaData, fileName)
				attachments = append(attachments, att)
				record, saveErr := saveMediaRecord("video", fileName, env.MediaID, att.Mime, mediaData)
				if saveErr != nil {
					log.Printf("wecom: save temp video failed: %v", saveErr)
				} else {
					mediaFiles = append(mediaFiles, *record)
				}
			}
		}
		return wecomIncomingMessage{
			MsgType:     "video",
			Content:     fallbackContent(content, "[视频消息]"),
			Attachments: attachments,
			MediaFiles:  mediaFiles,
		}, nil
	case "file":
		var mediaFiles []base.MediaFileRecord
		if env.MediaID != "" {
			mediaData, mediaMime, fileName, err := h.downloadMediaBytes(ctx, env.MediaID)
			if err != nil {
				log.Printf("wecom: file media_id download failed: %v", err)
			} else {
				if fileName == "" {
					fileName = "wecom_file.bin"
				}
				record, saveErr := saveMediaRecord("file", fileName, env.MediaID, mediaMime, mediaData)
				if saveErr != nil {
					log.Printf("wecom: save temp file failed: %v", saveErr)
				} else {
					mediaFiles = append(mediaFiles, *record)
				}
			}
		}
		return wecomIncomingMessage{
			MsgType:    "file",
			Content:    fallbackContent(content, fmt.Sprintf("[文件消息: %s]", firstNonEmpty(env.FileName, "未命名文件"))),
			MediaFiles: mediaFiles,
		}, nil
	default:
		return wecomIncomingMessage{MsgType: msgType, Content: fallbackContent(content, fmt.Sprintf("[%s消息]", msgType))}, nil
	}
}

func (h *Handler) parseCallbackEnvelope(body []byte, out *callbackEnvelope) error {
	if out == nil {
		return fmt.Errorf("nil envelope")
	}
	if err := json.Unmarshal(body, out); err == nil {
		if out.MsgType != "" || out.FromUserID != "" || out.Text.Content != "" {
			out.normalize()
			return nil
		}
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return err
	}

	out.MsgType = firstNonEmpty(readRawString(raw, "msgtype"), readRawString(raw, "MsgType"))
	out.Event = firstNonEmpty(readRawString(raw, "event"), readRawString(raw, "Event"))
	out.FromUserID = firstNonEmpty(readRawString(raw, "from_userid"), readRawString(raw, "FromUserName"), readRawString(raw, "FromUserID"), readRawString(raw, "fromUserId"))
	out.RoomID = firstNonEmpty(readRawString(raw, "roomid"), readRawString(raw, "RoomID"), readRawString(raw, "chatid"), readRawString(raw, "ChatID"))
	out.MsgID = firstNonEmpty(readRawString(raw, "msgid"), readRawString(raw, "MsgId"), readRawString(raw, "MsgID"))
	out.MediaID = firstNonEmpty(readRawString(raw, "media_id"), readRawString(raw, "MediaId"), readRawString(raw, "MediaID"))
	out.PicURL = firstNonEmpty(readRawString(raw, "picurl"), readRawString(raw, "PicUrl"), readRawString(raw, "PicURL"))
	out.FileName = firstNonEmpty(readRawString(raw, "filename"), readRawString(raw, "FileName"))
	out.Format = firstNonEmpty(readRawString(raw, "format"), readRawString(raw, "Format"))

	if nested, ok := raw["text"]; ok {
		var textObj map[string]json.RawMessage
		if err := json.Unmarshal(nested, &textObj); err == nil {
			out.Text.Content = firstNonEmpty(readRawString(textObj, "content"), readRawString(textObj, "Content"))
		}
	}
	if out.Text.Content == "" {
		out.Text.Content = firstNonEmpty(readRawString(raw, "content"), readRawString(raw, "Content"))
	}

	out.normalize()
	return nil
}

func (h *Handler) getAccessToken(ctx context.Context) (string, error) {
	h.tokenMu.Lock()
	defer h.tokenMu.Unlock()

	if h.accessToken != "" && time.Now().Before(h.tokenExpiry) {
		return h.accessToken, nil
	}

	corpID := strings.TrimSpace(h.cfg.CorpID)
	corpSecret := strings.TrimSpace(h.cfg.CorpSecret)
	if corpID == "" || corpSecret == "" {
		return "", fmt.Errorf("missing WECOM_CORP_ID or WECOM_CORP_SECRET")
	}

	tokenURL := fmt.Sprintf("%s/gettoken?corpid=%s&corpsecret=%s", wecomAPIEndpoint, url.QueryEscape(corpID), url.QueryEscape(corpSecret))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tokenURL, nil)
	if err != nil {
		return "", err
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var body struct {
		ErrCode     int    `json:"errcode"`
		ErrMsg      string `json:"errmsg"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return "", err
	}
	if body.ErrCode != 0 || body.AccessToken == "" {
		return "", fmt.Errorf("wecom token failed: errcode=%d errmsg=%s", body.ErrCode, body.ErrMsg)
	}

	h.accessToken = body.AccessToken
	validFor := time.Duration(body.ExpiresIn) * time.Second
	if validFor <= 0 {
		validFor = 2 * time.Hour
	}
	refreshBefore := 2 * time.Minute
	if validFor <= refreshBefore {
		h.tokenExpiry = time.Now().Add(validFor / 2)
	} else {
		h.tokenExpiry = time.Now().Add(validFor - refreshBefore)
	}

	return h.accessToken, nil
}

func (h *Handler) downloadMediaBytes(ctx context.Context, mediaID string) ([]byte, string, string, error) {
	if strings.TrimSpace(mediaID) == "" {
		return nil, "", "", fmt.Errorf("empty media id")
	}
	token, err := h.getAccessToken(ctx)
	if err != nil {
		return nil, "", "", err
	}
	mediaURL := fmt.Sprintf("%s/media/get?access_token=%s&media_id=%s", wecomAPIEndpoint, url.QueryEscape(token), url.QueryEscape(mediaID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, mediaURL, nil)
	if err != nil {
		return nil, "", "", err
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, "", "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", "", err
	}

	contentType := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Type")))
	if strings.Contains(contentType, "application/json") {
		var errResp struct {
			ErrCode int    `json:"errcode"`
			ErrMsg  string `json:"errmsg"`
		}
		if jErr := json.Unmarshal(body, &errResp); jErr == nil && errResp.ErrCode != 0 {
			return nil, "", "", fmt.Errorf("wecom media get failed: errcode=%d errmsg=%s", errResp.ErrCode, errResp.ErrMsg)
		}
	}

	fileName := extractFilenameFromDisposition(resp.Header.Get("Content-Disposition"))
	contentType = strings.TrimSpace(strings.Split(contentType, ";")[0])
	if contentType == "" {
		contentType = detectMimeByFilename(fileName)
	}
	return body, contentType, fileName, nil
}

func (h *Handler) downloadURLAsDataURI(ctx context.Context, rawURL, defaultMime string) (string, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return "", "", err
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", err
	}
	mimeType := strings.TrimSpace(strings.Split(resp.Header.Get("Content-Type"), ";")[0])
	if mimeType == "" {
		mimeType = strings.TrimSpace(defaultMime)
	}
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}
	return "data:" + mimeType + ";base64," + base64.StdEncoding.EncodeToString(body), mimeType, nil
}

func (h *Handler) sendTextToUser(ctx context.Context, userID, content string) error {
	token, err := h.getAccessToken(ctx)
	if err != nil {
		return err
	}
	body := map[string]interface{}{
		"touser":  userID,
		"msgtype": "text",
		"agentid": h.cfg.AgentID,
		"text": map[string]string{
			"content": content,
		},
		"safe": 0,
	}
	return h.postWeComJSON(ctx, fmt.Sprintf("%s/message/send?access_token=%s", wecomAPIEndpoint, url.QueryEscape(token)), body)
}

func (h *Handler) sendAppChatText(ctx context.Context, chatID, content string) error {
	token, err := h.getAccessToken(ctx)
	if err != nil {
		return err
	}
	body := map[string]interface{}{
		"chatid":  chatID,
		"msgtype": "text",
		"text": map[string]string{
			"content": content,
		},
		"safe": 0,
	}
	return h.postWeComJSON(ctx, fmt.Sprintf("%s/appchat/send?access_token=%s", wecomAPIEndpoint, url.QueryEscape(token)), body)
}

func (h *Handler) postWeComJSON(ctx context.Context, rawURL string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response failed: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		preview := string(bodyBytes)
		if len(preview) > 300 {
			preview = preview[:300]
		}
		return fmt.Errorf("http status=%d body=%s", resp.StatusCode, strings.TrimSpace(preview))
	}

	var result struct {
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
	}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		preview := string(bodyBytes)
		if len(preview) > 300 {
			preview = preview[:300]
		}
		return fmt.Errorf("decode response failed: %w, body=%s", err, strings.TrimSpace(preview))
	}
	if result.ErrCode != 0 {
		return fmt.Errorf("wecom api failed: errcode=%d errmsg=%s", result.ErrCode, result.ErrMsg)
	}
	return nil
}

func readRawString(raw map[string]json.RawMessage, key string) string {
	val, ok := raw[key]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(val, &s); err == nil {
		return strings.TrimSpace(s)
	}
	var n json.Number
	if err := json.Unmarshal(val, &n); err == nil {
		return strings.TrimSpace(n.String())
	}
	return strings.TrimSpace(string(bytes.Trim(val, `"`)))
}

func fallbackContent(content, fallback string) string {
	if strings.TrimSpace(content) != "" {
		return strings.TrimSpace(content)
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func extractFilenameFromDisposition(disposition string) string {
	if strings.TrimSpace(disposition) == "" {
		return ""
	}
	_, params, err := mime.ParseMediaType(disposition)
	if err != nil {
		return ""
	}
	if v, ok := params["filename*"]; ok {
		if idx := strings.Index(v, "''"); idx >= 0 {
			if decoded, decErr := url.QueryUnescape(v[idx+2:]); decErr == nil {
				return strings.TrimSpace(decoded)
			}
		}
	}
	if v, ok := params["filename"]; ok {
		return strings.TrimSpace(v)
	}
	return ""
}

func detectMimeByFilename(name string) string {
	ext := strings.ToLower(filepath.Ext(strings.TrimSpace(name)))
	switch ext {
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".png":
		return "image/png"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".mp4":
		return "video/mp4"
	case ".mov":
		return "video/quicktime"
	case ".amr":
		return "audio/amr"
	case ".mp3":
		return "audio/mpeg"
	case ".wav":
		return "audio/wav"
	case ".pdf":
		return "application/pdf"
	case ".doc":
		return "application/msword"
	case ".docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case ".xls":
		return "application/vnd.ms-excel"
	case ".xlsx":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case ".ppt":
		return "application/vnd.ms-powerpoint"
	case ".pptx":
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	case ".txt", ".md", ".log":
		return "text/plain"
	default:
		return ""
	}
}

func (h *Handler) handleCronTask(ctx context.Context, env callbackEnvelope, msg wecomIncomingMessage, content string) (bool, string, error) {
	if h.cronScheduler == nil {
		return true, "❌ 定时任务功能未启用", nil
	}
	nlText := strings.TrimSpace(strings.TrimPrefix(content, "/crontask"))
	if handled, reply, err := h.tryHandleNLScheduleOpt(ctx, env, msg, nlText, true); handled || err != nil {
		return handled, reply, err
	}
	return true, "❌ 未知的子命令，使用 /crontask 后接自然语言或标准命令。", nil
}

func (h *Handler) tryHandleNLSchedule(ctx context.Context, env callbackEnvelope, msg wecomIncomingMessage, text string) (bool, string, error) {
	return h.tryHandleNLScheduleOpt(ctx, env, msg, text, false)
}

func (h *Handler) tryHandleNLScheduleOpt(ctx context.Context, env callbackEnvelope, msg wecomIncomingMessage, text string, forceCreate bool) (bool, string, error) {
	if h.nlScheduleSvc == nil || strings.TrimSpace(text) == "" {
		return false, "", nil
	}
	if !forceCreate && !scheduler.ShouldTryNLScheduleText(text) {
		return false, "", nil
	}

	channel := strings.TrimSpace(env.RoomID)
	resp, err := h.nlScheduleSvc.HandleText(ctx, scheduler.NLScheduleRequest{
		AdapterType: "wecom",
		UserID:      strings.TrimSpace(env.FromUserID),
		Channel:     channel,
		Text:        text,
		ForceCreate: forceCreate,
		Metadata: map[string]interface{}{
			"room_id":      env.RoomID,
			"from_user_id": env.FromUserID,
		},
	})
	if err != nil {
		return true, "❌ 定时任务处理失败: " + err.Error(), err
	}
	if resp == nil || !resp.Handled {
		return false, "", nil
	}
	return true, resp.Message, nil
}

//  command handlers

func (h *Handler) handleHelp() (string, error) {
	helpText := `📖 OpenCode Gateway (企业微信)

📋 可用命令：
/help 或 帮助      - 显示此帮助
/fork 或 派生      - 派生当前会话（保留历史，开启新分支）
/todo 或 任务      - 查看当前任务进度列表
/diff 或 变更      - 查看本次会话的文件变更摘要
/abort 或 停止     - 中止正在运行的任务
/status 或 状态    - 查看当前会话状态
/summary 或 压缩   - 压缩会话上下文（释放token空间）
/devcore           - 查看 Dev Core 状态
/devcore <自然语言偏好> - 直接设置并开启 Dev Core
/devcore on|off    - 开关 Dev Core 注入
/devcore reset     - 清空偏好并关闭（默认状态）

💬 直接发送消息即可与 AI 对话`
	return helpText, nil
}

func (h *Handler) handleDevCore(content string) (string, error) {
	raw := strings.TrimSpace(content)
	parts := strings.Fields(raw)

	if len(parts) == 1 || (len(parts) >= 2 && strings.EqualFold(parts[1], "status")) {
		status := "off"
		if h.client.IsDevCoreEnabled() {
			status = "on"
		}
		prompt := strings.TrimSpace(h.client.GetDevCorePrompt())
		if prompt == "" {
			prompt = "（未设置）"
		}
		return fmt.Sprintf(" Dev Core 状态: %s（仅会话首条消息注入）\n\n当前提示词:\n%s\n\n使用方法:\n/devcore <自然语言偏好>\n/devcore on\n/devcore off\n/devcore set <提示词>\n/devcore reset", status, prompt), nil
	}

	arg := strings.ToLower(parts[1])
	switch arg {
	case "on", "true", "1":
		if strings.TrimSpace(h.client.GetDevCorePrompt()) == "" {
			return " 当前未设置 Dev Core 提示词\n\n请先使用:\n/devcore <自然语言偏好>\n或 /devcore set <提示词>", nil
		}
		h.client.SetDevCoreEnabled(true)
		return " 已开启 Dev Core（仅会话首条消息注入）", nil
	case "off", "false", "0":
		h.client.SetDevCoreEnabled(false)
		return " 已关闭 Dev Core 注入", nil
	case "reset":
		h.client.ResetDevCorePrompt()
		h.client.SetDevCoreEnabled(false)
		return " 已清空 Dev Core 提示词，并关闭注入（默认状态）", nil
	case "set":
		if len(parts) < 3 {
			return " 命令格式错误\n\n使用方法:\n/devcore set <提示词>", nil
		}
		prompt := strings.TrimSpace(strings.TrimPrefix(raw, parts[0]+" "+parts[1]))
		if prompt == "" {
			return " 提示词不能为空", nil
		}
		h.client.SetDevCorePrompt(prompt)
		h.client.SetDevCoreEnabled(true)
		return " Dev Core 提示词已更新（从新会话首条消息生效）", nil
	default:
		prompt := strings.TrimSpace(strings.TrimPrefix(raw, parts[0]))
		if prompt == "" {
			return " 命令格式错误\n\n使用方法:\n/devcore\n/devcore <自然语言偏好>\n/devcore status\n/devcore on\n/devcore off\n/devcore set <提示词>\n/devcore reset", nil
		}
		h.client.SetDevCorePrompt(prompt)
		h.client.SetDevCoreEnabled(true)
		return " Dev Core 偏好已通过自然语言设置（从新会话首条消息生效）", nil
	}
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

func (h *Handler) handleSummary(userID string) (string, error) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "ℹ️ 当前没有活跃的会话", nil
	}

	ctx := context.Background()
	if err := h.client.SummarizeSession(ctx, sessionID); err != nil {
		return fmt.Sprintf("❌ 上下文压缩失败: %v", err), err
	}

	return fmt.Sprintf("✅ 上下文压缩完成\n\n会话 %s 的历史消息已被总结压缩。", sessionID[:min(8, len(sessionID))]), nil
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
			if policy == wecomOverflowPolicySummary || policy == wecomOverflowPolicyNew {
				return policy
			}
		}
	}
	return wecomOverflowPolicyAsk
}

func (h *Handler) setTokenOverflowPolicy(userID, policy string) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return
	}
	if policy != wecomOverflowPolicySummary && policy != wecomOverflowPolicyNew {
		h.overflowPolicy.Delete(userID)
		return
	}
	h.overflowPolicy.Store(userID, policy)
}

func (h *Handler) storeTokenOverflowPending(userID string, state *wecomTokenOverflowPendingState) {
	if state == nil {
		return
	}
	h.overflowPending.Store(strings.TrimSpace(userID), state)
}

func (h *Handler) getTokenOverflowPending(userID string) (*wecomTokenOverflowPendingState, bool) {
	value, ok := h.overflowPending.Load(strings.TrimSpace(userID))
	if !ok {
		return nil, false
	}
	state, ok := value.(*wecomTokenOverflowPendingState)
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

func (h *Handler) handleTokenOverflowQuickReply(ctx context.Context, userID, content string) (bool, string, error) {
	state, ok := h.getTokenOverflowPending(userID)
	if !ok {
		return false, "", nil
	}

	// 如果已经在执行重试，阻止新消息并提示用户等待
	if state.Executing {
		return true, "⏳ 上下文压缩/重试正在进行中，请稍候...", nil
	}

	decision, setAlways, recognized := parseTokenOverflowDecision(content)
	if !recognized {
		return true, "请回复 1/2/3/4/5（或对应中文选项）来处理上下文超限问题。", nil
	}

	if decision == "cancel" {
		h.clearTokenOverflowPending(userID)
		return true, "✅ 已取消本次继续处理。你可以手动发送 /summary、/new 或重发消息。", nil
	}

	if setAlways {
		if decision == "summary" {
			h.setTokenOverflowPolicy(userID, wecomOverflowPolicySummary)
		} else if decision == "new" {
			h.setTokenOverflowPolicy(userID, wecomOverflowPolicyNew)
		}
	}

	reply, err := h.executeTokenOverflowDecision(ctx, userID, decision)
	if err != nil {
		return true, "", err
	}
	if setAlways {
		if decision == "summary" {
			reply = "✅ 已设置为：总是压缩并继续。\n\n" + reply
		} else if decision == "new" {
			reply = "✅ 已设置为：总是新会话并继续。\n\n" + reply
		}
	}
	return true, reply, nil
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

func (h *Handler) executeTokenOverflowDecision(ctx context.Context, userID, decision string) (string, error) {
	state, ok := h.getTokenOverflowPending(userID)
	if !ok {
		return "❌ 未找到待处理的超限请求，请重发原消息。", nil
	}

	// 标记为执行中，防止用户新消息创建并发请求
	state.Executing = true
	h.storeTokenOverflowPending(userID, state)

	decision = strings.TrimSpace(decision)
	if decision == "" {
		decision = "summary"
	}

	runCtx, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel()

	if decision == "summary" {
		sessionID := strings.TrimSpace(state.SessionID)
		if sessionID == "" {
			if sid, exists := h.adapter.GetSessionForUser(state.UserID); exists {
				sessionID = sid
			}
		}
		if sessionID == "" {
			decision = "new"
		} else {
			if err := h.client.SummarizeSession(runCtx, sessionID); err != nil {
				h.clearTokenOverflowPending(userID)
				return "", fmt.Errorf("自动压缩失败: %w", err)
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

	// 使用流式模式重试，避免长时间阻塞无反馈
	var retryReply strings.Builder
	retryCallback := func(chunk string) error {
		trimmed := strings.TrimSpace(chunk)
		if trimmed == "" {
			return nil
		}
		if strings.HasPrefix(trimmed, "ses_") && len(trimmed) < 100 {
			h.adapter.MapUserToSession(state.UserID, trimmed)
			return nil
		}
		if strings.HasPrefix(chunk, opencode.ThinkingSignalPrefix) {
			return nil
		}
		if chunk == opencode.FlushSignal {
			return nil
		}
		retryReply.WriteString(chunk)
		return nil
	}

	response, err := h.client.SendMessageStreaming(runCtx, opencode.MessagePayload{
		Channel:     "wecom",
		UserID:      state.UserID,
		ThreadID:    state.ThreadID,
		SessionID:   state.SessionID,
		Content:     state.Content,
		Streaming:   true,
		Attachments: append([]opencode.Attachment(nil), state.Attachments...),
		Metadata:    cloneStringMap(state.Metadata),
	}, retryCallback)
	if err != nil {
		h.clearTokenOverflowPending(userID)
		return "", fmt.Errorf("已尝试%s后重试，但仍失败: %w", tokenOverflowDecisionLabel(decision), err)
	}

	if response.SessionID != "" {
		h.adapter.MapUserToSession(state.UserID, response.SessionID)
	}

	h.clearTokenOverflowPending(userID)
	finalReply := strings.TrimSpace(retryReply.String())
	if finalReply == "" {
		finalReply = strings.TrimSpace(response.Reply)
	}
	if finalReply == "" {
		finalReply = "✅ 已完成重试，本次没有可直接返回的文本内容。"
	}
	return finalReply, nil
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

//  envelope types

// callbackEnvelope only models the subset of fields we currently need.
type callbackEnvelope struct {
	MsgType    string       `json:"msgtype"`
	Event      string       `json:"event"`
	FromUserID string       `json:"from_userid"`
	RoomID     string       `json:"roomid"`
	MsgID      string       `json:"msgid"`
	MediaID    string       `json:"media_id"`
	PicURL     string       `json:"picurl"`
	FileName   string       `json:"filename"`
	Format     string       `json:"format"`
	Text       textEnvelope `json:"text"`
}

// textEnvelope contains the user provided text.
type textEnvelope struct {
	Content string `json:"content"`
}

func (e *callbackEnvelope) normalize() {
	e.MsgType = strings.ToLower(strings.TrimSpace(e.MsgType))
	e.FromUserID = strings.TrimSpace(e.FromUserID)
	e.RoomID = strings.TrimSpace(e.RoomID)
	e.MsgID = strings.TrimSpace(e.MsgID)
	e.MediaID = strings.TrimSpace(e.MediaID)
	e.PicURL = strings.TrimSpace(e.PicURL)
	e.FileName = strings.TrimSpace(e.FileName)
	e.Format = strings.TrimSpace(e.Format)
	e.Text.Content = strings.TrimSpace(e.Text.Content)
}
