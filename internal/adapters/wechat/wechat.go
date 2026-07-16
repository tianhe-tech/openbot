package wechat

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/user/opencode-gateway/internal/adapters/base"
	"github.com/user/opencode-gateway/internal/opencode"
	"github.com/user/opencode-gateway/internal/scheduler"
)

// Config for the WeChat adapter. Populated from env vars.
type Config struct {
	// BotToken is the iLink bot token (from QR login or pre-saved).
	BotToken string
	// BaseURL is the iLink API base URL for this bot.
	BaseURL string
	// AccountID identifies the saved credential to load (optional; uses first if blank).
	AccountID string
	// StateDir is the directory to store credentials and sync tokens.
	StateDir string
	// CDNBaseURL is the base URL for CDN media downloads.
	CDNBaseURL string
}

// Handler processes incoming WeChat messages and forwards them to OpenCode.
type Handler struct {
	client        *opencode.Client
	cfg           Config
	adapter       *base.BidirectionalAdapter
	cronScheduler *scheduler.CronScheduler
	nlScheduleSvc *scheduler.NLScheduleService
	weClient      *Client
	store         *Store

	// mu protects lastContextTokens
	mu                sync.Mutex
	lastContextTokens map[string]string // userID -> contextToken

	// recentSentTexts tracks text we recently sent, to deduplicate echoes.
	sentTextsMu     sync.Mutex
	recentSentTexts map[string]time.Time // hash(userID+text) -> sentTime

	// sender is fallback-only for direct sends when outbound queue is
	// unavailable; normal runtime pacing is owned by outbound queue ticker.
	sender *senderRegistry

	// batcher coalesces rapid-fire inbound text messages from the same
	// user so paste-splits and forwarded bursts trigger one agent call.
	batcher *inboundBatcher

	// offline persists outbound text whose live delivery failed after the
	// per-call retry budget, so a background worker can redeliver it once
	// the per-user cooldown has elapsed.
	offline *offlineQueue

	// outbound is the durable async text queue for streaming event delivery.
	outbound *outboundTextQueue

	// pendingMu protects pendingMsgs and pendingTimers.
	// When a user sends a new message while a previous task is still
	// dispatching (dispatchToOpenCode running), the new message is queued
	// here instead of launching a concurrent dispatch. This mirrors opencode
	// TUI's behaviour where subsequent tasks wait for the current session to
	// reach idle before executing, and guarantees that task A's results are
	// fully enqueued to the outbound queue before task B starts — so FIFO
	// delivery naturally sends A's complete reply before B's.
	pendingMu     sync.Mutex
	pendingMsgs   map[string]*pendingMessage // userID → at most one queued message
	pendingTimers map[string]*time.Timer     // userID → long-run reminder timer

	// dispatching tracks which users currently have a dispatchToOpenCode
	// goroutine in flight. Checked by handleBatchedMessage before launching a
	// new dispatch to decide whether to queue or execute immediately. This is
	// more reliable than querying session status, which has a race window
	// between session.idle firing and the final flush completing.
	dispatching sync.Map // map[userID]bool

	cancel context.CancelFunc
}

// pendingMessage holds a user message that is waiting for a prior task's
// dispatch to complete before it can be executed.
type pendingMessage struct {
	UserID      string
	ChatID      string
	Content     string
	CtxToken    string
	Attachments []opencode.Attachment
	AgentName   string
	QueuedAt    time.Time
}

// NewHandler creates a WeChat adapter handler.
func NewHandler(client *opencode.Client, cfg Config) *Handler {
	stateDir := cfg.StateDir
	if stateDir == "" {
		home, _ := os.UserHomeDir()
		stateDir = filepath.Join(home, ".opencode-gateway-wechat")
	}

	h := &Handler{
		client:            client,
		cfg:               cfg,
		weClient:          NewClient(),
		store:             NewStore(stateDir),
		lastContextTokens: make(map[string]string),
		recentSentTexts:   make(map[string]time.Time),
		sender:            newSenderRegistry(),
		pendingMsgs:       make(map[string]*pendingMessage),
		pendingTimers:     make(map[string]*time.Timer),
	}
	h.adapter = base.NewBidirectionalAdapter("wechat", h)

	// Inbound debounce window — tunable via env, defaults align with Hermes.
	batchDelay := parseDurationEnv("WECHAT_TEXT_BATCH_DELAY_SECONDS", 3*time.Second)
	splitDelay := parseDurationEnv("WECHAT_TEXT_BATCH_SPLIT_DELAY_SECONDS", 5*time.Second)
	h.batcher = newInboundBatcher(batchDelay, splitDelay, 1800, h.handleBatchedMessage)

	// Offline queue path — sits under the configured state dir so credentials
	// and pending sends share the same protected location.
	queuePath := filepath.Join(stateDir, "pending_outbound.json")
	if envPath := strings.TrimSpace(os.Getenv("WECHAT_OFFLINE_QUEUE_PATH")); envPath != "" {
		queuePath = envPath
	}
	h.offline = newOfflineQueue(queuePath)

	// Durable outbound queue for async event delivery.
	outboundPath := filepath.Join(stateDir, "outbound_queue.db")
	if envPath := strings.TrimSpace(os.Getenv("WECHAT_OUTBOUND_DB_PATH")); envPath != "" {
		outboundPath = envPath
	}
	outboundInterval := parseMillisecondsEnv("WECHAT_OUTBOUND_TICK_MS", int(defaultOutboundTickInterval/time.Millisecond), defaultOutboundTickInterval)
	outboundMaxLen := parseIntEnv("WECHAT_OUTBOUND_MAX_TEXT_LEN", defaultOutboundMaxLen)
	statsLogInterval := parseDurationEnv("WECHAT_OUTBOUND_STATS_LOG_SECONDS", defaultStatsLogInterval)
	outbound, err := newOutboundTextQueue(outboundPath, outboundInterval, outboundMaxLen, h.sendQueuedText, func(userID, sessionID, ctxToken, content string) {
		// parkFunc: transfer exhausted chunks to the offline queue so they are
		// never lost. The user can recover them via /pending /recover.
		if h.offline != nil {
			h.offline.enqueueWithSession(userID, sessionID, ctxToken, content)
		}
	})
	if err != nil {
		log.Printf("wechat: outbound queue init failed, async streaming will fallback to inline send: %v", err)
	} else {
		outbound.setStatsLogInterval(statsLogInterval)
		cfg := outbound.config()
		log.Printf("wechat: outbound queue configured db=%s tick=%s maxLen=%d statsLog=%s nonCriticalTTL=%s stopDrain=%s",
			outboundPath,
			cfg.Interval,
			cfg.MaxLen,
			cfg.StatsLogInterval,
			cfg.NonCriticalTTL,
			cfg.StopDrainTimeout,
		)
		h.outbound = outbound
	}

	return h
}

// parseDurationEnv reads an integer-second env var; falls back to def on
// missing/invalid input.
func parseDurationEnv(name string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return time.Duration(n) * time.Second
}

func parseMillisecondsEnv(name string, defMs int, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return time.Duration(defMs) * time.Millisecond
	}
	return time.Duration(n) * time.Millisecond
}

func parseIntEnv(name string, def int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
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
func (h *Handler) RegisterCronSession(sessionID string, metadata map[string]interface{}) {
	cronUserID := fmt.Sprintf("cron:%s", sessionID[:min(12, len(sessionID))])
	h.adapter.MapUserToSession(cronUserID, sessionID)
	log.Printf("wechat: registered cron session %s (cronUser=%s)", sessionID[:min(8, len(sessionID))], cronUserID)
}

// SendMessage implements base.MessageSender for proactive event delivery.
// Supports MEDIA: path tags for inline media delivery.
func (h *Handler) SendMessage(ctx context.Context, channel, userID, content string) error {
	text := strings.TrimSpace(content)
	if text == "" {
		return nil
	}

	uid := strings.TrimSpace(userID)
	if uid == "" || strings.HasPrefix(uid, "cron:") {
		return fmt.Errorf("wechat proactive send requires a concrete user id")
	}

	h.mu.Lock()
	ctxToken := h.lastContextTokens[uid]
	h.mu.Unlock()

	// Extract MEDIA: tags before text delivery.
	mediaFiles, cleanedContent := extractMediaDirective(text)
	finalText := strings.TrimSpace(cleanedContent)

	log.Printf("wechat: proactive send user=%q len=%d media=%d", uid, len(text), len(mediaFiles))

	// Deliver media files first.
	var lastErr error
	for _, mediaPath := range mediaFiles {
		if err := h.sendMediaFile(uid, mediaPath, "", ctxToken); err != nil {
			log.Printf("wechat: media send failed user=%q path=%s err=%v", uid, mediaPath, err)
			lastErr = err
		}
	}

	// Deliver text content.
	if finalText != "" {
		h.trackSentText(uid, finalText)
		if err := h.sendTextChunks(uid, finalText, ctxToken); err != nil {
			log.Printf("wechat: proactive send failed user=%q err=%v", uid, err)
			return err
		}
	}

	return lastErr
}

// Start begins the long-poll message loop in a background goroutine.
// It loads saved credentials, notifies the server, and starts polling.
func (h *Handler) Start(parentCtx context.Context) error {
	if h.cfg.BotToken == "" && h.cfg.BaseURL == "" {
		log.Println("wechat: no credentials configured (WECHAT_BOT_TOKEN / WECHAT_BASE_URL), adapter disabled")
		return nil
	}

	if err := h.store.Init(); err != nil {
		return fmt.Errorf("wechat: init store: %w", err)
	}

	// If explicit token & base URL are provided, use them directly.
	if h.cfg.BotToken != "" && h.cfg.BaseURL != "" {
		h.weClient.SetCredentials(h.cfg.BaseURL, h.cfg.BotToken)
		log.Printf("wechat: using credentials from env (baseURL=%s)", h.cfg.BaseURL)
	} else {
		// Try loading from store.
		cred, err := h.loadCredential()
		if err != nil {
			return fmt.Errorf("wechat: load credentials: %w", err)
		}
		h.weClient.SetCredentials(cred.BaseURL, cred.BotToken)
		log.Printf("wechat: loaded credentials for account %s (%s)", cred.AccountID, cred.NickName)
	}

	if err := h.weClient.NotifyStart(); err != nil {
		log.Printf("wechat: notify start failed: %v (continuing)", err)
	} else {
		log.Println("wechat: notify start succeeded")
	}

	ctx, cancel := context.WithCancel(parentCtx)
	h.cancel = cancel
	if h.outbound != nil {
		h.outbound.Start()
	}

	// Legacy fallback worker: only enabled when the durable outbound queue
	// failed to initialize. In normal mode, outbound_queue.db is the single
	// source of truth for async text delivery.
	if h.outbound == nil && h.offline != nil {
		go h.offline.runWorker(ctx, h.offlineSend, h.notifyOfflineDrop)
	}

	go h.pollLoop(ctx)
	return nil
}

// Stop gracefully shuts down the polling loop.
func (h *Handler) Stop() {
	if h.batcher != nil {
		// Best-effort: dispatch any in-flight batched text before tearing
		// down so a fresh send right before shutdown is not silently lost.
		h.batcher.flushAll(context.Background())
	}
	if h.cancel != nil {
		h.cancel()
	}
	if h.outbound != nil {
		h.outbound.Stop()
	}
	if err := h.weClient.NotifyStop(); err != nil {
		log.Printf("wechat: notify stop failed: %v", err)
	}
}

func (h *Handler) loadCredential() (*Credentials, error) {
	if h.cfg.AccountID != "" {
		return h.store.LoadCredentials(h.cfg.AccountID)
	}
	accounts, err := h.store.ListAccounts()
	if err != nil {
		return nil, err
	}
	if len(accounts) == 0 {
		return nil, fmt.Errorf("no saved accounts; set WECHAT_BOT_TOKEN and WECHAT_BASE_URL")
	}
	return h.store.LoadCredentials(accounts[0])
}

// pollLoop performs long-polling to receive WeChat messages.
func (h *Handler) pollLoop(ctx context.Context) {
	log.Println("wechat: starting long-poll message loop...")

	accountID := h.cfg.AccountID
	if accountID == "" {
		accountID = "default"
	}

	buf, _ := h.store.LoadSyncToken(accountID)
	consecutiveFailures := 0

	for {
		select {
		case <-ctx.Done():
			_ = h.store.SaveSyncToken(accountID, buf)
			log.Println("wechat: poll loop stopped")
			return
		default:
		}

		resp, err := h.weClient.GetUpdates(buf, 35000)
		if err != nil {
			consecutiveFailures++
			log.Printf("wechat: getUpdates error (%d/3): %v", consecutiveFailures, err)
			if consecutiveFailures >= 3 {
				consecutiveFailures = 0
				h.sleep(ctx, 30*time.Second)
			} else {
				h.sleep(ctx, 2*time.Second)
			}
			continue
		}
		consecutiveFailures = 0

		if resp.Errcode == sessionExpiredCode || resp.Ret == sessionExpiredCode {
			log.Println("wechat: session expired, stopping poll loop")
			return
		}

		if resp.Ret != 0 || resp.Errcode != 0 {
			log.Printf("wechat: getUpdates API error: ret=%d errcode=%d errmsg=%s", resp.Ret, resp.Errcode, resp.Errmsg)
			h.sleep(ctx, 2*time.Second)
			continue
		}

		if resp.GetUpdatesBuf != "" && resp.GetUpdatesBuf != buf {
			buf = resp.GetUpdatesBuf
			_ = h.store.SaveSyncToken(accountID, buf)
		}

		for i := range resp.Msgs {
			msg := resp.Msgs[i]
			if err := h.handleMessage(ctx, &msg); err != nil {
				log.Printf("wechat: handle message error: %v", err)
			}
		}
	}
}

func (h *Handler) sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// trackSentText records text sent to a user for echo deduplication.
func (h *Handler) trackSentText(userID, text string) {
	key := userID + "|" + text
	h.sentTextsMu.Lock()
	h.recentSentTexts[key] = time.Now()
	// Purge old entries
	for k, t := range h.recentSentTexts {
		if time.Since(t) > 2*time.Minute {
			delete(h.recentSentTexts, k)
		}
	}
	h.sentTextsMu.Unlock()
}

// isSentText checks whether this text was recently sent by the bot to this user.
func (h *Handler) isSentText(userID, text string) bool {
	key := userID + "|" + text
	h.sentTextsMu.Lock()
	defer h.sentTextsMu.Unlock()
	_, exists := h.recentSentTexts[key]
	return exists
}

// handleMessage processes a single incoming WeChat message. Pure-text
// messages are routed through the inbound debounce batcher so paste-splits
// and rapid forwards collapse into one agent invocation. Messages that
// carry media items dispatch immediately because file uploads should not
// be aggregated.
func (h *Handler) handleMessage(ctx context.Context, msg *WeixinMessage) error {
	// Skip non-user messages (bot replies)
	if msg.MessageType != MessageTypeUser {
		return nil
	}

	// Skip messages we sent ourselves (echo from GetUpdates)
	if h.weClient.IsSentByUs(msg.ClientID) {
		log.Printf("wechat: [echo-skip] clientID=%s matches sent message, ignoring", msg.ClientID)
		return nil
	}

	if h.batcher != nil && isPureTextMessage(msg) && !isCommandText(firstTextBody(msg)) {
		text := firstTextBody(msg)
		h.batcher.enqueueText(ctx, msg, text)
		return nil
	}
	return h.handleBatchedMessage(ctx, msg)
}

// isPureTextMessage reports whether a WeChat message contains only text items
// (no images, voice, files, video). Mixed messages bypass the batcher so
// media downloads start immediately.
func isPureTextMessage(msg *WeixinMessage) bool {
	hasText := false
	for _, it := range msg.ItemList {
		switch it.Type {
		case ItemTypeText:
			if it.TextItem != nil && strings.TrimSpace(it.TextItem.Text) != "" {
				hasText = true
			}
		case ItemTypeImage, ItemTypeVoice, ItemTypeFile, ItemTypeVideo:
			return false
		}
	}
	return hasText
}

// isCommandText returns true for command-style inputs (leading "/" or
// known Chinese command keywords) so commands dispatch without waiting on
// the debounce window.
func isCommandText(text string) bool {
	t := strings.TrimSpace(text)
	if t == "" {
		return false
	}
	if strings.HasPrefix(t, "/") {
		return true
	}
	switch t {
	case "帮助", "新会话", "任务", "停止", "状态", "撤销", "重做", "变更", "总结", "压缩", "清除", "配置", "派生":
		return true
	}
	return false
}

// handleBatchedMessage is the original message-processing body, invoked
// either directly (commands / media messages) or by the batcher after its
// quiet window elapses (aggregated text).
func (h *Handler) handleBatchedMessage(ctx context.Context, msg *WeixinMessage) error {
	userID := msg.FromUserID
	chatID := msg.GroupID
	if chatID == "" {
		chatID = userID
	}

	// Extract media attachments first (downloads files to working dir).
	ctxToken := msg.ContextToken
	attachments, agentName, savedFiles := h.extractMediaAttachments(ctx, msg)

	// Build text after media extraction so saved file paths can be included.
	userText := h.extractText(msg, savedFiles)
	if userText == "" {
		return nil
	}

	// Fallback echo detection: skip if we recently sent this exact text to this user.
	if h.isSentText(userID, userText) {
		log.Printf("wechat: [echo-skip] text from %s matches recently sent content, ignoring", userID)
		return nil
	}

	log.Printf("wechat: [%s] %s: %s", time.Now().Format("15:04:05"), userID, truncate(userText, 120))

	// Record context token for proactive replies.
	h.mu.Lock()
	h.lastContextTokens[userID] = msg.ContextToken
	h.mu.Unlock()

	// Send typing indicator in background.
	go func() {
		cfgResp, err := h.weClient.GetConfig(userID, msg.ContextToken)
		if err == nil && cfgResp.TypingTicket != "" {
			_ = h.weClient.SendTypingIndicator(userID, cfgResp.TypingTicket)
		}
	}()

	// Command routing. Command replies are delivered directly and
	// synchronously (bypassing the durable outbound queue) so they return
	// immediately and independently of the AI streaming pipeline, matching
	// the DingTalk/Feishu behaviour.
	if cmd, handled := h.tryCommand(ctx, userID, chatID, userText, msg); handled {
		if cmd != "" {
			h.sendCommandReply(userID, cmd, msg.ContextToken)
		}
		return nil
	}

	// Quick reply: try to interpret non-command text as an answer to a pending
	// question or permission request (same as dingtalk's handleQuickReply).
	if !strings.HasPrefix(strings.TrimSpace(userText), "/") {
		if reply, handled := h.handleQuickReply(ctx, userID, userText, msg.ContextToken); handled {
			if reply != "" {
				_ = h.sendTextChunks(userID, reply, msg.ContextToken)
			}
			return nil
		}
	}

	// ★ Inbound gating: if a previous task is still dispatching for this
	// user, queue the new message instead of launching a concurrent dispatch.
	// This mirrors opencode TUI's session-level task serialization and
	// guarantees task A's results are fully enqueued before task B starts.
	if _, busy := h.dispatching.Load(userID); busy {
		// Deadlock guard: the dispatch may be blocked waiting for the user to
		// answer a question / permission request. In that case queueing this
		// message would deadlock (dispatch waits for an answer, the answer sits
		// in the pending queue). Detect the pending prompt and tell the user to
		// answer it first instead of silently queueing.
		if sessionID, ok := h.adapter.GetSessionForUser(userID); ok && sessionID != "" {
			if _, hasP := h.client.GetLatestPendingPermission(sessionID); hasP {
				_ = h.enqueueAsyncText(userID, sessionID, ctxToken, "info",
					"⚠️ 当前有一个待确认的权限请求，请先回复：允许 / 拒绝 / 始终允许（或回复选项编号）。", true)
				return nil
			}
			if _, hasQ := h.client.GetLatestPendingQuestion(sessionID); hasQ {
				_ = h.enqueueAsyncText(userID, sessionID, ctxToken, "info",
					"⚠️ 当前有一个待回答的问题，请先回复选项编号或文字后再继续。", true)
				return nil
			}
		}
		h.enqueuePending(userID, chatID, userText, ctxToken, attachments, agentName)
		return nil
	}

	// Run dispatch in a goroutine so the poll loop is not blocked.
	go h.dispatchToOpenCode(ctx, userID, chatID, userText, ctxToken, attachments, agentName)
	return nil
}

func (h *Handler) extractText(msg *WeixinMessage, savedFiles []string) string {
	var parts []string
	hasMedia := false
	for _, item := range msg.ItemList {
		switch item.Type {
		case ItemTypeText:
			if item.TextItem != nil && strings.TrimSpace(item.TextItem.Text) != "" {
				parts = append(parts, item.TextItem.Text)
			}
		case ItemTypeVoice:
			if item.VoiceItem != nil && item.VoiceItem.Text != "" {
				parts = append(parts, item.VoiceItem.Text)
			} else {
				parts = append(parts, "[语音消息]")
			}
		case ItemTypeImage:
			hasMedia = true
		case ItemTypeFile:
			// File info is added below from savedFiles
			hasMedia = true
		case ItemTypeVideo:
			hasMedia = true
		}
	}

	// Append saved file info so the model knows which files are available
	for _, f := range savedFiles {
		parts = append(parts, fmt.Sprintf("\n[文件已保存到工作目录: %s，请直接读取该文件]", filepath.Base(f)))
	}

	text := strings.Join(parts, "")
	// If there's media but no text, provide a default prompt so the model
	// knows an attachment is present.
	if text == "" && hasMedia {
		text = "请查看附件内容"
	}
	return text
}

func (h *Handler) tryCommand(ctx context.Context, userID, chatID, text string, msg *WeixinMessage) (string, bool) {
	switch text {
	case "/help", "帮助":
		return h.helpText(), true
	case "/fork", "派生":
		return h.handleFork(ctx, userID), true
	case "/new", "/reset", "新会话":
		return h.handleNewSession(userID), true
	case "/todo", "/todos", "任务":
		return h.handleTodo(userID), true
	case "/abort", "/stop", "停止":
		return h.handleAbort(ctx, userID), true
	case "/status", "状态":
		return h.handleStatus(userID), true
	case "/undo", "/revert", "撤销":
		return h.handleUndo(ctx, userID), true
	case "/redo", "/unrevert", "重做":
		return h.handleRedo(ctx, userID), true
	case "/diff", "/changes", "变更":
		return h.handleDiff(ctx, userID), true
	case "/sessions", "/list":
		return h.handleSessions(ctx), true
	case "/summary", "压缩", "总结":
		return h.handleSummary(ctx, userID), true
	case "/clear", "清除":
		return h.handleClear(ctx, userID), true
	case "/refresh":
		return h.handleRefresh(ctx), true
	case "/skills", "/agents":
		return h.handleListSkills(ctx), true
	case "/config", "配置":
		return h.handleConfig(ctx, userID), true
	}

	// /cmd <command>
	if strings.HasPrefix(text, "/cmd ") {
		command := strings.TrimPrefix(text, "/cmd ")
		return h.handleCmd(ctx, userID, command, msg.ContextToken), true
	}

	// /retry [message]
	if text == "/retry" || strings.HasPrefix(text, "/retry ") {
		msg := strings.TrimSpace(strings.TrimPrefix(text, "/retry"))
		return h.handleRetry(ctx, userID, chatID, msg), true
	}

	// /pending — list abandoned messages from previous sessions
	if text == "/pending" {
		return h.handlePending(userID), true
	}

	// /recover [session-id-prefix] — re-queue abandoned messages
	if text == "/recover" || strings.HasPrefix(text, "/recover ") {
		sessionFilter := strings.TrimSpace(strings.TrimPrefix(text, "/recover"))
		return h.handleRecover(userID, sessionFilter), true
	}

	// /model [provider/model]
	if strings.HasPrefix(text, "/model") || strings.HasPrefix(text, "/provider") {
		return h.handleModel(ctx, userID, text), true
	}

	// /thinking [on|off]
	if strings.HasPrefix(text, "/thinking") {
		return h.handleThinking(text), true
	}

	// /final [on|off]
	if strings.HasPrefix(text, "/final") {
		return h.handleFinal(text), true
	}

	// /steps [on|off]
	if strings.HasPrefix(text, "/steps") || strings.HasPrefix(text, "/step") {
		return h.handleSteps(text), true
	}

	// /devcore [on|off|set|reset|<prompt>]
	if strings.HasPrefix(text, "/devcore") {
		return h.handleDevCore(text), true
	}

	// /answer <answer>
	if strings.HasPrefix(text, "/answer ") {
		answer := strings.TrimSpace(strings.TrimPrefix(text, "/answer "))
		return h.handleAnswerCommand(ctx, userID, answer), true
	}

	// /crontask ...
	if strings.HasPrefix(text, "/crontask") {
		return h.handleCronTask(ctx, userID, chatID, text), true
	}

	// Natural language schedule
	if h.nlScheduleSvc != nil && strings.TrimSpace(text) != "" {
		if scheduler.ShouldTryNLScheduleText(text) {
			resp, err := h.nlScheduleSvc.HandleText(ctx, scheduler.NLScheduleRequest{
				AdapterType: "wechat",
				UserID:      userID,
				Channel:     chatID,
				Text:        text,
			})
			if err == nil && resp != nil && resp.Handled {
				return resp.Message, true
			}
		}
	}

	return "", false
}

func (h *Handler) dispatchToOpenCode(ctx context.Context, userID, chatID, content, ctxToken string, attachments []opencode.Attachment, agentName string) {
	// ★ Mark this user as dispatching. handleBatchedMessage checks this flag
	// to decide whether to queue or execute a new message. Cleared via defer
	// so it is always released even on panic or early return.
	h.dispatching.Store(userID, true)
	defer func() {
		h.dispatching.Delete(userID)
		// After this dispatch completes (all results enqueued to outbound
		// queue), check if a pending message was queued and dispatch it now.
		h.dispatchPendingIfAny(userID)
	}()

	sessionID, _ := h.adapter.GetSessionForUser(userID)
	threadID := chatID

	// ★ Proactively map the session BEFORE the streaming call to prevent a
	// race condition where the global event handler receives events for this
	// session before the streaming callback has a chance to call MapUserToSession.
	// Without this, early events (message.part.delta, session.status, etc.) are
	// silently dropped because no adapter is found for the session.
	if sessionID != "" {
		h.adapter.MapUserToSession(userID, sessionID)
	}

	// WeChat iLink is aggressively rate-limited, so verbose thinking is still
	// suppressed. Tool/todo updates and occasional non-final text previews are
	// sent as skippable progress, while the final answer remains critical.
	var fullReplyMu sync.Mutex
	var fullReply strings.Builder
	lastSentLength := 0
	sessionMapped := sessionID != "" // if we pre-mapped, skip the callback-based mapping

	enqueueBufferedTail := func(eventType string, critical bool) error {
		fullReplyMu.Lock()
		unsent := stripDSMLMarkup(fullReply.String()[lastSentLength:])
		currentLen := fullReply.Len()
		fullReplyMu.Unlock()
		if strings.TrimSpace(unsent) == "" {
			return nil
		}
		if err := h.enqueueAsyncText(userID, sessionID, ctxToken, eventType, unsent, critical); err != nil {
			return err
		}
		fullReplyMu.Lock()
		lastSentLength = currentLen
		fullReplyMu.Unlock()
		return nil
	}

	sendCtx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()

	response, err := h.client.SendMessageStreaming(sendCtx, opencode.MessagePayload{
		Channel:     "wechat",
		UserID:      userID,
		ThreadID:    threadID,
		SessionID:   sessionID,
		Content:     content,
		Agent:       agentName,
		Streaming:   true,
		Attachments: attachments,
	}, func(chunk string) error {
		// Session mapping. Always intercept ses_* chunks (they are the
		// session ID sent by client.go's callback) so they never leak into
		// the accumulated text buffer and get delivered to the user.
		// If we already mapped proactively (Fix 1), just suppress the chunk.
		if strings.HasPrefix(chunk, "ses_") && len(chunk) < 100 {
			if !sessionMapped {
				h.adapter.MapUserToSession(userID, chunk)
				log.Printf("wechat: mapped user %s to session %s", userID, chunk[:min(8, len(chunk))])
				sessionMapped = true
			}
			return nil
		}

		// Flush signal → send all remaining content as a single critical send.
		// On failure, do not advance lastSentLength so the final-send block
		// below (and offline queue) will retry the same tail.
		if chunk == opencode.FlushSignal {
			if err := enqueueBufferedTail("flush", true); err != nil {
				log.Printf("wechat: ⚠️ flush send failed user=%s: %v (will retry at final)", userID, err)
			}
			return nil
		}

		// Thinking, tool, and step progress are suppressed for WeChat.
		// WeChat iLink is aggressively rate-limited, so only the final result,
		// todo updates, permissions, and questions are sent. Intermediate
		// tool/step progress would flood the queue and trigger rate-limit
		// cooldowns, delaying the final result.
		if strings.HasPrefix(chunk, opencode.ThinkingSignalPrefix) {
			return nil
		}
		if strings.HasPrefix(chunk, opencode.ToolSignalPrefix) {
			return nil
		}
		if strings.HasPrefix(chunk, opencode.StepSignalPrefix) {
			return nil
		}

		// Todo signal → critical path. Todo updates must be delivered in full
		// and in order — they are the user's primary progress indicator.
		// critical=true ensures they are never dropped by TTL expiry.
		if strings.HasPrefix(chunk, opencode.TodoSignalPrefix) {
			msg := strings.TrimSpace(strings.TrimPrefix(chunk, opencode.TodoSignalPrefix))
			if msg != "" {
				if err := h.enqueueAsyncText(userID, sessionID, ctxToken, "todo", msg, true); err != nil {
					log.Printf("wechat: ⚠️ todo enqueue failed user=%s: %v", userID, err)
				}
			}
			return nil
		}

		// Question signal → critical path. Send directly (bypassing the
		// outbound queue) so the user sees the permission/question
		// immediately even when the queue is congested with progress
		// messages or rate-limited. Missing a question/permission is a
		// worse failure mode than missing tool/step progress — the
		// session blocks waiting for user input that never arrives.
		if strings.HasPrefix(chunk, opencode.QuestionSignalPrefix) {
			msg := strings.TrimSpace(strings.TrimPrefix(chunk, opencode.QuestionSignalPrefix))
			if msg != "" {
				// Drop accumulated low-priority (progress/todo) messages
				// for this user so the permission/question dialog isn't
				// flooded by stale progress updates from earlier in the
				// task. PriorityHigh/Normal entries are preserved.
				if h.outbound != nil {
					if n, _ := h.outbound.DropLowPriorityForUser(userID); n > 0 {
						log.Printf("wechat: 🧹 dropped %d low-priority queued messages before permission/question dialog user=%s", n, userID)
					}
				}
				if err := h.sendTextInline(userID, msg, ctxToken); err != nil {
					log.Printf("wechat: ⚠️ question direct send failed user=%s: %v — enqueuing as PriorityHigh", userID, err)
					// Fall back to the outbound queue with PriorityHigh so the
					// permission/question is retried and not lost. Without this
					// fallback, a rate-limit during the direct send would
					// silently drop the permission prompt and deadlock the session.
					_ = h.enqueueAsyncText(userID, sessionID, ctxToken, "question", msg, true)
				}
			}
			return nil
		}

		if chunk == "" {
			return nil
		}

		// Content chunk → accumulate silently. No intermediate previews are
		// sent for WeChat — only the final result (or FlushSignal) is delivered.
		// This avoids flooding the rate-limited iLink channel with partial text
		// that is superseded by the final reply.
		fullReplyMu.Lock()
		fullReply.WriteString(chunk)
		fullReplyMu.Unlock()
		return nil
	})

	if err != nil {
		log.Printf("wechat: streaming error user=%s: %v", userID, err)
		// Even on error, queue whatever accumulated so far.
		if sendErr := enqueueBufferedTail("error_partial", true); sendErr != nil {
			log.Printf("wechat: ⚠️ partial-result enqueue failed user=%s: %v", userID, sendErr)
		}
		_ = h.enqueueAsyncText(userID, sessionID, ctxToken, "error_notice", "⚠️ 处理出错："+truncate(err.Error(), 300), true)
		return
	}

	fullReplyMu.Lock()
	accumulatedContent := fullReply.String()
	unsent := stripDSMLMarkup(accumulatedContent[lastSentLength:])
	fullReplyMu.Unlock()

	// Detect MEDIA: directives in accumulated content for media dispatch.
	mediaPaths, _ := extractMediaDirective(accumulatedContent)

	if strings.TrimSpace(unsent) != "" {
		log.Printf("wechat: 📦 queueing final message (%d total, %d unsent)", len(accumulatedContent), len(unsent))
		if sendErr := enqueueBufferedTail("final", true); sendErr != nil {
			log.Printf("wechat: ⚠️ final enqueue failed user=%s: %v", userID, sendErr)
		}
	} else if len(accumulatedContent) == 0 {
		// No content was streamed; use the synchronous reply if available.
		if strings.TrimSpace(response.Reply) != "" {
			if sendErr := h.enqueueAsyncText(userID, sessionID, ctxToken, "sync_reply", stripDSMLMarkup(response.Reply), true); sendErr != nil {
				log.Printf("wechat: ⚠️ sync-reply enqueue failed user=%s: %v", userID, sendErr)
			}
		} else if len(mediaPaths) == 0 {
			_ = h.enqueueAsyncText(userID, sessionID, ctxToken, "done", "✅ 处理完成", true)
		}
	} else {
		log.Printf("wechat: all content already sent (%d bytes)", len(accumulatedContent))
	}

	// Send media files detected via MEDIA: directives
	for _, mediaPath := range mediaPaths {
		log.Printf("wechat: 📤 sending media from MEDIA: directive: %s", mediaPath)
		if err := h.sendMediaFile(userID, mediaPath, "", ctxToken); err != nil {
			log.Printf("wechat: ⚠️ media send failed user=%s path=%s err=%v", userID, mediaPath, err)
		}
	}
}

// forceDispatchReset clears the dispatching flag and discards any pending
// queued message for a user. This is called by /abort and /new to unblock
// the user when a previous dispatchToOpenCode goroutine is stuck (e.g.
// upstream model hung, neither session.idle nor session.error fires).
//
// The stuck goroutine will eventually exit (20-min context timeout or
// abort), and its defer will call dispatchPendingIfAny — but since we
// already cleared the pending message, that will be a no-op.
func (h *Handler) forceDispatchReset(userID string) {
	h.dispatching.Delete(userID)

	h.pendingMu.Lock()
	delete(h.pendingMsgs, userID)
	if timer, ok := h.pendingTimers[userID]; ok {
		timer.Stop()
		delete(h.pendingTimers, userID)
	}
	h.pendingMu.Unlock()

	log.Printf("wechat: 🔓 force dispatch reset for user %s (dispatching flag + pending queue cleared)", userID)
}

// enqueuePending stores a user message that arrived while a previous task was
// still dispatching. If a message is already queued for this user, the new
// text is appended (newline-joined) — matching the inbound batcher's
// coalescing behaviour. A long-run reminder timer is (re)armed so the user is
// notified if the current task takes more than 10 minutes.
func (h *Handler) enqueuePending(userID, chatID, content, ctxToken string, attachments []opencode.Attachment, agentName string) {
	h.pendingMu.Lock()
	if existing, ok := h.pendingMsgs[userID]; ok {
		existing.Content = existing.Content + "\n" + content
		h.pendingMu.Unlock()
		log.Printf("wechat: 📝 appended to pending queue for user %s (total len=%d)", userID, len(existing.Content))
		return
	}

	h.pendingMsgs[userID] = &pendingMessage{
		UserID:      userID,
		ChatID:      chatID,
		Content:     content,
		CtxToken:    ctxToken,
		Attachments: attachments,
		AgentName:   agentName,
		QueuedAt:    time.Now(),
	}

	// Arm a 10-minute reminder. If the current task runs this long, the user
	// gets a notice with options (wait / abort / new session / status).
	if timer, ok := h.pendingTimers[userID]; ok {
		timer.Stop()
	}
	h.pendingTimers[userID] = time.AfterFunc(10*time.Minute, func() {
		h.notifyLongRunningTask(userID)
	})
	h.pendingMu.Unlock()

	// Notify the user immediately that their message is queued.
	// critical=true so this notice isn't dropped by TTL during rate-limit congestion.
	_ = h.enqueueAsyncText(userID, "", ctxToken, "info",
		"⏳ 当前任务正在执行中，您的消息已排队，完成后将自动处理。", true)

	log.Printf("wechat: 📋 message queued for user %s (task in progress)", userID)
}

// dispatchPendingIfAny is called at the end of dispatchToOpenCode (via defer)
// to check if a message was queued while the task was running. If so, it is
// dispatched now — guaranteeing that the previous task's results were fully
// enqueued to the outbound queue before the next task starts.
func (h *Handler) dispatchPendingIfAny(userID string) {
	h.pendingMu.Lock()
	pending, ok := h.pendingMsgs[userID]
	if ok {
		delete(h.pendingMsgs, userID)
	}
	if timer, ok := h.pendingTimers[userID]; ok {
		timer.Stop()
		delete(h.pendingTimers, userID)
	}
	h.pendingMu.Unlock()

	if !ok || pending == nil {
		return
	}

	waitTime := time.Since(pending.QueuedAt).Round(time.Second)
	log.Printf("wechat: ▶️ dispatching pending message for user %s (queued %s ago)", userID, waitTime)

	// Notify the user that the previous task completed and the queued
	// message is now being processed.
	// critical=true so this notice isn't dropped by TTL during rate-limit congestion.
	_ = h.enqueueAsyncText(userID, "", pending.CtxToken, "info",
		"✅ 上一个任务已完成，正在处理您的下一条消息...", true)

	go h.dispatchToOpenCode(context.Background(), pending.UserID, pending.ChatID,
		pending.Content, pending.CtxToken, pending.Attachments, pending.AgentName)
}

// notifyLongRunningTask sends a user-facing notice when the current task has
// been running for more than 10 minutes while a pending message is queued.
// It uses GetSessionDiagnostics to distinguish "still executing normally"
// from "likely stuck" so the user gets actionable information instead of a
// generic timeout notice. It re-arms itself every 5 minutes for repeat checks.
func (h *Handler) notifyLongRunningTask(userID string) {
	h.pendingMu.Lock()
	_, hasPending := h.pendingMsgs[userID]
	h.pendingMu.Unlock()

	if !hasPending {
		return // pending message was already dispatched
	}

	sessionID, hasSession := h.adapter.GetSessionForUser(userID)

	// Use live diagnostics to determine whether the task is genuinely stuck
	// or just taking a long time. This avoids crying wolf when the model is
	// legitimately working on a complex task.
	var msg string
	if hasSession {
		diag := h.client.GetSessionDiagnostics(sessionID, "")
		sessionPart := fmt.Sprintf("\n会话ID: %s", sessionID[:min(8, len(sessionID))])

		switch {
		case !diag.Running && !diag.HasLiveHandler:
			// Session is not running and no live handler — the
			// dispatchToOpenCode goroutine is stuck somewhere else (e.g.
			// SDK call hung). This is a true deadlock that won't self-resolve.
			msg = fmt.Sprintf("🔴 检测到任务可能已卡死（会话不在运行状态但调度未完成）。%s\n\n"+
				"这通常意味着上游服务无响应。建议：\n"+
				"• 发送 /abort 中止并重新发送消息\n"+
				"• 发送 /new 创建新会话\n"+
				"• 发送 /status 查看详细诊断", sessionPart)

		case diag.ToolStuck:
			// Active tools but no SSE events for >= 5 minutes — a subagent
			// or tool call is hung on the opencode server side.
			msg = fmt.Sprintf("🔴 检测到工具执行可能卡死（有活跃工具但超过5分钟无事件更新）。%s\n\n"+
				"最后事件: %s（%s前）\n"+
				"活跃工具数: %d\n\n"+
				"建议：\n"+
				"• 发送 /abort 中止当前任务\n"+
				"• 发送 /status 查看详细诊断\n"+
				"• 发送 /new 创建新会话",
				sessionPart,
				diag.LastEventType,
				time.Since(diag.LastEventAt).Round(time.Second),
				diag.ActiveTools)

		case diag.RetryAttempt > 0:
			// Upstream provider is repeatedly retrying — model may be
			// unavailable or rate-limited at the provider level.
			retryInfo := ""
			if diag.RetryMessage != "" {
				retryInfo = fmt.Sprintf("\n错误: %s", truncate(diag.RetryMessage, 120))
			}
			msg = fmt.Sprintf("⚠️ 上游模型反复重试中（第%d次/%d上限），任务执行时间较长。%s%s\n\n"+
				"您可以选择：\n"+
				"• 继续等待（重试可能成功）\n"+
				"• 发送 /abort 中止当前任务\n"+
				"• 发送 /status 查看详细诊断\n"+
				"• 发送 /new 创建新会话",
				diag.RetryAttempt, diag.RetryMax, sessionPart, retryInfo)

		case diag.HasLiveHandler && time.Since(diag.LastEventAt) < 3*time.Minute:
			// Still receiving events recently — task is actively executing,
			// just taking a long time. Don't alarm the user.
			msg = fmt.Sprintf("⏳ 任务仍在执行中（最近%s前有更新），请耐心等待。%s\n\n"+
				"最后事件: %s\n"+
				"如需中止: 发送 /abort\n"+
				"查看详情: 发送 /status",
				time.Since(diag.LastEventAt).Round(time.Second),
				sessionPart,
				diag.LastEventType)

		default:
			// Running but no recent events (3-5 min gap) — ambiguous, could
			// be a long tool call or early-stage stuck. Give a gentle notice.
			lastEventInfo := "无事件记录"
			if !diag.LastEventAt.IsZero() {
				lastEventInfo = fmt.Sprintf("%s（%s前）", diag.LastEventType, time.Since(diag.LastEventAt).Round(time.Second))
			}
			msg = fmt.Sprintf("⚠️ 任务已执行较长时间（超过10分钟）。%s\n\n"+
				"最后事件: %s\n"+
				"您可以选择：\n"+
				"• 继续等待（任务可能即将完成）\n"+
				"• 发送 /abort 中止当前任务\n"+
				"• 发送 /status 查看任务状态\n"+
				"• 发送 /new 创建新会话", sessionPart, lastEventInfo)
		}
	} else {
		msg = "⚠️ 当前任务已执行较长时间（超过10分钟）。\n\n" +
			"您可以选择：\n" +
			"• 继续等待（任务可能即将完成）\n" +
			"• 发送 /abort 中止当前任务\n" +
			"• 发送 /status 查看任务状态\n" +
			"• 发送 /new 创建新会话"
	}

	_ = h.enqueueAsyncText(userID, "", "", "info", msg, true)

	// Re-arm for a follow-up check in 5 minutes.
	h.pendingMu.Lock()
	if _, ok := h.pendingMsgs[userID]; ok {
		h.pendingTimers[userID] = time.AfterFunc(5*time.Minute, func() {
			h.notifyLongRunningTask(userID)
		})
	}
	h.pendingMu.Unlock()
}

// sendCommandReply delivers a slash-command / status reply directly and
// synchronously, bypassing the durable outbound queue. Command responses are
// independent of the AI streaming pipeline (matching DingTalk/Feishu), so they
// must return immediately rather than being paced, priority-reordered, or
// queued behind streaming task output.
func (h *Handler) sendCommandReply(userID, text, ctxToken string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	if err := h.sendTextInline(userID, text, ctxToken); err != nil {
		log.Printf("wechat: command reply send failed user=%s: %v", userID, err)
	}
}

func (h *Handler) enqueueAsyncText(userID, sessionID, ctxToken, eventType, text string, critical bool) error {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	if h.outbound == nil {
		// Queue not available — fall back to direct send with senderRegistry
		// pacing so we still respect per-user rate limits.
		return h.sendTextInline(userID, text, ctxToken)
	}
	return h.outbound.EnqueueText(userID, sessionID, ctxToken, eventType, text, critical)
}

func (h *Handler) sendQueuedText(item *queuedOutboundText) error {
	ctxToken := item.ContextToken
	h.mu.Lock()
	if fresh := h.lastContextTokens[item.UserID]; fresh != "" {
		ctxToken = fresh
	}
	h.mu.Unlock()

	// Send a single chunk. We deliberately do NOT use senderRegistry.sendChunks
	// here because:
	//   1. The queue already serializes per-user dispatch (pickNextDueHead
	//      returns one item at a time per user).
	//   2. The queue's nack/backoff handles rate-limit retries — wrapping
	//      that in senderRegistry's own backoff would double-retry and
	//      block the dispatch goroutine for 12s+ per failed attempt.
	//   3. The queue tick interval (1.5s) already provides pacing between
	//      sends, equivalent to senderRegistry's minGap.
	//
	// senderRegistry is still used by sendTextInline (the no-queue fallback).
	effectiveToken := ctxToken
	h.trackSentText(item.UserID, item.Content)
	err := h.weClient.SendText(item.UserID, item.Content, effectiveToken)
	if err != nil && errors.Is(err, ErrSessionExpired) && effectiveToken != "" {
		log.Printf("wechat: queued send session expired user=%s; retrying tokenless", item.UserID)
		h.mu.Lock()
		delete(h.lastContextTokens, item.UserID)
		h.mu.Unlock()
		effectiveToken = ""
		h.trackSentText(item.UserID, item.Content)
		err = h.weClient.SendText(item.UserID, item.Content, "")
	}
	return err
}

// notifyDeferredDelivery sends a best-effort user notice that delivery is
// deferred and will be retried by the active outbound mechanism.
// critical=true so this notice isn't dropped by TTL during congestion.
func (h *Handler) notifyDeferredDelivery(userID, ctxToken string) {
	_ = h.enqueueAsyncText(userID, "", ctxToken, "deferred_notice", "⚠️ 网络繁忙，结果将稍后送达", true)
}

// offlineSend is the transport callback the offline worker uses to retry
// a parked message. It refreshes the context_token from the live cache (the
// originally-captured one may have expired during the wait) before delegating
// to the normal sender.
func (h *Handler) offlineSend(userID, ctxToken, content string) error {
	h.mu.Lock()
	freshToken, ok := h.lastContextTokens[userID]
	h.mu.Unlock()
	if ok && freshToken != "" {
		ctxToken = freshToken
	}
	// Route through the queue so the retried message respects pacing and
	// ordering relative to any other queued messages for this user.
	return h.enqueueAsyncText(userID, "", ctxToken, "offline_retry", content, true)
}

// notifyOfflineDrop tells the user that we have given up on an offline-queue
// entry. Best-effort: if this notice itself can't be delivered the user will
// have already noticed the missing result via the prior "稍后送达" notice.
func (h *Handler) notifyOfflineDrop(userID string) {
	h.mu.Lock()
	ctxToken := h.lastContextTokens[userID]
	h.mu.Unlock()
	_ = h.enqueueAsyncText(userID, "", ctxToken,
		"offline_drop_notice", "⚠️ 部分结果未能送达，请回复 /retry 重发上一次请求", true)
}

// --- Command handlers ---

func (h *Handler) helpText() string {
	return `📖 OpenCode Gateway (微信)

📋 可用命令：
/help 或 帮助     - 显示此帮助
/new 或 新会话    - 创建新会话（旧回复自动保存）
/pending          - 查看未送达的旧会话回复
/recover [会话ID] - 恢复并重新送达旧回复
/fork 或 派生     - 派生当前会话
/todo 或 任务     - 查看当前任务进度
/abort 或 停止    - 中止正在运行的任务
/status 或 状态   - 查看当前会话状态
/undo 或 撤销     - 撤销上一次操作
/redo 或 重做     - 重做已撤销的操作
/diff 或 变更     - 查看文件变更
/cmd <命令>       - 在会话中执行 shell 命令
/answer <回复>    - 回答待确认问题
/retry            - 重试上一条消息
/sessions         - 列出所有会话
/summary 或 总结  - 压缩上下文
/clear 或 清除    - 清除当前会话
/refresh          - 刷新技能缓存
/skills           - 列出可用技能
/model            - 查看/设置模型
/thinking on|off  - 开关 thinking 返回
/final on|off     - 开关 final-only 模式
/steps on|off     - 开关步骤显示
/devcore          - 查看/设置 Dev Core 提示词
/config 或 配置   - 查看当前配置
/crontask         - 管理定时任务
/crontask help    - 定时任务帮助

📸 发送图片/视频会自动使用支持视觉的模型处理。
收到权限请求时，直接回复：允许 / 拒绝 / 始终允许
收到选择题时，直接回复选项序号（如 1）即可。

直接发送文字即可与 AI 对话。`
}

func (h *Handler) handleFork(ctx context.Context, userID string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "❌ 当前没有活跃会话"
	}
	h.adapter.ClearSessionForUser(userID)
	return fmt.Sprintf("✅ 已派生会话（旧: %s），下次消息将创建新会话", sessionID[:min(8, len(sessionID))])
}

func (h *Handler) handleTodo(userID string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "❌ 当前没有活跃会话"
	}
	return fmt.Sprintf("📋 当前会话: %s\n（请直接发送消息查询任务进度）", sessionID[:min(8, len(sessionID))])
}

func (h *Handler) handleAbort(ctx context.Context, userID string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "❌ 当前没有活跃会话"
	}
	if err := h.client.AbortSession(ctx, sessionID); err != nil {
		return fmt.Sprintf("❌ 中止失败: %v", err)
	}
	// ★ Clear the dispatching flag and pending queue so the user can send
	// new messages immediately after abort. Without this, a stuck
	// dispatchToOpenCode goroutine (upstream model hung) would keep the flag
	// set forever, blocking all subsequent requests.
	h.forceDispatchReset(userID)
	return "✅ 已发送中止请求，您可以立即发送新消息"
}

func (h *Handler) handleStatus(userID string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "当前没有活跃会话，发送消息即可开始。"
	}
	status := fmt.Sprintf("📊 当前会话: %s", sessionID[:min(8, len(sessionID))])
	// 追加实时处理诊断（处理中/上游重试/活跃工具等），帮助用户判断为何"正在处理中"。
	if diag := h.client.GetSessionDiagnostics(sessionID, "").FormatSessionStatus(); diag != "" {
		status += "\n\n— 实时诊断 —\n" + diag
	}
	return status
}

func (h *Handler) handlePending(userID string) string {
	if h.offline == nil {
		return "ℹ️ 离线队列未启用"
	}
	entries := h.offline.ListAbandoned(userID)
	if len(entries) == 0 {
		return "ℹ️ 没有未送达的旧会话回复"
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("📦 未送达的旧会话回复（%d 条）\n", len(entries)))
	for i, e := range entries {
		sid := e.SessionID
		if len(sid) > 8 {
			sid = sid[:8]
		}
		preview := e.Content
		if len(preview) > 60 {
			preview = preview[:60] + "..."
		}
		b.WriteString(fmt.Sprintf("%d. [%s] %s\n", i+1, sid, preview))
	}
	b.WriteString("\n发送 /recover 重新送达全部，或 /recover <session-id前缀> 送达指定会话的回复")
	return strings.TrimRight(b.String(), "\n")
}

func (h *Handler) handleRecover(userID, sessionFilter string) string {
	if h.offline == nil {
		return "ℹ️ 离线队列未启用"
	}
	n := h.offline.RecoverSession(userID, sessionFilter)
	if n == 0 {
		return "ℹ️ 没有找到可恢复的回复"
	}
	return fmt.Sprintf("✅ 已恢复 %d 条回复，正在通过队列送达...", n)
}

func (h *Handler) handleNewSession(userID string) string {
	// Clear pending outbound messages from the previous session.
	// PriorityNormal entries (AI replies) are parked into the offline queue
	// tagged with sessionID so the user can recover them via /pending /recover.
	// PriorityHigh (question/permission) and PriorityLow (todo) are discarded.
	if h.outbound != nil {
		h.outbound.ClearForUser(userID, func(uid, sid, ctxToken, content string) {
			if h.offline != nil {
				h.offline.enqueueWithSession(uid, sid, ctxToken, content)
			}
		})
	}
	if sessionID, ok := h.adapter.GetSessionForUser(userID); ok {
		h.adapter.ClearSessionForUser(userID)
		return fmt.Sprintf("✅ 已清除旧会话（%s），下次消息将创建新会话\n\n💡 旧会话的未送达回复已保存，发送 /pending 查看", sessionID[:min(8, len(sessionID))])
	}
	return "ℹ️ 当前没有活跃会话，直接发送消息即可开始"
}

func (h *Handler) handleUndo(ctx context.Context, userID string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "❌ 当前没有活跃会话"
	}
	if _, err := h.client.RevertSession(ctx, sessionID, ""); err != nil {
		return fmt.Sprintf("❌ 撤销失败: %v", err)
	}
	return "↩️ 已撤销上一次操作，可使用 /redo 恢复"
}

func (h *Handler) handleRedo(ctx context.Context, userID string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "❌ 当前没有活跃会话"
	}
	if _, err := h.client.UnrevertSession(ctx, sessionID); err != nil {
		return fmt.Sprintf("❌ 重做失败: %v", err)
	}
	return "✅ 已重做"
}

func (h *Handler) handleDiff(_ context.Context, userID string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "❌ 当前没有活跃会话"
	}
	diffs := h.client.GetDiffForSession(sessionID)
	if len(diffs) == 0 {
		return "ℹ️ 当前会话没有文件变更"
	}
	var sb strings.Builder
	sb.WriteString("📁 文件变更:\n")
	for _, d := range diffs {
		sb.WriteString(fmt.Sprintf("  %s (+%d/-%d)\n", d.Path, d.Added, d.Removed))
	}
	return sb.String()
}

func (h *Handler) handleCmd(_ context.Context, userID, command, ctxToken string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "❌ 当前没有活跃会话，请先发送消息创建会话"
	}

	// Run the shell command off the inbound poll loop. The underlying SDK
	// Session.Shell() call has no timeout of its own and can block for minutes;
	// executing it inline would stall the single-threaded poll loop and prevent
	// any further WeChat messages from being processed. The goroutine bounds the
	// execution and delivers the result via a direct send.
	go func() {
		cmdCtx, cancel := context.WithTimeout(context.Background(), cmdExecTimeout)
		defer cancel()

		output, err := h.client.ExecuteShellOutput(cmdCtx, sessionID, command)
		var reply string
		switch {
		case err != nil && cmdCtx.Err() == context.DeadlineExceeded:
			reply = fmt.Sprintf("⏰ 命令执行超时（超过 %.0f 分钟已中止）。如需长时间运行的命令，请拆分或在后台执行。", cmdExecTimeout.Minutes())
		case err != nil:
			reply = fmt.Sprintf("❌ 命令执行失败: %v", err)
		case strings.TrimSpace(output) != "":
			reply = fmt.Sprintf("🖥️ 命令执行结果:\n\n%s", output)
		default:
			reply = "🖥️ 命令已执行，但没有可显示的输出"
		}
		h.sendCommandReply(userID, reply, ctxToken)
	}()

	return fmt.Sprintf("🖥️ 命令执行中，最长等待 %.0f 分钟…", cmdExecTimeout.Minutes())
}

func (h *Handler) handleAnswerCommand(ctx context.Context, userID, answer string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "❌ 当前没有活跃会话"
	}
	question, hasQ := h.client.GetLatestPendingQuestion(sessionID)
	permission, hasP := h.client.GetLatestPendingPermission(sessionID)

	if hasP {
		resp := replyToPermissionResponse(answer)
		if resp == "" {
			resp = "once"
		}
		if err := h.client.RespondToPermission(ctx, permission.ID, resp); err != nil {
			return fmt.Sprintf("❌ 权限回复失败: %v", err)
		}
		return fmt.Sprintf("✅ 已回复权限请求: %s", answer)
	}
	if hasQ {
		if err := h.client.AnswerQuestion(ctx, question.ID, answer); err != nil {
			return fmt.Sprintf("❌ 回复失败: %v", err)
		}
		return fmt.Sprintf("✅ 已回复: %s", answer)
	}
	return "ℹ️ 当前没有待回复的问题"
}

func (h *Handler) handleRetry(ctx context.Context, userID, chatID, extraMsg string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "❌ 当前没有活跃会话"
	}
	// Revert last message, then re-send
	_, err := h.client.RevertSession(ctx, sessionID, "")
	if err != nil {
		return fmt.Sprintf("❌ 重试失败（撤销步骤）: %v", err)
	}
	if extraMsg == "" {
		extraMsg = "请继续"
	}
	log.Printf("wechat: retry session %s with message: %s", sessionID[:min(8, len(sessionID))], extraMsg)

	// Re-dispatch to OpenCode (respecting the dispatching gate)
	h.mu.Lock()
	ctxToken := h.lastContextTokens[userID]
	h.mu.Unlock()
	if _, busy := h.dispatching.Load(userID); busy {
		h.enqueuePending(userID, chatID, extraMsg, ctxToken, nil, "")
	} else {
		go h.dispatchToOpenCode(ctx, userID, chatID, extraMsg, ctxToken, nil, "")
	}
	return ""
}

func (h *Handler) handleSessions(ctx context.Context) string {
	sessions, err := h.client.ListSessions(ctx)
	if err != nil {
		return fmt.Sprintf("❌ 获取会话列表失败: %v", err)
	}
	if len(sessions) == 0 {
		return "📋 暂无会话"
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("📋 会话列表 (共 %d 个)\n\n", len(sessions)))
	for i, s := range sessions {
		if i >= 10 {
			sb.WriteString(fmt.Sprintf("\n... 还有 %d 个会话", len(sessions)-10))
			break
		}
		sb.WriteString(fmt.Sprintf("%d. %s\n", i+1, s.ID[:min(12, len(s.ID))]))
	}
	return sb.String()
}

func (h *Handler) handleSummary(ctx context.Context, userID string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "❌ 当前没有活跃会话"
	}
	if err := h.client.SummarizeSession(ctx, sessionID); err != nil {
		return fmt.Sprintf("❌ 压缩上下文失败: %v", err)
	}
	return "✅ 已触发上下文压缩"
}

func (h *Handler) handleClear(ctx context.Context, userID string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "ℹ️ 当前没有活跃会话"
	}
	if err := h.client.DeleteSession(ctx, sessionID); err != nil {
		return fmt.Sprintf("❌ 清除会话失败: %v", err)
	}
	h.adapter.ClearSessionForUser(userID)
	return fmt.Sprintf("✅ 会话 %s 已清除", sessionID[:min(8, len(sessionID))])
}

func (h *Handler) handleRefresh(ctx context.Context) string {
	if err := h.client.RefreshSkills(ctx); err != nil {
		return "❌ 刷新技能缓存失败: " + err.Error()
	}
	return "✅ 技能缓存已刷新"
}

func (h *Handler) handleListSkills(ctx context.Context) string {
	agents, err := h.client.ListAgents(ctx)
	if err != nil {
		return fmt.Sprintf("❌ 获取技能列表失败: %v", err)
	}
	if len(agents) == 0 {
		return "📋 暂无可用技能\n\n使用 /refresh 刷新技能缓存"
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("📋 可用技能 (共 %d 个):\n\n", len(agents)))
	for i, a := range agents {
		if i >= 20 {
			sb.WriteString(fmt.Sprintf("\n... 还有 %d 个技能", len(agents)-20))
			break
		}
		sb.WriteString(fmt.Sprintf("%d. %s", i+1, a.Name))
		if a.Description != "" {
			sb.WriteString(fmt.Sprintf(" - %s", a.Description))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

func (h *Handler) handleCronTask(ctx context.Context, userID, chatID, text string) string {
	if h.cronScheduler == nil {
		return "❌ 定时任务功能未启用"
	}

	parts := strings.Fields(text)
	if len(parts) < 2 {
		return h.sendCronTaskHelp()
	}

	subCommand := parts[1]
	switch subCommand {
	case "add", "create", "新增":
		return h.handleCronTaskAdd(ctx, userID, chatID, parts[2:])
	case "list", "ls", "列表":
		return h.handleCronTaskList()
	case "delete", "del", "rm", "删除":
		return h.handleCronTaskDelete(parts[2:])
	case "enable", "启用":
		return h.handleCronTaskEnable(parts[2:])
	case "disable", "禁用":
		return h.handleCronTaskDisable(parts[2:])
	case "info", "详情":
		return h.handleCronTaskInfo(parts[2:])
	case "help", "帮助":
		return h.sendCronTaskHelp()
	default:
		// NL fallback
		nlText := strings.TrimSpace(strings.TrimPrefix(text, "/crontask"))
		if h.nlScheduleSvc != nil && nlText != "" {
			resp, err := h.nlScheduleSvc.HandleText(ctx, scheduler.NLScheduleRequest{
				AdapterType: "wechat",
				UserID:      userID,
				Channel:     chatID,
				Text:        nlText,
				ForceCreate: true,
			})
			if err == nil && resp != nil && resp.Handled {
				return resp.Message
			}
		}
		return "❌ 未知的子命令，使用 /crontask help 查看帮助"
	}
}

func (h *Handler) handleCronTaskAdd(_ context.Context, userID, chatID string, args []string) string {
	parsedArgs := parseQuotedArgs(strings.Join(args, " "))
	if len(parsedArgs) < 3 {
		return "❌ 参数不足\n\n" +
			"格式: /crontask add \"cron表达式\" \"任务名称\" \"任务内容\" [agent]\n" +
			"示例:\n" +
			"/crontask add \"0 0 9 * * *\" \"每日检查\" \"查看系统负载\""
	}

	cronExpr := parsedArgs[0]
	taskName := parsedArgs[1]
	taskContent := parsedArgs[2]
	agent := ""
	if len(parsedArgs) > 3 {
		agent = parsedArgs[3]
	}

	now := time.Now()
	task := &scheduler.ScheduledTask{
		Name:        taskName,
		Description: fmt.Sprintf("通过微信创建 (用户: %s)", userID),
		Type:        scheduler.TaskTypeAgent,
		CronExpr:    cronExpr,
		Enabled:     true,
		AdapterType: "wechat",
		Channel:     chatID,
		Content:     taskContent,
		Agent:       agent,
		Metadata: map[string]interface{}{
			"created_by":   userID,
			"created_from": "wechat",
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := h.cronScheduler.AddScheduledTask(task); err != nil {
		return fmt.Sprintf("❌ 创建定时任务失败: %v", err)
	}

	agentDisplay := "(默认)"
	if task.Agent != "" {
		agentDisplay = task.Agent
	}
	nextRun := "未知"
	if task.NextRunTime != nil {
		nextRun = task.NextRunTime.Format("2006-01-02 15:04:05")
	}

	return fmt.Sprintf(
		"✅ 定时任务创建成功！\n\n"+
			"📋 任务ID: %s\n"+
			"📝 名称: %s\n"+
			"⏰ Cron: %s\n"+
			"📄 内容: %s\n"+
			"🤖 Agent: %s\n"+
			"⏱️ 下次运行: %s\n\n"+
			"使用 /crontask list 查看所有任务",
		task.ID, task.Name, task.CronExpr, task.Content, agentDisplay, nextRun,
	)
}

func (h *Handler) handleCronTaskList() string {
	tasks := h.cronScheduler.GetScheduledTasksByAdapter("wechat")
	if len(tasks) == 0 {
		return "📋 暂无定时任务\n\n使用 /crontask help 查看如何创建"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("📋 定时任务列表 (共 %d 个)\n\n", len(tasks)))
	for i, task := range tasks {
		status := "✅"
		if !task.Enabled {
			status = "⏸️"
		}
		agentDisplay := "(默认)"
		if task.Agent != "" {
			agentDisplay = task.Agent
		}
		sb.WriteString(fmt.Sprintf(
			"%d. %s %s\n   ID: %s\n   Cron: %s\n   Agent: %s\n   运行次数: %d\n",
			i+1, status, task.Name, task.ID, task.CronExpr, agentDisplay, task.RunCount,
		))
		if task.NextRunTime != nil {
			sb.WriteString(fmt.Sprintf("   下次运行: %s\n", task.NextRunTime.Format("2006-01-02 15:04:05")))
		}
		if task.LastRunTime != nil {
			sb.WriteString(fmt.Sprintf("   上次运行: %s (%s)\n", task.LastRunTime.Format("2006-01-02 15:04:05"), task.LastRunStatus))
		}
		sb.WriteString("\n")
	}
	sb.WriteString("💡 使用 /crontask info <ID> 查看详情")
	return sb.String()
}

func (h *Handler) handleCronTaskDelete(args []string) string {
	if len(args) < 1 {
		return "❌ 请指定任务ID\n\n格式: /crontask delete <任务ID>"
	}
	if err := h.cronScheduler.RemoveScheduledTask(args[0]); err != nil {
		return fmt.Sprintf("❌ 删除任务失败: %v", err)
	}
	return fmt.Sprintf("✅ 任务 %s 已删除", args[0])
}

func (h *Handler) handleCronTaskEnable(args []string) string {
	if len(args) < 1 {
		return "❌ 请指定任务ID\n\n格式: /crontask enable <任务ID>"
	}
	if err := h.cronScheduler.EnableTask(args[0]); err != nil {
		return fmt.Sprintf("❌ 启用任务失败: %v", err)
	}
	return fmt.Sprintf("✅ 任务 %s 已启用", args[0])
}

func (h *Handler) handleCronTaskDisable(args []string) string {
	if len(args) < 1 {
		return "❌ 请指定任务ID\n\n格式: /crontask disable <任务ID>"
	}
	if err := h.cronScheduler.DisableTask(args[0]); err != nil {
		return fmt.Sprintf("❌ 禁用任务失败: %v", err)
	}
	return fmt.Sprintf("⏸️ 任务 %s 已禁用", args[0])
}

func (h *Handler) handleCronTaskInfo(args []string) string {
	if len(args) < 1 {
		return "❌ 请指定任务ID\n\n格式: /crontask info <任务ID>"
	}
	task, err := h.cronScheduler.GetScheduledTask(args[0])
	if err != nil {
		return fmt.Sprintf("❌ 获取任务信息失败: %v", err)
	}
	status := "✅ 启用"
	if !task.Enabled {
		status = "⏸️ 禁用"
	}
	agentDisplay := "(默认)"
	if task.Agent != "" {
		agentDisplay = task.Agent
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(
		"📋 定时任务详情\n\n"+
			"📝 名称: %s\n"+
			"🆔 ID: %s\n"+
			"📄 描述: %s\n"+
			"⏰ Cron: %s\n"+
			"📊 状态: %s\n"+
			"🤖 Agent: %s\n"+
			"📄 内容: %s\n"+
			"🔢 运行次数: %d\n",
		task.Name, task.ID, task.Description, task.CronExpr, status, agentDisplay, task.Content, task.RunCount,
	))
	if task.NextRunTime != nil {
		sb.WriteString(fmt.Sprintf("⏱️ 下次运行: %s\n", task.NextRunTime.Format("2006-01-02 15:04:05")))
	}
	if task.LastRunTime != nil {
		sb.WriteString(fmt.Sprintf("📅 上次运行: %s\n📊 运行状态: %s\n", task.LastRunTime.Format("2006-01-02 15:04:05"), task.LastRunStatus))
		if task.LastRunResult != "" {
			sb.WriteString(fmt.Sprintf("📝 运行结果: %s\n", task.LastRunResult))
		}
	}
	sb.WriteString(fmt.Sprintf("\n⏰ 创建时间: %s\n🔄 更新时间: %s", task.CreatedAt.Format("2006-01-02 15:04:05"), task.UpdatedAt.Format("2006-01-02 15:04:05")))
	return sb.String()
}

func (h *Handler) sendCronTaskHelp() string {
	return `📋 定时任务命令帮助

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

💡 也支持自然语言:
/crontask 每天早上9点提醒我检查系统`
}

// parseQuotedArgs parses space-separated arguments respecting double-quoted strings.
func parseQuotedArgs(input string) []string {
	var result []string
	var inQuote bool
	var current strings.Builder

	for _, r := range input {
		switch r {
		case '"':
			if inQuote {
				inQuote = false
				result = append(result, current.String())
				current.Reset()
			} else {
				inQuote = true
			}
		case ' ', '\t':
			if inQuote {
				current.WriteRune(r)
			} else if current.Len() > 0 {
				result = append(result, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}
	if current.Len() > 0 {
		result = append(result, current.String())
	}
	return result
}

// --- Model / Thinking / Final / Steps / DevCore / Config commands ---

func (h *Handler) handleModel(ctx context.Context, userID, content string) string {
	parts := strings.Fields(content)

	// Query mode: /model
	if len(parts) == 1 {
		return h.handleModelQuery(ctx, userID)
	}

	// Set mode: /model provider/model
	return h.handleModelSet(ctx, userID, parts[1:])
}

func (h *Handler) handleModelQuery(ctx context.Context, userID string) string {
	var sb strings.Builder

	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if ok {
		sb.WriteString(fmt.Sprintf("🤖 当前会话: %s\n\n", sessionID[:min(8, len(sessionID))]))
	} else {
		sb.WriteString("ℹ️ 当前没有活跃的会话\n\n")
	}

	providers, err := h.client.GetProviders(ctx)
	if err != nil {
		sb.WriteString(fmt.Sprintf("💡 可用模型列表获取失败: %v", err))
		return sb.String()
	}
	if len(providers) == 0 {
		sb.WriteString("💡 未获取到可用模型")
		return sb.String()
	}

	sb.WriteString("📚 可用模型:\n")
	for _, p := range providers {
		sb.WriteString(fmt.Sprintf("\n【%s】\n", p.ID))
		if len(p.Models) == 0 {
			sb.WriteString("  (无模型)\n")
			continue
		}
		maxShow := min(8, len(p.Models))
		for i := 0; i < maxShow; i++ {
			sb.WriteString(fmt.Sprintf("  /model %s/%s\n", p.ID, p.Models[i].ID))
		}
		if len(p.Models) > maxShow {
			sb.WriteString(fmt.Sprintf("  ... 还有 %d 个\n", len(p.Models)-maxShow))
		}
	}
	return sb.String()
}

func (h *Handler) handleModelSet(ctx context.Context, userID string, args []string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		if recovered, found := h.client.FindLatestSessionForUser(ctx, "wechat", userID); found {
			sessionID = recovered
			h.adapter.MapUserToSession(userID, sessionID)
		} else {
			return "❌ 当前没有活跃的会话\n\n请先发送消息创建会话，然后再设置模型"
		}
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
		return "❌ 提供商ID不能为空"
	}
	if modelID == "" {
		return "❌ 模型ID不能为空\n\n使用方法:\n/model <provider>/<model>\n例如:\n/model anthropic/claude-sonnet-4-20250514"
	}

	// Case-insensitive match against real providers
	providers, err := h.client.GetProviders(ctx)
	if err == nil && len(providers) > 0 {
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
			return fmt.Sprintf("❌ 未找到提供商: %s\n\n请先执行 /model 查看可用 provider/model", providerID)
		}
		if !modelMatched {
			return fmt.Sprintf("❌ 提供商 %s 下未找到模型: %s\n\n请先执行 /model 查看可用模型", providerID, modelID)
		}
	}

	if err := h.client.UpdateSessionProvider(ctx, sessionID, providerID, modelID); err != nil {
		return fmt.Sprintf("❌ 更新模型失败: %v", err)
	}

	return fmt.Sprintf("✅ 已设置会话模型\n\n提供商: %s\n模型: %s\n会话: %s", providerID, modelID, sessionID[:min(8, len(sessionID))])
}

func (h *Handler) handleThinking(content string) string {
	parts := strings.Fields(strings.TrimSpace(content))
	if len(parts) == 1 {
		status := "off"
		if h.client.IsThinkingEnabled() {
			status = "on"
		}
		return fmt.Sprintf("🧠 Thinking 状态: %s\n\n/thinking on  - 开启\n/thinking off - 关闭", status)
	}
	switch strings.ToLower(parts[1]) {
	case "on", "true", "1":
		h.client.SetThinkingEnabled(true)
		return "✅ 已开启 thinking 返回"
	case "off", "false", "0":
		h.client.SetThinkingEnabled(false)
		return "✅ 已关闭 thinking 返回"
	default:
		return "❌ 格式错误\n\n/thinking on - 开启\n/thinking off - 关闭"
	}
}

func (h *Handler) handleFinal(content string) string {
	parts := strings.Fields(strings.TrimSpace(content))
	if len(parts) == 1 {
		return "📦 Final-only 模式: 已内置开启\n\nWeChat 默认只发送最终结果、todo更新、权限和问题，不发送中间进度。此行为不可关闭。"
	}
	return "📦 Final-only 模式: 已内置开启\n\nWeChat 默认只发送最终结果、todo更新、权限和问题，不发送中间进度。此行为不可关闭。"
}

func (h *Handler) handleSteps(content string) string {
	parts := strings.Fields(strings.TrimSpace(content))
	if len(parts) == 1 {
		status := "off"
		if h.client.IsStepEnabled() {
			status = "on"
		}
		return fmt.Sprintf("🪜 步骤显示: %s\n\n/steps on  - 开启\n/steps off - 关闭", status)
	}
	switch strings.ToLower(parts[1]) {
	case "on", "true", "1":
		h.client.SetStepEnabled(true)
		return "✅ 已开启步骤显示"
	case "off", "false", "0":
		h.client.SetStepEnabled(false)
		return "✅ 已关闭步骤显示"
	default:
		return "❌ 格式错误\n\n/steps on - 开启\n/steps off - 关闭"
	}
}

func (h *Handler) handleDevCore(content string) string {
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
		return fmt.Sprintf("🧩 Dev Core 状态: %s\n\n当前提示词:\n%s\n\n用法:\n/devcore <偏好>\n/devcore on|off\n/devcore set <提示词>\n/devcore reset", status, prompt)
	}

	arg := strings.ToLower(parts[1])
	switch arg {
	case "on", "true", "1":
		if strings.TrimSpace(h.client.GetDevCorePrompt()) == "" {
			return "❌ 未设置提示词\n\n请先: /devcore <偏好>"
		}
		h.client.SetDevCoreEnabled(true)
		return "✅ 已开启 Dev Core"
	case "off", "false", "0":
		h.client.SetDevCoreEnabled(false)
		return "✅ 已关闭 Dev Core"
	case "reset":
		h.client.ResetDevCorePrompt()
		h.client.SetDevCoreEnabled(false)
		return "✅ 已清空 Dev Core 提示词"
	case "set":
		if len(parts) < 3 {
			return "❌ 格式: /devcore set <提示词>"
		}
		prompt := strings.TrimSpace(strings.TrimPrefix(raw, parts[0]+" "+parts[1]))
		h.client.SetDevCorePrompt(prompt)
		h.client.SetDevCoreEnabled(true)
		return "✅ Dev Core 提示词已更新"
	default:
		prompt := strings.TrimSpace(strings.TrimPrefix(raw, parts[0]))
		if prompt == "" {
			return "❌ 格式错误\n\n/devcore <偏好>\n/devcore on|off\n/devcore set <提示词>\n/devcore reset"
		}
		h.client.SetDevCorePrompt(prompt)
		h.client.SetDevCoreEnabled(true)
		return "✅ Dev Core 偏好已设置"
	}
}

func (h *Handler) handleConfig(ctx context.Context, userID string) string {
	var sb strings.Builder
	sb.WriteString("⚙️ 当前配置:\n\n")

	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if ok {
		info, err := h.client.GetSessionInfo(ctx, sessionID)
		if err == nil {
			sb.WriteString("📊 会话信息:\n")
			sb.WriteString(fmt.Sprintf("  ID: %s\n", info.SessionID[:min(8, len(info.SessionID))]))
			sb.WriteString(fmt.Sprintf("  标题: %s\n", info.Title))
			sb.WriteString(fmt.Sprintf("  目录: %s\n", info.Directory))
			sb.WriteString(fmt.Sprintf("  消息数: %d\n", info.MessageCount))
			sb.WriteString(fmt.Sprintf("  Token: %d/%d\n", info.TokenCount, info.ContextLength))
		} else {
			sb.WriteString(fmt.Sprintf("📊 会话: %s\n", sessionID[:min(8, len(sessionID))]))
		}
	} else {
		sb.WriteString("📊 会话: 无活跃会话\n")
	}

	sb.WriteString("\n🔧 可用命令:\n")
	sb.WriteString("  /model    - 查看/设置模型\n")
	sb.WriteString("  /thinking - 查看/设置 thinking\n")
	sb.WriteString("  /final    - 查看/设置 final-only\n")
	sb.WriteString("  /steps    - 查看/设置步骤显示\n")
	sb.WriteString("  /devcore  - 查看/设置 Dev Core\n")
	sb.WriteString("  /help     - 查看全部帮助")

	return sb.String()
}

// --- Media extraction ---

// extractMediaAttachments scans message items for images/videos/files, downloads
// them from CDN (with AES-ECB decrypt), and returns attachments, auto-selected agent,
// and a list of file paths saved to the working directory.
func (h *Handler) extractMediaAttachments(ctx context.Context, msg *WeixinMessage) ([]opencode.Attachment, string, []string) {
	var attachments []opencode.Attachment
	var savedFiles []string
	var agentName string
	hasImage := false
	hasVideo := false

	for _, item := range msg.ItemList {
		switch item.Type {
		case ItemTypeImage:
			if item.ImageItem == nil {
				continue
			}
			att, err := h.downloadImageAttachment(item.ImageItem)
			if err != nil {
				log.Printf("wechat: failed to download image: %v", err)
				continue
			}
			attachments = append(attachments, att)
			hasImage = true

		case ItemTypeVideo:
			if item.VideoItem == nil {
				continue
			}
			att, err := h.downloadVideoAttachment(item.VideoItem)
			if err != nil {
				log.Printf("wechat: failed to download video: %v", err)
				// Try thumbnail as fallback
				if item.VideoItem.ThumbMedia != nil {
					thumbAtt, thumbErr := h.downloadCDNMediaAsAttachment(
						item.VideoItem.ThumbMedia, "", "image/jpeg", "wechat_video_thumb.jpg",
					)
					if thumbErr == nil {
						attachments = append(attachments, thumbAtt)
						hasImage = true
					}
				}
				continue
			}
			attachments = append(attachments, att)
			hasVideo = true

		case ItemTypeFile:
			if item.FileItem == nil {
				continue
			}
			filePath, err := h.downloadFileToWorkDir(ctx, item.FileItem)
			if err != nil {
				log.Printf("wechat: failed to download file '%s': %v", item.FileItem.FileName, err)
				continue
			}
			log.Printf("wechat: ✅ file saved to %s", filePath)
			savedFiles = append(savedFiles, filePath)
		}
	}

	// Auto-select agent for media
	if hasVideo {
		if skill := h.client.FindVideoSkill(ctx); skill != "" {
			agentName = skill
			log.Printf("wechat: auto-using video skill '%s'", agentName)
		}
	} else if hasImage && agentName == "" {
		// Images generally work with the default model if it supports vision
		if !h.client.HasImageCapableModel() {
			log.Printf("wechat: ⚠️ no image-capable model configured")
		}
	}

	return attachments, agentName, savedFiles
}

// downloadImageAttachment downloads and decrypts a WeChat image from CDN,
// returning it as a data-URI attachment.
func (h *Handler) downloadImageAttachment(img *ImageItem) (opencode.Attachment, error) {
	media := img.Media
	if media == nil {
		return opencode.Attachment{}, fmt.Errorf("image has no media")
	}

	// Determine AES key. Priority: image_item.aeskey (hex) > media.aes_key (base64)
	aesKeyBase64 := ""
	if img.AESKey != "" {
		// image_item.aeskey is hex-encoded; convert to base64 for parseAesKey
		aesKeyBase64 = hexToBase64(img.AESKey)
	} else if media.AESKey != "" {
		aesKeyBase64 = media.AESKey
	}

	return h.downloadCDNMediaAsAttachment(media, aesKeyBase64, "image/jpeg", "wechat_image.jpg")
}

// downloadVideoAttachment downloads and decrypts a WeChat video from CDN.
func (h *Handler) downloadVideoAttachment(vid *VideoItem) (opencode.Attachment, error) {
	media := vid.Media
	if media == nil {
		return opencode.Attachment{}, fmt.Errorf("video has no media")
	}
	aesKeyBase64 := media.AESKey
	return h.downloadCDNMediaAsAttachment(media, aesKeyBase64, "video/mp4", "wechat_video.mp4")
}

// downloadFileToWorkDir downloads and decrypts a file from CDN and saves it
// to the OpenCode working directory so the model can access it directly.
func (h *Handler) downloadFileToWorkDir(ctx context.Context, fi *FileItem) (string, error) {
	media := fi.Media
	if media == nil {
		return "", fmt.Errorf("file has no media")
	}
	aesKeyBase64 := media.AESKey
	if aesKeyBase64 == "" {
		return "", fmt.Errorf("file has no AES key")
	}

	data, err := h.downloadAndDecryptCDN(media, aesKeyBase64)
	if err != nil {
		return "", err
	}

	// Save to the OpenCode working directory
	workDir := h.client.Directory()
	filename := fi.FileName
	if filename == "" {
		filename = "wechat_file"
	}
	destPath := filepath.Join(workDir, filename)
	if err := os.WriteFile(destPath, data, 0644); err != nil {
		return "", fmt.Errorf("write file: %w", err)
	}
	return destPath, nil
}

// downloadCDNMediaAsAttachment downloads (and optionally decrypts) a CDN media
// item and returns it as a data-URI attachment.
func (h *Handler) downloadCDNMediaAsAttachment(media *CDNMedia, aesKeyBase64, defaultMime, filename string) (opencode.Attachment, error) {
	var data []byte
	var err error
	if aesKeyBase64 != "" {
		data, err = h.downloadAndDecryptCDN(media, aesKeyBase64)
	} else {
		data, err = h.downloadPlainCDN(media)
	}
	if err != nil {
		return opencode.Attachment{}, err
	}

	mime := http.DetectContentType(data)
	if mime == "application/octet-stream" {
		mime = defaultMime
	}

	encoded := base64.StdEncoding.EncodeToString(data)
	dataURI := fmt.Sprintf("data:%s;base64,%s", mime, encoded)
	log.Printf("wechat: ✅ media downloaded (mime=%s, size=%d, encrypted=%v)", mime, len(data), aesKeyBase64 != "")
	return opencode.Attachment{
		Mime:     mime,
		URL:      dataURI,
		Filename: filename,
	}, nil
}

// downloadAndDecryptCDN fetches bytes from CDN and decrypts with AES-128-ECB.
func (h *Handler) downloadAndDecryptCDN(media *CDNMedia, aesKeyBase64 string) ([]byte, error) {
	key, err := parseAesKey(aesKeyBase64)
	if err != nil {
		return nil, fmt.Errorf("parse AES key: %w", err)
	}

	raw, err := h.fetchCDNBytes(media)
	if err != nil {
		return nil, err
	}

	decrypted, err := decryptAesEcb(raw, key)
	if err != nil {
		return nil, fmt.Errorf("AES decrypt: %w", err)
	}
	log.Printf("wechat: CDN decrypted %d → %d bytes", len(raw), len(decrypted))
	return decrypted, nil
}

// downloadPlainCDN fetches raw (unencrypted) bytes from CDN.
func (h *Handler) downloadPlainCDN(media *CDNMedia) ([]byte, error) {
	return h.fetchCDNBytes(media)
}

// fetchCDNBytes downloads raw bytes from the CDN URL.
func (h *Handler) fetchCDNBytes(media *CDNMedia) ([]byte, error) {
	url := media.FullURL
	if url == "" && media.EncryptQueryParam != "" && h.cfg.CDNBaseURL != "" {
		url = fmt.Sprintf("%s/download?encrypted_query_param=%s",
			h.cfg.CDNBaseURL, media.EncryptQueryParam)
	}
	if url == "" {
		return nil, fmt.Errorf("no CDN URL available")
	}

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("CDN fetch: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("CDN status %d", resp.StatusCode)
	}

	// Limit to 100MB (matching Tencent SDK)
	data, err := io.ReadAll(io.LimitReader(resp.Body, 100*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("CDN read body: %w", err)
	}
	return data, nil
}

// handleQuickReply tries to interpret non-command text as an answer to a
// pending permission or question (mirrors dingtalk's handleQuickReply).
func (h *Handler) handleQuickReply(ctx context.Context, userID, content, ctxToken string) (string, bool) {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return "", false
	}

	log.Printf("wechat: checking quick reply '%s' for user %s (session: %s)", content, userID, sessionID[:min(8, len(sessionID))])

	permission, hasP := h.client.GetLatestPendingPermission(sessionID)
	question, hasQ := h.client.GetLatestPendingQuestion(sessionID)

	// If both exist, prefer the newer one
	preferQuestion := false
	if hasP && hasQ && question.CreatedAt.After(permission.CreatedAt) {
		preferQuestion = true
	}

	// Handle permission reply
	if hasP && !preferQuestion {
		resp := replyToPermissionResponse(content)
		if resp == "" {
			if hasQ {
				log.Printf("wechat: permission parse miss, falling back to question")
			} else {
				return "", false // not a recognizable reply, let normal flow handle
			}
		} else {
			log.Printf("wechat: permission reply '%s' -> %s for %s", content, resp, permission.ID)
			if err := h.client.RespondToPermission(ctx, permission.ID, resp); err != nil {
				log.Printf("wechat: RespondToPermission failed: %v", err)
				return fmt.Sprintf("❌ 权限回复失败: %v", err), true
			}

			// For "always" / "reject", cascade the same answer to ALL pending
			// permissions for this session. A single user reply should clear all
			// stacked permissions so the user doesn't have to reply N times.
			// "once" is intentionally not cascaded — it means "just this one".
			if resp == "always" || resp == "reject" {
				allIDs := h.client.GetAllPendingPermissionIDs(sessionID)
				for _, id := range allIDs {
					if id == permission.ID {
						continue // already answered above
					}
					log.Printf("wechat: cascading permission response '%s' to %s", resp, id)
					if err := h.client.RespondToPermission(ctx, id, resp); err != nil {
						log.Printf("wechat: cascade permission %s failed: %v", id, err)
					}
				}
			}

			displayMap := map[string]string{"once": "允许", "reject": "拒绝", "always": "始终允许"}
			return fmt.Sprintf("✅ 已回复: %s\n⏳ 等待继续执行...", displayMap[resp]), true
		}
	}

	// Handle question reply
	if hasQ {
		answer := content
		// Numeric input → convert to option label
		if idx, err := strconv.Atoi(strings.TrimSpace(content)); err == nil {
			if len(question.Questions) > 0 && len(question.Questions[0].Options) > 0 {
				opts := question.Questions[0].Options
				if idx >= 1 && idx <= len(opts) {
					answer = opts[idx-1].Label
					log.Printf("wechat: converted %d -> %s", idx, answer)
				}
			} else if len(question.Options) > 0 {
				if idx >= 1 && idx <= len(question.Options) {
					answer = question.Options[idx-1]
					log.Printf("wechat: converted %d -> %s", idx, answer)
				}
			}
		}

		// ★ Validate the answer before consuming the question. If the user's
		// input doesn't match any option, it's likely an unrelated message —
		// don't pollute the question with a wrong answer.
		if !question.IsValidAnswer(content) {
			log.Printf("wechat: input '%s' is not a valid answer for question %s (opts=%d), ignoring",
				content, question.ID, len(question.Options))
			return fmt.Sprintf("⚠️ 回复未能匹配问题选项，请回复选项编号或关键词（如 %s）", formatQuestionOptions(question)), true
		}

		log.Printf("wechat: answering question %s with '%s'", question.ID, answer)
		if err := h.client.AnswerQuestion(ctx, question.ID, answer); err != nil {
			log.Printf("wechat: AnswerQuestion failed: %v", err)
			return fmt.Sprintf("❌ 回复失败: %v", err), true
		}
		return fmt.Sprintf("✅ 已回复: %s\n⏳ 等待继续执行...", answer), true
	}

	return "", false
}

// replyToPermissionResponse maps user input to the API permission response value.
// When the inbound batcher merges multiple messages separated by \n, we split
// and try each line, returning the first match.
func replyToPermissionResponse(input string) string {
	for _, line := range strings.Split(input, "\n") {
		s := strings.TrimSpace(line)
		switch s {
		case "允许", "allow", "yes", "y", "是", "1":
			return "once"
		case "拒绝", "deny", "reject", "no", "n", "否", "2":
			return "reject"
		case "始终允许", "always", "始终", "3":
			return "always"
		}
	}
	return ""
}

// formatQuestionOptions returns a concise summary of the question's valid
// options for user-facing error messages.
func formatQuestionOptions(q *opencode.Question) string {
	var parts []string
	if len(q.Questions) > 0 && len(q.Questions[0].Options) > 0 {
		for i, opt := range q.Questions[0].Options {
			parts = append(parts, fmt.Sprintf("%d=%s", i+1, opt.Label))
		}
	} else if len(q.Options) > 0 {
		for i, opt := range q.Options {
			parts = append(parts, fmt.Sprintf("%d=%s", i+1, opt))
		}
	}
	if len(parts) == 0 {
		return "输入任意文字回复"
	}
	return strings.Join(parts, ", ")
}

const maxWechatTextLength = defaultOutboundMaxLen

// sendTextChunks sends text using Hermes-style message chunking via the
// per-user serialized sender. Pacing and rate-limit backoff are handled
// inside the sender so callers may invoke this concurrently from different
// signal paths (content / tool / step / todo / question / proactive).
//
// On partial delivery (some chunks succeeded, then a chunk failed) the caller
// MUST treat the whole call as failed and not advance its cursor; otherwise
// the un-delivered tail would be lost. The sender stops at the first failure
// to make this contract simple.
func (h *Handler) sendTextChunks(userID, text, ctxToken string) error {
	// Unified throttling: all regular text paths enter the durable outbound
	// queue and are paced by the ticker dispatcher.
	return h.enqueueAsyncText(userID, "", ctxToken, "generic", text, true)
}

// sendTextChunksDirect is now a thin wrapper around enqueueAsyncText so that
// all outbound text goes through the durable queue for ordering and pacing.
// When the queue is not initialized, enqueueAsyncText falls back to
// sendTextInline which applies senderRegistry throttling directly.
func (h *Handler) sendTextChunksDirect(userID, text, ctxToken string) error {
	return h.enqueueAsyncText(userID, "", ctxToken, "generic", text, true)
}

// sendTextInline sends text directly via senderRegistry without queueing.
// Used only as a fallback when the outbound queue is not initialized.
func (h *Handler) sendTextInline(userID, text, ctxToken string) error {
	chunks := splitTextForWeixinDelivery(text, maxWechatTextLength, false)
	if len(chunks) == 0 {
		return nil
	}
	effectiveToken := ctxToken
	delivered, err := h.sender.sendChunks(userID, chunks, func(chunk string) error {
		h.trackSentText(userID, chunk)
		sendErr := h.weClient.SendText(userID, chunk, effectiveToken)
		if sendErr != nil && errors.Is(sendErr, ErrSessionExpired) && effectiveToken != "" {
			log.Printf("wechat: session expired user=%s; clearing context_token and retrying tokenless", userID)
			h.mu.Lock()
			delete(h.lastContextTokens, userID)
			h.mu.Unlock()
			effectiveToken = ""
			h.trackSentText(userID, chunk)
			sendErr = h.weClient.SendText(userID, chunk, "")
		}
		return sendErr
	})
	if err != nil {
		log.Printf("wechat: inline chunk send failed user=%s delivered=%d/%d: %v", userID, delivered, len(chunks), err)
	}
	return err
}

// --- Outbound Media Sending ---

// cmdExecTimeout bounds how long a /cmd shell execution may run before it is
// aborted. The underlying SDK Session.Shell() call has no timeout of its own.
const cmdExecTimeout = 5 * time.Minute

// dsmlBlockPattern matches a complete leaked DSML tool-call block, including
// the markup tags AND the payload between them, e.g.
//
//	<DSML｜tool_calls> … </DSML｜tool_calls>
//
// These arrive as plain text deltas (not tool/step signal parts), so the
// streaming signal filters never catch them. Tag names are variable
// (tool_calls, function_calls, …) and the separator may be an ASCII '|' or the
// fullwidth '｜' (U+FF5C); both are accepted. (?s) lets '.' span newlines.
var dsmlBlockPattern = regexp.MustCompile(`(?s)<DSML[|｜][^>]*>.*?</DSML[|｜][^>]*>`)

// dsmlTagPattern matches any leftover standalone DSML tag (an open or close
// tag whose pair was not present, e.g. a block split across a flush boundary).
var dsmlTagPattern = regexp.MustCompile(`</?DSML[|｜][^>]*>`)

// stripDSMLMarkup removes leaked DSML tool-call blocks (tags + payload) from
// assembled reply content so the raw XML-like markup never reaches the user.
func stripDSMLMarkup(content string) string {
	if !strings.Contains(content, "DSML") {
		return content
	}
	cleaned := dsmlBlockPattern.ReplaceAllString(content, "")
	cleaned = dsmlTagPattern.ReplaceAllString(cleaned, "")
	return cleaned
}

// mediaDirectivePattern matches MEDIA: path tags in text.
// Go's regexp (RE2) does not support (?=...) lookahead, so we match conservatively
// and validate/clean paths in Go code.
var mediaDirectivePattern = regexp.MustCompile(`MEDIA:\s*(\S+\.(?:png|jpe?g|gif|webp|mp4|mov|avi|mkv|webm|ogg|opus|mp3|wav|m4a|flac|epub|pdf|zip|rar|7z|docx?|xlsx?|pptx?|txt|csv|apk|ipa))`)

// extractMediaDirective extracts MEDIA: path tags from text content.
func extractMediaDirective(content string) (paths []string, cleaned string) {
	cleaned = content
	if !strings.Contains(content, "MEDIA:") {
		return nil, content
	}

	// Remove [[audio_as_voice]] and [[as_document]] directives
	cleaned = strings.ReplaceAll(cleaned, "[[audio_as_voice]]", "")
	cleaned = strings.ReplaceAll(cleaned, "[[as_document]]", "")

	for _, match := range mediaDirectivePattern.FindAllStringSubmatch(content, -1) {
		if len(match) < 2 {
			continue
		}
		path := strings.TrimSpace(match[1])
		if path == "" {
			continue
		}
		// Strip surrounding quote/backtick/' wrappers
		if len(path) >= 2 && (path[0] == path[len(path)-1]) && (path[0] == '`' || path[0] == '"' || path[0] == '\'') {
			path = path[1 : len(path)-1]
		}
		// Clean trailing punctuation/delimiters that the simpler regex may have captured
		path = strings.TrimRight(path, "`\"',.;:)}] ")
		path = strings.TrimLeft(path, "`\"'")
		if path == "" || !strings.HasPrefix(path, "/") && !strings.HasPrefix(path, "~/") {
			continue
		}
		// Expand ~/ to home directory
		if strings.HasPrefix(path, "~/") {
			home, _ := os.UserHomeDir()
			path = filepath.Join(home, path[2:])
		}
		paths = append(paths, path)
	}

	if len(paths) > 0 {
		cleaned = mediaDirectivePattern.ReplaceAllString(cleaned, "")
		// Collapse multiple newlines
		cleaned = regexp.MustCompile(`\n{3,}`).ReplaceAllString(cleaned, "\n\n")
		cleaned = strings.TrimSpace(cleaned)
	}
	return
}

// sendMediaFile uploads a local file to WeChat CDN and sends it as a media message.
func (h *Handler) sendMediaFile(userID, filePath, caption, ctxToken string) error {
	if h.weClient == nil {
		return fmt.Errorf("wechat client not initialized")
	}

	plaintext, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	mediaType, itemBuilder := outboundMediaBuilder(filePath, false)

	filekeyBytes := make([]byte, 16)
	if _, err := rand.Read(filekeyBytes); err != nil {
		return fmt.Errorf("generate filekey: %w", err)
	}
	filekey := fmt.Sprintf("%x", filekeyBytes)

	aesKey := make([]byte, 16)
	if _, err := rand.Read(aesKey); err != nil {
		return fmt.Errorf("generate aes key: %w", err)
	}

	rawSize := len(plaintext)
	rawFileMD5 := fmt.Sprintf("%x", md5.Sum(plaintext))
	fileSize := aesPaddedSize(rawSize)
	aesKeyHex := fmt.Sprintf("%x", aesKey)

	uploadResp, err := h.weClient.GetUploadURL(userID, mediaType, filekey, aesKeyHex, rawSize, rawFileMD5, fileSize)
	if err != nil {
		return fmt.Errorf("get upload url: %w", err)
	}

	ciphertext, err := encryptAesEcb(plaintext, aesKey)
	if err != nil {
		return fmt.Errorf("aes encrypt: %w", err)
	}

	uploadURL := uploadResp.UploadFullURL
	if uploadURL == "" && uploadResp.UploadParam != "" {
		cdnBase := h.cfg.CDNBaseURL
		if cdnBase == "" {
			cdnBase = "https://novac2c.cdn.weixin.qq.com/c2c"
		}
		uploadURL = fmt.Sprintf("%s/upload?encrypted_query_param=%s&filekey=%s",
			cdnBase, uploadResp.UploadParam, filekey)
	}
	if uploadURL == "" {
		return fmt.Errorf("getUploadUrl returned neither upload_full_url nor upload_param")
	}

	encryptedQueryParam, err := h.weClient.UploadCiphertext(uploadURL, ciphertext)
	if err != nil {
		return fmt.Errorf("cdn upload: %w", err)
	}

	// The iLink API expects aes_key as base64(hex_string), not base64(raw_bytes).
	aesKeyForAPI := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%x", aesKey)))

	itemKwargs := mediaItemArgs{
		encryptQueryParam: encryptedQueryParam,
		aesKeyForAPI:      aesKeyForAPI,
		ciphertextSize:    len(ciphertext),
		plaintextSize:     rawSize,
		filename:          filepath.Base(filePath),
		rawFileMD5:        rawFileMD5,
	}
	if mediaType == UploadMediaTypeVoice && strings.HasSuffix(filePath, ".silk") {
		itemKwargs.encodeType = 6
		itemKwargs.sampleRate = 24000
		itemKwargs.bitsPerSample = 16
	}
	mediaItem := itemBuilder(itemKwargs)

	// Send caption text first (if any)
	if caption != "" {
		if err := h.sendTextChunks(userID, caption, ctxToken); err != nil {
			log.Printf("wechat: caption send before media failed: %v", err)
		}
	}

	// Send media message
	msg := &WeixinMessage{
		Seq:          int(nextSeq()),
		ToUserID:     userID,
		ClientID:     generateClientID(),
		ContextToken: ctxToken,
		MessageType:  MessageTypeBot,
		MessageState: MessageStateFinish,
		ItemList:     []MessageItem{mediaItem},
	}
	return h.weClient.SendWeixinMessage(msg)
}

type mediaItemArgs struct {
	encryptQueryParam string
	aesKeyForAPI      string
	ciphertextSize    int
	plaintextSize     int
	filename          string
	rawFileMD5        string
	encodeType        int
	sampleRate        int
	bitsPerSample     int
}

// outboundMediaBuilder returns upload media type and item builder function based on file extension.
func outboundMediaBuilder(path string, forceFileAttachment bool) (int, func(mediaItemArgs) MessageItem) {
	ext := strings.ToLower(filepath.Ext(path))
	mt := mime.TypeByExtension(ext)
	if mt == "" {
		mt = "application/octet-stream"
	}

	if strings.HasPrefix(mt, "image/") && !forceFileAttachment {
		return UploadMediaTypeImage, func(kw mediaItemArgs) MessageItem {
			return MessageItem{
				Type: ItemTypeImage,
				ImageItem: &ImageItem{
					Media: &CDNMedia{
						EncryptQueryParam: kw.encryptQueryParam,
						AESKey:            kw.aesKeyForAPI,
						EncryptType:       1,
					},
					MidSize: kw.ciphertextSize,
				},
			}
		}
	}

	if strings.HasPrefix(mt, "video/") {
		return UploadMediaTypeVideo, func(kw mediaItemArgs) MessageItem {
			return MessageItem{
				Type: ItemTypeVideo,
				VideoItem: &VideoItem{
					Media: &CDNMedia{
						EncryptQueryParam: kw.encryptQueryParam,
						AESKey:            kw.aesKeyForAPI,
						EncryptType:       1,
					},
					VideoSize:  kw.ciphertextSize,
					PlayLength: 0,
					VideoMD5:   kw.rawFileMD5,
				},
			}
		}
	}

	if ext == ".silk" && !forceFileAttachment {
		return UploadMediaTypeVoice, func(kw mediaItemArgs) MessageItem {
			return MessageItem{
				Type: ItemTypeVoice,
				VoiceItem: &VoiceItem{
					Media: &CDNMedia{
						EncryptQueryParam: kw.encryptQueryParam,
						AESKey:            kw.aesKeyForAPI,
						EncryptType:       1,
					},
					EncodeType:   kw.encodeType,
					BitsPerSampl: kw.bitsPerSample,
					SampleRate:   kw.sampleRate,
				},
			}
		}
	}

	if strings.HasPrefix(mt, "audio/") {
		return UploadMediaTypeFile, func(kw mediaItemArgs) MessageItem {
			return MessageItem{
				Type: ItemTypeFile,
				FileItem: &FileItem{
					Media: &CDNMedia{
						EncryptQueryParam: kw.encryptQueryParam,
						AESKey:            kw.aesKeyForAPI,
						EncryptType:       1,
					},
					FileName: kw.filename,
					Len:      strconv.Itoa(kw.plaintextSize),
				},
			}
		}
	}

	// Default: send as file attachment
	return UploadMediaTypeFile, func(kw mediaItemArgs) MessageItem {
		return MessageItem{
			Type: ItemTypeFile,
			FileItem: &FileItem{
				Media: &CDNMedia{
					EncryptQueryParam: kw.encryptQueryParam,
					AESKey:            kw.aesKeyForAPI,
					EncryptType:       1,
				},
				FileName: kw.filename,
				Len:      strconv.Itoa(kw.plaintextSize),
			},
		}
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
