package dingtalk

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	nls "github.com/aliyun/alibabacloud-nls-go-sdk"
	"github.com/open-dingtalk/dingtalk-stream-sdk-go/chatbot"
	dtclient "github.com/open-dingtalk/dingtalk-stream-sdk-go/client"
	"github.com/user/opencode-gateway/internal/adapters/base"
	"github.com/user/opencode-gateway/internal/memstore"
	"github.com/user/opencode-gateway/internal/opencode"
	"github.com/user/opencode-gateway/internal/retryworker"
	"github.com/user/opencode-gateway/internal/scheduler"
)

// pictureContent 图片消息内容
type pictureContent struct {
	DownloadCode        string `json:"downloadCode"`
	PictureDownloadCode string `json:"pictureDownloadCode"`
}

// flexInt 可以接受 JSON 数字或字符串形式的整数（钉钉 API 有时返回 "3" 也有时返回 3）
type flexInt int

func (f *flexInt) UnmarshalJSON(b []byte) error {
	// 去掉引号后当成 int 解析
	s := strings.Trim(string(b), `"`)
	n, err := strconv.Atoi(s)
	if err != nil {
		return err
	}
	*f = flexInt(n)
	return nil
}

// audioContent 语音消息内容
type audioContent struct {
	DownloadCode string  `json:"downloadCode"`
	Duration     flexInt `json:"duration"`    // 毫秒，钉钉可能返回字符串或数字
	Format       string  `json:"format"`      // 音频格式，如 "amr"
	SampleRate   flexInt `json:"sampleRate"`  // 采样率，钉钉 AMR 为 8000
	Recognition  string  `json:"recognition"` // 钉钉服务端已识别的文字（直接可用）
}

// videoContent 视频消息内容
type videoContent struct {
	DownloadCode string  `json:"downloadCode"`
	Duration     flexInt `json:"duration"` // 毫秒，钉钉可能返回字符串或数字
	VideoType    string  `json:"videoType"`
}

// fileContent 文件消息内容
type fileContent struct {
	DownloadCode string  `json:"downloadCode"`
	FileName     string  `json:"fileName"`
	FileType     string  `json:"fileType"`
	FileSize     flexInt `json:"fileSize"`
}

// richTextItem 图文消息中的单个元素
type richTextItem struct {
	Type                string `json:"type"`                          // "text", "picture", "video", "file" 等
	Text                string `json:"text,omitempty"`                // type=text 时有值
	DownloadCode        string `json:"downloadCode,omitempty"`        // type=picture/video/file 时有值
	PictureDownloadCode string `json:"pictureDownloadCode,omitempty"` // type=picture 时有值（旧版API专用）
	VideoDownloadCode   string `json:"videoDownloadCode,omitempty"`   // type=video 时可能的字段
	FileDownloadCode    string `json:"fileDownloadCode,omitempty"`    // type=file 时可能的字段
	FileName            string `json:"fileName,omitempty"`            // 文件名
	Duration            int    `json:"duration,omitempty"`            // 视频时长（毫秒）
	Size                int64  `json:"size,omitempty"`                // 文件大小
}

// richTextContent 图文混合消息内容
type richTextContent struct {
	RichText []richTextItem `json:"richText"`
}

const (
	// MessageDeduplicationWindow 消息去重时间窗口
	MessageDeduplicationWindow = 5 * time.Minute
	// MessageDeduplicationCleanupInterval 去重缓存清理间隔
	MessageDeduplicationCleanupInterval = 10 * time.Minute
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
	AutoAnswer        bool // 是否自动回答问题（选择首选选项）
	UserWhitelist     []string
	OwnerUserID       string
	NonOwnerPlanMode  bool // 非owner用户是否自动使用plan模式
	// 阿里云 NLS 语音识别配置（可选，不填则语音消息以占位文本转发）
	AliyunNLSAkID   string // 阿里云 AccessKey ID
	AliyunNLSAkKey  string // 阿里云 AccessKey Secret
	AliyunNLSAppKey string // NLS 控制台 AppKey
}

// Handler processes DingTalk callbacks and proxies them to OpenCode.
// Supports both Stream mode and traditional Webhook mode.
type Handler struct {
	client          *opencode.Client
	cfg             Config
	adapter         *base.BidirectionalAdapter
	streamClient    *dtclient.StreamClient
	cronScheduler   *scheduler.CronScheduler // 定时任务调度器
	nlScheduleSvc   *scheduler.NLScheduleService
	processedMsgIDs sync.Map  // map[string]time.Time - 已处理的消息ID及其时间戳
	cleanupOnce     sync.Once // 确保清理goroutine只启动一次
	overflowPolicy  sync.Map  // map[userID]string, token超限恢复策略
	overflowPending sync.Map  // map[userID]*tokenOverflowPendingState, 待处理的token超限恢复
	// access token 缓存（避免每次都获取）
	accessToken       string
	accessTokenExpiry time.Time
	accessTokenMu     sync.Mutex
	allowedUserSet    map[string]struct{}
	whitelistMu       sync.RWMutex
	// retryStore and retryWorker support the /retry command and off-peak retry queue.
	retryStore  *memstore.Store
	retryWorker *retryworker.Worker
}

const (
	tokenOverflowPolicyAsk     = "ask"
	tokenOverflowPolicySummary = "summary"
	tokenOverflowPolicyNew     = "new"
)

type tokenOverflowPendingState struct {
	SessionID        string
	ThreadID         string
	UserID           string
	Agent            string
	Content          string
	Attachments      []opencode.Attachment
	Metadata         map[string]string
	SessionWebhook   string
	ConversationType string
	CreatedAt        time.Time
	Executing        bool // 标记 executeTokenOverflowDecision 是否已在执行中
}

// NewHandler wires the adapter with an OpenCode client.
func NewHandler(ocClient *opencode.Client, cfg Config) *Handler {
	h := &Handler{
		client:         ocClient,
		cfg:            cfg,
		allowedUserSet: make(map[string]struct{}),
	}

	for _, uid := range cfg.UserWhitelist {
		normalized := strings.TrimSpace(uid)
		if normalized == "" {
			continue
		}
		h.allowedUserSet[normalized] = struct{}{}
	}

	ownerUserID := strings.TrimSpace(cfg.OwnerUserID)
	if ownerUserID != "" {
		h.allowedUserSet[ownerUserID] = struct{}{}
	}
	if len(h.allowedUserSet) > 0 {
		log.Printf("dingtalk: user whitelist enabled (%d users)", len(h.allowedUserSet))
	}

	h.adapter = base.NewBidirectionalAdapter("dingtalk", h)

	// Register a stuck session hook for logging. DingTalk doesn't have an async
	// push channel (messages require a sessionWebhook from an inbound message),
	// so the user will see the stuck diagnosis via /status instead.
	h.client.SetStuckSessionHook(func(parentSessionID, childSessionID, reason string) {
		if childSessionID != "" {
			log.Printf("dingtalk: ⚠️ stuck child session %s (parent=%s): %s",
				childSessionID[:min(8, len(childSessionID))],
				parentSessionID[:min(8, len(parentSessionID))], reason)
		} else {
			log.Printf("dingtalk: ⚠️ stuck session %s: %s",
				parentSessionID[:min(8, len(parentSessionID))], reason)
		}
	})

	return h
}

// SetCronScheduler 设置定时任务调度器
func (h *Handler) SetCronScheduler(cronScheduler *scheduler.CronScheduler) {
	h.cronScheduler = cronScheduler
}

// SetRetryWorker wires up the retry store and worker for /retry command and
// off-peak automatic re-processing.
func (h *Handler) SetRetryWorker(store *memstore.Store, worker *retryworker.Worker) {
	h.retryStore = store
	h.retryWorker = worker
}

// AdapterName implements retryworker.RetrySender.
func (h *Handler) AdapterName() string { return "dingtalk" }

// SendRetryMessage implements retryworker.RetrySender.
func (h *Handler) SendRetryMessage(ctx context.Context, r memstore.PendingRetry) (string, error) {
	payload := retryworker.BuildOpenCodePayload(r)
	payload.Streaming = false
	resp, err := h.client.SendMessage(ctx, payload)
	if err != nil {
		return "", err
	}
	return resp.Reply, nil
}

// NotifyUser implements retryworker.RetrySender.
func (h *Handler) NotifyUser(ctx context.Context, r memstore.PendingRetry, reply string) error {
	return h.adapter.SendToUserInChannel(ctx, r.Channel, r.UserID, reply)
}

// SetNLScheduleService 设置自然语言定时任务服务
func (h *Handler) SetNLScheduleService(svc *scheduler.NLScheduleService) {
	h.nlScheduleSvc = svc
}

// RegisterCronSession 注册定时任务session到adapter，使SSE事件能正确路由
// 实现 scheduler.SessionRegistrar 接口
func (h *Handler) RegisterCronSession(sessionID string, metadata map[string]interface{}) {
	cronUserID := fmt.Sprintf("cron:%s", sessionID[:min(12, len(sessionID))])
	h.adapter.MapUserToSession(cronUserID, sessionID)

	// 存储webhook URL用于发送消息
	if webhook, ok := metadata["session_webhook"].(string); ok && webhook != "" {
		h.adapter.MapSessionData(sessionID, "channel", webhook)
	}

	log.Printf("dingtalk: registered cron session %s (cronUser=%s)", sessionID[:min(8, len(sessionID))], cronUserID)
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
	log.Printf("dingtalk: using ClientID: %s...", h.cfg.ClientID[:min(20, len(h.cfg.ClientID))])

	// Create Stream client. Use SDK router registration like the working check script.
	h.streamClient = dtclient.NewStreamClient(
		dtclient.WithAppCredential(
			dtclient.NewAppCredentialConfig(h.cfg.ClientID, h.cfg.ClientSecret),
		),
	)
	h.streamClient.RegisterChatBotCallbackRouter(h.onChatBotMessageReceived)

	log.Println("dingtalk: starting Stream client connection...")
	if err := h.streamClient.Start(ctx); err != nil {
		return fmt.Errorf("dingtalk: failed to start Stream client: %w", err)
	}

	log.Println("dingtalk: Stream client connected successfully")
	return nil
}

// Stop closes the Stream client if running.
func (h *Handler) Stop() {
	if h.streamClient != nil {
		h.streamClient.Close()
		log.Println("dingtalk: Stream client closed")
	}
}

// cleanupProcessedMessages 定期清理过期的消息ID记录
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
			log.Printf("dingtalk: cleaned up %d expired message IDs from deduplication cache", cleanedCount)
		}
	}
}

// onChatBotMessageReceived handles incoming messages from DingTalk Stream.
func (h *Handler) onChatBotMessageReceived(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
	// 启动清理goroutine（只启动一次）
	h.cleanupOnce.Do(func() {
		go h.cleanupProcessedMessages()
	})

	// 消息去重检查：使用钉钉的MsgId进行去重
	msgID := data.MsgId
	if msgID != "" {
		if _, exists := h.processedMsgIDs.Load(msgID); exists {
			log.Printf("dingtalk stream: duplicate message detected, msgId=%s, ignoring", msgID)
			return nil, nil // 静默忽略重复消息
		}
		// 标记消息为已处理
		h.processedMsgIDs.Store(msgID, time.Now())
		log.Printf("dingtalk stream: processing message msgId=%s", msgID)
	}

	content := strings.TrimSpace(data.Text.Content)
	senderStaffID := strings.TrimSpace(data.SenderStaffId)
	senderID := strings.TrimSpace(data.SenderId)
	userID := senderStaffID
	if userID == "" {
		userID = senderID
	}
	conversationID := data.ConversationId

	if !h.isUserAllowedAny(senderStaffID, senderID) {
		log.Printf("dingtalk stream: blocked user by whitelist (senderStaffId=%s, senderId=%s, resolved=%s)", senderStaffID, senderID, userID)
		replier := chatbot.NewChatbotReplier()
		//ownerUserID := h.currentOwnerUserID()
		msg := fmt.Sprintf("❌ 当前机器人未对您开放（staffId=%s, senderId=%s），请联系机器人主人开通权限。", senderStaffID, senderID)
		// if ownerUserID != "" {
		// 	msg = fmt.Sprintf("❌ 当前机器人未对您开放（您的userID: %s），请联系机器人主人（%s）开通权限。", userID, data.SenderNick)
		// }
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	if userID == "" {
		log.Printf("dingtalk stream: cannot resolve sender user id (senderStaffId=%s, senderId=%s)", senderStaffID, senderID)
		replier := chatbot.NewChatbotReplier()
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 无法识别您的用户标识（staffId/senderId 都为空），请联系管理员检查机器人权限配置。"))
		return nil, nil
	}

	// 处理不同类型的消息
	msgType := data.Msgtype
	var mediaInfo map[string]interface{}
	var extraAttachments []opencode.Attachment // 用于 richText 等多附件场景
	var mediaFiles []base.MediaFileRecord
	var videoSkillName string // 视频处理 skill 名称
	mediaSessionID := "new"
	if existingSessionID, ok := h.adapter.GetSessionForUser(userID); ok && strings.TrimSpace(existingSessionID) != "" {
		mediaSessionID = strings.TrimSpace(existingSessionID)
	}

	log.Printf("dingtalk stream: 🔍 [DEBUG] Message received:")
	log.Printf("  - MsgID: %s", msgID)
	log.Printf("  - UserID: %s", userID)
	log.Printf("  - ConversationID: %s", conversationID)
	log.Printf("  - MsgType: %s", msgType)
	log.Printf("  - Text.Content: %s", data.Text.Content)
	log.Printf("  - Content (interface{}): %+v", data.Content)
	log.Printf("  - SenderNick: %s", data.SenderNick)

	switch msgType {
	case "", "text":
		// 文本消息（Msgtype 为空时也是文本）
		log.Printf("dingtalk stream: 📝 [TEXT] received from %s: %s", userID, content)

	case "picture":
		// 图片消息
		log.Printf("dingtalk stream: 🖼️ [PICTURE] received from %s", userID)
		var picContent pictureContent
		if data.Content != nil {
			contentBytes, _ := json.Marshal(data.Content)
			log.Printf("  - Picture content JSON: %s", string(contentBytes))
			if err := json.Unmarshal(contentBytes, &picContent); err != nil {
				log.Printf("  - ⚠️ Failed to parse picture content: %v", err)
			} else {
				log.Printf("  - DownloadCode: %s", picContent.DownloadCode)
				log.Printf("  - PictureDownloadCode: %s", picContent.PictureDownloadCode)
				// 新版 v1.0 API 使用 downloadCode；pictureDownloadCode 是旧版 oapi 专用
				picCode := picContent.DownloadCode
				if picCode == "" {
					picCode = picContent.PictureDownloadCode
				}
				dataURI, mime, err := h.downloadMediaAsDataURI(ctx, picCode, "image/jpeg")
				if err != nil && picContent.PictureDownloadCode != "" && picCode != picContent.PictureDownloadCode {
					log.Printf("  - ↩️ Retrying with pictureDownloadCode after error: %v", err)
					dataURI, mime, err = h.downloadMediaAsDataURI(ctx, picContent.PictureDownloadCode, "image/jpeg")
				}
				if err != nil {
					log.Printf("  - ⚠️ Failed to download image: %v", err)
				} else {
					log.Printf("  - ✅ Image downloaded as data URI (mime=%s, len=%d)", mime, len(dataURI))
					mediaInfo = map[string]interface{}{
						"type": "image",
						"url":  dataURI,
						"mime": mime,
					}
				}
			}
		}
		// 图片消息：构建合理的提示文本
		// 如果有用户输入的文字就用文字，否则给模型一个识别图片的提示
		content = ""
		if data.Text.Content != "" {
			content = data.Text.Content
		}
		// 如果用户没有提供文字说明，给模型一个默认提示
		if content == "" {
			content = "请分析这张图片的内容。"
		}

	case "audio", "voice":
		// 语音消息：优先使用阿里云 NLS 语音识别转文字
		log.Printf("dingtalk stream: 🎤 [AUDIO] received from %s", userID)
		asrMime := ""
		asrFormat := ""
		asrRate := 0
		asrTextLen := 0
		asrStatus := "init"
		var audContent audioContent
		if data.Content != nil {
			contentBytes, _ := json.Marshal(data.Content)
			log.Printf("  - Audio content JSON: %s", string(contentBytes))
			if err := json.Unmarshal(contentBytes, &audContent); err != nil {
				log.Printf("  - ⚠️ Failed to parse audio content: %v", err)
				asrStatus = "parse_failed"
			} else if audContent.Recognition != "" {
				// 钉钉已内置语音识别，直接使用结果，无需调用 NLS
				log.Printf("  - ✅ 使用钉钉内置识别结果: %s", audContent.Recognition)
				content = fmt.Sprintf("[语音转文字] %s", audContent.Recognition)
				asrTextLen = len(audContent.Recognition)
				asrStatus = "builtin_recognition"
			} else if audContent.DownloadCode != "" {
				if h.cfg.AliyunNLSAkID != "" && h.cfg.AliyunNLSAkKey != "" && h.cfg.AliyunNLSAppKey != "" {
					// DingTalk 语音时长：v1.0 Stream API 返回秒数（小值），旧版返回毫秒
					durRaw := int(audContent.Duration)
					durSec := durRaw
					if durRaw >= 1000 {
						durSec = durRaw / 1000
					}
					// 从钉钉消息内容读取格式，默认 amr/8000
					audioFmt := audContent.Format
					if audioFmt == "" {
						audioFmt = "amr"
					}
					audioRate := int(audContent.SampleRate)
					if audioRate == 0 {
						audioRate = 8000
					}
					asrFormat = audioFmt
					asrRate = audioRate
					log.Printf("  - 🎤 NLS 语音识别中（时长=%ds, raw=%d, fmt=%s, rate=%d）...", durSec, durRaw, audioFmt, audioRate)
					audioBytes, dlMime, dlErr := h.downloadMediaBytes(ctx, audContent.DownloadCode, "audio/amr")
					asrMime = dlMime
					if dlErr != nil {
						log.Printf("  - ⚠️ Failed to download audio: %v", dlErr)
						asrStatus = "download_failed"
					} else {
						if len(audioBytes) >= 8 {
							log.Printf("  - 🔍 Audio first 8 bytes: %X", audioBytes[:min(8, len(audioBytes))])
						}
						// 根据文件魔数自动识别真实格式
						var text string
						var srErr error
						switch {
						case len(audioBytes) >= 4 && string(audioBytes[:4]) == "OggS":
							// OGG/Opus 容器：解封装为裸 Opus 包列表，format=opus 逐包发送
							opusPkts, oErr := extractOpusFromOGG(audioBytes)
							if oErr != nil {
								log.Printf("  - ⚠️ OGG demux failed: %v", oErr)
								srErr = oErr
							} else {
								log.Printf("  - ℹ️ OGG/Opus demuxed: %d packets, format=opus", len(opusPkts))
								text, srErr = h.transcribeOpusPackets(ctx, opusPkts, 16000)
							}
						case strings.HasPrefix(string(audioBytes), "#!AMR-WB\n"):
							audioBytes = audioBytes[len("#!AMR-WB\n"):]
							audioFmt = "amr-wb"
							audioRate = 16000
							log.Printf("  - ℹ️ Stripped AMR-WB file header (%d bytes remain)", len(audioBytes))
							text, srErr = h.transcribeAudioBytes(ctx, audioBytes, audioFmt, audioRate)
						case strings.HasPrefix(string(audioBytes), "#!AMR\n"):
							audioBytes = audioBytes[len("#!AMR\n"):]
							log.Printf("  - ℹ️ Stripped AMR-NB file header (%d bytes remain)", len(audioBytes))
							text, srErr = h.transcribeAudioBytes(ctx, audioBytes, audioFmt, audioRate)
						default:
							text, srErr = h.transcribeAudioBytes(ctx, audioBytes, audioFmt, audioRate)
						}
						if srErr != nil {
							log.Printf("  - ⚠️ NLS transcription failed: %v", srErr)
							asrStatus = "nls_failed"
						} else if text != "" {
							log.Printf("  - ✅ NLS result: %s", text)
							content = fmt.Sprintf("[语音转文字] %s", text)
							asrTextLen = len(text)
							asrStatus = "ok"
						} else {
							log.Printf("  - ⚠️ NLS returned empty text")
							asrStatus = "empty"
						}
					}
				} else {
					log.Printf("  - ℹ️ Aliyun NLS 未配置，跳过语音识别（ALIYUN_NLS_AKID/AKKEY/APPKEY）")
					asrStatus = "nls_not_configured"
				}
			}
		}
		log.Printf("asr-summary platform=dingtalk mime=%s format=%s sampleRate=%d textLen=%d status=%s", asrMime, asrFormat, asrRate, asrTextLen, asrStatus)
		if content == "" {
			durRaw2 := int(audContent.Duration)
			durSec2 := durRaw2
			if durRaw2 >= 1000 {
				durSec2 = durRaw2 / 1000
			}
			content = fmt.Sprintf("[语音消息，时长: %d秒，请配置 ALIYUN_NLS_* 环境变量以启用语音识别]", durSec2)
		}

	case "video":
		// 视频消息处理流程（和图片一样自动选择模型）：
		// 1. 有明确支持视频的模型 → 直接发送视频
		// 2. 有图片模型但无视频模型 → Gateway 提取帧图片 → 发送帧图片
		// 3. 都没有 → 返回错误
		log.Printf("dingtalk stream: 🎬 [VIDEO] received from %s", userID)
		var vidContent videoContent
		if data.Content != nil {
			contentBytes, _ := json.Marshal(data.Content)
			log.Printf("  - Video content JSON: %s", string(contentBytes))
			if err := json.Unmarshal(contentBytes, &vidContent); err != nil {
				log.Printf("  - ⚠️ Failed to parse video content: %v", err)
			} else if vidContent.DownloadCode != "" {
				videoBytes, videoMime, videoErr := h.downloadMediaBytes(ctx, vidContent.DownloadCode, "video/mp4")
				if videoErr != nil {
					log.Printf("  - ⚠️ Failed to download video: %v", videoErr)
				} else {
					now := time.Now().UTC()
					relDir := base.BuildMediaRelativeDir("dingtalk", userID, mediaSessionID, now)
					mediaRoot := base.MediaRootDirForOpenCode(h.client.Directory())
					saved, saveErr := base.SaveTempMedia(
						mediaRoot,
						relDir,
						"video",
						msgID,
						"dingtalk_video.mp4",
						videoMime,
						videoBytes,
						base.MediaTTLFromEnv(),
						base.MediaMaxBytesFromEnv(),
					)
					if saveErr != nil {
						log.Printf("  - ⚠️ Failed to save temp video file: %v", saveErr)
					} else {
						log.Printf("  - 🗂️ Video temp saved: %s (size: %d bytes)", saved.LocalPath, saved.Size)

						durRaw := int(vidContent.Duration)
						durSec := durRaw
						if durRaw >= 1000 {
							durSec = durRaw / 1000
						}

						// 1. 检查是否有明确支持视频的模型
						if h.client.HasVideoCapableModel() {
							mediaFiles = append(mediaFiles, base.MediaFileRecord{
								MessageID:    msgID,
								UserID:       userID,
								SessionID:    mediaSessionID,
								Platform:     "dingtalk",
								MsgType:      "video",
								Filename:     saved.Filename,
								Mime:         saved.Mime,
								Size:         saved.Size,
								SHA256:       saved.SHA256,
								LocalPath:    saved.LocalPath,
								RelativePath: saved.RelativePath,
								CreatedAt:    saved.CreatedAt,
								ExpireAt:     saved.ExpireAt,
							})
							log.Printf("  - ✅ Using video-capable model to process video directly")
							dataURI, mime, err := h.downloadMediaAsDataURI(ctx, vidContent.DownloadCode, "video/mp4")
							if err != nil {
								log.Printf("  - ⚠️ Failed to create data URI for video: %v", err)
								content = fmt.Sprintf("请分析这个视频（文件已保存到: %s，大小: %d bytes）。", saved.LocalPath, saved.Size)
							} else {
								mediaInfo = map[string]interface{}{
									"type": "video",
									"url":  dataURI,
									"mime": mime,
								}
								if durSec > 0 {
									content = fmt.Sprintf("请分析这个视频的内容（时长: %d秒）。", durSec)
								} else {
									content = "请分析这个视频的内容。"
								}
							}
						} else if h.client.HasImageCapableModel() {
							// 2. 有图片模型 → Gateway 提取帧图片，然后发送帧图片（和图片一样处理）
							log.Printf("  - 🔄 No video-capable model, extracting frames from video...")
							frames, extractErr := base.ExtractVideoFrames(ctx, saved.LocalPath, h.client.Directory(), 10)
							if extractErr != nil {
								log.Printf("  - ⚠️ Failed to extract frames: %v", extractErr)
								content = fmt.Sprintf("⚠️ 视频帧提取失败: %v\n\n视频已保存到: %s", extractErr, saved.LocalPath)
							} else {
								log.Printf("  - ✅ Extracted %d frames from video", len(frames))
								// 将帧图片作为附件发送（和图片一样）
								for i, frame := range frames {
									frameDataURI, err := base.ReadFileAsDataURI(frame.FramePath)
									if err != nil {
										log.Printf("  - ⚠️ Failed to read frame %d: %v", i, err)
										continue
									}
									extraAttachments = append(extraAttachments, opencode.Attachment{
										Mime:     "image/jpeg",
										URL:      frameDataURI,
										Filename: fmt.Sprintf("frame_%d.jpg", frame.FrameNumber),
									})
								}
								if len(extraAttachments) > 0 {
									log.Printf("  - 📎 Sending %d frame images as attachments", len(extraAttachments))
									if durSec > 0 {
										content = fmt.Sprintf("这是一个视频的关键帧截图（视频时长: %d秒），请分析视频内容。", durSec)
									} else {
										content = "这是一个视频的关键帧截图，请分析视频内容。"
									}
								} else {
									content = fmt.Sprintf("⚠️ 视频帧提取失败。\n\n视频已保存到: %s", saved.LocalPath)
								}
							}
						} else {
							// 3. 既没有视频模型也没有图片模型
							log.Printf("  - ⚠️ No video-capable model and no image-capable model found")
							content = fmt.Sprintf("⚠️ 视频处理暂不可用。\n\n视频已保存到: %s\n大小: %d bytes\n时长: %d秒\n\n请配置支持视频或图片的模型。", saved.LocalPath, saved.Size, durSec)
						}
					}
				}
			}
		}
		// 如果内容为空，设置默认提示
		if content == "" {
			content = "请分析这个视频的内容。"
		}

	case "file":
		log.Printf("dingtalk stream: 📄 [FILE] received from %s", userID)
		var fContent fileContent
		if data.Content != nil {
			contentBytes, _ := json.Marshal(data.Content)
			log.Printf("  - File content JSON: %s", string(contentBytes))
			if err := json.Unmarshal(contentBytes, &fContent); err != nil {
				log.Printf("  - ⚠️ Failed to parse file content: %v", err)
			} else if strings.TrimSpace(fContent.DownloadCode) != "" {
				fileBytes, fileMime, dlErr := h.downloadMediaBytes(ctx, fContent.DownloadCode, "application/octet-stream")
				if dlErr != nil {
					log.Printf("  - ⚠️ Failed to download file: %v", dlErr)
				} else {
					fileName := strings.TrimSpace(fContent.FileName)
					if fileName == "" {
						fileName = "dingtalk_file.bin"
					}
					now := time.Now().UTC()
					relDir := base.BuildMediaRelativeDir("dingtalk", userID, mediaSessionID, now)
					saved, saveErr := base.SaveTempMedia(
						base.MediaRootDirFromEnv(),
						relDir,
						"file",
						msgID,
						fileName,
						fileMime,
						fileBytes,
						base.MediaTTLFromEnv(),
						base.MediaMaxBytesFromEnv(),
					)
					if saveErr != nil {
						log.Printf("  - ⚠️ Failed to save temp file: %v", saveErr)
					} else {
						mediaFiles = append(mediaFiles, base.MediaFileRecord{
							MessageID:    msgID,
							UserID:       userID,
							SessionID:    mediaSessionID,
							Platform:     "dingtalk",
							MsgType:      "file",
							Filename:     saved.Filename,
							Mime:         saved.Mime,
							Size:         saved.Size,
							SHA256:       saved.SHA256,
							LocalPath:    saved.LocalPath,
							RelativePath: saved.RelativePath,
							CreatedAt:    saved.CreatedAt,
							ExpireAt:     saved.ExpireAt,
						})
						log.Printf("  - 🗂️ File temp saved: %s", saved.LocalPath)
					}
				}
			}
		}
		if strings.TrimSpace(content) == "" {
			fileName := strings.TrimSpace(fContent.FileName)
			if fileName == "" {
				fileName = "未命名文件"
			}
			content = fmt.Sprintf("[文件消息: %s]", fileName)
		}

	case "richText":
		// 图文混合消息
		log.Printf("dingtalk stream: 📝🖼️ [RICHTEXT] received from %s", userID)
		var rtContent richTextContent
		if data.Content != nil {
			contentBytes, _ := json.Marshal(data.Content)
			log.Printf("  - RichText content JSON: %s", string(contentBytes))
			if err := json.Unmarshal(contentBytes, &rtContent); err != nil {
				log.Printf("  - ⚠️ Failed to parse richText content: %v", err)
			} else {
				var textParts []string
				imgIndex := 0
				failedImages := 0
				for _, item := range rtContent.RichText {
					// 钉钉 richText 中文字元素可能没有 type 字段，只有 text 字段
					itemType := item.Type
					if itemType == "" && item.Text != "" {
						itemType = "text"
					}

					// 调试日志：打印每个 item 的详细信息
					itemJSON, _ := json.Marshal(item)
					log.Printf("  - RichText item: type=%s, JSON=%s", itemType, string(itemJSON))

					switch itemType {
					case "text":
						if t := strings.TrimSpace(item.Text); t != "" {
							textParts = append(textParts, t)
						}
					case "picture":
						imgIndex++
						// 新版 v1.0 API 使用 downloadCode；pictureDownloadCode 是旧版 oapi 专用，作为 fallback
						picCode := item.DownloadCode
						if picCode == "" {
							picCode = item.PictureDownloadCode
						}
						if picCode == "" {
							log.Printf("  - ⚠️ RichText image #%d: no download code", imgIndex)
							failedImages++
							continue
						}
						log.Printf("  - 📷 图片 #%d，downloadCode=%s",
							imgIndex, picCode[:min(20, len(picCode))])
						dataURI, mime, err := h.downloadMediaAsDataURI(ctx, picCode, "image/jpeg")
						if err != nil && item.PictureDownloadCode != "" && picCode != item.PictureDownloadCode {
							log.Printf("  - ↩️ RichText image #%d: retrying with pictureDownloadCode after error: %v", imgIndex, err)
							dataURI, mime, err = h.downloadMediaAsDataURI(ctx, item.PictureDownloadCode, "image/jpeg")
						}
						if err != nil {
							log.Printf("  - ⚠️ Failed to download richText image #%d: %v", imgIndex, err)
							failedImages++
						} else {
							log.Printf("  - ✅ RichText image #%d downloaded (mime=%s, len=%d)", imgIndex, mime, len(dataURI))
							extraAttachments = append(extraAttachments, opencode.Attachment{
								Mime:     mime,
								URL:      dataURI,
								Filename: fmt.Sprintf("dingtalk_image_%d.jpg", imgIndex),
							})
						}
					case "video":
						// 处理 richText 中的视频
						videoCode := item.DownloadCode
						if videoCode == "" {
							videoCode = item.VideoDownloadCode
						}
						if videoCode == "" {
							log.Printf("  - ⚠️ RichText video: no download code, item=%+v", item)
							continue
						}
						log.Printf("  - 🎬 RichText 视频，downloadCode=%s", videoCode[:min(20, len(videoCode))])
						videoBytes, videoMime, videoErr := h.downloadMediaBytes(ctx, videoCode, "video/mp4")
						if videoErr != nil {
							log.Printf("  - ⚠️ Failed to download richText video: %v", videoErr)
						} else {
							now := time.Now().UTC()
							relDir := base.BuildMediaRelativeDir("dingtalk", userID, mediaSessionID, now)
							mediaRoot := base.MediaRootDirForOpenCode(h.client.Directory())
							saved, saveErr := base.SaveTempMedia(
								mediaRoot, relDir, "video", msgID, "dingtalk_video.mp4",
								videoMime, videoBytes,
								base.MediaTTLFromEnv(), base.MediaMaxBytesFromEnv(),
							)
							if saveErr != nil {
								log.Printf("  - ⚠️ Failed to save richText video: %v", saveErr)
							} else {
								log.Printf("  - ✅ RichText video saved: %s (size: %d bytes)", saved.LocalPath, saved.Size)
								mediaFiles = append(mediaFiles, base.MediaFileRecord{
									MessageID:    msgID,
									UserID:       userID,
									SessionID:    mediaSessionID,
									Platform:     "dingtalk",
									MsgType:      "video",
									Filename:     saved.Filename,
									Mime:         saved.Mime,
									Size:         saved.Size,
									SHA256:       saved.SHA256,
									LocalPath:    saved.LocalPath,
									RelativePath: saved.RelativePath,
									CreatedAt:    saved.CreatedAt,
									ExpireAt:     saved.ExpireAt,
								})
								videoSkillName = h.client.FindVideoSkill(ctx)
							}
						}
					case "file":
						// 处理 richText 中的文件
						fileCode := item.DownloadCode
						if fileCode == "" {
							fileCode = item.FileDownloadCode
						}
						if fileCode == "" {
							log.Printf("  - ⚠️ RichText file: no download code, item=%+v", item)
							continue
						}
						log.Printf("  - 📄 RichText 文件，downloadCode=%s", fileCode[:min(20, len(fileCode))])
						fileBytes, fileMime, fileErr := h.downloadMediaBytes(ctx, fileCode, "application/octet-stream")
						if fileErr != nil {
							log.Printf("  - ⚠️ Failed to download richText file: %v", fileErr)
						} else {
							fileName := item.FileName
							if fileName == "" {
								fileName = "dingtalk_file.bin"
							}
							now := time.Now().UTC()
							relDir := base.BuildMediaRelativeDir("dingtalk", userID, mediaSessionID, now)
							saved, saveErr := base.SaveTempMedia(
								base.MediaRootDirFromEnv(), relDir, "file", msgID, fileName,
								fileMime, fileBytes,
								base.MediaTTLFromEnv(), base.MediaMaxBytesFromEnv(),
							)
							if saveErr != nil {
								log.Printf("  - ⚠️ Failed to save richText file: %v", saveErr)
							} else {
								log.Printf("  - ✅ RichText file saved: %s (size: %d bytes)", saved.LocalPath, saved.Size)
								mediaFiles = append(mediaFiles, base.MediaFileRecord{
									MessageID:    msgID,
									UserID:       userID,
									SessionID:    mediaSessionID,
									Platform:     "dingtalk",
									MsgType:      "file",
									Filename:     saved.Filename,
									Mime:         saved.Mime,
									Size:         saved.Size,
									SHA256:       saved.SHA256,
									LocalPath:    saved.LocalPath,
									RelativePath: saved.RelativePath,
									CreatedAt:    saved.CreatedAt,
									ExpireAt:     saved.ExpireAt,
								})
							}
						}
					default:
						// 如果有 text 字段但没有 type，也当作文本处理
						if t := strings.TrimSpace(item.Text); t != "" {
							textParts = append(textParts, t)
						} else if item.Type != "" {
							// 未知的类型，打印完整信息便于调试
							log.Printf("  - ⚠️ Unknown richText item type: %s, full item: %+v", item.Type, item)
						}
					}
				}
				if failedImages > 0 {
					log.Printf("  - ⚠️ %d/%d images failed to download", failedImages, imgIndex)
				}
				content = strings.Join(textParts, " ")
			}
		}
		// richText 消息：如果用户没有提供文字，且只有图片，给默认提示
		if content == "" && len(extraAttachments) > 0 {
			content = "请分析这张图片的内容。"
		} else if content == "" {
			content = "[图文消息]"
		}

	default:
		// 其他类型暂不支持
		log.Printf("dingtalk stream: ⚠️ [UNSUPPORTED] message type '%s' from %s", msgType, userID)
		replier := chatbot.NewChatbotReplier()
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook,
			[]byte(fmt.Sprintf("暂不支持 %s 类型的消息，请发送文本、图片、语音、视频或文件消息。", msgType)))
		return nil, nil
	}

	if content == "" {
		return nil, fmt.Errorf("empty message content")
	}

	// 构建附件列表
	var attachments []opencode.Attachment
	attachments = append(attachments, extraAttachments...)
	if mediaInfo != nil {
		if url, ok := mediaInfo["url"].(string); ok && strings.HasPrefix(url, "data:") {
			mime, _ := mediaInfo["mime"].(string)
			filename := ""
			switch mediaInfo["type"] {
			case "image":
				filename = "dingtalk_image.jpg"
			case "audio":
				filename = "dingtalk_audio.amr"
			case "video":
				filename = "dingtalk_video.mp4"
			}
			attachments = append(attachments, opencode.Attachment{
				Mime:     mime,
				URL:      url,
				Filename: filename,
			})
			log.Printf("dingtalk stream: 📎 attached %s to OpenCode message (mime=%s, dataURI_len=%d)", filename, mime, len(url))
		}
	}

	// 先尝试把非命令文本当作“待确认问题”的直接回复（无需 /answer）。
	// 仅对纯文本消息启用，避免图片/语音/文件等非文本消息被误当成权限或问题回复而提前消费。
	isPlainTextMessage := msgType == "" || msgType == "text"
	if isPlainTextMessage && !strings.HasPrefix(strings.TrimSpace(content), "/") {
		if result, err := h.handleQuickReply(ctx, data, userID, content); result != nil || err != nil {
			return result, err
		}
	} else if !isPlainTextMessage {
		log.Printf("dingtalk stream: skip quick reply for non-text message (msgType=%s, user=%s)", msgType, userID)
	}

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

	// Handle /retry command
	if content == "/retry" || strings.HasPrefix(content, "/retry ") {
		return h.handleRetry(ctx, data, userID, strings.TrimPrefix(strings.TrimPrefix(content, "/retry"), " "))
	}

	// Handle /new or /reset command to create new session
	if content == "/new" || content == "/reset" || content == "新会话" {
		return h.handleNewSession(ctx, data, userID)
	}

	// Handle /sessions or /list command to list sessions
	if content == "/sessions" || content == "/list" {
		return h.handleListSessions(ctx, data)
	}

	// Handle /status command to check session status
	if content == "/status" || content == "状态" {
		return h.handleStatus(ctx, data, userID)
	}

	// Handle /summary command to trigger context compression
	if content == "/summary" || content == "压缩" || content == "总结" {
		return h.handleSummary(ctx, data, userID)
	}

	// Handle /clear command to clear/delete current session
	if content == "/clear" || content == "清除" {
		return h.handleClear(ctx, data, userID)
	}

	// Handle /model command to get/set model
	if strings.HasPrefix(content, "/model") || strings.HasPrefix(content, "/provider") {
		return h.handleModel(ctx, data, userID, content)
	}

	// Handle /thinking command to toggle reasoning output
	if strings.HasPrefix(content, "/thinking") {
		return h.handleThinking(ctx, data, content)
	}

	// Handle /final command to toggle final-only output mode
	if strings.HasPrefix(content, "/final") {
		return h.handleFinal(ctx, data, content)
	}

	// Handle /steps command to toggle step visibility
	if strings.HasPrefix(content, "/steps") || strings.HasPrefix(content, "/step") {
		return h.handleSteps(ctx, data, content)
	}

	// Handle /devcore command to configure dev core profile injection
	if strings.HasPrefix(content, "/devcore") {
		return h.handleDevCore(ctx, data, content)
	}

	// Handle /config command to view configuration
	if content == "/config" || content == "配置" {
		return h.handleConfig(ctx, data, userID)
	}

	// Handle /cmd command to execute skill scripts directly
	if strings.HasPrefix(content, "/cmd ") {
		if h.isNonOwnerReadOnly(userID) {
			replier := chatbot.NewChatbotReplier()
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 当前为只读模式（非 owner），禁止执行 /cmd"))
			return nil, nil
		}
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

	// Handle /fork command to fork the current session
	if content == "/fork" {
		return h.handleFork(ctx, data, userID)
	}

	// Handle /undo command to revert last message
	if content == "/undo" || content == "/revert" || content == "撤销" {
		return h.handleUndo(ctx, data, userID)
	}

	// Handle /redo command to unrevert (redo) last undone message
	if content == "/redo" || content == "/unrevert" || content == "重做" {
		return h.handleRedo(ctx, data, userID)
	}

	// Handle /todo command to show current todo list
	if content == "/todo" || content == "/todos" || content == "任务" {
		return h.handleTodo(ctx, data, userID)
	}

	// Handle /diff command to show current file changes
	if content == "/diff" || content == "/changes" || content == "变更" {
		return h.handleDiff(ctx, data, userID)
	}

	// Handle /crontask command for scheduled tasks
	if strings.HasPrefix(content, "/crontask") {
		return h.handleCronTask(ctx, data, userID, content)
	}

	// Handle /whitelist command for runtime whitelist management
	if strings.HasPrefix(content, "/whitelist") {
		return h.handleWhitelist(ctx, data, userID, content)
	}

	// Handle /answer command to answer pending questions
	if strings.HasPrefix(content, "/answer ") {
		return h.handleAnswer(ctx, data, userID, content)
	}

	// Handle natural-language scheduling for plain text.
	if isPlainTextMessage {
		if handled, err := h.tryHandleNLSchedule(ctx, data, userID, content); handled || err != nil {
			return nil, err
		}
	}

	// 如果是命令形式的回复，走 /answer 命令处理。

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

	// 如果有视频 skill 且用户没有指定 agent，使用视频 skill
	if videoSkillName != "" && agentName == "" {
		agentName = videoSkillName
		log.Printf("dingtalk stream: auto-using video skill '%s' for video message", agentName)
	}

	// 非owner用户自动使用plan模式（如果配置了NonOwnerPlanMode且用户未指定agent）
	if agentName == "" && h.cfg.NonOwnerPlanMode && h.isNonOwnerReadOnly(userID) {
		agentName = "plan"
		log.Printf("dingtalk stream: auto-using plan agent for non-owner user %s", userID)
	}

	// 非owner用户且未使用plan agent时，添加只读guard
	if h.isNonOwnerReadOnly(userID) && agentName != "plan" {
		content = h.withReadOnlyGuard(content)
	}

	// Get or create session for user BEFORE sending message
	// This ensures the mapping exists when questions/permissions arrive
	var sessionID string
	if sid, ok := h.adapter.GetSessionForUser(userID); ok {
		sessionID = sid
		log.Printf("dingtalk stream: reusing existing session %s for user %s", sessionID, userID)
	}

	// Send to OpenCode with streaming
	// 使用独立的context，避免被钉钉SDK的context超时影响
	// 给予充足的超时时间，让OpenCode能够完成复杂任务
	timeout := 20 * time.Minute // 默认20分钟
	if agentName != "" {
		timeout = 30 * time.Minute // agent模式给30分钟
	}
	sendCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	log.Printf("dingtalk stream: sending to OpenCode (timeout: %v, agent: %s, session: %s, content_len: %d)",
		timeout, agentName, sessionID, len(content))

	var fullReply strings.Builder
	var lastSentLength int
	lastUpdateTime := time.Now()              // 初始化为当前时间，确保5秒冷却期从消息接收时开始计算
	const minUpdateInterval = 5 * time.Second // 最小更新间隔：5秒
	const minUpdateChars = 300                // 最小更新字符数：300字符（减少频率）

	// Track session mapping status
	sessionMapped := false
	var sessionMappingMu sync.Mutex

	// 第一个 callback 是 sessionID (特殊信号)
	callbackCalled := false
	var thinkingBuffer strings.Builder
	thinkingSent := false
	preferAICardForAllReplies := h.shouldPreferAICardForOpenCodeReply()
	if preferAICardForAllReplies {
		log.Printf("dingtalk stream: AI Card is enabled for all OpenCode replies for user %s", userID)
	}
	bufferFinalUntilFlush := h.client.IsFinalOnlyEnabled() || h.client.IsThinkingEnabled() || preferAICardForAllReplies

	formatThinkingBlock := func(content string) string {
		trimmed := strings.TrimSpace(content)
		if trimmed == "" {
			return ""
		}
		return "思考过程：\n" + trimmed + "\n思考结束"
	}

	sendReply := func(msg string) error {
		if !preferAICardForAllReplies {
			return h.sendReplyBySource(sendCtx, data.SessionWebhook, data.ConversationType, conversationID, userID, msg)
		}
		return h.sendReplyWithAICardFallback(sendCtx, data.SessionWebhook, data.ConversationType, conversationID, userID, msg)
	}
	sendFinalReply := func(msg string) error {
		return sendReply(msg)
	}

	sendContent := content
	metadata := map[string]string{
		"conversation_type": data.ConversationType,
		"sender_nick":       data.SenderNick,
		"message_type":      msgType,
	}
	if len(mediaFiles) > 0 && (msgType == "file" || msgType == "video") {
		taskSessionID := sessionID
		if strings.TrimSpace(taskSessionID) == "" {
			taskSessionID = mediaSessionID
		}
		mediaCtx := base.MediaTaskContext{
			Platform:    "dingtalk",
			MessageType: msgType,
			UserID:      userID,
			SessionID:   taskSessionID,
			MessageID:   msgID,
			Files:       mediaFiles,
		}
		if mediaMD, mdErr := base.BuildMediaMetadata(mediaCtx); mdErr != nil {
			log.Printf("dingtalk stream: ⚠️ failed to build media metadata: %v", mdErr)
		} else {
			for k, v := range mediaMD {
				metadata[k] = v
			}
			sendContent = base.BuildMediaPromptPrefix(mediaCtx) + sendContent
		}
	}
	// Snapshot content/metadata for potential off-peak retry enqueue.
	retryContent := sendContent
	retryMetadata := cloneStringMap(metadata)

	response, err := h.client.SendMessageStreamingWithEvents(sendCtx, opencode.MessagePayload{
		Channel:     "dingtalk",
		UserID:      userID,
		ThreadID:    conversationID,
		SessionID:   sessionID, // Pass existing session if available
		Content:     sendContent,
		Agent:       agentName,
		Streaming:   true,
		Attachments: attachments,
		Metadata:    metadata,
	}, func(chunk string) error {
		// 🔍 诊断：记录第一个 callback
		if !callbackCalled {
			callbackCalled = true
			log.Printf("dingtalk stream: 🔍 FIRST CALLBACK - chunk='%s', len=%d",
				chunk[:min(30, len(chunk))], len(chunk))
		}

		// Session IDs are internal control signals for every turn, including
		// fallback turns that reuse this callback. Never append them to the
		// user-visible reply buffer.
		sessionMappingMu.Lock()
		if strings.HasPrefix(chunk, "ses_") && len(chunk) < 100 {
			h.adapter.MapUserToSession(userID, chunk)
			h.adapter.MapSessionData(chunk, "channel", data.SessionWebhook)
			log.Printf("dingtalk stream: mapped user %s to session %s (webhook: %s)", userID, chunk, data.SessionWebhook)
			sessionMapped = true
			sessionMappingMu.Unlock()
			return nil
		}
		sessionMappingMu.Unlock()

		if h.isTokenOverflowErrorChunk(chunk) {
			log.Printf("dingtalk stream: token overflow detected for user %s", userID)
			state := &tokenOverflowPendingState{
				SessionID:        sessionID,
				ThreadID:         conversationID,
				UserID:           userID,
				Agent:            agentName,
				Content:          sendContent,
				Attachments:      append([]opencode.Attachment(nil), attachments...),
				Metadata:         cloneStringMap(metadata),
				SessionWebhook:   data.SessionWebhook,
				ConversationType: data.ConversationType,
				CreatedAt:        time.Now(),
			}
			if state.SessionID == "" {
				if mappedSessionID, ok := h.adapter.GetSessionForUser(userID); ok {
					state.SessionID = mappedSessionID
				}
			}

			policy := h.getTokenOverflowPolicy(userID)
			switch policy {
			case tokenOverflowPolicySummary:
				h.storeTokenOverflowPending(userID, state)
				if err := sendReply("⚠️ 检测到上下文已超限，已按偏好自动执行“压缩并继续”，请稍候..."); err != nil {
					log.Printf("dingtalk stream: failed to send token overflow auto-summary hint: %v", err)
				}
				go h.executeTokenOverflowDecision(context.Background(), userID, "summary")
			case tokenOverflowPolicyNew:
				h.storeTokenOverflowPending(userID, state)
				if err := sendReply("⚠️ 检测到上下文已超限，已按偏好自动执行“新会话并继续”，请稍候..."); err != nil {
					log.Printf("dingtalk stream: failed to send token overflow auto-new hint: %v", err)
				}
				go h.executeTokenOverflowDecision(context.Background(), userID, "new")
			default:
				h.storeTokenOverflowPending(userID, state)
				if err := sendReply(h.buildTokenOverflowPrompt()); err != nil {
					log.Printf("dingtalk stream: failed to send token overflow prompt: %v", err)
				}
			}
			return nil
		}

		if strings.HasPrefix(chunk, opencode.ThinkingSignalPrefix) {
			thinkingDelta := strings.TrimPrefix(chunk, opencode.ThinkingSignalPrefix)
			if strings.TrimSpace(thinkingDelta) == "" {
				return nil
			}
			thinkingBuffer.WriteString(thinkingDelta)
			log.Printf("dingtalk stream: 🧠 buffered thinking chunk (len=%d)", len(thinkingDelta))
			return nil
		}

		if strings.HasPrefix(chunk, opencode.ToolSignalPrefix) {
			toolMsg := strings.TrimSpace(strings.TrimPrefix(chunk, opencode.ToolSignalPrefix))
			if toolMsg == "" {
				return nil
			}
			if err := sendReply(toolMsg); err != nil {
				log.Printf("dingtalk stream: ⚠️ failed to send tool message: %v", err)
				return err
			}
			return nil
		}

		if strings.HasPrefix(chunk, opencode.StepSignalPrefix) {
			stepMsg := strings.TrimSpace(strings.TrimPrefix(chunk, opencode.StepSignalPrefix))
			if stepMsg == "" {
				return nil
			}
			if err := sendReply(stepMsg); err != nil {
				log.Printf("dingtalk stream: ⚠️ failed to send step message: %v", err)
				return err
			}
			return nil
		}

		if strings.HasPrefix(chunk, opencode.TodoSignalPrefix) {
			todoMsg := strings.TrimSpace(strings.TrimPrefix(chunk, opencode.TodoSignalPrefix))
			if todoMsg == "" {
				return nil
			}
			if err := sendReply(todoMsg); err != nil {
				log.Printf("dingtalk stream: ⚠️ failed to send todo progress: %v", err)
				return err
			}
			return nil
		}
		if strings.HasPrefix(chunk, opencode.QuestionSignalPrefix) || strings.HasPrefix(chunk, opencode.WaitHintSignalPrefix) {
			qMsg := strings.TrimSpace(strings.TrimPrefix(strings.TrimPrefix(chunk, opencode.QuestionSignalPrefix), opencode.WaitHintSignalPrefix))
			if qMsg == "" {
				return nil
			}
			if err := sendReply(qMsg); err != nil {
				log.Printf("dingtalk stream: ⚠️ failed to send question/permission message: %v", err)
				return err
			}
			return nil
		}

		// FlushSignal: session 结束，立即发送所有尚未发送的内容
		if chunk == opencode.FlushSignal {
			if !thinkingSent {
				if thinkingMsg := formatThinkingBlock(thinkingBuffer.String()); thinkingMsg != "" {
					log.Printf("dingtalk stream: 📤 flush signal: sending thinking block (%d bytes)", len(thinkingMsg))
					if err := sendReply(thinkingMsg); err != nil {
						log.Printf("dingtalk stream: ⚠️ flush thinking send failed: %v", err)
					} else {
						thinkingSent = true
					}
				}
			}

			toSend := fullReply.String()[lastSentLength:]
			if len(toSend) > 0 {
				log.Printf("dingtalk stream: 📤 flush signal: sending final %d bytes", len(toSend))
				if err := sendReply(toSend); err != nil {
					log.Printf("dingtalk stream: ⚠️ flush send failed: %v", err)
				} else {
					lastSentLength = len(fullReply.String())
					log.Printf("dingtalk stream: ✅ flush send done")
				}
			}
			return nil
		}

		// 🔍 诊断：记录内容 callback
		if len(chunk) > 0 && !strings.HasPrefix(chunk, "ses_") {
			log.Printf("dingtalk stream: 🔍 CONTENT CALLBACK - len=%d, prefix='%s'",
				len(chunk), truncateForLog(chunk, 50))
		}

		// 处理特殊消息：权限请求、问题确认、等待提示等需要立即发送的消息
		// 使用 Robot API（sendReplyRobot），不依赖过期的 sessionWebhook
		if strings.HasPrefix(chunk, "⏳") || strings.HasPrefix(chunk, "⏱️") ||
			strings.HasPrefix(chunk, "🔐") || strings.HasPrefix(chunk, "❓") ||
			strings.HasPrefix(chunk, "🤔💭") {
			// 这些是需要立即发送的提示消息
			log.Printf("dingtalk stream: 📤 sending immediate message: %s", truncateForLog(chunk, 50))
			if err := sendReply(chunk); err != nil {
				log.Printf("dingtalk stream: ⚠️ failed to send immediate message: %v", err)
				// 发送失败时的处理：
				// - 对于"正在处理中"提示：只返回error保持waiting timer活跃，不加入fullReply（避免污染最终内容）
				// - 对于其他重要消息（权限、问题等）：加入fullReply确保不丢失
				if !strings.Contains(chunk, "正在努力处理中") && !strings.Contains(chunk, "正在处理") {
					fullReply.WriteString(chunk)
				}
				return err
			}
			log.Printf("dingtalk stream: ✅ immediate message sent successfully")
			return nil
		}

		// 累积实际内容
		fullReply.WriteString(chunk)
		currentLength := fullReply.Len()
		newContentLength := currentLength - lastSentLength

		// 动态决定是否发送中间更新（基于内容长度和时间间隔）
		now := time.Now()
		timeSinceLastUpdate := now.Sub(lastUpdateTime)

		// 发送中间更新的条件：
		// 1. 累积了足够多的新内容（至少300字符）
		// 2. 距离上次更新已经过了至少5秒
		// 3. 这样可以避免过于频繁的更新，同时保证用户能及时看到进度
		shouldUpdate := !bufferFinalUntilFlush && newContentLength >= minUpdateChars && timeSinceLastUpdate >= minUpdateInterval

		if shouldUpdate {
			log.Printf("dingtalk stream: 📤 sending intermediate update (new content: %d chars, interval: %v)",
				newContentLength, timeSinceLastUpdate)

			// 直接发送累积的内容，使用 Robot API（不依赖过期的 sessionWebhook）
			if err := sendReply(fullReply.String()); err != nil {
				log.Printf("dingtalk stream: ⚠️ failed to send intermediate update: %v", err)
				// 不返回错误，继续累积内容，下次再试或最后发送
			} else {
				lastSentLength = currentLength
				lastUpdateTime = now
			}
		}

		return nil
	}, func(event opencode.StreamEvent) error {
		switch event.Kind {
		case opencode.StreamEventSessionReady:
			if strings.TrimSpace(event.SessionID) == "" {
				return nil
			}
			sessionMappingMu.Lock()
			if !sessionMapped {
				h.adapter.MapUserToSession(userID, event.SessionID)
				h.adapter.MapSessionData(event.SessionID, "channel", data.SessionWebhook)
				sessionMapped = true
				log.Printf("dingtalk stream: structured session ready mapped user %s -> %s", userID, event.SessionID)
			}
			sessionMappingMu.Unlock()
		case opencode.StreamEventTodoSnapshot:
			log.Printf("dingtalk stream: structured todo snapshot received (%d items, session=%s)", len(event.Todos), event.SessionID)
		case opencode.StreamEventDiffSnapshot:
			log.Printf("dingtalk stream: structured diff snapshot received (%d files, session=%s)", len(event.Diff), event.SessionID)
		}
		return nil
	})

	// 🔍 诊断日志：streaming完成时的状态
	accumulatedContent := fullReply.String()
	log.Printf("dingtalk stream: 🔍 SendMessageStreaming returned - userID=%s, err=%v, reply_len=%d, accumulated_len=%d",
		userID, err, len(response.Reply), len(accumulatedContent))

	// Enqueue for off-peak retry when deadline exceeded with zero content.
	if retryworker.IsDeadlineErr(err) && len(accumulatedContent) == 0 && len(response.Reply) == 0 {
		if h.retryStore != nil {
			attJSON := ""
			if len(attachments) > 0 {
				if b, jerr := json.Marshal(attachments); jerr == nil {
					attJSON = string(b)
				}
			}
			_, inserted, qErr := h.retryStore.SavePendingRetry(memstore.PendingRetry{
				Adapter:         "dingtalk",
				UserID:          userID,
				ThreadID:        conversationID,
				Channel:         data.SessionWebhook,
				Content:         retryContent,
				AttachmentsJSON: attJSON,
				Metadata:        retryMetadata,
				FailReason:      err.Error(),
			})
			if qErr != nil {
				log.Printf("dingtalk: enqueue retry failed for user %s: %v", userID, qErr)
			} else if inserted {
				log.Printf("dingtalk: enqueued retry for user %s (thread=%s)", userID, conversationID)
			}
		}
	}

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
		_ = sendReply(errMsg)
		return nil, err
	}

	// Map user to session (whether new or existing)
	if response.SessionID != "" {
		h.adapter.MapUserToSession(userID, response.SessionID)
		// Store the session webhook for later use
		h.adapter.MapSessionData(response.SessionID, "channel", data.SessionWebhook)
	}

	// 🔍 诊断日志：streaming完成时的状态 (accumulatedContent already defined above)
	log.Printf("dingtalk stream: 🔍 STREAMING END - userID=%s, sessionID=%s, "+
		"fullReplyLen=%d, lastSentLen=%d, syncReply=%t, contentPreview='%s'",
		userID,
		func() string {
			if response.SessionID != "" {
				return response.SessionID[:min(8, len(response.SessionID))]
			}
			return "(none)"
		}(),
		len(accumulatedContent), lastSentLength, response.Reply != "",
		truncateForLog(accumulatedContent, 50))

	// Send final complete reply (only if we have synchronous content)
	// For async mode, the SSE callbacks already sent the content
	if response.Reply != "" {
		if !thinkingSent {
			if thinkingMsg := formatThinkingBlock(thinkingBuffer.String()); thinkingMsg != "" {
				if err := sendReply(thinkingMsg); err != nil {
					log.Printf("dingtalk stream: failed to send thinking block: %v", err)
				}
				thinkingSent = true
			}
		}

		if err := sendFinalReply(response.Reply); err != nil {
			log.Printf("dingtalk stream: failed to reply: %v", err)
			return nil, err
		}
		log.Printf("dingtalk stream: ✅ sent sync reply to user %s (%d chars)", userID, len(response.Reply))
	} else {
		// Async mode - 检查是否有未发送的内容
		if !thinkingSent {
			if thinkingMsg := formatThinkingBlock(thinkingBuffer.String()); thinkingMsg != "" {
				if err := sendReply(thinkingMsg); err != nil {
					log.Printf("dingtalk stream: failed to send thinking block: %v", err)
				}
				thinkingSent = true
			}
		}

		if len(accumulatedContent) > 0 {
			// 有内容但可能没有全部发送（因为中间更新有间隔和字符数限制）
			unsentLength := len(accumulatedContent) - lastSentLength
			if unsentLength > 0 {
				log.Printf("dingtalk stream: 📤 sending final message (%d total chars, %d unsent)",
					len(accumulatedContent), unsentLength)

				// 🔧 修复：只发送未发送的部分，避免重复，使用 Robot API
				unsentContent := accumulatedContent[lastSentLength:]
				if err := sendFinalReply(unsentContent); err != nil {
					log.Printf("dingtalk stream: ❌ failed to send final message: %v", err)
					// 不返回错误，避免影响session映射
				} else {
					log.Printf("dingtalk stream: ✅ sent final message to user %s (%d chars total, %d new)",
						userID, len(accumulatedContent), unsentLength)
				}
			} else {
				log.Printf("dingtalk stream: ✓ all content already sent via intermediate updates (%d chars)",
					len(accumulatedContent))
			}
		} else {
			// 没有内容累积（可能是权限请求之类的交互式任务，或者全都是特殊消息）
			log.Printf("dingtalk stream: ℹ️ async mode completed with no accumulated content "+
				"(callbackCalled=%t, user=%s)", callbackCalled, userID)
		}
	}

	return nil, nil
}

func (h *Handler) isUserAllowed(userID string) bool {
	h.whitelistMu.RLock()
	defer h.whitelistMu.RUnlock()

	if len(h.allowedUserSet) == 0 {
		return true
	}
	_, ok := h.allowedUserSet[strings.TrimSpace(userID)]
	return ok
}

func (h *Handler) isUserAllowedAny(userIDs ...string) bool {
	h.whitelistMu.RLock()
	defer h.whitelistMu.RUnlock()

	if len(h.allowedUserSet) == 0 {
		return true
	}

	for _, id := range userIDs {
		normalized := strings.TrimSpace(id)
		if normalized == "" {
			continue
		}
		if _, ok := h.allowedUserSet[normalized]; ok {
			return true
		}
	}

	return false
}

func truncateForLog(s string, maxRunes int) string {
	if maxRunes <= 0 || s == "" {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes])
}

// handleListSkills handles the /skills command to list available agents.
func (h *Handler) handleListSkills(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
	agents, err := h.client.ListAgents(ctx)
	if err != nil {
		log.Printf("dingtalk: failed to list agents: %v", err)
		return nil, err
	}

	customSkills := listCustomSkillsForDisplay(h.client.Directory())

	// Build response message
	var reply strings.Builder
	reply.WriteString("📋 可用的 Skills:\n\n")

	if len(customSkills) > 0 {
		reply.WriteString("🧩 自定义 Skills（与 TUI /skills 一致）：\n")
		for i, item := range customSkills {
			reply.WriteString(fmt.Sprintf("%d. %s (%s)\n", i+1, item.Name, item.Source))
		}
		reply.WriteString("\n")
	}

	if len(agents) == 0 {
		if len(customSkills) == 0 {
			reply.WriteString("暂无可用 skills。\n")
		}
	} else {
		reply.WriteString("🤖 内置 Agents：\n")
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
	}

	reply.WriteString("💡 使用方法: @agent_name 你的消息\n")
	reply.WriteString("例如: @build 帮我编译项目")
	if len(customSkills) > 0 {
		reply.WriteString("\n\n📁 自定义 skills 目录: ~/.config/opencode/skills")
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

type customSkillItem struct {
	Name   string
	Source string
}

func listCustomSkillsForDisplay(baseDir string) []customSkillItem {
	seen := map[string]bool{}
	out := []customSkillItem{}
	dirs := []string{
		filepath.Join(resolveOpenCodeDirectory(baseDir), ".opencode", "skills"),
		filepath.Join(getHomeDirSafe(), ".config", "opencode", "skills"),
	}
	for i, dir := range dirs {
		source := "project"
		if i == 1 {
			source = "global"
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			name := e.Name()
			if seen[name] {
				continue
			}
			seen[name] = true
			out = append(out, customSkillItem{Name: name, Source: source})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func resolveOpenCodeDirectory(baseDir string) string {
	d := strings.TrimSpace(baseDir)
	if d == "" {
		d = "."
	}
	if abs, err := filepath.Abs(d); err == nil {
		return abs
	}
	return d
}

func getHomeDirSafe() string {
	h, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return h
}

// handleWhitelist 处理白名单管理命令（运行时生效，不持久化）
func (h *Handler) handleWhitelist(ctx context.Context, data *chatbot.BotCallbackDataModel, userID, content string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	ownerUserID := h.currentOwnerUserID()
	if ownerUserID != "" && strings.TrimSpace(userID) != ownerUserID {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 仅机器人主人可管理白名单"))
		return nil, nil
	}

	parts := strings.Fields(strings.TrimSpace(content))
	if len(parts) < 2 {
		return h.sendWhitelistHelp(ctx, data)
	}

	subCommand := strings.ToLower(parts[1])
	switch subCommand {
	case "add", "create", "新增":
		return h.handleWhitelistAdd(ctx, data, parts[2:])
	case "delete", "del", "rm", "remove", "删除":
		return h.handleWhitelistDelete(ctx, data, parts[2:])
	case "list", "ls", "列表":
		return h.handleWhitelistList(ctx, data)
	case "help", "帮助":
		return h.sendWhitelistHelp(ctx, data)
	default:
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 未知子命令，使用 /whitelist help 查看帮助"))
		return nil, nil
	}
}

func (h *Handler) handleWhitelistAdd(ctx context.Context, data *chatbot.BotCallbackDataModel, args []string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()
	if len(args) == 0 {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 请指定 userID\n\n格式: /whitelist add <userID> [更多userID]"))
		return nil, nil
	}

	ids := flattenWhitelistArgs(args)
	added := make([]string, 0, len(ids))
	existed := make([]string, 0, len(ids))

	h.whitelistMu.Lock()
	for _, id := range ids {
		if _, ok := h.allowedUserSet[id]; ok {
			existed = append(existed, id)
			continue
		}
		h.allowedUserSet[id] = struct{}{}
		added = append(added, id)
	}
	total := len(h.allowedUserSet)
	h.whitelistMu.Unlock()

	var msg strings.Builder
	msg.WriteString("✅ 白名单已更新（运行时生效）\n")
	if len(added) > 0 {
		msg.WriteString(fmt.Sprintf("新增: %s\n", strings.Join(added, ", ")))
	}
	if len(existed) > 0 {
		msg.WriteString(fmt.Sprintf("已存在: %s\n", strings.Join(existed, ", ")))
	}
	msg.WriteString(fmt.Sprintf("当前总数: %d\n", total))
	msg.WriteString("ℹ️ 重启后会恢复为环境变量配置")

	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg.String()))
	log.Printf("dingtalk: whitelist add done, added=%d existed=%d total=%d", len(added), len(existed), total)
	return nil, nil
}

func (h *Handler) handleWhitelistDelete(ctx context.Context, data *chatbot.BotCallbackDataModel, args []string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()
	if len(args) == 0 {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 请指定 userID\n\n格式: /whitelist delete <userID> [更多userID]"))
		return nil, nil
	}

	ids := flattenWhitelistArgs(args)
	removed := make([]string, 0, len(ids))
	notFound := make([]string, 0, len(ids))
	ownerUserID := h.currentOwnerUserID()

	h.whitelistMu.Lock()
	for _, id := range ids {
		if ownerUserID != "" && id == ownerUserID {
			notFound = append(notFound, id+"(owner，禁止删除)")
			continue
		}
		if _, ok := h.allowedUserSet[id]; ok {
			delete(h.allowedUserSet, id)
			removed = append(removed, id)
		} else {
			notFound = append(notFound, id)
		}
	}
	total := len(h.allowedUserSet)
	h.whitelistMu.Unlock()

	var msg strings.Builder
	msg.WriteString("✅ 白名单已更新（运行时生效）\n")
	if len(removed) > 0 {
		msg.WriteString(fmt.Sprintf("删除: %s\n", strings.Join(removed, ", ")))
	}
	if len(notFound) > 0 {
		msg.WriteString(fmt.Sprintf("未找到: %s\n", strings.Join(notFound, ", ")))
	}
	msg.WriteString(fmt.Sprintf("当前总数: %d\n", total))
	if total == 0 {
		msg.WriteString("⚠️ 当前白名单为空，表示不做白名单限制\n")
	}
	msg.WriteString("ℹ️ 重启后会恢复为环境变量配置")

	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg.String()))
	log.Printf("dingtalk: whitelist delete done, removed=%d notFound=%d total=%d", len(removed), len(notFound), total)
	return nil, nil
}

func (h *Handler) handleWhitelistList(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	h.whitelistMu.RLock()
	ids := make([]string, 0, len(h.allowedUserSet))
	for id := range h.allowedUserSet {
		ids = append(ids, id)
	}
	h.whitelistMu.RUnlock()

	sort.Strings(ids)

	if len(ids) == 0 {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("📋 当前白名单为空（不做白名单限制）"))
		return nil, nil
	}

	var msg strings.Builder
	msg.WriteString(fmt.Sprintf("📋 白名单用户（共 %d 个）:\n", len(ids)))
	for i, id := range ids {
		msg.WriteString(fmt.Sprintf("%d. %s\n", i+1, id))
	}
	msg.WriteString("\nℹ️ 运行时修改，重启后恢复环境变量配置")

	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg.String()))
	return nil, nil
}

func (h *Handler) sendWhitelistHelp(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	helpMsg := `📋 白名单命令帮助

🔹 查看白名单
/whitelist list

🔹 新增用户
/whitelist add <userID>
/whitelist add <userID1> <userID2>

🔹 删除用户
/whitelist delete <userID>
/whitelist del <userID1> <userID2>

💡 说明：
• 仅机器人主人可管理（配置了 DINGTALK_OWNER_USERID 时）
• ownerID 仅支持启动时通过环境变量 DINGTALK_OWNER_USERID 设置
• 命令会持久化白名单，本次更新后重启仍生效
• 当前实现按 userID 精确匹配`

	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(helpMsg))
	return nil, nil
}

func flattenWhitelistArgs(args []string) []string {
	set := make(map[string]struct{})
	for _, arg := range args {
		for _, item := range strings.Split(arg, ",") {
			id := strings.TrimSpace(item)
			if id == "" {
				continue
			}
			set[id] = struct{}{}
		}
	}

	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (h *Handler) currentOwnerUserID() string {
	h.whitelistMu.RLock()
	defer h.whitelistMu.RUnlock()
	return strings.TrimSpace(h.cfg.OwnerUserID)
}

func (h *Handler) isNonOwnerReadOnly(userID string) bool {
	ownerUserID := h.currentOwnerUserID()
	if ownerUserID == "" {
		return false
	}
	return strings.TrimSpace(userID) != ownerUserID
}

func (h *Handler) withReadOnlyGuard(content string) string {
	guard := "[系统约束-只读模式]\n当前用户不是机器人 owner。你只能执行只读操作：分析、解释、检索、列出信息、给出修改建议。\n严格禁止任何写操作和副作用操作，包括但不限于：创建/修改/删除文件，执行会改变环境或数据的命令，安装/卸载依赖，提交代码。\n若用户要求写操作，请明确拒绝并改为提供可执行方案。\n\n"
	return guard + content
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
	output, err := h.client.ExecuteShellOutput(ctx, sessionID, command)
	if err != nil {
		log.Printf("dingtalk: command execution failed: %v", err)
		errMsg := fmt.Sprintf("❌ 命令执行失败: %v", err)
		replier := chatbot.NewChatbotReplier()
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(errMsg))
		return nil, err
	}

	// Build response message
	var reply string
	if strings.TrimSpace(output) != "" {
		reply = fmt.Sprintf("🖥️ 命令执行结果:\n\n```\n%s\n```", output)
	} else {
		reply = "🖥️ 命令已执行，但没有可显示的输出"
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
// Used for sending messages back to users through the platform's messaging API.
// For scheduled tasks, channel may contain the SessionWebhook URL.
func (h *Handler) SendMessage(ctx context.Context, channel, userID, content string) error {
	if !h.cfg.UseStream {
		log.Printf("dingtalk: SendMessage only supports Stream mode")
		return fmt.Errorf("webhook mode does not support push messages")
	}

	// 检查 channel 是否是 webhook URL（以 https:// 开头）
	if strings.HasPrefix(channel, "https://") || strings.HasPrefix(channel, "http://") {
		// 使用 webhook URL 直接发送消息
		log.Printf("dingtalk: sending message via webhook URL")
		return h.sendViaWebhook(ctx, channel, content)
	}

	// Stream 模式下，没有webhook URL 无法主动推送消息
	log.Printf("dingtalk: no webhook URL provided, cannot send message (channel: %s)", channel)
	return nil // 返回 nil 避免阻塞任务
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

🔧 基本命令：
/help 或 帮助 - 显示此帮助信息
/skills 或 /agents - 查看可用的技能列表
/abort 或 /stop - 中止正在运行的任务
/refresh - 刷新技能缓存

📊 会话管理：
/status 或 状态 - 查看当前会话状态
/new 或 /reset - 创建新会话
/clear 或 清除 - 删除当前会话
/fork - 派生(fork)当前会话（保留历史，创建新分支）
/undo 或 撤销 - 撤销上一次操作
/redo 或 重做 - 重做已撤销的操作
/sessions 或 /list - 列出所有会话
/summary 或 压缩 - 压缩会话上下文（释放token空间）

📋 任务追踪（对应 TUI 实时看板）：
/todo 或 任务 - 查看 AI 当前的任务进度
/diff 或 变更 - 查看本次会话的文件变更摘要

🤖 模型配置：
/model - 查看可用模型（含当前会话信息）
/model <provider>/<model> - 设置模型
/devcore - 查看 Dev Core 状态
/devcore <自然语言偏好> - 直接设置并开启 Dev Core
/devcore on|off - 开关 Dev Core 注入
/devcore reset - 清空偏好并关闭（默认状态）
/thinking - 查看 thinking 开关状态
/thinking on|off - 开关 thinking 返回
/final - 查看最终返回模式
/final on|off - 开关仅结束时返回最终结果
/steps - 查看步骤显示状态
/steps on|off - 开关步骤显示
/config 或 配置 - 查看完整配置

📋 OpenCode 模式说明：

1️⃣ Chat模式（默认）
   - 直接对话，立即响应

2️⃣ Plan模式
   - AI先制定计划再执行

3️⃣ Build模式（需要确认）
   - AI生成操作计划并等待确认
   - 回复 '允许' 或序号确认授权

💡 使用技巧：
• @build / @plan / @chat 消息 - 调用特定 agent（Build/Plan/Chat 模式）
• 任务进行中可发 /todo 查看进度
• 文件变更可通过 /diff 手动查看摘要
• /fork 创建当前上下文的副本继续探索

🛠️ 高级命令：
/cmd <command> - 在当前会话中执行 shell 命令
/answer <answer> - 回答最近的待确认问题（可选：/answer <question_id> <answer>）
/crontask - 管理定时任务

🔐 白名单命令：
/whitelist list - 查看白名单
/whitelist add <userID...> - 添加白名单用户
/whitelist del <userID...> - 删除白名单用户

⚠️ 白名单说明：
• ownerID 仅启动时通过 DINGTALK_OWNER_USERID 设置
• ownerID 会自动加入白名单且不可删除`

	replier := chatbot.NewChatbotReplier()
	if err := replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(helpText)); err != nil {
		log.Printf("dingtalk: failed to send help: %v", err)
		return nil, err
	}

	return nil, nil
}

// handleThinking 处理 thinking 输出开关命令（全局）
func (h *Handler) handleThinking(ctx context.Context, data *chatbot.BotCallbackDataModel, content string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()
	parts := strings.Fields(strings.TrimSpace(content))

	if len(parts) == 1 {
		status := "off"
		if h.client.IsThinkingEnabled() {
			status = "on"
		}
		msg := fmt.Sprintf("🧠 Thinking 返回状态: %s\n\n使用方法:\n/thinking on  - 开启\n/thinking off - 关闭", status)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	arg := strings.ToLower(parts[1])
	switch arg {
	case "on", "true", "1":
		h.client.SetThinkingEnabled(true)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ 已开启 thinking 返回（将按 'Thinking:' 分段输出）"))
		return nil, nil
	case "off", "false", "0":
		h.client.SetThinkingEnabled(false)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ 已关闭 thinking 返回（仅返回最终正文）"))
		return nil, nil
	default:
		msg := "❌ 命令格式错误\n\n使用方法:\n/thinking - 查看状态\n/thinking on - 开启\n/thinking off - 关闭"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}
}

// handleFinal 处理最终输出模式开关命令（全局）
func (h *Handler) handleFinal(ctx context.Context, data *chatbot.BotCallbackDataModel, content string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()
	parts := strings.Fields(strings.TrimSpace(content))

	if len(parts) == 1 {
		status := "off"
		if h.client.IsFinalOnlyEnabled() {
			status = "on"
		}
		msg := fmt.Sprintf("📦 Final-only 模式: %s\n\n使用方法:\n/final on  - 开启（仅结束时返回最终结果）\n/final off - 关闭（允许中间增量）", status)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	arg := strings.ToLower(parts[1])
	switch arg {
	case "on", "true", "1":
		h.client.SetFinalOnlyEnabled(true)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ 已开启 final-only 模式（正文与thinking均在结束后分段返回）"))
		return nil, nil
	case "off", "false", "0":
		h.client.SetFinalOnlyEnabled(false)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ 已关闭 final-only 模式（允许中间增量返回）"))
		return nil, nil
	default:
		msg := "❌ 命令格式错误\n\n使用方法:\n/final - 查看状态\n/final on - 开启\n/final off - 关闭"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}
}

// handleSteps 处理步骤显示开关命令（全局）
func (h *Handler) handleSteps(ctx context.Context, data *chatbot.BotCallbackDataModel, content string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()
	parts := strings.Fields(strings.TrimSpace(content))

	if len(parts) == 1 {
		status := "off"
		if h.client.IsStepEnabled() {
			status = "on"
		}
		msg := fmt.Sprintf("🪜 步骤显示状态: %s\n\n使用方法:\n/steps on  - 开启\n/steps off - 关闭", status)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	arg := strings.ToLower(parts[1])
	switch arg {
	case "on", "true", "1":
		h.client.SetStepEnabled(true)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ 已开启步骤显示（会显示步骤开始/完成）"))
		return nil, nil
	case "off", "false", "0":
		h.client.SetStepEnabled(false)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ 已关闭步骤显示"))
		return nil, nil
	default:
		msg := "❌ 命令格式错误\n\n使用方法:\n/steps - 查看状态\n/steps on - 开启\n/steps off - 关闭"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}
}

// handleDevCore 处理 Dev Core 提示词设置命令（全局）
func (h *Handler) handleDevCore(ctx context.Context, data *chatbot.BotCallbackDataModel, content string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()
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
		msg := fmt.Sprintf("🧩 Dev Core 状态: %s（仅会话首条消息注入）\n\n当前提示词:\n%s\n\n使用方法:\n/devcore <自然语言偏好>\n/devcore on\n/devcore off\n/devcore set <提示词>\n/devcore reset", status, prompt)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	arg := strings.ToLower(parts[1])
	switch arg {
	case "on", "true", "1":
		if strings.TrimSpace(h.client.GetDevCorePrompt()) == "" {
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 当前未设置 Dev Core 提示词\n\n请先使用:\n/devcore <自然语言偏好>\n或 /devcore set <提示词>"))
			return nil, nil
		}
		h.client.SetDevCoreEnabled(true)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ 已开启 Dev Core（仅会话首条消息注入）"))
		return nil, nil
	case "off", "false", "0":
		h.client.SetDevCoreEnabled(false)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ 已关闭 Dev Core 注入"))
		return nil, nil
	case "reset":
		h.client.ResetDevCorePrompt()
		h.client.SetDevCoreEnabled(false)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ 已清空 Dev Core 提示词，并关闭注入（默认状态）"))
		return nil, nil
	case "set":
		if len(parts) < 3 {
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 命令格式错误\n\n使用方法:\n/devcore set <提示词>"))
			return nil, nil
		}
		prompt := strings.TrimSpace(strings.TrimPrefix(content, parts[0]+" "+parts[1]))
		if prompt == "" {
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 提示词不能为空"))
			return nil, nil
		}
		h.client.SetDevCorePrompt(prompt)
		h.client.SetDevCoreEnabled(true)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ Dev Core 提示词已更新（从新会话首条消息生效）"))
		return nil, nil
	default:
		// 支持自然语言直接设置：/devcore 我希望回答更简洁...
		prompt := strings.TrimSpace(strings.TrimPrefix(raw, parts[0]))
		if prompt == "" {
			msg := "❌ 命令格式错误\n\n使用方法:\n/devcore\n/devcore <自然语言偏好>\n/devcore status\n/devcore on\n/devcore off\n/devcore set <提示词>\n/devcore reset"
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
			return nil, nil
		}
		h.client.SetDevCorePrompt(prompt)
		h.client.SetDevCoreEnabled(true)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ Dev Core 偏好已通过自然语言设置（从新会话首条消息生效）"))
		return nil, nil
	}
}

// handleAnswer 处理回答问题命令
func (h *Handler) handleAnswer(ctx context.Context, data *chatbot.BotCallbackDataModel, userID, content string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	// 解析命令:
	// 1) /answer <answer>
	// 2) /answer <questionID> <answer>
	parts := strings.Fields(content)
	if len(parts) < 2 {
		msg := h.buildPendingRequirementHint(userID)
		if msg == "" {
			msg = "❌ 当前没有待确认问题。请先等待 OpenCode 提问后直接回复选项内容（如：1、允许、yes）。"
		}
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	var questionID, answer string
	if len(parts) >= 3 && (strings.HasPrefix(parts[1], "q_") || strings.HasPrefix(parts[1], "que_") || strings.HasPrefix(parts[1], "per_")) {
		questionID = parts[1]
		answer = strings.Join(parts[2:], " ")
	} else {
		sessionID, ok := h.adapter.GetSessionForUser(userID)
		if !ok {
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 当前没有活跃会话，无法定位待确认问题"))
			return nil, nil
		}

		if permission, ok := h.client.GetLatestPendingPermission(sessionID); ok {
			questionID = permission.ID
		} else if question, ok := h.client.GetLatestPendingQuestion(sessionID); ok {
			questionID = question.ID
		} else {
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 当前会话没有待确认问题"))
			return nil, nil
		}
		answer = strings.Join(parts[1:], " ")
	}

	// 获取问题
	question, ok := h.client.GetPendingQuestion(questionID)
	if !ok {
		msg := fmt.Sprintf("❌ 找不到问题 %s\n\n可能原因:\n• 问题已被回答\n• 问题ID不正确\n• 问题已过期", questionID)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	if h.isNonOwnerReadOnly(userID) && strings.HasPrefix(questionID, "per_") {
		log.Printf("dingtalk: read-only user %s attempted to answer permission %s with '%s', force reject", userID, questionID, answer)
		if err := h.client.AnswerQuestion(ctx, questionID, "拒绝"); err != nil {
			msg := fmt.Sprintf("❌ 只读策略拒绝权限时失败: %v", err)
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
			return nil, err
		}
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("🔒 当前为只读模式（非 owner），已自动拒绝本次写权限请求"))
		return nil, nil
	}

	// 如果有选项，验证答案
	if len(question.Options) > 0 {
		// 尝试解析为数字索引
		if idx, err := strconv.Atoi(answer); err == nil {
			if idx < 1 || idx > len(question.Options) {
				msg := fmt.Sprintf("❌ 选项序号无效：%d\n\n请选择 1-%d 之间的序号", idx, len(question.Options))
				_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
				return nil, nil
			}
			// 使用实际选项文本
			answer = question.Options[idx-1]
		}
	}

	// 提交答案
	log.Printf("dingtalk: submitting answer '%s' for question %s", answer, questionID)

	if err := h.client.AnswerQuestion(ctx, questionID, answer); err != nil {
		msg := fmt.Sprintf("❌ 提交答案失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		log.Printf("dingtalk: failed to answer question %s: %v", questionID, err)
		return nil, err
	}

	msg := fmt.Sprintf("✅ 已提交答案: %s\n\n⏳ 等待 OpenCode 继续执行...", answer)
	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
	log.Printf("dingtalk: answered question %s successfully", questionID)

	return nil, nil
}

func (h *Handler) buildPendingRequirementHint(userID string) string {
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		return ""
	}

	if permission, ok := h.client.GetLatestPendingPermission(sessionID); ok {
		return fmt.Sprintf("OpenCode 需要确认：\n%s\n\n请直接回复：允许 / 拒绝 / 始终允许", permission.Text)
	}
	if question, ok := h.client.GetLatestPendingQuestion(sessionID); ok {
		var b strings.Builder
		b.WriteString("OpenCode 需要选择：\n")
		if question.Text != "" {
			b.WriteString(question.Text)
			b.WriteString("\n")
		}
		if len(question.Questions) > 0 {
			for _, q := range question.Questions {
				if q.Header != "" {
					b.WriteString("\n")
					b.WriteString(q.Header)
					b.WriteString("\n")
				}
				if q.Question != "" {
					b.WriteString(q.Question)
					b.WriteString("\n")
				}
				for i, opt := range q.Options {
					b.WriteString(fmt.Sprintf("%d. %s\n", i+1, opt.Label))
				}
			}
		} else if len(question.Options) > 0 {
			for i, opt := range question.Options {
				b.WriteString(fmt.Sprintf("%d. %s\n", i+1, opt))
			}
		}
		b.WriteString("\n请直接回复选项内容（无需输入 /answer）")
		return b.String()
	}

	return ""
}

// isQuickReply 检查是否是快捷回复（权限回复或问题选项）
func (h *Handler) isQuickReply(content string) bool {
	if replyToPermissionResponse(content) != "" {
		return true
	}

	lower := normalizePermissionReplyText(content)

	// 数字选项（1-9）
	if len(lower) == 1 && lower[0] >= '1' && lower[0] <= '9' {
		return true
	}

	// 多问题格式：分号分隔的多个答案，如 "1;2,3;1" 或 "1.1;2.2"
	// 检查是否符合 "数字/带点的数字;数字/带点的数字;..." 格式
	if strings.Contains(lower, ";") {
		parts := strings.Split(lower, ";")
		allValid := true
		for _, part := range parts {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			// 检查每部分是否是数字、逗号分隔的数字、或 "数字.数字" 格式
			subParts := strings.Split(part, ",")
			for _, sub := range subParts {
				sub = strings.TrimSpace(sub)
				if sub == "" {
					continue
				}
				// 支持 "1.1" 格式（问题编号.选项编号）
				if strings.Contains(sub, ".") {
					dotParts := strings.Split(sub, ".")
					if len(dotParts) != 2 {
						allValid = false
						break
					}
					// 检查点号两边是否都是数字
					if _, err := strconv.Atoi(dotParts[0]); err != nil {
						allValid = false
						break
					}
					if _, err := strconv.Atoi(dotParts[1]); err != nil {
						allValid = false
						break
					}
				} else {
					// 普通数字格式
					if _, err := strconv.Atoi(sub); err != nil {
						allValid = false
						break
					}
				}
			}
			if !allValid {
				break
			}
		}
		if allValid {
			return true
		}
	}

	return false
}

// replyToPermissionResponse maps any form of user permission reply to the canonical English
// API value: "once" (allow once), "reject" (deny), "always" (always allow), or "" (unrecognized).
func replyToPermissionResponse(content string) string {
	normalized := normalizePermissionReplyText(content)

	alwaysTokens := []string{"3", "always", "始终允许", "始终", "一直允许", "总是允许", "濮嬬粓鍏佽", "濮嬬粓", "涓€鐩村厑璁", "鎬绘槸鍏佽"}
	rejectTokens := []string{"2", "deny", "reject", "no", "n", "拒绝", "不同意", "不允许", "取消", "鎷掔粷", "涓嶅悓鎰", "鍙栨秷", "涓嶅厑璁"}
	allowTokens := []string{"1", "allow", "yes", "y", "ok", "okay", "允许", "同意", "确认", "可以", "行", "鍏佽", "鍚屾剰", "纭", "鍙互"}

	if containsAnyPermissionToken(normalized, alwaysTokens) {
		return "always"
	}
	if containsAnyPermissionToken(normalized, rejectTokens) {
		return "reject"
	}
	if containsAnyPermissionToken(normalized, allowTokens) {
		return "once"
	}
	return ""
}
func normalizePermissionReplyText(content string) string {
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

func containsAnyPermissionToken(text string, tokens []string) bool {
	for _, token := range tokens {
		t := normalizePermissionReplyText(token)
		if t != "" && strings.Contains(text, t) {
			return true
		}
	}
	return false
}

// handleQuickReply 处理快捷回复（权限或问题选项）
func (h *Handler) handleQuickReply(ctx context.Context, data *chatbot.BotCallbackDataModel, userID, content string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	if handled, err := h.handleTokenOverflowQuickReply(ctx, data, userID, content); handled || err != nil {
		if handled {
			return []byte("handled"), nil
		}
		return nil, err
	}

	// 获取用户的 session
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		log.Printf("dingtalk: no session found for user %s when handling quick reply '%s'", userID, content)
		return nil, nil // 没有session，让后续逻辑处理
	}

	log.Printf("dingtalk: checking quick reply '%s' for user %s (session: %s)", content, userID, sessionID[:min(8, len(sessionID))])

	// 同时读取待处理权限和问题，避免旧权限误拦截新问题回复
	permission, hasPermission := h.client.GetLatestPendingPermission(sessionID)
	question, hasQuestion := h.client.GetLatestPendingQuestion(sessionID)
	preferQuestion := false
	if hasPermission && hasQuestion && question.CreatedAt.After(permission.CreatedAt) {
		preferQuestion = true
		log.Printf("dingtalk: both pending permission(%s) and question(%s) exist, prefer newer question",
			permission.ID, question.ID)
	}

	if hasPermission && !preferQuestion {
		log.Printf("dingtalk: user %s replied '%s' (bytes=% X) to permission %s (session: %s)",
			userID, content, []byte(content), permission.ID, sessionID[:min(8, len(sessionID))])

		// 直接映射为英文 API 值，绕过所有中文解析链
		englishResponse := replyToPermissionResponse(content)
		if englishResponse == "" {
			log.Printf("dingtalk: unrecognized permission reply from %s: raw=%q bytes=% X", userID, content, []byte(content))
			if hasQuestion {
				log.Printf("dingtalk: fallback to pending question %s after permission parse miss", question.ID)
			} else {
				_ = replier.SimpleReplyText(ctx, data.SessionWebhook,
					[]byte("❌ 未能识别权限回复，请回复：允许 / 拒绝 / 始终允许"))
				return []byte("handled"), nil
			}
		}

		if englishResponse != "" {
			if h.isNonOwnerReadOnly(userID) {
				log.Printf("dingtalk: read-only user %s, forcing permission to reject", userID)
				englishResponse = "reject"
			}

			log.Printf("dingtalk: resolved permission reply '%s' -> %s for %s", content, englishResponse, permission.ID)

			if err := h.client.RespondToPermission(ctx, permission.ID, englishResponse); err != nil {
				log.Printf("dingtalk: RespondToPermission failed for %s: %v", permission.ID, err)
				_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 权限回复失败，请重试"))
				return nil, err
			}

			// For "always" / "reject", cascade the same answer to ALL pending
			// permissions for this session so the user doesn't have to reply
			// once per stacked permission.
			if englishResponse == "always" || englishResponse == "reject" {
				for _, id := range h.client.GetAllPendingPermissionIDs(sessionID) {
					if id == permission.ID {
						continue
					}
					log.Printf("dingtalk: cascading permission response '%s' to %s", englishResponse, id)
					if err := h.client.RespondToPermission(ctx, id, englishResponse); err != nil {
						log.Printf("dingtalk: cascade permission %s failed: %v", id, err)
					}
				}
			}

			displayMap := map[string]string{"once": "允许", "reject": "拒绝", "always": "始终允许"}
			msg := fmt.Sprintf("✅ 已回复: %s\n\n⏳ 等待 OpenCode 继续执行...", displayMap[englishResponse])
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
			log.Printf("dingtalk: successfully responded to permission %s (%s)", permission.ID, englishResponse)
			return []byte("handled"), nil
		}
	}

	// 再尝试查找待处理的普通问题
	if hasQuestion {
		log.Printf("dingtalk: user %s replied '%s' to question %s", userID, content, question.ID)
		log.Printf("dingtalk: question details - ID: %s, Options count: %d, Questions count: %d",
			question.ID, len(question.Options), len(question.Questions))

		// 解析答案 - 支持多种格式：
		// 1. 纯数字 "1" -> 选择第一个选项
		// 2. 多问题格式 "1;2" -> 第一个问题选1，第二个问题选2
		// 3. 选项文本 -> 直接使用
		answer := content

		// 如果内容包含分号，可能是多问题格式
		if strings.Contains(content, ";") {
			// 保持原样，AnswerQuestion 会解析分号格式
			log.Printf("dingtalk: using multi-question answer format: %s", content)
		} else if strings.Contains(content, ",") {
			// 单问题多选，保持原样
			log.Printf("dingtalk: using multi-select answer format: %s", content)
		} else if idx, err := strconv.Atoi(strings.TrimSpace(content)); err == nil {
			// 纯数字，转换为对应选项
			log.Printf("dingtalk: numeric input '%s', converting to option", content)

			// 优先使用 Questions 数组（新版格式）
			if len(question.Questions) > 0 && len(question.Questions[0].Options) > 0 {
				qi := question.Questions[0]
				if idx >= 1 && idx <= len(qi.Options) {
					answer = qi.Options[idx-1].Label
					log.Printf("dingtalk: converted %d -> %s (from Questions array)", idx, answer)
				} else {
					log.Printf("dingtalk: index %d out of range (1-%d), using original", idx, len(qi.Options))
				}
			} else if len(question.Options) > 0 {
				// 回退到简化 Options 数组
				if idx >= 1 && idx <= len(question.Options) {
					answer = question.Options[idx-1]
					log.Printf("dingtalk: converted %d -> %s (from Options array)", idx, answer)
				} else {
					log.Printf("dingtalk: index %d out of range (1-%d), using original", idx, len(question.Options))
				}
			}
		} else {
			// 文本输入，检查是否是有效的选项标签
			log.Printf("dingtalk: using text input as answer: %s", content)
		}

		// ★ Validate the answer before consuming the question. If the user's
		// input doesn't match any option, it's likely an unrelated message —
		// don't pollute the question with a wrong answer.
		if !question.IsValidAnswer(content) {
			log.Printf("dingtalk: input '%s' is not a valid answer for question %s, ignoring", content, question.ID)
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook,
				[]byte(fmt.Sprintf("⚠️ 回复未能匹配问题选项，请回复选项编号或关键词")))
			return []byte("handled"), nil
		}

		log.Printf("dingtalk: submitting answer '%s' for question %s (original input: %s)", answer, question.ID, content)

		if err := h.client.AnswerQuestion(ctx, question.ID, answer); err != nil {
			msg := fmt.Sprintf("❌ 回复失败: %v\n\n问题ID: %s\n答案: %s", err, question.ID, answer)
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
			log.Printf("dingtalk: failed to answer question %s: %v", question.ID, err)
			return nil, err
		}

		msg := fmt.Sprintf("✅ 已回复: %s\n\n⏳ 等待 OpenCode 继续执行...", answer)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		log.Printf("dingtalk: successfully answered question %s", question.ID)
		// 返回非空结果表示已处理，阻止后续继续发送给OpenCode
		return []byte("handled"), nil
	}

	// 没有待处理的问题，返回 nil 让后续逻辑处理
	log.Printf("dingtalk: no pending question/permission found for session %s, treating '%s' as normal message",
		sessionID[:min(8, len(sessionID))], content)
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

func (h *Handler) isTokenOverflowErrorChunk(chunk string) bool {
	msg := strings.ToLower(strings.TrimSpace(chunk))
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
		"5. 总是新会话并继续\n\n" +
		"也可用命令：\n" +
		"/summary 或 /new"
}

func (h *Handler) getTokenOverflowPolicy(userID string) string {
	if value, ok := h.overflowPolicy.Load(strings.TrimSpace(userID)); ok {
		if policy, ok := value.(string); ok {
			policy = strings.TrimSpace(policy)
			if policy == tokenOverflowPolicySummary || policy == tokenOverflowPolicyNew {
				return policy
			}
		}
	}
	return tokenOverflowPolicyAsk
}

func (h *Handler) setTokenOverflowPolicy(userID, policy string) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return
	}
	if policy != tokenOverflowPolicySummary && policy != tokenOverflowPolicyNew {
		h.overflowPolicy.Delete(userID)
		return
	}
	h.overflowPolicy.Store(userID, policy)
}

func (h *Handler) storeTokenOverflowPending(userID string, state *tokenOverflowPendingState) {
	if state == nil {
		return
	}
	h.overflowPending.Store(strings.TrimSpace(userID), state)
}

func (h *Handler) getTokenOverflowPending(userID string) (*tokenOverflowPendingState, bool) {
	value, ok := h.overflowPending.Load(strings.TrimSpace(userID))
	if !ok {
		return nil, false
	}
	state, ok := value.(*tokenOverflowPendingState)
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

func (h *Handler) handleTokenOverflowQuickReply(ctx context.Context, data *chatbot.BotCallbackDataModel, userID, content string) (bool, error) {
	state, ok := h.getTokenOverflowPending(userID)
	if !ok {
		return false, nil
	}

	// 如果已经在执行重试，阻止新消息并提示用户等待
	if state.Executing {
		replier := chatbot.NewChatbotReplier()
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("⏳ 上下文压缩/重试正在进行中，请稍候..."))
		return true, nil
	}

	replier := chatbot.NewChatbotReplier()
	decision, setAlways, recognized := parseTokenOverflowDecision(content)
	if !recognized {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("请回复 1/2/3/4/5（或对应中文选项）来处理上下文超限问题。"))
		return true, nil
	}

	if decision == "cancel" {
		h.clearTokenOverflowPending(userID)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ 已取消本次继续处理。你可以手动发送 /summary、/new 或重发消息。"))
		return true, nil
	}

	if setAlways {
		if decision == "summary" {
			h.setTokenOverflowPolicy(userID, tokenOverflowPolicySummary)
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ 已设置为：总是压缩并继续。正在处理本条消息..."))
		} else if decision == "new" {
			h.setTokenOverflowPolicy(userID, tokenOverflowPolicyNew)
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("✅ 已设置为：总是新会话并继续。正在处理本条消息..."))
		}
	}

	h.storeTokenOverflowPending(userID, state)
	go h.executeTokenOverflowDecision(context.Background(), userID, decision)
	return true, nil
}

func parseTokenOverflowDecision(content string) (decision string, setAlways bool, recognized bool) {
	normalized := normalizePermissionReplyText(content)
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

	if containsAnyPermissionToken(normalized, alwaysSummaryTokens) {
		return "summary", true, true
	}
	if containsAnyPermissionToken(normalized, alwaysNewTokens) {
		return "new", true, true
	}
	if containsAnyPermissionToken(normalized, summaryTokens) {
		return "summary", false, true
	}
	if containsAnyPermissionToken(normalized, newTokens) {
		return "new", false, true
	}

	return "", false, false
}

func (h *Handler) executeTokenOverflowDecision(ctx context.Context, userID, decision string) {
	state, ok := h.getTokenOverflowPending(userID)
	if !ok {
		return
	}

	// 标记为执行中，防止用户新消息创建并发请求
	state.Executing = true
	h.storeTokenOverflowPending(userID, state)

	decision = strings.TrimSpace(decision)
	if decision == "" {
		decision = "summary"
	}

	timeout := 20 * time.Minute
	if strings.TrimSpace(state.Agent) != "" {
		timeout = 30 * time.Minute
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if decision == "summary" {
		sessionID := strings.TrimSpace(state.SessionID)
		if sessionID == "" {
			if sid, exists := h.adapter.GetSessionForUser(state.UserID); exists {
				sessionID = sid
			}
		}
		if sessionID == "" {
			_ = h.sendReplyBySource(runCtx, state.SessionWebhook, state.ConversationType, state.ThreadID, state.UserID, "❌ 无法定位当前会话，已改为新会话继续。")
			decision = "new"
		} else {
			// 清除已总结标记，允许再次压缩（会话可能已再次溢出）
			h.client.ClearSessionSummary(sessionID)
			// 重置 gateway 侧 token 计数，避免下一条消息的预判因残留计数误判
			h.client.ResetSessionTokenCount(sessionID)
			if err := h.client.SummarizeSession(runCtx, sessionID); err != nil {
				_ = h.sendReplyBySource(runCtx, state.SessionWebhook, state.ConversationType, state.ThreadID, state.UserID, fmt.Sprintf("❌ 自动压缩失败：%v", err))
				h.clearTokenOverflowPending(userID)
				return
			}
			_ = h.sendReplyBySource(runCtx, state.SessionWebhook, state.ConversationType, state.ThreadID, state.UserID, "✅ 压缩完成，正在重试消息...")
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

	// 使用流式模式重试，复用 AI Card 路径避免每个 delta 变成独立消息
	preferAICard := h.shouldPreferAICardForOpenCodeReply()
	sendRetryReply := func(msg string) error {
		if preferAICard {
			return h.sendReplyWithAICardFallback(runCtx, state.SessionWebhook, state.ConversationType, state.ThreadID, state.UserID, msg)
		}
		return h.sendReplyBySource(runCtx, state.SessionWebhook, state.ConversationType, state.ThreadID, state.UserID, msg)
	}

	var retryBuf strings.Builder
	var retryLastSent int
	retryLastUpdate := time.Now()
	const retryMinInterval = 5 * time.Second
	const retryMinChars = 300

	retryCallback := func(chunk string) error {
		trimmed := strings.TrimSpace(chunk)
		if trimmed == "" {
			return nil
		}
		if strings.HasPrefix(trimmed, "ses_") && len(trimmed) < 100 {
			h.adapter.MapUserToSession(state.UserID, trimmed)
			h.adapter.MapSessionData(trimmed, "channel", state.SessionWebhook)
			return nil
		}
		if strings.HasPrefix(chunk, opencode.ThinkingSignalPrefix) {
			return nil
		}
		// FlushSignal: 发送所有剩余未发送的内容
		if chunk == opencode.FlushSignal {
			unsent := retryBuf.String()[retryLastSent:]
			if len(unsent) > 0 {
				if preferAICard {
					// AI Card 路径会完整更新，跳过此处；由 streaming end 发送
					log.Printf("dingtalk stream: retry flush: deferring %d bytes to AI Card final path", len(unsent))
				} else {
					if err := sendRetryReply(unsent); err != nil {
						log.Printf("dingtalk stream: retry flush send failed: %v", err)
					} else {
						retryLastSent = retryBuf.Len()
					}
				}
			}
			return nil
		}
		// 工具/步骤等立即发送的特殊消息
		if strings.HasPrefix(chunk, opencode.ToolSignalPrefix) ||
			strings.HasPrefix(chunk, opencode.StepSignalPrefix) ||
			strings.HasPrefix(chunk, opencode.TodoSignalPrefix) ||
			strings.HasPrefix(chunk, opencode.QuestionSignalPrefix) ||
			strings.HasPrefix(chunk, opencode.WaitHintSignalPrefix) {
			sigMsg := chunk
			for _, p := range []string{opencode.ToolSignalPrefix, opencode.StepSignalPrefix, opencode.TodoSignalPrefix, opencode.QuestionSignalPrefix, opencode.WaitHintSignalPrefix} {
				sigMsg = strings.TrimPrefix(sigMsg, p)
			}
			if strings.TrimSpace(sigMsg) != "" {
				_ = sendRetryReply(strings.TrimSpace(sigMsg))
			}
			return nil
		}
		// 立即发送的交互式提示消息
		if strings.HasPrefix(chunk, "⏳") || strings.HasPrefix(chunk, "⏱️") ||
			strings.HasPrefix(chunk, "🔐") || strings.HasPrefix(chunk, "❓") ||
			strings.HasPrefix(chunk, "🤔💭") {
			return sendRetryReply(chunk)
		}

		// 累积内容，按间隔批量发送
		retryBuf.WriteString(chunk)
		newLen := retryBuf.Len() - retryLastSent
		now := time.Now()
		if !preferAICard && newLen >= retryMinChars && now.Sub(retryLastUpdate) >= retryMinInterval {
			if err := sendRetryReply(retryBuf.String()); err == nil {
				retryLastSent = retryBuf.Len()
				retryLastUpdate = now
			}
		}
		return nil
	}

	response, err := h.client.SendMessageStreaming(runCtx, opencode.MessagePayload{
		Channel:     "dingtalk",
		UserID:      state.UserID,
		ThreadID:    state.ThreadID,
		SessionID:   state.SessionID,
		Content:     state.Content,
		Agent:       state.Agent,
		Streaming:   true,
		Attachments: append([]opencode.Attachment(nil), state.Attachments...),
		Metadata:    cloneStringMap(state.Metadata),
	}, retryCallback)
	if err != nil {
		_ = h.sendReplyBySource(runCtx, state.SessionWebhook, state.ConversationType, state.ThreadID, state.UserID,
			fmt.Sprintf("❌ 已尝试%s后重试，但仍失败：%v", tokenOverflowDecisionLabel(decision), err))
		h.clearTokenOverflowPending(userID)
		return
	}

	// 发送剩余未发送的缓冲内容
	if unsent := retryBuf.String()[retryLastSent:]; len(unsent) > 0 {
		if err := sendRetryReply(unsent); err != nil {
			log.Printf("dingtalk stream: retry final send failed: %v", err)
		}
	}

	if response.SessionID != "" {
		h.adapter.MapUserToSession(state.UserID, response.SessionID)
		h.adapter.MapSessionData(response.SessionID, "channel", state.SessionWebhook)
	}

	h.clearTokenOverflowPending(userID)
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

// handleCronTask 处理定时任务命令
func (h *Handler) handleCronTask(ctx context.Context, data *chatbot.BotCallbackDataModel, userID, content string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	// 检查是否设置了cronScheduler
	if h.cronScheduler == nil {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 定时任务功能未启用"))
		return nil, nil
	}

	// 解析命令
	parts := strings.Fields(content)
	if len(parts) < 2 {
		return h.sendCronTaskHelp(ctx, data)
	}

	subCommand := parts[1]

	switch subCommand {
	case "add", "create", "新增":
		return h.handleCronTaskAdd(ctx, data, userID, parts[2:])
	case "list", "ls", "列表":
		return h.handleCronTaskList(ctx, data)
	case "delete", "del", "rm", "删除":
		return h.handleCronTaskDelete(ctx, data, parts[2:])
	case "enable", "启用":
		return h.handleCronTaskEnable(ctx, data, parts[2:])
	case "disable", "禁用":
		return h.handleCronTaskDisable(ctx, data, parts[2:])
	case "info", "详情":
		return h.handleCronTaskInfo(ctx, data, parts[2:])
	case "help", "帮助":
		return h.sendCronTaskHelp(ctx, data)
	default:
		// 支持 /crontask <自然语言描述> 的兜底解析。
		nlText := strings.TrimSpace(strings.TrimPrefix(content, "/crontask"))
		if handled, err := h.tryHandleNLScheduleOpt(ctx, data, userID, nlText, true); handled || err != nil {
			return nil, err
		}
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 未知的子命令，使用 /crontask help 查看帮助"))
		return nil, nil
	}
}

func (h *Handler) tryHandleNLSchedule(ctx context.Context, data *chatbot.BotCallbackDataModel, userID, text string) (bool, error) {
	return h.tryHandleNLScheduleOpt(ctx, data, userID, text, false)
}

func (h *Handler) tryHandleNLScheduleOpt(ctx context.Context, data *chatbot.BotCallbackDataModel, userID, text string, forceCreate bool) (bool, error) {
	if h.nlScheduleSvc == nil || strings.TrimSpace(text) == "" {
		return false, nil
	}
	if !forceCreate && !scheduler.ShouldTryNLScheduleText(text) {
		return false, nil
	}

	resp, err := h.nlScheduleSvc.HandleText(ctx, scheduler.NLScheduleRequest{
		AdapterType: "dingtalk",
		UserID:      userID,
		Channel:     data.ConversationId,
		Text:        text,
		ForceCreate: forceCreate,
		Metadata: map[string]interface{}{
			"conversation_id": data.ConversationId,
			"session_webhook": data.SessionWebhook,
		},
	})
	if err != nil {
		replier := chatbot.NewChatbotReplier()
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 定时任务处理失败: "+err.Error()))
		return true, err
	}
	if resp == nil || !resp.Handled {
		return false, nil
	}

	replier := chatbot.NewChatbotReplier()
	if strings.TrimSpace(resp.Message) != "" {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(resp.Message))
	}
	return true, nil
}

// handleCronTaskAdd 添加定时任务
func (h *Handler) handleCronTaskAdd(ctx context.Context, data *chatbot.BotCallbackDataModel, userID string, args []string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	// 解析带引号的参数: "0 */30 * * * *" "测试任务" "查看系统负载"
	parsedArgs := h.parseQuotedArgs(strings.Join(args, " "))

	if len(parsedArgs) < 3 {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(
			"❌ 参数不足\n\n"+
				"格式: /crontask add \"cron表达式\" \"任务名称\" \"任务内容\" [agent]\n"+
				"示例:\n"+
				"/crontask add \"0 0 9 * * *\" \"每日检查\" \"查看系统负载\"\n"+
				"/crontask add \"0 */30 * * * *\" \"半小时监控\" \"检查服务状态\" system_monitor",
		))
		return nil, nil
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
		Description: fmt.Sprintf("通过钉钉创建 (用户: %s)", userID),
		Type:        scheduler.TaskTypeAgent,
		CronExpr:    cronExpr,
		Enabled:     true,
		AdapterType: "dingtalk",
		Channel:     data.ConversationId,
		Content:     taskContent,
		Agent:       agent,
		Metadata: map[string]interface{}{
			"created_by":      userID,
			"created_from":    "dingtalk",
			"conversation_id": data.ConversationId,
			"session_webhook": data.SessionWebhook, // 保存webhook用于发送结果
		},
		CreatedAt: now,
		UpdatedAt: now,
	}

	// 添加到调度器
	if err := h.cronScheduler.AddScheduledTask(task); err != nil {
		errMsg := fmt.Sprintf("❌ 创建定时任务失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(errMsg))
		return nil, err
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

	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
	return nil, nil
}

// handleCronTaskList 列出定时任务
func (h *Handler) handleCronTaskList(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	tasks := h.cronScheduler.GetScheduledTasksByAdapter("dingtalk")
	if len(tasks) == 0 {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("📋 暂无定时任务\n\n使用 /crontask help 查看如何创建"))
		return nil, nil
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

	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg.String()))
	return nil, nil
}

// handleCronTaskDelete 删除定时任务
func (h *Handler) handleCronTaskDelete(ctx context.Context, data *chatbot.BotCallbackDataModel, args []string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	if len(args) < 1 {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 请指定任务ID\n\n格式: /crontask delete <任务ID>"))
		return nil, nil
	}

	taskID := args[0]

	if err := h.cronScheduler.RemoveScheduledTask(taskID); err != nil {
		errMsg := fmt.Sprintf("❌ 删除任务失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(errMsg))
		return nil, err
	}

	msg := fmt.Sprintf("✅ 任务 %s 已删除", taskID)
	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
	return nil, nil
}

// handleCronTaskEnable 启用定时任务
func (h *Handler) handleCronTaskEnable(ctx context.Context, data *chatbot.BotCallbackDataModel, args []string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	if len(args) < 1 {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 请指定任务ID\n\n格式: /crontask enable <任务ID>"))
		return nil, nil
	}

	taskID := args[0]

	if err := h.cronScheduler.EnableTask(taskID); err != nil {
		errMsg := fmt.Sprintf("❌ 启用任务失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(errMsg))
		return nil, err
	}

	msg := fmt.Sprintf("✅ 任务 %s 已启用", taskID)
	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
	return nil, nil
}

// handleCronTaskDisable 禁用定时任务
func (h *Handler) handleCronTaskDisable(ctx context.Context, data *chatbot.BotCallbackDataModel, args []string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	if len(args) < 1 {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 请指定任务ID\n\n格式: /crontask disable <任务ID>"))
		return nil, nil
	}

	taskID := args[0]

	if err := h.cronScheduler.DisableTask(taskID); err != nil {
		errMsg := fmt.Sprintf("❌ 禁用任务失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(errMsg))
		return nil, err
	}

	msg := fmt.Sprintf("⏸️ 任务 %s 已禁用", taskID)
	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
	return nil, nil
}

// handleCronTaskInfo 查看任务详情
func (h *Handler) handleCronTaskInfo(ctx context.Context, data *chatbot.BotCallbackDataModel, args []string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	if len(args) < 1 {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("❌ 请指定任务ID\n\n格式: /crontask info <任务ID>"))
		return nil, nil
	}

	taskID := args[0]

	task, err := h.cronScheduler.GetScheduledTask(taskID)
	if err != nil {
		errMsg := fmt.Sprintf("❌ 获取任务信息失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(errMsg))
		return nil, err
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

	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg.String()))
	return nil, nil
}

// sendCronTaskHelp 发送定时任务帮助信息
func (h *Handler) sendCronTaskHelp(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

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

	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(helpMsg))
	return nil, nil
}

// parseQuotedArgs 解析带引号的参数
// 例如: ""cron" "name" "content"" -> ["cron", "name", "content"]
func (h *Handler) parseQuotedArgs(input string) []string {
	var result []string
	var inQuote bool
	var current strings.Builder

	for _, r := range input {
		switch r {
		case '"':
			if inQuote {
				// 结束引号
				inQuote = false
				result = append(result, current.String())
				current.Reset()
			} else {
				// 开始引号
				inQuote = true
			}
		case ' ', '\t':
			if inQuote {
				// 引号内的空格保留
				current.WriteRune(r)
			} else if current.Len() > 0 {
				// 引号外的空格，结束当前词
				result = append(result, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}
	}

	// 处理最后一个词
	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// sendGroupMessage 发送消息到钉钉群聊
// 使用 SessionWebhook 发送消息
func (h *Handler) sendViaWebhook(ctx context.Context, webhookURL, content string) error {
	// 使用 chatbot SDK 的 SimpleReplyText 发送消息
	replier := chatbot.NewChatbotReplier()

	err := replier.SimpleReplyText(ctx, webhookURL, []byte(content))
	if err != nil {
		log.Printf("dingtalk: failed to send message via webhook: %v", err)
		return fmt.Errorf("send via webhook: %w", err)
	}

	log.Printf("dingtalk: successfully sent message via webhook")
	return nil
}

// sendGroupMessage 发送消息到钉钉群聊 (已弃用，使用 sendViaWebhook 代替)
// 使用机器人发送群消息 API
func (h *Handler) sendGroupMessage(ctx context.Context, conversationId, content string) error {
	// 钉钉 Stream 模式下，使用机器人发送群消息 API
	// API文档: https://open.dingtalk.com/document/orgapp/chatbots-send-one-on-one-chat-messages-in-batches

	// 使用独立 context，避免父 context 取消影响消息发送
	httpCtx, httpCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer httpCancel()

	// 获取 access_token
	accessToken, err := h.getAccessToken(httpCtx)
	if err != nil {
		log.Printf("dingtalk: failed to get access token: %v", err)
		return fmt.Errorf("get access token: %w", err)
	}

	// 使用机器人发送普通消息接口
	// 注意：这个接口需要机器人在群里，并且使用 openConversationId
	apiURL := "https://api.dingtalk.com/v1.0/robot/oToMessages/batchSend"

	payload := map[string]interface{}{
		"robotCode":           h.cfg.ClientID,
		"msgKey":              "sampleText",
		"msgParam":            fmt.Sprintf("{\"content\":\"%s\"}", escapeJSON(content)),
		"openConversationIds": []string{conversationId},
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(httpCtx, http.MethodPost, apiURL, strings.NewReader(string(data)))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", accessToken)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		log.Printf("dingtalk: API returned status %d, body: %s", resp.StatusCode, string(bodyBytes))
		return fmt.Errorf("dingtalk API returned status: %d", resp.StatusCode)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	// 检查响应
	if code, ok := result["code"].(string); ok && code != "" && code != "0" {
		log.Printf("dingtalk: API error response: %v", result)
		return fmt.Errorf("dingtalk API error: %v, %v", code, result["message"])
	}

	log.Printf("dingtalk: successfully sent message to conversation %s", conversationId)
	return nil
}

// escapeJSON 转义 JSON 字符串中的特殊字符
func escapeJSON(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "\"", "\\\"")
	s = strings.ReplaceAll(s, "\n", "\\n")
	s = strings.ReplaceAll(s, "\r", "\\r")
	s = strings.ReplaceAll(s, "\t", "\\t")
	return s
}

// getAccessToken 获取访问令牌（带缓存，避免频繁请求）
func (h *Handler) getAccessToken(ctx context.Context) (string, error) {
	h.accessTokenMu.Lock()
	defer h.accessTokenMu.Unlock()

	// 如果缓存的 token 还有超过5分钟有效期，直接返回
	if h.accessToken != "" && time.Now().Add(5*time.Minute).Before(h.accessTokenExpiry) {
		return h.accessToken, nil
	}

	// 重新获取 access token，使用独立 context 避免父 context 取消
	apiURL := "https://api.dingtalk.com/v1.0/oauth2/accessToken"

	payload := map[string]string{
		"appKey":    h.cfg.ClientID,
		"appSecret": h.cfg.ClientSecret,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}

	// 使用独立的 context，避免父 context 取消时影响 token 获取
	reqCtx, reqCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer reqCancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, apiURL, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	var result struct {
		AccessToken string `json:"accessToken"`
		ExpireIn    int    `json:"expireIn"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return "", fmt.Errorf("decode response: %w, body: %s", err, string(body))
	}
	if result.AccessToken == "" {
		return "", fmt.Errorf("access token is empty, body: %s", string(body))
	}

	// 缓存 token
	h.accessToken = result.AccessToken
	expireIn := result.ExpireIn
	if expireIn <= 0 {
		expireIn = 7200 // 默认2小时
	}
	h.accessTokenExpiry = time.Now().Add(time.Duration(expireIn) * time.Second)
	log.Printf("dingtalk: got new access token, expires in %ds", expireIn)

	return h.accessToken, nil
}

// sendReplyRobot 通过钉钉 Robot API 发送消息（不依赖过期的 sessionWebhook）
// 使用接口：POST /v1.0/robot/oToMessages/batchSend
func (h *Handler) sendReplyRobot(ctx context.Context, conversationID, userID, content string) error {
	if content == "" {
		return nil
	}

	// 使用独立的 context，避免父 context 取消时影响消息发送
	httpCtx, httpCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer httpCancel()

	accessToken, err := h.getAccessToken(httpCtx)
	if err != nil {
		return fmt.Errorf("get access token: %w", err)
	}

	apiURL := "https://api.dingtalk.com/v1.0/robot/oToMessages/batchSend"
	payload := map[string]interface{}{
		"robotCode":           h.cfg.ClientID,
		"msgKey":              "sampleText",
		"msgParam":            fmt.Sprintf(`{"content":%s}`, jsonStringEscape(content)),
		"userIds":             []string{userID},
		"openConversationIds": []string{conversationID},
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(httpCtx, http.MethodPost, apiURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", accessToken)

	httpClient := &http.Client{Timeout: 15 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("dingtalk robot API status %d: %s", resp.StatusCode, string(body))
	}

	log.Printf("dingtalk stream: ✅ sent via robot API (%d chars) to conversation %s",
		len(content), conversationID[:min(8, len(conversationID))])
	return nil
}

func (h *Handler) sendReplyBySource(ctx context.Context, sessionWebhook, conversationType, conversationID, userID, content string) error {
	if content == "" {
		return nil
	}

	routeToGroup := isGroupConversation(conversationType)
	log.Printf("dingtalk stream: route reply conversationType=%q -> %s (content_len=%d)",
		conversationType,
		func() string {
			if routeToGroup {
				return "group(webhook)"
			}
			return "single(robot-api)"
		}(),
		len(content),
	)

	if routeToGroup {
		if strings.TrimSpace(sessionWebhook) == "" {
			return fmt.Errorf("group conversation but sessionWebhook is empty")
		}
		return h.sendViaWebhook(ctx, sessionWebhook, content)
	}

	return h.sendReplyRobot(ctx, conversationID, userID, content)
}

func (h *Handler) shouldPreferAICardForOpenCodeReply() bool {
	cardCfg := getCardSendConfig()
	if !cardCfg.Enabled {
		return false
	}
	return strings.TrimSpace(h.cfg.ClientID) != "" && strings.TrimSpace(h.cfg.ClientSecret) != ""
}

func (h *Handler) sendReplyWithAICardFallback(ctx context.Context, sessionWebhook, conversationType, conversationID, userID, content string) error {
	if strings.TrimSpace(content) == "" {
		return nil
	}

	cardCfg := getCardSendConfig()
	if !cardCfg.Enabled {
		log.Printf("dingtalk stream: send_mode=fallback reason=aicard_disabled content_len=%d", len(content))
		return h.sendReplyBySource(ctx, sessionWebhook, conversationType, conversationID, userID, content)
	}

	cardCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := SendStreamingAICard(cardCtx, h.cfg.ClientID, h.cfg.ClientSecret, userID, content); err == nil {
		log.Printf("dingtalk stream: send_mode=aicard content_len=%d", len(content))
		return nil
	} else {
		log.Printf("dingtalk stream: send_mode=fallback reason=aicard_error content_len=%d err=%v", len(content), err)
		if !cardCfg.AutoDowngrade {
			return fmt.Errorf("aicard send failed and auto downgrade disabled: %w", err)
		}
	}

	return h.sendReplyBySource(ctx, sessionWebhook, conversationType, conversationID, userID, content)
}

func isGroupConversation(conversationType string) bool {
	t := strings.ToLower(strings.TrimSpace(conversationType))
	if t == "" {
		return false
	}
	if t == "2" || t == "group" || t == "group_chat" || t == "groupchat" {
		return true
	}
	return strings.Contains(t, "group")
}

// jsonStringEscape 将字符串编码为 JSON 字符串值（含引号）
func jsonStringEscape(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// ========== New Command Handlers ==========

// handleNewSession 处理创建新会话命令.
// 如果当前有活跃 session，会先将其消息历史压缩保存为 pending handoff，
// 这样下一条消息会自动携带上下文摘要进入新 session。
func (h *Handler) handleNewSession(ctx context.Context, data *chatbot.BotCallbackDataModel, userID string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	// 获取当前session（如果有）
	var oldSessionID string
	if sid, ok := h.adapter.GetSessionForUser(userID); ok {
		oldSessionID = sid
	}

	threadID := data.ConversationId
	var contextSaved bool
	if oldSessionID != "" && threadID != "" {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte("⏳ 正在保存当前会话上下文..."))
		saveCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		var err error
		contextSaved, err = h.client.SaveHandoffAndReset(saveCtx, threadID, "dingtalk", userID, oldSessionID)
		cancel()
		if err != nil {
			log.Printf("dingtalk: handleNewSession: SaveHandoffAndReset failed: %v", err)
		}
	} else if threadID != "" {
		h.client.ResetSession(threadID)
	}
	h.adapter.ClearSessionForUser(userID)

	var msg string
	if contextSaved {
		msg = "✅ 已重置会话，上下文已保存\n\n"
		if oldSessionID != "" {
			msg += fmt.Sprintf("旧会话: %s\n", oldSessionID[:min(8, len(oldSessionID))])
		}
		msg += "下一条消息将自动开启新会话并携带上下文摘要，直接发送你想继续的任务即可。"
	} else {
		msg = "✅ 已重置会话\n\n"
		if oldSessionID != "" {
			msg += fmt.Sprintf("旧会话: %s\n", oldSessionID[:min(8, len(oldSessionID))])
		}
		msg += "下次发送消息将创建新会话"
	}

	if err := replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg)); err != nil {
		log.Printf("dingtalk: failed to send new session response: %v", err)
		return nil, err
	}

	return nil, nil
}

// handleRetry 处理 /retry 命令
func (h *Handler) handleRetry(ctx context.Context, data *chatbot.BotCallbackDataModel, userID, subCmd string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()
	send := func(msg string) {
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
	}
	if h.retryStore == nil || h.retryWorker == nil {
		send("ℹ️ 离线重试队列未启用（RETRY_QUEUE_ENABLED=true 可开启）")
		return nil, nil
	}
	switch strings.TrimSpace(subCmd) {
	case "now":
		send("⏳ 正在处理离线重试队列...")
		go func() {
			runCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer cancel()
			succ, total, err := h.retryWorker.RunOnce(runCtx)
			var result string
			if err != nil {
				result = fmt.Sprintf("❌ 重试执行出错: %v", err)
			} else if total == 0 {
				result = "✅ 队列为空，没有需要重试的请求"
			} else {
				result = fmt.Sprintf("✅ 重试完成：成功 %d / 总计 %d", succ, total)
			}
			notifyCtx, notifyCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer notifyCancel()
			_ = h.adapter.SendToUserInChannel(notifyCtx, data.SessionWebhook, userID, result)
		}()
	case "clear":
		n, err := h.retryStore.ClearRetryQueue("pending")
		if err != nil {
			send(fmt.Sprintf("❌ 清空队列失败: %v", err))
			return nil, err
		}
		send(fmt.Sprintf("✅ 已清空 %d 条待重试请求", n))
	default:
		send(h.retryWorker.StatusSummary(ctx) +
			"\n\n命令：\n/retry now - 立即处理队列\n/retry clear - 清空等待中的请求")
	}
	return nil, nil
}

// handleListSessions 处理列出所有会话命令
func (h *Handler) handleListSessions(ctx context.Context, data *chatbot.BotCallbackDataModel) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	sessions, err := h.client.ListSessions(ctx)
	if err != nil {
		msg := fmt.Sprintf("❌ 获取会话列表失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, err
	}

	if len(sessions) == 0 {
		msg := "📝 当前没有活跃的会话"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	var msgBuilder strings.Builder
	msgBuilder.WriteString(fmt.Sprintf("📝 会话列表 (%d个):\n\n", len(sessions)))

	// 只显示最近的10个
	maxShow := 10
	if len(sessions) > maxShow {
		sessions = sessions[:maxShow]
	}

	for i, session := range sessions {
		msgBuilder.WriteString(fmt.Sprintf("%d. %s\n", i+1, session.Title))
		msgBuilder.WriteString(fmt.Sprintf("   ID: %s\n", session.ID[:8]))
		msgBuilder.WriteString(fmt.Sprintf("   目录: %s\n", session.Directory))
		updatedTime := time.Unix(int64(session.Time.Updated), 0).Format("2006-01-02 15:04")
		msgBuilder.WriteString(fmt.Sprintf("   更新: %s\n", updatedTime))
		msgBuilder.WriteString("\n")
	}

	if len(sessions) == maxShow {
		msgBuilder.WriteString(fmt.Sprintf("\n💡 只显示最近%d个会话", maxShow))
	}

	if err := replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msgBuilder.String())); err != nil {
		log.Printf("dingtalk: failed to send sessions list: %v", err)
		return nil, err
	}

	return nil, nil
}

// handleStatus 处理查看会话状态命令
func (h *Handler) handleStatus(ctx context.Context, data *chatbot.BotCallbackDataModel, userID string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	// 获取当前session
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		msg := "ℹ️ 当前没有活跃的会话\n\n发送消息将自动创建新会话"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	// 获取session详情
	info, err := h.client.GetSessionInfo(ctx, sessionID)
	if err != nil {
		msg := fmt.Sprintf("❌ 获取会话信息失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, err
	}

	var msgBuilder strings.Builder
	msgBuilder.WriteString("📊 当前会话状态:\n\n")
	msgBuilder.WriteString(fmt.Sprintf("会话ID: %s\n", info.SessionID[:8]))
	msgBuilder.WriteString(fmt.Sprintf("标题: %s\n", info.Title))
	msgBuilder.WriteString(fmt.Sprintf("目录: %s\n", info.Directory))
	msgBuilder.WriteString(fmt.Sprintf("消息数: %d\n", info.MessageCount))
	msgBuilder.WriteString(fmt.Sprintf("Token数: %d\n", info.TokenCount))
	if info.ContextLength > 0 {
		msgBuilder.WriteString(fmt.Sprintf("上下文: %d/%d (%.1f%%)\n",
			info.TokenCount, info.ContextLength, info.ContextUsage*100))
	}
	msgBuilder.WriteString(fmt.Sprintf("创建时间: %s", info.Created))

	// 追加实时处理诊断（处理中/上游重试/活跃工具等），帮助用户判断为何"正在处理中"。
	if diag := h.client.GetSessionDiagnostics(sessionID, data.ConversationId).FormatSessionStatus(); diag != "" {
		msgBuilder.WriteString("\n\n— 实时诊断 —\n")
		msgBuilder.WriteString(diag)
	}

	if err := replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msgBuilder.String())); err != nil {
		log.Printf("dingtalk: failed to send status: %v", err)
		return nil, err
	}

	return nil, nil
}

// handleSummary 处理上下文压缩命令
func (h *Handler) handleSummary(ctx context.Context, data *chatbot.BotCallbackDataModel, userID string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	// 获取当前session
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		msg := "ℹ️ 当前没有活跃的会话\n\n发送消息将自动创建新会话"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	// 发送提示消息
	msg := "⏳ 正在进行上下文压缩..."
	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))

	// 调用 summary API
	if err := h.client.SummarizeSession(ctx, sessionID); err != nil {
		errMsg := fmt.Sprintf("❌ 上下文压缩失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(errMsg))
		return nil, err
	}

	successMsg := fmt.Sprintf("✅ 上下文压缩完成\n\n会话 %s 的历史消息已被总结压缩，上下文空间已释放。", sessionID[:8])
	if err := replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(successMsg)); err != nil {
		log.Printf("dingtalk: failed to send summary response: %v", err)
		return nil, err
	}

	return nil, nil
}

// handleClear 处理清除会话命令
func (h *Handler) handleClear(ctx context.Context, data *chatbot.BotCallbackDataModel, userID string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	// 获取当前session
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		msg := "ℹ️ 当前没有活跃的会话"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	// 删除session
	if err := h.client.DeleteSession(ctx, sessionID); err != nil {
		msg := fmt.Sprintf("❌ 删除会话失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, err
	}

	// 清除本地映射
	h.adapter.ClearSessionForUser(userID)
	threadID := data.ConversationId
	if threadID != "" {
		h.client.ResetSession(threadID)
	}

	msg := fmt.Sprintf("✅ 已删除会话 %s\n\n下次发送消息将创建新会话", sessionID[:8])
	if err := replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg)); err != nil {
		log.Printf("dingtalk: failed to send clear response: %v", err)
		return nil, err
	}

	return nil, nil
}

// handleModel 处理模型配置命令
func (h *Handler) handleModel(ctx context.Context, data *chatbot.BotCallbackDataModel, userID, content string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	// 解析命令
	parts := strings.Fields(content)

	// 如果只是查询
	if len(parts) == 1 {
		return h.handleModelQuery(ctx, data, userID)
	}

	// 如果是设置: /model <provider>/<model> 或 /model <provider> <model>
	if len(parts) >= 2 {
		return h.handleModelSet(ctx, data, userID, parts[1:])
	}

	msg := "❌ 命令格式错误\n\n使用方法:\n/model - 查看可用模型\n/model <provider>/<model> - 设置模型\n/model <provider> <model> - 设置模型\n\n例如:\n/model anthropic/claude-3-opus\n/model openai gpt-4"
	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
	return nil, nil
}

// handleModelQuery 查询当前模型
func (h *Handler) handleModelQuery(ctx context.Context, data *chatbot.BotCallbackDataModel, userID string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	// 获取当前session
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		var msgBuilder strings.Builder
		msgBuilder.WriteString("ℹ️ 当前没有活跃的会话\n")

		providers, err := h.client.GetProviders(ctx)
		if err != nil {
			msgBuilder.WriteString("\n💡 可用模型列表获取失败，请稍后重试或在 OpenCode Web 界面查看\n")
			msgBuilder.WriteString(fmt.Sprintf("错误: %v", err))
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msgBuilder.String()))
			return nil, nil
		}

		if len(providers) == 0 {
			msgBuilder.WriteString("\n💡 未获取到可用模型，请在 OpenCode Web 界面查看")
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msgBuilder.String()))
			return nil, nil
		}

		msgBuilder.WriteString("\n\n📚 可用模型（示例）:\n")
		for _, p := range providers {
			msgBuilder.WriteString(fmt.Sprintf("\n【%s】\n", p.ID))
			if len(p.Models) == 0 {
				msgBuilder.WriteString("  (无模型)\n")
				continue
			}
			maxShow := min(8, len(p.Models))
			for i := 0; i < maxShow; i++ {
				msgBuilder.WriteString(fmt.Sprintf("  /model %s/%s\n", p.ID, p.Models[i].ID))
			}
			if len(p.Models) > maxShow {
				msgBuilder.WriteString(fmt.Sprintf("  ... 还有 %d 个\n", len(p.Models)-maxShow))
			}
		}

		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msgBuilder.String()))
		return nil, nil
	}

	// 获取当前provider和model
	_, _, err := h.client.GetCurrentProvider(ctx, sessionID)
	if err != nil {
		msg := fmt.Sprintf("❌ 获取当前模型失败: %v\n\n"+
			"💡 模型配置功能需要OpenCode SDK的支持\n"+
			"目前的SDK版本可能不包含此API", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, err
	}

	var msgBuilder strings.Builder
	msgBuilder.WriteString("🤖 当前会话配置:\n\n")
	msgBuilder.WriteString(fmt.Sprintf("会话: %s\n", sessionID[:8]))
	msgBuilder.WriteString("\n💡 当前会话的默认模型信息在SDK中不可直接读取\n")

	providers, err := h.client.GetProviders(ctx)
	if err != nil {
		msgBuilder.WriteString("\n可用模型列表获取失败，请稍后重试或在 OpenCode Web 界面查看\n")
		msgBuilder.WriteString(fmt.Sprintf("错误: %v", err))
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msgBuilder.String()))
		return nil, nil
	}

	if len(providers) == 0 {
		msgBuilder.WriteString("\n未获取到可用模型，请在 OpenCode Web 界面查看")
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msgBuilder.String()))
		return nil, nil
	}

	msgBuilder.WriteString("\n📚 可用模型（示例）:\n")
	for _, p := range providers {
		msgBuilder.WriteString(fmt.Sprintf("\n【%s】\n", p.ID))
		if len(p.Models) == 0 {
			msgBuilder.WriteString("  (无模型)\n")
			continue
		}
		maxShow := min(8, len(p.Models))
		for i := 0; i < maxShow; i++ {
			msgBuilder.WriteString(fmt.Sprintf("  /model %s/%s\n", p.ID, p.Models[i].ID))
		}
		if len(p.Models) > maxShow {
			msgBuilder.WriteString(fmt.Sprintf("  ... 还有 %d 个\n", len(p.Models)-maxShow))
		}
	}

	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msgBuilder.String()))
	return nil, nil
}

// handleModelSet 设置模型
func (h *Handler) handleModelSet(ctx context.Context, data *chatbot.BotCallbackDataModel, userID string, args []string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	// 获取当前session
	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		// gateway 重启后本地映射丢失，尝试从 opencode server 恢复
		if recovered, found := h.client.FindLatestSessionForUser(ctx, "dingtalk", userID); found {
			sessionID = recovered
			h.adapter.MapUserToSession(userID, sessionID)
		} else {
			msg := "❌ 当前没有活跃的会话\n\n请先发送消息创建会话，然后再设置模型"
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
			return nil, nil
		}
	}

	var providerID, modelID string

	// 解析参数: provider/model 或 provider model
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
		msg := "❌ 提供商ID不能为空"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}
	if modelID == "" {
		msg := "❌ 模型ID不能为空\n\n使用方法:\n/model <provider>/<model>\n例如:\n/model TH-AI/Kimi-K2.5"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	providers, err := h.client.GetProviders(ctx)
	if err != nil {
		log.Printf("dingtalk: failed to fetch providers for case-insensitive model resolve: %v", err)
	} else if len(providers) > 0 {
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
			msg := fmt.Sprintf("❌ 未找到提供商: %s\n\n请先执行 /model 查看可用 provider/model", providerID)
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
			return nil, nil
		}
		if !modelMatched {
			msg := fmt.Sprintf("❌ 提供商 %s 下未找到模型: %s\n\n请先执行 /model 查看可用模型", providerID, modelID)
			_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
			return nil, nil
		}
	}

	// 更新session的provider和model
	if err := h.client.UpdateSessionProvider(ctx, sessionID, providerID, modelID); err != nil {
		msg := fmt.Sprintf("❌ 更新模型失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, err
	}

	msg := fmt.Sprintf("✅ 已设置会话模型\n\n提供商: %s\n模型: %s\n会话: %s\n\n"+
		"该设置会由 gateway 在后续请求中强制携带。",
		providerID, modelID, sessionID[:8])
	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
	return nil, nil
}

// handleConfig 处理配置查看命令
func (h *Handler) handleConfig(ctx context.Context, data *chatbot.BotCallbackDataModel, userID string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	// 获取当前session
	sessionID, ok := h.adapter.GetSessionForUser(userID)

	var msgBuilder strings.Builder
	msgBuilder.WriteString("⚙️ 当前配置:\n\n")

	if ok {
		info, err := h.client.GetSessionInfo(ctx, sessionID)
		if err == nil {
			msgBuilder.WriteString(fmt.Sprintf("📊 会话信息:\n"))
			msgBuilder.WriteString(fmt.Sprintf("  ID: %s\n", info.SessionID[:8]))
			msgBuilder.WriteString(fmt.Sprintf("  标题: %s\n", info.Title))
			msgBuilder.WriteString(fmt.Sprintf("  目录: %s\n", info.Directory))
			msgBuilder.WriteString(fmt.Sprintf("  消息数: %d\n", info.MessageCount))
			msgBuilder.WriteString(fmt.Sprintf("  Token: %d/%d\n", info.TokenCount, info.ContextLength))
		}
	} else {
		msgBuilder.WriteString("📊 会话信息: 无活跃会话\n")
	}

	msgBuilder.WriteString("\n🔧 可用命令:\n")
	msgBuilder.WriteString("  /model - 查看/设置模型\n")
	msgBuilder.WriteString("  /thinking - 查看/设置 thinking 返回\n")
	msgBuilder.WriteString("  /final - 查看/设置 final-only 输出\n")
	msgBuilder.WriteString("  /steps - 查看/设置步骤显示\n")
	msgBuilder.WriteString("  /devcore - 查看/设置 Dev Core 提示词\n")
	msgBuilder.WriteString("  /status - 查看会话状态\n")
	msgBuilder.WriteString("  /new - 创建新会话\n")
	msgBuilder.WriteString("  /clear - 清除当前会话\n")
	msgBuilder.WriteString("  /fork - 派生(fork)当前会话\n")
	msgBuilder.WriteString("  /todo - 查看当前任务进度\n")
	msgBuilder.WriteString("  /diff - 查看文件变更\n")
	msgBuilder.WriteString("  /sessions - 列出所有会话\n")
	msgBuilder.WriteString("  /skills - 查看可用技能\n")
	msgBuilder.WriteString("  /help - 查看帮助")

	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msgBuilder.String()))
	return nil, nil
}

// handleUndo 处理撤销命令
// 对应 TUI 中的 /undo 操作
func (h *Handler) handleUndo(ctx context.Context, data *chatbot.BotCallbackDataModel, userID string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		msg := "ℹ️ 当前没有活跃的会话\n\n发送消息将自动创建新会话"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	session, err := h.client.RevertSession(ctx, sessionID, "")
	if err != nil {
		msg := fmt.Sprintf("❌ 撤销失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, err
	}

	msg := fmt.Sprintf("↩️ 已撤销上一次操作\n\n会话: %s\n版本: %s\n\n可以使用 /redo 恢复",
		sessionID[:min(8, len(sessionID))], session.Version)
	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
	log.Printf("dingtalk: reverted session %s to version %s for user %s", sessionID[:8], session.Version, userID)
	return nil, nil
}

// handleRedo 处理重做命令
// 对应 TUI 中的 /redo 操作
func (h *Handler) handleRedo(ctx context.Context, data *chatbot.BotCallbackDataModel, userID string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		msg := "ℹ️ 当前没有活跃的会话\n\n发送消息将自动创建新会话"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	session, err := h.client.UnrevertSession(ctx, sessionID)
	if err != nil {
		msg := fmt.Sprintf("❌ 重做失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, err
	}

	msg := fmt.Sprintf("↪️ 已重做操作\n\n会话: %s\n版本: %s",
		sessionID[:min(8, len(sessionID))], session.Version)
	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
	log.Printf("dingtalk: unreverted session %s to version %s for user %s", sessionID[:8], session.Version, userID)
	return nil, nil
}

// handleFork 处理派生会话命令
// 对应 TUI 中的 session.fork 操作
func (h *Handler) handleFork(ctx context.Context, data *chatbot.BotCallbackDataModel, userID string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		msg := "ℹ️ 当前没有活跃的会话\n\n发送消息将自动创建新会话"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	newSessionID, err := h.client.ForkSession(ctx, sessionID)
	if err != nil {
		msg := fmt.Sprintf("❌ 派生会话失败: %v", err)
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, err
	}

	// 更新用户的 session 映射到新派生的 session
	h.adapter.MapUserToSession(userID, newSessionID)
	h.client.ResetSession(data.ConversationId)
	// 存储新 session 到 thread 映射
	if data.ConversationId != "" {
		h.adapter.MapSessionData(newSessionID, "channel", data.SessionWebhook)
	}

	msg := fmt.Sprintf("🔀 已派生新会话\n\n原会话: %s\n新会话: %s\n\n继续对话将使用新的派生会话（与原会话历史相同）",
		sessionID[:min(8, len(sessionID))], newSessionID[:min(8, len(newSessionID))])
	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
	log.Printf("dingtalk: forked session %s -> %s for user %s", sessionID[:8], newSessionID[:8], userID)
	return nil, nil
}

// handleCompact 处理压缩/总结会话命令
// 对应 TUI 中的 session.compact 操作（调用 summarize API）
// handleTodo 处理查看任务进度命令
// 对应 TUI 中的 todo.updated 事件展示
func (h *Handler) handleTodo(ctx context.Context, data *chatbot.BotCallbackDataModel, userID string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		msg := "ℹ️ 当前没有活跃的会话"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	todos := h.client.GetTodosForSession(sessionID)
	if len(todos) == 0 {
		msg := "📋 当前没有进行中的任务\n\n当 AI 处理复杂请求时，这里会显示任务进度。"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	var sb strings.Builder
	sb.WriteString("📋 当前任务进度:\n\n")
	pending, inProgress, completed := 0, 0, 0
	for _, todo := range todos {
		var icon string
		switch todo.Status {
		case "completed":
			icon = "✅"
			completed++
		case "in_progress":
			icon = "🔄"
			inProgress++
		case "cancelled":
			icon = "❌"
		default:
			icon = "⬜"
			pending++
		}
		sb.WriteString(fmt.Sprintf("%s [优先级:%s] %s\n", icon, todo.PriorityLabel(), todo.Text()))
	}
	sb.WriteString(fmt.Sprintf("\n进度: %d 完成, %d 进行中, %d 待处理", completed, inProgress, pending))

	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(sb.String()))
	return nil, nil
}

// handleDiff 处理查看文件变更命令
// 对应 TUI 中的 session.diff 事件展示
func (h *Handler) handleDiff(ctx context.Context, data *chatbot.BotCallbackDataModel, userID string) ([]byte, error) {
	replier := chatbot.NewChatbotReplier()

	sessionID, ok := h.adapter.GetSessionForUser(userID)
	if !ok {
		msg := "ℹ️ 当前没有活跃的会话"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	diff := h.client.GetDiffForSession(sessionID)
	if len(diff) == 0 {
		msg := "📁 本次会话暂无文件变更\n\n当 AI 修改文件时，这里会显示变更摘要。"
		_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(msg))
		return nil, nil
	}

	var sb strings.Builder
	sb.WriteString("📁 文件变更摘要:\n\n")
	totalAdded, totalRemoved := 0, 0
	for _, f := range diff {
		icon := "📝"
		if f.Added > 0 && f.Removed == 0 {
			icon = "🆕"
		} else if f.Added == 0 && f.Removed > 0 {
			icon = "🗑️"
		}
		sb.WriteString(fmt.Sprintf("%s %s (+%d/-%d)\n", icon, f.Path, f.Added, f.Removed))
		totalAdded += f.Added
		totalRemoved += f.Removed
	}
	sb.WriteString(fmt.Sprintf("\n共 %d 个文件，+%d/-%d 行", len(diff), totalAdded, totalRemoved))

	_ = replier.SimpleReplyText(ctx, data.SessionWebhook, []byte(sb.String()))
	return nil, nil
}

// downloadMediaAsDataURI 通过钉钉 v1.0 API 下载媒体文件并返回 data URI
// 使用 POST https://api.dingtalk.com/v1.0/robot/messageFiles/download
func (h *Handler) downloadMediaAsDataURI(ctx context.Context, downloadCode string, defaultMime string) (string, string, error) {
	if downloadCode == "" {
		return "", "", fmt.Errorf("downloadCode is empty")
	}

	token, err := h.getAccessToken(ctx)
	if err != nil {
		return "", "", fmt.Errorf("failed to get access token: %w", err)
	}

	reqBody := map[string]string{
		"downloadCode": downloadCode,
		"robotCode":    h.cfg.ClientID,
	}
	reqBytes, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(ctx, "POST",
		"https://api.dingtalk.com/v1.0/robot/messageFiles/download",
		bytes.NewReader(reqBytes))
	if err != nil {
		return "", "", fmt.Errorf("failed to create download request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("download API request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)
	log.Printf("dingtalk: downloadMediaAsDataURI response status=%d, body=%s", resp.StatusCode, string(respBytes))

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("download API returned status=%d, body=%s", resp.StatusCode, string(respBytes))
	}

	var downloadResp struct {
		DownloadURL string `json:"downloadUrl"`
	}
	if err := json.Unmarshal(respBytes, &downloadResp); err != nil {
		return "", "", fmt.Errorf("failed to parse download response: %w", err)
	}
	if downloadResp.DownloadURL == "" {
		return "", "", fmt.Errorf("download API returned empty downloadUrl, body=%s", string(respBytes))
	}

	log.Printf("dingtalk: downloadMediaAsDataURI got downloadUrl=%s", downloadResp.DownloadURL[:min(80, len(downloadResp.DownloadURL))])
	return h.downloadURLAsDataURI(ctx, downloadResp.DownloadURL, defaultMime)
}

// downloadURLAsDataURI 下载指定 URL 的文件并返回 data URI 和 MIME 类型
func (h *Handler) downloadURLAsDataURI(ctx context.Context, rawURL string, defaultMime string) (string, string, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return "", "", fmt.Errorf("failed to create URL request: %w", err)
	}

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("failed to fetch media URL: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("media URL returned status=%d", resp.StatusCode)
	}

	fileBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("failed to read media content: %w", err)
	}

	mime := resp.Header.Get("Content-Type")
	if mime == "" || mime == "application/octet-stream" {
		mime = defaultMime
	}
	// 去掉 "; charset=..." 等参数
	if idx := strings.Index(mime, ";"); idx != -1 {
		mime = strings.TrimSpace(mime[:idx])
	}

	b64 := base64.StdEncoding.EncodeToString(fileBytes)
	dataURI := fmt.Sprintf("data:%s;base64,%s", mime, b64)
	return dataURI, mime, nil
}

// downloadMediaBytes 通过钉钉 v1.0 API 下载媒体文件并返回原始字节（不经过 base64 编码）
func (h *Handler) downloadMediaBytes(ctx context.Context, downloadCode string, defaultMime string) ([]byte, string, error) {
	if downloadCode == "" {
		return nil, "", fmt.Errorf("downloadCode is empty")
	}

	token, err := h.getAccessToken(ctx)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get access token: %w", err)
	}

	reqBody := map[string]string{
		"downloadCode": downloadCode,
		"robotCode":    h.cfg.ClientID,
	}
	reqBytes, _ := json.Marshal(reqBody)

	req, err := http.NewRequestWithContext(ctx, "POST",
		"https://api.dingtalk.com/v1.0/robot/messageFiles/download",
		bytes.NewReader(reqBytes))
	if err != nil {
		return nil, "", fmt.Errorf("failed to create download request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-acs-dingtalk-access-token", token)

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("download API request failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)
	log.Printf("dingtalk: downloadMediaBytes response status=%d", resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("download API returned status=%d, body=%s", resp.StatusCode, string(respBytes))
	}

	var downloadResp struct {
		DownloadURL string `json:"downloadUrl"`
	}
	if err := json.Unmarshal(respBytes, &downloadResp); err != nil || downloadResp.DownloadURL == "" {
		return nil, "", fmt.Errorf("failed to get downloadUrl, body=%s", string(respBytes))
	}

	// 获取原始字节
	urlReq, err := http.NewRequestWithContext(ctx, "GET", downloadResp.DownloadURL, nil)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create URL request: %w", err)
	}
	urlClient := &http.Client{Timeout: 60 * time.Second}
	urlResp, err := urlClient.Do(urlReq)
	if err != nil {
		return nil, "", fmt.Errorf("failed to fetch media URL: %w", err)
	}
	defer urlResp.Body.Close()
	if urlResp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("media URL returned status=%d", urlResp.StatusCode)
	}
	fileBytes, err := io.ReadAll(urlResp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read media content: %w", err)
	}
	mime := urlResp.Header.Get("Content-Type")
	if mime == "" || mime == "application/octet-stream" {
		mime = defaultMime
	}
	if idx := strings.Index(mime, ";"); idx != -1 {
		mime = strings.TrimSpace(mime[:idx])
	}
	return fileBytes, mime, nil
}

// extractOpusFromOGG 解析 OGG/Opus 容器，返回裸 Opus 包列表（跳过 OpusHead/OpusTags 页）。
// 每个元素是一个完整 Opus 包，可直接传给 NLS SendAudioData (format=opus)。
func extractOpusFromOGG(data []byte) ([][]byte, error) {
	var packets [][]byte
	pos := 0
	pageIndex := 0

	for pos+27 <= len(data) {
		// OGG 页魔数 "OggS"
		if data[pos] != 'O' || data[pos+1] != 'g' || data[pos+2] != 'g' || data[pos+3] != 'S' {
			break
		}
		nsegments := int(data[pos+26])
		if pos+27+nsegments > len(data) {
			break
		}
		segTable := data[pos+27 : pos+27+nsegments]
		pageDataSize := 0
		for _, s := range segTable {
			pageDataSize += int(s)
		}
		pageHeaderSize := 27 + nsegments
		if pos+pageHeaderSize+pageDataSize > len(data) {
			break
		}
		pageData := data[pos+pageHeaderSize : pos+pageHeaderSize+pageDataSize]
		pos += pageHeaderSize + pageDataSize
		pageIndex++

		// 跳过前两页 OpusHead / OpusTags
		if pageIndex <= 2 {
			continue
		}
		if len(pageData) >= 8 {
			magic := string(pageData[:8])
			if magic == "OpusHead" || magic == "OpusTags" {
				continue
			}
		}

		// 按 OGG lacing 重组 Opus 包：segment 长度 < 255 表示包结束
		dataPos := 0
		var pkt []byte
		for _, segLen := range segTable {
			hi := dataPos + int(segLen)
			if hi > len(pageData) {
				hi = len(pageData)
			}
			pkt = append(pkt, pageData[dataPos:hi]...)
			dataPos += int(segLen)
			if segLen < 255 {
				// 包结束：复制出来追加到列表
				out := make([]byte, len(pkt))
				copy(out, pkt)
				packets = append(packets, out)
				pkt = pkt[:0]
			}
		}
		// 跨页不完整包丢弃
	}
	return packets, nil
}

// transcribeOpusPackets 使用阿里云 NLS 将裸 Opus 包列表转为文字。
// format=opus：每次 SendAudioData 传入一个裸 Opus 包（无长度前缀）。
func (h *Handler) transcribeOpusPackets(ctx context.Context, packets [][]byte, sampleRate int) (string, error) {
	config, err := nls.NewConnectionConfigWithAKInfoDefault(
		nls.DEFAULT_URL,
		h.cfg.AliyunNLSAppKey,
		h.cfg.AliyunNLSAkID,
		h.cfg.AliyunNLSAkKey,
	)
	if err != nil {
		return "", fmt.Errorf("NLS connection config error: %w", err)
	}

	type nlsCbParam struct {
		resultCh chan string
		errCh    chan error
		latest   string
		mu       sync.Mutex
	}
	cbp := &nlsCbParam{
		resultCh: make(chan string, 1),
		errCh:    make(chan error, 1),
	}

	nlsLogger := nls.NewNlsLogger(io.Discard, "nls", log.LstdFlags)
	nlsLogger.SetLogSil(true)

	sr, err := nls.NewSpeechRecognition(config, nlsLogger,
		func(text string, p interface{}) { // taskFailed
			cp := p.(*nlsCbParam)
			select {
			case cp.errCh <- fmt.Errorf("NLS task failed: %s", text):
			default:
			}
		},
		nil,
		func(text string, p interface{}) { // resultChanged
			cp := p.(*nlsCbParam)
			if recognized := extractNLSRecognizedText(text); recognized != "" {
				cp.mu.Lock()
				cp.latest = recognized
				cp.mu.Unlock()
			}
		},
		func(text string, p interface{}) { // completed
			cp := p.(*nlsCbParam)
			log.Printf("dingtalk: NLS completed raw JSON: %.800s", text)
			recognized := extractNLSRecognizedText(text)
			if recognized == "" {
				cp.mu.Lock()
				recognized = cp.latest
				cp.mu.Unlock()
			}
			log.Printf("dingtalk: NLS recognized text: %q", recognized)
			select {
			case cp.resultCh <- recognized:
			default:
			}
		},
		func(p interface{}) {}, // closed
		cbp,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create NLS SpeechRecognition: %w", err)
	}
	defer sr.Shutdown()

	srParam := nls.DefaultSpeechRecognitionParam()
	srParam.Format = "opus"
	srParam.SampleRate = sampleRate

	ready, err := sr.Start(srParam, nil)
	if err != nil {
		return "", fmt.Errorf("NLS SR Start error: %w", err)
	}
	select {
	case ok := <-ready:
		if !ok {
			return "", fmt.Errorf("NLS SR Start failed")
		}
	case <-time.After(15 * time.Second):
		return "", fmt.Errorf("NLS SR Start timeout")
	case <-ctx.Done():
		return "", ctx.Err()
	}

	// 每个 Opus 包单独发送一次（format=opus 要求每次 SendAudioData 是一个完整 Opus 帧）
	for _, pkt := range packets {
		select {
		case ferr := <-cbp.errCh:
			return "", ferr
		default:
		}
		if err := sr.SendAudioData(pkt); err != nil {
			return "", fmt.Errorf("NLS SendAudioData error: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if _, err = sr.Stop(); err != nil {
		return "", fmt.Errorf("NLS SR Stop error: %w", err)
	}

	select {
	case text := <-cbp.resultCh:
		return text, nil
	case ferr := <-cbp.errCh:
		return "", ferr
	case <-time.After(30 * time.Second):
		return "", fmt.Errorf("NLS result timeout")
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

// format: 音频格式，"amr"、"pcm"、"wav" 等；sampleRate: 采样率，AMR 为 8000
func (h *Handler) transcribeAudioBytes(ctx context.Context, audioBytes []byte, format string, sampleRate int) (string, error) {
	config, err := nls.NewConnectionConfigWithAKInfoDefault(
		nls.DEFAULT_URL,
		h.cfg.AliyunNLSAppKey,
		h.cfg.AliyunNLSAkID,
		h.cfg.AliyunNLSAkKey,
	)
	if err != nil {
		return "", fmt.Errorf("NLS connection config error: %w", err)
	}

	type nlsCbParam struct {
		resultCh chan string
		errCh    chan error
		latest   string
		mu       sync.Mutex
	}
	cbp := &nlsCbParam{
		resultCh: make(chan string, 1),
		errCh:    make(chan error, 1),
	}

	nlsLogger := nls.NewNlsLogger(io.Discard, "nls", log.LstdFlags)
	nlsLogger.SetLogSil(true)

	sr, err := nls.NewSpeechRecognition(config, nlsLogger,
		func(text string, p interface{}) { // taskFailed
			cp := p.(*nlsCbParam)
			select {
			case cp.errCh <- fmt.Errorf("NLS task failed: %s", text):
			default:
			}
		},
		nil, // started
		func(text string, p interface{}) { // resultChanged
			cp := p.(*nlsCbParam)
			if recognized := extractNLSRecognizedText(text); recognized != "" {
				cp.mu.Lock()
				cp.latest = recognized
				cp.mu.Unlock()
			}
		},
		func(text string, p interface{}) { // completed
			cp := p.(*nlsCbParam)
			log.Printf("dingtalk: NLS completed raw JSON: %.800s", text)
			recognized := extractNLSRecognizedText(text)
			if recognized == "" {
				cp.mu.Lock()
				recognized = cp.latest
				cp.mu.Unlock()
			}
			log.Printf("dingtalk: NLS recognized text: %q", recognized)
			select {
			case cp.resultCh <- recognized:
			default:
			}
		},
		func(p interface{}) {}, // closed
		cbp,
	)
	if err != nil {
		return "", fmt.Errorf("failed to create NLS SpeechRecognition: %w", err)
	}
	defer sr.Shutdown()

	srParam := nls.DefaultSpeechRecognitionParam()
	srParam.Format = format
	srParam.SampleRate = sampleRate

	ready, err := sr.Start(srParam, nil)
	if err != nil {
		return "", fmt.Errorf("NLS SR Start error: %w", err)
	}
	select {
	case ok := <-ready:
		if !ok {
			return "", fmt.Errorf("NLS SR Start failed")
		}
	case <-time.After(15 * time.Second):
		return "", fmt.Errorf("NLS SR Start timeout")
	case <-ctx.Done():
		return "", ctx.Err()
	}

	// 发送音频数据
	// opu 模式：直接发送 OGG/Opus 容器字节，NLS 服务端解封装；其他格式按固定分块发送
	const chunkSize = 3200
	for i := 0; i < len(audioBytes); i += chunkSize {
		// 如果 NLS 已返回错误（TaskFailed 等），提前中止发送
		select {
		case ferr := <-cbp.errCh:
			return "", ferr
		default:
		}
		end := i + chunkSize
		if end > len(audioBytes) {
			end = len(audioBytes)
		}
		if err := sr.SendAudioData(audioBytes[i:end]); err != nil {
			return "", fmt.Errorf("NLS SendAudioData error: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Stop() creates sr.stopCh under a lock, but RecognitionCompleted may arrive before
	// that lock is acquired — in which case the handler sees stopCh==nil and never signals it.
	// To avoid that race entirely we discard the stop channel and wait only on resultCh/errCh.
	if _, err = sr.Stop(); err != nil {
		return "", fmt.Errorf("NLS SR Stop error: %w", err)
	}

	// 等待识别结果（completed 回调会发送结果）
	select {
	case text := <-cbp.resultCh:
		return text, nil
	case ferr := <-cbp.errCh:
		return "", ferr
	case <-time.After(30 * time.Second):
		return "", fmt.Errorf("NLS result timeout")
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func extractNLSRecognizedText(raw string) string {
	text := strings.TrimSpace(raw)
	if text == "" {
		return ""
	}

	var obj map[string]interface{}
	if err := json.Unmarshal([]byte(text), &obj); err != nil {
		return ""
	}

	lookup := func(m map[string]interface{}, keys ...string) string {
		cur := interface{}(m)
		for _, k := range keys {
			next, ok := cur.(map[string]interface{})
			if !ok {
				return ""
			}
			cur, ok = next[k]
			if !ok {
				return ""
			}
		}
		s, _ := cur.(string)
		return strings.TrimSpace(s)
	}

	candidates := []string{
		lookup(obj, "payload", "result"),
		lookup(obj, "payload", "text"),
		lookup(obj, "result"),
		lookup(obj, "text"),
		lookup(obj, "payload", "output", "text"),
		lookup(obj, "payload", "output", "sentence"),
	}

	for _, v := range candidates {
		if v != "" {
			return v
		}
	}

	return ""
}
