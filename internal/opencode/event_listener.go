package opencode

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/sst/opencode-sdk-go"
)

// EventDispatcher manages event handlers and dispatches events from OpenCode server.
type EventDispatcher struct {
	handlers   map[string][]EventHandler
	handlersMu sync.RWMutex
}

// MessageSender 定义消息发送接口，用于adapter推送消息
type MessageSender interface {
	SendMessageToSession(ctx context.Context, sessionID, content string) error
}

// StreamingSessionHandler 处理流式会话输出
type StreamingSessionHandler struct {
	sessionID          string
	callback           StreamCallback
	eventCallback      StreamEventCallback
	lastContent        string
	lastUpdateTime     time.Time
	lastEventTime      time.Time // 最后一次收到SSE事件的时间
	lastEventType      string    // 最后一次收到的事件类型
	lastActivityTime   time.Time // 最后一次实质任务活动时间（排除状态心跳）
	lastActivityType   string    // 最后一次实质任务活动类型
	mu                 sync.Mutex
	completed          bool
	contentSent        bool        // 标记是否已发送过内容
	waitingHintSent    bool        // 标记是否已发送等待提示
	receivedStepFinish bool        // 标记是否已收到 step-finish 事件
	stepFinishTime     time.Time   // 收到 step-finish 的时间
	waitingTimer       *time.Timer // 等待提示定时器
	onComplete         func()
	onCompleteOnce     sync.Once     // 确保 onComplete 只执行一次（session.idle 和 finalizeAsync 都可能触发）
	client             *Client       // 用于存储问题
	messageSender      MessageSender // 用于主动推送消息到用户
	showThinking       bool          // 是否输出 reasoning/thinking 内容
	showSteps          bool          // 是否输出 step-start/step-finish 步骤提示
	lastTodoSummary    string        // 最近一次自动推送的 todo 文本（用于日志参考）
	lastTodoStateHash  string        // 最近一次 todo 列表的结构化哈希（用于去重）
	lastTodoPushTime   time.Time     // 最近一次自动推送 todo 的时间
	// 增量内容追踪（对照 TUI sync.tsx 中 message.part.delta / message.part.updated 机制）
	partTextCache   sync.Map        // partID -> string: 每个 part 的累积全文，防止 message.part.updated 重复计算
	toolSignalCache sync.Map        // partID -> string: 最近一次已发送的工具状态签名，避免重复推送
	partRoles       sync.Map        // partID -> string: 每个 part 所属 message 的 role（"user"/"assistant"），用于过滤用户消息
	sessionTodos    []TodoItem      // 当前 todo 列表（来自 todo.updated 事件）
	sessionDiff     []FileDiff      // 本次会话的文件变更（来自 session.diff 事件）
	activeToolParts map[string]bool // partID -> true：当前处于 pending/running 状态的 tool part，用于判断是否仍有活跃 tool

	// 上游 provider 重试状态：opencode 在模型不可用/限流时会发出 type=="retry" 的
	// message part（{"type":"retry","attempt":N,"error":{"data":{"message":...}}}）。
	// 这类 part 既不是 session.error，也不会写入 AssistantMessage.Error.Name，
	// 因此需要在这里单独跟踪，否则会话会一直"正在处理中"而不报错。
	retryAttempt    int       // 已观察到的最大 attempt
	retryMessage    string    // 最近一次 retry 的错误信息（error.data.message）
	retryStatusCode int       // 最近一次 retry 的 HTTP 状态码（若有）
	lastRetryTime   time.Time // 最近一次收到 retry part 的时间
	retrySurfaced   bool      // 是否已因超过重试上限而向用户报错

	// pendingQuestionSince records when a permission.asked / question.asked
	// event was received. The streaming ticker uses this to avoid infinite
	// idle-extension: if the session has been waiting for user input for
	// longer than the no-content idle threshold, a reminder is sent instead
	// of silently extending forever (which would deadlock the session when
	// the permission message is stuck behind a congested outbound queue).
	pendingQuestionSince     time.Time
	pendingQuestionSent      bool // whether a "still waiting for your input" reminder has been sent
	pendingQuestionReminders int  // number of reminders already sent (multi-level escalation)

	// serverCompacting 标记 opencode server 正在进行自动压缩（compaction）。
	// 压缩期间会产生 session.idle 事件，但此时并非真正完成，需要忽略。
	// 通过 message.part 类型为 "compaction" 来检测压缩开始，
	// 通过 session.compacted 事件来检测压缩结束。
	serverCompacting bool

	// Circuit-breaker integration: the model used for this turn (so the
	// session.error handler can attribute failures to the right provider)
	// and the original payload (so a fallback retry can be dispatched).
	// fallbackRetryAttempted prevents infinite retry loops: only one
	// automatic model switch is attempted per turn.
	lastModelUsed           *opencode.SessionPromptParamsModel
	payload                 MessagePayload
	fallbackRetryAttempted  bool
	fallbackRetryDispatched bool
}

// NewStreamingSessionHandler 创建流式会话处理器
func NewStreamingSessionHandler(sessionID string, callback StreamCallback, eventCallback StreamEventCallback, onComplete func(), client *Client, messageSender MessageSender, showThinking bool, showSteps bool) *StreamingSessionHandler {
	return NewStreamingSessionHandlerWithModel(sessionID, callback, eventCallback, onComplete, client, messageSender, showThinking, showSteps, nil, MessagePayload{})
}

// NewStreamingSessionHandlerWithModel is the full constructor that also
// captures the model override used for this turn and the original payload.
// These are consumed by the session.error handler to attribute failures to
// the correct provider/model and to dispatch an automatic fallback retry.
func NewStreamingSessionHandlerWithModel(sessionID string, callback StreamCallback, eventCallback StreamEventCallback, onComplete func(), client *Client, messageSender MessageSender, showThinking bool, showSteps bool, modelUsed *opencode.SessionPromptParamsModel, payload MessagePayload) *StreamingSessionHandler {
	h := &StreamingSessionHandler{
		sessionID:        sessionID,
		callback:         callback,
		eventCallback:    eventCallback,
		lastUpdateTime:   time.Now(),
		lastActivityTime: time.Now(),
		lastActivityType: "turn.started",
		onComplete:       onComplete,
		client:           client,
		messageSender:    messageSender,
		showThinking:     showThinking,
		showSteps:        showSteps,
		activeToolParts:  make(map[string]bool),
		lastModelUsed:    modelUsed,
		payload:          payload,
	}
	// 8秒后若仍未发送过内容，给用户一个等待提示
	h.waitingTimer = time.AfterFunc(8*time.Second, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if !h.contentSent && !h.completed && !h.waitingHintSent {
			h.waitingHintSent = true
			_ = h.callback(QuestionSignalPrefix + "⏳ 正在处理中，无需操作，请稍候...")
		}
	})
	return h
}

// SetModelUsed records the final model selected by SendMessage. Streaming
// handlers are registered before automatic model selection completes.
func (s *StreamingSessionHandler) SetModelUsed(model *opencode.SessionPromptParamsModel) {
	if model == nil {
		return
	}
	copyModel := *model
	s.mu.Lock()
	s.lastModelUsed = &copyModel
	s.mu.Unlock()
}

// FallbackRetryDispatched reports whether a replacement turn now owns user
// delivery after this handler received a provider error.
func (s *StreamingSessionHandler) FallbackRetryDispatched() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fallbackRetryDispatched
}

func (s *StreamingSessionHandler) emitEvent(kind StreamEventKind, content string, question *Question, todos []TodoItem, diff []FileDiff, rawType string) {
	if s.eventCallback == nil {
		return
	}
	evt := StreamEvent{
		Kind:      kind,
		SessionID: s.sessionID,
		Content:   content,
		Question:  question,
		Todos:     todos,
		Diff:      diff,
		RawType:   rawType,
	}
	if err := s.eventCallback(evt); err != nil {
		log.Printf("opencode: stream event callback error (kind=%s, session=%s): %v", kind, s.sessionID[:min(8, len(s.sessionID))], err)
	}
}

func (s *StreamingSessionHandler) emitEventFromChunk(chunk string) {
	if chunk == "" {
		return
	}

	if chunk == FlushSignal {
		s.emitEvent(StreamEventFlush, "", nil, nil, nil, "flush")
		return
	}

	if strings.HasPrefix(chunk, ThinkingSignalPrefix) {
		s.emitEvent(StreamEventThinking, strings.TrimPrefix(chunk, ThinkingSignalPrefix), nil, nil, nil, "thinking")
		return
	}

	if strings.HasPrefix(chunk, ToolSignalPrefix) {
		s.emitEvent(StreamEventTool, strings.TrimPrefix(chunk, ToolSignalPrefix), nil, nil, nil, "tool")
		return
	}

	if strings.HasPrefix(chunk, StepSignalPrefix) {
		s.emitEvent(StreamEventStep, strings.TrimPrefix(chunk, StepSignalPrefix), nil, nil, nil, "step")
		return
	}

	if strings.HasPrefix(chunk, TodoSignalPrefix) {
		s.emitEvent(StreamEventTodo, strings.TrimPrefix(chunk, TodoSignalPrefix), nil, nil, nil, "todo")
		return
	}

	if strings.HasPrefix(chunk, QuestionSignalPrefix) {
		s.emitEvent(StreamEventInfo, strings.TrimPrefix(chunk, QuestionSignalPrefix), nil, nil, nil, "question")
		return
	}

	if strings.HasPrefix(chunk, "ses_") && len(chunk) < 100 {
		s.emitEvent(StreamEventSessionReady, chunk, nil, nil, nil, "session")
		return
	}

	s.emitEvent(StreamEventTextDelta, chunk, nil, nil, nil, "text")
}

// NewEventDispatcher creates a new event dispatcher.
func NewEventDispatcher() *EventDispatcher {
	return &EventDispatcher{
		handlers: make(map[string][]EventHandler),
	}
}

// RegisterHandler registers an event handler for a specific event type.
// If eventType is empty, the handler will be called for all events.
func (d *EventDispatcher) RegisterHandler(eventType string, handler EventHandler) {
	d.handlersMu.Lock()
	defer d.handlersMu.Unlock()

	if eventType == "" {
		eventType = "*" // Wildcard for all events
	}

	d.handlers[eventType] = append(d.handlers[eventType], handler)
}

// Dispatch sends an event to all registered handlers.
func (d *EventDispatcher) Dispatch(ctx context.Context, event *opencode.EventListResponse) error {
	d.handlersMu.RLock()
	defer d.handlersMu.RUnlock()

	// Call wildcard handlers
	if handlers, ok := d.handlers["*"]; ok {
		for _, handler := range handlers {
			if err := handler(ctx, event); err != nil {
				log.Printf("opencode: wildcard handler error: %v", err)
			}
		}
	}

	// Call type-specific handlers
	eventType := string(event.Type)
	if handlers, ok := d.handlers[eventType]; ok {
		for _, handler := range handlers {
			if err := handler(ctx, event); err != nil {
				log.Printf("opencode: handler error for type %s: %v", eventType, err)
			}
		}
	}

	return nil
}

// HandleEvent 处理会话更新事件并提取增量内容
func (s *StreamingSessionHandler) HandleEvent(ctx context.Context, event *opencode.EventListResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidateSessionID := extractSessionIDFromEvent(event)
	eventType := string(event.Type)

	log.Printf("opencode: StreamingHandler received event - type=%s, eventSessionID=%s, handlerSessionID=%s",
		eventType,
		func() string {
			if candidateSessionID != "" {
				return candidateSessionID[:min(8, len(candidateSessionID))]
			}
			return "(not found in event)"
		}(),
		s.sessionID[:8])

	if s.completed {
		// 即使 completed，仍然处理权限和问题请求（skill 可能在 session.idle 后才请求权限）
		if eventType != "permission.asked" && eventType != "question.asked" {
			log.Printf("opencode: ignoring event for completed session %s (type=%s)", s.sessionID[:8], eventType)
			return nil
		}
		log.Printf("opencode: processing %s event for completed session %s (skill may need permission)", eventType, s.sessionID[:8])
	}

	// 仅处理与当前session相关的事件
	log.Printf("opencode: streaming handler event type=%s", eventType)

	s.lastEventTime = time.Now()
	s.lastEventType = eventType
	if eventType != "session.status" {
		s.lastActivityTime = s.lastEventTime
		s.lastActivityType = eventType
	}

	// 只处理 message.part.updated 事件获取增量内容
	switch eventType {
	case "message.part.updated":
		// 先解析 partID 和 role，记录到 partRoles（供 message.part.delta 过滤用户消息）
		if jsonData := event.JSON.RawJSON(); jsonData != "" {
			// 🔍 输出原始 JSON 用于诊断
			log.Printf("opencode: 🔍 message.part.updated RAW JSON (first 800 chars): %.800s", jsonData)

			var partMeta2 struct {
				Properties struct {
					Part struct {
						ID     string `json:"id"`
						Type   string `json:"type"`
						Tokens struct {
							Input  int `json:"input"`
							Output int `json:"output"`
							Total  int `json:"total"`
						} `json:"tokens"`
					} `json:"part"`
					Message struct {
						Role string `json:"role"`
					} `json:"message"`
				} `json:"properties"`
			}
			if err := json.Unmarshal([]byte(jsonData), &partMeta2); err == nil {
				partID := partMeta2.Properties.Part.ID
				role := partMeta2.Properties.Message.Role
				log.Printf("opencode: 🔍 message.part.updated parsed - partID=%s, role=%s, partType=%s",
					partID[:min(8, len(partID))], role, partMeta2.Properties.Part.Type)
				if partID != "" && role != "" {
					s.partRoles.Store(partID, role)
				}
				if partMeta2.Properties.Part.Type == "step-finish" {
					s.receivedStepFinish = true
					s.stepFinishTime = time.Now()
					log.Printf("opencode: 🏁 received step-finish for session %s", s.sessionID[:8])
					// 用 API 返回的真实 token 数覆盖估算值。
					// 注意：必须使用 total（完整上下文窗口占用），而不是 input（仅 cache-miss 的新 token）。
					// 在支持 KV cache 的模型上，input 可能只有几百而 total 已达十几万，
					// 用 input 会导致自动压缩阈值永远不触发。
					if total := partMeta2.Properties.Part.Tokens.Total; total > 0 {
						if s.client != nil {
							s.client.UpdateTokenCount(s.sessionID, total)
						}
					} else if inputTokens := partMeta2.Properties.Part.Tokens.Input; inputTokens > 0 {
						// fallback: 部分模型只返回 input，没有 total
						if s.client != nil {
							s.client.UpdateTokenCount(s.sessionID, inputTokens)
						}
					}
				}

				// 上游 provider 重试 part：opencode 在模型不可用/限流时反复重试，
				// 每次都发一个 type=="retry" 的 part。这类 part 永远不会触发
				// session.idle，若不拦截会导致会话一直"正在处理中"。
				if partMeta2.Properties.Part.Type == "retry" {
					s.handleRetryPart(jsonData)
				}

				// 检测 server 端自动压缩（compaction）：当 opencode server 在
				// prompt loop 中检测到 overflow 时，会创建 type=="compaction" 的
				// part。设置 serverCompacting 标志，使后续 session.idle 被忽略，
				// 直到收到 session.compacted 事件才清除。
				if partMeta2.Properties.Part.Type == "compaction" {
					if !s.serverCompacting {
						s.serverCompacting = true
						log.Printf("opencode: 📦 compaction part detected for session %s, setting serverCompacting flag", s.sessionID[:8])
					}
				}
			}
		}

		// 提取增量内容并发送（extractContentFromEvent 内部已检查 role=user 则跳过）
		// 注意：extractContentFromEvent 内部会更新 partTextCache，但只有 callback 成功后
		// 才能认为内容已送达；若 callback 失败则需由后续快照重新补发
		incremental, partID, newCacheText := s.extractContentFromEvent(event)
		if incremental != "" {
			if err := s.callback(incremental); err != nil {
				log.Printf("opencode: streaming callback error: %v", err)
				// callback 失败：撤销 extractContentFromEvent 中已做的缓存更新，让下个快照重试
				if partID != "" && newCacheText != "" {
					// 回退到快照前的状态（即去掉刚才 store 进去的新内容）
					prevText := newCacheText[:len(newCacheText)-len(incremental)]
					if prevText == "" {
						s.partTextCache.Delete(partID)
					} else {
						s.partTextCache.Store(partID, prevText)
					}
				}
			} else {
				s.emitEventFromChunk(incremental)
				s.lastContent += incremental
				if !s.contentSent {
					s.stopWaitingTimer()
				}
				s.contentSent = true
			}
			s.lastUpdateTime = time.Now()
		}

	case "question.answered", "permission.answered":
		// 用户已回答问题或权限请求
		s.pendingQuestionSince = time.Time{}
		s.pendingQuestionSent = false
		s.pendingQuestionReminders = 0
		// 清理本地缓存的权限/问题（防止级联自动清理后残留过期条目）
		s.cleanupPendingQuestionFromEvent(event)
		log.Printf("opencode: %s event processed for session %s", eventType, s.sessionID[:8])

	case "permission.replied":
		// 权限已确认（兼容性事件）
		s.pendingQuestionSince = time.Time{}
		s.pendingQuestionSent = false
		s.pendingQuestionReminders = 0
		// 清理本地缓存的权限/问题（防止级联自动清理后残留过期条目）
		s.cleanupPendingQuestionFromEvent(event)
		log.Printf("opencode: %s event processed for session %s", eventType, s.sessionID[:8])

	case "question.asked", "permission.asked":
		// OpenCode 需要用户确认（build 模式或权限请求）
		// ⚠️ 不再设置 waitingForResponse，以免阻止后续事件处理
		// s.waitingForResponse = true // 注释掉，让事件继续流动
		s.pendingQuestionSince = time.Now()
		s.pendingQuestionSent = false
		s.pendingQuestionReminders = 0
		log.Printf("opencode: user response needed (type: %s) for session %s", event.Type, s.sessionID[:8])

		question, questionMsg := s.extractQuestionFromEvent(event)
		if question != nil && s.client != nil {
			// 存储问题供后续回答
			s.client.StorePendingQuestion(question)
			log.Printf("opencode: stored question %s for session %s (type: %s, isPermission=%t)",
				question.ID, s.sessionID[:8], event.Type, question.IsPermission)

			// 追加待处理权限/问题计数，让用户知道总共需要回答几个
			if question.IsPermission {
				if total := s.client.CountPendingPermissions(s.sessionID); total > 1 {
					questionMsg = fmt.Sprintf("🔢 共 %d 个待确认权限\n\n%s", total, questionMsg)
				}
			}
		}

		if questionMsg == "" {
			if event.Type == "permission.asked" {
				questionMsg = "🔔 需要您确认才能继续\n\n" +
					"请直接回复选项编号或文字（如 1、允许、拒绝）。\n" +
					"确认后任务将自动继续执行。"
			} else {
				questionMsg = "🔔 需要您回复才能继续\n\n" +
					"请直接回复选项编号或文字。\n" +
					"回复后任务将自动继续执行。"
			}
			log.Printf("opencode: using fallback message for %s", event.Type)
		}
		log.Printf("opencode: sending %s message (len=%d, prefix=%s) for session %s",
			event.Type, len(questionMsg), questionMsg[:min(10, len(questionMsg))], s.sessionID[:8])

		// 只发送一次！通过streaming callback发送，使用 QuestionSignalPrefix
		// 让 adapter 立即发送（不进入内容积累缓冲区）
		if err := s.callback(QuestionSignalPrefix + questionMsg); err != nil {
			log.Printf("opencode: question/permission callback error: %v", err)
		} else {
			if question != nil {
				if question.IsPermission {
					s.emitEvent(StreamEventPermissionAsked, questionMsg, question, nil, nil, eventType)
				} else {
					s.emitEvent(StreamEventQuestionAsked, questionMsg, question, nil, nil, eventType)
				}
			} else {
				s.emitEvent(StreamEventQuestionAsked, questionMsg, nil, nil, nil, eventType)
			}
			log.Printf("opencode: %s message sent via callback successfully", event.Type)
			s.contentSent = true
		}

	case "session.idle":
		// opencode server 自动压缩（compaction）期间也会产生 session.idle 事件。
		// 如果在此期间触发 completed，会导致 handler 提前注销，后续真实响应事件
		// （包括 permission.asked）全部被丢弃，session 在 opencode 服务端死锁。
		// 通过 serverCompacting 标志区分压缩产生的 idle 和真实任务完成的 idle。
		if s.serverCompacting {
			log.Printf("opencode: ignoring session.idle during server-side compaction for session %s", s.sessionID[:min(8, len(s.sessionID))])
			return nil
		}
		s.completed = true
		s.notifyCompletion()
		log.Printf("opencode: 🏁 streaming session completed (session=%s, contentSent=%t, lastContentLen=%d)",
			s.sessionID[:8], s.contentSent, len(s.lastContent))

	case "file.edited":
		// 文件编辑事件，静默处理
		log.Printf("opencode: file edited for session %s", s.sessionID[:8])

	case "message.updated":
		// 从 message.updated 中提取 messageID→role 映射，存入 client.messageRoles，
		// 供 extractContentFromEvent 过滤用户自身消息（避免把用户输入当 AI 回复回传）。
		if s.client != nil {
			if raw := event.JSON.RawJSON(); raw != "" {
				var msgUpdate struct {
					Properties struct {
						Info struct {
							ID   string `json:"id"`
							Role string `json:"role"`
						} `json:"info"`
					} `json:"properties"`
				}
				if err := json.Unmarshal([]byte(raw), &msgUpdate); err == nil {
					msgID := msgUpdate.Properties.Info.ID
					role := msgUpdate.Properties.Info.Role
					if msgID != "" && role != "" {
						s.client.messageRoles.Store(msgID, role)
					}
				}
			}
		}

	case "session.updated":
		// Session 更新事件，静默处理
		log.Printf("opencode: session updated for session %s", s.sessionID[:8])

	case "session.status":
		// Session 状态事件，静默处理
		log.Printf("opencode: session status changed for session %s", s.sessionID[:8])

	case "session.error":
		rawJSON := event.JSON.RawJSON()
		log.Printf("opencode: 🔴 session.error RAW JSON: %s", rawJSON)
		errorMsg := s.extractSessionError(event)
		log.Printf("opencode: 🔴 session.error extracted message: %q", errorMsg)

		// Circuit breaker: if this is a provider-level failure (e.g. "No
		// available client"), record it against the model used for this
		// turn and attempt an automatic fallback to a healthy model.
		// The fallback re-dispatches the original payload with a new
		// model; if it succeeds we return without notifying the user of
		// the error (the retry turn's content reaches them instead).
		if s.client != nil && s.client.breaker != nil && s.lastModelUsed != nil && !s.fallbackRetryAttempted {
			if s.client.breaker.IsProviderLevelError(errorMsg) {
				pid := s.lastModelUsed.ProviderID.Value
				mid := s.lastModelUsed.ModelID.Value
				s.client.breaker.RecordFailure(pid, mid, errorMsg)
				log.Printf("opencode: 🔌 circuit breaker recorded failure for %s/%s (state=%s)",
					pid, mid, s.client.breaker.StateString(pid, mid))

				s.fallbackRetryAttempted = true
				if s.client.attemptFallbackRetry(s.sessionID, s.payload, s.lastModelUsed, s.callback, s.eventCallback) {
					// Fallback dispatched: do not surface the error to the
					// user. Mark this handler complete so the SSE loop
					// exits; the new turn's handler takes over.
					s.fallbackRetryDispatched = true
					s.completed = true
					s.notifyCompletion()
					log.Printf("opencode: session.error handled via fallback retry for session %s", s.sessionID[:8])
					return nil
				}
			}
		}

		if errorMsg == "" {
			errorMsg = "⚠️ OpenCode 会话发生错误，请稍后重试。"
		} else {
			errorMsg = fmt.Sprintf("⚠️ OpenCode 会话出错：%s", errorMsg)
		}

		if err := s.callback(QuestionSignalPrefix + errorMsg); err != nil {
			log.Printf("opencode: session.error callback error: %v", err)
		} else {
			s.emitEvent(StreamEventError, errorMsg, nil, nil, nil, eventType)
			s.contentSent = true
		}

		s.completed = true
		s.notifyCompletion()
		log.Printf("opencode: session error handled for session %s", s.sessionID[:8])

	case "message.part.delta":
		// 来自 TUI sync.tsx: message.part.delta 是增量文本的主要来源
		// 结构: {properties: {messageID, partID, field, delta}}
		// 修复：
		//   1. 通过 partRoles 过滤 role=user 的消息（避免把用户输入回传给用户）
		//   2. 同步更新 partTextCache，防止后续 message.part.updated 重复计算 delta
		if raw := event.JSON.RawJSON(); raw != "" {
			var d struct {
				Properties struct {
					MessageID string `json:"messageID"`
					PartID    string `json:"partID"`
					Field     string `json:"field"`
					Delta     string `json:"delta"`
				} `json:"properties"`
			}
			if err := json.Unmarshal([]byte(raw), &d); err == nil && d.Properties.Delta != "" {
				if d.Properties.Field == "reasoning" {
					if !s.showThinking {
						break
					}
					reasoningChunk := ThinkingSignalPrefix + d.Properties.Delta
					if err := s.callback(reasoningChunk); err != nil {
						log.Printf("opencode: message.part.delta reasoning callback error: %v", err)
					} else {
						s.emitEventFromChunk(reasoningChunk)
						s.lastContent += reasoningChunk
						if !s.contentSent {
							s.stopWaitingTimer()
						}
						s.contentSent = true
					}
					s.lastUpdateTime = time.Now()
					break
				}

				if d.Properties.Field != "text" {
					break
				}

				partID := d.Properties.PartID
				delta := d.Properties.Delta

				log.Printf("opencode: message.part.delta - session=%s, msgID=%s, partID=%s, delta_len=%d",
					s.sessionID[:8],
					d.Properties.MessageID[:min(8, len(d.Properties.MessageID))],
					partID[:min(8, len(partID))],
					len(delta))

				// 过滤用户消息：先查 partRoles（由 message.part.updated 填充），
				// 再查 client.messageRoles（全局 messageID→role 映射，更早可用）
				if role, ok := s.partRoles.Load(partID); ok && role.(string) == "user" {
					log.Printf("opencode: 🚫 message.part.delta FILTERED via partRoles (user message, partID=%s)",
						partID[:min(8, len(partID))])
					break
				}
				if s.client != nil && d.Properties.MessageID != "" {
					if role, ok := s.client.messageRoles.Load(d.Properties.MessageID); ok && role.(string) == "user" {
						log.Printf("opencode: 🚫 message.part.delta FILTERED via messageRoles (user message, msgID=%s)",
							d.Properties.MessageID[:min(8, len(d.Properties.MessageID))])
						break
					}
				}

				// 必须在 callback 成功后才更新 partTextCache；
				// 若先更新缓存再调 callback，一旦 callback 失败，
				// 后续 message.part.updated 快照 delta=0 会永久丢丢内容。
				if err := s.callback(delta); err != nil {
					log.Printf("opencode: message.part.delta callback error: %v", err)
					// 不更新缓存：让下一个 message.part.updated 快照把这部分内容补发
				} else {
					s.emitEventFromChunk(delta)
					// callback 成功后才提交缓存，确保"缓存 == 已送达"
					if partID != "" {
						existing, _ := s.partTextCache.Load(partID)
						existingStr, _ := existing.(string)
						s.partTextCache.Store(partID, existingStr+delta)
					}
					s.lastContent += delta
					if !s.contentSent {
						s.stopWaitingTimer()
					}
					s.contentSent = true
				}
				s.lastUpdateTime = time.Now()
			}
		}

	case "todo.updated":
		// TUI sync.tsx: todo.updated 含 {sessionID, todos[]}
		todos := extractTodosFromEvent(event)
		if todos != nil {
			s.sessionTodos = todos
			s.emitEvent(StreamEventTodoSnapshot, "", nil, todos, nil, eventType)
			log.Printf("opencode: todos updated for session %s (%d items)", s.sessionID[:8], len(todos))

			summary := formatTodoSummaryFromTodos(todos)
			if summary != "" {
				now := time.Now()
				// Dedup by structural hash (status+text of each item) instead
				// of the formatted summary string. This ensures that when any
				// todo item's status changes (e.g. pending→in_progress→completed)
				// the update is pushed even if the formatted summary happens to
				// be identical due to formatting quirks.
				stateHash := todoStateHash(todos)
				if stateHash != s.lastTodoStateHash {
					if err := s.callback(TodoSignalPrefix + summary); err != nil {
						log.Printf("opencode: todo auto-push callback error: %v", err)
					} else {
						s.emitEventFromChunk(TodoSignalPrefix + summary)
						s.lastTodoSummary = summary
						s.lastTodoStateHash = stateHash
						s.lastTodoPushTime = now
						if !s.contentSent {
							s.stopWaitingTimer()
						}
						s.contentSent = true
					}
				}
			}
		}

	case "session.diff":
		// 文件变更事件，存储后在 session.idle 时汇总发送
		diff := extractDiffFromEvent(event)
		if len(diff) > 0 {
			s.sessionDiff = diff
			s.emitEvent(StreamEventDiffSnapshot, "", nil, nil, diff, eventType)
			log.Printf("opencode: session diff updated for session %s (%d files)", s.sessionID[:8], len(diff))
		}

	case "message.part.removed":
		// 清理 partTextCache 中对应的 part 缓存
		if raw := event.JSON.RawJSON(); raw != "" {
			var w struct {
				Properties struct {
					PartID string `json:"partID"`
				} `json:"properties"`
			}
			if err := json.Unmarshal([]byte(raw), &w); err == nil && w.Properties.PartID != "" {
				s.partTextCache.Delete(w.Properties.PartID)
				s.toolSignalCache.Delete(w.Properties.PartID)
			}
		}
		log.Printf("opencode: message part removed for session %s", s.sessionID[:8])

	case "message.removed":
		log.Printf("opencode: message removed for session %s", s.sessionID[:8])

	case "session.compacted":
		// opencode 服务端自主压缩完成通知（可能是 /summary 命令触发，也可能是服务端自动触发）
		// 压缩后重置 token 计数器，等待下次 step-finish 携带真实值
		s.serverCompacting = false
		if s.client != nil {
			s.client.tokenCount.Store(s.sessionID, 0)
		}
		log.Printf("opencode: ✅ session.compacted received for session %s, token counter reset, compaction flag cleared", s.sessionID[:8])

	case "server.instance.disposed":
		// 服务器重启，事件监听器主循环负责重连
		log.Printf("opencode: ⚠️ server instance disposed (session=%s)", s.sessionID[:8])

	case "session.deleted":
		// 如果被删除的是当前 session，标记为完成
		deletedID := extractSessionIDFromEvent(event)
		if deletedID == s.sessionID {
			log.Printf("opencode: session %s was deleted", s.sessionID[:8])
			s.completed = true
			s.notifyCompletion()
		}

	case "session.created":
		// Session 创建事件，静默处理
		log.Printf("opencode: session created: %s", s.sessionID[:8])

	case "server.heartbeat":
		// 服务器心跳，静默处理
		// log.Printf("opencode: received server heartbeat")

	case "lsp.updated":
		// LSP 更新事件，静默处理
		log.Printf("opencode: lsp updated for session %s", s.sessionID[:8])

	case "file.watcher.updated":
		// 文件监视器更新事件，静默处理
		// 这是 OpenCode 内部文件监视器检测到文件变化时触发的事件
		// log.Printf("opencode: file watcher updated for session %s", s.sessionID[:8])

	default:
		// 未知事件类型，记录日志但不中断
		if eventType != "" {
			log.Printf("opencode: unhandled event type '%s' for session %s", eventType, s.sessionID[:8])
		}
	}

	return nil
}

// extractContentFromEvent 从 message.part.updated 事件中提取用户可见内容。
// 返回 (delta, partID, newCacheText)：
//   - delta：需要发送给用户的增量文本
//   - partID：对应的 partID（仅 text 类型有值）
//   - newCacheText：cache 更新后的完整文本（用于 caller 在 callback 失败时回滚）
//
// 注意：函数内部会立即更新 partTextCache。若 caller 的 callback 失败，
// 应将 partTextCache[partID] 回退到 newCacheText[:len(newCacheText)-len(delta)]。
func (s *StreamingSessionHandler) extractContentFromEvent(event *opencode.EventListResponse) (string, string, string) {
	if event == nil || event.Type != "message.part.updated" {
		return "", "", ""
	}

	type ToolInput struct {
		Command      string `json:"command"`
		Description  string `json:"description"`
		Filepath     string `json:"filepath"`
		URL          string `json:"url"`
		SubagentType string `json:"subagent_type"`
	}

	type ToolMetadata struct {
		ParentSessionID string                 `json:"parentSessionId"`
		SessionID       string                 `json:"sessionId"`
		Model           map[string]interface{} `json:"model"`
		Truncated       bool                   `json:"truncated"`
	}

	type ToolState struct {
		Status   string       `json:"status"`
		Input    ToolInput    `json:"input"`
		Output   string       `json:"output"`
		Error    string       `json:"error"`
		Metadata ToolMetadata `json:"metadata"`
	}

	type PartUpdateProps struct {
		Delta string `json:"delta"`
		Part  struct {
			ID        string    `json:"id"`
			MessageID string    `json:"messageID"` // messageID for role lookup via client.messageRoles
			Type      string    `json:"type"`
			Tool      string    `json:"tool"`
			State     ToolState `json:"state"`
			Text      string    `json:"text"`
		} `json:"part"`
	}

	jsonData := event.JSON.RawJSON()
	if jsonData == "" {
		log.Printf("opencode: 🔍 extractContent - empty JSON data for session %s", s.sessionID[:8])
		return "", "", ""
	}

	var wrapper struct {
		Properties PartUpdateProps `json:"properties"`
	}
	if err := json.Unmarshal([]byte(jsonData), &wrapper); err != nil {
		log.Printf("opencode: failed to unmarshal message.part.updated: %v, JSON preview: %.200s", err, jsonData)
		return "", "", ""
	}

	props := wrapper.Properties

	// 🔍 详细诊断日志
	log.Printf("opencode: 🔍 extractContent - session=%s, partType=%s, partID=%s, msgID=%s, delta_len=%d, text_len=%d",
		s.sessionID[:8], props.Part.Type, props.Part.ID, props.Part.MessageID, len(props.Delta), len(props.Part.Text))

	// ⚠️ 关键过滤：忽略用户角色的消息
	var messageRole string
	if s.client != nil && props.Part.MessageID != "" {
		if role, ok := s.client.messageRoles.Load(props.Part.MessageID); ok {
			messageRole = role.(string)
		}
	}

	if messageRole == "user" {
		msgIDPreview := props.Part.MessageID
		if len(msgIDPreview) > 8 {
			msgIDPreview = msgIDPreview[:8]
		}
		log.Printf("opencode: 🚫 extractContent - FILTERED user role message (partType=%s, partID=%s, msgID=%s)",
			props.Part.Type, props.Part.ID, msgIDPreview)
		return "", "", ""
	}

	partType := props.Part.Type

	switch partType {
	case "text":
		// 优先使用 delta 字段（部分 server 版本在 message.part.updated 中直接送 delta）
		if props.Delta != "" {
			if props.Part.ID != "" {
				existing, _ := s.partTextCache.Load(props.Part.ID)
				existingStr, _ := existing.(string)
				newCache := existingStr + props.Delta
				s.partTextCache.Store(props.Part.ID, newCache)
				log.Printf("opencode: 🔍 extractContent - returning delta from part.updated (%d chars)", len(props.Delta))
				return props.Delta, props.Part.ID, newCache
			}
			log.Printf("opencode: 🔍 extractContent - returning delta from part.updated (no partID, %d chars)", len(props.Delta))
			return props.Delta, "", ""
		}

		// 没有 delta 时，利用 partTextCache 计算增量
		// 这对应 TUI 中 message.part.updated 携带完整 part.text 而非 delta 的情况
		if props.Part.Text != "" && props.Part.ID != "" {
			existing, _ := s.partTextCache.Load(props.Part.ID)
			existingStr, _ := existing.(string)
			if len(props.Part.Text) > len(existingStr) {
				delta := props.Part.Text[len(existingStr):]
				s.partTextCache.Store(props.Part.ID, props.Part.Text)
				log.Printf("opencode: 🔍 extractContent - computed delta from cached text (%d new chars, total=%d)",
					len(delta), len(props.Part.Text))
				return delta, props.Part.ID, props.Part.Text
			}
			// 文本与缓存相同或更短（可能是 snapshot 重置），不发送但仍更新缓存
			s.partTextCache.Store(props.Part.ID, props.Part.Text)
		} else if props.Part.Text != "" {
			// 没有 partID 时，用 lastContent 估算增量（兼容旧行为）
			if len(props.Part.Text) > len(s.lastContent) {
				delta := props.Part.Text[len(s.lastContent):]
				log.Printf("opencode: 🔍 extractContent - inferred delta (no partID, %d chars)", len(delta))
				return delta, "", ""
			}
		}

	case "tool":
		// 工具调用事件：通过专用信号下发给 adapter，和正文分轨。
		toolName := props.Part.Tool
		state := props.Part.State
		partID := props.Part.ID
		log.Printf("opencode: 🔍 extractContent - tool event: name=%s, status=%s", toolName, state.Status)

		// 追踪活跃 tool parts（pending/running 视为活跃，completed/error 视为结束）
		if partID != "" {
			switch state.Status {
			case "pending", "running":
				s.activeToolParts[partID] = true
			case "completed", "error":
				delete(s.activeToolParts, partID)
			}
		}

		// Capture child session info from task tool calls (subagent sessions).
		// The metadata.sessionId is the child session, metadata.parentSessionId
		// is the parent (should match s.sessionID).
		if toolName == "task" && s.client != nil && state.Metadata.SessionID != "" {
			switch state.Status {
			case "running", "pending":
				s.client.RegisterChildSession(
					s.sessionID,
					state.Metadata.SessionID,
					state.Input.Description,
					state.Input.SubagentType,
				)
			case "completed":
				s.client.UpdateChildSessionStatus(state.Metadata.SessionID, "completed")
			case "error":
				s.client.UpdateChildSessionStatus(state.Metadata.SessionID, "error")
			}
		}

		switch state.Status {
		case "running":
			desc := state.Input.Description
			if desc == "" {
				desc = state.Input.Command
			}
			if desc == "" {
				desc = state.Input.Filepath
			}
			if desc == "" {
				desc = state.Input.URL
			}
			if desc != "" {
				if len([]rune(desc)) > 120 {
					desc = string([]rune(desc)[:120]) + "..."
				}
				msg := fmt.Sprintf("🔧 工具 %s：%s", toolName, desc)
				if partID != "" {
					signature := "running|" + msg
					if prev, ok := s.toolSignalCache.Load(partID); ok && prev.(string) == signature {
						return "", "", ""
					}
					s.toolSignalCache.Store(partID, signature)
				}
				return ToolSignalPrefix + msg, "", ""
			}
			msg := fmt.Sprintf("🔧 工具 %s：执行中", toolName)
			if partID != "" {
				signature := "running|" + msg
				if prev, ok := s.toolSignalCache.Load(partID); ok && prev.(string) == signature {
					return "", "", ""
				}
				s.toolSignalCache.Store(partID, signature)
			}
			return ToolSignalPrefix + msg, "", ""

		case "completed":
			// 每次工具完成递增计数器，供 skillgen 评估任务复杂度
			if s.client != nil {
				s.client.IncrementToolCall(s.sessionID)
			}
			output := strings.TrimSpace(state.Output)
			if output != "" {
				if len([]rune(output)) > 160 {
					output = string([]rune(output)[:160]) + "..."
				}
				msg := fmt.Sprintf("✅ 工具 %s：已完成（%s）", toolName, output)
				if partID != "" {
					signature := "completed|" + msg
					if prev, ok := s.toolSignalCache.Load(partID); ok && prev.(string) == signature {
						return "", "", ""
					}
					s.toolSignalCache.Store(partID, signature)
				}
				return ToolSignalPrefix + msg, "", ""
			}
			msg := fmt.Sprintf("✅ 工具 %s：已完成", toolName)
			if partID != "" {
				signature := "completed|" + msg
				if prev, ok := s.toolSignalCache.Load(partID); ok && prev.(string) == signature {
					return "", "", ""
				}
				s.toolSignalCache.Store(partID, signature)
			}
			return ToolSignalPrefix + msg, "", ""

		case "error":
			errMsg := strings.TrimSpace(state.Error)
			if errMsg == "" {
				errMsg = "执行失败"
			}
			if len([]rune(errMsg)) > 160 {
				errMsg = string([]rune(errMsg)[:160]) + "..."
			}
			msg := fmt.Sprintf("❌ 工具 %s：失败（%s）", toolName, errMsg)
			if partID != "" {
				signature := "error|" + msg
				if prev, ok := s.toolSignalCache.Load(partID); ok && prev.(string) == signature {
					return "", "", ""
				}
				s.toolSignalCache.Store(partID, signature)
			}
			return ToolSignalPrefix + msg, "", ""
		}
		return "", "", ""

	case "reasoning", "step-start", "step-finish", "snapshot", "patch", "compaction":
		if partType == "reasoning" && s.showThinking {
			if props.Delta != "" {
				return ThinkingSignalPrefix + props.Delta, "", ""
			}
			if props.Part.Text != "" && props.Part.ID != "" {
				existing, _ := s.partTextCache.Load(props.Part.ID)
				existingStr, _ := existing.(string)
				if len(props.Part.Text) > len(existingStr) {
					delta := props.Part.Text[len(existingStr):]
					s.partTextCache.Store(props.Part.ID, props.Part.Text)
					return ThinkingSignalPrefix + delta, props.Part.ID, props.Part.Text
				}
				s.partTextCache.Store(props.Part.ID, props.Part.Text)
			}
		}

		if partType == "step-start" {
			if s.showSteps {
				return StepSignalPrefix + "步骤开始", "", ""
			}
			return "", "", ""
		}
		if partType == "step-finish" {
			if s.showSteps {
				return StepSignalPrefix + "步骤完成", "", ""
			}
			return "", "", ""
		}

		// 其他内部事件，不向用户发送
		log.Printf("opencode: 🔍 extractContent - internal event type: %s (not sent to user)", partType)

	case "":
		log.Printf("opencode: 🔍 extractContent - empty part type, JSON preview: %.300s", jsonData)

	default:
		log.Printf("opencode: 🔍 extractContent - unhandled part type '%s' for session %s, JSON preview: %.300s",
			partType, s.sessionID[:8], jsonData)
	}

	return "", "", ""
}

// extractSessionError 从 session.error 事件中提取错误信息
func (s *StreamingSessionHandler) extractSessionError(event *opencode.EventListResponse) string {
	if event == nil {
		return ""
	}

	raw := event.JSON.RawJSON()
	if raw == "" {
		return ""
	}

	type SessionErrorProps struct {
		SessionID string `json:"sessionID"`
		MessageID string `json:"messageID"`
		Error     struct {
			Type    string `json:"type"`
			Name    string `json:"name"`
			Message string `json:"message"`
			Code    string `json:"code"`
			Detail  string `json:"detail"`
			Data    struct {
				Message string `json:"message"`
			} `json:"data"`
		} `json:"error"`
		Message string `json:"message"`
		Detail  string `json:"detail"`
		Code    string `json:"code"`
		Reason  string `json:"reason"`
	}

	var wrapper struct {
		Properties SessionErrorProps `json:"properties"`
	}

	if err := json.Unmarshal([]byte(raw), &wrapper); err != nil {
		log.Printf("opencode: failed to parse session.error event: %v, raw: %s", err, raw)
		return ""
	}

	props := wrapper.Properties
	log.Printf("opencode: session.error parsed props: Error.Name=%q, Error.Message=%q, Error.Data.Message=%q, Message=%q, Detail=%q, Reason=%q, Code=%q",
		props.Error.Name, props.Error.Message, props.Error.Data.Message, props.Message, props.Detail, props.Reason, props.Code)

	var parts []string

	if props.Error.Data.Message != "" {
		parts = append(parts, props.Error.Data.Message)
	}
	if props.Error.Message != "" && props.Error.Message != props.Error.Data.Message {
		parts = append(parts, props.Error.Message)
	}
	if props.Message != "" && props.Message != props.Error.Message && props.Message != props.Error.Data.Message {
		parts = append(parts, props.Message)
	}
	if props.Detail != "" {
		parts = append(parts, props.Detail)
	}
	if props.Error.Detail != "" && props.Error.Detail != props.Detail {
		parts = append(parts, props.Error.Detail)
	}
	if props.Reason != "" {
		parts = append(parts, props.Reason)
	}

	if len(parts) == 0 {
		return ""
	}

	code := props.Error.Code
	if code == "" {
		code = props.Code
	}
	if code == "" {
		code = props.Error.Type
	}
	if code == "" {
		code = props.Error.Name
	}

	message := strings.Join(parts, "; ")
	if code != "" {
		return fmt.Sprintf("%s (%s)", message, code)
	}
	return message
}

// extractQuestionFromEvent 从 question.asked 或 permission.asked 事件中提取问题内容和选项
func (s *StreamingSessionHandler) extractQuestionFromEvent(event *opencode.EventListResponse) (*Question, string) {
	if event == nil {
		return nil, ""
	}

	// 支持 question.asked 和 permission.asked 两种事件类型
	if event.Type != "question.asked" && event.Type != "permission.asked" {
		return nil, ""
	}

	jsonData := event.JSON.RawJSON()
	if jsonData == "" {
		return nil, ""
	}

	// 添加调试日志查看实际内容
	log.Printf("opencode: extracting from %s event, json: %s", event.Type, jsonData)

	// 处理 permission.asked 事件
	if event.Type == "permission.asked" {
		return s.extractPermissionQuestion(jsonData)
	}

	// 处理 question.asked 事件
	return s.extractNormalQuestion(jsonData)
}

// extractPermissionQuestion 从 permission.asked 事件中提取权限请求
func (s *StreamingSessionHandler) extractPermissionQuestion(jsonData string) (*Question, string) {
	// permission.asked 事件结构：
	// {"type":"permission.asked","properties":{"id":"per_xxx","sessionID":"ses_xxx",
	//   "permission":"external_directory","patterns":["path/*"],
	//   "metadata":{"filepath":"...","parentDir":"..."},"always":["path/*"],
	//   "tool":{"messageID":"msg_xxx","callID":"xxx"}}}
	type PermissionProps struct {
		ID         string   `json:"id"`
		SessionID  string   `json:"sessionID"`
		Permission string   `json:"permission"`
		Patterns   []string `json:"patterns"`
		Metadata   struct {
			Filepath  string `json:"filepath"`
			ParentDir string `json:"parentDir"`
		} `json:"metadata"`
		Always []string `json:"always"`
		Tool   struct {
			MessageID string `json:"messageID"`
			CallID    string `json:"callID"`
		} `json:"tool"`
	}

	var wrapper struct {
		Properties PermissionProps `json:"properties"`
	}
	if err := json.Unmarshal([]byte(jsonData), &wrapper); err != nil {
		log.Printf("opencode: failed to parse permission.asked event: %v", err)
		return nil, ""
	}

	props := wrapper.Properties
	if props.ID == "" {
		log.Printf("opencode: permission.asked event missing id")
		return nil, ""
	}

	// 构造用户友好的权限描述
	var permDesc string
	switch props.Permission {
	case "external_directory":
		permDesc = "访问外部目录"
	case "write_file":
		permDesc = "写入文件"
	case "execute_command":
		permDesc = "执行命令"
	case "network_access":
		permDesc = "网络访问"
	default:
		permDesc = props.Permission
	}

	// 构造描述文本
	var details string
	if len(props.Patterns) > 0 {
		details = fmt.Sprintf("路径: %s", strings.Join(props.Patterns, ", "))
	}
	if props.Metadata.Filepath != "" {
		if details != "" {
			details += "\n"
		}
		details += fmt.Sprintf("文件: %s", props.Metadata.Filepath)
	}

	questionText := fmt.Sprintf("%s\n%s", permDesc, details)

	log.Printf("opencode: extracted permission - id: '%s', permission: '%s', patterns: %v, parentDir: '%s'",
		props.ID, props.Permission, props.Patterns, props.Metadata.ParentDir)

	// 使用权限事件的原始 ID
	question := &Question{
		ID:           props.ID, // 使用原始权限 ID
		SessionID:    props.SessionID,
		MessageID:    props.Tool.MessageID,
		Text:         questionText,
		Options:      []string{"允许", "拒绝", "始终允许"},
		IsPermission: true,                     // 标记为权限请求
		Directory:    props.Metadata.ParentDir, // 保存工作目录
		CreatedAt:    time.Now(),
	}

	// 构造消息 - 简化用户回复方式（去掉Markdown格式避免钉钉显示问题）
	msg := fmt.Sprintf("🔐 OpenCode 请求权限：\n\n"+
		"【%s】\n\n"+
		"%s\n\n"+
		"请回复：\n"+
		"• 允许 - 本次允许\n"+
		"• 拒绝 - 拒绝此请求\n"+
		"• 始终允许 - 以后都允许",
		permDesc, details)

	log.Printf("opencode: permission message to send: %s", msg)
	return question, msg
}

// cleanupPendingQuestionFromEvent extracts the question/permission ID from a
// permission.answered, permission.replied, or question.answered event and
// removes it from the client's pending questions map. This prevents stale
// entries when the opencode server auto-clears related permissions (e.g. an
// "always" answer for one permission cascades to clear other permissions for
// the same directory, but the gateway's local cache would otherwise retain
// those stale entries indefinitely).
func (s *StreamingSessionHandler) cleanupPendingQuestionFromEvent(event *opencode.EventListResponse) {
	if event == nil || s.client == nil {
		return
	}
	jsonData := event.JSON.RawJSON()
	if jsonData == "" {
		return
	}
	// permission.replied / permission.answered / question.answered events have
	// the same structure as their "asked" counterparts: {"properties":{"id":"per_xxx",...}}
	var wrapper struct {
		Properties struct {
			ID string `json:"id"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(jsonData), &wrapper); err != nil {
		return
	}
	if wrapper.Properties.ID != "" {
		s.client.DeletePendingQuestion(wrapper.Properties.ID)
		log.Printf("opencode: cleaned up pending question/permission %s after %s event for session %s",
			wrapper.Properties.ID, event.Type, s.sessionID[:8])
	}
}

// extractNormalQuestion 从 question.asked 事件中提取普通问题
func (s *StreamingSessionHandler) extractNormalQuestion(jsonData string) (*Question, string) {
	// question.asked 事件结构：
	// {"type":"question.asked","properties":{
	//   "id":"que_xxx","sessionID":"ses_xxx",
	//   "questions":[{"header":"标题","question":"问题","multiple":false,
	//     "options":[{"label":"选项1","description":"描述1"},...]},...],
	//   "tool":{"messageID":"msg_xxx","callID":"xxx"}}}

	// 本地类型用于 JSON 解析（避免与包级别类型冲突）
	type jsonOptionItem struct {
		Label       string `json:"label"`
		Description string `json:"description"`
	}

	type jsonQuestionItem struct {
		Header   string           `json:"header"`
		Question string           `json:"question"`
		Multiple bool             `json:"multiple"`
		Options  []jsonOptionItem `json:"options"`
	}

	type QuestionProps struct {
		ID        string             `json:"id"`
		SessionID string             `json:"sessionID"`
		Questions []jsonQuestionItem `json:"questions"`
		Tool      struct {
			MessageID string `json:"messageID"`
			CallID    string `json:"callID"`
		} `json:"tool"`
		// 兼容旧格式
		Question string   `json:"question"`
		Text     string   `json:"text"`
		Message  string   `json:"message"`
		Options  []string `json:"options"`
		Choices  []string `json:"choices"`
	}

	var wrapper struct {
		Properties QuestionProps `json:"properties"`
	}
	if err := json.Unmarshal([]byte(jsonData), &wrapper); err != nil {
		log.Printf("opencode: failed to parse question.asked event: %v", err)
		return nil, ""
	}

	props := wrapper.Properties

	// 使用事件中的 ID（如 que_xxx）
	questionID := props.ID
	if questionID == "" {
		questionID = fmt.Sprintf("q_%d", time.Now().UnixNano())
	}

	messageID := props.Tool.MessageID
	sessionID := props.SessionID
	if sessionID == "" {
		sessionID = s.sessionID
	}

	// 处理新格式：questions 数组
	if len(props.Questions) > 0 {
		var msgBuilder strings.Builder
		msgBuilder.WriteString("❓ OpenCode 需要您的选择：\n\n")

		var allOptions []string          // 收集所有选项用于存储
		var questionItems []QuestionItem // 保存详细的问题信息

		for i, q := range props.Questions {
			if q.Header != "" {
				msgBuilder.WriteString(fmt.Sprintf("**【%s】**\n", q.Header))
			}
			msgBuilder.WriteString(fmt.Sprintf("%s\n", q.Question))
			if q.Multiple {
				msgBuilder.WriteString("（可多选）\n")
			}

			// 转换为 client.go 中定义的 QuestionItem 类型
			qi := QuestionItem{
				Header:   q.Header,
				Question: q.Question,
				Multiple: q.Multiple,
				Options:  make([]QuestionOption, 0, len(q.Options)),
			}

			for j, opt := range q.Options {
				optNum := j + 1
				if len(props.Questions) > 1 {
					// 多个问题时使用 "问题序号.选项序号" 格式
					msgBuilder.WriteString(fmt.Sprintf("  %d.%d. %s", i+1, optNum, opt.Label))
				} else {
					msgBuilder.WriteString(fmt.Sprintf("  %d. %s", optNum, opt.Label))
				}
				if opt.Description != "" {
					msgBuilder.WriteString(fmt.Sprintf(" - %s", opt.Description))
				}
				msgBuilder.WriteString("\n")
				allOptions = append(allOptions, opt.Label)

				// 添加到 QuestionItem
				qi.Options = append(qi.Options, QuestionOption{
					Label:       opt.Label,
					Description: opt.Description,
				})
			}
			msgBuilder.WriteString("\n")
			questionItems = append(questionItems, qi)
		}

		// 简化回复方式
		if len(props.Questions) == 1 && !props.Questions[0].Multiple {
			msgBuilder.WriteString("直接回复选项序号（如 `1`）或选项名称")
		} else if len(props.Questions) > 1 {
			msgBuilder.WriteString("多个问题用分号分隔回复，如 `1;2,3;1`（第一个问题选1，第二个问题选2和3，第三个问题选1）")
		} else {
			msgBuilder.WriteString(fmt.Sprintf("回复 `/answer %s <选择>` 来确认", questionID))
		}

		question := &Question{
			ID:           questionID,
			SessionID:    sessionID,
			MessageID:    messageID,
			Text:         props.Questions[0].Question, // 使用第一个问题作为主文本
			Options:      allOptions,
			Questions:    questionItems, // 保存详细的问题信息
			IsPermission: false,
			CreatedAt:    time.Now(),
		}

		log.Printf("opencode: extracted questions - id: '%s', messageID: '%s', questions: %d, options: %v",
			questionID, messageID, len(props.Questions), allOptions)

		return question, msgBuilder.String()
	}

	// 兼容旧格式
	questionText := props.Question
	if questionText == "" {
		questionText = props.Text
	}
	if questionText == "" {
		questionText = props.Message
	}

	options := props.Options
	if len(options) == 0 {
		options = props.Choices
	}

	log.Printf("opencode: extracted question (legacy) - text: '%s', messageID: '%s', options: %v",
		questionText, messageID, options)

	if questionText == "" {
		return nil, ""
	}

	question := &Question{
		ID:        questionID,
		SessionID: sessionID,
		MessageID: messageID,
		Text:      questionText,
		Options:   options,
		CreatedAt: time.Now(),
	}

	// 构造消息
	msg := fmt.Sprintf("⚠️ OpenCode 需要您的确认：\n\n%s\n\n", questionText)

	if len(options) > 0 {
		msg += "请选择：\n"
		for i, opt := range options {
			msg += fmt.Sprintf("%d. %s\n", i+1, opt)
		}
		msg += "\n直接回复选项序号（如 `1`）或选项名称"
	} else {
		msg += "回复 `yes` 确认，或 `no` 取消"
	}

	return question, msg
}

// IsCompleted 检查是否已完成
func (s *StreamingSessionHandler) IsCompleted() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.completed
}

// GetLastContent 获取最后的内容
func (s *StreamingSessionHandler) GetLastContent() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastContent
}

// HasSentContent 检查是否已发送过内容
func (s *StreamingSessionHandler) HasSentContent() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.contentSent
}

// HasReceivedStepFinish 检查是否已收到 step-finish 事件
func (s *StreamingSessionHandler) HasReceivedStepFinish() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.receivedStepFinish
}

// GetStepFinishTime 获取收到 step-finish 的时间
func (s *StreamingSessionHandler) GetStepFinishTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stepFinishTime
}

// HasActiveTools 检查是否仍有 pending/running 状态的 tool part
func (s *StreamingSessionHandler) HasActiveTools() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.activeToolParts) > 0
}

// ActiveToolCount 返回当前活跃（pending/running）tool part 数量
func (s *StreamingSessionHandler) ActiveToolCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.activeToolParts)
}

// GetLastEventInfo returns the latest meaningful turn activity. Passive
// session.status heartbeats are excluded so they cannot keep a stuck turn
// alive or make /status claim the model is still making progress.
func (s *StreamingSessionHandler) GetLastEventInfo() (time.Time, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastActivityTime, s.lastActivityType
}

// RetryState describes the upstream provider retry status observed for the
// current turn. Attempt is the highest retry attempt seen; Message is the most
// recent upstream error text (e.g. "No available channel for model GLM-5.1").
type RetryState struct {
	Attempt    int
	Message    string
	StatusCode int
	LastAt     time.Time
}

// GetRetryState returns a snapshot of the upstream retry status for this turn.
func (s *StreamingSessionHandler) GetRetryState() RetryState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return RetryState{
		Attempt:    s.retryAttempt,
		Message:    s.retryMessage,
		StatusCode: s.retryStatusCode,
		LastAt:     s.lastRetryTime,
	}
}

// PendingQuestionInfo describes whether the session is currently blocked
// waiting for user input (permission.asked / question.asked) and for how long.
type PendingQuestionInfo struct {
	Since         time.Time
	ReminderSent  bool
	ReminderCount int
}

// GetPendingQuestionInfo returns the pending-question state for the ticker.
func (s *StreamingSessionHandler) GetPendingQuestionInfo() PendingQuestionInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return PendingQuestionInfo{
		Since:         s.pendingQuestionSince,
		ReminderSent:  s.pendingQuestionSent,
		ReminderCount: s.pendingQuestionReminders,
	}
}

// MarkPendingQuestionReminderSent records that a "still waiting for your
// input" reminder has been pushed, so the ticker does not spam it.
func (s *StreamingSessionHandler) MarkPendingQuestionReminderSent() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pendingQuestionSent = true
	s.pendingQuestionReminders++
}

// handleRetryPart parses a retry message part and tracks the upstream provider
// retry status. opencode surfaces transient provider failures (model
// unavailable, rate-limited, no available channel, etc.) as parts of type
// "retry" with an incrementing attempt counter; these never produce a
// session.idle event, so without intervention the adapter stays stuck in
// "正在处理中". When the attempt count reaches maxRetryAttempts, the upstream
// error is surfaced to the user and the turn is completed.
//
// Caller must NOT hold s.mu (this method locks internally).
func (s *StreamingSessionHandler) handleRetryPart(rawJSON string) {
	if rawJSON == "" {
		return
	}
	var wrapper struct {
		Properties struct {
			Part struct {
				Type    string  `json:"type"`
				Attempt float64 `json:"attempt"`
				Error   struct {
					Name string `json:"name"`
					Data struct {
						Message      string `json:"message"`
						StatusCode   int    `json:"statusCode"`
						IsRetryable  bool   `json:"isRetryable"`
						ResponseBody string `json:"responseBody"`
					} `json:"data"`
				} `json:"error"`
			} `json:"part"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(rawJSON), &wrapper); err != nil {
		log.Printf("opencode: failed to parse retry part for session %s: %v", s.sessionID[:min(8, len(s.sessionID))], err)
		return
	}

	part := wrapper.Properties.Part
	attempt := int(part.Attempt)
	msg := strings.TrimSpace(part.Error.Data.Message)
	if msg == "" {
		msg = strings.TrimSpace(part.Error.Name)
	}

	s.mu.Lock()
	if attempt > s.retryAttempt {
		s.retryAttempt = attempt
	}
	if msg != "" {
		s.retryMessage = msg
	}
	if part.Error.Data.StatusCode != 0 {
		s.retryStatusCode = part.Error.Data.StatusCode
	}
	s.lastRetryTime = time.Now()
	curAttempt := s.retryAttempt
	curMsg := s.retryMessage
	curCode := s.retryStatusCode
	alreadySurfaced := s.retrySurfaced
	reachedLimit := curAttempt >= maxRetryAttempts
	if reachedLimit && !alreadySurfaced {
		s.retrySurfaced = true
	}
	s.mu.Unlock()

	log.Printf("opencode: 🔁 retry part for session %s (attempt=%d/%d, statusCode=%d, msg=%q)",
		s.sessionID[:min(8, len(s.sessionID))], curAttempt, maxRetryAttempts, curCode, curMsg)

	// 未达上限：给用户一条轻量的"重试中"提示，让体感不至于失联，但不结束会话。
	if !reachedLimit {
		notice := fmt.Sprintf("⏳ 上游模型暂时不可用，正在重试（第 %d 次）…", curAttempt)
		s.emitEvent(StreamEventInfo, notice, nil, nil, nil, "retry")
		return
	}

	// 已达上限且尚未报错过：尝试熔断 + 自动切换模型。
	// 如果切换成功，则不向用户报错；否则 surface 一条明确的错误并结束本轮。
	if alreadySurfaced {
		return
	}

	// Circuit breaker: record the failure and attempt an automatic
	// fallback to a healthy model. This is the primary path for provider
	// failures — opencode's upstream retry exhausts, and we switch models
	// instead of just surfacing the error.
	if s.client != nil && s.client.breaker != nil && s.lastModelUsed != nil && !s.fallbackRetryAttempted {
		if s.client.breaker.IsProviderLevelError(curMsg) {
			pid := s.lastModelUsed.ProviderID.Value
			mid := s.lastModelUsed.ModelID.Value
			s.client.breaker.RecordFailure(pid, mid, curMsg)
			log.Printf("opencode: 🔌 circuit breaker recorded failure for %s/%s (state=%s) from retry-limit",
				pid, mid, s.client.breaker.StateString(pid, mid))

			s.fallbackRetryAttempted = true
			if s.client.attemptFallbackRetry(s.sessionID, s.payload, s.lastModelUsed, s.callback, s.eventCallback) {
				// Fallback dispatched: do not surface the error to the
				// user. Mark this handler complete so the SSE loop
				// exits; the new turn's handler takes over.
				s.fallbackRetryDispatched = true
				s.completed = true
				s.notifyCompletion()
				log.Printf("opencode: retry-limit handled via fallback retry for session %s", s.sessionID[:8])
				return
			}
		}
	}

	errText := fmt.Sprintf("⚠️ 上游模型多次重试仍失败（已重试 %d 次），本轮中止。", curAttempt)
	if curMsg != "" {
		detail := curMsg
		if len(detail) > 300 {
			detail = detail[:300] + "…"
		}
		errText += "\n\n" + detail
	}
	if curCode != 0 {
		errText += fmt.Sprintf("\n(HTTP %d)", curCode)
	}
	errText += "\n\n会话上下文已保留，请稍后重试或更换模型。"

	if err := s.callback(QuestionSignalPrefix + errText); err != nil {
		log.Printf("opencode: retry-limit error callback failed for session %s: %v", s.sessionID[:min(8, len(s.sessionID))], err)
	} else {
		s.emitEvent(StreamEventError, errText, nil, nil, nil, "retry")
		s.mu.Lock()
		s.contentSent = true
		s.mu.Unlock()
	}

	s.mu.Lock()
	s.completed = true
	s.mu.Unlock()
	s.notifyCompletion()
	log.Printf("opencode: 🛑 retry limit reached for session %s, turn aborted (attempt=%d)",
		s.sessionID[:min(8, len(s.sessionID))], curAttempt)
}

// IsActivelyProcessing 检查session是否正在活跃处理中
// 通过判断最近30秒内是否收到过活动事件来确定
func (s *StreamingSessionHandler) IsActivelyProcessing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 如果已完成，则不是活跃处理中
	if s.completed {
		return false
	}

	// 如果从未收到过事件
	if s.lastEventTime.IsZero() {
		return false
	}

	// 检查最近30秒内是否收到过事件
	return time.Since(s.lastEventTime) < 30*time.Second
}

// FlushSignal is a special callback token that tells adapters to immediately
// flush all accumulated-but-unsent content to the user.  It is sent by
// notifyCompletion AFTER all regular callbacks so that the flush runs
// synchronously inside the SSE goroutine (while s.mu is held), guaranteeing
// that SendMessageStreaming cannot return before the final send completes.
const FlushSignal = "\x00flush"

// ThinkingSignalPrefix is a special callback token that marks reasoning/thinking
// chunks. Adapters should route these chunks as independent "thinking" output
// and MUST NOT append them into final answer accumulation buffers.
const ThinkingSignalPrefix = "\x00thinking:"

// ToolSignalPrefix marks tool lifecycle messages (running/completed/error).
const ToolSignalPrefix = "\x00tool:"

// StepSignalPrefix marks step lifecycle messages.
const StepSignalPrefix = "\x00step:"

// TodoSignalPrefix marks auto-pushed todo progress updates.
const TodoSignalPrefix = "\x00todo:"

// QuestionSignalPrefix marks permission/question messages that need immediate
// delivery to the user (not buffered in content accumulation).
const QuestionSignalPrefix = "\x00question:"

// TodoAutoPushInterval controls minimum interval between auto todo updates.
const TodoAutoPushInterval = 5 * time.Second

func (s *StreamingSessionHandler) notifyCompletion() {
	s.stopWaitingTimer()

	// 发送 flush 信号：让 adapter 立即把 fullReply 中尚未发送的内容全部发出去。
	// 这发生在 SSE goroutine 持有 s.mu 期间，确保在 SendMessageStreaming 返回之前
	// 最终内容已经发送完毕，避免任何竞态条件。
	_ = s.callback(FlushSignal)
	s.emitEventFromChunk(FlushSignal)

	s.fireOnComplete()
}

// fireOnComplete 通过 sync.Once 保证 onComplete 回调只执行一次。
// session.idle(notifyCompletion) 和 finalizeAsyncStreamingResponse 都可能触发，
// 幂等化避免重复注销 handler / 重复清理。
func (s *StreamingSessionHandler) fireOnComplete() {
	if s.onComplete != nil {
		s.onCompleteOnce.Do(func() {
			go s.onComplete()
		})
	}
}

// stopWaitingTimer 取消等待提示定时器
func (s *StreamingSessionHandler) stopWaitingTimer() {
	if s.waitingTimer != nil {
		s.waitingTimer.Stop()
	}
}

// ResetForNewPrompt clears state that may have been polluted by a preceding
// compression (SummarizeSession) operation. Compression emits its own
// message.part.delta / step-finish / session.idle events which populate
// lastContent, set contentSent/receivedStepFinish, and may even trigger
// onComplete deregistration. Without resetting, the async ticker observes
// stale "completed" state and returns immediately, dropping all real
// response events (including permission.asked — which deadlocks the
// session on the opencode server side).
//
// This must be called right after the real prompt_async is accepted and
// before waiting for SSE events.
func (s *StreamingSessionHandler) ResetForNewPrompt() {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("opencode: resetting handler state for session %s (clearing compression artifacts: contentSent=%t, completed=%t, lastContentLen=%d)",
		s.sessionID[:min(8, len(s.sessionID))], s.contentSent, s.completed, len(s.lastContent))

	s.completed = false
	s.contentSent = false
	s.lastContent = ""
	s.receivedStepFinish = false
	s.stepFinishTime = time.Time{}
	s.waitingHintSent = false
	s.lastEventTime = time.Now()
	s.lastEventType = ""
	s.lastActivityTime = time.Now()
	s.lastActivityType = ""
	s.pendingQuestionSince = time.Time{}
	s.pendingQuestionSent = false
	s.pendingQuestionReminders = 0
	s.activeToolParts = make(map[string]bool)
	s.sessionTodos = nil
	s.sessionDiff = nil
	s.lastTodoSummary = ""
	s.lastTodoStateHash = ""
	s.lastTodoPushTime = time.Time{}

	// Clear part caches so compression-era partIDs don't shadow real parts.
	s.partTextCache.Range(func(k, _ any) bool { s.partTextCache.Delete(k); return true })
	s.toolSignalCache.Range(func(k, _ any) bool { s.toolSignalCache.Delete(k); return true })
	s.partRoles.Range(func(k, _ any) bool { s.partRoles.Delete(k); return true })

	// Reset the onCompleteOnce so a future real session.idle can fire it.
	// This is safe because fireOnComplete is guarded by sync.Once; resetting
	// the Once allows the real completion (post-compression) to trigger
	// the cleanup callback exactly once.
	s.onCompleteOnce = sync.Once{}

	// Restart the waiting hint timer for the new prompt.
	if s.waitingTimer != nil {
		s.waitingTimer.Stop()
	}
	s.waitingTimer = time.AfterFunc(8*time.Second, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.contentSent && !s.completed && !s.waitingHintSent {
			s.waitingHintSent = true
			_ = s.callback(QuestionSignalPrefix + "⏳ 正在处理中，无需操作，请稍候...")
		}
	})
}

// extractPartDelta 从 message.part.delta 事件中提取增量文本
// 对照 TUI sync.tsx 中的 "message.part.delta" case 实现
// 事件结构: {properties: {messageID, partID, field, delta}}
func (s *StreamingSessionHandler) extractPartDelta(event *opencode.EventListResponse) string {
	if event == nil || event.Type != "message.part.delta" {
		return ""
	}
	raw := event.JSON.RawJSON()
	if raw == "" {
		return ""
	}
	var wrapper struct {
		Properties struct {
			MessageID string `json:"messageID"`
			PartID    string `json:"partID"`
			Field     string `json:"field"`
			Delta     string `json:"delta"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(raw), &wrapper); err != nil {
		log.Printf("opencode: failed to parse message.part.delta: %v", err)
		return ""
	}
	props := wrapper.Properties
	// 只处理 text 字段的增量（忽略 reasoning、snapshot 等内部字段）
	if props.Field != "text" {
		log.Printf("opencode: message.part.delta field=%s (not text, ignored for session %s)", props.Field, s.sessionID[:8])
		return ""
	}
	if props.Delta == "" {
		return ""
	}
	// 更新 partTextCache 保持与全文状态同步
	if props.PartID != "" {
		existing, _ := s.partTextCache.Load(props.PartID)
		existingStr, _ := existing.(string)
		s.partTextCache.Store(props.PartID, existingStr+props.Delta)
	}
	log.Printf("opencode: message.part.delta: partID=%s, delta_len=%d for session %s",
		props.PartID, len(props.Delta), s.sessionID[:8])
	return props.Delta
}

// extractTodosFromEvent 从 todo.updated 事件中解析 todo 列表
// 对照 TUI sync.tsx: {properties: {sessionID, todos[]}}
func extractTodosFromEvent(event *opencode.EventListResponse) []TodoItem {
	if event == nil || event.Type != "todo.updated" {
		return nil
	}
	raw := event.JSON.RawJSON()
	if raw == "" {
		return nil
	}
	var wrapper struct {
		Properties struct {
			SessionID string     `json:"sessionID"`
			Todos     []TodoItem `json:"todos"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(raw), &wrapper); err != nil {
		log.Printf("opencode: failed to parse todo.updated: %v", err)
		return nil
	}
	return wrapper.Properties.Todos
}

// extractDiffFromEvent 从 session.diff 事件中解析文件变更列表
// 对照 TUI sync.tsx: {properties: {sessionID, diff[]}}
func extractDiffFromEvent(event *opencode.EventListResponse) []FileDiff {
	if event == nil || event.Type != "session.diff" {
		return nil
	}
	raw := event.JSON.RawJSON()
	if raw == "" {
		return nil
	}
	var wrapper struct {
		Properties struct {
			SessionID string     `json:"sessionID"`
			Diff      []FileDiff `json:"diff"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(raw), &wrapper); err != nil {
		log.Printf("opencode: failed to parse session.diff: %v", err)
		return nil
	}
	return wrapper.Properties.Diff
}

// GetTodos 返回本次会话当前的 todo 列表（线程安全）
func (s *StreamingSessionHandler) GetTodos() []TodoItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]TodoItem, len(s.sessionTodos))
	copy(result, s.sessionTodos)
	return result
}

// GetDiff 返回本次会话的文件变更列表（线程安全）
func (s *StreamingSessionHandler) GetDiff() []FileDiff {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]FileDiff, len(s.sessionDiff))
	copy(result, s.sessionDiff)
	return result
}

// FormatDiffSummary 将文件变更格式化为用户友好的文本摘要
func (s *StreamingSessionHandler) FormatDiffSummary() string {
	if len(s.sessionDiff) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("\n\n📁 **文件变更摘要**\n")
	totalAdded, totalRemoved := 0, 0
	for _, f := range s.sessionDiff {
		icon := "📝"
		if f.Added > 0 && f.Removed == 0 {
			icon = "🆕"
		} else if f.Added == 0 && f.Removed > 0 {
			icon = "🗑️"
		}
		sb.WriteString(fmt.Sprintf("%s `%s`", icon, f.Path))
		if f.Added > 0 || f.Removed > 0 {
			sb.WriteString(fmt.Sprintf(" (+%d/-%d)", f.Added, f.Removed))
		}
		sb.WriteString("\n")
		totalAdded += f.Added
		totalRemoved += f.Removed
	}
	if len(s.sessionDiff) > 1 {
		sb.WriteString(fmt.Sprintf("共 %d 个文件，+%d/-%d 行\n", len(s.sessionDiff), totalAdded, totalRemoved))
	}
	return sb.String()
}

// FormatTodoSummary 将 todo 列表格式化为用户友好的文本
func (s *StreamingSessionHandler) FormatTodoSummary() string {
	todos := s.GetTodos()
	return formatTodoSummaryFromTodos(todos)
}

func formatTodoSummaryFromTodos(todos []TodoItem) string {
	if len(todos) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString("📋 任务进度更新\n")
	for _, todo := range todos {
		var icon string
		var statusText string
		switch todo.Status {
		case "completed":
			icon = "✅"
			statusText = "已完成"
		case "in_progress":
			icon = "🔄"
			statusText = "进行中"
		case "cancelled":
			icon = "❌"
			statusText = "已取消"
		default:
			icon = "⬜"
			statusText = "待处理"
		}
		sb.WriteString(fmt.Sprintf("%s [%s][优先级:%s] %s\n", icon, statusText, todo.PriorityLabel(), todo.Text()))
	}
	return sb.String()
}

// todoStateHash computes a structural hash of the todo list based on each
// item's status and text. Two todo lists with the same items in the same
// states produce the same hash, regardless of formatting. This is used for
// dedup so that any status change (e.g. pending→in_progress) triggers a push
// even when the formatted summary string happens to be identical.
func todoStateHash(todos []TodoItem) string {
	if len(todos) == 0 {
		return ""
	}
	var sb strings.Builder
	for _, t := range todos {
		sb.WriteString(t.Status)
		sb.WriteByte('|')
		sb.WriteString(t.Text())
		sb.WriteByte('\n')
	}
	return sb.String()
}

// extractSessionIDFromEvent 从事件中提取 sessionID
func extractSessionIDFromEvent(event *opencode.EventListResponse) string {
	if event == nil || event.JSON.RawJSON() == "" {
		return ""
	}

	var wrapper struct {
		Properties struct {
			SessionID string `json:"sessionID"`
			Message   struct {
				SessionID string `json:"sessionID"`
			} `json:"message"`
			Part struct {
				SessionID string `json:"sessionID"`
			} `json:"part"`
			Info struct {
				ID string `json:"id"`
			} `json:"info"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(event.JSON.RawJSON()), &wrapper); err != nil {
		return ""
	}

	if wrapper.Properties.SessionID != "" {
		return wrapper.Properties.SessionID
	}
	if wrapper.Properties.Message.SessionID != "" {
		return wrapper.Properties.Message.SessionID
	}
	if wrapper.Properties.Part.SessionID != "" {
		return wrapper.Properties.Part.SessionID
	}
	// session.created/session.updated events have sessionID in properties.info.id
	if wrapper.Properties.Info.ID != "" && strings.HasPrefix(wrapper.Properties.Info.ID, "ses_") {
		return wrapper.Properties.Info.ID
	}
	return ""
}
