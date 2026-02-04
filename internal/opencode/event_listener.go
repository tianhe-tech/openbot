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
	lastContent        string
	lastUpdateTime     time.Time
	lastEventTime      time.Time // 最后一次收到SSE事件的时间
	lastEventType      string    // 最后一次收到的事件类型
	mu                 sync.Mutex
	completed          bool
	contentSent        bool // 标记是否已发送过内容
	waitingForResponse bool // 标记是否正在等待用户回复（如权限确认、问题回答）
	onComplete         func()
	client             *Client       // 用于存储问题
	messageSender      MessageSender // 用于主动推送消息到用户
}

// NewStreamingSessionHandler 创建流式会话处理器
func NewStreamingSessionHandler(sessionID string, callback StreamCallback, onComplete func(), client *Client, messageSender MessageSender) *StreamingSessionHandler {
	return &StreamingSessionHandler{
		sessionID:      sessionID,
		callback:       callback,
		lastUpdateTime: time.Now(),
		onComplete:     onComplete,
		client:         client,
		messageSender:  messageSender,
	}
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

// AdapterMessageHandler creates an event handler that forwards messages to adapters.
type AdapterMessageHandler struct {
	sendToAdapter func(ctx context.Context, channel, userID, content string) error
}

// NewAdapterMessageHandler creates a handler that forwards OpenCode responses to adapters.
func NewAdapterMessageHandler(sender func(ctx context.Context, channel, userID, content string) error) *AdapterMessageHandler {
	return &AdapterMessageHandler{
		sendToAdapter: sender,
	}
}

// Handle processes incoming events and forwards appropriate messages to adapters.
func (h *AdapterMessageHandler) Handle(ctx context.Context, event *opencode.EventListResponse) error {
	// Extract message information from event
	log.Printf("opencode: received event (type=%s)", event.Type)

	// TODO: Implement actual event processing based on SDK Event structure
	// Based on the event type, route to appropriate handler:
	// - session.updated -> check for new messages
	// - message.updated -> forward to user
	// - session.error -> notify user of errors

	return nil
}

// HandleEvent 处理会话更新事件并提取增量内容
func (s *StreamingSessionHandler) HandleEvent(ctx context.Context, event *opencode.EventListResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 检查是否已完成
	if s.completed {
		return nil
	}

	if !s.belongsToSession(event) {
		return nil
	}

	// 仅处理与当前session相关的事件
	eventType := string(event.Type)
	log.Printf("opencode: streaming handler event type=%s", eventType)

	// 记录最后一次事件的时间和类型
	s.lastEventTime = time.Now()
	s.lastEventType = eventType

	// 只处理 message.part.updated 事件获取增量内容
	switch eventType {
	case "message.part.updated":
		// 提取增量内容并发送
		incremental := s.extractContentFromEvent(event)
		if incremental != "" {
			s.lastContent += incremental
			if err := s.callback(incremental); err != nil {
				log.Printf("opencode: streaming callback error: %v", err)
			} else {
				s.contentSent = true
			}
			s.lastUpdateTime = time.Now()
		}

	case "question.answered", "permission.answered":
		// 用户已回答问题或权限请求
		log.Printf("opencode: %s event processed for session %s", eventType, s.sessionID[:8])

	case "permission.replied":
		// 权限已确认（兼容性事件）
		log.Printf("opencode: %s event processed for session %s", eventType, s.sessionID[:8])

	case "question.asked", "permission.asked":
		// OpenCode 需要用户确认（build 模式或权限请求）
		// ⚠️ 不再设置 waitingForResponse，以免阻止后续事件处理
		// s.waitingForResponse = true // 注释掉，让事件继续流动
		log.Printf("opencode: user response needed (type: %s) for session %s", event.Type, s.sessionID[:8])

		question, questionMsg := s.extractQuestionFromEvent(event)
		if question != nil && s.client != nil {
			// 存储问题供后续回答
			s.client.StorePendingQuestion(question)
			log.Printf("opencode: stored question %s for session %s (type: %s, isPermission=%t)",
				question.ID, s.sessionID[:8], event.Type, question.IsPermission)
		}

		if questionMsg == "" {
			questionMsg = "⚠️ OpenCode 需要您的确认！\n\n" +
				"请在钉钉中回复相应的选项。\n" +
				"确认后结果将自动继续处理。"
			log.Printf("opencode: using fallback message for %s", event.Type)
		}
		log.Printf("opencode: sending %s message (len=%d, prefix=%s) for session %s",
			event.Type, len(questionMsg), questionMsg[:min(10, len(questionMsg))], s.sessionID[:8])

		// 只发送一次！通过streaming callback发送（将权限请求内联到输出流中）
		if err := s.callback(questionMsg); err != nil {
			log.Printf("opencode: question/permission callback error: %v", err)
		} else {
			log.Printf("opencode: %s message sent via callback successfully", event.Type)
			s.contentSent = true
		}

	case "message.completed":
		// 完成事件，处理最终内容
		finalContent := s.extractCompleteContent(event)
		if finalContent != "" && finalContent != s.lastContent {
			if strings.HasPrefix(finalContent, s.lastContent) {
				incremental := strings.TrimPrefix(finalContent, s.lastContent)
				if incremental != "" {
					if err := s.callback(incremental); err != nil {
						log.Printf("opencode: streaming callback error: %v", err)
					} else {
						s.contentSent = true
					}
				}
			} else {
				if err := s.callback(finalContent); err != nil {
					log.Printf("opencode: streaming callback error: %v", err)
				} else {
					s.contentSent = true
				}
			}
			s.lastContent = finalContent
		}
		s.completed = true
		s.notifyCompletion()
		log.Printf("opencode: streaming session completed")

	case "session.idle":
		s.completed = true
		s.notifyCompletion()
		log.Printf("opencode: streaming session completed")

	case "file.edited":
		// 文件编辑事件，静默处理
		log.Printf("opencode: file edited for session %s", s.sessionID[:8])

	case "message.updated":
		// 消息更新事件，通常由 message.part.updated 处理，这里静默忽略
		// log.Printf("opencode: message updated for session %s (ignored, handled by part.updated)", s.sessionID[:8])

	case "session.updated":
		// Session 更新事件，静默处理
		log.Printf("opencode: session updated for session %s", s.sessionID[:8])

	case "session.status":
		// Session 状态事件，静默处理
		log.Printf("opencode: session status changed for session %s", s.sessionID[:8])

	case "session.diff":
		// Session 差异事件，静默处理
		log.Printf("opencode: session diff for session %s", s.sessionID[:8])

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

// extractCompleteContent 从完成事件中提取完整内容
func (s *StreamingSessionHandler) extractCompleteContent(event *opencode.EventListResponse) string {
	if event == nil {
		return ""
	}

	if event.Type == "message.completed" {
		type MessageCompletedProps struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		}

		if jsonData := event.JSON.RawJSON(); jsonData != "" {
			var wrapper struct {
				Properties MessageCompletedProps `json:"properties"`
			}
			if err := json.Unmarshal([]byte(jsonData), &wrapper); err == nil {
				content := wrapper.Properties.Message.Content
				if content != "" {
					return content
				}
			}
		}
	}
	return ""
}

// extractContentFromEvent 从事件中提取内容
func (s *StreamingSessionHandler) extractContentFromEvent(event *opencode.EventListResponse) string {
	if event == nil {
		return ""
	}

	// 尝试解析 message.part.updated 事件
	// 根据 OpenCode SDK 源码，EventListResponseEventMessagePartUpdatedProperties 包含：
	// - Delta string (增量文本)
	// - Part.Text string (part的完整文本)
	if event.Type == "message.part.updated" {
		type PartUpdateProps struct {
			Delta string `json:"delta"`
			Part  struct {
				Type string `json:"type"`
			} `json:"part"`
		}

		if jsonData := event.JSON.RawJSON(); jsonData != "" {
			var wrapper struct {
				Properties PartUpdateProps `json:"properties"`
			}
			if err := json.Unmarshal([]byte(jsonData), &wrapper); err == nil {
				props := wrapper.Properties
				if props.Delta != "" && props.Part.Type == "text" {
					return props.Delta
				}
			}
		}
	}

	// 其他事件类型暂时不提取内容
	return ""
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

// IsWaitingForResponse 检查是否正在等待用户回复（如权限确认、问题回答）
// 修改：总是返回 false，不再阻止事件处理
func (s *StreamingSessionHandler) IsWaitingForResponse() bool {
	// 不再检查 waitingForResponse 标志，让事件继续流动
	return false
}

// GetLastEventInfo 获取最后一次事件的时间和类型
func (s *StreamingSessionHandler) GetLastEventInfo() (time.Time, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastEventTime, s.lastEventType
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

func (s *StreamingSessionHandler) notifyCompletion() {
	if s.onComplete != nil {
		go s.onComplete()
	}
}

func (s *StreamingSessionHandler) belongsToSession(event *opencode.EventListResponse) bool {
	if s.sessionID == "" || event == nil {
		return true
	}

	raw := event.JSON.RawJSON()
	if raw == "" {
		return true
	}

	var wrapper struct {
		Properties struct {
			SessionID string `json:"sessionID"`
			Message   struct {
				SessionID string `json:"sessionID"`
			} `json:"message"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(raw), &wrapper); err != nil {
		return false
	}

	candidate := wrapper.Properties.SessionID
	if candidate == "" {
		candidate = wrapper.Properties.Message.SessionID
	}

	if candidate == "" {
		return true
	}

	return candidate == s.sessionID
}
