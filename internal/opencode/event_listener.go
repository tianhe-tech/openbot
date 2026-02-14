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
	contentSent        bool        // 标记是否已发送过内容
	waitingHintSent    bool        // 标记是否已发送等待提示
	receivedStepFinish bool        // 标记是否已收到 step-finish 事件
	stepFinishTime     time.Time   // 收到 step-finish 的时间
	waitingTimer       *time.Timer // 等待提示定时器
	onComplete         func()
	client             *Client       // 用于存储问题
	messageSender      MessageSender // 用于主动推送消息到用户
}

// NewStreamingSessionHandler 创建流式会话处理器
func NewStreamingSessionHandler(sessionID string, callback StreamCallback, onComplete func(), client *Client, messageSender MessageSender) *StreamingSessionHandler {
	h := &StreamingSessionHandler{
		sessionID:      sessionID,
		callback:       callback,
		lastUpdateTime: time.Now(),
		onComplete:     onComplete,
		client:         client,
		messageSender:  messageSender,
	}
	// 8秒后若仍未发送过内容，给用户一个等待提示
	h.waitingTimer = time.AfterFunc(8*time.Second, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if !h.contentSent && !h.completed && !h.waitingHintSent {
			h.waitingHintSent = true
			_ = h.callback("⏳ 正在努力处理中...\n")
		}
	})
	return h
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
		log.Printf("opencode: ignoring event for completed session %s", s.sessionID[:8])
		return nil
	}

	// 仅处理与当前session相关的事件
	log.Printf("opencode: streaming handler event type=%s", eventType)

	s.lastEventTime = time.Now()
	s.lastEventType = eventType

	// 只处理 message.part.updated 事件获取增量内容
	switch eventType {
	case "message.part.updated":
		// 检查是否是 step-finish 事件
		if jsonData := event.JSON.RawJSON(); jsonData != "" {
			var partData struct {
				Properties struct {
					Part struct {
						Type string `json:"type"`
					} `json:"part"`
				} `json:"properties"`
			}
			if err := json.Unmarshal([]byte(jsonData), &partData); err == nil {
				if partData.Properties.Part.Type == "step-finish" {
					s.receivedStepFinish = true
					s.stepFinishTime = time.Now()
					log.Printf("opencode: 🏁 received step-finish for session %s", s.sessionID[:8])
				}
			}
		}

		// 提取增量内容并发送
		incremental := s.extractContentFromEvent(event)
		if incremental != "" {
			s.lastContent += incremental
			if err := s.callback(incremental); err != nil {
				log.Printf("opencode: streaming callback error: %v", err)
			} else {
				if !s.contentSent {
					s.stopWaitingTimer()
				}
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

	case "session.idle":
		s.completed = true
		s.notifyCompletion()
		log.Printf("opencode: 🏁 streaming session completed (session=%s, contentSent=%t, lastContentLen=%d)",
			s.sessionID[:8], s.contentSent, len(s.lastContent))

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

	case "session.error":
		errorMsg := s.extractSessionError(event)
		if errorMsg == "" {
			errorMsg = "⚠️ OpenCode 会话发生错误，请稍后重试。"
		} else {
			errorMsg = fmt.Sprintf("⚠️ OpenCode 会话出错：%s", errorMsg)
		}

		if err := s.callback(errorMsg); err != nil {
			log.Printf("opencode: session.error callback error: %v", err)
		} else {
			s.contentSent = true
		}

		s.completed = true
		s.notifyCompletion()
		log.Printf("opencode: session error handled for session %s", s.sessionID[:8])

		retryMsg := "\n\n是否需要建立新的连接继续操作？\n•回复 '是' 或 'yes' 继续使用旧session\n•回复 '新' 或 'new' 创建新session"
		if err := s.callback(retryMsg); err != nil {
			log.Printf("opencode: session.error retry prompt error: %v", err)
		}

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

// extractContentFromEvent 从 message.part.updated 事件中提取用户可见内容
func (s *StreamingSessionHandler) extractContentFromEvent(event *opencode.EventListResponse) string {
	if event == nil || event.Type != "message.part.updated" {
		return ""
	}

	type ToolInput struct {
		Command     string `json:"command"`
		Description string `json:"description"`
		Filepath    string `json:"filepath"`
		URL         string `json:"url"`
	}

	type ToolState struct {
		Status string    `json:"status"`
		Input  ToolInput `json:"input"`
		Output string    `json:"output"`
		Error  string    `json:"error"`
	}

	type PartUpdateProps struct {
		Delta string `json:"delta"`
		Part  struct {
			Type  string    `json:"type"`
			Tool  string    `json:"tool"`
			State ToolState `json:"state"`
			Text  string    `json:"text"`
		} `json:"part"`
		Message struct {
			Role string `json:"role"`
		} `json:"message"`
	}

	jsonData := event.JSON.RawJSON()
	if jsonData == "" {
		log.Printf("opencode: 🔍 extractContent - empty JSON data for session %s", s.sessionID[:8])
		return ""
	}

	var wrapper struct {
		Properties PartUpdateProps `json:"properties"`
	}
	if err := json.Unmarshal([]byte(jsonData), &wrapper); err != nil {
		log.Printf("opencode: failed to unmarshal message.part.updated: %v, JSON preview: %.200s", err, jsonData)
		return ""
	}

	props := wrapper.Properties

	// 🔍 详细诊断日志
	log.Printf("opencode: 🔍 extractContent - session=%s, role=%s, partType=%s, delta_len=%d, text_len=%d",
		s.sessionID[:8], props.Message.Role, props.Part.Type, len(props.Delta), len(props.Part.Text))

	// 忽略用户角色的消息
	if props.Message.Role == "user" {
		log.Printf("opencode: 🔍 extractContent - ignoring user role message")
		return ""
	}

	partType := props.Part.Type

	switch partType {
	case "text":
		// 文本增量 - 优先使用 delta
		if props.Delta != "" {
			log.Printf("opencode: 🔍 extractContent - returning text delta (%d chars)", len(props.Delta))
			return props.Delta
		}

		// delta 为空但 text 不为空：检查是否有未发送的内容
		if props.Part.Text != "" {
			// 如果当前没有任何已发送内容，这可能是一个完整消息，应该发送
			if s.lastContent == "" {
				log.Printf("opencode: 🔍 extractContent - no previous content, sending full text (%d chars)", len(props.Part.Text))
				return props.Part.Text
			}

			// 检查完整文本是否与已发送内容一致
			if !strings.HasPrefix(props.Part.Text, s.lastContent) && props.Part.Text != s.lastContent {
				// 完整文本与已发送内容不一致，可能有遗漏，记录警告
				log.Printf("opencode: ⚠️ extractContent - text mismatch! current_len=%d, accumulated_len=%d, text_prefix=%.50s...",
					len(props.Part.Text), len(s.lastContent), props.Part.Text)
				// 计算差异部分（简单处理：如果完整文本更长，发送差异）
				if len(props.Part.Text) > len(s.lastContent) {
					diff := props.Part.Text[len(s.lastContent):]
					log.Printf("opencode: 🔍 extractContent - sending missing content (%d chars)", len(diff))
					return diff
				}
			}

			log.Printf("opencode: 🔍 extractContent - text type but delta is empty, text already sent (text_len=%d, sent_len=%d)",
				len(props.Part.Text), len(s.lastContent))
		}

	case "tool":
		// 工具调用事件
		toolName := props.Part.Tool
		state := props.Part.State
		log.Printf("opencode: 🔍 extractContent - tool event: name=%s, status=%s", toolName, state.Status)

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
				if len([]rune(desc)) > 80 {
					desc = string([]rune(desc)[:80]) + "..."
				}
				return fmt.Sprintf("🔧 [%s] %s\n", toolName, desc)
			}
			return fmt.Sprintf("🔧 正在执行 %s...\n", toolName)

		case "completed":
			output := strings.TrimSpace(state.Output)
			if output != "" && len([]rune(output)) <= 200 {
				return fmt.Sprintf("✅ [%s] %s\n", toolName, output)
			}
			// 输出为空或过长，仍发送简短完成通知
			return fmt.Sprintf("✅ [%s] 完成\n", toolName)

		case "error":
			errMsg := strings.TrimSpace(state.Error)
			if errMsg == "" {
				errMsg = "执行失败"
			}
			if len([]rune(errMsg)) > 200 {
				errMsg = string([]rune(errMsg)[:200]) + "..."
			}
			return fmt.Sprintf("❌ [%s] %s\n", toolName, errMsg)
		}

	case "reasoning", "step-start", "step-finish", "snapshot", "patch", "compaction":
		// 内部事件，不向用户发送
		log.Printf("opencode: 🔍 extractContent - internal event type: %s (not sent to user)", partType)

	case "":
		log.Printf("opencode: 🔍 extractContent - empty part type, JSON preview: %.300s", jsonData)

	default:
		log.Printf("opencode: 🔍 extractContent - unhandled part type '%s' for session %s, JSON preview: %.300s",
			partType, s.sessionID[:8], jsonData)
	}

	return ""
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
			Message string `json:"message"`
			Code    string `json:"code"`
			Detail  string `json:"detail"`
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
		log.Printf("opencode: failed to parse session.error event: %v", err)
		return ""
	}

	props := wrapper.Properties

	var parts []string

	if props.Error.Message != "" {
		parts = append(parts, props.Error.Message)
	}
	if props.Message != "" && props.Message != props.Error.Message {
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
	s.stopWaitingTimer()
	if s.onComplete != nil {
		go s.onComplete()
	}
}

// stopWaitingTimer 取消等待提示定时器
func (s *StreamingSessionHandler) stopWaitingTimer() {
	if s.waitingTimer != nil {
		s.waitingTimer.Stop()
	}
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
