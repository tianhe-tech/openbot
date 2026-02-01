package opencode

import (
	"context"
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

// StreamingSessionHandler 处理流式会话输出
type StreamingSessionHandler struct {
	sessionID      string
	callback       StreamCallback
	lastContent    string
	lastUpdateTime time.Time
	mu             sync.Mutex
	completed      bool
}

// NewStreamingSessionHandler 创建流式会话处理器
func NewStreamingSessionHandler(sessionID string, callback StreamCallback) *StreamingSessionHandler {
	return &StreamingSessionHandler{
		sessionID:      sessionID,
		callback:       callback,
		lastUpdateTime: time.Now(),
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

	// 仅处理与当前session相关的事件
	eventType := string(event.Type)
	log.Printf("opencode: streaming handler received event type=%s for session=%s", eventType, s.sessionID[:8])

	// 根据事件类型处理
	switch eventType {
	case "session.updated", "message.updated", "message.completed":
		// 提取新内容（这里需要根据实际SDK结构调整）
		// 假设事件中包含消息内容或增量更新
		newContent := s.extractContentFromEvent(event)
		if newContent != "" && newContent != s.lastContent {
			// 计算增量内容
			incremental := newContent
			if strings.HasPrefix(newContent, s.lastContent) {
				incremental = strings.TrimPrefix(newContent, s.lastContent)
			}

			if incremental != "" {
				// 调用回调发送增量内容
				if err := s.callback(incremental); err != nil {
					log.Printf("opencode: streaming callback error: %v", err)
				}
				s.lastContent = newContent
				s.lastUpdateTime = time.Now()
			}
		}

		// 如果是完成事件，标记为已完成
		if eventType == "message.completed" {
			s.completed = true
		}

	case "session.error":
		log.Printf("opencode: session %s encountered error", s.sessionID[:8])
		s.completed = true
	}

	return nil
}

// extractContentFromEvent 从事件中提取内容
func (s *StreamingSessionHandler) extractContentFromEvent(event *opencode.EventListResponse) string {
	// TODO: 根据实际SDK事件结构提取内容
	// 这里需要查看opencode.EventListResponse的具体字段
	// 可能的字段：event.Data, event.Message, event.Content等

	// 暂时返回空字符串，需要根据实际情况调整
	return ""
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
