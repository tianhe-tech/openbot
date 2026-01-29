package opencode

import (
	"context"
	"log"
	"sync"

	"github.com/sst/opencode-sdk-go"
)

// EventDispatcher manages event handlers and dispatches events from OpenCode server.
type EventDispatcher struct {
	handlers   map[string][]EventHandler
	handlersMu sync.RWMutex
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
