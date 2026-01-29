package opencode

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
)

// ErrEmptyPayload indicates the caller attempted to send an empty message.
var ErrEmptyPayload = errors.New("opencode: empty payload")

// Response represents the minimal data we expect back from OpenCode.
type Response struct {
	Reply     string                 `json:"reply"`
	Trace     string                 `json:"trace_id"`
	SessionID string                 `json:"session_id"`
	MessageID string                 `json:"message_id"`
	Raw       map[string]interface{} `json:"raw,omitempty"`
}

// MessagePayload collects the metadata adapters send to OpenCode.
type MessagePayload struct {
	Channel   string            `json:"channel"`
	UserID    string            `json:"user_id"`
	ThreadID  string            `json:"thread_id,omitempty"`
	SessionID string            `json:"session_id,omitempty"`
	Content   string            `json:"content"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// EventHandler defines a callback for incoming OpenCode events.
type EventHandler func(ctx context.Context, event *opencode.EventListResponse) error

// Client knows how to talk to the remote OpenCode service using the official SDK.
type Client struct {
	sdk             *opencode.Client
	eventHandlers   []EventHandler
	eventListenerMu sync.RWMutex
	sessions        sync.Map // map[threadID]sessionID
	directory       string
}

// Option mutates a client during construction.
type Option func(*Client)

// WithDirectory sets the working directory for sessions.
func WithDirectory(dir string) Option {
	return func(c *Client) {
		c.directory = dir
	}
}

// WithEventHandler registers a handler for incoming events.
func WithEventHandler(handler EventHandler) Option {
	return func(c *Client) {
		c.eventHandlers = append(c.eventHandlers, handler)
	}
}

// NewClient builds a Client instance using the official OpenCode SDK.
func NewClient(endpoint, apiKey string, opts ...Option) *Client {
	client := &Client{
		sdk: opencode.NewClient(
			option.WithBaseURL(endpoint),
			// option.WithAPIKey(apiKey), // If SDK supports API key authentication
		),
		eventHandlers: make([]EventHandler, 0),
		directory:     ".",
	}

	for _, opt := range opts {
		opt(client)
	}

	return client
}

// Ready reports if the client has enough data to operate.
func (c *Client) Ready() bool {
	return c.sdk != nil
}

// SendMessage forwards an adapter payload to OpenCode and returns its response.
func (c *Client) SendMessage(ctx context.Context, payload MessagePayload) (Response, error) {
	if !c.Ready() {
		return Response{}, fmt.Errorf("opencode: client not configured")
	}

	if strings.TrimSpace(payload.Content) == "" {
		return Response{}, ErrEmptyPayload
	}

	// Get or create session for this thread
	sessionID := payload.SessionID
	if sessionID == "" && payload.ThreadID != "" {
		if sid, ok := c.sessions.Load(payload.ThreadID); ok {
			sessionID = sid.(string)
		}
	}

	// Create new session if needed
	if sessionID == "" {
		session, err := c.sdk.Session.New(ctx, opencode.SessionNewParams{
			Title: opencode.F(fmt.Sprintf("%s-%s", payload.Channel, payload.UserID)),
		})
		if err != nil {
			return Response{}, fmt.Errorf("opencode: create session: %w", err)
		}
		sessionID = session.ID
		if payload.ThreadID != "" {
			c.sessions.Store(payload.ThreadID, sessionID)
		}
	}

	// Send chat message to session
	result, err := c.sdk.Session.Chat(ctx, sessionID, opencode.SessionChatParams{
		Parts: opencode.F([]opencode.SessionChatParamsPartUnion{
			opencode.SessionChatParamsPart{
				Text: opencode.F(payload.Content),
				Type: opencode.F(opencode.SessionChatParamsPartsTypeText),
			},
		}),
	})
	if err != nil {
		return Response{}, fmt.Errorf("opencode: send chat: %w", err)
	}

	// Extract reply from assistant message
	reply := extractReplyFromMessage(result)

	return Response{
		Reply:     reply,
		SessionID: sessionID,
		MessageID: result.Info.ID,
		Trace:     sessionID,
	}, nil
}

// GetSession retrieves session details.
func (c *Client) GetSession(ctx context.Context, sessionID string) (*opencode.Session, error) {
	return c.sdk.Session.Get(ctx, sessionID)
}

// ListSessions retrieves all sessions.
func (c *Client) ListSessions(ctx context.Context) ([]opencode.Session, error) {
	result, err := c.sdk.Session.List(ctx)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return []opencode.Session{}, nil
	}
	return *result, nil
}

// StartEventListener begins listening for OpenCode events via SSE.
func (c *Client) StartEventListener(ctx context.Context) error {
	stream := c.sdk.Event.ListStreaming(ctx)

	go func() {
		defer stream.Close()

		for stream.Next() {
			event := stream.Current()

			// Dispatch to all registered handlers
			c.eventListenerMu.RLock()
			handlers := c.eventHandlers
			c.eventListenerMu.RUnlock()

			for _, handler := range handlers {
				if err := handler(ctx, &event); err != nil {
					log.Printf("opencode: event handler error: %v", err)
				}
			}
		}

		if err := stream.Err(); err != nil {
			log.Printf("opencode: event stream error: %v", err)
		}
	}()

	return nil
}

// RegisterEventHandler adds a new event handler dynamically.
func (c *Client) RegisterEventHandler(handler EventHandler) {
	c.eventListenerMu.Lock()
	defer c.eventListenerMu.Unlock()
	c.eventHandlers = append(c.eventHandlers, handler)
}

// extractReplyFromMessage extracts text content from a chat response.
func extractReplyFromMessage(msg *opencode.SessionChatResponse) string {
	if msg == nil || len(msg.Parts) == 0 {
		return "(no response)"
	}

	var textParts []string
	for _, part := range msg.Parts {
		switch p := part.AsUnion().(type) {
		case opencode.TextPart:
			textParts = append(textParts, p.Text)
		}
	}

	if len(textParts) == 0 {
		return "Response received (message ID: " + msg.Info.ID + ")"
	}

	return strings.Join(textParts, "\n")
}
