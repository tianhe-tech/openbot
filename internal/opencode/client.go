package opencode

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

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
	Agent     string            `json:"agent,omitempty"`     // 可选：指定使用的agent/skill名称
	Streaming bool              `json:"streaming,omitempty"` // 是否使用流式返回
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// StreamCallback defines a callback for streaming responses.
type StreamCallback func(chunk string) error

// EventHandler defines a callback for incoming OpenCode events.
type EventHandler func(ctx context.Context, event *opencode.EventListResponse) error

// Client knows how to talk to the remote OpenCode service using the official SDK.
type Client struct {
	sdk             *opencode.Client
	eventHandlers   []EventHandler
	eventListenerMu sync.RWMutex
	sessions        sync.Map // map[threadID]sessionID
	sessionLocks    sync.Map // map[threadID]*sync.Mutex for preventing concurrent session operations
	directory       string
	timeout         time.Duration // 默认超时时间
}

// Option mutates a client during construction.
type Option func(*Client)

// WithDirectory sets the working directory for sessions.
func WithDirectory(dir string) Option {
	return func(c *Client) {
		c.directory = dir
	}
}

// WithTimeout sets the default timeout for OpenCode operations.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) {
		c.timeout = timeout
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
		timeout:       180 * time.Second, // 默认60秒超时
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

	// Apply timeout to context
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// Get or create session with lock to prevent concurrent session creation
	threadLock := c.getThreadLock(payload.ThreadID)
	threadLock.Lock()
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
			threadLock.Unlock()
			return Response{}, fmt.Errorf("opencode: create session: %w", err)
		}
		sessionID = session.ID
		if payload.ThreadID != "" {
			c.sessions.Store(payload.ThreadID, sessionID)
		}
		log.Printf("opencode: created new session %s for thread %s", sessionID, payload.ThreadID)
	}
	threadLock.Unlock()

	// Build message parts
	parts := []opencode.SessionPromptParamsPartUnion{}

	// Add agent part if specified
	if payload.Agent != "" {
		parts = append(parts, opencode.AgentPartInputParam{
			Name: opencode.F(payload.Agent),
			Type: opencode.F(opencode.AgentPartInputTypeAgent),
		})
	}

	// Add text content
	parts = append(parts, opencode.TextPartInputParam{
		Text: opencode.F(payload.Content),
		Type: opencode.F(opencode.TextPartInputTypeText),
	})

	// Send prompt message to session
	result, err := c.sdk.Session.Prompt(ctx, sessionID, opencode.SessionPromptParams{
		Parts: opencode.F(parts),
	})
	if err != nil {
		return Response{}, fmt.Errorf("opencode: send prompt: %w", err)
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
	return c.sdk.Session.Get(ctx, sessionID, opencode.SessionGetParams{})
}

// ListAgents retrieves all available agents/skills.
func (c *Client) ListAgents(ctx context.Context) ([]opencode.Agent, error) {
	result, err := c.sdk.Agent.List(ctx, opencode.AgentListParams{
		Directory: opencode.F(c.directory),
	})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return []opencode.Agent{}, nil
	}
	return *result, nil
}

// ExecuteCommand executes a command in the session context.
// This can be used to directly invoke skill scripts like websearch.py
func (c *Client) ExecuteCommand(ctx context.Context, sessionID, command string) (*opencode.SessionCommandResponse, error) {
	return c.sdk.Session.Command(ctx, sessionID, opencode.SessionCommandParams{
		Command:   opencode.F(command),
		Directory: opencode.F(c.directory),
	})
}

// ExecuteShell executes a shell command in the session context.
func (c *Client) ExecuteShell(ctx context.Context, sessionID, command string) (*opencode.AssistantMessage, error) {
	return c.sdk.Session.Shell(ctx, sessionID, opencode.SessionShellParams{
		Command:   opencode.F(command),
		Directory: opencode.F(c.directory),
	})
}

// ListSessions retrieves all sessions.
func (c *Client) ListSessions(ctx context.Context) ([]opencode.Session, error) {
	result, err := c.sdk.Session.List(ctx, opencode.SessionListParams{})
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
	stream := c.sdk.Event.ListStreaming(ctx, opencode.EventListParams{})

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

// extractReplyFromMessage extracts text content from a prompt response.
func extractReplyFromMessage(msg *opencode.SessionPromptResponse) string {
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

// getThreadLock gets or creates a lock for a specific thread to prevent concurrent operations.
func (c *Client) getThreadLock(threadID string) *sync.Mutex {
	if threadID == "" {
		return &sync.Mutex{} // Return a new mutex for single-use
	}

	lock, _ := c.sessionLocks.LoadOrStore(threadID, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

// SendMessageStreaming sends a message and calls the callback for each chunk of the response.
// Note: OpenCode SDK doesn't support true streaming yet, so we send the full response.
func (c *Client) SendMessageStreaming(ctx context.Context, payload MessagePayload, callback StreamCallback) (Response, error) {
	// For now, just call SendMessage and invoke callback with full response
	// In future, can implement chunking or use SSE if SDK supports it
	response, err := c.SendMessage(ctx, payload)
	if err != nil {
		return response, err
	}

	// Call callback with full reply if provided
	if callback != nil && response.Reply != "" {
		if err := callback(response.Reply); err != nil {
			log.Printf("opencode: stream callback error: %v", err)
		}
	}

	return response, nil
}
