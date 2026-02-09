package base

import (
	"context"
	"fmt"
	"log"
	"sync"
)

// MessageSender defines the interface for sending messages back to users.
type MessageSender interface {
	SendMessage(ctx context.Context, channel, userID, content string) error
}

// BidirectionalAdapter enables two-way communication between OpenCode and messaging platforms.
type BidirectionalAdapter struct {
	name         string
	sender       MessageSender
	userSessions sync.Map // map[userID]sessionID
	sessionUsers sync.Map // map[sessionID]userID
	sessionData  sync.Map // map[sessionID]map[string]string - 存储session的额外数据（如webhook URL）
}

// NewBidirectionalAdapter creates a new bidirectional adapter.
func NewBidirectionalAdapter(name string, sender MessageSender) *BidirectionalAdapter {
	return &BidirectionalAdapter{
		name:   name,
		sender: sender,
	}
}

// Name returns the adapter name.
func (a *BidirectionalAdapter) Name() string {
	return a.name
}

// MapUserToSession associates a user with a session.
func (a *BidirectionalAdapter) MapUserToSession(userID, sessionID string) {
	a.userSessions.Store(userID, sessionID)
	a.sessionUsers.Store(sessionID, userID)
}

// MapSessionData stores additional data for a session (like webhook URL).
func (a *BidirectionalAdapter) MapSessionData(sessionID, key, value string) {
	if val, ok := a.sessionData.Load(sessionID); ok {
		data := val.(map[string]string)
		data[key] = value
		a.sessionData.Store(sessionID, data)
	} else {
		data := map[string]string{key: value}
		a.sessionData.Store(sessionID, data)
	}
}

// GetSessionData retrieves additional data for a session.
func (a *BidirectionalAdapter) GetSessionData(sessionID, key string) (string, bool) {
	if val, ok := a.sessionData.Load(sessionID); ok {
		data := val.(map[string]string)
		if v, exists := data[key]; exists {
			return v, true
		}
	}
	return "", false
}

// GetSessionForUser retrieves the session ID for a user.
func (a *BidirectionalAdapter) GetSessionForUser(userID string) (string, bool) {
	if sessionID, ok := a.userSessions.Load(userID); ok {
		return sessionID.(string), true
	}
	return "", false
}

// GetUserForSession retrieves the user ID for a session.
func (a *BidirectionalAdapter) GetUserForSession(sessionID string) (string, bool) {
	if userID, ok := a.sessionUsers.Load(sessionID); ok {
		return userID.(string), true
	}
	return "", false
}

// SendToUser sends a message to a user via the platform's messaging API.
// channel can be empty for direct messages, or specify a group/channel ID
func (a *BidirectionalAdapter) SendToUser(ctx context.Context, userID, content string) error {
	return a.SendToUserInChannel(ctx, "", userID, content)
}

// SendToUserInChannel sends a message to a user in a specific channel
func (a *BidirectionalAdapter) SendToUserInChannel(ctx context.Context, channel, userID, content string) error {
	if a.sender == nil {
		return fmt.Errorf("adapter %s: no sender configured", a.name)
	}

	log.Printf("adapter %s: sending message to channel %s, user %s", a.name, channel, userID)
	return a.sender.SendMessage(ctx, channel, userID, content)
}

// HandleIncomingEvent processes events from OpenCode and routes them to users.
func (a *BidirectionalAdapter) HandleIncomingEvent(ctx context.Context, sessionID, content string) error {
	userID, ok := a.GetUserForSession(sessionID)
	if !ok {
		return fmt.Errorf("adapter %s: no user found for session %s", a.name, sessionID)
	}

	// Get channel/webhook from session data if available
	channel, _ := a.GetSessionData(sessionID, "channel")

	return a.SendToUserInChannel(ctx, channel, userID, content)
}

// AdapterRegistry manages multiple bidirectional adapters.
type AdapterRegistry struct {
	adapters sync.Map // map[name]*BidirectionalAdapter
}

// NewAdapterRegistry creates a new adapter registry.
func NewAdapterRegistry() *AdapterRegistry {
	return &AdapterRegistry{}
}

// Register adds an adapter to the registry.
func (r *AdapterRegistry) Register(adapter *BidirectionalAdapter) {
	r.adapters.Store(adapter.Name(), adapter)
}

// Get retrieves an adapter by name.
func (r *AdapterRegistry) Get(name string) (*BidirectionalAdapter, bool) {
	if adapter, ok := r.adapters.Load(name); ok {
		return adapter.(*BidirectionalAdapter), true
	}
	return nil, false
}

// RouteEventToAdapter routes an OpenCode event to the appropriate adapter.
func (r *AdapterRegistry) RouteEventToAdapter(ctx context.Context, channel, sessionID, content string) error {
	adapter, ok := r.Get(channel)
	if !ok {
		return fmt.Errorf("no adapter registered for channel: %s", channel)
	}

	return adapter.HandleIncomingEvent(ctx, sessionID, content)
}
