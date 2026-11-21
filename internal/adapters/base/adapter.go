package base

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
)

// MessageSender defines the interface for sending messages back to users.
type MessageSender interface {
	SendMessage(ctx context.Context, channel, userID, content string) error
}

// MaxSessionListRows bounds how many sessions a /sessions reply renders.
// The list is ordered newest first, so the rows that get cut are the ones a
// user is least likely to want, and the reply stays inside the message-size
// limits of every platform.
const MaxSessionListRows = 10

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

// SessionData returns a copy of a session's platform delivery metadata.
//
// The copy matters: the stored map is mutated in place by MapSessionData, so
// handing out the live map would let a later write silently change a snapshot
// that a failed /switch is about to restore.
func (a *BidirectionalAdapter) SessionData(sessionID string) map[string]string {
	val, ok := a.sessionData.Load(sessionID)
	if !ok {
		return nil
	}
	data, ok := val.(map[string]string)
	if !ok || len(data) == 0 {
		return nil
	}
	out := make(map[string]string, len(data))
	for k, v := range data {
		out[k] = v
	}
	return out
}

// RestoreSessionData replaces a session's delivery metadata with a copy of data.
// A nil or empty map removes the entry entirely, which is how a snapshot taken
// before the session had any metadata is put back.
func (a *BidirectionalAdapter) RestoreSessionData(sessionID string, data map[string]string) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}
	if len(data) == 0 {
		a.sessionData.Delete(sessionID)
		return
	}
	out := make(map[string]string, len(data))
	for k, v := range data {
		out[k] = v
	}
	a.sessionData.Store(sessionID, out)
}

// RestoreUserSession points a user back at a session without touching any
// per-session data. An empty sessionID clears the user's mapping.
//
// This is the compensation half of SwitchUserSession, which deletes the
// previous session's reverse mapping and metadata. Restoring the user mapping
// alone is not enough — the caller must also restore the reverse mapping and
// the metadata — but it must not go through SwitchUserSession to do it, because
// that would delete the target's data on the way back.
func (a *BidirectionalAdapter) RestoreUserSession(userID, sessionID string) {
	userID = strings.TrimSpace(userID)
	if userID == "" {
		return
	}
	if strings.TrimSpace(sessionID) == "" {
		a.userSessions.Delete(userID)
		return
	}
	a.userSessions.Store(userID, sessionID)
}

// RestoreSessionUser restores the reverse mapping sessionID → userID. An empty
// userID removes it.
func (a *BidirectionalAdapter) RestoreSessionUser(sessionID, userID string) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}
	if strings.TrimSpace(userID) == "" {
		a.sessionUsers.Delete(sessionID)
		return
	}
	a.sessionUsers.Store(sessionID, userID)
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

// SoleUser returns the only user this adapter has ever seen, and reports
// ok=false when there is none or more than one.
//
// It backs the channel-only claim form ("[feishu] my work"), which exists so a
// single-user deployment does not have to type a platform user ID into
// /rename. Returning ok=false for an ambiguous adapter is deliberate: guessing
// would deliver one user's session output to another.
//
// Cron pseudo-users ("cron:<sessionID>") are ignored — they are internal
// bookkeeping, not a person, and counting them would make every adapter that
// has ever run a scheduled task look ambiguous.
func (a *BidirectionalAdapter) SoleUser() (string, bool) {
	var (
		sole  string
		count int
	)
	a.userSessions.Range(func(key, _ any) bool {
		userID, ok := key.(string)
		if !ok || userID == "" || strings.HasPrefix(userID, "cron:") {
			return true
		}
		count++
		if count > 1 {
			return false
		}
		sole = userID
		return true
	})
	if count != 1 {
		return "", false
	}
	return sole, true
}

// ClearSessionForUser removes the session mapping for a user.
func (a *BidirectionalAdapter) ClearSessionForUser(userID string) {
	if sessionID, ok := a.userSessions.Load(userID); ok {
		a.userSessions.Delete(userID)
		a.sessionUsers.Delete(sessionID)
		a.sessionData.Delete(sessionID)
	}
}

// SwitchUserSession points a user at an existing session, replacing whatever
// session they were on.
//
// This is the adapter half of /switch. It differs from MapUserToSession in one
// way that matters: the previous session's reverse mapping and per-session data
// are dropped first. Leaving them behind would keep routing that session's
// events to this user after they moved away from it, and would leak the old
// session's webhook/receive_id into the new one.
//
// The caller must also bind the thread on the opencode client
// (Client.BindSessionToThread). The two maps are independent: this one is what
// the adapters read to fill MessagePayload.SessionID, and the client's is the
// fallback used when that field is empty. Updating only one leaves the other
// pointing at the previous session.
func (a *BidirectionalAdapter) SwitchUserSession(userID, sessionID string) {
	userID = strings.TrimSpace(userID)
	sessionID = strings.TrimSpace(sessionID)
	if userID == "" || sessionID == "" {
		return
	}
	if previous, ok := a.userSessions.Load(userID); ok {
		if previousID, ok := previous.(string); ok && previousID != sessionID {
			a.sessionUsers.Delete(previousID)
			a.sessionData.Delete(previousID)
		}
	}
	a.MapUserToSession(userID, sessionID)
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
