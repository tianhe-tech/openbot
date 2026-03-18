package opencode

// SSEDispatcher is the central hub that routes OpenCode server-sent events
// to per-session handlers and global adapter handlers.
//
// # Ordering guarantee
//
// Events for the same session are delivered strictly in arrival order.
// This is achieved by calling Dispatch synchronously from the single SSE
// reader goroutine inside eventListenerLoop, so no event for session X
// can overtake an earlier event for session X.
//
// # Extension path
//
// The public API of SSEDispatcher is designed so that the underlying
// delivery mechanism can later be upgraded to a per-session buffered-channel
// + goroutine design (for parallel session throughput) without changing any
// call sites.  All that changes is the Dispatch implementation.
//
// # Handler call order
//
// For every event that carries a sessionID:
//  1. The session-specific streaming handler is called first.
//  2. All global handlers are called in registration order.
//
// For events without a sessionID (heartbeats, server.connected, etc.) only
// global handlers are called.
//
// This ordering ensures that session.idle is processed by the
// StreamingSessionHandler (which flushes content and closes doneCh) before
// the global main-event-handler can clear the session mapping, eliminating
// the "task ended but nothing returned" race condition.

import (
	"context"
	"log"
	"sync"

	opencodesdk "github.com/sst/opencode-sdk-go"
)

// SSEDispatcher routes SSE events from OpenCode to the correct handlers.
type SSEDispatcher struct {
	sessionHandlers sync.Map // sessionID → EventHandler
	globalMu        sync.RWMutex
	globalHandlers  []EventHandler
}

// NewSSEDispatcher creates a ready-to-use dispatcher.
func NewSSEDispatcher() *SSEDispatcher {
	return &SSEDispatcher{}
}

// AddGlobalHandler registers a handler that receives every dispatched event
// regardless of sessionID.  Handlers are called in registration order AFTER
// the session-specific handler.  Must not block; slow work should be
// dispatched to a separate goroutine inside the handler.
func (d *SSEDispatcher) AddGlobalHandler(h EventHandler) {
	d.globalMu.Lock()
	d.globalHandlers = append(d.globalHandlers, h)
	d.globalMu.Unlock()
}

// SetSessionHandler installs (or replaces) the handler for sessionID.
// Called when a new StreamingSessionHandler is registered for a session.
func (d *SSEDispatcher) SetSessionHandler(sessionID string, h EventHandler) {
	d.sessionHandlers.Store(sessionID, h)
}

// RemoveSessionHandler deregisters the session-specific handler for sessionID.
// Called when a streaming request completes and its handler is torn down.
func (d *SSEDispatcher) RemoveSessionHandler(sessionID string) {
	d.sessionHandlers.Delete(sessionID)
}

// Dispatch routes a single SSE event synchronously:
//
//  1. If sessionID is non-empty and a per-session handler is registered,
//     that handler is called first.
//  2. All global handlers are then called in registration order.
//
// Dispatch is intended to be called from a single goroutine (the SSE
// reader loop) so callers do not need additional synchronisation.
func (d *SSEDispatcher) Dispatch(ctx context.Context, sessionID string, event *opencodesdk.EventListResponse) {
	if sessionID != "" {
		if raw, ok := d.sessionHandlers.Load(sessionID); ok {
			if err := raw.(EventHandler)(ctx, event); err != nil {
				log.Printf("sseDispatcher: session %s handler error (type=%s): %v",
					sessionID[:min(8, len(sessionID))], event.Type, err)
			}
		} else {
			// Only log for meaningful event types to reduce noise.
			t := string(event.Type)
			if t != "server.heartbeat" && t != "server.connected" &&
				t != "session.created" && t != "session.updated" {
				log.Printf("sseDispatcher: no handler for session %s (type=%s); registered sessions:",
					sessionID[:min(8, len(sessionID))], t)
				d.sessionHandlers.Range(func(k, _ any) bool {
					log.Printf("sseDispatcher:   %v", k)
					return true
				})
			}
		}
	}

	d.globalMu.RLock()
	globals := d.globalHandlers
	d.globalMu.RUnlock()

	for i, h := range globals {
		if err := h(ctx, event); err != nil {
			log.Printf("sseDispatcher: global handler[%d] error (type=%s): %v",
				i, event.Type, err)
		}
	}
}
