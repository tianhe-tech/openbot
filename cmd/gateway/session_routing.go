package main

import (
	"fmt"
	"log"
	"strings"

	"github.com/user/opencode-gateway/internal/adapters/base"
	"github.com/user/opencode-gateway/internal/opencode"
)

// registrySessionRouting is the adapter half of a transactional /switch.
//
// It lives in the host rather than in internal/opencode because the opencode
// package must not import the adapters (the adapters already import it), and it
// lives here rather than in internal/adapters/base because base is a set of
// platform primitives that has no business knowing about session switching.
//
// The three methods are deliberately separate. ClaimSession cannot be used as a
// side-effect-free prepare step — it rewrites the session title and runs the
// routing hook — so the shared switch logic snapshots, commits and compensates
// explicitly instead of relying on one "do the switch" call.
type registrySessionRouting struct {
	registry *base.AdapterRegistry
}

// SnapshotSessionRouting records the routing a switch is about to overwrite.
//
// It captures three things that SwitchUserSession destroys: the user's previous
// session, that session's reverse mapping and delivery metadata, and the
// target's own delivery metadata. Without the last one, switching to a session
// that already had a webhook and then failing would leave the session with no
// way to deliver anything.
func (r registrySessionRouting) SnapshotSessionRouting(channel, userID, sessionID string) opencode.SessionRoutingSnapshot {
	snapshot := opencode.SessionRoutingSnapshot{
		Channel:         channel,
		UserID:          userID,
		TargetSessionID: sessionID,
	}
	adapter, ok := r.registry.Get(channel)
	if !ok {
		return snapshot
	}
	if previous, ok := adapter.GetSessionForUser(userID); ok && previous != "" {
		snapshot.PreviousSessionID = previous
		snapshot.PreviousSessionData = adapter.SessionData(previous)
	}
	snapshot.TargetSessionData = adapter.SessionData(sessionID)
	if targetUser, ok := adapter.GetUserForSession(sessionID); ok {
		snapshot.TargetUser = targetUser
	}
	return snapshot
}

// ApplySessionRouting points the user at the target session and records the
// platform delivery context of the message that asked for the switch.
//
// SwitchUserSession is used rather than MapUserToSession so the previous
// session stops routing to this user and its webhook/receive_id does not leak
// into the target. That is also why the previous session's metadata has to be
// in the snapshot: this call deletes it.
func (r registrySessionRouting) ApplySessionRouting(channel, userID, sessionID string, channelData map[string]string) error {
	adapter, ok := r.registry.Get(channel)
	if !ok {
		return fmt.Errorf("no adapter registered for channel %q", channel)
	}
	adapter.SwitchUserSession(userID, sessionID)
	for key, value := range channelData {
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		adapter.MapSessionData(sessionID, key, value)
	}
	return nil
}

// RestoreSessionRouting puts back a snapshot taken before ApplySessionRouting.
//
// It restores the user mapping, the previous session's reverse mapping and
// metadata, and the target's own metadata. It deliberately does not call
// SwitchUserSession: that would delete the target's data on the way back and
// leave the compensation as destructive as the failure it is undoing.
func (r registrySessionRouting) RestoreSessionRouting(snapshot opencode.SessionRoutingSnapshot) error {
	adapter, ok := r.registry.Get(snapshot.Channel)
	if !ok {
		return fmt.Errorf("no adapter registered for channel %q", snapshot.Channel)
	}
	adapter.RestoreUserSession(snapshot.UserID, snapshot.PreviousSessionID)
	if snapshot.PreviousSessionID != "" {
		adapter.RestoreSessionUser(snapshot.PreviousSessionID, snapshot.UserID)
		adapter.RestoreSessionData(snapshot.PreviousSessionID, snapshot.PreviousSessionData)
	}
	if snapshot.TargetSessionID != "" && snapshot.TargetSessionID != snapshot.PreviousSessionID {
		adapter.RestoreSessionUser(snapshot.TargetSessionID, snapshot.TargetUser)
		adapter.RestoreSessionData(snapshot.TargetSessionID, snapshot.TargetSessionData)
	}
	log.Printf("main: restored session routing for %s/%s (previous=%q target=%q)",
		snapshot.Channel, snapshot.UserID, snapshot.PreviousSessionID, snapshot.TargetSessionID)
	return nil
}

// ValidateSessionDelivery reports whether a restore-claim could actually deliver
// the session's output to this user.
//
// It is a pre-check, not a guarantee: only the platform's send path knows
// whether a specific chat is reachable. What it does catch is the cold-start
// case that would otherwise fail silently — a claim restored with no delivery
// context for a user the adapter has never seen. DingTalk returns nil when it
// has no target at all, so without this check the user would be told their TUI
// replies are mirrored while every send was dropped.
func (r registrySessionRouting) ValidateSessionDelivery(channel, userID, sessionID string, channelData map[string]string) error {
	adapter, ok := r.registry.Get(channel)
	if !ok {
		return fmt.Errorf("no adapter registered for channel %q", channel)
	}
	if strings.TrimSpace(userID) == "" {
		return fmt.Errorf("cannot mirror session %s: no user to deliver to", sessionID)
	}
	for _, value := range channelData {
		if strings.TrimSpace(value) != "" {
			return nil
		}
	}
	if len(adapter.SessionData(sessionID)) > 0 {
		return nil
	}
	if _, known := adapter.GetSessionForUser(userID); known {
		return nil
	}
	return fmt.Errorf("cannot mirror session %s: %s has no delivery target for user %s", sessionID, channel, userID)
}
