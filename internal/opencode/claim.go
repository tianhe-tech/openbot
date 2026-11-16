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

// ClaimedSessionSender delivers a message produced by a claimed session to the
// channel user who claimed it.
//
// It is a function rather than an interface because the opencode package must
// not import the adapters package (the adapters already import opencode, so an
// interface here would create an import cycle). The gateway wires this up at
// startup with a closure over its adapter registry.
type ClaimedSessionSender func(ctx context.Context, sessionID, channel, userID, content string) error

// ClaimedSessionHook is notified after a session is successfully claimed, so
// the host can update its own routing tables (adapter user→session mapping and
// per-session channel data). It runs before the forwarding handler is attached,
// so the very first event already has somewhere to go.
type ClaimedSessionHook func(ctx context.Context, sessionID, channel, userID, channelData string) error

// ClaimUserResolver resolves the delivery target for a claim whose title names
// only a channel ("[feishu] my work").
//
// It exists so a single-user deployment does not have to type a platform user
// ID into /rename. It must return ok=false when the channel has no unambiguous
// single user: guessing would deliver one person's session output to another,
// so an ambiguous deployment falls back to requiring an explicit ID.
//
// Like ClaimedSessionSender it is a function rather than an interface, because
// the opencode package must not import the adapters package.
type ClaimUserResolver func(ctx context.Context, channel string) (userID string, ok bool)

// ClaimChannelChecker reports whether a channel name is routable, i.e. whether
// the host has an adapter registered for it.
//
// It exists so /transfer can reject a typo ("/transfer feshu") BEFORE the
// session's title is rewritten. Without it the claim would be recorded against
// a channel that has no adapter, the delivery hook would fail, and the session
// would be left claimed by nobody — invisible in every /sessions list and
// unreachable from the chat it was moved away from.
type ClaimChannelChecker func(channel string) bool

// claimedSession is the bookkeeping for one session adopted from the TUI.
type claimedSession struct {
	Channel string
	UserID  string
	Handler *StreamingSessionHandler
}

// SetClaimedSessionSender installs the delivery function used by claimed
// sessions. Without it, claimed sessions still get their title rewritten (so
// they show up in /sessions) but their output is only logged.
func (c *Client) SetClaimedSessionSender(sender ClaimedSessionSender) {
	c.claimedSenderMu.Lock()
	defer c.claimedSenderMu.Unlock()
	c.claimedSender = sender
}

// SetClaimedSessionHook installs the post-claim notification hook.
func (c *Client) SetClaimedSessionHook(hook ClaimedSessionHook) {
	c.claimedHookMu.Lock()
	defer c.claimedHookMu.Unlock()
	c.claimedHook = hook
}

// SetClaimUserResolver installs the resolver used by channel-only claims.
// Without it, a title like "[feishu] my work" is ignored and the user must
// write the full "[feishu:ou_xxx]" prefix.
func (c *Client) SetClaimUserResolver(resolver ClaimUserResolver) {
	c.claimedUserResolverMu.Lock()
	defer c.claimedUserResolverMu.Unlock()
	c.claimedUserResolver = resolver
}

func (c *Client) claimUserResolver() ClaimUserResolver {
	c.claimedUserResolverMu.RLock()
	defer c.claimedUserResolverMu.RUnlock()
	return c.claimedUserResolver
}

// SetClaimChannelChecker installs the validator used by /transfer to reject an
// unknown target channel. Without it, any channel name is accepted.
func (c *Client) SetClaimChannelChecker(checker ClaimChannelChecker) {
	c.claimedChannelCheckerMu.Lock()
	defer c.claimedChannelCheckerMu.Unlock()
	c.claimedChannelChecker = checker
}

func (c *Client) claimChannelChecker() ClaimChannelChecker {
	c.claimedChannelCheckerMu.RLock()
	defer c.claimedChannelCheckerMu.RUnlock()
	return c.claimedChannelChecker
}

func (c *Client) claimedSessionSender() ClaimedSessionSender {
	c.claimedSenderMu.RLock()
	defer c.claimedSenderMu.RUnlock()
	return c.claimedSender
}

func (c *Client) claimedSessionHook() ClaimedSessionHook {
	c.claimedHookMu.RLock()
	defer c.claimedHookMu.RUnlock()
	return c.claimedHook
}

// ClaimSession adopts an existing session for a channel user.
//
// Sessions created in the opencode TUI carry no ownership prefix, so
// ListSessionsForUser cannot see them and FindLatestSessionForUser cannot
// recover them after a restart. Claiming fixes both by rewriting the title to
// "[channel:userID] <original>" — the exact format SendMessage mints — and by
// registering a streaming handler so the session's SSE events are forwarded to
// the adapter instead of being dropped with "no session handler found".
//
// The title rewrite is idempotent: a session that already carries the prefix is
// left untouched, so claiming twice does not stack prefixes.
//
// channelData is optional platform routing data (a DingTalk session webhook, a
// Feishu receive_id_type, a WeChat chat id). It is handed to the hook so the
// adapter can deliver replies; without it, platforms that require a webhook
// (DingTalk) cannot answer.
//
// Returns the session title after the claim.
func (c *Client) ClaimSession(ctx context.Context, sessionID, channel, userID, channelData string) (string, error) {
	sessionID = strings.TrimSpace(sessionID)
	if _, busy := c.claimTransitions.LoadOrStore(sessionID, struct{}{}); busy {
		return "", fmt.Errorf("opencode: session ownership change in progress")
	}
	defer c.claimTransitions.Delete(sessionID)
	return c.claimSession(ctx, sessionID, channel, userID, channelData, false)
}

func (c *Client) claimSession(ctx context.Context, sessionID, channel, userID, channelData string, requireTitle bool) (string, error) {
	sessionID = strings.TrimSpace(sessionID)
	channel = strings.TrimSpace(channel)
	userID = strings.TrimSpace(userID)
	if sessionID == "" || channel == "" || userID == "" {
		return "", fmt.Errorf("opencode: claim session: sessionID, channel and userID are required")
	}

	session, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return "", fmt.Errorf("opencode: claim session: %w", err)
	}
	if session == nil {
		return "", fmt.Errorf("opencode: claim session: session %s not found", sessionID)
	}
	if c.isCronSession(sessionID, session.Title) {
		return "", fmt.Errorf("opencode: claim session: cron session %s cannot be claimed", sessionID)
	}
	if requireTitle {
		currentChannel, currentUser, valid := parseSessionTitlePrefix(session.Title)
		if !valid || currentChannel != channel || (currentUser != "" && currentUser != userID) {
			return "", fmt.Errorf("opencode: stale session ownership event")
		}
	}

	prefix := sessionTitlePrefix(channel, userID)
	title := strings.TrimSpace(session.Title)
	newTitle := title
	if !strings.HasPrefix(title, prefix) {
		if _, _, ok := parseSessionTitlePrefix(title); ok {
			title = strings.TrimSpace(title[strings.Index(title, "]")+1:])
		}
		newTitle = prefix
		if title != "" {
			newTitle = prefix + " " + title
		}
		updated, err := c.sdk.Session.Update(ctx, sessionID, opencode.SessionUpdateParams{
			Title: opencode.F(newTitle),
		})
		if err != nil {
			return "", fmt.Errorf("opencode: claim session: rewrite title: %w", err)
		}
		if updated != nil && strings.TrimSpace(updated.Title) != "" {
			newTitle = updated.Title
		}
		log.Printf("opencode: claimed session %s for %s/%s (title=%q)",
			sessionID[:min(8, len(sessionID))], channel, userID, newTitle)
	} else {
		log.Printf("opencode: session %s already claimed by %s/%s, reusing",
			sessionID[:min(8, len(sessionID))], channel, userID)
	}

	// Update the host's routing tables before attaching the handler, so the
	// first forwarded event already resolves to a user.
	if hook := c.claimedSessionHook(); hook != nil {
		if err := hook(ctx, sessionID, channel, userID, channelData); err != nil {
			_, rollbackErr := c.sdk.Session.Update(ctx, sessionID, opencode.SessionUpdateParams{Title: opencode.F(session.Title)})
			return "", fmt.Errorf("opencode: claim hook: %w (title rollback: %v)", err, rollbackErr)
		}
	}

	c.attachClaimedSessionHandler(sessionID, channel, userID)
	return newTitle, nil
}

// parseSessionTitlePrefix splits a "[channel:userID] rest" title into its
// ownership parts. It reports ok=false when the title carries no prefix, which
// is the normal state of a session created in the opencode TUI.
//
// A title that names only a channel — "[feishu] rest" or "[feishu:] rest" —
// parses with an empty userID. That form is resolved later by the host's
// ClaimUserResolver, which is what lets a single-user deployment skip typing a
// platform user ID into /rename. An empty channel is never valid.
func parseSessionTitlePrefix(title string) (channel, userID string, ok bool) {
	title = strings.TrimSpace(title)
	if !strings.HasPrefix(title, "[") {
		return "", "", false
	}
	closeIdx := strings.Index(title, "]")
	if closeIdx <= 1 {
		return "", "", false
	}
	channel, userID, _ = strings.Cut(title[1:closeIdx], ":")
	channel = strings.TrimSpace(channel)
	userID = strings.TrimSpace(userID)
	if channel == "" {
		return "", "", false
	}
	return channel, userID, true
}

// isGatewayOwnedSession reports whether sessionID was created by this client
// (SendMessage / SendMessageStreamingWithEvents) rather than adopted from the
// TUI.
//
// The distinction matters for auto-claiming: a gateway-created session already
// carries a "[channel:userID]" title, so a title-based claim would adopt it a
// second time and replace its per-turn streaming handler with a long-lived
// mirror. The thread→session table is the authoritative record of what this
// process created.
func (c *Client) isGatewayOwnedSession(sessionID string) bool {
	found := false
	c.sessions.Range(func(_, value any) bool {
		if id, ok := value.(string); ok && id == sessionID {
			found = true
			return false
		}
		return true
	})
	return found
}

// claimFromTitleParts adopts a session given its already-parsed ownership
// prefix. It is the shared tail of AutoClaimFromTitle and
// ClaimSessionFromTitle.
func (c *Client) claimFromTitleParts(ctx context.Context, sessionID, channel, userID string) (bool, error) {
	if sessionID == "" || channel == "" {
		return false, nil
	}
	// skillgen drafts are internal working sessions; they are not routable to a
	// chat adapter and must never be claimed.
	if channel == "skillgen" {
		return false, nil
	}
	// A channel-only title ("[feishu] my work") leaves the target user to the
	// host. The resolver refuses when the channel has no unambiguous single
	// user, so an ambiguous deployment falls back to requiring an explicit ID
	// rather than guessing and delivering to the wrong person.
	if userID == "" {
		resolver := c.claimUserResolver()
		if resolver == nil {
			return false, nil
		}
		resolved, ok := resolver(ctx, channel)
		if !ok || strings.TrimSpace(resolved) == "" {
			return false, nil
		}
		userID = strings.TrimSpace(resolved)
		log.Printf("opencode: resolved channel-only claim for %s to user %s", channel, userID)
	}
	if _, claimed := c.claimedSessions.Load(sessionID); claimed {
		return false, nil
	}
	if c.isGatewayOwnedSession(sessionID) {
		return false, nil
	}
	if _, busy := c.claimTransitions.LoadOrStore(sessionID, struct{}{}); busy {
		return false, nil
	}
	defer c.claimTransitions.Delete(sessionID)
	if _, err := c.claimSession(ctx, sessionID, channel, userID, "", true); err != nil {
		return false, err
	}
	return true, nil
}

// syncClaimProbeWindow collapses bursts of session.updated.1 sync envelopes for
// the same session into a single GetSession probe.
//
// The sync envelope carries no title, so the only way to learn whether a rename
// happened is to fetch the session. session.updated.1 fires on every session
// record change (including time.updated bumps during a turn), so a busy session
// can emit dozens per minute. A short window keeps the probe rate bounded
// without risking a missed rename: an idle session — the normal state when the
// user runs /rename — emits exactly one envelope, so there is nothing to
// collapse, and a busy session emits another one within the window anyway.
const syncClaimProbeWindow = 2 * time.Second

// AutoClaimFromTitle adopts a session whose title carries a "[channel:userID]"
// prefix.
//
// This is the entry point for the TUI→chat handoff. The user runs /rename in
// the opencode TUI (a built-in slash command, so no TUI code change is needed)
// and types "[feishu:ou_xxx] my work". The server then emits an event naming
// the session, and this method turns that rename into a claim: the session
// starts appearing in that user's /sessions and its output is mirrored into the
// chat.
//
// Two event shapes are accepted, because opencode publishes session updates on
// two streams:
//
//   - "session.updated" on the directory-scoped /event stream. It carries the
//     full session object, so the title is read straight out of the event and
//     the common path costs no HTTP round-trip beyond the GetSession inside
//     ClaimSession.
//   - "sync" on the process-wide /global/event stream. It names the session in
//     syncEvent.aggregateID but carries no title, so the title has to be
//     fetched. This is the safety net for a /event stream that is scoped to a
//     different directory or has silently stalled.
//
// Returns true when a claim was actually performed.
func (c *Client) AutoClaimFromTitle(ctx context.Context, event *opencode.EventListResponse) (bool, error) {
	if event == nil {
		return false, nil
	}
	switch string(event.Type) {
	case "session.updated":
		return c.autoClaimFromSessionUpdated(ctx, event)
	case "sync":
		return c.autoClaimFromSyncEnvelope(ctx, event)
	default:
		return false, nil
	}
}

// autoClaimFromSessionUpdated handles the directory-scoped /event shape, which
// carries the full session object including the title.
func (c *Client) autoClaimFromSessionUpdated(ctx context.Context, event *opencode.EventListResponse) (bool, error) {
	raw := event.JSON.RawJSON()
	if raw == "" {
		return false, nil
	}

	var probe struct {
		Properties struct {
			SessionID string `json:"sessionID"`
			Info      struct {
				ID    string `json:"id"`
				Title string `json:"title"`
			} `json:"info"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(raw), &probe); err != nil {
		return false, nil
	}

	sessionID := strings.TrimSpace(probe.Properties.Info.ID)
	if sessionID == "" {
		sessionID = strings.TrimSpace(probe.Properties.SessionID)
	}
	if c.isCronSession(sessionID, probe.Properties.Info.Title) {
		return false, nil
	}
	channel, userID, ok := parseSessionTitlePrefix(probe.Properties.Info.Title)
	if !ok {
		return false, nil
	}
	return c.claimFromTitleParts(ctx, sessionID, channel, userID)
}

// autoClaimFromSyncEnvelope handles the process-wide /global/event shape.
//
// The envelope looks like:
//
//	{"type":"sync","syncEvent":{"id":"evt_...","type":"session.updated.1",
//	 "seq":390,"aggregateID":"ses_...","data":{"sessionID":"ses_..."}}}
//
// There is no title and no properties object, so the session has to be fetched.
// Cheap rejections run first so an already-adopted or gateway-created session
// never costs a round-trip.
func (c *Client) autoClaimFromSyncEnvelope(ctx context.Context, event *opencode.EventListResponse) (bool, error) {
	raw := event.JSON.RawJSON()
	if raw == "" {
		return false, nil
	}

	var envelope struct {
		SyncEvent struct {
			Type        string `json:"type"`
			AggregateID string `json:"aggregateID"`
		} `json:"syncEvent"`
	}
	if err := json.Unmarshal([]byte(raw), &envelope); err != nil {
		return false, nil
	}
	if envelope.SyncEvent.Type != "session.updated.1" {
		return false, nil
	}
	sessionID := strings.TrimSpace(envelope.SyncEvent.AggregateID)
	if sessionID == "" {
		return false, nil
	}

	if _, claimed := c.claimedSessions.Load(sessionID); claimed {
		return false, nil
	}
	if c.isGatewayOwnedSession(sessionID) {
		return false, nil
	}
	if !c.allowSyncClaimProbe(sessionID) {
		return false, nil
	}
	return c.ClaimSessionFromTitle(ctx, sessionID)
}

// allowSyncClaimProbe reports whether a sync envelope may trigger a GetSession
// for sessionID, recording the probe when it may.
func (c *Client) allowSyncClaimProbe(sessionID string) bool {
	now := time.Now()
	if value, ok := c.syncClaimProbes.Load(sessionID); ok {
		if last, ok := value.(time.Time); ok && now.Sub(last) < syncClaimProbeWindow {
			return false
		}
	}
	c.syncClaimProbes.Store(sessionID, now)
	return true
}

// ClaimSessionFromTitle adopts a session by reading its title from the server.
//
// AutoClaimFromTitle only fires on session.updated, which is enough for the
// live rename flow but not for a gateway restart: after a restart the first
// event for an already-renamed session is an ordinary message event, and
// without a claim its output would be dropped with "no session handler found".
// This method covers that case by fetching the title once.
//
// Returns true when a claim was actually performed.
func (c *Client) ClaimSessionFromTitle(ctx context.Context, sessionID string) (bool, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return false, nil
	}
	if _, claimed := c.claimedSessions.Load(sessionID); claimed {
		return false, nil
	}
	if c.isGatewayOwnedSession(sessionID) {
		return false, nil
	}

	session, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return false, err
	}
	if session == nil {
		return false, nil
	}
	if c.isCronSession(sessionID, session.Title) {
		return false, nil
	}
	channel, userID, ok := parseSessionTitlePrefix(session.Title)
	if !ok {
		return false, nil
	}
	return c.claimFromTitleParts(ctx, sessionID, channel, userID)
}

// restoreClaimedHandler re-registers a claimed session's long-lived forwarding
// handler after a per-turn handler has finished with it.
//
// A claimed session is bidirectional: the hook calls SwitchUserSession, so the
// user's next chat message resolves to this session and goes through
// SendMessageStreamingWithEvents, which registers its OWN per-turn handler under
// the same session ID and unregisters it on completion. That unregister removes
// the claimed handler too, because both live in the same sessionHandlers map.
// Without this restore the mirror dies after the first chat reply and every
// later TUI turn is dropped with "no session handler found".
func (c *Client) restoreClaimedHandler(sessionID string) {
	value, ok := c.claimedSessions.Load(sessionID)
	if !ok {
		return
	}
	claimed, ok := value.(claimedSession)
	if !ok || claimed.Handler == nil {
		return
	}
	// A suspended session was paused by /switch on purpose. Re-registering here
	// would silently undo that on the next turn completion.
	if _, suspended := c.suspendedClaims.Load(sessionID); suspended {
		return
	}
	if _, registered := c.sessionHandlers.Load(sessionID); registered {
		return
	}
	c.RegisterSessionHandler(sessionID, claimed.Handler.HandleEvent)
	c.activeHandlers.Store(sessionID, claimed.Handler)
	log.Printf("opencode: restored claimed forwarding handler for session %s",
		sessionID[:min(8, len(sessionID))])
}

// ReleaseClaimedSession stops forwarding a claimed session's events and forgets
// the claim. The title is left alone: the session still belongs to the user, it
// is just no longer mirrored into the chat.
func (c *Client) ReleaseClaimedSession(sessionID string) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return
	}
	if value, ok := c.claimedSessions.LoadAndDelete(sessionID); ok {
		if claimed, ok := value.(claimedSession); ok && claimed.Handler != nil {
			claimed.Handler.stopWaitingTimer()
		}
	}
	c.suspendedClaims.Delete(sessionID)
	c.UnregisterSessionHandler(sessionID)
	c.activeHandlers.Delete(sessionID)
	log.Printf("opencode: released claimed session %s", sessionID[:min(8, len(sessionID))])
}

// UnclaimSession stops mirroring a claimed session into the chat AND removes
// the ownership prefix from its title.
//
// ReleaseClaimedSession alone is not enough for a user-facing /release: the
// title still carries "[channel:userID]", and that prefix is exactly what
// AutoClaimFromTitle keys on, so the very next session.updated event would
// re-claim the session and the mirror would come straight back. Stripping the
// prefix is what makes the release stick, and it is also what lets the user
// /rename the session to a different channel afterwards.
//
// Returns the title after the rewrite. A session that carries no prefix is left
// untouched, so releasing twice is harmless.
func (c *Client) UnclaimSession(ctx context.Context, sessionID string) (string, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return "", fmt.Errorf("opencode: unclaim session: sessionID is required")
	}
	if _, busy := c.claimTransitions.LoadOrStore(sessionID, struct{}{}); busy {
		return "", fmt.Errorf("opencode: session ownership change in progress")
	}
	defer c.claimTransitions.Delete(sessionID)

	session, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return "", fmt.Errorf("opencode: unclaim session: %w", err)
	}
	if session == nil {
		return "", fmt.Errorf("opencode: unclaim session: session %s not found", sessionID)
	}

	title := strings.TrimSpace(session.Title)
	if _, _, ok := parseSessionTitlePrefix(title); !ok {
		// Nothing to strip, but the claim (if any) still has to go.
		c.ReleaseClaimedSession(sessionID)
		c.clearSessionThreads(sessionID)
		return title, nil
	}

	rest := strings.TrimSpace(title[strings.Index(title, "]")+1:])
	if rest == "" {
		// opencode needs a non-empty title; the session ID prefix is the only
		// stable thing left to name it by.
		rest = "session " + sessionID[:min(8, len(sessionID))]
	}

	updated, err := c.sdk.Session.Update(ctx, sessionID, opencode.SessionUpdateParams{
		Title: opencode.F(rest),
	})
	if err != nil {
		return "", fmt.Errorf("opencode: unclaim session: rewrite title: %w", err)
	}
	c.ReleaseClaimedSession(sessionID)
	c.clearSessionThreads(sessionID)
	newTitle := rest
	if updated != nil && strings.TrimSpace(updated.Title) != "" {
		newTitle = updated.Title
	}
	log.Printf("opencode: unclaimed session %s (title=%q)",
		sessionID[:min(8, len(sessionID))], newTitle)
	return newTitle, nil
}

// TransferSession moves a claimed session from one chat channel to another.
//
// It is the chat-side equivalent of running /rename in the TUI: the session
// keeps its history and its ID, only the ownership prefix changes. The user
// does not have to go back to the TUI to switch channels.
//
// userID may be empty, in which case the host's ClaimUserResolver picks the
// target (single-user deployments). It must resolve to exactly one user;
// guessing would deliver one person's session output to another.
//
// Returns the title after the transfer.
func (c *Client) TransferSession(ctx context.Context, sessionID, channel, userID string) (string, error) {
	sessionID = strings.TrimSpace(sessionID)
	channel = strings.TrimSpace(channel)
	userID = strings.TrimSpace(userID)
	if sessionID == "" || channel == "" {
		return "", fmt.Errorf("opencode: transfer session: sessionID and channel are required")
	}
	if _, busy := c.claimTransitions.LoadOrStore(sessionID, struct{}{}); busy {
		return "", fmt.Errorf("opencode: session ownership change in progress")
	}
	defer c.claimTransitions.Delete(sessionID)
	// skillgen drafts are internal working sessions; they are not routable.
	if channel == "skillgen" {
		return "", fmt.Errorf("opencode: transfer session: %q is not a routable channel", channel)
	}
	if checker := c.claimChannelChecker(); checker != nil && !checker(channel) {
		return "", fmt.Errorf("opencode: transfer session: unknown channel %q", channel)
	}
	if userID == "" {
		resolver := c.claimUserResolver()
		if resolver == nil {
			return "", fmt.Errorf("opencode: transfer session: channel %q needs an explicit user ID", channel)
		}
		resolved, ok := resolver(ctx, channel)
		if !ok || strings.TrimSpace(resolved) == "" {
			return "", fmt.Errorf("opencode: transfer session: cannot resolve a single user for channel %q, use %s:<userID>", channel, channel)
		}
		userID = strings.TrimSpace(resolved)
	}

	session, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return "", fmt.Errorf("opencode: transfer session: %w", err)
	}
	if session == nil {
		return "", fmt.Errorf("opencode: transfer session: session %s not found", sessionID)
	}
	if c.isCronSession(sessionID, session.Title) {
		return "", fmt.Errorf("opencode: transfer session: cron session %s cannot be transferred", sessionID)
	}

	// Keep the original title text, minus any existing ownership prefix, so a
	// second transfer does not stack prefixes.
	rest := strings.TrimSpace(session.Title)
	if _, _, ok := parseSessionTitlePrefix(rest); ok {
		rest = strings.TrimSpace(rest[strings.Index(rest, "]")+1:])
	}
	if rest == "" {
		// opencode needs a non-empty title; the session ID prefix is the only
		// stable thing left to name it by.
		rest = "session " + sessionID[:min(8, len(sessionID))]
	}

	newTitle := sessionTitlePrefix(channel, userID) + " " + rest
	updated, err := c.sdk.Session.Update(ctx, sessionID, opencode.SessionUpdateParams{
		Title: opencode.F(newTitle),
	})
	if err != nil {
		return "", fmt.Errorf("opencode: transfer session: rewrite title: %w", err)
	}
	if updated != nil && strings.TrimSpace(updated.Title) != "" {
		newTitle = updated.Title
	}

	if hook := c.claimedSessionHook(); hook != nil {
		if err := hook(ctx, sessionID, channel, userID, ""); err != nil {
			_, rollbackErr := c.sdk.Session.Update(ctx, sessionID, opencode.SessionUpdateParams{Title: opencode.F(session.Title)})
			return "", fmt.Errorf("opencode: transfer hook: %w (title rollback: %v)", err, rollbackErr)
		}
	}
	c.ReleaseClaimedSession(sessionID)
	c.clearSessionThreads(sessionID)
	c.attachClaimedSessionHandler(sessionID, channel, userID)
	log.Printf("opencode: transferred session %s to %s/%s (title=%q)",
		sessionID[:min(8, len(sessionID))], channel, userID, newTitle)
	return newTitle, nil
}

func (c *Client) clearSessionThreads(sessionID string) {
	c.sessions.Range(func(key, value any) bool {
		if value == sessionID {
			c.sessions.CompareAndDelete(key, value)
		}
		return true
	})
}

// sessionUsableBy reports whether a session may be driven by the given chat
// identity.
//
// The thread→session binding is only a fallback for callers that omit
// payload.SessionID, but it is the layer a /transfer has to revoke, and
// clearing it at transfer time is not enough on its own: a binding can also be
// stale for reasons the transfer never observes — a process that restarted
// mid-transfer, an adapter that kept its own copy of the mapping, or a session
// that was re-claimed by a different chat after a /release. Trusting a binding
// without checking the current owner is what let a source chat keep writing
// into a session that had already been handed to someone else.
//
// A session that is not claimed at all is usable: gateway-created sessions are
// bound to exactly one thread and never appear in claimedSessions.
func (c *Client) sessionUsableBy(sessionID, channel, userID string) bool {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return false
	}
	value, ok := c.claimedSessions.Load(sessionID)
	if !ok {
		return true
	}
	claimed, ok := value.(claimedSession)
	if !ok {
		return true
	}
	return claimed.Channel == strings.TrimSpace(channel) && claimed.UserID == strings.TrimSpace(userID)
}

// resolveThreadSession returns the session a thread is bound to, provided the
// binding is still valid for the requesting chat identity.
//
// It is the single place that decides whether the thread→session fallback may
// be used, so SendMessage and SendMessageStreamingWithEvents cannot drift apart
// on the ownership rule. A binding that fails the check is deleted rather than
// merely ignored: leaving it in place would make every later message from this
// thread re-run the same rejected lookup.
func (c *Client) resolveThreadSession(threadID, channel, userID string) (string, bool) {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return "", false
	}
	value, ok := c.sessions.Load(threadID)
	if !ok {
		return "", false
	}
	sessionID, ok := value.(string)
	if !ok || sessionID == "" {
		return "", false
	}
	if !c.sessionUsableBy(sessionID, channel, userID) {
		c.sessions.CompareAndDelete(threadID, value)
		log.Printf("opencode: dropped stale thread binding %s -> %s (session now belongs to another chat)",
			threadID, sessionID[:min(8, len(sessionID))])
		return "", false
	}
	return sessionID, true
}

// checkExplicitSessionOwnership rejects a payload that names a session now owned
// by a different chat.
//
// The explicit path is the adapter's own user→session map, which /transfer
// clears — but only for the chat that ran the command. A second chat that had
// adopted the same session (two /rename claims in one chat, or a stale adapter
// map after a restart) would otherwise keep driving a session that no longer
// belongs to it, which is the same defect as the stale thread binding.
//
// Internal payloads are exempt: they are gateway-generated work (scheduler
// bootstrap prompts) that deliberately targets a session the gateway created.
func (c *Client) checkExplicitSessionOwnership(payload MessagePayload) error {
	if payload.Internal {
		return nil
	}
	sessionID := strings.TrimSpace(payload.SessionID)
	if sessionID == "" {
		return nil
	}
	if c.sessionUsableBy(sessionID, payload.Channel, payload.UserID) {
		return nil
	}
	return fmt.Errorf("opencode: session %s belongs to another chat", sessionID[:min(8, len(sessionID))])
}

// ClaimedSessions lists the session IDs currently mirrored into a chat.
func (c *Client) ClaimedSessions() []string {
	var ids []string
	c.claimedSessions.Range(func(key, _ any) bool {
		if id, ok := key.(string); ok {
			ids = append(ids, id)
		}
		return true
	})
	return ids
}

// IsClaimedSession reports whether sessionID is currently mirrored into a chat.
//
// It exists so a user-facing /release can refuse to touch a session the gateway
// created itself. Those sessions carry the same "[channel:userID]" title prefix
// but are NOT in claimedSessions, and stripping their prefix would remove them
// from ListSessionsForUser — the user would lose the session from /sessions.
func (c *Client) IsClaimedSession(sessionID string) bool {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return false
	}
	_, ok := c.claimedSessions.Load(sessionID)
	return ok
}

func (c *Client) IsClaimedSessionFor(sessionID, channel, userID string) bool {
	value, ok := c.claimedSessions.Load(sessionID)
	if !ok {
		return false
	}
	claimed := value.(claimedSession)
	return claimed.Channel == channel && claimed.UserID == userID
}

// IsSuspendedClaimedSession reports whether a claimed session's mirroring is
// currently paused by /switch.
//
// It is true only for a session that is still claimed: a released session is
// gone from claimedSessions and reports false here too.
func (c *Client) IsSuspendedClaimedSession(sessionID string) bool {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return false
	}
	if _, ok := c.claimedSessions.Load(sessionID); !ok {
		return false
	}
	_, suspended := c.suspendedClaims.Load(sessionID)
	return suspended
}

// SuspendClaimedSession pauses mirroring for a claimed session without giving
// up the claim.
//
// It exists for /switch. Moving to another session must stop the previous one
// from writing into the same chat — otherwise two conversations interleave in
// one window — but it must NOT be a /release: the title prefix is what keeps
// the session in /sessions, and stripping it would make the session
// unreachable from the very listing the user is switching with.
//
// So the claim record and the title stay, and only the forwarding handler is
// detached. Because the record stays in claimedSessions, AutoClaimFromTitle
// keeps skipping the session (it bails on an existing claim), so a later
// session.updated cannot silently re-attach the mirror.
//
// Returns true when the session was claimed and is now suspended.
func (c *Client) SuspendClaimedSession(sessionID string) bool {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return false
	}
	value, ok := c.claimedSessions.Load(sessionID)
	if !ok {
		return false
	}
	claimed, ok := value.(claimedSession)
	if !ok {
		return false
	}
	if claimed.Handler != nil {
		claimed.Handler.stopWaitingTimer()
	}
	c.suspendedClaims.Store(sessionID, struct{}{})
	c.UnregisterSessionHandler(sessionID)
	c.activeHandlers.Delete(sessionID)
	log.Printf("opencode: suspended claimed session %s (%s/%s)",
		sessionID[:min(8, len(sessionID))], claimed.Channel, claimed.UserID)
	return true
}

// ResumeClaimedSession re-attaches the forwarding handler of a session that an
// earlier /switch paused.
//
// Returns true when the session was actually suspended and is now mirroring
// again. A session that was never suspended returns false, so a caller can tell
// "resumed" from "was already live" without a second lookup.
func (c *Client) ResumeClaimedSession(sessionID string) bool {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return false
	}
	if _, suspended := c.suspendedClaims.LoadAndDelete(sessionID); !suspended {
		return false
	}
	if value, ok := c.claimedSessions.LoadAndDelete(sessionID); ok {
		claimed := value.(claimedSession)
		c.attachClaimedSessionHandler(sessionID, claimed.Channel, claimed.UserID)
	}
	log.Printf("opencode: resumed claimed session %s", sessionID[:min(8, len(sessionID))])
	return true
}

// SuspendClaimedSessionsForUser pauses every claimed session owned by
// channel/userID except keepSessionID, and returns the IDs it paused.
//
// /switch uses this rather than suspending a single session because one chat can
// hold several claims at once: /rename can adopt two different TUI sessions
// into the same chat, and both would otherwise keep writing into it. The target
// of the switch is excluded so switching to a session does not pause it.
func (c *Client) SuspendClaimedSessionsForUser(channel, userID, keepSessionID string) []string {
	channel = strings.TrimSpace(channel)
	userID = strings.TrimSpace(userID)
	keepSessionID = strings.TrimSpace(keepSessionID)
	if channel == "" || userID == "" {
		return nil
	}

	var suspended []string
	c.claimedSessions.Range(func(key, value any) bool {
		sessionID, ok := key.(string)
		if !ok || sessionID == keepSessionID {
			return true
		}
		claimed, ok := value.(claimedSession)
		if !ok || claimed.Channel != channel || claimed.UserID != userID {
			return true
		}
		if c.SuspendClaimedSession(sessionID) {
			suspended = append(suspended, sessionID)
		}
		return true
	})
	return suspended
}

// claimedFlushChars is the buffer size at which a claimed session's output is
// pushed to the chat before the turn ends. Without it a long answer would only
// appear once the turn finished; without buffering at all, every token delta
// would become its own HTTP request.
const claimedFlushChars = 1200

// claimedForwarder buffers a claimed session's output and delivers it to the
// chat in batches.
//
// Buffering is required, not an optimisation: the streaming handler invokes the
// callback once per token delta, and each delivery is a platform HTTP call made
// while the handler lock is held. Sending per delta would both flood the
// platform and stall the SSE event loop.
type claimedForwarder struct {
	client    *Client
	sessionID string

	mu  sync.Mutex
	buf strings.Builder
}

func (f *claimedForwarder) write(chunk string) {
	f.mu.Lock()
	f.buf.WriteString(chunk)
	pending := f.buf.Len()
	f.mu.Unlock()

	if pending >= claimedFlushChars {
		f.flush()
	}
}

func (f *claimedForwarder) flush() {
	f.mu.Lock()
	content := f.buf.String()
	f.buf.Reset()
	f.mu.Unlock()

	if strings.TrimSpace(content) == "" {
		return
	}
	if err := f.client.SendMessageToSession(context.Background(), f.sessionID, content); err != nil {
		log.Printf("opencode: claimed session %s delivery failed: %v",
			f.sessionID[:min(8, len(f.sessionID))], err)
	}
}

// attachClaimedSessionHandler registers a streaming handler for a session the
// gateway did not create.
//
// Without it, eventListenerLoop finds no handler for the session and drops every
// event with "no session handler found" — which is exactly why a TUI session
// never reached a chat adapter before.
//
// The handler is deliberately minimal: it forwards real answer text and
// permission/question prompts, and drops the progress signals (thinking, tool,
// step, todo, wait-hint) that adapters already suppress. Unlike the handler
// created by SendMessageStreamingWithEvents, this one is NOT unregistered when a
// turn completes — a claimed session is a long-lived mirror, so the handler
// resets itself for the next turn instead.
func (c *Client) attachClaimedSessionHandler(sessionID, channel, userID string) {
	if existing, ok := c.claimedSessions.Load(sessionID); ok {
		if claimed, ok := existing.(claimedSession); ok && claimed.Handler != nil {
			if _, registered := c.sessionHandlers.Load(sessionID); registered {
				log.Printf("opencode: claimed session %s already has a forwarding handler",
					sessionID[:min(8, len(sessionID))])
				return
			}
		}
	}

	payload := MessagePayload{Channel: channel, UserID: userID, SessionID: sessionID}
	forwarder := &claimedForwarder{client: c, sessionID: sessionID}
	var handler *StreamingSessionHandler

	callback := func(chunk string) error {
		value, ok := c.claimedSessions.Load(sessionID)
		if !ok || value.(claimedSession).Handler != handler || c.IsSuspendedClaimedSession(sessionID) {
			return nil
		}
		if chunk == "" {
			return nil
		}
		switch {
		case chunk == FlushSignal:
			forwarder.flush()
			return nil
		case strings.HasPrefix(chunk, ThinkingSignalPrefix),
			strings.HasPrefix(chunk, ToolSignalPrefix),
			strings.HasPrefix(chunk, StepSignalPrefix),
			strings.HasPrefix(chunk, TodoSignalPrefix),
			strings.HasPrefix(chunk, WaitHintSignalPrefix):
			return nil
		case strings.HasPrefix(chunk, QuestionSignalPrefix):
			// Permission and question prompts block the turn, so they must not
			// wait behind buffered answer text.
			forwarder.flush()
			forwarder.write(strings.TrimPrefix(chunk, QuestionSignalPrefix))
			forwarder.flush()
			return nil
		}
		forwarder.write(chunk)
		return nil
	}

	handler = NewStreamingSessionHandlerWithModel(
		sessionID, callback, nil,
		func() {
			value, ok := c.claimedSessions.Load(sessionID)
			if !ok || value.(claimedSession).Handler != handler || c.IsSuspendedClaimedSession(sessionID) {
				return
			}
			// Deliver anything still buffered. notifyCompletion already sends
			// FlushSignal, but a turn that ends via session.error or a fallback
			// retry may not, and losing the tail of an answer is worse than a
			// redundant empty flush.
			forwarder.flush()
			log.Printf("opencode: claimed session %s turn finished",
				sessionID[:min(8, len(sessionID))])
		},
		c, c, false, false, nil, payload,
	)
	// A claimed session is a long-lived mirror: the handler stays registered
	// across turns and resets itself when the next turn starts.
	handler.longLived = true

	c.RegisterSessionHandler(sessionID, handler.HandleEvent)
	c.activeHandlers.Store(sessionID, handler)
	// A fresh claim is live by definition: clear any pause left over from a
	// previous /switch away from this session.
	c.suspendedClaims.Delete(sessionID)
	c.claimedSessions.Store(sessionID, claimedSession{
		Channel: channel,
		UserID:  userID,
		Handler: handler,
	})
	log.Printf("opencode: attached forwarding handler to claimed session %s (%s/%s)",
		sessionID[:min(8, len(sessionID))], channel, userID)
}

// beginLongLivedTurnLocked decides whether an event that arrived after the
// previous turn completed actually belongs to a new turn, and if so resets the
// per-turn state so it is delivered normally.
//
// The caller must hold s.mu. It returns true when a new turn was started.
//
// Resetting here rather than from onComplete is deliberate. fireOnComplete runs
// onComplete in a goroutine, so a reset performed there can lose a race against
// a fast next turn: the new turn's first events would be dropped as "already
// completed" and its answer would never reach the chat.
//
// A new turn is recognised by an assistant message ID that differs from the one
// being mirrored. Events that carry no message ID (session.idle, session.error,
// permission.asked, ...) are treated as trailing events of the finished turn and
// are not allowed to start a new one — otherwise a stray idle would reset the
// handler and re-arm the completion path.
func (s *StreamingSessionHandler) beginLongLivedTurnLocked(event *opencode.EventListResponse) bool {
	messageID := s.assistantMessageIDFromEvent(event)
	if messageID == "" || messageID == s.longLivedTurnMessageID {
		return false
	}

	s.longLivedTurnMessageID = messageID
	s.resetForClaimedTurnLocked()
	return true
}

// assistantMessageIDFromEvent returns the assistant message ID an event belongs
// to, or "" when the event carries none or belongs to a user message.
//
// The role check matters: a user message ID must never be mistaken for a new
// turn, or the user's own prompt would reset the handler in the middle of an
// answer. message.updated and message.part.updated carry the role inline;
// message.part.delta does not, so the role is looked up in the client's
// messageRoles table, which message.updated populates.
func (s *StreamingSessionHandler) assistantMessageIDFromEvent(event *opencode.EventListResponse) string {
	if event == nil {
		return ""
	}
	raw := event.JSON.RawJSON()
	if raw == "" {
		return ""
	}

	var probe struct {
		Properties struct {
			MessageID string `json:"messageID"`
			Part      struct {
				MessageID string `json:"messageID"`
			} `json:"part"`
			Message struct {
				Role string `json:"role"`
			} `json:"message"`
			Info struct {
				ID   string `json:"id"`
				Role string `json:"role"`
			} `json:"info"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(raw), &probe); err != nil {
		return ""
	}

	// message.updated: the role is authoritative and inline.
	if probe.Properties.Info.ID != "" {
		if probe.Properties.Info.Role != "assistant" {
			return ""
		}
		return probe.Properties.Info.ID
	}

	// message.part.updated: the role is inline on the parent message.
	if probe.Properties.Part.MessageID != "" {
		if role := probe.Properties.Message.Role; role != "" && role != "assistant" {
			return ""
		}
		return probe.Properties.Part.MessageID
	}

	// message.part.delta: no inline role, so consult the role table.
	if probe.Properties.MessageID != "" {
		if s.client != nil {
			if role, ok := s.client.messageRoles.Load(probe.Properties.MessageID); ok {
				if value, ok := role.(string); ok && value != "assistant" {
					return ""
				}
			}
		}
		return probe.Properties.MessageID
	}
	return ""
}

// resetForClaimedTurnLocked clears per-turn state so the next turn on a claimed
// session is delivered normally.
//
// It differs from ResetForNewPrompt in one way that matters: it does NOT re-arm
// the "still working" hint timer. A claimed session sits idle between turns, and
// a re-armed timer would fire 30s later and tell the user a turn is running when
// nothing is.
func (s *StreamingSessionHandler) resetForClaimedTurnLocked() {
	s.stopWaitingTimer()
	s.completed = false
	s.contentSent = false
	s.lastContent = ""
	s.receivedStepFinish = false
	s.stepFinishTime = time.Time{}
	s.waitingHintSent = false
	s.pendingQuestionSince = time.Time{}
	s.pendingQuestionSent = false
	s.pendingQuestionReminders = 0
	s.activeToolParts = make(map[string]bool)
	s.toolObserved = false
	s.sessionTodos = nil
	s.sessionDiff = nil
	s.lastTodoSummary = ""
	s.lastTodoStateHash = ""
	s.lastTodoPushTime = time.Time{}
	s.serverCompacting = false
	s.failedTurn = false
	s.retryAttempt = 0
	s.retryMessage = ""
	s.retryStatusCode = 0
	s.retrySurfaced = false
	s.fallbackRetryAttempted = false
	s.fallbackRetryDispatched = false

	s.partTextCache.Range(func(k, _ any) bool { s.partTextCache.Delete(k); return true })
	s.toolSignalCache.Range(func(k, _ any) bool { s.toolSignalCache.Delete(k); return true })
	s.partRoles.Range(func(k, _ any) bool { s.partRoles.Delete(k); return true })

	// Allow the next turn's session.idle to fire onComplete again.
	s.onCompleteOnce = sync.Once{}
}
