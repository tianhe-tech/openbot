package opencode

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
)

// ErrSessionBusy is returned when a /switch target is mid-turn and cannot be
// taken over without losing that turn's output.
//
// It is a distinct error rather than a generic failure because the user can act
// on it: waiting for the turn to finish (or aborting it) makes the switch
// possible, whereas a generic error reads as "this session is broken".
var ErrSessionBusy = errors.New("opencode: session is busy")

// SessionRoutingSnapshot is the adapter-side routing state a /switch mutates.
//
// It exists because the two halves of the routing live in different places: the
// opencode client owns the thread→session map and the claim records, while the
// adapter owns the user→session map and the per-session platform delivery
// metadata. A switch has to change both, and a failed switch has to put both
// back — including the delivery metadata, which SwitchUserSession deletes.
type SessionRoutingSnapshot struct {
	Channel string
	UserID  string

	// PreviousSessionID is the session the user was routed to before the
	// switch, empty when the user had no binding yet.
	PreviousSessionID string
	// PreviousSessionData is that session's platform delivery metadata.
	PreviousSessionData map[string]string

	// TargetSessionID is the session being switched to.
	TargetSessionID string
	// TargetSessionData is the target's delivery metadata before the switch.
	TargetSessionData map[string]string
	// TargetUser is the user the target was reverse-mapped to before the
	// switch, empty when it had no reverse mapping.
	TargetUser string
}

// SessionSwitchRouting is the adapter half of a transactional /switch.
//
// It is an interface rather than a direct call because the opencode package must
// not import the adapters package (the adapters already import opencode). The
// gateway wires an implementation over its adapter registry at startup.
//
// The three methods are deliberately separate. ClaimSession cannot be used as a
// side-effect-free prepare step — it rewrites the title and runs the routing
// hook — so the shared switch logic has to snapshot, commit and compensate
// explicitly instead of relying on a single "do the switch" call.
type SessionSwitchRouting interface {
	// SnapshotSessionRouting records the current routing for userID and the
	// delivery metadata of sessionID. It must not mutate anything.
	SnapshotSessionRouting(channel, userID, sessionID string) SessionRoutingSnapshot
	// ApplySessionRouting points userID at sessionID and records channelData as
	// the session's platform delivery context when it is non-empty.
	//
	// channelData is a key/value map rather than one string because the
	// platforms disagree on what a delivery target is: Feishu needs a
	// receive_id plus a receive_id_type, DingTalk needs a session webhook,
	// WeChat and WeCom need nothing beyond the user ID. Flattening those into a
	// single "channel" string is what made the old claimed-session sender pass
	// an adapter name where Feishu expected a receive_id_type.
	//
	// It returns an error when the channel has no adapter, which is the only
	// delivery-context validation available at this layer: whether a specific
	// platform can reach a specific user is decided by that platform's send
	// path, not here.
	ApplySessionRouting(channel, userID, sessionID string, channelData map[string]string) error
	// RestoreSessionRouting puts back a snapshot taken before
	// ApplySessionRouting. It must be safe to call after a partial apply.
	RestoreSessionRouting(snapshot SessionRoutingSnapshot) error
	// ValidateSessionDelivery reports whether the adapter can actually deliver
	// this session's output to userID.
	//
	// It exists because a restore-claim re-attaches a mirror that will only ever
	// speak through the adapter, and some adapters silently drop a send they
	// cannot route (DingTalk returns nil when it has neither a webhook nor a
	// user). Claiming such a session would tell the user their TUI replies are
	// mirrored when nothing will arrive, so the switch must fail instead.
	ValidateSessionDelivery(channel, userID, sessionID string, channelData map[string]string) error
}

// SessionSwitchRequest is one /switch invocation.
type SessionSwitchRequest struct {
	// Channel is the adapter name ("wechat", "feishu", "dingtalk", "wecom").
	Channel string
	// UserID is the platform user issuing the command.
	UserID string
	// ThreadID is the chat/room the command arrived in. It is the key of the
	// client's fallback thread→session map.
	ThreadID string
	// Selector is the /switch argument: a 1-based index or a session ID prefix.
	Selector string
	// ChannelData is the platform delivery context of the inbound message: a
	// DingTalk session webhook, a Feishu receive_id/receive_id_type pair, or
	// nothing at all for WeChat and WeCom. It is recorded for the target so a
	// restored claim can deliver replies. Empty is valid: the adapter then
	// resolves the target from its own per-user state, which is what a /rename
	// claim already relies on.
	ChannelData map[string]string
}

// SessionSwitchResult reports what a successful /switch actually did, so the
// adapter can tell the user which parts of the mirroring changed.
type SessionSwitchResult struct {
	Session UserSession
	// Resumed is true when the target had been paused by an earlier /switch and
	// its mirroring is live again.
	Resumed bool
	// Claimed is true when the target had no local claim and one was restored,
	// so later TUI turns will now reach this chat.
	Claimed bool
	// Suspended lists the other claimed sessions this chat was mirroring that
	// were paused to stop two conversations interleaving in one window.
	Suspended []string
}

// SetSessionSwitchRouting installs the adapter-side routing bridge used by
// SwitchSession. Without it a /switch still moves the client's thread mapping
// and the claim state, but the adapter's user→session map is left alone — which
// means the next chat message would still resolve to the previous session.
func (c *Client) SetSessionSwitchRouting(routing SessionSwitchRouting) {
	c.sessionSwitchRoutingMu.Lock()
	defer c.sessionSwitchRoutingMu.Unlock()
	c.sessionSwitchRouting = routing
}

func (c *Client) sessionSwitchRoutingBridge() SessionSwitchRouting {
	c.sessionSwitchRoutingMu.RLock()
	defer c.sessionSwitchRoutingMu.RUnlock()
	return c.sessionSwitchRouting
}

// SwitchSession makes an existing session the current session of a chat.
//
// It is transactional. The previous implementation bound the thread, rewrote
// the adapter mapping, suspended every other claim and reported success without
// checking whether any of it worked; a failure halfway through left the chat
// pointing at a session whose delivery metadata had already been deleted, and
// the user was told the switch succeeded.
//
// The order below is what makes it safe:
//
//  1. resolve the target and validate ownership and the cron guard;
//  2. take the per-session ownership transition lock, then re-validate, so a
//     concurrent /transfer or /release cannot be overwritten by a stale
//     snapshot;
//  3. refuse a session that is mid-turn — its output is bound to the session,
//     not to the chat, so moving the routing would redirect a running answer
//     into a different conversation;
//  4. snapshot the claim state, gateway ownership, active handler and delivery
//     metadata BEFORE mutating anything;
//  5. commit the adapter routing first (the only fallible step), then the
//     thread mapping, then the claim suspension/resume/restore;
//  6. on a failed commit, restore the snapshot and return an explicit error.
func (c *Client) SwitchSession(ctx context.Context, req SessionSwitchRequest) (SessionSwitchResult, error) {
	channel := strings.TrimSpace(req.Channel)
	userID := strings.TrimSpace(req.UserID)
	selector := strings.TrimSpace(req.Selector)
	if channel == "" || userID == "" {
		return SessionSwitchResult{}, fmt.Errorf("opencode: switch session: channel and userID are required")
	}
	if selector == "" {
		return SessionSwitchResult{}, fmt.Errorf("opencode: switch session: selector is required")
	}

	target, err := c.ResolveUserSession(ctx, channel, userID, selector)
	if err != nil {
		return SessionSwitchResult{}, err
	}
	sessionID := strings.TrimSpace(target.ID)
	if sessionID == "" {
		return SessionSwitchResult{}, fmt.Errorf("opencode: switch session: resolved an empty session ID")
	}

	// Serialize against every other ownership change for this session. The lock
	// is taken before the snapshot so a concurrent claim cannot slip in between
	// reading and committing.
	if _, busy := c.claimTransitions.LoadOrStore(sessionID, struct{}{}); busy {
		return SessionSwitchResult{}, fmt.Errorf("opencode: switch session: session ownership change in progress")
	}
	defer c.claimTransitions.Delete(sessionID)

	// Re-validate under the lock. The listing above is an HTTP round-trip old,
	// and a concurrent /transfer or /release could have changed the owner in the
	// meantime; committing a stale snapshot would hand this chat a session that
	// now belongs to someone else.
	session, err := c.GetSession(ctx, sessionID)
	if err != nil {
		return SessionSwitchResult{}, fmt.Errorf("opencode: switch session: %w", err)
	}
	if session == nil {
		return SessionSwitchResult{}, fmt.Errorf("opencode: switch session: session %s not found", sessionID)
	}
	if c.isCronSession(sessionID, session.Title) {
		return SessionSwitchResult{}, fmt.Errorf("opencode: switch session: cron session %s cannot become the current chat session", sessionID)
	}
	ownerChannel, ownerUser, ok := parseSessionTitlePrefix(session.Title)
	if !ok || ownerChannel != channel || (ownerUser != "" && ownerUser != userID) {
		return SessionSwitchResult{}, ErrSessionNotOwned
	}

	// A turn in flight cannot be moved losslessly. The per-turn handler is bound
	// to the session, and the adapter routing is what decides where its deltas
	// go, so switching now would deliver the rest of a running answer into a
	// different conversation.
	if c.IsSessionRunning(sessionID) {
		return SessionSwitchResult{}, fmt.Errorf("%w: %s", ErrSessionBusy, sessionID)
	}

	// Snapshot before mutating. Everything below reads from these values, so a
	// failure can be undone without re-deriving state that has already changed.
	priorClaimValue, hadClaim := c.claimedSessions.Load(sessionID)
	gatewayOwned := c.isGatewayOwnedSession(sessionID)
	routing := c.sessionSwitchRoutingBridge()
	var routingSnapshot SessionRoutingSnapshot
	if routing != nil {
		routingSnapshot = routing.SnapshotSessionRouting(channel, userID, sessionID)
	}

	// Decide what the target needs before touching any state.
	//
	//   - already claimed by this chat: resume it if an earlier /switch paused
	//     it. Re-claiming would rewrite the title and replace the long-lived
	//     handler for no reason.
	//   - claimed by another chat: refuse. The ownership filter above should
	//     already have excluded it, so this is a defence against a title that
	//     changed between the listing and the switch.
	//   - created by this gateway: only the routing changes. Its per-turn
	//     handler is the delivery path and must not be replaced by a mirror.
	//   - adopted from the TUI (ownership title, no local claim): restore the
	//     claim so the mirror survives the next TUI turn.
	needClaim := false
	if hadClaim {
		claimed, ok := priorClaimValue.(claimedSession)
		if !ok || claimed.Channel != channel || claimed.UserID != userID {
			return SessionSwitchResult{}, fmt.Errorf("opencode: switch session: session %s is claimed by another chat", sessionID)
		}
	} else if !gatewayOwned {
		needClaim = true
	}

	// A restore-claim must never overwrite a live handler. A registered handler
	// on an unclaimed session is either a per-turn handler whose turn has not
	// been cleaned up yet or a mirror this process does not know about; either
	// way replacing it would silently drop that turn's output.
	if needClaim {
		if _, registered := c.sessionHandlers.Load(sessionID); registered {
			return SessionSwitchResult{}, fmt.Errorf("%w: %s", ErrSessionBusy, sessionID)
		}
		// A restore-claim promises that later TUI turns reach this chat. Some
		// adapters drop a send they cannot route without returning an error, so
		// the promise has to be checked here rather than discovered by the user
		// waiting for a reply that never arrives.
		if routing != nil {
			if err := routing.ValidateSessionDelivery(channel, userID, sessionID, req.ChannelData); err != nil {
				return SessionSwitchResult{}, fmt.Errorf("opencode: switch session: %w", err)
			}
		}
	}

	// Commit. ApplySessionRouting is the only fallible step, so it runs first:
	// if it fails, nothing else has been mutated yet and the compensation is
	// limited to whatever the adapter changed before returning.
	if routing != nil {
		if applyErr := routing.ApplySessionRouting(channel, userID, sessionID, req.ChannelData); applyErr != nil {
			if restoreErr := routing.RestoreSessionRouting(routingSnapshot); restoreErr != nil {
				log.Printf("opencode: switch session %s: restore routing after failed apply: %v", sessionID, restoreErr)
			}
			return SessionSwitchResult{}, fmt.Errorf("opencode: switch session: %w", applyErr)
		}
	}

	threadID := strings.TrimSpace(req.ThreadID)
	if threadID == "" {
		threadID = userID
	}
	c.BindSessionToThread(threadID, sessionID)

	// Pause every other claimed session this chat is mirroring, then resume or
	// restore the target. Without the pause, two TUI sessions adopted into the
	// same chat would keep writing into it at once and their answers would
	// interleave.
	suspended := c.SuspendClaimedSessionsForUser(channel, userID, sessionID)
	resumed := false
	if hadClaim {
		resumed = c.ResumeClaimedSession(sessionID)
	}
	if needClaim {
		c.attachClaimedSessionHandler(sessionID, channel, userID)
	}

	log.Printf("opencode: /switch %s user=%s thread=%s -> session=%s (claimed=%t resumed=%t suspended=%d)",
		channel, userID, threadID, sessionID, needClaim, resumed, len(suspended))

	return SessionSwitchResult{
		Session:   target,
		Resumed:   resumed,
		Claimed:   needClaim,
		Suspended: suspended,
	}, nil
}

// SwitchFailureMessage renders a /switch error for a chat user.
//
// It lives here rather than in each adapter so all four channels report the same
// states: "not yours" must stay indistinguishable from "does not exist", and a
// busy session must read as retryable rather than broken.
func SwitchFailureMessage(selector string, err error) string {
	switch {
	case errors.Is(err, ErrSessionNotOwned):
		return fmt.Sprintf("❌ 找不到会话 %q\n\n发送 /sessions 查看你自己的会话列表", selector)
	case errors.Is(err, ErrSessionBusy):
		return "⏳ 该会话正在执行中，暂时无法切换\n\n等这一轮结束后再试，或先发送 /abort 中止当前任务"
	default:
		return fmt.Sprintf("❌ 切换会话失败: %v", err)
	}
}

// Message renders a /switch confirmation.
func (r SessionSwitchResult) Message() string {
	note := ""
	if r.Resumed {
		note += "\n\n▶ 已恢复该会话的 TUI 转发"
	}
	if r.Claimed {
		note += "\n\n🔗 已恢复该会话的 TUI 转发（之后在 TUI 里继续追问，回复会同步到本渠道）"
	}
	if len(r.Suspended) > 0 {
		note += fmt.Sprintf("\n⏸ 已暂停 %d 个其他认领会话的转发（/switch 切回即可恢复）", len(r.Suspended))
	}
	return fmt.Sprintf("✅ 已切换到会话: %s\n\nID: %s\n更新: %s\n\n💡 直接发送消息即可继续该会话的上下文%s",
		r.Session.Title, r.Session.ID[:min(8, len(r.Session.ID))],
		r.Session.Updated.Format("2006-01-02 15:04"), note)
}
