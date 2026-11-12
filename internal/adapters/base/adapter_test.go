package base

import "testing"

func TestSwitchUserSessionReplacesTheMapping(t *testing.T) {
	adapter := NewBidirectionalAdapter("test", nil)

	adapter.MapUserToSession("alice", "ses_first")
	if got, ok := adapter.GetSessionForUser("alice"); !ok || got != "ses_first" {
		t.Fatalf("after the initial map: got (%q, %t), want (ses_first, true)", got, ok)
	}

	adapter.SwitchUserSession("alice", "ses_second")
	if got, ok := adapter.GetSessionForUser("alice"); !ok || got != "ses_second" {
		t.Fatalf("after /switch: got (%q, %t), want (ses_second, true)", got, ok)
	}
	if got, ok := adapter.GetUserForSession("ses_second"); !ok || got != "alice" {
		t.Fatalf("the new session must map back to the user: got (%q, %t)", got, ok)
	}
}

// TestSwitchUserSessionDropsThePreviousSession is the assertion that matters:
// leaving the old reverse mapping in place would keep routing that session's
// events to a user who has moved away from it.
func TestSwitchUserSessionDropsThePreviousSession(t *testing.T) {
	adapter := NewBidirectionalAdapter("test", nil)

	adapter.MapUserToSession("alice", "ses_first")
	adapter.MapSessionData("ses_first", "receive_id", "chat-1")

	adapter.SwitchUserSession("alice", "ses_second")

	if got, ok := adapter.GetUserForSession("ses_first"); ok {
		t.Fatalf("the previous session still maps back to %q", got)
	}
	if got, ok := adapter.GetSessionData("ses_first", "receive_id"); ok {
		t.Fatalf("the previous session's data survived the switch: %q", got)
	}
}

func TestSwitchUserSessionKeepsDataWhenTheSessionIsUnchanged(t *testing.T) {
	adapter := NewBidirectionalAdapter("test", nil)

	adapter.MapUserToSession("alice", "ses_same")
	adapter.MapSessionData("ses_same", "receive_id", "chat-1")

	adapter.SwitchUserSession("alice", "ses_same")

	if got, ok := adapter.GetSessionData("ses_same", "receive_id"); !ok || got != "chat-1" {
		t.Fatalf("switching to the session already in use must not drop its data: got (%q, %t)", got, ok)
	}
}

func TestSwitchUserSessionIgnoresEmptyArguments(t *testing.T) {
	adapter := NewBidirectionalAdapter("test", nil)
	adapter.MapUserToSession("alice", "ses_keep")

	adapter.SwitchUserSession("", "ses_other")
	adapter.SwitchUserSession("alice", "")
	adapter.SwitchUserSession("   ", "   ")

	if got, ok := adapter.GetSessionForUser("alice"); !ok || got != "ses_keep" {
		t.Fatalf("an empty argument must not disturb the mapping: got (%q, %t)", got, ok)
	}
}

func TestSwitchUserSessionDoesNotDisturbOtherUsers(t *testing.T) {
	adapter := NewBidirectionalAdapter("test", nil)

	adapter.MapUserToSession("alice", "ses_a")
	adapter.MapUserToSession("bob", "ses_b")

	adapter.SwitchUserSession("alice", "ses_a2")

	if got, ok := adapter.GetSessionForUser("bob"); !ok || got != "ses_b" {
		t.Fatalf("another user's mapping changed: got (%q, %t)", got, ok)
	}
	if got, ok := adapter.GetUserForSession("ses_b"); !ok || got != "bob" {
		t.Fatalf("another user's reverse mapping changed: got (%q, %t)", got, ok)
	}
}

// TestSoleUser covers the single-user shortcut behind a channel-only claim
// ("[feishu] my work"). It must report the one user, and must refuse as soon as
// a second user appears — guessing there would deliver one person's session
// output to another.
func TestSoleUser(t *testing.T) {
	t.Run("no users", func(t *testing.T) {
		adapter := NewBidirectionalAdapter("test", nil)
		if got, ok := adapter.SoleUser(); ok {
			t.Fatalf("SoleUser() = (%q, true) on an empty adapter", got)
		}
	})

	t.Run("one user", func(t *testing.T) {
		adapter := NewBidirectionalAdapter("test", nil)
		adapter.MapUserToSession("alice", "ses_a")
		if got, ok := adapter.SoleUser(); !ok || got != "alice" {
			t.Fatalf("SoleUser() = (%q, %t), want (alice, true)", got, ok)
		}
	})

	t.Run("two users", func(t *testing.T) {
		adapter := NewBidirectionalAdapter("test", nil)
		adapter.MapUserToSession("alice", "ses_a")
		adapter.MapUserToSession("bob", "ses_b")
		if got, ok := adapter.SoleUser(); ok {
			t.Fatalf("SoleUser() = (%q, true) with two users mapped", got)
		}
	})

	t.Run("cron pseudo-users are ignored", func(t *testing.T) {
		adapter := NewBidirectionalAdapter("test", nil)
		adapter.MapUserToSession("alice", "ses_a")
		adapter.MapUserToSession("cron:ses_scheduled", "ses_scheduled")
		if got, ok := adapter.SoleUser(); !ok || got != "alice" {
			t.Fatalf("SoleUser() = (%q, %t), want (alice, true)", got, ok)
		}
	})

	t.Run("only cron pseudo-users", func(t *testing.T) {
		adapter := NewBidirectionalAdapter("test", nil)
		adapter.MapUserToSession("cron:ses_scheduled", "ses_scheduled")
		if got, ok := adapter.SoleUser(); ok {
			t.Fatalf("SoleUser() = (%q, true) with only cron users", got)
		}
	})
}
