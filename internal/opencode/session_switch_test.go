package opencode

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// sessionListServer serves GET /session with a fixed body. The gateway's
// ownership filter is client-side (SessionListParams carries only Directory),
// so every test here drives the real ListSessions path against a fake upstream.
func sessionListServer(t *testing.T, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/session" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
}

// sessionJSON builds one element of the GET /session array. time.created and
// time.updated are epoch milliseconds, matching the real upstream.
func sessionJSON(id, title string, updatedMillis int64) string {
	return fmt.Sprintf(
		`{"id":%q,"directory":"/work","projectID":"proj","version":"1.18.32","title":%q,"time":{"created":%d,"updated":%d}}`,
		id, title, updatedMillis, updatedMillis)
}

func TestListSessionsForUserExcludesOtherUsers(t *testing.T) {
	now := time.Now().UnixMilli()
	body := "[" + strings.Join([]string{
		sessionJSON("ses_mine_1", "[wechat:alice] chat-a", now-3000),
		sessionJSON("ses_theirs_1", "[wechat:bob] chat-b", now-1000),
		sessionJSON("ses_mine_2", "[wechat:alice] chat-c", now-2000),
		sessionJSON("ses_other_channel", "[feishu:alice] chat-d", now-500),
		sessionJSON("ses_unowned", "no ownership prefix", now),
	}, ",") + "]"

	server := sessionListServer(t, body)
	defer server.Close()

	client := NewClient(server.URL, "")
	sessions, err := client.ListSessionsForUser(context.Background(), "wechat", "alice")
	if err != nil {
		t.Fatalf("ListSessionsForUser returned error: %v", err)
	}

	if len(sessions) != 2 {
		t.Fatalf("expected 2 owned sessions, got %d: %#v", len(sessions), sessions)
	}
	for _, session := range sessions {
		if session.ID != "ses_mine_1" && session.ID != "ses_mine_2" {
			t.Fatalf("leaked a session this user does not own: %s (%s)", session.ID, session.Title)
		}
	}
}

func TestListSessionsForUserStripsPrefixAndSortsNewestFirst(t *testing.T) {
	now := time.Now().UnixMilli()
	body := "[" + strings.Join([]string{
		sessionJSON("ses_old", "[wechat:alice] oldest", now-9000),
		sessionJSON("ses_new", "[wechat:alice] newest", now-1000),
		sessionJSON("ses_mid", "[wechat:alice] middle", now-5000),
	}, ",") + "]"

	server := sessionListServer(t, body)
	defer server.Close()

	client := NewClient(server.URL, "")
	sessions, err := client.ListSessionsForUser(context.Background(), "wechat", "alice")
	if err != nil {
		t.Fatalf("ListSessionsForUser returned error: %v", err)
	}

	wantOrder := []string{"ses_new", "ses_mid", "ses_old"}
	if len(sessions) != len(wantOrder) {
		t.Fatalf("expected %d sessions, got %d", len(wantOrder), len(sessions))
	}
	for i, want := range wantOrder {
		if sessions[i].ID != want {
			t.Fatalf("position %d: got %s, want %s (order must be newest first)", i, sessions[i].ID, want)
		}
	}

	wantTitles := []string{"newest", "middle", "oldest"}
	for i, want := range wantTitles {
		if sessions[i].Title != want {
			t.Fatalf("position %d: title = %q, want %q (the ownership prefix must be stripped)", i, sessions[i].Title, want)
		}
	}

	// The timestamps must land in this decade. time.Unix on a millisecond value
	// would put them in 1970, which is the defect this assertion pins.
	if sessions[0].Updated.Year() < 2000 {
		t.Fatalf("updated time %v looks like a millisecond value read as seconds", sessions[0].Updated)
	}
}

func TestListSessionsForUserKeepsBarePrefixTitle(t *testing.T) {
	now := time.Now().UnixMilli()
	body := "[" + sessionJSON("ses_bare", "[wechat:alice]", now) + "]"

	server := sessionListServer(t, body)
	defer server.Close()

	client := NewClient(server.URL, "")
	sessions, err := client.ListSessionsForUser(context.Background(), "wechat", "alice")
	if err != nil {
		t.Fatalf("ListSessionsForUser returned error: %v", err)
	}
	if len(sessions) != 1 {
		t.Fatalf("expected 1 session, got %d", len(sessions))
	}
	if sessions[0].Title != "[wechat:alice]" {
		t.Fatalf("a session whose title is only the prefix must keep its raw title, got %q", sessions[0].Title)
	}
}

func TestListSessionsForUserRequiresIdentity(t *testing.T) {
	client := NewClient("http://127.0.0.1:1", "")
	if _, err := client.ListSessionsForUser(context.Background(), "", "alice"); err == nil {
		t.Fatal("expected an error when the channel is empty")
	}
	if _, err := client.ListSessionsForUser(context.Background(), "wechat", ""); err == nil {
		t.Fatal("expected an error when the user ID is empty")
	}
}

func TestResolveUserSessionByIndex(t *testing.T) {
	now := time.Now().UnixMilli()
	body := "[" + strings.Join([]string{
		sessionJSON("ses_a", "[wechat:alice] a", now-3000),
		sessionJSON("ses_b", "[wechat:alice] b", now-2000),
		sessionJSON("ses_c", "[wechat:alice] c", now-1000),
	}, ",") + "]"

	server := sessionListServer(t, body)
	defer server.Close()

	client := NewClient(server.URL, "")
	got, err := client.ResolveUserSession(context.Background(), "wechat", "alice", "2")
	if err != nil {
		t.Fatalf("ResolveUserSession returned error: %v", err)
	}
	if got.ID != "ses_b" {
		t.Fatalf("index 2 resolved to %s, want ses_b (1-based, newest first)", got.ID)
	}
}

func TestResolveUserSessionByUniquePrefix(t *testing.T) {
	now := time.Now().UnixMilli()
	body := "[" + strings.Join([]string{
		sessionJSON("ses_alpha", "[wechat:alice] a", now-2000),
		sessionJSON("ses_beta", "[wechat:alice] b", now-1000),
	}, ",") + "]"

	server := sessionListServer(t, body)
	defer server.Close()

	client := NewClient(server.URL, "")
	got, err := client.ResolveUserSession(context.Background(), "wechat", "alice", "ses_al")
	if err != nil {
		t.Fatalf("ResolveUserSession returned error: %v", err)
	}
	if got.ID != "ses_alpha" {
		t.Fatalf("prefix ses_al resolved to %s, want ses_alpha", got.ID)
	}
}

func TestResolveUserSessionRejectsAmbiguousPrefix(t *testing.T) {
	now := time.Now().UnixMilli()
	body := "[" + strings.Join([]string{
		sessionJSON("ses_dup_1", "[wechat:alice] a", now-2000),
		sessionJSON("ses_dup_2", "[wechat:alice] b", now-1000),
	}, ",") + "]"

	server := sessionListServer(t, body)
	defer server.Close()

	client := NewClient(server.URL, "")
	_, err := client.ResolveUserSession(context.Background(), "wechat", "alice", "ses_dup")
	if err == nil {
		t.Fatal("expected an ambiguous prefix to be rejected rather than silently picking one")
	}
	if errors.Is(err, ErrSessionNotOwned) {
		t.Fatalf("an ambiguous prefix is not a missing session, got %v", err)
	}
	if !strings.Contains(err.Error(), "matches 2 sessions") {
		t.Fatalf("expected the error to name the ambiguity, got %v", err)
	}
}

func TestResolveUserSessionRejectsAnotherUsersSession(t *testing.T) {
	now := time.Now().UnixMilli()
	body := "[" + strings.Join([]string{
		sessionJSON("ses_mine", "[wechat:alice] a", now-2000),
		sessionJSON("ses_theirs", "[wechat:bob] b", now-1000),
	}, ",") + "]"

	server := sessionListServer(t, body)
	defer server.Close()

	client := NewClient(server.URL, "")

	// An exact ID match for a session owned by someone else must be refused.
	_, err := client.ResolveUserSession(context.Background(), "wechat", "alice", "ses_theirs")
	if !errors.Is(err, ErrSessionNotOwned) {
		t.Fatalf("expected ErrSessionNotOwned for another user's session, got %v", err)
	}

	// A prefix that only matches someone else's session must be refused too.
	_, err = client.ResolveUserSession(context.Background(), "wechat", "alice", "ses_th")
	if !errors.Is(err, ErrSessionNotOwned) {
		t.Fatalf("expected ErrSessionNotOwned for another user's session prefix, got %v", err)
	}
}

func TestResolveUserSessionRejectsOutOfRangeIndex(t *testing.T) {
	now := time.Now().UnixMilli()
	body := "[" + sessionJSON("ses_only", "[wechat:alice] a", now) + "]"

	server := sessionListServer(t, body)
	defer server.Close()

	client := NewClient(server.URL, "")

	for _, selector := range []string{"0", "2", "-1"} {
		_, err := client.ResolveUserSession(context.Background(), "wechat", "alice", selector)
		if err == nil {
			t.Fatalf("selector %q should be out of range for a single-session list", selector)
		}
		if errors.Is(err, ErrSessionNotOwned) {
			t.Fatalf("selector %q is out of range, not a missing session: %v", selector, err)
		}
	}
}

func TestResolveUserSessionWithNoOwnedSessions(t *testing.T) {
	now := time.Now().UnixMilli()
	body := "[" + sessionJSON("ses_theirs", "[wechat:bob] b", now) + "]"

	server := sessionListServer(t, body)
	defer server.Close()

	client := NewClient(server.URL, "")
	_, err := client.ResolveUserSession(context.Background(), "wechat", "alice", "1")
	if !errors.Is(err, ErrSessionNotOwned) {
		t.Fatalf("expected ErrSessionNotOwned when the user owns nothing, got %v", err)
	}
}

func TestBindSessionToThreadMovesTheThread(t *testing.T) {
	client := NewClient("http://127.0.0.1:1", "")

	client.BindSessionToThread("thread-1", "ses_first")
	if got, ok := client.GetSessionForThread("thread-1"); !ok || got != "ses_first" {
		t.Fatalf("after the first bind: got (%q, %t), want (ses_first, true)", got, ok)
	}

	client.BindSessionToThread("thread-1", "ses_second")
	if got, ok := client.GetSessionForThread("thread-1"); !ok || got != "ses_second" {
		t.Fatalf("after /switch: got (%q, %t), want (ses_second, true)", got, ok)
	}
}

func TestBindSessionToThreadIgnoresEmptyArguments(t *testing.T) {
	client := NewClient("http://127.0.0.1:1", "")
	client.BindSessionToThread("thread-1", "ses_keep")

	client.BindSessionToThread("", "ses_other")
	client.BindSessionToThread("thread-1", "")
	client.BindSessionToThread("   ", "   ")

	if got, ok := client.GetSessionForThread("thread-1"); !ok || got != "ses_keep" {
		t.Fatalf("an empty argument must not disturb the mapping: got (%q, %t)", got, ok)
	}
}
