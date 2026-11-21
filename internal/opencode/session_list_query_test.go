package opencode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// queryRecorder captures the query string of every GET /session request so a
// test can assert on what the client actually sent upstream.
type queryRecorder struct {
	mu      sync.Mutex
	queries []string
}

func (r *queryRecorder) record(raw string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queries = append(r.queries, raw)
}

func (r *queryRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.queries...)
}

// listing returns the query of the listing call.
//
// NewClient starts a background health check that also hits GET /session, so a
// test cannot assume the listing is the only request the server sees. The
// listing call is identified by its limit parameter, which the health probe
// never sends.
func (r *queryRecorder) listing(t *testing.T) url.Values {
	t.Helper()
	for _, raw := range r.all() {
		values, err := url.ParseQuery(raw)
		if err != nil {
			t.Fatalf("could not parse the recorded query %q: %v", raw, err)
		}
		if values.Get("limit") != "" {
			return values
		}
	}
	t.Fatalf("no listing request was recorded; queries were %#v", r.all())
	return nil
}

// recordingSessionServer serves GET /session with a fixed body and records the
// query string of each request.
func recordingSessionServer(t *testing.T, body string) (*httptest.Server, *queryRecorder) {
	t.Helper()
	rec := &queryRecorder{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/session" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		rec.record(r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	return server, rec
}

// TestListSessionsRequestsFullPage pins the fix for the 100-item default.
//
// GET /session returns only the 100 most recently updated sessions unless limit
// is set, and SessionListParams has no field for it. On a deployment with
// thousands of sessions that window is entirely cron and internal traffic, so
// every user's /sessions listing came back empty. The client must therefore
// inject limit itself.
func TestListSessionsRequestsFullPage(t *testing.T) {
	server, rec := recordingSessionServer(t, "[]")
	defer server.Close()

	client := NewClient(server.URL, "")
	if _, err := client.ListSessions(context.Background()); err != nil {
		t.Fatalf("ListSessions returned error: %v", err)
	}

	queries := rec.all()
	values := rec.listing(t)
	got := values.Get("limit")
	if got == "" {
		t.Fatalf("limit was not sent upstream; queries were %#v", queries)
	}
	n, convErr := strconv.Atoi(got)
	if convErr != nil {
		t.Fatalf("limit %q is not an integer: %v", got, convErr)
	}
	if n < 1000 {
		t.Fatalf("limit %d is too small to escape the upstream 100-item default", n)
	}
}

// TestListSessionsScopesToConfiguredDirectory pins the second half of the fix:
// the listing must be narrowed to the workspace the gateway writes to, not the
// whole project the serve process happens to sit in.
func TestListSessionsScopesToConfiguredDirectory(t *testing.T) {
	server, rec := recordingSessionServer(t, "[]")
	defer server.Close()

	client := NewClient(server.URL, "", WithDirectory("/srv/openbot"))

	if _, err := client.ListSessions(context.Background()); err != nil {
		t.Fatalf("ListSessions returned error: %v", err)
	}

	queries := rec.all()
	values := rec.listing(t)
	if got := values.Get("directory"); got != "/srv/openbot" {
		t.Fatalf("directory = %q, want /srv/openbot (queries were %#v)", got, queries)
	}
}

// TestListSessionsOmitsUnsetDirectory guards the "." default. Sending
// directory="." would resolve against the serve process's working directory and
// silently drop sessions created in subdirectories, which is narrower than the
// unfiltered listing it replaces.
func TestListSessionsOmitsUnsetDirectory(t *testing.T) {
	server, rec := recordingSessionServer(t, "[]")
	defer server.Close()

	client := NewClient(server.URL, "")
	if client.Directory() != "." {
		t.Fatalf("precondition failed: default directory is %q, expected \".\"", client.Directory())
	}

	if _, err := client.ListSessions(context.Background()); err != nil {
		t.Fatalf("ListSessions returned error: %v", err)
	}

	queries := rec.all()
	values := rec.listing(t)
	if _, present := values["directory"]; present {
		t.Fatalf("directory must not be sent when it is the unset default; queries were %#v", queries)
	}
}

// TestListSessionsForUserSendsSearchPrefilter verifies the ownership prefix is
// pushed upstream as a search term, so the scan is not limited to the newest
// page of sessions.
func TestListSessionsForUserSendsSearchPrefilter(t *testing.T) {
	now := time.Now().UnixMilli()
	body := "[" + sessionJSON("ses_mine", "[wechat:alice] chat", now) + "]"
	server, rec := recordingSessionServer(t, body)
	defer server.Close()

	client := NewClient(server.URL, "")
	if _, err := client.ListSessionsForUser(context.Background(), "wechat", "alice"); err != nil {
		t.Fatalf("ListSessionsForUser returned error: %v", err)
	}

	queries := rec.all()
	values := rec.listing(t)
	if got := values.Get("search"); got != "[wechat:alice]" {
		t.Fatalf("search = %q, want [wechat:alice] (queries were %#v)", got, queries)
	}
	if values.Get("limit") == "" {
		t.Fatalf("limit was not sent alongside search; queries were %#v", queries)
	}
}

// TestListSessionsForUserStillEnforcesPrefix is the security regression test.
//
// The upstream search parameter is a substring match, so it can return a title
// that merely contains the prefix. The client-side prefix check must still run,
// otherwise a user could see a session whose title embeds their prefix.
func TestListSessionsForUserStillEnforcesPrefix(t *testing.T) {
	now := time.Now().UnixMilli()
	body := "[" + strings.Join([]string{
		sessionJSON("ses_mine", "[wechat:alice] chat", now-1000),
		// Contains the prefix but does not start with it: a substring search
		// would return this, and it must be dropped.
		sessionJSON("ses_embedded", "notes about [wechat:alice] chat", now),
		sessionJSON("ses_suffix", "x[wechat:alice] chat", now-500),
	}, ",") + "]"
	server, _ := recordingSessionServer(t, body)
	defer server.Close()

	client := NewClient(server.URL, "")
	sessions, err := client.ListSessionsForUser(context.Background(), "wechat", "alice")
	if err != nil {
		t.Fatalf("ListSessionsForUser returned error: %v", err)
	}

	if len(sessions) != 1 {
		t.Fatalf("expected only the prefix-owned session, got %d: %#v", len(sessions), sessions)
	}
	if sessions[0].ID != "ses_mine" {
		t.Fatalf("leaked a session whose title only contains the prefix: %s (%q)",
			sessions[0].ID, sessions[0].Title)
	}
}

// TestFindLatestSessionForUserSendsSearchPrefilter keeps the restart-recovery
// path consistent with the listing path: it must also escape the 100-item
// window, otherwise a gateway restart silently loses the user's session.
func TestFindLatestSessionForUserSendsSearchPrefilter(t *testing.T) {
	now := time.Now().UnixMilli()
	body := "[" + sessionJSON("ses_recovered", "[wechat:alice] chat", now) + "]"
	server, rec := recordingSessionServer(t, body)
	defer server.Close()

	client := NewClient(server.URL, "")
	id, found := client.FindLatestSessionForUser(context.Background(), "wechat", "alice")
	if !found {
		t.Fatal("expected to recover the user's session")
	}
	if id != "ses_recovered" {
		t.Fatalf("recovered %s, want ses_recovered", id)
	}

	queries := rec.all()
	values := rec.listing(t)
	if got := values.Get("search"); got != "[wechat:alice]" {
		t.Fatalf("search = %q, want [wechat:alice] (queries were %#v)", got, queries)
	}
}
