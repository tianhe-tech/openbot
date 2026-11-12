package opencode

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// previewUpstream is a fake opencode instance that answers both the session
// listing and per-session history, and counts how many histories were fetched.
//
// The count matters: SessionMessagesParams carries no limit, so every preview
// costs one full-history fetch. The caching and limit tests exist to pin that
// cost down, because a regression there turns /sessions into a stampede.
type previewUpstream struct {
	t        *testing.T
	sessions string
	messages map[string]string

	mu      sync.Mutex
	fetches map[string]int
}

func newPreviewUpstream(t *testing.T, sessions string, messages map[string]string) *previewUpstream {
	t.Helper()
	return &previewUpstream{
		t:        t,
		sessions: sessions,
		messages: messages,
		fetches:  map[string]int{},
	}
}

func (u *previewUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.URL.Path == "/session" {
		_, _ = w.Write([]byte(u.sessions))
		return
	}

	const prefix = "/session/"
	const suffix = "/message"
	if strings.HasPrefix(r.URL.Path, prefix) && strings.HasSuffix(r.URL.Path, suffix) {
		id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, prefix), suffix)
		u.mu.Lock()
		u.fetches[id]++
		u.mu.Unlock()

		body, ok := u.messages[id]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(body))
		return
	}

	u.t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
	http.NotFound(w, r)
}

func (u *previewUpstream) fetchCount(sessionID string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.fetches[sessionID]
}

func (u *previewUpstream) totalFetches() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	total := 0
	for _, count := range u.fetches {
		total += count
	}
	return total
}

// userTextMessageJSON builds one element of GET /session/{id}/message: a user
// message carrying a single text part. synthetic marks the part as upstream
// scaffolding rather than something the user typed.
func userTextMessageJSON(sessionID, text string, synthetic bool) string {
	return fmt.Sprintf(
		`{"info":{"id":"msg_1","role":"user","sessionID":%q,"time":{"created":1700000000000}},`+
			`"parts":[{"id":"prt_1","messageID":"msg_1","sessionID":%q,"type":"text","text":%q,"synthetic":%t}]}`,
		sessionID, sessionID, text, synthetic)
}

// userSummaryMessageJSON builds a user message with no text parts but a
// compaction summary body, which is the only other prose the upstream keeps.
func userSummaryMessageJSON(sessionID, body string) string {
	return fmt.Sprintf(
		`{"info":{"id":"msg_1","role":"user","sessionID":%q,"time":{"created":1700000000000},`+
			`"summary":{"diffs":[],"body":%q}},"parts":[]}`,
		sessionID, body)
}

// messageArray wraps message elements into the array the endpoint returns.
func messageArray(elements ...string) string {
	return "[" + strings.Join(elements, ",") + "]"
}

// previewSessions builds the slice FillSessionPreviews operates on. The preview
// derivation does not depend on how the slice was produced, so these tests skip
// the listing round-trip and construct it directly.
func previewSessions(ids ...string) []UserSession {
	sessions := make([]UserSession, 0, len(ids))
	for _, id := range ids {
		sessions = append(sessions, UserSession{ID: id, Title: "opaque-thread-id"})
	}
	return sessions
}

func TestFillSessionPreviewsUsesFirstUserText(t *testing.T) {
	upstream := newPreviewUpstream(t, "[]", map[string]string{
		"ses_1": messageArray(userTextMessageJSON("ses_1", "帮我排查一下构建失败的原因", false)),
	})
	server := httptest.NewServer(upstream)
	defer server.Close()

	sessions := previewSessions("ses_1")
	NewClient(server.URL, "").FillSessionPreviews(context.Background(), sessions, 10)

	if sessions[0].Preview != "帮我排查一下构建失败的原因" {
		t.Fatalf("preview = %q, want the first user message text", sessions[0].Preview)
	}
}

func TestFillSessionPreviewsSkipsSyntheticParts(t *testing.T) {
	upstream := newPreviewUpstream(t, "[]", map[string]string{
		"ses_1": messageArray(
			userTextMessageJSON("ses_1", "gateway scaffolding the user never typed", true),
			userTextMessageJSON("ses_1", "真正的问题在这里", false),
		),
	})
	server := httptest.NewServer(upstream)
	defer server.Close()

	sessions := previewSessions("ses_1")
	NewClient(server.URL, "").FillSessionPreviews(context.Background(), sessions, 10)

	if sessions[0].Preview != "真正的问题在这里" {
		t.Fatalf("preview = %q, want the first non-synthetic text", sessions[0].Preview)
	}
}

func TestFillSessionPreviewsTruncates(t *testing.T) {
	long := strings.Repeat("长", sessionPreviewMaxRunes+20)
	upstream := newPreviewUpstream(t, "[]", map[string]string{
		"ses_1": messageArray(userTextMessageJSON("ses_1", long, false)),
	})
	server := httptest.NewServer(upstream)
	defer server.Close()

	sessions := previewSessions("ses_1")
	NewClient(server.URL, "").FillSessionPreviews(context.Background(), sessions, 10)

	want := strings.Repeat("长", sessionPreviewMaxRunes) + "..."
	if sessions[0].Preview != want {
		t.Fatalf("preview = %q, want %q (truncation must be rune-safe)", sessions[0].Preview, want)
	}
}

func TestFillSessionPreviewsFallsBackToSummary(t *testing.T) {
	upstream := newPreviewUpstream(t, "[]", map[string]string{
		"ses_1": messageArray(userSummaryMessageJSON("ses_1", "重构了会话切换逻辑")),
	})
	server := httptest.NewServer(upstream)
	defer server.Close()

	sessions := previewSessions("ses_1")
	NewClient(server.URL, "").FillSessionPreviews(context.Background(), sessions, 10)

	if sessions[0].Preview != "重构了会话切换逻辑" {
		t.Fatalf("preview = %q, want the compaction summary body", sessions[0].Preview)
	}
}

func TestFillSessionPreviewsLeavesEmptyWhenNoMessages(t *testing.T) {
	upstream := newPreviewUpstream(t, "[]", map[string]string{"ses_1": "[]"})
	server := httptest.NewServer(upstream)
	defer server.Close()

	sessions := previewSessions("ses_1")
	NewClient(server.URL, "").FillSessionPreviews(context.Background(), sessions, 10)

	if sessions[0].Preview != "" {
		t.Fatalf("preview = %q, want empty for a session with no history", sessions[0].Preview)
	}
}

func TestFillSessionPreviewsStripsHandoffPreamble(t *testing.T) {
	// Mirrors BuildHandoffPreamble: the scaffolding ends with the
	// 【当前用户消息】 marker, and the "---" line above it is only decoration.
	raw := "【会话接续·自动恢复】\n上一会话因服务端调度异常中断，以下是压缩摘要：\n\n摘要正文\n\n---\n\n【当前用户消息】\n继续把 /switch 做完"
	upstream := newPreviewUpstream(t, "[]", map[string]string{
		"ses_1": messageArray(userTextMessageJSON("ses_1", raw, false)),
	})
	server := httptest.NewServer(upstream)
	defer server.Close()

	sessions := previewSessions("ses_1")
	NewClient(server.URL, "").FillSessionPreviews(context.Background(), sessions, 10)

	if sessions[0].Preview != "继续把 /switch 做完" {
		t.Fatalf("preview = %q, want the user text after the handoff preamble", sessions[0].Preview)
	}
}

func TestFillSessionPreviewsStripsNestedHandoffPreamble(t *testing.T) {
	// Repeated recoveries nest preambles. Only the innermost marker holds the
	// text the user actually typed; taking the first one would show the
	// previous preamble verbatim.
	raw := "【会话接续·自动恢复】\n外层摘要\n\n---\n\n【当前用户消息】\n" +
		"【会话接续·自动恢复】\n内层摘要\n\n---\n\n【当前用户消息】\n真正要问的问题"
	upstream := newPreviewUpstream(t, "[]", map[string]string{
		"ses_1": messageArray(userTextMessageJSON("ses_1", raw, false)),
	})
	server := httptest.NewServer(upstream)
	defer server.Close()

	sessions := previewSessions("ses_1")
	NewClient(server.URL, "").FillSessionPreviews(context.Background(), sessions, 10)

	if sessions[0].Preview != "真正要问的问题" {
		t.Fatalf("preview = %q, want the innermost user text", sessions[0].Preview)
	}
}

func TestFillSessionPreviewsStripsRecallBlock(t *testing.T) {
	raw := "【历史工作记忆】\n- 之前修过时间戳\n- 之前加过测试\n---\n上次那个 bug 还在吗"
	upstream := newPreviewUpstream(t, "[]", map[string]string{
		"ses_1": messageArray(userTextMessageJSON("ses_1", raw, false)),
	})
	server := httptest.NewServer(upstream)
	defer server.Close()

	sessions := previewSessions("ses_1")
	NewClient(server.URL, "").FillSessionPreviews(context.Background(), sessions, 10)

	if sessions[0].Preview != "上次那个 bug 还在吗" {
		t.Fatalf("preview = %q, want the user text after the recall block", sessions[0].Preview)
	}
}

func TestFillSessionPreviewsCollapsesWhitespace(t *testing.T) {
	upstream := newPreviewUpstream(t, "[]", map[string]string{
		"ses_1": messageArray(userTextMessageJSON("ses_1", "第一行\n\n第二行\t  第三行", false)),
	})
	server := httptest.NewServer(upstream)
	defer server.Close()

	sessions := previewSessions("ses_1")
	NewClient(server.URL, "").FillSessionPreviews(context.Background(), sessions, 10)

	if sessions[0].Preview != "第一行 第二行 第三行" {
		t.Fatalf("preview = %q, want newlines and tabs collapsed to single spaces", sessions[0].Preview)
	}
}

func TestFillSessionPreviewsRespectsLimit(t *testing.T) {
	upstream := newPreviewUpstream(t, "[]", map[string]string{
		"ses_1": messageArray(userTextMessageJSON("ses_1", "第一个会话", false)),
		"ses_2": messageArray(userTextMessageJSON("ses_2", "第二个会话", false)),
		"ses_3": messageArray(userTextMessageJSON("ses_3", "第三个会话", false)),
	})
	server := httptest.NewServer(upstream)
	defer server.Close()

	sessions := previewSessions("ses_1", "ses_2", "ses_3")
	NewClient(server.URL, "").FillSessionPreviews(context.Background(), sessions, 2)

	if sessions[0].Preview == "" || sessions[1].Preview == "" {
		t.Fatalf("the first two rows must be previewed, got %q and %q", sessions[0].Preview, sessions[1].Preview)
	}
	if sessions[2].Preview != "" {
		t.Fatalf("row beyond the limit was previewed: %q", sessions[2].Preview)
	}
	if got := upstream.fetchCount("ses_3"); got != 0 {
		t.Fatalf("fetched history for a row beyond the limit %d times", got)
	}
}

func TestFillSessionPreviewsCachesResults(t *testing.T) {
	upstream := newPreviewUpstream(t, "[]", map[string]string{
		"ses_1": messageArray(userTextMessageJSON("ses_1", "缓存过的会话", false)),
	})
	server := httptest.NewServer(upstream)
	defer server.Close()

	client := NewClient(server.URL, "")

	first := previewSessions("ses_1")
	client.FillSessionPreviews(context.Background(), first, 10)
	if first[0].Preview != "缓存过的会话" {
		t.Fatalf("first call preview = %q", first[0].Preview)
	}

	second := previewSessions("ses_1")
	client.FillSessionPreviews(context.Background(), second, 10)
	if second[0].Preview != "缓存过的会话" {
		t.Fatalf("second call preview = %q, want the cached value", second[0].Preview)
	}

	if got := upstream.fetchCount("ses_1"); got != 1 {
		t.Fatalf("history fetched %d times, want 1 (the preview must be cached)", got)
	}
}

func TestFillSessionPreviewsSurvivesFetchFailure(t *testing.T) {
	// No entry for ses_1, so the fake upstream answers 404.
	upstream := newPreviewUpstream(t, "[]", map[string]string{})
	server := httptest.NewServer(upstream)
	defer server.Close()

	sessions := previewSessions("ses_1")
	NewClient(server.URL, "").FillSessionPreviews(context.Background(), sessions, 10)

	if sessions[0].Preview != "" {
		t.Fatalf("preview = %q, want empty when the history cannot be read", sessions[0].Preview)
	}
	if sessions[0].ID != "ses_1" {
		t.Fatalf("the session must survive a failed preview, got %#v", sessions[0])
	}
}

func TestFillSessionPreviewsHandlesEmptyInput(t *testing.T) {
	upstream := newPreviewUpstream(t, "[]", map[string]string{})
	server := httptest.NewServer(upstream)
	defer server.Close()

	client := NewClient(server.URL, "")
	// Neither call may panic or issue a request.
	client.FillSessionPreviews(context.Background(), nil, 10)
	client.FillSessionPreviews(context.Background(), previewSessions(), 10)

	if got := upstream.totalFetches(); got != 0 {
		t.Fatalf("issued %d history fetches for an empty listing", got)
	}
}

func TestSessionPreviewTextRejectsBlankInput(t *testing.T) {
	// The last two cases are pure scaffolding with no user text behind it: a
	// preview must be empty rather than echo the gateway's own boilerplate.
	for _, raw := range []string{
		"",
		"   ",
		"\n\n\t",
		"【会话接续·自动恢复】\n---\n   ",
		"【会话接续·自动恢复】\n只有摘要，没有用户消息",
		"【历史工作记忆】\n- 只有记忆块",
	} {
		if got := sessionPreviewText(raw); got != "" {
			t.Fatalf("sessionPreviewText(%q) = %q, want empty", raw, got)
		}
	}
}

func TestFillSessionPreviewsUsesPerFetchTimeout(t *testing.T) {
	// A session whose history never arrives must degrade to "no preview"
	// instead of stalling the whole listing.
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/session" {
			_, _ = w.Write([]byte("[]"))
			return
		}
		<-release
	}))
	defer server.Close()
	defer close(release)

	client := NewClient(server.URL, "")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	sessions := previewSessions("ses_slow")
	done := make(chan struct{})
	go func() {
		client.FillSessionPreviews(ctx, sessions, 10)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(sessionPreviewTimeout + 2*time.Second):
		t.Fatal("FillSessionPreviews did not return after the caller's context expired")
	}

	if sessions[0].Preview != "" {
		t.Fatalf("preview = %q, want empty for an unreadable history", sessions[0].Preview)
	}
}
