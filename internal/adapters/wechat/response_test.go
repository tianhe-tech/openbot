package wechat

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/user/opencode-gateway/internal/adapters/base"
	"github.com/user/opencode-gateway/internal/opencode"
)

func TestUnstreamedResponseDoesNotCompleteBackgroundRetry(t *testing.T) {
	tests := []struct {
		name     string
		response opencode.Response
		media    bool
		want     string
	}{
		{name: "background retry", response: opencode.Response{Deferred: true}},
		{name: "partial reply with background retry", response: opencode.Response{Deferred: true, Reply: "partial answer"}, want: "partial answer"},
		{name: "empty terminal turn", want: "✅ 处理完成"},
		{name: "media turn", media: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sent []queuedOutboundText
			queue, err := newOutboundTextQueue(filepath.Join(t.TempDir(), "response.db"), time.Second, 1600,
				func(item *queuedOutboundText) error { sent = append(sent, *item); return nil })
			if err != nil {
				t.Fatal(err)
			}
			defer queue.Stop()
			queue.setMinGap(0)
			handler := &Handler{outbound: queue}
			if err := handler.enqueueUnstreamedResponse("alice", "ses_original", "token", tt.response, tt.media); err != nil {
				t.Fatal(err)
			}
			did, err := queue.dispatchOne()
			if err != nil {
				t.Fatal(err)
			}
			if tt.want == "" {
				if did || len(sent) != 0 {
					t.Fatalf("unexpected completion output while another owner handles delivery: %+v", sent)
				}
				return
			}
			if !did || len(sent) != 1 || sent[0].Content != tt.want || sent[0].UserID != "alice" || sent[0].ContextToken != "token" {
				t.Fatalf("wrong delivered content or recipient: %+v", sent)
			}
		})
	}
}

func TestParkedPromptRefreshDisplaysOnlyActualPendingRequest(t *testing.T) {
	for _, response := range []string{
		`[{"id":"que_new","sessionID":"ses_old","questions":[{"question":"Current question B","options":[{"label":"Continue"}]}]}]`,
		`[]`,
		`null`,
		`transferred`,
	} {
		t.Run(response, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/session" {
					owner := "alice"
					if response == "transferred" {
						owner = "bob"
					}
					_, _ = w.Write([]byte(`[{"id":"ses_old","title":"[wechat:` + owner + `] main"}]`))
					return
				}
				if r.URL.Path == "/question" {
					if response == "transferred" {
						t.Error("attempted to refresh another recipient's current prompts")
					}
					_, _ = w.Write([]byte(response))
					return
				}
				_, _ = w.Write([]byte(`[]`))
			}))
			defer server.Close()
			var sent []queuedOutboundText
			queue, err := newOutboundTextQueue(filepath.Join(t.TempDir(), "outbound.db"), time.Second, 1600,
				func(item *queuedOutboundText) error { sent = append(sent, *item); return nil })
			if err != nil {
				t.Fatal(err)
			}
			defer queue.Stop()
			queue.setMinGap(0)
			if err := queue.EnqueueText("alice", "ses_old", "token", "question", "Old question A", true); err != nil {
				t.Fatal(err)
			}
			if err := queue.classifyStartup(); err != nil {
				t.Fatal(err)
			}
			client := opencode.NewClient(server.URL, "")
			client.StorePendingQuestion(&opencode.Question{ID: "que_old", SessionID: "ses_old"})
			handler := &Handler{outbound: queue, client: client, adapter: base.NewBidirectionalAdapter("wechat", nil)}
			handler.adapter.MapUserToSession("alice", "ses_current")
			count, err := handler.refreshParkedPrompts(context.Background(), "alice", "")
			if response == "null" {
				if err == nil || count != 0 {
					t.Fatalf("unverifiable state accepted: count=%d err=%v", count, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if did, err := queue.dispatchOne(); err != nil || did != (count > 0) {
				t.Fatalf("unexpected dispatch did=%v count=%d err=%v", did, count, err)
			}
			if strings.Contains(response, "que_new") {
				if len(sent) != 1 || !strings.Contains(sent[0].Content, "Current question B") ||
					!strings.Contains(sent[0].Content, "/switch ses_old") || strings.Contains(sent[0].Content, "Old question A") {
					t.Fatalf("old wording replayed or new prompt not shown: %+v", sent)
				}
				if _, ok := client.GetPendingQuestion("que_old"); ok {
					t.Fatal("quick replies would still answer the obsolete request")
				}
			} else if len(sent) != 0 {
				t.Fatalf("obsolete prompt replayed: %+v", sent)
			}
			if current, _ := handler.adapter.GetSessionForUser("alice"); current != "ses_current" {
				t.Fatal("prompt recovery replaced the current chat")
			}
			recovery, err := queue.RecoverParked("alice", "")
			if err != nil || recovery.Recovered != 0 || recovery.SkippedPrompts != 1 {
				t.Fatalf("old question became explicitly recoverable: %+v err=%v", recovery, err)
			}
		})
	}
}
