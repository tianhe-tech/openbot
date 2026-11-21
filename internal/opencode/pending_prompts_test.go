package opencode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	sdk "github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
)

func TestRefreshPendingPromptsReconcilesExactRequests(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if got := r.URL.Query().Get("directory"); got != `C:\work\project` {
			t.Errorf("pending request lookup used the wrong project: %q", got)
		}
		switch r.URL.Path {
		case "/permission":
			_, _ = w.Write([]byte(`[{"id":"per_current","sessionID":"ses_target","permission":"external_directory","patterns":["C:\\work\\*"]}]`))
		case "/question":
			_, _ = w.Write([]byte(` [
				{"id":"que_current","sessionID":"ses_target","questions":[{"header":"Choose","question":"Current question","options":[{"label":"Yes","description":"Continue"}]}]},
				{"id":"que_unrelated","sessionID":"ses_other","questions":[{"question":"Other question","options":[]}]}
			] `))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := &Client{sdk: sdk.NewClient(option.WithBaseURL(server.URL), option.WithMaxRetries(0)), directory: `C:\work\project`}
	client.StorePendingQuestion(&Question{ID: "que_stale", SessionID: "ses_target"})
	client.StorePendingQuestion(&Question{ID: "que_keep", SessionID: "ses_other"})
	prompts, err := client.RefreshPendingPrompts(context.Background(), []string{"ses_target"})
	if err != nil || len(prompts) != 2 {
		t.Fatalf("refresh failed: prompts=%+v err=%v", prompts, err)
	}
	if _, ok := client.GetPendingQuestion("que_stale"); ok {
		t.Fatal("a different current request validated the old question")
	}
	permission, ok := client.GetPendingQuestion("per_current")
	if !ok || !permission.IsPermission || permission.SessionID != "ses_target" {
		t.Fatalf("permission identity was lost: %+v", permission)
	}
	question, ok := client.GetPendingQuestion("que_current")
	if !ok || len(question.Questions) != 1 || question.Questions[0].Question != "Current question" ||
		len(question.Options) != 1 || question.Options[0] != "Yes" {
		t.Fatalf("question details were lost: %+v", question)
	}
	if _, ok := client.GetPendingQuestion("que_keep"); !ok {
		t.Fatal("another channel's cached request was removed")
	}
	if _, ok := client.GetPendingQuestion("que_unrelated"); ok {
		t.Fatal("unrelated server requests leaked into the scoped refresh")
	}
}

func TestRefreshPendingPromptsRejectsUnverifiedState(t *testing.T) {
	for _, response := range []string{"null", `{}`, `[{"sessionID":"ses_target"}]`, "unsupported", "concurrent update", "[]"} {
		t.Run(response, func(t *testing.T) {
			client := &Client{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/permission" {
					_, _ = w.Write([]byte(`[{"id":"per_current","sessionID":"ses_target","permission":"read"}]`))
					return
				}
				switch response {
				case "unsupported":
					http.NotFound(w, r)
				case "concurrent update":
					client.StorePendingQuestion(&Question{ID: "que_live", SessionID: "ses_target"})
					_, _ = w.Write([]byte(`[]`))
				default:
					_, _ = w.Write([]byte(response))
				}
			}))
			defer server.Close()
			client.sdk = sdk.NewClient(option.WithBaseURL(server.URL), option.WithMaxRetries(0))
			client.StorePendingQuestion(&Question{ID: "que_stale", SessionID: "ses_target"})
			prompts, err := client.RefreshPendingPrompts(context.Background(), []string{"ses_target"})
			if response == "[]" {
				if err != nil || len(prompts) != 1 {
					t.Fatalf("valid empty question list rejected: prompts=%+v err=%v", prompts, err)
				}
				return
			}
			if err == nil || len(prompts) != 0 {
				t.Fatalf("unverified response accepted: prompts=%+v err=%v", prompts, err)
			}
			if _, ok := client.GetPendingQuestion("que_stale"); !ok {
				t.Fatal("failed refresh partially cleared the cache")
			}
			if _, ok := client.GetPendingQuestion("per_current"); ok {
				t.Fatal("failed refresh partially installed a snapshot")
			}
			if response == "concurrent update" {
				if _, ok := client.GetPendingQuestion("que_live"); !ok {
					t.Fatal("server snapshot overwrote a newer SSE request")
				}
			}
		})
	}
}
