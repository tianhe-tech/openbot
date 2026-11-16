package opencode

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
)

func TestSilentHangBreakerDistinguishesStoppedFromBackgroundRetry(t *testing.T) {
	for _, abortOK := range []bool{true, false} {
		t.Run(map[bool]string{true: "stopped", false: "abort-failed"}[abortOK], func(t *testing.T) {
			var aborts, prompts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case strings.HasSuffix(r.URL.Path, "/message"):
					_, _ = w.Write([]byte(`[{"info":{"id":"msg_user","role":"user"},"parts":[]}]`))
				case strings.HasSuffix(r.URL.Path, "/abort"):
					aborts.Add(1)
					if !abortOK {
						w.WriteHeader(http.StatusBadRequest)
					}
					_, _ = w.Write([]byte(`true`))
				case strings.HasSuffix(r.URL.Path, "/prompt_async"):
					prompts.Add(1)
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
				}
			}))
			defer server.Close()
			client := &Client{sdk: sdk.NewClient(option.WithBaseURL(server.URL), option.WithMaxRetries(0))}
			client.recentHandoffs.Store("chat", []time.Time{time.Now()})
			handler := &StreamingSessionHandler{}
			var notices []string
			state := client.recoverSilentModelHang(MessagePayload{ThreadID: "chat"}, "ses_silent_hang", handler, func(chunk string) error {
				notices = append(notices, chunk)
				return nil
			}, nil)
			if abortOK {
				if state != silentHangStopped || !handler.IsCompleted() || len(notices) != 1 || notices[0] != silentHangStoppedNotice {
					t.Fatalf("breaker did not report explicit stopped result: state=%d notices=%v", state, notices)
				}
			} else if state != silentHangNotRecovered || handler.IsCompleted() || len(notices) != 0 {
				t.Fatalf("failed abort incorrectly relinquished live turn: state=%d notices=%v", state, notices)
			}
			if aborts.Load() != 1 || prompts.Load() != 0 {
				t.Fatalf("stopped recovery scheduled work: aborts=%d prompts=%d", aborts.Load(), prompts.Load())
			}
		})
	}
}

func TestFinalizeHandoffIsNotDeferred(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/message") {
			_, _ = w.Write([]byte(`[{"info":{"id":"msg_user","role":"user"},"parts":[]}]`))
		} else {
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer server.Close()
	client := &Client{sdk: sdk.NewClient(option.WithBaseURL(server.URL))}
	notice := ""
	response := client.finalizeAsyncStreamingResponse(MessagePayload{ThreadID: "chat"}, "ses_handoff_only", nil,
		func(chunk string) error { notice += chunk; return nil }, Response{})
	if response.Deferred || response.Reply == "" || notice != response.Reply || !strings.Contains(notice, "重新发送") {
		t.Fatalf("handoff falsely reported a background retry: %+v notice=%q", response, notice)
	}
}
