package opencode

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
)

func TestResolveSessionOnlyReplacesNotFound(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusNotFound, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var creates atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/session":
					creates.Add(1)
					_, _ = w.Write([]byte(`{"id":"ses_replacement"}`))
				case r.URL.Path == "/session/ses_existing":
					w.WriteHeader(status)
					_, _ = w.Write([]byte(`{"id":"ses_existing"}`))
				default:
					_, _ = w.Write([]byte(`{}`))
				}
			}))
			defer server.Close()
			client := &Client{sdk: sdk.NewClient(option.WithBaseURL(server.URL), option.WithMaxRetries(0))}
			client.sessions.Store("chat", "ses_existing")
			committed := ""
			id, err := client.ResolveOrCreateSession(context.Background(), MessagePayload{
				ThreadID: "chat", SessionID: "ses_existing",
				OnSessionResolved: func(_ context.Context, id string) error { committed = id; return nil },
			})
			if status == http.StatusServiceUnavailable {
				if err == nil || creates.Load() != 0 || committed != "" {
					t.Fatalf("transient error created or committed a session: id=%s creates=%d err=%v", id, creates.Load(), err)
				}
				if cached, _ := client.sessions.Load("chat"); cached != "ses_existing" {
					t.Fatal("transient error removed the original binding")
				}
			} else {
				want, count := "ses_existing", int32(0)
				if status == http.StatusNotFound {
					want, count = "ses_replacement", 1
				}
				if err != nil || id != want || committed != want || creates.Load() != count {
					t.Fatalf("id=%s committed=%s creates=%d err=%v", id, committed, creates.Load(), err)
				}
			}
		})
	}
}

func TestSendMessageCommitFailureRetainsSessionWithoutPrompt(t *testing.T) {
	var creates, prompts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/session" && r.Method == http.MethodPost:
			creates.Add(1)
			_, _ = w.Write([]byte(`{"id":"ses_pending"}`))
		case r.URL.Path == "/session" && r.Method == http.MethodGet:
			_, _ = w.Write([]byte(`[]`))
		case strings.HasSuffix(r.URL.Path, "/message"):
			prompts.Add(1)
			_, _ = w.Write([]byte(`{}`))
		case r.URL.Path == "/session/ses_pending":
			_, _ = w.Write([]byte(`{"id":"ses_pending"}`))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer server.Close()
	client := &Client{
		sdk:     sdk.NewClient(option.WithBaseURL(server.URL), option.WithMaxRetries(0)),
		timeout: time.Second,
	}
	diskErr := errors.New("disk full")
	for i := 0; i < 2; i++ {
		_, err := client.SendMessage(context.Background(), MessagePayload{
			Channel: "wechat", UserID: "alice", ThreadID: "chat", Content: "hello",
			OnSessionResolved: func(context.Context, string) error { return diskErr },
		})
		if !errors.Is(err, diskErr) {
			t.Fatalf("expected commit failure, got %v", err)
		}
	}
	if creates.Load() != 1 || prompts.Load() != 0 {
		t.Fatalf("creates=%d prompts=%d; binding must survive without dispatch", creates.Load(), prompts.Load())
	}
}

func TestCronSessionNotSelectableOrRecoveredAsChat(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
			{"id":"ses_cron","title":"[wechat:alice] cron-task-one","time":{"created":2000,"updated":3000}},
			{"id":"ses_chat","title":"[wechat:alice] ordinary","time":{"created":1000,"updated":2000}}
		]`))
	}))
	defer server.Close()
	client := &Client{sdk: sdk.NewClient(option.WithBaseURL(server.URL))}
	for _, selector := range []string{"1", "ses_cron", "ses_cro"} {
		if _, err := client.ResolveUserSession(context.Background(), "wechat", "alice", selector); err == nil {
			t.Fatalf("cron session selectable by %q", selector)
		}
	}
	if id, ok := client.FindLatestSessionForUser(context.Background(), "wechat", "alice"); !ok || id != "ses_chat" {
		t.Fatalf("latest chat recovery selected %s, ok=%t", id, ok)
	}
	sessions, err := client.ListSessionsForUser(context.Background(), "wechat", "alice")
	if err != nil || len(sessions) != 2 {
		t.Fatalf("cron session should remain visible in /sessions: %v %v", sessions, err)
	}
}

func TestStreamingResolvesBeforeRegisteringHandler(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var creates, prompts, commits atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.URL.Path == "/session" && r.Method == http.MethodGet:
					_, _ = w.Write([]byte(`[]`))
				case r.URL.Path == "/session" && r.Method == http.MethodPost:
					creates.Add(1)
					_, _ = w.Write([]byte(`{"id":"ses_replacement"}`))
				case r.URL.Path == "/session/ses_existing":
					w.WriteHeader(status)
					_, _ = w.Write([]byte(`{}`))
				case r.URL.Path == "/session/ses_replacement/message" && r.Method == http.MethodPost:
					prompts.Add(1)
					if commits.Load() != 1 {
						t.Error("streaming prompt preceded session commit")
					}
					_, _ = w.Write([]byte(`{"info":{"id":"msg_done","role":"assistant"},"parts":[{"type":"text","text":"answer"}]}`))
				default:
					_, _ = w.Write([]byte(`{}`))
				}
			}))
			defer server.Close()
			client := &Client{
				sdk:     sdk.NewClient(option.WithBaseURL(server.URL), option.WithMaxRetries(0)),
				timeout: time.Second,
			}
			notifiedID := ""
			response, err := client.SendMessageStreaming(context.Background(), MessagePayload{
				ThreadID: "chat", SessionID: "ses_existing", Content: "hello",
				OnSessionResolved: func(context.Context, string) error { commits.Add(1); return nil },
			}, func(chunk string) error {
				if strings.HasPrefix(chunk, "ses_") {
					notifiedID = chunk
				}
				return nil
			})
			if status == http.StatusNotFound {
				if err != nil || response.SessionID != "ses_replacement" || notifiedID != "ses_replacement" || prompts.Load() != 1 || creates.Load() != 1 {
					t.Fatalf("streaming handler/prompt session diverged: response=%+v notified=%s prompts=%d creates=%d err=%v",
						response, notifiedID, prompts.Load(), creates.Load(), err)
				}
			} else {
				if err == nil || notifiedID != "" || commits.Load() != 0 || prompts.Load() != 0 || creates.Load() != 0 {
					t.Fatalf("uncertain session resolution started streaming: response=%+v err=%v", response, err)
				}
				if _, active := client.activeHandlers.Load("ses_existing"); active {
					t.Fatal("resolution failure left a live handler")
				}
			}
		})
	}
}
