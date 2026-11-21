package opencode

import (
	"context"
	"encoding/json"
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

func TestForegroundTerminalOutcomes(t *testing.T) {
	for _, mode := range []string{"success", "error after partial", "permission", "question", "retry", "send failure", "cancel", "timeout", "callback error", "abort failure", "abort false", "replaced handler"} {
		t.Run(mode, func(t *testing.T) {
			const id = "ses_foreground"
			var client *Client
			var prompts, aborts atomic.Int32
			timeout := 2 * time.Second
			if mode == "timeout" {
				timeout = 200 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			emit := func(raw string) {
				var event sdk.EventListResponse
				if err := json.Unmarshal([]byte(raw), &event); err != nil {
					t.Error(err)
					return
				}
				if handler, ok := client.sessionHandlers.Load(id); ok {
					if err := handler.(EventHandler)(context.Background(), &event); err != nil {
						t.Error(err)
					}
				} else {
					t.Error("event arrived without a registered handler")
				}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.URL.Path == "/session" && r.Method == http.MethodPost:
					_, _ = w.Write([]byte(`{"id":"` + id + `","title":"[skillgen:alice] draft"}`))
				case r.URL.Path == "/session":
					_, _ = w.Write([]byte(`[]`))
				case strings.HasSuffix(r.URL.Path, "/abort"):
					aborts.Add(1)
					if mode == "abort failure" {
						w.WriteHeader(http.StatusBadRequest)
					}
					_, _ = w.Write([]byte(map[bool]string{true: "false", false: "true"}[mode == "abort false"]))
				case strings.HasSuffix(r.URL.Path, "/prompt_async"):
					prompts.Add(1)
					if mode == "send failure" {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					switch mode {
					case "permission", "question":
						kind, request := "permission.asked", `"id":"per_asked","permission":"external_directory"`
						if mode == "question" {
							kind, request = "question.asked", `"id":"que_asked","questions":[{"question":"Continue?","options":[]}]`
						}
						emit(`{"type":"` + kind + `","properties":{"sessionID":"` + id + `",` + request + `}}`)
					case "retry":
						raw := retryPartJSON(maxRetryAttempts, "No available channel", 503)
						emit(`{"type":"message.part.updated",` + strings.TrimPrefix(raw, "{"))
					case "cancel":
						cancel()
					case "timeout":
					default:
						emit(`{"type":"message.part.delta","properties":{"sessionID":"` + id + `","messageID":"msg_assistant","partID":"part_text","field":"text","delta":"body"}}`)
						if mode == "success" || mode == "callback error" {
							emit(`{"type":"session.idle","properties":{"sessionID":"` + id + `"}}`)
						} else {
							emit(`{"type":"session.error","properties":{"sessionID":"` + id + `","error":{"name":"APIError","data":{"message":"No available channel"}}}}`)
							if mode == "replaced handler" {
								client.RegisterSessionHandler(id, func(context.Context, *sdk.EventListResponse) error { return nil })
							}
						}
					}
					w.WriteHeader(http.StatusNoContent)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client = &Client{
				sdk:      sdk.NewClient(option.WithBaseURL(server.URL), option.WithMaxRetries(0)),
				endpoint: server.URL, httpClient: server.Client(),
			}
			var text strings.Builder
			response, err := client.SendMessageStreamingWithEvents(ctx, MessagePayload{
				Channel: "skillgen", UserID: "alice", ThreadID: "skillgen-draft-test",
				Content: "draft", Model: "provider/model", ForegroundOnly: true, Internal: true,
			}, func(string) error { return nil }, func(event StreamEvent) error {
				if mode == "callback error" && event.Kind == StreamEventTextDelta {
					return errors.New("test callback failed")
				}
				if event.Kind == StreamEventTextDelta {
					text.WriteString(event.Content)
				}
				return nil
			})
			if mode == "success" {
				if err != nil || response.Reply != "body" || text.String() != "body" || response.Deferred || aborts.Load() != 0 {
					t.Fatalf("fast foreground completion lost content: response=%+v text=%q aborts=%d err=%v", response, text.String(), aborts.Load(), err)
				}
			} else {
				if err == nil || response.Deferred {
					t.Fatalf("failed foreground attempt looked successful: response=%+v err=%v", response, err)
				}
				if mode == "timeout" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("timeout returned the wrong terminal cause: %v", err)
				}
				var uncertain *RemoteStopUnconfirmedError
				wantUncertain := mode == "abort failure" || mode == "abort false" || mode == "replaced handler"
				if errors.As(err, &uncertain) != wantUncertain {
					t.Fatalf("wrong stop certainty: error=%v", err)
				}
				if mode == "permission" || mode == "question" {
					if !errors.Is(err, ErrForegroundInteractionRequired) {
						t.Fatalf("interaction requirement not propagated: %v", err)
					}
				}
				wantAborts := int32(1)
				if mode == "replaced handler" {
					wantAborts = 0
				}
				if aborts.Load() != wantAborts {
					t.Fatalf("wrong abort count: got=%d want=%d", aborts.Load(), wantAborts)
				}
			}
			if prompts.Load() != 1 {
				t.Fatalf("foreground mode dispatched a background fallback: prompts=%d", prompts.Load())
			}
			if _, present := client.sessionHandlers.Load(id); present != (mode == "replaced handler") {
				t.Fatalf("handler cleanup removed a replacement or retained the old handler: present=%v", present)
			}
			if _, active := client.activeHandlers.Load(id); active {
				t.Fatal("foreground attempt left its active handler behind")
			}
		})
	}
}

func TestForegroundSilenceAbortsInsteadOfHandingOff(t *testing.T) {
	original := streamIdleTimeoutNoContent
	streamIdleTimeoutNoContent = 20 * time.Millisecond
	defer func() { streamIdleTimeoutNoContent = original }()
	var aborts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(r.URL.Path, "/prompt_async"):
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/abort"):
			aborts.Add(1)
			_, _ = w.Write([]byte(`true`))
		case r.URL.Path == "/session" && r.Method == http.MethodPost:
			_, _ = w.Write([]byte(`{"id":"ses_silent_foreground"}`))
		default:
			_, _ = w.Write([]byte(`[]`))
		}
	}))
	defer server.Close()
	client := &Client{sdk: sdk.NewClient(option.WithBaseURL(server.URL)), endpoint: server.URL, httpClient: server.Client()}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	response, err := client.SendMessageStreamingWithEvents(ctx, MessagePayload{
		Channel: "skillgen", UserID: "alice", ThreadID: "skillgen-draft-silence",
		Content: "draft", Model: "provider/model", ForegroundOnly: true, Internal: true,
	}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "stalled") || response.Deferred || aborts.Load() != 1 {
		t.Fatalf("silent foreground generation did not stop: response=%+v aborts=%d err=%v", response, aborts.Load(), err)
	}
}
