package opencode

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sst/opencode-sdk-go"
)

func TestGetSessionDiagnosticsLiveHandlerIsRunning(t *testing.T) {
	client := &Client{}
	sessionID := "ses_status_race"
	client.activeHandlers.Store(sessionID, &StreamingSessionHandler{})

	diagnostics := client.GetSessionDiagnostics(sessionID, "")

	if !diagnostics.Running {
		t.Fatal("expected a session with a live streaming handler to be running")
	}
	if !diagnostics.HasLiveHandler {
		t.Fatal("expected live handler to be reported")
	}
	if status := diagnostics.FormatSessionStatus(); !strings.Contains(status, "处理状态: 处理中") {
		t.Fatalf("expected processing status, got %q", status)
	}
}

func TestGetSessionDiagnosticsUsesServerBusyStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/session/status":
			_, _ = w.Write([]byte(`{"ses_server_busy":{"type":"busy"}}`))
		case "/session", "/session/ses_server_busy/children":
			_, _ = w.Write([]byte(`[]`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client := NewClient(server.URL, "")
	diagnostics := client.GetSessionDiagnostics("ses_server_busy", "")

	if diagnostics.ServerStatus != "busy" {
		t.Fatalf("expected server status busy, got %q", diagnostics.ServerStatus)
	}
	if !diagnostics.Running {
		t.Fatal("expected server busy state to mark the session as running")
	}
	if !diagnostics.ZombieSession {
		t.Fatal("expected busy server session without a local handler to be a zombie")
	}
	if status := diagnostics.FormatSessionStatus(); !strings.Contains(status, "服务器状态: 忙（⚠️僵尸会话）") {
		t.Fatalf("expected server diagnostic in status, got %q", status)
	}

	statuses, err := client.GetSessionStatus(context.Background())
	if err != nil {
		t.Fatalf("GetSessionStatus returned error: %v", err)
	}
	if statuses["ses_server_busy"].Type != "busy" {
		t.Fatalf("expected busy entry, got %#v", statuses)
	}
}

func TestGetLastEventInfoUsesMeaningfulActivity(t *testing.T) {
	activityAt := time.Now().Add(-time.Minute)
	heartbeatAt := time.Now()
	handler := &StreamingSessionHandler{
		lastEventTime:    heartbeatAt,
		lastEventType:    "session.status",
		lastActivityTime: activityAt,
		lastActivityType: "turn.started",
	}

	gotAt, gotType := handler.GetLastEventInfo()

	if !gotAt.Equal(activityAt) || gotType != "turn.started" {
		t.Fatalf("expected meaningful activity, got type=%q at=%v", gotType, gotAt)
	}
}

func TestSilentModelHangCandidateExcludesExistingProgress(t *testing.T) {
	tests := []struct {
		name    string
		handler *StreamingSessionHandler
		want    bool
	}{
		{name: "no progress", handler: &StreamingSessionHandler{}, want: true},
		{name: "content sent", handler: &StreamingSessionHandler{contentSent: true}, want: false},
		{name: "tool observed", handler: &StreamingSessionHandler{toolObserved: true}, want: false},
		{name: "provider retry", handler: &StreamingSessionHandler{retryAttempt: 1}, want: false},
		{name: "waiting for user", handler: &StreamingSessionHandler{pendingQuestionSince: time.Now()}, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.handler.IsSilentModelHangCandidate(); got != test.want {
				t.Fatalf("IsSilentModelHangCandidate() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestSelectFallbackModelSkipsTextDefaultForImagePayload(t *testing.T) {
	client := &Client{
		providerCacheAt: time.Now(),
		providerCache: []Provider{
			{ID: "text-provider", Models: []Model{{ID: "text-model"}}},
			{ID: "vision-provider", Models: []Model{{ID: "vision-model"}}},
		},
		capabilityCache: map[string]modelCapability{
			"text-provider/text-model": {
				ProviderID:      "text-provider",
				ModelID:         "text-model",
				InputModalities: map[string]struct{}{"text": {}},
			},
			"vision-provider/vision-model": {
				ProviderID:      "vision-provider",
				ModelID:         "vision-model",
				InputModalities: map[string]struct{}{"text": {}, "image": {}},
			},
		},
		defaultModelHint: modelFromRef("text-provider", "text-model"),
	}

	fallback, reason := client.selectFallbackModel(context.Background(), modelFromRef("failed", "model"), MessagePayload{
		Attachments: []Attachment{{Mime: "image/jpeg"}},
	})
	if fallback == nil {
		t.Fatal("expected a vision-capable fallback model")
	}
	if got := fallback.ProviderID.Value + "/" + fallback.ModelID.Value; got != "vision-provider/vision-model" {
		t.Fatalf("expected vision fallback, got %s (reason=%s)", got, reason)
	}
}

func TestSelectFallbackModelSkipsEarlierModelsInRetryChain(t *testing.T) {
	client := &Client{
		providerCacheAt: time.Now(),
		providerCache: []Provider{
			{ID: "provider-a", Models: []Model{{ID: "model-a"}, {ID: "model-b"}}},
			{ID: "provider-c", Models: []Model{{ID: "model-c"}}},
		},
		capabilityCache: map[string]modelCapability{
			"provider-a/model-a": {},
			"provider-a/model-b": {},
			"provider-c/model-c": {},
		},
	}

	fallback, reason := client.selectFallbackModel(context.Background(), modelFromRef("provider-a", "model-b"), MessagePayload{
		fallbackAttemptedModels: map[string]struct{}{
			"provider-a/model-a": {},
			"provider-a/model-b": {},
		},
	})
	if fallback == nil {
		t.Fatal("expected an untried fallback model")
	}
	if got := fallback.ProviderID.Value + "/" + fallback.ModelID.Value; got != "provider-c/model-c" {
		t.Fatalf("expected retry chain to skip provider-a models, got %s (reason=%s)", got, reason)
	}
}

func TestSelectFallbackModelReturnsNilAfterAllModelsWereTried(t *testing.T) {
	client := &Client{
		providerCacheAt: time.Now(),
		providerCache:   []Provider{{ID: "provider", Models: []Model{{ID: "model-a"}, {ID: "model-b"}}}},
		capabilityCache: map[string]modelCapability{"provider/model-a": {}, "provider/model-b": {}},
	}

	fallback, _ := client.selectFallbackModel(context.Background(), modelFromRef("provider", "model-b"), MessagePayload{
		fallbackAttemptedModels: map[string]struct{}{
			"provider/model-a": {},
			"provider/model-b": {},
		},
	})
	if fallback != nil {
		t.Fatalf("expected no fallback after all models were tried, got %s/%s", fallback.ProviderID.Value, fallback.ModelID.Value)
	}
}

func TestStreamingHandlerTracksFallbackRetryDispatch(t *testing.T) {
	handler := &StreamingSessionHandler{fallbackRetryDispatched: true}
	if !handler.FallbackRetryDispatched() {
		t.Fatal("expected fallback retry dispatch to be reported")
	}
}

func TestProviderErrorClassification(t *testing.T) {
	breaker := NewCircuitBreaker(DefaultCircuitBreakerConfig())
	for _, message := range []string{"Access denied, please make sure your account is in good standing", "Model not found: squilla/auto.", "Insufficient Balance"} {
		if !breaker.IsProviderSessionError("", message) {
			t.Errorf("expected provider error: %s", message)
		}
	}
	raw := `{"properties":{"error":{"data":{"responseBody":"{\"error\":{\"code\":\"Arrearage\"}}"}}}}`
	if !breaker.IsProviderSessionError(raw, "Account rejected") {
		t.Fatal("expected structured Arrearage error to trigger fallback")
	}
	if breaker.IsProviderSessionError("", "Invalid tool arguments") {
		t.Fatal("tool errors must not trigger model fallback")
	}
	breaker = NewCircuitBreaker(CircuitBreakerConfig{Enabled: false})
	if breaker.IsProviderSessionError(raw, "Model not found") {
		t.Fatal("disabled breaker must not trigger fallback")
	}
}

func TestAttemptFallbackRetryPreservesOverrideWhenFailedModelDiffers(t *testing.T) {
	client := &Client{
		providerCacheAt: time.Now(),
		providerCache: []Provider{
			{ID: "user-provider", Models: []Model{{ID: "user-model"}}},
			{ID: "fallback-provider", Models: []Model{{ID: "fallback-model"}}},
		},
		capabilityCache: map[string]modelCapability{
			"user-provider/user-model":         {},
			"fallback-provider/fallback-model": {},
		},
	}
	oldSessionID := "ses_override_keep"
	override := opencode.SessionPromptParamsModel{
		ProviderID: opencode.F("user-provider"),
		ModelID:    opencode.F("user-model"),
	}
	client.modelOverride.Store(oldSessionID, override)

	client.sessions.Store("thread_keep", oldSessionID)
	client.attemptFallbackRetry(oldSessionID, MessagePayload{
		ThreadID: "thread_keep",
	}, modelFromRef("fallback-provider", "fallback-model"), nil, nil)

	if _, ok := client.getSessionModelOverride(oldSessionID); !ok {
		t.Fatal("expected user override to be preserved when the failed model differs")
	}
	if sessionID, ok := client.GetSessionForThread("thread_keep"); !ok || sessionID != oldSessionID {
		t.Fatal("expected fallback to preserve the original session mapping")
	}
}

func TestAttemptFallbackRetryPreservesOverrideWhenFailedModelIsOverride(t *testing.T) {
	client := &Client{
		providerCacheAt: time.Now(),
		providerCache: []Provider{
			{ID: "user-provider", Models: []Model{{ID: "user-model"}}},
			{ID: "fallback-provider", Models: []Model{{ID: "fallback-model"}}},
		},
		capabilityCache: map[string]modelCapability{
			"user-provider/user-model":         {},
			"fallback-provider/fallback-model": {},
		},
	}
	oldSessionID := "ses_override_drop"
	override := opencode.SessionPromptParamsModel{
		ProviderID: opencode.F("user-provider"),
		ModelID:    opencode.F("user-model"),
	}
	client.modelOverride.Store(oldSessionID, override)

	client.sessions.Store("thread_drop", oldSessionID)
	client.attemptFallbackRetry(oldSessionID, MessagePayload{
		ThreadID: "thread_drop",
	}, modelFromRef("user-provider", "user-model"), nil, nil)

	if _, ok := client.getSessionModelOverride(oldSessionID); !ok {
		t.Fatal("expected user override to survive even when its model failed")
	}
	if _, ok := client.pendingOverride.Load("thread_drop"); ok {
		t.Fatal("expected no migration because the original session is reused")
	}
}

func TestMissingModelFromError(t *testing.T) {
	model := missingModelFromError("Model not found: squilla/auto. (UnknownError)")
	if safeModelID(model) != "squilla/auto" {
		t.Fatalf("expected actual failed model, got %s", safeModelID(model))
	}
	if missingModelFromError("Access denied") != nil {
		t.Fatal("unrelated errors must not change the attributed model")
	}
}

func TestResetForNewPromptPreservesTerminalError(t *testing.T) {
	handler := &StreamingSessionHandler{failedTurn: true, completed: true, fallbackRetryDispatched: true}
	handler.ResetForNewPrompt()
	if !handler.IsCompleted() || !handler.FallbackRetryDispatched() {
		t.Fatal("prompt acceptance must not erase an earlier terminal SSE error")
	}
}

func TestCircuitOpenOverrideNeverSilentlyUsesServerDefault(t *testing.T) {
	cfg := DefaultCircuitBreakerConfig()
	cfg.FailureThreshold = 1
	client := &Client{
		breaker:          NewCircuitBreaker(cfg),
		providerCacheAt:  time.Now(),
		providerCache:    []Provider{{ID: "alternate", Models: []Model{{ID: "model"}}}},
		capabilityCache:  map[string]modelCapability{"alternate/model": {}},
		defaultModelHint: modelFromRef("alternate", "model"),
	}
	client.modelOverride.Store("ses_open_override", *modelFromRef("chosen", "model"))
	client.breaker.RecordFailure("chosen", "model", "Access denied")
	model, _ := client.selectModelOverride(context.Background(), "ses_open_override", MessagePayload{})
	if safeModelID(model) != "alternate/model" {
		t.Fatalf("expected explicit alternate, got %s", safeModelID(model))
	}
	client.breaker.RecordFailure("alternate", "model", "Access denied")
	model, _ = client.selectModelOverride(context.Background(), "ses_open_override", MessagePayload{})
	if safeModelID(model) != "chosen/model" {
		t.Fatalf("without alternatives, retain explicit user model rather than server default, got %s", safeModelID(model))
	}
}

func TestPrepareFallbackSessionAbortsBusyTurnWithoutResettingSession(t *testing.T) {
	var aborted int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/session":
			_, _ = w.Write([]byte(`[]`))
		case "/session/status":
			if atomic.LoadInt32(&aborted) == 0 {
				_, _ = w.Write([]byte(`{"ses_busy_fallback":{"type":"retry"}}`))
			} else {
				_, _ = w.Write([]byte(`{"ses_busy_fallback":{"type":"idle"}}`))
			}
		case "/session/ses_busy_fallback/abort":
			atomic.AddInt32(&aborted, 1)
			_, _ = w.Write([]byte(`true`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := NewClient(server.URL, "")
	client.sessions.Store("thread-busy", "ses_busy_fallback")
	client.modelOverride.Store("ses_busy_fallback", *modelFromRef("chosen", "model"))
	if err := client.prepareFallbackSession(context.Background(), "ses_busy_fallback"); err != nil {
		t.Fatalf("prepare retry: %v", err)
	}
	if atomic.LoadInt32(&aborted) != 1 {
		t.Fatal("expected one abort of the previous upstream retry loop")
	}
	if model, ok := client.getSessionModelOverride("ses_busy_fallback"); !ok || safeModelID(&model) != "chosen/model" {
		t.Fatal("aborting the failed turn must preserve user model")
	}
	if sessionID, ok := client.GetSessionForThread("thread-busy"); !ok || sessionID != "ses_busy_fallback" {
		t.Fatal("aborting the failed turn must preserve session mapping")
	}
}

func TestStreamingFallbackChainReusesSession(t *testing.T) {
	for _, withOverride := range []bool{false, true} {
		name := "server-default"
		if withOverride {
			name = "session-override"
		}
		t.Run(name, func(t *testing.T) {
			const sessionID = "ses_fallback_chain"
			var client *Client
			var created int32
			requests := make(chan string, 4)
			finished := make(chan struct{}, 1)
			terminalReady := make(chan (<-chan struct{}), 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/session/status":
					_, _ = w.Write([]byte(`{"ses_fallback_chain":{"type":"idle"}}`))
				case "/session":
					if r.Method == http.MethodPost {
						atomic.AddInt32(&created, 1)
					}
					_, _ = w.Write([]byte(`[]`))
				case "/session/" + sessionID:
					_, _ = w.Write([]byte(`{"id":"ses_fallback_chain"}`))
				case "/session/" + sessionID + "/prompt_async":
					var body struct {
						Model struct {
							ProviderID string `json:"providerID"`
							ModelID    string `json:"modelID"`
						} `json:"model"`
					}
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Errorf("decode prompt: %v", err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					requests <- body.Model.ProviderID + "/" + body.Model.ModelID
					message := "Model not found: squilla/auto."
					if body.Model.ModelID == "model-a" {
						message = "Access denied, please make sure your account is in good standing"
					} else if body.Model.ModelID == "model-b" {
						message = "No available channel"
					}
					raw, _ := json.Marshal(map[string]interface{}{
						"type": "session.error",
						"properties": map[string]interface{}{
							"sessionID": sessionID,
							"error":     map[string]interface{}{"name": "APIError", "data": map[string]interface{}{"message": message}},
						},
					})
					var event opencode.EventListResponse
					if err := json.Unmarshal(raw, &event); err != nil {
						t.Errorf("decode event: %v", err)
						return
					}
					active, ok := client.activeHandlers.Load(sessionID)
					if !ok {
						t.Error("no active handler for retry session")
						return
					}
					if body.Model.ModelID == "model-b" {
						terminalReady <- active.(*StreamingSessionHandler).payload.fallbackReady
					}
					if err := active.(*StreamingSessionHandler).HandleEvent(r.Context(), &event); err != nil {
						t.Errorf("handle event: %v", err)
					}
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			client = NewClient(server.URL, "")
			client.timeout = 30 * time.Second
			client.providerCacheAt = time.Now()
			client.providerCache = []Provider{{ID: "alternate", Models: []Model{{ID: "model-a"}, {ID: "model-b"}}}}
			client.capabilityCache = map[string]modelCapability{"alternate/model-a": {}, "alternate/model-b": {}}
			client.defaultModelHint = modelFromRef("alternate", "model-a")
			client.sessions.Store("thread-chain", sessionID)
			if withOverride {
				client.modelOverride.Store(sessionID, *modelFromRef("squilla", "auto"))
			}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			_, err := client.SendMessageStreamingWithEvents(ctx, MessagePayload{
				SessionID: sessionID, ThreadID: "thread-chain", Content: "hello", Streaming: true,
			}, func(string) error { return nil }, func(event StreamEvent) error {
				if event.Kind == StreamEventError {
					finished <- struct{}{}
				}
				return nil
			})
			if err != nil {
				t.Fatalf("streaming request: %v", err)
			}
			select {
			case <-finished:
			case <-ctx.Done():
				t.Fatal("fallback chain did not terminate")
			}
			select {
			case ready := <-terminalReady:
				select {
				case <-ready:
				case <-ctx.Done():
					t.Fatal("terminal streaming call did not exit")
				}
			case <-ctx.Done():
				t.Fatal("missing terminal streaming call")
			}
			initial := "/"
			if withOverride {
				initial = "squilla/auto"
			}
			for _, want := range []string{initial, "alternate/model-a", "alternate/model-b"} {
				select {
				case got := <-requests:
					if got != want {
						t.Errorf("model = %s, want %s", got, want)
					}
				default:
					t.Fatalf("missing prompt for %s", want)
				}
			}
			select {
			case extra := <-requests:
				t.Fatalf("unexpected repeated model: %s", extra)
			default:
			}
			if atomic.LoadInt32(&created) != 0 {
				t.Fatal("model failure must not create a new session")
			}
			if _, ok := client.activeHandlers.Load(sessionID); ok {
				t.Fatal("failed turn left an active handler after completion")
			}
			if _, ok := client.sessionHandlers.Load(sessionID); ok {
				t.Fatal("prompt acceptance re-registered an already failed handler")
			}
			if got, ok := client.GetSessionForThread("thread-chain"); !ok || got != sessionID {
				t.Fatal("session mapping was lost")
			}
			if withOverride {
				if model, ok := client.getSessionModelOverride(sessionID); !ok || safeModelID(&model) != "squilla/auto" {
					t.Fatal("user model preference was lost")
				}
			}
			if client.breaker.StateString("squilla", "auto") == "disabled" || len(client.breaker.Snapshot()) != 3 {
				t.Fatal("expected failures attributed to the three actual models")
			}
		})
	}
}
