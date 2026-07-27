package opencode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
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
