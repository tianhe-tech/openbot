package opencode

import (
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
