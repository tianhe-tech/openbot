package opencode

import (
	"strconv"
	"strings"
	"sync"
	"testing"

	sdk "github.com/sst/opencode-sdk-go"
)

// newTestRetryHandler builds a minimal StreamingSessionHandler whose callback
// records every chunk it receives, so tests can assert what the user would see.
func newTestRetryHandler() (*StreamingSessionHandler, *[]string, *sync.Mutex) {
	var mu sync.Mutex
	chunks := make([]string, 0, 8)
	cb := func(chunk string) error {
		mu.Lock()
		chunks = append(chunks, chunk)
		mu.Unlock()
		return nil
	}
	h := &StreamingSessionHandler{
		sessionID:       "ses_testretry000000000000000000",
		callback:        cb,
		activeToolParts: make(map[string]bool),
	}
	return h, &chunks, &mu
}

func retryPartJSON(attempt int, message string, statusCode int) string {
	// Mirrors the opencode message.part.updated payload for a retry part.
	return `{"properties":{"part":{"id":"prt_x","type":"retry","attempt":` +
		strconv.Itoa(attempt) + `,"error":{"name":"APIError","data":{"message":"` +
		message + `","statusCode":` + strconv.Itoa(statusCode) + `,"isRetryable":true}}}}}`
}

func TestHandleRetryPartBelowThresholdDoesNotComplete(t *testing.T) {
	if maxRetryAttempts < 2 {
		t.Skipf("maxRetryAttempts too small for this test: %d", maxRetryAttempts)
	}
	h, _, _ := newTestRetryHandler()

	h.handleRetryPart(retryPartJSON(1, "No available channel for model GLM-5.1", 503))

	if h.IsCompleted() {
		t.Fatalf("handler should not be completed after a single low-attempt retry")
	}
	rs := h.GetRetryState()
	if rs.Attempt != 1 {
		t.Fatalf("RetryState.Attempt = %d, want 1", rs.Attempt)
	}
	if !strings.Contains(rs.Message, "No available channel") {
		t.Fatalf("RetryState.Message = %q, want it to contain the upstream message", rs.Message)
	}
	if rs.StatusCode != 503 {
		t.Fatalf("RetryState.StatusCode = %d, want 503", rs.StatusCode)
	}
}

func TestHandleRetryPartAtThresholdSurfacesErrorAndCompletes(t *testing.T) {
	h, chunks, mu := newTestRetryHandler()

	h.handleRetryPart(retryPartJSON(maxRetryAttempts, "No available channel for model GLM-5.1", 503))

	if !h.IsCompleted() {
		t.Fatalf("handler should be completed once attempt reaches maxRetryAttempts (%d)", maxRetryAttempts)
	}

	mu.Lock()
	joined := strings.Join(*chunks, "\n")
	mu.Unlock()

	if !strings.Contains(joined, "中止") {
		t.Fatalf("expected an abort error to be surfaced to the user, got chunks: %q", joined)
	}
	if !strings.Contains(joined, "No available channel") {
		t.Fatalf("expected the upstream error detail to be surfaced, got chunks: %q", joined)
	}
}

func TestHandleRetryPartSurfacesErrorOnlyOnce(t *testing.T) {
	h, chunks, mu := newTestRetryHandler()

	// Two retry parts at/above threshold should only produce one abort notice.
	h.handleRetryPart(retryPartJSON(maxRetryAttempts, "No available channel", 503))
	h.handleRetryPart(retryPartJSON(maxRetryAttempts+1, "No available channel", 503))

	mu.Lock()
	defer mu.Unlock()
	aborts := 0
	for _, c := range *chunks {
		if strings.Contains(c, "中止") {
			aborts++
		}
	}
	if aborts != 1 {
		t.Fatalf("expected exactly one abort notice, got %d (chunks=%v)", aborts, *chunks)
	}
}

func TestRetryPartSummary(t *testing.T) {
	parts := []sdk.Part{
		{Type: sdk.PartTypeText, Text: "thinking..."},
	}
	if _, ok := retryPartSummary(parts); ok {
		t.Fatalf("retryPartSummary should report ok=false when no retry part is present")
	}

	// A standalone retry part as opencode would serialize it inside a message.
	retryPartRaw := []byte(`{"id":"prt_x","type":"retry","attempt":7,"messageID":"msg_x","sessionID":"ses_x","time":{},"error":{"name":"APIError","data":{"isRetryable":true,"message":"rate limited","statusCode":429}}}`)
	var rp sdk.Part
	if err := rp.UnmarshalJSON(retryPartRaw); err != nil {
		t.Fatalf("failed to unmarshal retry part: %v", err)
	}
	summary, ok := retryPartSummary([]sdk.Part{rp})
	if !ok {
		t.Fatalf("retryPartSummary should report ok=true for a retry part")
	}
	if !strings.Contains(summary, "7") {
		t.Fatalf("summary should mention the attempt count, got %q", summary)
	}
	if !strings.Contains(summary, "rate limited") {
		t.Fatalf("summary should include the upstream message, got %q", summary)
	}
}
