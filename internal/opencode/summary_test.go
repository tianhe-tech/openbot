package opencode

import (
	"encoding/json"
	"testing"

	"github.com/sst/opencode-sdk-go"
)

// decodeMessages builds the SDK's message list from raw JSON. The union field
// that AsUnion() reads is only populated by the SDK's own unmarshaller, so a
// struct literal would produce a Message whose union is nil and every type
// assertion in extractSummaryFromSession would fail for the wrong reason.
func decodeMessages(t *testing.T, raw string) []opencode.SessionMessagesResponse {
	t.Helper()
	var messages []opencode.SessionMessagesResponse
	if err := json.Unmarshal([]byte(raw), &messages); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	return messages
}

func TestExtractSummaryFromSessionReadsUserMessageBody(t *testing.T) {
	messages := decodeMessages(t, `[
		{"info":{"id":"m1","role":"user","sessionID":"s1","time":{"created":1}},
		 "parts":[{"type":"text","text":"hello"}]},
		{"info":{"id":"m2","role":"user","sessionID":"s1","time":{"created":2},
		         "summary":{"body":"the compacted summary","title":"t","diffs":[]}},
		 "parts":[]}
	]`)

	if got := extractSummaryFromSession(messages); got != "the compacted summary" {
		t.Fatalf("summary = %q, want %q", got, "the compacted summary")
	}
}

func TestExtractSummaryFromSessionPrefersTheLatestCompaction(t *testing.T) {
	// A session can be compacted more than once. The last summary is the one
	// that describes the conversation as it stands now; returning the first
	// would hand the caller a stale description of a much shorter history.
	messages := decodeMessages(t, `[
		{"info":{"id":"m1","role":"user","sessionID":"s1","time":{"created":1},
		         "summary":{"body":"first summary","diffs":[]}},"parts":[]},
		{"info":{"id":"m2","role":"user","sessionID":"s1","time":{"created":2},
		         "summary":{"body":"second summary","diffs":[]}},"parts":[]}
	]`)

	if got := extractSummaryFromSession(messages); got != "second summary" {
		t.Fatalf("summary = %q, want %q", got, "second summary")
	}
}

func TestExtractSummaryFromSessionIgnoresAssistantAndEmptyBodies(t *testing.T) {
	messages := decodeMessages(t, `[
		{"info":{"id":"m1","role":"assistant","sessionID":"s1","time":{"created":1,"completed":2},
		         "parentID":"m0","modelID":"x","providerID":"y","cost":0,"mode":"build"},
		 "parts":[{"type":"text","text":"assistant text"}]},
		{"info":{"id":"m2","role":"user","sessionID":"s1","time":{"created":3},
		         "summary":{"body":"   ","diffs":[]}},"parts":[]}
	]`)

	if got := extractSummaryFromSession(messages); got != "" {
		t.Fatalf("summary = %q, want empty", got)
	}
}

func TestExtractSummaryFromSessionOnEmptyList(t *testing.T) {
	if got := extractSummaryFromSession(nil); got != "" {
		t.Fatalf("summary = %q, want empty", got)
	}
}
