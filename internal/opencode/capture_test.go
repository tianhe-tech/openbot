package opencode

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
)

type captureRecorderStub struct {
	ConversationRecorder
	input    TurnInput
	err      error
	existing bool
}

func (recorder *captureRecorderStub) BeginTurn(_ context.Context, input TurnInput) (TurnRef, error) {
	recorder.input = input
	return TurnRef{ID: "turn", Existing: recorder.existing}, recorder.err
}

func TestCaptureFailurePreventsDispatch(t *testing.T) {
	recorder := &captureRecorderStub{err: errors.New("storage full")}
	client := &Client{sdk: sdk.NewClient(), conversationRecorder: recorder}
	_, err := client.SendMessage(context.Background(), MessagePayload{Channel: "test", UserID: "owner", Content: " original input "})
	if !errors.Is(err, ErrCapture) {
		t.Fatalf("expected capture failure before network access: %v", err)
	}
	if recorder.input.Content != " original input " || recorder.input.DeliveryID == "" {
		t.Fatalf("original input not retained: %+v", recorder.input)
	}
}

func TestCaptureDeliveryReplayAndInternalExclusion(t *testing.T) {
	recorder := &captureRecorderStub{existing: true}
	client := &Client{conversationRecorder: recorder}
	payload := MessagePayload{Channel: "test", UserID: "owner", DeliveryID: "delivery", Content: "request"}
	if err := client.beginCapture(context.Background(), &payload); !errors.Is(err, ErrCapturedDelivery) {
		t.Fatalf("duplicate could dispatch: %v", err)
	}
	recorder.existing = false
	if err := client.beginCapture(context.Background(), &payload); err != nil {
		t.Fatal(err)
	}
	recorder.err = errors.New("must not be called")
	if err := client.beginCapture(context.Background(), &payload); err != nil {
		t.Fatalf("internal retry began a second turn: %v", err)
	}
	internal := MessagePayload{Channel: "skillgen", Content: "draft"}
	if err := client.beginCapture(context.Background(), &internal); err != nil {
		t.Fatal(err)
	}
}

func TestCaptureInternalBootstrapPromptIsNotArchived(t *testing.T) {
	recorder := &captureRecorderStub{err: errors.New("must not be called")}
	client := &Client{conversationRecorder: recorder}
	payload := MessagePayload{Channel: "dingtalk", UserID: "owner", Content: "Initialize session", Internal: true}
	if err := client.beginCapture(context.Background(), &payload); err != nil {
		t.Fatalf("internal bootstrap prompt failed capture: %v", err)
	}
	if payload.capture != nil {
		t.Fatal("internal bootstrap prompt became an archived turn")
	}
	if payload.DeliveryID != "" {
		t.Fatalf("internal bootstrap prompt minted a delivery ID: %q", payload.DeliveryID)
	}
}

func TestCaptureRetryOfDeliveryContinuesExistingTurn(t *testing.T) {
	recorder := &captureRecorderStub{existing: true}
	client := &Client{conversationRecorder: recorder}
	payload := MessagePayload{Channel: "dingtalk", UserID: "owner", DeliveryID: "delivery", Content: "request", RetryOfDelivery: true}
	if err := client.beginCapture(context.Background(), &payload); err != nil {
		t.Fatalf("deliberate retry was rejected as a replay: %v", err)
	}
	if payload.capture == nil || payload.capture.ID != "turn" {
		t.Fatalf("retry did not continue the existing turn: %+v", payload.capture)
	}
	if payload.captureState == nil {
		t.Fatal("retry did not reset the capture turn state")
	}
	// The same delivery without the retry marker must still fail closed.
	replay := MessagePayload{Channel: "dingtalk", UserID: "owner", DeliveryID: "delivery", Content: "request"}
	if err := client.beginCapture(context.Background(), &replay); !errors.Is(err, ErrCapturedDelivery) {
		t.Fatalf("unmarked replay dispatched: %v", err)
	}
}

func TestCapturePromptDoesNotRetryUncertainHTTPFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if body["messageID"] != "msg_stable" {
			t.Errorf("missing correlation ID: %v", body["messageID"])
		}
		response.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := &Client{sdk: sdk.NewClient(option.WithBaseURL(server.URL)), timeout: time.Second, retryConfig: RetryConfig{MaxRetries: 5}}
	if _, err := client.sendPromptWithRetry(context.Background(), "ses_capture", nil, nil, "msg_stable"); err == nil {
		t.Fatal("503 reported success")
	}
	if calls.Load() != 1 {
		t.Fatalf("uncertain request replayed %d times", calls.Load())
	}
}

func TestCaptureDistinctDeliveriesDoNotUseContentCache(t *testing.T) {
	first := MessagePayload{Channel: "test", Content: "same", capture: &TurnRef{ID: "first"}}
	second := first
	second.capture = &TurnRef{ID: "second"}
	if generateRequestHash(first) == generateRequestHash(second) {
		t.Fatal("distinct deliveries collapsed by content")
	}
}

func TestCaptureShutdownStopsNewProducers(t *testing.T) {
	client := &Client{conversationRecorder: &captureRecorderStub{}}
	if err := client.enterCaptureProducer(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.StopCapture(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("active producer not tracked: %v", err)
	}
	if err := client.enterCaptureProducer(); !errors.Is(err, ErrCapture) {
		t.Fatalf("shutdown accepted new producer: %v", err)
	}
	client.leaveCaptureProducer()
	if err := client.StopCapture(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCaptureExcludesInjectedMediaPrompt(t *testing.T) {
	recorder := &captureRecorderStub{}
	client := &Client{conversationRecorder: recorder}
	original := " inspect this file "
	payload := MessagePayload{Channel: "test", UserID: "owner", Content: "media metadata plus prompt", OriginalContent: &original}
	if err := client.beginCapture(context.Background(), &payload); err != nil {
		t.Fatal(err)
	}
	if recorder.input.Content != original {
		t.Fatalf("captured injected prompt: %q", recorder.input.Content)
	}
}

func TestCaptureMessageIdentifiersMatchUpstreamLayout(t *testing.T) {
	pattern := regexp.MustCompile(`^msg_[0-9a-f]{12}[0-9A-Za-z]{14}$`)
	previous := ""
	for index := 0; index < 50; index++ {
		identity := strings.TrimPrefix(newCaptureAttemptID(), "attempt_")
		if !pattern.MatchString(identity) || identity <= previous {
			t.Fatalf("invalid/nonmonotonic upstream identity: %s after %s", identity, previous)
		}
		stamp, err := strconv.ParseUint(identity[4:16], 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		now := uint64(time.Now().UnixMilli()) & 0xfffffffff
		if stamp>>12 > now+1000 || stamp>>12+1000 < now {
			t.Fatal("message ID timestamp does not match upstream millisecond layout")
		}
		previous = identity
	}
}

func TestCapturePartialCoalescesByIntervalAndBytes(t *testing.T) {
	state := &captureTurnState{}
	if state.shouldFlushPartial("msg", "text") {
		t.Fatal("unbound attempt flushed a snapshot")
	}
	state.bind(AttemptRef{ID: "attempt"})
	if !state.shouldFlushPartial("msg", "first") {
		t.Fatal("first snapshot suppressed")
	}
	if state.shouldFlushPartial("msg", "first plus a little") {
		t.Fatal("snapshot flushed before interval or byte threshold")
	}
	// Crossing the byte threshold flushes immediately, without waiting.
	grown := "first" + strings.Repeat("x", capturePartialBytes)
	if !state.shouldFlushPartial("msg", grown) {
		t.Fatal("byte threshold did not flush")
	}
	if state.shouldFlushPartial("msg", grown+"y") {
		t.Fatal("snapshot flushed twice for the same size")
	}
	// A new upstream message resets the coalescing window.
	if !state.shouldFlushPartial("msg_other", "second message") {
		t.Fatal("new message suppressed")
	}
	if state.shouldFlushPartial("msg_other", "") {
		t.Fatal("empty snapshot flushed")
	}
}

func TestCapturePartialFlushesAfterInterval(t *testing.T) {
	state := &captureTurnState{}
	state.bind(AttemptRef{ID: "attempt"})
	if !state.shouldFlushPartial("msg", "first") {
		t.Fatal("first snapshot suppressed")
	}
	state.mu.Lock()
	state.lastPartialAt = time.Now().Add(-capturePartialInterval - time.Second)
	state.mu.Unlock()
	if !state.shouldFlushPartial("msg", "first and more") {
		t.Fatal("interval did not flush")
	}
}

func TestCaptureToolEventParsesCompletedTool(t *testing.T) {
	recorder := &captureToolRecorder{}
	client := &Client{conversationRecorder: recorder}
	state := &captureTurnState{}
	state.bind(AttemptRef{ID: "attempt", TurnID: "turn", SessionID: "session"})
	payload := MessagePayload{captureState: state}

	raw := `{"type":"message.part.updated","properties":{"part":{"id":"part-1","callID":"call-1","messageID":"msg-1","tool":"bash","type":"tool","state":{"status":"completed","title":"run tests","output":"ok","time":{"start":1700000000000,"end":1700000001000},"attachments":[{"id":"file-1","mime":"image/png","filename":"shot.png","url":"data:image/png;base64,AAAA"}]}}}}`
	event := &sdk.EventListResponse{}
	if err := json.Unmarshal([]byte(raw), event); err != nil {
		t.Fatal(err)
	}
	client.captureToolEvent(payload, event)
	recorder.wait(t)

	if len(recorder.tools) != 1 {
		t.Fatalf("tool calls=%d", len(recorder.tools))
	}
	tool := recorder.tools[0]
	if tool.CallID != "call-1" || tool.Tool != "bash" || tool.State != "completed" || tool.Output != "ok" {
		t.Fatalf("tool metadata=%+v", tool)
	}
	if tool.StartedAt != 1700000000000 || tool.EndedAt != 1700000001000 {
		t.Fatalf("tool timing=%+v", tool)
	}
	if len(recorder.attachments) != 1 || recorder.attachments[0].Role != "output" || recorder.attachments[0].Filename != "shot.png" {
		t.Fatalf("attachments=%+v", recorder.attachments)
	}
}

func TestCaptureToolEventIgnoresNonToolParts(t *testing.T) {
	recorder := &captureToolRecorder{}
	client := &Client{conversationRecorder: recorder}
	state := &captureTurnState{}
	state.bind(AttemptRef{ID: "attempt"})
	payload := MessagePayload{captureState: state}

	for _, raw := range []string{
		`{"type":"message.part.updated","properties":{"part":{"id":"p","type":"text","text":"hello"}}}`,
		`{"type":"message.part.updated","properties":{"part":{"id":"p","type":"tool","tool":"bash"}}}`,
		`not json`,
	} {
		event := &sdk.EventListResponse{}
		_ = json.Unmarshal([]byte(raw), event)
		client.captureToolEvent(payload, event)
	}
	recorder.settle()
	if len(recorder.tools) != 0 {
		t.Fatalf("non-tool parts recorded: %+v", recorder.tools)
	}
}

func TestCaptureInputAttachmentsRecordsMetadata(t *testing.T) {
	recorder := &captureToolRecorder{}
	client := &Client{conversationRecorder: recorder}
	state := &captureTurnState{}
	state.bind(AttemptRef{ID: "attempt"})
	payload := MessagePayload{
		captureState: state,
		Attachments: []Attachment{
			{Mime: "image/png", Filename: "a.png", URL: "data:image/png;base64,AAAA"},
			{Mime: "image/jpeg", Filename: "b.jpg", URL: "https://example.test/b.jpg"},
		},
	}
	client.captureInputAttachments(payload)
	recorder.wait(t)
	if len(recorder.attachments) != 2 {
		t.Fatalf("attachments=%d", len(recorder.attachments))
	}
	for index, attachment := range recorder.attachments {
		if attachment.Role != "input" || attachment.PartID != "input-"+strconv.Itoa(index) {
			t.Fatalf("attachment identity=%+v", attachment)
		}
	}
}

// captureToolRecorder captures the asynchronous tool/attachment writes.
type captureToolRecorder struct {
	ConversationRecorder
	mu          sync.Mutex
	tools       []ToolCallInput
	attachments []AttachmentInput
}

func (recorder *captureToolRecorder) SaveToolCall(_ context.Context, input ToolCallInput) error {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.tools = append(recorder.tools, input)
	return nil
}

func (recorder *captureToolRecorder) SaveAttachment(_ context.Context, input AttachmentInput) error {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.attachments = append(recorder.attachments, input)
	return nil
}

// wait blocks until the asynchronous capture goroutines have finished. The
// writes are dispatched in the background so the SSE loop is never blocked.
func (recorder *captureToolRecorder) wait(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		recorder.mu.Lock()
		settled := len(recorder.tools)+len(recorder.attachments) > 0
		recorder.mu.Unlock()
		if settled {
			// Give the goroutine a moment to finish any follow-up writes.
			time.Sleep(50 * time.Millisecond)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// settle gives any in-flight asynchronous write a chance to land, for cases
// where the expected outcome is that nothing is recorded.
func (recorder *captureToolRecorder) settle() {
	time.Sleep(100 * time.Millisecond)
}
