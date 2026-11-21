package opencode

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sdk "github.com/sst/opencode-sdk-go"
)

var ErrCapture = errors.New("opencode: durable conversation capture failed")
var ErrCapturedDelivery = errors.New("opencode: delivery already archived; refusing automatic replay")

var captureIDClock atomic.Uint64

func newCaptureAttemptID() string {
	var stamp uint64
	for {
		previous := captureIDClock.Load()
		stamp = uint64(time.Now().UnixMilli())<<12 | 1
		if stamp <= previous {
			stamp = previous + 1
		}
		if captureIDClock.CompareAndSwap(previous, stamp) {
			break
		}
	}
	var randomBytes [14]byte
	_, _ = rand.Read(randomBytes[:])
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	for index, value := range randomBytes {
		randomBytes[index] = alphabet[int(value)%len(alphabet)]
	}
	return fmt.Sprintf("attempt_msg_%012x%s", stamp&0xffffffffffff, randomBytes[:])
}

type TurnInput struct {
	Adapter, UserID, ThreadID, DeliveryID, Content, WorkDir string
}

type TurnRef struct {
	ID       string
	Existing bool
}

type AttemptRef struct {
	ID, TurnID, SessionID, UserMessageID string
}

type AttemptInput struct {
	Turn          TurnRef
	ID, SessionID string
	// Retry marks a deliberate re-dispatch of an already archived delivery.
	// It permits a new attempt on a turn that already reached a terminal
	// state, which is what token-overflow recovery and explicit retries do.
	Retry bool
}

type MessageInput struct {
	Attempt         AttemptRef
	ID, Text, State string
	CreatedAt       int64
}

// PartialInput is a rolling snapshot of assistant text that is still being
// produced. Snapshots are coalesced and replaced by newer revisions; they never
// overwrite a finalized message. ID is the upstream assistant message ID, or
// empty when the upstream has not yet announced one.
type PartialInput struct {
	Attempt   AttemptRef
	ID, Text  string
	CreatedAt int64
}

// ToolCallInput records tool-call metadata only. Output bytes are retained
// separately (bounded) and referenced through ArtifactRef; media bytes are
// never embedded in searchable text.
type ToolCallInput struct {
	Attempt                                AttemptRef
	MessageID, CallID, PartID, Tool, State string
	Title, ErrorText, Output               string
	StartedAt, EndedAt                     int64
}

// AttachmentInput records attachment metadata for either an inbound user
// attachment or an attachment produced by a tool/assistant part.
type AttachmentInput struct {
	Attempt                 AttemptRef
	MessageID, PartID, Role string
	Mime, Filename, URL     string
	ByteLength              int
	CreatedAt               int64
}

type AttemptResult struct {
	Attempt AttemptRef
	Status  string
}

type ConversationRecorder interface {
	BeginTurn(context.Context, TurnInput) (TurnRef, error)
	BindAttempt(context.Context, AttemptInput) (AttemptRef, error)
	SaveMessage(context.Context, MessageInput) error
	SavePartial(context.Context, PartialInput) error
	SaveToolCall(context.Context, ToolCallInput) error
	SaveAttachment(context.Context, AttachmentInput) error
	FinishAttempt(context.Context, AttemptResult) error
	PendingAttempts(context.Context, int) ([]AttemptRef, error)
}

func WithConversationRecorder(recorder ConversationRecorder) Option {
	return func(client *Client) { client.conversationRecorder = recorder }
}

// Partial snapshot coalescing bounds. These limit only locally observed
// partial capture; they say nothing about upstream data availability.
const (
	capturePartialInterval = 5 * time.Second
	capturePartialBytes    = 16 * 1024
)

// captureTurnState is shared by value-copied MessagePayload instances so the
// streaming handler can observe the attempt bound by SendMessage.
type captureTurnState struct {
	mu               sync.Mutex
	attempt          AttemptRef
	bound            bool
	lastPartialAt    time.Time
	lastPartialBytes int
	lastPartialID    string
}

func (state *captureTurnState) bind(attempt AttemptRef) {
	state.mu.Lock()
	state.attempt, state.bound = attempt, true
	state.mu.Unlock()
}

func (state *captureTurnState) current() (AttemptRef, bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.attempt, state.bound
}

// shouldFlushPartial reports whether a snapshot of text should be persisted
// now. Snapshots are coalesced to at most one per interval or per byte
// threshold, whichever comes first.
func (state *captureTurnState) shouldFlushPartial(messageID, text string) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.bound || text == "" {
		return false
	}
	if messageID != state.lastPartialID {
		state.lastPartialID, state.lastPartialAt, state.lastPartialBytes = messageID, time.Time{}, 0
	}
	if len(text)-state.lastPartialBytes < capturePartialBytes && time.Since(state.lastPartialAt) < capturePartialInterval {
		return false
	}
	state.lastPartialAt, state.lastPartialBytes = time.Now(), len(text)
	return true
}

// capturePartial persists a coalesced rolling snapshot of assistant text that
// is still being produced. Failures are logged, never fatal: partial capture is
// best-effort and must not interrupt delivery. The write is dispatched
// asynchronously so the SSE event loop is never blocked by storage.
func (c *Client) capturePartial(payload MessagePayload, messageID, text string) {
	if c.conversationRecorder == nil || payload.captureState == nil || strings.TrimSpace(text) == "" {
		return
	}
	if !payload.captureState.shouldFlushPartial(messageID, text) {
		return
	}
	attempt, ok := payload.captureState.current()
	if !ok {
		return
	}
	if err := c.enterCaptureProducer(); err != nil {
		return
	}
	go func() {
		defer c.leaveCaptureProducer()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.conversationRecorder.SavePartial(ctx, PartialInput{Attempt: attempt, ID: messageID, Text: text, CreatedAt: time.Now().UnixMilli()}); err != nil {
			log.Printf("opencode: partial snapshot not persisted for attempt %s: %v", attempt.ID, err)
		}
	}()
}

// captureToolEvent records tool-call metadata and any attachments produced by a
// completed tool. Only metadata and bounded output are retained; media bytes
// are never embedded. The write is dispatched asynchronously.
func (c *Client) captureToolEvent(payload MessagePayload, event *sdk.EventListResponse) {
	if c.conversationRecorder == nil || payload.captureState == nil || event == nil {
		return
	}
	attempt, ok := payload.captureState.current()
	if !ok {
		return
	}
	raw := event.JSON.RawJSON()
	if raw == "" {
		return
	}
	var wrapper struct {
		Properties struct {
			Part struct {
				ID        string `json:"id"`
				CallID    string `json:"callID"`
				MessageID string `json:"messageID"`
				Tool      string `json:"tool"`
				Type      string `json:"type"`
				State     struct {
					Status string `json:"status"`
					Title  string `json:"title"`
					Output string `json:"output"`
					Error  string `json:"error"`
					Time   struct {
						Start float64 `json:"start"`
						End   float64 `json:"end"`
					} `json:"time"`
					Attachments []struct {
						ID       string `json:"id"`
						Mime     string `json:"mime"`
						Filename string `json:"filename"`
						URL      string `json:"url"`
					} `json:"attachments"`
				} `json:"state"`
			} `json:"part"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(raw), &wrapper); err != nil {
		return
	}
	part := wrapper.Properties.Part
	if part.Type != "tool" || part.CallID == "" || part.Tool == "" {
		return
	}
	if err := c.enterCaptureProducer(); err != nil {
		return
	}
	go func() {
		defer c.leaveCaptureProducer()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		input := ToolCallInput{
			Attempt: attempt, MessageID: part.MessageID, CallID: part.CallID, PartID: part.ID,
			Tool: part.Tool, State: part.State.Status, Title: part.State.Title,
			ErrorText: part.State.Error, Output: part.State.Output,
			StartedAt: int64(part.State.Time.Start), EndedAt: int64(part.State.Time.End),
		}
		if err := c.conversationRecorder.SaveToolCall(ctx, input); err != nil {
			log.Printf("opencode: tool metadata not persisted for attempt %s: %v", attempt.ID, err)
			return
		}
		for _, attachment := range part.State.Attachments {
			if attachment.URL == "" && attachment.Filename == "" {
				continue
			}
			if err := c.conversationRecorder.SaveAttachment(ctx, AttachmentInput{
				Attempt: attempt, MessageID: part.MessageID, PartID: part.ID, Role: "output",
				Mime: attachment.Mime, Filename: attachment.Filename, URL: attachment.URL,
				CreatedAt: time.Now().UnixMilli(),
			}); err != nil {
				log.Printf("opencode: tool attachment metadata not persisted for attempt %s: %v", attempt.ID, err)
			}
		}
	}()
}

// captureInputAttachments records metadata for inbound user attachments. Media
// bytes stay in the transport payload; only identity and size are archived.
func (c *Client) captureInputAttachments(payload MessagePayload) {
	if c.conversationRecorder == nil || payload.captureState == nil || len(payload.Attachments) == 0 {
		return
	}
	attempt, ok := payload.captureState.current()
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for index, attachment := range payload.Attachments {
		if attachment.URL == "" && attachment.Filename == "" {
			continue
		}
		if err := c.conversationRecorder.SaveAttachment(ctx, AttachmentInput{
			Attempt: attempt, PartID: fmt.Sprintf("input-%d", index), Role: "input",
			Mime: attachment.Mime, Filename: attachment.Filename, URL: attachment.URL,
			CreatedAt: time.Now().UnixMilli(),
		}); err != nil {
			log.Printf("opencode: input attachment metadata not persisted for attempt %s: %v", attempt.ID, err)
		}
	}
}

func (c *Client) DurableCaptureEnabled() bool { return c.conversationRecorder != nil }

func (c *Client) enterCaptureProducer() error {
	if !c.DurableCaptureEnabled() {
		return nil
	}
	c.captureMu.Lock()
	defer c.captureMu.Unlock()
	if c.captureClosing {
		return fmt.Errorf("%w: gateway is shutting down", ErrCapture)
	}
	c.captureProducers.Add(1)
	return nil
}

func (c *Client) leaveCaptureProducer() {
	if c.DurableCaptureEnabled() {
		c.captureProducers.Done()
	}
}

func (c *Client) StopCapture(ctx context.Context) error {
	if !c.DurableCaptureEnabled() {
		return nil
	}
	c.captureMu.Lock()
	c.captureClosing = true
	c.captureMu.Unlock()
	done := make(chan struct{})
	go func() { c.captureProducers.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Client) beginCapture(ctx context.Context, payload *MessagePayload) error {
	if c.conversationRecorder == nil || payload.capture != nil || payload.Internal || payload.Channel == "skillgen" || strings.HasPrefix(payload.ThreadID, "skillgen-draft-") {
		return nil
	}
	if payload.DeliveryID == "" {
		payload.DeliveryID = "synthetic:" + rand.Text()
	}
	content := payload.Content
	if payload.OriginalContent != nil {
		content = *payload.OriginalContent
	}
	ref, err := c.conversationRecorder.BeginTurn(ctx, TurnInput{
		Adapter: payload.Channel, UserID: payload.UserID, ThreadID: payload.ThreadID,
		DeliveryID: payload.DeliveryID, Content: content, WorkDir: c.sessionDirectory(payload.SessionID),
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCapture, err)
	}
	if payload.OnCaptureCommitted != nil {
		payload.OnCaptureCommitted()
	}
	// A deliberate re-dispatch of the same delivery (token-overflow recovery,
	// explicit retry) continues the existing turn with a new attempt instead of
	// being rejected as a replay. BeginTurn already verified that the stored
	// input, thread, and working directory match, so this cannot silently
	// substitute a different request.
	if ref.Existing && !payload.RetryOfDelivery {
		return ErrCapturedDelivery
	}
	payload.capture = &ref
	payload.captureState = &captureTurnState{}
	return nil
}

func (c *Client) bindCapture(ctx context.Context, turn TurnRef, sessionID string, retry bool) (AttemptRef, error) {
	ref, err := c.conversationRecorder.BindAttempt(ctx, AttemptInput{Turn: turn, ID: newCaptureAttemptID(), SessionID: sessionID, Retry: retry})
	if err != nil {
		return AttemptRef{}, fmt.Errorf("%w: %w", ErrCapture, err)
	}
	return ref, nil
}

func (c *Client) RunCaptureRecovery(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if err := c.ReconcileConversations(ctx, 16); err != nil && ctx.Err() == nil {
			log.Printf("opencode: archive recovery pending: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (c *Client) ReconcileConversations(ctx context.Context, limit int) error {
	if c.conversationRecorder == nil {
		return nil
	}
	attempts, err := c.conversationRecorder.PendingAttempts(ctx, limit)
	if err != nil {
		return err
	}
	var failures []error
	for _, attempt := range attempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		checkCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		err := c.reconcileAttempt(checkCtx, attempt)
		cancel()
		if err != nil {
			failures = append(failures, fmt.Errorf("attempt %s: %w", attempt.ID, err))
		}
	}
	return errors.Join(failures...)
}

func (c *Client) reconcileAttempt(ctx context.Context, attempt AttemptRef) error {
	messages, err := c.sdk.Session.Messages(ctx, attempt.SessionID, sdk.SessionMessagesParams{})
	if err != nil {
		status := "awaiting_reconciliation"
		var apiError *sdk.Error
		if errors.As(err, &apiError) && apiError.StatusCode == 404 {
			status = "unavailable"
		}
		finishErr := c.conversationRecorder.FinishAttempt(ctx, AttemptResult{Attempt: attempt, Status: status})
		return errors.Join(fmt.Errorf("upstream evidence unavailable"), finishErr)
	}
	if messages == nil {
		return fmt.Errorf("upstream evidence absent")
	}
	sort.SliceStable(*messages, func(first, second int) bool {
		firstInfo, _ := (*messages)[first].Info.AsUnion().(sdk.AssistantMessage)
		secondInfo, _ := (*messages)[second].Info.AsUnion().(sdk.AssistantMessage)
		return firstInfo.Time.Created < secondInfo.Time.Created
	})
	status := "awaiting_reconciliation"
	for _, message := range *messages {
		info, ok := message.Info.AsUnion().(sdk.AssistantMessage)
		if !ok {
			continue
		}
		outcome, matched, err := c.saveCapturedAssistant(ctx, attempt, info, message.Parts)
		if err != nil {
			return err
		}
		if matched {
			status = outcome
		}
	}
	return c.conversationRecorder.FinishAttempt(ctx, AttemptResult{Attempt: attempt, Status: status})
}

func (c *Client) capturePromptResult(ctx context.Context, attempt AttemptRef, result *sdk.SessionPromptResponse) error {
	if result == nil {
		return fmt.Errorf("upstream returned no message evidence")
	}
	_, matched, err := c.saveCapturedAssistant(ctx, attempt, result.Info, result.Parts)
	if err != nil {
		return err
	}
	if !matched {
		return fmt.Errorf("upstream response does not match captured attempt")
	}
	return c.conversationRecorder.FinishAttempt(ctx, AttemptResult{Attempt: attempt, Status: "awaiting_reconciliation"})
}

func (c *Client) saveCapturedAssistant(ctx context.Context, attempt AttemptRef, info sdk.AssistantMessage, parts []sdk.Part) (string, bool, error) {
	if info.ParentID != attempt.UserMessageID || info.SessionID != attempt.SessionID || info.Summary {
		return "", false, nil
	}
	if info.Time.Completed <= 0 && info.Error.Name == "" {
		return "awaiting_reconciliation", true, nil
	}
	state := "completed"
	if info.Error.Name != "" {
		state = "failed"
	}
	var text strings.Builder
	for _, part := range parts {
		if value, ok := part.AsUnion().(sdk.TextPart); ok {
			text.WriteString(value.Text)
		}
	}
	if err := c.conversationRecorder.SaveMessage(ctx, MessageInput{Attempt: attempt, ID: info.ID,
		Text: text.String(), State: state, CreatedAt: int64(info.Time.Created)}); err != nil {
		return "", true, err
	}
	if state == "failed" {
		return "failed", true, nil
	}
	var metadata struct {
		Finish string `json:"finish"`
	}
	if err := json.Unmarshal([]byte(info.JSON.RawJSON()), &metadata); err != nil {
		return "", true, err
	}
	switch metadata.Finish {
	case "stop", "length", "content-filter":
		return "completed", true, nil
	default:
		return "awaiting_reconciliation", true, nil
	}
}
