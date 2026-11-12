package opencode

// Real-upstream compatibility test.
//
// The implementation plan's PR 3 gate requires that the wire format be asserted
// against a real OpenCode server, not a fake one:
//
//	"The test must assert the wire format (session creation, message and part
//	 JSON shape, event envelope) against that server, not model quality, so it
//	 needs no paid model access. A fake server does not satisfy this gate."
//
// The test is skipped unless OPENCODE_ENDPOINT is set, so `go test ./...` stays
// hermetic. To run it:
//
//	opencode serve --port 4096 --hostname 127.0.0.1
//	$env:OPENCODE_ENDPOINT = "http://127.0.0.1:4096"
//	go test ./internal/opencode/ -run TestRealUpstream -v
//
// Nothing here asserts model output. Every prompt is sent with noReply=true,
// which makes the server persist the user message and emit the corresponding
// events without invoking a provider. That is what makes the gate runnable
// without paid model access: the wire format is exercised end to end, the
// model is never called.

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	sdk "github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
)

const upstreamEndpointEnv = "OPENCODE_ENDPOINT"

// upstreamEndpoint returns the configured endpoint or skips the test. The skip
// is the whole reason this file can live in the normal package: without an
// endpoint there is nothing real to assert against, and a fake server would
// defeat the gate.
func upstreamEndpoint(t *testing.T) string {
	t.Helper()
	endpoint := strings.TrimSpace(os.Getenv(upstreamEndpointEnv))
	if endpoint == "" {
		t.Skipf("set %s to a running `opencode serve` endpoint to run the real-upstream compatibility test", upstreamEndpointEnv)
	}
	return strings.TrimRight(endpoint, "/")
}

// upstreamHTTPClient is used for the request/response assertions. It has a
// timeout because a hung POST would otherwise stall the suite forever.
func upstreamHTTPClient() *http.Client {
	return &http.Client{Timeout: 30 * time.Second}
}

func upstreamPostJSON(t *testing.T, endpoint, path string, body any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal %s body: %v", path, err)
	}
	req, err := http.NewRequest(http.MethodPost, endpoint+path, strings.NewReader(string(encoded)))
	if err != nil {
		t.Fatalf("build POST %s: %v", path, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := upstreamHTTPClient().Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		t.Fatalf("read POST %s response: %v", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s returned %d: %s", path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode POST %s response as object: %v (body: %.400s)", path, err, raw)
	}
	return decoded
}

func upstreamGetJSON(t *testing.T, endpoint, path string) any {
	t.Helper()
	resp, err := upstreamHTTPClient().Get(endpoint + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		t.Fatalf("read GET %s response: %v", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s returned %d: %s", path, resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode GET %s response: %v (body: %.400s)", path, err, raw)
	}
	return decoded
}

// upstreamCreateSession creates a session over raw HTTP and registers its
// deletion, so repeated runs do not accumulate sessions on the developer's
// machine.
func upstreamCreateSession(t *testing.T, endpoint, title string) string {
	t.Helper()
	created := upstreamPostJSON(t, endpoint, "/session", map[string]any{"title": title})
	id, _ := created["id"].(string)
	if id == "" {
		t.Fatalf("session creation returned no id: %v", created)
	}
	t.Cleanup(func() {
		req, err := http.NewRequest(http.MethodDelete, endpoint+"/session/"+id, nil)
		if err != nil {
			return
		}
		resp, err := upstreamHTTPClient().Do(req)
		if err == nil {
			resp.Body.Close()
		}
	})
	return id
}

// upstreamPostNoReplyMessage persists a user message without invoking a model.
// noReply is what makes this gate runnable without paid model access: the
// server still writes the message, emits message.updated and
// message.part.updated, and returns the full message body.
func upstreamPostNoReplyMessage(t *testing.T, endpoint, sessionID, messageID, text string) map[string]any {
	t.Helper()
	return upstreamPostJSON(t, endpoint, "/session/"+sessionID+"/message", map[string]any{
		"parts":     []map[string]any{{"type": "text", "text": text}},
		"messageID": messageID,
		"noReply":   true,
	})
}

// collectUpstreamEvents subscribes to an SSE endpoint, runs during, then drains
// whatever arrived. The grace period exists because the server emits the events
// for a request slightly after the request returns.
func collectUpstreamEvents(t *testing.T, endpoint, path string, during func()) []map[string]any {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint+path, nil)
	if err != nil {
		t.Fatalf("build SSE request for %s: %v", path, err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")

	// No client timeout: http.Client.Timeout bounds the whole response, which
	// for a stream means it would cut the connection mid-test.
	streamClient := &http.Client{}
	resp, err := streamClient.Do(req)
	if err != nil {
		t.Fatalf("open SSE stream %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SSE stream %s returned %d", path, resp.StatusCode)
	}

	frames := make(chan map[string]any, 512)
	go func() {
		defer close(frames)
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), 4<<20)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if payload == "" {
				continue
			}
			var frame map[string]any
			if err := json.Unmarshal([]byte(payload), &frame); err != nil {
				continue
			}
			select {
			case frames <- frame:
			default:
			}
		}
	}()

	during()

	// Drain for a bounded grace period. The stream never ends on its own, so
	// there is no "done" signal to wait for.
	grace := time.After(3 * time.Second)
	collected := make([]map[string]any, 0, 32)
	for {
		select {
		case frame, ok := <-frames:
			if !ok {
				return collected
			}
			collected = append(collected, frame)
		case <-grace:
			return collected
		}
	}
}

// eventTypeOf reads the event type from either envelope. /event puts it at the
// top level; /global/event nests it under payload.
func eventTypeOf(frame map[string]any) string {
	if payload, ok := frame["payload"].(map[string]any); ok {
		if eventType, ok := payload["type"].(string); ok {
			return eventType
		}
		return ""
	}
	eventType, _ := frame["type"].(string)
	return eventType
}

func findEvent(frames []map[string]any, eventType string) map[string]any {
	for _, frame := range frames {
		if eventTypeOf(frame) == eventType {
			return frame
		}
	}
	return nil
}

// unwrapEvent returns the event object itself, stripping the /global/event
// payload envelope when present.
func unwrapEvent(frame map[string]any) map[string]any {
	if payload, ok := frame["payload"].(map[string]any); ok {
		return payload
	}
	return frame
}

func requireString(t *testing.T, object map[string]any, key, context string) string {
	t.Helper()
	value, _ := object[key].(string)
	if value == "" {
		t.Fatalf("%s: expected a non-empty %q, got %v", context, key, object[key])
	}
	return value
}

// TestRealUpstreamSessionCreationWireFormat asserts the session creation shape
// the gateway depends on: an ses_ identifier, a version, and a directory.
func TestRealUpstreamSessionCreationWireFormat(t *testing.T) {
	endpoint := upstreamEndpoint(t)

	created := upstreamPostJSON(t, endpoint, "/session", map[string]any{"title": "gateway-wire-format"})
	id := requireString(t, created, "id", "session creation")
	if !strings.HasPrefix(id, "ses_") {
		t.Fatalf("session id %q does not carry the ses_ prefix the gateway parses", id)
	}
	requireString(t, created, "version", "session creation")
	requireString(t, created, "directory", "session creation")
	requireString(t, created, "projectID", "session creation")

	// time.created is what the gateway uses to order sessions; a zero value
	// would silently sort every session first.
	timeObject, ok := created["time"].(map[string]any)
	if !ok {
		t.Fatalf("session creation returned no time object: %v", created)
	}
	createdAt, _ := timeObject["created"].(float64)
	if createdAt <= 0 {
		t.Fatalf("session time.created = %v, want a positive epoch millisecond", timeObject["created"])
	}

	// The SDK must decode the same body into the same fields. A divergence here
	// means the gateway's typed client and the raw wire format disagree.
	client := sdk.NewClient(option.WithBaseURL(endpoint))
	session, err := client.Session.New(context.Background(), sdk.SessionNewParams{Title: sdk.F("gateway-wire-format-sdk")})
	if err != nil {
		t.Fatalf("SDK Session.New against the real server: %v", err)
	}
	if !strings.HasPrefix(session.ID, "ses_") {
		t.Fatalf("SDK decoded session id %q without the ses_ prefix", session.ID)
	}
	if session.Version == "" {
		t.Fatalf("SDK decoded an empty session version")
	}
	if session.Directory == "" {
		t.Fatalf("SDK decoded an empty session directory")
	}
	t.Cleanup(func() {
		_, _ = client.Session.Delete(context.Background(), session.ID, sdk.SessionDeleteParams{})
	})
}

// TestRealUpstreamMessageAndPartWireFormat asserts the message and part shape,
// including that a caller-supplied messageID is accepted and echoed back. The
// gateway's capture layer depends on that echo: it sends a stable messageID so
// a retried prompt cannot create a second upstream message.
func TestRealUpstreamMessageAndPartWireFormat(t *testing.T) {
	endpoint := upstreamEndpoint(t)
	sessionID := upstreamCreateSession(t, endpoint, "gateway-message-wire-format")

	const messageID = "msg_gatewaywireformat0000000001"
	const text = "wire format probe: the gateway must see this text verbatim"

	posted := upstreamPostNoReplyMessage(t, endpoint, sessionID, messageID, text)

	info, ok := posted["info"].(map[string]any)
	if !ok {
		t.Fatalf("prompt response carried no info object: %v", posted)
	}
	if got := requireString(t, info, "id", "prompt response info"); got != messageID {
		t.Fatalf("prompt response info.id = %q, want the caller-supplied %q; the capture layer relies on this echo", got, messageID)
	}
	if role := requireString(t, info, "role", "prompt response info"); role != "user" {
		t.Fatalf("prompt response info.role = %q, want user", role)
	}
	if got := requireString(t, info, "sessionID", "prompt response info"); got != sessionID {
		t.Fatalf("prompt response info.sessionID = %q, want %q", got, sessionID)
	}

	parts, ok := posted["parts"].([]any)
	if !ok || len(parts) == 0 {
		t.Fatalf("prompt response carried no parts: %v", posted)
	}
	part, ok := parts[0].(map[string]any)
	if !ok {
		t.Fatalf("prompt response part is not an object: %v", parts[0])
	}
	partID := requireString(t, part, "id", "prompt response part")
	if !strings.HasPrefix(partID, "prt_") {
		t.Fatalf("part id %q does not carry the prt_ prefix the gateway parses", partID)
	}
	if got := requireString(t, part, "messageID", "prompt response part"); got != messageID {
		t.Fatalf("part.messageID = %q, want %q", got, messageID)
	}
	if got := requireString(t, part, "sessionID", "prompt response part"); got != sessionID {
		t.Fatalf("part.sessionID = %q, want %q", got, sessionID)
	}
	if partType := requireString(t, part, "type", "prompt response part"); partType != "text" {
		t.Fatalf("part.type = %q, want text", partType)
	}
	if got := requireString(t, part, "text", "prompt response part"); got != text {
		t.Fatalf("part.text did not round-trip: got %q, want %q", got, text)
	}

	// Reading the session back must return the same identifiers. The capture
	// layer reconciles its own rows against this list, so a different id here
	// would look like a second message.
	listed, ok := upstreamGetJSON(t, endpoint, "/session/"+sessionID+"/message").([]any)
	if !ok {
		t.Fatalf("message list is not an array")
	}
	var found map[string]any
	for _, entry := range listed {
		entryObject, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		entryInfo, ok := entryObject["info"].(map[string]any)
		if !ok {
			continue
		}
		if entryInfo["id"] == messageID {
			found = entryObject
			break
		}
	}
	if found == nil {
		t.Fatalf("message list did not contain the message we just posted (%s)", messageID)
	}
	foundParts, ok := found["parts"].([]any)
	if !ok || len(foundParts) == 0 {
		t.Fatalf("listed message carried no parts: %v", found)
	}
	foundPart, ok := foundParts[0].(map[string]any)
	if !ok {
		t.Fatalf("listed part is not an object: %v", foundParts[0])
	}
	if got := requireString(t, foundPart, "id", "listed part"); got != partID {
		t.Fatalf("listed part id = %q, want the posted %q", got, partID)
	}

	// The SDK must decode the same body into the same identifiers.
	client := sdk.NewClient(option.WithBaseURL(endpoint))
	messages, err := client.Session.Messages(context.Background(), sessionID, sdk.SessionMessagesParams{})
	if err != nil {
		t.Fatalf("SDK Session.Messages against the real server: %v", err)
	}
	var matched bool
	for _, message := range *messages {
		if message.Info.ID != messageID {
			continue
		}
		matched = true
		if message.Info.Role != sdk.MessageRoleUser {
			t.Fatalf("SDK decoded role %q, want user", message.Info.Role)
		}
		if message.Info.SessionID != sessionID {
			t.Fatalf("SDK decoded sessionID %q, want %q", message.Info.SessionID, sessionID)
		}
		if len(message.Parts) == 0 {
			t.Fatalf("SDK decoded no parts for the posted message")
		}
		if message.Parts[0].ID != partID {
			t.Fatalf("SDK decoded part id %q, want %q", message.Parts[0].ID, partID)
		}
		if message.Parts[0].MessageID != messageID {
			t.Fatalf("SDK decoded part.messageID %q, want %q", message.Parts[0].MessageID, messageID)
		}
		if message.Parts[0].Type != sdk.PartTypeText {
			t.Fatalf("SDK decoded part.type %q, want text", message.Parts[0].Type)
		}
	}
	if !matched {
		t.Fatalf("SDK Session.Messages did not return the posted message %s", messageID)
	}
}

// TestRealUpstreamSessionEventEnvelope asserts the /event envelope: no payload
// wrapper, an evt_ identifier, and the properties shape the gateway's parsers
// read.
func TestRealUpstreamSessionEventEnvelope(t *testing.T) {
	endpoint := upstreamEndpoint(t)

	var sessionID string
	const messageID = "msg_gatewayeventenvelope000001"
	frames := collectUpstreamEvents(t, endpoint, "/event", func() {
		sessionID = upstreamCreateSession(t, endpoint, "gateway-event-envelope")
		upstreamPostNoReplyMessage(t, endpoint, sessionID, messageID, "event envelope probe")
	})

	if len(frames) == 0 {
		t.Fatalf("the /event stream produced no frames")
	}

	// The first frame on a real server is server.connected. Asserting it is what
	// distinguishes a real server from a fake one that only replays fixtures.
	if first := eventTypeOf(frames[0]); first != "server.connected" {
		t.Fatalf("first /event frame type = %q, want server.connected", first)
	}

	for _, frame := range frames {
		if _, wrapped := frame["payload"]; wrapped {
			t.Fatalf("/event frame unexpectedly carries a payload wrapper: %v", frame)
		}
		if id, _ := frame["id"].(string); id != "" && !strings.HasPrefix(id, "evt_") {
			t.Fatalf("/event frame id %q does not carry the evt_ prefix", id)
		}
		if _, ok := frame["properties"].(map[string]any); !ok {
			t.Fatalf("/event frame has no properties object: %v", frame)
		}
	}

	for _, expected := range []string{"session.created", "message.updated", "message.part.updated"} {
		if findEvent(frames, expected) == nil {
			t.Fatalf("the /event stream never emitted %s; got types %v", expected, eventTypes(frames))
		}
	}

	partFrame := findEvent(frames, "message.part.updated")
	properties, ok := partFrame["properties"].(map[string]any)
	if !ok {
		t.Fatalf("message.part.updated has no properties object: %v", partFrame)
	}
	if got := requireString(t, properties, "sessionID", "message.part.updated properties"); got != sessionID {
		t.Fatalf("message.part.updated properties.sessionID = %q, want %q", got, sessionID)
	}
	part, ok := properties["part"].(map[string]any)
	if !ok {
		t.Fatalf("message.part.updated has no properties.part object: %v", properties)
	}
	if id := requireString(t, part, "id", "message.part.updated part"); !strings.HasPrefix(id, "prt_") {
		t.Fatalf("message.part.updated part.id %q does not carry the prt_ prefix", id)
	}
	if got := requireString(t, part, "messageID", "message.part.updated part"); got != messageID {
		t.Fatalf("message.part.updated part.messageID = %q, want %q", got, messageID)
	}
	if got := requireString(t, part, "sessionID", "message.part.updated part"); got != sessionID {
		t.Fatalf("message.part.updated part.sessionID = %q, want %q", got, sessionID)
	}
	requireString(t, part, "type", "message.part.updated part")

	// A real message.part.updated does NOT carry properties.message. The
	// gateway's partMeta2 struct reads properties.message.role, so that field is
	// always empty in production and the role must come from message.updated
	// instead. Pinning the absence here is what stops someone from "simplifying"
	// the pre-dispatch in client.go on the assumption that the role is present.
	if _, present := properties["message"]; present {
		t.Fatalf("message.part.updated now carries properties.message; the role fallback in client.go can be revisited")
	}

	// message.updated is where the role actually lives, which is why the
	// gateway pre-dispatches it into messageRoles.
	messageFrame := findEvent(frames, "message.updated")
	messageProperties, ok := messageFrame["properties"].(map[string]any)
	if !ok {
		t.Fatalf("message.updated has no properties object: %v", messageFrame)
	}
	messageInfo, ok := messageProperties["info"].(map[string]any)
	if !ok {
		t.Fatalf("message.updated has no properties.info object: %v", messageProperties)
	}
	if got := requireString(t, messageInfo, "id", "message.updated info"); got != messageID {
		t.Fatalf("message.updated info.id = %q, want %q", got, messageID)
	}
	if role := requireString(t, messageInfo, "role", "message.updated info"); role != "user" {
		t.Fatalf("message.updated info.role = %q, want user", role)
	}
}

// TestRealUpstreamGlobalEventEnvelope asserts the /global/event envelope: every
// frame is wrapped in payload, and the server also emits a sync variant that
// carries no properties at all. The gateway unwraps payload before decoding, so
// both shapes must be tolerated.
func TestRealUpstreamGlobalEventEnvelope(t *testing.T) {
	endpoint := upstreamEndpoint(t)

	var sessionID string
	const messageID = "msg_gatewayglobalevent0000001"
	frames := collectUpstreamEvents(t, endpoint, "/global/event", func() {
		sessionID = upstreamCreateSession(t, endpoint, "gateway-global-envelope")
		upstreamPostNoReplyMessage(t, endpoint, sessionID, messageID, "global envelope probe")
	})

	if len(frames) == 0 {
		t.Fatalf("the /global/event stream produced no frames")
	}

	for _, frame := range frames {
		payload, ok := frame["payload"].(map[string]any)
		if !ok {
			t.Fatalf("/global/event frame has no payload wrapper: %v", frame)
		}
		if eventType, _ := payload["type"].(string); eventType == "" {
			t.Fatalf("/global/event payload has no type: %v", payload)
		}
	}

	// The real server decorates most frames with directory and project. The
	// gateway ignores them, but their presence is what proves this is the real
	// global stream rather than a replayed fixture.
	var decorated bool
	for _, frame := range frames {
		if _, ok := frame["directory"].(string); ok {
			decorated = true
			break
		}
	}
	if !decorated {
		t.Fatalf("/global/event frames carried no directory field; got %v", frames[0])
	}

	// The sync variant is a real shape the gateway must not choke on: it has a
	// payload.type of "sync" and no properties object.
	var sawSync bool
	for _, frame := range frames {
		payload := unwrapEvent(frame)
		if payload["type"] != "sync" {
			continue
		}
		sawSync = true
		syncEvent, ok := payload["syncEvent"].(map[string]any)
		if !ok {
			t.Fatalf("sync payload has no syncEvent object: %v", payload)
		}
		if _, ok := syncEvent["seq"].(float64); !ok {
			t.Fatalf("syncEvent has no numeric seq: %v", syncEvent)
		}
		if _, present := payload["properties"]; present {
			t.Fatalf("sync payload unexpectedly carries properties: %v", payload)
		}
	}
	if !sawSync {
		t.Fatalf("/global/event never emitted a sync payload; got types %v", eventTypes(frames))
	}

	// The session-scoped events must also appear here, wrapped. The gateway
	// filters them out of the global stream so they are not dispatched twice.
	if findEvent(frames, "message.part.updated") == nil {
		t.Fatalf("/global/event never emitted message.part.updated; got types %v", eventTypes(frames))
	}
}

// TestRealUpstreamEventsParseWithGatewayParsers is the compatibility assertion
// proper: it feeds events captured from a real server into the gateway's own
// parsers and checks that they extract what the gateway expects.
func TestRealUpstreamEventsParseWithGatewayParsers(t *testing.T) {
	endpoint := upstreamEndpoint(t)

	var sessionID string
	const messageID = "msg_gatewayparsers000000000001"
	const text = "parser compatibility probe"
	frames := collectUpstreamEvents(t, endpoint, "/event", func() {
		sessionID = upstreamCreateSession(t, endpoint, "gateway-parser-compat")
		upstreamPostNoReplyMessage(t, endpoint, sessionID, messageID, text)
	})

	partFrame := findEvent(frames, "message.part.updated")
	if partFrame == nil {
		t.Fatalf("no message.part.updated frame captured; got types %v", eventTypes(frames))
	}

	raw, err := json.Marshal(partFrame)
	if err != nil {
		t.Fatalf("re-marshal the captured frame: %v", err)
	}
	var event sdk.EventListResponse
	if err := event.UnmarshalJSON(raw); err != nil {
		t.Fatalf("the gateway's SDK type cannot decode a real message.part.updated: %v", err)
	}
	if event.Type != sdk.EventListResponseTypeMessagePartUpdated {
		t.Fatalf("SDK decoded event type %q, want message.part.updated", event.Type)
	}

	// extractSessionIDFromEvent is what routes an event to the right adapter.
	// If it returns empty for a real event, the gateway drops the event.
	if got := extractSessionIDFromEvent(&event); got != sessionID {
		t.Fatalf("extractSessionIDFromEvent returned %q for a real event, want %q", got, sessionID)
	}

	// isSessionScopedEvent must classify real session events as session-scoped,
	// otherwise the global listener dispatches them a second time.
	client := &Client{}
	if !client.isSessionScopedEvent(&event) {
		t.Fatalf("isSessionScopedEvent did not classify a real message.part.updated as session-scoped")
	}

	// The user's own text must not be echoed back as an assistant reply. The
	// gateway learns the role from message.updated, so seed it the same way the
	// pre-dispatch in client.go does.
	client.messageRoles.Store(messageID, "user")
	handler := &StreamingSessionHandler{
		sessionID:       sessionID,
		client:          client,
		activeToolParts: make(map[string]bool),
	}
	if delta, _, _ := handler.extractContentFromEvent(&event); delta != "" {
		t.Fatalf("extractContentFromEvent echoed the user's own text back (%q); the role filter did not apply to a real event", delta)
	}

	// With the role recorded as assistant, the same real event must yield the
	// text. This proves the parser reads properties.part.text correctly from
	// real data rather than only from hand-written fixtures.
	client.messageRoles.Store(messageID, "assistant")
	assistantHandler := &StreamingSessionHandler{
		sessionID:       sessionID,
		client:          client,
		activeToolParts: make(map[string]bool),
	}
	delta, partID, _ := assistantHandler.extractContentFromEvent(&event)
	if delta != text {
		t.Fatalf("extractContentFromEvent returned %q for a real assistant text part, want %q", delta, text)
	}
	if !strings.HasPrefix(partID, "prt_") {
		t.Fatalf("extractContentFromEvent returned part id %q without the prt_ prefix", partID)
	}

	// The global envelope must survive the gateway's unwrap-then-decode path.
	globalFrames := collectUpstreamEvents(t, endpoint, "/global/event", func() {
		upstreamPostNoReplyMessage(t, endpoint, sessionID, "msg_gatewayparsers000000000002", "global parser probe")
	})
	globalPart := findEvent(globalFrames, "message.part.updated")
	if globalPart == nil {
		t.Fatalf("no message.part.updated frame on the global stream; got types %v", eventTypes(globalFrames))
	}
	unwrapped, err := json.Marshal(unwrapEvent(globalPart))
	if err != nil {
		t.Fatalf("re-marshal the unwrapped global frame: %v", err)
	}
	var globalEvent sdk.EventListResponse
	if err := globalEvent.UnmarshalJSON(unwrapped); err != nil {
		t.Fatalf("the gateway's SDK type cannot decode an unwrapped global event: %v", err)
	}
	if globalEvent.Type != sdk.EventListResponseTypeMessagePartUpdated {
		t.Fatalf("unwrapped global event type = %q, want message.part.updated", globalEvent.Type)
	}
	if got := extractSessionIDFromEvent(&globalEvent); got != sessionID {
		t.Fatalf("extractSessionIDFromEvent returned %q for an unwrapped global event, want %q", got, sessionID)
	}
}

// eventTypes summarises the captured frames for failure messages.
func eventTypes(frames []map[string]any) []string {
	types := make([]string, 0, len(frames))
	for _, frame := range frames {
		types = append(types, eventTypeOf(frame))
	}
	return types
}

// TestRealUpstreamEndpointIsReachable fails loudly rather than skipping when an
// endpoint is configured but wrong, so a typo does not silently disable the
// gate.
func TestRealUpstreamEndpointIsReachable(t *testing.T) {
	endpoint := upstreamEndpoint(t)

	resp, err := upstreamHTTPClient().Get(endpoint + "/app")
	if err != nil {
		t.Fatalf("%s is set to %s but the server is unreachable: %v", upstreamEndpointEnv, endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s/app returned %d; %s does not look like an opencode server", endpoint, resp.StatusCode, endpoint)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		t.Fatalf("read %s/app: %v", endpoint, err)
	}
	if !strings.Contains(strings.ToLower(string(body)), "opencode") {
		t.Fatalf("%s/app does not look like the opencode web UI; refusing to treat it as a real upstream", endpoint)
	}
}
