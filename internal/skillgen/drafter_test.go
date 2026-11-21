package skillgen

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/user/opencode-gateway/internal/opencode"
)

type streamDraftClient struct {
	events   []opencode.StreamEvent
	response opencode.Response
	err      error
	payload  opencode.MessagePayload
	cleared  bool
}

func (c *streamDraftClient) SendMessageStreamingWithEvents(_ context.Context, payload opencode.MessagePayload, legacy opencode.StreamCallback, callback opencode.StreamEventCallback) (opencode.Response, error) {
	c.payload = payload
	_ = legacy("ses_legacy_not_body")
	for _, event := range append([]opencode.StreamEvent{{Kind: opencode.StreamEventSessionReady, SessionID: "ses_draft", Content: "ses_draft"}}, c.events...) {
		if err := callback(event); err != nil {
			return opencode.Response{}, err
		}
	}
	return c.response, c.err
}

func (c *streamDraftClient) ClearSkillgenSession(threadID, sessionID string) {
	c.cleared = strings.HasPrefix(threadID, "skillgen-draft-") && sessionID == "ses_draft"
}

func TestParseDraftReplyToleratesProseAndStrayJSON(t *testing.T) {
	const decision = `{"accept": false, "action": "create", "patch_target": "", "title": "", "confidence": 0, "skill_md": "", "reason": "not reusable"}`
	const accepted = `{"accept": true, "action": "create", "title": "pdf-to-excel", "confidence": 0.8, "skill_md": "---\nname: pdf-to-excel\n---\nbody"}`
	for _, tc := range []struct {
		name     string
		reply    string
		declined bool
		title    string
		wantErr  bool
	}{
		{
			name:     "clean decline",
			reply:    decision,
			declined: true,
		},
		{
			name:     "prose before decline",
			reply:    "Let me analyze this conversation first.\n\n" + decision,
			declined: true,
		},
		{
			name:     "stray dot before decline",
			reply:    "{." + decision,
			declined: true,
		},
		{
			name:     "quoted image payload before decline",
			reply:    `{"type":"file","mime":"image/png","data":"base64..."} was never observed. Writing a skill would be speculative.` + decision,
			declined: true,
		},
		{
			name:  "fenced accepted reply",
			reply: "```json\n" + accepted + "\n```",
			title: "pdf-to-excel",
		},
		{
			name:  "prose then accepted reply",
			reply: "Here is the skill.\n\n" + accepted + "\n\nHope this helps.",
			title: "pdf-to-excel",
		},
		{
			name:    "no json at all",
			reply:   "I could not decide.",
			wantErr: true,
		},
		{
			name:    "json without accept key is not a decline",
			reply:   `{"type":"file","mime":"image/png","data":"base64..."}`,
			wantErr: true,
		},
		{
			name:    "accepted but empty skill_md",
			reply:   `{"accept": true, "title": "x", "skill_md": ""}`,
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, declined, err := parseDraftReply(tc.reply)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got output=%+v declined=%v", out, declined)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if declined != tc.declined {
				t.Fatalf("declined=%v want=%v", declined, tc.declined)
			}
			if out.Title != tc.title {
				t.Fatalf("title=%q want=%q", out.Title, tc.title)
			}
		})
	}
}

func TestDraftTreatsProseWrappedDeclineAsSuccess(t *testing.T) {
	client := &streamDraftClient{response: opencode.Response{SessionID: "ses_draft"}}
	client.events = append(client.events, opencode.StreamEvent{
		Kind:    opencode.StreamEventTextDelta,
		Content: "Let me analyze this conversation.\n\n" + `{"accept": false, "confidence": 0, "reason": "one-off debugging session"}`,
	})
	drafter := &OpencodeDrafter{Client: client}
	output, err := drafter.Draft(context.Background(), DraftInput{ThreadID: "chat", UserID: "alice", ModelID: "provider/model",
		Conversation: []Turn{{Role: "user", Text: "repeatable task"}, {Role: "assistant", Text: "procedure"}}})
	if err != nil || output.Title != "" {
		t.Fatalf("prose-wrapped decline was treated as a generation failure: output=%+v err=%v", output, err)
	}
	if !client.cleared {
		t.Fatal("declined draft did not clear its session")
	}
}

func TestDraftUsesOnlyCompletedBody(t *testing.T) {
	const reply = `{"accept":true,"title":"useful-skill","confidence":0.9,"skill_md":"---\nname: useful-skill\ndescription: test\n---\nbody"}`
	for _, mode := range []string{"success", "session only", "error after body", "stop unconfirmed", "deferred", "declined"} {
		t.Run(mode, func(t *testing.T) {
			client := &streamDraftClient{response: opencode.Response{SessionID: "ses_draft"}}
			for _, kind := range []opencode.StreamEventKind{
				opencode.StreamEventThinking, opencode.StreamEventTool, opencode.StreamEventStep,
				opencode.StreamEventTodo, opencode.StreamEventInfo, opencode.StreamEventFlush,
			} {
				client.events = append(client.events, opencode.StreamEvent{Kind: kind, Content: "must not be parsed"})
			}
			if mode != "session only" {
				body := reply
				if mode == "declined" {
					body = `{"accept":false,"confidence":0,"reason":"not reusable"}`
				}
				client.events = append(client.events, opencode.StreamEvent{Kind: opencode.StreamEventTextDelta, Content: body})
			}
			switch mode {
			case "error after body":
				client.err = errors.New("provider failed after partial output")
			case "stop unconfirmed":
				client.err = &opencode.RemoteStopUnconfirmedError{SessionID: "ses_draft", Cause: errors.New("abort failed")}
			case "deferred":
				client.response.Deferred = true
			}
			drafter := &OpencodeDrafter{Client: client}
			output, err := drafter.Draft(context.Background(), DraftInput{ThreadID: "chat", UserID: "alice", ModelID: "provider/model",
				Conversation: []Turn{{Role: "user", Text: "repeatable task"}, {Role: "assistant", Text: "procedure"}}})
			if mode == "success" {
				if err != nil || output.Title != "useful-skill" || strings.Contains(output.SkillMD, "must not") {
					t.Fatalf("structured text body was polluted: output=%+v err=%v", output, err)
				}
			} else if mode == "declined" {
				if err != nil || output.Title != "" {
					t.Fatalf("deliberate decline was treated as generation failure: output=%+v err=%v", output, err)
				}
			} else if err == nil || output.SkillMD != "" {
				t.Fatalf("incomplete or failed draft became approvable: output=%+v err=%v", output, err)
			}
			if !client.payload.ForegroundOnly || !client.payload.Internal || !client.payload.Streaming {
				t.Fatalf("draft escaped the foreground completion contract: %+v", client.payload)
			}
			wantClear := mode != "stop unconfirmed" && mode != "deferred"
			if client.cleared != wantClear {
				t.Fatalf("unsafe or missing session cleanup: mode=%s cleared=%v", mode, client.cleared)
			}
		})
	}
}
