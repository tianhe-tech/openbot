package memstore

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/user/opencode-gateway/internal/opencode"
)

func TestLifecycleOriginalInputAttemptsAndReopen(t *testing.T) {
	path, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	input := opencode.TurnInput{Adapter: "test", UserID: "owner", ThreadID: "thread", DeliveryID: "delivery", Content: " original text ", WorkDir: "/work"}
	turn, err := writer.BeginTurn(ctx, input)
	if err != nil || turn.Existing {
		t.Fatalf("begin: %+v %v", turn, err)
	}
	repeated, err := writer.BeginTurn(ctx, input)
	if err != nil || !repeated.Existing || repeated.ID != turn.ID {
		t.Fatalf("replay: %+v %v", repeated, err)
	}
	input.Content = "changed"
	if _, err := writer.BeginTurn(ctx, input); !errors.Is(err, ErrArchiveConflict) {
		t.Fatalf("changed delivery accepted: %v", err)
	}
	first, err := writer.BindAttempt(ctx, opencode.AttemptInput{ID: "attempt-one", Turn: turn, SessionID: "session-original"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := writer.BindAttempt(ctx, opencode.AttemptInput{ID: "attempt-two", Turn: turn, SessionID: "session-fallback"})
	if err != nil {
		t.Fatal(err)
	}
	message := opencode.MessageInput{Attempt: second, ID: "answer", Text: "answer bytes", State: "completed", CreatedAt: 1700000000000}
	if err := writer.SaveMessage(ctx, message); err != nil {
		t.Fatal(err)
	}
	if err := writer.SaveMessage(ctx, message); err != nil {
		t.Fatalf("duplicate answer: %v", err)
	}
	if err := writer.FinishAttempt(ctx, opencode.AttemptResult{Attempt: second, Status: "completed"}); err != nil {
		t.Fatal(err)
	}
	if err := writer.FinishAttempt(ctx, opencode.AttemptResult{Attempt: first, Status: "awaiting_reconciliation"}); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := writer.store.db.QueryRow(`SELECT status FROM mem_v2_turns WHERE id=?`, turn.ID).Scan(&status); err != nil || status != "completed" {
		t.Fatalf("old attempt overwrote fallback: %s %v", status, err)
	}
	writer.Close(ctx)
	writer, err = OpenArchiveWriter(ctx, path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(ctx)
	var count int
	if err := writer.store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_messages`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("message count: %d %v", count, err)
	}
	if body := blockTextForTest(t, writer.store.db, `SELECT b.codec,b.codec_version,b.raw_bytes,b.payload FROM mem_v2_messages m JOIN mem_v2_blocks b ON b.id=m.block_id WHERE m.role='user'`); body != " original text " {
		t.Fatalf("input changed: %q", body)
	}
}

func TestLifecycleRetryReopensCompletedTurn(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	turn, err := writer.BeginTurn(ctx, opencode.TurnInput{Adapter: "test", UserID: "owner", DeliveryID: "delivery", Content: "request"})
	if err != nil {
		t.Fatal(err)
	}
	first, err := writer.BindAttempt(ctx, opencode.AttemptInput{ID: "attempt-one", Turn: turn, SessionID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.FinishAttempt(ctx, opencode.AttemptResult{Attempt: first, Status: "completed"}); err != nil {
		t.Fatal(err)
	}
	// A completed turn stays closed to unmarked attempts: that is what makes a
	// duplicate delivery ID fail closed.
	if _, err := writer.BindAttempt(ctx, opencode.AttemptInput{ID: "attempt-two", Turn: turn, SessionID: "session"}); !errors.Is(err, ErrArchiveConflict) {
		t.Fatalf("completed turn accepted an unmarked attempt: %v", err)
	}
	// A deliberate retry of the same delivery reopens the turn with a new
	// attempt instead of replaying the archived request.
	retry, err := writer.BindAttempt(ctx, opencode.AttemptInput{ID: "attempt-two", Turn: turn, SessionID: "session", Retry: true})
	if err != nil {
		t.Fatalf("deliberate retry rejected: %v", err)
	}
	if retry.TurnID != turn.ID || retry.ID != "attempt-two" {
		t.Fatalf("retry bound to the wrong turn: %+v", retry)
	}
	var status, dispatch string
	if err := writer.store.db.QueryRow(`SELECT status,dispatch_state FROM mem_v2_turns WHERE id=?`, turn.ID).Scan(&status, &dispatch); err != nil {
		t.Fatal(err)
	}
	if status != "active" || dispatch != "uncertain" {
		t.Fatalf("retry did not reopen the turn: %s/%s", status, dispatch)
	}
	var attempts int
	if err := writer.store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_attempts WHERE turn_id=?`, turn.ID).Scan(&attempts); err != nil || attempts != 2 {
		t.Fatalf("retry did not add exactly one attempt: %d %v", attempts, err)
	}
}

func TestLifecycleRecoveryLeaseAndAtomicBinding(t *testing.T) {
	_, writer := archiveForTest(t, WriterOptions{})
	ctx := context.Background()
	turn, err := writer.BeginTurn(ctx, opencode.TurnInput{Adapter: "test", UserID: "owner", DeliveryID: "delivery", Content: "request"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.store.db.Exec(`CREATE TRIGGER reject_reconcile BEFORE INSERT ON mem_v2_jobs WHEN NEW.kind='reconcile' BEGIN SELECT RAISE(ABORT,'job rejected'); END`); err != nil {
		t.Fatal(err)
	}
	input := opencode.AttemptInput{ID: "attempt", Turn: turn, SessionID: "session"}
	if _, err := writer.BindAttempt(ctx, input); err == nil {
		t.Fatal("binding committed without recovery job")
	}
	var count int
	writer.store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_attempts`).Scan(&count)
	if count != 0 {
		t.Fatal("partial binding")
	}
	writer.store.db.Exec(`DROP TRIGGER reject_reconcile`)
	ref, err := writer.BindAttempt(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if repeated, err := writer.BindAttempt(ctx, input); err != nil || repeated != ref {
		t.Fatalf("binding not idempotent: %+v %v", repeated, err)
	}
	attempts, err := writer.PendingAttempts(ctx, 10)
	if err != nil || len(attempts) != 1 || attempts[0] != ref {
		t.Fatalf("missing recovery: %+v %v", attempts, err)
	}
	if attempts, err := writer.PendingAttempts(ctx, 10); err != nil || len(attempts) != 0 {
		t.Fatalf("lease ignored: %+v %v", attempts, err)
	}
	writer.store.db.Exec(`UPDATE mem_v2_jobs SET lease_until=0 WHERE kind='reconcile'`)
	if attempts, err := writer.PendingAttempts(ctx, 10); err != nil || len(attempts) != 1 {
		t.Fatalf("expired lease not recovered: %+v %v", attempts, err)
	}
}

func TestLifecycleAsyncLateReplyAfterRestart(t *testing.T) {
	path, writer := archiveForTest(t, WriterOptions{})
	var mutex sync.Mutex
	var userMessageID string
	var completed bool
	var dispatches int
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		mutex.Lock()
		defer mutex.Unlock()
		response.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/prompt_async"):
			var body struct {
				MessageID string `json:"messageID"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			userMessageID = body.MessageID
			dispatches++
			response.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(request.URL.Path, "/message"):
			messages := []map[string]any{
				captureFixtureMessage("stale", "msg_old", "wrong turn", true),
				captureFixtureMessage("answer", userMessageID, " final original text \n", completed),
			}
			json.NewEncoder(response).Encode(messages)
		case request.URL.Path == "/session":
			json.NewEncoder(response).Encode([]any{})
		default:
			json.NewEncoder(response).Encode(map[string]string{"id": "ses_capture"})
		}
	}))
	defer server.Close()
	client := opencode.NewClient(server.URL, "", opencode.WithConversationRecorder(writer), opencode.WithTimeout(time.Second))
	payload := opencode.MessagePayload{Channel: "test", UserID: "owner", ThreadID: "thread", SessionID: "ses_capture", DeliveryID: "delivery", Content: " original request ", Model: "provider/model", Streaming: true}
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := client.SendMessage(ctx, payload); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := client.ReconcileConversations(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := writer.store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_messages`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("stale or partial reply archived as final: %d %v", count, err)
	}
	writer.Close(context.Background())
	writer, err := OpenArchiveWriter(context.Background(), path, WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close(context.Background())
	mutex.Lock()
	completed = true
	mutex.Unlock()
	if _, err := writer.store.db.Exec(`UPDATE mem_v2_jobs SET next_run_at=0,lease_until=0 WHERE kind='reconcile'`); err != nil {
		t.Fatal(err)
	}
	restarted := opencode.NewClient(server.URL, "", opencode.WithConversationRecorder(writer))
	if err := restarted.ReconcileConversations(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	if err := restarted.ReconcileConversations(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.SendMessage(context.Background(), payload); !errors.Is(err, opencode.ErrCapturedDelivery) {
		t.Fatalf("restart replayed a delivery: %v", err)
	}
	if body := blockTextForTest(t, writer.store.db, `SELECT b.codec,b.codec_version,b.raw_bytes,b.payload FROM mem_v2_messages m JOIN mem_v2_blocks b ON b.id=m.block_id WHERE m.role='assistant'`); body != " final original text \n" {
		t.Fatalf("late result missing or changed: %q", body)
	}
	var status string
	if err := writer.store.db.QueryRow(`SELECT status FROM mem_v2_turns`).Scan(&status); err != nil || status != "completed" {
		t.Fatalf("final status: %s %v", status, err)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if dispatches != 1 || userMessageID == "" {
		t.Fatalf("dispatch count=%d identity=%q", dispatches, userMessageID)
	}
}

func captureFixtureMessage(id, parent, text string, completed bool) map[string]any {
	timestamp := map[string]any{"created": 1700000000000}
	if completed {
		timestamp["completed"] = 1700000001000
	}
	return map[string]any{
		"info":  map[string]any{"id": id, "role": "assistant", "parentID": parent, "sessionID": "ses_capture", "time": timestamp, "finish": "stop"},
		"parts": []any{map[string]any{"id": "text-" + id, "type": "text", "text": text}, map[string]any{"id": "reasoning-" + id, "type": "reasoning", "text": "must not archive private reasoning"}},
	}
}

func TestLifecycleSyncAndFailedStreamingDelivery(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		name := "sync"
		if streaming {
			name = "streaming-callback-fails"
		}
		t.Run(name, func(t *testing.T) {
			_, writer := archiveForTest(t, WriterOptions{})
			var mutex sync.Mutex
			var sourceID string
			var permitHistoryQuery bool
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				mutex.Lock()
				defer mutex.Unlock()
				response.Header().Set("Content-Type", "application/json")
				if request.Method == http.MethodPost {
					var body struct {
						MessageID string `json:"messageID"`
					}
					if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
						t.Error(err)
					}
					sourceID = body.MessageID
					if streaming {
						response.WriteHeader(http.StatusNoContent)
					} else {
						json.NewEncoder(response).Encode(captureFixtureMessage("answer", sourceID, " raw reply ", true))
					}
					return
				}
				if strings.HasSuffix(request.URL.Path, "/message") {
					if !streaming && !permitHistoryQuery {
						t.Error("synchronous capture unnecessarily refetched history")
						response.WriteHeader(404)
						return
					}
					step := captureFixtureMessage("step", sourceID, "intermediate assistant text", true)
					step["info"].(map[string]any)["finish"] = "tool-calls"
					json.NewEncoder(response).Encode([]any{step, captureFixtureMessage("answer", sourceID, " raw reply ", true)})
					return
				}
				if request.URL.Path == "/session" {
					json.NewEncoder(response).Encode([]any{})
					return
				}
				json.NewEncoder(response).Encode(map[string]string{"id": "ses_capture"})
			}))
			defer server.Close()
			client := opencode.NewClient(server.URL, "", opencode.WithConversationRecorder(writer), opencode.WithMemStore(NewGatewayAdapter(writer.store)))
			payload := opencode.MessagePayload{Channel: "test", UserID: "owner", SessionID: "ses_capture", ThreadID: "thread", DeliveryID: name, Content: "request", Model: "provider/model", Streaming: streaming}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if streaming {
				if _, err := client.SendMessageStreaming(ctx, payload, func(string) error { return errors.New("outbound unavailable") }); err != nil {
					t.Fatal(err)
				}
				if err := client.ReconcileConversations(context.Background(), 10); err != nil {
					t.Fatal(err)
				}
			} else if _, err := client.SendMessage(ctx, payload); err != nil {
				t.Fatal(err)
			}
			if !streaming {
				mutex.Lock()
				permitHistoryQuery = true
				mutex.Unlock()
				if _, err := writer.store.db.Exec(`UPDATE mem_v2_jobs SET next_run_at=0 WHERE kind='reconcile'`); err != nil {
					t.Fatal(err)
				}
				if err := client.ReconcileConversations(context.Background(), 10); err != nil {
					t.Fatal(err)
				}
			}
			var count int
			if err := writer.store.db.QueryRow(`SELECT COUNT(*) FROM mem_records`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("strict mode dual-wrote legacy: %d %v", count, err)
			}
			if body := blockTextForTest(t, writer.store.db, `SELECT b.codec,b.codec_version,b.raw_bytes,b.payload FROM mem_v2_messages m JOIN mem_v2_blocks b ON b.id=m.block_id WHERE m.upstream_message_id='answer'`); body != " raw reply " {
				t.Fatalf("reply not independently captured: %q", body)
			}
			if err := writer.store.db.QueryRow(`SELECT COUNT(*) FROM mem_v2_messages WHERE role='assistant'`).Scan(&count); err != nil || count != 2 {
				t.Fatalf("intermediate assistant message lost: %d %v", count, err)
			}
		})
	}
}

func TestLifecycleRecoveryDistinguishesOutcomes(t *testing.T) {
	for _, status := range []string{"unavailable", "failed", "awaiting_reconciliation"} {
		t.Run(status, func(t *testing.T) {
			_, writer := archiveForTest(t, WriterOptions{})
			ctx := context.Background()
			turn, err := writer.BeginTurn(ctx, opencode.TurnInput{Adapter: "test", UserID: "owner", DeliveryID: "delivery", Content: "request"})
			if err != nil {
				t.Fatal(err)
			}
			attempt, err := writer.BindAttempt(ctx, opencode.AttemptInput{ID: "attempt", Turn: turn, SessionID: "ses_capture"})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				response.Header().Set("Content-Type", "application/json")
				if !strings.HasSuffix(request.URL.Path, "/message") {
					json.NewEncoder(response).Encode([]any{})
					return
				}
				if status == "unavailable" {
					response.WriteHeader(http.StatusNotFound)
					return
				}
				message := captureFixtureMessage("answer", attempt.UserMessageID, "preserved output", true)
				info := message["info"].(map[string]any)
				if status == "failed" {
					info["error"] = map[string]any{"name": "MessageAbortedError", "data": map[string]any{"message": "aborted"}}
				} else {
					info["finish"] = "tool-calls"
				}
				json.NewEncoder(response).Encode([]any{message})
			}))
			defer server.Close()
			client := opencode.NewClient(server.URL, "", opencode.WithConversationRecorder(writer))
			err = client.ReconcileConversations(ctx, 10)
			if status == "unavailable" && err == nil {
				t.Fatal("missing source not reported")
			}
			if status != "unavailable" && err != nil {
				t.Fatal(err)
			}
			var actual, jobStatus string
			if err := writer.store.db.QueryRow(`SELECT a.status,j.status FROM mem_v2_attempts a JOIN mem_v2_jobs j ON j.source_id=a.id AND j.kind='reconcile'`).Scan(&actual, &jobStatus); err != nil {
				t.Fatal(err)
			}
			if actual != status {
				t.Fatalf("status %s, want %s", actual, status)
			}
			if (jobStatus == "completed") != (status == "failed") {
				t.Fatalf("incorrect recovery termination: %s %s", actual, jobStatus)
			}
		})
	}
}
