package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/user/opencode-gateway/internal/opencode"
)

type cronTestServer struct {
	url      string
	mu       sync.Mutex
	sessions map[string]string
	created  atomic.Int32
	prompts  atomic.Int32
	status   atomic.Value
	onPrompt func(string)
}

func newCronTestScheduler(t *testing.T) (*CronScheduler, *TaskScheduler, *cronTestServer) {
	t.Helper()
	fake := &cronTestServer{sessions: make(map[string]string)}
	fake.status.Store(`{}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/session/status":
			_, _ = w.Write([]byte(fake.status.Load().(string)))
		case r.URL.Path == "/session" && r.Method == http.MethodPost:
			var body struct{ Title string }
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode session: %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			id := fmt.Sprintf("ses_cron_%d", fake.created.Add(1))
			fake.mu.Lock()
			fake.sessions[id] = body.Title
			fake.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]string{"id": id, "title": body.Title})
		case r.URL.Path == "/session":
			fake.mu.Lock()
			list := make([]map[string]string, 0, len(fake.sessions))
			for id, title := range fake.sessions {
				list = append(list, map[string]string{"id": id, "title": title})
			}
			fake.mu.Unlock()
			_ = json.NewEncoder(w).Encode(list)
		case strings.HasSuffix(r.URL.Path, "/message") && r.Method == http.MethodPost:
			id := strings.Split(r.URL.Path, "/")[2]
			fake.prompts.Add(1)
			if fake.onPrompt != nil {
				fake.onPrompt(id)
			}
			_, _ = w.Write([]byte(`{"info":{"id":"msg_done","role":"assistant"},"parts":[{"type":"text","text":"done"}]}`))
		case strings.HasSuffix(r.URL.Path, "/message"):
			_, _ = w.Write([]byte(`[]`))
		case strings.HasPrefix(r.URL.Path, "/session/"):
			id := strings.TrimPrefix(r.URL.Path, "/session/")
			fake.mu.Lock()
			title, ok := fake.sessions[id]
			fake.mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{}`))
			} else {
				_ = json.NewEncoder(w).Encode(map[string]string{"id": id, "title": title})
			}
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(server.Close)
	fake.url = server.URL
	client := opencode.NewClient(server.URL, "")
	worker := NewTaskScheduler(client, DefaultTaskSchedulerConfig())
	cs := NewCronScheduler(worker)
	cs.SetStoragePath(filepath.Join(t.TempDir(), "tasks.json"))
	client.SetCronSessionChecker(worker.IsCronSession)
	t.Cleanup(func() {
		cs.cancel()
		if err := worker.Stop(); err != nil {
			t.Errorf("stop worker: %v", err)
		}
	})
	return cs, worker, fake
}

func addCronTestTask(t *testing.T, cs *CronScheduler, id string) {
	t.Helper()
	if err := cs.AddScheduledTask(&ScheduledTask{
		ID: id, Name: "test", Enabled: true, CronExpr: "0 0 9 * * *",
		AdapterType: "wechat", UserID: "alice", Channel: "chat", ThreadID: "chat",
		Content: "check status",
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCronPersistsIsolatedSessionBeforePromptAndAcrossRestart(t *testing.T) {
	cs, worker, fake := newCronTestScheduler(t)
	addCronTestTask(t, cs, "task-one")
	fake.mu.Lock()
	fake.sessions["ses_original_chat"] = "[wechat:alice] chat"
	fake.mu.Unlock()
	worker.opencodeClient.BindSessionToThread("chat", "ses_original_chat")
	fake.onPrompt = func(id string) {
		data, err := os.ReadFile(cs.storagePath)
		if err != nil {
			t.Errorf("read committed task: %v", err)
			return
		}
		var saved []*ScheduledTask
		if err := json.Unmarshal(data, &saved); err != nil || len(saved) != 1 || saved[0].SessionID != id {
			t.Errorf("prompt preceded durable binding: %s (%v)", data, err)
		}
		if !worker.IsCronSession(id) {
			t.Error("cron session was not registered before prompt")
		}
	}
	cs.executeScheduledTask("task-one")
	task := <-worker.taskQueue
	if task.ThreadID != "cron-task-one" || task.Channel != "chat" || task.SessionID == "" {
		t.Fatalf("isolated model thread/recipient lost: %+v", task)
	}
	worker.executeTask(0, task)
	if task.Status != TaskStatusCompleted || fake.prompts.Load() != 1 {
		t.Fatalf("cron did not complete: status=%s error=%s", task.Status, task.Error)
	}
	chatID, err := worker.opencodeClient.ResolveOrCreateSession(context.Background(), opencode.MessagePayload{
		Channel: "wechat", UserID: "alice", ThreadID: "chat",
	})
	if err != nil || chatID != "ses_original_chat" {
		t.Fatalf("cron replaced ordinary chat binding: id=%s err=%v", chatID, err)
	}
	// A restarted client has no in-memory thread bindings.
	worker.opencodeClient = opencode.NewClient(fake.url, "")
	restarted := NewCronScheduler(worker)
	restarted.SetStoragePath(cs.storagePath)
	if err := restarted.loadTasks(); err != nil {
		t.Fatal(err)
	}
	restarted.executeScheduledTask("task-one")
	retry := <-worker.taskQueue
	if retry.SessionID != task.SessionID || fake.created.Load() != 1 {
		t.Fatalf("restart created another session: first=%s restart=%s created=%d", task.SessionID, retry.SessionID, fake.created.Load())
	}
	worker.executeTask(0, retry)
	saved, _ := restarted.GetScheduledTask("task-one")
	if saved.LastRunStatus != TaskStatusCompleted {
		t.Fatalf("terminal status not persisted: %+v", saved)
	}
}

func TestCronOverlapAndBusySessionDoNotDispatchAnotherPrompt(t *testing.T) {
	cs, worker, fake := newCronTestScheduler(t)
	addCronTestTask(t, cs, "task-one")
	addCronTestTask(t, cs, "task-two")
	cs.executeScheduledTask("task-one")
	cs.executeScheduledTask("task-one")
	cs.executeScheduledTask("task-two")
	if len(worker.taskQueue) != 2 || fake.created.Load() != 2 {
		t.Fatal("same task overlapped or different tasks blocked one another")
	}
	task := <-worker.taskQueue
	fake.status.Store(fmt.Sprintf(`{"%s":{"type":"busy"}}`, task.SessionID))
	worker.executeTask(0, task)
	if fake.prompts.Load() != 0 || task.Status != TaskStatusFailed {
		t.Fatalf("busy session received a prompt or no terminal failure: prompts=%d status=%s", fake.prompts.Load(), task.Status)
	}
	if task.RetryCount != task.MaxRetries || len(worker.callbacks[task.ID]) != 0 {
		t.Fatal("bounded retries or terminal callback cleanup failed")
	}
	cs.tasksMu.RLock()
	busy := cs.inFlight["task-one"]
	cs.tasksMu.RUnlock()
	if busy {
		t.Fatal("terminal failure did not release cron run gate")
	}
}

func TestCronPersistenceFailureRetainsPendingIDAndSendsNoPrompt(t *testing.T) {
	cs, worker, fake := newCronTestScheduler(t)
	addCronTestTask(t, cs, "task-one")
	// A directory at the temporary-file path deterministically prevents commit.
	if err := os.Mkdir(cs.storagePath+".tmp", 0o755); err != nil {
		t.Fatal(err)
	}
	cs.executeScheduledTask("task-one")
	first, _ := cs.GetScheduledTask("task-one")
	if first.SessionID == "" || first.LastRunStatus != TaskStatusFailed || len(worker.taskQueue) != 0 || fake.prompts.Load() != 0 {
		t.Fatalf("persistence failure did not fail closed: %+v", first)
	}
	if err := os.Remove(cs.storagePath + ".tmp"); err != nil {
		t.Fatal(err)
	}
	cs.executeScheduledTask("task-one")
	queued := <-worker.taskQueue
	if queued.SessionID != first.SessionID || fake.created.Load() != 1 {
		t.Fatal("retry lost the pending session ID and created an orphan")
	}
	worker.executeTask(0, queued)
}

func TestCronLoadDisablesUnknownOwnerAndBackfillsCreator(t *testing.T) {
	cs, _, _ := newCronTestScheduler(t)
	data := `[
		{"id":"unknown","cron_expr":"0 0 9 * * *","enabled":true,"adapter_type":"feishu","metadata":{"receive_id":"group-id"}},
		{"id":"known","cron_expr":"0 0 9 * * *","enabled":true,"adapter_type":"feishu","metadata":{"created_by":"alice","receive_id":"group-id"}}
	]`
	if err := os.WriteFile(cs.storagePath, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cs.loadTasks(); err != nil {
		t.Fatal(err)
	}
	unknown, _ := cs.GetScheduledTask("unknown")
	known, _ := cs.GetScheduledTask("known")
	if unknown.Enabled || unknown.UserID != "" || unknown.LastRunStatus != TaskStatusFailed {
		t.Fatalf("group recipient was guessed as an owner: %+v", unknown)
	}
	if !known.Enabled || known.UserID != "alice" || known.Channel != "group-id" {
		t.Fatalf("unambiguous legacy task was not migrated: %+v", known)
	}
}

func TestCronConcurrentSavesAndReturnedSnapshots(t *testing.T) {
	cs, _, _ := newCronTestScheduler(t)
	addCronTestTask(t, cs, "task-one")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				snapshot, _ := cs.GetScheduledTask("task-one")
				snapshot.Content = "caller-owned edit"
				if err := cs.saveTasks(); err != nil {
					t.Errorf("concurrent save: %v", err)
				}
			}
		}()
	}
	wg.Wait()
	task, _ := cs.GetScheduledTask("task-one")
	if task.Content != "check status" {
		t.Fatal("getter exposed mutable scheduler state")
	}
	var persisted []*ScheduledTask
	data, err := os.ReadFile(cs.storagePath)
	if err != nil || json.Unmarshal(data, &persisted) != nil || len(persisted) != 1 {
		t.Fatalf("concurrent saves corrupted storage: %s %v", data, err)
	}
}

func TestSchedulerStoppedSubmissionAndQueuedCancellationCompleteCallbacks(t *testing.T) {
	worker := NewTaskScheduler(nil, DefaultTaskSchedulerConfig())
	task := &Task{ID: "queued", Status: TaskStatusPending, Result: "stale successful answer"}
	calls := 0
	worker.RegisterCallback(task.ID, func(ctx context.Context, result *TaskResult) error {
		calls++
		if ctx.Err() != nil || result.Status != TaskStatusCanceled || result.Error == "" || result.Result != "" {
			t.Errorf("invalid cancellation terminal result/context: %+v, %v", result, ctx.Err())
		}
		return nil
	})
	if err := worker.SubmitTask(task); err != nil {
		t.Fatal(err)
	}
	if err := worker.Stop(); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(worker.callbacks) != 0 {
		t.Fatalf("queued callback not completed exactly once: calls=%d", calls)
	}
	if err := worker.SubmitTask(&Task{ID: "after-stop"}); err == nil {
		t.Fatal("submission after shutdown succeeded")
	}
}

func TestCronQueueRejectionFinishesAndReleasesRun(t *testing.T) {
	cs, worker, fake := newCronTestScheduler(t)
	addCronTestTask(t, cs, "task-one")
	if err := worker.Stop(); err != nil {
		t.Fatal(err)
	}
	cs.executeScheduledTask("task-one")
	saved, _ := cs.GetScheduledTask("task-one")
	if saved.LastRunStatus != TaskStatusFailed || saved.SessionID == "" || saved.LastRunResult == "" {
		t.Fatalf("rejected submission lacks terminal result/binding: %+v", saved)
	}
	if cs.inFlight["task-one"] || len(worker.callbacks) != 0 || fake.prompts.Load() != 0 {
		t.Fatal("rejected submission leaked callbacks/run ownership or sent a prompt")
	}
}

func TestCronLoadRejectsAnOrdinaryChatBinding(t *testing.T) {
	cs, worker, fake := newCronTestScheduler(t)
	fake.mu.Lock()
	fake.sessions["ses_user_chat"] = "[wechat:alice] ordinary"
	fake.mu.Unlock()
	data := `[{"id":"task-one","cron_expr":"0 0 9 * * *","enabled":true,"adapter_type":"wechat","user_id":"alice","session_id":"ses_user_chat"}]`
	if err := os.WriteFile(cs.storagePath, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := cs.loadTasks(); err != nil {
		t.Fatal(err)
	}
	saved, _ := cs.GetScheduledTask("task-one")
	if saved.Enabled || saved.LastRunStatus != TaskStatusFailed || worker.IsCronSession("ses_user_chat") {
		t.Fatalf("ordinary chat was adopted as cron during startup: %+v", saved)
	}
	if fake.created.Load() != 0 || fake.prompts.Load() != 0 {
		t.Fatal("invalid binding triggered replacement work")
	}
}

type cronRecordingSender struct {
	messages []string
}

func (s *cronRecordingSender) SendMessage(_ context.Context, _, _ string, content string) error {
	s.messages = append(s.messages, content)
	return nil
}

func TestSchedulerExhaustedRetryNotifiesFailureOnce(t *testing.T) {
	worker := NewTaskScheduler(nil, DefaultTaskSchedulerConfig())
	sender := &cronRecordingSender{}
	worker.RegisterAdapter("wechat", sender)
	task := &Task{
		ID: "failed", Type: TaskTypeCron, AdapterType: "wechat", UserID: "alice",
		Channel: "chat", MaxRetries: 2, Result: "stale successful answer",
	}
	callbacks := 0
	worker.RegisterCallback(task.ID, func(ctx context.Context, result *TaskResult) error {
		callbacks++
		if ctx.Err() != nil || result.Status != TaskStatusFailed || result.Error == "" || result.Result != "" {
			t.Errorf("invalid failure result/context: %+v %v", result, ctx.Err())
		}
		return nil
	})
	worker.executeTask(0, task)
	if callbacks != 1 || task.RetryCount != 2 || len(sender.messages) != 1 ||
		!strings.Contains(sender.messages[0], "定时任务执行失败") || strings.Contains(sender.messages[0], "stale successful answer") {
		t.Fatalf("retry terminal notification was missing, duplicated or success-shaped: callbacks=%d messages=%v", callbacks, sender.messages)
	}
}

func TestSchedulerConcurrentStopAndSubmissionLeavesNoQueuedWork(t *testing.T) {
	worker := NewTaskScheduler(nil, DefaultTaskSchedulerConfig())
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = worker.SubmitTask(&Task{ID: fmt.Sprintf("task-%d", id)})
		}(i)
	}
	if err := worker.Stop(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if len(worker.taskQueue) != 0 {
		t.Fatal("concurrent shutdown stranded queued tasks")
	}
}
