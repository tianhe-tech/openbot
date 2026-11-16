package skillgen

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/user/opencode-gateway/internal/adapters/base"
	"github.com/user/opencode-gateway/internal/asyncwork"
	"github.com/user/opencode-gateway/internal/memstore"
	"github.com/user/opencode-gateway/internal/opencode"
)

type draftFunc func(context.Context, DraftInput) (DraftOutput, error)

func (f draftFunc) Draft(ctx context.Context, in DraftInput) (DraftOutput, error) { return f(ctx, in) }

type recordedNotice struct {
	adapter, user, id, stage, reason string
	approval                         bool
}
type recordingNotifier struct {
	mu                   sync.Mutex
	candidates, failures []recordedNotice
	done                 chan struct{}
}

func (n *recordingNotifier) NotifyCandidate(adapter, user, id, title string, approval bool) error {
	n.mu.Lock()
	n.candidates = append(n.candidates, recordedNotice{adapter: adapter, user: user, id: id, approval: approval})
	n.mu.Unlock()
	if n.done != nil {
		n.done <- struct{}{}
	}
	return nil
}
func (n *recordingNotifier) NotifyFailure(adapter, user, stage, reason string) error {
	n.mu.Lock()
	n.failures = append(n.failures, recordedNotice{adapter: adapter, user: user, stage: stage, reason: reason})
	n.mu.Unlock()
	if n.done != nil {
		n.done <- struct{}{}
	}
	return nil
}
func (n *recordingNotifier) snapshot() ([]recordedNotice, []recordedNotice) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]recordedNotice(nil), n.candidates...), append([]recordedNotice(nil), n.failures...)
}

func pipelineEvent() opencode.SkillCandidateEvent {
	return opencode.SkillCandidateEvent{Trigger: opencode.SkillTriggerHandoff, Adapter: "wechat", UserID: "alice", ThreadID: "chat", SessionID: "ses_history"}
}
func pipelineOutput(model string) DraftOutput {
	return DraftOutput{Title: "test-skill", SkillMD: "---\nname: test-skill\ndescription: test\n---\nprocedure", Score: 0.9, ModelID: model, Action: "create"}
}
func newPipelineService(t *testing.T, drafter Drafter) (*Service, *memstore.Store, *recordingNotifier) {
	t.Helper()
	root := t.TempDir()
	store, err := memstore.Open(filepath.Join(root, "memory.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(r.URL.Path, "/message") {
			_, _ = w.Write([]byte(`[{"info":{"role":"user"},"parts":[{"type":"text","text":"repeatable work"}]},{"info":{"role":"assistant"},"parts":[{"type":"text","text":"reusable procedure"}]}]`))
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(server.Close)
	cfg := DefaultConfig()
	cfg.Enabled, cfg.ModelSelfSelect = true, false
	cfg.DraftModel = "model-a"
	cfg.AlternateModels = []string{"model-b"}
	cfg.PerModelTimeout = 100 * time.Millisecond
	cfg.CandidateDir = filepath.Join(root, "candidates")
	cfg.InstallDir = filepath.Join(root, "installed")
	cfg.ReferenceSkillPath = filepath.Join(root, "missing-reference.md")
	cfg, err = ResolveConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	notifier := &recordingNotifier{}
	service := NewService(cfg, store, opencode.NewClient(server.URL, ""), nil, drafter, notifier)
	return service, store, notifier
}

func TestPipelineModelFailuresNotifyOnceAndStopUnsafeFallback(t *testing.T) {
	for _, mode := range []string{"all models fail", "stop unconfirmed", "parent canceled", "declined", "low confidence"} {
		t.Run(mode, func(t *testing.T) {
			var calls int
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			service, store, notifier := newPipelineService(t, draftFunc(func(ctx context.Context, in DraftInput) (DraftOutput, error) {
				calls++
				switch mode {
				case "stop unconfirmed":
					return DraftOutput{}, &opencode.RemoteStopUnconfirmedError{SessionID: "ses_unstopped", Cause: errors.New("secret-upstream-payload")}
				case "parent canceled":
					cancel()
					return DraftOutput{}, ctx.Err()
				case "declined":
					return DraftOutput{}, nil
				case "low confidence":
					out := pipelineOutput(in.ModelID)
					out.Score = 0.1
					return out, nil
				default:
					return DraftOutput{}, errors.New("secret-upstream-payload")
				}
			}))
			err := service.run(ctx, pipelineEvent())
			candidates, failures := notifier.snapshot()
			if mode == "declined" || mode == "low confidence" {
				if err != nil || len(candidates) != 0 || len(failures) != 0 {
					t.Fatalf("deliberate skip notified a failure: error=%v candidates=%v failures=%v", err, candidates, failures)
				}
			} else {
				if err == nil || len(candidates) != 0 || len(failures) != 1 {
					t.Fatalf("failure was silent or looked successful: error=%v candidates=%v failures=%v", err, candidates, failures)
				}
				if strings.Contains(failures[0].reason, "secret-upstream-payload") {
					t.Fatal("upstream payload leaked into user failure notification")
				}
				if mode == "stop unconfirmed" && failures[0].stage != "stop_unconfirmed" {
					t.Fatalf("remote uncertainty was not distinguished: %+v", failures[0])
				}
				if mode == "parent canceled" && failures[0].stage != "budget" {
					t.Fatalf("cancellation stage lost: %+v", failures[0])
				}
			}
			wantCalls := 1
			if mode == "all models fail" {
				wantCalls = 2
			}
			if calls != wantCalls {
				t.Fatalf("wrong model fallback count: got=%d want=%d", calls, wantCalls)
			}
			rows, err := store.ListSkillCandidatesByStatus("", 20)
			if err != nil || len(rows) != 0 {
				t.Fatalf("failed draft persisted an approvable candidate: %+v err=%v", rows, err)
			}
		})
	}
}

func TestPipelineStorageAndInstallFailuresNeverAnnounceSuccess(t *testing.T) {
	for _, stage := range []string{"storage", "install", "draft_file"} {
		t.Run(stage, func(t *testing.T) {
			service, store, notifier := newPipelineService(t, draftFunc(func(_ context.Context, in DraftInput) (DraftOutput, error) { return pipelineOutput(in.ModelID), nil }))
			service.cfg.ApprovalRequired = false
			switch stage {
			case "storage":
				if err := store.Close(); err != nil {
					t.Fatal(err)
				}
			case "install", "draft_file":
				path := service.cfg.InstallDir
				if stage == "draft_file" {
					path = service.cfg.CandidateDir
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("not a directory"), 0644); err != nil {
					t.Fatal(err)
				}
			}
			err := service.run(context.Background(), pipelineEvent())
			candidates, failures := notifier.snapshot()
			if err == nil || len(candidates) != 0 || len(failures) != 1 || failures[0].stage != stage {
				t.Fatalf("failed %s looked successful: error=%v candidates=%v failures=%v", stage, err, candidates, failures)
			}
			if stage == "storage" {
				if _, err := os.Stat(filepath.Join(service.cfg.InstallDir, "test-skill", "SKILL.md")); !os.IsNotExist(err) {
					t.Fatalf("installed a skill before persistence succeeded: %v", err)
				}
			}
			if stage == "install" {
				rows, err := store.ListSkillCandidatesByStatus("", 20)
				if err != nil || len(rows) != 1 || rows[0].Status != memstore.SkillStatusDraft {
					t.Fatalf("failed install recorded an approved candidate: %+v err=%v", rows, err)
				}
			}
		})
	}
}

func TestPipelineModelTimeoutsRespectRemainingParentBudget(t *testing.T) {
	var deadlines []time.Time
	service, _, notifier := newPipelineService(t, draftFunc(func(ctx context.Context, _ DraftInput) (DraftOutput, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("model attempt has no deadline")
		}
		deadlines = append(deadlines, deadline)
		<-ctx.Done()
		return DraftOutput{}, ctx.Err()
	}))
	service.cfg.PerModelTimeout = 300 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	parentDeadline, _ := ctx.Deadline()
	err := service.run(ctx, pipelineEvent())
	if !errors.Is(err, context.DeadlineExceeded) || len(deadlines) != 2 {
		t.Fatalf("fallback did not obey deadlines: deadlines=%v error=%v", deadlines, err)
	}
	if !deadlines[0].Before(parentDeadline) || !deadlines[1].Equal(parentDeadline) {
		t.Fatalf("child budgets escaped remaining total budget: children=%v parent=%v", deadlines, parentDeadline)
	}
	candidates, failures := notifier.snapshot()
	if len(candidates) != 0 || len(failures) != 1 || failures[0].stage != "budget" {
		t.Fatalf("budget exhaustion did not notify once: candidates=%v failures=%v", candidates, failures)
	}
}

func TestPipelineAutoInstallPersistsBeforeSuccessNotification(t *testing.T) {
	service, store, notifier := newPipelineService(t, draftFunc(func(_ context.Context, in DraftInput) (DraftOutput, error) {
		return pipelineOutput(in.ModelID), nil
	}))
	service.cfg.ApprovalRequired = false
	service.notifier = &checkingInstallNotifier{t: t, store: store, installDir: service.cfg.InstallDir, recordingNotifier: notifier}
	if err := service.run(context.Background(), pipelineEvent()); err != nil {
		t.Fatal(err)
	}
	candidates, failures := notifier.snapshot()
	if len(candidates) != 1 || candidates[0].approval || len(failures) != 0 {
		t.Fatalf("auto-install notification incorrect: candidates=%v failures=%v", candidates, failures)
	}
}

type checkingInstallNotifier struct {
	*recordingNotifier
	t          *testing.T
	store      *memstore.Store
	installDir string
}

func (n *checkingInstallNotifier) NotifyCandidate(adapter, user, id, title string, approval bool) error {
	c, err := n.store.GetSkillCandidate(id)
	if err != nil {
		n.t.Fatal(err)
	}
	wantPath := filepath.Join(n.installDir, "test-skill", "SKILL.md")
	if c.Status != memstore.SkillStatusApproved || c.DraftPath != wantPath {
		n.t.Fatalf("success announced before installed status persisted: %+v", c)
	}
	body, err := os.ReadFile(wantPath)
	if err != nil || string(body) != c.SkillMD {
		n.t.Fatalf("success announced without installed body: error=%v body=%q", err, body)
	}
	return n.recordingNotifier.NotifyCandidate(adapter, user, id, title, approval)
}

func TestPipelineSuccessCanBeReviewedOnlyByResolvedRecipient(t *testing.T) {
	var calls int
	service, store, notifier := newPipelineService(t, draftFunc(func(_ context.Context, in DraftInput) (DraftOutput, error) {
		calls++
		if in.ModelID == "model-a" {
			return DraftOutput{}, errors.New("first model unavailable")
		}
		return pipelineOutput(in.ModelID), nil
	}))
	if err := service.run(context.Background(), pipelineEvent()); err != nil {
		t.Fatal(err)
	}
	candidates, failures := notifier.snapshot()
	if calls != 2 || len(candidates) != 1 || len(failures) != 0 || !candidates[0].approval {
		t.Fatalf("fallback success notification wrong: calls=%d candidates=%v failures=%v", calls, candidates, failures)
	}
	c, err := store.GetSkillCandidate(candidates[0].id)
	if err != nil || c.UserID != "alice" || c.Adapter != "wechat" || c.Status != memstore.SkillStatusPendingReview {
		t.Fatalf("candidate ownership lost: %+v err=%v", c, err)
	}
	for _, cmd := range []string{"/skill-view ", "/skill-approve ", "/skill-reject "} {
		reply, _ := service.HandleCommand("wechat", "bob", cmd+c.ID)
		if !strings.HasPrefix(reply, "❌") {
			t.Fatalf("another user could review candidate: command=%s reply=%s", cmd, reply)
		}
	}
	for i := 0; i < 25; i++ {
		if err := store.SaveSkillCandidate(memstore.SkillCandidate{ID: fmt.Sprintf("other-%d", i), Adapter: "wechat", UserID: "bob", Title: "other-skill", Status: memstore.SkillStatusPendingReview, CreatedAt: time.Now().Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	list, _ := service.HandleCommand("wechat", "alice", "/skill-list")
	if !strings.Contains(list, c.ID) || strings.Contains(list, "other-skill") {
		t.Fatalf("listing paginated before recipient filtering: %s", list)
	}
	reply, _ := service.HandleCommand("wechat", "alice", "/skill-approve "+c.ID)
	if !strings.HasPrefix(reply, "✅") {
		t.Fatalf("owner could not approve candidate: %s", reply)
	}
	c, err = store.GetSkillCandidate(c.ID)
	if err != nil || c.Status != memstore.SkillStatusApproved || c.DraftPath != filepath.Join(service.cfg.InstallDir, "test-skill", "SKILL.md") {
		t.Fatalf("approval did not share resolved install path: %+v err=%v", c, err)
	}
	if _, err := os.Stat(c.DraftPath); err != nil {
		t.Fatal(err)
	}
}

func TestRecipientResolvedBeforeEnqueueAndSnapshotIsConsistent(t *testing.T) {
	inputs := make(chan DraftInput, 1)
	service, store, notifier := newPipelineService(t, draftFunc(func(_ context.Context, in DraftInput) (DraftOutput, error) {
		inputs <- in
		return pipelineOutput(in.ModelID), nil
	}))
	notifier.done = make(chan struct{}, 1)
	queue := asyncwork.New(4)
	queue.Start(context.Background())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		queue.Stop(ctx)
	})
	service.queue = queue
	var resolutions atomic.Int32
	service.cfg.ResolveRecipient = func(adapter string) (string, bool) {
		if adapter != "wechat" {
			t.Errorf("recipient resolution crossed adapters: %s", adapter)
		}
		if resolutions.Add(1) == 1 {
			return "alice", true
		}
		return "bob", true
	}
	ev := pipelineEvent()
	ev.UserID = ""
	service.OnSkillCandidate(ev)
	select {
	case <-notifier.done:
	case <-time.After(3 * time.Second):
		t.Fatal("candidate never notified")
	}
	in := <-inputs
	candidates, failures := notifier.snapshot()
	if resolutions.Load() != 1 || in.UserID != "alice" || len(candidates) != 1 || len(failures) != 0 || candidates[0].user != "alice" {
		t.Fatalf("recipient changed between admission and notify: resolutions=%d input=%+v candidates=%v failures=%v", resolutions.Load(), in, candidates, failures)
	}
	c, err := store.GetSkillCandidate(candidates[0].id)
	if err != nil || c.UserID != "alice" || c.Adapter != in.Adapter {
		t.Fatalf("candidate does not share resolved recipient: %+v err=%v", c, err)
	}
}

func TestUnresolvableRecipientsAndInternalThreadsDoNotGenerate(t *testing.T) {
	service, _, _ := newPipelineService(t, draftFunc(func(context.Context, DraftInput) (DraftOutput, error) {
		t.Error("undeliverable/internal event started generation")
		return DraftOutput{}, nil
	}))
	queue := asyncwork.New(4)
	service.queue = queue
	service.cfg.ResolveRecipient = func(string) (string, bool) { return "", false }
	for _, mode := range []string{"unresolved", "cron-task", "skillgen-draft-chat", "skillgen adapter"} {
		ev := pipelineEvent()
		switch mode {
		case "unresolved":
			ev.UserID = ""
		case "skillgen adapter":
			ev.Adapter = "skillgen"
		default:
			ev.ThreadID = mode
		}
		service.OnSkillCandidate(ev)
	}
	if enqueued, _, _, _ := queue.Stats(); enqueued != 0 {
		t.Fatalf("invalid recipient/internal event enqueued work: %d", enqueued)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	queue.Stop(ctx)
}

func TestFailureNotificationLimitIsIndependentPerRecipient(t *testing.T) {
	notifier := &recordingNotifier{}
	service := NewService(Config{MaxPerDay: 1}, nil, nil, nil, nil, notifier)
	ev := pipelineEvent()
	for i := 0; i < 5; i++ {
		if err := service.notifyFailure(ev, "draft"); err != nil {
			t.Fatal(err)
		}
	}
	ev.UserID = "bob"
	if err := service.notifyFailure(ev, "draft"); err != nil {
		t.Fatal(err)
	}
	ev.UserID, ev.Adapter = "alice", "feishu"
	if err := service.notifyFailure(ev, "draft"); err != nil {
		t.Fatal(err)
	}
	_, failures := notifier.snapshot()
	if len(failures) != 5 {
		t.Fatalf("failure limits used candidate caps or crossed recipients: %+v", failures)
	}
	service.failureNotices[[2]string{"wechat", "alice"}] = []time.Time{time.Now().Add(-25 * time.Hour)}
	service.failureNotices[[2]string{"expired", "user"}] = []time.Time{time.Now().Add(-25 * time.Hour)}
	ev = pipelineEvent()
	if err := service.notifyFailure(ev, "draft"); err != nil {
		t.Fatal(err)
	}
	if _, exists := service.failureNotices[[2]string{"expired", "user"}]; exists {
		t.Fatal("failure limiter retained expired recipient state")
	}
}

func TestRegistryNotifierRejectsMissingDeliveryTargets(t *testing.T) {
	for _, notifier := range []*RegistryNotifier{nil, NewRegistryNotifier(nil), NewRegistryNotifier(base.NewAdapterRegistry())} {
		if err := notifier.NotifyFailure("wechat", "alice", "draft", "failed"); err == nil {
			t.Fatal("missing registry/adapter silently accepted failure notification")
		}
		if err := notifier.NotifyCandidate("wechat", "alice", "candidate", "title", true); err == nil {
			t.Fatal("missing registry/adapter silently accepted candidate notification")
		}
	}
}
