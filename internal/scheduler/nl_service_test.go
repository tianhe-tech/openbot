package scheduler

import (
	"context"
	"strings"
	"testing"
)

type staticIntentParser struct {
	intent *NLScheduleIntent
	err    error
}

func (p *staticIntentParser) Parse(text string) (*NLScheduleIntent, error) {
	return p.intent, p.err
}

func TestNLScheduleServiceCreateNeedsConfirmThenExecutes(t *testing.T) {
	cron := NewCronScheduler(nil)
	state := NewInMemoryNLScheduleStateStore(0)
	parser := &staticIntentParser{intent: &NLScheduleIntent{
		Action:         NLScheduleActionCreate,
		CronExpr:       "0 0 9 * * *",
		Name:           "每日检查",
		Content:        "检查系统状态",
		ConfirmMessage: "请确认创建该定时任务（回复：确认/取消）",
	}}
	svc := NewNLScheduleService(cron, nil, parser, state)

	ctx := context.Background()
	req := NLScheduleRequest{
		AdapterType: "dingtalk",
		UserID:      "u1",
		Channel:     "c1",
		Text:        "每天早上9点检查系统",
	}

	first, err := svc.HandleText(ctx, req)
	if err != nil {
		t.Fatalf("HandleText(first) error = %v", err)
	}
	if first == nil || !first.Handled || !first.NeedConfirm {
		t.Fatalf("first response should require confirmation")
	}
	if _, ok := state.Get("u1", "dingtalk"); !ok {
		t.Fatalf("pending state should be created")
	}

	second, err := svc.HandleText(ctx, NLScheduleRequest{
		AdapterType: "dingtalk",
		UserID:      "u1",
		Channel:     "c1",
		Text:        "确认",
	})
	if err != nil {
		t.Fatalf("HandleText(confirm) error = %v", err)
	}
	if second == nil || !second.Handled {
		t.Fatalf("confirm response should be handled")
	}
	if _, ok := state.Get("u1", "dingtalk"); ok {
		t.Fatalf("pending state should be cleared after confirm")
	}

	tasks := cron.GetScheduledTasksByAdapter("dingtalk")
	if len(tasks) != 1 {
		t.Fatalf("created tasks = %d, want 1", len(tasks))
	}
	if tasks[0].Name != "每日检查" {
		t.Fatalf("task name = %q, want %q", tasks[0].Name, "每日检查")
	}
}

func TestNLScheduleServiceCancelClearsPending(t *testing.T) {
	cron := NewCronScheduler(nil)
	state := NewInMemoryNLScheduleStateStore(0)
	parser := &staticIntentParser{intent: &NLScheduleIntent{
		Action:         NLScheduleActionCreate,
		CronExpr:       "0 0 9 * * *",
		Name:           "每日检查",
		Content:        "检查系统状态",
		ConfirmMessage: "请确认创建该定时任务（回复：确认/取消）",
	}}
	svc := NewNLScheduleService(cron, nil, parser, state)

	_, err := svc.HandleText(context.Background(), NLScheduleRequest{
		AdapterType: "feishu",
		UserID:      "u2",
		Channel:     "chat1",
		Text:        "每天9点提醒我",
	})
	if err != nil {
		t.Fatalf("HandleText(create) error = %v", err)
	}
	if _, ok := state.Get("u2", "feishu"); !ok {
		t.Fatalf("pending state should exist before cancel")
	}

	resp, err := svc.HandleText(context.Background(), NLScheduleRequest{
		AdapterType: "feishu",
		UserID:      "u2",
		Text:        "取消",
	})
	if err != nil {
		t.Fatalf("HandleText(cancel) error = %v", err)
	}
	if resp == nil || !resp.Handled {
		t.Fatalf("cancel response should be handled")
	}
	if _, ok := state.Get("u2", "feishu"); ok {
		t.Fatalf("pending state should be cleared after cancel")
	}
	if got := len(cron.GetScheduledTasksByAdapter("feishu")); got != 0 {
		t.Fatalf("created tasks = %d, want 0", got)
	}
}

func TestNLScheduleServiceForceCreateFromCrontaskCommand(t *testing.T) {
	cron := NewCronScheduler(nil)
	state := NewInMemoryNLScheduleStateStore(0)
	// Let the real parser run so ForceCreate coercion is exercised.
	svc := NewNLScheduleService(cron, nil, nil, state)

	ctx := context.Background()
	// "每小时查看一次cpu使用率" has no explicit scheduleHint, but the real parser
	// now detects it via pureFrequencyWords. Verify the full path end-to-end.
	resp, err := svc.HandleText(ctx, NLScheduleRequest{
		AdapterType: "feishu",
		UserID:      "u10",
		Channel:     "chat1",
		Text:        "每小时查看一次cpu使用率",
		ForceCreate: true,
	})
	if err != nil {
		t.Fatalf("HandleText error = %v", err)
	}
	if resp == nil || !resp.Handled || !resp.NeedConfirm {
		t.Fatalf("expected draft confirmation response")
	}
	pending, ok := state.Get("u10", "feishu")
	if !ok || pending.Draft == nil {
		t.Fatalf("pending draft should be stored")
	}
	if pending.Draft.CronExpr != "0 0 * * * *" {
		t.Fatalf("inferred cron = %q, want %q", pending.Draft.CronExpr, "0 0 * * * *")
	}

	// Confirm
	resp2, err := svc.HandleText(ctx, NLScheduleRequest{
		AdapterType: "feishu",
		UserID:      "u10",
		Channel:     "chat1",
		Text:        "确认",
	})
	if err != nil {
		t.Fatalf("confirm error = %v", err)
	}
	if resp2 == nil || !resp2.Handled || resp2.NeedConfirm {
		t.Fatalf("confirm should succeed without NeedConfirm")
	}
	tasks := cron.GetScheduledTasksByAdapter("feishu")
	if len(tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(tasks))
	}
	if tasks[0].CronExpr != "0 0 * * * *" {
		t.Fatalf("task cron = %q, want %q", tasks[0].CronExpr, "0 0 * * * *")
	}
}

func TestNLScheduleServiceConfirmFailureIsRecoverable(t *testing.T) {
	cron := NewCronScheduler(nil)
	state := NewInMemoryNLScheduleStateStore(0)
	parser := &staticIntentParser{intent: &NLScheduleIntent{
		Action:         NLScheduleActionCreate,
		CronExpr:       "",
		Name:           "每日检查",
		Content:        "检查系统状态",
		ConfirmMessage: "请确认创建该定时任务（回复：确认/取消）",
	}}
	svc := NewNLScheduleService(cron, nil, parser, state)

	_, err := svc.HandleText(context.Background(), NLScheduleRequest{
		AdapterType: "feishu",
		UserID:      "u3",
		Channel:     "chat1",
		Text:        "每天早上提醒我",
	})
	if err != nil {
		t.Fatalf("HandleText(create) error = %v", err)
	}

	resp, err := svc.HandleText(context.Background(), NLScheduleRequest{
		AdapterType: "feishu",
		UserID:      "u3",
		Channel:     "chat1",
		Text:        "确认",
	})
	if err != nil {
		t.Fatalf("HandleText(confirm) error = %v", err)
	}
	if resp == nil || !resp.Handled || !resp.NeedConfirm {
		t.Fatalf("confirm failure should return handled recoverable response")
	}
	if !strings.Contains(resp.Message, "执行失败") {
		t.Fatalf("message = %q, want contains 执行失败", resp.Message)
	}
	if _, ok := state.Get("u3", "feishu"); !ok {
		t.Fatalf("pending state should be kept after confirm failure")
	}
}

func TestNLScheduleServiceUpdateChangesCronAndKeepsIdentity(t *testing.T) {
	cron := NewCronScheduler(nil)
	existing := &ScheduledTask{
		ID:          "cron-1",
		Name:        "每日检查",
		Type:        TaskTypeAgent,
		CronExpr:    "0 0 9 * * *",
		Enabled:     true,
		AdapterType: "dingtalk",
		Channel:     "c1",
		UserID:      "u4",
		Content:     "检查系统状态",
	}
	if err := cron.AddScheduledTask(existing); err != nil {
		t.Fatalf("AddScheduledTask error = %v", err)
	}

	state := NewInMemoryNLScheduleStateStore(0)
	parser := &staticIntentParser{intent: &NLScheduleIntent{
		Action:   NLScheduleActionUpdate,
		CronExpr: "0 0 10 * * *",
		Selector: NLScheduleTaskSelector{TaskID: "cron-1"},
	}}
	svc := NewNLScheduleService(cron, nil, parser, state)

	ctx := context.Background()
	req := NLScheduleRequest{AdapterType: "dingtalk", UserID: "u4", Channel: "c1", Text: "把每日检查改到10点"}

	first, err := svc.HandleText(ctx, req)
	if err != nil {
		t.Fatalf("HandleText(draft) error = %v", err)
	}
	if first == nil || !first.Handled || !first.NeedConfirm {
		t.Fatalf("update should require confirmation before mutating")
	}
	// The draft must not have touched the scheduler yet.
	if got, _ := cron.GetScheduledTask("cron-1"); got.CronExpr != "0 0 9 * * *" {
		t.Fatalf("cron changed before confirm: %q", got.CronExpr)
	}

	second, err := svc.HandleText(ctx, NLScheduleRequest{AdapterType: "dingtalk", UserID: "u4", Channel: "c1", Text: "确认"})
	if err != nil {
		t.Fatalf("HandleText(confirm) error = %v", err)
	}
	if second == nil || !second.Handled || second.NeedConfirm {
		t.Fatalf("confirm should succeed without NeedConfirm")
	}

	updated, err := cron.GetScheduledTask("cron-1")
	if err != nil {
		t.Fatalf("GetScheduledTask error = %v", err)
	}
	if updated.CronExpr != "0 0 10 * * *" {
		t.Fatalf("cron = %q, want %q", updated.CronExpr, "0 0 10 * * *")
	}
	// Identity and payload must survive an update that only names a new time.
	if updated.ID != "cron-1" || updated.Name != "每日检查" || updated.Content != "检查系统状态" {
		t.Fatalf("update clobbered identity: %+v", updated)
	}
	if got := len(cron.GetScheduledTasksByAdapter("dingtalk")); got != 1 {
		t.Fatalf("tasks = %d, want 1 (update must not create a second task)", got)
	}
}

func TestNLScheduleServiceUpdateWithoutSelectorIsRecoverable(t *testing.T) {
	cron := NewCronScheduler(nil)
	state := NewInMemoryNLScheduleStateStore(0)
	parser := &staticIntentParser{intent: &NLScheduleIntent{
		Action:   NLScheduleActionUpdate,
		CronExpr: "0 0 10 * * *",
	}}
	svc := NewNLScheduleService(cron, nil, parser, state)

	ctx := context.Background()
	if _, err := svc.HandleText(ctx, NLScheduleRequest{AdapterType: "feishu", UserID: "u5", Channel: "chat1", Text: "改一下任务时间"}); err != nil {
		t.Fatalf("HandleText(draft) error = %v", err)
	}

	resp, err := svc.HandleText(ctx, NLScheduleRequest{AdapterType: "feishu", UserID: "u5", Channel: "chat1", Text: "确认"})
	if err != nil {
		t.Fatalf("HandleText(confirm) error = %v", err)
	}
	if resp == nil || !resp.Handled || !resp.NeedConfirm {
		t.Fatalf("missing selector should be a recoverable failure")
	}
	// The hint must name the actual problem. Telling the user to restate the
	// time would be wrong here: the time parsed fine, the target did not.
	if !strings.Contains(resp.Message, "任务") {
		t.Fatalf("message = %q, want a selector hint", resp.Message)
	}
	if strings.Contains(resp.Message, "补充更明确的时间") {
		t.Fatalf("message = %q, want a selector hint rather than a time hint", resp.Message)
	}
	if _, ok := state.Get("u5", "feishu"); !ok {
		t.Fatalf("pending state should be kept after confirm failure")
	}
}

func TestBuildDraftMessageRendersActionInChinese(t *testing.T) {
	// The draft is a user-facing confirmation prompt. Printing the raw action
	// constant ("update") leaks an internal identifier into a Chinese sentence.
	msg := buildDraftMessage(&NLScheduleIntent{
		Action:   NLScheduleActionUpdate,
		CronExpr: "0 0 10 * * *",
		Selector: NLScheduleTaskSelector{TaskID: "cron-1"},
	})
	if !strings.Contains(msg, "修改") {
		t.Fatalf("draft = %q, want the action rendered as 修改", msg)
	}
	if strings.Contains(msg, "update") {
		t.Fatalf("draft = %q, must not leak the raw action constant", msg)
	}
}
