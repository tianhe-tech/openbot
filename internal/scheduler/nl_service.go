package scheduler

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// NLScheduleRequest is the adapter-level request payload for NL scheduling.
type NLScheduleRequest struct {
	AdapterType string
	UserID      string
	Channel     string
	Text        string
	Timezone    string
	Metadata    map[string]interface{}

	// ForceCreate hints that the text originates from an explicit /crontask command,
	// so an unknown intent should be coerced into a Create attempt.
	ForceCreate bool
}

// NLScheduleResponse is returned to adapter for user-facing replies.
type NLScheduleResponse struct {
	Handled      bool
	NeedConfirm  bool
	Message      string
	Intent       *NLScheduleIntent
	PendingState *NLSchedulePendingState
}

// NLScheduleService coordinates parser, pending state, and scheduler actions.
type NLScheduleService struct {
	cron   *CronScheduler
	task   *TaskScheduler
	parser NLScheduleIntentParser
	state  NLScheduleStateStore
	nowFn  func() time.Time
}

func NewNLScheduleService(
	cron *CronScheduler,
	task *TaskScheduler,
	parser NLScheduleIntentParser,
	state NLScheduleStateStore,
) *NLScheduleService {
	if parser == nil {
		parser = NewRuleBasedNLScheduleIntentParser()
	}
	if state == nil {
		state = NewInMemoryNLScheduleStateStore(10 * time.Minute)
	}
	return &NLScheduleService{
		cron:   cron,
		task:   task,
		parser: parser,
		state:  state,
		nowFn:  time.Now,
	}
}

// HandleText handles one piece of text and returns NL scheduling response.
func (s *NLScheduleService) HandleText(ctx context.Context, req NLScheduleRequest) (*NLScheduleResponse, error) {
	req.Text = strings.TrimSpace(req.Text)
	if req.Text == "" {
		return &NLScheduleResponse{Handled: false}, nil
	}

	if isCancelReply(req.Text) {
		if _, ok := s.state.Get(req.UserID, req.AdapterType); ok {
			s.state.Delete(req.UserID, req.AdapterType)
			return &NLScheduleResponse{Handled: true, Message: "已取消本次定时任务操作。"}, nil
		}
		return &NLScheduleResponse{Handled: false}, nil
	}

	if isConfirmReply(req.Text) {
		pending, ok := s.state.Get(req.UserID, req.AdapterType)
		if !ok || pending.Draft == nil {
			return &NLScheduleResponse{Handled: false}, nil
		}
		msg, err := s.executeIntent(ctx, req, pending.Draft)
		if err != nil {
			return &NLScheduleResponse{
				Handled:      true,
				NeedConfirm:  true,
				Intent:       pending.Draft,
				PendingState: pending,
				Message:      fmt.Sprintf("⚠️ 执行失败：%v\n请补充更明确的时间（例如 每天早上9点 或标准 cron）后再回复“确认”，或回复“取消”。", err),
			}, nil
		}
		s.state.Delete(req.UserID, req.AdapterType)
		return &NLScheduleResponse{Handled: true, Message: msg, Intent: pending.Draft}, nil
	}

	intent, err := s.parser.Parse(req.Text)
	if err != nil {
		return nil, err
	}
	if intent == nil || intent.Action == NLScheduleActionUnknown {
		if !req.ForceCreate {
			return &NLScheduleResponse{Handled: false}, nil
		}
		// Text came from an explicit /crontask command: coerce into a Create intent.
		intent = &NLScheduleIntent{
			Action:       NLScheduleActionCreate,
			OriginalText: req.Text,
			Content:      req.Text,
			Confidence:   0.6,
		}
		intent.CronExpr = extractCronExpression(req.Text)
		if intent.CronExpr == "" {
			intent.CronExpr = inferCronFromNaturalText(req.Text)
		}
		if intent.CronExpr == "" {
			intent.Ambiguities = append(intent.Ambiguities, "未解析出标准 cron 表达式，请补充时间（例如 每天早上9点）")
		}
		intent.RequiresConfirm = true
		intent.ConfirmMessage = defaultConfirmMessage(intent)
		intent.Normalize()
	}

	if !intent.IsWriteAction() {
		msg, execErr := s.executeIntent(ctx, req, intent)
		if execErr != nil {
			return nil, execErr
		}
		return &NLScheduleResponse{Handled: true, Intent: intent, Message: msg}, nil
	}

	pending := &NLSchedulePendingState{
		UserID:      req.UserID,
		AdapterType: req.AdapterType,
		Channel:     req.Channel,
		Draft:       intent,
		CreatedAt:   s.nowFn(),
	}
	s.state.Upsert(pending)

	msg := buildDraftMessage(intent)
	return &NLScheduleResponse{
		Handled:      true,
		NeedConfirm:  true,
		Message:      msg,
		Intent:       intent,
		PendingState: pending,
	}, nil
}

func (s *NLScheduleService) executeIntent(ctx context.Context, req NLScheduleRequest, intent *NLScheduleIntent) (string, error) {
	s.state.Cleanup(s.nowFn())
	if s.cron == nil {
		return "", fmt.Errorf("cron scheduler is not configured")
	}
	_ = ctx

	switch intent.Action {
	case NLScheduleActionList:
		tasks := s.cron.GetScheduledTasksByAdapter(req.AdapterType)
		if len(tasks) == 0 {
			return "当前没有定时任务。", nil
		}
		return fmt.Sprintf("当前共有 %d 个定时任务。", len(tasks)), nil

	case NLScheduleActionInfo:
		task, err := s.findTask(req.AdapterType, intent.Selector)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("任务详情：ID=%s 名称=%s Cron=%s 启用=%t", task.ID, task.Name, task.CronExpr, task.Enabled), nil

	case NLScheduleActionEnable:
		task, err := s.findTask(req.AdapterType, intent.Selector)
		if err != nil {
			return "", err
		}
		if err := s.cron.EnableTask(task.ID); err != nil {
			return "", err
		}
		return fmt.Sprintf("已启用任务：%s", task.ID), nil

	case NLScheduleActionDisable:
		task, err := s.findTask(req.AdapterType, intent.Selector)
		if err != nil {
			return "", err
		}
		if err := s.cron.DisableTask(task.ID); err != nil {
			return "", err
		}
		return fmt.Sprintf("已禁用任务：%s", task.ID), nil

	case NLScheduleActionDelete:
		task, err := s.findTask(req.AdapterType, intent.Selector)
		if err != nil {
			return "", err
		}
		if err := s.cron.RemoveScheduledTask(task.ID); err != nil {
			return "", err
		}
		return fmt.Sprintf("已删除任务：%s", task.ID), nil

	case NLScheduleActionCreate:
		if strings.TrimSpace(intent.CronExpr) == "" {
			return "", fmt.Errorf("cannot create task: unresolved cron expression")
		}
		now := s.nowFn()
		task := &ScheduledTask{
			Name:        nonEmpty(intent.Name, "自然语言任务"),
			Description: fmt.Sprintf("通过自然语言创建 (用户: %s)", req.UserID),
			Type:        TaskTypeAgent,
			CronExpr:    intent.CronExpr,
			Enabled:     true,
			AdapterType: req.AdapterType,
			Channel:     req.Channel,
			Content:     intent.Content,
			Agent:       intent.Agent,
			Metadata: map[string]interface{}{
				"created_by":   req.UserID,
				"created_from": req.AdapterType,
				"source":       "natural_language",
			},
			CreatedAt: now,
			UpdatedAt: now,
		}
		if req.Metadata != nil {
			for k, v := range req.Metadata {
				task.Metadata[k] = v
			}
		}
		if err := s.cron.AddScheduledTask(task); err != nil {
			return "", err
		}
		return fmt.Sprintf("已创建定时任务：%s (%s)", task.ID, task.Name), nil

	case NLScheduleActionRunOnce:
		if s.task == nil {
			return "", fmt.Errorf("task scheduler is not configured")
		}
		runTask := NewTask(TaskTypeAgent, req.AdapterType, req.UserID, intent.Content)
		runTask.Channel = req.Channel
		runTask.Agent = intent.Agent
		runTask.Metadata["source"] = "natural_language_run_once"
		if err := s.task.SubmitTask(runTask); err != nil {
			return "", err
		}
		return fmt.Sprintf("已提交一次性执行任务：%s", runTask.ID), nil

	case NLScheduleActionUpdate:
		return "", fmt.Errorf("update flow is not implemented in PR-1 skeleton")
	}

	return "", fmt.Errorf("unsupported action: %s", intent.Action)
}

func (s *NLScheduleService) findTask(adapterType string, selector NLScheduleTaskSelector) (*ScheduledTask, error) {
	if strings.TrimSpace(selector.TaskID) != "" {
		return s.cron.GetScheduledTask(selector.TaskID)
	}
	nameContains := strings.TrimSpace(selector.NameContains)
	if nameContains == "" {
		return nil, fmt.Errorf("task selector is required")
	}

	tasks := s.cron.GetScheduledTasksByAdapter(adapterType)
	var matched []*ScheduledTask
	for _, task := range tasks {
		if strings.Contains(task.Name, nameContains) {
			matched = append(matched, task)
		}
	}
	if len(matched) == 0 {
		return nil, fmt.Errorf("task not found: %s", nameContains)
	}
	if len(matched) > 1 {
		return nil, fmt.Errorf("multiple tasks matched: %s", nameContains)
	}
	return matched[0], nil
}

func buildDraftMessage(intent *NLScheduleIntent) string {
	if intent == nil {
		return "未识别到有效任务。"
	}
	var b strings.Builder
	b.WriteString("已解析到定时任务草稿:\n")
	b.WriteString(fmt.Sprintf("- 动作: %s\n", intent.Action))
	if intent.Selector.TaskID != "" {
		b.WriteString(fmt.Sprintf("- 任务ID: %s\n", intent.Selector.TaskID))
	}
	if intent.Name != "" {
		b.WriteString(fmt.Sprintf("- 名称: %s\n", intent.Name))
	}
	if intent.CronExpr != "" {
		b.WriteString(fmt.Sprintf("- Cron: %s\n", intent.CronExpr))
	}
	if intent.TimeExpression != "" {
		b.WriteString(fmt.Sprintf("- 时间描述: %s\n", intent.TimeExpression))
	}
	if intent.Content != "" {
		b.WriteString(fmt.Sprintf("- 内容: %s\n", intent.Content))
	}
	if intent.Agent != "" {
		b.WriteString(fmt.Sprintf("- Agent: %s\n", intent.Agent))
	}
	if len(intent.Ambiguities) > 0 {
		b.WriteString("- 待补充:\n")
		for _, a := range intent.Ambiguities {
			b.WriteString("  * ")
			b.WriteString(a)
			b.WriteString("\n")
		}
	}
	if intent.ConfirmMessage != "" {
		b.WriteString("\n")
		b.WriteString(intent.ConfirmMessage)
	}
	return b.String()
}

func nonEmpty(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func isConfirmReply(text string) bool {
	norm := strings.ToLower(strings.TrimSpace(text))
	return norm == "确认" || norm == "是" || norm == "yes" || norm == "y" || norm == "ok" || norm == "1"
}

func isCancelReply(text string) bool {
	norm := strings.ToLower(strings.TrimSpace(text))
	return norm == "取消" || norm == "否" || norm == "no" || norm == "n" || norm == "0"
}
