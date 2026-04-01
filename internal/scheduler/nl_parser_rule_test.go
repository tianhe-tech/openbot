package scheduler

import "testing"

func TestRuleBasedParserActionDetection(t *testing.T) {
	p := NewRuleBasedNLScheduleIntentParser()

	tests := []struct {
		name   string
		input  string
		action NLScheduleAction
	}{
		{name: "list", input: "请列出定时任务", action: NLScheduleActionList},
		{name: "delete", input: "删除任务 cron-123", action: NLScheduleActionDelete},
		{name: "run once", input: "把这个任务试运行一次", action: NLScheduleActionRunOnce},
		{name: "create", input: "每天早上9点提醒我看监控", action: NLScheduleActionCreate},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			intent, err := p.Parse(tc.input)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if intent.Action != tc.action {
				t.Fatalf("Parse() action = %s, want %s", intent.Action, tc.action)
			}
		})
	}
}

func TestRuleBasedParserExtractTaskID(t *testing.T) {
	p := NewRuleBasedNLScheduleIntentParser()
	intent, err := p.Parse("禁用任务 cron-987654321")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if intent.Selector.TaskID != "cron-987654321" {
		t.Fatalf("task id = %s, want cron-987654321", intent.Selector.TaskID)
	}
}

func TestRuleBasedParserExtractCronExpr(t *testing.T) {
	p := NewRuleBasedNLScheduleIntentParser()
	intent, err := p.Parse("/crontask add 0 0 9 * * * 每天早上提醒我站会")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if intent.Action != NLScheduleActionCreate {
		t.Fatalf("Parse() action = %s, want %s", intent.Action, NLScheduleActionCreate)
	}
	if intent.CronExpr != "0 0 9 * * *" {
		t.Fatalf("cron = %q, want %q", intent.CronExpr, "0 0 9 * * *")
	}
}

func TestRuleBasedParserInferDailyCronFromChineseTime(t *testing.T) {
	p := NewRuleBasedNLScheduleIntentParser()
	intent, err := p.Parse("每天早上9点提醒我检查系统")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if intent.Action != NLScheduleActionCreate {
		t.Fatalf("Parse() action = %s, want %s", intent.Action, NLScheduleActionCreate)
	}
	if intent.CronExpr != "0 0 9 * * *" {
		t.Fatalf("cron = %q, want %q", intent.CronExpr, "0 0 9 * * *")
	}
}

func TestRuleBasedParserInferBareHour(t *testing.T) {
	p := NewRuleBasedNLScheduleIntentParser()
	intent, err := p.Parse("每小时查看一次cpu使用率")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if intent.Action != NLScheduleActionCreate {
		t.Fatalf("Parse() action = %s, want create", intent.Action)
	}
	if intent.CronExpr != "0 0 * * * *" {
		t.Fatalf("cron = %q, want %q", intent.CronExpr, "0 0 * * * *")
	}
}

func TestRuleBasedParserInferBareMinute(t *testing.T) {
	p := NewRuleBasedNLScheduleIntentParser()
	intent, err := p.Parse("每分钟查看一次cpu使用率")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if intent.Action != NLScheduleActionCreate {
		t.Fatalf("Parse() action = %s, want create", intent.Action)
	}
	if intent.CronExpr != "0 * * * * *" {
		t.Fatalf("cron = %q, want %q", intent.CronExpr, "0 * * * * *")
	}
}

func TestRuleBasedParserAvoidSimpleFalsePositive(t *testing.T) {
	p := NewRuleBasedNLScheduleIntentParser()
	intent, err := p.Parse("每次启动都要检查日志")
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if intent.Action != NLScheduleActionUnknown {
		t.Fatalf("Parse() action = %s, want %s", intent.Action, NLScheduleActionUnknown)
	}
}
