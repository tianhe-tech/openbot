package scheduler

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var taskIDPattern = regexp.MustCompile(`(?i)(cron-[0-9]+|task-[0-9]+)`)
var dailyTimePattern = regexp.MustCompile(`(每天|每日)\s*(早上|上午|中午|下午|晚上)?\s*([0-2]?\d)点(?:\s*([0-5]?\d)分?)?`)
var everyMinutesPattern = regexp.MustCompile(`每\s*(\d{1,2})\s*分钟`)
var everyHoursPattern = regexp.MustCompile(`每\s*(\d{1,2})\s*小时`)
var everyBareMinutePattern = regexp.MustCompile(`每分钟`)
var everyBareHourPattern = regexp.MustCompile(`每小时`)
var pureFrequencyWords = []string{"每分钟", "每小时", "每天", "每日", "每周", "每月"}

// NLScheduleIntentParser converts user text into scheduling intent.
type NLScheduleIntentParser interface {
	Parse(text string) (*NLScheduleIntent, error)
}

// RuleBasedNLScheduleIntentParser provides a deterministic baseline parser.
type RuleBasedNLScheduleIntentParser struct{}

func NewRuleBasedNLScheduleIntentParser() *RuleBasedNLScheduleIntentParser {
	return &RuleBasedNLScheduleIntentParser{}
}

func (p *RuleBasedNLScheduleIntentParser) Parse(text string) (*NLScheduleIntent, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil, fmt.Errorf("empty input")
	}

	intent := &NLScheduleIntent{
		Action:       NLScheduleActionUnknown,
		OriginalText: trimmed,
		Confidence:   0.35,
	}

	lower := strings.ToLower(trimmed)
	scheduleHints := []string{"定时", "提醒", "cron", "schedule", "闹钟", "/crontask", "任务"}
	timeHints := []string{"每天", "每周", "每月", "工作日", "分钟", "小时", "早上", "下午", "晚上", "明天", "下周", "点", "every day", "every week", "every month", "every"}

	switch {
	case containsAny(lower, []string{"/crontask list", "定时任务列表", "列出定时任务", "查看定时任务", "list tasks", "list schedules"}):
		intent.Action = NLScheduleActionList
		intent.Confidence = 0.95
	case containsAny(lower, []string{"/crontask info", "任务详情", "查看任务", "show task", "task info"}):
		intent.Action = NLScheduleActionInfo
		intent.Confidence = 0.9
	case containsAny(lower, []string{"/crontask enable", "启用任务", "恢复任务", "enable task"}):
		intent.Action = NLScheduleActionEnable
		intent.Confidence = 0.9
	case containsAny(lower, []string{"/crontask disable", "禁用任务", "暂停任务", "disable task"}):
		intent.Action = NLScheduleActionDisable
		intent.Confidence = 0.9
	case containsAny(lower, []string{"/crontask delete", "删除任务", "移除任务", "delete task", "remove schedule"}):
		intent.Action = NLScheduleActionDelete
		intent.Confidence = 0.9
	case containsAny(lower, []string{"试运行", "立即运行", "执行一次", "run once", "test run"}):
		intent.Action = NLScheduleActionRunOnce
		intent.Confidence = 0.82
	case containsAny(lower, []string{"修改任务", "更新任务", "change schedule", "update schedule", "reschedule"}):
		intent.Action = NLScheduleActionUpdate
		intent.Confidence = 0.8
	case containsAny(lower, []string{"/crontask add", "创建定时任务", "新增定时任务"}) ||
		(containsAny(lower, scheduleHints) && containsAny(lower, timeHints)) ||
		(containsAny(lower, pureFrequencyWords) && !strings.Contains(lower, "每次")):
		intent.Action = NLScheduleActionCreate
		intent.Confidence = 0.75
	}

	if taskID := extractTaskID(lower); taskID != "" {
		intent.Selector.TaskID = taskID
	}

	if intent.Action == NLScheduleActionCreate {
		intent.CronExpr = extractCronExpression(trimmed)
		if intent.CronExpr == "" {
			intent.CronExpr = inferCronFromNaturalText(trimmed)
		}
		intent.TimeExpression = extractTimeExpression(trimmed)
		if intent.Content == "" {
			intent.Content = trimmed
		}
		if intent.TimeExpression == "" {
			intent.Ambiguities = append(intent.Ambiguities, "缺少明确时间描述")
		}
		if strings.TrimSpace(intent.CronExpr) == "" {
			intent.Ambiguities = append(intent.Ambiguities, "未解析出标准 cron 表达式")
		}
	}

	intent.RequiresConfirm = intent.IsWriteAction()
	intent.ConfirmMessage = defaultConfirmMessage(intent)
	intent.Normalize()
	return intent, nil
}

func defaultConfirmMessage(intent *NLScheduleIntent) string {
	if intent == nil {
		return ""
	}
	switch intent.Action {
	case NLScheduleActionCreate:
		return "请确认创建该定时任务（回复：确认/取消）"
	case NLScheduleActionEnable:
		return "请确认启用该任务（回复：确认/取消）"
	case NLScheduleActionDisable:
		return "请确认禁用该任务（回复：确认/取消）"
	case NLScheduleActionDelete:
		return "请确认删除该任务（回复：确认/取消）"
	case NLScheduleActionUpdate:
		return "请确认更新该任务（回复：确认/取消）"
	case NLScheduleActionRunOnce:
		return "请确认立即执行一次（回复：确认/取消）"
	default:
		return ""
	}
}

func containsAny(source string, keywords []string) bool {
	for _, kw := range keywords {
		if strings.Contains(source, kw) {
			return true
		}
	}
	return false
}

func extractTaskID(text string) string {
	m := taskIDPattern.FindStringSubmatch(text)
	if len(m) > 0 {
		return m[0]
	}
	return ""
}

func hasCronExpression(text string) bool {
	parts := strings.Fields(text)
	if len(parts) < 5 {
		return false
	}
	count := 0
	for _, p := range parts {
		if strings.ContainsAny(p, "*-/,") || isAllDigits(p) {
			count++
		}
	}
	return count >= 5
}

func extractCronExpression(text string) string {
	fields := strings.Fields(text)
	if len(fields) < 5 {
		return ""
	}

	for i := 0; i < len(fields); i++ {
		if i+6 <= len(fields) {
			candidate := fields[i : i+6]
			if isCronFields(candidate) {
				return strings.Join(candidate, " ")
			}
		}
		if i+5 <= len(fields) {
			candidate := fields[i : i+5]
			if isCronFields(candidate) {
				return strings.Join(candidate, " ")
			}
		}
	}

	return ""
}

func isCronFields(fields []string) bool {
	if len(fields) != 5 && len(fields) != 6 {
		return false
	}
	for _, f := range fields {
		if f == "" {
			return false
		}
		for _, c := range f {
			if !((c >= '0' && c <= '9') || c == '*' || c == '/' || c == '-' || c == ',' || c == '?') {
				return false
			}
		}
	}
	return true
}

func extractTimeExpression(text string) string {
	candidates := []string{"每天", "每周", "每月", "工作日", "分钟", "小时", "早上", "下午", "晚上", "明天", "下周", "every"}
	for _, c := range candidates {
		if strings.Contains(strings.ToLower(text), strings.ToLower(c)) {
			return text
		}
	}
	return ""
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func inferCronFromNaturalText(text string) string {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return ""
	}

	// Bare "每分钟" must be checked before the digit-prefixed pattern to avoid
	// falling through when no number is present.
	if everyBareMinutePattern.MatchString(trimmed) && !everyMinutesPattern.MatchString(trimmed) {
		return "0 * * * * *"
	}
	if everyBareHourPattern.MatchString(trimmed) && !everyHoursPattern.MatchString(trimmed) {
		return "0 0 * * * *"
	}

	if m := everyMinutesPattern.FindStringSubmatch(trimmed); len(m) == 2 {
		if step, err := strconv.Atoi(m[1]); err == nil && step > 0 && step <= 59 {
			return fmt.Sprintf("0 */%d * * * *", step)
		}
	}

	if m := everyHoursPattern.FindStringSubmatch(trimmed); len(m) == 2 {
		if step, err := strconv.Atoi(m[1]); err == nil && step > 0 && step <= 23 {
			return fmt.Sprintf("0 0 */%d * * *", step)
		}
	}

	m := dailyTimePattern.FindStringSubmatch(trimmed)
	if len(m) == 5 {
		period := strings.TrimSpace(m[2])
		hour, hErr := strconv.Atoi(m[3])
		if hErr != nil {
			return ""
		}
		minute := 0
		if strings.TrimSpace(m[4]) != "" {
			if mm, mErr := strconv.Atoi(m[4]); mErr == nil {
				minute = mm
			}
		}

		if hour < 0 || hour > 23 || minute < 0 || minute > 59 {
			return ""
		}
		switch period {
		case "下午", "晚上":
			if hour < 12 {
				hour += 12
			}
		case "中午":
			if hour < 11 {
				hour += 12
			}
		}
		return fmt.Sprintf("0 %d %d * * *", minute, hour)
	}

	return ""
}
