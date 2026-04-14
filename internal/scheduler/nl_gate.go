package scheduler

import "strings"

// scheduleMaxRunes is the maximum message length (in runes) for NL scheduling routing.
// A scheduling request is always a short, focused instruction. Long messages are
// regular conversation that may incidentally contain frequency words (每天/每周/分钟)
// and must NOT be mistaken for scheduling commands.
const scheduleMaxRunes = 60

// ShouldTryNLScheduleText decides whether a text should be routed to NL scheduling.
func ShouldTryNLScheduleText(text string) bool {
	norm := strings.ToLower(strings.TrimSpace(text))
	if norm == "" {
		return false
	}

	// Confirmation/cancel should always pass so pending draft can be consumed.
	// These are always short, no length check needed.
	switch norm {
	case "确认", "是", "yes", "y", "ok", "1", "取消", "否", "no", "n", "0":
		return true
	}

	// Explicit /crontask command always passes (user intent is unambiguous).
	if strings.HasPrefix(norm, "/crontask") {
		return true
	}

	// Non-crontask slash commands never route to scheduling.
	if strings.HasPrefix(norm, "/") {
		return false
	}

	// Long messages are normal conversation. Keyword scanning on long text
	// produces too many false positives (e.g., "我每天都会检查" in a bug report).
	// Only short, focused messages are candidates for scheduling intent.
	if len([]rune(norm)) > scheduleMaxRunes {
		return false
	}

	hints := []string{
		"定时", "cron", "提醒", "闹钟", "schedule",
		"每天", "每周", "每月", "工作日", "分钟", "小时", "下周", "明天", "run once", "试运行",
	}
	for _, hint := range hints {
		if strings.Contains(norm, hint) {
			return true
		}
	}

	return false
}
