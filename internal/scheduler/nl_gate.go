package scheduler

import "strings"

// ShouldTryNLScheduleText decides whether a text should be routed to NL scheduling.
func ShouldTryNLScheduleText(text string) bool {
	norm := strings.ToLower(strings.TrimSpace(text))
	if norm == "" {
		return false
	}

	// Confirmation/cancel should always pass so pending draft can be consumed.
	switch norm {
	case "确认", "是", "yes", "y", "ok", "1", "取消", "否", "no", "n", "0":
		return true
	}

	if strings.HasPrefix(norm, "/") && !strings.HasPrefix(norm, "/crontask") {
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
