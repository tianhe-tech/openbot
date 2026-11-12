package scheduler

import "testing"

func TestShouldTryNLScheduleText(t *testing.T) {
	tests := []struct {
		name string
		text string
		want bool
	}{
		{name: "empty", text: "", want: false},
		{name: "confirm", text: "确认", want: true},
		{name: "cancel", text: "取消", want: true},
		{name: "slash_non_crontask", text: "/help", want: false},
		{name: "slash_crontask_nl", text: "/crontask 每天9点提醒我", want: true},
		{name: "plain_schedule_text", text: "每天早上9点提醒我看日报", want: true},
		{name: "plain_non_schedule_text", text: "请帮我看一下这个报错", want: false},
		// An update instruction often names no frequency at all: "把检查任务
		// 改到下午3点" has no 每天/提醒, so without an update hint the gate
		// would drop it and the update flow would be unreachable.
		{name: "update_instruction", text: "把检查任务改到下午3点", want: true},
		{name: "update_instruction_explicit", text: "修改任务 cron-1 改到 0 0 8 * * *", want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := ShouldTryNLScheduleText(tc.text)
			if got != tc.want {
				t.Fatalf("ShouldTryNLScheduleText(%q) = %v, want %v", tc.text, got, tc.want)
			}
		})
	}
}
