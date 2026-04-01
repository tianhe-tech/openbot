package scheduler

import "testing"

func TestShouldTryNLScheduleText(t *testing.T) {
	tests := []struct {
		name  string
		text  string
		want  bool
	}{
		{name: "empty", text: "", want: false},
		{name: "confirm", text: "确认", want: true},
		{name: "cancel", text: "取消", want: true},
		{name: "slash_non_crontask", text: "/help", want: false},
		{name: "slash_crontask_nl", text: "/crontask 每天9点提醒我", want: true},
		{name: "plain_schedule_text", text: "每天早上9点提醒我看日报", want: true},
		{name: "plain_non_schedule_text", text: "请帮我看一下这个报错", want: false},
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
