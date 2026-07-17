package wechat

import "testing"

func TestStripDSMLMarkup(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "no markup passthrough",
			in:   "这是一条正常的回复。",
			want: "这是一条正常的回复。",
		},
		{
			name: "fullwidth separator block removed",
			in:   "结果：<DSML｜tool_calls>{\"name\":\"read\"}</DSML｜tool_calls>完成",
			want: "结果：完成",
		},
		{
			name: "ascii separator block removed",
			in:   "before<DSML|function_calls>payload</DSML|function_calls>after",
			want: "beforeafter",
		},
		{
			name: "multiline block removed",
			in:   "head<DSML｜tool_calls>\nline1\nline2\n</DSML｜tool_calls>tail",
			want: "headtail",
		},
		{
			name: "dangling open tag removed",
			in:   "text<DSML｜tool_calls>leftover",
			want: "textleftover",
		},
		{
			name: "dangling close tag removed",
			in:   "leftover</DSML｜tool_calls>text",
			want: "leftovertext",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := stripDSMLMarkup(tc.in); got != tc.want {
				t.Fatalf("stripDSMLMarkup(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
