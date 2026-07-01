package memstore

import (
	"strings"
	"testing"
)

func TestSanitizeHandoffContent_StripsThinkBlocks(t *testing.T) {
	in := "<think>internal reasoning\nmulti-line</think>real answer"
	got := SanitizeHandoffContent("assistant", in)
	if got != "real answer" {
		t.Fatalf("think block not stripped: %q", got)
	}
}

func TestSanitizeHandoffContent_StripsUnterminatedThinkTail(t *testing.T) {
	in := "user ok\n<think>cut off mid reason"
	got := SanitizeHandoffContent("assistant", in)
	if got != "user ok" {
		t.Fatalf("unterminated think not stripped: %q", got)
	}
}

func TestSanitizeHandoffContent_UnwrapsNestedHandoffPreamble(t *testing.T) {
	preamble := BuildHandoffPreamble("old summary", "prev q", "actual user text")
	got := SanitizeHandoffContent("user", preamble)
	if got != "actual user text" {
		t.Fatalf("preamble not unwrapped: %q", got)
	}
}

func TestBuildHandoffSummary_NoRecursiveGoal(t *testing.T) {
	// Simulate a second-generation handoff: the "first user turn" is itself
	// a preamble-wrapped message.
	firstUser := BuildHandoffPreamble("gen1 summary text", "prev", "继续啊")
	turns := []HandoffTurn{
		{Role: "user", Content: firstUser},
		{Role: "assistant", Content: "<think>reasoning here</think>ok"},
		{Role: "user", Content: "follow up"},
		{Role: "assistant", Content: "final"},
	}
	got := BuildHandoffSummary(turns, 2000)
	if strings.Contains(got, "【会话接续·自动恢复】") {
		t.Fatalf("recursive preamble leaked into summary:\n%s", got)
	}
	if strings.Contains(got, "<think>") {
		t.Fatalf("think tag leaked:\n%s", got)
	}
	if !strings.Contains(got, "继续啊") {
		t.Fatalf("real user anchor lost:\n%s", got)
	}
}

func TestSanitizeHandoffContent_StripsHistoricalRecallBlock(t *testing.T) {
	in := "【历史工作记忆】以下是用户过去的工作记录，供参考：\n\n## 项目概览\n- foo\n- bar\n\n---\n真实的用户请求"
	got := SanitizeHandoffContent("user", in)
	if got != "真实的用户请求" {
		t.Fatalf("recall block not stripped: %q", got)
	}
}
