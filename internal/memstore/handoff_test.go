package memstore

import (
	"strconv"
	"strings"
	"testing"
)

// A recovery loop used to nest preambles (A → B → C …): each cycle re-embedded
// the whole previous prompt, so the real user text ended up after several
// 【当前用户消息】 markers and the request grew without bound until the upstream
// model stopped producing output. Unwrapping must always land on the innermost
// (last) marker.
func TestSanitizeHandoffContent_UnwrapsDeeplyNestedPreamble(t *testing.T) {
	real := "把昨天那个脚本再跑一遍"
	nested := real
	for i := 0; i < 6; i++ {
		nested = "【会话接续·自动恢复】\n上一会话因服务端调度异常中断，以下是压缩摘要：\n\n" +
			"【目标】gen" + strconv.Itoa(i) + "\n\n【上一轮用户提问（未获回复，已自动重发）】\ngen" + strconv.Itoa(i) + "\n\n" +
			"---\n\n【当前用户消息】\n" + nested
	}

	got := SanitizeHandoffContent("user", nested)
	if got != real {
		t.Fatalf("nested preamble not fully unwrapped: got %q, want %q", got, real)
	}
}

// BuildHandoffPreamble must not wrap an already-wrapped message, otherwise the
// recovery loop re-creates the recursion the sanitizer just removed.
func TestBuildHandoffPreamble_DoesNotNest(t *testing.T) {
	nested := "【会话接续·自动恢复】\nold summary\n\n---\n\n【当前用户消息】\n真正的用户问题"

	got := BuildHandoffPreamble("new summary", "previous question", nested)
	if n := strings.Count(got, "【会话接续·自动恢复】"); n != 1 {
		t.Fatalf("preamble nested %d generations deep, want 1:\n%s", n, got)
	}
	if !strings.Contains(got, "真正的用户问题") {
		t.Fatalf("innermost user message lost:\n%s", got)
	}
}

// The whole preamble is hard-capped so a huge resumed prompt can never be the
// reason the next turn hangs.
func TestBuildHandoffPreamble_IsCapped(t *testing.T) {
	got := BuildHandoffPreamble(
		strings.Repeat("摘", 20000),
		strings.Repeat("问", 20000),
		strings.Repeat("长", 20000),
	)
	if n := len([]rune(got)); n > maxHandoffPreambleChars {
		t.Fatalf("preamble not capped: %d runes (cap %d)", n, maxHandoffPreambleChars)
	}
	if !strings.Contains(got, "【当前用户消息】") {
		t.Fatalf("preamble lost its user-message marker:\n%s", got)
	}
}

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
