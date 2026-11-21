package skillgen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/user/opencode-gateway/internal/opencode"
)

// OpencodeDrafter uses a fresh temporary opencode session to generate the SKILL.md.
// It is the default Drafter wired in main.go; custom Drafter implementations can
// replace it (e.g., for tests) by passing their own implementation to NewService.
type draftClient interface {
	SendMessageStreamingWithEvents(context.Context, opencode.MessagePayload, opencode.StreamCallback, opencode.StreamEventCallback) (opencode.Response, error)
	ClearSkillgenSession(threadID, sessionID string)
}

type OpencodeDrafter struct {
	Client draftClient
	// ReferenceSkillPath points at an exemplar SKILL.md the drafter uses as a
	// style/anatomy guide. Resolved from InstallDir in NewOpencodeDrafter.
	ReferenceSkillPath string
}

// NewOpencodeDrafter constructs a drafter with sensible defaults.
// If referenceSkillPath is empty, it tries <installDir>/skill-creator/SKILL.md.
func NewOpencodeDrafter(c *opencode.Client, referenceSkillPath, installDir string) *OpencodeDrafter {
	if referenceSkillPath == "" {
		base := installDir
		if base == "" {
			base = "skills"
		}
		referenceSkillPath = filepath.Join(base, "skill-creator", "SKILL.md")
	}
	drafter := &OpencodeDrafter{ReferenceSkillPath: referenceSkillPath}
	if c != nil {
		drafter.Client = c
	}
	return drafter
}

// Draft asks the opencode server to convert a conversation into a SKILL.md.
func (d *OpencodeDrafter) Draft(ctx context.Context, in DraftInput) (DraftOutput, error) {
	if d == nil || d.Client == nil {
		return DraftOutput{}, fmt.Errorf("skillgen: nil drafter/client")
	}
	if len(in.Conversation) < 2 {
		return DraftOutput{}, nil // not enough evidence
	}
	exemplar := d.loadReference()
	if exemplar == "" {
		log.Printf("skillgen: drafter reference skill not found or empty at path=%q, prompt quality may degrade", d.ReferenceSkillPath)
	} else {
		log.Printf("skillgen: drafter loaded reference skill (%d chars) from %s", len(exemplar), d.ReferenceSkillPath)
	}
	in.Conversation = capConversation(in.Conversation)
	prompt := truncatePrompt(buildDraftPrompt(in, exemplar))

	// Use an isolated thread so the drafting work doesn't pollute user threads.
	threadID := fmt.Sprintf("skillgen-draft-%s-%d", in.ThreadID, time.Now().UnixNano())
	payload := opencode.MessagePayload{
		Channel:        "skillgen",
		UserID:         in.UserID,
		ThreadID:       threadID,
		Content:        prompt,
		Model:          in.ModelID, // pass selected model to opencode so it actually uses it
		Streaming:      true,       // use prompt_async to avoid sync API 500 errors
		ForegroundOnly: true,
		Internal:       true,
	}
	log.Printf("skillgen: drafter sending prompt to opencode (model=%s thread=%s streaming=true)", in.ModelID, threadID)
	var mu sync.Mutex
	var text strings.Builder
	var sessionID string
	resp, err := d.Client.SendMessageStreamingWithEvents(ctx, payload, func(string) error { return nil },
		func(event opencode.StreamEvent) error {
			mu.Lock()
			defer mu.Unlock()
			switch event.Kind {
			case opencode.StreamEventSessionReady:
				sessionID = event.SessionID
			case opencode.StreamEventTextDelta:
				text.WriteString(event.Content)
			}
			return nil
		})
	mu.Lock()
	if sessionID == "" {
		sessionID = resp.SessionID
	}
	reply := text.String()
	mu.Unlock()
	if err == nil && resp.Deferred {
		err = &opencode.RemoteStopUnconfirmedError{SessionID: sessionID, Cause: fmt.Errorf("skillgen: foreground draft unexpectedly deferred")}
	}
	var uncertain *opencode.RemoteStopUnconfirmedError
	if sessionID != "" && !errors.As(err, &uncertain) {
		d.Client.ClearSkillgenSession(threadID, sessionID)
	}
	if err != nil {
		return DraftOutput{}, err
	}
	if strings.TrimSpace(reply) == "" {
		reply = resp.Reply
	}
	if strings.TrimSpace(reply) == "" {
		return DraftOutput{}, fmt.Errorf("skillgen: completed draft has no body: session=%s", sessionID)
	}
	log.Printf("skillgen: drafter received reply (len=%d) from model=%s thread=%s", len(reply), in.ModelID, threadID)
	out, declined, perr := parseDraftReply(reply)
	if declined {
		log.Printf("skillgen: drafter declined evidence thread=%s session=%s", in.ThreadID, sessionID)
		return DraftOutput{}, nil
	}
	if perr != nil {
		return DraftOutput{}, perr
	}
	out.ModelID = in.ModelID
	log.Printf("skillgen: drafter parsed OK (title=%s score=%.2f action=%s patchTarget=%s)", out.Title, out.Score, out.Action, out.PatchTarget)
	return out, nil
}

func (d *OpencodeDrafter) loadReference() string {
	if d.ReferenceSkillPath == "" {
		return ""
	}
	b, err := os.ReadFile(d.ReferenceSkillPath)
	if err != nil {
		log.Printf("skillgen: reference read failed path=%s: %v", d.ReferenceSkillPath, err)
		return ""
	}
	// Truncate to keep the prompt sane.
	max := 4000
	s := string(b)
	if len(s) > max {
		s = s[:max] + "\n…(reference truncated)…"
	}
	return s
}

// buildDraftPrompt assembles the mining instructions + evidence for the drafter model.
func buildDraftPrompt(in DraftInput, reference string) string {
	var b strings.Builder
	b.WriteString("你是 Skill Author（技能作者）。任务：从下面的真实对话记录中抽取出一个**可复用的技能（skill）**，并输出一份严格符合 anthropic skill-creator 规范的 SKILL.md。\n\n")
	b.WriteString("## 判定规则\n")
	b.WriteString("- **CLASS-FIRST：先判断任务类别**，再判断是否有现有技能覆盖此类。技能命名应是类别名（\"pdf-to-excel-pipeline\"\uff09，而非具体实例名（\"convert-john-report-march\"\uff09。\n")
	b.WriteString("- 对话必须体现一个**可复现的流程**（输入 → 步骤 → 输出），否则直接拒绝输出（见下方“拒绝格式”）。\n")
	b.WriteString("- 对话中如果只是闲聊、问答、单次查询、测试、bug 报告，**不要生成技能**。\n")
	b.WriteString("- SKILL.md 必须包含 YAML frontmatter：`name`（kebab-case）、`description`（第一人称描述技能做什么 + 何时触发 + 输出格式）。\n")
	b.WriteString("- Body 部分简洁，包含：简介 / 触发条件 / 步骤 / 示例输入输出 / 注意事项。\n")
	b.WriteString("- 所有内容使用中文编写，不要加额外解释。\n\n")
	b.WriteString("## ⚠️ 输出约束（极其重要）\n")
	b.WriteString("- **不要输出任何思考过程、分析过程、解释性文字。**\n")
	b.WriteString("- **直接输出 JSON 对象，不要有任何前缀文字。**\n")
	b.WriteString("- 不要以 \"The user is...\" 或 \"Let me analyze...\" 等任何英文/中文思考开头。\n")
	b.WriteString("- 你的回复必须以 `{` 开头，以 `}` 结尾。\n\n")

	if len(in.ExistingSkillTitles) > 0 {
		b.WriteString("## 现有技能库\n")
		b.WriteString("以下技能已安装。**首先判断本次对话是否属于某个现有技能的类别**：\n")
		b.WriteString("- 若属于：action=patch，patch_target=<已有slug>，skill_md 输出更新后的完整内容。\n")
		b.WriteString("- 若不属于任何现有类别：action=create，创建新技能。\n")
		for _, t := range in.ExistingSkillTitles {
			b.WriteString(fmt.Sprintf("- %s\n", t))
		}
		b.WriteString("\n")
	}

	b.WriteString("## 输出格式（必须严格遵守）\n")
	b.WriteString("输出**单个 JSON 对象**，且只输出 JSON（前后不要有任何多余字符或代码围栏）：\n")
	b.WriteString("```\n")
	b.WriteString(`{"accept": true|false, "action": "create"|"patch", "patch_target": "<现有技能slug或空>", "title": "<kebab-case>", "confidence": 0.0-1.0, "skill_md": "<完整 SKILL.md 文本，包含 frontmatter>", "reason": "<简短说明>"}`)
	b.WriteString("\n```\n")
	b.WriteString("当 accept=false 时：confidence 填 0，title、skill_md、patch_target 可为空，reason 说明为什么这段对话不适合做成技能。\n\n")

	if reference != "" {
		b.WriteString("## 参考：skill-creator 规范节选\n")
		b.WriteString("```md\n")
		b.WriteString(reference)
		b.WriteString("\n```\n\n")
	}

	b.WriteString("## 元信息\n")
	b.WriteString(fmt.Sprintf("- trigger: %s\n- adapter: %s\n- user: %s\n- thread: %s\n- turns: %d\n\n",
		in.Trigger, in.Adapter, in.UserID, in.ThreadID, len(in.Conversation)))

	b.WriteString("## 对话记录\n")
	for i, t := range in.Conversation {
		b.WriteString(fmt.Sprintf("### turn %d · %s\n", i+1, t.Role))
		b.WriteString(truncate(t.Text, 2000))
		b.WriteString("\n\n")
	}
	return b.String()
}

// Default values capping the drafter prompt size. A 300+ turn conversation
// produces a 350k+ char prompt that the model cannot finish answering within
// the draft timeout — the #1 cause of "timed out waiting for assistant reply".
const (
	// maxDraftTurns caps how many conversation turns are sent to the drafter.
	// The most recent turns are kept (they carry the actual procedure).
	maxDraftTurns = 40
	// maxDraftPromptChars caps the total prompt length.
	maxDraftPromptChars = 60000
)

// capConversation keeps the most recent maxDraftTurns turns and prepends a
// marker when earlier turns were dropped.
func capConversation(turns []Turn) []Turn {
	if len(turns) <= maxDraftTurns {
		return turns
	}
	dropped := len(turns) - maxDraftTurns
	capped := make([]Turn, 0, maxDraftTurns+1)
	capped = append(capped, Turn{
		Role: "user",
		Text: fmt.Sprintf("（注意：对话共 %d 轮，前 %d 轮已省略，以下为最近 %d 轮）", len(turns), dropped, maxDraftTurns),
	})
	capped = append(capped, turns[dropped:]...)
	return capped
}

// truncatePrompt hard-caps the assembled prompt so it stays within a size the
// model can realistically process within the draft timeout.
func truncatePrompt(prompt string) string {
	if len(prompt) <= maxDraftPromptChars {
		return prompt
	}
	log.Printf("skillgen: drafter prompt truncated %d → %d chars to fit draft timeout", len(prompt), maxDraftPromptChars)
	return prompt[:maxDraftPromptChars] + "\n…(prompt truncated to fit timeout)…"
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// parseDraftReply extracts the decision JSON from the model's reply.
//
// declined reports a deliberate accept=false answer, which is a valid outcome
// (the evidence thread was not reusable) and must not be treated as a failure.
// err is only returned when no usable decision could be recovered.
func parseDraftReply(reply string) (out DraftOutput, declined bool, err error) {
	raw := extractJSONObject(reply)
	if raw == "" {
		log.Printf("skillgen: parseDraftReply — no JSON object found in reply (len=%d, first 300 chars=%.300s)", len(reply), reply)
		return DraftOutput{}, false, fmt.Errorf("skillgen: drafter returned unparseable reply (len=%d)", len(reply))
	}
	var obj struct {
		Accept      bool    `json:"accept"`
		Action      string  `json:"action"`
		PatchTarget string  `json:"patch_target"`
		Title       string  `json:"title"`
		Confidence  float64 `json:"confidence"`
		SkillMD     string  `json:"skill_md"`
		Reason      string  `json:"reason"`
	}
	if uerr := json.Unmarshal([]byte(raw), &obj); uerr != nil {
		log.Printf("skillgen: parseDraftReply — JSON unmarshal failed: %v (raw len=%d, first 300 chars=%.300s)", uerr, len(raw), raw)
		return DraftOutput{}, false, fmt.Errorf("skillgen: drafter returned unparseable reply (len=%d)", len(reply))
	}
	if !obj.Accept {
		return DraftOutput{}, true, nil
	}
	t := strings.TrimSpace(obj.Title)
	md := strings.TrimSpace(obj.SkillMD)
	if t == "" || md == "" {
		log.Printf("skillgen: parseDraftReply — accepted reply missing title or skill_md (raw len=%d)", len(raw))
		return DraftOutput{}, false, fmt.Errorf("skillgen: drafter reply missing title or skill_md (len=%d)", len(reply))
	}
	act := obj.Action
	if act == "" {
		act = "create"
	}
	return DraftOutput{
		Title:       t,
		SkillMD:     md,
		Score:       obj.Confidence,
		Action:      act,
		PatchTarget: strings.TrimSpace(obj.PatchTarget),
	}, false, nil
}

// extractJSONObject returns the first JSON object embedded in s that carries an
// "accept" key. Models routinely wrap the decision in reasoning prose, code
// fences, or even quote unrelated JSON fragments (an image payload, a tool
// result) before the real answer, so a single greedy `\{.*\}` match is not
// enough — we scan every `{` and keep the first object that decodes.
//
// Objects without an "accept" key are ignored on purpose: returning one would
// unmarshal into a zero-valued decision and be misread as a deliberate decline.
func extractJSONObject(s string) string {
	s = stripCodeFences(s)
	// Bound the scan: each attempt decodes up to len(s) bytes, so a reply full
	// of braces would otherwise cost O(n²).
	const maxAttempts = 256
	attempts := 0
	for i := 0; i < len(s); i++ {
		if s[i] != '{' {
			continue
		}
		if attempts++; attempts > maxAttempts {
			break
		}
		raw, ok := decodeObjectAt(s[i:])
		if !ok {
			continue
		}
		if hasAcceptKey(raw) {
			return raw
		}
	}
	return ""
}

// decodeObjectAt decodes one JSON object starting at the beginning of s and
// returns its exact source text.
func decodeObjectAt(s string) (string, bool) {
	var raw json.RawMessage
	if err := json.NewDecoder(strings.NewReader(s)).Decode(&raw); err != nil {
		return "", false
	}
	if len(raw) == 0 || raw[0] != '{' {
		return "", false
	}
	return string(raw), true
}

func hasAcceptKey(raw string) bool {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &probe); err != nil {
		return false
	}
	_, ok := probe["accept"]
	return ok
}

// stripCodeFences removes a leading/trailing ```json ... ``` wrapper.
func stripCodeFences(s string) string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "```") {
		return s
	}
	s = strings.TrimPrefix(s, "```json")
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")
	return strings.TrimSpace(s)
}
