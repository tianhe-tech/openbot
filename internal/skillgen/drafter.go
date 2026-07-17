package skillgen

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/user/opencode-gateway/internal/opencode"
)

// OpencodeDrafter uses a fresh temporary opencode session to generate the SKILL.md.
// It is the default Drafter wired in main.go; custom Drafter implementations can
// replace it (e.g., for tests) by passing their own implementation to NewService.
type OpencodeDrafter struct {
	Client *opencode.Client
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
	return &OpencodeDrafter{Client: c, ReferenceSkillPath: referenceSkillPath}
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
	prompt := buildDraftPrompt(in, exemplar)
	log.Printf("skillgen: drafter prompt built (len=%d) for thread=%s model=%s turns=%d", len(prompt), in.ThreadID, in.ModelID, len(in.Conversation))

	// Use an isolated thread so the drafting work doesn't pollute user threads.
	threadID := fmt.Sprintf("skillgen-draft-%s", in.ThreadID)
	payload := opencode.MessagePayload{
		Channel:   "skillgen",
		UserID:    in.UserID,
		ThreadID:  threadID,
		Content:   prompt,
		Model:     in.ModelID, // pass selected model to opencode so it actually uses it
		Streaming: true,       // use prompt_async to avoid sync API 500 errors
	}
	log.Printf("skillgen: drafter sending prompt to opencode (model=%s thread=%s streaming=true)", in.ModelID, threadID)
	resp, err := d.Client.SendMessage(ctx, payload)
	if err != nil {
		log.Printf("skillgen: drafter SendMessage failed (model=%s thread=%s): %v", in.ModelID, threadID, err)
		return DraftOutput{}, err
	}

	// With streaming=true, SendMessage returns immediately with an empty Reply.
	// The model generates content asynchronously; we poll FetchSessionTurns
	// until the assistant reply appears, then extract it.
	reply := resp.Reply
	if strings.TrimSpace(reply) == "" && resp.SessionID != "" {
		reply, err = d.waitForAssistantReply(ctx, resp.SessionID)
		// Clear all session state so model fallback creates a fresh session
		// instead of reusing this one (which may contain a failed/error reply).
		d.Client.ClearSkillgenSession(threadID, resp.SessionID)
		if err != nil {
			log.Printf("skillgen: drafter waitForAssistantReply failed (model=%s thread=%s session=%s): %v", in.ModelID, threadID, resp.SessionID, err)
			return DraftOutput{}, err
		}
	}
	log.Printf("skillgen: drafter received reply (len=%d) from model=%s thread=%s", len(reply), in.ModelID, threadID)
	title, body, score, action, patchTarget := parseDraftReply(reply)
	if title == "" || body == "" {
		log.Printf("skillgen: drafter reply unparseable (len=%d, first 200 chars=%.200s)", len(reply), reply)
		return DraftOutput{}, fmt.Errorf("skillgen: drafter returned unparseable reply (len=%d)", len(reply))
	}
	log.Printf("skillgen: drafter parsed OK (title=%s score=%.2f action=%s patchTarget=%s)", title, score, action, patchTarget)
	return DraftOutput{
		Title:       title,
		SkillMD:     body,
		Score:       score,
		ModelID:     in.ModelID,
		Action:      action,
		PatchTarget: patchTarget,
	}, nil
}

// waitForAssistantReply polls FetchSessionTurns until the assistant's reply
// appears, then returns it. This is used with streaming=true (prompt_async)
// because the async API returns immediately without the reply content.
//
// Polling strategy: check every 3s, with a total timeout bounded by ctx.
// We detect completion by seeing the assistant turn count increase beyond
// what it was at send time (which is 0 for a fresh skillgen session).
func (d *OpencodeDrafter) waitForAssistantReply(ctx context.Context, sessionID string) (string, error) {
	if sessionID == "" {
		return "", fmt.Errorf("skillgen: empty sessionID for waitForAssistantReply")
	}
	const pollInterval = 3 * time.Second
	const maxWait = 7 * time.Minute // bounded by the 8m draft timeout in service.go

	deadline := time.Now().Add(maxWait)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}

	pollCtx, pollCancel := context.WithDeadline(context.Background(), deadline)
	defer pollCancel()

	for {
		turns, err := d.Client.FetchSessionTurns(pollCtx, sessionID)
		if err != nil {
			log.Printf("skillgen: poll FetchSessionTurns error session=%s: %v", sessionID, err)
		} else {
			// Find the last assistant turn — that's the drafter's reply.
			for i := len(turns) - 1; i >= 0; i-- {
				if turns[i].Role == "assistant" && strings.TrimSpace(turns[i].Text) != "" {
					text := turns[i].Text
					log.Printf("skillgen: poll found assistant reply (len=%d) for session=%s after %s",
						len(text), sessionID, time.Since(deadline.Add(-maxWait)).Round(time.Second))
					return text, nil
				}
			}
		}

		select {
		case <-pollCtx.Done():
			return "", fmt.Errorf("skillgen: timed out waiting for assistant reply (session=%s)", sessionID)
		case <-time.After(pollInterval):
		}
	}
}

func (d *OpencodeDrafter) loadReference() string {
	if d.ReferenceSkillPath == "" {
		return ""
	}
	b, err := os.ReadFile(d.ReferenceSkillPath)
	if err != nil {
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// parseDraftReply extracts the JSON object from the model's reply. Tolerates
// surrounding code fences and whitespace. Returns title, full SKILL.md, score,
// action ("create"/"patch"), and patch_target slug.
// On parse failure or accept=false, returns empties.
func parseDraftReply(reply string) (title string, body string, score float64, action string, patchTarget string) {
	raw := extractJSONObject(reply)
	if raw == "" {
		log.Printf("skillgen: parseDraftReply — no JSON object found in reply (len=%d, first 300 chars=%.300s)", len(reply), reply)
		return
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
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		log.Printf("skillgen: parseDraftReply — JSON unmarshal failed: %v (raw len=%d, first 300 chars=%.300s)", err, len(raw), raw)
		return
	}
	if !obj.Accept {
		return
	}
	t := strings.TrimSpace(obj.Title)
	md := strings.TrimSpace(obj.SkillMD)
	if t == "" || md == "" {
		return
	}
	act := obj.Action
	if act == "" {
		act = "create"
	}
	return t, md, obj.Confidence, act, strings.TrimSpace(obj.PatchTarget)
}

var jsonObjRe = regexp.MustCompile(`(?s)\{.*\}`)

// extractJSONObject pulls out the last {...} block from text, stripping
// code fences. Some models prepend thinking/reasoning text before the JSON;
// we find the widest JSON object by searching for the last `{` that starts
// a valid-looking object. If the text starts with `{` (no thinking), the
// simple regex match suffices.
func extractJSONObject(s string) string {
	s = strings.TrimSpace(s)
	// strip ```json ... ``` or ``` ... ``` fences
	if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```json")
		s = strings.TrimPrefix(s, "```")
		s = strings.TrimSuffix(s, "```")
		s = strings.TrimSpace(s)
	}
	// If the entire string is a JSON object (no thinking prefix), use it directly.
	if strings.HasPrefix(s, "{") {
		return s
	}
	// Otherwise, find the last {...} block — the model may have prepended
	// thinking/reasoning text before the JSON output.
	m := jsonObjRe.FindString(s)
	return strings.TrimSpace(m)
}
