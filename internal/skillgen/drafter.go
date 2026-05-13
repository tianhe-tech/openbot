package skillgen

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/user/opencode-gateway/internal/opencode"
)

// OpencodeDrafter uses a fresh temporary opencode session to generate the SKILL.md.
// It is the default Drafter wired in main.go; custom Drafter implementations can
// replace it (e.g., for tests) by passing their own implementation to NewService.
type OpencodeDrafter struct {
	Client *opencode.Client
	// ReferenceSkillPath points at an exemplar SKILL.md the drafter uses as a
	// style/anatomy guide (default: "skills/skill-creator/SKILL.md").
	ReferenceSkillPath string
}

// NewOpencodeDrafter constructs a drafter with sensible defaults.
func NewOpencodeDrafter(c *opencode.Client, referenceSkillPath string) *OpencodeDrafter {
	if referenceSkillPath == "" {
		referenceSkillPath = filepath.Join("skills", "skill-creator", "SKILL.md")
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
	prompt := buildDraftPrompt(in, exemplar)

	// Use an isolated thread so the drafting work doesn't pollute user threads.
	threadID := fmt.Sprintf("skillgen-draft-%s", in.ThreadID)
	payload := opencode.MessagePayload{
		Channel:  "skillgen",
		UserID:   in.UserID,
		ThreadID: threadID,
		Content:  prompt,
	}
	resp, err := d.Client.SendMessage(ctx, payload)
	if err != nil {
		return DraftOutput{}, err
	}
	title, body, score, action, patchTarget := parseDraftReply(resp.Reply)
	if title == "" || body == "" {
		return DraftOutput{}, fmt.Errorf("skillgen: drafter returned unparseable reply (len=%d)", len(resp.Reply))
	}
	return DraftOutput{
		Title:       title,
		SkillMD:     body,
		Score:       score,
		ModelID:     in.ModelID,
		Action:      action,
		PatchTarget: patchTarget,
	}, nil
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

// extractJSONObject pulls out the widest `{...}` block from text, stripping
// code fences.
func extractJSONObject(s string) string {
	s = strings.TrimSpace(s)
	// strip ```json ... ``` or ``` ... ```
	s = strings.TrimPrefix(s, "```json")
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")
	m := jsonObjRe.FindString(s)
	return strings.TrimSpace(m)
}
