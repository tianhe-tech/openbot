package knowledge

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/user/opencode-gateway/internal/opencode"
)

// OpencodeDrafter asks the opencode server to extract durable facts from a
// captured conversation. It is the default Drafter wired in main.go; tests pass
// their own implementation to NewService.
type OpencodeDrafter struct {
	Client *opencode.Client
}

// NewOpencodeDrafter constructs a drafter.
func NewOpencodeDrafter(c *opencode.Client) *OpencodeDrafter {
	return &OpencodeDrafter{Client: c}
}

// Draft converts a conversation into candidate facts.
func (d *OpencodeDrafter) Draft(ctx context.Context, in DraftInput) (DraftOutput, error) {
	if d == nil || d.Client == nil {
		return DraftOutput{}, fmt.Errorf("knowledge: nil drafter/client")
	}
	if len(in.Conversation) < 2 {
		return DraftOutput{}, nil // not enough evidence
	}
	prompt := buildExtractionPrompt(in)
	log.Printf("knowledge: drafter prompt built (len=%d) for thread=%s model=%s turns=%d",
		len(prompt), in.ThreadID, in.ModelID, len(in.Conversation))

	// An isolated thread so extraction never pollutes a user conversation. The
	// prefix is also what the recursion guard checks, so it must match.
	threadID := fmt.Sprintf("knowledge-draft-%s", in.ThreadID)
	payload := opencode.MessagePayload{
		Internal:  true,
		Channel:   "knowledge",
		UserID:    in.UserID,
		ThreadID:  threadID,
		Content:   prompt,
		Model:     in.ModelID,
		Streaming: true, // prompt_async avoids the sync API's 500s on long prompts
	}
	resp, err := d.Client.SendMessage(ctx, payload)
	if err != nil {
		log.Printf("knowledge: drafter SendMessage failed (model=%s thread=%s): %v", in.ModelID, threadID, err)
		return DraftOutput{}, err
	}

	// With streaming=true SendMessage returns immediately with an empty Reply;
	// the model generates asynchronously, so poll until the reply appears.
	reply := resp.Reply
	if strings.TrimSpace(reply) == "" && resp.SessionID != "" {
		reply, err = d.waitForAssistantReply(ctx, resp.SessionID)
		d.Client.ClearSkillgenSession(threadID, resp.SessionID)
		if err != nil {
			log.Printf("knowledge: drafter waitForAssistantReply failed (model=%s thread=%s session=%s): %v",
				in.ModelID, threadID, resp.SessionID, err)
			return DraftOutput{}, err
		}
	}
	log.Printf("knowledge: drafter received reply (len=%d) from model=%s thread=%s", len(reply), in.ModelID, threadID)
	facts, err := parseExtractionReply(reply)
	if err != nil {
		log.Printf("knowledge: drafter reply unparseable (len=%d, first 200 chars=%.200s)", len(reply), reply)
		return DraftOutput{}, err
	}
	log.Printf("knowledge: drafter parsed %d candidate facts", len(facts))
	return DraftOutput{Facts: facts, ModelID: in.ModelID}, nil
}

// waitForAssistantReply polls FetchSessionTurns until the assistant's reply
// appears, then returns it.
//
// Polling is necessary because streaming=true (prompt_async) returns before the
// content exists. The interval is 3s and the total wait is bounded by ctx, which
// the Service already capped at PerModelTimeout.
func (d *OpencodeDrafter) waitForAssistantReply(ctx context.Context, sessionID string) (string, error) {
	if sessionID == "" {
		return "", fmt.Errorf("knowledge: empty sessionID for waitForAssistantReply")
	}
	const pollInterval = 3 * time.Second
	const maxWait = 3 * time.Minute

	deadline := time.Now().Add(maxWait)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	pollCtx, pollCancel := context.WithDeadline(ctx, deadline)
	defer pollCancel()

	for {
		turns, err := d.Client.FetchSessionTurns(pollCtx, sessionID)
		if err != nil {
			log.Printf("knowledge: poll FetchSessionTurns error session=%s: %v", sessionID, err)
		} else {
			for i := len(turns) - 1; i >= 0; i-- {
				if turns[i].Role == "assistant" && turns[i].Completed && strings.TrimSpace(turns[i].Text) != "" {
					return turns[i].Text, nil
				}
			}
		}
		select {
		case <-pollCtx.Done():
			return "", fmt.Errorf("knowledge: timed out waiting for assistant reply (session=%s)", sessionID)
		case <-time.After(pollInterval):
		}
	}
}

// extractionFact is the wire shape the model is asked to produce. It is a
// separate type from DraftFact so the JSON tags live next to the prompt that
// documents them, and so a field rename in DraftFact cannot silently change the
// contract with the model.
type extractionFact struct {
	Kind       string   `json:"kind"`
	Statement  string   `json:"statement"`
	Status     string   `json:"status"`
	Scope      string   `json:"scope"`
	ProjectKey string   `json:"project_key"`
	SessionID  string   `json:"session_id"`
	Confidence float64  `json:"confidence"`
	Evidence   []string `json:"evidence"`
	Supersedes string   `json:"supersedes"`
}

// parseExtractionReply extracts the JSON array from a model reply.
//
// Models wrap JSON in prose and code fences even when told not to, so the parser
// finds the outermost array rather than requiring the reply to be exactly JSON.
// A reply with no array is an error: it means the model did not follow the
// contract, and silently returning zero facts would look like "nothing durable
// here" when the truth is "the extraction did not run".
func parseExtractionReply(reply string) ([]DraftFact, error) {
	trimmed := strings.TrimSpace(reply)
	start := strings.Index(trimmed, "[")
	end := strings.LastIndex(trimmed, "]")
	if start < 0 || end <= start {
		return nil, fmt.Errorf("knowledge: reply contains no JSON array (len=%d)", len(reply))
	}
	var raw []extractionFact
	if err := json.Unmarshal([]byte(trimmed[start:end+1]), &raw); err != nil {
		return nil, fmt.Errorf("knowledge: reply JSON is malformed: %w", err)
	}
	facts := make([]DraftFact, 0, len(raw))
	for _, item := range raw {
		if strings.TrimSpace(item.Statement) == "" {
			continue
		}
		facts = append(facts, DraftFact{
			Kind:       item.Kind,
			Statement:  item.Statement,
			Status:     item.Status,
			Scope:      item.Scope,
			ProjectKey: item.ProjectKey,
			SessionID:  item.SessionID,
			Confidence: item.Confidence,
			Evidence:   item.Evidence,
			Supersedes: item.Supersedes,
		})
	}
	return facts, nil
}

// buildExtractionPrompt assembles the extraction instructions and evidence.
func buildExtractionPrompt(in DraftInput) string {
	var b strings.Builder
	b.WriteString("你是 Knowledge Extractor（知识抽取器）。任务：从下面的真实对话记录中，抽取**值得长期记住的事实**，并输出 JSON 数组。\n\n")

	b.WriteString("## 什么值得抽取\n")
	b.WriteString("- 用户的**稳定偏好**（语言、风格、工具选择、禁忌）。\n")
	b.WriteString("- 关于用户或其环境的**稳定事实**（项目结构、技术栈、部署方式）。\n")
	b.WriteString("- 用户确认过的**可复用流程**（步骤、命令、注意事项）。\n")
	b.WriteString("- 明确的**约束**（不能做什么、必须遵守什么）。\n")
	b.WriteString("- 已经做出的**决定**（选型、方案，不应被悄悄推翻）。\n\n")

	b.WriteString("## 什么不要抽取\n")
	b.WriteString("- 一次性的任务细节（\"把这份 PDF 转成 Excel\"）。\n")
	b.WriteString("- 闲聊、寒暄、测试消息。\n")
	b.WriteString("- 助手自己的猜测或建议，除非用户明确确认。\n")
	b.WriteString("- 只对当前这一次任务成立的临时指令 —— 这类必须标成 `session` 作用域，绝不能标成 `global`。\n\n")

	b.WriteString("## 证据规则（最重要）\n")
	b.WriteString("- 每条事实的 `evidence` 必须是下面给出的**消息 ID**，不能编造。\n")
	b.WriteString("- 一条事实至少需要一个证据 ID；没有证据的事实**不要输出**。\n")
	b.WriteString("- 证据必须真的支持这条陈述。宁可少输出，也不要输出没有依据的结论。\n\n")

	b.WriteString("## 作用域规则\n")
	b.WriteString("- `global`：对所有对话都成立（例如\"用户偏好中文\"）。\n")
	b.WriteString("- `project`：只在这个项目内成立，需要填 `project_key`。\n")
	b.WriteString("- `session`：只对当前这次会话成立，需要填 `session_id`。\n")
	b.WriteString("- **拿不准时选 `session`**，不要选 `global`。把临时指令变成全局偏好是最严重的错误。\n\n")

	if in.Project != "" {
		b.WriteString(fmt.Sprintf("当前项目：`%s`\n", in.Project))
	}
	if in.SessionID != "" {
		b.WriteString(fmt.Sprintf("当前会话：`%s`\n", in.SessionID))
	}
	b.WriteString("\n")

	if len(in.ExistingFacts) > 0 {
		b.WriteString("## 已知事实\n")
		b.WriteString("以下事实已经存在。如果本次对话**修正**了其中某条，请输出新事实并把旧事实的 ID 填进 `supersedes`；如果只是重复，**不要输出**。\n")
		for _, fact := range in.ExistingFacts {
			b.WriteString(fmt.Sprintf("- %s\n", fact))
		}
		b.WriteString("\n")
	}

	b.WriteString("## 输出格式（必须严格遵守）\n")
	b.WriteString("直接输出 JSON 数组，不要有任何前缀文字、解释或 Markdown 代码块标记。\n")
	b.WriteString("没有值得抽取的内容时，输出 `[]`。\n\n")
	b.WriteString("```\n")
	b.WriteString("[\n")
	b.WriteString("  {\n")
	b.WriteString("    \"kind\": \"preference|fact|procedure|constraint|decision\",\n")
	b.WriteString("    \"statement\": \"一句话陈述，不超过 200 字\",\n")
	b.WriteString("    \"status\": \"observed|proposed|completed|verified\",\n")
	b.WriteString("    \"scope\": \"global|project|session\",\n")
	b.WriteString("    \"project_key\": \"仅 project 作用域填写\",\n")
	b.WriteString("    \"session_id\": \"仅 session 作用域填写\",\n")
	b.WriteString("    \"confidence\": 0.0,\n")
	b.WriteString("    \"evidence\": [\"消息ID\"],\n")
	b.WriteString("    \"supersedes\": \"被修正的旧事实ID，没有就留空\"\n")
	b.WriteString("  }\n")
	b.WriteString("]\n")
	b.WriteString("```\n\n")

	b.WriteString("## 对话记录\n")
	for _, turn := range in.Conversation {
		b.WriteString(fmt.Sprintf("\n[%s] (message_id=%s)\n%s\n", turn.Role, turn.MessageID, turn.Text))
	}
	return b.String()
}
