package knowledge

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/user/opencode-gateway/internal/memstore"
)

// HandleCommand implements opencode.CommandInterceptor for the knowledge
// control commands.
//
// The plan requires "explicit remember/forget controls with trusted
// authorization and auditable outcomes", and it is specific about what that
// rules out: "An assistant statement that it remembered something is not a
// successful write receipt." So /remember does not reply "记住了" — it performs
// the write and reports the fact ID the archive assigned, or reports the
// rejection and why. A reply that claims success without a stored row would be
// exactly the failure the plan names.
func (s *Service) HandleCommand(adapter, userID, message string) (string, bool) {
	if s == nil {
		return "", false
	}
	trimmed := strings.TrimSpace(message)
	if !strings.HasPrefix(trimmed, "/") {
		return "", false
	}
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return "", false
	}
	command := strings.ToLower(fields[0])
	argument := strings.TrimSpace(strings.TrimPrefix(trimmed, fields[0]))
	switch command {
	case "/remember":
		return s.cmdRemember(adapter, userID, argument), true
	case "/forget":
		return s.cmdForget(adapter, userID, argument), true
	case "/knowledge-list", "/knowledge":
		return s.cmdList(adapter, userID), true
	case "/knowledge-help":
		return cmdHelp(), true
	default:
		return "", false
	}
}

// cmdRemember stores one fact and reports the real outcome.
func (s *Service) cmdRemember(adapter, userID, statement string) string {
	if strings.TrimSpace(statement) == "" {
		return "用法：`/remember <要记住的内容>`\n例如：`/remember 以后都用中文回答我`"
	}
	if s.writer == nil || s.store == nil {
		return "❌ 记忆归档不可用，无法写入。"
	}
	scope := memstore.SearchScope{Adapter: adapter, UserID: userID}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fact, err := s.writer.RememberKnowledgeFact(ctx, scope, statement)
	if err != nil {
		log.Printf("knowledge: /remember failed (adapter=%s user=%s): %v", adapter, userID, err)
		return fmt.Sprintf("❌ 写入失败：%v", err)
	}
	return fmt.Sprintf("✅ 已记住（ID `%s`，全局生效，依据消息 `%s`）。\n内容：%s\n\n用 `/knowledge-list` 查看，用 `/forget %s` 撤销。",
		fact.ID, short(fact.Evidence[0]), fact.Statement, fact.ID)
}

// cmdForget removes one fact by ID, or by exact statement.
func (s *Service) cmdForget(adapter, userID, argument string) string {
	argument = strings.TrimSpace(argument)
	if argument == "" {
		return "用法：`/forget <事实ID>` 或 `/forget <完整内容>`\n用 `/knowledge-list` 查看 ID。"
	}
	if s.writer == nil || s.store == nil {
		return "❌ 记忆归档不可用，无法删除。"
	}
	scope := memstore.SearchScope{Adapter: adapter, UserID: userID}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	factID := argument
	// A statement is not an ID. If the argument does not look like one, resolve
	// it by exact statement match so the user does not have to copy a hash.
	if !looksLikeFactID(argument) {
		// IncludeSessionFacts: a session-scoped fact is still the user's fact,
		// and a statement they cannot resolve here is a statement they cannot
		// delete.
		facts, err := s.store.ListKnowledgeFacts(ctx, scope, memstore.KnowledgeListOptions{Limit: 200, IncludeSessionFacts: true})
		if err != nil {
			return fmt.Sprintf("❌ 查询失败：%v", err)
		}
		matched := ""
		for _, fact := range facts {
			if strings.EqualFold(strings.TrimSpace(fact.Statement), argument) {
				matched = fact.ID
				break
			}
		}
		if matched == "" {
			return fmt.Sprintf("❌ 没有找到内容完全匹配的事实：%s\n用 `/knowledge-list` 查看现有事实。", argument)
		}
		factID = matched
	}

	removed, err := s.writer.ForgetKnowledgeFact(ctx, scope, factID)
	if err != nil {
		log.Printf("knowledge: /forget failed (adapter=%s user=%s fact=%s): %v", adapter, userID, factID, err)
		return fmt.Sprintf("❌ 删除失败：%v", err)
	}
	if !removed {
		return fmt.Sprintf("❌ 没有找到事实 `%s`（可能已被删除，或不属于当前用户）。", short(factID))
	}
	return fmt.Sprintf("✅ 已删除事实 `%s`。\n如果它曾经修正过旧事实，旧事实已恢复为当前有效。", short(factID))
}

// cmdList renders the current facts.
func (s *Service) cmdList(adapter, userID string) string {
	if s.store == nil {
		return "❌ 记忆归档不可用。"
	}
	scope := memstore.SearchScope{Adapter: adapter, UserID: userID}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// IncludeSessionFacts: the list is the review surface, so it must show
	// everything the user owns — including facts scoped to one conversation,
	// which would otherwise be written and never seen again.
	facts, err := s.store.ListKnowledgeFacts(ctx, scope, memstore.KnowledgeListOptions{Limit: 50, IncludeSessionFacts: true})
	if err != nil {
		return fmt.Sprintf("❌ 查询失败：%v", err)
	}
	if len(facts) == 0 {
		return "📭 当前没有已记住的事实。\n用 `/remember <内容>` 添加。"
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("📚 已记住 %d 条事实：\n", len(facts)))
	for _, fact := range facts {
		builder.WriteString(fmt.Sprintf("\n`%s` [%s/%s] %s\n", fact.ID, fact.Kind, fact.Scope, fact.Statement))
	}
	builder.WriteString("\n用 `/forget <ID>` 删除某条。")
	return builder.String()
}

func cmdHelp() string {
	return strings.Join([]string{
		"📚 记忆命令：",
		"",
		"`/remember <内容>` — 记住一条事实（全局生效，返回真实写入 ID）",
		"`/forget <ID|内容>` — 删除一条事实",
		"`/knowledge-list` — 列出当前事实",
		"`/knowledge-help` — 显示本帮助",
		"",
		"说明：自动抽取的事实来自历史对话，可能已过时；与当前指令冲突时以当前指令为准。",
	}, "\n")
}

// looksLikeFactID reports whether the argument is plausibly a fact ID rather
// than a statement. Fact IDs are hex digests, so a statement containing spaces
// or non-hex characters is not one.
func looksLikeFactID(value string) bool {
	if len(value) < 16 || len(value) > 64 {
		return false
	}
	for _, r := range value {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		case r >= 'A' && r <= 'F':
		default:
			return false
		}
	}
	return true
}

// short abbreviates an ID for display without making it ambiguous in a list of
// a few dozen rows.
func short(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}
