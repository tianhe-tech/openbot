package memstore

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"
)

// HandoffTTL is how long a pending session handoff remains eligible for
// auto-injection into a new session before being discarded.
const HandoffTTL = 24 * time.Hour

// HandoffRecord is one saved "session continuation" — a compressed view of a
// session that got stuck (e.g. opencode scheduler deadlock) so the next user
// turn on the same thread can transparently resume in a fresh session.
type HandoffRecord struct {
	ThreadID     string
	Adapter      string
	UserID       string
	OldSessionID string
	CreatedAt    time.Time
	Summary      string // rule-compressed recap of the old session
	LastUserMsg  string // the user's message that never got answered
	Consumed     bool
}

// SaveHandoff upserts one pending handoff for the given thread.
// Any existing un-consumed handoff for the same thread is replaced.
func (s *Store) SaveHandoff(rec HandoffRecord) error {
	if strings.TrimSpace(rec.ThreadID) == "" {
		return fmt.Errorf("memstore: SaveHandoff: empty thread_id")
	}
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = time.Now()
	}
	consumed := 0
	if rec.Consumed {
		consumed = 1
	}
	_, err := s.db.Exec(`
		INSERT OR REPLACE INTO session_handoff
			(thread_id, adapter, user_id, old_session_id, created_at, summary, last_user_msg, consumed)
		VALUES (?,?,?,?,?,?,?,?)`,
		rec.ThreadID, rec.Adapter, rec.UserID, rec.OldSessionID,
		rec.CreatedAt.Unix(), rec.Summary, rec.LastUserMsg, consumed,
	)
	if err != nil {
		return fmt.Errorf("memstore: save handoff: %w", err)
	}
	return nil
}

// LoadPendingHandoff returns the un-consumed handoff for thread if one exists
// and is within HandoffTTL. Returns (zero, false, nil) when nothing pending.
func (s *Store) LoadPendingHandoff(threadID string) (HandoffRecord, bool, error) {
	if strings.TrimSpace(threadID) == "" {
		return HandoffRecord{}, false, nil
	}
	cutoff := time.Now().Add(-HandoffTTL).Unix()
	row := s.db.QueryRow(`
		SELECT thread_id, adapter, user_id, old_session_id, created_at, summary, last_user_msg, consumed
		FROM   session_handoff
		WHERE  thread_id = ? AND consumed = 0 AND created_at >= ?`,
		threadID, cutoff)
	var rec HandoffRecord
	var ts int64
	var consumed int
	err := row.Scan(&rec.ThreadID, &rec.Adapter, &rec.UserID, &rec.OldSessionID,
		&ts, &rec.Summary, &rec.LastUserMsg, &consumed)
	if err == sql.ErrNoRows {
		return HandoffRecord{}, false, nil
	}
	if err != nil {
		return HandoffRecord{}, false, fmt.Errorf("memstore: load handoff: %w", err)
	}
	rec.CreatedAt = time.Unix(ts, 0)
	rec.Consumed = consumed != 0
	return rec, true, nil
}

// MarkHandoffConsumed flags the handoff for thread as consumed so it is not
// re-injected on subsequent turns.
func (s *Store) MarkHandoffConsumed(threadID string) error {
	if strings.TrimSpace(threadID) == "" {
		return nil
	}
	_, err := s.db.Exec(`UPDATE session_handoff SET consumed = 1 WHERE thread_id = ?`, threadID)
	if err != nil {
		return fmt.Errorf("memstore: mark handoff consumed: %w", err)
	}
	return nil
}

// PurgeExpiredHandoffs removes handoff rows older than 2*TTL; safe to call
// periodically from a cleanup goroutine.
func (s *Store) PurgeExpiredHandoffs() error {
	cutoff := time.Now().Add(-2 * HandoffTTL).Unix()
	_, err := s.db.Exec(`DELETE FROM session_handoff WHERE created_at < ?`, cutoff)
	return err
}

// ---------- Rule-based summary builder ----------

// HandoffTurn is one request/response pair to feed into BuildHandoffSummary.
type HandoffTurn struct {
	Role    string // "user" | "assistant"
	Content string
}

// BuildHandoffSummary compresses an ordered list of turns into a short recap.
//
// Strategy (no LLM, purely mechanical):
//   - Keep the first user turn verbatim (short-truncated) as the "goal anchor".
//   - Keep the last up to 3 turns verbatim (short-truncated) as recent context.
//   - For everything in between, take the first sentence of each user turn and
//     the first sentence of each assistant turn, prefixed with role tags.
//   - Total output capped at maxChars (default 2000) by further truncating the
//     middle section.
//
// The result is plain text suitable for injection as a preamble.
func BuildHandoffSummary(turns []HandoffTurn, maxChars int) string {
	if maxChars <= 0 {
		maxChars = 2000
	}
	if len(turns) == 0 {
		return ""
	}

	const perLineMax = 180  // max chars per middle bullet
	const headFootMax = 400 // max chars for first/last verbatim block
	const tailCount = 3     // keep last N turns verbatim

	// Sanitize every turn up-front: strip nested handoff preambles, memstore
	// recall wrappers, and model <think> reasoning blocks. Without this, a
	// second-generation handoff (A → B → C) would embed previous preambles
	// verbatim into 【目标】 and leak model reasoning / cross-thread recall
	// into 【中间对话要点】, causing the resumed session to hallucinate that
	// the old task is unrelated recalled work.
	cleaned := make([]HandoffTurn, 0, len(turns))
	for _, t := range turns {
		c := sanitizeHandoffContent(t.Role, t.Content)
		if strings.TrimSpace(c) == "" {
			continue
		}
		cleaned = append(cleaned, HandoffTurn{Role: t.Role, Content: c})
	}
	if len(cleaned) == 0 {
		return ""
	}
	turns = cleaned

	var b strings.Builder
	// Anchor: first user turn.
	for _, t := range turns {
		if t.Role == "user" && strings.TrimSpace(t.Content) != "" {
			b.WriteString("【目标】")
			b.WriteString(truncateRunes(t.Content, headFootMax))
			b.WriteString("\n")
			break
		}
	}

	// Determine split points.
	tailStart := len(turns) - tailCount
	if tailStart < 1 {
		tailStart = 1
	}

	// Middle: bullet summary.
	if tailStart > 1 {
		b.WriteString("【中间对话要点】\n")
		for i := 1; i < tailStart; i++ {
			t := turns[i]
			line := firstSentence(t.Content)
			if line == "" {
				continue
			}
			tag := "·"
			switch t.Role {
			case "user":
				tag = "用户"
			case "assistant":
				tag = "助手"
			}
			b.WriteString("- ")
			b.WriteString(tag)
			b.WriteString("：")
			b.WriteString(truncateRunes(line, perLineMax))
			b.WriteString("\n")
		}
	}

	// Tail: verbatim (short) recent turns.
	if tailStart < len(turns) {
		b.WriteString("【最近对话】\n")
		for i := tailStart; i < len(turns); i++ {
			t := turns[i]
			if strings.TrimSpace(t.Content) == "" {
				continue
			}
			tag := "·"
			switch t.Role {
			case "user":
				tag = "用户"
			case "assistant":
				tag = "助手"
			}
			b.WriteString(tag)
			b.WriteString("：")
			b.WriteString(truncateRunes(t.Content, headFootMax))
			b.WriteString("\n")
		}
	}

	out := strings.TrimSpace(b.String())
	if len(out) > maxChars {
		out = truncateRunes(out, maxChars)
	}
	return out
}

// thinkBlockRe matches a single <think>…</think> block, including newlines.
var thinkBlockRe = regexp.MustCompile(`(?s)<think>.*?</think>`)

// openThinkTailRe strips an unterminated trailing <think>… block so we don't
// keep the model's chain-of-thought when the end tag is missing (e.g. the turn
// was truncated mid-reasoning).
var openThinkTailRe = regexp.MustCompile(`(?s)<think>.*$`)

// handoffInnerUserRe extracts the real user input from a previously-injected
// handoff preamble. BuildHandoffPreamble always ends with
// "---\n\n【当前用户消息】\n<actual user text>".
var handoffInnerUserRe = regexp.MustCompile(`(?s)【当前用户消息】\s*\n(.*)$`)

// SanitizeHandoffContent prepares one turn's text for inclusion in a new
// handoff summary or for saving as lastUserMsg. It removes:
//   - model <think> reasoning blocks (they leak internal monologue as if it
//     were the assistant's answer);
//   - nested handoff preambles on user turns (keeping only the inner actual
//     user message, otherwise 【目标】 becomes a recursive preamble);
//   - memstore recall blocks injected before user text (they carry other
//     threads' task lists that poison the resumed session).
//
// Exported variant of sanitizeHandoffContent; prefer this from other packages.
func SanitizeHandoffContent(role, content string) string {
	return sanitizeHandoffContent(role, content)
}

func sanitizeHandoffContent(role, content string) string {
	s := strings.TrimSpace(content)
	if s == "" {
		return ""
	}
	// Strip <think>...</think> blocks (common in assistant turns).
	s = thinkBlockRe.ReplaceAllString(s, "")
	// Strip an unterminated trailing <think> block (truncated reasoning).
	s = openThinkTailRe.ReplaceAllString(s, "")
	s = strings.TrimSpace(s)

	// For user turns, unwrap a previously-injected handoff preamble so the
	// inner real user text becomes the turn's content. Without this, the
	// next-generation handoff's 【目标】 is the previous preamble verbatim.
	if role == "user" {
		if m := handoffInnerUserRe.FindStringSubmatch(s); len(m) == 2 {
			s = strings.TrimSpace(m[1])
		}
		// Strip memstore recall prefix: "【历史工作记忆】... <blank line> <real text>".
		// The real user text follows the first blank line after the block, but
		// we don't have a hard delimiter. Heuristic: if content starts with
		// the recall tag, drop everything up to the last "---" line if any,
		// otherwise drop the whole recall block.
		if strings.HasPrefix(s, "【历史工作记忆】") {
			if idx := strings.LastIndex(s, "\n---\n"); idx > 0 {
				s = strings.TrimSpace(s[idx+len("\n---\n"):])
			} else if idx := strings.LastIndex(s, "---"); idx > 0 && idx < len(s)-3 {
				s = strings.TrimSpace(s[idx+3:])
			}
		}
	}
	return strings.TrimSpace(s)
}

// BuildHandoffPreamble renders the preamble injected as the first part of
// the new session's prompt, combining the saved summary, the previously
// unanswered user message (auto-resent), and the current turn's user input.
func BuildHandoffPreamble(summary, prevUnanswered, currentUserMsg string) string {
	var b strings.Builder
	b.WriteString("【会话接续·自动恢复】\n")
	b.WriteString("上一会话因服务端调度异常中断，以下是压缩摘要：\n\n")
	if strings.TrimSpace(summary) != "" {
		b.WriteString(summary)
		b.WriteString("\n\n")
	}
	if strings.TrimSpace(prevUnanswered) != "" {
		b.WriteString("【上一轮用户提问（未获回复，已自动重发）】\n")
		b.WriteString(truncateRunes(prevUnanswered, 600))
		b.WriteString("\n\n")
	}
	b.WriteString("---\n\n【当前用户消息】\n")
	b.WriteString(currentUserMsg)
	return b.String()
}

// firstSentence returns the first sentence (or line) of s, trimmed.
func firstSentence(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Prefer the first line.
	if idx := strings.IndexAny(s, "\n\r"); idx > 0 {
		s = s[:idx]
	}
	// Then split on sentence terminators.
	for _, sep := range []string{"。", "！", "？", ". ", "! ", "? "} {
		if idx := strings.Index(s, sep); idx > 0 {
			return strings.TrimSpace(s[:idx])
		}
	}
	return s
}

// truncateRunes truncates s to at most n runes, appending "…" if truncated.
// NOTE: package already defines truncateRunes in recall.go; this inline variant
// is unused — kept only as documentation; remove if/when a shared helper exists.
// (left intentionally omitted to avoid duplicate declaration)
