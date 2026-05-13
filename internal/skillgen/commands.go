package skillgen

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/user/opencode-gateway/internal/memstore"
)

// HandleCommand attempts to handle a slash command related to skill review.
// Returns (reply, true) when the command was recognized and handled; the
// caller must NOT forward the original message to opencode in that case.
// Returns ("", false) when the message is not a skill-* command.
//
// Supported commands (all case-insensitive, leading `/`):
//
//	/skill-list [pending|approved|rejected|all]  — list candidates
//	/skill-view <id>                             — print full SKILL.md
//	/skill-approve <id>                          — mark approved + move to install dir
//	/skill-reject <id> [reason]                  — mark rejected + delete draft
//	/skill-stats                                 — per-model approval stats
//	/skill-help                                  — this help
func (s *Service) HandleCommand(adapter, userID, message string) (string, bool) {
	if s == nil || s.store == nil {
		return "", false
	}
	trimmed := strings.TrimSpace(message)
	if !strings.HasPrefix(trimmed, "/skill-") && trimmed != "/skill-help" {
		return "", false
	}
	fields := strings.Fields(trimmed)
	if len(fields) == 0 {
		return "", false
	}
	cmd := strings.ToLower(fields[0])
	args := fields[1:]

	switch cmd {
	case "/skill-help":
		return s.cmdHelp(), true
	case "/skill-list":
		return s.cmdList(args), true
	case "/skill-view":
		return s.cmdView(args), true
	case "/skill-approve":
		return s.cmdApprove(args), true
	case "/skill-reject":
		return s.cmdReject(args), true
	case "/skill-stats":
		return s.cmdStats(), true
	}
	return "", false
}

func (s *Service) cmdHelp() string {
	return "【Skill Autogen 命令】\n" +
		"/skill-list [pending|approved|rejected|all]  列出候选技能（默认 pending）\n" +
		"/skill-view <id>                             查看某个候选的完整 SKILL.md\n" +
		"/skill-approve <id>                          批准并安装到 skills/ 目录\n" +
		"/skill-reject <id> [原因]                    拒绝并删除草稿\n" +
		"/skill-stats                                 查看各模型的生成/通过统计\n" +
		"/skill-help                                  显示本帮助"
}

func (s *Service) cmdList(args []string) string {
	status := memstore.SkillStatusPendingReview
	if len(args) > 0 {
		switch strings.ToLower(args[0]) {
		case "pending":
			status = memstore.SkillStatusPendingReview
		case "approved":
			status = memstore.SkillStatusApproved
		case "rejected":
			status = memstore.SkillStatusRejected
		case "draft":
			status = memstore.SkillStatusDraft
		case "all":
			status = ""
		}
	}
	list, err := s.store.ListSkillCandidatesByStatus(status, 20)
	if err != nil {
		return fmt.Sprintf("❌ 查询失败：%v", err)
	}
	if len(list) == 0 {
		return "（没有符合条件的候选技能）"
	}
	var b strings.Builder
	b.WriteString(fmt.Sprintf("【候选技能 · status=%s · %d 条】\n", statusLabel(status), len(list)))
	for _, c := range list {
		b.WriteString(fmt.Sprintf("• %s  %s  score=%.2f  model=%s  trigger=%s\n  id=%s  created=%s\n",
			c.Title, statusEmoji(c.Status), c.Score, short(c.ModelID, 24), c.Trigger,
			c.ID, c.CreatedAt.Format("2006-01-02 15:04")))
	}
	return strings.TrimRight(b.String(), "\n")
}

func (s *Service) cmdView(args []string) string {
	if len(args) == 0 {
		return "用法：/skill-view <id>"
	}
	c, err := s.store.GetSkillCandidate(args[0])
	if err != nil || c == nil {
		return fmt.Sprintf("❌ 未找到候选 %s", args[0])
	}
	return fmt.Sprintf("【%s】 id=%s  status=%s  score=%.2f  model=%s\npath: %s\n\n---\n%s",
		c.Title, c.ID, c.Status, c.Score, c.ModelID, c.DraftPath, c.SkillMD)
}

func (s *Service) cmdApprove(args []string) string {
	if len(args) == 0 {
		return "用法：/skill-approve <id>"
	}
	c, err := s.store.GetSkillCandidate(args[0])
	if err != nil || c == nil {
		return fmt.Sprintf("❌ 未找到候选 %s", args[0])
	}
	if c.Status == memstore.SkillStatusApproved {
		return fmt.Sprintf("ℹ️ 候选 %s 已经是 approved 状态", c.ID)
	}
	// Move draft file from candidate dir to install dir.
	installedPath, err := moveSkillFile(c, s.cfg.InstallDir)
	if err != nil {
		return fmt.Sprintf("❌ 安装失败：%v", err)
	}
	c.Status = memstore.SkillStatusApproved
	c.ReviewedAt = time.Now()
	c.DraftPath = installedPath
	if err := s.store.SaveSkillCandidate(*c); err != nil {
		return fmt.Sprintf("❌ 已安装文件但状态保存失败：%v", err)
	}
	if c.ModelID != "" {
		_ = s.store.RecordModelOutcome(c.ModelID, true, c.Score)
	}
	return fmt.Sprintf("✅ 已批准并安装：%s\n路径：%s", c.Title, installedPath)
}

func (s *Service) cmdReject(args []string) string {
	if len(args) == 0 {
		return "用法：/skill-reject <id> [原因]"
	}
	c, err := s.store.GetSkillCandidate(args[0])
	if err != nil || c == nil {
		return fmt.Sprintf("❌ 未找到候选 %s", args[0])
	}
	if c.Status == memstore.SkillStatusRejected {
		return fmt.Sprintf("ℹ️ 候选 %s 已经是 rejected 状态", c.ID)
	}
	reason := ""
	if len(args) > 1 {
		reason = strings.Join(args[1:], " ")
	}
	// Delete the draft file (best-effort).
	if c.DraftPath != "" {
		if dir := filepath.Dir(c.DraftPath); strings.Contains(dir, s.cfg.CandidateDir) {
			_ = os.RemoveAll(dir)
		}
	}
	c.Status = memstore.SkillStatusRejected
	c.ReviewedAt = time.Now()
	c.Notes = reason
	if err := s.store.SaveSkillCandidate(*c); err != nil {
		return fmt.Sprintf("❌ 状态保存失败：%v", err)
	}
	if c.ModelID != "" {
		_ = s.store.RecordModelOutcome(c.ModelID, false, 0)
	}
	return fmt.Sprintf("🗑 已拒绝：%s", c.Title)
}

func (s *Service) cmdStats() string {
	rows, err := s.store.ListModelStats()
	if err != nil {
		return fmt.Sprintf("❌ 查询失败：%v", err)
	}
	if len(rows) == 0 {
		return "（尚无模型统计）"
	}
	sort.Slice(rows, func(i, j int) bool {
		ri := approvalRate(rows[i])
		rj := approvalRate(rows[j])
		return ri > rj
	})
	var b strings.Builder
	b.WriteString("【Skill-Gen 模型统计】\n")
	for _, r := range rows {
		rate := approvalRate(r)
		b.WriteString(fmt.Sprintf("• %s  attempts=%d  ✅%d  ❌%d  approval=%.1f%%  avgScore=%.2f\n",
			short(r.ModelID, 40), r.Attempts, r.Approved, r.Rejected, rate*100, r.AvgScore))
	}
	return strings.TrimRight(b.String(), "\n")
}

func approvalRate(m memstore.ModelStats) float64 {
	total := m.Approved + m.Rejected
	if total == 0 {
		return 0
	}
	return float64(m.Approved) / float64(total)
}

func moveSkillFile(c *memstore.SkillCandidate, installDir string) (string, error) {
	slug := slugify(c.Title)
	if slug == "" {
		return "", fmt.Errorf("skillgen: invalid title")
	}
	destDir := filepath.Join(installDir, slug)
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return "", err
	}
	destPath := filepath.Join(destDir, "SKILL.md")
	if err := os.WriteFile(destPath, []byte(c.SkillMD), 0o644); err != nil {
		return "", err
	}
	// Delete source draft directory if it differs.
	if c.DraftPath != "" {
		srcDir := filepath.Dir(c.DraftPath)
		if srcDir != destDir {
			_ = os.RemoveAll(srcDir)
		}
	}
	return destPath, nil
}

func statusLabel(s string) string {
	if s == "" {
		return "all"
	}
	return s
}

func statusEmoji(s string) string {
	switch s {
	case memstore.SkillStatusApproved:
		return "✅"
	case memstore.SkillStatusRejected:
		return "❌"
	case memstore.SkillStatusPendingReview:
		return "⏳"
	default:
		return "·"
	}
}

func short(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
