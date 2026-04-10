package memstore

import (
	"fmt"
	"strings"
	"time"
	"unicode"
)

// ---- Intent Detection ----

// recallTriggers are Chinese phrases that indicate the user wants to recall past conversations.
var recallTriggers = []string{
	"之前", "以前", "上次", "上一次", "曾经", "历史",
	"回忆", "回顾", "记得", "还记得",
	"开发了", "做过", "写过", "建过", "实现过",
	"之前做的", "以前做的", "我做过", "我开发过",
	"哪天", "什么时候做的", "什么时候开发的",
	// Question-form indicators: always a recall/meta query, never real work
	"是啊", "啥项目", "啥程序", "啥软件",
	"什么项目", "什么程序", "哪些项目", "哪个项目",
	"最近开发", "近期开发", "近来开发",
}

// DetectRecallIntent returns true if the text is likely asking about past work.
func DetectRecallIntent(text string) bool {
	lower := strings.ToLower(text)
	for _, kw := range recallTriggers {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

// DetectTimeWindow returns how many days back the query is asking about.
// Defaults to 30 days for generic recall queries.
func DetectTimeWindow(text string) int {
	lower := strings.ToLower(text)
	switch {
	case strings.Contains(lower, "今天") || strings.Contains(lower, "今日"):
		return 1
	case strings.Contains(lower, "昨天") || strings.Contains(lower, "昨日"):
		return 2
	case strings.Contains(lower, "这周") || strings.Contains(lower, "本周") ||
		strings.Contains(lower, "这星期") || strings.Contains(lower, "这个星期"):
		return 7
	case strings.Contains(lower, "上周") || strings.Contains(lower, "上个星期"):
		return 14
	case strings.Contains(lower, "这个月") || strings.Contains(lower, "本月"):
		return 30
	case strings.Contains(lower, "最近") || strings.Contains(lower, "近期") ||
		strings.Contains(lower, "近来") || strings.Contains(lower, "近几天"):
		return 30
	default:
		return 30
	}
}

// ---- Keyword Extraction ----

// stopWords are common Chinese function words to skip during keyword extraction.
var stopWords = map[string]struct{}{
	"的": {}, "了": {}, "是": {}, "在": {}, "我": {}, "你": {}, "他": {},
	"她": {}, "它": {}, "们": {}, "这": {}, "那": {}, "个": {}, "和": {},
	"与": {}, "或": {}, "但": {}, "也": {}, "一": {}, "有": {}, "没": {},
	"不": {}, "都": {}, "很": {}, "要": {}, "会": {}, "能": {}, "可": {},
	"就": {}, "从": {}, "到": {}, "对": {}, "为": {}, "以": {}, "把": {},
	"让": {}, "被": {}, "吗": {}, "呢": {}, "吧": {}, "啊": {}, "哦": {},
	"嗯": {}, "么": {}, "什": {}, "哪": {}, "谁": {}, "怎": {}, "如": {},
	"给": {}, "用": {}, "去": {}, "来": {}, "还": {}, "再": {},
}

// ExtractKeywords returns meaningful keywords from text (for tag storage and search).
func ExtractKeywords(text string) []string {
	var result []string
	seen := make(map[string]struct{})

	// Extract CJK bigrams and trigrams
	runes := []rune(text)
	for i := 0; i < len(runes); i++ {
		for size := 3; size >= 2; size-- {
			if i+size > len(runes) {
				continue
			}
			chunk := string(runes[i : i+size])
			// Skip if any rune is not CJK or letter/digit
			valid := true
			for _, r := range chunk {
				if !unicode.Is(unicode.Han, r) && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
					valid = false
					break
				}
			}
			if !valid {
				continue
			}
			low := strings.ToLower(chunk)
			if _, stop := stopWords[chunk]; stop {
				continue
			}
			if _, dup := seen[low]; dup {
				continue
			}
			seen[low] = struct{}{}
			result = append(result, chunk)
		}
	}

	// Extract ASCII words (tech terms, identifiers)
	words := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_'
	})
	for _, w := range words {
		if len(w) < 2 {
			continue
		}
		low := strings.ToLower(w)
		if _, dup := seen[low]; dup {
			continue
		}
		seen[low] = struct{}{}
		result = append(result, w)
	}

	// Limit to top 15 keywords
	if len(result) > 15 {
		result = result[:15]
	}
	return result
}

// ---- Action & Project Extraction ----

type actionPattern struct {
	action   string
	keywords []string
}

var actionPatterns = []actionPattern{
	{"创建", []string{"创建", "新建", "建立", "开发", "写", "编写", "实现", "做", "搭建"}},
	{"修改", []string{"修改", "更新", "调整", "改", "优化", "完善", "重构"}},
	{"调试", []string{"调试", "debug", "排查", "修复", "解决", "修bug", "fix"}},
	{"部署", []string{"部署", "发布", "上线", "运行", "启动", "docker", "k8s"}},
	{"查询", []string{"查询", "查看", "获取", "读取", "查找", "搜索"}},
}

// ExtractAction returns a coarse action label from the request text.
func ExtractAction(text string) string {
	lower := strings.ToLower(text)
	for _, ap := range actionPatterns {
		for _, kw := range ap.keywords {
			if strings.Contains(lower, kw) {
				return ap.action
			}
		}
	}
	return "other"
}

// projectHints are anchor words that often precede a project description.
var projectHints = []string{
	"项目", "系统", "工具", "程序", "脚本", "服务", "平台", "应用",
	"爬虫", "接口", "API", "机器人", "bot", "插件", "功能", "模块",
}

// interrogativePrefixes marks extracted text that is a question pronoun, not a real project name.
var interrogativePrefixes = []string{"什么", "哪些", "哪个", "哪", "怎么", "为什么", "为何", "多少", "怎样", "什"}

func hasInterrogativePrefix(s string) bool {
	for _, p := range interrogativePrefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

// bareGenericNouns are hint words that when extracted alone (no real modifier before them)
// are not meaningful project names and should be discarded.
var bareGenericNouns = map[string]struct{}{
	"项目": {}, "系统": {}, "工具": {}, "程序": {}, "脚本": {},
	"服务": {}, "平台": {}, "应用": {}, "接口": {}, "功能": {}, "模块": {},
}

// ExtractProject returns a short project label from the request text.
// Strategy: find the nearest modifier cluster (up to 8 runes) before a project hint word.
// All indexing is done in rune space to avoid byte-offset truncation of multi-byte CJK chars.
func ExtractProject(text string) string {
	runes := []rune(text)
	lowerRunes := []rune(strings.ToLower(text))

	for _, hint := range projectHints {
		hintRunes := []rune(strings.ToLower(hint))
		hintLen := len(hintRunes)

		// Find hint position in rune space
		hintIdx := -1
		for i := 0; i <= len(lowerRunes)-hintLen; i++ {
			match := true
			for j := 0; j < hintLen; j++ {
				if lowerRunes[i+j] != hintRunes[j] {
					match = false
					break
				}
			}
			if match {
				hintIdx = i
				break
			}
		}
		if hintIdx < 0 {
			continue
		}

		// Grab up to 8 runes before the hint + the hint itself
		start := hintIdx - 8
		if start < 0 {
			start = 0
		}
		end := hintIdx + hintLen

		project := strings.TrimSpace(string(runes[start:end]))
		// Remove leading non-CJK / non-alpha runes
		project = strings.TrimLeftFunc(project, func(r rune) bool {
			return !unicode.Is(unicode.Han, r) && !unicode.IsLetter(r)
		})
		if len([]rune(project)) < 2 || hasInterrogativePrefix(project) {
			continue
		}
		// Discard bare generic nouns with no real modifier (e.g. "项目" alone is not a project name)
		if _, bare := bareGenericNouns[project]; bare {
			continue
		}
		return truncateRunes(project, 10)
	}

	return ""
}

// ---- Summary Construction ----

// BuildSummary creates a one-line summary from request and response used for storage and recall context.
func BuildSummary(request, response, action, project string) string {
	req := truncateRunes(request, 60)
	if project != "" && action != "" && action != "other" {
		return fmt.Sprintf("[%s] %s：%s", action, project, req)
	}
	if action != "" && action != "other" {
		return fmt.Sprintf("[%s] %s", action, req)
	}
	return req
}

// BuildRecallContext formats a list of records into a prompt context string.
// Project summaries are capped at 5; detail records are deduplicated by (project,date) and capped at 5.
func BuildRecallContext(records []MemRecord, projectSummaries []ProjectSummary) string {
	if len(records) == 0 && len(projectSummaries) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("【历史工作记忆】以下是用户过去的工作记录，供参考：\n\n")

	// Cap project overview at 5
	shown := projectSummaries
	if len(shown) > 5 {
		shown = shown[:5]
	}
	if len(shown) > 0 {
		sb.WriteString("## 项目概览\n")
		for _, ps := range shown {
			daysSince := int(time.Since(ps.Last).Hours() / 24)
			actions := strings.Join(unique(ps.Actions), "、")
			sb.WriteString(fmt.Sprintf("- **%s**（%s）：共 %d 次操作（%s），最近一次在 %d 天前（%s）\n",
				ps.Project, ps.Adapter,
				ps.Count, actions,
				daysSince, ps.Last.Format("2006-01-02"),
			))
		}
		sb.WriteString("\n")
	}

	// Deduplicate detail records by (project, date), cap at 5
	if len(records) > 0 {
		seen := make(map[string]struct{})
		var deduped []MemRecord
		for _, r := range records {
			key := r.Project + "|" + r.Ts.Format("2006-01-02")
			if r.Project == "" {
				key = r.Summary + "|" + r.Ts.Format("2006-01-02")
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			deduped = append(deduped, r)
			if len(deduped) >= 5 {
				break
			}
		}
		if len(deduped) > 0 {
			sb.WriteString("## 相关记录\n")
			for _, r := range deduped {
				sb.WriteString(fmt.Sprintf("- [%s][%s] %s：%s\n",
					r.Ts.Format("2006-01-02"), r.Adapter,
					r.Action, r.Summary,
				))
			}
			sb.WriteString("\n")
		}
	}

	return sb.String()
}

// ---- helpers ----

func truncateRunes(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "…"
}

func unique(ss []string) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, s := range ss {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
