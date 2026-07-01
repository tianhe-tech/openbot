package memstore

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

// ---- Directory Extraction ----

// dirContextPhrases are Chinese phrases that typically precede or follow a directory path
// in a conversation about development work (e.g. "在 /root/myapp 目录下").
var dirContextPhrases = []string{
	"目录下", "目录里", "目录中", "目录",
	"路径", "项目路径", "工作目录",
	"下面", "里面", "下开发", "下写",
}

// dirPathRe matches Unix-style absolute paths (/foo/bar) and Windows-style absolute paths
// (C:\foo\bar or D:/foo/bar) that appear in text.  We require at least one path separator
// after the root so bare "/" or "C:\" alone are ignored.
var dirPathRe = regexp.MustCompile(
	`(?:^|[\s\(\["'\x{300C}\x{300D}\x{3010}\x{3011}])(` + // leading boundary
		`(?:[A-Za-z]:[/\\][^\s\x{300C}\x{300D}\x{3010}\x{3011}"'\)\]\x{3002}\x{FF01}\x{FF0C}]+)` + // Windows: C:\...
		`|(?:/[^\s\x{300C}\x{300D}\x{3010}\x{3011}"'\)\]\x{3002}\x{FF01}\x{FF0C}]{2,})` + // Unix: /...
		`)`,
)

// ExtractDirFromText attempts to find a filesystem path mentioned in the request text.
// It combines two strategies:
//  1. Regex scan for Unix/Windows absolute paths.
//  2. Context-phrase gating: the path must appear near a directory-context phrase OR
//     stand alone as a likely path (starts with / or drive letter).
//
// Returns "" when nothing convincing is found.
func ExtractDirFromText(text string) string {
	matches := dirPathRe.FindAllStringSubmatch(text, -1)
	if len(matches) == 0 {
		return ""
	}

	// If a directory-context phrase is nearby, return the first path match.
	lower := strings.ToLower(text)
	for _, phrase := range dirContextPhrases {
		if strings.Contains(lower, phrase) {
			// Return the first captured path from regex.
			for _, m := range matches {
				if len(m) >= 2 {
					return strings.TrimRight(m[1], "/\\")
				}
			}
		}
	}

	// Even without a context phrase, if the text is a short imperative (≤120 runes)
	// and contains an absolute path, capture it — e.g. "帮我在 /data/proj 开发爬虫".
	if len([]rune(text)) <= 120 {
		for _, m := range matches {
			if len(m) >= 2 {
				return strings.TrimRight(m[1], "/\\")
			}
		}
	}

	return ""
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

	// Fallback: capture a standalone ASCII identifier (≥3 chars, contains a letter)
	// that follows an action verb. Catches phrases like "做了 aicfs"、"开发 aicfs 的 xxx".
	if id := extractIdentAfterVerb(text); id != "" {
		return truncateRunes(id, 16)
	}

	return ""
}

// identAfterVerbRe matches an action verb followed (within a few spaces/punct chars)
// by an ASCII identifier of 3+ chars containing at least one letter.
var identAfterVerbRe = regexp.MustCompile(
	`(?:开发|做|搞|写|搭建|实现|编写|新建|创建|部署|帮我做|帮我写|帮我开发)[了过\s:：,，的个]{0,4}([A-Za-z][A-Za-z0-9_\-]{2,})`,
)

// genericIdents are common English words that look like identifiers but are not
// real project names.
var genericIdents = map[string]struct{}{
	"app": {}, "api": {}, "bot": {}, "demo": {}, "test": {}, "tests": {},
	"the": {}, "and": {}, "for": {}, "with": {}, "new": {}, "old": {},
	"docker": {}, "k8s": {}, "项目": {},
}

func extractIdentAfterVerb(text string) string {
	m := identAfterVerbRe.FindStringSubmatch(text)
	if len(m) < 2 {
		return ""
	}
	ident := m[1]
	if _, generic := genericIdents[strings.ToLower(ident)]; generic {
		return ""
	}
	return ident
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

// ---- helpers ----

func truncateRunes(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes]) + "…"
}
