package wechat

import (
	"os"
	"regexp"
	"strings"
	"unicode/utf8"
)

var fenceRE = regexp.MustCompile("^```([^`\n]*)$")
var headerRE = regexp.MustCompile("^(#{1,6})\\s+(.+?)\\s*$")
var tableRuleRE = regexp.MustCompile(`^\s*\|?(?:\s*:?-{3,}:?\s*\|)+\s*:?-{3,}:?\s*\|?\s*$`)

// splitTextForWeixinDelivery splits content into WeChat-friendly message chunks.
//
// Compact mode (default): single message when under the limit, unless the
// content looks like a short chatty exchange (2-6 lines, all short), in which
// case split for more natural chat feel. Oversized content is packed with
// Markdown block awareness.
//
// Per-line mode (splitPerLine=true): top-level line breaks become separate messages.
func splitTextForWeixinDelivery(content string, maxLength int, splitPerLine bool) []string {
	if strings.TrimSpace(content) == "" {
		return nil
	}
	if splitPerLine {
		// Legacy: one message per top-level delivery unit
		if len([]rune(content)) <= maxLength && !strings.Contains(content, "\n") {
			return []string{content}
		}
		var chunks []string
		for _, unit := range splitDeliveryUnits(content) {
			if len([]rune(unit)) <= maxLength {
				chunks = append(chunks, unit)
				continue
			}
			chunks = append(chunks, packMarkdownBlocks(unit, maxLength)...)
		}
		return filterEmpty(chunks, content)
	}

	// Compact mode
	runes := []rune(content)
	if len(runes) <= maxLength {
		if shortChatAutoSplitEnabled() && shouldSplitShortChatBlock(content) {
			return filterEmpty(splitDeliveryUnits(content), content)
		}
		return []string{content}
	}
	result := filterEmpty(packMarkdownBlocks(content, maxLength), content)
	if len(result) == 0 {
		return []string{content}
	}
	return result
}

func filterEmpty(chunks []string, fallback string) []string {
	var out []string
	for _, c := range chunks {
		if strings.TrimSpace(c) != "" {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// wrapCopyFriendlyLines wraps long lines (>120 chars) that are hard to copy in WeChat.
func wrapCopyFriendlyLines(content string) string {
	wrapWidth := 120
	var result []string
	inCodeBlock := false

	for _, rawLine := range strings.Split(content, "\n") {
		line := strings.TrimRight(rawLine, " \t")
		stripped := strings.TrimSpace(line)
		if fenceRE.MatchString(stripped) {
			inCodeBlock = !inCodeBlock
			result = append(result, line)
			continue
		}
		if inCodeBlock || len(line) <= wrapWidth || stripped == "" ||
			strings.HasPrefix(stripped, "|") || tableRuleRE.MatchString(stripped) {
			result = append(result, line)
			continue
		}
		// Wrap long lines
		result = append(result, wordWrap(line, wrapWidth)...)
	}
	return strings.Join(result, "\n")
}

// wordWrap wraps text at width, respecting rune boundaries.
func wordWrap(text string, width int) []string {
	var lines []string
	for len(text) > 0 {
		if utf8.RuneCountInString(text) <= width {
			lines = append(lines, text)
			break
		}
		// Find a good break point
		runes := []rune(text)
		breakAt := width
		// Try to break at a space
		for i := width; i > width-20 && i > 0; i-- {
			if runes[i-1] == ' ' {
				breakAt = i
				break
			}
		}
		lines = append(lines, string(runes[:breakAt]))
		text = string(runes[breakAt:])
	}
	return lines
}

// splitDeliveryUnits splits formatted content into chat-friendly delivery units.
// Keeps fenced code blocks intact; blank lines become unit separators.
func splitDeliveryUnits(content string) []string {
	var units []string
	for _, block := range splitMarkdownBlocks(content) {
		if fenceRE.MatchString(strings.TrimSpace(strings.Split(block, "\n")[0])) {
			units = append(units, block)
			continue
		}
		var current []string
		for _, rawLine := range strings.Split(block, "\n") {
			line := strings.TrimRight(rawLine, " \t")
			if strings.TrimSpace(line) == "" {
				if len(current) > 0 {
					units = append(units, strings.TrimSpace(strings.Join(current, "\n")))
					current = nil
				}
				continue
			}
			isContinuation := len(current) > 0 && (strings.HasPrefix(rawLine, " ") || strings.HasPrefix(rawLine, "\t"))
			if isContinuation {
				current = append(current, line)
				continue
			}
			if len(current) > 0 {
				units = append(units, strings.TrimSpace(strings.Join(current, "\n")))
			}
			current = []string{line}
		}
		if len(current) > 0 {
			units = append(units, strings.TrimSpace(strings.Join(current, "\n")))
		}
	}
	return units
}

// splitMarkdownBlocks splits content at blank-line boundaries,
// keeping fenced code blocks intact.
func splitMarkdownBlocks(content string) []string {
	if strings.TrimSpace(content) == "" {
		return nil
	}
	var blocks []string
	var current []string
	inCodeBlock := false

	for _, rawLine := range strings.Split(content, "\n") {
		line := strings.TrimRight(rawLine, " \t")
		if fenceRE.MatchString(strings.TrimSpace(line)) {
			if !inCodeBlock && len(current) > 0 {
				blocks = append(blocks, strings.TrimSpace(strings.Join(current, "\n")))
				current = nil
			}
			current = append(current, line)
			inCodeBlock = !inCodeBlock
			if !inCodeBlock {
				blocks = append(blocks, strings.TrimSpace(strings.Join(current, "\n")))
				current = nil
			}
			continue
		}
		if inCodeBlock {
			current = append(current, line)
			continue
		}
		if strings.TrimSpace(line) == "" {
			if len(current) > 0 {
				blocks = append(blocks, strings.TrimSpace(strings.Join(current, "\n")))
				current = nil
			}
			continue
		}
		current = append(current, line)
	}
	if len(current) > 0 {
		blocks = append(blocks, strings.TrimSpace(strings.Join(current, "\n")))
	}
	return blocks
}

// looksLikeChattyLine returns true when a line looks like a standalone chat utterance.
func looksLikeChattyLine(line string) bool {
	s := strings.TrimSpace(line)
	if s == "" {
		return false
	}
	runes := []rune(s)
	if len(runes) > 48 {
		return false
	}
	if strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
		return false
	}
	if strings.HasPrefix(s, ">") || strings.HasPrefix(s, "-") || strings.HasPrefix(s, "*") ||
		strings.HasPrefix(s, "【") || strings.HasPrefix(s, "#") || strings.HasPrefix(s, "|") {
		return false
	}
	if tableRuleRE.MatchString(s) {
		return false
	}
	if strings.Count(s, "*") >= 2 && strings.HasPrefix(s, "**") && strings.HasSuffix(s, "**") {
		return false
	}
	if matched, _ := regexp.MatchString(`^\d+\.\s`, s); matched {
		return false
	}
	return true
}

// looksLikeHeadingLine returns true when a short line behaves like a heading.
func looksLikeHeadingLine(line string) bool {
	s := strings.TrimSpace(line)
	if s == "" {
		return false
	}
	if headerRE.MatchString(s) {
		return true
	}
	runes := []rune(s)
	return len(runes) <= 24 && (strings.HasSuffix(s, ":") || strings.HasSuffix(s, "："))
}

// shouldSplitShortChatBlock returns true when a block of 2-6 short lines should
// be split into separate messages for a more natural chat feel.
func shouldSplitShortChatBlock(content string) bool {
	var lines []string
	for _, l := range strings.Split(content, "\n") {
		if strings.TrimSpace(l) != "" {
			lines = append(lines, l)
		}
	}
	if len(lines) < 2 || len(lines) > 6 {
		return false
	}
	if looksLikeHeadingLine(lines[0]) {
		return false
	}
	for _, l := range lines {
		if !looksLikeChattyLine(l) {
			return false
		}
	}
	return true
}

// shortChatAutoSplitEnabled controls whether compact mode should split short
// chat-style blocks (2-6 short lines) into multiple messages.
//
// Env: WECHAT_SHORT_CHAT_SPLIT
//   - true values: 1, true, yes, on
//   - false values/default: 0, false, no, off, empty
func shortChatAutoSplitEnabled() bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv("WECHAT_SHORT_CHAT_SPLIT")))
	switch v {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// packMarkdownBlocks packs content into length-limited chunks,
// keeping Markdown code blocks intact across chunk boundaries.
func packMarkdownBlocks(content string, maxLength int) []string {
	if len([]rune(content)) <= maxLength {
		return []string{content}
	}
	var chunks []string
	var current []string
	currentLen := 0

	for _, block := range splitMarkdownBlocks(content) {
		blockLen := len([]rune(block))
		// If the block alone exceeds maxLength, split it into smaller pieces
		if blockLen > maxLength {
			// Flush current buffer first
			if len(current) > 0 {
				chunks = append(chunks, strings.Join(current, "\n\n"))
				current = nil
				currentLen = 0
			}
			// If it's a code block, keep it as one
			if fenceRE.MatchString(strings.TrimSpace(strings.Split(block, "\n")[0])) {
				chunks = append(chunks, block)
				continue
			}
			// Split long text block at line boundaries
			for _, line := range strings.Split(block, "\n") {
				lineLen := len([]rune(line))
				if lineLen > maxLength {
					// Extremely long line, split mid-line by rune count
					if len(current) > 0 {
						chunks = append(chunks, strings.Join(current, "\n"))
						current = nil
						currentLen = 0
					}
					runes := []rune(line)
					for start := 0; start < len(runes); start += maxLength {
						end := start + maxLength
						if end > len(runes) {
							end = len(runes)
						}
						chunks = append(chunks, string(runes[start:end]))
					}
					continue
				}
				if currentLen+len(current)+lineLen > maxLength {
					chunks = append(chunks, strings.Join(current, "\n"))
					current = nil
					currentLen = 0
				}
				current = append(current, line)
				currentLen += lineLen
			}
			continue
		}
		// Fits in current chunk?
		separatorLen := 2 // "\n\n" between blocks
		if currentLen+len(current)*separatorLen+blockLen > maxLength {
			chunks = append(chunks, strings.Join(current, "\n\n"))
			current = nil
			currentLen = 0
		}
		current = append(current, block)
		currentLen += blockLen
	}
	if len(current) > 0 {
		chunks = append(chunks, strings.Join(current, "\n\n"))
	}
	return chunks
}
