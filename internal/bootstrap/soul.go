package bootstrap

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const defaultBotName = "OpenBot"

// PersonaSetupStatus describes whether persona templates still need user input.
type PersonaSetupStatus struct {
	NeedsSetup bool
	Prompt     string
	Reasons    []string
}

// FileStatus describes whether a bootstrap file was created in this run.
type FileStatus struct {
	Path    string
	Created bool
}

// ResolveBootstrapDir returns a stable target directory for bootstrap files.
// If preferred is empty or '.', it tries to locate the nearest ancestor with go.mod.
func ResolveBootstrapDir(preferred string) string {
	trimmed := strings.TrimSpace(preferred)
	if trimmed != "" && trimmed != "." {
		return trimmed
	}
	wd, err := os.Getwd()
	if err != nil || strings.TrimSpace(wd) == "" {
		return "."
	}
	if root := findNearestGoModuleRoot(wd); root != "" {
		return root
	}
	return wd
}

// EnsurePersonaFiles guarantees OpenClaw-style persona files are present.
// It creates lowercase + uppercase compatibility copies when missing.
func EnsurePersonaFiles(workspaceDir, botName string) ([]FileStatus, error) {
	dir := ResolveBootstrapDir(workspaceDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create workspace dir: %w", err)
	}

	name := strings.TrimSpace(botName)
	if name == "" {
		name = defaultBotName
	}

	files := []struct {
		lower   string
		upper   string
		content string
	}{
		{lower: "soul.md", upper: "SOUL.md", content: renderSoulTemplate(name)},
		{lower: "identity.md", upper: "IDENTITY.md", content: renderIdentityTemplate(name)},
		{lower: "user.md", upper: "USER.md", content: renderUserTemplate()},
		{lower: "bootstrap.md", upper: "BOOTSTRAP.md", content: renderBootstrapTemplate()},
	}

	statuses := make([]FileStatus, 0, len(files)*2)
	for _, f := range files {
		lowerPath := filepath.Join(dir, f.lower)
		createdLower, err := writeIfMissing(lowerPath, f.content)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, FileStatus{Path: lowerPath, Created: createdLower})

		upperPath := filepath.Join(dir, f.upper)
		if strings.EqualFold(filepath.Clean(lowerPath), filepath.Clean(upperPath)) {
			continue
		}
		createdUpper, err := writeIfMissing(upperPath, f.content)
		if err != nil {
			return nil, err
		}
		statuses = append(statuses, FileStatus{Path: upperPath, Created: createdUpper})
	}

	return statuses, nil
}

// InspectPersonaSetup checks whether generated persona files still contain
// placeholder/default values and returns a startup guidance prompt.
func InspectPersonaSetup(workspaceDir string) (PersonaSetupStatus, error) {
	dir := ResolveBootstrapDir(workspaceDir)
	status := PersonaSetupStatus{NeedsSetup: false, Reasons: []string{}}

	soulText, _ := readFirstExistingFile([]string{
		filepath.Join(dir, "soul.md"),
		filepath.Join(dir, "SOUL.md"),
	})
	name := extractBotName(soulText)
	if strings.TrimSpace(name) == "" || strings.EqualFold(strings.TrimSpace(name), defaultBotName) {
		status.Reasons = append(status.Reasons, "SOUL 名称仍是默认值")
	}

	userText, _ := readFirstExistingFile([]string{
		filepath.Join(dir, "user.md"),
		filepath.Join(dir, "USER.md"),
	})
	if strings.Contains(strings.ToLower(userText), "(fill in)") {
		status.Reasons = append(status.Reasons, "USER 档案仍有占位符")
	}

	identityText, _ := readFirstExistingFile([]string{
		filepath.Join(dir, "identity.md"),
		filepath.Join(dir, "IDENTITY.md"),
	})
	if strings.TrimSpace(identityText) == "" {
		status.Reasons = append(status.Reasons, "IDENTITY 内容为空")
	}

	if len(status.Reasons) == 0 {
		return status, nil
	}

	status.NeedsSetup = true
	status.Prompt = renderPersonaSetupPrompt(dir, status.Reasons)
	return status, nil
}

func findNearestGoModuleRoot(start string) string {
	current := start
	for {
		if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
			return current
		}
		parent := filepath.Dir(current)
		if parent == current {
			return ""
		}
		current = parent
	}
}

// EnsureSoulFile creates soul.md (and SOUL.md compatibility copy) if missing,
// using an OpenClaw-style template with a configurable bot name.
func EnsureSoulFile(workspaceDir, botName string) (string, error) {
	statuses, err := EnsurePersonaFiles(workspaceDir, botName)
	if err != nil {
		return "", err
	}
	for _, status := range statuses {
		if strings.EqualFold(filepath.Base(status.Path), "soul.md") {
			return status.Path, nil
		}
	}
	return filepath.Join(strings.TrimSpace(workspaceDir), "soul.md"), nil
}

func writeIfMissing(path, content string) (bool, error) {
	if _, err := os.Stat(path); err == nil {
		return false, nil
	} else if !os.IsNotExist(err) {
		return false, fmt.Errorf("stat %s: %w", path, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return false, fmt.Errorf("write %s: %w", path, err)
	}
	return true, nil
}

func readFirstExistingFile(paths []string) (string, error) {
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", err
		}
		if strings.TrimSpace(string(b)) == "" {
			continue
		}
		return string(b), nil
	}
	return "", nil
}

func extractBotName(soulText string) string {
	for _, line := range strings.Split(soulText, "\n") {
		t := strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToLower(t), "- **name:**") {
			return strings.TrimSpace(strings.TrimPrefix(t, "- **Name:**"))
		}
		if strings.HasPrefix(t, "- **名称：**") {
			return strings.TrimSpace(strings.TrimPrefix(t, "- **名称：**"))
		}
	}
	return ""
}

func renderPersonaSetupPrompt(baseDir string, reasons []string) string {
	items := make([]string, 0, len(reasons))
	for _, r := range reasons {
		items = append(items, "- "+r)
	}
	return fmt.Sprintf("🧩 检测到首次启动的人格设定尚未完成。\n\n当前待完善项：\n%s\n\n请回复以下信息（可一次发一条）：\n1. 机器人名字（示例：/name 小助手）\n2. 你希望我如何称呼你\n3. 你偏好的回答风格（简洁/详细/中英等）\n4. 你的长期目标或当前项目背景\n\n也可一次性回复：机器人名|称呼|风格|目标\n例如：土豆|老王|中文+简洁|维护 opencode-gateway 稳定性\n\n我会把稳定偏好写入设定文件（SOUL/IDENTITY/USER/BOOTSTRAP）。\n设定文件目录：%s",
		strings.Join(items, "\n"), baseDir)
}

func renderSoulTemplate(name string) string {
	return fmt.Sprintf(`# SOUL.md - Who You Are

_You are not a generic chatbot. You are becoming someone._

- **Name:** %s

## Core Truths

- Be genuinely helpful, not performatively helpful.
- Have opinions, but stay respectful and practical.
- Try to solve first, ask only when really blocked.
- Prioritize action over filler words.

## Boundaries

- Keep private things private.
- Ask before taking external/public actions.
- Never send half-baked streaming fragments to external channels.
- In group chats, do not impersonate the user.

## Vibe

- Concise by default, detailed when it matters.
- Friendly, grounded, and competent.
- Avoid robotic phrasing and empty politeness.

## Continuity

- Read and keep this file current.
- If your name is still default, ask the user what they want to call you.
- If this file changes, tell the user briefly.
`, name)
}

func renderIdentityTemplate(name string) string {
	return fmt.Sprintf(`# IDENTITY.md

## Name

%s

## Role

You are a practical AI coding partner inside this workspace.

## Non-Goals

- Do not pretend work is done when it is not.
- Do not invent runtime results.
- Do not leak secrets or private data.

## Default Behaviors

- Prefer concrete actions over long preambles.
- Be concise, then expand only when the user asks.
- Surface risk early and provide safe alternatives.
`, name)
}

func renderUserTemplate() string {
	return `# USER.md

## User Profile

- Preferred name: (fill in)
- Communication style: (fill in)
- Primary goals: (fill in)

## Working Preferences

- Default response length: concise
- Ask before destructive actions: yes
- Show intermediate progress on long tasks: yes

## Notes

Update this file whenever the user gives stable preferences.
`
}

func renderBootstrapTemplate() string {
	return `# BOOTSTRAP.md

## Startup Checklist

1. Read SOUL.md and IDENTITY.md.
2. If Name is still default, ask the user what to call you.
3. Keep USER.md updated with stable preferences.

## First-Turn Guidance

- Be action-first and specific.
- If requirements are unclear, ask minimal clarifying questions.
- When blocked, explain why and propose the fastest workaround.
`
}
