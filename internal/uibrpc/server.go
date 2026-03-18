package uibrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/user/opencode-gateway/internal/bootstrap"
	"github.com/user/opencode-gateway/internal/opencode"
)

type Server struct {
	proxyKey     string
	oc           *opencode.Client
	restartServe func(context.Context, string) (string, error)

	mu       sync.Mutex
	sessions map[string]*uiSessionState // ui_session_id -> state
}

type uiSessionState struct {
	UISessionID     string `json:"ui_session_id"`
	SessionID       string `json:"session_id"`
	ThreadID        string `json:"thread_id"`
	ProviderBaseURL string `json:"provider_base_url"`
	APIKey          string `json:"api_key"`
	Model           string `json:"model"`
	Soul            string `json:"soul"`
	UpdatedAt       string `json:"updated_at"`
	PendingReset    bool   `json:"pending_reset,omitempty"`
	ResetFrom       string `json:"reset_from,omitempty"`
}

type chatReq struct {
	UISessionID string `json:"ui_session_id"`
	Message     string `json:"message"`
}

type modelReq struct {
	UISessionID     string `json:"ui_session_id"`
	ProviderBaseURL string `json:"provider_base_url"`
	APIKey          string `json:"api_key"`
	Model           string `json:"model"`
}

type soulReq struct {
	UISessionID string `json:"ui_session_id"`
	Soul        string `json:"soul"`
}

type stateReq struct {
	UISessionID string `json:"ui_session_id"`
}

type skillInstallReq struct {
	Skill string `json:"skill"`
}

type skillInfo struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	Path         string `json:"path"`
	Source       string `json:"source"`
	CanUninstall bool   `json:"can_uninstall"`
}

type restartReq struct {
	Reason string `json:"reason,omitempty"`
}

func NewServer(proxyKey string, oc *opencode.Client, restartServe func(context.Context, string) (string, error)) *Server {
	return &Server{
		proxyKey:     proxyKey,
		oc:           oc,
		restartServe: restartServe,
		sessions:     make(map[string]*uiSessionState),
	}
}

func (s *Server) Handle(ctx context.Context, action string, payload json.RawMessage) (interface{}, error) {
	switch strings.TrimSpace(action) {
	case "chat":
		var req chatReq
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, fmt.Errorf("invalid chat payload")
		}
		return s.handleChat(ctx, req)
	case "set_model_config":
		var req modelReq
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, fmt.Errorf("invalid model payload")
		}
		return s.handleSetModel(req)
	case "set_soul":
		var req soulReq
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, fmt.Errorf("invalid soul payload")
		}
		return s.handleSetSoul(req)
	case "get_state":
		var req stateReq
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, fmt.Errorf("invalid state payload")
		}
		return s.handleGetState(req)
	case "skill_install":
		var req skillInstallReq
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, fmt.Errorf("invalid skill payload")
		}
		return s.handleSkillInstall(ctx, req)
	case "skill_uninstall":
		var req skillInstallReq
		if err := json.Unmarshal(payload, &req); err != nil {
			return nil, fmt.Errorf("invalid skill payload")
		}
		return s.handleSkillUninstall(ctx, req)
	case "skillshub_list":
		return s.handleOpencodeSkillsList(ctx)
	case "list_models":
		return s.handleOpencodeModelsList(ctx)
	case "list_skills":
		return s.handleOpencodeSkillsList(ctx)
	case "restart_opencode_serve":
		var req restartReq
		_ = json.Unmarshal(payload, &req)
		return s.handleRestartOpenCodeServe(ctx, req)
	default:
		return nil, fmt.Errorf("unsupported action: %s", action)
	}
}

func (s *Server) handleChat(ctx context.Context, req chatReq) (interface{}, error) {
	msg := strings.TrimSpace(req.Message)
	if msg == "" {
		return nil, fmt.Errorf("消息不能为空")
	}
	if len(msg) > 4000 {
		return nil, fmt.Errorf("消息长度不能超过 4000")
	}

	state := s.getState(req.UISessionID)
	if strings.HasPrefix(msg, "/") {
		return s.handleSlashCommand(ctx, state, msg)
	}

	if state.PendingReset {
		return s.handlePendingResetChoice(ctx, state, msg)
	}

	content := msg

	if state.SessionID != "" && strings.TrimSpace(state.Model) != "" {
		_ = s.oc.UpdateSessionProvider(ctx, state.SessionID, "openai", state.Model)
	}

	resp, err := s.oc.SendMessage(ctx, opencode.MessagePayload{
		Channel:   "webui",
		UserID:    "proxy:" + shortForID(s.proxyKey),
		ThreadID:  state.ThreadID,
		SessionID: state.SessionID,
		Content:   content,
	})
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	state.SessionID = resp.SessionID
	state.UpdatedAt = time.Now().Format(time.RFC3339)
	s.mu.Unlock()

	return map[string]interface{}{
		"reply":      strings.TrimSpace(resp.Reply),
		"session_id": resp.SessionID,
		"trace_id":   resp.Trace,
	}, nil
}

func buildUIResetPromptMessage(sessionID string) string {
	return fmt.Sprintf("当前有活跃会话 %s。\n\n请选择接下来怎么做：\n1. 继续原来的任务（先压缩当前会话，再派生新会话）\n2. 开始新的对话（不继承历史）\n\n请直接回复 1 或 2。", shortForID(sessionID))
}

func parseUIResetChoice(input string) string {
	normalized := strings.TrimSpace(strings.ToLower(input))
	normalized = strings.Map(func(r rune) rune {
		if strings.ContainsRune(" \t\r\n,.;:!?，。；：！？()（）[]【】", r) {
			return -1
		}
		return r
	}, normalized)

	switch normalized {
	case "1", "/1", "继续", "继续原任务", "继续原来的任务", "继续任务", "continue", "/continue", "resume", "keep":
		return "continue"
	case "2", "/2", "新对话", "开始新对话", "开始新的对话", "新任务", "重新开始", "fresh", "new", "/new", "reset":
		return "fresh"
	default:
		return ""
	}
}

func (s *Server) handlePendingResetChoice(ctx context.Context, state *uiSessionState, input string) (interface{}, error) {
	choice := parseUIResetChoice(input)
	if choice == "" {
		return map[string]string{"message": "请直接回复 1 或 2。\n\n" + buildUIResetPromptMessage(state.ResetFrom)}, nil
	}

	if choice == "fresh" {
		s.mu.Lock()
		state.SessionID = ""
		state.ThreadID = "thread-" + state.UISessionID
		state.PendingReset = false
		state.ResetFrom = ""
		state.UpdatedAt = time.Now().Format(time.RFC3339)
		s.mu.Unlock()
		return map[string]string{"message": "已开始新的对话。下条非命令消息将创建全新的会话，不会继承之前的任务上下文。"}, nil
	}

	oldSessionID := state.ResetFrom
	newSessionID, err := s.oc.CompactAndForkSession(ctx, oldSessionID)
	if err != nil {
		return map[string]string{"message": "继续原任务失败: " + err.Error() + "\n\n请回复 1 重试，或回复 2 改为开始新的对话。"}, nil
	}

	s.mu.Lock()
	state.SessionID = newSessionID
	state.PendingReset = false
	state.ResetFrom = ""
	state.UpdatedAt = time.Now().Format(time.RFC3339)
	threadID := state.ThreadID
	model := state.Model
	uiSessionID := state.UISessionID
	s.mu.Unlock()

	if strings.TrimSpace(threadID) == "" {
		threadID = "thread-" + uiSessionID
	}
	s.oc.SetSessionForThread(threadID, newSessionID)
	if strings.TrimSpace(model) != "" {
		_ = s.oc.UpdateSessionProvider(ctx, newSessionID, "openai", model)
	}

	return map[string]string{"message": fmt.Sprintf("已继续原任务。\n\n旧会话: %s\n新会话: %s\n\n后续对话将使用新的派生会话继续处理。", shortForID(oldSessionID), shortForID(newSessionID))}, nil
}

func (s *Server) handleSlashCommand(ctx context.Context, state *uiSessionState, cmd string) (interface{}, error) {
	fields := strings.Fields(cmd)
	if len(fields) == 0 {
		return nil, fmt.Errorf("空命令")
	}

	switch fields[0] {
	case "/help":
		return map[string]string{"message": "可用命令: /help, /model, /model openai/<model>, /provider <base_url> <api_key>, /soul, /soul set <text>, /soul set <vibe|boundaries|core|continuity> <text>, /skills, /skills install <name>, /reset（可选择延续当前任务）"}, nil
	case "/model":
		if len(fields) == 1 {
			return s.handleModelQuery(ctx, state)
		}
		modelArg := strings.TrimSpace(fields[1])
		modelArg = strings.TrimPrefix(modelArg, "openai/")
		if modelArg == "" {
			return nil, fmt.Errorf("模型不能为空")
		}
		s.mu.Lock()
		state.Model = modelArg
		state.UpdatedAt = time.Now().Format(time.RFC3339)
		s.mu.Unlock()
		if state.SessionID != "" {
			_ = s.oc.UpdateSessionProvider(ctx, state.SessionID, "openai", modelArg)
		}
		return map[string]string{"message": "模型已设置为 openai/" + modelArg}, nil
	case "/models":
		return s.handleModelQuery(ctx, state)
	case "/provider":
		if len(fields) < 3 {
			return nil, fmt.Errorf("用法: /provider <base_url> <api_key>")
		}
		s.mu.Lock()
		state.ProviderBaseURL = fields[1]
		state.APIKey = fields[2]
		state.UpdatedAt = time.Now().Format(time.RFC3339)
		s.mu.Unlock()
		return map[string]string{"message": "provider 配置已保存 (openai format)"}, nil
	case "/soul":
		if len(fields) == 1 {
			if fromFile, err := s.loadSoulFromFile(); err == nil && strings.TrimSpace(fromFile) != "" {
				s.mu.Lock()
				state.Soul = fromFile
				state.UpdatedAt = time.Now().Format(time.RFC3339)
				s.mu.Unlock()
			}
			text := strings.TrimSpace(state.Soul)
			if text == "" {
				text = "<未设置>"
			}
			return map[string]string{"message": text}, nil
		}
		if len(fields) >= 3 && fields[1] == "set" {
			section, text := parseSoulSetInput(fields, cmd)
			if text == "" {
				return nil, fmt.Errorf("soul 不能为空")
			}
			if err := s.saveSoulToFile(section, text); err != nil {
				return nil, fmt.Errorf("保存 soul.md 失败: %w", err)
			}
			latest := text
			if fromFile, err := s.loadSoulFromFile(); err == nil && strings.TrimSpace(fromFile) != "" {
				latest = fromFile
			}
			s.mu.Lock()
			state.Soul = latest
			state.UpdatedAt = time.Now().Format(time.RFC3339)
			s.mu.Unlock()
			return map[string]string{"message": fmt.Sprintf("灵魂设定已更新（写入 SOUL 的 %s 段），并已同步到 soul.md", soulSectionLabel(section))}, nil
		}
		return nil, fmt.Errorf("用法: /soul 或 /soul set <text> 或 /soul set <vibe|boundaries|core|continuity> <text>")
	case "/skills":
		if len(fields) == 1 {
			return s.handleSkillsQuery(ctx)
		}
		if len(fields) >= 3 && fields[1] == "install" {
			return s.handleSkillInstall(ctx, skillInstallReq{Skill: fields[2]})
		}
		return nil, fmt.Errorf("用法: /skills 或 /skills install <name>")
	case "/reset":
		if strings.TrimSpace(state.SessionID) == "" {
			s.mu.Lock()
			state.SessionID = ""
			state.ThreadID = "thread-" + state.UISessionID
			state.PendingReset = false
			state.ResetFrom = ""
			state.UpdatedAt = time.Now().Format(time.RFC3339)
			s.mu.Unlock()
			return map[string]string{"message": "当前没有活跃会话。下条消息将创建新会话。"}, nil
		}

		s.mu.Lock()
		state.PendingReset = true
		state.ResetFrom = state.SessionID
		state.UpdatedAt = time.Now().Format(time.RFC3339)
		currentSessionID := state.SessionID
		s.mu.Unlock()
		return map[string]string{"message": buildUIResetPromptMessage(currentSessionID)}, nil
	default:
		return nil, fmt.Errorf("未知命令: %s", fields[0])
	}
}

func (s *Server) handleSetModel(req modelReq) (interface{}, error) {
	state := s.getState(req.UISessionID)
	model := strings.TrimSpace(req.Model)
	if model == "" {
		return nil, fmt.Errorf("model 不能为空")
	}

	s.mu.Lock()
	state.ProviderBaseURL = strings.TrimSpace(req.ProviderBaseURL)
	state.APIKey = strings.TrimSpace(req.APIKey)
	state.Model = model
	state.UpdatedAt = time.Now().Format(time.RFC3339)
	s.mu.Unlock()

	restartMessage := ""
	if restartResp, err := s.handleRestartOpenCodeServe(context.Background(), restartReq{Reason: "model-config-updated"}); err == nil {
		if m, ok := restartResp.(map[string]string); ok {
			restartMessage = m["message"]
		}
	} else {
		restartMessage = "重启 opencode serve 失败: " + err.Error()
	}
	refreshMessage := ""
	if providers, err := s.oc.GetProviders(context.Background()); err == nil {
		refreshMessage = fmt.Sprintf("模型列表已刷新，provider 数量: %d", len(providers))
	} else {
		refreshMessage = "模型列表刷新失败: " + err.Error()
	}

	msg := "OpenAI provider/model 配置已保存"
	if restartMessage != "" {
		msg = msg + "\n" + restartMessage
	}
	if refreshMessage != "" {
		msg = msg + "\n" + refreshMessage
	}

	return map[string]string{"message": msg}, nil
}

func (s *Server) handleSetSoul(req soulReq) (interface{}, error) {
	state := s.getState(req.UISessionID)
	text := strings.TrimSpace(req.Soul)
	if text == "" {
		return nil, fmt.Errorf("soul 不能为空")
	}
	if len(text) > 2000 {
		return nil, fmt.Errorf("soul 太长，最多 2000 字")
	}
	if err := s.saveSoulToFile("vibe", text); err != nil {
		return nil, fmt.Errorf("保存 soul.md 失败: %w", err)
	}
	latest := text
	if fromFile, err := s.loadSoulFromFile(); err == nil && strings.TrimSpace(fromFile) != "" {
		latest = fromFile
	}

	s.mu.Lock()
	state.Soul = latest
	state.UpdatedAt = time.Now().Format(time.RFC3339)
	s.mu.Unlock()

	return map[string]string{"message": "灵魂设定已保存（默认写入 SOUL 的 Vibe 段），并已同步到 soul.md"}, nil
}

func (s *Server) handleGetState(req stateReq) (interface{}, error) {
	state := s.getState(req.UISessionID)
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *state
	return copy, nil
}

func (s *Server) handleSkillInstall(ctx context.Context, req skillInstallReq) (interface{}, error) {
	skill := strings.TrimSpace(req.Skill)
	if skill == "" {
		return nil, fmt.Errorf("skill 不能为空")
	}

	totPath, err := findTotPy()
	if err != nil {
		return nil, err
	}

	out, runErr := runPythonTOT(ctx, totPath, "install", skill)
	if runErr != nil {
		return map[string]string{"message": out}, fmt.Errorf("安装失败: %v", runErr)
	}

	restartMessage := ""
	if restartResp, err := s.handleRestartOpenCodeServe(context.Background(), restartReq{Reason: "skill-installed"}); err == nil {
		if m, ok := restartResp.(map[string]string); ok {
			restartMessage = m["message"]
		}
	} else {
		restartMessage = "重启 opencode serve 失败: " + err.Error()
	}

	msg := out
	if restartMessage != "" {
		msg = trimOutput(msg + "\n" + restartMessage)
	}

	return map[string]string{"message": msg}, nil
}

func (s *Server) handleSkillUninstall(ctx context.Context, req skillInstallReq) (interface{}, error) {
	skill := strings.TrimSpace(req.Skill)
	if skill == "" {
		return nil, fmt.Errorf("skill 不能为空")
	}

	isCustom, checkErr := isCustomSkillInstalled(skill, s.oc.Directory())
	if checkErr != nil {
		return nil, checkErr
	}
	if !isCustom {
		return nil, fmt.Errorf("内置 skills 不支持 uninstall，仅支持卸载自定义 skills")
	}

	totPath, err := findTotPy()
	if err != nil {
		return nil, err
	}

	out, runErr := runPythonTOT(ctx, totPath, "uninstall", skill)
	if runErr != nil {
		return map[string]string{"message": out}, fmt.Errorf("卸载失败: %v", runErr)
	}

	restartMessage := ""
	if restartResp, err := s.handleRestartOpenCodeServe(context.Background(), restartReq{Reason: "skill-uninstalled"}); err == nil {
		if m, ok := restartResp.(map[string]string); ok {
			restartMessage = m["message"]
		}
	} else {
		restartMessage = "重启 opencode serve 失败: " + err.Error()
	}

	msg := out
	if restartMessage != "" {
		msg = trimOutput(msg + "\n" + restartMessage)
	}

	return map[string]string{"message": msg}, nil
}

func (s *Server) handleOpencodeSkillsList(ctx context.Context) (interface{}, error) {
	builtin, err := listBuiltinSkills(ctx, s.oc)
	if err != nil {
		return nil, err
	}
	custom, err := listCustomSkills(s.oc.Directory())
	if err != nil {
		return nil, err
	}
	items := append(builtin, custom...)
	return map[string]interface{}{"skills": items}, nil
}

func listBuiltinSkills(ctx context.Context, oc *opencode.Client) ([]skillInfo, error) {
	agents, err := oc.ListAgents(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]skillInfo, 0, len(agents))
	for _, a := range agents {
		name := strings.TrimSpace(a.Name)
		if name == "" {
			name = "unknown-agent"
		}
		items = append(items, skillInfo{
			Name:         name,
			Description:  strings.TrimSpace(a.Description),
			Path:         strings.TrimSpace(a.Prompt),
			Source:       "opencode-builtin",
			CanUninstall: false,
		})
	}
	return items, nil
}

func listCustomSkills(baseDir string) ([]skillInfo, error) {
	projectDir := filepath.Join(resolveOpenCodeDirectory(baseDir), ".opencode", "skills")
	globalDir := filepath.Join(getHomeDirSafe(), ".config", "opencode", "skills")

	itemsByName := map[string]skillInfo{}
	for _, p := range []struct {
		dir    string
		source string
	}{
		{projectDir, "custom-project"},
		{globalDir, "custom-global"},
	} {
		entries, err := os.ReadDir(p.dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			name := entry.Name()
			if _, exists := itemsByName[name]; exists {
				continue
			}
			desc := readSkillDescription(filepath.Join(p.dir, name, "SKILL.md"))
			itemsByName[name] = skillInfo{
				Name:         name,
				Description:  desc,
				Path:         filepath.Join(p.dir, name),
				Source:       p.source,
				CanUninstall: true,
			}
		}
	}

	items := make([]skillInfo, 0, len(itemsByName))
	for _, v := range itemsByName {
		items = append(items, v)
	}
	return items, nil
}

func isCustomSkillInstalled(skill, baseDir string) (bool, error) {
	paths := []string{
		filepath.Join(resolveOpenCodeDirectory(baseDir), ".opencode", "skills", skill),
		filepath.Join(getHomeDirSafe(), ".config", "opencode", "skills", skill),
	}
	for _, p := range paths {
		st, err := os.Stat(p)
		if err == nil && st.IsDir() {
			return true, nil
		}
		if err != nil && !os.IsNotExist(err) {
			return false, err
		}
	}
	return false, nil
}

func getHomeDirSafe() string {
	h, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return h
}

func readSkillDescription(skillFile string) string {
	b, err := os.ReadFile(skillFile)
	if err != nil {
		return ""
	}
	content := string(b)
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToLower(line), "description:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "description:"))
		}
	}
	return ""
}

func resolveOpenCodeDirectory(baseDir string) string {
	d := strings.TrimSpace(baseDir)
	if d == "" {
		d = "."
	}
	if abs, err := filepath.Abs(d); err == nil {
		return abs
	}
	return d
}

func (s *Server) handleOpencodeModelsList(ctx context.Context) (interface{}, error) {
	providers, err := s.oc.GetProviders(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"providers": providers}, nil
}

func (s *Server) handleSkillsQuery(ctx context.Context) (interface{}, error) {
	raw, err := s.handleOpencodeSkillsList(ctx)
	if err != nil {
		return nil, err
	}
	obj := raw.(map[string]interface{})
	items, _ := obj["skills"].([]skillInfo)
	if len(items) == 0 {
		return map[string]string{"message": "当前未发现 skills"}, nil
	}
	b := strings.Builder{}
	b.WriteString("skills 列表:\n")
	for i, it := range items {
		if i >= 30 {
			b.WriteString("...\n")
			break
		}
		b.WriteString("- ")
		b.WriteString(it.Name)
		if it.Source != "" {
			b.WriteString(" [")
			b.WriteString(it.Source)
			b.WriteString("]")
		}
		if it.Description != "" {
			b.WriteString(" : ")
			b.WriteString(it.Description)
		}
		b.WriteString("\n")
	}
	return map[string]string{"message": b.String()}, nil
}

func (s *Server) handleModelQuery(ctx context.Context, state *uiSessionState) (interface{}, error) {
	providers, err := s.oc.GetProviders(ctx)
	if err != nil {
		return nil, err
	}
	current := state.Model
	if strings.TrimSpace(current) == "" {
		current = "<未设置>"
	}
	b := strings.Builder{}
	b.WriteString("当前模型: ")
	b.WriteString(current)
	b.WriteString("\n可用模型(最多展示每个 provider 前10个):\n")
	for _, p := range providers {
		b.WriteString("[" + p.ID + "]\n")
		for i, m := range p.Models {
			if i >= 10 {
				b.WriteString("  ...\n")
				break
			}
			b.WriteString("  - ")
			b.WriteString(m.ID)
			b.WriteString("\n")
		}
	}
	return map[string]string{"message": b.String()}, nil
}

func (s *Server) handleRestartOpenCodeServe(ctx context.Context, req restartReq) (interface{}, error) {
	if s.restartServe == nil {
		return nil, fmt.Errorf("opencode serve manager 未启用")
	}
	out, err := s.restartServe(ctx, req.Reason)
	if err != nil {
		return nil, err
	}
	return map[string]string{"message": out}, nil
}

func findTotPy() (string, error) {
	cwd, _ := os.Getwd()
	candidates := []string{
		filepath.Join(cwd, "skill-install", "tot.py"),
		filepath.Join(cwd, "..", "..", "skill-install", "tot.py"),
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	return "", fmt.Errorf("未找到 skill-install/tot.py")
}

func runPythonTOT(ctx context.Context, totPath string, args ...string) (string, error) {
	fullArgs := append([]string{totPath}, args...)
	cmd := exec.CommandContext(ctx, "python", fullArgs...)
	out, err := cmd.CombinedOutput()
	if err == nil {
		return trimOutput(string(out)), nil
	}

	fallback := exec.CommandContext(ctx, "py", fullArgs...)
	out2, err2 := fallback.CombinedOutput()
	if err2 == nil {
		return trimOutput(string(out2)), nil
	}
	return trimOutput(string(out) + "\n" + string(out2)), err
}

func trimOutput(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return "(无输出)"
	}
	if len(v) > 3000 {
		return v[:3000] + "..."
	}
	return v
}

func (s *Server) getState(uiSessionID string) *uiSessionState {
	id := strings.TrimSpace(uiSessionID)
	if id == "" {
		id = "default"
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.sessions[id]; ok {
		return st
	}
	st := &uiSessionState{
		UISessionID: id,
		ThreadID:    "thread-" + id,
		UpdatedAt:   time.Now().Format(time.RFC3339),
	}
	if soulText, err := s.loadSoulFromFile(); err == nil {
		st.Soul = soulText
	}
	s.sessions[id] = st
	return st
}

func (s *Server) soulFilePaths() []string {
	base := bootstrap.ResolveBootstrapDir(strings.TrimSpace(s.oc.Directory()))
	return []string{
		filepath.Join(base, "soul.md"),
		filepath.Join(base, "SOUL.md"),
	}
}

func (s *Server) loadSoulFromFile() (string, error) {
	for _, p := range s.soulFilePaths() {
		b, err := os.ReadFile(p)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return "", err
		}
		text := strings.TrimSpace(string(b))
		if text != "" {
			return text, nil
		}
	}
	return "", nil
}

func (s *Server) saveSoulToFile(section, text string) error {
	base := bootstrap.ResolveBootstrapDir(strings.TrimSpace(s.oc.Directory()))
	botName := strings.TrimSpace(os.Getenv("OPENBOT_NAME"))
	if _, err := bootstrap.EnsurePersonaFiles(base, botName); err != nil {
		return err
	}

	for _, p := range s.soulFilePaths() {
		existing := ""
		if b, err := os.ReadFile(p); err == nil {
			existing = string(b)
		} else if !os.IsNotExist(err) {
			return err
		}

		merged := mergeSoulContent(existing, section, text)
		if err := os.WriteFile(p, []byte(merged), 0o644); err != nil {
			return err
		}
	}
	return nil
}

func mergeSoulContent(existing, section, update string) string {
	trimmedUpdate := strings.TrimSpace(update)
	if trimmedUpdate == "" {
		return strings.TrimSpace(existing) + "\n"
	}

	// If caller provides a full SOUL document, respect full replacement.
	if strings.Contains(trimmedUpdate, "# SOUL.md") || strings.Contains(trimmedUpdate, "## Core Truths") {
		return trimmedUpdate + "\n"
	}

	base := strings.TrimSpace(existing)
	if base == "" {
		base = "# SOUL.md - Who You Are"
	}

	heading := soulSectionHeading(section)
	lines := strings.Split(base, "\n")
	start := -1
	end := -1
	for i, line := range lines {
		if strings.EqualFold(strings.TrimSpace(line), heading) {
			start = i
			continue
		}
		if start >= 0 && strings.HasPrefix(strings.TrimSpace(line), "## ") {
			end = i
			break
		}
	}
	if start >= 0 && end < 0 {
		end = len(lines)
	}

	sectionLines := make([]string, 0)
	for _, line := range strings.Split(trimmedUpdate, "\n") {
		t := strings.TrimSpace(line)
		if t == "" {
			continue
		}
		if strings.HasPrefix(t, "-") {
			sectionLines = append(sectionLines, t)
		} else {
			sectionLines = append(sectionLines, "- "+t)
		}
	}
	if len(sectionLines) == 0 {
		sectionLines = append(sectionLines, "- "+trimmedUpdate)
	}

	replacement := append([]string{heading, ""}, sectionLines...)

	if start >= 0 {
		merged := append([]string{}, lines[:start]...)
		merged = append(merged, replacement...)
		merged = append(merged, lines[end:]...)
		return strings.TrimSpace(strings.Join(merged, "\n")) + "\n"
	}

	if len(lines) > 0 && strings.TrimSpace(lines[len(lines)-1]) != "" {
		lines = append(lines, "")
	}
	lines = append(lines, replacement...)
	return strings.TrimSpace(strings.Join(lines, "\n")) + "\n"
}

func parseSoulSetInput(fields []string, cmd string) (string, string) {
	section := "vibe"
	remainder := strings.TrimSpace(strings.TrimPrefix(cmd, "/soul set"))
	if len(fields) >= 4 {
		if normalized, ok := normalizeSoulSection(fields[2]); ok {
			section = normalized
			prefix := "/soul set " + fields[2]
			remainder = strings.TrimSpace(strings.TrimPrefix(cmd, prefix))
		}
	}
	return section, remainder
}

func normalizeSoulSection(raw string) (string, bool) {
	s := strings.ToLower(strings.TrimSpace(raw))
	switch s {
	case "vibe", "style", "语气", "风格":
		return "vibe", true
	case "boundaries", "boundary", "guardrails", "边界":
		return "boundaries", true
	case "core", "truths", "coretruths", "核心":
		return "core", true
	case "continuity", "memory", "持续性", "连续性":
		return "continuity", true
	default:
		return "", false
	}
}

func soulSectionHeading(section string) string {
	switch section {
	case "boundaries":
		return "## Boundaries"
	case "core":
		return "## Core Truths"
	case "continuity":
		return "## Continuity"
	default:
		return "## Vibe"
	}
}

func soulSectionLabel(section string) string {
	switch section {
	case "boundaries":
		return "Boundaries"
	case "core":
		return "Core Truths"
	case "continuity":
		return "Continuity"
	default:
		return "Vibe"
	}
}

func shortForID(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:12]
}
