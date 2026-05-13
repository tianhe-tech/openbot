// cmd/mcp is a Model Context Protocol (MCP) stdio server that exposes the
// gateway's memory store as tools callable by OpenCode's LLM.
//
// Usage (configured in opencode.json):
//
//	{ "mcp": { "gateway-memory": { "type": "local", "command": ["opencode-gateway-mcp", "--db", "/path/to/memory.db"] } } }
//
// The server communicates via JSON-RPC 2.0 over stdin/stdout (MCP stdio transport).
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/user/opencode-gateway/internal/memstore"
)

func main() {
	dbPath := flag.String("db", "", "path to memory.db (required)")
	flag.Parse()

	if *dbPath == "" {
		// Try environment variable as fallback
		if envPath := os.Getenv("GATEWAY_MEMORY_DB"); envPath != "" {
			*dbPath = envPath
		} else {
			fmt.Fprintf(os.Stderr, "Usage: opencode-gateway-mcp --db /path/to/memory.db\n")
			os.Exit(1)
		}
	}

	store, err := memstore.Open(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open memory store: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	// Disable log output to stdout (MCP uses stdout for JSON-RPC)
	log.SetOutput(os.Stderr)

	srv := &mcpServer{store: store}
	srv.run()
}

// ---- MCP JSON-RPC types ----

type jsonrpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonrpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ---- MCP protocol types ----

type mcpToolSchema struct {
	Type       string                    `json:"type"`
	Properties map[string]mcpPropertyDef `json:"properties"`
	Required   []string                  `json:"required,omitempty"`
}

type mcpPropertyDef struct {
	Type        string `json:"type"`
	Description string `json:"description"`
}

type mcpToolDef struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	InputSchema mcpToolSchema `json:"inputSchema"`
}

type mcpToolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
}

type mcpContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// ---- Server ----

type mcpServer struct {
	store *memstore.Store
}

func (s *mcpServer) run() {
	scanner := bufio.NewScanner(os.Stdin)
	// MCP messages can be large; increase buffer
	scanner.Buffer(make([]byte, 0, 1024*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var req jsonrpcRequest
		if err := json.Unmarshal(line, &req); err != nil {
			s.writeError(nil, -32700, "Parse error")
			continue
		}

		s.handleRequest(req)
	}
}

func (s *mcpServer) handleRequest(req jsonrpcRequest) {
	switch req.Method {
	case "initialize":
		s.writeResult(req.ID, map[string]interface{}{
			"protocolVersion": "2024-11-05",
			"capabilities": map[string]interface{}{
				"tools": map[string]interface{}{},
			},
			"serverInfo": map[string]interface{}{
				"name":    "gateway-memory",
				"version": "1.0.0",
			},
		})

	case "notifications/initialized":
		// No response needed for notifications

	case "tools/list":
		s.writeResult(req.ID, map[string]interface{}{
			"tools": s.toolDefinitions(),
		})

	case "tools/call":
		var params mcpToolCallParams
		if err := json.Unmarshal(req.Params, &params); err != nil {
			s.writeError(req.ID, -32602, "Invalid params")
			return
		}
		s.handleToolCall(req.ID, params)

	case "ping":
		s.writeResult(req.ID, map[string]interface{}{})

	default:
		s.writeError(req.ID, -32601, fmt.Sprintf("Method not found: %s", req.Method))
	}
}

func (s *mcpServer) toolDefinitions() []mcpToolDef {
	return []mcpToolDef{
		{
			Name:        "memory_search",
			Description: "搜索用户的历史工作记忆。当用户询问过去做过什么项目、之前的工作内容、历史记录时调用此工具。支持关键词搜索，返回匹配的工作记录和项目概览。使用场景：用户问'我最近做了什么'、'之前那个爬虫项目'、'上次部署的项目'等。",
			InputSchema: mcpToolSchema{
				Type: "object",
				Properties: map[string]mcpPropertyDef{
					"query": {
						Type:        "string",
						Description: "搜索关键词，如 '爬虫'、'部署'、'Python项目'。支持中英文。",
					},
					"days": {
						Type:        "number",
						Description: "回溯天数，默认30天。用户说'最近'用30，'这周'用7，'今天'用1，'这个月'用30，'近半年'用180。",
					},
				},
				Required: []string{"query"},
			},
		},
		{
			Name:        "memory_list_projects",
			Description: "列出用户的所有项目及其活动概览。当用户问'我有哪些项目'、'最近在做什么'、'开发了哪些东西'时调用。不需要关键词，直接返回项目列表。",
			InputSchema: mcpToolSchema{
				Type: "object",
				Properties: map[string]mcpPropertyDef{
					"days": {
						Type:        "number",
						Description: "回溯天数，默认90天。",
					},
				},
			},
		},
		{
			Name:        "memory_recent",
			Description: "获取最近的工作记录（按时间倒序）。当用户问'我刚才在做什么'、'今天做了什么'、'最近的工作'时调用。",
			InputSchema: mcpToolSchema{
				Type: "object",
				Properties: map[string]mcpPropertyDef{
					"days": {
						Type:        "number",
						Description: "回溯天数，默认7天。",
					},
					"limit": {
						Type:        "number",
						Description: "返回条数，默认10条。",
					},
				},
			},
		},
	}
}

func (s *mcpServer) handleToolCall(id json.RawMessage, params mcpToolCallParams) {
	var args map[string]interface{}
	if len(params.Arguments) > 0 {
		if err := json.Unmarshal(params.Arguments, &args); err != nil {
			s.writeError(id, -32602, "Invalid tool arguments")
			return
		}
	}
	if args == nil {
		args = make(map[string]interface{})
	}

	var result string
	var toolErr error

	switch params.Name {
	case "memory_search":
		result, toolErr = s.toolMemorySearch(args)
	case "memory_list_projects":
		result, toolErr = s.toolListProjects(args)
	case "memory_recent":
		result, toolErr = s.toolRecent(args)
	default:
		s.writeError(id, -32602, fmt.Sprintf("Unknown tool: %s", params.Name))
		return
	}

	if toolErr != nil {
		s.writeResult(id, map[string]interface{}{
			"content": []mcpContent{{Type: "text", Text: fmt.Sprintf("Error: %v", toolErr)}},
			"isError": true,
		})
		return
	}

	s.writeResult(id, map[string]interface{}{
		"content": []mcpContent{{Type: "text", Text: result}},
	})
}

// ---- Tool implementations ----

func (s *mcpServer) toolMemorySearch(args map[string]interface{}) (string, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return "", fmt.Errorf("query is required")
	}
	days := intArg(args, "days", 30)
	since := time.Now().AddDate(0, 0, -days)

	keywords := memstore.ExtractKeywords(query)
	// Also add the raw query words for coverage
	for _, w := range strings.Fields(query) {
		if len([]rune(w)) >= 2 {
			keywords = append(keywords, w)
		}
	}

	records, err := s.store.Recall(keywords, "", "", since, 10)
	if err != nil {
		return "", err
	}

	projects, _ := s.store.ProjectSummaries("", "", since, 1)

	return formatSearchResult(records, projects), nil
}

func (s *mcpServer) toolListProjects(args map[string]interface{}) (string, error) {
	days := intArg(args, "days", 90)
	since := time.Now().AddDate(0, 0, -days)

	projects, err := s.store.ProjectSummaries("", "", since, 1)
	if err != nil {
		return "", err
	}

	if len(projects) == 0 {
		return fmt.Sprintf("最近 %d 天没有找到项目记录。", days), nil
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("## 项目列表（最近 %d 天）\n\n", days))
	for _, p := range projects {
		daysSince := int(time.Since(p.Last).Hours() / 24)
		actions := strings.Join(uniqueStrings(p.Actions), "、")
		sb.WriteString(fmt.Sprintf("- **%s**：共 %d 次操作（%s），最近一次在 %d 天前（%s）\n",
			p.Project, p.Count, actions, daysSince, p.Last.Format("2006-01-02")))
	}
	return sb.String(), nil
}

func (s *mcpServer) toolRecent(args map[string]interface{}) (string, error) {
	days := intArg(args, "days", 7)
	limit := intArg(args, "limit", 10)

	records, err := s.store.Recent("", "", days, limit)
	if err != nil {
		return "", err
	}

	if len(records) == 0 {
		return fmt.Sprintf("最近 %d 天没有工作记录。", days), nil
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("## 最近工作记录（%d 天内，最多 %d 条）\n\n", days, limit))
	for _, r := range records {
		dirPart := ""
		if r.WorkDir != "" && r.WorkDir != "." {
			dirPart = fmt.Sprintf(" (%s)", r.WorkDir)
		}
		sb.WriteString(fmt.Sprintf("- [%s] **%s** %s%s\n",
			r.Ts.Format("2006-01-02 15:04"), r.Action, r.Summary, dirPart))
	}
	return sb.String(), nil
}

// ---- Formatting helpers ----

func formatSearchResult(records []memstore.MemRecord, projects []memstore.ProjectSummary) string {
	if len(records) == 0 && len(projects) == 0 {
		return "没有找到匹配的记忆记录。"
	}

	var sb strings.Builder

	if len(projects) > 0 {
		sb.WriteString("## 相关项目\n\n")
		shown := projects
		if len(shown) > 5 {
			shown = shown[:5]
		}
		for _, p := range shown {
			daysSince := int(time.Since(p.Last).Hours() / 24)
			actions := strings.Join(uniqueStrings(p.Actions), "、")
			sb.WriteString(fmt.Sprintf("- **%s**：%d 次操作（%s），最近 %d 天前（%s）\n",
				p.Project, p.Count, actions, daysSince, p.Last.Format("2006-01-02")))
		}
		sb.WriteString("\n")
	}

	if len(records) > 0 {
		sb.WriteString("## 匹配记录\n\n")
		// Dedup by (project, date)
		seen := make(map[string]struct{})
		count := 0
		for _, r := range records {
			key := r.Project + "|" + r.Ts.Format("2006-01-02")
			if r.Project == "" {
				key = r.Summary + "|" + r.Ts.Format("2006-01-02")
			}
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			dirPart := ""
			if r.WorkDir != "" && r.WorkDir != "." {
				dirPart = fmt.Sprintf(" (%s)", r.WorkDir)
			}
			sb.WriteString(fmt.Sprintf("- [%s] **%s** %s%s\n",
				r.Ts.Format("2006-01-02"), r.Action, r.Summary, dirPart))
			count++
			if count >= 8 {
				break
			}
		}
	}

	return sb.String()
}

// ---- Utility ----

func intArg(args map[string]interface{}, key string, defaultVal int) int {
	v, ok := args[key]
	if !ok {
		return defaultVal
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	default:
		return defaultVal
	}
}

func uniqueStrings(ss []string) []string {
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

// ---- JSON-RPC IO ----

func (s *mcpServer) writeResult(id json.RawMessage, result interface{}) {
	resp := jsonrpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Result:  result,
	}
	data, _ := json.Marshal(resp)
	fmt.Fprintf(os.Stdout, "%s\n", data)
}

func (s *mcpServer) writeError(id json.RawMessage, code int, message string) {
	resp := jsonrpcResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &rpcError{Code: code, Message: message},
	}
	data, _ := json.Marshal(resp)
	fmt.Fprintf(os.Stdout, "%s\n", data)
}
