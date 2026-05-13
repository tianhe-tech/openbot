// Package memstore — gateway adapter wires Store methods to the opencode.MemStoreRecorder
// interface without creating an import cycle (opencode ← memstore, never the reverse).
package memstore

import (
	"log"
	"path/filepath"
	"strings"
	"time"
)

// GatewayAdapter wraps *Store and implements opencode.MemStoreRecorder.
// It is the single integration point created in main.go and passed to WithMemStore().
type GatewayAdapter struct {
	store *Store
}

// NewGatewayAdapter wraps an open Store.
func NewGatewayAdapter(store *Store) *GatewayAdapter {
	return &GatewayAdapter{store: store}
}

// RecordConversation extracts metadata from the request/response text and persists
// a MemRecord.  Runs synchronously inside the goroutine the caller already launched.
func (g *GatewayAdapter) RecordConversation(adapter, userID, request, response, workDir string) {
	request = strings.TrimSpace(request)
	response = strings.TrimSpace(response)
	if request == "" || response == "" {
		return
	}
	// Skip storing pure recall/history queries — they are meta-questions, not real work.
	// Storing them creates noise: "我最近开发了什么" → project="项目" → pollutes memory.
	if DetectRecallIntent(request) {
		return
	}

	action := ExtractAction(request)

	// Resolve the effective working directory:
	// 1. Path explicitly mentioned in the request text takes highest priority
	//    (e.g. "在 /root/crawler 目录下开发爬虫" → "/root/crawler").
	// 2. Fall back to the session-level workDir passed in from the client.
	effectiveDir := workDir
	if textDir := ExtractDirFromText(request); textDir != "" {
		effectiveDir = textDir
	}

	// Use the last path segment of effectiveDir as project label when available;
	// fall back to text extraction so old sessions without a workDir still work.
	project := ExtractProject(request)
	if wd := strings.TrimSpace(effectiveDir); wd != "" && wd != "." {
		if seg := workDirBasename(wd); seg != "" {
			project = seg
		}
	}
	keywords := ExtractKeywords(request)
	tags := strings.Join(keywords, ",")
	summary := BuildSummary(request, response, action, project)

	rec := MemRecord{
		Adapter:  adapter,
		UserID:   userID,
		Request:  request,
		Response: response,
		Summary:  summary,
		Project:  project,
		WorkDir:  strings.TrimSpace(effectiveDir),
		Action:   action,
		Tags:     tags,
		Strength: 1.0,
	}

	if err := g.store.Record(rec); err != nil {
		log.Printf("memstore: record error (adapter=%s user=%s): %v", adapter, userID, err)
	}
}

// InjectRecallContext checks if request looks like a recall query; if so, searches the store
// and returns a formatted preamble to prepend to the user message.  Returns "" otherwise.
func (g *GatewayAdapter) InjectRecallContext(request, adapter, userID string) string {
	if !DetectRecallIntent(request) {
		return ""
	}

	window := DetectTimeWindow(request)
	since := time.Now().AddDate(0, 0, -window)
	keywords := ExtractKeywords(request)

	// Cross-platform recall: this is a single-user personal gateway, so all records
	// belong to the same person regardless of adapter/platform. No userID isolation needed.
	records, err := g.store.Recall(keywords, "", "", since, 8)
	if err != nil {
		log.Printf("memstore: recall error: %v", err)
		return ""
	}

	// Project summaries: all adapters, same time window, minCount=1
	projectSums, err := g.store.ProjectSummaries("", "", since, 1)
	if err != nil {
		log.Printf("memstore: project summaries error: %v", err)
	}

	ctx := BuildRecallContext(records, projectSums)
	if ctx == "" {
		return ""
	}
	return ctx + "\n\n---\n\n【用户消息】"
}

// ---- Session handoff (opencode scheduler-deadlock recovery) ----

// SaveSessionHandoff persists a compressed snapshot of a stuck session so the
// next turn on the same thread can auto-resume in a fresh session.
func (g *GatewayAdapter) SaveSessionHandoff(threadID, adapter, userID, oldSessionID, summary, lastUserMsg string) error {
	if g == nil || g.store == nil {
		return nil
	}
	return g.store.SaveHandoff(HandoffRecord{
		ThreadID:     threadID,
		Adapter:      adapter,
		UserID:       userID,
		OldSessionID: oldSessionID,
		Summary:      summary,
		LastUserMsg:  lastUserMsg,
		CreatedAt:    time.Now(),
	})
}

// LoadPendingHandoff returns (summary, lastUserMsg, ok). When ok is false,
// no pending handoff exists (or it expired).
func (g *GatewayAdapter) LoadPendingHandoff(threadID string) (string, string, bool) {
	if g == nil || g.store == nil {
		return "", "", false
	}
	rec, ok, err := g.store.LoadPendingHandoff(threadID)
	if err != nil {
		log.Printf("memstore: load handoff error (thread=%s): %v", threadID, err)
		return "", "", false
	}
	if !ok {
		return "", "", false
	}
	return rec.Summary, rec.LastUserMsg, true
}

// MarkHandoffConsumed flags the handoff for thread as consumed.
func (g *GatewayAdapter) MarkHandoffConsumed(threadID string) {
	if g == nil || g.store == nil {
		return
	}
	if err := g.store.MarkHandoffConsumed(threadID); err != nil {
		log.Printf("memstore: mark handoff consumed error (thread=%s): %v", threadID, err)
	}
}

// BuildHandoffSummary compresses parallel role/content slices using the
// rule-based strategy defined in handoff.go.
func (g *GatewayAdapter) BuildHandoffSummary(roles []string, contents []string) string {
	n := len(roles)
	if len(contents) < n {
		n = len(contents)
	}
	turns := make([]HandoffTurn, 0, n)
	for i := 0; i < n; i++ {
		turns = append(turns, HandoffTurn{Role: roles[i], Content: contents[i]})
	}
	return BuildHandoffSummary(turns, 2000)
}

// BuildHandoffPreamble renders the preamble that wraps the current user
// message with the recovered summary and auto-resent prior prompt.
func (g *GatewayAdapter) BuildHandoffPreamble(summary, prevUnanswered, currentUserMsg string) string {
	return BuildHandoffPreamble(summary, prevUnanswered, currentUserMsg)
}

// SanitizeUserContent strips nested handoff preambles, <think> blocks, and
// memstore recall wrappers from raw user content.
func (g *GatewayAdapter) SanitizeUserContent(content string) string {
	return SanitizeHandoffContent("user", content)
}

// workDirBasename returns the last meaningful segment of a working directory path.
// Returns "" for empty, "." or root-like paths.
func workDirBasename(dir string) string {
	dir = strings.TrimSpace(dir)
	if dir == "" || dir == "." {
		return ""
	}
	base := filepath.Base(dir)
	if base == "." || base == "/" || base == "\\" {
		return ""
	}
	return base
}
