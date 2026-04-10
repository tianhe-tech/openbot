// Package memstore — gateway adapter wires Store methods to the opencode.MemStoreRecorder
// interface without creating an import cycle (opencode ← memstore, never the reverse).
package memstore

import (
	"log"
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
func (g *GatewayAdapter) RecordConversation(adapter, userID, request, response string) {
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
	project := ExtractProject(request)
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
