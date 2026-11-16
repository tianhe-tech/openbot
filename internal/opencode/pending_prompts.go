package opencode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/sst/opencode-sdk-go/option"
)

// RefreshPendingPrompts reconciles only the requested sessions. A concurrent
// SSE update invalidates the snapshot rather than resurrecting an answered
// request or deleting a newer one.
func (c *Client) RefreshPendingPrompts(ctx context.Context, sessionIDs []string) ([]*Question, error) {
	c.pendingQuestionsMu.Lock()
	revision := c.pendingQuestionsRevision
	c.pendingQuestionsMu.Unlock()
	prompts, err := c.listPendingPrompts(ctx)
	if err != nil {
		return nil, err
	}
	c.pendingQuestionsMu.Lock()
	defer c.pendingQuestionsMu.Unlock()
	if revision != c.pendingQuestionsRevision {
		return nil, fmt.Errorf("opencode: pending requests changed during refresh; retry required")
	}
	sessions := make(map[string]bool, len(sessionIDs))
	for _, id := range sessionIDs {
		sessions[id] = true
	}
	c.pendingQuestions.Range(func(key, value interface{}) bool {
		if sessions[value.(*Question).SessionID] {
			c.pendingQuestions.Delete(key)
		}
		return true
	})
	var selected []*Question
	for _, prompt := range prompts {
		if sessions[prompt.SessionID] {
			c.pendingQuestions.Store(prompt.ID, prompt)
			selected = append(selected, prompt)
		}
	}
	c.pendingQuestionsRevision++
	return selected, nil
}

func (c *Client) listPendingPrompts(ctx context.Context) ([]*Question, error) {
	if c.sdk == nil {
		return nil, fmt.Errorf("opencode: pending prompt lookup requires an initialized client")
	}
	var result []*Question
	var opts []option.RequestOption
	if dir := strings.TrimSpace(c.directory); dir != "" && dir != "." {
		opts = append(opts, option.WithQuery("directory", dir))
	}
	extractor := &StreamingSessionHandler{}
	for _, endpoint := range []string{"permission", "question"} {
		var raw json.RawMessage
		if err := c.sdk.Execute(ctx, http.MethodGet, endpoint, nil, &raw, opts...); err != nil {
			return nil, fmt.Errorf("opencode: list pending %s requests: %w", endpoint, err)
		}
		var requests []json.RawMessage
		raw = bytes.TrimSpace(raw)
		if len(raw) == 0 || raw[0] != '[' {
			return nil, fmt.Errorf("opencode: invalid pending %s response: expected an array", endpoint)
		}
		if err := json.Unmarshal(raw, &requests); err != nil {
			return nil, fmt.Errorf("opencode: decode pending %s requests: %w", endpoint, err)
		}
		for _, request := range requests {
			var identity struct {
				ID        string `json:"id"`
				SessionID string `json:"sessionID"`
			}
			if err := json.Unmarshal(request, &identity); err != nil {
				return nil, fmt.Errorf("opencode: decode pending %s identity: %w", endpoint, err)
			}
			if identity.ID == "" || identity.SessionID == "" {
				return nil, fmt.Errorf("opencode: pending %s request lacks id or sessionID", endpoint)
			}
			wrapper, err := json.Marshal(struct {
				Properties json.RawMessage `json:"properties"`
			}{Properties: request})
			if err != nil {
				return nil, fmt.Errorf("opencode: wrap pending %s request: %w", endpoint, err)
			}
			var prompt *Question
			if endpoint == "permission" {
				prompt, _ = extractor.extractPermissionQuestion(string(wrapper))
			} else {
				prompt, _ = extractor.extractNormalQuestion(string(wrapper))
			}
			if prompt == nil {
				return nil, fmt.Errorf("opencode: invalid pending %s request %s", endpoint, identity.ID)
			}
			result = append(result, prompt)
		}
	}
	return result, nil
}
