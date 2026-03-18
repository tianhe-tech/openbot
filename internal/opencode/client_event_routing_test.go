package opencode

import (
	"sync"
	"testing"
)

func TestParseMessagePartUpdatedMeta(t *testing.T) {
	raw := `{"properties":{"message":{"id":"msg_123","role":"assistant","sessionID":"ses_abc"},"part":{"id":"part_1"}}}`

	meta, ok := parseMessagePartUpdatedMeta(raw)
	if !ok {
		t.Fatalf("expected parse success")
	}
	if meta.SessionID != "ses_abc" {
		t.Fatalf("unexpected session id: %q", meta.SessionID)
	}
	if meta.MessageID != "msg_123" {
		t.Fatalf("unexpected message id: %q", meta.MessageID)
	}
	if meta.PartID != "part_1" {
		t.Fatalf("unexpected part id: %q", meta.PartID)
	}
	if meta.Role != "assistant" {
		t.Fatalf("unexpected role: %q", meta.Role)
	}
}

func TestResolveSessionIDForPartDelta(t *testing.T) {
	var messageToSession sync.Map
	var partToSession sync.Map

	messageToSession.Store("msg_known", "ses_from_message")
	partToSession.Store("part_known", "ses_from_part")

	if got := resolveSessionIDForPartDelta("msg_known", "part_known", &messageToSession, &partToSession); got != "ses_from_message" {
		t.Fatalf("expected message mapping priority, got %q", got)
	}
	if got := resolveSessionIDForPartDelta("msg_missing", "part_known", &messageToSession, &partToSession); got != "ses_from_part" {
		t.Fatalf("expected part fallback mapping, got %q", got)
	}
	if got := resolveSessionIDForPartDelta("msg_missing", "part_missing", &messageToSession, &partToSession); got != "" {
		t.Fatalf("expected empty for unknown mappings, got %q", got)
	}
}

func TestRecallCategoryQuotaScalesWhenExceedsInjectLimit(t *testing.T) {
	c := &Client{
		memoryCategoryQuota: map[string]int{
			"preference": 4,
			"project":    3,
			"profile":    2,
		},
	}

	quota := c.recallCategoryQuota(4)
	sum := 0
	for _, v := range quota {
		sum += v
		if v < 1 {
			t.Fatalf("quota value must be >=1, got %d", v)
		}
	}
	if sum != 4 {
		t.Fatalf("scaled quota sum must equal inject limit, got sum=%d limit=4", sum)
	}
}

func TestRecallCategoryQuotaNoScalingWhenWithinLimit(t *testing.T) {
	c := &Client{
		memoryCategoryQuota: map[string]int{
			"preference": 2,
			"project":    1,
		},
	}

	quota := c.recallCategoryQuota(8)
	if quota["preference"] != 2 || quota["project"] != 1 {
		t.Fatalf("expected unchanged quota, got %v", quota)
	}
}
