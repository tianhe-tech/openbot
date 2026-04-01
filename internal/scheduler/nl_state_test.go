package scheduler

import (
	"testing"
	"time"
)

func TestInMemoryStateStoreUpsertGetDelete(t *testing.T) {
	store := NewInMemoryNLScheduleStateStore(time.Minute)
	state := &NLSchedulePendingState{
		UserID:      "u1",
		AdapterType: "dingtalk",
		Draft: &NLScheduleIntent{
			Action: NLScheduleActionCreate,
		},
	}

	store.Upsert(state)
	got, ok := store.Get("u1", "dingtalk")
	if !ok {
		t.Fatalf("Get() ok = false, want true")
	}
	if got.Draft == nil || got.Draft.Action != NLScheduleActionCreate {
		t.Fatalf("unexpected draft state")
	}

	store.Delete("u1", "dingtalk")
	if _, ok := store.Get("u1", "dingtalk"); ok {
		t.Fatalf("Get() ok = true after Delete(), want false")
	}
}

func TestInMemoryStateStoreCleanupExpired(t *testing.T) {
	store := NewInMemoryNLScheduleStateStore(time.Minute)
	now := time.Now()
	store.Upsert(&NLSchedulePendingState{
		UserID:      "u2",
		AdapterType: "feishu",
		Draft:       &NLScheduleIntent{Action: NLScheduleActionCreate},
		CreatedAt:   now.Add(-2 * time.Minute),
		ExpiresAt:   now.Add(-time.Minute),
	})

	store.Cleanup(now)
	if _, ok := store.Get("u2", "feishu"); ok {
		t.Fatalf("expired state should be removed")
	}
}
