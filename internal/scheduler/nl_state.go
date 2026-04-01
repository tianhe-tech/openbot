package scheduler

import (
	"fmt"
	"sync"
	"time"
)

// NLSchedulePendingState stores an unconfirmed scheduling draft.
type NLSchedulePendingState struct {
	UserID      string            `json:"user_id"`
	AdapterType string            `json:"adapter_type"`
	Channel     string            `json:"channel"`
	Draft       *NLScheduleIntent `json:"draft"`
	CreatedAt   time.Time         `json:"created_at"`
	ExpiresAt   time.Time         `json:"expires_at"`
	Revision    int64             `json:"revision"`
}

// NLScheduleStateStore stores per-user pending scheduling drafts.
type NLScheduleStateStore interface {
	Upsert(state *NLSchedulePendingState)
	Get(userID, adapterType string) (*NLSchedulePendingState, bool)
	Delete(userID, adapterType string)
	Cleanup(now time.Time)
}

// InMemoryNLScheduleStateStore is a TTL-based in-memory implementation.
type InMemoryNLScheduleStateStore struct {
	mu         sync.RWMutex
	states     map[string]*NLSchedulePendingState
	defaultTTL time.Duration
}

func NewInMemoryNLScheduleStateStore(defaultTTL time.Duration) *InMemoryNLScheduleStateStore {
	if defaultTTL <= 0 {
		defaultTTL = 10 * time.Minute
	}
	return &InMemoryNLScheduleStateStore{
		states:     make(map[string]*NLSchedulePendingState),
		defaultTTL: defaultTTL,
	}
}

func makeNLScheduleStateKey(userID, adapterType string) string {
	return fmt.Sprintf("%s::%s", adapterType, userID)
}

func (s *InMemoryNLScheduleStateStore) Upsert(state *NLSchedulePendingState) {
	if state == nil {
		return
	}
	if state.CreatedAt.IsZero() {
		state.CreatedAt = time.Now()
	}
	if state.ExpiresAt.IsZero() {
		state.ExpiresAt = state.CreatedAt.Add(s.defaultTTL)
	}
	state.Revision++

	key := makeNLScheduleStateKey(state.UserID, state.AdapterType)
	s.mu.Lock()
	s.states[key] = state
	s.mu.Unlock()
}

func (s *InMemoryNLScheduleStateStore) Get(userID, adapterType string) (*NLSchedulePendingState, bool) {
	key := makeNLScheduleStateKey(userID, adapterType)
	s.mu.RLock()
	state, ok := s.states[key]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	if !state.ExpiresAt.IsZero() && time.Now().After(state.ExpiresAt) {
		s.Delete(userID, adapterType)
		return nil, false
	}
	return state, true
}

func (s *InMemoryNLScheduleStateStore) Delete(userID, adapterType string) {
	key := makeNLScheduleStateKey(userID, adapterType)
	s.mu.Lock()
	delete(s.states, key)
	s.mu.Unlock()
}

func (s *InMemoryNLScheduleStateStore) Cleanup(now time.Time) {
	s.mu.Lock()
	for key, state := range s.states {
		if !state.ExpiresAt.IsZero() && now.After(state.ExpiresAt) {
			delete(s.states, key)
		}
	}
	s.mu.Unlock()
}
