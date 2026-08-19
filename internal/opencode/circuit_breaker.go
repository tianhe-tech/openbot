package opencode

import (
	"log"
	"strings"
	"sync"
	"time"
)

// CircuitState describes the operational state of a provider/model pair in
// the circuit breaker.
type CircuitState int

const (
	// StateClosed means the model is healthy and requests are allowed.
	StateClosed CircuitState = iota
	// StateOpen means the model has failed enough consecutive times that
	// requests are short-circuited until the cooldown elapses.
	StateOpen
	// StateHalfOpen means the cooldown has elapsed and a single probe
	// request is allowed to test whether the model has recovered.
	StateHalfOpen
)

func (s CircuitState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// CircuitBreakerConfig configures the per-model circuit breaker.
type CircuitBreakerConfig struct {
	// Enabled gates the entire breaker. When false, Allow always returns
	// true and RecordFailure/RecordSuccess are no-ops.
	Enabled bool
	// FailureThreshold is the number of consecutive provider-level
	// failures required to transition from closed to open.
	FailureThreshold int
	// Cooldown is the initial open duration before a half-open probe is
	// permitted.
	Cooldown time.Duration
	// MaxCooldown caps the exponential backoff applied on repeated
	// half-open probe failures.
	MaxCooldown time.Duration
	// TripKeywords lists error-message substrings (case-insensitive) that
	// are considered provider-level failures worth tripping the breaker.
	// Empty list means all session.error events trip the breaker.
	TripKeywords []string
}

// DefaultCircuitBreakerConfig returns a sane default configuration. The
// breaker is enabled by default because provider outages (like the
// "No available client" failure that motivated this feature) otherwise
// cause repeated silent failures until a human intervenes.
func DefaultCircuitBreakerConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		Enabled:          true,
		FailureThreshold: 2,
		Cooldown:         60 * time.Second,
		MaxCooldown:      30 * time.Minute,
		TripKeywords: []string{
			"No available client",
			"No available channel",
			"Insufficient Balance",
			"provider unavailable",
		},
	}
}

// breakerEntry is the per-model state. The breaker key is
// normalizeModelKey(providerID, modelID) so that providers sharing a model
// name (e.g. Tianhe-AI/GLM-5.2 vs starfire-jd/GLM-5.2) are tracked
// independently.
type breakerEntry struct {
	state            CircuitState
	consecutiveFails int
	openedAt         time.Time
	cooldown         time.Duration // current open duration (doubles on repeated half-open failures)
	lastFailMsg      string
	lastFailAt       time.Time
	// halfOpenInFlight tracks whether a half-open probe is currently
	// running. Only one probe is permitted at a time; additional requests
	// are rejected (treated as open) until the probe resolves.
	halfOpenInFlight bool
}

// BreakerStatus is a read-only snapshot of one model's breaker state, used
// for /status reporting and logging.
type BreakerStatus struct {
	ProviderID       string
	ModelID          string
	State            CircuitState
	ConsecutiveFails int
	OpenedAt         time.Time
	LastFailAt       time.Time
	LastFailMsg      string
	Cooldown         time.Duration
}

// CircuitBreaker tracks per provider/model failure rates and decides
// whether a request should be allowed through. It is safe for concurrent
// use.
type CircuitBreaker struct {
	mu      sync.RWMutex
	entries map[string]*breakerEntry
	cfg     CircuitBreakerConfig
}

// NewCircuitBreaker constructs a breaker with the given configuration.
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	return &CircuitBreaker{
		entries: make(map[string]*breakerEntry),
		cfg:     cfg,
	}
}

// Config returns the breaker's configuration.
func (b *CircuitBreaker) Config() CircuitBreakerConfig {
	return b.cfg
}

// Allow reports whether a request to the given provider/model should be
// permitted. When the breaker is disabled it always returns true.
//
// Side effects: if the entry is open but the cooldown has elapsed, the
// entry transitions to half-open and a single in-flight probe is
// permitted (subsequent callers see false until the probe resolves via
// RecordSuccess/RecordFailure).
func (b *CircuitBreaker) Allow(providerID, modelID string) bool {
	if b == nil || !b.cfg.Enabled {
		return true
	}
	key := normalizeModelKey(providerID, modelID)

	b.mu.Lock()
	defer b.mu.Unlock()

	e, ok := b.entries[key]
	if !ok {
		return true // never failed → healthy
	}

	switch e.state {
	case StateClosed:
		return true
	case StateOpen:
		if time.Since(e.openedAt) >= e.cooldown {
			// Cooldown elapsed: permit exactly one probe.
			e.state = StateHalfOpen
			e.halfOpenInFlight = true
			log.Printf("circuit_breaker: %s transitioning open→half-open (probe allowed)", key)
			return true
		}
		return false
	case StateHalfOpen:
		// Only one probe at a time; others are rejected.
		return !e.halfOpenInFlight
	default:
		return true
	}
}

// IsOpen reports whether the model is currently in the open or half-open
// state (i.e. not fully healthy). Unlike Allow it does not mutate state.
// Used by selectModelOverride to skip unhealthy models without consuming a
// half-open probe slot.
func (b *CircuitBreaker) IsOpen(providerID, modelID string) bool {
	if b == nil || !b.cfg.Enabled {
		return false
	}
	key := normalizeModelKey(providerID, modelID)

	b.mu.RLock()
	defer b.mu.RUnlock()

	e, ok := b.entries[key]
	if !ok {
		return false
	}
	// Consider cooldown-elapsed open entries as "not open" so they become
	// eligible for a probe via Allow().
	if e.state == StateOpen && time.Since(e.openedAt) >= e.cooldown {
		return false
	}
	return e.state == StateOpen || e.state == StateHalfOpen
}

// RecordFailure registers a provider-level failure for the given model.
// After FailureThreshold consecutive failures the breaker opens. Repeated
// half-open probe failures double the cooldown (capped at MaxCooldown).
func (b *CircuitBreaker) RecordFailure(providerID, modelID, errMsg string) {
	if b == nil || !b.cfg.Enabled {
		return
	}
	key := normalizeModelKey(providerID, modelID)

	b.mu.Lock()
	defer b.mu.Unlock()

	e, ok := b.entries[key]
	if !ok {
		e = &breakerEntry{state: StateClosed, cooldown: b.cfg.Cooldown}
		b.entries[key] = e
	}

	e.consecutiveFails++
	e.lastFailMsg = truncateMsg(errMsg, 300)
	e.lastFailAt = time.Now()

	wasHalfOpen := e.state == StateHalfOpen
	// A failure always clears any in-flight probe flag.
	e.halfOpenInFlight = false

	switch {
	case wasHalfOpen:
		// Probe failed: re-open with doubled cooldown (exponential backoff).
		e.state = StateOpen
		e.openedAt = time.Now()
		e.cooldown *= 2
		if e.cooldown > b.cfg.MaxCooldown {
			e.cooldown = b.cfg.MaxCooldown
		}
		log.Printf("circuit_breaker: %s half-open probe failed, re-opened (cooldown=%v, fails=%d)",
			key, e.cooldown, e.consecutiveFails)
	case e.state == StateClosed:
		if e.consecutiveFails >= b.cfg.FailureThreshold {
			e.state = StateOpen
			e.openedAt = time.Now()
			log.Printf("circuit_breaker: %s opened after %d consecutive failures (cooldown=%v, lastErr=%q)",
				key, e.consecutiveFails, e.cooldown, e.lastFailMsg)
		} else {
			log.Printf("circuit_breaker: %s failure %d/%d (lastErr=%q)",
				key, e.consecutiveFails, b.cfg.FailureThreshold, e.lastFailMsg)
		}
	}
}

// RecordSuccess registers a success for the given model, resetting the
// consecutive-failure counter and closing the breaker. A half-open probe
// success is the primary recovery path.
func (b *CircuitBreaker) RecordSuccess(providerID, modelID string) {
	if b == nil || !b.cfg.Enabled {
		return
	}
	key := normalizeModelKey(providerID, modelID)

	b.mu.Lock()
	defer b.mu.Unlock()

	e, ok := b.entries[key]
	if !ok {
		return
	}

	wasOpen := e.state != StateClosed
	e.consecutiveFails = 0
	e.state = StateClosed
	e.halfOpenInFlight = false
	e.cooldown = b.cfg.Cooldown
	if wasOpen {
		log.Printf("circuit_breaker: %s recovered (closed) after successful request", key)
	}
}

// StateString returns the current state name for logging/status.
func (b *CircuitBreaker) StateString(providerID, modelID string) string {
	if b == nil || !b.cfg.Enabled {
		return "disabled"
	}
	key := normalizeModelKey(providerID, modelID)

	b.mu.RLock()
	defer b.mu.RUnlock()

	e, ok := b.entries[key]
	if !ok {
		return StateClosed.String()
	}
	return e.state.String()
}

// Snapshot returns a read-only copy of all tracked breaker entries. Used
// by /status endpoints to surface breaker state to operators.
func (b *CircuitBreaker) Snapshot() []BreakerStatus {
	if b == nil || !b.cfg.Enabled {
		return nil
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]BreakerStatus, 0, len(b.entries))
	for key, e := range b.entries {
		providerID, modelID := splitModelKey(key)
		out = append(out, BreakerStatus{
			ProviderID:       providerID,
			ModelID:          modelID,
			State:            e.state,
			ConsecutiveFails: e.consecutiveFails,
			OpenedAt:         e.openedAt,
			LastFailAt:       e.lastFailAt,
			LastFailMsg:      e.lastFailMsg,
			Cooldown:         e.cooldown,
		})
	}
	return out
}

// IsProviderLevelError reports whether the given error message matches any
// TripKeyword (case-insensitive). When TripKeywords is empty, every
// non-empty error message is considered provider-level. This is the gate
// used by the session.error handler to decide whether to trip the breaker.
func (b *CircuitBreaker) IsProviderLevelError(msg string) bool {
	if b == nil || !b.cfg.Enabled {
		return false
	}
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return false
	}
	if len(b.cfg.TripKeywords) == 0 {
		return true
	}
	lower := strings.ToLower(msg)
	for _, kw := range b.cfg.TripKeywords {
		if strings.Contains(lower, strings.ToLower(strings.TrimSpace(kw))) {
			return true
		}
	}
	return false
}

// splitModelKey reverses normalizeModelKey for status reporting. The key
// is "provider/model" (both lowercased); we return the two parts as-is
// (lowercased) since the caller only needs them for display.
func splitModelKey(key string) (providerID, modelID string) {
	idx := strings.Index(key, "/")
	if idx < 0 {
		return key, ""
	}
	return key[:idx], key[idx+1:]
}

func truncateMsg(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
