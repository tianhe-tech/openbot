package base

import (
	"log"
	"sync"
	"time"
)

// StagingPromptDebouncer delays the "📥 已收到媒体，请告诉我你想做什么" staging
// reply by a short window. Some channels (notably Feishu) deliver a file and
// its accompanying text as two separate events even when the user sends them
// together in one message. Without the delay the user first sees the staging
// prompt, then the merged dispatch — which reads as "the bot didn't handle my
// message properly".
//
// Usage in a channel adapter's staging branch:
//
//	if h.stagingDebounce.Trigger(platform, userID, func() {
//	    // send the staging prompt here
//	}) {
//	    return nil // prompt scheduled; wait for it to fire or be cancelled
//	}
//
// A follow-up text message calls Cancel before consuming staged items, so the
// pending prompt never fires and the media merges silently with the text.
type StagingPromptDebouncer struct {
	mu     sync.Mutex
	timers map[string]*time.Timer
	delay  time.Duration
}

// NewStagingPromptDebouncer creates a debouncer. delay <= 0 falls back to
// 3 seconds — long enough to catch a text event that follows a file event,
// short enough not to feel sluggish.
func NewStagingPromptDebouncer(delay time.Duration) *StagingPromptDebouncer {
	if delay <= 0 {
		delay = 3 * time.Second
	}
	return &StagingPromptDebouncer{
		timers: make(map[string]*time.Timer),
		delay:  delay,
	}
}

// Trigger schedules fn to run after the debounce window unless Cancel is
// called first. Returns true when the callback was scheduled (caller should
// return and wait), false when a callback for the same key is already pending
// (caller should proceed without staging again).
func (d *StagingPromptDebouncer) Trigger(key string, fn func()) bool {
	if d == nil || fn == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timers == nil {
		d.timers = make(map[string]*time.Timer)
	}
	if _, pending := d.timers[key]; pending {
		// A prompt is already scheduled for this user; don't schedule another.
		return false
	}
	d.timers[key] = time.AfterFunc(d.delay, func() {
		d.mu.Lock()
		delete(d.timers, key)
		d.mu.Unlock()
		fn()
	})
	return true
}

// Cancel drops a pending callback for the key. Returns true when a pending
// callback was actually cancelled.
func (d *StagingPromptDebouncer) Cancel(key string) bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	timer, ok := d.timers[key]
	if !ok {
		return false
	}
	timer.Stop()
	delete(d.timers, key)
	return true
}

// CancelAndLog cancels a pending staging prompt and logs it. Convenience
// wrapper used by adapters before consuming staged items.
func (d *StagingPromptDebouncer) CancelAndLog(platform, userID string) {
	if d == nil {
		return
	}
	if d.Cancel(StageKey(platform, userID)) {
		log.Printf("%s: 🔇 cancelled pending staging prompt for user %s (text arrived, merging silently)", platform, shortID(userID))
	}
}

// Pending reports whether a callback is scheduled for the key.
func (d *StagingPromptDebouncer) Pending(key string) bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.timers[key]
	return ok
}

// shortID returns a truncated identifier safe for logging.
func shortID(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:12]
}
