package wechat

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"
)

// inboundBatcher coalesces rapid-fire inbound text messages from the same
// user into a single dispatch. iLink delivers WeChat messages individually,
// so a paste-split or batch-forward triggers one agent invocation per chunk,
// which in turn produces multiple outbound replies and quickly trips the
// rate limiter. Hermes uses the same pattern (~3s quiet window, ~5s for
// long messages) and it cuts iLink rate-limit events dramatically.
//
// Only text messages flow through the batcher; media-bearing messages
// dispatch immediately because file uploads should not be aggregated.
type inboundBatcher struct {
	delay      time.Duration
	splitDelay time.Duration
	splitChars int

	dispatch func(ctx context.Context, msg *WeixinMessage) error

	mu      sync.Mutex
	pending map[string]*pendingInbound
}

type pendingInbound struct {
	msg    *WeixinMessage
	timer  *time.Timer
	lastCh int // length of the most recently appended chunk's text
}

func newInboundBatcher(delay, splitDelay time.Duration, splitChars int,
	dispatch func(ctx context.Context, msg *WeixinMessage) error) *inboundBatcher {
	if delay <= 0 {
		delay = 3 * time.Second
	}
	if splitDelay <= 0 {
		splitDelay = 5 * time.Second
	}
	if splitChars <= 0 {
		splitChars = 1800
	}
	return &inboundBatcher{
		delay:      delay,
		splitDelay: splitDelay,
		splitChars: splitChars,
		dispatch:   dispatch,
		pending:    make(map[string]*pendingInbound),
	}
}

// keyFor groups by sender + chat so a user's DM thread accumulates while
// their group chat (if any) stays independent.
func (b *inboundBatcher) keyFor(msg *WeixinMessage) string {
	chat := msg.GroupID
	if chat == "" {
		chat = msg.FromUserID
	}
	return msg.FromUserID + "|" + chat
}

// enqueueText buffers a text-only message and (re)arms the flush timer.
// It returns true if the caller should NOT dispatch directly (the batcher
// took ownership). For messages that contain media the caller should bypass
// the batcher entirely.
func (b *inboundBatcher) enqueueText(ctx context.Context, msg *WeixinMessage, text string) {
	key := b.keyFor(msg)

	b.mu.Lock()
	defer b.mu.Unlock()

	if existing, ok := b.pending[key]; ok {
		// Stop the previous timer; ignore the bool — even if Stop returns
		// false (timer already fired) the goroutine will see pending was
		// replaced via the *pendingInbound identity check.
		if existing.timer != nil {
			existing.timer.Stop()
		}
		// Concatenate text bodies. Avoid mutating msg further; mutate the
		// stored one so the dispatched payload carries the combined text.
		if text != "" {
			merged := combineText(existing.msg, text)
			setMessageText(existing.msg, merged)
		}
		existing.lastCh = len(text)
	} else {
		// Take ownership of msg. Copy is unnecessary because the poll loop
		// hands us a fresh allocation per message.
		b.pending[key] = &pendingInbound{msg: msg, lastCh: len(text)}
	}

	entry := b.pending[key]
	delay := b.delay
	if entry.lastCh >= b.splitChars {
		delay = b.splitDelay
	}
	entry.timer = time.AfterFunc(delay, func() { b.flush(ctx, key, entry) })
}

func (b *inboundBatcher) flush(ctx context.Context, key string, expected *pendingInbound) {
	b.mu.Lock()
	current, ok := b.pending[key]
	// Identity check: if a newer entry replaced expected after Stop() raced
	// past us, defer to that newer timer.
	if !ok || current != expected {
		b.mu.Unlock()
		return
	}
	delete(b.pending, key)
	b.mu.Unlock()

	if ctx.Err() != nil {
		return
	}
	if err := b.dispatch(ctx, current.msg); err != nil {
		log.Printf("wechat: batched dispatch error from=%s: %v", current.msg.FromUserID, err)
	}
}

// flushAll drains all pending messages synchronously. Called on shutdown so
// in-flight batches are not silently dropped.
func (b *inboundBatcher) flushAll(ctx context.Context) {
	b.mu.Lock()
	all := make([]*pendingInbound, 0, len(b.pending))
	for _, p := range b.pending {
		if p.timer != nil {
			p.timer.Stop()
		}
		all = append(all, p)
	}
	b.pending = make(map[string]*pendingInbound)
	b.mu.Unlock()

	for _, p := range all {
		if err := b.dispatch(ctx, p.msg); err != nil {
			log.Printf("wechat: shutdown-flush dispatch error from=%s: %v", p.msg.FromUserID, err)
		}
	}
}

// combineText returns the merged text body for the existing buffered message
// plus a newly-arrived chunk. Both pieces are extracted from the first text
// item in the message.
func combineText(existing *WeixinMessage, addition string) string {
	prev := firstTextBody(existing)
	if prev == "" {
		return addition
	}
	if addition == "" {
		return prev
	}
	return strings.TrimRight(prev, "\n") + "\n" + addition
}

func firstTextBody(msg *WeixinMessage) string {
	for _, it := range msg.ItemList {
		if it.Type == ItemTypeText && it.TextItem != nil {
			return it.TextItem.Text
		}
	}
	return ""
}

// setMessageText rewrites the first text item in-place. If no text item
// exists one is appended.
func setMessageText(msg *WeixinMessage, text string) {
	for i, it := range msg.ItemList {
		if it.Type == ItemTypeText && it.TextItem != nil {
			msg.ItemList[i].TextItem.Text = text
			return
		}
	}
	msg.ItemList = append(msg.ItemList, MessageItem{
		Type:     ItemTypeText,
		TextItem: &TextItem{Text: text},
	})
}
