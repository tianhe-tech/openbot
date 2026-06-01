package wechat

import (
	"errors"
	"log"
	"sync"
	"time"
)

// senderRegistry serializes outbound WeChat messages per user and applies
// pacing + rate-limit-aware backoff so that the iLink bot API does not start
// rejecting messages with ret=-2 mid-stream.
type senderRegistry struct {
	mu    sync.Mutex
	users map[string]*userSender

	// minGap is the minimum interval between two successful sends to a user.
	minGap time.Duration
	// chunkGap is the minimum interval between two chunks of the same call.
	chunkGap time.Duration
	// backoffs is the sequence of waits applied when ErrRateLimited is seen.
	backoffs []time.Duration
	// postExhaustCooldown is the cooldown set after all retries have been
	// exhausted on a rate-limit error; subsequent skippable sends drop and
	// critical sends wait this long before attempting again. iLink keeps a
	// user rate-limited for a long time once tripped, so this needs to be
	// measured in tens of seconds, not seconds.
	postExhaustCooldown time.Duration
}

type userSender struct {
	mu       sync.Mutex
	lastSent time.Time
	// cooldownUntil suppresses sends until this timestamp once the API has
	// signalled rate-limit; it is extended on repeated rejections.
	cooldownUntil time.Time
}

func newSenderRegistry() *senderRegistry {
	return &senderRegistry{
		users:    make(map[string]*userSender),
		minGap:   1500 * time.Millisecond,
		chunkGap: 1500 * time.Millisecond,
		// iLink keeps a user rate-limited for much longer than expected once
		// tripped — short retries (2/5/10s) all fail. Give the server real
		// time to forgive us between attempts.
		backoffs: []time.Duration{
			10 * time.Second,
			30 * time.Second,
			60 * time.Second,
		},
		postExhaustCooldown: 90 * time.Second,
	}
}

// ErrSkipped is returned when a skippable send was dropped because the user's
// sender is currently rate-limited or paced; intermediate notifications
// (tool/step/todo/progress) should not consume retry budget that the final
// answer needs.
var ErrSkipped = errors.New("wechat send skipped (rate-limit pacing)")

func (r *senderRegistry) get(userID string) *userSender {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.users[userID]
	if !ok {
		s = &userSender{}
		r.users[userID] = s
	}
	return s
}

// sendChunks delivers chunks to a user serially, pacing between chunks and
// retrying with backoff on rate-limit. send is the actual transport call.
// Returns the number of chunks successfully delivered and the first non-retry
// error encountered (or the last rate-limit error if retries are exhausted).
func (r *senderRegistry) sendChunks(userID string, chunks []string, send func(chunk string) error) (delivered int, firstErr error) {
	return r.sendChunksOpt(userID, chunks, send, false)
}

// sendChunksSkippable is like sendChunks but drops the batch immediately if
// the user is currently in a rate-limit cooldown or the last send was within
// minGap. Intended for non-critical notifications (tool progress, step, todo,
// intermediate streaming previews) so they cannot starve the final answer of
// retry budget.
func (r *senderRegistry) sendChunksSkippable(userID string, chunks []string, send func(chunk string) error) (delivered int, firstErr error) {
	return r.sendChunksOpt(userID, chunks, send, true)
}

func (r *senderRegistry) sendChunksOpt(userID string, chunks []string, send func(chunk string) error, skippable bool) (delivered int, firstErr error) {
	if len(chunks) == 0 {
		return 0, nil
	}
	u := r.get(userID)

	// For skippable sends, peek under the lock without blocking on it: if the
	// user-sender is busy or hot, drop now rather than queueing behind
	// retries.
	if skippable {
		if !u.mu.TryLock() {
			return 0, ErrSkipped
		}
	} else {
		u.mu.Lock()
	}
	defer u.mu.Unlock()

	// Drop skippable batches if we're in an active rate-limit cooldown or
	// the previous send was too recent to pace this one without waiting.
	if skippable {
		if time.Now().Before(u.cooldownUntil) {
			return 0, ErrSkipped
		}
		if time.Since(u.lastSent) < r.minGap {
			return 0, ErrSkipped
		}
	}

	for i, chunk := range chunks {
		// Respect cooldown from a prior rate-limit episode.
		if wait := time.Until(u.cooldownUntil); wait > 0 {
			time.Sleep(wait)
		}
		// Pace between chunks within this call and against the previous call.
		gap := r.minGap
		if i > 0 && r.chunkGap > gap {
			gap = r.chunkGap
		}
		if since := time.Since(u.lastSent); since < gap {
			time.Sleep(gap - since)
		}

		err := r.sendWithRetry(userID, chunk, send, u)
		u.lastSent = time.Now()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			// On rate-limit exhaustion or other failures, stop the rest of
			// this batch — re-attempting now will only deepen the cooldown.
			break
		}
		delivered++
	}

	return delivered, firstErr
}

func (r *senderRegistry) sendWithRetry(userID, chunk string, send func(chunk string) error, u *userSender) error {
	err := send(chunk)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrRateLimited) {
		return err
	}
	for attempt, wait := range r.backoffs {
		// Extend cooldown so concurrent calls into this sender also wait.
		until := time.Now().Add(wait)
		if until.After(u.cooldownUntil) {
			u.cooldownUntil = until
		}
		log.Printf("wechat: rate-limited user=%s attempt=%d backoff=%s", userID, attempt+1, wait)
		time.Sleep(wait)
		err = send(chunk)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrRateLimited) {
			return err
		}
	}
	// Retries exhausted and we're still rate-limited. Set a sticky cooldown
	// so subsequent skippable sends drop without queuing more attempts and
	// any critical send waits a meaningful interval before trying again.
	until := time.Now().Add(r.postExhaustCooldown)
	if until.After(u.cooldownUntil) {
		u.cooldownUntil = until
	}
	log.Printf("wechat: rate-limit retries exhausted user=%s; entering %s cooldown", userID, r.postExhaustCooldown)
	return err
}
