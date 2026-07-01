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
	// backoffs is the sequence of waits applied when ErrRateLimited is seen.
	backoffs []time.Duration
}

type userSender struct {
	mu       sync.Mutex
	lastSent time.Time
}

func newSenderRegistry() *senderRegistry {
	return &senderRegistry{
		users:  make(map[string]*userSender),
		minGap: 1500 * time.Millisecond,
		// Hermes-aligned: short fixed retries instead of long exponential
		// backoff. With tool/step/thinking previews suppressed the per-user
		// budget is no longer being burned by background notifications, so a
		// transient rate-limit usually clears within a few seconds.
		backoffs: []time.Duration{
			3 * time.Second,
			3 * time.Second,
			3 * time.Second,
			3 * time.Second,
		},
	}
}

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
	if len(chunks) == 0 {
		return 0, nil
	}
	u := r.get(userID)
	u.mu.Lock()
	defer u.mu.Unlock()

	for _, chunk := range chunks {
		// Pace between chunks within this call and against the previous call.
		gap := r.minGap
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
	// Retries exhausted. Return the error and let the caller decide what to
	// do (the dispatch path falls back to the offline queue). Do NOT set a
	// sticky cooldown — that previously blocked subsequent critical sends
	// for 90s and made the user experience strictly worse.
	log.Printf("wechat: rate-limit retries exhausted user=%s after %d attempts", userID, len(r.backoffs))
	return err
}
