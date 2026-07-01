// Package retryworker processes failed message requests that were queued for
// off-peak retry. It reads from the pending_retry table in memstore, re-sends
// each message to the opencode client, and pushes the result back to the user
// via the originating adapter's proactive send path.
package retryworker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/user/opencode-gateway/internal/memstore"
	"github.com/user/opencode-gateway/internal/opencode"
)

// Config controls the retry worker behaviour.
type Config struct {
	// Enabled: when false the worker is a no-op (default false).
	Enabled bool
	// CronExpr is the cron expression for automatic off-peak runs
	// (default "0 22 * * *" — every day at 22:00). An empty string disables
	// the cron trigger; the worker can still be triggered via RunOnce.
	CronExpr string
	// MaxRetries is the per-message retry limit before marking it failed (default 3).
	MaxRetries int
	// BatchSize is the max number of pending messages processed per run (default 20).
	BatchSize int
	// MessageTimeout is the per-message opencode call timeout (default 25 min).
	MessageTimeout time.Duration
}

// DefaultConfig returns conservative defaults.
func DefaultConfig() Config {
	return Config{
		Enabled:        false,
		CronExpr:       "0 22 * * *",
		MaxRetries:     3,
		BatchSize:      20,
		MessageTimeout: 25 * time.Minute,
	}
}

// RetrySender is the interface an adapter must implement to participate in
// retry processing. The worker calls SendRetryMessage to replay the original
// request and NotifyUser to push the final result back.
type RetrySender interface {
	// AdapterName returns the adapter identifier ("feishu", "dingtalk", "wecom").
	AdapterName() string
	// SendRetryMessage replays the pending message through opencode and returns
	// the AI reply. It must use a fresh session context so it does not collide
	// with live sessions.
	SendRetryMessage(ctx context.Context, r memstore.PendingRetry) (string, error)
	// NotifyUser sends text back to the user who originally made the request.
	NotifyUser(ctx context.Context, r memstore.PendingRetry, reply string) error
}

// Registry holds one RetrySender per adapter.
type Registry struct {
	mu      sync.RWMutex
	senders map[string]RetrySender
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{senders: make(map[string]RetrySender)}
}

// Register adds or replaces the sender for an adapter.
func (reg *Registry) Register(s RetrySender) {
	reg.mu.Lock()
	defer reg.mu.Unlock()
	reg.senders[s.AdapterName()] = s
}

// get retrieves a sender (nil if not registered).
func (reg *Registry) get(adapter string) RetrySender {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	return reg.senders[adapter]
}

// Worker orchestrates off-peak retry processing.
type Worker struct {
	cfg      Config
	store    *memstore.Store
	registry *Registry
}

// New creates a Worker. store and registry must be non-nil when cfg.Enabled is true.
func New(cfg Config, store *memstore.Store, registry *Registry) *Worker {
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 3
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 20
	}
	if cfg.MessageTimeout <= 0 {
		cfg.MessageTimeout = 25 * time.Minute
	}
	return &Worker{cfg: cfg, store: store, registry: registry}
}

// RunOnce processes up to BatchSize pending messages sequentially.
// It is safe to call from a cron job, a slash command, or any goroutine.
// Returns the number of messages successfully retried.
func (w *Worker) RunOnce(ctx context.Context) (succeeded int, total int, err error) {
	if !w.cfg.Enabled || w.store == nil {
		return 0, 0, nil
	}

	rows, loadErr := w.store.LoadPendingRetries(w.cfg.BatchSize)
	if loadErr != nil {
		return 0, 0, fmt.Errorf("retryworker: load pending: %w", loadErr)
	}
	total = len(rows)
	if total == 0 {
		log.Printf("retryworker: no pending retries")
		return 0, 0, nil
	}
	log.Printf("retryworker: processing %d pending retry record(s)", total)

	for _, r := range rows {
		if ctx.Err() != nil {
			break
		}
		ok := w.processOne(ctx, r)
		if ok {
			succeeded++
		}
	}
	log.Printf("retryworker: done — succeeded=%d total=%d", succeeded, total)
	return succeeded, total, nil
}

// StatusSummary returns a human-readable queue summary string.
func (w *Worker) StatusSummary(ctx context.Context) string {
	if w.store == nil {
		return "❌ 重试队列未启用（内存存储不可用）"
	}
	pending, processing, done, failed, err := w.store.CountPendingRetries()
	if err != nil {
		return fmt.Sprintf("❌ 查询队列失败: %v", err)
	}
	return fmt.Sprintf(
		"📋 离线重试队列\n• 等待处理: %d\n• 处理中: %d\n• 已完成: %d\n• 已失败: %d",
		pending, processing, done, failed,
	)
}

// processOne claims one row, calls the adapter's SendRetryMessage, and
// updates the row status. Returns true on success.
func (w *Worker) processOne(ctx context.Context, r memstore.PendingRetry) bool {
	sender := w.registry.get(r.Adapter)
	if sender == nil {
		log.Printf("retryworker: no sender for adapter %q, skipping %s", r.Adapter, r.ID)
		return false
	}

	if err := w.store.MarkRetryProcessing(r.ID); err != nil {
		log.Printf("retryworker: mark processing failed for %s: %v", r.ID, err)
		return false
	}

	msgCtx, cancel := context.WithTimeout(ctx, w.cfg.MessageTimeout)
	defer cancel()

	reply, sendErr := sender.SendRetryMessage(msgCtx, r)

	if sendErr != nil {
		log.Printf("retryworker: retry failed for %s (adapter=%s user=%s attempt=%d): %v",
			r.ID, r.Adapter, r.UserID, r.RetryCount+1, sendErr)
		permanent, markErr := w.store.MarkRetryFailed(r.ID, w.cfg.MaxRetries)
		if markErr != nil {
			log.Printf("retryworker: MarkRetryFailed error for %s: %v", r.ID, markErr)
		}
		if permanent {
			// Max retries reached — notify user of permanent failure.
			notifyCtx, notifCancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer notifCancel()
			msg := fmt.Sprintf(
				"❌ 您的离线任务重试失败（已尝试 %d 次）\n\n【原始消息（%s）】\n%s\n\n请稍后手动重新发送。",
				w.cfg.MaxRetries,
				r.CreatedAt.Format("01-02 15:04"),
				truncate(r.Content, 200),
			)
			if nErr := sender.NotifyUser(notifyCtx, r, msg); nErr != nil {
				log.Printf("retryworker: notify permanent failure error for %s: %v", r.ID, nErr)
			}
		}
		return false
	}

	if err := w.store.MarkRetryDone(r.ID); err != nil {
		log.Printf("retryworker: MarkRetryDone error for %s: %v", r.ID, err)
	}

	// Push result back to user.
	notifyCtx, notifCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer notifCancel()
	header := fmt.Sprintf(
		"🔄 离线任务已处理完成（原发送时间: %s）\n\n【原始请求】\n%s\n\n【回复】\n",
		r.CreatedAt.Format("01-02 15:04"),
		truncate(r.Content, 200),
	)
	if nErr := sender.NotifyUser(notifyCtx, r, header+reply); nErr != nil {
		log.Printf("retryworker: notify success error for %s: %v", r.ID, nErr)
	}
	return true
}

// BuildOpenCodePayload constructs an opencode.MessagePayload from a PendingRetry.
// Adapters can call this helper inside their SendRetryMessage implementation.
func BuildOpenCodePayload(r memstore.PendingRetry) opencode.MessagePayload {
	payload := opencode.MessagePayload{
		Channel:  r.Adapter,
		UserID:   r.UserID,
		ThreadID: r.ThreadID,
		Content:  r.Content,
	}
	if r.AttachmentsJSON != "" {
		var attachments []opencode.Attachment
		if err := json.Unmarshal([]byte(r.AttachmentsJSON), &attachments); err == nil {
			payload.Attachments = attachments
		}
	}
	if r.Metadata != nil {
		payload.Metadata = r.Metadata
	}
	return payload
}

// IsDeadlineErr returns true when an error looks like a context/deadline exceeded
// with zero accumulated content — the only case we want to queue for retry.
func IsDeadlineErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "context deadline exceeded") ||
		strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "request timeout")
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
