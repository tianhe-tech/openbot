package base

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// StagedMediaItem describes a single media item staged for a user, waiting
// for the user to state their intent before being forwarded to OpenCode.
//
// The staging layer is intentionally decoupled from the opencode package:
// media content is stored as a data URI (or referenced via MediaFileRecord
// for files saved on disk), and each channel adapter converts staged items
// into its own attachment representation when consuming them.
type StagedMediaItem struct {
	Platform string `json:"platform"`
	MsgType  string `json:"msg_type"`
	Filename string `json:"filename"`
	Mime     string `json:"mime"`
	// DataURI holds inline media content (data:<mime>;base64,...) for
	// image/video/audio attachments. Empty when the media is only available
	// on disk (see MediaFile).
	DataURI string `json:"data_uri,omitempty"`
	// MediaFile references a media file persisted via SaveTempMedia. May be
	// nil for inline-only items.
	MediaFile *MediaFileRecord `json:"media_file,omitempty"`
	// ExtraAttachments holds additional derived attachments (e.g. extracted
	// video frames) that should be forwarded together with this item.
	ExtraAttachments []StagedAttachment `json:"extra_attachments,omitempty"`
	CreatedAt        time.Time          `json:"created_at"`
	ExpireAt         time.Time          `json:"expire_at"`
}

// StagedAttachment is a generic attachment descriptor used by the staging
// layer. Channels convert it into their own attachment types.
type StagedAttachment struct {
	Mime     string `json:"mime"`
	DataURI  string `json:"data_uri"`
	Filename string `json:"filename,omitempty"`
}

// MediaStagingStore stages media items per user until the user states their
// intent with a follow-up text message. Items expire after the configured
// media TTL and are dropped lazily on access.
//
// Each channel adapter should own its own store instance (userIDs may collide
// across platforms), or share one keyed by platform+userID via StageKey.
type MediaStagingStore struct {
	mu    sync.Mutex
	items map[string][]StagedMediaItem
	ttl   time.Duration
}

// NewMediaStagingStore creates a staging store. ttl <= 0 falls back to
// MediaTTLFromEnv.
func NewMediaStagingStore(ttl time.Duration) *MediaStagingStore {
	if ttl <= 0 {
		ttl = MediaTTLFromEnv()
	}
	return &MediaStagingStore{
		items: make(map[string][]StagedMediaItem),
		ttl:   ttl,
	}
}

// StageKey builds the staging key for a platform-scoped user.
func StageKey(platform, userID string) string {
	return strings.TrimSpace(platform) + ":" + strings.TrimSpace(userID)
}

// Stage appends a media item for the given user. Expired items are pruned
// first. Returns an error if the item has already expired.
func (s *MediaStagingStore) Stage(platform, userID string, item StagedMediaItem) error {
	if s == nil {
		return fmt.Errorf("staging store is nil")
	}
	now := time.Now()
	if item.ExpireAt.IsZero() {
		item.ExpireAt = now.Add(s.ttl)
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	if !item.ExpireAt.After(now) {
		return fmt.Errorf("staged media already expired")
	}
	if item.Platform == "" {
		item.Platform = platform
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.items == nil {
		s.items = make(map[string][]StagedMediaItem)
	}
	key := StageKey(platform, userID)
	s.items[key] = pruneExpiredLocked(s.items[key], now)
	s.items[key] = append(s.items[key], item)
	return nil
}

// Consume returns and clears all staged items for the user. Expired items are
// dropped. Returns (nil, false) when nothing is staged.
func (s *MediaStagingStore) Consume(platform, userID string) ([]StagedMediaItem, bool) {
	if s == nil {
		return nil, false
	}
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	key := StageKey(platform, userID)
	items := pruneExpiredLocked(s.items[key], now)
	if len(items) == 0 {
		delete(s.items, key)
		return nil, false
	}
	delete(s.items, key)
	return items, true
}

// Peek returns staged items for the user without consuming them.
func (s *MediaStagingStore) Peek(platform, userID string) ([]StagedMediaItem, bool) {
	if s == nil {
		return nil, false
	}
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	items := pruneExpiredLocked(s.items[StageKey(platform, userID)], now)
	return items, len(items) > 0
}

// Clear drops all staged items for the user (e.g. on /new or /abort).
func (s *MediaStagingStore) Clear(platform, userID string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, StageKey(platform, userID))
}

// CleanupExpired removes every expired item across all users and returns the
// number of remaining users with staged items. Suitable for a periodic
// goroutine.
func (s *MediaStagingStore) CleanupExpired() int {
	if s == nil {
		return 0
	}
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	remaining := 0
	for key, items := range s.items {
		items = pruneExpiredLocked(items, now)
		if len(items) == 0 {
			delete(s.items, key)
			continue
		}
		s.items[key] = items
		remaining++
	}
	return remaining
}

// StageAll stages multiple items and returns the ones successfully staged.
// Items that fail to stage (e.g. already expired) are skipped.
func (s *MediaStagingStore) StageAll(platform, userID string, items []StagedMediaItem) []StagedMediaItem {
	if s == nil || len(items) == 0 {
		return nil
	}
	staged := make([]StagedMediaItem, 0, len(items))
	for _, item := range items {
		if err := s.Stage(platform, userID, item); err != nil {
			continue
		}
		staged = append(staged, item)
	}
	return staged
}

// StartCleanupLoop runs CleanupExpired periodically until ctx is cancelled.
func (s *MediaStagingStore) StartCleanupLoop(ctx context.Context, interval time.Duration) {
	if s == nil || interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				s.CleanupExpired()
			}
		}
	}()
}

// pruneExpiredLocked filters out expired items. Caller must hold s.mu.
func pruneExpiredLocked(items []StagedMediaItem, now time.Time) []StagedMediaItem {
	if len(items) == 0 {
		return nil
	}
	kept := items[:0]
	for _, item := range items {
		if item.ExpireAt.IsZero() || item.ExpireAt.After(now) {
			kept = append(kept, item)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

// StagedItemsToPromptHint builds a short human-readable summary of staged
// items, used in the "media received" reply so the user knows what is pending.
func StagedItemsToPromptHint(items []StagedMediaItem) string {
	if len(items) == 0 {
		return ""
	}
	counts := make(map[string]int)
	for _, item := range items {
		counts[strings.ToLower(strings.TrimSpace(item.MsgType))]++
	}
	parts := make([]string, 0, len(counts))
	for _, msgType := range []string{"image", "video", "file", "audio"} {
		if n := counts[msgType]; n > 1 {
			parts = append(parts, fmt.Sprintf("%s×%d", mediaTypeLabel(msgType), n))
		} else if n == 1 {
			parts = append(parts, mediaTypeLabel(msgType))
		}
	}
	// Include any non-standard types.
	for msgType, n := range counts {
		switch msgType {
		case "image", "video", "file", "audio", "":
		default:
			if n > 1 {
				parts = append(parts, fmt.Sprintf("%s×%d", msgType, n))
			} else {
				parts = append(parts, msgType)
			}
		}
	}
	if len(parts) == 0 {
		return "媒体文件"
	}
	return strings.Join(parts, "、")
}

func mediaTypeLabel(msgType string) string {
	switch msgType {
	case "image":
		return "图片"
	case "video":
		return "视频"
	case "file":
		return "文件"
	case "audio":
		return "语音"
	default:
		return msgType
	}
}
