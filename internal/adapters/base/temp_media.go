package base

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	defaultMediaTTL      = 2 * time.Hour
	defaultMediaMaxBytes = int64(200 * 1024 * 1024) // 200MB
)

// TempMediaSaved describes a persisted temporary media file.
type TempMediaSaved struct {
	LocalPath    string
	RelativePath string
	Filename     string
	Mime         string
	Size         int64
	SHA256       string
	CreatedAt    time.Time
	ExpireAt     time.Time
}

func MediaRootDirFromEnv() string {
	if v := strings.TrimSpace(os.Getenv("MEDIA_TEMP_ROOT")); v != "" {
		return v
	}
	return filepath.Join(os.TempDir(), "openbot-media")
}

func MediaTTLFromEnv() time.Duration {
	v := strings.TrimSpace(os.Getenv("MEDIA_TEMP_TTL_MINUTES"))
	if v == "" {
		return defaultMediaTTL
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return defaultMediaTTL
	}
	return time.Duration(n) * time.Minute
}

func MediaMaxBytesFromEnv() int64 {
	v := strings.TrimSpace(os.Getenv("MEDIA_TEMP_MAX_BYTES"))
	if v == "" {
		return defaultMediaMaxBytes
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		return defaultMediaMaxBytes
	}
	return n
}

// SaveTempMedia writes bytes to temporary storage with size guard and metadata.
func SaveTempMedia(rootDir, relativeDir, msgType, messageID, filename, mime string, data []byte, ttl time.Duration, maxBytes int64) (*TempMediaSaved, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty media data")
	}
	if maxBytes <= 0 {
		maxBytes = defaultMediaMaxBytes
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("media too large: %d > %d", len(data), maxBytes)
	}
	if ttl <= 0 {
		ttl = defaultMediaTTL
	}

	safeMsgType := sanitizeName(msgType)
	if safeMsgType == "" {
		safeMsgType = "media"
	}
	safeMessageID := sanitizeName(messageID)
	if safeMessageID == "" {
		safeMessageID = "msg"
	}
	safeFilename := sanitizeName(filename)
	if safeFilename == "" {
		safeFilename = safeMsgType + guessExtByMime(mime)
	}

	now := time.Now().UTC()
	storedFilename := fmt.Sprintf("%s_%s_%d_%s", safeMsgType, safeMessageID, now.Unix(), safeFilename)

	fullDir := filepath.Join(rootDir, relativeDir)
	if err := os.MkdirAll(fullDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir temp media dir: %w", err)
	}

	localPath := filepath.Join(fullDir, storedFilename)
	if err := os.WriteFile(localPath, data, 0o600); err != nil {
		return nil, fmt.Errorf("write temp media file: %w", err)
	}

	sum := sha256.Sum256(data)
	shaHex := hex.EncodeToString(sum[:])
	relPath := filepath.Join(relativeDir, storedFilename)

	return &TempMediaSaved{
		LocalPath:    localPath,
		RelativePath: relPath,
		Filename:     safeFilename,
		Mime:         strings.TrimSpace(mime),
		Size:         int64(len(data)),
		SHA256:       shaHex,
		CreatedAt:    now,
		ExpireAt:     now.Add(ttl),
	}, nil
}

func sanitizeName(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			continue
		}
		switch r {
		case '-', '_', '.':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return strings.Trim(b.String(), "._")
}

func guessExtByMime(mime string) string {
	m := strings.ToLower(strings.TrimSpace(mime))
	switch {
	case strings.Contains(m, "video/mp4"):
		return ".mp4"
	case strings.Contains(m, "video/"):
		return ".video"
	case strings.Contains(m, "pdf"):
		return ".pdf"
	case strings.Contains(m, "word"):
		return ".doc"
	case strings.Contains(m, "excel"):
		return ".xls"
	case strings.Contains(m, "powerpoint"):
		return ".ppt"
	case strings.Contains(m, "json"):
		return ".json"
	case strings.Contains(m, "text"):
		return ".txt"
	default:
		return ".bin"
	}
}
