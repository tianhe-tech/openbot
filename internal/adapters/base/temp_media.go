package base

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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

// MediaRootDirForOpenCode returns the media storage directory within OpenCode's working directory.
// This ensures skill scripts can access the stored media files.
// Priority: MEDIA_TEMP_ROOT (if absolute) > opencodeDir/tmp > system temp
func MediaRootDirForOpenCode(opencodeDir string) string {
	// If MEDIA_TEMP_ROOT is set and is an absolute path, use it
	if v := strings.TrimSpace(os.Getenv("MEDIA_TEMP_ROOT")); v != "" {
		if filepath.IsAbs(v) {
			return v
		}
		// Relative path: resolve relative to opencodeDir
		return filepath.Join(opencodeDir, v)
	}
	// Default: store within OpenCode working directory so skills can access
	if opencodeDir != "" {
		return filepath.Join(opencodeDir, "tmp", "media")
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

// ExtractedFrame represents an extracted video frame
type ExtractedFrame struct {
	FramePath   string `json:"frame_path"`
	FrameNumber int    `json:"frame_number"`
	Timestamp   string `json:"timestamp"`
}

// ExtractVideoFrames extracts keyframes from a video file using the video-analyzer skill script.
// Returns a list of extracted frame paths.
func ExtractVideoFrames(ctx context.Context, videoPath, opencodeDir string, maxFrames int) ([]ExtractedFrame, error) {
	// Find the extract_frames.py script
	scriptPath := findExtractFramesScript(opencodeDir)
	if scriptPath == "" {
		return nil, fmt.Errorf("extract_frames.py script not found")
	}

	// Create output directory for frames
	outputDir := filepath.Join(filepath.Dir(videoPath), "frames")
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return nil, fmt.Errorf("create frames output dir: %w", err)
	}

	// Run the extraction script
	args := []string{
		scriptPath,
		videoPath,
		"--output-dir", outputDir,
		fmt.Sprintf("--max-frames=%d", maxFrames),
		"--json",
	}

	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(ctx, "python", args...)
	} else {
		cmd = exec.CommandContext(ctx, "python3", args...)
	}

	output, err := cmd.Output()
	if err != nil {
		// Try with just "python" on non-Windows
		if runtime.GOOS != "windows" {
			cmd = exec.CommandContext(ctx, "python", args...)
			output, err = cmd.Output()
		}
		if err != nil {
			return nil, fmt.Errorf("extract frames failed: %w, output: %s", err, string(output))
		}
	}

	// Parse the JSON output
	var result struct {
		Success bool             `json:"success"`
		Frames  []ExtractedFrame `json:"frames"`
		Error   string           `json:"error"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		return nil, fmt.Errorf("parse extraction result: %w", err)
	}
	if !result.Success {
		return nil, fmt.Errorf("extraction failed: %s", result.Error)
	}

	return result.Frames, nil
}

// findExtractFramesScript finds the extract_frames.py script location
func findExtractFramesScript(opencodeDir string) string {
	// Check common locations
	candidates := []string{
		filepath.Join(opencodeDir, ".opencode", "skills", "video-analyzer", "scripts", "extract_frames.py"),
	}

	// Add user config directory based on OS
	if runtime.GOOS == "windows" {
		if home, err := os.UserHomeDir(); err == nil {
			candidates = append(candidates,
				filepath.Join(home, ".config", "opencode", "skills", "video-analyzer", "scripts", "extract_frames.py"),
			)
		}
	} else {
		candidates = append(candidates,
			filepath.Join(os.Getenv("HOME"), ".config", "opencode", "skills", "video-analyzer", "scripts", "extract_frames.py"),
			"/root/.config/opencode/skills/video-analyzer/scripts/extract_frames.py",
		)
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return ""
}

// ReadFileAsDataURI reads a file and returns it as a data URI
func ReadFileAsDataURI(filePath string) (string, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("read file: %w", err)
	}

	// Determine MIME type based on extension
	ext := strings.ToLower(filepath.Ext(filePath))
	var mime string
	switch ext {
	case ".jpg", ".jpeg":
		mime = "image/jpeg"
	case ".png":
		mime = "image/png"
	case ".gif":
		mime = "image/gif"
	case ".webp":
		mime = "image/webp"
	default:
		mime = "image/jpeg"
	}

	return fmt.Sprintf("data:%s;base64,%s", mime, encodeBase64(data)), nil
}

func encodeBase64(data []byte) string {
	const base64Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	result := make([]byte, 0, (len(data)+2)/3*4)

	for i := 0; i < len(data); i += 3 {
		var n uint32
		remaining := len(data) - i

		if remaining >= 3 {
			n = uint32(data[i])<<16 | uint32(data[i+1])<<8 | uint32(data[i+2])
			result = append(result, base64Chars[n>>18&0x3F], base64Chars[n>>12&0x3F], base64Chars[n>>6&0x3F], base64Chars[n&0x3F])
		} else if remaining == 2 {
			n = uint32(data[i])<<16 | uint32(data[i+1])<<8
			result = append(result, base64Chars[n>>18&0x3F], base64Chars[n>>12&0x3F], base64Chars[n>>6&0x3F], '=')
		} else {
			n = uint32(data[i]) << 16
			result = append(result, base64Chars[n>>18&0x3F], base64Chars[n>>12&0x3F], '=', '=')
		}
	}

	return string(result)
}
