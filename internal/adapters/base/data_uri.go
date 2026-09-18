package base

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// DecodeDataURI decodes a data URI (data:<mime>;base64,<payload>) into its
// MIME type and raw bytes. Returns an error for non-data URIs or malformed
// payloads.
func DecodeDataURI(dataURI string) (string, []byte, error) {
	uri := strings.TrimSpace(dataURI)
	if !strings.HasPrefix(uri, "data:") {
		return "", nil, fmt.Errorf("not a data URI")
	}
	rest := uri[len("data:"):]
	commaIdx := strings.Index(rest, ",")
	if commaIdx < 0 {
		return "", nil, fmt.Errorf("data URI missing comma separator")
	}
	header := rest[:commaIdx]
	payload := rest[commaIdx+1:]

	mimeType := strings.TrimSpace(header)
	if idx := strings.Index(mimeType, ";"); idx >= 0 {
		mimeType = strings.TrimSpace(mimeType[:idx])
	}
	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return "", nil, fmt.Errorf("decode base64 payload: %w", err)
	}
	return mimeType, data, nil
}

// SaveDataURIToTempMedia decodes a data URI and persists the bytes via
// SaveTempMedia under the given relative directory. This lets channel
// adapters convert inline media (e.g. WeChat video data URIs) into local
// files so the model can read them from disk instead of receiving a file
// part that providers may reject.
func SaveDataURIToTempMedia(dataURI, relativeDir, msgType, messageID, filename string) (*TempMediaSaved, error) {
	mimeType, data, err := DecodeDataURI(dataURI)
	if err != nil {
		return nil, err
	}
	if filename == "" {
		filename = msgType + guessExtByMime(mimeType)
	}
	return SaveTempMedia(
		MediaRootDirForOpenCode(""),
		relativeDir,
		msgType,
		messageID,
		filename,
		mimeType,
		data,
		MediaTTLFromEnv(),
		MediaMaxBytesFromEnv(),
	)
}

// guessExtByMime is defined in temp_media.go; keep a compile-time reference
// to filepath/os so this file stays self-contained if helpers move.
var _ = filepath.Join
var _ = os.TempDir
