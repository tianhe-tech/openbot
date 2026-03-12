package base

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const (
	MediaTaskVersion = "v1"

	MediaMsgTypeFile  = "file"
	MediaMsgTypeVideo = "video"

	RouteSkillDocument = "document_processor"
	RouteSkillVideo    = "video_understanding"
)

// MediaFileRecord stores metadata for a temporarily saved media file.
type MediaFileRecord struct {
	MessageID    string    `json:"message_id"`
	UserID       string    `json:"user_id"`
	SessionID    string    `json:"session_id"`
	Platform     string    `json:"platform"`
	MsgType      string    `json:"msg_type"`
	Filename     string    `json:"filename"`
	Mime         string    `json:"mime"`
	Size         int64     `json:"size"`
	SHA256       string    `json:"sha256"`
	LocalPath    string    `json:"local_path"`
	RelativePath string    `json:"relative_path"`
	CreatedAt    time.Time `json:"created_at"`
	ExpireAt     time.Time `json:"expire_at"`
}

// MediaTaskContext captures routing and prompt context for file/video tasks.
type MediaTaskContext struct {
	Platform    string
	MessageType string
	UserID      string
	SessionID   string
	MessageID   string
	Files       []MediaFileRecord
}

func SelectRouteSkill(messageType string) string {
	switch strings.ToLower(strings.TrimSpace(messageType)) {
	case MediaMsgTypeFile:
		return RouteSkillDocument
	case MediaMsgTypeVideo:
		return RouteSkillVideo
	default:
		return ""
	}
}

// BuildMediaMetadata returns metadata keys to merge into MessagePayload.Metadata.
func BuildMediaMetadata(ctx MediaTaskContext) (map[string]string, error) {
	filesJSON, err := json.Marshal(ctx.Files)
	if err != nil {
		return nil, fmt.Errorf("marshal media files: %w", err)
	}

	return map[string]string{
		"media_task":         "true",
		"media_task_version": MediaTaskVersion,
		"media_platform":     strings.TrimSpace(ctx.Platform),
		"media_message_type": strings.TrimSpace(ctx.MessageType),
		"media_route_skill":  SelectRouteSkill(ctx.MessageType),
		"media_files":        string(filesJSON),
	}, nil
}

// BuildMediaPromptPrefix returns a stable prompt prefix for media skills routing.
func BuildMediaPromptPrefix(ctx MediaTaskContext) string {
	filesJSON, _ := json.Marshal(ctx.Files)
	return fmt.Sprintf(
		"[MEDIA_TASK %s]\n"+
			"platform: %s\n"+
			"message_type: %s\n"+
			"route_skill: %s\n"+
			"user_id: %s\n"+
			"session_id: %s\n"+
			"message_id: %s\n\n"+
			"[MEDIA_FILES]\n%s\n\n"+
			"[INSTRUCTIONS]\n"+
			"1. 必须优先读取 local_path 对应文件。\n"+
			"2. file 类型使用 %s。\n"+
			"3. video 类型使用 %s。\n"+
			"4. 若文件不可读或过期，明确返回失败原因和重试建议。\n"+
			"5. 输出中文，先给结论，再给关键要点。\n\n",
		MediaTaskVersion,
		strings.TrimSpace(ctx.Platform),
		strings.TrimSpace(ctx.MessageType),
		SelectRouteSkill(ctx.MessageType),
		strings.TrimSpace(ctx.UserID),
		strings.TrimSpace(ctx.SessionID),
		strings.TrimSpace(ctx.MessageID),
		string(filesJSON),
		RouteSkillDocument,
		RouteSkillVideo,
	)
}

// BuildMediaRelativeDir builds a date/user/session folder path.
func BuildMediaRelativeDir(platform, userID, sessionID string, t time.Time) string {
	day := t.Format("2006-01-02")
	return filepath.Join(strings.TrimSpace(platform), day, strings.TrimSpace(userID), strings.TrimSpace(sessionID))
}
