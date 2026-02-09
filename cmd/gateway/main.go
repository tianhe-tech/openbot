package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	opencodesdk "github.com/sst/opencode-sdk-go"
	"github.com/user/opencode-gateway/internal/adapters/base"
	"github.com/user/opencode-gateway/internal/adapters/dingtalk"
	"github.com/user/opencode-gateway/internal/adapters/feishu"
	"github.com/user/opencode-gateway/internal/adapters/wecom"
	"github.com/user/opencode-gateway/internal/config"
	"github.com/user/opencode-gateway/internal/opencode"
	"github.com/user/opencode-gateway/internal/scheduler"
	"github.com/user/opencode-gateway/internal/server"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	// Create adapter registry for bidirectional communication
	adapterRegistry := base.NewAdapterRegistry()

	// Create OpenCode client with event handling support
	ocClient := opencode.NewClient(cfg.OpenCodeEndpoint, cfg.OpenCodeAPIKey,
		opencode.WithDirectory("."),
	)

	// ========== Create Task Scheduler ==========
	schedulerCfg := scheduler.DefaultTaskSchedulerConfig()
	taskScheduler := scheduler.NewTaskScheduler(ocClient, schedulerCfg)
	cronScheduler := scheduler.NewCronScheduler(taskScheduler)
	webhookHandler := scheduler.NewWebhookHandler(taskScheduler, cronScheduler)

	// Create adapters
	wecomHandler := wecom.NewHandler(ocClient, cfg.WeCom)
	feishuHandler := feishu.NewHandler(ocClient, cfg.FeiShu)
	dingtalkHandler := dingtalk.NewHandler(ocClient, cfg.DingTalk)

	// Set cronScheduler to adapters so they can manage scheduled tasks
	dingtalkHandler.SetCronScheduler(cronScheduler)
	feishuHandler.SetCronScheduler(cronScheduler)
	// TODO: Add SetCronScheduler to other adapters if needed
	// wecomHandler.SetCronScheduler(cronScheduler)

	// Register adapters in registry
	adapterRegistry.Register(wecomHandler.GetAdapter())
	adapterRegistry.Register(feishuHandler.GetAdapter())
	adapterRegistry.Register(dingtalkHandler.GetAdapter())

	// Register event handler for OpenCode -> Adapter communication
	ocClient.RegisterEventHandler(func(ctx context.Context, event *opencodesdk.EventListResponse) error {
		eventType := string(event.Type)
		log.Printf("🔍 MAIN EVENT HANDLER - type=%s", eventType)

		// Extract sessionID from event
		sessionID := extractSessionIDFromEvent(event)
		if sessionID == "" {
			log.Printf("opencode event: no sessionID found in event type %s, JSON preview: %.200s",
				eventType, event.JSON.RawJSON())
			return nil
		}

		// Try to find the user for this session across all adapters
		var foundAdapter *base.BidirectionalAdapter
		var foundUserID string
		var foundChannel string

		for _, adapter := range []struct {
			name    string
			handler interface {
				GetAdapter() *base.BidirectionalAdapter
			}
		}{
			{"dingtalk", dingtalkHandler},
			{"feishu", feishuHandler},
			{"wecom", wecomHandler},
		} {
			adapter := adapter.handler.GetAdapter()
			if userID, ok := adapter.GetUserForSession(sessionID); ok {
				foundAdapter = adapter
				foundUserID = userID
				foundChannel = adapter.Name()
				log.Printf("opencode event: found session %s in adapter %s for user %s",
					sessionID[:min(8, len(sessionID))], foundChannel, foundUserID)
				break
			}
		}

		if foundAdapter == nil {
			log.Printf("opencode event: no adapter found for session %s, skipping", sessionID[:min(8, len(sessionID))])
			return nil
		}

		// Extract content from event based on type
		content, err := extractContentFromEvent(event)
		if err != nil {
			log.Printf("opencode event: failed to extract content from event type %s: %v", event.Type, err)
			return err
		}

		if content == "" {
			log.Printf("opencode event: no content extracted from event type %s for session %s",
				eventType, sessionID[:min(8, len(sessionID))])
			return nil
		}

		log.Printf("🔍 ROUTING TO ADAPTER - adapter=%s, session=%s, user=%s, content_len=%d, preview=%.100s",
			foundChannel, sessionID[:min(8, len(sessionID))], foundUserID, len(content), content)

		// Route to adapter
		return adapterRegistry.RouteEventToAdapter(ctx, foundChannel, sessionID, content)
	})

	// Start event listener for bidirectional communication
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Register adapters to taskScheduler for sending results
	taskScheduler.RegisterAdapter("dingtalk", dingtalkHandler)
	taskScheduler.RegisterAdapter("feishu", feishuHandler)
	taskScheduler.RegisterAdapter("wecom", wecomHandler)

	// Start task scheduler
	if err := taskScheduler.Start(); err != nil {
		log.Fatalf("failed to start task scheduler: %v", err)
	}
	defer taskScheduler.Stop()

	// Start cron scheduler
	if err := cronScheduler.Start(); err != nil {
		log.Fatalf("failed to start cron scheduler: %v", err)
	}
	defer cronScheduler.Stop()

	if err := ocClient.StartEventListener(ctx); err != nil {
		log.Printf("warning: could not start event listener: %v", err)
	} else {
		log.Println("opencode event listener started")
	}

	// Start DingTalk Stream client if enabled
	if err := dingtalkHandler.Start(ctx); err != nil {
		log.Printf("warning: could not start dingtalk stream client: %v", err)
	}
	defer dingtalkHandler.Stop()

	// Start Feishu WebSocket client if enabled
	if err := feishuHandler.Start(ctx); err != nil {
		log.Printf("warning: could not start feishu websocket client: %v", err)
	}
	defer feishuHandler.Stop()

	// Setup HTTP server
	srv := server.New(server.Config{
		Addr:          cfg.ServerAddr,
		ReadTimeout:   cfg.ReadTimeout,
		WriteTimeout:  cfg.WriteTimeout,
		ShutdownGrace: cfg.ShutdownGrace,
	})

	mux := srv.Mux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	// Register scheduler webhook routes
	webhookHandler.RegisterRoutes(mux)
	log.Println("scheduler webhook routes registered")

	// Mount adapters (webhook endpoints)
	wecomHandler.Mount(mux)
	feishuHandler.Mount(mux)
	dingtalkHandler.Mount(mux) // Only mounts if not using Stream mode

	log.Printf("opencode gateway ready on %s (bidirectional mode)", cfg.ServerAddr)

	// Log registered adapters and their modes
	var adapters []string
	adapters = append(adapters, "wecom (webhook)")
	if cfg.FeiShu.UseWebSocket {
		adapters = append(adapters, "feishu (websocket)")
	} else {
		adapters = append(adapters, "feishu (webhook)")
	}
	if cfg.DingTalk.UseStream {
		adapters = append(adapters, "dingtalk (stream)")
	} else {
		adapters = append(adapters, "dingtalk (webhook)")
	}
	log.Printf("adapters registered: %v", adapters)
	log.Printf("event listener: active")

	if err := srv.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("server error: %v", err)
	}

	log.Println("gateway stopped")
}

// extractSessionIDFromEvent 从OpenCode事件中提取sessionID
func extractSessionIDFromEvent(event *opencodesdk.EventListResponse) string {
	if event == nil || event.JSON.RawJSON() == "" {
		return ""
	}

	jsonData := event.JSON.RawJSON()

	// 尝试提取多个位置的sessionID
	sessionID := ""

	// 1. 从 properties 中提取 sessionID（支持多个位置）
	var propsWrapper struct {
		Properties struct {
			SessionID string `json:"sessionID"`
			Message   struct {
				SessionID string `json:"sessionID"`
			} `json:"message"`
			Info struct {
				SessionID string `json:"sessionID"`
				ID        string `json:"id"`
			} `json:"info"`
			Part struct {
				SessionID string `json:"sessionID"`
			} `json:"part"`
		} `json:"properties"`
	}
	if err := json.Unmarshal([]byte(jsonData), &propsWrapper); err == nil {
		if propsWrapper.Properties.SessionID != "" {
			sessionID = propsWrapper.Properties.SessionID
		} else if propsWrapper.Properties.Message.SessionID != "" {
			sessionID = propsWrapper.Properties.Message.SessionID
		} else if propsWrapper.Properties.Part.SessionID != "" {
			sessionID = propsWrapper.Properties.Part.SessionID
		} else if propsWrapper.Properties.Info.SessionID != "" {
			sessionID = propsWrapper.Properties.Info.SessionID
		} else if propsWrapper.Properties.Info.ID != "" && strings.HasPrefix(propsWrapper.Properties.Info.ID, "ses_") {
			// session.created/session.updated events store session ID in properties.info.id
			sessionID = propsWrapper.Properties.Info.ID
		}
	}

	// 2. 从根级别 sessionID 提取
	if sessionID == "" {
		var rootWrapper struct {
			SessionID string `json:"sessionID"`
		}
		if err := json.Unmarshal([]byte(jsonData), &rootWrapper); err == nil {
			sessionID = rootWrapper.SessionID
		}
	}

	if sessionID != "" {
		return sessionID
	}

	// 3. 特殊处理：从 data 提取
	var dataWrapper struct {
		Data struct {
			SessionID string `json:"sessionID"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(jsonData), &dataWrapper); err == nil {
		return dataWrapper.Data.SessionID
	}

	return ""
}

// extractContentFromEvent 从OpenCode事件中提取内容
func extractContentFromEvent(event *opencodesdk.EventListResponse) (string, error) {
	if event == nil || event.JSON.RawJSON() == "" {
		return "", nil
	}

	eventType := string(event.Type)

	// 根据不同的事件类型提取内容
	switch eventType {
	case "message.part.updated":
		// 💡 重要：message.part.updated 由 StreamingSessionHandler 处理并通过 callback 发送
		// 这里不处理，避免重复发送。只返回空字符串告诉调用者忽略此事件。
		return "", nil

	case "message.completed":
		var wrapper struct {
			Properties struct {
				Message struct {
					Content string `json:"content"`
				} `json:"message"`
			} `json:"properties"`
		}
		if err := json.Unmarshal([]byte(event.JSON.RawJSON()), &wrapper); err == nil {
			if wrapper.Properties.Message.Content != "" {
				return wrapper.Properties.Message.Content, nil
			}
		}
		return "", nil

	case "question.asked", "permission.asked":
		// 💡 重要：question.asked/permission.asked 由 StreamingSessionHandler 处理并通过 callback 发送
		// 这里不处理，避免重复发送给adapter。
		return "", nil

	case "session.error":
		var wrapper struct {
			Properties struct {
				Message string `json:"message"`
				Detail  string `json:"detail"`
			} `json:"properties"`
		}
		if err := json.Unmarshal([]byte(event.JSON.RawJSON()), &wrapper); err == nil {
			var parts []string
			if wrapper.Properties.Message != "" {
				parts = append(parts, wrapper.Properties.Message)
			}
			if wrapper.Properties.Detail != "" {
				parts = append(parts, wrapper.Properties.Detail)
			}
			if len(parts) > 0 {
				return "⚠️ OpenCode 会话出错：" + strings.Join(parts, "; "), nil
			}
		}
		return "⚠️ OpenCode 会话发生错误，请稍后重试。", nil

	case "session.status", "session.updated", "session.created", "session.diff",
		"file.edited", "file.watcher.updated", "lsp.updated", "server.heartbeat", "message.updated":
		return "", nil

	case "session.idle":
		// 不发送额外提示，StreamingSessionHandler 已处理完成逻辑
		// 实际内容由流式回调发送，避免重复消息
		return "", nil
	}

	// 默认返回空字符串
	return "", nil
}

// extractQuestionFromEventJSON 从事件JSON中提取问题内容
func extractQuestionFromEventJSON(jsonData string) (*opencode.Question, string) {
	var wrapper struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(jsonData), &wrapper); err != nil {
		return nil, ""
	}

	if wrapper.Type == "permission.asked" {
		return extractPermissionQuestionJSON(jsonData)
	} else if wrapper.Type == "question.asked" {
		return extractNormalQuestionJSON(jsonData)
	}

	return nil, ""
}

// extractPermissionQuestionJSON 提取权限问题
func extractPermissionQuestionJSON(jsonData string) (*opencode.Question, string) {
	type PermissionProps struct {
		ID         string   `json:"id"`
		SessionID  string   `json:"sessionID"`
		Permission string   `json:"permission"`
		Patterns   []string `json:"patterns"`
		Metadata   struct {
			Filepath  string `json:"filepath"`
			ParentDir string `json:"parentDir"`
		} `json:"metadata"`
	}

	var wrapper struct {
		Properties PermissionProps `json:"properties"`
	}
	if err := json.Unmarshal([]byte(jsonData), &wrapper); err != nil {
		return nil, ""
	}

	props := wrapper.Properties
	if props.ID == "" {
		return nil, ""
	}

	var permDesc string
	switch props.Permission {
	case "external_directory":
		permDesc = "访问外部目录"
	case "write_file":
		permDesc = "写入文件"
	case "execute_command":
		permDesc = "执行命令"
	case "network_access":
		permDesc = "网络访问"
	default:
		permDesc = props.Permission
	}

	var details string
	if len(props.Patterns) > 0 {
		details = "路径: " + strings.Join(props.Patterns, ", ")
	}
	if props.Metadata.Filepath != "" {
		if details != "" {
			details += "\\n"
		}
		details += "文件: " + props.Metadata.Filepath
	}

	msg := fmt.Sprintf("🔐 OpenCode 请求权限：\\n\\n"+
		"【%s】\\n\\n%s\\n\\n"+
		"请回复：\\n"+
		"• 允许 - 本次允许\\n"+
		"• 拒绝 - 拒绝此请求\\n"+
		"• 始终允许 - 以后都允许",
		permDesc, details)

	return &opencode.Question{
		ID:           props.ID,
		SessionID:    props.SessionID,
		Text:         fmt.Sprintf("%s\\n%s", permDesc, details),
		Options:      []string{"允许", "拒绝", "始终允许"},
		IsPermission: true,
		Directory:    props.Metadata.ParentDir,
	}, msg
}

// extractNormalQuestionJSON 提取普通问题
func extractNormalQuestionJSON(jsonData string) (*opencode.Question, string) {
	type jsonOptionItem struct {
		Label       string `json:"label"`
		Description string `json:"description"`
	}

	type jsonQuestionItem struct {
		Header   string           `json:"header"`
		Question string           `json:"question"`
		Multiple bool             `json:"multiple"`
		Options  []jsonOptionItem `json:"options"`
	}

	type QuestionProps struct {
		ID        string             `json:"id"`
		SessionID string             `json:"sessionID"`
		Questions []jsonQuestionItem `json:"questions"`
		Question  string             `json:"question"`
		Text      string             `json:"text"`
		Options   []string           `json:"options"`
	}

	var wrapper struct {
		Properties QuestionProps `json:"properties"`
	}
	if err := json.Unmarshal([]byte(jsonData), &wrapper); err != nil {
		return nil, ""
	}

	props := wrapper.Properties
	questionText := props.Question
	if questionText == "" {
		questionText = props.Text
	}

	if questionText == "" {
		return nil, ""
	}

	var msgBuilder strings.Builder
	msgBuilder.WriteString("❓ OpenCode 需要您的选择：\\n\\n")
	msgBuilder.WriteString(questionText)

	if len(props.Options) > 0 {
		msgBuilder.WriteString("\\n请选择：\\n")
		for i, opt := range props.Options {
			msgBuilder.WriteString(fmt.Sprintf("%d. %s\\n", i+1, opt))
		}
		msgBuilder.WriteString("\\n直接回复选项序号（如 `1`）或选项名称")
	} else {
		msgBuilder.WriteString("\\n回复 `yes` 确认，或 `no` 取消")
	}

	return &opencode.Question{
		ID:        props.ID,
		SessionID: props.SessionID,
		Text:      questionText,
		Options:   props.Options,
	}, msgBuilder.String()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
