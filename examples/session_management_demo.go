package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/user/opencode-gateway/internal/opencode"
)

// 这个示例演示如何使用改进的 session 管理功能
func main() {
	// 创建 OpenCode 客户端
	client := opencode.NewClient(
		"http://localhost:4096",
		"",
		opencode.WithTimeout(60*time.Second),
	)

	ctx := context.Background()
	threadID := "demo-thread-001"
	userID := "demo-user"

	fmt.Println("=== OpenCode Session 管理演示 ===\n")

	// 模拟多轮对话
	messages := []string{
		"你好，我想了解 Go 语言的 goroutine",
		"goroutine 和线程有什么区别？",
		"如何创建一个 goroutine？",
		"channel 是用来做什么的？",
		"能给我一个使用 channel 的例子吗？",
	}

	var currentSessionID string

	for i, msg := range messages {
		fmt.Printf("--- 消息 #%d ---\n", i+1)
		fmt.Printf("用户: %s\n", msg)

		resp, err := client.SendMessage(ctx, opencode.MessagePayload{
			Channel:  "demo",
			UserID:   userID,
			ThreadID: threadID,
			Content:  msg,
		})

		if err != nil {
			log.Printf("❌ 发送消息失败: %v\n", err)

			// 如果是超时错误，说明可能是上下文过长
			if isContextError(err) {
				fmt.Println("⚠️  检测到上下文错误，尝试重置 session...")
				client.ResetSession(threadID)

				// 重试
				resp, err = client.SendMessage(ctx, opencode.MessagePayload{
					Channel:  "demo",
					UserID:   userID,
					ThreadID: threadID,
					Content:  msg,
				})

				if err != nil {
					log.Printf("❌ 重试失败: %v\n", err)
					continue
				}
			} else {
				continue
			}
		}

		// 检查 session 是否发生变化
		if currentSessionID != "" && currentSessionID != resp.SessionID {
			fmt.Printf("🔄 Session 已更新: %s -> %s\n", currentSessionID[:8], resp.SessionID[:8])
		}
		currentSessionID = resp.SessionID

		// 获取当前消息计数
		count := client.GetMessageCount(resp.SessionID)

		fmt.Printf("OpenCode: %s\n", truncateResponse(resp.Reply, 100))
		fmt.Printf("📊 Session: %s | 消息计数: %d/%d\n",
			resp.SessionID[:8], count, opencode.MaxMessagesPerSession)

		// 提示用户 session 状态
		if count >= opencode.MessageThresholdForSummary {
			fmt.Printf("💡 提示: 已达到 %d 条消息，系统正在后台总结...\n",
				opencode.MessageThresholdForSummary)
		}

		if count >= opencode.MaxMessagesPerSession-5 {
			fmt.Printf("⚠️  警告: 接近消息上限（%d/%d），即将创建新 session\n",
				count, opencode.MaxMessagesPerSession)
		}

		fmt.Println()
		time.Sleep(2 * time.Second)
	}

	fmt.Println("=== 演示结束 ===")
	fmt.Println("\n💡 提示:")
	fmt.Printf("- 当消息数达到 %d 时，系统会开始后台总结\n", opencode.MessageThresholdForSummary)
	fmt.Printf("- 当消息数超过 %d 时，系统会自动创建新 session\n", opencode.MaxMessagesPerSession)
	fmt.Println("- 新 session 会携带旧 session 的总结作为上下文")
	fmt.Println("- 这样可以避免 'context deadline exceeded' 错误")
}

// isContextError 检查是否是上下文相关的错误
func isContextError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return containsAny(errMsg, []string{
		"context deadline exceeded",
		"context canceled",
		"timeout",
	})
}

// containsAny 检查字符串是否包含任意一个子串
func containsAny(s string, substrs []string) bool {
	for _, substr := range substrs {
		if len(s) >= len(substr) && contains(s, substr) {
			return true
		}
	}
	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && findSubstr(s, substr))
}

func findSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// truncateResponse 截断响应内容以便显示
func truncateResponse(s string, maxLen int) string {
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	return string(runes[:maxLen]) + "..."
}
