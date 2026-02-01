package opencode

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestSessionManagement 测试session管理功能
func TestSessionManagement(t *testing.T) {
	// 注意：这个测试需要实际的OpenCode服务器
	// 运行前请设置正确的endpoint
	endpoint := "http://localhost:4096"

	client := NewClient(endpoint, "", WithTimeout(30*time.Second))

	ctx := context.Background()
	threadID := "test-thread-001"
	userID := "test-user-001"

	t.Run("MessageCountTracking", func(t *testing.T) {
		// 发送第一条消息
		resp, err := client.SendMessage(ctx, MessagePayload{
			Channel:  "test",
			UserID:   userID,
			ThreadID: threadID,
			Content:  "第一条测试消息",
		})

		if err != nil {
			t.Logf("发送消息失败（这是正常的，如果没有运行OpenCode服务器）: %v", err)
			t.Skip("跳过测试 - 需要运行OpenCode服务器")
			return
		}

		// 检查消息计数
		count := client.GetMessageCount(resp.SessionID)
		if count != 1 {
			t.Errorf("期望消息数为1，实际为%d", count)
		}

		t.Logf("Session ID: %s, Message Count: %d", resp.SessionID, count)
	})

	t.Run("AutoSessionRenewal", func(t *testing.T) {
		// 基于token使用率自动切换session
		t.Log("新策略：基于上下文使用率而不是固定消息数")
		t.Logf("当上下文使用率达到%.0f%%时，系统会开始后台总结", SummaryThreshold*100)
		t.Logf("当上下文使用率超过%.0f%%时，系统会自动创建新session", ContextUsageThreshold*100)
		t.Logf("默认最大上下文: %d tokens", DefaultMaxTokens)
	})
}

// TestSessionSummarization 测试session总结功能
func TestSessionSummarization(t *testing.T) {
	endpoint := "http://localhost:4096"
	client := NewClient(endpoint, "")

	ctx := context.Background()

	t.Run("SummarizeNonExistentSession", func(t *testing.T) {
		err := client.SummarizeSession(ctx, "non-existent-session")
		if err == nil {
			t.Error("期望总结不存在的session时返回错误")
		}
		t.Logf("正确处理不存在的session: %v", err)
	})
}

// TestContextManagement 演示如何处理上下文超限
func TestContextManagement(t *testing.T) {
	t.Log("=== 上下文管理策略 ===")
	t.Log("1. 每个session维护token计数器（估算）")
	t.Logf("2. 上下文使用率达到%.0f%%时开始后台总结", SummaryThreshold*100)
	t.Logf("3. 上下文使用率超过%.0f%%时自动创建新session", ContextUsageThreshold*100)
	t.Log("4. 新session的title包含旧session的总结信息")
	t.Log("5. 第一条消息会携带旧session的总结作为上下文")
	t.Log("=== Token估算策略 ===")
	t.Log("6. 中文字符按2 token计算")
	t.Log("7. 英文单词按1 token计算")
	t.Logf("8. 默认最大上下文: %d tokens", DefaultMaxTokens)
	t.Log("9. 自动获取模型实际上下文长度")
	t.Log("=== 错误处理 ===")
	t.Log("10. 如果总结失败，会记录日志但继续处理")
	t.Log("11. 如果创建新session失败，会继续使用当前session")
}

// Example_usage 演示如何使用改进后的client
func Example_usage() {
	// 创建client
	client := NewClient(
		"http://localhost:4096",
		"",
		WithTimeout(60*time.Second),
	)

	ctx := context.Background()
	threadID := "user-thread-123"

	// 发送消息 - client会自动管理session
	for i := 1; i <= 55; i++ {
		resp, err := client.SendMessage(ctx, MessagePayload{
			Channel:  "dingtalk",
			UserID:   "user-001",
			ThreadID: threadID,
			Content:  fmt.Sprintf("消息 #%d", i),
		})

		if err != nil {
			fmt.Printf("发送消息失败: %v\n", err)
			continue
		}

		count := client.GetMessageCount(resp.SessionID)
		fmt.Printf("消息 #%d 已发送，Session: %s, 计数: %d\n", i, resp.SessionID, count)

		// 当达到30条消息时，会开始后台总结
		// 当达到50条消息时，会创建新session
	}
}

// BenchmarkMessageSending 性能基准测试
func BenchmarkMessageSending(b *testing.B) {
	// 这个测试需要实际的OpenCode服务器
	b.Skip("需要运行OpenCode服务器")

	endpoint := "http://localhost:4096"
	client := NewClient(endpoint, "")
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = client.SendMessage(ctx, MessagePayload{
			Channel:  "test",
			UserID:   "bench-user",
			ThreadID: fmt.Sprintf("bench-thread-%d", i),
			Content:  "benchmark message",
		})
	}
}
