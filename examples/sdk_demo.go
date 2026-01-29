package main

import (
	"context"
	"log"

	"github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
)

// 演示如何使用 opencode-sdk-go 的基本功能
func main() {
	ctx := context.Background()

	// 1. 创建客户端
	client := opencode.NewClient(
		option.WithBaseURL("http://localhost:54321"),
	)

	// 2. 创建新会话
	session, err := client.Session.New(ctx, opencode.SessionNewParams{
		Directory: opencode.F("."),
		Title:     opencode.F("测试会话"),
	})
	if err != nil {
		log.Fatalf("创建会话失败: %v", err)
	}
	log.Printf("✓ 创建会话: %s", session.ID)

	// 3. 发送消息
	response, err := client.Session.Prompt(ctx, session.ID, opencode.SessionPromptParams{
		Parts: opencode.F([]opencode.SessionPromptParamsPartUnion{
			opencode.TextPartInputParam{
				Text: opencode.F("你好，请介绍一下你自己"),
				Type: opencode.F(opencode.TextPartInputTypeText),
			},
		}),
		Directory: opencode.F("."),
	})
	if err != nil {
		log.Fatalf("发送消息失败: %v", err)
	}
	log.Printf("✓ 收到回复: %s", response.ID)

	// 4. 获取会话历史
	messages, err := client.Session.Messages(ctx, session.ID, opencode.SessionMessagesParams{
		Directory: opencode.F("."),
	})
	if err != nil {
		log.Fatalf("获取消息失败: %v", err)
	}
	log.Printf("✓ 会话包含 %d 条消息", len(*messages))

	// 5. 列出所有会话
	sessions, err := client.Session.List(ctx, opencode.SessionListParams{
		Directory: opencode.F("."),
	})
	if err != nil {
		log.Fatalf("列出会话失败: %v", err)
	}
	log.Printf("✓ 共有 %d 个会话", len(sessions))

	// 6. 监听事件（SSE）
	log.Println("✓ 开始监听事件...")
	stream := client.Event.Stream(ctx, opencode.EventStreamParams{
		Directory: opencode.F("."),
	})
	defer stream.Close()

	// 读取前 5 个事件
	eventCount := 0
	for stream.Next() && eventCount < 5 {
		event := stream.Current()
		log.Printf("  事件: %+v", event)
		eventCount++
	}

	if err := stream.Err(); err != nil {
		log.Printf("事件流错误: %v", err)
	}

	log.Println("✅ 示例完成")
}
