//go:build event_stream_check

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	opencode "github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
)

func main() {
	endpoint := os.Getenv("OPENCODE_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:4096"
	}

	fmt.Printf("OpenCode 事件流测试\n")
	fmt.Printf("==================\n")
	fmt.Printf("Endpoint: %s\n", endpoint)
	fmt.Println()

	fmt.Println("1. 创建 OpenCode SDK 客户端...")
	client := opencode.NewClient(
		option.WithBaseURL(endpoint),
	)
	fmt.Println("   ✅ 客户端创建成功")
	fmt.Println()

	fmt.Println("2. 启动事件流监听...")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stream := client.Event.ListStreaming(ctx, opencode.EventListParams{})
	fmt.Println("   ✅ 事件流已连接")
	fmt.Println()

	fmt.Println("3. 监听事件（30秒）...")
	fmt.Println("   （在另一个终端发送消息以触发事件）")
	fmt.Println()

	eventCount := 0
	go func() {
		defer stream.Close()

		for stream.Next() {
			event := stream.Current()
			eventCount++

			eventType := string(event.Type)
			rawJSON := event.JSON.RawJSON()

			fmt.Printf("   [事件 %d] 类型: %s\n", eventCount, eventType)
			if len(rawJSON) > 0 && len(rawJSON) < 500 {
				fmt.Printf("   JSON: %s\n", rawJSON)
			} else if len(rawJSON) > 0 {
				fmt.Printf("   JSON (前500字符): %.500s...\n", rawJSON)
			}
			fmt.Println()
		}

		if err := stream.Err(); err != nil {
			log.Printf("   ❌ 事件流错误: %v\n", err)
		} else {
			log.Printf("   ✅ 事件流正常结束\n")
		}
	}()

	// 等待30秒
	<-ctx.Done()

	fmt.Println()
	fmt.Println("==================")
	fmt.Printf("监听完成！共收到 %d 个事件\n", eventCount)

	if eventCount == 0 {
		fmt.Println()
		fmt.Println("⚠️  没有收到任何事件，可能的原因：")
		fmt.Println("   1. OpenCode server 没有运行")
		fmt.Println("   2. 没有活动的 session 产生事件")
		fmt.Println("   3. 事件流连接有问题")
		fmt.Println()
		fmt.Println("💡 建议：")
		fmt.Println("   1. 确保 OpenCode server 正在运行")
		fmt.Println("   2. 在运行此测试时，在另一个终端发送消息到 OpenCode")
		fmt.Println("   3. 检查 OpenCode server 的日志")
	}
}
