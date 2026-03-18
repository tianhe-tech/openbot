//go:build opencode_check

package main

import (
	"context"
	"fmt"
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

	fmt.Printf("OpenCode 连接测试\n")
	fmt.Printf("================\n")
	fmt.Printf("Endpoint: %s\n", endpoint)
	fmt.Println()

	fmt.Println("1. 创建 OpenCode SDK 客户端...")
	client := opencode.NewClient(
		option.WithBaseURL(endpoint),
	)
	fmt.Println("   客户端创建成功")
	fmt.Println()

	fmt.Println("2. 检查 OpenCode 服务健康状态 (列出sessions)...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	sessions, err := client.Session.List(ctx, opencode.SessionListParams{})
	cancel()

	if err != nil {
		fmt.Printf("   健康检查失败: %v\n", err)
		fmt.Println()
		fmt.Println("可能的原因:")
		fmt.Println("   - OpenCode server 未启动")
		fmt.Println("   - 端点地址不正确")
		fmt.Println("   - 网络连接问题")
		os.Exit(1)
	}

	sessionCount := 0
	if sessions != nil {
		sessionCount = len(*sessions)
	}
	fmt.Printf("   OpenCode 服务正常运行 (发现 %d 个sessions)\n", sessionCount)
	fmt.Println()

	fmt.Println("3. 创建新session并发送测试消息...")
	createCtx, createCancel := context.WithTimeout(context.Background(), 10*time.Second)
	session, err := client.Session.New(createCtx, opencode.SessionNewParams{
		Title: opencode.F("test-session"),
	})
	createCancel()

	if err != nil {
		fmt.Printf("   创建session失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("   Session创建成功 (ID: %s)\n", session.ID)
	fmt.Println()

	fmt.Println("4. 发送prompt消息...")
	promptCtx, promptCancel := context.WithTimeout(context.Background(), 60*time.Second)

	parts := []opencode.SessionPromptParamsPartUnion{
		opencode.TextPartInputParam{
			Text: opencode.F("你好，这是一个测试。请简单回复一个问候语。"),
			Type: opencode.F(opencode.TextPartInputTypeText),
		},
	}

	response, err := client.Session.Prompt(promptCtx, session.ID, opencode.SessionPromptParams{
		Parts: opencode.F(parts),
	})
	promptCancel()

	if err != nil {
		fmt.Printf("   发送prompt失败: %v\n", err)
		fmt.Println()
		fmt.Println("可能的原因:")
		fmt.Println("   - OpenCode server 内部错误")
		fmt.Println("   - 请求超时")
		fmt.Println("   - 模型配置问题")
		os.Exit(1)
	}

	fmt.Println("   Prompt发送成功")
	fmt.Printf("   Response ID: %s\n", response.Info.ID)
	fmt.Printf("   Parts数量: %d\n", len(response.Parts))
	fmt.Println()

	fmt.Println("5. 提取回复内容...")
	if len(response.Parts) == 0 {
		fmt.Println("   警告: 回复中没有parts")
		fmt.Println("   这通常意味着:")
		fmt.Println("      - OpenCode正在等待模型响应")
		fmt.Println("      - 需要使用事件流(SSE)获取实时响应")
		fmt.Println("      - 模型处理时间较长")
	} else {
		for i, part := range response.Parts {
			switch p := part.AsUnion().(type) {
			case opencode.TextPart:
				fmt.Printf("   Part %d (Text): %s\n", i+1, p.Text)
			default:
				fmt.Printf("   Part %d (Type: %T)\n", i+1, p)
			}
		}
	}
	fmt.Println()

	fmt.Println("6. 获取session详情...")
	detailCtx, detailCancel := context.WithTimeout(context.Background(), 10*time.Second)
	sessionDetail, err := client.Session.Get(detailCtx, session.ID, opencode.SessionGetParams{})
	detailCancel()

	if err != nil {
		fmt.Printf("   获取session详情失败: %v\n", err)
	} else {
		fmt.Printf("   Session详情获取成功\n")
		fmt.Printf("   Title: %s\n", sessionDetail.Title)
		fmt.Printf("   ID: %s\n", sessionDetail.ID)
	}
	fmt.Println()

	fmt.Println("================")
	fmt.Println("所有测试完成！")
	fmt.Println()
	fmt.Println("总结:")
	fmt.Printf("   - OpenCode服务: 正常 (%s)\n", endpoint)
	fmt.Printf("   - Session创建: 成功\n")
	fmt.Printf("   - Prompt发送: 成功\n")
	if len(response.Parts) == 0 {
		fmt.Printf("   - 响应内容: 空 (可能需要事件流监听)\n")
	} else {
		fmt.Printf("   - 响应内容: %d parts\n", len(response.Parts))
	}
}
