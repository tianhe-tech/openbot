//go:build opencode_revert_check

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	opencode "github.com/sst/opencode-sdk-go"
	"github.com/sst/opencode-sdk-go/option"
)

func main() {
	endpoint := os.Getenv("OPENCODE_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:4096"
	}

	fmt.Printf("OpenCode Revert 文件修改撤销测试\n")
	fmt.Printf("================================\n")
	fmt.Printf("Endpoint: %s\n", endpoint)
	fmt.Println()

	client := opencode.NewClient(
		option.WithBaseURL(endpoint),
	)

	ctx := context.Background()
	filePath := "E:\\Study\\test_revert_demo.txt"

	// 0. 预先创建一个文件
	fmt.Println("0. 预先创建一个文件...")
	originalContent := "Original content - Version 0\nLine 2\nLine 3"
	err := os.WriteFile(filePath, []byte(originalContent), 0644)
	if err != nil {
		fmt.Printf("   创建文件失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("   文件创建成功，原始内容:\n   %s\n", originalContent)
	fmt.Println()

	// 1. 创建新 session
	fmt.Println("1. 创建新 session...")
	session, err := client.Session.New(ctx, opencode.SessionNewParams{
		Title: opencode.F("revert-file-modify-test"),
	})
	if err != nil {
		fmt.Printf("   创建session失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("   Session创建成功 (ID: %s)\n", session.ID)
	fmt.Println()

	// 2. 让 AI 修改文件
	fmt.Println("2. 让 AI 修改文件...")
	parts1 := []opencode.SessionPromptParamsPartUnion{
		opencode.TextPartInputParam{
			Text: opencode.F("请修改 test_revert_demo.txt 文件，将内容改为 'MODIFIED CONTENT - Version 1'"),
			Type: opencode.F(opencode.TextPartInputTypeText),
		},
	}

	promptCtx1, cancel1 := context.WithTimeout(ctx, 120*time.Second)
	resp1, err := client.Session.Prompt(promptCtx1, session.ID, opencode.SessionPromptParams{
		Parts: opencode.F(parts1),
	})
	cancel1()
	if err != nil {
		fmt.Printf("   发送消息失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("   消息发送成功 (MessageID: %s)\n", resp1.Info.ID)
	printResponse(resp1)
	fmt.Println()

	// 3. 检查文件是否被修改
	fmt.Println("3. 检查文件是否被修改...")
	content1, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Printf("   读取文件失败: %v\n", err)
	} else {
		fmt.Printf("   文件内容:\n   %s\n", string(content1))
	}
	fmt.Println()

	// 4. 获取消息列表和 session 状态
	fmt.Println("4. 获取消息列表和 session 状态...")
	messages, err := client.Session.Messages(ctx, session.ID, opencode.SessionMessagesParams{})
	if err != nil {
		fmt.Printf("   获取消息列表失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("   消息数量: %d\n", len(*messages))

	sessionInfo, err := client.Session.Get(ctx, session.ID, opencode.SessionGetParams{})
	if err != nil {
		fmt.Printf("   获取session失败: %v\n", err)
	} else {
		fmt.Printf("   Session summary: %d diffs\n", len(sessionInfo.Summary.Diffs))
	}
	fmt.Println()

	// 5. 检查 write part 是否有 snapshot
	fmt.Println("5. 检查 write part 详情...")
	var writePartID string
	var writeMsgID string
	for _, msg := range *messages {
		if msg.Info.Role == "assistant" {
			for _, part := range msg.Parts {
				if part.Tool == "write" {
					stateMap := make(map[string]interface{})
					stateJSON, _ := json.Marshal(part.State)
					json.Unmarshal(stateJSON, &stateMap)

					if status, ok := stateMap["status"]; ok && status == "completed" {
						writePartID = part.ID
						writeMsgID = msg.Info.ID
						fmt.Printf("   找到 write part: ID=%s, MsgID=%s\n", writePartID, writeMsgID)
						fmt.Printf("   Snapshot: %s\n", part.Snapshot)
					}
				}
			}
		}
	}
	fmt.Println()

	// 6. 执行 revert - 尝试使用 partID
	fmt.Println("6. 执行 revert...")

	// 找到 user 消息
	var userMsgID string
	for i := len(*messages) - 1; i >= 0; i-- {
		if (*messages)[i].Info.Role == "user" {
			userMsgID = (*messages)[i].Info.ID
			break
		}
	}

	fmt.Printf("   尝试 revert messageID=%s, partID=%s\n", userMsgID, writePartID)
	revertedSession, err := client.Session.Revert(ctx, session.ID, opencode.SessionRevertParams{
		MessageID: opencode.F(userMsgID),
		PartID:    opencode.F(writePartID),
	})
	if err != nil {
		fmt.Printf("   Revert失败: %v\n", err)
	} else {
		fmt.Printf("   Revert成功! Session Version: %s\n", revertedSession.Version)
		printSessionRevert(revertedSession)
	}
	fmt.Println()

	// 7. 检查文件是否恢复
	fmt.Println("7. 检查文件是否恢复原始内容...")
	content2, err := os.ReadFile(filePath)
	if err != nil {
		fmt.Printf("   读取文件失败: %v\n", err)
	} else {
		currentContent := string(content2)
		fmt.Printf("   当前内容:\n   %s\n", currentContent)
		if currentContent == originalContent {
			fmt.Printf("   ✅ 文件已恢复到原始内容!\n")
		} else if strings.Contains(currentContent, "MODIFIED") {
			fmt.Printf("   ❌ 文件仍然是修改后的内容\n")
		} else {
			fmt.Printf("   ⚠️ 文件内容有变化但不完全是原始内容\n")
		}
	}
	fmt.Println()

	// 8. 清理
	fmt.Println("8. 清理测试文件...")
	os.Remove(filePath)
	fmt.Println("   测试文件已删除")
	fmt.Println()

	fmt.Println("测试完成!")
}

func printResponse(resp *opencode.SessionPromptResponse) {
	for _, part := range resp.Parts {
		if part.Text != "" {
			text := part.Text
			if len(text) > 200 {
				text = text[:200] + "..."
			}
			text = strings.ReplaceAll(text, "\n", " ")
			fmt.Printf("   %s\n", text)
		}
	}
}

func printSessionRevert(session *opencode.Session) {
	data, err := json.MarshalIndent(session, "   ", "  ")
	if err != nil {
		return
	}

	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return
	}

	if revert, ok := m["revert"]; ok && revert != nil {
		revertJSON, _ := json.MarshalIndent(revert, "   ", "  ")
		fmt.Printf("   Revert 状态:\n   %s\n", string(revertJSON))
	} else {
		fmt.Printf("   Revert 状态: (空)\n")
	}
}
