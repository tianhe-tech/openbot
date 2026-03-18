//go:build dingtalk_aicard_adapter_test

package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/user/opencode-gateway/internal/adapters/dingtalk"
)

func main() {
	clientID := strings.TrimSpace(os.Getenv("DINGTALK_CLIENT_ID"))
	clientSecret := strings.TrimSpace(os.Getenv("DINGTALK_CLIENT_SECRET"))
	userID := strings.TrimSpace(os.Getenv("DINGTALK_OWNER_USERID"))

	if clientID == "" || clientSecret == "" || userID == "" {
		fmt.Println("请设置：DINGTALK_CLIENT_ID, DINGTALK_CLIENT_SECRET, DINGTALK_OWNER_USERID")
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	fmt.Println("🚀 开始测试钉钉 AI 卡片流式消息（Adapter 版本）...")
	fmt.Println()

	content := `# 📊 AI 卡片流式测试

## 特性支持
- ✅ **Markdown** 格式
- ✅ **代码块** 高亮
- ✅ **表格** 渲染
- ✅ **Emoji** 表情
- ✅ **流式** 打字机效果

## 代码示例

` + "```go" + `
package main

import "fmt"

func main() {
    fmt.Println("Hello, DingTalk AI Card!")
}
` + "```\n\n" + `

## 表格测试

| 功能 | 状态 | 说明 |
| --- | --- | --- |
| AI 卡片 | ✅ | 自动创建 |
| 流式更新 | ✅ | 打字机效果 |
| Markdown | ✅ | 完整支持 |
| 代码块 | ✅ | 语法高亮 |
| 表格 | ✅ | 自动格式化 |

---
测试时间：` + time.Now().Format("2006-01-02 15:04:05") + `
✅ 测试完成！`

	fmt.Println("📤 发送内容：")
	fmt.Println(content)
	fmt.Println()
	fmt.Println("🚀 开始流式发送...")

	err := dingtalk.SendStreamingAICard(ctx, clientID, clientSecret, userID, content)
	if err != nil {
		fmt.Printf("❌ 发送失败：%v\n", err)
		os.Exit(1)
	}

	fmt.Println()
	fmt.Println("✅ AI 卡片流式消息发送成功！")
	fmt.Println("📱 请在钉钉中查看卡片消息（应有打字机效果）")
}
