# OpenCode Gateway Examples

## SDK 基础使用示例

### 运行示例

```bash
# 确保 OpenCode Server 运行在 localhost:54321
cd examples
go run sdk_demo.go
```

### 输出示例

```
✓ 创建会话: ses_abc123
✓ 收到回复: msg_xyz789
✓ 会话包含 2 条消息
✓ 共有 5 个会话
✓ 开始监听事件...
  事件: {Type:session.created SessionID:ses_abc123 ...}
  事件: {Type:message.sent MessageID:msg_xyz789 ...}
✅ 示例完成
```

## 集成测试

### 测试飞书 Webhook

```bash
# 发送测试消息
curl -X POST http://localhost:8080/feishu/callback \
  -H "Content-Type: application/json" \
  -d '{
    "type": "event_callback",
    "event": {
      "sender": {
        "sender_id": {
          "open_id": "ou_test123"
        }
      },
      "message": {
        "message_id": "msg_test456",
        "message_type": "text",
        "chat_id": "oc_test789",
        "content": {
          "text": "你好，AI助手"
        }
      }
    }
  }'
```

### 预期响应

```json
{
  "msg_type": "text",
  "content": {
    "text": "你好！我是 AI 助手，很高兴为您服务。"
  },
  "trace": "ses_abc123",
  "session_id": "ses_abc123"
}
```

## 事件监听测试

### 模拟 OpenCode 事件推送

```go
// 在你的测试代码中
client.RegisterEventHandler(func(ctx context.Context, event *opencode.Event) error {
    log.Printf("收到事件: %+v", event)
    
    // 根据事件类型处理
    switch event.Type {
    case "message.completed":
        // 处理消息完成事件
        return handleMessageCompleted(event)
    case "file.changed":
        // 处理文件变更事件
        return handleFileChanged(event)
    }
    
    return nil
})
```

## 性能测试

### 并发消息测试

```bash
# 安装 Apache Bench
apt-get install apache2-utils  # Debian/Ubuntu
brew install httpd              # macOS

# 发送 100 个并发请求
ab -n 100 -c 10 -p test_payload.json -T application/json \
   http://localhost:8080/feishu/callback
```

### 预期性能指标

- **平均响应时间**: < 200ms
- **并发处理能力**: > 100 req/s
- **错误率**: < 1%

## 调试技巧

### 启用详细日志

```bash
export LOG_LEVEL=debug
go run cmd/gateway/main.go
```

### 查看 SDK 请求

```go
import "github.com/sst/opencode-sdk-go/option"

client := opencode.NewClient(
    option.WithBaseURL("http://localhost:54321"),
    option.WithDebugLog(nil), // 启用调试日志
)
```

### 检查事件流

```bash
# 直接连接到 OpenCode 事件流
curl -N http://localhost:54321/event/stream
```

## 故障排查

### 常见问题

1. **连接失败**
   - 检查 `OPENCODE_BASE_URL` 配置
   - 确认 OpenCode Server 运行中
   
2. **会话创建失败**
   - 检查工作目录权限
   - 确认 `directory` 参数正确

3. **事件监听无响应**
   - 检查网络连接
   - 确认事件处理器已注册
   
4. **Webhook 验证失败**
   - 检查 verification token
   - 确认签名算法正确

## 下一步

- 阅读 [ARCHITECTURE.md](../ARCHITECTURE.md) 了解详细架构
- 查看 [opencode-sdk-go 文档](https://github.com/anomalyco/opencode-sdk-go)
- 加入社区讨论
