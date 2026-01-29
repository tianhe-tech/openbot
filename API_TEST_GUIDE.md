# OpenCode Gateway API 测试指南

## 快速开始

### 1. 启动 OpenCode Server
```bash
# 在另一个终端运行 OpenCode
opencode server
# 默认运行在 http://localhost:3000
```

### 2. 启动 Gateway
```powershell
# 编译
go build -o bin/gateway.exe cmd/gateway/main.go

# 运行
.\bin\gateway.exe
# 默认运行在 http://localhost:8080
```

## 测试脚本

### 📋 test_all.ps1 - 快速功能测试
简单的通过/失败测试，验证所有适配器是否正常工作。

```powershell
.\scripts\test_all.ps1
```

**输出示例：**
```
✅ Health check passed
✅ Feishu message sent
✅ WeCom message sent  
✅ DingTalk message sent
```

---

### 🔍 test_api_detailed.ps1 - 详细 API 测试
显示完整的请求和响应 JSON，适合调试和查看数据流。

```powershell
.\scripts\test_api_detailed.ps1
```

**输出示例：**
```
[Feishu Adapter]
Endpoint: POST http://localhost:8080/feishu/callback

>>> REQUEST PAYLOAD >>>
{
  "schema": "2.0",
  "token": "test_token_feishu",
  "type": "im.message.receive_v1",
  "event": {
    "sender": {
      "sender_id": {
        "open_id": "ou_test_feishu_user_001"
      }
    },
    "message": {
      "message_type": "text",
      "content": "{\"text\": \"你好\"}"
    }
  }
}

<<< RESPONSE (Status: 200) <<<
{
  "msg_type": "text",
  "content": {
    "text": "Hello! How can I help you today?"
  },
  "session_id": "ses_3f7b4d62dffeT8B5JGPJfdf93b",
  "trace": "ses_3f7b4d62dffeT8B5JGPJfdf93b"
}

=== Key Information ===
AI Reply: Hello! How can I help you today?
Session ID: ses_3f7b4d62dffeT8B5JGPJfdf93b
✅ SUCCESS
```

---

### 💬 test_interactive.ps1 - 交互式测试
实时对话模式，可以手动输入消息并立即看到 AI 响应。

```powershell
.\scripts\test_interactive.ps1

# 或指定适配器
.\scripts\test_interactive.ps1 -Adapter feishu
.\scripts\test_interactive.ps1 -Adapter wecom
.\scripts\test_interactive.ps1 -Adapter dingtalk
```

**使用方法：**
```
========================================
  Interactive Gateway API Tester
========================================

Current Adapter: FEISHU
Endpoint: http://localhost:8080/feishu/callback

Type your message and press Enter to send.
Type 'exit' or 'quit' to stop.
Type 'switch' to change adapter.

[feishu] > Hello, what is REST API?

[14:23:45] YOU > Hello, what is REST API?
[14:23:45] REQUEST > Sending to /feishu/callback...
[14:23:46] RESPONSE < Session: ses_abc123
[14:23:46] AI > REST API (Representational State Transfer) is an 
architectural style for building web services...

[feishu] > 
```

**交互命令：**
- 直接输入文本 - 发送消息到 OpenCode
- `switch` - 切换适配器（Feishu/WeCom/DingTalk）
- `history` - 查看所有会话 ID
- `help` - 显示帮助
- `exit` 或 `quit` - 退出

---

## API 端点说明

### Feishu (飞书)
**端点：** `POST /feishu/callback`

**请求格式：**
```json
{
  "schema": "2.0",
  "type": "im.message.receive_v1",
  "event": {
    "sender": {
      "sender_id": {
        "open_id": "ou_xxx"
      }
    },
    "message": {
      "message_id": "om_xxx",
      "message_type": "text",
      "content": "{\"text\": \"你的消息\"}"
    }
  }
}
```

**响应格式：**
```json
{
  "msg_type": "text",
  "content": {
    "text": "AI 响应内容"
  },
  "session_id": "ses_xxx",
  "trace": "ses_xxx"
}
```

---

### WeCom (企业微信)
**端点：** `POST /wecom/callback`

**请求格式：**
```json
{
  "msgtype": "text",
  "from_userid": "user_xxx",
  "text": {
    "content": "你的消息"
  }
}
```

**响应格式：**
```json
{
  "reply": "AI 响应内容",
  "session_id": "ses_xxx",
  "trace": "ses_xxx"
}
```

---

### DingTalk (钉钉)
**端点：** `POST /dingtalk/callback`

**请求格式：**
```json
{
  "msgtype": "text",
  "conversationType": "1",
  "conversationId": "cid_xxx",
  "senderStaffId": "staff_xxx",
  "text": {
    "content": "你的消息"
  }
}
```

**响应格式：**
```json
{
  "msgtype": "text",
  "text": {
    "content": "AI 响应内容"
  },
  "session_id": "ses_xxx",
  "trace": "ses_xxx"
}
```

---

## 使用 curl 测试

### Feishu
```bash
curl -X POST http://localhost:8080/feishu/callback \
  -H "Content-Type: application/json" \
  -d '{
    "schema": "2.0",
    "type": "im.message.receive_v1",
    "event": {
      "sender": {
        "sender_id": {"open_id": "ou_test"}
      },
      "message": {
        "message_id": "om_123",
        "message_type": "text",
        "content": "{\"text\": \"Hello\"}"
      }
    }
  }'
```

### WeCom
```bash
curl -X POST http://localhost:8080/wecom/callback \
  -H "Content-Type: application/json" \
  -d '{
    "msgtype": "text",
    "from_userid": "user_test",
    "text": {"content": "Hello"}
  }'
```

### DingTalk
```bash
curl -X POST http://localhost:8080/dingtalk/callback \
  -H "Content-Type: application/json" \
  -d '{
    "msgtype": "text",
    "conversationType": "1",
    "conversationId": "cid_test",
    "senderStaffId": "staff_test",
    "text": {"content": "Hello"}
  }'
```

---

## PowerShell 直接测试

```powershell
# Feishu
$body = @{
    schema = "2.0"
    type = "im.message.receive_v1"
    event = @{
        sender = @{
            sender_id = @{ open_id = "ou_test" }
        }
        message = @{
            message_id = "om_123"
            message_type = "text"
            content = '{"text": "Hello"}'
        }
    }
} | ConvertTo-Json -Depth 10

Invoke-RestMethod -Uri "http://localhost:8080/feishu/callback" `
  -Method Post -Body $body -ContentType "application/json"
```

---

## 验证检查清单

在生产环境部署前，请确保：

- [ ] ✅ 健康检查通过 (`GET /healthz`)
- [ ] ✅ Feishu 适配器正常响应
- [ ] ✅ WeCom 适配器正常响应
- [ ] ✅ DingTalk 适配器正常响应
- [ ] ✅ Session ID 正确生成
- [ ] ✅ OpenCode Server 连接正常
- [ ] ✅ 事件监听器运行中
- [ ] ✅ 日志输出正常

---

## 故障排除

### Gateway 无法启动
```powershell
# 检查端口占用
netstat -ano | findstr :8080

# 查看进程
Get-Process | Where-Object { $_.ProcessName -eq "gateway" }
```

### OpenCode Server 连接失败
```powershell
# 测试 OpenCode Server
curl http://localhost:3000/healthz

# 检查 gateway 配置
$env:OPENCODE_BASE_URL = "http://localhost:3000"
```

### 查看详细日志
Gateway 启动时会输出：
```
opencode event listener started
opencode gateway ready on :8080 (bidirectional mode)
adapters registered: wecom, feishu, dingtalk
event listener: active
```

---

## 性能测试

使用 `test_api_detailed.ps1` 进行基准测试：

```powershell
# 测量响应时间
Measure-Command { .\scripts\test_api_detailed.ps1 }
```

---

## 下一步

1. **集成真实平台 API**
   - 实现 Feishu 消息推送 API
   - 实现 WeCom 消息推送 API
   - 实现 DingTalk 消息推送 API

2. **完善事件路由**
   - 实现双向消息推送
   - 处理 OpenCode Server 事件

3. **添加监控**
   - 请求计数
   - 响应时间
   - 错误率

4. **生产部署**
   - 配置 HTTPS
   - 添加认证
   - 设置限流
