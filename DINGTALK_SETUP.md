# 钉钉机器人接入指南

## 概述

Gateway 支持钉钉的两种接入模式：
1. **Stream 模式**（推荐）- 使用长连接，无需公网 IP
2. **Webhook 模式**（传统）- 需要公网可访问的回调地址

## Stream 模式接入（推荐）

### 优势
- ✅ 无需公网 IP 或域名
- ✅ 无需配置回调 URL
- ✅ 实时双向通信
- ✅ 更安全，支持内网部署
- ✅ 自动重连机制

### 步骤 1: 创建钉钉机器人应用

1. 访问[钉钉开放平台](https://open.dingtalk.com/)
2. 进入"应用开发" > "企业内部开发"
3. 创建"机器人"应用
4. 获取 **Client ID** 和 **Client Secret**

### 步骤 2: 配置 Stream 模式

在机器人应用设置中：
1. 开启 **Stream 模式**
2. 配置机器人权限：
   - 消息接收能力
   - 消息发送能力
3. 发布应用

### 步骤 3: 配置环境变量

```bash
# 钉钉 Stream 模式配置
export DINGTALK_CLIENT_ID="your-client-id"
export DINGTALK_CLIENT_SECRET="your-client-secret"
export DINGTALK_USE_STREAM="true"

# OpenCode 配置
export OPENCODE_ENDPOINT="http://localhost:3000"
export OPENCODE_API_KEY="your-api-key"
```

或者在 Windows PowerShell：
```powershell
$env:DINGTALK_CLIENT_ID="your-client-id"
$env:DINGTALK_CLIENT_SECRET="your-client-secret"
$env:DINGTALK_USE_STREAM="true"
$env:OPENCODE_ENDPOINT="http://localhost:3000"
$env:OPENCODE_API_KEY="your-api-key"
```

### 步骤 4: 启动 Gateway

```bash
./bin/gateway.exe
```

启动日志示例：
```
opencode event listener started
dingtalk: starting Stream mode connection...
dingtalk: Stream mode client started
opencode gateway ready on :8080 (bidirectional mode)
adapters registered: [wecom feishu dingtalk (stream)]
event listener: active
```

### 步骤 5: 测试机器人

1. 在钉钉中找到你的机器人
2. 发送消息给机器人
3. 查看 Gateway 日志：
```
dingtalk stream: received message from staff_xxx: Hello
dingtalk stream: mapped user staff_xxx to session ses_xxx
dingtalk stream: replied to user staff_xxx
```

---

## Webhook 模式接入（传统）

### 要求
- 需要公网可访问的服务器
- 需要配置回调 URL

### 步骤 1: 创建钉钉机器人应用

同 Stream 模式步骤 1

### 步骤 2: 配置 Webhook

1. 在机器人应用设置中配置回调 URL：
   ```
   https://your-domain.com/dingtalk/callback
   ```
2. 获取：
   - AppKey
   - AppSecret
   - Verification Token
   - Encrypt Key

### 步骤 3: 配置环境变量

```bash
# 钉钉 Webhook 模式配置
export DINGTALK_APP_KEY="your-app-key"
export DINGTALK_APP_SECRET="your-app-secret"
export DINGTALK_VERIFICATION_TOKEN="your-token"
export DINGTALK_ENCRYPT_KEY="your-encrypt-key"
export DINGTALK_USE_STREAM="false"  # 或不设置

# OpenCode 配置
export OPENCODE_ENDPOINT="http://localhost:3000"
export OPENCODE_API_KEY="your-api-key"
```

### 步骤 4: 启动 Gateway

```bash
./bin/gateway.exe
```

### 步骤 5: 测试 Webhook

```bash
curl -X POST http://your-domain.com/dingtalk/callback \
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

## 配置对比

| 配置项 | Stream 模式 | Webhook 模式 |
|--------|-------------|--------------|
| DINGTALK_CLIENT_ID | ✅ 必需 | ❌ |
| DINGTALK_CLIENT_SECRET | ✅ 必需 | ❌ |
| DINGTALK_USE_STREAM | ✅ 设为 "true" | ❌ 或 "false" |
| DINGTALK_APP_KEY | ❌ | ✅ 必需 |
| DINGTALK_APP_SECRET | ❌ | ✅ 必需 |
| DINGTALK_VERIFICATION_TOKEN | ❌ | ✅ 必需 |
| DINGTALK_ENCRYPT_KEY | ❌ | ✅ 可选 |
| 公网 IP | ❌ 不需要 | ✅ 需要 |
| 回调 URL | ❌ 不需要 | ✅ 需要配置 |

---

## 完整配置示例

### .env 文件示例（Stream 模式）

```bash
# Server
SERVER_ADDR=:8080

# OpenCode
OPENCODE_ENDPOINT=http://localhost:3000
OPENCODE_API_KEY=your-opencode-key

# DingTalk Stream Mode
DINGTALK_CLIENT_ID=dingxxxxxxxxxxxxxx
DINGTALK_CLIENT_SECRET=your-secret-here
DINGTALK_USE_STREAM=true

# Feishu (optional)
# FEISHU_APP_ID=cli_xxx
# FEISHU_APP_SECRET=xxx

# WeCom (optional)
# WECOM_CORP_ID=xxx
# WECOM_AGENT_ID=xxx
```

### PowerShell 启动脚本（Stream 模式）

```powershell
# set-dingtalk-env.ps1

# DingTalk Stream Configuration
$env:DINGTALK_CLIENT_ID = "dingxxxxxxxxxxxxxx"
$env:DINGTALK_CLIENT_SECRET = "your-client-secret-here"
$env:DINGTALK_USE_STREAM = "true"

# OpenCode Configuration
$env:OPENCODE_ENDPOINT = "http://localhost:3000"
$env:OPENCODE_API_KEY = "your-api-key"

# Start Gateway
Write-Host "Starting OpenCode Gateway with DingTalk Stream mode..." -ForegroundColor Green
.\bin\gateway.exe
```

使用方法：
```powershell
.\set-dingtalk-env.ps1
```

---

## 故障排除

### Stream 模式常见问题

#### 1. 连接失败
```
dingtalk stream error: connection failed
```

**解决方案：**
- 检查 Client ID 和 Client Secret 是否正确
- 确认应用已发布
- 检查网络连接

#### 2. 未收到消息
```
# 没有日志输出
```

**解决方案：**
- 确认机器人应用已添加到群聊或个人会话
- 检查机器人权限配置
- 查看钉钉开放平台的"消息推送"日志

#### 3. 回复失败
```
dingtalk stream: failed to reply: xxx
```

**解决方案：**
- 确认机器人有发送消息权限
- 检查 OpenCode Server 是否运行正常
- 查看完整错误信息

### Webhook 模式常见问题

#### 1. 回调 URL 无法访问
**解决方案：**
- 确认服务器有公网 IP
- 检查防火墙配置
- 测试 URL 可访问性

#### 2. 验证失败
```
invalid verification token
```

**解决方案：**
- 检查 DINGTALK_VERIFICATION_TOKEN 配置
- 对比钉钉后台的 Token 配置

---

## 功能对比

### Stream 模式支持
- ✅ 接收用户消息
- ✅ 回复用户消息
- ✅ 会话管理
- ✅ 与 OpenCode 交互
- 🚧 主动推送消息（开发中）

### Webhook 模式支持
- ✅ 接收用户消息
- ✅ 回复用户消息
- ✅ 会话管理
- ✅ 与 OpenCode 交互
- ❌ 主动推送消息（需额外 API）

---

## 数据流向

### Stream 模式
```
钉钉用户
  ↓ (消息)
钉钉 Stream 服务器
  ↓ (WebSocket)
Gateway (dingtalk-stream-sdk-go)
  ↓ (onChatBotMessageReceived)
OpenCode Client
  ↓ (Session.Chat)
OpenCode Server
  ↓ (AI 响应)
Gateway
  ↓ (SimpleReplyText)
钉钉 Stream 服务器
  ↓
钉钉用户
```

### Webhook 模式
```
钉钉用户
  ↓ (消息)
钉钉服务器
  ↓ (HTTP POST)
Gateway /dingtalk/callback
  ↓
OpenCode Client
  ↓
OpenCode Server
  ↓ (AI 响应)
Gateway
  ↓ (HTTP Response)
钉钉服务器
  ↓
钉钉用户
```

---

## 技术架构

### 依赖库
```go
github.com/open-dingtalk/dingtalk-stream-sdk-go v0.9.1
  ├── github.com/gorilla/websocket v1.5.0
  └── github.com/google/uuid v1.3.0
```

### 核心代码位置
- 适配器实现：`internal/adapters/dingtalk/dingtalk.go`
- 配置加载：`internal/config/config.go`
- 启动逻辑：`cmd/gateway/main.go`

---

## 相关链接

- [钉钉开放平台](https://open.dingtalk.com/)
- [Stream 模式文档](https://opensource.dingtalk.com/developerpedia/docs/explore/tutorials/stream/bot/go/build-bot)
- [钉钉 Go SDK](https://github.com/open-dingtalk/dingtalk-stream-sdk-go)
- [示例代码](https://github.com/open-dingtalk/dingtalk-tutorial-go)

---

## 下一步

### 开发中的功能
- [ ] 主动推送消息到用户
- [ ] 支持富文本消息
- [ ] 支持卡片消息
- [ ] 支持图片和文件
- [ ] 会话持久化

### 生产部署建议
1. 使用 Stream 模式（无需公网 IP）
2. 配置日志收集
3. 添加监控指标
4. 实现错误重试
5. 配置多实例负载均衡
