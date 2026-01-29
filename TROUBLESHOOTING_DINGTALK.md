# 钉钉机器人没有返回 - 问题诊断和解决方案

## 🔍 问题诊断

你遇到的问题：**已配置钉钉 Client ID 和 Secret，启动 gateway 后，与机器人对话没有返回**

## ❌ 原因分析

在你的配置文件 `internal/config/config.go` 中：

```go
DingTalk: dingtalk.Config{
    ClientID:     "dingk90c2agr1blmauzh",
    ClientSecret: "QRRH6n44_3QAW0HYobnr3znqhRPtkqk9Yd3KZSoGraUy9AqPLUif5tIh-vSqloNV",
    UseStream:    os.Getenv("DINGTALK_USE_STREAM") == "true",  // ❌ 问题在这里
    // ...
}
```

**问题：** `UseStream` 从环境变量读取，但环境变量没有设置，导致默认值为 `false`。

**结果：** Stream 模式没有启动，钉钉 SDK 没有连接到钉钉服务器，所以收不到消息。

## ✅ 解决方案

### 方案 1: 修改配置文件（已完成）

我已经修改了配置文件，直接启用 Stream 模式：

```go
DingTalk: dingtalk.Config{
    ClientID:     "dingk90c2agr1blmauzh",
    ClientSecret: "QRRH6n44_3QAW0HYobnr3znqhRPtkqk9Yd3KZSoGraUy9AqPLUif5tIh-vSqloNV",
    UseStream:    true,  // ✅ 直接设置为 true
    // ...
}
```

### 方案 2: 设置环境变量

或者你也可以设置环境变量：

```powershell
$env:DINGTALK_USE_STREAM = "true"
```

## 🚀 快速修复步骤

### 选项 A: 使用自动脚本（推荐）

```powershell
.\scripts\fix_and_start.ps1
```

这个脚本会：
1. 停止现有进程
2. 重新编译
3. 启动 Gateway

### 选项 B: 手动操作

1. **停止当前运行的 Gateway**
   ```powershell
   # 在运行 Gateway 的终端按 Ctrl+C
   # 或者
   Get-Process | Where-Object { $_.ProcessName -eq "gateway" } | Stop-Process -Force
   ```

2. **重新编译**
   ```powershell
   cd E:\Work\projects\gos\src\opencode-gateway
   go build -o bin\gateway.exe cmd\gateway\main.go
   ```

3. **启动 Gateway**
   ```powershell
   .\bin\gateway.exe
   ```

## 📋 验证步骤

启动后，你应该看到以下日志：

```
✅ opencode event listener started
✅ dingtalk: starting Stream mode connection...
✅ dingtalk: using ClientID: dingk90c2agr1blmauzh...
✅ dingtalk: starting Stream client connection...
✅ dingtalk: Stream mode client started (connecting in background)
✅ opencode gateway ready on :8080 (bidirectional mode)
✅ adapters registered: [wecom feishu dingtalk (stream)]  ← 注意这里显示 "stream"
✅ event listener: active
```

**关键日志：**
- `dingtalk: Stream mode client started` - Stream 客户端已启动
- `adapters registered: [... dingtalk (stream)]` - 钉钉使用 Stream 模式

如果连接成功，还会看到：
```
✅ dingtalk: Stream client connected successfully
```

## 🔧 测试机器人

1. **在钉钉中找到你的机器人**
2. **发送消息**：例如 "你好"
3. **查看 Gateway 日志**，应该看到：

```
dingtalk stream: received message from staff_xxx: 你好
dingtalk stream: mapped user staff_xxx to session ses_xxx
dingtalk stream: replied to user staff_xxx
```

4. **机器人应该回复消息**

## ❌ 常见问题排查

### 1. 没有看到 "Stream client connected successfully"

**可能原因：**
- Client ID 或 Secret 错误
- 网络连接问题
- 钉钉应用未发布或未启用

**排查方法：**
```powershell
.\scripts\debug_dingtalk.ps1
```

### 2. 看到 "dingtalk (webhook)" 而不是 "dingtalk (stream)"

**说明：** Stream 模式没有启用

**解决：** 检查配置文件中的 `UseStream` 是否为 `true`

### 3. 收到消息但没有回复

**查看日志：**
- 是否有 "received message from" 日志？
- 是否有错误信息？

**可能原因：**
- OpenCode Server 没有运行
- OpenCode Server 连接失败

**验证 OpenCode：**
```powershell
curl http://localhost:3000
```

### 4. 连接失败错误

```
dingtalk stream error: connection failed
```

**解决方法：**
1. 验证 Client ID 和 Secret 是否正确
2. 检查钉钉开放平台中应用是否已发布
3. 确认应用已启用 Stream 模式

## 📊 完整的数据流

```
钉钉用户发送消息
    ↓
钉钉 Stream 服务器
    ↓ (WebSocket 长连接)
Gateway - dingtalk Stream 客户端
    ↓ (onChatBotMessageReceived)
OpenCode Client
    ↓ (Session.Chat)
OpenCode Server (localhost:3000)
    ↓ (AI 处理)
Gateway
    ↓ (SimpleReplyText)
钉钉 Stream 服务器
    ↓
钉钉用户收到回复
```

## 🛠️ 调试工具

### 查看当前配置
```powershell
.\scripts\debug_dingtalk.ps1
```

### 查看日志（如果使用后台运行）
```powershell
# 如果 Gateway 在后台运行，可以查看日志文件
Get-Content .\gateway.log -Tail 50 -Wait
```

### 测试 OpenCode 连接
```powershell
curl http://localhost:3000
```

## ✅ 成功标志

当一切正常时，你应该：

1. **看到正确的启动日志** - 包含 "Stream client connected successfully"
2. **适配器显示 Stream 模式** - `dingtalk (stream)`
3. **发送消息后有日志** - "received message from"
4. **机器人有回复** - 在钉钉中收到消息

## 📞 需要帮助？

如果问题仍然存在，请提供以下信息：

1. **Gateway 启动日志**（完整输出）
2. **发送消息后的日志**
3. **钉钉开放平台的配置截图**
4. **是否看到任何错误信息**

---

**注意：** 修改配置后，一定要重新编译并重启 Gateway！
