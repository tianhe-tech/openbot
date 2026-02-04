# 飞书机器人接收消息排查指南

## 检查清单

### 1. 确认配置
```env
# 在 .env 文件中检查以下配置
FEISHU_APP_ID=cli_xxxxxxxxxxxxxx      # 必须以 cli_ 开头
FEISHU_APP_SECRET=xxxxxxxxxxxxxxx      # 从飞书开放平台复制的完整密钥
FEISHU_USE_WEBSOCKET=true              # 必须设置为 true
```

### 2. 检查飞书开放平台配置 ⭐ 最重要

#### 2.1 开启WebSocket模式
1. 访问 [飞书开放平台](https://open.feishu.cn/)
2. 进入你的应用 → 事件订阅
3. 找到 **"WebSocket模式"**
4. **确认已开通** (非常重要！)
5. 获取 WebSocket 连接地址（类似：`wss://open.feishu.cn/...`）

#### 2.2 订阅消息事件
在同一页面的 **"订阅事件"** 部分：
- 确保已订阅：`im.message.receive_v1`
- 点击 **"保存"**

#### 2.3 开通应用权限
进入 **权限管理** → **权限配置**：
- 搜索并开启：`获取与发送单聊、群组消息` (im:message)
- 搜索并开启：`接收消息` (im:message)
- 点击 **"申请权限"**

#### 2.4 发布应用
进入 **版本管理与发布**：
- 创建版本 → 发布

### 3. 启动程序并观察日志

```bash
# 方式1：直接运行gateway
go run ./cmd/gateway

# 方式2：运行编译的程序
./bin/gateway.exe
```

**预期看到的日志：**
```
feishu: starting WebSocket mode connection...
feishu: using AppID: cli_xxxxxx...
feishu: starting WebSocket client connection...
feishu: WebSocket client connected successfully  ← 这个很重要
```

### 4. 测试消息接收

1. 在飞书中搜索你的应用（机器人）
2. 添加机器人到联系人
3. 打开单聊对话框
4. 发送消息（例如："测试"）

**程序应该输出：**
```
feishu: received from ou_xxxxa4a1 (chat_type=p2p): 测试
feishu: processing msgId=om_xxxxxxxx
feishu: mapped user ou_xxxxa4a1 to session ses_xxxxxx
feishu: sending to ou_xxxxa4a1 (len=xx)
feishu: sent success
```

### 5. 常见问题排查

#### 问题1：程序启动后没有看到 "connected successfully"
```
原因：飞书WebSocket模式未开通
解决：在飞书开放平台开启WebSocket模式
```

#### 问题2：程序启动报错 "AppID and AppSecret are required"
```
原因：环境变量未设置
解决：
Windows PowerShell:
$env:FEISHU_APP_ID="cli_xxxxxxxxxxxxxx"
$env:FEISHU_APP_SECRET="xxxxxxxxxxxxxxxxxxxxxxx"
```

#### 问题3：连接成功但收不到消息
```
可能原因：
1. im.message.receive_v1 事件未订阅
2. 应用权限未开通
3. 应用未发布
4. 消息类型不支持（只支持私聊文本消息）
```

#### 问题4：消息收到但转发失败
```
原因：OpenCode server未启动或配置错误
解决：检查 OPENCODE_ENDPOINT 和 OPENCODE_API_KEY
```

### 6. 调试日志

如果想看更详细的日志，修改代码中的日志级别：
```go
larkws.WithLogLevel(larkcore.LogLevelDebug), // 改为Debug
```

### 7. 对比测试

先使用test目录下的程序测试飞书连接：
```bash
cd test
$env:FEISHU_APP_ID="cli_xxxxxxxxxxxxxx"
$env:FEISHU_APP_SECRET="xxxxxxxxxxxxxxxxxxxxxxx"
go run main.go
```

如果test程序能收到消息，说明飞书配置正确，问题在gateway集成。

### 8. 网络检查

确保能访问飞书API：
```
https://open.feishu.cn
```