# 飞书机器人长连接消息测试

基于飞书官方Go SDK测试长连接方式的消息接收和发送。

## 错误代码说明

- `1000040346: app_id is invalid` - APP_ID无效，请使用从飞书开放平台获取的真实APP_ID

## 环境准备

### 1. 创建飞书应用

1. 访问 [飞书开放平台](https://open.feishu.cn/)
2. 进入"开发者后台" → "创建企业自建应用"
3. 填写应用名称并创建

### 2. 获取凭据

1. 进入应用详情页面
2. 左侧菜单选择"凭证与基础信息"
3. 复制以下信息：
   - `App ID` (格式: `cli_xxxxxxxxxxxxxx`)
   - `App Secret` (点击查看并复制)

### 3. 开启WebSocket模式

**重要：** 长连接需要启用WebSocket模式

1. 在应用详情页面，选择"事件订阅"
2. 点击下方"推送方式"或"WebSocket模式"
3. 如果未开通，点击"立即开通"
4. 订阅所需事件：
   - `im.message.receive_v1` - 接收消息

### 4. 配置权限

1. 选择"权限管理"
2. 搜索并开启以下权限：
   - `获取与发送单聊、群组消息` (im:message)
   - `接收消息` (im:message)
3. 点击"申请权限"并提交审核

### 5. 发布应用

1. 在"版本管理与发布"页面
2. 创建新版本并发布
3. 获取应用可用性（需要在飞书中使用）

## 运行步骤

### Linux/Mac
```bash
export FEISHU_APP_ID=cli_xxxxxxxxxxxxxx
export FEISHU_APP_SECRET=xxxxxxxxxxxxxxxxxxxxxxx
go run main.go
```

### Windows (PowerShell)
```powershell
$env:FEISHU_APP_ID="cli_xxxxxxxxxxxxxx"
$env:FEISHU_APP_SECRET="xxxxxxxxxxxxxxxxxxxxxxx"
go run main.go
# 或运行已编译的程序
.\larkbot-test.exe
```

### Windows (CMD)
```cmd
set FEISHU_APP_ID=cli_xxxxxxxxxxxxxx
set FEISHU_APP_SECRET=xxxxxxxxxxxxxxxxxxxxxxx
go run main.go
# 或运行已编译的程序
larkbot-test.exe
```

## 代码说明

```go
// OnP2MessageReceiveV1 - 接收消息 v2.0（推荐）
OnP2MessageReceiveV1(func(ctx context.Context, event *larkim.P2MessageReceiveV1) error {
    fmt.Printf("[ OnP2MessageReceiveV1 access ], data: %s\n", larkcore.Prettify(event))
    return nil
})

// OnCustomizedEvent - 接收消息 v1.0（兼容版本）
OnCustomizedEvent("message", func(ctx context.Context, event *larkevent.EventReq) error {
    fmt.Printf("[ OnCustomizedEvent access ], type: message, data: %s\n", string(event.Body))
    return nil
})
```

## 测试方式

1. 确保`WebSocket模式`已在飞书开放平台开通
2. 确保`im.message.receive_v1`事件已订阅
3. 启动程序后会看到调试日志
4. 在飞书中找到该应用并打开单聊对话框
5. 发送消息，观察控制台输出
6. 按 Ctrl+C 退出程序

## 常见问题

### 连接失败 `1000040346: app_id is invalid`
- 检查APP_ID是否正确，格式应为 `cli_xxxxxxxxxxxxxx`
- 确保使用的是"企业自建应用"的凭证
- 确认应用已发布并可用

### 注意事项

- APP_ID和APP_SECRET属于敏感信息，不要提交到代码仓库
- 长连接使用WebSocket，保持程序持续运行
- Debug级别日志会输出详细信息，生产环境建议改为Info级别

## 参考文档

- [Go SDK安装准备](https://open.feishu.cn/document/server-side-sdk/golang-sdk-guide/preparations)
- [WebSocket模式文档](https://open.feishu.cn/document/common-capabilities/event-subscription/event-subscription-modes/pusher-mode/websocket-mode/enable-websocket-mode)
- [SDK示例代码](https://github.com/larksuite/oapi-sdk-go/tree/v3_main/sample/ws)