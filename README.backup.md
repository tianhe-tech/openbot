# 🎉 OpenCode Gateway v2.0 - 双向通信版本

基于官方 [opencode-sdk-go](https://github.com/anomalyco/opencode-sdk-go) 的企业级消息网关

## ✨ 主要特性

- ✅ 使用官方 SDK 与 OpenCode Server 通信
- ✅ 支持双向数据交互（用户 ↔ OpenCode）
- ✅ 自动 Session 管理
- ✅ SSE 事件流监听
- ✅ 支持飞书和企业微信适配器
- ✅ 可扩展的适配器架构

## 🚀 快速开始

### 步骤 1: 克隆项目

```bash
git clone https://github.com/user/opencode-gateway.git
cd opencode-gateway
```

### 步骤 2: 安装依赖

```bash
go mod download
```

### 步骤 3: 配置环境变量

```bash
# Windows PowerShell
$env:OPENCODE_BASE_URL = "http://localhost:54321"
$env:OPENCODE_API_KEY = "your-api-key"

# 飞书配置
$env:FEISHU_APP_ID = "cli_xxx"
$env:FEISHU_APP_SECRET = "xxx"
$env:FEISHU_VERIFICATION_TOKEN = "xxx"

# 企业微信配置
$env:WECOM_CORP_ID = "xxx"
$env:WECOM_TOKEN = "xxx"
$env:WECOM_AES_KEY = "xxx"
```

### 步骤 4: 编译并运行

```bash
# 编译
go build -o bin/gateway.exe cmd/gateway/main.go

# 运行
.\bin\gateway.exe
```

## 📁 项目结构

```
opencode-gateway/
├── cmd/gateway/           # 主程序入口
├── internal/
│   ├── opencode/         # OpenCode SDK 封装
│   │   ├── client.go    # 客户端实现
│   │   └── event_listener.go  # 事件监听器
│   ├── adapters/         # 消息平台适配器
│   │   ├── base/        # 基础设施
│   │   ├── feishu/      # 飞书适配器
│   │   └── wecom/       # 企业微信适配器
│   ├── config/          # 配置管理
│   └── server/          # HTTP 服务器
├── examples/            # 示例代码
├── scripts/             # 构建脚本
├── bin/                # 编译输出
├── ARCHITECTURE.md     # 架构文档
├── UPGRADE_SUMMARY.md  # 升级总结
└── README.md          # 本文件
```

## 📚 文档

- **[ARCHITECTURE.md](ARCHITECTURE.md)** - 详细架构设计和数据流
- **[UPGRADE_SUMMARY.md](UPGRADE_SUMMARY.md)** - v2.0 升级内容和变更日志
- **[examples/README.md](examples/README.md)** - 示例代码和测试方法

## 🔄 工作原理

### 1. 用户发起对话（Adapter → OpenCode）

```
用户（飞书） → Webhook → Gateway → OpenCode Client → Session.Chat() → OpenCode Server
```

### 2. OpenCode 主动推送（OpenCode → Adapter）

```
OpenCode Server → SSE Event → Event Listener → Adapter Registry → Platform API → 用户
```

## 🛠️ 开发指南

### 添加新的消息平台

1. 创建适配器文件 `internal/adapters/xxx/xxx.go`
2. 实现 `MessageSender` 接口
3. 在 `main.go` 中注册适配器

示例：

```go
// 1. 实现 MessageSender
func (h *Handler) SendMessage(ctx context.Context, userID, content string) error {
    // 调用平台 API 发送消息
    return nil
}

// 2. 在 main.go中注册
xxxHandler := xxx.NewHandler(ocClient, cfg.XXX)
adapterRegistry.Register(xxxHandler.GetAdapter())
xxxHandler.Mount(mux)
```

## 📊 监控

### 健康检查

```bash
curl http://localhost:8080/healthz
```

### 日志输出

```
2026/01/29 13:18:00 opencode gateway ready on :8080 (bidirectional mode)
2026/01/29 13:18:00 adapters registered: wecom, feishu
2026/01/29 13:18:00 event listener: active
```

## 🧪 测试

### 运行测试

```bash
go test ./...
```

### 手动测试

```bash
# 测试飞书 Webhook
curl -X POST http://localhost:8080/feishu/callback \
  -H "Content-Type: application/json" \
  -d @test_payload.json
```

## 📦 依赖

- Go 1.24.3+
- [github.com/sst/opencode-sdk-go](https://github.com/anomalyco/opencode-sdk-go) v0.1.0

## 🤝 贡献

欢迎 Pull Request！

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/amazing`)
3. 提交改动 (`git commit -am 'Add amazing feature'`)
4. 推送分支 (`git push origin feature/amazing`)
5. 创建 Pull Request

## 📝 许可证

MIT License

## 📞 联系方式

- GitHub: https://github.com/user/opencode-gateway
- Issues: https://github.com/user/opencode-gateway/issues

---

**祝使用愉快！** 🚀

如有问题，请查阅 [ARCHITECTURE.md](ARCHITECTURE.md) 或提交 Issue。
