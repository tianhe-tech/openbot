# OpenCode Gateway

Enterprise messaging platform gateway for OpenCode AI assistant integration.

## 🚀 Features

- **Multi-Platform Support**
  - ✅ WeCom (企业微信)
  - ✅ Feishu (飞书)
  - ✅ DingTalk (钉钉) - **Stream Mode + Webhook Mode**

- **Bidirectional Communication**
  - Message forwarding: Platform → OpenCode
  - Event routing: OpenCode → Platform
  - Session management and user mapping

- **DingTalk Stream Mode** (New!)
  - No public IP required
  - Real-time WebSocket connection
  - Auto-reconnect mechanism
  - Easier deployment for internal networks

## 📋 Quick Start

### 1. Prerequisites

- Go 1.24.3 or later
- OpenCode Server running (default: http://localhost:3000)
- Platform credentials (see setup guides below)

### 2. Build

```bash
go build -o bin/gateway.exe cmd/gateway/main.go
```

### 3. Configure

#### DingTalk Stream Mode (Recommended)
```powershell
$env:DINGTALK_CLIENT_ID = "your-client-id"
$env:DINGTALK_CLIENT_SECRET = "your-client-secret"
$env:DINGTALK_USE_STREAM = "true"
```

#### DingTalk Webhook Mode (Legacy)
```powershell
$env:DINGTALK_APP_KEY = "your-app-key"
$env:DINGTALK_APP_SECRET = "your-app-secret"
```

#### OpenCode
```powershell
$env:OPENCODE_ENDPOINT = "http://localhost:3000"
$env:OPENCODE_API_KEY = "your-api-key"
```

### 4. Run

```bash
.\bin\gateway.exe
```

Expected output:
```
opencode event listener started
dingtalk: Stream mode client started
opencode gateway ready on :8080 (bidirectional mode)
adapters registered: [wecom feishu dingtalk (stream)]
event listener: active
```

## 📖 Documentation

| Document | Description |
|----------|-------------|
| **[DINGTALK_SETUP.md](DINGTALK_SETUP.md)** | **DingTalk Stream + Webhook setup guide** ⭐ |
| [API_TEST_GUIDE.md](API_TEST_GUIDE.md) | API testing guide with examples |
| [API_TEST_SUMMARY.md](API_TEST_SUMMARY.md) | Test results and quick reference |

## 🧪 Testing

### Quick Test All Adapters
```powershell
.\scripts\test_all.ps1
```

### Detailed I/O Demo
```powershell
.\scripts\demo_io.ps1
```

### Interactive Testing
```powershell
.\scripts\test_interactive.ps1 -Adapter dingtalk
```

### DingTalk Configuration Check
```powershell
.\scripts\test_dingtalk_config.ps1
```

## 🏗️ Architecture

```
┌─────────────┐
│   DingTalk  │ (Stream WebSocket)
│   Feishu    │ (Webhook)
│   WeCom     │ (Webhook)
└──────┬──────┘
       │
       ▼
┌─────────────────────────────┐
│    Gateway (Adapters)       │
│  - DingTalk Stream Client   │
│  - Webhook Endpoints        │
│  - Session Management       │
└──────────┬──────────────────┘
           │
           ▼ (opencode-sdk-go)
┌─────────────────────────────┐
│    OpenCode Server          │
│    http://localhost:3000    │
└─────────────────────────────┘
```

## 🌟 What's New

### v0.2.0 - DingTalk Stream Mode
- ✨ Added DingTalk Stream mode support (github.com/open-dingtalk/dingtalk-stream-sdk-go)
- ✨ No public IP required for DingTalk deployment
- ✨ Real-time WebSocket connection with auto-reconnect
- 🔧 Backward compatible with webhook mode
- 📚 Comprehensive documentation and test scripts

## 📊 API Endpoints

| Platform | Endpoint | Method | Mode |
|----------|----------|--------|------|
| DingTalk | N/A | WebSocket | Stream |
| DingTalk | `/dingtalk/callback` | POST | Webhook |
| Feishu | `/feishu/callback` | POST | Webhook |
| WeCom | `/wecom/callback` | GET/POST | Webhook |
| Health | `/healthz` | GET | All |

## 🔧 Configuration

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DINGTALK_CLIENT_ID` | ✅ Stream | DingTalk Client ID (for Stream mode) |
| `DINGTALK_CLIENT_SECRET` | ✅ Stream | DingTalk Client Secret (for Stream mode) |
| `DINGTALK_USE_STREAM` | ⚙️ | Set to "true" to enable Stream mode |
| `OPENCODE_ENDPOINT` | ✅ | OpenCode Server URL |
| `OPENCODE_API_KEY` | ✅ | OpenCode API Key |

See [DINGTALK_SETUP.md](DINGTALK_SETUP.md) for complete configuration guide.

## 🐛 Troubleshooting

### DingTalk Stream Mode

**Connection Failed:**
```
dingtalk stream error: connection failed
```
- Verify Client ID and Client Secret
- Ensure app is published in DingTalk console
- Check network connectivity

**No Messages Received:**
- Confirm bot is added to chat
- Check bot permissions in DingTalk console
- Review logs in DingTalk Open Platform

See [DINGTALK_SETUP.md](DINGTALK_SETUP.md) for detailed troubleshooting.

## 🔗 Dependencies

- `github.com/sst/opencode-sdk-go` - OpenCode official SDK
- `github.com/open-dingtalk/dingtalk-stream-sdk-go` v0.9.1 - DingTalk Stream SDK
- `github.com/gorilla/websocket` v1.5.0 - WebSocket support
- `github.com/google/uuid` v1.3.0 - UUID generation

## 📝 Project Structure

```
opencode-gateway/
├── cmd/gateway/main.go                    # Entry point
├── internal/
│   ├── adapters/
│   │   ├── dingtalk/dingtalk.go          # DingTalk (Stream + Webhook)
│   │   ├── feishu/feishu.go              # Feishu adapter
│   │   └── wecom/wecom.go                # WeCom adapter
│   ├── config/config.go                  # Configuration
│   └── opencode/                         # OpenCode client
├── scripts/                              # Test scripts
│   ├── test_dingtalk_config.ps1         # Config checker
│   ├── demo_io.ps1                      # I/O demo
│   └── test_interactive.ps1             # Interactive test
├── DINGTALK_SETUP.md                    # DingTalk setup guide ⭐
└── README.md                            # This file
```

## 📜 License

MIT License

## 🔗 Links

- [OpenCode](https://github.com/sst/opencode)
- [DingTalk Open Platform](https://open.dingtalk.com/)
- [DingTalk Stream SDK](https://github.com/open-dingtalk/dingtalk-stream-sdk-go)
- [DingTalk Stream Tutorial](https://opensource.dingtalk.com/developerpedia/docs/explore/tutorials/stream/bot/go/build-bot)

---

**For DingTalk integration, see [DINGTALK_SETUP.md](DINGTALK_SETUP.md) for detailed setup instructions.**
