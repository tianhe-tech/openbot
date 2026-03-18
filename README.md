# OpenCode Gateway

Enterprise messaging platform gateway for OpenCode AI assistant integration.

## 🌟 Features

- **Multi-Platform Support**
  - ✅ WeCom (企业微信)
  - ✅ Feishu (飞书)
  - ✅ DingTalk (钉钉) - Stream Mode + Webhook Mode

- **Bidirectional Communication**
  - Message forwarding: Platform → OpenCode
  - Event routing: OpenCode → Platform
  - Session management and user mapping
  - Permission/Q&A auto-reply support

- **Smart Session Management**
  - Official session lifecycle: Reuses the same OpenCode session and relies on server-side compaction
  - Explicit new-dialog flow: `/new` and `/reset` ask whether to continue the current task or start fresh, and users reply `1` or `2`
  - Task continuation: The gateway uses official summarize/compaction plus fork to continue work in a new session
  - Session status visibility: Token estimates remain available for diagnostics and status views

- **Streaming Progress Updates** ⚡
  - Real-time progress notifications via SSE
  - Incremental content delivery for long responses
  - Prevents timeout issues for long-running tasks

- **Task Scheduler** 🗓️
  - Multi-session task execution
  - Priority-based task queue
  - Worker pool (10 concurrent workers by default)
  - REST API for task submission and monitoring

- **Cron Scheduler & `/crontask` Command** ⏰
  - Create scheduled tasks directly from chat
  - Full cron expression support (5-6 fields)
  - Task management: add, list, enable, disable, delete
  - Perfect for monitoring, reports, and reminders
  - Command format: `/crontask add "0 */30 * * * *" "任务名" "任务内容"`

- **DingTalk Stream Mode**
  - No public IP required
  - Real-time WebSocket connection
  - Auto-reconnect mechanism
  - Easier deployment for internal networks

- **Secure Proxy Tunnel (Key Differentiator)** 🔐
  - By default, openbot does not expose HTTP service ports (`HTTP_ENABLED=false`)
  - Uses centralized `tools/http-server` as transit hub
  - Uses `tools/client-proxy` to bridge OpenCode TUI traffic through a reusable `proxy_key`
  - openbot only needs outbound access to the designated network
  - Enables mobile bots (DingTalk/Feishu/WeCom) + OpenCode TUI in a relatively safer network model

---

## 🚀 Quick Start

### 1. Prerequisites

- Go 1.21 or later
- OpenCode Server running (default: `http://localhost:4096`)
- Platform credentials (DingTalk/Feishu/WeCom)

### 2. Build

```bash
go build -o bin/openbot cmd/gateway/main.go
```

### 3. Configure

```bash
# Copy example configuration
cp .env.example .env

# Edit .env with your settings
# Required: OPENCODE_ENDPOINT, DINGTALK_CLIENT_ID, DINGTALK_CLIENT_SECRET
```

#### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OPENCODE_ENDPOINT` | ❌ | http://127.0.0.1:4096 | OpenCode Server URL |
| `OPENCODE_API_KEY` | ❌* | empty | OpenCode API Key (Bearer/X-API-Key mode) |
| `OPENCODE_SERVER_PASSWORD` | ❌* | empty | OpenCode Server password (HTTP Basic Auth mode) |
| `OPENCODE_SERVER_USERNAME` | ❌ | `opencode` | Username for HTTP Basic Auth when `OPENCODE_SERVER_PASSWORD` is set |
| `HTTP_ENABLED` | ❌ | `false` | Enable built-in HTTP server |
| `LOG_LEVEL` | ❌ | `info` | Log level (`debug`/`info`/`warn`/`error`) |
| `SERVER_ADDR` | ❌ | `:8080` | Service listen address |
| `DINGTALK_USE_STREAM` | ❌ | `false` | Enable DingTalk Stream mode |
| `DINGTALK_CLIENT_ID` | ✅ Stream | DingTalk Client ID |
| `DINGTALK_CLIENT_SECRET` | ✅ Stream | DingTalk Client Secret |
| `DINGTALK_USER_WHITELIST` | ❌ | - | Ignored at startup (startup whitelist is owner only) |
| `DINGTALK_OWNER_USERID` | ✅ | - | Required owner user ID; auto-added to whitelist |

\* `OPENCODE_API_KEY` and `OPENCODE_SERVER_PASSWORD` set either one. If both are set, openbot prefers `OPENCODE_SERVER_PASSWORD` (Basic Auth).

### 4. Run

```bash
./bin/openbot
```

With explicit log level:

```bash
./bin/openbot --log-level warn
```

With JSON config file:

```bash
cat > gateway.json << 'EOF'
{
  "log_level": "error"
}
EOF

./bin/openbot --config gateway.json
```

### 5. Verify

```bash
# Health check
curl http://localhost:8080/healthz

# Task statistics
curl http://localhost:8080/api/tasks/stats
```

---

## 🔐 Secure Tunnel Mode (http-server + client-proxy)

This is the core architecture difference from openclawd in this project: openbot does not require exposing OpenCode `4096` to the public network. It uses a centralized HTTP/WebSocket gateway for traffic relay.

### Components

- `tools/http-server`: centralized relay server (publicly reachable)
- `cmd/gateway` (openbot): runs near OpenCode and actively connects to relay
- `tools/client-proxy`: local TCP bridge for OpenCode TUI / attach

### Typical flow

1. Start `tools/http-server` on the central node.
2. Start openbot with `PROXY_HUB_WS_URL`; openbot reads existing `proxy_key` from `.opencode-gateway-proxy.json` (or generates one if missing).
3. Start `tools/client-proxy` locally with the same `proxy_key`.
4. Connect OpenCode TUI/attach to local proxy address (for example `127.0.0.1:14096`), and traffic is relayed to remote OpenCode.

### Quick run

```bash
# 1) central relay server
go run ./tools/http-server -addr :18080

# 2) openbot side (same host/network as OpenCode)
PROXY_HUB_WS_URL=http://<gateway-host>:18080 go run ./cmd/gateway

# 3) local client bridge
go run ./tools/client-proxy \
  -hub ws://<gateway-host>:18080/ws \
  -proxy-key <proxy_key_from_.opencode-gateway-proxy.json> \
  -listen 127.0.0.1:14096
```

For full details, see [docs/PROXY_TUNNEL.md](docs/PROXY_TUNNEL.md).

---

## 📖 Documentation

| Document | Description |
|----------|-------------|
| **[API.md](docs/API.md)** | Complete API reference |
| **[DEPLOY.md](docs/DEPLOY.md)** | Deployment and usage guide |
| **[CONFIGURATION.md](docs/CONFIGURATION.md)** | Configuration reference |
| **[QUICK_START_CRONTASK.md](docs/QUICK_START_CRONTASK.md)** | Cron task quick start |
| **[CRONTASK_COMMAND.md](docs/CRONTASK_COMMAND.md)** | Cron command reference |
| **[SCHEDULER_GUIDE.md](docs/SCHEDULER_GUIDE.md)** | Task scheduler guide |
| **[PROXY_TUNNEL.md](docs/PROXY_TUNNEL.md)** | 远程 OpenCode 反向代理隧道（无需暴露4096端口） |
| **[ARCHITECTURE.md](docs/ARCHITECTURE.md)** | System architecture |
| **[DOCKER.md](DOCKER.md)** | Docker setup guide |

---

## 🧪 Usage Examples

### Basic Message

In DingTalk/Feishu/WeCom:

```
帮我写一个 Python 爬虫
```

### Using Agent/Skill

```
@build 帮我编译这个项目
```

Available agents:
- `@build` - Build mode
- `@plan` - Planning mode
- `@chat` - Conversation mode

### Cron Task Commands

| Command | Description |
|---------|-------------|
| `/crontask add "cron" "name" "content" [agent]` | Add scheduled task |
| `/crontask list` | List all tasks |
| `/crontask enable <id>` | Enable a task |
| `/crontask disable <id>` | Disable a task |
| `/crontask delete <id>` | Delete a task |
| `/crontask info <id>` | Show task details |
| `/crontask help` | Show help |

**Examples:**
```bash
# Daily report at 9 AM
/crontask add "0 0 9 * * *" "日报" "生成每日日报"

# Every 30 minutes
/crontask add "0 */30 * * * *" "监控" "检查系统状态"

# Workday noon (Mon-Fri)
/crontask add "0 0 12 * * 1-5" "午间检查" "查看日志"
```

### Other Commands

| Command | Description |
|---------|-------------|
| `/skills` | List available agents |
| `/whitelist list` | List whitelist users |
| `/whitelist add <userID...>` | Add whitelist users at runtime |
| `/whitelist del <userID...>` | Remove whitelist users at runtime |
| `/help` | Show help information |
| `/abort` | Abort current task |
| `/cmd <command>` | Execute shell command |

---

## 🏗️ Architecture

```
┌─────────────┐
│  DingTalk   │ (Stream WebSocket + /crontask)
│   Feishu    │ (Webhook)
│    WeCom    │ (Webhook)
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────┐
│        Gateway (Adapters)               │
│  - DingTalk Stream Client               │
│  - Webhook Endpoints                    │
│  - Session Management                   │
│  - Task Scheduler (Worker Pool)         │
│  - Cron Scheduler (robfig/cron)         │
│  - Event Handling & Permission Reply    │
└──────────┬──────────────────────────────┘
           │
           ▼ (opencode-sdk-go)
┌─────────────────────────────────────────┐
│        OpenCode Server                  │
│        http://localhost:4096            │
└─────────────────────────────┘
```

---

## 📊 API Endpoints

### Health & Status
- `GET /healthz` - Service health check
- `GET /health` - Detailed health + task stats

### Task Management
- `POST /api/tasks/submit` - Submit a task
- `GET /api/tasks/{task_id}` - Get task details
- `GET /api/tasks/active` - List active tasks
- `GET /api/tasks/history` - Task history
- `GET /api/tasks/stats` - Task statistics

### Scheduled Tasks
- `GET /api/scheduled-tasks` - List all
- `POST /api/scheduled-tasks` - Create task
- `POST /api/scheduled-tasks/{id}` - Update task
- `DELETE /api/scheduled-tasks/{id}` - Delete task
- `POST /api/scheduled-tasks/enable/{id}` - Enable task
- `POST /api/scheduled-tasks/disable/{id}` - Disable task

### Platform Webhooks
- `POST /dingtalk/callback` - DingTalk webhook
- `POST /feishu/callback` - Feishu webhook
- `GET/POST /wecom/callback` - WeCom webhook

See [API.md](docs/API.md) for complete API documentation.

---

## 🔧 Configuration

Environment variable definitions are listed in **Quick Start → Environment Variables** above to avoid duplication.

Auth note: configure either `OPENCODE_API_KEY` or `OPENCODE_SERVER_PASSWORD`. If both are set, openbot prefers `OPENCODE_SERVER_PASSWORD` (Basic Auth).

At startup, whitelist is initialized to owner only (`DINGTALK_OWNER_USERID`). Runtime `/whitelist` command changes are not persisted across restart.

`DINGTALK_OWNER_USERID` is startup-only (environment variable). It cannot be changed by commands, and will be automatically included in whitelist.

---

## 🐳 Docker Deployment

### Build Image

```bash
docker build -t opencode-gateway:latest .
```

### Run Container

```bash
docker run -d \
  --name opencode-gateway \
  -p 8080:8080 \
  --env-file .env \
  --restart unless-stopped \
  opencode-gateway:latest
```

### Docker Compose

```yaml
version: '3.8'
services:
  gateway:
    build: .
    ports:
      - "8080:8080"
    environment:
      - OPENCODE_ENDPOINT=http://opencode:4096
      - DINGTALK_USE_STREAM=true
      - DINGTALK_CLIENT_ID=${DINGTALK_CLIENT_ID}
      - DINGTALK_CLIENT_SECRET=${DINGTALK_CLIENT_SECRET}
    depends_on:
      - opencode
  opencode:
    image: anomaly/opencode:latest
    ports:
      - "4096:4096"
```

Run:
```bash
docker-compose up -d
```

---

## 🐛 Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| Service won't start | Check Go version (≥1.21), OpenCode server status |
| DingTalk Stream not connecting | Verify Client ID/Secret, check network |
| Tasks not executing | Check Cron expression (5-6 fields), verify task is enabled |
| Context timeout errors | Fixed automatically by session management |
| `POST /session` returns `401 Unauthorized` | Check auth mode mismatch: `OPENCODE_SERVER_PASSWORD` requires HTTP Basic Auth (`OPENCODE_SERVER_USERNAME` + password). Restart gateway after changing env vars and avoid mixing both auth modes unintentionally. |

### Debug Mode

```bash
# Enable verbose logging
LOG_LEVEL=debug ./bin/gateway
```

### Logs

Check logs in:
- Console output
- Docker logs: `docker logs opencode-gateway`
- systemd: `journalctl -u opencode-gateway -f`

---

## 📝 Project Structure

```
opencode-gateway/
├── bin/                          # Build outputs
├── cmd/
│   ├── attach/
│   │   └── main.go              # Attach entry (client side)
│   └── gateway/
│       └── main.go              # Entry point
├── internal/
│   ├── adapters/
│   │   ├── base/
│   │   │   └── adapter.go       # Base adapter interface
│   │   ├── dingtalk/
│   │   │   └── dingtalk.go     # DingTalk (Stream + Webhook)
│   │   ├── feishu/
│   │   │   └── feishu.go       # Feishu adapter
│   │   └── wecom/
│   │       └── wecom.go         # WeCom adapter
│   ├── config/
│   │   └── config.go           # Configuration loader
│   ├── opencode/
│   │   ├── client.go           # OpenCode client with session management
│   │   └── event_listener.go   # SSE event handling
│   ├── persistence/
│   │   └── dingtalk_runtime.go # Runtime persistence helpers
│   ├── proxy/
│   │   └── tunnel.go           # Reverse proxy tunnel runtime
│   ├── server/
│   │   └── server.go           # HTTP server
│   └── scheduler/
│       ├── cron_scheduler.go    # Cron scheduler
│       ├── scheduler.go        # Task scheduler
│       ├── task.go             # Task model
│       └── webhook.go          # HTTP API endpoints
├── tools/
│   ├── http-server/
│   │   └── main.go             # Centralized relay gateway
│   ├── client-proxy/
│   │   └── main.go             # Local bridge for TUI/attach
│   └── batch_get_userid.py
├── skill-install/
│   ├── README.md
│   └── tot.py
├── test/                        # Utility checks/debug tools
├── docs/                       # Documentation
│   ├── API.md                  # API reference
│   ├── DEPLOY.md               # Deployment guide
│   ├── CONFIGURATION.md        # Configuration guide
│   ├── QUICK_START_CRONTASK.md # Cron task quick start
│   ├── CRONTASK_COMMAND.md     # Cron command reference
│   ├── SCHEDULER_GUIDE.md      # Task scheduler guide
│   └── PROXY_TUNNEL.md         # Proxy tunnel usage
├── .env.example                # Example environment variables
├── docker-compose.yml          # Docker Compose
├── Dockerfile                  # Docker image
├── go.mod                      # Go modules
├── go.sum                      # Go dependencies
├── docs/ARCHITECTURE.md        # System architecture
├── DOCKER.md                   # Docker setup guide
└── README.md                   # This file
```

---

## 🔥 Key Features Explained

### Permission & Q&A Auto-Reply

The gateway automatically handles OpenCode permission requests and questions:

1. Detects `permission.asked` and `question.asked` events
2. Stores pending questions/permissions
3. Users can reply with simple options:
   - `允许` / `拒绝` / `始终允许` for permissions
   - `1`, `2`, `3` for single-choice questions
   - `选项名` for option labels

### Session Management

- Auto-summarize after 60% context usage
- Auto-create new session at 80% context threshold
- Preserve summary across sessions
- Token estimation to prevent overflow
- **🆕 Session Title Encoding**: Automatically encodes adapter and user info in session title
- **🆕 Auto-Recovery**: Recovers session mappings after service restart from title
- **🆕 Diagnostic Endpoint**: `GET /debug/sessions` for session mapping status

### Streaming Output

- Real-time content delivery via SSE
- Progress notifications for long-running tasks
- Handles permission requests in-stream
- 5-minute timeout for long-running tasks

---

## 🔧 Recent Improvements

### v1.2.0 - Secure Tunnel & Auth/Health Diagnostics (2026-03-05)

**What improved:**
1. **Secure tunnel workflow clarified**: README now documents `tools/http-server` + `tools/client-proxy` as the default secure relay path.
2. **Auth mode behavior aligned**: `OPENCODE_API_KEY` and `OPENCODE_SERVER_PASSWORD` modes are clearly separated, and startup checks provide actionable guidance.
3. **Startup diagnostics enhanced**: health check errors now distinguish endpoint unreachable, timeout, `401`, `403`, and `404` with targeted hints.
4. **Proxy key isolation fixed**: tunnel `proxy_key` is no longer reused as OpenCode API auth credentials.

**Operational benefit:**
- Safer default deployment (no public OpenCode port exposure)
- Faster root-cause diagnosis at startup
- Fewer auth misconfiguration regressions

### v1.1.0 - Session Mapping Enhancement (2026-02-13)

**Problem Fixed**: 
- Messages couldn't be routed to DingTalk/Feishu after service restart
- "no adapter found for session" errors when OpenCode returns results

**Solution Implemented**:
1. **Title Encoding**: Session creation now encodes adapter/user info in session title (format: `[adapter:userId] threadId`)
2. **Auto-Recovery**: Event handler automatically recovers mappings by parsing session title when not found in memory
3. **Diagnostic Tools**: Added `/debug/sessions` endpoint for troubleshooting

**Benefits**:
- ✅ Service restart no longer breaks ongoing sessions
- ✅ External sessions (created via OpenCode API) can now be routed if they have metadata
- ✅ Better observability with recovery logging

**Migration**: 
- Existing sessions (created before v1.1.0) cannot auto-recover (title format doesn't match)
- Users should resend messages to establish new mappings
- See [SESSION_MAPPING_TROUBLESHOOTING.md](docs/SESSION_MAPPING_TROUBLESHOOTING.md) for details

---

## 🔗 Dependencies

- `github.com/sst/opencode-sdk-go` - OpenCode official SDK
- `github.com/open-dingtalk/dingtalk-stream-sdk-go` - DingTalk Stream SDK
- `github.com/robfig/cron` - Cron scheduler

---

## 📜 License

MIT License

---

## 🔗 Links

- [OpenCode](https://github.com/sst/opencode)
- [OpenCode Server API](https://opencode.ai/docs/server/)
- [DingTalk Open Platform](https://open.dingtalk.com/)
- [DingTalk Stream SDK](https://github.com/open-dingtalk/dingtalk-stream-sdk-go)

---

**For detailed setup and usage, see [DEPLOY.md](docs/DEPLOY.md).**