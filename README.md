# OpenCode Gateway

Enterprise messaging gateway that bridges DingTalk, Feishu, and WeCom to an [OpenCode](https://opencode.ai) AI server.

## Features

- **Multi-platform support**: DingTalk (Stream + Webhook), Feishu (WebSocket + Webhook), WeCom (Webhook)
- **Bidirectional communication**: User messages → OpenCode; OpenCode SSE events → Platform
- **Smart session management**: Auto-summarization, context-aware session renewal, token estimation
- **Natural language scheduling**: Create and manage cron tasks in plain Chinese/English chat
- **Cron scheduler**: Persistent 6-field cron with confirmation flow, task enable/disable/delete
- **Task scheduler**: Worker pool (10 concurrent), priority queue, one-shot and recurring tasks
- **Memory store**: Per-user episodic recall across sessions (SQLite + forgetting curve decay)
- **Secure proxy tunnel**: No public port exposure required; outbound-only WS relay via `tools/http-server`
- **Config hot-reload**: Watches `opencode.json` (global + project) and restarts opencode automatically
- **Voice input**: Aliyun NLS speech-to-text (DingTalk + Feishu)
- **Skill/Agent routing**: Forwards `@agent` prefix to named OpenCode skills

---

## Quick Start

### Prerequisites

- Go 1.21+
- OpenCode server (`opencode serve`, default port 4096)
- Platform credentials (DingTalk / Feishu / WeCom)

### Build

```bash
go build -o bin/openbot ./cmd/gateway
```

### Minimum configuration

```bash
# DingTalk Stream mode (recommended — no public IP needed)
export DINGTALK_CLIENT_ID=your_client_id
export DINGTALK_CLIENT_SECRET=your_client_secret
export DINGTALK_OWNER_USERID=your_dingtalk_userid

# OpenCode (auto-managed by default)
export OPENCODE_DIRECTORY=/path/to/your/project

./bin/openbot
```

### Run with explicit log level or JSON config

```bash
./bin/openbot --log-level debug
./bin/openbot --config gateway.json    # { "log_level": "warn" }
```

---

## Environment Variables

### Core

| Variable | Default | Description |
|---|---|---|
| `HTTP_ENABLED` | `false` | Enable built-in HTTP server (webhook mode requires `true`) |
| `SERVER_ADDR` | `:8080` | Listen address when HTTP is enabled |
| `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `OPENCODE_ENDPOINT` | `http://localhost:4096` | OpenCode server URL |
| `OPENCODE_API_KEY` | — | Bearer / X-API-Key auth |
| `OPENCODE_SERVER_PASSWORD` | — | HTTP Basic Auth password (takes priority over API key) |
| `OPENCODE_SERVER_USERNAME` | `opencode` | HTTP Basic Auth username |
| `OPENCODE_DIRECTORY` | `.` | Working directory for opencode; also root for config/memory paths |
| `OPENCODE_MANAGE_SERVE` | `true` | Let gateway start/restart the opencode subprocess |
| `OPENCODE_SERVE_COMMAND` | `opencode` | Binary name for managed serve |
| `OPENCODE_SERVE_ARGS` | `serve` | Args passed to opencode when managed |
| `OPENCODE_MAX_RETRY_ATTEMPTS` | `5` | Max upstream-provider retries (`type:"retry"` parts, e.g. "No available channel for model …") for one turn before the gateway surfaces the error and ends the turn instead of hanging in "正在处理中" |
| `OPENCODE_STREAM_IDLE_TIMEOUT_SECONDS` | `600` | No-content streaming idle timeout (long prefill / extended-thinking models) |
| `OPENCODE_STREAM_IDLE_TIMEOUT_HASSENT_SECONDS` | `120` | Has-content streaming idle timeout |
| `OPENCODE_STREAM_BUSY_PROBE_EXTEND` | `true` | Extend idle timeout while the session still shows activity (`0`/`false` to disable) |
| `MEMORY_STORE_PATH` | `<OPENCODE_DIRECTORY>/tmp/memory.db` | SQLite path for episodic memory (`""` disables) |

### DingTalk

| Variable | Default | Description |
|---|---|---|
| `DINGTALK_CLIENT_ID` | — | Stream mode App key |
| `DINGTALK_CLIENT_SECRET` | — | Stream mode App secret |
| `DINGTALK_OWNER_USERID` | — | **Required.** Owner DingTalk user ID; auto-added to whitelist |
| `DINGTALK_NON_OWNER_PLAN_MODE` | `false` | Non-owner messages go to OpenCode in plan-only mode |
| `DINGTALK_APP_KEY` | — | Webhook mode app key (legacy) |
| `DINGTALK_APP_SECRET` | — | Webhook mode app secret (legacy) |
| `DINGTALK_VERIFICATION_TOKEN` | — | Webhook verification token |
| `DINGTALK_ENCRYPT_KEY` | — | Webhook encryption key |

### Feishu

| Variable | Default | Description |
|---|---|---|
| `FEISHU_APP_ID` | — | App ID |
| `FEISHU_APP_SECRET` | — | App secret |
| `FEISHU_VERIFICATION_TOKEN` | — | Webhook verification token |
| `FEISHU_ENCRYPT_KEY` | — | Webhook encryption key |
| `FEISHU_USE_WEBSOCKET` | `true` | Use WebSocket mode (no public IP needed) |

### WeCom

| Variable | Default | Description |
|---|---|---|
| `WECOM_CORP_ID` | — | Corp ID |
| `WECOM_CORP_SECRET` | — | Corp secret |
| `WECOM_AGENT_ID` | — | Agent ID |
| `WECOM_TOKEN` | — | Webhook token |
| `WECOM_AES_KEY` | — | Webhook AES key |

### Voice (Aliyun NLS, optional)

| Variable | Description |
|---|---|
| `ALIYUN_NLS_AKID` | Access key ID |
| `ALIYUN_NLS_AKKEY` | Access key secret |
| `ALIYUN_NLS_APPKEY` | NLS project app key |

### Proxy Tunnel

| Variable | Default | Description |
|---|---|---|
| `PROXY_HUB_WS_URL` | — | WebSocket URL of relay hub (e.g. `ws://hub:18080/ws`) |
| `PROXY_KEY_FILE` | `.opencode-gateway-proxy.json` | JSON file storing the reusable proxy key |
| `PROXY_LOCAL_OPENCODE_ADDR` | `127.0.0.1:4096` | Local opencode address for proxy forwarding |
| `PROXY_RECONNECT_DELAY` | `5s` | Reconnect delay on tunnel failure |

---

## Chat Commands

### General

| Command | Description |
|---|---|
| `/help` | Show help |
| `/skills` / `/agents` | List available OpenCode skills/agents |
| `/abort` / `/stop` | Abort current task |
| `/refresh` | Refresh skill cache |

### Session Management

| Command | Description |
|---|---|
| `/status` | Session ID, title, token usage |
| `/new` / `/reset` | Drop session mapping (next message creates new session) |
| `/clear` | Delete current session and all its history |
| `/fork` | Fork current session |
| `/undo` / `/redo` | Undo / redo last message |
| `/sessions` / `/list` | List recent sessions (up to 10) |
| `/summary` | Summarize session context to free token space |

### Model & Output

| Command | Description |
|---|---|
| `/models` | List available models across all configured providers |
| `/model <provider>/<model>` | Set model for current session |
| `/thinking on\|off` | Toggle thinking output |
| `/final on\|off` | Return only final result (skip streaming steps) |
| `/steps on\|off` | Toggle intermediate step display |
| `/config` | Show current configuration |

### Task Tracking

| Command | Description |
|---|---|
| `/todo` | Show AI task/todo progress |
| `/diff` | Show file change summary for this session |

### Interaction

| Command | Description |
|---|---|
| `/answer <answer>` | Answer the latest pending question |
| `/answer <question_id> <answer>` | Answer a specific question |
| `/cmd <shell_command>` | Execute a shell command in the current session context |

### Cron Tasks

| Command | Description |
|---|---|
| `/crontask add "cron" "name" "content" [agent]` | Add scheduled task |
| `/crontask list` | List all tasks |
| `/crontask info <id>` | Show task details |
| `/crontask enable <id>` | Enable a task |
| `/crontask disable <id>` | Disable a task |
| `/crontask delete <id>` | Delete a task |
| `/crontask help` | Show cron help |

Cron expressions use 6 fields (seconds-precision): `sec min hour dom month dow`.

```
/crontask add "0 0 9 * * *"     "日报"   "生成每日工作日报"
/crontask add "0 */30 * * * *"  "监控"   "检查系统状态"
/crontask add "0 0 12 * * 1-5"  "午检"   "查看错误日志"
```

### Natural Language Scheduling

Instead of `/crontask`, use plain language. Write operations show a draft first; reply `确认` / `yes` to execute, `取消` / `no` to abort.

```
每天早上9点提醒我生成日报
列出我的定时任务
禁用任务 cron-xxx
把这个任务试运行一次
```

### DingTalk Whitelist

| Command | Description |
|---|---|
| `/whitelist list` | Show current whitelist |
| `/whitelist add <userID...>` | Add users at runtime |
| `/whitelist del <userID...>` | Remove users at runtime |

Whitelist is initialized to owner only (`DINGTALK_OWNER_USERID`) at startup. Changes are in-memory and do not persist across restart.

### Agent / Skill Routing

Prefix a message with `@agent_name` to route to a named OpenCode skill:

```
@build 帮我编译这个项目
@plan 规划新功能的开发步骤
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Enterprise Messaging Platforms                          │
│  DingTalk (Stream WS)  ·  Feishu (WS/Webhook)  ·  WeCom │
└────────────────────────────┬─────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────┐
│  openbot  (cmd/gateway)                                  │
│                                                          │
│  Adapters          NL Scheduler        Task Scheduler    │
│  ┌──────────┐      ┌─────────────┐    ┌──────────────┐  │
│  │DingTalk  │      │ Rule parser  │    │ Worker pool  │  │
│  │Feishu    │ ───► │ Confirm gate│ ──►│ (10 workers) │  │
│  │WeCom     │      │ Cron exec   │    │ Cron fire    │  │
│  └──────────┘      └─────────────┘    └──────────────┘  │
│                                                          │
│  OpenCode Client                                         │
│  ┌────────────────────────────────────────────────────┐  │
│  │ Session cache · Token estimation · Memory recall   │  │
│  │ Provider catalog (30s TTL, invalidated on restart) │  │
│  │ SSE event listener · Permission auto-reply         │  │
│  └────────────────────────────────────────────────────┘  │
│                                                          │
│  Config Watcher (fsnotify)                               │
│  global + project opencode.json → drain → restart        │
└──────────────────────────┬───────────────────────────────┘
                           │ opencode-sdk-go
                           ▼
              ┌────────────────────────┐
              │  OpenCode Server :4096 │
              │  (managed subprocess)  │
              └────────────────────────┘
```

### Config Hot-Reload

When `OPENCODE_MANAGE_SERVE=true` (default), the gateway watches for changes in:

1. Global: `%APPDATA%\opencode\opencode.json` (Windows) / `~/.config/opencode/opencode.json`
2. Project: `OPENCODE_DIRECTORY/opencode.json`

On change (2s debounce): waits up to 30s for active streaming sessions to finish → restarts opencode subprocess → immediately flushes provider/model cache. The `/models` command reflects the new provider list on the next call.

### Memory Store

SQLite at `<OPENCODE_DIRECTORY>/tmp/memory.db`. Per-user episodic notes are written by the AI during sessions and injected as context on relevant future queries. Forgetting curve decay runs hourly. Set `MEMORY_STORE_PATH` to an absolute path for predictable location.

---

## Secure Proxy Tunnel

By default `HTTP_ENABLED=false` — openbot exposes no listening ports. For remote access via OpenCode TUI, use the relay hub:

```
[OpenCode TUI] ──► [tools/client-proxy :14096] ──► [tools/http-server :18080] ◄── [openbot]
                                                           (public relay)
```

### Setup

```bash
# 1. Start relay server (publicly reachable host)
go run ./tools/http-server -addr :18080

# 2. Start openbot (near OpenCode, outbound access to relay only)
PROXY_HUB_WS_URL=ws://<relay-host>:18080/ws ./bin/openbot
# Proxy key is auto-generated and saved to PROXY_KEY_FILE on first run.

# 3. Start local bridge (developer machine)
go run ./tools/client-proxy \
  -hub ws://<relay-host>:18080/ws \
  -proxy-key <key_from_PROXY_KEY_FILE> \
  -listen 127.0.0.1:14096

# 4. Connect OpenCode TUI to local bridge
opencode --endpoint http://127.0.0.1:14096
```

See [docs/PROXY_TUNNEL.md](docs/PROXY_TUNNEL.md) for full details.

---

## HTTP API (when `HTTP_ENABLED=true`)

### Health

| Endpoint | Description |
|---|---|
| `GET /healthz` | Liveness check |
| `GET /health` | Health + task stats |

### Tasks

| Endpoint | Description |
|---|---|
| `POST /api/tasks/submit` | Submit a one-shot task |
| `GET /api/tasks/{id}` | Task details |
| `GET /api/tasks/active` | Active tasks |
| `GET /api/tasks/history` | Task history |
| `GET /api/tasks/stats` | Statistics |

### Scheduled Tasks

| Endpoint | Description |
|---|---|
| `GET /api/scheduled-tasks` | List all |
| `POST /api/scheduled-tasks` | Create |
| `POST /api/scheduled-tasks/{id}` | Update |
| `DELETE /api/scheduled-tasks/{id}` | Delete |
| `POST /api/scheduled-tasks/enable/{id}` | Enable |
| `POST /api/scheduled-tasks/disable/{id}` | Disable |

### Platform Webhooks

| Endpoint | Description |
|---|---|
| `POST /dingtalk/callback` | DingTalk webhook |
| `POST /feishu/callback` | Feishu webhook |
| `GET/POST /wecom/callback` | WeCom webhook |

### Diagnostics

| Endpoint | Description |
|---|---|
| `GET /debug/sessions` | Session/adapter status |
| `GET /debug/opencode` | OpenCode endpoint info |

See [docs/API.md](docs/API.md) for full reference.

---

## Project Structure

```
opencode-gateway/
├── cmd/
│   ├── gateway/main.go          # Main entry point
│   └── attach/main.go           # Attach client
├── internal/
│   ├── adapters/
│   │   ├── base/                # Shared adapter interface + registry
│   │   ├── dingtalk/            # DingTalk Stream + Webhook
│   │   ├── feishu/              # Feishu WebSocket + Webhook
│   │   └── wecom/               # WeCom Webhook
│   ├── config/config.go         # Environment-based configuration
│   ├── memstore/                # Episodic memory (SQLite)
│   ├── opencode/
│   │   ├── client.go            # OpenCode SDK wrapper, session + provider cache
│   │   ├── event_listener.go    # SSE event routing
│   │   ├── queue.go             # Message queue (worker pool)
│   │   └── stream_event.go      # Stream event types
│   ├── opencodesvc/manager.go   # OpenCode subprocess lifecycle
│   ├── proxy/                   # Proxy tunnel runtime + RPC client
│   ├── scheduler/
│   │   ├── cron_scheduler.go    # Persistent cron (robfig/cron v3, JSON storage)
│   │   ├── scheduler.go         # Task scheduler worker pool
│   │   ├── task.go              # Task model
│   │   ├── webhook.go           # HTTP API handlers
│   │   ├── nl_gate.go           # NL prefilter (length + keyword gate)
│   │   ├── nl_intent.go         # Intent data structure
│   │   ├── nl_parser_rule.go    # Rule-based NL → cron expression parser
│   │   ├── nl_service.go        # NL confirm state machine
│   │   └── nl_state.go          # Per-user pending draft (TTL 10min)
│   └── server/server.go         # HTTP server with graceful shutdown
├── skills/                      # OpenCode skill definitions (SKILL.md bundles)
│   ├── dingtalk-file-sender/
│   ├── feishu-file-sender/
│   ├── video-analyzer/
│   └── skill-creator/
├── tools/
│   ├── http-server/main.go      # Relay hub
│   └── client-proxy/main.go     # Local TCP bridge
├── docs/                        # Extended documentation
└── test/                        # Debug/check utilities
```

---

## Docker

```bash
# Build
docker build -t opencode-gateway .

# Run
docker run -d \
  --name opencode-gateway \
  --env-file .env \
  opencode-gateway

# Or with Compose
docker-compose up -d
```

See [DOCKER.md](DOCKER.md) for compose configuration details.

---

## Documentation

| Doc | Description |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Detailed architecture |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Full configuration reference |
| [docs/API.md](docs/API.md) | HTTP API reference |
| [docs/DEPLOY.md](docs/DEPLOY.md) | Deployment guide |
| [docs/PROXY_TUNNEL.md](docs/PROXY_TUNNEL.md) | Proxy tunnel setup |
| [docs/SCHEDULER_GUIDE.md](docs/SCHEDULER_GUIDE.md) | Task scheduler guide |
| [docs/CRONTASK_COMMAND.md](docs/CRONTASK_COMMAND.md) | Cron command reference |
| [docs/QUICK_START_CRONTASK.md](docs/QUICK_START_CRONTASK.md) | Cron quick start |
| [DOCKER.md](DOCKER.md) | Docker setup |

---

## Troubleshooting

| Issue | Solution |
|---|---|
| No `memory.db` created | Check shell cwd at startup; file goes into `<cwd>/tmp/`. Set `MEMORY_STORE_PATH` to an absolute path to fix. |
| `401 Unauthorized` on session create | Auth mode mismatch. Set either `OPENCODE_API_KEY` or `OPENCODE_SERVER_PASSWORD`, not both. |
| DingTalk Stream not connecting | Verify `DINGTALK_CLIENT_ID` / `DINGTALK_CLIENT_SECRET`; check outbound WebSocket access. |
| Tasks not running | Check cron expression (must be 6 fields); verify task is enabled (`/crontask list`). |
| `/models` returns stale list after config change | Edit and save `opencode.json`; gateway auto-restarts and flushes provider cache within ~3s. |
| Config change not detected | Check startup log for `opencode: watching config files: [...]` — only existing files are watched. |

```bash
# Verbose logging
LOG_LEVEL=debug ./bin/openbot
```
