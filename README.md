# OpenCode Gateway

> 企业级即时通讯 AI 网关 — 将钉钉、飞书、企业微信、个人微信（iLink）桥接到 [OpenCode](https://github.com/sst/opencode) AI 服务器，实现多平台统一的智能对话、任务调度、技能自学习与记忆管理。

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## 目录

- [核心特性](#核心特性)
- [快速开始](#快速开始)
- [支持平台](#支持平台)
- [聊天命令](#聊天命令)
- [技能自动生成](#技能自动生成skillgen)
- [定时任务调度](#定时任务调度)
- [记忆系统](#记忆系统)
- [MCP 服务器](#mcp-服务器)
- [安全代理隧道](#安全代理隧道)
- [环境变量](#环境变量)
- [Docker 部署](#docker-部署)
- [项目结构](#项目结构)
- [构建命令](#构建命令)
- [文档索引](#文档索引)
- [故障排查](#故障排查)
- [许可证](#许可证)

---

## 核心特性

### 🌐 多平台统一接入

| 平台 | 模式 | 公网暴露 | 特色功能 |
|------|------|---------|---------|
| **钉钉** | Stream (WebSocket) / Webhook | ❌ Stream 不需要 | AI 卡片流式更新、owner 白名单、非 owner 计划模式 |
| **飞书** | WebSocket / Webhook | ❌ WS 不需要 | 用户目标映射、token 缓存、调试模式 |
| **企业微信** | Webhook | ✅ 需要 | 应用消息发送、群聊 fallback |
| **个人微信** | iLink Bot API | ❌ 不需要 | QR 登录、入站批处理、持久化出站队列、离线重试 |

### 🧠 智能会话管理

- **流式 SSE 支持** — 实时接收 OpenCode 的 `message.part.updated` 事件，逐字推送
- **会话接续** — 卡死会话自动检测分类（provider 错误 / 无助手回复 / 无新鲜事件），自动 handoff 摘要注入
- **空闲超时** — 无内容 600s / 有内容 120s 超时，busy 探测自动延长
- **Provider 重试检测** — 超过最大重试次数（默认 5）时浮出错误而非挂起
- **配置热重载** — `fsnotify` 监听 `opencode.json` 变更，2s 防抖，等待活跃流式会话排空后重启

### 🤖 技能自动生成（Skillgen）

- **自动挖掘** — 长会话（≥3 次工具调用）或 handoff 后自动分析对话，提取可复用技能
- **多模型探索** — epsilon-greedy 策略在候选模型池中探索，Wilson 收缩评分
- **人工审核** — 草稿写入 `skills-candidates/`，`/skill-approve` 批准后安装
- **模型 Fallback** — 主模型失败时自动切换备选模型

### ⏰ 定时任务调度

- **Cron 调度器** — 6 位秒级精度（`robfig/cron/v3`），JSON 持久化，支持 `@daily` 等描述符
- **自然语言调度** — "每天早上9点提醒我生成日报" → 自动解析为 cron 表达式，确认后生效
- **任务调度器** — 10 worker 池，优先级队列，30 分钟超时，任务历史

### 💾 记忆系统

- **SQLite + FTS5** — 纯 Go 驱动（`modernc.org/sqlite`），trigram 分词支持中文
- **情景回忆** — 对话记录自动存储，BM25 搜索召回
- **遗忘曲线** — 每小时衰减强度，模拟艾宾浩斯遗忘曲线
- **会话 Handoff** — 卡死会话摘要保存，下次同线程自动注入

### 🔒 安全代理隧道

- **零公网暴露** — OpenCode 4096 端口永不暴露
- **WS 中继** — 公网中继 Hub + 网关出站 WS + 本地 TCP 桥接
- **密钥管理** — 32 字节 hex 密钥，文件权限 0600，Hub URL 变更自动轮换

### 📦 其他特性

- **MCP 服务器** — 将记忆库暴露为 LLM 工具（`memory_search` / `memory_list_projects` / `memory_recent`）
- **离线重试** — 消息发送失败 5 次后进入离线队列，`/recover` 恢复
- **语音输入** — 阿里云 NLS 语音转文字（钉钉 + 飞书）
- **技能/Agent 路由** — `@agent_name` 前缀转发到指定 OpenCode 技能
- **远程 WebUI** — JSON-RPC over WS，支持远程聊天、模型切换、技能管理

---

## 快速开始

### 前置条件

- [Go 1.25+](https://go.dev/dl/)
- [OpenCode](https://github.com/sst/opencode) 服务器（网关可自动管理子进程）
- 至少一个 IM 平台的开发者账号

### 构建与运行

```bash
# 克隆仓库
git clone https://github.com/user/opencode-gateway.git
cd opencode-gateway

# 构建所有二进制
make build

# 从模板生成配置
make init-env
# 编辑 .env 填入你的平台凭证

# 加载 .env 并运行
make run-env

# 或直接运行（使用当前环境变量）
make run
```

### 产出的二进制

| 二进制 | 说明 |
|--------|------|
| `bin/openbot` | 主网关进程 |
| `bin/openbot-mcp` | MCP stdio 服务器（记忆工具） |
| `bin/openbot-attach` | `opencode attach` 本地 TCP→Hub 桥接 |
| `bin/openbot-wechat` | 微信 QR 登录 + 凭证管理 |

### 最小配置示例（钉钉 Stream 模式）

```bash
# .env
OPENCODE_ENDPOINT=http://localhost:4096
OPENCODE_DIRECTORY=/root/openbot
DINGTALK_CLIENT_ID=dingxxxxxxxx
DINGTALK_CLIENT_SECRET=xxxxxxxx
DINGTALK_OWNER_USERID=1601434517956472
```

---

## 支持平台

### 钉钉（DingTalk）

- **Stream 模式**（推荐）：WebSocket 长连接，无需公网 IP
- **Webhook 模式**（传统）：需要公网回调地址
- **AI 卡片**：流式更新，实时展示 AI 生成过程
- **白名单**：`DINGTALK_OWNER_USERID` 自动白名单，非 owner 计划模式
- **多媒体**：图片/音频/视频/文件/richText，通过 `downloadCode` 下载

### 飞书（Feishu）

- **WebSocket 模式**（默认）：无需公网 IP
- **Webhook 模式**：需要公网回调地址
- 基于 `larksuite/oapi-sdk-go/v3`
- 支持语音输入、重试队列

### 企业微信（WeCom）

- Webhook 模式（`/wecom/callback`）
- 应用消息 + 群聊消息发送
- 支持 cron 会话注册

### 个人微信（WeChat via iLink）

最复杂的适配器，专为 iLink Bot API 设计：

- **QR 登录**：`openbot-wechat` 扫码登录，凭证加密存储
- **入站批处理**：自动合并快速连续的消息（粘贴/转发拆分），可调延迟
- **用户级序列化**：同一用户的消息 FIFO 处理，第二个消息排队等待第一个完成
- **持久化出站队列**：SQLite 3 级优先级系统
  - `PriorityHigh`（2）：权限/问题确认 — 跳过限流冷却
  - `PriorityNormal`（1）：最终结果、todo、错误通知
  - `PriorityLow`（0）：进度更新 — 拥堵时可 TTL 丢弃
- **离线队列**：5 次热重试失败后进入离线队列，`/pending` `/recover` 恢复
- **顺序权限确认**：多个权限请求逐个显示，回答当前才能看到下一个
- **限流自适应**：30s→60s→120s 递增冷却，成功后自动清除

---

## 聊天命令

### 通用

| 命令 | 说明 |
|------|------|
| `/help` / `帮助` | 显示帮助 |
| `/skills` / `/agents` | 列出可用技能/Agent |
| `/abort` / `/stop` / `停止` | 中止当前任务 |
| `/refresh` | 刷新技能缓存 |

### 会话管理

| 命令 | 说明 |
|------|------|
| `/status` / `状态` | 会话 ID、token 用量、诊断信息 |
| `/new` / `/reset` / `新会话` | 清除旧会话，下次消息创建新会话 |
| `/clear` / `清除` | 删除当前会话及历史 |
| `/fork` | 派生当前会话 |
| `/undo` / `/redo` | 撤销/重做上次操作 |
| `/sessions` / `/list` | 列出最近会话（最多 10 个） |
| `/summary` | 摘要会话上下文以释放 token |

### 模型与输出

| 命令 | 说明 |
|------|------|
| `/models` | 列出所有可用模型 |
| `/model <provider>/<model>` | 设置会话模型 |
| `/thinking on\|off` | 切换思考过程输出 |
| `/final on\|off` | 仅返回最终结果（WeChat 内置开启） |
| `/steps on\|off` | 切换中间步骤显示 |
| `/config` / `配置` | 显示当前配置 |

### 任务跟踪

| 命令 | 说明 |
|------|------|
| `/todo` | 显示 AI 任务/todo 进度 |
| `/diff` | 显示会话文件变更摘要 |

### 交互

| 命令 | 说明 |
|------|------|
| `/answer <答案>` | 回答最新待处理问题 |
| `/answer <问题ID> <答案>` | 回答指定问题 |
| `/cmd <shell命令>` | 在会话上下文中执行 shell 命令 |

### 定时任务

| 命令 | 说明 |
|------|------|
| `/crontask add "cron" "名称" "内容" [agent]` | 添加定时任务 |
| `/crontask list` | 列出所有任务 |
| `/crontask info <id>` | 查看任务详情 |
| `/crontask enable/disable <id>` | 启用/禁用任务 |
| `/crontask delete <id>` | 删除任务 |
| `每天早上9点提醒我生成日报` | 自然语言创建（自动解析） |

### 技能管理

| 命令 | 说明 |
|------|------|
| `/skill-help` | 技能帮助 |
| `/skill-list [pending\|approved\|rejected\|all]` | 列出技能候选 |
| `/skill-view <id>` | 查看完整 SKILL.md |
| `/skill-approve <id>` | 批准技能（安装到 `skills/`） |
| `/skill-reject <id> [原因]` | 拒绝技能 |
| `/skill-stats` | 模型生成/批准统计 |

### 微信离线队列

| 命令 | 说明 |
|------|------|
| `/pending` | 查看待处理的离线消息 |
| `/recover` | 恢复离线消息 |

### Agent 路由

```
@build 帮我构建项目
@plan 分析这个架构
@chat 闲聊模式
```

---

## 技能自动生成（Skillgen）

### 工作原理

```
用户长会话完成 → 自动分析对话 → AI 生成 SKILL.md → 人工审核 → 安装
```

1. **触发条件**：会话完成且工具调用 ≥3 次（`SKILLGEN_MIN_TOOL_CALLS`），或会话 handoff 时
2. **模型选择**：epsilon-greedy 策略在 `SKILLGEN_DRAFT_MODEL` + `SKILLGEN_ALTERNATE_MODELS` 中选择
3. **草稿生成**：专用 opencode 会话，加载参考技能，输出 JSON 格式的 SKILL.md
4. **模型 Fallback**：主模型失败时自动切换备选模型重试
5. **人工审核**：草稿写入 `skills-candidates/`，通知用户审核
6. **安装**：`/skill-approve` 后移动到 `skills/` 目录

### 配置

```bash
SKILLGEN_ENABLED=true
SKILLGEN_DRAFT_MODEL=Tianhe-AI/GLM-5.2
SKILLGEN_ALTERNATE_MODELS=Tianhe-AI/GLM-5.2,Tianhe-AI/Qwen3.6-35B-A3B-FP8
SKILLGEN_APPROVAL_REQUIRED=true
SKILLGEN_MAX_PER_DAY=5
SKILLGEN_MIN_CONFIDENCE=0.4
SKILLGEN_INSTALL_DIR=/root/.config/opencode/skills
```

### 内置技能

| 技能 | 说明 |
|------|------|
| `skill-creator` | 技能创建参考（Apache-2.0） |
| `dingtalk-file-sender` | 钉钉文件发送 |
| `feishu-file-sender` | 飞书文件发送 |
| `video-analyzer` | 视频分析（关键帧提取 + 视觉 AI） |

---

## 定时任务调度

### Cron 格式

6 位秒级精度：`秒 分 时 日 月 周`

```
0 0 9 * * *        # 每天 9:00
0 30 8 * * 1-5     # 工作日 8:30
0 0 0 1 * *        # 每月 1 号 0:00
@daily              # 每天 0:00
```

### 自然语言调度

直接在聊天中输入：

```
每天早上9点提醒我生成日报
每周一上午10点检查项目状态
列出我的定时任务
禁用任务 cron-xxx
```

系统会先展示解析结果，回复 `确认` 执行，`取消` 中止。

---

## 记忆系统

### 架构

- **存储**：SQLite（`modernc.org/sqlite`，纯 Go，WAL 模式）
- **全文搜索**：FTS5 虚拟表，trigram 分词器（支持中文）
- **路径**：`MEMORY_STORE_PATH`（默认 `<OPENCODE_DIRECTORY>/mem/memory.db`）

### 功能

- **情景回忆**：对话记录自动存储（请求、响应摘要、项目、工作目录、操作类型、标签）
- **BM25 搜索**：通过 MCP 工具或 API 搜索历史对话
- **遗忘曲线**：每小时衰减强度，模拟艾宾浩斯遗忘曲线
- **会话 Handoff**：卡死会话的摘要保存，下次同线程自动注入为新会话的前言

### 自动 MCP 注册

网关启动时自动在 `opencode.json` 中注册 `gateway-memory` MCP 工具，AI 可主动搜索记忆。

---

## MCP 服务器

`openbot-mcp` 是一个 MCP stdio 服务器，将记忆库暴露为 LLM 工具：

| 工具 | 说明 |
|------|------|
| `memory_search` | FTS + 模糊搜索（query 必填，可选 project/days/limit） |
| `memory_list_projects` | 项目列表（含计数和最后活跃时间） |
| `memory_recent` | 最近对话（days 默认 7，limit 默认 20） |

### 配置

```json
// opencode.json
{
  "mcp": {
    "gateway-memory": {
      "type": "local",
      "command": ["./bin/openbot-mcp", "--db", "./tmp/memory.db"],
      "enabled": true
    }
  }
}
```

---

## 安全代理隧道

### 架构

```
┌─────────────┐     WS      ┌─────────────┐     TCP     ┌─────────────┐
│  公网中继 Hub │◄──────────►│  网关 (openbot)│◄──────────►│ OpenCode 4096│
│  (http-server)│            │  出站 WS 连接  │            │  (本地 only)  │
└─────────────┘             └─────────────┘             └─────────────┘
                                   ▲
                                   │ WS
                                   ▼
                            ┌─────────────┐
                            │  远程 WebUI   │
                            │ (client-proxy)│
                            └─────────────┘
```

### 三个组件

1. **`tools/http-server`** — 公网 WS 中继 Hub
2. **网关内置** — 出站 WS control + data 连接，桥接到本地 `127.0.0.1:4096`
3. **`tools/client-proxy`** — 本地 TCP→WS 桥接，供 TUI/attach 使用

### 配置

```bash
PROXY_HUB_WS_URL=ws://your-hub:18080/ws
PROXY_KEY_FILE=.opencode-gateway-proxy.json
PROXY_LOCAL_OPENCODE_ADDR=127.0.0.1:4096
```

---

## 环境变量

### OpenCode 核心

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `OPENCODE_API_KEY` | — | Bearer / X-API-Key 认证 |
| `OPENCODE_SERVER_PASSWORD` | — | HTTP Basic Auth 密码 |
| `OPENCODE_SERVER_USERNAME` | `opencode` | Basic Auth 用户名 |
| `OPENCODE_ENDPOINT` | `http://localhost:4096` | OpenCode 服务器地址 |
| `OPENCODE_DIRECTORY` | `.` | 工作目录 |
| `OPENCODE_MANAGE_SERVE` | `true` | 网关管理 opencode 子进程 |
| `OPENCODE_SERVE_COMMAND` | `opencode` | opencode 二进制名 |
| `OPENCODE_SERVE_ARGS` | `serve` | 启动参数 |
| `OPENCODE_MAX_RETRY_ATTEMPTS` | `5` | Provider 最大重试次数 |
| `OPENCODE_STREAM_IDLE_TIMEOUT_SECONDS` | `600` | 无内容空闲超时 |
| `OPENCODE_STREAM_IDLE_TIMEOUT_HASSENT_SECONDS` | `120` | 有内容空闲超时 |
| `OPENCODE_STREAM_BUSY_PROBE_EXTEND` | `true` | 活跃时延长超时 |

### 日志与 HTTP

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `LOG_LEVEL` | `info` | debug/info/warn/error |
| `HTTP_ENABLED` | `false` | 启用 HTTP 服务器 |
| `SERVER_ADDR` | `:8080` | 监听地址 |
| `SERVER_READ_TIMEOUT` | `30s` | HTTP 读超时 |
| `SERVER_WRITE_TIMEOUT` | `300s` | HTTP 写超时 |
| `SERVER_SHUTDOWN_GRACE` | `30s` | 优雅关闭超时 |

### 记忆

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `MEMORY_STORE_PATH` | `<OPENCODE_DIRECTORY>/mem/memory.db` | SQLite 路径（空=禁用） |

### 钉钉

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DINGTALK_CLIENT_ID` | — | Stream 模式 App Key |
| `DINGTALK_CLIENT_SECRET` | — | Stream 模式 App Secret |
| `DINGTALK_OWNER_USERID` | — | Owner 用户 ID（必填） |
| `DINGTALK_NON_OWNER_PLAN_MODE` | `false` | 非 owner 计划模式 |
| `DINGTALK_APP_KEY` | — | Webhook 模式（传统） |
| `DINGTALK_APP_SECRET` | — | Webhook 模式（传统） |
| `DINGTALK_VERIFICATION_TOKEN` | — | Webhook 验证 |
| `DINGTALK_ENCRYPT_KEY` | — | Webhook 加密 |
| `DINGTALK_SIGNING_SECRET` | — | Webhook 签名 |

### 飞书

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `FEISHU_APP_ID` | — | App ID |
| `FEISHU_APP_SECRET` | — | App Secret |
| `FEISHU_VERIFICATION_TOKEN` | — | Webhook 验证 |
| `FEISHU_ENCRYPT_KEY` | — | Webhook 加密 |
| `FEISHU_USE_WEBSOCKET` | `true` | WebSocket 模式 |

### 企业微信

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `WECOM_CORP_ID` | — | 企业 ID |
| `WECOM_CORP_SECRET` | — | 企业 Secret |
| `WECOM_AGENT_ID` | — | 应用 ID |
| `WECOM_TOKEN` | — | Webhook Token |
| `WECOM_AES_KEY` | — | Webhook AES Key |

### 个人微信（iLink）

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `WECHAT_BOT_TOKEN` | — | iLink Bot Token |
| `WECHAT_BASE_URL` | `https://ilinkai.weixin.qq.com` | iLink API 地址 |
| `WECHAT_ACCOUNT_ID` | — | 凭证 ID |
| `WECHAT_STATE_DIR` | `~/.opencode-gateway-wechat` | 状态目录 |
| `WECHAT_CDN_BASE_URL` | `https://ilinkai.weixin.qq.com` | CDN 地址 |
| `WECHAT_TEXT_BATCH_DELAY_SECONDS` | `3` | 入站批处理延迟 |
| `WECHAT_TEXT_BATCH_SPLIT_DELAY_SECONDS` | `5` | 拆分检测延迟 |
| `WECHAT_OUTBOUND_DB_PATH` | `<stateDir>/outbound_queue.db` | 出站队列 SQLite |
| `WECHAT_OUTBOUND_TICK_MS` | `3000` | 出站 ticker 间隔 |
| `WECHAT_OUTBOUND_MAX_TEXT_LEN` | `1600` | 最大文本块长度 |
| `WECHAT_OFFLINE_QUEUE_PATH` | `<stateDir>/pending_outbound.json` | 离线队列路径 |

### 语音（阿里云 NLS，可选）

| 变量 | 说明 |
|------|------|
| `ALIYUN_NLS_AKID` | Access Key ID |
| `ALIYUN_NLS_AKKEY` | Access Key Secret |
| `ALIYUN_NLS_APPKEY` | NLS 项目 App Key |

### 代理隧道

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `PROXY_HUB_WS_URL` | — | 中继 Hub WS URL |
| `PROXY_KEY_FILE` | `.opencode-gateway-proxy.json` | 密钥文件 |
| `PROXY_LOCAL_OPENCODE_ADDR` | `127.0.0.1:4096` | 本地 OpenCode 地址 |
| `PROXY_RECONNECT_DELAY` | `5s` | 重连延迟 |

### 技能自动生成

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `SKILLGEN_ENABLED` | `false` | 总开关 |
| `SKILLGEN_DRAFT_MODEL` | — | 首选模型 |
| `SKILLGEN_ALTERNATE_MODELS` | — | 备选模型池（逗号分隔） |
| `SKILLGEN_EPSILON` | `0.15` | 探索概率 |
| `SKILLGEN_MODEL_SELF_SELECT` | `true` | 启用 epsilon-greedy |
| `SKILLGEN_MAX_PER_DAY` | `5` | 每日上限 |
| `SKILLGEN_ON_HANDOFF` | `true` | Handoff 时挖掘 |
| `SKILLGEN_ON_LONG_SESSION` | `true` | 长会话时挖掘 |
| `SKILLGEN_LONG_SESSION_MIN_TURNS` | `8` | 最小轮次 |
| `SKILLGEN_MIN_TOOL_CALLS` | `3` | 最小工具调用数 |
| `SKILLGEN_CANDIDATE_DIR` | `skills-candidates` | 草稿目录 |
| `SKILLGEN_INSTALL_DIR` | `skills` | 安装目录 |
| `SKILLGEN_APPROVAL_REQUIRED` | `true` | 需要人工审核 |
| `SKILLGEN_MIN_CONFIDENCE` | `0.4` | 最低置信度 |
| `SKILLGEN_QUEUE_CAPACITY` | `128` | 异步队列容量 |
| `SKILLGEN_REFERENCE_SKILL` | `skills/skill-creator/SKILL.md` | 参考技能路径 |

### 重试队列

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `RETRY_QUEUE_ENABLED` | `false` | 启用离峰重试 |
| `RETRY_QUEUE_CRON` | `0 22 * * *` | 运行时间 |
| `RETRY_QUEUE_MAX_RETRIES` | `3` | 每条消息最大重试 |
| `RETRY_QUEUE_BATCH_SIZE` | `20` | 每次批量大小 |

---

## Docker 部署

### docker-compose

```bash
# 构建并启动
docker-compose up -d

# 查看日志
docker-compose logs -f openbot
```

`docker-compose.yml` 包含两个服务：

- **`opencode`** — OpenCode 服务器（端口 3000）
- **`openbot`** — 网关（端口 8080，依赖 opencode 健康检查）

### 手动 Docker

```bash
make docker-build
docker run --rm --env-file .env -p 8080:8080 opencode-gateway:latest
```

### systemd 部署

```ini
# /etc/systemd/system/openbot.service
[Unit]
Description=OpenBot Service
After=network.target

[Service]
Type=simple
ExecStart=/usr/local/bin/openbot
EnvironmentFile=/etc/systemd/openbot.env
Restart=always
RestartSec=5
User=root
StandardOutput=append:/var/log/openbot.log
StandardError=append:/var/log/openbot.log

[Install]
WantedBy=multi-user.target
```

```bash
systemctl daemon-reload
systemctl enable openbot
systemctl start openbot
journalctl -u openbot -f
```

---

## 项目结构

```
opencode-gateway/
├── cmd/                            # 入口点
│   ├── gateway/main.go             # 主网关 openbot
│   ├── mcp/main.go                 # MCP 服务器 openbot-mcp
│   ├── attach/main.go              # attach 桥接 openbot-attach
│   └── wechat-login/main.go        # 微信登录 openbot-wechat
├── internal/
│   ├── adapters/                   # 平台适配器
│   │   ├── base/                   # 双向适配器基类 + 注册表
│   │   ├── dingtalk/               # 钉钉（Stream + Webhook + AI卡片）
│   │   ├── feishu/                 # 飞书（WebSocket + Webhook）
│   │   ├── wechat/                 # 个人微信（iLink + 出站队列 + 离线队列）
│   │   └── wecom/                  # 企业微信
│   ├── asyncwork/                  # 异步任务队列（handoff/skillgen）
│   ├── config/                     # 环境变量配置
│   ├── memstore/                   # SQLite 记忆库（FTS5 + 遗忘曲线）
│   ├── opencode/                   # OpenCode SDK 封装 + SSE 事件监听
│   ├── opencodesvc/                # opencode 子进程管理 + 自动重启
│   ├── proxy/                      # WS 隧道 + UI RPC
│   ├── retryworker/                # 离峰重试工作器
│   ├── scheduler/                  # Cron + 任务调度 + 自然语言调度
│   ├── server/                     # HTTP 服务器
│   ├── skillgen/                   # 技能自动生成
│   └── uibrpc/                     # 远程 WebUI JSON-RPC
├── skills/                         # 内置技能
├── skill-install/                  # 技能搜索安装 CLI
├── tools/                          # 构建工具 + 中继 Hub + 代理
├── docs/                           # 文档
├── test/                           # 调试工具
├── Dockerfile
├── docker-compose.yml
├── Makefile
├── go.mod
└── .env.example
```

---

## 构建命令

```bash
make build              # 构建所有二进制
make build-gateway      # 仅构建 openbot
make build-mcp          # 仅构建 openbot-mcp
make build-attach       # 仅构建 openbot-attach
make build-wechat       # 仅构建 openbot-wechat
make build-linux        # 交叉编译 linux/amd64
make build-darwin       # 交叉编译 macOS/amd64
make build-darwin-arm64 # 交叉编译 macOS/Apple Silicon
make build-windows      # 交叉编译 windows/amd64
make build-all          # 交叉编译所有平台
make install            # 安装到 $GOPATH/bin
make run                # 构建并运行
make run-env            # 加载 .env 并运行
make init-env           # 从模板生成 .env
make test               # 运行测试
make vet                # go vet
make lint               # golangci-lint
make docker-build       # Docker 构建
make docker-run         # Docker 运行
make clean              # 清理构建产物
make help               # 显示帮助
```

---

## 文档索引

| 文档 | 说明 |
|------|------|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | 架构设计、组件概览、适配器开发指南 |
| [docs/API.md](docs/API.md) | HTTP REST API 完整参考 |
| [docs/COMMANDS.md](docs/COMMANDS.md) | 聊天命令完整参考 |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | 会话管理配置与场景预设 |
| [docs/CRONTASK_COMMAND.md](docs/CRONTASK_COMMAND.md) | 定时任务命令参考 |
| [docs/DEPLOY.md](docs/DEPLOY.md) | 部署指南（Docker/systemd/Nginx） |
| [docs/MCP_MEMORY_TOOL.md](docs/MCP_MEMORY_TOOL.md) | MCP 记忆工具配置 |
| [docs/MEDIA_MESSAGE_SUPPORT.md](docs/MEDIA_MESSAGE_SUPPORT.md) | 多媒体消息支持 |
| [docs/PROXY_TUNNEL.md](docs/PROXY_TUNNEL.md) | 代理隧道架构与配置 |
| [docs/QUICK_START_CRONTASK.md](docs/QUICK_START_CRONTASK.md) | 定时任务快速上手 |
| [docs/SCHEDULER_GUIDE.md](docs/SCHEDULER_GUIDE.md) | 调度器架构与 API |

---

## 故障排查

### 网关无法连接 OpenCode

```bash
# 检查 OpenCode 是否运行
curl http://localhost:4096/health

# 检查网关日志
tail -f /var/log/openbot.log | grep -E "error|fail|refuse"
```

### 钉钉 Stream 连接失败

- 确认 `DINGTALK_CLIENT_ID` 和 `DINGTALK_CLIENT_SECRET` 正确
- 确认钉钉应用已启用 Stream 模式
- 检查网络是否能访问钉钉 WebSocket 服务

### 飞书消息收不到

- WebSocket 模式：确认 `FEISHU_USE_WEBSOCKET=true`
- Webhook 模式：确认公网回调地址可访问，`FEISHU_VERIFICATION_TOKEN` 正确

### 微信限流频繁

- 微信 iLink API 有严格的速率限制
- 网关已内置自适应冷却（30s→60s→120s）
- 默认只发送最终结果和 todo 更新，不发送中间进度
- 检查出站队列状态：日志中搜索 `outbound queue stats`

### 技能生成失败

- 确认 `SKILLGEN_ENABLED=true`
- 确认模型在 OpenCode 中可用：`/models`
- 检查 `SKILLGEN_INSTALL_DIR` 路径存在且可写
- 查看日志：`grep skillgen /var/log/openbot.log`

### 会话卡死

- 发送 `/status` 查看会话状态
- 发送 `/abort` 中止当前任务
- 发送 `/new` 创建新会话
- 严重时发送 `/clear` 清除会话

---

## 许可证

[MIT License](LICENSE)

```
MIT License

Copyright (c) 2026 OpenCode Gateway Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

内置的 `skills/skill-creator/` 使用 [Apache License 2.0](skills/skill-creator/LICENSE.txt)。
