# OpenCode Gateway

A lightweight Go service that keeps enterprise chat platforms (Enterprise WeChat, FeiShu, DingTalk) connected to OpenCode. Each adapter handles the platform-specific callback contract and forwards user messages to OpenCode over HTTP, exposing the AI agent as a remote bot inside corporate chat tools.

## Features

- Separate adapters for Enterprise WeChat, FeiShu, and DingTalk, each mounted on its own callback URL.
- Simple health endpoint for load balancers: `GET /healthz`.
- Centralized OpenCode client with pluggable HTTP transport.
- Runtime configuration via environment variables.

## Getting Started

```bash
# from the project root
export OPENCODE_ENDPOINT="https://opencode.company"
export OPENCODE_API_KEY="your-api-key"
export WECOM_TOKEN="token"
export WECOM_AES_KEY="aes-key"
export WECOM_CORP_ID="corp-id"
export WECOM_AGENT_ID="agent-id"
export FEISHU_APP_ID="app-id"
export FEISHU_APP_SECRET="app-secret"
export FEISHU_VERIFICATION_TOKEN="token"
export FEISHU_ENCRYPT_KEY="encrypt"
export DINGTALK_APP_KEY="app-key"
export DINGTALK_APP_SECRET="app-secret"
export DINGTALK_VERIFICATION_TOKEN="token"
export DINGTALK_ENCRYPT_KEY="encrypt"
export DINGTALK_SIGNING_SECRET="signing-secret"

go run ./cmd/gateway
```

## Configuration Reference

| Variable | Description | Default |
| --- | --- | --- |
| `SERVER_ADDR` | HTTP bind address | `:8080` |
| `SERVER_READ_TIMEOUT` | Request read timeout (Go duration or seconds) | `10s` |
| `SERVER_WRITE_TIMEOUT` | Response write timeout | `10s` |
| `SERVER_SHUTDOWN_GRACE` | Graceful shutdown budget | `15s` |
| `OPENCODE_ENDPOINT` | Base URL used to reach OpenCode | _(required)_ |
| `OPENCODE_API_KEY` | Bearer token for OpenCode | _(required)_ |
| `WECOM_*` | Enterprise WeChat adapter settings | _(optional)_ |
| `FEISHU_*` | FeiShu adapter settings | _(optional)_ |
| `DINGTALK_*` | DingTalk adapter settings | _(optional)_ |

## Project Layout

```
cmd/gateway      - service entrypoint
internal/config  - env parsing helpers
internal/server  - HTTP server wrapper
internal/opencode- remote client abstraction
internal/adapters- platform-specific integrations
```

## Next Steps

- Implement signature verification and message encryption for each adapter.
- Add WebSocket support toward OpenCode for streaming replies.
- Persist conversation state for better continuity across platforms.
