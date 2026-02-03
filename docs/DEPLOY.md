# OpenCode Gateway 部署使用指南

## 概述

OpenCode Gateway 是一个将消息平台（钉钉、飞书、企业微信）与 OpenCode AI 编程助手连接起来的网关服务。

---

## 系统要求

- **Go**: 1.21 或更高版本
- **OpenCode Server**: 运行在 `http://localhost:4096`（可配置）
- **操作系统**: Linux / macOS / Windows

---

## 快速开始

### 1. 获取代码

```bash
git clone https://github.com/user/opencode-gateway.git
cd opencode-gateway
```

### 2. 配置环境变量

复制 `.env.example` 为 `.env` 并修改配置：

```bash
cp .env.example .env
```

**必需的配置项：**

```env
# OpenCode Server 配置
OPENCODE_ENDPOINT=http://localhost:4096
OPENCODE_API_KEY=your_api_key_here

# 服务监听地址
SERVER_ADDR=:8080

# 钉钉 Stream 模式配置（推荐）
DINGTALK_USE_STREAM=true
DINGTALK_CLIENT_ID=your_client_id
DINGTALK_CLIENT_SECRET=your_client_secret

# 或使用传统 Webhook 模式
DINGTALK_USE_STREAM=false
DINGTALK_VERIFICATION_TOKEN=your_token
DINGTALK_SIGNING_SECRET=your_secret

# 飞书配置
FEISHU_APP_ID=your_app_id
FEISHU_APP_SECRET=your_app_secret

# 企业微信配置
WECOM_CORP_ID=your_corp_id
WECOM_AGENT_ID=your_agent_id
WECOM_TOKEN=your_token
WECOM_AES_KEY=your_aes_key
```

### 3. 构建

```bash
go build -o bin/gateway cmd/gateway/main.go
```

### 4. 运行

```bash
./bin/gateway
```

或直接运行：

```bash
go run cmd/gateway/main.go
```

### 5. 验证

健康检查：
```bash
curl http://localhost:8080/healthz
```

---

## Docker 部署

### 1. 构建镜像

```bash
docker build -t opencode-gateway:latest .
```

### 2. 运行容器

```bash
docker run -d \
  --name opencode-gateway \
  -p 8080:8080 \
  --env-file .env \
  opencode-gateway:latest
```

### 3. 使用 Docker Compose

```bash
docker-compose up -d
```

`docker-compose.yml` 示例：

```yaml
version: '3.8'
services:
  gateway:
    build: .
    ports:
      - "8080:8080"
    environment:
      - OPENCODE_ENDPOINT=http://opencode:3000
      - DINGTALK_CLIENT_ID=${DINGTALK_CLIENT_ID}
      - DINGTALK_CLIENT_SECRET=${DINGTALK_CLIENT_SECRET}
      - DINGTALK_USE_STREAM=true
    depends_on:
      - opencode
  opencode:
    image: anomaly/opencode:latest
    ports:
      - "4096:4096"
```

---

## 配置说明

### 环境变量

| 变量名 | 必需 | 默认值 | 说明 |
|--------|------|--------|------|
| `OPENCODE_ENDPOINT` | 是 | - | OpenCode Server 地址 |
| `OPENCODE_API_KEY` | 可选 | - | OpenCode API 密钥 |
| `SERVER_ADDR` | 否 | `:8080` | 服务监听地址 |
| `READ_TIMEOUT` | 否 | `30s` | 读取超时 |
| `WRITE_TIMEOUT` | 否 | `30s` | 写入超时 |
| `SHUTDOWN_GRACE` | 否 | `30s` | 优雅关闭超时 |

### 钉钉配置

| 变量名 | 说明 |
|--------|------|
| `DINGTALK_USE_STREAM` | 是否使用 Stream 模式（推荐 `true`） |
| `DINGTALK_CLIENT_ID` | Stream 模式需要的 Client ID |
| `DINGTALK_CLIENT_SECRET` | Stream 模式需要的 Client Secret |
| `DINGTALK_VERIFICATION_TOKEN` | Webhook 模式验证 Token |
| `DINGTALK_SIGNING_SECRET` | Webhook 模式签名密钥 |

### 飞书配置

| 变量名 | 说明 |
|--------|------|
| `FEISHU_APP_ID` | 飞书应用 ID |
| `FEISHU_APP_SECRET` | 飞书应用密钥 |

### 企业微信配置

| 变量名 | 说明 |
|--------|------|
| `WECOM_CORP_ID` | 企业 ID |
| `WECOM_AGENT_ID` | 应用 ID |
| `WECOM_TOKEN` | Webhook Token |
| `WECOM_AES_KEY` | 加密密钥 |

---

## 钉钉机器人配置

### Stream 模式（推荐）

1. **创建应用**
   - 访问 [阿里云开放平台](https://open.dingtalk.com/)
   - 创建「企业内部机器人」应用

2. **获取凭证**
   - Client ID (AppKey)
   - Client Secret (AppSecret)

3. **配置权限**
   - 开启「机器人」能力
   - 配置应用可见范围

4. **配置环境变量**
   ```env
   DINGTALK_USE_STREAM=true
   DINGTALK_CLIENT_ID=your_client_id
   DINGTALK_CLIENT_SECRET=your_client_secret
   ```

### Webhook 模式

1. **创建机器人**
   - 钉钉群 -> 智能群助手 -> 自定义机器人

2. **安全设置**
   - 选择「加签」方式

3. **配置环境变量**
   ```env
   DINGTALK_USE_STREAM=false
   DINGTALK_VERIFICATION_TOKEN=your_token
   DINGTALK_SIGNING_SECRET=your_secret
   ```

---

## 使用示例

### 基本用法

在钉钉群中发送消息：

```
帮我写一个 Python 爬虫
```

Gateway 会：
1. 接收消息
2. 创建 OpenCode 会话
3. 将消息转发给 OpenCode
4. 将 OpenCode 的回复返回给钉钉

### 使用 Agent/Skill

```
@build 帮我编译这个项目
```

可用的 Agent：
- `@build` - 构建模式
- `@plan` - 规划模式
- `@chat` - 普通对话模式

### 定时任务命令

```
/cron add "0 9 * * *" "生成每日报告"
```

其他命令：
- `/cron list` - 列出所有定时任务
- `/cron enable <id>` - 启用定时任务
- `/cron disable <id>` - 禁用定时任务
- `/cron delete <id>` - 删除定时任务
- `/cron info <id>` - 查看定时任务详情

### 快捷命令

| 命令 | 作用 |
|------|------|
| `/skills` | 列出可用的 Skills |
| `/help` | 显示帮助信息 |
| `/abort` | 中止当前任务 |
| `/cmd <command>` | 直接执行 Shell 命令 |

---

## 生产环境部署

### 1. 使用 systemd

创建 `/etc/systemd/system/opencode-gateway.service`:

```ini
[Unit]
Description=OpenCode Gateway
After=network.target

[Service]
User=opencode
Group=opencode
WorkingDirectory=/opt/opencode-gateway
ExecStart=/opt/opencode-gateway/bin/gateway
Restart=always
RestartSec=10
EnvironmentFile=/opt/opencode-gateway/.env

[Install]
WantedBy=multi-user.target
```

启动服务：
```bash
sudo systemctl daemon-reload
sudo systemctl enable opencode-gateway
sudo systemctl start opencode-gateway
sudo systemctl status opencode-gateway
```

### 2. 使用 Nginx 反向代理

nginx 配置：
```nginx
server {
    listen 80;
    server_name gateway.example.com;

    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }

    # 钉钉 Webhook
    location /dingtalk/callback {
        proxy_pass http://localhost:8080;
    }
}
```

### 3. 日志管理

使用 `journalctl` 查看日志：
```bash
sudo journalctl -u opencode-gateway -f
```

---

## 监控与运维

### 健康检查

```bash
curl http://localhost:8080/healthz
```

### 查看任务统计

```bash
curl http://localhost:8080/api/tasks/stats
```

### 查看定时任务

```bash
curl http://localhost:8080/api/scheduled-tasks
```

---

## 故障排查

### 服务无法启动

检查：
1. Go 版本是否 ≥ 1.21
2. OpenCode Server 是否运行
3. 端口是否被占用
4. 环境变量配置是否正确

### 钉钉/Webhook 不工作

检查：
1. `DINGTALK_USE_STREAM` 设置是否正确
2. Client ID/Secret 是否正确
3. 防火墙是否放行 8080 端口
4. 查看服务器日志获取详细错误信息

### 任务超时

解决方案：
1. 增加 `TASK_TIMEOUT` 环境变量
2. 在钉钉中发送 `/abort` 中止任务
3. 检查 OpenCode Server 是否正常响应

---

## 附录

### 相关文档

- [API 参考文档](./API.md)
- [架构说明](../ARCHITECTURE.md)
- [配置指南](./CONFIGURATION.md)
- [定时任务指南](./SCHEDULER_GUIDE.md)

### 支持的频道

- 钉钉 - Stream 模式 / Webhook 模式
- 飞书 - Webhook 模式
- 企业微信 - Webhook 模式

### 开源许可

MIT License