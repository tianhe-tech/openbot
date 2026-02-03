# OpenCode Gateway API 文档

## 概述

OpenCode Gateway 提供 HTTP REST API 用于管理任务调度和定时任务调用 OpenCode。

**Base URL**: `http://localhost:8080`（可通过 `SERVER_ADDR` 环境变量配置）

---

## 健康检查

### GET /healthz

**作用**：检查服务是否正常运行

**响应**：
```
ok
```

### GET /health

**作用**：检查健康状态（调度器相关）

**响应**：
```json
{
  "status": "healthy",
  "tasks": {
    "active": 0,
    "queued": 0,
    "completed": 5
  },
  "scheduled_tasks": 2
}
```

---

## 任务管理 API

### POST /api/tasks/submit

**作用**：提交一个新的 OpenCode 任务

**请求体**：
```json
{
  "user_id": "user123",
  "content": "帮我分析这个项目的架构",
  "channel": "dingtalk",
  "agent": "build",
  "metadata": {
    "extra_info": "可选的附加信息"
  }
}
```

**字段说明**：
- `user_id` (required): 用户ID
- `content` (required): 发送给 OpenCode 的消息内容
- `channel` (optional): 渠道名称（如 "dingtalk", "feishu", "wecom"）
- `agent` (optional): 指定使用的 agent/skill 名称
- `metadata` (optional): 附加元数据

**响应**：
```json
{
  "task_id": "task_abc123",
  "status": "pending",
  "submitted_at": "2024-02-03T12:00:00Z"
}
```

---

### GET /api/tasks/{task_id}

**作用**：获取任务详情

**路径参数**：
- `task_id`: 任务ID

**响应**：
```json
{
  "task_id": "task_abc123",
  "user_id": "user123",
  "content": "帮我分析这个项目的架构",
  "status": "running",
  "created_at": "2024-02-03T12:00:00Z",
  "started_at": "2024-02-03T12:00:05Z",
  "result": null,
  "error": null
}
```

**状态枚举**：
- `pending`: 等待执行
- `running`: 正在执行
- `completed`: 已完成
- `failed`: 执行失败

---

### GET /api/tasks/active

**作用**：获取所有正在执行的任务

**响应**：
```json
{
  "tasks": [
    {
      "task_id": "task_abc123",
      "user_id": "user123",
      "status": "running"
    }
  ]
}
```

---

### GET /api/tasks/history

**作用**：获取任务历史记录

**查询参数**：
- `limit`: 返回记录数（默认 10）
- `offset`: 偏移量（默认 0）

**响应**：
```json
{
  "total": 100,
  "tasks": [
    {
      "task_id": "task_abc123",
      "user_id": "user123",
      "status": "completed",
      "completed_at": "2024-02-03T12:01:00Z"
    }
  ]
}
```

---

### GET /api/tasks/stats

**作用**：获取任务统计信息

**响应**：
```json
{
  "total": 150,
  "pending": 0,
  "running": 1,
  "completed": 140,
  "failed": 9,
  "success_rate": 94.0
}
```

---

## 定时任务 API

### GET /api/scheduled-tasks

**作用**：获取所有定时任务

**响应**：
```json
{
  "tasks": [
    {
      "id": "daily_report",
      "cron": "0 8 * * *",
      "description": "每日报告",
      "enabled": true,
      "next_run": "2024-02-04T08:00:00Z",
      "task": {
        "user_id": "user123",
        "content": "生成每日报告"
      }
    }
  ]
}
```

---

### POST /api/scheduled-tasks

**作用**：创建新的定时任务

**请求体**：
```json
{
  "id": "daily_report",
  "cron": "0 8 * * *",
  "task": {
    "user_id": "user123",
    "content": "生成每日报告",
    "channel": "dingtalk"
  }
}
```

**Cron 格式**：`分 时 日 月 周`

---

### POST /api/scheduled-tasks/{task_id}

**作用**：更新定时任务

**路径参数**：
- `task_id`: 任务ID

**请求体**：同创建定时任务

---

### DELETE /api/scheduled-tasks/{task_id}

**作用**：删除定时任务

**路径参数**：
- `task_id`: 任务ID

---

### POST /api/scheduled-tasks/enable/{task_id}

**作用**：启用定时任务

**响应**：
```json
{
  "id": "daily_report",
  "enabled": true
}
```

---

### POST /api/scheduled-tasks/disable/{task_id}

**作用**：禁用定时任务

**响应**：
```json
{
  "id": "daily_report",
  "enabled": false
}
```

---

## 适配器 API

### POST /api/adapters/register

**作用**：注册新的适配器

**请求体**：
```json
{
  "name": "slack",
  "type": "webhook",
  "config": {
    "webhook_url": "https://hooks.slack.com/services/xxx"
  }
}
```

---

## 错误码

| 状态码 | 说明 |
|--------|------|
| 200 | 成功 |
| 400 | 请求参数错误 |
| 404 | 资源不存在 |
| 405 | 方法不允许 |
| 500 | 服务器内部错误 |

**错误响应格式**：
```json
{
  "error": "Invalid request body",
  "details": "content field is required"
}
```

---

## 钉钉 Webhook

### POST /dingtalk/callback

**作用**：接收钉钉机器人消息回调

**请求头**：
- `Content-Type: application/json`

**请求体**：
```json
{
  "msgId": "msg123",
  "text": {
    "content": "帮我写个Python脚本"
  },
  "senderNick": "张三",
  "senderStaffId": "1601434517956472",
  "conversationId": "conv123"
}
```

---

## 飞书 Webhook

### POST /feishu/callback

**作用**：接收飞书机器人消息回调

**请求体**：
```json
{
  "type": "event_callback",
  "event": {
    "message": {
      "content": "{\"text\":\"帮我写个Python脚本\"}",
      "message_id": "msg123"
    },
    "sender": {
      "sender_id": {
        "open_id": "ou_abc123"
      }
    }
  }
}
```

---

## 企业微信 Webhook

### POST /wecom/callback

**作用**：接收企业微信机器人消息回调

**请求体**：
```json
{
  "ToUserName": "bot",
  "FromUserName": "user123",
  "MsgType": "text",
  "Content": "帮我写个Python脚本"
}
```

---

## 注意事项

1. **超时设置**：默认任务超时时间为 30 分钟，可通过配置调整
2. **并发限制**：默认最大并发任务数为 10，队列大小为 1000
3. **Cron 表达式**：使用标准 Cron 格式（6字段或5字段皆可）
4. **流式输出**：任务结果通过 SSE 事件流实时返回
5. **权限确认**：OpenCode 在需要权限时会发送 `permission.asked` 事件

---

## 常见问题

### Q: 如何调试 API 请求？

A: 查看服务器日志，查看详细的请求和错误信息

### Q: 任务提交后如何获取结果？

A: 通过 SSE 事件流实时监听，或轮询 `/api/tasks/{task_id}` 端点

### Q: 定时任务没有执行怎么办？

A: 检查:
1. Cron 表达式是否正确
2. 任务是否启用 (`enabled: true`)
3. 服务器时区设置

### Q: 如何停止正在运行的任务？

A: 发送 `/abort` 命令给机器人（如钉钉发送 `/abort`）