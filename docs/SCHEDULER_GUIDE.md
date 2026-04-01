# 任务调度系统使用指南

## 系统架构

任务调度系统由三个核心组件组成：

1. **TaskScheduler** - 任务调度器，管理任务队列和执行
2. **CronScheduler** - 定时任务调度器，支持cron表达式
3. **WebhookHandler** - HTTP API接口，用于管理任务和适配器

## 功能特性

### 1. 任务调度器（TaskScheduler）
- ✅ 统一接收来自各adapter的任务
- ✅ 支持多种任务类型（消息、脚本、智能体、监控）
- ✅ 任务队列和优先级管理
- ✅ 并发控制（可配置最大并发数）
- ✅ 自动重试机制
- ✅ 任务历史记录
- ✅ 支持多个OpenCode session

### 2. 定时任务调度器（CronScheduler）
- ✅ 支持标准Cron表达式（秒级精度）
- ✅ 固定脚本执行
- ✅ 智能体流程执行
- ✅ 社交媒体监控等自动化任务
- ✅ 任务启用/禁用
- ✅ 执行历史和状态追踪

### 4. 自然语言调度层（NLScheduleService）
- ✅ 三端统一（DingTalk/Feishu/WeCom）
- ✅ 自然语言意图识别（创建/列表/详情/启停/删除/试运行）
- ✅ 写操作草稿确认（确认/取消）
- ✅ 通过 adapter 元数据回写来源上下文

自然语言入口：
- 直接聊天文本（如“每天早上9点提醒我看监控”）
- `/crontask <自然语言描述>` 兜底

确认机制：
1. 首次命中写操作 -> 返回草稿
2. 用户回复 `确认`/`yes`/`1` -> 执行
3. 用户回复 `取消`/`no`/`0` -> 丢弃

### 3. Webhook API
- ✅ RESTful API接口
- ✅ 任务提交、查询、统计
- ✅ 定时任务管理（增删改查）
- ✅ Adapter注册

## API文档

### 任务管理

#### 提交任务
```bash
POST /api/tasks/submit
Content-Type: application/json

{
  "type": "message",
  "adapter_type": "dingtalk",
  "user_id": "user123",
  "channel": "conversation_id",
  "content": "执行某个任务",
  "agent": "build",
  "priority": 10
}
```

#### 查询任务
```bash
GET /api/tasks/{task_id}
```

#### 列出活跃任务
```bash
GET /api/tasks/active
```

#### 查看任务历史
```bash
GET /api/tasks/history
```

#### 获取统计信息
```bash
GET /api/tasks/stats
```

### 定时任务管理

#### 创建定时任务
```bash
POST /api/scheduled-tasks
Content-Type: application/json

{
  "name": "每日代码检查",
  "description": "每天上午9点自动执行代码质量检查",
  "type": "agent",
  "cron_expr": "0 0 9 * * *",
  "enabled": true,
  "adapter_type": "dingtalk",
  "channel": "tech_group",
  "content": "执行每日代码质量检查",
  "agent": "code_review"
}
```

#### Cron表达式格式
```
秒 分 时 日 月 周

示例：
"0 0 9 * * *"     - 每天9点
"0 */30 * * * *"  - 每30分钟
"0 0 12 * * 1-5"  - 周一到周五中午12点
"0 0 0 1 * *"     - 每月1号零点
```

#### 列出所有定时任务
```bash
GET /api/scheduled-tasks
```

#### 获取单个定时任务
```bash
GET /api/scheduled-tasks/{task_id}
```

#### 更新定时任务
```bash
PUT /api/scheduled-tasks/{task_id}
Content-Type: application/json

{
  "name": "每日代码检查（更新）",
  "cron_expr": "0 0 10 * * *",
  "enabled": true
}
```

#### 删除定时任务
```bash
DELETE /api/scheduled-tasks/{task_id}
```

#### 启用定时任务
```bash
POST /api/scheduled-tasks/enable/{task_id}
```

#### 禁用定时任务
```bash
POST /api/scheduled-tasks/disable/{task_id}
```

### Adapter管理

#### 注册Adapter
```bash
POST /api/adapters/register
Content-Type: application/json

{
  "adapter_type": "dingtalk",
  "webhook_url": "https://your-server.com/dingtalk/webhook"
}
```

### 健康检查
```bash
GET /health
```

## 使用场景示例

### 场景1：社交媒体监控
创建一个定时任务，每小时检查Twitter上的特定话题，有新内容时自动发送到钉钉群：

```json
{
  "name": "Twitter话题监控",
  "description": "监控#AI话题，发现新内容自动推送",
  "type": "monitoring",
  "cron_expr": "0 0 * * * *",
  "enabled": true,
  "adapter_type": "dingtalk",
  "channel": "ai_news_group",
  "content": "搜索Twitter #AI话题的最新内容，如果有新内容，总结并发送",
  "agent": "social_monitor",
  "metadata": {
    "platform": "twitter",
    "keyword": "#AI",
    "language": "zh"
  }
}
```

### 场景2：定时代码部署
每天下午6点自动执行部署脚本：

```json
{
  "name": "每日自动部署",
  "description": "自动部署到测试环境",
  "type": "script",
  "cron_expr": "0 0 18 * * 1-5",
  "enabled": true,
  "adapter_type": "dingtalk",
  "channel": "devops_group",
  "script_path": "./scripts/deploy-test.sh"
}
```

### 场景3：智能日报生成
每个工作日上午9点生成项目进度报告：

```json
{
  "name": "项目日报",
  "description": "自动生成项目进度日报",
  "type": "agent",
  "cron_expr": "0 0 9 * * 1-5",
  "enabled": true,
  "adapter_type": "feishu",
  "channel": "project_group",
  "content": "分析昨天的Git提交、Issue状态、PR情况，生成项目进度日报",
  "agent": "report_generator"
}
```

## 任务类型说明

### TaskType
- `message` - 普通消息任务，发送到OpenCode处理
- `script` - 执行指定的shell脚本
- `agent` - 使用特定智能体处理任务
- `monitoring` - 监控类任务（如社交媒体监控）
- `cron` - 定时任务

### TaskPriority
- `0` (Low) - 低优先级
- `5` (Normal) - 普通优先级（默认）
- `10` (High) - 高优先级
- `15` (Urgent) - 紧急任务

### TaskStatus
- `pending` - 等待执行
- `running` - 执行中
- `completed` - 已完成
- `failed` - 失败
- `canceled` - 已取消

## 配置说明

```go
TaskSchedulerConfig{
    MaxConcurrentTasks: 10,              // 最大并发任务数
    MaxQueueSize:       1000,            // 最大队列长度
    SessionPoolSize:    20,              // Session池大小
    TaskTimeout:        30 * time.Minute,// 任务超时时间
    CleanupInterval:    1 * time.Hour,   // 清理间隔
}
```

## 集成示例

参见 `cmd/gateway/main.go` 中的完整集成示例。

## 注意事项

1. **任务超时**：默认30分钟超时，长时间任务请调整配置
2. **并发控制**：根据服务器资源调整MaxConcurrentTasks
3. **误触发防护**：自然语言解析已加入调度关键词与时间关键词双重门控，避免普通聊天误判为定时任务
4. **WeCom回推策略**：主动消息优先走群聊 `appchat/send`，失败后回退到用户消息 `message/send`
3. **重试机制**：失败任务会自动重试最多3次
4. **历史清理**：24小时前的任务历史会自动清理
5. **Cron精度**：支持秒级精度的cron表达式
6. **Adapter注册**：使用前需要注册adapter，才能接收结果回调

## 监控和调试

查看统计信息：
```bash
curl http://localhost:8080/api/tasks/stats
```

返回示例：
```json
{
  "active_tasks": 3,
  "queued_tasks": 5,
  "history_count": 42,
  "max_concurrent": 10,
  "max_queue_size": 1000,
  "registered_adapters": 2,
  "total_scheduled_tasks": 8,
  "enabled_tasks": 6,
  "disabled_tasks": 2,
  "cron_entries": 6
}
```
