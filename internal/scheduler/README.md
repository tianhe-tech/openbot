# 任务调度系统

完整的任务调度和定时任务管理系统，支持从多个adapter接收任务，统一调度到OpenCode执行。

## 🎯 核心功能

### 1. 统一任务调度
- 接收来自所有adapter（钉钉、飞书、企业微信等）的任务
- 任务队列管理，支持优先级
- 并发控制，可配置最大并发任务数
- 自动重试机制
- 任务历史记录和状态追踪

### 2. 定时任务
- 支持标准Cron表达式（秒级精度）
- 多种任务类型：
  - 固定脚本执行
  - 智能体流程
  - 社交媒体监控
  - 自动化报告生成
- 任务启用/禁用控制
- 执行历史和状态监控

### 3. HTTP API
- RESTful API接口
- 任务提交和查询
- 定时任务管理
- Adapter注册和管理
- 实时统计信息

## 📦 组件架构

```
┌─────────────────────────────────────────────────────────────┐
│                        HTTP API                              │
│              (Webhook Handler - Port 8080)                   │
└────────────────────────┬────────────────────────────────────┘
                         │
         ┌───────────────┴───────────────┐
         │                               │
┌────────▼─────────┐          ┌─────────▼──────────┐
│  Task Scheduler  │          │  Cron Scheduler     │
│  (任务调度器)     │          │  (定时任务调度器)    │
│                  │          │                     │
│ • 任务队列        │          │ • Cron表达式        │
│ • 并发控制        │          │ • 定时触发          │
│ • 优先级管理      │◄─────────│ • 任务管理          │
│ • 重试机制        │          │                     │
└────────┬─────────┘          └─────────────────────┘
         │
         │ 创建Session执行任务
         │
┌────────▼─────────────────────────────────────────┐
│           OpenCode Client                        │
│        (与OpenCode Server通信)                   │
└────────┬─────────────────────────────────────────┘
         │
         │ 结果回调
         │
┌────────▼─────────┐
│    Adapters      │
│ • DingTalk       │
│ • Feishu         │
│ • WeCom          │
└──────────────────┘
```

## 🚀 快速开始

### 安装依赖

```bash
go get github.com/robfig/cron/v3
```

### 基本使用

```go
// 1. 创建OpenCode客户端
opencodeClient := opencode.NewClient(endpoint, apiKey)

// 2. 创建任务调度器
taskScheduler := scheduler.NewTaskScheduler(opencodeClient, 
    scheduler.DefaultTaskSchedulerConfig())

// 3. 创建定时任务调度器
cronScheduler := scheduler.NewCronScheduler(taskScheduler)

// 4. 创建Webhook处理器
webhookHandler := scheduler.NewWebhookHandler(taskScheduler, cronScheduler)

// 5. 启动服务
taskScheduler.Start()
cronScheduler.Start()

// 6. 注册路由
webhookHandler.RegisterRoutes(mux)
```

详细示例请参考 [examples/scheduler_integration.go](../examples/scheduler_integration.go)

## 📖 API文档

完整的API文档请参考 [docs/SCHEDULER_GUIDE.md](../docs/SCHEDULER_GUIDE.md)

### 快速API示例

#### 提交任务
```bash
curl -X POST http://localhost:8080/api/tasks/submit \
  -H "Content-Type: application/json" \
  -d '{
    "type": "agent",
    "adapter_type": "dingtalk",
    "user_id": "user123",
    "content": "帮我分析这段代码",
    "agent": "code_review"
  }'
```

#### 创建定时任务
```bash
curl -X POST http://localhost:8080/api/scheduled-tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "每日代码检查",
    "cron_expr": "0 0 9 * * *",
    "type": "agent",
    "enabled": true,
    "adapter_type": "dingtalk",
    "channel": "dev_group",
    "content": "执行代码质量检查",
    "agent": "code_analyzer"
  }'
```

#### 查看统计
```bash
curl http://localhost:8080/api/tasks/stats
```

## 🔧 配置说明

```go
TaskSchedulerConfig{
    MaxConcurrentTasks: 10,              // 最大并发任务数
    MaxQueueSize:       1000,            // 最大队列长度
    SessionPoolSize:    20,              // Session池大小（未来使用）
    TaskTimeout:        30 * time.Minute,// 单个任务超时时间
    CleanupInterval:    1 * time.Hour,   // 历史清理间隔
}
```

## 💡 使用场景

### 1. 社交媒体自动监控
```json
{
  "name": "Twitter话题监控",
  "cron_expr": "0 0 * * * *",
  "type": "monitoring",
  "content": "监控Twitter #AI话题，发现新内容自动推送",
  "agent": "social_monitor"
}
```

### 2. 定时代码部署
```json
{
  "name": "每日自动部署",
  "cron_expr": "0 0 18 * * 1-5",
  "type": "script",
  "script_path": "./scripts/deploy.sh"
}
```

### 3. 智能日报生成
```json
{
  "name": "项目日报",
  "cron_expr": "0 0 9 * * 1-5",
  "type": "agent",
  "content": "分析昨天的项目进展，生成日报",
  "agent": "report_generator"
}
```

## 📝 任务类型

- `message` - 普通消息任务
- `script` - Shell脚本执行
- `agent` - 智能体流程
- `monitoring` - 监控任务（如社交媒体）
- `cron` - 定时任务标识

## 🔍 监控和调试

### 查看活跃任务
```bash
curl http://localhost:8080/api/tasks/active
```

### 查看任务历史
```bash
curl http://localhost:8080/api/tasks/history
```

### 查看所有定时任务
```bash
curl http://localhost:8080/api/scheduled-tasks
```

### 查看系统统计
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
  "total_scheduled_tasks": 8,
  "enabled_tasks": 6
}
```

## 🛡️ 最佳实践

1. **合理设置并发数**：根据服务器资源和OpenCode负载设置`MaxConcurrentTasks`
2. **任务超时配置**：长时间任务需要调整`TaskTimeout`
3. **使用优先级**：重要任务设置更高优先级
4. **监控队列长度**：队列过长说明处理能力不足
5. **定期查看统计**：通过API监控系统运行状态
6. **合理设置Cron**：避免定时任务过于频繁

## 🔄 集成步骤

1. **创建调度器实例**
2. **注册adapter**到任务调度器
3. **配置webhook路由**
4. **添加定时任务**
5. **启动所有服务**

完整代码示例：[examples/scheduler_integration.go](../examples/scheduler_integration.go)

## 📚 更多文档

- [完整API文档](../docs/SCHEDULER_GUIDE.md)
- [集成示例](../examples/scheduler_integration.go)
- [架构设计](../ARCHITECTURE.md)

## 🤝 贡献

欢迎提交Issue和Pull Request！

## 📄 License

MIT License
