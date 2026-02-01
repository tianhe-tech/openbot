# /crontask 功能快速启动指南

## 前置条件

1. OpenCode Server 正在运行（默认端口：4096）
2. 钉钉机器人已配置（需要 Client ID 和 Client Secret）
3. 已设置环境变量：
   - `DINGTALK_CLIENT_ID`
   - `DINGTALK_CLIENT_SECRET`

## 启动步骤

### 1. 构建并启动 Gateway

```bash
cd /Users/tiandoudou/work/openbot/cmd/gateway
go build
./gateway
```

你应该看到类似的输出：

```
2026/02/01 21:00:00 gateway: starting...
2026/02/01 21:00:00 scheduler: starting task scheduler with 10 workers
2026/02/01 21:00:00 cron-scheduler: starting cron scheduler
2026/02/01 21:00:00 dingtalk: connecting to stream server...
2026/02/01 21:00:00 opencode: connecting to OpenCode server at http://localhost:4096
2026/02/01 21:00:01 HTTP server listening on :8080
2026/02/01 21:00:01 gateway started successfully
```

### 2. 在钉钉中测试

#### 测试 1：查看帮助

在钉钉群中发送：

```
/crontask help
```

你应该收到完整的帮助信息。

#### 测试 2：添加简单的定时任务

```
/crontask add "0 */5 * * * *" "测试任务" "当前时间是多少？"
```

系统会返回：

```
✅ 定时任务创建成功！

任务 ID: task-1234567890
任务名称: 测试任务
Cron 表达式: 0 */5 * * * *
执行内容: 当前时间是多少？
下次执行: 2026-02-01 21:05:00

该任务将每5分钟执行一次。
```

#### 测试 3：列出所有任务

```
/crontask list
```

系统会返回：

```
📋 定时任务列表（共 1 个任务）

1. 📌 测试任务 (task-1234567890)
   Cron: 0 */5 * * * *
   状态: ✅ 已启用
   下次执行: 2026-02-01 21:05:00

---
💡 使用 /crontask info <任务ID> 查看详情
💡 使用 /crontask disable <任务ID> 禁用任务
```

#### 测试 4：查看任务详情

```
/crontask info task-1234567890
```

#### 测试 5：等待任务执行

等待 5 分钟，任务会自动执行，你会在钉钉群中收到 AI 的回复。

#### 测试 6：禁用任务

```
/crontask disable task-1234567890
```

系统返回：

```
✅ 定时任务已禁用

任务 ID: task-1234567890
任务名称: 测试任务
状态: ⏸️ 已禁用
```

#### 测试 7：启用任务

```
/crontask enable task-1234567890
```

#### 测试 8：删除任务

```
/crontask delete task-1234567890
```

系统返回：

```
✅ 定时任务已删除

任务 ID: task-1234567890
```

## 实用示例

### 系统监控

```
/crontask add "0 */30 * * * *" "系统监控" "查看系统负载、CPU、内存使用情况，如果有异常请提醒"
```

### 每日晨会提醒

```
/crontask add "0 0 9 * * 1-5" "晨会提醒" "提醒今天的站会时间为9:30，请准备好昨日工作总结和今日计划"
```

### 周报提醒

```
/crontask add "0 0 17 * * 5" "周报提醒" "提醒提交本周工作周报"
```

### 服务健康检查

```
/crontask add "0 */10 * * * *" "健康检查" "检查所有微服务是否正常运行"
```

## 验证功能

### 方法 1：通过钉钉命令

使用上述命令在钉钉中直接测试。

### 方法 2：通过 REST API

```bash
# 添加任务
curl -X POST http://localhost:8080/api/scheduled-tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "API测试任务",
    "cron_expression": "0 */5 * * * *",
    "adapter_id": "dingtalk",
    "task": {
      "type": "message",
      "priority": 1,
      "payload": {
        "content": "这是一个API测试任务"
      }
    }
  }'

# 列出任务
curl http://localhost:8080/api/scheduled-tasks

# 删除任务
curl -X DELETE http://localhost:8080/api/scheduled-tasks/<task-id>
```

### 方法 3：查看日志

Gateway 会输出详细的日志：

```
2026/02/01 21:05:00 cron-scheduler: executing scheduled task task-1234567890
2026/02/01 21:05:00 scheduler: submitting task task-1234567890 (priority: 1)
2026/02/01 21:05:00 scheduler: worker 0 picked up task task-1234567890
2026/02/01 21:05:01 opencode: sending message to session ses_abc123
2026/02/01 21:05:05 opencode: received response for session ses_abc123
2026/02/01 21:05:05 scheduler: task task-1234567890 completed
```

## 故障排查

### 问题 1：命令没有响应

**原因：** Gateway 未启动或钉钉连接失败

**解决：**
1. 检查 Gateway 进程是否运行
2. 检查日志中是否有 "dingtalk: connecting to stream server"
3. 验证环境变量 `DINGTALK_CLIENT_ID` 和 `DINGTALK_CLIENT_SECRET`

### 问题 2：任务创建成功但不执行

**原因：** CronScheduler 未启动或 cron 表达式错误

**解决：**
1. 检查日志中是否有 "cron-scheduler: starting cron scheduler"
2. 验证 cron 表达式格式是否正确
3. 使用 `/crontask list` 查看任务状态
4. 检查任务是否被禁用

### 问题 3：任务执行但没有收到回复

**原因：** OpenCode Server 未响应或 session 创建失败

**解决：**
1. 检查 OpenCode Server 是否运行（http://localhost:4096）
2. 查看日志中是否有 "opencode: sending message to session"
3. 检查 OpenCode Server 日志

### 问题 4：Cron 表达式报错

**原因：** 格式不正确

**解决：**
- 确保使用 6 个字段（秒 分 时 日 月 周）
- 使用引号包裹表达式
- 参考 Cron 表达式示例

## 架构说明

```
DingTalk 群消息
    ↓
Handler.OnChatBotMessageReceived
    ↓
handleCronTask (解析命令)
    ↓
handleCronTaskAdd (创建任务)
    ↓
CronScheduler.AddScheduledTask
    ↓
Cron 触发 (指定时间)
    ↓
executeScheduledTask
    ↓
TaskScheduler.SubmitTask
    ↓
Worker Pool 处理
    ↓
OpenCode Client 发送消息
    ↓
DingTalk 返回结果
```

## 下一步

1. 在其他 adapter（企业微信、飞书）中实现 `/crontask` 命令
2. 添加任务执行历史记录
3. 实现任务执行失败重试
4. 添加任务执行统计和监控
5. 支持更复杂的任务类型（脚本、智能体流程）

## 相关文档

- [/crontask 命令详细说明](CRONTASK_COMMAND.md)
- [调度器完整指南](SCHEDULER_GUIDE.md)
- [Scheduler README](../internal/scheduler/README.md)
