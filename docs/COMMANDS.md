# OpenCode Gateway 命令参考

本文档列出了所有可用的命令及其用法。

## 📋 基本命令

### `/help` 或 `帮助`
显示完整的帮助信息，包括所有可用命令和使用说明。

### `/skills` 或 `/agents`
列出所有可用的技能/代理。

### `/abort` 或 `/stop` 或 `停止`
中止当前正在运行的任务。

### `/refresh`
刷新技能缓存，重新加载可用的agents列表。

---

## 📊 会话管理命令

### `/status` 或 `状态`
查看当前会话的详细状态，包括：
- 会话ID
- 标题
- 工作目录
- 消息数量
- Token使用情况
- 上下文使用率
- 创建时间

**示例：**
```
/status
```

### `/new` 或 `/reset` 或 `新会话`
重置当前会话映射，下次发送消息时将创建新会话。

**示例：**
```
/new
```

### `/clear` 或 `清除`
删除当前会话及其所有数据。

**示例：**
```
/clear
```

### `/sessions` 或 `/list`
列出所有可用的会话（最多显示10个最近的会话）。

**示例：**
```
/sessions
```

---

## 🤖 模型配置命令

> **注意**: 当前版本支持通过 SDK 查询可用 provider/model（`/config/providers`）。
> 但会话默认模型的动态切换在部分服务端实现中可能不稳定，建议在 OpenCode Web 界面设置默认模型。

### `/model`
查看可用模型列表（并显示当前会话信息）。

**示例：**
```
/model
```

### `/model <provider>/<model>`
设置当前会话的模型提供商和模型。

**示例：**
```
/model anthropic/claude-3-opus
/model openai/gpt-4
```

### `/model <provider> <model>`
另一种设置模型的格式（使用空格分隔）。

**示例：**
```
/model anthropic claude-3-opus
/model openai gpt-4
```

### `/provider`
`/model`命令的别名。

---

## ⚙️ 配置命令

### `/config` 或 `配置`
查看当前的完整配置信息，包括：
- 会话信息（如果有活跃会话）
- 可用命令列表

**示例：**
```
/config
```

---

## 🛠️ 高级命令

### `/cmd <command>`
在当前会话中执行 shell 命令，并返回命令输出结果。

**示例：**
```
/cmd ls -la
/cmd cat README.md
/cmd git status
```

### `/answer <question_id> <answer>`
回答OpenCode提出的问题或确认请求。

**示例：**
```
/answer q_123456 1
/answer q_123456 yes
```

### `/crontask`
管理定时任务。详见 [CRONTASK_COMMAND.md](CRONTASK_COMMAND.md)。

**子命令：**
- `/crontask add` - 添加定时任务
- `/crontask list` - 列出所有任务
- `/crontask delete` - 删除任务
- `/crontask enable` - 启用任务
- `/crontask disable` - 禁用任务
- `/crontask info` - 查看任务详情

### 自然语言定时任务（推荐）
除 `/crontask` 外，也可直接发送自然语言让网关识别定时任务意图（DingTalk/Feishu/WeCom）：

```
每天早上9点提醒我检查日志
列出定时任务
禁用任务 cron-1746000000
试运行任务 cron-1746000000
```

写操作（创建/启用/禁用/删除/试运行）默认采用“草稿确认”机制：

```
确认 / yes / 1   # 执行
取消 / no / 0    # 放弃
```

也支持 `/crontask <自然语言>` 兜底写法：

```
/crontask 每天早上9点提醒我发日报
```

---

## 💡 使用技巧

### 快速回复
对于问题和权限请求，支持直接回复选项序号或名称，无需使用`/answer`命令：
```
1          # 选择第一个选项
允许        # 直接回答"允许"
yes        # 确认
```

### 指定Agent
使用`@agent_name`前缀来调用特定的agent：
```
@build 创建一个Python脚本
@plan 制定项目计划
```

### 会话管理最佳实践
- 定期使用`/status`检查token使用情况
- 当上下文接近上限时使用`/new`创建新会话
- 使用`/clear`清理不再需要的会话以节省资源

### 模型切换
- 不同的模型适合不同的任务
- Claude模型更适合复杂的代码理解和生成
- GPT-4适合快速响应和对话
- 可以使用`/model`查看可用模型并尝试切换

---

## ❓ 常见问题

### Q: 为什么某些命令不工作？
A: 某些功能可能依赖OpenCode SDK的特定版本。请确保：
1. OpenCode Server正常运行
2. SDK版本是最新的
3. 查看错误提示信息

### Q: 如何查看我的会话历史？
A: 使用`/sessions`命令查看所有会话，然后可以在OpenCode Web界面中查看详细历史。

### Q: 模型配置不生效怎么办？
A: 当前SDK支持查询可用模型，但会话默认模型切换在部分场景可能不稳定。建议：
1. 在OpenCode Web界面中配置默认模型
2. 或在创建新会话前在Web界面中设置

### Q: 如何节省Token使用？
A: 
1. 定期使用`/new`创建新会话
2. 删除不需要的旧会话（`/clear`）
3. 使用`/status`监控使用情况
4. 对简单问题使用token更少的模型

---

## 🔗 相关文档

- [快速开始](QUICK_START_CRONTASK.md)
- [定时任务](CRONTASK_COMMAND.md)
- [API文档](API.md)
- [配置指南](CONFIGURATION.md)

---

## 📝 更新日志

### v1.1.0 (2026-02-14)
- ✨ 新增会话管理命令（/status, /new, /clear, /sessions）
- ✨ 新增模型配置命令（/model, /provider）
- ✨ 新增配置查看命令（/config）
- 🐛 修复streaming完成后内容未发送的bug
- 📚 更新帮助文档
