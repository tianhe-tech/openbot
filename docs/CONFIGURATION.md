# Session Management Configuration
# 配置 session 管理参数

## 默认配置

默认情况下，系统使用以下参数（在 `internal/opencode/client.go` 中定义）：

```go
const (
    // 超过此消息数时创建新 session
    MaxMessagesPerSession = 50
    
    // 达到此消息数时开始后台总结
    MessageThresholdForSummary = 30
)
```

## 自定义配置

### 方式 1: 修改源代码

编辑 `internal/opencode/client.go`:

```go
const (
    MaxMessagesPerSession = 80  // 改为 80
    MessageThresholdForSummary = 50  // 改为 50
)
```

然后重新编译：

```bash
go build -o bin/gateway.exe cmd/gateway/main.go
```

### 方式 2: 通过环境变量（未来计划）

```powershell
$env:OPENCODE_MAX_MESSAGES_PER_SESSION = "80"
$env:OPENCODE_MESSAGE_THRESHOLD_FOR_SUMMARY = "50"
```

> 注意：环境变量支持将在未来版本实现

## 场景配置建议

### 客服场景
快速对话，频繁切换话题

```go
MaxMessagesPerSession = 30
MessageThresholdForSummary = 20
```

**特点**：
- ✅ 快速创建新 session
- ✅ 避免上下文混乱
- ✅ 节省资源

### 技术支持场景
中等长度对话，需要维持上下文

```go
MaxMessagesPerSession = 50  // 默认值
MessageThresholdForSummary = 30  // 默认值
```

**特点**：
- ✅ 平衡性能和上下文
- ✅ 适合大多数场景
- ✅ 推荐配置

### 代码开发场景
长对话，需要大量上下文

```go
MaxMessagesPerSession = 100
MessageThresholdForSummary = 60
```

**特点**：
- ✅ 更长的对话历史
- ✅ 更好的上下文连贯性
- ⚠️ 需要更多资源

### 资源受限场景
内存或性能受限

```go
MaxMessagesPerSession = 20
MessageThresholdForSummary = 10
```

**特点**：
- ✅ 最小内存占用
- ✅ 快速响应
- ⚠️ 频繁切换可能影响体验

## 超时配置

默认超时为 180 秒，可以在创建 client 时调整：

```go
client := opencode.NewClient(
    endpoint,
    apiKey,
    opencode.WithTimeout(120 * time.Second), // 2 分钟
)
```

### 超时配置建议

| 场景 | 超时时间 | 说明 |
|------|---------|------|
| 快速问答 | 30s | 简单查询 |
| 一般对话 | 60s | 默认配置 |
| 复杂任务 | 120s | 代码生成等 |
| 长时任务 | 180s | 大规模分析 |

## 监控阈值

添加监控可以帮助优化配置：

```go
// 在发送消息后检查
count := client.GetMessageCount(sessionID)

if count > MaxMessagesPerSession * 0.8 {
    log.Printf("警告: session %s 接近消息上限", sessionID)
}

if count >= MessageThresholdForSummary {
    log.Printf("信息: session %s 已触发总结", sessionID)
}
```

## 动态调整（高级）

根据实际使用情况动态调整（未来功能）：

```go
// 伪代码 - 未来可能实现
if avgMessageLength > 1000 {
    // 消息较长，减少阈值
    effectiveMaxMessages = MaxMessagesPerSession * 0.7
} else {
    // 消息较短，使用默认值
    effectiveMaxMessages = MaxMessagesPerSession
}
```

## 调试模式

在开发和测试时，可以临时降低阈值：

```go
// 测试模式：快速触发 session 切换
const (
    MaxMessagesPerSession = 5       // 5 条就切换
    MessageThresholdForSummary = 3  // 3 条就总结
)
```

这样可以快速验证 session 管理功能。

## 生产环境最佳实践

1. **从默认值开始**
   ```go
   MaxMessagesPerSession = 50
   MessageThresholdForSummary = 30
   ```

2. **监控指标**
   - Session 平均生命周期
   - 总结成功率
   - context deadline exceeded 错误率

3. **根据监控调整**
   - 错误率高 → 降低阈值
   - Session 切换频繁 → 提高阈值
   - 响应时间长 → 降低阈值或增加超时

4. **定期审查**
   - 每月检查配置是否合适
   - 根据用户反馈调整
   - 关注 OpenCode 版本更新

## 常见问题

### Q: 如何知道当前使用的配置？

A: 查看日志或添加启动日志：

```go
log.Printf("Session 配置: Max=%d, Threshold=%d", 
    MaxMessagesPerSession, MessageThresholdForSummary)
```

### Q: 可以为不同用户使用不同配置吗？

A: 当前版本不支持，但可以通过多实例实现：

```bash
# 实例 1: VIP 用户
OPENCODE_MAX_MESSAGES=100 ./gateway -port 8080

# 实例 2: 普通用户
OPENCODE_MAX_MESSAGES=50 ./gateway -port 8081
```

### Q: 修改配置需要重启吗？

A: 是的，当前需要重新编译和重启。热更新将在未来版本支持。

### Q: 如何验证配置生效？

A: 运行测试脚本：

```powershell
.\scripts\test_session_management.ps1
```

或发送测试消息并观察日志。

## 配置检查清单

部署前检查：

- [ ] 已根据场景选择合适的阈值
- [ ] 已设置合适的超时时间
- [ ] 已添加必要的日志和监控
- [ ] 已在测试环境验证配置
- [ ] 已准备回滚方案

## 联系和反馈

如果您有配置建议或遇到问题，请：

1. 查看 [docs/TESTING_GUIDE.md](TESTING_GUIDE.md)
2. 检查日志获取详细信息
3. 提交 Issue 或 Pull Request
