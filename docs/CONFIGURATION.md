# Session Management Configuration
# 配置 session 管理参数

## 默认配置

### Memory Storage and Recall

`MEMORY_STORE_PATH` selects the SQLite database; empty uses
`<OPENCODE_DIRECTORY>/mem/memory.db` and does not disable capture. The gateway
creates parent directories and migrates the search index. Back up before upgrading.

New `gateway-memory` MCP registrations are disabled. Recall requires an isolated
single-owner OpenCode/MCP instance and operator-provided `--single-owner`,
`--adapter`, and `--user` arguments. The MCP `--db` flag (or `GATEWAY_MEMORY_DB`)
must refer to the same database. Shared-user recall is not supported yet.

See [MCP memory configuration](MCP_MEMORY_TOOL.md) for executable paths, binding,
upgrade order, limits, and read-only behavior. Hourly decay and startup keyword
deletion are no longer enabled.

`MEMORY_CAPTURE_MODE=legacy` is the default. Experimental `strict` requires an
explicitly migrated v2 archive, commits input before dispatch, and recovers final
assistant text independently of outbound delivery. Invalid modes or missing archive
schema fail startup. Strict mode stops legacy conversation writes; MCP searches
those v2 records through the chunked lexical index described below. Legacy offline
automatic retries are disabled in strict mode so uncertain tasks are not silently
replayed.

Internal bootstrap prompts (the synthetic "initialize session" requests that only
materialize an upstream session for shell execution) are never archived as
conversation turns. Token-overflow recovery and explicit retries re-dispatch the
original delivery with `RetryOfDelivery`, which continues the existing turn with a
new attempt instead of failing closed as a replay or creating a duplicate user
request; the adapters carry the original transport delivery ID through their
pending-overflow state so the retry keeps the same turn identity.

See [archive migration and strict capture](MEMORY_MIGRATION.md) before activation.
Strict mode also records rolling partial snapshots (coalesced to one write per 5
seconds or 16 KiB of additional text) and tool/attachment metadata (tool output
bounded to 8 KiB, attachment URLs to 2 KiB, `data:` URIs reduced to their media
type). These writes are best-effort and never interrupt delivery.

Strict mode also starts a background index worker. It consumes the `kind='index'`
jobs written with each archived message, chunks the stored body (1 KiB target,
2 KiB cap, 128-byte overlap, boundary-aware and rune-aligned), and writes
`mem_v2_chunks` plus the `mem_v2_chunk_fts` trigram index. Indexing is decoupled
from capture, so a slow index never delays delivery, and a single unindexable
message records `last_error` without stalling the queue. The worker stops before
the archive writer drains on shutdown.

`memory_search` reports `Coverage: PARTIAL` while index jobs are pending, so an
unindexed message is never reported as a confident miss. `memory_get` and
`memory_scroll` require the migrated archive and refuse a legacy-only database.

Strict mode compresses message bodies automatically; there is no setting to
enable or disable it. A block is stored with zstd only if it is at least 4 KiB
and the compressed frame saves at least one byte in eight. Everything else stays
raw, including short replies, incompressible payloads, and anything above the
64 MiB decode ceiling. Each block records its codec, codec version, original
length, stored length, and a checksum over the original text, and every read path
decodes through one function, so a block written by an older binary stays
readable. Compression runs inside the capture commit rather than in a background
pass: the measured cost is 392 µs for a typical 16 KiB answer and 8.93 ms for a
1 MiB message, against a 2 s commit deadline. `memory_health` reports raw versus
stored bytes so the saving is visible rather than assumed.

Knowledge extraction, semantic search, and source-backed handoff remain pending.
Keep the default mode for production until the gates listed in
[Gate status](MEMORY_IMPLEMENTATION_PLAN.md#gate-status) close.

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

## 模型错误与自动降级

模型、账户或上游错误不会清除当前 session 或 `/model` 设置。自动降级只改变本轮请求使用的模型，在原 session 上重试，不通过新建会话或摘要接续恢复上下文。

降级前会等待旧流式回合完成清理。如果服务端仍在重试，会中止该失败回合并等待 session 空闲；无法确认空闲时停止自动重试并提示错误，保留会话和模型设置。

每条降级链不会重复尝试同一个模型；候选模型全部失败后提示错误。后续消息保留用户模型偏好；如果该模型处于熔断状态且有健康候选，则明确使用候选模型，而不是不传模型参数并回到服务端默认模型。

| 环境变量 | 默认值 | 作用 |
|---|---|---|
| `OPENCODE_CIRCUIT_BREAKER_ENABLED` | `true` | 是否启用熔断和自动降级 |
| `OPENCODE_CB_FAILURE_THRESHOLD` | `2` | 连续失败多少次后熔断 |
| `OPENCODE_CB_COOLDOWN_SECONDS` | `60` | 初始熔断冷却时间 |
| `OPENCODE_CB_MAX_COOLDOWN_SECONDS` | `1800` | 最大冷却时间 |
| `OPENCODE_CB_TRIP_KEYWORDS` | 见下文 | 触发降级的错误关键词，逗号分隔 |

默认关键词为 `No available client,No available channel,Insufficient Balance,provider unavailable,Access denied,Arrearage,Model not found`。除错误文本外，也检查上游 JSON 响应体中的 `error.code` 和 `error.type`。`Model not found` 按错误中的实际 provider/model 记录失败。

设置 `OPENCODE_CB_TRIP_KEYWORDS` 会替换默认列表。已有自定义配置若需要处理账户异常和模型不存在，也需加入相应关键词。

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
