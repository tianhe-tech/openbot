# 实施计划：WeChat 适配器三大问题修复

> 基于 2026-07-01 日志分析 + opencode 服务器 `/session/status` 验证

---

## 问题总览

| # | 问题 | 根因 |
|---|------|------|
| 1 | "完事了吗"等消息返回"正在处理中"，且与 `/status` 显示矛盾 | 8s waitingTimer 在新 turn 刚开始（模型 prefill 阶段）就发误导性"正在处理中，无需操作"；`/status` 只查本地内存不查服务器 |
| 2 | 复杂任务结果在下一轮对话才收到 | 20min 超时不 abort 服务器 session → 僵尸 session + handler 覆盖竞态 |
| 3 | `/status` 显示"空闲"但服务器实际 "busy" | `GetSessionDiagnostics` 只查本地内存，不查服务器 `/session/status` |
| 4 | 18 个僵尸 session 堆积在服务器 | 所有超时/错误路径都不调用 `AbortSession` |
| 5 | skillgen 500 错误 + 复用主 session | drafter 未强制新建 session + prompt 70万字符超限 + 500 被当可重试 |

---

## 修复方案

### 方案 A'：修正 waitingTimer 误导性提示（问题 1）

> **设计决策**：不拦截"完事了吗"等消息。这类消息有歧义——用户可能是在问 AI
> "你那个任务做完了吗？"，需要 AI 回答。拦截会过滤掉合法的用户消息。
>
> 真正的问题是 8s waitingTimer 在新 turn 刚开始（模型 prefill 阶段）就发
> "正在处理中，无需操作"，而此时用户刚发了一条消息，"无需操作"是误导。
>
> 修正方向：根据上一轮是否已完成，区分提示内容或抑制提示。

**文件**: `internal/opencode/event_listener.go`

**位置**: `NewStreamingSessionHandler` 中的 `waitingTimer`（约 line 95）

**当前代码**:
```go
h.waitingTimer = time.AfterFunc(8*time.Second, func() {
    h.mu.Lock()
    defer h.mu.Unlock()
    if !h.contentSent && !h.completed && !h.waitingHintSent {
        h.waitingHintSent = true
        _ = h.callback(QuestionSignalPrefix + "⏳ 正在处理中，无需操作，请稍候...")
    }
})
```

**改为**: 根据上一轮是否已完成，区分提示内容：
```go
h.waitingTimer = time.AfterFunc(8*time.Second, func() {
    h.mu.Lock()
    defer h.mu.Unlock()
    if !h.contentSent && !h.completed && !h.waitingHintSent {
        h.waitingHintSent = true
        // ★ 区分：上一轮已完成（新 turn 正常 prefill）vs 上一轮未完成（可能卡住）
        if h.previousTurnCompleted {
            // 新 turn 的正常等待，不发"无需操作"（用户刚发了消息）
            _ = h.callback(QuestionSignalPrefix + "⏳ 正在思考...")
        } else {
            // 上一轮可能还在跑，提示用户等待
            _ = h.callback(QuestionSignalPrefix + "⏳ 正在处理中，请稍候...")
        }
    }
})
```

需要在 `StreamingSessionHandler` 结构体增加字段：
```go
type StreamingSessionHandler struct {
    // ... 现有字段 ...
    previousTurnCompleted bool // 上一轮是否已完成（session.idle 后到新 prompt 之间）
}
```

在 `NewStreamingSessionHandler` 中，通过检查 `runningSessions` 判断上一轮状态：
```go
// 在创建 handler 时记录上一轮是否已完成
// 如果 runningSessions 中没有这个 sessionID，说明上一轮已完成
_, h.previousTurnCompleted = client.runningSessions.Load(sessionID)
h.previousTurnCompleted = !h.previousTurnCompleted // 取反：不在 map 中 = 已完成
```

**效果**:
- 用户发"完事了吗" → 新 turn 开始 → 8s 内模型还在 prefill → 提示"正在思考..."（不误导）
- 模型回复后 → 用户得到 AI 的真实回答（"是的，完成了"或"还在最后一步"）
- 如果上一轮确实还在跑（session 未 idle），提示"正在处理中，请稍候..."（合理）

---

### 方案 B：超时/错误路径强制 Abort 服务器 Session（问题 2 + 4）

这是**最核心的修复**。所有 gateway 放弃一个 turn 的路径都必须调用 `AbortSession` 清理服务器端。

#### B1: 流式路径 context deadline（问题 2 主因）

**文件**: `internal/opencode/client.go`
**位置**: context deadline 处理分支（约 line 3759）

**当前代码**:
```go
case <-ctx.Done():
    log.Printf("opencode: ⚠️ context deadline for session %s, flushing accumulated content (no abort)", sessionID[:8])
    // ... flush 逻辑 ...
    // 不 abort session
```

**改为**:
```go
case <-ctx.Done():
    log.Printf("opencode: ⚠️ context deadline for session %s, flushing accumulated content + aborting server session", sessionID[:8])
    
    // ★ 新增：abort 服务器端 session，防止僵尸
    abortCtx, abortCancel := context.WithTimeout(context.Background(), 10*time.Second)
    if abortErr := c.AbortSession(abortCtx, sessionID); abortErr != nil {
        log.Printf("opencode: ⚠️ abort failed on context deadline for session %s: %v", sessionID[:8], abortErr)
    }
    abortCancel()
    
    // ... 原有 flush 逻辑不变 ...
```

#### B2: 同步路径 sendPromptWithRetry 失败后（问题 4 cron 僵尸来源）

**文件**: `internal/opencode/client.go`
**位置**: `sendPromptWithRetry` 函数末尾，返回错误前（约 line 5173）

**当前代码**:
```go
    return nil, fmt.Errorf("%w: %v", ErrMaxRetriesExceeded, lastErr)
}
```

**改为**:
```go
    // ★ 新增：所有重试耗尽后 abort 服务器端 session
    abortCtx, abortCancel := context.WithTimeout(context.Background(), 10*time.Second)
    if abortErr := c.AbortSession(abortCtx, sessionID); abortErr != nil {
        log.Printf("opencode: ⚠️ abort failed after retries exhausted for session %s: %v", sessionID[:8], abortErr)
    }
    abortCancel()
    
    return nil, fmt.Errorf("%w: %v", ErrMaxRetriesExceeded, lastErr)
}
```

#### B3: 500 Internal Server Error 不应重试（问题 5）

**文件**: `internal/opencode/client.go`
**位置**: `RetryableErrors` 列表（约 line 535）

**当前代码**:
```go
RetryableErrors: []string{
    "connection refused",
    "connection reset",
    "temporarily unavailable",
    "503",
    "502",
    "500",   // ← 问题：500 被当成可重试
},
```

**改为**: 移除 `"500"`，或改为更精确的判断：
```go
RetryableErrors: []string{
    "connection refused",
    "connection reset",
    "temporarily unavailable",
    "503",
    "502",
    // 移除 "500" — 500 Internal Server Error 通常是请求本身有问题
    // （如 prompt 过大），重试只会重复失败并产生僵尸 session
},
```

同时在 `isRetryableError` 中增加对 `UnknownError` 的排除：
```go
// 500 + UnknownError 不重试（通常是 prompt 过大或服务器内部错误）
if strings.Contains(err.Error(), "500") && strings.Contains(err.Error(), "UnknownError") {
    return false
}
```

---

### 方案 C：修复 Handler 覆盖竞态（问题 2 核心机制）

**文件**: `internal/opencode/client.go`
**位置**: `RegisterSessionHandler`（line 2993）

**当前代码**:
```go
func (c *Client) RegisterSessionHandler(sessionID string, handler EventHandler) {
    c.sessionHandlers.Store(sessionID, handler)
}
```

**改为**: 检测已有 handler，先信号旧 handler 退出再安装新的：
```go
func (c *Client) RegisterSessionHandler(sessionID string, handler EventHandler) {
    // ★ 检测已有 handler：如果存在且未完成，先标记完成并清理
    if old, loaded := c.sessionHandlers.LoadAndDelete(sessionID); loaded {
        if oldHandler, ok := old.(EventHandler); ok {
            // 如果旧 handler 是 StreamingSessionHandler，标记完成让它退出
            if h, ok := oldHandler.(interface{ MarkCompletedAndExit() }); ok {
                h.MarkCompletedAndExit()
                log.Printf("opencode: replaced existing handler for session %s (marked old as completed)", sessionID[:min(8, len(sessionID))])
            }
        }
    }
    c.sessionHandlers.Store(sessionID, handler)
}
```

需要在 `StreamingSessionHandler` 上新增 `MarkCompletedAndExit` 方法：
```go
// MarkCompletedAndExit signals the handler's ticker loop to exit immediately.
// Called by RegisterSessionHandler when a new handler replaces this one.
func (s *StreamingSessionHandler) MarkCompletedAndExit() {
    s.mu.Lock()
    if !s.completed {
        s.completed = true
        s.stopWaitingTimer()
    }
    s.mu.Unlock()
    s.fireOnComplete() // 幂等，sync.Once 保护
}
```

---

### 方案 D：`/status` 查询服务器真实状态（问题 3）

**文件**: `internal/opencode/client.go`

#### D1: 实现 `GetSessionStatus` —— 用 SDK 的 `Execute` 调用 `/session/status`

**当前代码** (line 1012, 是个 stub):
```go
func (c *Client) GetSessionStatus(ctx context.Context, sessionID string) (string, error) {
    // TODO: SDK可能需要添加SessionStatus方法
    _, err := c.GetSession(ctx, sessionID)
    return "unknown", nil
}
```

**改为**:
```go
// SessionStatusEntry represents one session's status from the server.
type SessionStatusEntry struct {
    Type string `json:"type"` // "busy" | "idle" | "error"
}

// GetSessionStatus queries the opencode server's /session/status endpoint
// for the real running state of all sessions. Uses SDK's generic Execute
// method since the SDK doesn't wrap this endpoint.
func (c *Client) GetSessionStatus(ctx context.Context) (map[string]SessionStatusEntry, error) {
    var result map[string]SessionStatusEntry
    err := c.sdk.Execute(ctx, http.MethodGet, "session/status", nil, &result)
    if err != nil {
        return nil, fmt.Errorf("opencode: get session status: %w", err)
    }
    return result, nil
}

// IsServerSideBusy checks if the server considers a specific session busy.
func (c *Client) IsServerSideBusy(ctx context.Context, sessionID string) bool {
    statuses, err := c.GetSessionStatus(ctx)
    if err != nil {
        return false // 查询失败时不阻塞，保守返回 false
    }
    entry, ok := statuses[sessionID]
    return ok && entry.Type == "busy"
}
```

#### D2: `GetSessionDiagnostics` 增加服务器端状态

**位置**: `GetSessionDiagnostics`（line 3216）

在现有逻辑末尾增加服务器端状态查询：
```go
func (c *Client) GetSessionDiagnostics(sessionID, threadID string) SessionStatusInfo {
    // ... 现有逻辑 ...
    
    // ★ 新增：查询服务器端真实状态
    statusCtx, statusCancel := context.WithTimeout(context.Background(), 3*time.Second)
    defer statusCancel()
    if statuses, err := c.GetSessionStatus(statusCtx); err == nil {
        if entry, ok := statuses[sessionID]; ok {
            info.ServerStatus = entry.Type // "busy" | "idle" | "error"
        }
    }
    
    return info
}
```

在 `SessionStatusInfo` 结构体增加字段：
```go
type SessionStatusInfo struct {
    // ... 现有字段 ...
    ServerStatus string // 服务器端真实状态: "busy"/"idle"/"error"/""(未知)
}
```

#### D3: `FormatSessionStatus` 显示服务器状态

```go
func (info SessionStatusInfo) FormatSessionStatus() string {
    // ... 现有逻辑 ...
    
    // ★ 新增：显示服务器端状态
    if info.ServerStatus != "" {
        serverLabel := info.ServerStatus
        if info.ServerStatus == "busy" && !info.Running {
            serverLabel = "busy (⚠️ 僵尸会话 — 服务器busy但gateway无活跃handler)"
        }
        b.WriteString(fmt.Sprintf("服务器状态: %s\n", serverLabel))
    }
    
    return strings.TrimRight(b.String(), "\n")
}
```

---

### 方案 E：Skillgen 修复（问题 5）

#### E1: Drafter 强制新建 session

**文件**: `internal/skillgen/drafter.go`
**位置**: `Draft` 方法（约 line 55）

**当前代码**:
```go
threadID := fmt.Sprintf("skillgen-draft-%s", in.ThreadID)
payload := opencode.MessagePayload{
    Channel:  "skillgen",
    UserID:   in.UserID,
    ThreadID: threadID,
    Content:  prompt,
    Model:    in.ModelID,
}
resp, err := d.Client.SendMessage(ctx, payload)
```

**改为**: 显式清除 thread→session 缓存，强制新建 session：
```go
threadID := fmt.Sprintf("skillgen-draft-%s", in.ThreadID)

// ★ 强制新建 session：清除可能存在的旧缓存映射
d.Client.ClearThreadSessionMapping(threadID)

payload := opencode.MessagePayload{
    Channel:    "skillgen",
    UserID:     in.UserID,
    ThreadID:   threadID,
    Content:    prompt,
    Model:      in.ModelID,
    Streaming:  false,
}
resp, err := d.Client.SendMessage(ctx, payload)
```

需要在 `client.go` 新增：
```go
// ClearThreadSessionMapping removes the thread→session cache mapping,
// forcing the next SendMessage on this thread to create a fresh session.
func (c *Client) ClearThreadSessionMapping(threadID string) {
    c.sessions.Delete(threadID)
}
```

#### E2: 限制 prompt 大小

**文件**: `internal/skillgen/drafter.go`
**位置**: `buildDraftPrompt` 函数

**当前代码** (每轮截断 2000 字符，但不限制轮数):
```go
for i, t := range in.Conversation {
    b.WriteString(fmt.Sprintf("### turn %d · %s\n", i+1, t.Role))
    b.WriteString(truncate(t.Text, 2000))
    b.WriteString("\n\n")
}
```

**改为**: 限制总轮数和总 prompt 大小：
```go
// ★ 限制：最多取最近 50 轮 + 采样更早的轮次，总 prompt 不超过 5 万字符
const maxTurns = 50
const maxPromptChars = 50000

selected := selectRepresentativeTurns(in.Conversation, maxTurns)

b.WriteString("## 对话记录\n")
totalChars := 0
for i, t := range selected {
    turnText := truncate(t.Text, 1500) // 每轮降到 1500 字符
    entry := fmt.Sprintf("### turn %d · %s\n%s\n\n", i+1, t.Role, turnText)
    
    if totalChars+len(entry) > maxPromptChars {
        b.WriteString("…(更早的对话已省略)…\n\n")
        break
    }
    b.WriteString(entry)
    totalChars += len(entry)
}
```

新增采样函数：
```go
// selectRepresentativeTurns picks up to maxTurns turns from a conversation,
// preferring recent turns and turns with tool-call evidence.
func selectRepresentativeTurns(turns []Turn, maxTurns int) []Turn {
    if len(turns) <= maxTurns {
        return turns
    }
    // 取最后 maxTurns 轮（最近的对话最有参考价值）
    start := len(turns) - maxTurns
    return turns[start:]
}
```

---

### 方案 F：任务完成标记（问题 2 用户体验）

**文件**: `internal/adapters/wechat/wechat.go`
**位置**: `dispatchToOpenCode` 末尾，最终内容发送后（约 line 870）

**当前代码**: 有内容时只发内容，无结束标记

**改为**: 在最终内容发送后追加完成标记：
```go
if strings.TrimSpace(unsent) != "" {
    log.Printf("wechat: 📦 queueing final message (%d total, %d unsent)", len(accumulatedContent), len(unsent))
    if sendErr := h.enqueueAsyncText(userID, sessionID, ctxToken, "final", unsent, true); sendErr != nil {
        log.Printf("wechat: ⚠️ final enqueue failed user=%s: %v", userID, sendErr)
    }
    // ★ 新增：发送明确的任务完成标记
    _ = h.enqueueAsyncText(userID, sessionID, ctxToken, "done_marker", "✅ 任务完成", false)
}
```

---

### 方案 G'：僵尸 Session 检测 + 通知 + 手动清理（问题 4 兜底）

> **设计决策**：不自动杀死僵尸 session。一个服务器端 "busy" 但 gateway 无
> handler 的 session，可能是：
> - 20min 超时后的僵尸（该杀）
> - gateway 重启后遗留的还在跑的长任务（不该杀）
> - scheduler 已标记失败但服务器还在跑（该杀）
>
> 静默 AbortSession 会丢失可能还在进行的工作，且用户完全不知情。
> 改为：检测到僵尸时通知用户，提供 `/sweep` 命令让用户手动清理。

#### G'1: 僵尸检测 + 通知

**文件**: `internal/opencode/client.go`

新增检测函数（不自动 abort，只检测和通知）：
```go
// ZombieSessionInfo describes a detected zombie session.
type ZombieSessionInfo struct {
    SessionID string
    // Source is the adapter/channel that originally created this session,
    // if known from gateway's session registry. Empty if unknown.
    Source string
}

// DetectZombieSessions queries the server's /session/status and returns
// sessions that the server considers "busy" but the gateway has no active
// handler for. Does NOT abort them — caller decides what to do.
func (c *Client) DetectZombieSessions() []ZombieSessionInfo {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    
    statuses, err := c.GetSessionStatus(ctx)
    if err != nil {
        log.Printf("opencode: zombie detect failed: %v", err)
        return nil
    }
    
    var zombies []ZombieSessionInfo
    for sessionID, entry := range statuses {
        if entry.Type != "busy" {
            continue
        }
        // gateway 有活跃 handler → 不是僵尸
        if _, ok := c.activeHandlers.Load(sessionID); ok {
            continue
        }
        // 服务器 busy 但 gateway 无 handler → 疑似僵尸
        source := ""
        // 尝试从 session→thread 反查来源（如果有映射的话）
        c.sessions.Range(func(threadID, sid any) bool {
            if sid.(string) == sessionID {
                source = threadID.(string)
                return false // found
            }
            return true
        })
        zombies = append(zombies, ZombieSessionInfo{
            SessionID: sessionID,
            Source:    source,
        })
    }
    return zombies
}

// SweepZombieSessions aborts the given list of zombie sessions.
// Returns the number successfully aborted.
func (c *Client) SweepZombieSessions(zombies []ZombieSessionInfo) int {
    swept := 0
    for _, z := range zombies {
        log.Printf("opencode: 🧹 sweeping zombie session %s (source=%s)", z.SessionID[:8], z.Source)
        abortCtx, abortCancel := context.WithTimeout(context.Background(), 10*time.Second)
        if err := c.AbortSession(abortCtx, z.SessionID); err != nil {
            log.Printf("opencode: 🧹 abort failed for zombie %s: %v", z.SessionID[:8], err)
        } else {
            swept++
        }
        abortCancel()
    }
    if swept > 0 {
        log.Printf("opencode: 🧹 zombie sweep complete, aborted %d sessions", swept)
    }
    return swept
}
```

#### G'2: 定期检测 + 通知用户

**文件**: `cmd/gateway/main.go`

```go
// 每 10 分钟检测一次僵尸，有则通知用户
go func() {
    ticker := time.NewTicker(10 * time.Minute)
    defer ticker.Stop()
    for range ticker.C {
        zombies := client.DetectZombieSessions()
        if len(zombies) == 0 {
            continue
        }
        // 通知所有适配器的用户
        msg := fmt.Sprintf("⚠️ 检测到 %d 个可能卡住的会话（服务器busy但gateway无活跃handler）\n"+
            "发送 /sweep 清理，或 /status 查看详情", len(zombies))
        notifier.NotifyAll(msg) // 需要各适配器实现 NotifyAll
    }
}()
```

#### G'3: 新增 `/sweep` 命令

**文件**: `internal/adapters/wechat/wechat.go`（及 dingtalk/feishu/wecom 同理）

在 `tryCommand` 中新增：
```go
case "/sweep", "清理":
    return h.handleSweep(userID), true
```

```go
func (h *Handler) handleSweep(userID string) string {
    zombies := h.client.DetectZombieSessions()
    if len(zombies) == 0 {
        return "✅ 没有检测到僵尸会话"
    }
    
    var b strings.Builder
    b.WriteString(fmt.Sprintf("🧹 检测到 %d 个僵尸会话，正在清理...\n\n", len(zombies)))
    for i, z := range zombies {
        sid := z.SessionID
        if len(sid) > 8 {
            sid = sid[:8]
        }
        source := z.Source
        if source == "" {
            source = "(未知来源)"
        }
        b.WriteString(fmt.Sprintf("%d. %s (来源: %s)\n", i+1, sid, source))
    }
    
    swept := h.client.SweepZombieSessions(zombies)
    b.WriteString(fmt.Sprintf("\n✅ 已清理 %d/%d 个僵尸会话", swept, len(zombies)))
    return b.String()
}
```

#### G'4: `/status` 中显示僵尸警告

方案 D3 的 `FormatSessionStatus` 已包含：
```
服务器状态: busy (⚠️ 僵尸会话 — 服务器busy但gateway无活跃handler)
```
用户通过 `/status` 就能看到僵尸警告，再决定是否 `/sweep`。

---

## 修复优先级

| 优先级 | 方案 | 解决的问题 | 风险 |
|--------|------|-----------|------|
| **P0** | B1: 流式超时后 abort | 问题 2 主因 + 僵尸 | 低 — abort 是幂等操作 |
| **P0** | B2: 同步重试耗尽后 abort | 问题 4 cron 僵尸 | 低 |
| **P0** | C: Handler 覆盖竞态 | 问题 2 核心机制 | 中 — 需确保 MarkCompletedAndExit 不死锁 |
| **P1** | D: `/status` 查服务器 | 问题 3 | 低 — 只读查询 |
| **P1** | G': 僵尸检测+通知+`/sweep` | 问题 4 兜底 | 低 — 用户手动确认后才清理 |
| **P1** | A': 修正 waitingTimer | 问题 1 | 低 — 只改提示文案和条件 |
| **P1** | E1: Skillgen 强制新 session | 问题 5 | 低 |
| **P2** | E2: Skillgen prompt 限制 | 问题 5 | 低 |
| **P2** | B3: 500 不重试 | 问题 5 | 中 — 可能影响真正的临时 500 |
| **P2** | F: 任务完成标记 | 问题 2 UX | 低 |

---

## 验证方法

1. **问题 1**: 发送"完事了吗" → 消息正常转发给 AI，8s 内若模型还在 prefill → 提示"正在思考..."（非"正在处理中，无需操作"）；AI 最终回复真实回答
2. **问题 2**: 发送复杂任务 → 20min 超时后 → `curl /session/status` 应显示该 session 不再 busy
3. **问题 3**: 发送 `/status` → 应显示"服务器状态: busy/idle"，僵尸会话标注警告
4. **问题 4**: `curl /session/status` → 僵尸 session 通过 `/sweep` 手动清理后消失；定期检测到僵尸时用户收到通知
5. **问题 5**: 触发 skillgen → 日志应显示"created new session"而非"reusing existing"
