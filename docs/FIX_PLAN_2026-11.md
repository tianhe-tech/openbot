# 实施计划：四个会话/投递问题的修复与改进

> 状态：2026-11-16，S1 代码及自动化回归已落地；S2–S4 尚未实施，真实渠道联调待完成。
> 依据：`openbot.log`、`openbot_new.log`、`wechat.log` 与当前源码。
> 前序文档：[FIX_PLAN_WECHAT_ISSUES.md](./FIX_PLAN_WECHAT_ISSUES.md)。
> 涉及相同问题时以本文修订后的契约与验收标准为准，不沿用旧文档中未经验证的归因。

---

## 已拍板的决策与本次修订

| 决策 | 保留的结论 | 修订后的依据 |
|------|------------|--------------|
| A：是否实施两级 handler 注册表 | 本次不做；保留现有注册表，先修会话切换和生命周期缺口 | 控制改动范围，先用完整交互回归定位；不再使用“只能解决 2.8%／其余覆盖 97%”作为依据 |
| B：cron 会话在 `/sessions` 中的显示 | 接受 `cron-<taskID>`，不新增图标、不隐藏会话 | 保留会话可见性，不改 `displaySessionTitle`；列表可能优先显示首条用户消息的 preview |

本次修订解决原计划的九项问题：

1. 撤回按截断 session ID 推导的故障比例及收益承诺。
2. 回到“TUI 继续交互后渠道不同步”的原问题，不把反方向的显示问题当成答案。
3. 修正 `/switch` 的认领边界、执行顺序、失败反馈及多渠道覆盖。
4. 为 skillgen 定义前台完成契约，避免正文混入控制信号及后台重试与清理互相冲突。
5. 区分后台投递、等待下次用户消息的 handoff、停止恢复这三种结果。
6. 分别处理技能参考文件和安装目录，不再用“必须不是目录”的函数解析安装目录。
7. 按消息语义处理重启重放，覆盖 critical 的旧完成提示。
8. 补齐 cron 的失败持久化、回调注册顺序、重叠运行和会话替换流程。
9. 补齐 `Notifier` 接口、实现和测试替身的修改。

## 证据边界

### 源码已经确认的事实

- 微信的空内容完成提示在 [wechat.go:1173](../internal/adapters/wechat/wechat.go#L1173)；
  该提示以 `critical=true` 入队。
- [SendMessage 的流式分支](../internal/opencode/client.go#L1244)只提交异步 prompt，
  不负责注册流式 handler。
- [事件分发](../internal/opencode/client.go#L2932)找不到 session handler 时只是记录日志，
  随后仍可能调用全局处理器；不是“提前 return，整个事件被丢弃”。
- [自动认领路径](../internal/opencode/claim.go#L255)有网关自建会话过滤；
  [显式 ClaimSession](../internal/opencode/claim.go#L134)没有同样的过滤。
- [临时 handler 的完成回调](../internal/opencode/client.go#L5443)已经调用
  [restoreClaimedHandler](../internal/opencode/claim.go#L480)恢复长期 handler。
- [cron 执行](../internal/scheduler/cron_scheduler.go#L390)没有传固定 session，
  当前还在提交任务之后才注册完成回调。
- [scheduler 的发送失败分支](../internal/scheduler/scheduler.go#L262)返回 nil 结果，
  而终态回调只在结果非 nil 时调用。

### 日志能支持、但不能过度推导的结论

- 日志出现过空归属 cron 会话、标题认领、模型 fallback、skillgen 草稿轮询超时，
  与用户反馈的风险路径相符。
- 日志中的 `ses_e32c` 切换记录来自飞书；不能只改微信就宣称修复了这个样本。
- 日志常只输出 session ID 的前八个字符；同一前缀下存在不同的完整 ID。
  因此不能据“同前缀曾被认领、后来没有 handler”证明同一会话被覆盖。
- `no session handler found` 次数不等于用户丢失消息数，也不能证明全部是注册表竞态。
  cron 的同步 prompt 本来就不一定注册流式 handler。
- `pending=668` 长期不变不能单独证明所有行都是 parked；必须查看队列实际状态。
- 仓库内参考技能存在，不代表部署包中也存在；参考文件缺失可能来自工作目录、
  打包遗漏或配置错误，不能仅凭告警断定是哪一种。
- skillgen 没有 handler 会使错误不可见，但不必然使模型生成失败：
  原轮询仍可能读到成功结果；具体上游失败原因需要增加可观测性后验证。

**撤回旧版中“已完整证实”“③只能解决2.8%”“其余覆盖97%”及固定下降比例的结论。**
后续按完整 session ID、任务 ID、投递 ID 和时间顺序关联；历史记录无法唯一关联时标记为未知。

---

## 问题总览与不变量

| 问题 | 修复目标 | 不允许发生的回归 |
|------|----------|----------------|
| 1：过早收到“处理完成” | 正确区分终态、后台接管和需要用户重发；重启不重放失效完成提示 | 无后台任务却声称稍后会投递；旧完成提示冒充新请求终态 |
| 2：TUI 后续输出不同步 | 验证并恢复同一 session 的模型输出镜像，覆盖三个渠道 | 普通切换覆盖活跃 handler；修复只覆盖微信 |
| 3：cron 替换默认会话 | 调度上下文与聊天上下文隔离，结果仍发往原渠道 | 修改用户默认 session；失败或重启后反复创建不可追踪会话 |
| 4：skillgen 不通知确认 | 草稿成功才持久化并通知；失败可定位、可反馈 | 把 session ID、提示或部分文本当草稿；后台任务未结束就清理 |

---

## 问题 1：过早完成提示与重启重放

### 1.1 修正投递契约

在 [Response](../internal/opencode/client.go#L199)新增零值兼容字段：

```go
Deferred bool `json:"deferred,omitempty"`
```

`Deferred=true` 只表示：**本轮最终结果尚未确定，已由明确的后台执行者接管，
并负责最终成功或失败通知**。它不保证重试成功，也不要求当前完全没有部分内容。

| 实际结果 | 返回/投递行为 |
|----------|---------------|
| 已产生最终答案 | 正常完成；不因空同步 `Reply` 重复发送 |
| 后台重试已接管 | `Deferred=true`；保留部分内容及后续回调，不发送成功终态 |
| 保存 handoff，等待下一条用户消息 | 返回明确提示；`Deferred=false`，不宣称正在自动重试 |
| 熔断、停止恢复或重试未能启动 | 返回明确错误/停止提示；不得标记“后台接管” |

修改点：

- [finalizeAsyncStreamingResponse](../internal/opencode/client.go#L6378)的
  `FallbackRetryDispatched()` 分支置位 Deferred。
- [recoverSilentModelHang](../internal/opencode/client.go#L6274)不能再只靠 bool 表达结果：
  区分“不适用”“重试已接管”“已停止恢复”。熔断分支目前也返回 true，
  必须改为明确的停止结果；调用方仅在真正接管时置位 Deferred。
- [triggerSessionHandoff](../internal/opencode/client.go#L6140)只保存上下文供下一轮恢复，
  不是自动重发。保留其现有终态提示，不添加“notice 为空即 Deferred”的分支。
- 后台接管后的所有失败出口都必须通知原渠道；不能只有日志，也不能无限重试。
  区分“已接管”和“重试已开始”，接管者负责启动失败、取消及熔断的终态反馈。

### 1.2 适配器终态与后续回调

修改 [微信空内容分支](../internal/adapters/wechat/wechat.go#L1173)：
Deferred 时不发“✅ 处理完成”；飞书、钉钉记录相同的投递状态，避免把已接管记成成功。

保留原回调交付后续文本，但不能仅因为闭包仍有效就认定投递可靠：

- 验证原调用返回后，文本、错误和 Flush 仍能送达正确的用户与会话。
- 最终结果可能是成功、重试失败或停止；验收不是“后续必须有正文”。
- 对同一投递 lineage，成功终态或失败终态只通知一次；部分结果不得重复发送。
- 媒体和权限/提问仍走各自的路径，不把它们当正文或空完成。

删除 [流式入口调试打印](../internal/opencode/client.go#L5309)，不再输出整个 payload。

### 1.3 重启重放按语义处理，不新增任意的两小时阈值

现有 [dispatchOne](../internal/adapters/wechat/outbound_queue.go#L515)
在实际发送之前已经检查 TTL 和 24 小时陈旧年龄；旧文档对此描述不准确。
保留现有正文年龄策略，启动清理只提前执行等价分类，不缩短最终答案的交付窗口。

启动恢复必须在 dispatcher 开始消费之前完成：

| 持久化消息 | 启动/恢复策略 |
|------------|---------------|
| 旧 `done` 完成提示 | 无论 critical 与否均 park，不自动重放；无法证明属于当前请求的提示不能冒充当前终态 |
| 未过期的最终正文或部分正文 | 保留现有去重、分块顺序和重试语义；不能因为进程重启就丢弃 |
| 已过期的非 critical 进度 | 按现有 TTL 清理 |
| 超过现有24小时窗口的正文 | 按现有陈旧策略 park，不删除；可查看并显式恢复 |
| 旧权限/提问 | 核验实际待回答请求；失效或无法确认的旧提示先 park，再从实际待回答状态重新展示有效提示 |

补齐 [RecoverParked](../internal/adapters/wechat/outbound_queue.go#L1087)：

- `/recover` 不得无差别复活旧 `done` 或失效问题；明确反馈哪些条目不能恢复以及原因。
- 用户显式恢复旧正文时，必须有一次有界的重新投递窗口，避免刚恢复就因原始
  `created_at` 超龄再次被 park。保留原创建时间，单独记录恢复窗口，不伪造消息年龄。
- 无法核验的旧问题不能仅凭“同一 session 还有一个问题”就当作原问题仍有效；
  应显示实际待回答的问题，而不是重放旧文案。
- 启动分类失败要明确报错，不得记录“清理成功”后照常重放未知状态的旧通知。

### 1.4 统计与验收

[pendingCount](../internal/adapters/wechat/outbound_queue.go#L889)拆分 active pending 与 parked，
统计同时输出到期等待、发送失败等状态；不擅自改变用户消息入站 gating 的含义。

测试覆盖：

- fallback 接管后无完成提示，最终成功或最终失败均只通知一次。
- 静默恢复熔断后没有后台任务，不返回 Deferred。
- 普通 handoff 提示用户继续，不伪装成后台自动恢复。
- 重启前持久化的 critical `done` 不会重放，新请求的真实结果正常送达。
- 未发送正文不被误 park；过期进度正确清理；有效/失效问题分开处理。
- 显式恢复旧正文可以真正尝试发送，但不会复活失效完成提示。
- 统计区分 pending 与 parked，不再根据冻结总数推断队列内容。

---

## 问题 3：定时任务上下文隔离与持久化

### 3.1 隔离聊天路由、任务上下文与结果收件人

增加 `ScheduledTask.SessionID` 并持久化；执行 `Task` 透传该字段。
定时任务的模型 thread 使用独立命名空间：

```go
const CronThreadPrefix = "cron-"
```

模型 thread 为 `CronThreadPrefix + scheduledTask.ID`；
原 `UserID`、`Channel`、收件人 metadata 仍用于归属和结果投递，不能用 cron thread 替换收件人。

固定 session 的目的仅是让同一任务的执行/重试可追踪，不能调用用户默认会话切换 hook。
任务结果通过现有 scheduler 投递路径回到原渠道，不依赖把任务认领为聊天会话。

**B1保持不变**：cron session 可以出现在 `/sessions`，不改
[displaySessionTitle](../internal/opencode/client.go#L2262)。
可见不等于允许作为默认聊天会话：三个渠道的 `/switch` 必须在改映射前拒绝 cron 目标，
并给出明确提示。用户仍可查看任务结果，但普通聊天上下文不能切到任务上下文。

### 3.2 创建/解析会话必须与持久化闭环

从 [SendMessage 会话解析](../internal/opencode/client.go#L1029)提取共享的
`ResolveOrCreateSession`，不要复制一套查表/建会话逻辑。

必须保持的边界：

- 只负责确定 session；不发送 prompt，不重复触发 capture、handoff、请求去重或结果投递。
- 使用相同的 thread 锁与所有权校验；网络操作不持有 cron 的全局任务锁。
- 仅在明确不存在/已失效时替换会话；网络超时等不确定错误不能当作“不存在”。
- 固定的旧 ID 发生替换时，新的实际 ID 也必须回写，不能只判断传入 ID 是否为空。
- 解析失败就结束该次尝试并进入受控重试，禁止回退到“不固定 session 的旧发送路径”。

执行顺序：

1. 对 scheduled task ID 获取运行占用，复制任务快照。
2. 解析隔离 session，将实际 ID 写入 ScheduledTask 并成功落盘。
3. 在发送任何 prompt 前，将该 ID 注册到 cron session 判定表。
4. 注册终态回调，再提交执行 Task；提交失败时撤销回调并释放占用。
5. 真正发送时仍需处理 session 在准备后被删除的情况：共享解析流程若产生替换 ID，
   必须先重新登记并持久化，再发送 prompt。

为第5步提供窄的 session-resolved 回调/等价内部钩子：
仅在实际 ID 确定后、prompt 之前执行；cron 用它同步更新绑定及持久化。
持久化失败必须阻止发送，不能只打一条日志后继续。
非 cron 调用不安装此钩子，保留原行为。

远端创建与本地落盘无法构成同一事务，不承诺跨进程崩溃“绝对只创建一次”：
若创建后落盘失败，保留内存中的待持久化 ID，重试优先落盘，不重复创建。
重启后按隔离 thread 的精确标题和归属核对已有候选；仅唯一、可确认的任务会话可恢复复用，
不确定时显式告警，不靠八位 ID 或模糊标题猜测，更不能绑定到用户聊天会话。

### 3.3 终态回调、重试与重叠运行

修正 [executeTask](../internal/scheduler/scheduler.go#L164)
及 [完成回调](../internal/scheduler/scheduler.go#L445)：

- 成功、重试耗尽、重试提交失败都构造包含 TaskID、实际 SessionID、Status、Error 的 TaskResult。
  不能因为 `SendMessage` 返回错误、结果为 nil 就跳过终态回调。
- 中间重试不释放 scheduled task 的运行占用；终态通知至多一次。
- 重试提交失败必须进入终态，不能忽略错误后直接 return。
- 终态失败向原渠道发送失败状态及简短原因，不能沿用“定时任务执行完成”的成功模板，
  也不能带上先前尝试遗留的 Result 冒充本次成功结果。
- 回调先于任务可执行注册；增加针对提交失败的回调注销，避免泄漏或误触发。
- 持久化/终态记账使用独立、有界的清理 context，不依赖已经超时的执行 context。
- 每次重试结束本次 active bookkeeping 后才允许同一 Task 再执行，避免两个 worker
  同时修改同一个任务对象，或旧尝试清掉新尝试的 active 记录。
- 进程停止时，对已登记但未执行的运行显式清理占用与回调。

同一 scheduled task 的固定 session 不并发运行：

- 从准备、排队、执行到重试结束期间，下一次 tick 合并/跳过并明确记录
  `reason=already_running`，不再启动第二个 prompt；不建立无限积压。
- 终态路径用 defer 释放占用，回调或持久化失败也不能把任务永久锁死。
- 每次恢复或重试前查询持久化 session 是否仍 busy；本地超时不代表远端已停止。
  busy 时推迟本次尝试，重叠 tick 继续跳过；状态未知时报错并有界重试，
  不盲目向仍在工作的远端 session 追加 prompt。
  超出本地预算但远端状态不确定时，持久化待核验状态；本地运行占用仍按终态释放，
  后续 tick 必须先核验远端再决定能否启动，不能把本地释放当作远端空闲。
- 不同 scheduled task 仍可并行；用户聊天不受任务占用影响。

新增频繁落盘后，必须补齐 [saveTasks](../internal/scheduler/cron_scheduler.go#L109)
的并发安全：在锁内复制稳定快照（含 metadata），串行化快照与写盘次序，
再原子替换文件；不能仅复制指针后无锁 Marshal，也不能并发写同一个 `.tmp`。
锁顺序保持一致，网络操作不持有持久化锁。

### 3.4 认领护栏与迁移

在 [claimSession](../internal/opencode/claim.go#L143)执行任何标题改写、
路由 hook 或 handler 安装之前，拒绝：

- cron 判定表中的 session；
- 标题所有权前缀后的独立 cron thread 标记。

host 将判定器接到 [IsCronSession](../internal/scheduler/scheduler.go#L621)；
启动时从已验证的持久化任务绑定恢复判定表，不能等第一次成功执行完才登记。
标题标记负责覆盖“会话刚创建、尚未来得及登记”的事件窗口。

保留 `[feishu] 标题` / `[feishu:] 标题` 到 SoleUser 的既有功能；
不能通过一刀切拒绝空 userID 来修 cron。
旧任务缺归属字段时，只从可验证的创建者/收件人 metadata 回填：
`receive_id` 可能是群 ID，不能直接当作用户 ID。
无法唯一确定则禁用并明确告警，不猜用户、不触发认领，也不自动释放用户已有会话。

校验要覆盖加载、创建/更新、提交执行，不是只在重启加载时生效。
旧 `SessionID` 复用前也须验证其为该任务的隔离会话，不能把旧聊天绑定直接迁移为任务上下文。

### 3.5 创建入口与验收

- [飞书创建入口](../internal/adapters/feishu/feishu.go#L3955)补齐 Channel，
  保留 `receive_id_type`，区分私聊与群聊目标。
- [自然语言创建入口](../internal/scheduler/nl_service.go#L208)保证模型 thread 与归属有效；
  收件人仍按平台语义验证，不能用 userID 盲目填充所有类型的 Channel。
- 微信、钉钉及其他创建入口通过共享校验保持一致。

测试覆盖：

- 任务首次运行、连续失败重试、下一次执行及网关重启，复用同一有效任务 session。
- 准备/落盘/发送失败分别可见；无替换和崩溃时只创建一个 session。
- 任务创建后落盘失败不发送 prompt；替换旧 session 后先落盘再发送。
- 快速成功也不漏回调；重试耗尽和队列提交失败都产生一次终态。
- 同一任务重叠 tick 不并发发 prompt；不同任务可以并行。
- 并发任务回写及停止/重启不丢绑定、不留下永久占用。
- cron 创建早期事件、自动认领、显式认领、`/switch`、重启恢复均不改变用户聊天映射。
- 原渠道收到结果，用户下一条聊天仍进入原聊天 session；不把“没有 handler 日志归零”当验收。

---

## 问题 4：skillgen 完成契约、错误与确认通知

### 4.1 草稿使用有界的前台完成模式

原 [Draft](../internal/skillgen/drafter.go#L41)提交异步 prompt 后轮询；
改为有 handler 的事件驱动等待，但不能原样照搬渠道流式入口的后台恢复行为。

为流式入口增加仅由 skillgen 启用的 `ForegroundOnly` 选项，默认 false 保持渠道行为。
该模式仍使用 `prompt_async` 传输，但本地调用必须等待该次尝试的明确终态：

| 事件/状态 | 草稿模式行为 |
|-----------|--------------|
| 正常生成并最终完成 | 返回完整答案 |
| `session.error` / provider 重试耗尽 | 返回真实 error，不把错误通知当作成功正文 |
| 需要权限或交互回答 | 返回明确的需要交互错误，并有界中止本次草稿；不让隔离草稿无人回答地挂起 |
| 超时、取消或静默挂起 | 有界 Abort、清理本次 handler，再返回 error |
| 可切换模型 | 不启动脱离当前 context 的后台恢复；由 Service 现有的模型候选链管理下一次尝试 |

实现范围包含 [StreamingSessionHandler](../internal/opencode/event_listener.go#L322)
和 [SendMessageStreamingWithEvents](../internal/opencode/client.go#L5304)：

- handler 保存受锁保护的终态错误，调用方在完成时读取；不靠解析提示字符串判断成功。
- provider fallback、provider retry-limit、silent-hang、普通 handoff、ctx 超时、
  初始发送失败均遵守前台模式；不能遗漏某条返回“仍在执行”通知的旧分支。
- 本次尝试返回前，完成有界中止及 handler 清理；若中止失败、远端是否结束不确定，
  返回明确错误并停止继续启动下一模型，记录待核验会话，不伪装成已清理成功。
  该结果通过可判断的错误类型传播，Service 必须区分“可尝试下一模型”与
  “远端未确认停止”，不能依赖错误字符串，也不能对所有 error 无条件继续模型链。
- 不在持有 SSE handler 锁时等待自己的完成回调；清理只移除本次拥有的 handler，
  不能删除其他尝试已注册的新 handler。
- 不修改普通渠道默认的“本地超时后允许继续投递”行为。

### 4.2 正文与控制事件彻底分离

复用已有 [StreamEventKind](../internal/opencode/stream_event.go#L3)：

- 文本兼容回调使用 no-op；结构化回调单独保存 SessionReady 的完整 ID，
  只将 TextDelta 累积为草稿正文。
- Thinking、Tool、Step、Todo、Info、Flush 均不得进入草稿。
- Error、PermissionAsked、QuestionAsked 作为前台失败/交互结果处理。
  回调返回 error 目前只会被日志记录，因此必须通过前台模式的状态和取消路径传播，
  不能只从 eventCallback 返回一个 error 就认为生成已终止。
- `session.error` 即使已经积累部分文本，也不能产出可审批的成功草稿。
- 对同步 fallback 的 `Response.Reply` 只在调用成功且最终完成后使用；
  不能把 Deferred、handoff 提示或超时提示当作模型答案。

只有最终成功、无错误且格式有效时才调用 `parseDraftReply`；
本次流式尝试终态及清理完成后才调用
[ClearSkillgenSession](../internal/opencode/client.go#L2689)。
优先移除盲轮询；如保留短轮询兜底，仅允许在已确认最终成功但文本缺失时执行，
且服从原 context 和同一 session，不用它等待另一条后台 lineage。

测试必须覆盖“只有 SessionReady 没有正文”，避免 session ID 让空内容检查错误通过。

### 4.3 预算、收件人与失败通知

[Service.run](../internal/skillgen/service.go#L280)已经有 context；
不再以“run 没有可用 context 生命周期”为由丢弃取消语义。
保留总任务预算和每模型预算的父子关系，剩余预算不足时不启动新尝试；
Service 是草稿唯一的模型 fallback 管理者。

收件人在入队前解析一次，写入同一事件快照，贯穿 DraftInput、候选记录和通知：

- 原 adapter/userID 有效时直接使用。
- 缺 userID 时，只允许该 adapter 的 SoleUser 唯一解析；不跨渠道猜测。
- 多用户/无用户不能解析时，记录不可投递原因并跳过生成。
- cron thread 和 skillgen 自身的草稿 thread 不触发 skillgen。
- 不能只在 STEP6 临时补收件人，否则候选归属与通知对象仍不一致。

扩展 [Notifier 接口](../internal/skillgen/service.go#L156)：

```go
type Notifier interface {
    NotifyCandidate(adapter, userID, candidateID, title string, approvalRequired bool) error
    NotifyFailure(adapter, userID, stage, reason string) error
}
```

同步更新 RegistryNotifier、所有测试 fake/mock 和构造调用；不使用类型断言静默忽略缺失实现。

失败通知规则：

- 所有模型失败、预算耗尽、持久化失败、安装失败均记录具体阶段；
  有有效收件人时发送一次明确失败通知。低置信度主动放弃与生成失败分开记录。
- 用户消息使用简短、脱敏的原因，不直接转发上游原始 JSON 或完整 payload。
- 按 adapter/userID 设置独立的失败通知限额；现有 MaxPerDay 统计候选记录，
  不能直接用它约束“未生成候选”的失败通知。
- 被限流的失败仍记录日志；通知发送失败必须记录，不能返回成功形状的静默结果。
- 无效收件人是可观测的跳过/错误，不是 NotifyFailure 的无声成功。
- 安装或存储未成功时，不能发“已安装”或无法实际审批的成功通知。

### 4.4 参考文件与安装目录分别解析

替换旧版共用的 `resolveSkillPath`：

- 参考文件：必须是可读的普通文件；默认相对路径先沿用 CWD 解析，
  找不到时才检查 exe 目录下同一相对路径，不改变已有可用相对配置的优先级；
  其他部署布局应显式配置路径，不猜测祖先目录。
- 安装目录：必须按目录处理；允许目标目录尚未创建，确定绝对路径后创建并报告错误。
  不能使用 `!IsDir()`，也不能因为目录不存在就原样返回一个含糊的相对路径。
- 显式绝对路径不改写；存在但类型不对、无权限等错误直接报告，不悄悄换到另一位置。
- 新建安装目录没有已有布局可参考时，保持 CWD 相对语义并记录最终绝对路径，
  不自动选择某个“恰好存在”的无关目录。
- CandidateDir、InstallDir 和 ReferenceSkillPath 在启动时统一完成解析，
  Service、Drafter 和审批命令共享同一份解析后的配置。
- 参考文件未随包部署、为空或不可读时启动即告警，明确草稿将降级运行；
  不再把参考缺失描述为必然导致生成失败。
- 部署验收确认安装目录确实在 opencode 的技能发现范围内，不能以“文件已写出”替代“技能可用”。

### 4.5 无 handler 错误的可观测性

在 [无 handler 分支](../internal/opencode/client.go#L2932)补充有限长度的错误类型和消息，
同时保持后续全局事件分发；日志使用完整 session ID，便于关联。
不要添加无清理策略、永久增长的 `dumpedOrphanErrors` map。

### 4.6 验收

- 成功草稿只包含完整正文；控制事件和 session ID 不污染解析。
- session.error、权限/提问、发送失败、取消、超时、静默挂起均作为明确失败返回。
- 模型失败时仅 Service 启动下一尝试；无脱离预算的后台草稿。
- 上一尝试尚未确认停止时不启动下一模型，不清掉仍活跃的新 handler。
- 唯一用户解析后的归属在候选记录、通知、审批之间一致。
- 成功候选可查看、批准并安装；失败通知有独立限流，Notifier 的所有实现通过编译。
- 文件路径、已存在目录、不存在目录、错误类型、不同 CWD 和缺失部署文件均有针对性测试。
- 不要求轮询超时日志绝对为零；真实故障允许发生，必须有明确阶段、终态与反馈。

---

## 问题 2：TUI继续交互后的渠道同步

### 2.1 先明确验收方向，不误判为“设计如此”

原问题是“TUI 继续交互后，渠道没有同步”，不是“渠道消息没有显示到 TUI”。
[现有命令文档](./COMMANDS.md#把-tui-里的会话转交给聊天渠道在-tui-里-rename)
已经承诺认领后回到 TUI 继续追问，输出会同步到渠道。

本次范围：

- 保证同一被认领 session 的后续模型回复到达其渠道。
- 当前 handler 会过滤用户 role 的消息；镜像 TUI 用户输入原文本不是当前已有的输出镜像能力，
  单独记录为范围外需求，不能把“回复没同步”也归入此限制。
- 渠道到 TUI 的即时界面展示取决于 TUI 对同一 session 的订阅行为，
  不能仅因网关没有主动“注入显示”的代码就断言 TUI 必然不展示。
  本次不新增反向界面注入机制。

### 2.2 `/switch`的边界与执行顺序

覆盖 [微信](../internal/adapters/wechat/wechat.go#L1848)、
[飞书](../internal/adapters/feishu/feishu.go#L2789)、
[钉钉](../internal/adapters/dingtalk/dingtalk.go#L4325)。
复用共享的准备/校验逻辑，适配器只提供平台真实的投递上下文。

不能把 `if !IsClaimedSession { ClaimSession(...) }` 直接贴在 Bind 之后：
显式 ClaimSession 没有网关自建保护，且 Bind 本身会影响
[isGatewayOwnedSession](../internal/opencode/claim.go#L240)的判断。

按以下顺序处理：

1. 解析完整目标 ID，校验用户所有权及 cron 护栏。
2. 在 Bind 之前记录目标原有的 claimed 状态、是否由当前网关持有、活跃 handler 及投递上下文。
3. 已认领目标恢复原镜像，不重新认领；普通网关目标只切换路由，
   不借此替换活跃 per-turn handler。
4. 对带有效归属标题、当前没有本地网关绑定也没有 claim 的恢复目标，
   执行受保护的恢复认领：校验归属与真实投递上下文，不能覆盖活跃 handler。
5. 所有会失败的校验和准备先完成，再提交线程映射、adapter 映射及其他 claim 的暂停。
   若提交中失败，恢复先前映射/暂停状态并返回明确错误，不只打日志后返回“切换成功”。

现有 ClaimSession 本身会改标题并执行路由 hook，不能直接把它当作无副作用的准备步骤。
共享切换逻辑须拆开准备与提交；补偿范围包括标题、两个映射、原投递 metadata 和暂停状态。
并发所有权变更沿用现有 transition 互斥；准备后重新校验，避免用过期快照覆盖新归属。

不要把模型 threadID 当成所有平台通用的 Channel；飞书私聊/群聊、钉钉 webhook
和微信聊天目标按各自 adapter 的真实字段保存。
若同一 session 正在执行、无法无损切换，明确返回忙碌状态，不强行覆盖。

当前进程自建、未认领的普通 session 切换后，不承诺自动获得永久 TUI 镜像能力；
该能力需要独立的镜像生命周期设计，不能通过给所有 session 强行 Claim 来实现。
恢复已有认领及渠道继续对话后再回 TUI 的场景属于本次必须通过的范围。

### 2.3 保留现有注册表，修可复现的生命周期缺口

本次不实施两级注册表，不将 `restoreClaimedHandler` 改为空实现。
临时 handler 本身会把回复送给渠道；替换长期 handler 不等于“这轮镜像完全死掉”。
现有 ctx 超时分支也刻意保留 handler，不能笼统声称取消必然导致永久丢失。

重点回归：

- TUI发起一轮 → 渠道收到回复 → 渠道继续一轮 → TUI再发起一轮 → 渠道仍收到回复。
- 过程中包含正常完成、provider错误、取消、超时及重连。
- handler 清理只移除它自己；旧轮次清理不得删掉新轮次或恢复后的 handler。
- 有意 `/switch` 暂停的旧 claim 不得因完成回调而意外恢复。
- 重启后通过已认领标题恢复镜像；不误认领 cron 或 skillgen。

如回归暴露具体竞态，在现有单槽机制内先做身份校验/原子替换等最小修复，
不可因为“两级表暂缓”就忽略可复现的正确性问题。
若最小修复不足以满足验收，先记录阻塞和失败用例，再重新讨论架构，不宣称本轮已修复。

### 2.4 验收与文档

- 上述连续交互序列分别在微信、飞书、钉钉通过。
- 冷启动恢复、普通网关会话、已认领/已暂停目标、活跃目标、cron目标分别测试。
- 失败切换保持原默认会话、镜像和投递上下文；不把错误伪装成成功。
- 关联完整 session ID 和实际接收内容，不按 handler 日志总数或固定下降比例判断修复。
- 更新 [COMMANDS.md](./COMMANDS.md)，区分模型输出镜像、用户原文、路由切换与 TUI 显示；
  只写已验证的行为，不能用新文档把原承诺的回复同步故障改称设计限制。

---

## 实施顺序与风险

| 阶段 | 内容 | 交付与依赖 | 风险 |
|------|------|------------|------|
| S1 | cron隔离/持久化/回调/认领护栏；Deferred准确置位；移除payload打印 | 先确定两个契约，各自形成聚焦变更与回归；不要求塞进同一个PR | 中：跨调度、存储、认领及投递，不能标为纯新增字段的低风险 |
| S2 | 队列重放分类、显式恢复与统计；完整ID错误日志 | 队列迁移和启动恢复独立验证；保持现有正文交付窗口 | 中：影响持久化消息恢复 |
| S3 | skillgen前台模式、控制事件隔离、路径和通知接口 | 前台完成与清理契约先通过测试，再迁移Drafter；包含所有fake/mock更新 | 中：不能仅称“把轮询换回调” |
| S4 | 三渠道switch修复、连续镜像回归与命令文档 | 依赖S1的cron护栏；复现测试可提前编写 | 中：恢复认领与handler生命周期有关 |

S5两级注册表继续延后。旧版按未经验证收益给出的工期和百分比承诺撤回；
实现时根据失败用例、接口变动和部署验证范围再评估。
cron优先是因为会改变用户路由，不意味着上游会话数据已被删除。

### 本轮开发记录（S1）

- 已实现共享 `ResolveOrCreateSession`，同步与流式发送共用解析逻辑；
  流式 handler 与实际 prompt 使用同一解析结果，只有明确的 404 才替换会话。
- cron 使用独立 thread 和持久化 session，发送前同步保存实际 ID、登记 cron 类型；
  保存失败不发 prompt，保留待保存 ID。启动核验旧绑定，不将普通聊天绑定当作 cron 恢复。
- 同任务从准备到终态防重入；重试改为同一 worker 内有界循环，不再重新入队同一可变 Task，
  因而没有中间重试的入队失败/旧 worker 清理新尝试问题。初次入队失败仍执行失败终态、
  取走回调并释放占用。成功、失败、取消均有终态结果；停机清理尚未执行的队列任务。
- 存储在任务锁内序列化快照，写盘串行化；getter 返回独立的任务/顶层 metadata 快照。
  Windows 替换遇到短暂权限错误时有界重试，失败仍向上传播，不删除上次成功快照。
- cron 保留在 `/sessions`，但共享解析器阻止三个渠道 `/switch` 到 cron；
  显式/自动认领、转交和默认聊天冷启动恢复也排除 cron。旧数据只有 `created_by`
  可明确识别创建者时才回填，不能确认归属或旧绑定的任务禁用并记录原因。
- 已增加 Deferred，区分后台重试、熔断停止和普通 handoff；微信后台接管时不生成完成通知。
  飞书、钉钉主消息流的晚到回调使用独立、有界的投递 context，并串行保护缓冲投递。
  已移除整个 payload 的调试打印。
- 正在运行的任务拒绝编辑或删除，避免替换其持久化绑定及终态归属；可先禁用以阻止后续 tick。

验证：受影响的 OpenCode、scheduler、微信测试通过，网关和飞书/钉钉包编译通过。
新增回归覆盖持久化失败、重启复用、错误旧绑定、忙碌防重叠、提交失败、
重试耗尽、并发保存/停机、404 与暂时故障区别、fallback Deferred、熔断停止及微信实际出站内容。
并发保存/停机等测试额外重复运行 5 次。

限制：当前环境 `CGO_ENABLED=0`，`go test -race` 未能运行；飞书/钉钉没有现成包测试，
不能把编译通过当作真实平台投递验收。S2 的后续实现见下节；S3 的 skillgen、
S4 的完整切换事务/镜像生命周期仍待开发；三个真实渠道的连续交互及晚到回调联调也尚未完成。
本文前文的源码行号来自开发前审阅快照，实施后的定位以当前文件和符号为准。

### 本轮开发记录（S2）

- 队列构造时以事务分类旧记录，在 dispatcher 启动前完成：旧 `done` 和无请求身份的
  `question` 一律停放；有效正文保留幂等 ID、分块顺序、重试次数和退避时间；
  过期非关键消息清理，超过原 24 小时窗口的内容停放。初始化/分类失败向微信启动入口返回错误。
- 增加持久化 `recovery_until`，显式恢复正文时给予独立的 24 小时投递窗口，
  不修改 `created_at` 或幂等 ID；重启保留未到期恢复窗口，到期后再次停放。
  分发公平性选出的替代消息也检查过期/年龄，不能绕过陈旧策略。
- `/recover` 仅恢复 `final`、`flush`、`error_partial`、`sync_reply` 正文，
  返回旧完成、旧确认、其他系统/进度及未分类通知的排除数量；会话筛选使用字面前缀，
  不将 `%`、`_` 当作 SQL 通配符。`/pending` 标明可恢复类型。
- 确认提示先用现有用户会话解析器核验归属，再读取服务端 `GET /permission`、
  `GET /question` 的实际待回答请求，复用现有事件解析和展示逻辑，不用旧文案或
  “同会话还有一个问题”冒充原请求有效。更新相关会话的待回答缓存，清除已失效请求；
  查询期间有 SSE 更新则拒绝覆盖新状态。API 契约核对了上游 OpenCode v1.2.0 的路由，
  运行时若旧服务不支持或查询失败则显式报错、保持停放，可稍后 `/recover` 重试。
- 重新展示旧会话的有效请求时给出完整 `/switch` 指令，不自动改动当前聊天绑定；
  已转交其他用户的会话不会据旧队列收件人重新生成提示。
- 统计区分 `active_pending`、`parked`、`due`、`waiting_backoff`、`failed_entries`
  及累计发送失败数；查询失败直接报告，不输出伪造的全零快照。保留原入站计数语义。
  队列投递失败/停放日志增加完整 session ID。

验证：网关、OpenCode、scheduler、微信完整包测试通过，飞书/钉钉包编译通过；
`git diff --check` 通过。聚焦回归额外重复运行 5 次，覆盖旧表升级、重启分类、
分类失败整笔回滚、正文恢复窗口及跨重启幂等、字面前缀、公平性过期检查、
统计分离、实际问题替换旧问题、失效/无法核验提示不重放、并发 SSE 更新保护及归属变更。
新增文件无编辑器诊断错误；保留原有无关 lint 建议不改。
当前环境仍不能运行 race detector；真实微信重连、上游版本兼容及实际消息投递仍需联调。

### 本轮开发记录（S3）

- 新增默认关闭的 `ForegroundOnly`，Skillgen 使用事件驱动前台等待：
  handler 在提交前登记，必须同时收到提交成功和明确完成，保留早于 HTTP 回应到达的正文。
  普通渠道与 knowledge 不启用此选项，继续沿用原有后台恢复行为。
- `session.error`、权限/提问、provider 重试耗尽、初次发送失败、回调错误、
  超时/取消及静默挂起均返回真实失败；取消提交后进行有界等待和 Abort。
  Abort 错误、false 或提交未结束返回 `RemoteStopUnconfirmedError`，
  保留待核验状态并停止本次模型 fallback 链，不伪装成已停止。
  handler 清理按登记令牌和实例核验归属，不删除后来登记的 handler。
- Drafter 仅累积结构化 TextDelta，SessionReady 单独保存；控制事件与兼容回调不进入正文。
  删除盲轮询，每次模型尝试使用独立临时 thread，完成且确认停止后才清理绑定。
  只有最终成功的正文进入解析；只有 session ID、部分正文后失败或 Deferred 均不能产出候选。
  `accept:false` 与低置信度是主动跳过，不发送故障通知。
- Service 在入队前只解析一次同渠道 SoleUser，事件、DraftInput、候选、通知与审批共享归属。
  cron、skillgen adapter 和草稿 thread 排除；归属不明跳过并记录原因。
  审批命令按渠道/用户隔离，列表先过滤归属再分页；旧缺失归属记录不猜测所有者。
- 每模型使用新的子预算，但受总任务 10 分钟剩余预算约束；父 context 结束后不再启动下一模型。
  仅 Service 管理模型切换，无配置模型时只尝试一次服务端默认模型。
  失败记录阶段并向原收件人发送脱敏提示，滚动 24 小时每渠道/用户最多 3 条，
  独立于候选数量上限；通知失败继续报告错误，不静默吞掉。
- 手动与自动模式均先写候选目录并持久化；自动安装后保存 approved 状态，最后才通知成功。
  路径在启动时统一解析并共享：目录按 CWD 创建为绝对路径；参考文件先 CWD、
  仅不存在时尝试 exe 下同一路径，绝对路径不改写。参考不可用告警降级，
  目录错误明确禁用 Skillgen。README 与命令文档已同步。
- 修复紧耦合的 retry part 自锁：事件入口不再持有 handler 锁调用同样获取该锁的处理函数。
  无 handler 的会话错误日志包含完整 session ID 和有长度限制的错误信息，保持全局事件分发。

验证：

- 网关、OpenCode、Skillgen、scheduler、微信完整包测试通过；飞书/钉钉包编译通过。
  knowledge 完整包测试通过，确认共用客户端仍兼容默认调用。
- 新增前台终态与流水线回归重复运行 5 次通过，覆盖快速完成、部分正文后失败、
  权限/问题、retry 耗尽、初次发送失败、超时/取消、回调失败、Abort 失败/false、
  新 handler 保护、静默中止、预算父子 deadline、收件人快照、审核归属、
  存储/安装失败无成功通知、自动安装通知前文件与 approved 状态已存在及独立失败限流。
  路径与正文隔离测试通过。
- memstore 全量验证未完成：首次 240 秒上限耗尽于既有
  `TestContentlessIndexSpaceSaving`；排除后再次耗尽于既有
  `TestIndexBatchScalesWithArchiveSize`。未修改这两项无关性能/空间测试。
  memstore 的 `TestOpen*`、`TestBM25FiltersBeforeCandidateLimit` 聚焦测试通过；
  本次候选保存、归属分页与安装路径查询由 Skillgen 流水线使用实际 SQLite 覆盖。
- 新增/修改 S3 文件无新增编辑器诊断，保留原有无关 TrimPrefix 建议；
  `git diff --check` 通过。

限制：不能将以上结果称为整个 memstore 包全绿；当前环境仍未运行 race detector。
真实渠道失败提示、真实 OpenCode 版本行为、部署后的技能发现范围仍需联调。
S4 的切换事务及 TUI 连续交互镜像见下节；本轮未提交 Git，也未启动生产服务。

### 本轮开发记录（S4）

- 新增与渠道无关的 `Client.SwitchSession`，四个渠道（微信、飞书、钉钉、企微）的
  `/switch` 全部改为调用它，适配器只负责提供本平台真实的投递字段。
  执行顺序固定为：解析完整会话 ID 并校验归属与 cron 护栏 → 取 `claimTransitions`
  会话锁 → 重新读取会话确认归属未被并发改写 → 运行中会话直接返回忙碌 →
  快照认领状态/网关归属/活动 handler/投递上下文 → 判定是否需要恢复认领 →
  恢复认领前校验投递目标 → 先执行唯一可能失败的 adapter 路由提交 →
  再绑定 thread、暂停同用户其他认领、恢复目标认领、挂载长期 handler。
- 提交阶段失败时用快照补偿：恢复用户映射、上一会话的反向映射与投递元数据、
  目标会话自身的投递元数据，并返回明确错误，不再“记日志后报告成功”。
  `SwitchUserSession` 会删除上一会话的反向映射和元数据，因此快照必须包含它们；
  补偿路径刻意不走 `SwitchUserSession`，否则回滚本身也会破坏目标会话的数据。
- 恢复认领（标题有归属前缀、本地无绑定、无认领记录）在改动任何状态前校验投递目标：
  入站消息带平台字段、会话已有投递元数据、或 adapter 已认识该用户三者之一成立才继续。
  钉钉在没有目标时返回 nil，缺这一步会把“每次发送都被丢弃”报告成镜像成功。
- 已有活动 handler 的会话拒绝恢复认领，不覆盖按轮次 handler；已认领目标只恢复镜像，
  不重复认领；普通网关会话只切换路由，不强行认领（避免把每个会话变成永久镜像）。
- `ChannelData` 使用 `map[string]string` 保留各平台真实字段：飞书 `receive_id` +
  `receive_id_type`，钉钉 `channel`（会话 webhook），微信/企微无额外字段。
  新增 `base.SessionData`/`RestoreSessionData`/`RestoreUserSession`/`RestoreSessionUser`
  四个快照/恢复原语，`SessionData` 返回副本，避免快照被后续 `MapSessionData` 就地改写。
- 路由桥接放在 `cmd/gateway`（`registrySessionRouting`）：`internal/opencode` 不能
  导入 adapters，`internal/adapters/base` 也不应知道会话切换语义。

验证：

- 新增 `internal/opencode/session_switch_flow_test.go`（11 项）与
  `cmd/gateway/session_routing_test.go`（10 项），覆盖：绑定 thread 并提交路由、
  提交失败补偿、忙碌拒绝、cron 目标拒绝、他人会话拒绝、活动 handler 拒绝恢复认领、
  无投递目标拒绝恢复认领、TUI 会话恢复认领、恢复被暂停的认领、暂停同用户其他认领、
  TUI 轮次 → 渠道回复 → 渠道轮次 → TUI 轮次后渠道仍收到输出；
  以及快照完整性、元数据副本隔离、飞书/钉钉字段保留、空字段丢弃、
  缺失 adapter 报错、补偿恢复上一会话与目标元数据、无历史元数据时清除、
  首次切换失败清除用户映射、投递校验的通过与拒绝分支。
- 网关、OpenCode、Skillgen、scheduler、微信完整包测试通过；飞书/钉钉/企微包编译通过。
  聚焦回归（`TestSwitch*`、路由桥接、`TestClaim*`、`TestSuspended*`、`TestForeground*`）
  重复运行 5 次通过。`git diff --check` 通过。

限制：以上为 fake 上游与 fake 路由的单元级验证，未在真实飞书/钉钉/企微/微信上
跑通连续交互；`ValidateSessionDelivery` 只是前置检查，平台是否真能投递仍由发送路径决定。
当前环境仍不能运行 race detector。S5 两级注册表继续延后。

## 通用验证与发布要求

1. 测试使用仓库现有 runner、fake server 和已有测试文件，按改动范围运行；
   不为文档修订本身新增构建工具。
2. 对新增的前台模式、Deferred、终态回调和共享解析入口，保持默认值兼容，
   验证普通渠道原有发送、权限回答、后台长任务和非cron调度行为。
3. 日志包含完整 session ID，以及适用的任务/投递 ID、阶段和结果；不输出整个 payload。
   历史日志不能唯一关联时明确记为未知，不做精确收益估计。
4. 日志可辅助排查，但端到端验收必须检查实际渠道输出、默认session绑定、
   持久化内容及清理后的handler状态，不能用“某条日志没出现”代替功能验证。
5. 失败路径也是合法终态：上游不可用、用户取消、预算耗尽、通知失败必须可观测；
   不要求所有错误日志为零，也不保证所有后台接管最终都产生答案。
6. 发布前保存任务/队列数据库备份并验证旧数据升级；park不删除正文；
   不自动释放或改写用户现有认领。部署包单独验证技能文件布局和实际技能发现路径。
