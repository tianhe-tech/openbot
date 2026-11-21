# 记忆系统改造审查

## 范围与结论

审查对象：提交 `3cf08de`（update memory system，相对 `78edeb5`）及当前未提交的记忆改造。范围包括 capture、归档存储、MCP 检索、知识提取与注入、事实管理、数据库升级/删除和实例租约。不包含 Session claim/transfer/suspend 的功能验收。

结论：存在 12 项主要缺陷，其中 6 项经临时回归探针复现。**12 项现已全部修复并转为常驻回归测试**，原始输入持久确认、自动知识提取、持久任务恢复及事实作用域等要求在单元层面成立。真实平台端到端、双进程失租隔离、race 与生产规模升级仍未验收（见“待验收项”），因此不能据此宣布记忆系统改造整体完成。

本报告整理已有审查证据，没有新增业务修改、生产数据库操作、部署或提交。“复现”指临时数据库/模拟服务实验；“源码确认”不等于真实环境故障注入。原合并报告的 F 编号仅用于追溯，以下使用独立 M 编号。

## 主要缺陷

### M01 · P1 · 原文提交前删除微信持久入站记录【已修复，原 F03】

- 位置：[batcher 确认](../internal/adapters/wechat/inbound_batcher.go#L161)、[生产 dispatch](../internal/adapters/wechat/wechat.go#L751)、[重放](../internal/adapters/wechat/inbound_queue.go#L241)、[beginCapture](../internal/opencode/capture.go#L345)。HEAD 已存在此路径。
- batcher 按 dispatch 返回 nil 删除记录；生产 handler 却在启动 goroutine 后立即返回 nil，尚未等待 BeginTurn 提交。busy 分支只进入内存待处理队列，也返回 nil。重启重放使用同一 handler。
- 崩溃或 strict capture 失败发生在这个窗口时，消息可能既不在持久 inbox，也未进入 archive。违反“成功确认代表持久提交，而非仅入队”。同步 fake dispatch 的单测覆盖不到生产边界。
- 修复：dispatch 改为返回 `errInboundDeferred`，batcher 收到该错误时保留 inbox 行；删除动作移到 `OnCaptureCommitted` 回调（`acknowledgeCapture`），即 BeginTurn 真正提交之后。busy 分支同样保留行。重放改为翻页读取，不再只取首 200 条。
- 回归测试：`TestInboundDeferredDispatchRetainsRowsUntilCapture`、`TestInboundBatcherPersistsBeforeDispatch`、`TestInboundBatcherKeepsRowOnDispatchFailure`、`TestInboundReplayReadsBeyondFirstPage`、`TestInboundQueueSurvivesRestart`。

### M02 · P1 · 非法搜索游标触发 panic【已修复，原 F04】

- 位置：[解码](../internal/memstore/search.go#L142)、[切片](../internal/memstore/search.go#L277)。HEAD 已存在。
- query/scope/generation 校验不约束 Offset 非负。保持其它字段合法、将 Offset 改为 -1，进入 `hits[offset:end]`。
- `TestAuditNegativeSearchCursor` 捕获 `runtime error: slice bounds out of range [-1:]`。进程级影响取决于外层是否恢复 panic，不应把字段绑定当成输入安全校验。
- 修复：解码阶段校验 `cursor.Offset < 0 || cursor.Offset > maxSearchCandidates` 并返回 `memstore: invalid search cursor offset`，切片前不再信任游标字段。
- 回归测试：`TestSearchCursorRejectsInvalidOffsets`、`TestSearchPaginationAndCursorBinding`。

### M03 · P1 · 自动知识提取使用错误会话 ID【已修复，原 F05】

- 位置：[候选事件](../internal/opencode/client.go#L6230)、[提取查询](../internal/knowledge/service.go#L260)、[RecentEvidence](../internal/memstore/knowledge.go#L953)、[归档 ID](../internal/memstore/lifecycle.go#L65)。
- 事件携带上游 `ses_*`，查询却匹配归档 `messages.session_id`，其值为 principal/thread/workdir 生成的哈希。上游 ID 存在 attempt 绑定中。
- 正常 strict capture 数据因此查不到证据，不调用 drafter。`TestAuditProductionSessionEvidence` 使用真实 BeginTurn/BindAttempt/SaveMessage 数据，drafter 调用次数为 0。人工使用相同 ID 的测试掩盖了接线问题。
- 修复：`RecentEvidence` 改为按 attempt 绑定解析上游 `ses_*` 到归档会话，证据查询走真实映射而不是直接匹配哈希 ID；持久任务只保存 scope，重启后从会话读回，避免把共享归档容器中其它上游会话内容混入本次提取。
- 回归测试：`TestKnowledgeWorkerRestoresRealCapturedSource`（真实 BeginTurn/BindAttempt/SaveMessage 数据，drafter 必须被调用且拿到 2 条证据）、`TestRecentEvidenceReturnsTheTailInChronologicalOrder`、`TestKnowledgeJobScopeReadsTheScopeBackFromTheSession`。

### M04 · P2 · 正常关闭后无法删除实例租约【已修复，原 F08】

- 位置：[关闭顺序](../cmd/gateway/main.go#L345)、[租约](../internal/memstore/lease.go)。
- 网关先关闭 archiveWriter 及底层 store，再调用 lease.Release 删除租约。`TestAuditGatewayLeaseShutdownOrder` 返回 `memstore: release instance lease: sql: database is closed`。
- 正常停机后新实例可能被阻挡直到默认约 90 秒 TTL 到期，不满足立即交接预期。
- 修复：关闭顺序改为停止接收 → 排空写入 → 释放租约 → 关闭 store，租约释放发生在数据库仍可用时。
- 回归测试：`TestLeaseWriterCloseReleasesBeforeDatabase`、`TestLeaseReleaseAllowsTakeover`。

### M05 · P2 · 失租后没有阻止旧写者【已修复，原 F09】

- 位置：[heartbeat/renew](../internal/memstore/lease.go#L105)、[writer](../internal/memstore/writer.go)。
- heartbeat 续约失败只退出，不通知网关停止写入；提交没有持有者/代次校验。旧实例暂停超过 TTL、被另一实例接管后，恢复时仍可写入。
- “启动时拒绝第二实例”不等于“运行期只有合法持有者可写”。未做真实双进程实验。
- 修复：引入租约 fencing——提交时校验持有者与代次，失租后写入 fail-closed 返回错误；heartbeat 续约失败即标记失效，旧写者无法再提交。
- 回归测试：`TestLeaseLostWriterCannotCommit`、`TestLeaseHolderIgnoresAnExpiredRow`、`TestLeaseExpiresAfterACrash`、`TestLeaseRejectsASecondHolder`、`TestLeaseHeartbeatKeepsItAlive`。真实双进程隔离仍未验收。

### M06 · P2 · 持久知识任务未接入生产【已修复，原 F10】

- 位置：[任务入口](../internal/knowledge/service.go#L241)、[persist](../internal/knowledge/service.go#L371)。
- 存储层有持久任务 API，但生产使用 asyncwork.Queue，没有连接 EnqueueKnowledgeJob/ClaimKnowledgeJobs/CompleteKnowledgeJob。重启丢内存任务，入队返回值未处理，队列满可静默丢任务。
- 部分落库失败计为 rejected，run 仍可能成功返回，没有持久重试。PR7 的 durable jobs 目前只是底层能力，不能视为完整集成。
- 修复：生产路径改为先 `EnqueueKnowledgeJob` 持久化，再由 worker `ClaimKnowledgeJobs` 领取、`CompleteKnowledgeJob` 完成；入队返回值被检查，瞬态存储失败保留任务以便重试，无效候选才计为 rejected。
- 回归测试：`TestKnowledgeWorkerRestoresRealCapturedSource`（重启后任务仍在并被执行）、`TestEnqueueKnowledgeJobCollapsesRepeats`、`TestClaimKnowledgeJobsLeasesOnce`。

### M07 · P2 · 手动记忆和项目知识注入断路【已修复，原 F11】

- 位置：[接线](../cmd/gateway/main.go#L417)、[注入](../internal/opencode/client.go#L6029)、[项目键写入](../internal/memstore/lifecycle.go#L77)。
- `/remember` 拦截器可用，但 SetKnowledgeIndex 只在 KnowledgeEnabled=true 时接线。默认关闭自动提取时，用户保存的“全局生效”知识不会自动注入。
- 另一个独立原因：存储 Project 使用 workdir basename，注入查询使用完整 c.directory，精确匹配失败。global 事实不受这个项目键差异影响。
- 修复：`SetKnowledgeIndex` 与自动提取开关解耦，只要 store 与 writer 可用就接线；`LoadKnowledgeIndex` 查询前统一走 `workDirBasename`，与写入侧的项目键生成一致。
- 回归测试：`TestLoadKnowledgeIndexIsScopedAndBounded`、`TestLoadKnowledgeIndexSurvivesANilStore`、`TestRememberProducesARealWriteReceipt`。

### M08 · P2 · 事实身份不包含作用域【已修复，原 F12】

- 位置：[knowledgeFactID](../internal/memstore/knowledge.go#L264)、[保存](../internal/memstore/knowledge.go#L351)。
- ID 只由 principal、kind、标准化 statement 组成。同一用户在 alpha、beta 项目保存同文事实产生相同 ID；ON CONFLICT 不更新 scope/project/session，第二次返回成功却保留第一条范围。
- `TestAuditKnowledgeScopeIdentity` 复现相同 ID。影响可见范围、证据混合、删除耦合，以及项目事实提升全局范围；不等于跨 principal 泄漏。
- 修复：`knowledgeFactID` 纳入 scope、project_key、session_id；保存时校验作用域与身份一致，冲突不再静默保留旧范围。旧库通过 `TestKnowledgeReusesLegacyScopedIdentity` 覆盖的兼容路径复用既有 ID，避免重复事实。
- 回归测试：`TestKnowledgeIdentityIncludesScope`、`TestKnowledgeReusesLegacyScopedIdentity`、`TestKnowledgeScopeKeepsTaskLocalFactsLocal`、`TestSessionFactsAreReviewableButNotInjectable`。

### M09 · P2 · 列表 ID 无法用于忘记命令【已修复，原 F13】

- 位置：[列表](../internal/knowledge/commands.go#L139)、[忘记](../internal/knowledge/commands.go#L91)、[ID 判断](../internal/knowledge/commands.go#L181)。
- 列表显示前 12 字符，解析要求至少 16 字符，且没有前缀解析。短 ID 被当作 statement，无法匹配事实。
- `TestAuditForgetDisplayedID` 使用列表的 `effe090dc40b` 删除失败；完整 ID 路径可以工作。
- 修复：列表展示完整 ID；`/forget` 在参数不像 ID 时按精确 statement 在本人作用域内解析成 ID，解析不到就明确报错，不再把短 ID 当 statement 静默失败。
- 回归测试：`TestForgetResolvesAStatementToAnID`、`TestForgetRemovesTheFactAndRestoresTheSupersededOne`、`TestKnowledgeHelpListsEveryCommand`。

### M10 · P2 · 长消息详情续读不前进【已修复，原 F14】

- 位置：[GetMessages](../internal/memstore/detail.go#L117)、[MCP](../cmd/mcp/main.go)。HEAD 已存在。
- 单条消息超过预算时，NextCursor 只带消息 ID，没有内部偏移。按说明续读再次返回同一前缀。
- `TestAuditDetailContinuationProgress` 使用大于 256 KiB 最大预算的回答，两次结果相同，尾部无法通过该续读协议获取。原文可能完整存储，不能据此推断 scroll 等其它接口也无法读取。
- 修复：`detailCursor` 增加 `Offset` 与 `Digest`，续读游标绑定 scope、消息版本和字节偏移；解码时校验偏移非负、落在 rune 边界且摘要匹配，否则返回 `memstore: detail cursor no longer matches message`。
- 回归测试：`TestDetailContinuationReconstructsLongMessage`、`TestGetMessagesRespectsBudget`、`TestGetMessagesReturnsOriginalBodies`。

### M11 · P2 · 内部知识提取没有排除归档【已修复，原 F15】

- 位置：[drafter payload](../internal/knowledge/drafter.go#L42)、[capture 排除条件](../internal/opencode/capture.go#L345)。
- drafter 使用 knowledge channel、knowledge-draft-* ThreadID，却未设 Internal；capture 只显式排除 skillgen。
- 提取一旦运行，内部抽取提示与输出会进入原始归档。M03 当前会更早阻断常规触发，掩盖此问题。服务防递归逻辑不能替代归档排除。
- 修复：drafter payload 设置 `Internal: true`；`beginCapture` 在边界统一排除 `payload.Internal`，与 skillgen 的 channel/thread 前缀排除并列，内部派生工作不再进入原始归档。
- 回归测试：`TestCaptureInternalBootstrapPromptIsNotArchived`、`TestCaptureDeliveryReplayAndInternalExclusion`。

### M12 · P2 · 手动事实引用的不是本次记忆指令【已修复，原 F16】

- 位置：[cmdRemember](../internal/knowledge/commands.go#L52)。
- 命令在 capture 前拦截，保存时取 principal 下 LatestMessageID 作证据；该消息可能来自另一个会话且完全无关。没有历史消息的新用户也不能直接保存第一条事实。
- 引用存在且权限正确不等于它支持声明。
- 修复：`RememberKnowledgeFact` 先 `BeginTurn` 持久化本次 `/remember` 指令本身，再用该 turn 的输入消息 ID 作为证据，不再借用 principal 下的 `LatestMessageID`。
- 回归测试：`TestRememberArchivesItsOwnEvidence`、`TestRememberProducesARealWriteReceipt`、`TestLatestMessageIDIsScopedAndHonest`。

## 其它风险

1. ~~[ExistingFacts](../internal/knowledge/service.go#L302) 只提供 statement，但 [drafter 提示](../internal/knowledge/drafter.go#L207) 要求 supersedes 填旧 ID。~~ 已修复：`ExistingFacts` 现在渲染为 `id=<ID>: <statement>`，替换链可建立；由 `TestBuildExtractionPromptCarriesTheEvidenceIDs` 覆盖。
2. ~~[waitForAssistantReply](../internal/knowledge/drafter.go#L83) 发现首个非空 assistant 文本即返回，未确认终态。~~ 已修复：改为等待 `time.completed` 终态；由 `TestDrafterWaitsForCompletedReply` 覆盖。
3. ~~[微信重放](../internal/adapters/wechat/inbound_queue.go#L241) 启动时只 List(200) 一次，没有继续翻页。~~ 已修复：重放改为翻页读取直到取空；由 `TestInboundReplayReadsBeyondFirstPage` 覆盖。

这些源码发现不能冒充已完成的真实故障注入。本轮没有建立“所有 scope 查询都存在跨用户泄漏”的结论。

## 原设计对照

基线：[记忆实现计划](MEMORY_IMPLEMENTATION_PLAN.md)、[知识说明](MEMORY_KNOWLEDGE.md)。

| 要求 | 结论 | 依据 |
| --- | --- | --- |
| 原始输入持久提交后才确认 | 单元层面满足 | M01 修复 + 5 项回归测试 |
| 原文可按详情续读协议完整获取 | 单元层面满足 | M10 修复 + 3 项回归测试 |
| 读取绑定可信 principal | 单元层面满足 | M02 修复 + 2 项回归测试 |
| 知识提取可恢复、使用真实来源 | 单元层面满足 | M03/M06/M12 修复 + 6 项回归测试 |
| 作用域正确、知识可纠正和删除 | 单元层面满足 | M07/M08/M09 修复 + 7 项回归测试 |
| 内部派生工作不污染原始记忆 | 单元层面满足 | M11 修复 + 2 项回归测试 |
| 单实例写入与干净交接 | 单元层面满足 | M04/M05 修复 + 7 项回归测试 |

数据库升级、删除一致性和性能等没有获得全面验收结论。计划本来就列为未完成的阶段应保持未完成，不能一律算新增 bug，也不能因其它包测试通过就标为完成。

## 独立验证记录

记忆系统的 6 个临时探针及其对应的常驻回归测试：

| 原临时探针 | 复现的缺陷 | 现常驻回归测试 | 结果 |
| --- | --- | --- | --- |
| TestAuditNegativeSearchCursor | M02：负偏移 panic | `TestSearchCursorRejectsInvalidOffsets`、`TestSearchPaginationAndCursorBinding` | 通过 |
| TestAuditProductionSessionEvidence | M03：drafter 未调用 | `TestKnowledgeWorkerRestoresRealCapturedSource`、`TestRecentEvidenceReturnsTheTailInChronologicalOrder` | 通过 |
| TestAuditGatewayLeaseShutdownOrder | M04：数据库已关闭 | `TestLeaseWriterCloseReleasesBeforeDatabase`、`TestLeaseReleaseAllowsTakeover` | 通过 |
| TestAuditKnowledgeScopeIdentity | M08：跨项目 ID 相同 | `TestKnowledgeIdentityIncludesScope`、`TestKnowledgeReusesLegacyScopedIdentity` | 通过 |
| TestAuditForgetDisplayedID | M09：短 ID 删除失败 | `TestForgetResolvesAStatementToAnID`、`TestForgetRemovesTheFactAndRestoresTheSupersededOne` | 通过 |
| TestAuditDetailContinuationProgress | M10：续读重复前缀 | `TestDetailContinuationReconstructsLongMessage`、`TestGetMessagesRespectsBudget` | 通过 |

这些探针在修复前失败，原因与对应控制流一致，不是编译/环境失败；现已删除，其断言已并入上表的常驻测试。

修复后执行：`go test ./internal/memstore ./internal/knowledge ./internal/opencode ./internal/adapters/... ./cmd/... -p 1 -count=1 -timeout 40m` 全部通过，`go vet ./cmd/... ./internal/...` 无输出。飞书、钉钉、企微无包内测试。共享包结果只说明编译及已覆盖断言通过，不替代本功能的端到端验收。

`gofmt -l internal cmd` 仍会列出约 60 个文件，全部是仓库既有的 CRLF 换行文件（`core.autocrlf` 造成）。本次改动涉及的 Go 文件中被列出的少数几个（如 `internal/memstore/space.go`、`internal/scheduler/nl_service.go`）经逐字节核对为纯 CRLF 差异（CRLF 数等于 LF 数），不含真实格式问题；本次新增的 Go 文件除用户自有的临时文件 `test/test1.go` 外均不在该列表中。

`-timeout 40m` 是必需的：`internal/memstore` 会构建真实归档并运行索引 worker，单包耗时已超过 Go 默认的 10 分钟（本轮实测 `ok 852.833s`），省略该参数会以 `panic: test timed out after 10m0s` 结束（panic 会指向当时正在运行的测试，看起来像该测试失败，实际是包级超时）。`Makefile` 的 `test` 目标同样固定使用该参数。

## 待验收项

以下各项**仍未验证**，单元测试通过不构成验收：

- 真实入站确认边界：崩溃、捕获失败、busy 排队、重放超过 200 条（真实 iLink 链路）。
- 真实 capture ID 到知识提取、持久任务重启恢复、项目注入的完整链路（真实上游模型）。
- 事实保存、注入、替换、列表和忘记闭环；来源必须支持声明。
- 长原文分页完整拼接、非法游标、scope 隔离及索引一致性。
- 旧库各版本升级与中断恢复、生产规模升级；未操作生产数据库。
- 删除后原文、索引、事实、证据和任务的整体一致性；现有删除子集通过不等于完整证明。
- 双进程失租隔离、真实崩溃恢复、race、大规模性能；本轮未完成这些验证。

## 修复优先级

1. ~~先修 M01/M02，阻断输入丢失和搜索异常输入风险。~~ 已完成（单元层面）。
2. ~~修 M03，并同时处理 M11；接通持久任务、统一项目标识，再验收自动提取和注入。~~ 已完成（单元层面）。
3. ~~修 M04/M05，验证关闭交接与失租旧写者隔离。~~ 已完成（单元层面；双进程隔离仍待验收）。
4. ~~修事实身份、命令 ID、真实证据及详情游标，补完整行为测试。~~ 已完成（单元层面）。
5. 完成上述待验收项后，单独更新记忆系统设计门槛。Session 迁移的状态不作为本功能通过或失败的依据。