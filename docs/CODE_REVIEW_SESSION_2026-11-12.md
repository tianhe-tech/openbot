# Session 迁移审查

## 范围与结论

审查对象：当前工作区的 Session claim/release/transfer/switch/suspend/resume、标题认领、流事件 handler，以及适配器与客户端绑定。不包含记忆归档、知识提取、MCP 检索、数据库升级/删除及存储租约的验收。

结论：4 项主要缺陷均经临时回归探针复现，现已全部修复并转为常驻回归测试。源端解绑、唯一目标归属、失败保持原状态和暂停恢复轮次隔离在单元层面已满足；真实平台端到端、race 与重启语义仍未验收（见“待验收项”）。

本报告整理已有证据，未修改业务代码、部署或提交。“复现”为模拟 HTTP/流事件实验，不等于真实聊天平台端到端验证。原合并报告 F 编号仅用于追溯，以下使用独立 S 编号。

## 主要缺陷

### S01 · P1 · 转移后源聊天仍能写入目标会话【已修复，原 F01】

- 位置：[TransferSession](../internal/opencode/claim.go)、[resolveThreadSession](../internal/opencode/claim.go)、[checkExplicitSessionOwnership](../internal/opencode/claim.go)、[线程回退](../internal/opencode/client.go)。
- 原缺陷：`/switch` 把源 ThreadID 绑定到被认领 Session。转移只释放 claim，没有撤销 c.sessions 的旧绑定；清理适配器用户绑定不能清掉这层映射。源端下一条消息没有显式 SessionID 时，按 ThreadID 回退继续使用已经转给另一平台/用户的会话，只验证存在性而不验证新归属。
- 修复：转移在受控状态转换中撤销旧绑定（`clearSessionThreads`），并在发送入口增加归属校验。`resolveThreadSession` 成为 thread→session 回退的唯一判定点，`SendMessage` 与 `SendMessageStreamingWithEvents` 共用，避免两条路径规则漂移；校验失败的绑定被删除而非仅忽略，否则该线程后续每条消息都会重复同一次被拒查找。`checkExplicitSessionOwnership` 覆盖适配器显式传 SessionID 的路径（第二个认领同一会话的聊天、重启后残留的适配器映射），`Internal` 载荷豁免（调度器自建会话）。
- 回归测试：`TestTransferClearsSourceThread`、`TestTransferRejectsSourceThreadEvenIfBindingSurvives`、`TestResolveThreadSessionAllowsUnclaimedSessions`、`TestExplicitSessionOwnershipRejectsOtherChat`。

### S02 · P1 · 返回成功但输出仍发给旧接收方【已修复，原 F02】

- 位置：[TransferSession](../internal/opencode/claim.go)、[claimTransitions](../internal/opencode/claim.go)、[claimSession](../internal/opencode/claim.go)。
- 原缺陷：release 到 PATCH 之间收到旧标题事件，会重建旧接收方 claim；随后目标认领看到已有 handler 提前返回，不替换旧归属。结果可为标题 `[feishu:bob]`、命令成功，实际 claim 仍为 `wechat/alice`。
- 修复：按 Session 串行化所有权转换（`claimTransitions` 覆盖 claim/unclaim/transfer），并在标题认领路径校验事件代次——`claimSession(requireTitle=true)` 在改写标题前重新读取上游标题，前缀与事件声明不一致时返回 `stale session ownership event`，旧事件无法重建旧所有者。
- 回归测试：`TestTransferIgnoresOldTitleDuringPatch`、`TestClaimSessionIsIdempotent`、`TestUnclaimAllowsReclaimByAnotherChannel`。

### S03 · P1 · 失败后原有转发也被破坏【已修复，原 F06】

- 位置：[TransferSession](../internal/opencode/claim.go)、[UnclaimSession](../internal/opencode/claim.go)。
- 原缺陷：修改上游标题前已释放 claim/handler；PATCH 失败没有恢复，导致标题保留旧归属而旧转发停止。
- 修复：转换顺序改为“先 PATCH 标题、再通知 hook、最后才释放并重挂”，任何一步失败都直接返回且不触碰现有 claim/handler；hook 失败时额外回滚标题，使上游与本地状态一致。`UnclaimSession` 走同一顺序。
- 回归测试：`TestTransferFailurePreservesSource`、`TestClaimSessionPropagatesPatchFailure`、`TestUnclaimSessionStripsPrefixAndReleases`。

### S04 · P2 · 暂停恢复后混合两轮回答【已修复，原 F07】

- 位置：[SuspendClaimedSession](../internal/opencode/claim.go)、[ResumeClaimedSession](../internal/opencode/claim.go)、[attachClaimedSessionHandler](../internal/opencode/claim.go)。
- 原缺陷：暂停注销 handler，但保留同一个 forwarder/buffer；期间的 idle/end 不会到达它。恢复复用未结束对象，新轮 delta 混入旧缓冲。
- 修复：恢复时丢弃未结束的 handler 与缓冲，重新构建 forwarder，并以 `longLivedTurnMessageID` 对齐上游轮次；旧 handler 的迟到事件因 `claimedSessions` 中的 handler 指针已不匹配而被丢弃。
- 回归测试：`TestResumeDiscardsUnfinishedTurn`、`TestSuspendClaimedSessionDetachesHandler`、`TestSuspendClaimedSessionDropsDelivery`、`TestResumeClaimedSessionReattachesHandler`、`TestSuspendClaimedSessionsForUserSkipsTarget`、`TestRestoreClaimedHandlerRespectsSuspend`、`TestReleaseClaimedSessionClearsSuspend`。

## 设计边界与其它风险

1. **暂停状态不持久。** suspendedClaims 只在内存，标题保留前缀。重启后标题认领可重新启用暂停会话。若“只让当前选中会话转发”需跨重启保持，这一要求未实现；未做进程重启 E2E。
2. **在途输出隔离部分覆盖。** 注销 handler 不能撤回执行中的回调或排队发送。当前以 handler 指针比对 + `suspendedClaims` 双重判定丢弃迟到投递（`TestSuspendClaimedSessionDropsDelivery`），但“已进入平台 HTTP 调用”的那一次发送无法撤回；需要真实平台故障注入才能验收。
3. **同平台跨用户清理已覆盖。** 归属校验按 channel+userID 完整比对，不再按“目标 channel 是否为 wechat”判断，因此转给同平台另一用户同样会解绑源端（`TestExplicitSessionOwnershipRejectsOtherChat`）。
4. **注册不等于可投递。** channel checker 确认 adapter 注册，不保证凭证、用户标识和实际发送可用；claim hook 错误还有只记录日志的路径。应明确成功回执承诺的是路由更新还是目标可接收。
5. **路由迁移不等于历史迁移。** 转移标题不会复制归档 principal 的历史；纯 TUI 回合也不会仅因 claim 自动获得 capture state。此处仅说明边界，不把完整历史同步默认为本功能要求。跨用户历史转移需要独立权限与数据设计。

## 原设计对照

基线为会话中明确的要求：直接转移 TUI 会话归属；源端解除绑定；切走暂停但保留原会话及可选性；能够切回。

| 要求 | 结论 | 依据 |
| --- | --- | --- |
| 转移后源端不继续使用目标会话 | 单元层面满足 | S01 修复 + 4 项回归测试 |
| 成功回执与实际输出目标一致 | 单元层面满足 | S02 修复 + 3 项回归测试 |
| 转移/释放失败不破坏原有功能 | 单元层面满足 | S03 修复 + 3 项回归测试 |
| 切回后回答不混合轮次 | 单元层面满足 | S04 修复 + 7 项回归测试 |
| 保留会话且能选择切回 | 有实现 | `TestResumeClaimedSessionReattachesHandler` |
| 暂停状态跨重启保持 | 当前不持久，需要明确验收要求 | 设计边界 1 |

## 独立验证记录

| 原临时探针 | 复现的缺陷 | 现常驻回归测试 | 结果 |
| --- | --- | --- | --- |
| TestAuditTransferClearsSourceThread | S01：源线程映射残留 | `TestTransferClearsSourceThread`、`TestTransferRejectsSourceThreadEvenIfBindingSurvives`、`TestResolveThreadSessionAllowsUnclaimedSessions`、`TestExplicitSessionOwnershipRejectsOtherChat` | 通过 |
| TestAuditTransferOldEventInterleaving | S02：成功但旧接收方仍有效 | `TestTransferIgnoresOldTitleDuringPatch`、`TestClaimSessionIsIdempotent`、`TestUnclaimAllowsReclaimByAnotherChannel` | 通过 |
| TestAuditTransferFailureKeepsClaim | S03：失败后旧 claim 丢失 | `TestTransferFailurePreservesSource`、`TestClaimSessionPropagatesPatchFailure`、`TestUnclaimSessionStripsPrefixAndReleases` | 通过 |
| TestAuditSuspendMidTurnBuffer | S04：旧轮和新轮缓冲混合 | `TestResumeDiscardsUnfinishedTurn`、`TestSuspendClaimedSessionDetachesHandler`、`TestSuspendClaimedSessionDropsDelivery`、`TestResumeClaimedSessionReattachesHandler`、`TestSuspendClaimedSessionsForUserSkipsTarget`、`TestRestoreClaimedHandlerRespectsSuspend`、`TestReleaseClaimedSessionClearsSuspend` | 通过 |

原 4 项临时探针在修复前失败，原因与控制流吻合，不是编译或真实服务连接失败；探针已删除，其断言已并入上表的常驻测试。

修复后执行：`go test ./internal/opencode ./internal/adapters/... ./cmd/... -p 1 -count=1` 全部通过（单次运行耗时，随机器波动：opencode 32.2s、base 0.9s、wechat 4.4s、gateway 1.2s、mcp 3.1s、memory-delete 1.9s、memory-migrate 1.4s），`go vet ./internal/opencode` 无输出。飞书、钉钉、企微仍为 `[no test files]`，不能认为这些平台命令已验收。共享包测试通过不代表迁移状态机在真实平台上正确，也不借用记忆功能测试作为本功能证明。

`gofmt -l internal cmd` 仍会列出约 60 个文件，全部是仓库既有的 CRLF 换行文件（`core.autocrlf` 造成）。本次改动涉及的 Go 文件中被列出的少数几个经逐字节核对为纯 CRLF 差异（CRLF 数等于 LF 数），不含真实格式问题；本次新增的 Go 文件除用户自有的临时文件 `test/test1.go` 外均不在该列表中。

`internal/memstore` 未列入本命令：该包会构建真实归档并运行索引 worker，单包耗时超过 Go 默认 10 分钟（本轮实测 `ok 852.833s`），需单独以 `-timeout 40m` 运行（见记忆系统报告）。

## 待验收项

以下各项**仍未验证**，单元测试通过不构成验收：

- 源端 switch 后转移，源端下一条消息、目标端下一条消息和输出目标的完整链路（真实平台）。
- 旧标题事件交错、重复事件、连续转移、PATCH/claim hook 失败及补偿（真实上游 + 故障注入）。
- 同平台跨用户和跨平台转移，两层映射都正确解绑（真实适配器命令链）。
- 半轮暂停、错过 idle、恢复新轮，以及已排队/执行中输出的处理（真实流事件时序）。
- 暂停后的进程重启，确认已选定的恢复语义。
- 四个平台的真实命令链、异常投递和 race；本轮未完成这些验证。

## 修复优先级

1. ~~将 S01/S02/S03 作为同一个转移状态机问题处理，验证源绑定、目标归属、旧事件拒绝及失败补偿。~~ 已完成（单元层面）。
2. ~~修 S04，明确暂停期间输出策略、恢复轮次和重启语义。~~ 已完成（单元层面；重启语义仍待验收）。
3. 补实际适配器命令链与故障注入，再单独验收 Session 迁移。记忆系统的通过或失败不替代本功能结论。