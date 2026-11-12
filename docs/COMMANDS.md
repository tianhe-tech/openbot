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

### `/new` 或 `新会话`
开启新会话（软重置）：保存当前会话的上下文摘要，下一条消息自动携带摘要进入新会话，AI 仍记得之前做过什么。旧会话历史保留在服务器，可通过 `/sessions` 查看，并用 `/switch` 切回去。

**示例：**
```
/new
```

### `/reset` 或 `重置`
硬重置会话：不保存上下文摘要，直接删除会话映射，下一条消息在全新会话中开始，旧上下文全部丢弃。适用于上下文被污染、需要干净开始的场景。

**示例：**
```
/reset
```

### `/clear` 或 `清除`
删除当前会话及其所有数据。

**示例：**
```
/clear
```

### `/sessions` 或 `/list`
列出**你自己的**会话（最多显示最近 10 个，按更新时间倒序）。

会话归属由会话标题里的 `[渠道:用户ID]` 前缀决定，因此列表只包含你创建的会话，不会出现其他用户的会话标题或工作目录。当前正在使用的会话前面会标 `▶`。

每行开头显示的是**会话摘要**，取自该会话里你发的第一条真实消息（截断到 48 字），而不是会话标题。原因是网关创建会话时标题固定为 `[渠道:用户ID] <会话/群ID>`，去掉归属前缀后只剩一串无意义的聊天 ID，无法据此判断"这个会话是干什么的"。摘要取不到时（会话没有历史、或上游读取失败）自动回退显示标题。

**示例：**
```
/sessions
```
```
� 我的会话 (共 3 个，按更新时间排序)

▶ 1. 帮我排查一下构建失败的原因
     ID: ses_a1b2 · 更新: 11-09 09:54
  2. 把 /sessions 改成显示会话摘要
     ID: ses_c3d4 · 更新: 11-08 21:10
  3. 重构微信适配器的重试逻辑
     ID: ses_e5f6 · 更新: 11-07 15:02
```

> 说明：摘要需要读取会话历史，因此每个要显示的行会额外请求一次上游（最多 10 次，4 个并发，单次 5 秒超时），结果按会话 ID 缓存，同一会话不会重复读取。读取失败的会话仍会出现在列表里，只是没有摘要。

> 说明：列表不是只扫"最近 100 个会话"。上游 `GET /session` 默认只返回最近更新的 100 条，在会话量大的部署上这个窗口会被定时任务和内部会话占满，导致每个人的列表看起来都是空的。网关会显式请求更大的页（`limit=5000`），并把 `[渠道:用户ID]` 前缀作为 `search` 参数下推给上游做预筛，因此即使你的会话很旧也能被找到。`search` 是**子串**匹配（不区分大小写、只匹配标题），所以最终仍由网关做前缀校验来保证归属，不会把标题里恰好包含你前缀的别人的会话列出来。列表范围限定在 `OPENCODE_DIRECTORY` 指向的工作区。

### `/switch <序号|会话ID前缀>`
切换到列表中的某个会话继续对话。切换后直接发送消息即可，AI 会接着该会话的上下文。

`<序号>` 是 `/sessions` 列表里的编号（从 1 开始，按更新时间倒序）；也可以直接给会话 ID 的前几位，但前缀必须唯一，否则会提示你多输入几位。

**示例：**
```
/switch 2
/switch ses_a1b2
```

> 说明：`/switch` 只切换"下一条消息发到哪个会话"，不会复制或合并历史。旧会话的历史仍保留在服务器上，随时可以再切回去。

### 把 TUI 里的会话转交给聊天渠道（在 TUI 里 `/rename`）

场景：你一直在 PC 上用 opencode TUI 干活，马上下班了，想把当前这个会话转到手机上继续。

**不需要改 TUI 代码，也不需要开 HTTP 接口。** opencode TUI 自带 `/rename` 斜杠命令，网关监听会话更新事件，看到标题带上 `[渠道:用户ID]` 前缀就自动接管。

**操作步骤：**

1. 在 TUI 里输入 `/rename`，回车。
2. 输入新标题，格式为 `[渠道:用户ID] 原标题`，例如：
   ```
   [feishu:ou_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx] 重构 gateway 的 claim 逻辑
   ```
   （`ou_xxx...` 是占位符，换成你自己的真实 `open_id`，见下表）
3. 网关收到会话更新事件，自动完成认领。**认领本身不发消息**——确认方式是看网关日志出现
   `opencode: claimed session ...` 和 `opencode: attached forwarding handler to claimed session ...`。
4. 手机上打开飞书/钉钉/企微/微信，直接发消息即可继续这个会话。
5. 回到 TUI 继续追问，输出会同步镜像到聊天（见下方"认领是双向的"）。

**渠道名与用户 ID 怎么填：**

| 渠道 | `渠道` 填什么 | `用户ID` 填什么 |
|------|--------------|----------------|
| 飞书 | `feishu` | `open_id`（`ou_` 开头） |
| 钉钉 | `dingtalk` | `staffId`（或 `senderId`） |
| 企微 | `wecom` | 企微 `userid` |
| 微信 | `wechat` | 微信 `wxid` |

> **这个 ID 不是随机的，也不是 session ID 的前缀。** 它就是网关自己建会话时写进标题的那个所有权前缀
> （`client.go` 里 `fmt.Sprintf("[%s:%s] %s", payload.Channel, payload.UserID, payload.ThreadID)`），
> 所以 `/rename` 只是让你手工写出同一个前缀。飞书的 `open_id` 按应用隔离——同一个用户换一个机器人应用就是另一个 `ou_`，必须用这个 bot 自己的那个。

**单人部署可以省略用户 ID：**

如果这个渠道上只有你一个人用过机器人，`/rename` 里只写渠道名就够了：

```
[feishu] 重构 gateway 的 claim 逻辑
```

网关会把这个渠道唯一的已知用户补进去，日志里出现：

```
opencode: resolved channel-only claim for feishu to user ou_xxxxxxxx
opencode: claimed session ses_xxx for feishu/ou_xxxxxxxx (title="[feishu:ou_xxxxxxxx] 重构 gateway 的 claim 逻辑")
```

标题会被改写成完整的 `[feishu:ou_xxxxxxxx] 原标题`，所以 `/sessions`、`/switch`、重启恢复都照常工作。

> **什么时候会失效：** 网关只在"该渠道恰好只有一个已知用户"时才补 ID（`BidirectionalAdapter.SoleUser()`）。
> 零个用户（你还没给机器人发过消息）或多个用户（多人共用同一个 bot）时，认领会被**静默忽略**，
> 必须写完整的 `[渠道:用户ID]` 形式。这是刻意的：猜错就会把一个人的会话输出发给另一个人。
> 定时任务的伪用户 `cron:<sessionID>` 不计入统计。

**怎么拿到自己的 ID：**

| 渠道 | 方法 | 日志是否完整 |
|------|------|-------------|
| 飞书 | 在飞书里发 `/status`，看回复里的 `标题:` 行，把 `[feishu:ou_xxx]` 原样复制 | ❌ 日志里被截断到 8/12 位，**不要看日志** |
| 钉钉 | 日志 `- UserID: %s` | ✅ 完整 |
| 企微 | 日志 `wecom: mapped user %s to session %s` | ✅ 完整 |
| 微信 | 日志 `wechat: [时间] %s: %s` | ✅ 完整 |

> 飞书为什么不能看日志：`feishu.go` 里 `userLabel := msg.UserID` 之后做了 `userLabel[:8]` 截断，
> `👤 发送者:` 那行也只留 12 位，抄不全。`/status` 打印的是 `session.Title` 原文，是唯一完整的来源。

**自动认领做了三件事：**

1. **注册转发 handler**。这是关键：`eventListenerLoop` 只把事件派发给 `sessionHandlers` 里注册过的会话，TUI 创建的会话没有注册，事件会被丢弃并打印 `no session handler found`。认领后该会话的 SSE 事件就会走 `StreamingSessionHandler`，正文和权限/提问提示被转发到聊天渠道。
2. **更新 adapter 路由表**（`SwitchUserSession`），让该用户在聊天里发的下一条消息直接进入这个会话。
   注意 `/rename` 只带渠道名和用户 ID，不带平台路由信息，所以 `MapSessionData` 会被跳过——
   投递目标由 adapter 自己按用户解析（见下方"前提"）。
3. **标题已经是对的**，所以不再改写——它立刻出现在这个用户的 `/sessions` 里，重启后 `FindLatestSessionForUser` 也能恢复它。
   （唯一的例外是单人部署的省略写法 `[feishu] 原标题`：此时标题会被补全成 `[feishu:ou_xxx] 原标题`，见上文。）

**几个边界情况：**

- **重复 rename 不会重复认领。** 已经认领过的会话再次触发会话更新事件会被跳过。
- **网关自己创建的会话不会被误认领。** 那些会话标题本来就带前缀，但它们有自己的按轮次 handler，重复认领会把 handler 换成长期镜像。网关用 `threadID → sessionID` 表区分。
- **`[skillgen:...]` 会被跳过。** 那是 skill 生成的内部草稿会话，不可路由。
- **网关重启后仍能接管。** `AutoClaimFromTitle` 只在会话更新时触发；重启后第一条事件通常是普通消息事件，此时 `ClaimSessionFromTitle` 会读一次标题并完成同样的认领。
- **一个会话只能被认领一次。** `claimedSessions` 按 **sessionID** 索引，不是按渠道。第一次认领成功后，再 `/rename` 成别的渠道是**静默无操作**（不打日志、不报错、不改标题、不换 handler），输出继续发到原渠道。不同会话可以分别认领到不同渠道，互不影响。
- **想换渠道？用 `/release`。** 见下一节。

### 解除认领：`/release`

在聊天里发 `/release`（别名 `/unclaim`），可以带序号或会话 ID 前缀：

```
/release              # 解除当前会话
/release 2            # 解除 /sessions 里的第 2 个
/release ses_e3cb069a # 按 ID 前缀
```

它做两件事：

1. **摘掉标题里的 `[渠道:用户ID]` 前缀**（`UnclaimSession`）。这一步是必须的——`AutoClaimFromTitle` 就是靠这个前缀识别的，只清内存里的认领记录而不改标题，下一条 `session.updated` 事件会立刻把它重新认领回来。
2. **注销转发 handler 并清掉 adapter 绑定**（`ReleaseClaimedSession` + `ClearSessionForUser`），下一条聊天消息会开新会话，而不是继续一个已经不再镜像的会话。

顺序是**先解除认领、再改标题**，因为改标题本身会触发 `session.updated` 事件；先解除就不会被自己触发的事件重新认领。

解除之后就可以在 TUI 里 `/rename` 成别的渠道了：

```
/release                    # 在微信里发
# 然后在 TUI 里
/rename
[dingtalk] 原标题
```

**网关自己创建的会话会被拒绝。** 它们标题也带前缀，但不在 `claimedSessions` 里；摘掉前缀会让它们从 `ListSessionsForUser` 里消失，用户就再也看不到这个会话了。`IsClaimedSession` 就是用来区分这两种情况的。

> 标题只有前缀、没有正文时（`[wechat:bob]`），解除后会补成 `session <ID前8位>`，因为 opencode 不接受空标题。

### 直接转交：`/transfer`

`/release` 之后还得回 TUI 敲 `/rename`。如果只是想换个渠道，用 `/transfer` 一步到位：

```
/transfer feishu              # 单人部署，自动解析目标用户
/transfer dingtalk:manager123 # 显式指定用户 ID
/transfer wechat 2            # 转交 /sessions 里的第 2 个会话
```

它做三件事（`TransferSession`）：

1. **丢掉旧认领**（`ReleaseClaimedSession`）
2. **把标题改写成 `[新渠道:用户ID] 原标题`**——只换前缀，正文原样保留，所以连续转交不会叠加前缀
3. **同步完成新认领**（`ClaimSession`）——不等 `session.updated` 事件，这样成功失败能立刻回报给你

顺序同样是**先解除、再改标题**，理由和 `/release` 一样。

**两个前提：**

| 前提 | 不满足时 |
|------|---------|
| 目标渠道有对应的 adapter | 直接报错，标题不会被改（`ClaimChannelChecker` 在改写前校验，避免把会话改成没人服务的渠道，那样它会从所有 `/sessions` 里消失） |
| 目标用户给那个 bot 发过至少一条消息 | 转交会成功，但那边收不到输出——平台侧没有投递目标。这跟 `/rename` 的限制完全一样 |

单人部署可以省略用户 ID，走 `ClaimUserResolver` → `SoleUser()`；解析不出唯一用户时会报错让你显式指定，不会猜。

**网关自己创建的会话同样会被拒绝**，理由和 `/release` 一样。

转交成功后当前渠道的会话绑定会被清掉，你下一条消息会开新会话，而不是继续一个已经属于别的渠道的会话。

### `/switch` 会自动暂停其他认领会话

一个聊天里可以同时存在多个认领会话（`/rename` 可以认领多个 TUI 会话到同一个聊天）。如果不管，它们会**同时**往这个聊天里写，答案互相穿插。

所以 `/switch` 到某个会话时：

1. **暂停**这个聊天里其他所有认领会话的转发（`SuspendClaimedSessionsForUser`）
2. **恢复**目标会话的转发，如果它之前被暂停过（`ResumeClaimedSession`）

暂停**不是**解除认领：

| | `/switch` 暂停 | `/release` 解除 |
|---|---|---|
| 转发 handler | 摘掉 | 摘掉 |
| 认领记录 | **保留** | 删除 |
| 标题前缀 | **保留** | 摘掉 |
| 还在 `/sessions` 里 | ✅ | ❌ |
| 怎么恢复 | `/switch` 切回去 | 重新 `/rename` |

保留前缀是关键：`/sessions` 和 `/switch` 都靠前缀过滤，摘掉就再也切不回来了。

暂停状态也挡住了两条会“偷偷恢复”的路径：`restoreClaimedHandler`（每轮对话结束时重挂 handler）会跳过暂停的会话，`SendMessageToSession` 也会丢弃暂停会话的残留 flush。

**认领靠两条事件流，互为备份：**

| 事件流 | 事件类型 | 带标题？ | 作用域 |
|--------|---------|---------|--------|
| `/event` | `session.updated` | ✅ 带完整 `info.title` | **目录作用域**（只推 serve 进程 cwd 下的会话） |
| `/global/event` | `sync`（`syncEvent.type == "session.updated.1"`） | ❌ 只有 `aggregateID` | 进程级，不受目录限制 |

`/event` 是主路径，零额外请求。`/global/event` 是兜底：`sync` 信封里没有标题，所以网关会按 `aggregateID` 回查一次 `GET /session/{id}` 拿标题。

> **为什么需要兜底：** `/event` 是目录作用域的（SDK `EventListParams.Directory`），而网关传的是空参数，所以只有 `OPENCODE_DIRECTORY` 下的会话事件能收到。TUI 如果 attach 到别的目录，`/rename` 在 `/event` 上完全不可见。`/global/event` 不受这个限制。

> **回查有节流。** `session.updated.1` 在会话记录每次变化时都会发（一轮对话里可能几十条），所以同一会话 2 秒内只回查一次（`syncClaimProbeWindow`）。空闲会话——也就是你 `/rename` 时的正常状态——只会发一条，不存在被节流掉的风险。

**部署前提：端口必须钉死。**

`OPENCODE_MANAGE_SERVE=true` 时，如果 `OPENCODE_ENDPOINT` 没显式设置，网关会 `pickFreeTCPPort()` 随机选端口，并且**每次重启都会杀掉旧 serve 换一个新端口**（`defer serveManager.Stop()` → `proc.Kill()`）。TUI 连的是旧端口，重启后就被孤立了，表现就是"`/rename` 没反应"。

```env
OPENCODE_MANAGE_SERVE=true
OPENCODE_ENDPOINT=http://127.0.0.1:38741
OPENCODE_DIRECTORY=/root/openbot
```

端口写死后 `endpointFromEnv != ""`，不再走随机分支。启动日志里**不应该**出现 `auto-selected managed endpoint`；出现就说明 `.env` 没生效。

TUI 侧必须用 `opencode attach <url>`（裸 `opencode` 会自己起一个内嵌 server，不会自动发现网关的）：

```bash
opencode attach http://127.0.0.1:38741 --dir /root/openbot
```

`--dir` 要和 `OPENCODE_DIRECTORY` 一致，否则会话落在别的 project 目录，`/event` 收不到。

> 说明：认领后的 handler 是**长期驻留**的，一轮对话结束只重置状态而不注销，所以你在 TUI 里继续追问，输出仍会同步到聊天。这与网关自己创建的会话不同——那些会话一轮结束就注销 handler。

> 认领是**双向**的。hook 里调的是 `SwitchUserSession(userID, sessionID)`，所以该用户在聊天里发的下一条消息会直接进入这个会话（adapter 用 `GetSessionForUser` 取到 sessionID 后作为 `MessagePayload.SessionID` 传给 opencode）。也就是说：TUI 和聊天可以同时对着同一个会话说话，输出会镜像到聊天。

> **前提：这个用户必须先在对应渠道里给机器人发过至少一条消息。** 认领只带渠道名和用户 ID，不带平台路由信息（飞书的 `receive_id_type`、钉钉的 `openConversationId`），所以网关要靠 adapter 自己记住的"这个用户上次从哪来"来投递。飞书用 `userTargets`（入站时 `rememberChatTarget` 写入），钉钉用 Robot API 的 1:1 消息（只需要 `userIds`）。从没发过消息的用户，认领能成功但第一条转发会失败。这条前提同时也是单人部署省略用户 ID 的前提——`SoleUser()` 读的就是这张入站时建立的用户表。

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
把每日检查改到每天下午3点
```

写操作（创建/启用/禁用/删除/试运行/修改）默认采用“草稿确认”机制：

```
确认 / yes / 1   # 执行
取消 / no / 0    # 放弃
```

修改任务时只需说明要改的部分，未提及的字段保持原值：

```
把 cron-1746000000 改到每天早上8点
修改任务 cron-1746000000 改到 0 0 8 * * *
```

修改必须能解析出新的执行时间，否则会提示补充时间；如果任务 ID 或名称匹配不到唯一任务，会提示改用任务 ID 指定。

也支持 `/crontask <自然语言>` 兜底写法：

```
/crontask 每天早上9点提醒我发日报
```

---

## 🧠 记忆命令

以下命令用于显式管理“记住的事实”。它们**不需要**开启 `MEMORY_KNOWLEDGE_ENABLED`：那是后台自动抽取的开关，而这几条是用户主动操作。

### `/remember <内容>`

把一条事实写入归档，作用域为全局，状态为 `verified`，来源标记为 `user`。

```
/remember 以后都用中文回答我
```

成功时会打印**真实的写入回执**，包含事实 ID 和作为依据的消息 ID：

```
✅ 已记住（ID 3f2a9c1b8d4e，全局生效，依据消息 msg_01H...）
以后都用中文回答我
用 /forget 3f2a9c1b8d4e 可以删除。
```

如果归档里还没有可引用的消息，命令会**拒绝**而不是假装成功：

```
❌ 无法记住：归档中还没有可引用的消息
```

这一点是刻意的：助手说“我记住了”不等于写入成功。没有回执的“记住了”和真的写进去在用户看来完全一样，区别要到几个月后事实不存在时才会暴露。

### `/forget <ID 或内容>`

删除一条事实。参数可以是事实 ID，也可以是事实内容（精确匹配，忽略大小写）。

```
/forget 3f2a9c1b8d4e
/forget 以后都用中文回答我
```

如果被删除的事实曾经**取代**过另一条事实，那条旧事实会重新变为有效，而不是留下一个空洞。

### `/knowledge-list`

列出当前用户拥有的全部事实，包括作用域为单个会话的事实。

```
📚 已记住 3 条事实：

`3f2a9c1b8d4e` [preference/global] 以后都用中文回答我
`7b1e4d2a9f03` [constraint/project] 这个项目不要碰生产库
`c9d5a0e7b218` [procedure/session] 这个任务里注释用英文写
```

### `/knowledge-help`

显示以上命令的说明，并提示抽取出的事实可能已过时、与当前指令冲突时以当前指令为准。

详见 [MEMORY_KNOWLEDGE.md](MEMORY_KNOWLEDGE.md)。

---

## 🗄️ 记忆归档运维工具

以下两个是**命令行工具**，不是聊天命令。它们不在网关进程里，也不通过 MCP 暴露，必须由运维人员在服务器上显式执行。`make build` 会一并构建到 `bin/`。

### `openbot-memory-migrate`

把已有的网关数据库迁移到 v2 归档 schema，并导入历史记录。迁移是增量的：旧表、handoff、重试队列和技能表都不会被删除或改写。

```bash
# 首次迁移：先做一致性备份，再事务性应用 schema
openbot-memory-migrate --db mem/memory.db --backup mem/memory.before-v2.db

# 中断后继续导入，或导入新增的历史记录（不会重复导入）
openbot-memory-migrate --db mem/memory.db --resume --batch-size 64
```

| 参数 | 说明 |
| --- | --- |
| `--db` | 已存在的网关数据库路径。必填。 |
| `--backup` | 新的备份目标路径。首次迁移必填，且目标不能已存在。 |
| `--resume` | 在已初始化的归档上继续导入，不再做备份、不再跑迁移。 |
| `--batch-size` | 每个事务导入的历史记录条数，1–1000，默认 64。 |
| `--queue-bytes` | 允许的在途字节上限，默认 16 MiB；超大的历史行会显式失败而不是被静默截断。 |

`--backup` 与 `--resume` 互斥。备份用 `VACUUM INTO` 生成（包含已提交的 WAL 数据），随后校验完整性；备份失败会阻止迁移，残留的半成品备份会保留供检查。迁移期间请预留备份、新归档、WAL 和 SQLite 临时文件所需的空间。

**最终补齐前请先停掉网关写入。** 运行中的网关可能在某一批报告零待处理之后继续追加记录；备份是快照，不包含之后的写入。这个命令也不会把实时捕获切换到 v2——那需要 `MEMORY_CAPTURE_MODE=strict`。

### `openbot-memory-delete`

按 principal 删除一个用户的归档会话数据。删除在写入器队列上的单个事务里完成，因此不会与捕获交错，且每条语句都带 principal 谓词——不存在只凭一个 ID 就能删的代码路径。

```bash
openbot-memory-delete --db mem/memory.db \
                      --adapter dingtalk --user 0123456789 --confirm
```

| 参数 | 说明 |
| --- | --- |
| `--db` | 归档路径。必填。 |
| `--adapter` | 数据所属的适配器，如 `dingtalk`。必填。 |
| `--user` | 被删除归档的用户 ID。必填。 |
| `--session` | 只删这一个归档会话 ID。 |
| `--project` | 只删归档在该 project key 下的会话。 |
| `--since` / `--until` | 只删创建时间在区间内的消息（Unix 毫秒）。 |
| `--confirm` | 必填。确认此操作不可撤销。 |

`--confirm` 不是形式主义。其余每个参数都是**收窄**选择的谓词，而一个意外为空的谓词会选中全部：只给 `--adapter` 和 `--user` 而不加任何会话、项目或时间限制，删的就是该 principal 的全部历史。要求显式确认是唯一无法靠"忘记传过滤条件"满足的护栏。

命令把 `DeleteReport` 以 JSON 打印出来，便于运维记录删掉了什么。

**这不是安全擦除。** SQLite 的 `DELETE` 只是把行从 b-tree 里移除，字节仍可能残留在空闲页、WAL 以及删除之前做的任何备份里。物理擦除取决于存储与备份策略，而不是这个程序。详见 [MEMORY_DELETION.md](MEMORY_DELETION.md)。

### 为什么删除不在 MCP 里

MCP 服务器以只读方式打开归档，这是刻意的：它是模型对话的进程，模型不能因为某个提示词要求就删掉用户的历史。把删除放进去会让"只读"这个保证变成"取决于某个 handler 是否守规矩"，那不是保证。同理，删除也不做成网关的环境变量开关——那会让"用错配置重启一次"就能触发删除。

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
A: 使用`/sessions`命令查看你自己的会话列表，用`/switch <序号>`切回其中任意一个继续对话；更详细的历史可以在OpenCode Web界面中查看。

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
- [MCP 记忆工具](MCP_MEMORY_TOOL.md)
- [归档迁移](MEMORY_MIGRATION.md)
- [按 principal 删除](MEMORY_DELETION.md)

---

## 📝 更新日志

### 未发布
- ✨ `/sessions` 改为只列出**当前用户自己的**会话（按 `[渠道:用户ID]` 标题前缀过滤），并标注当前会话
- ✨ `/sessions` 每行改为显示**会话摘要**（取自该会话第一条真实用户消息，48 字截断），不再显示无意义的会话标题；摘要按会话 ID 缓存
- ✨ 新增 `/switch <序号|会话ID前缀>`，可切回任意历史会话继续对话
- ✨ 上述两个命令在微信、飞书、钉钉、企业微信、WebUI 五个渠道全部可用

### v1.1.0 (2026-02-14)
- ✨ 新增会话管理命令（/status, /new, /clear, /sessions）
- ✨ 新增模型配置命令（/model, /provider）
- ✨ 新增配置查看命令（/config）
- 🐛 修复streaming完成后内容未发送的bug
- 📚 更新帮助文档
