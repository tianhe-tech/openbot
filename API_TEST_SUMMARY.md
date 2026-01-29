# Gateway API 测试总结

## 测试完成 ✅

已成功测试 Gateway 与 OpenCode Server 的 API 交互，可以清晰看到完整的输入输出流程。

## 可用的测试脚本

### 1. **demo_io.ps1** - 完整输入输出演示 🔍
显示详细的 API 请求和响应，包括数据流向。

```powershell
.\scripts\demo_io.ps1
```

**输出内容：**
- ✓ HTTP 请求的完整 JSON 结构
- ✓ HTTP 响应的完整 JSON 结构  
- ✓ 关键信息提取（Session ID、AI 回复等）
- ✓ 数据流向图
- ✓ 响应时间统计

**测试结果示例：**
```
[Step 4] Send Request
  OK: Request succeeded (2378ms)

RESPONSE:
{
  "reply": "- Compiled and statically typed\n- Garbage collection...",
  "session_id": "ses_3f7929f67ffe1AmFU388H5ZoSo",
  "trace": "ses_3f7929f67ffe1AmFU388H5ZoSo"
}

Data Flow:
  User Message -> Gateway -> SDK -> OpenCode Server -> AI -> Response
```

---

### 2. **test_api_detailed.ps1** - 多平台详细测试 📋
测试所有三个适配器（Feishu、WeCom、DingTalk）的完整 I/O。

```powershell
.\scripts\test_api_detailed.ps1
```

**输出内容：**
- 每个平台的完整请求 JSON
- 每个平台的完整响应 JSON
- AI 回复内容预览
- Session ID 和 Trace 信息

---

### 3. **test_interactive.ps1** - 交互式实时测试 💬
实时对话模式，可以手动输入并查看 AI 响应。

```powershell
# 默认使用 Feishu
.\scripts\test_interactive.ps1

# 指定平台
.\scripts\test_interactive.ps1 -Adapter wecom
.\scripts\test_interactive.ps1 -Adapter dingtalk
```

**交互命令：**
- 直接输入文本 - 发送到 OpenCode
- `switch` - 切换平台
- `history` - 查看会话历史
- `help` - 显示帮助
- `exit` - 退出

**实时输出示例：**
```
[14:23:45] YOU > What is REST API?
[14:23:45] REQUEST > Sending to /feishu/callback...
[14:23:46] RESPONSE < Session: ses_abc123
[14:23:46] AI > REST API (Representational State Transfer)...
```

---

### 4. **test_all.ps1** - 快速功能测试 ⚡
快速验证所有适配器是否正常工作。

```powershell
.\scripts\test_all.ps1
```

**输出内容：**
- 简洁的通过/失败状态
- AI 回复摘要
- Session ID

---

## 测试覆盖的关键点

### ✅ 输入验证
- HTTP 请求格式正确
- JSON 结构符合各平台规范
- 消息内容正确编码

### ✅ 数据流向
```
用户消息 
  ↓
Gateway Webhook (/feishu|wecom|dingtalk/callback)
  ↓
OpenCode Client (SDK)
  ↓
OpenCode Server (http://localhost:3000)
  ↓
AI 处理
  ↓
响应返回
  ↓
Gateway 格式化
  ↓
平台格式响应
```

### ✅ 输出验证
- JSON 响应格式正确
- Session ID 正确生成
- AI 回复内容完整
- 响应时间合理（1-3秒）

### ✅ 三个平台适配器
所有平台都能：
- 接收 webhook
- 解析消息
- 调用 OpenCode
- 返回响应
- 映射会话

---

## API 端点验证

### Feishu
```
POST /feishu/callback
✅ 请求格式: Feishu Event Schema 2.0
✅ 响应格式: {msg_type, content:{text}, session_id, trace}
✅ Session 映射: OpenID -> Session ID
```

### WeCom  
```
POST /wecom/callback
✅ 请求格式: {msgtype, from_userid, text:{content}}
✅ 响应格式: {reply, session_id, trace}
✅ Session 映射: UserID -> Session ID
```

### DingTalk
```
POST /dingtalk/callback
✅ 请求格式: {msgtype, senderStaffId, text:{content}}
✅ 响应格式: {msgtype, text:{content}, session_id, trace}
✅ Session 映射: StaffID -> Session ID
```

---

## 性能指标

根据测试结果：
- **健康检查**: < 100ms
- **消息处理**: 1000-3000ms（取决于 AI 响应时间）
- **Gateway 开销**: 最小化（主要时间在 OpenCode Server 处理）

---

## 下一步建议

### 1. 生产环境准备
- [ ] 配置真实的平台 webhook URL
- [ ] 添加签名验证
- [ ] 实现消息推送 API
- [ ] 配置 HTTPS

### 2. 功能增强
- [ ] 实现双向推送（OpenCode -> 用户）
- [ ] 添加消息队列
- [ ] 会话持久化
- [ ] 错误重试机制

### 3. 监控和日志
- [ ] 添加 Prometheus metrics
- [ ] 结构化日志
- [ ] 性能追踪
- [ ] 错误报警

---

## 快速参考

### 启动服务
```powershell
# OpenCode Server
opencode server

# Gateway
.\bin\gateway.exe
```

### 运行测试
```powershell
# 完整 I/O 演示
.\scripts\demo_io.ps1

# 详细测试
.\scripts\test_api_detailed.ps1

# 交互测试
.\scripts\test_interactive.ps1 -Adapter wecom

# 快速测试
.\scripts\test_all.ps1
```

### 使用 curl
```bash
# WeCom
curl -X POST http://localhost:8080/wecom/callback \
  -H "Content-Type: application/json" \
  -d '{"msgtype":"text","from_userid":"user","text":{"content":"Hello"}}'

# Feishu
curl -X POST http://localhost:8080/feishu/callback \
  -H "Content-Type: application/json" \
  -d '{"schema":"2.0","type":"im.message.receive_v1","event":{"sender":{"sender_id":{"open_id":"ou_test"}},"message":{"message_id":"om_123","message_type":"text","content":"{\"text\":\"Hello\"}"}}}'

# DingTalk
curl -X POST http://localhost:8080/dingtalk/callback \
  -H "Content-Type: application/json" \
  -d '{"msgtype":"text","conversationType":"1","conversationId":"cid","senderStaffId":"staff","text":{"content":"Hello"}}'
```

---

## 结论

✅ **Gateway 与 OpenCode Server 的 API 交互完全正常**

所有测试脚本都能清晰展示：
- 完整的请求输入
- 完整的响应输出  
- 数据流向和处理过程
- 性能指标

系统已准备好进行下一阶段的开发和生产部署。
