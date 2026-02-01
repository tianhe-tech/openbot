# OpenCode 事件流式返回问题分析与解决

## 问题描述

### 现象
OpenCode 执行长时间运行的脚本（如斐波那契数列计算）时：
- 日志显示大量 `message.part.updated` 事件（数百次）
- 日志显示最终成功提取回复内容（"extracted reply: 2098 chars"）
- 日志显示成功发送到 DingTalk（"sending intermediate update"）
- 但用户在 DingTalk 中只能看到进度提示（⏳），看不到实际的执行过程和结果

### 根本原因

通过分析 OpenCode SDK v0.19.2 源码（`/Users/tiandoudou/go/pkg/mod/github.com/anomalyco/opencode-sdk-go@v0.19.2/event.go`），发现：

**`message.part.updated` 事件确实包含增量内容！**

#### SDK 事件结构

```go
// OpenCode SDK 定义
type EventListResponseEventMessagePartUpdatedProperties struct {
    Part  Part   `json:"part,required"`    // Part对象
    Delta string `json:"delta"`             // ⚠️ 增量文本 - 这是关键！
    JSON  eventListResponseEventMessagePartUpdatedPropertiesJSON `json:"-"`
}

type Part struct {
    ID        string   `json:"id,required"`
    Type      PartType `json:"type,required"`  // "text", "tool", etc.
    Text      string   `json:"text"`           // Part的完整文本
    // ... 其他字段
}
```

#### 之前的错误实现

`internal/opencode/event_listener.go` 中的 `extractContentFromEvent` 函数：

```go
func (s *StreamingSessionHandler) extractContentFromEvent(event *opencode.EventListResponse) string {
    // ❌ 错误的假设：认为事件不包含内容
    // ❌ 直接返回空字符串，完全没有尝试解析
    return ""
}
```

**这导致**：
- 所有 `message.part.updated` 事件都被浪费
- 无法实现真正的流式返回
- 只能等到最终 `SendMessage` 完成后才能获取全部内容
- 用户体验差：长时间看不到任何进展

## 解决方案

### 修改 `extractContentFromEvent` 函数

```go
func (s *StreamingSessionHandler) extractContentFromEvent(event *opencode.EventListResponse) string {
    if event == nil {
        return ""
    }

    // ✅ 解析 message.part.updated 事件
    if event.Type == "message.part.updated" {
        type PartUpdateProps struct {
            Delta string `json:"delta"`  // 增量文本
            Part  struct {
                Text string `json:"text"` // 完整文本
                Type string `json:"type"` // part类型
            } `json:"part"`
        }
        
        var props PartUpdateProps
        if jsonData := event.JSON.RawJSON(); jsonData != "" {
            var wrapper struct {
                Properties PartUpdateProps `json:"properties"`
            }
            if err := json.Unmarshal([]byte(jsonData), &wrapper); err == nil {
                props = wrapper.Properties
                
                // ✅ 优先使用 Delta 字段（增量内容）
                if props.Delta != "" {
                    log.Printf("opencode: extracted delta content (%d chars)", len(props.Delta))
                    return props.Delta
                }
                
                // ✅ 降级：使用 Part.Text（适用于 type="text" 的 Part）
                if props.Part.Text != "" && props.Part.Type == "text" {
                    log.Printf("opencode: extracted text content (%d chars)", len(props.Part.Text))
                    return props.Part.Text
                }
            }
        }
    }

    return ""
}
```

### 关键改进

1. **添加 JSON 解析**：使用 `event.JSON.RawJSON()` 获取原始 JSON，然后反序列化
2. **提取 Delta 字段**：这是 OpenCode 提供的增量文本，每个事件只包含新增的内容
3. **降级方案**：如果没有 Delta，尝试使用 Part.Text
4. **添加日志**：便于调试和验证

## 工作流程对比

### 修改前（❌ 错误流程）

```
1. OpenCode 生成回复 → 发送 message.part.updated 事件（Delta="一段文本"）
2. event_listener.extractContentFromEvent() → 返回 ""（忽略！）
3. 继续等待...
4. OpenCode 生成更多 → 发送 message.part.updated 事件（Delta="更多文本"）
5. event_listener.extractContentFromEvent() → 返回 ""（继续忽略！）
6. ... 重复数百次 ...
7. OpenCode 完成 → SendMessage 返回完整响应
8. client.extractReplyFromMessage() → 提取全部内容
9. 通过 callback 发送给 DingTalk
```

**问题**：用户需要等到第 7 步才能看到任何内容！

### 修改后（✅ 正确流程）

```
1. OpenCode 生成回复 → 发送 message.part.updated 事件（Delta="一段文本"）
2. event_listener.extractContentFromEvent() → 返回 "一段文本" ✅
3. 检查累积内容 >= 500 chars → 通过 callback 发送给 DingTalk ✅
4. 用户立即看到第一批内容！✅
5. OpenCode 生成更多 → 发送 message.part.updated 事件（Delta="更多文本"）
6. event_listener.extractContentFromEvent() → 返回 "更多文本" ✅
7. 检查新增 >= 500 chars → 再次发送更新 ✅
8. 用户看到持续更新！✅
9. ... 继续实时流式更新 ...
10. OpenCode 完成 → session.idle 事件 → 标记完成
```

**优势**：
- ✅ 真正的流式返回
- ✅ 实时反馈（每 500 chars）
- ✅ 用户体验极大改善

## 技术细节

### Delta vs Part.Text

- **Delta**: 增量文本，每个事件只包含**新增**的内容
- **Part.Text**: Part 对象的完整文本（对于 text 类型的 Part）

### 为什么需要累积？

```go
// StreamingSessionHandler 中
lastContent string  // 累积所有 Delta 的内容

func (s *StreamingSessionHandler) HandleEvent(ctx context.Context, event *opencode.EventListResponse) error {
    newContent := s.extractContentFromEvent(event)  // 获取 Delta
    if newContent != "" {
        s.lastContent += newContent  // 累积！
        
        // 每 500 chars 发送一次更新
        if len(s.lastContent)-s.lastSentLength >= 500 {
            s.callback.OnProgress(s.lastContent)
            s.lastSentLength = len(s.lastContent)
        }
    }
    return nil
}
```

### Part 类型

OpenCode 的 Part 可以是多种类型：
- `"text"`: 文本内容（AI 生成的回复）
- `"tool"`: 工具调用（如执行 Python 脚本）
- `"reasoning"`: 推理过程
- `"retry"`: 重试
- 等等...

我们主要关注 `type="text"` 的 Part。

## 测试验证

### 预期效果

1. 运行长时间脚本（如斐波那契数列）
2. 日志应该显示：
   ```
   opencode: extracted delta content (50 chars) from message.part.updated event
   opencode: extracted delta content (120 chars) from message.part.updated event
   ...
   dingtalk stream: sending intermediate update (update 1/5, new content: 500 chars)
   ...
   opencode: extracted delta content (80 chars) from message.part.updated event
   dingtalk stream: sending intermediate update (update 2/5, new content: 500 chars)
   ```
3. DingTalk 用户应该看到：
   - ⏳ 正在处理中...
   - [第一批内容 500 chars]
   - [更新：累积到 1000 chars]
   - [更新：累积到 1500 chars]
   - ...
   - ✅ 完成

### 测试命令

```bash
# 编译
cd /Users/tiandoudou/work/openbot
go build -o bin/gateway cmd/gateway/main.go

# 运行
./bin/gateway

# 在 DingTalk 中发送
帮我写一个计算前100个斐波那契数的python脚本并执行
```

## 相关文件

- `internal/opencode/event_listener.go` - 事件监听器（主要修改）
- `internal/opencode/client.go` - OpenCode 客户端
- `internal/adapters/dingtalk/dingtalk.go` - DingTalk 适配器

## 学习总结

1. **不要假设 SDK 的行为**：应该查看 SDK 源码确认
2. **interface{} 字段需要反序列化**：OpenCode SDK 使用 `Properties interface{}` 存储动态类型
3. **SSE 事件流的正确用法**：Delta 字段就是为流式返回设计的
4. **累积式更新**：适合长内容的渐进式展示

## 参考

- OpenCode SDK: `github.com/sst/opencode-sdk-go` v0.19.2
- SDK 源码位置: `/Users/tiandoudou/go/pkg/mod/github.com/anomalyco/opencode-sdk-go@v0.19.2/event.go`
- 相关类型定义:
  - `EventListResponseEventMessagePartUpdated`
  - `EventListResponseEventMessagePartUpdatedProperties`
  - `Part`

---

**修改时间**: 2026-02-01  
**问题发现**: 用户观察到日志显示成功但 DingTalk 无实时更新  
**根因分析**: extractContentFromEvent 未解析 Delta 字段  
**解决方案**: 添加 JSON 解析逻辑提取增量内容  
**预期效果**: 真正的流式实时返回
