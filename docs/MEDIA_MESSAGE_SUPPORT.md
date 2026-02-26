# 钉钉多媒体消息支持文档

## 功能说明

钉钉网关现已支持接收和处理以下类型的消息：

### 支持的消息类型

1. **📝 文本消息 (text)** - 原有支持
2. **🖼️ 图片消息 (picture)** - ✅ 新增支持
3. **🎤 语音消息 (audio/voice)** - ✅ 新增支持  
4. **🎬 视频消息 (video)** - ✅ 新增支持

## 实现细节

### 1. 消息数据结构

新增了以下结构体定义：

```go
// 图片消息内容
type pictureContent struct {
    DownloadCode        string `json:"downloadCode"`
    PictureDownloadCode string `json:"pictureDownloadCode"`
}

// 语音消息内容
type audioContent struct {
    DownloadCode string `json:"downloadCode"`
    Duration     int64  `json:"duration"` // 单位：毫秒
}

// 视频消息内容
type videoContent struct {
    DownloadCode        string `json:"downloadCode"`
    VideoType           string `json:"videoType"`
    Duration            int64  `json:"duration"` // 单位：毫秒
    PictureDownloadCode string `json:"pictureDownloadCode"` // 视频封面图
}
```

### 2. 消息处理流程

当收到多媒体消息时，系统会：

1. **识别消息类型**：根据 `data.Msgtype` 字段判断
2. **解析媒体内容**：从 `data.Content` 中提取下载码等信息
3. **获取媒体URL**：调用 `getMediaURL()` 方法获取实际下载链接
4. **传递给OpenCode**：将媒体信息通过 `metadata` 传递

### 3. 调试日志

系统会输出详细的调试日志，包括：

```
dingtalk stream: 🔍 [DEBUG] Message received:
  - MsgID: xxx
  - UserID: xxx
  - ConversationID: xxx
  - MsgType: picture/audio/video
  - Text.Content: xxx
  - Content (interface{}): {...}
  - SenderNick: xxx

dingtalk stream: 🖼️ [PICTURE] received from xxx
  - Picture content JSON: {...}
  - DownloadCode: xxx
  - PictureDownloadCode: xxx
  - 🔍 [DEBUG] Getting media URL for downloadCode: xxx
  - ✅ Image URL: https://...

dingtalk stream: 📎 added media info to metadata: {"type":"image","url":"...","downloadCode":"..."}
```

### 4. 媒体信息传递

媒体信息会作为 JSON 字符串添加到消息的 `metadata["media"]` 字段中：

```json
{
  "type": "image",           // 类型：image/audio/video
  "url": "https://...",      // 下载URL
  "downloadCode": "...",     // 钉钉下载码
  "duration": 15000          // 时长（仅音视频，单位：毫秒）
}
```

## 测试指南

### 前置条件

确保设置了以下环境变量：

```powershell
$env:DINGTALK_CLIENT_ID = "your_client_id"
$env:DINGTALK_CLIENT_SECRET = "your_client_secret"
$env:OPENCODE_ENDPOINT = "your_opencode_endpoint"
```

### 测试步骤

1. **启动服务**
   ```powershell
   cd E:\Work\projects\gos\src\opencode-gateway\cmd\gateway
   go run .
   ```

2. **发送测试消息**
   - 在钉钉中发送图片消息
   - 在钉钉中发送语音消息
   - 在钉钉中发送视频消息

3. **查看日志输出**
   日志中会显示：
   - 消息类型识别
   - 媒体内容解析
   - 下载URL获取
   - 元数据传递

### 预期日志示例

#### 图片消息
```
dingtalk stream: 🔍 [DEBUG] Message received:
  - MsgType: picture
dingtalk stream: 🖼️ [PICTURE] received from user123
  - Picture content JSON: {"downloadCode":"xxx","pictureDownloadCode":"xxx"}
  - DownloadCode: xxx
  - ✅ Image URL: https://oapi.dingtalk.com/...
dingtalk stream: 📎 added media info to metadata: {"type":"image","url":"..."}
```

#### 语音消息
```
dingtalk stream: 🔍 [DEBUG] Message received:
  - MsgType: audio
dingtalk stream: 🎤 [AUDIO] received from user123
  - Audio content JSON: {"downloadCode":"xxx","duration":15000}
  - DownloadCode: xxx
  - Duration: 15000 ms
  - ✅ Audio URL: https://oapi.dingtalk.com/...
dingtalk stream: 📎 added media info to metadata: {"type":"audio","url":"...","duration":15000}
```

## API 实现

### getMediaURL 方法

```go
func (h *Handler) getMediaURL(ctx context.Context, downloadCode string) (string, error)
```

**功能**：根据钉钉的 downloadCode 获取媒体文件的实际下载URL

**实现细节**：
1. 获取 access token
2. 调用钉钉文件下载接口：
   ```
   GET https://oapi.dingtalk.com/robot/messageFiles/download
       ?access_token={token}&downloadCode={code}
   ```
3. 处理重定向或 JSON 响应
4. 返回可访问的下载URL

## 错误处理

### 不支持的消息类型

如果收到不支持的消息类型（如 file、location 等），会回复：

```
暂不支持 {msgtype} 类型的消息，请发送文本、图片或语音消息。
```

### 解析失败

如果媒体内容解析失败，会输出警告日志：

```
dingtalk stream: ⚠️ Failed to parse picture content: xxx
```

但仍会继续处理消息，只是不包含媒体URL信息。

## 注意事项

1. **下载码有效期**：钉钉的 downloadCode 有时效限制，建议及时处理
2. **文件大小限制**：钉钉对媒体文件有大小限制
3. **访问权限**：需要机器人有获取媒体文件的权限
4. **网络访问**：确保服务可以访问钉钉API（https://oapi.dingtalk.com）

## 后续优化建议

1. **媒体文件缓存**：可以考虑下载并缓存媒体文件，避免下载码过期
2. **文件类型识别**：根据文件扩展名或 MIME 类型进一步识别
3. **文件大小检查**：在下载前检查文件大小，避免下载过大文件
4. **更多消息类型**：支持 file、location、richText 等其他消息类型
5. **媒体转换**：对于某些格式，可以考虑转换为更通用的格式

## 相关文档

- [钉钉机器人消息类型文档](https://open.dingtalk.com/document/robots/robot-message-types-1)
- [钉钉媒体文件下载接口](https://open.dingtalk.com/document/orgapp/download-media-files)
- [OpenCode Gateway 配置文档](./CONFIGURATION.md)
