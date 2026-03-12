---
name: dingtalk-file-sender
description: |
  用于通过钉钉API发送文件给指定用户。当用户需要将生成的文档、报告、图片等文件发送到钉钉时，使用此skill。
  适用场景：
  - 用户要求"把XX文件发送到钉钉"
  - 用户需要"通过钉钉发送文件"
  - 生成报告后需要分享给钉钉用户
  - 任何需要将本地文件推送到钉钉的场景
  
  此skill使用钉钉企业内部应用API（需要AgentID）或Webhook方式发送文件。
  
  触发关键词：钉钉发送、发送到钉钉、dingtalk send、推送到钉钉、分享至钉钉
---

# DingTalk File Sender Skill

用于通过钉钉API向指定用户发送文件的skill。

## 使用前提

1. **钉钉应用配置**：需要在 `/root/.env` 文件中配置以下环境变量：
   - `DINGTALK_CLIENT_ID` - 钉钉应用的Client ID
   - `DINGTALK_CLIENT_SECRET` - 钉钉应用的Client Secret
   - `DINGTALK_AGENT_ID` - 钉钉应用的Agent ID（用于企业内部应用方式）
   - `DINGTALK_OWNER_USERID` - 默认接收用户的UserID

## 发送文件流程

### 方式一：企业内部应用（推荐）

适用于有钉钉企业内部应用权限的场景，支持发送文件、图片、语音等多种消息类型。

```javascript
// 调用脚本发送文件
node /root/.config/opencode/skills/dingtalk-file-sender/scripts/send_file.js <文件路径> [用户ID]
```

**参数说明**：
- `<文件路径>`：要发送的文件完整路径（必需）
- `[用户ID]`：接收者的钉钉UserID（可选，默认为env中的DINGTALK_OWNER_USERID）

**示例**：
```bash
node /root/.config/opencode/skills/dingtalk-file-sender/scripts/send_file.js /tmp/report.docx
node /root/.config/opencode/skills/dingtalk-file-sender/scripts/send_file.js /tmp/data.xlsx 1601434517956472
```

### 方式二：Webhook机器人（文本/链接）

钉钉Webhook机器人不支持直接发送文件，只能发送文本、Markdown或链接。
如需发送文件内容，可以：
1. 将文件上传到云存储获取链接
2. 发送Markdown格式的链接消息

## 支持的文件类型

- 文档：.docx, .doc, .pdf, .txt, .md
- 表格：.xlsx, .xls, .csv
- 图片：.jpg, .jpeg, .png, .gif, .bmp
- 其他：任意文件类型（通过file消息类型发送）

## 实现细节

### 发送流程

1. **获取AccessToken**
   ```
   GET https://oapi.dingtalk.com/gettoken?appkey={CLIENT_ID}&appsecret={CLIENT_SECRET}
   ```

2. **上传媒体文件**
   ```
   POST https://oapi.dingtalk.com/media/upload?access_token={ACCESS_TOKEN}&type=file
   ```

3. **发送消息**
   ```
   POST https://oapi.dingtalk.com/message/send?access_token={ACCESS_TOKEN}
   ```

### 消息类型

- `file` - 文件消息（本skill默认使用）
- `image` - 图片消息
- `text` - 文本消息
- `markdown` - Markdown消息

## 错误处理

常见错误码：
- `40012` - 缺少sender参数（使用了错误的API端点）
- `41011` - 缺少agentid参数（需要在.env中配置DINGTALK_AGENT_ID）
- `40014` - AccessToken失效或无效
- `40035` - 媒体文件不存在或上传失败

## 环境变量示例

```bash
# /root/.env
export DINGTALK_CLIENT_ID="dingxxxxxxxxx"
export DINGTALK_CLIENT_SECRET="xxxxxxxxxxxx"
export DINGTALK_AGENT_ID="123456789"
export DINGTALK_OWNER_USERID="1601434517956472"
```

## 脚本位置

主要脚本：`/root/.config/opencode/skills/dingtalk-file-sender/scripts/send_file.js`

使用方法详见脚本内注释。