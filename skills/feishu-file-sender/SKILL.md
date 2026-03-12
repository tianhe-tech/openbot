# Skill: feishu-file-sender

# 飞书文件发送 Skill

用于通过飞书 API 向指定用户发送文件的 skill。

## 使用前提

1. **飞书应用配置**：需要在 `/root/.env` 文件中配置以下环境变量：
   - `FEISHU_APP_ID` - 飞书应用的 App ID
   - `FEISHU_APP_SECRET` - 飞书应用的 App Secret
   - `FEISHU_DEFAULT_OPEN_ID` - 默认接收用户的 Open ID

2. **权限要求**：飞书应用需要以下权限：
   - `im:chat:readonly` - 读取群组信息
   - `im:message:send` - 发送消息
   - `im:message.group_msg` - 发送群消息

## 发送文件流程

```bash
# 调用脚本发送文件
python3 /root/.config/opencode/skills/feishu-file-sender/scripts/send_file.py <文件路径> [用户ID]
```

**参数说明**：
- `<文件路径>`：要发送的文件完整路径（必需）
- `[用户ID]`：接收者的飞书 Open ID（可选，默认为 env 中的 FEISHU_DEFAULT_OPEN_ID）

**示例**：
```bash
python3 /root/.config/opencode/skills/feishu-file-sender/scripts/send_file.py /tmp/report.docx
python3 /root/.config/opencode/skills/feishu-file-sender/scripts/send_file.py /tmp/data.xlsx ou_1234567890abcdef
```

## 支持的文件类型

- 文档：.docx, .doc, .pdf, .txt, .md
- 表格：.xlsx, .xls, .csv
- 图片：.jpg, .jpeg, .png, .gif, .bmp
- 其他：任意文件类型（通过文件消息发送）

## 实现细节

### 发送流程

1. **获取 tenant_access_token**
   ```
   POST https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal
   Body: { "app_id": "xxx", "app_secret": "xxx" }
   ```

2. **上传文件**
   ```
   POST https://open.feishu.cn/open-apis/im/v1/files
   Headers: Authorization: Bearer {ACCESS_TOKEN}
   Body: multipart/form-data { file, file_type, file_name }
   ```

3. **发送消息**
   ```
   POST https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=open_id
   Headers: Authorization: Bearer {ACCESS_TOKEN}
   Body: { receive_id, msg_type: "file", content: { file_key, file_name } }
   ```

### 消息类型

- `file` - 文件消息（本 skill 默认使用）
- `image` - 图片消息
- `text` - 文本消息
- `post` - 富文本消息

## 错误处理

常见错误码：
- `99991663` - tenant_access_token 过期或无效
- `99991661` - app_id 或 app_secret 错误
- `112` - 用户不在应用可见范围内
- `10001` - 参数错误

## 环境变量示例

```bash
# /root/.env
export FEISHU_APP_ID="cli_xxxxxxxxxx"
export FEISHU_APP_SECRET="xxxxxxxxxxxxxxxxxxxx"
export FEISHU_DEFAULT_OPEN_ID="ou_xxxxxxxxxxxxxx"
```

## 脚本位置

主要脚本：`/root/.config/opencode/skills/feishu-file-sender/scripts/send_file.py`

使用方法详见脚本内注释。
