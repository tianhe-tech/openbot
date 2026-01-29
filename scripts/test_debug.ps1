# Simple test to check response format
$baseUrl = "http://localhost:8080"

Write-Host "Testing Feishu..." -ForegroundColor Cyan
$feishuPayload = @{
    schema = "2.0"
    token = "test_token"
    type = "im.message.receive_v1"
    event = @{
        sender = @{
            sender_id = @{
                open_id = "ou_test123"
            }
        }
        message = @{
            message_id = "om_test_msg_123"
            message_type = "text"
            chat_id = "oc_test_chat_123"
            content = '{"text": "Hello OpenCode"}'
        }
    }
} | ConvertTo-Json -Depth 10

$resp = Invoke-WebRequest -Uri "$baseUrl/feishu/callback" -Method Post -Body $feishuPayload -ContentType "application/json"
Write-Host "Status: $($resp.StatusCode)"
Write-Host "Body: $($resp.Content)"
Write-Host ""

Write-Host "Testing DingTalk..." -ForegroundColor Cyan
$dingtalkPayload = @{
    msgtype = "text"
    conversationType = "1"
    conversationId = "cid_test_123"
    senderStaffId = "staff_123"
    text = @{
        content = "Hello from DingTalk"
    }
} | ConvertTo-Json -Depth 5

$resp = Invoke-WebRequest -Uri "$baseUrl/dingtalk/callback" -Method Post -Body $dingtalkPayload -ContentType "application/json"
Write-Host "Status: $($resp.StatusCode)"
Write-Host "Body: $($resp.Content)"
