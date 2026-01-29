# Test script for OpenCode Gateway
# Tests gateway-OpenCode server interaction and all adapters

Write-Host "=== OpenCode Gateway Test Suite ===" -ForegroundColor Cyan
Write-Host ""

$baseUrl = "http://localhost:8080"

# Test 1: Health Check
Write-Host "[1/4] Testing health check..." -ForegroundColor Yellow
try {
    $health = Invoke-RestMethod -Uri "$baseUrl/healthz" -Method Get -ErrorAction Stop
    if ($health -eq "ok") {
        Write-Host "OK: Health check passed - $health" -ForegroundColor Green
    } else {
        Write-Host "FAILED: Unexpected health response: $health" -ForegroundColor Red
    }
} catch {
    Write-Host "FAILED: Health check error: $_" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Test 2: Feishu Adapter
Write-Host "[2/4] Testing Feishu adapter..." -ForegroundColor Yellow
$feishuPayload = @{
    schema = "2.0"
    token = "test_token"
    type = "im.message.receive_v1"
    event = @{
        sender = @{
            sender_id = @{
                open_id = "ou_test123"
                user_id = "test_user_123"
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

try {
    $feishuResp = Invoke-RestMethod -Uri "$baseUrl/feishu/callback" -Method Post -Body $feishuPayload -ContentType "application/json" -ErrorAction Stop
    Write-Host "OK: Feishu message sent" -ForegroundColor Green
    $reply = if ($feishuResp.content -and $feishuResp.content.text) { $feishuResp.content.text } else { $feishuResp.reply }
    Write-Host "  Reply: $($reply.Substring(0, [Math]::Min(80, $reply.Length)))..." -ForegroundColor Gray
    Write-Host "  Session ID: $($feishuResp.session_id)" -ForegroundColor Gray
} catch {
    Write-Host "FAILED: Feishu adapter error: $_" -ForegroundColor Red
}
Write-Host ""

# Test 3: WeCom Adapter
Write-Host "[3/4] Testing WeCom adapter..." -ForegroundColor Yellow
$wecomPayload = @{
    msgtype = "text"
    from_userid = "test_user_456"
    text = @{
        content = "Hello from WeCom"
    }
} | ConvertTo-Json -Depth 5

try {
    $wecomResp = Invoke-RestMethod -Uri "$baseUrl/wecom/callback" -Method Post -Body $wecomPayload -ContentType "application/json" -ErrorAction Stop
    Write-Host "OK: WeCom message sent" -ForegroundColor Green
    Write-Host "  Reply: $($wecomResp.reply.Substring(0, [Math]::Min(80, $wecomResp.reply.Length)))..." -ForegroundColor Gray
    Write-Host "  Session ID: $($wecomResp.session_id)" -ForegroundColor Gray
} catch {
    Write-Host "FAILED: WeCom adapter error: $_" -ForegroundColor Red
}
Write-Host ""

# Test 4: DingTalk Adapter
Write-Host "[4/4] Testing DingTalk adapter..." -ForegroundColor Yellow
$dingtalkPayload = @{
    msgtype = "text"
    conversationType = "1"
    conversationId = "cid_test_123"
    senderStaffId = "staff_123"
    text = @{
        content = "Hello from DingTalk"
    }
} | ConvertTo-Json -Depth 5

try {
    $dingtalkResp = Invoke-RestMethod -Uri "$baseUrl/dingtalk/callback" -Method Post -Body $dingtalkPayload -ContentType "application/json" -ErrorAction Stop
    Write-Host "OK: DingTalk message sent" -ForegroundColor Green
    $reply = if ($dingtalkResp.text -and $dingtalkResp.text.content) { $dingtalkResp.text.content } else { $dingtalkResp.reply }
    Write-Host "  Reply: $($reply.Substring(0, [Math]::Min(80, $reply.Length)))..." -ForegroundColor Gray
    Write-Host "  Session ID: $($dingtalkResp.session_id)" -ForegroundColor Gray
} catch {
    Write-Host "FAILED: DingTalk adapter error: $_" -ForegroundColor Red
}
Write-Host ""

Write-Host "=== Test Suite Complete ===" -ForegroundColor Cyan
