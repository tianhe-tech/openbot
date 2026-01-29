# Detailed API Test - Shows full input/output for gateway-OpenCode interaction
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Gateway <-> OpenCode API Test" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

$baseUrl = "http://localhost:8080"

function Test-Adapter {
    param(
        [string]$Name,
        [string]$Endpoint,
        [hashtable]$Payload
    )
    
    Write-Host "[$Name Adapter]" -ForegroundColor Yellow
    Write-Host "Endpoint: POST $baseUrl$Endpoint" -ForegroundColor Gray
    Write-Host ""
    
    # Convert payload to JSON
    $jsonPayload = $Payload | ConvertTo-Json -Depth 10
    
    Write-Host ">>> REQUEST PAYLOAD >>>" -ForegroundColor Green
    Write-Host $jsonPayload -ForegroundColor White
    Write-Host ""
    
    try {
        # Send request
        $response = Invoke-WebRequest -Uri "$baseUrl$Endpoint" -Method Post -Body $jsonPayload -ContentType "application/json" -UseBasicParsing -ErrorAction Stop
        
        Write-Host "<<< RESPONSE (Status: $($response.StatusCode)) <<<" -ForegroundColor Cyan
        
        # Pretty print JSON response
        $jsonResponse = $response.Content | ConvertFrom-Json | ConvertTo-Json -Depth 10
        Write-Host $jsonResponse -ForegroundColor White
        
        # Parse key information
        $respObj = $response.Content | ConvertFrom-Json
        Write-Host ""
        Write-Host "=== Key Information ===" -ForegroundColor Magenta
        
        # Extract reply text based on adapter
        $replyText = $null
        if ($respObj.reply) {
            $replyText = $respObj.reply
        } elseif ($respObj.content -and $respObj.content.text) {
            $replyText = $respObj.content.text
        } elseif ($respObj.text -and $respObj.text.content) {
            $replyText = $respObj.text.content
        }
        
        if ($replyText) {
            Write-Host "AI Reply: " -NoNewline -ForegroundColor Gray
            Write-Host $replyText.Substring(0, [Math]::Min(100, $replyText.Length)) -ForegroundColor Yellow
            if ($replyText.Length -gt 100) { Write-Host "..." -ForegroundColor Yellow }
        }
        
        if ($respObj.session_id) {
            Write-Host "Session ID: " -NoNewline -ForegroundColor Gray
            Write-Host $respObj.session_id -ForegroundColor Yellow
        }
        
        if ($respObj.trace) {
            Write-Host "Trace: " -NoNewline -ForegroundColor Gray
            Write-Host $respObj.trace -ForegroundColor Yellow
        }
        
        Write-Host ""
        Write-Host "✅ SUCCESS" -ForegroundColor Green
        
    } catch {
        Write-Host "<<< ERROR <<<" -ForegroundColor Red
        Write-Host $_.Exception.Message -ForegroundColor Red
        if ($_.ErrorDetails) {
            Write-Host $_.ErrorDetails.Message -ForegroundColor Red
        }
        Write-Host ""
        Write-Host "❌ FAILED" -ForegroundColor Red
    }
    
    Write-Host ""
    Write-Host "========================================" -ForegroundColor Cyan
    Write-Host ""
}

# Test 1: Feishu
$feishuPayload = @{
    schema = "2.0"
    token = "test_token_feishu"
    type = "im.message.receive_v1"
    event = @{
        sender = @{
            sender_id = @{
                open_id = "ou_test_feishu_user_001"
                user_id = "user_feishu_001"
            }
        }
        message = @{
            message_id = "om_feishu_msg_$(Get-Random -Maximum 9999)"
            message_type = "text"
            chat_id = "oc_feishu_chat_123"
            content = '{"text": "你好，请帮我解释一下什么是REST API"}'
        }
    }
}

Test-Adapter -Name "Feishu" -Endpoint "/feishu/callback" -Payload $feishuPayload

Start-Sleep -Seconds 1

# Test 2: WeCom
$wecomPayload = @{
    msgtype = "text"
    from_userid = "wecom_user_456"
    text = @{
        content = "What is the difference between HTTP and HTTPS?"
    }
}

Test-Adapter -Name "WeCom" -Endpoint "/wecom/callback" -Payload $wecomPayload

Start-Sleep -Seconds 1

# Test 3: DingTalk
$dingtalkPayload = @{
    msgtype = "text"
    conversationType = "1"
    conversationId = "cid_dingtalk_conv_789"
    senderStaffId = "staff_dingtalk_001"
    text = @{
        content = "Explain what is Git in simple terms"
    }
}

Test-Adapter -Name "DingTalk" -Endpoint "/dingtalk/callback" -Payload $dingtalkPayload

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  All Tests Complete" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
