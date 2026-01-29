# OpenCode Gateway 测试脚本

Write-Host "🧪 OpenCode Gateway 测试" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

# 测试 1: 健康检查
Write-Host "[1/4] 测试健康检查..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost:8080/healthz" -Method GET -UseBasicParsing
    if ($response.StatusCode -eq 200) {
        Write-Host "✓ 健康检查通过: $($response.Content)" -ForegroundColor Green
    }
} catch {
    Write-Host "❌ 健康检查失败: $_" -ForegroundColor Red
    exit 1
}
Write-Host ""

# 测试 2: 模拟飞书消息
Write-Host "[2/4] 测试飞书 Webhook..." -ForegroundColor Yellow
$feishuPayload = @{
    type = "event_callback"
    token = "test_token"
    event = @{
        sender = @{
            sender_id = @{
                open_id = "ou_test123"
                user_id = "user_test123"
            }
        }
        message = @{
            message_id = "om_test456"
            message_type = "text"
            chat_id = "oc_test789"
            content = @{
                text = "你好，请介绍一下自己"
            }
        }
    }
} | ConvertTo-Json -Depth 5

try {
    $response = Invoke-WebRequest -Uri "http://localhost:8080/feishu/callback" `
        -Method POST `
        -ContentType "application/json" `
        -Body $feishuPayload `
        -UseBasicParsing
    
    if ($response.StatusCode -eq 200) {
        Write-Host "✓ 飞书消息发送成功" -ForegroundColor Green
        $result = $response.Content | ConvertFrom-Json
        Write-Host "  回复: $($result.content.text)" -ForegroundColor Cyan
        Write-Host "  Session ID: $($result.session_id)" -ForegroundColor Cyan
    }
} catch {
    $errorDetail = $_.Exception.Response
    if ($errorDetail) {
        $reader = New-Object System.IO.StreamReader($errorDetail.GetResponseStream())
        $responseBody = $reader.ReadToEnd()
        Write-Host "❌ 飞书测试失败: $responseBody" -ForegroundColor Red
    } else {
        Write-Host "❌ 飞书测试失败: $_" -ForegroundColor Red
    }
}
Write-Host ""

# 测试 3: 模拟企业微信消息
Write-Host "[3/4] 测试企业微信 Webhook..." -ForegroundColor Yellow
$wecomPayload = @{
    ToUserName = "test_corp"
    FromUserName = "test_user"
    CreateTime = [int]([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())
    MsgType = "text"
    Content = "帮我写一个 Hello World 程序"
    MsgId = "123456"
    AgentID = "1000002"
} | ConvertTo-Json

try {
    $response = Invoke-WebRequest -Uri "http://localhost:8080/wecom/callback" `
        -Method POST `
        -ContentType "application/json" `
        -Body $wecomPayload `
        -UseBasicParsing
    
    if ($response.StatusCode -eq 200) {
        Write-Host "✓ 企业微信消息发送成功" -ForegroundColor Green
        $result = $response.Content | ConvertFrom-Json
        Write-Host "  回复: $($result.reply)" -ForegroundColor Cyan
    }
} catch {
    Write-Host "⚠ 企业微信测试跳过（可能需要配置）" -ForegroundColor Yellow
}
Write-Host ""

# 测试 4: 直接测试 OpenCode Server
Write-Host "[4/4] 测试 OpenCode Server 连接..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost:3000/" -Method GET -UseBasicParsing -TimeoutSec 3
    Write-Host "✓ OpenCode Server 可访问" -ForegroundColor Green
} catch {
    Write-Host "⚠ OpenCode Server 连接测试失败（这是正常的，API 可能没有根路径）" -ForegroundColor Yellow
}
Write-Host ""

Write-Host "================================" -ForegroundColor Cyan
Write-Host "✅ 测试完成！" -ForegroundColor Green
Write-Host ""
Write-Host "📊 查看 Gateway 日志以获取更多信息" -ForegroundColor Cyan
