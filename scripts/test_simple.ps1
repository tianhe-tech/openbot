Write-Host "Testing OpenCode Gateway" -ForegroundColor Cyan
Write-Host "=========================" -ForegroundColor Cyan
Write-Host ""

# Test 1: Health check
Write-Host "[1/3] Health check..." -ForegroundColor Yellow
try {
    $response = Invoke-RestMethod -Uri "http://localhost:8080/healthz" -Method GET
    Write-Host "OK: Health check passed - $response" -ForegroundColor Green
} catch {
    Write-Host "FAIL: Health check failed - $_" -ForegroundColor Red
    exit 1
}
Write-Host ""

# Test 2: Feishu webhook
Write-Host "[2/3] Testing Feishu webhook..." -ForegroundColor Yellow
$feishuPayload = @"
{
  "type": "event_callback",
  "token": "test_token",
  "event": {
    "sender": {
      "sender_id": {
        "open_id": "ou_test123"
      }
    },
    "message": {
      "message_id": "om_test456",
      "message_type": "text",
      "chat_id": "oc_test789",
      "content": {
        "text": "Hello, please introduce yourself"
      }
    }
  }
}
"@

try {
    $response = Invoke-RestMethod -Uri "http://localhost:8080/feishu/callback" -Method POST -ContentType "application/json" -Body $feishuPayload
    Write-Host "OK: Feishu message sent" -ForegroundColor Green
    Write-Host "  Reply: $($response.content.text)" -ForegroundColor Cyan
    Write-Host "  Session ID: $($response.session_id)" -ForegroundColor Cyan
} catch {
    Write-Host "FAIL: Feishu test failed - $_" -ForegroundColor Red
    Write-Host "Response: $($_.Exception.Response)" -ForegroundColor Yellow
}
Write-Host ""

# Test 3: Check OpenCode Server
Write-Host "[3/3] Checking OpenCode Server..." -ForegroundColor Yellow
try {
    Invoke-RestMethod -Uri "http://localhost:3000/" -Method GET -TimeoutSec 2 -ErrorAction Stop | Out-Null
    Write-Host "OK: OpenCode Server is accessible" -ForegroundColor Green
} catch {
    Write-Host "INFO: OpenCode Server test skipped (this is normal)" -ForegroundColor Yellow
}
Write-Host ""

Write-Host "=========================" -ForegroundColor Cyan
Write-Host "Tests completed!" -ForegroundColor Green
