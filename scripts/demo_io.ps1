# Gateway API Test - Full Input/Output Demo

Write-Host ""
Write-Host "=======================================" -ForegroundColor Cyan
Write-Host "  Gateway API I/O Test" -ForegroundColor Cyan
Write-Host "=======================================" -ForegroundColor Cyan
Write-Host ""

$baseUrl = "http://localhost:8080"

# Step 1: Health Check
Write-Host "[Step 1] Health Check" -ForegroundColor Yellow
try {
    $health = Invoke-RestMethod -Uri "$baseUrl/healthz" -Method Get
    Write-Host "  OK: Gateway is running" -ForegroundColor Green
} catch {
    Write-Host "  FAILED: Gateway is not running" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "---------------------------------------" -ForegroundColor DarkGray
Write-Host ""

# Step 2: Prepare Message
Write-Host "[Step 2] Prepare Test Message" -ForegroundColor Yellow
Write-Host ""

$testMessage = "What are the main features of Go language?"
Write-Host "  Platform: WeCom" -ForegroundColor Gray
Write-Host "  Message: $testMessage" -ForegroundColor White

Write-Host ""
Write-Host "[Step 3] Build API Request" -ForegroundColor Yellow

$payload = @{
    msgtype = "text"
    from_userid = "test_user"
    text = @{
        content = $testMessage
    }
}

$jsonPayload = $payload | ConvertTo-Json -Depth 5

Write-Host ""
Write-Host "REQUEST:" -ForegroundColor Cyan
Write-Host "POST $baseUrl/wecom/callback" -ForegroundColor Gray
Write-Host $jsonPayload -ForegroundColor White

Write-Host ""
Write-Host "[Step 4] Send Request" -ForegroundColor Yellow

$startTime = Get-Date

try {
    $response = Invoke-RestMethod -Uri "$baseUrl/wecom/callback" -Method Post -Body $jsonPayload -ContentType "application/json"
    
    $duration = (Get-Date) - $startTime
    $ms = [math]::Round($duration.TotalMilliseconds)
    Write-Host "  OK: Request succeeded (${ms}ms)" -ForegroundColor Green
    
    Write-Host ""
    Write-Host "RESPONSE:" -ForegroundColor Cyan
    
    $responseJson = $response | ConvertTo-Json -Depth 5
    Write-Host $responseJson -ForegroundColor White
    
    Write-Host ""
    Write-Host "[Step 5] Key Information" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "  Session ID: $($response.session_id)" -ForegroundColor Yellow
    Write-Host "  Trace ID:   $($response.trace)" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  AI Reply:" -ForegroundColor Magenta
    Write-Host "  +--------------------------------------+" -ForegroundColor DarkGray
    
    $reply = $response.reply
    $lines = $reply -split "`n"
    foreach ($line in $lines) {
        if ($line.Length -gt 38) {
            $short = $line.Substring(0, 35) + "..."
            Write-Host "  | $short" -ForegroundColor Cyan
        } else {
            Write-Host "  | $line" -ForegroundColor Cyan
        }
    }
    
    Write-Host "  +--------------------------------------+" -ForegroundColor DarkGray
    
    Write-Host ""
    Write-Host "---------------------------------------" -ForegroundColor DarkGray
    Write-Host ""
    Write-Host "SUCCESS: Communication is working" -ForegroundColor Green
    Write-Host ""
    Write-Host "Data Flow:" -ForegroundColor Gray
    Write-Host "  User Message -> Gateway -> SDK -> OpenCode Server -> AI -> Response" -ForegroundColor DarkGray
    Write-Host ""
    
} catch {
    Write-Host "  FAILED: Request error" -ForegroundColor Red
    Write-Host "  Error: $($_.Exception.Message)" -ForegroundColor Red
    Write-Host ""
}
