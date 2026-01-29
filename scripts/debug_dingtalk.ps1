# DingTalk Stream Mode Debug Script

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  DingTalk Stream Debug" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "[1] Checking Configuration..." -ForegroundColor Yellow
Write-Host ""

# Check environment variable
$useStream = $env:DINGTALK_USE_STREAM
if ($useStream -eq "true") {
    Write-Host "  DINGTALK_USE_STREAM: " -NoNewline
    Write-Host "true" -ForegroundColor Green
} else {
    Write-Host "  DINGTALK_USE_STREAM: " -NoNewline
    Write-Host "NOT SET (默认 false)" -ForegroundColor Red
    Write-Host ""
    Write-Host "  问题: Stream 模式未启用!" -ForegroundColor Red
    Write-Host ""
    Write-Host "  解决方案:" -ForegroundColor Yellow
    Write-Host '    $env:DINGTALK_USE_STREAM = "true"' -ForegroundColor White
    Write-Host ""
}

Write-Host ""
Write-Host "[2] Checking Gateway Status..." -ForegroundColor Yellow
Write-Host ""

try {
    $health = Invoke-RestMethod -Uri "http://localhost:8080/healthz" -Method Get -TimeoutSec 2
    Write-Host "  Gateway: " -NoNewline
    Write-Host "Running" -ForegroundColor Green
} catch {
    Write-Host "  Gateway: " -NoNewline
    Write-Host "Not Running" -ForegroundColor Red
}

Write-Host ""
Write-Host "[3] Checking OpenCode Server..." -ForegroundColor Yellow
Write-Host ""

try {
    $response = Invoke-WebRequest -Uri "http://localhost:3000" -Method Get -TimeoutSec 2 -UseBasicParsing
    Write-Host "  OpenCode Server: " -NoNewline
    Write-Host "Running" -ForegroundColor Green
} catch {
    Write-Host "  OpenCode Server: " -NoNewline
    Write-Host "Not Running" -ForegroundColor Red
    Write-Host ""
    Write-Host "  请先启动 OpenCode Server:" -ForegroundColor Yellow
    Write-Host "    opencode server" -ForegroundColor Gray
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# If Stream mode not enabled, show fix
if ($useStream -ne "true") {
    Write-Host "修复步骤:" -ForegroundColor Yellow
    Write-Host ""
    Write-Host "1. 设置环境变量:" -ForegroundColor White
    Write-Host '   $env:DINGTALK_USE_STREAM = "true"' -ForegroundColor Gray
    Write-Host ""
    Write-Host "2. 重启 Gateway:" -ForegroundColor White
    Write-Host "   cd E:\Work\projects\gos\src\opencode-gateway\cmd\gateway" -ForegroundColor Gray
    Write-Host "   go run .\main.go" -ForegroundColor Gray
    Write-Host ""
    Write-Host "3. 查看日志确认 Stream 模式启动:" -ForegroundColor White
    Write-Host '   应该看到: "dingtalk: Stream mode client started"' -ForegroundColor Gray
    Write-Host ""
}
