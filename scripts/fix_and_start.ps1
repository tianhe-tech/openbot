# Quick Fix and Restart Script for DingTalk Stream Mode

Write-Host ""
Write-Host "========================================" -ForegroundColor Green
Write-Host "  DingTalk Stream Mode - Quick Fix" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""

Write-Host "已修复问题:" -ForegroundColor Yellow
Write-Host "  1. 启用 Stream 模式 (UseStream = true)" -ForegroundColor Gray
Write-Host "  2. 添加详细日志输出" -ForegroundColor Gray
Write-Host ""

Write-Host "停止现有进程..." -ForegroundColor Yellow
Get-Process | Where-Object { $_.ProcessName -eq "gateway" -or $_.ProcessName -eq "main" } | Stop-Process -Force -ErrorAction SilentlyContinue
Start-Sleep -Seconds 1

Write-Host "重新编译..." -ForegroundColor Yellow
Push-Location E:\Work\projects\gos\src\opencode-gateway

go build -o bin\gateway.exe cmd\gateway\main.go

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "编译失败!" -ForegroundColor Red
    Pop-Location
    exit 1
}

Write-Host "编译成功!" -ForegroundColor Green
Write-Host ""

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  启动 Gateway (Stream 模式)" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

Write-Host "期望看到的日志:" -ForegroundColor Yellow
Write-Host "  - dingtalk: starting Stream mode connection..." -ForegroundColor Gray
Write-Host "  - dingtalk: using ClientID: dingk90c2agr1blmauzh..." -ForegroundColor Gray
Write-Host "  - dingtalk: Stream mode client started..." -ForegroundColor Gray
Write-Host "  - dingtalk: Stream client connected successfully" -ForegroundColor Gray
Write-Host "  - adapters registered: [wecom feishu dingtalk (stream)]" -ForegroundColor Gray
Write-Host ""

Write-Host "启动中..." -ForegroundColor Green
Write-Host ""

.\bin\gateway.exe

Pop-Location
