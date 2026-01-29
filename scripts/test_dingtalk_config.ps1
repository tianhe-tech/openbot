# DingTalk Configuration Test Script

Write-Host ""
Write-Host "=======================================" -ForegroundColor Cyan
Write-Host "  DingTalk Integration Configuration" -ForegroundColor Cyan
Write-Host "=======================================" -ForegroundColor Cyan
Write-Host ""

# Check current configuration
Write-Host "[1] Checking Configuration..." -ForegroundColor Yellow
Write-Host ""

$useStream = $env:DINGTALK_USE_STREAM
$clientId = $env:DINGTALK_CLIENT_ID
$clientSecret = $env:DINGTALK_CLIENT_SECRET
$appKey = $env:DINGTALK_APP_KEY
$appSecret = $env:DINGTALK_APP_SECRET

if ($useStream -eq "true") {
    Write-Host "  Mode: " -NoNewline -ForegroundColor Gray
    Write-Host "Stream (Recommended)" -ForegroundColor Green
    Write-Host ""
    
    if ($clientId -and $clientSecret) {
        Write-Host "  Client ID: " -NoNewline -ForegroundColor Gray
        Write-Host "$($clientId.Substring(0, [Math]::Min(20, $clientId.Length)))..." -ForegroundColor Green
        
        Write-Host "  Client Secret: " -NoNewline -ForegroundColor Gray
        Write-Host "***" -ForegroundColor Green
        
        Write-Host ""
        Write-Host "  Status: " -NoNewline -ForegroundColor Gray
        Write-Host "Ready for Stream mode" -ForegroundColor Green
    } else {
        Write-Host ""
        Write-Host "  Status: " -NoNewline -ForegroundColor Gray
        Write-Host "Missing credentials" -ForegroundColor Red
        Write-Host ""
        Write-Host "  Required:" -ForegroundColor Yellow
        Write-Host "    DINGTALK_CLIENT_ID" -ForegroundColor Gray
        Write-Host "    DINGTALK_CLIENT_SECRET" -ForegroundColor Gray
    }
} else {
    Write-Host "  Mode: " -NoNewline -ForegroundColor Gray
    Write-Host "Webhook (Legacy)" -ForegroundColor Yellow
    Write-Host ""
    
    if ($appKey -and $appSecret) {
        Write-Host "  App Key: " -NoNewline -ForegroundColor Gray
        Write-Host "$($appKey.Substring(0, [Math]::Min(20, $appKey.Length)))..." -ForegroundColor Green
        
        Write-Host "  App Secret: " -NoNewline -ForegroundColor Gray
        Write-Host "***" -ForegroundColor Green
        
        Write-Host ""
        Write-Host "  Status: " -NoNewline -ForegroundColor Gray
        Write-Host "Ready for Webhook mode" -ForegroundColor Green
    } else {
        Write-Host ""
        Write-Host "  Status: " -NoNewline -ForegroundColor Gray
        Write-Host "Missing credentials" -ForegroundColor Red
        Write-Host ""
        Write-Host "  Required:" -ForegroundColor Yellow
        Write-Host "    DINGTALK_APP_KEY" -ForegroundColor Gray
        Write-Host "    DINGTALK_APP_SECRET" -ForegroundColor Gray
    }
}

Write-Host ""
Write-Host "---------------------------------------" -ForegroundColor DarkGray
Write-Host ""

# Configuration guide
Write-Host "[2] Configuration Guide" -ForegroundColor Yellow
Write-Host ""

Write-Host "To use Stream mode (Recommended):" -ForegroundColor Cyan
Write-Host ""
Write-Host '  $env:DINGTALK_CLIENT_ID = "your-client-id"' -ForegroundColor Gray
Write-Host '  $env:DINGTALK_CLIENT_SECRET = "your-secret"' -ForegroundColor Gray
Write-Host '  $env:DINGTALK_USE_STREAM = "true"' -ForegroundColor Gray
Write-Host ""

Write-Host "To use Webhook mode (Legacy):" -ForegroundColor Cyan
Write-Host ""
Write-Host '  $env:DINGTALK_APP_KEY = "your-app-key"' -ForegroundColor Gray
Write-Host '  $env:DINGTALK_APP_SECRET = "your-app-secret"' -ForegroundColor Gray
Write-Host '  $env:DINGTALK_USE_STREAM = "false"' -ForegroundColor Gray
Write-Host ""

Write-Host "---------------------------------------" -ForegroundColor DarkGray
Write-Host ""

# Test Gateway connection
Write-Host "[3] Testing Gateway..." -ForegroundColor Yellow
Write-Host ""

try {
    $health = Invoke-RestMethod -Uri "http://localhost:8080/healthz" -Method Get -ErrorAction Stop
    Write-Host "  Gateway Status: " -NoNewline -ForegroundColor Gray
    Write-Host "Running" -ForegroundColor Green
} catch {
    Write-Host "  Gateway Status: " -NoNewline -ForegroundColor Gray
    Write-Host "Not running" -ForegroundColor Red
    Write-Host ""
    Write-Host "  Start Gateway with:" -ForegroundColor Yellow
    Write-Host "    .\bin\gateway.exe" -ForegroundColor Gray
}

Write-Host ""
Write-Host "---------------------------------------" -ForegroundColor DarkGray
Write-Host ""

# Documentation links
Write-Host "[4] Documentation" -ForegroundColor Yellow
Write-Host ""
Write-Host "  Setup Guide: DINGTALK_SETUP.md" -ForegroundColor Gray
Write-Host "  API Testing: API_TEST_GUIDE.md" -ForegroundColor Gray
Write-Host ""
Write-Host "  Official Docs:" -ForegroundColor Gray
Write-Host "    https://open.dingtalk.com/" -ForegroundColor DarkGray
Write-Host "    https://opensource.dingtalk.com/developerpedia/" -ForegroundColor DarkGray
Write-Host ""

Write-Host "=======================================" -ForegroundColor Cyan
Write-Host ""
