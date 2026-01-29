# Quick Start Script for DingTalk Stream Mode
# 钉钉 Stream 模式快速启动脚本

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  OpenCode Gateway - DingTalk Stream" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if credentials are provided as arguments
param(
    [Parameter()]
    [string]$ClientId,
    
    [Parameter()]
    [string]$ClientSecret
)

# If not provided via params, check environment or prompt
if (-not $ClientId) {
    $ClientId = $env:DINGTALK_CLIENT_ID
    if (-not $ClientId) {
        Write-Host "Enter your DingTalk Client ID:" -ForegroundColor Yellow
        $ClientId = Read-Host
    }
}

if (-not $ClientSecret) {
    $ClientSecret = $env:DINGTALK_CLIENT_SECRET
    if (-not $ClientSecret) {
        Write-Host "Enter your DingTalk Client Secret:" -ForegroundColor Yellow
        $ClientSecret = Read-Host -AsSecureString
        $ClientSecret = [Runtime.InteropServices.Marshal]::PtrToStringAuto(
            [Runtime.InteropServices.Marshal]::SecureStringToBSTR($ClientSecret)
        )
    }
}

# Validate inputs
if ([string]::IsNullOrWhiteSpace($ClientId) -or [string]::IsNullOrWhiteSpace($ClientSecret)) {
    Write-Host ""
    Write-Host "ERROR: Client ID and Client Secret are required!" -ForegroundColor Red
    Write-Host ""
    Write-Host "Get your credentials from:" -ForegroundColor Yellow
    Write-Host "  https://open.dingtalk.com/" -ForegroundColor Gray
    Write-Host ""
    Write-Host "Usage:" -ForegroundColor Yellow
    Write-Host "  .\start-dingtalk-stream.ps1 -ClientId 'your-id' -ClientSecret 'your-secret'" -ForegroundColor Gray
    Write-Host ""
    exit 1
}

Write-Host "Configuration:" -ForegroundColor Green
Write-Host "  Client ID: $($ClientId.Substring(0, [Math]::Min(20, $ClientId.Length)))..." -ForegroundColor Gray
Write-Host "  Client Secret: ***" -ForegroundColor Gray
Write-Host ""

# Check if OpenCode is running
Write-Host "Checking OpenCode Server..." -ForegroundColor Yellow
try {
    $health = Invoke-RestMethod -Uri "http://localhost:3000" -Method Get -TimeoutSec 2 -ErrorAction Stop
    Write-Host "  OpenCode Server: Running" -ForegroundColor Green
} catch {
    Write-Host "  WARNING: OpenCode Server not responding" -ForegroundColor Yellow
    Write-Host "  Make sure OpenCode is running on http://localhost:3000" -ForegroundColor Gray
    Write-Host ""
    Write-Host "  Start OpenCode with:" -ForegroundColor Yellow
    Write-Host "    opencode server" -ForegroundColor Gray
    Write-Host ""
    
    $continue = Read-Host "Continue anyway? (y/n)"
    if ($continue -ne "y") {
        exit 1
    }
}

Write-Host ""
Write-Host "Setting environment variables..." -ForegroundColor Yellow

# Set environment variables
$env:DINGTALK_CLIENT_ID = $ClientId
$env:DINGTALK_CLIENT_SECRET = $ClientSecret
$env:DINGTALK_USE_STREAM = "true"
$env:OPENCODE_ENDPOINT = "http://localhost:3000"
$env:OPENCODE_API_KEY = "123"

Write-Host "  DINGTALK_USE_STREAM = true" -ForegroundColor Gray
Write-Host "  OPENCODE_ENDPOINT = http://localhost:3000" -ForegroundColor Gray
Write-Host ""

# Check if binary exists
if (-not (Test-Path ".\bin\gateway.exe")) {
    Write-Host "Gateway binary not found. Building..." -ForegroundColor Yellow
    go build -o bin\gateway.exe cmd\gateway\main.go
    
    if ($LASTEXITCODE -ne 0) {
        Write-Host ""
        Write-Host "ERROR: Build failed!" -ForegroundColor Red
        exit 1
    }
    
    Write-Host "  Build successful" -ForegroundColor Green
    Write-Host ""
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  Starting Gateway..." -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Start the gateway
.\bin\gateway.exe
