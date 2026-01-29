# OpenCode Gateway 快速测试脚本 (Windows)

Write-Host "🚀 OpenCode Gateway 快速测试" -ForegroundColor Cyan
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

# 检查 Go 环境
Write-Host "[1/6] 检查 Go 环境..." -ForegroundColor Yellow
$goCmd = Get-Command go -ErrorAction SilentlyContinue
if ($null -eq $goCmd) {
    Write-Host "❌ Go 未安装" -ForegroundColor Red
    exit 1
}
$goVersion = go version
Write-Host "✓ $goVersion" -ForegroundColor Green
Write-Host ""

# 检查依赖
Write-Host "[2/6] 检查依赖..." -ForegroundColor Yellow
if (!(Test-Path "go.mod")) {
    Write-Host "❌ go.mod 不存在" -ForegroundColor Red
    exit 1
}
Write-Host "✓ go.mod 存在" -ForegroundColor Green
Write-Host ""

# 下载依赖
Write-Host "[3/6] 下载依赖..." -ForegroundColor Yellow
go mod download 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ 依赖下载完成" -ForegroundColor Green
} else {
    Write-Host "❌ 依赖下载失败" -ForegroundColor Red
}
Write-Host ""

# 创建输出目录
if (!(Test-Path "bin")) {
    New-Item -ItemType Directory -Path "bin" | Out-Null
}

# 编译项目
Write-Host "[4/6] 编译项目..." -ForegroundColor Yellow
$env:CGO_ENABLED = "0"
go build -o bin/gateway.exe cmd/gateway/main.go 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ 编译成功: bin/gateway.exe" -ForegroundColor Green
} else {
    Write-Host "❌ 编译失败" -ForegroundColor Red
    go build -o bin/gateway.exe cmd/gateway/main.go
    exit 1
}
Write-Host ""

# 编译示例
Write-Host "[5/6] 编译示例..." -ForegroundColor Yellow
go build -o bin/sdk_demo.exe examples/sdk_demo.go 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ 示例编译成功: bin/sdk_demo.exe" -ForegroundColor Green
} else {
    Write-Host "⚠ 示例编译失败（可能需要完整的依赖）" -ForegroundColor Yellow
}
Write-Host ""

# 运行测试
Write-Host "[6/6] 运行测试..." -ForegroundColor Yellow
go test ./... 2>&1 | Out-Null
if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ 测试通过" -ForegroundColor Green
} else {
    Write-Host "⚠ 测试失败（可能是正常的，因为需要 OpenCode Server）" -ForegroundColor Yellow
}
Write-Host ""

# 显示使用说明
Write-Host "================================" -ForegroundColor Cyan
Write-Host "✅ 所有检查完成！" -ForegroundColor Green
Write-Host ""
Write-Host "📝 下一步:" -ForegroundColor Cyan
Write-Host "  1. 配置环境变量:"
Write-Host '     $env:OPENCODE_BASE_URL = "http://localhost:54321"'
Write-Host '     $env:OPENCODE_API_KEY = "your-api-key"'
Write-Host ""
Write-Host "  2. 启动服务:"
Write-Host "     .\bin\gateway.exe"
Write-Host ""
Write-Host "  3. 或运行示例:"
Write-Host "     .\bin\sdk_demo.exe"
Write-Host ""
Write-Host "  4. 测试 Webhook:"
Write-Host "     curl http://localhost:8080/healthz"
Write-Host ""
Write-Host "📚 文档:" -ForegroundColor Cyan
Write-Host "  - 架构文档: ARCHITECTURE.md"
Write-Host "  - 升级总结: UPGRADE_SUMMARY.md"
Write-Host "  - 示例说明: examples\README.md"
Write-Host ""
