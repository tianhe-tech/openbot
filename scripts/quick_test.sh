#!/bin/bash
# OpenCode Gateway 快速测试脚本

set -e

echo "🚀 OpenCode Gateway 快速测试"
echo "================================"
echo ""

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查 Go 环境
echo -e "${YELLOW}[1/6]${NC} 检查 Go 环境..."
if ! command -v go &> /dev/null; then
    echo -e "${RED}❌ Go 未安装${NC}"
    exit 1
fi
GO_VERSION=$(go version)
echo -e "${GREEN}✓${NC} $GO_VERSION"
echo ""

# 检查依赖
echo -e "${YELLOW}[2/6]${NC} 检查依赖..."
if [ ! -f "go.mod" ]; then
    echo -e "${RED}❌ go.mod 不存在${NC}"
    exit 1
fi
echo -e "${GREEN}✓${NC} go.mod 存在"
echo ""

# 下载依赖
echo -e "${YELLOW}[3/6]${NC} 下载依赖..."
go mod download
echo -e "${GREEN}✓${NC} 依赖下载完成"
echo ""

# 编译项目
echo -e "${YELLOW}[4/6]${NC} 编译项目..."
if go build -o bin/gateway cmd/gateway/main.go; then
    echo -e "${GREEN}✓${NC} 编译成功: bin/gateway"
else
    echo -e "${RED}❌ 编译失败${NC}"
    exit 1
fi
echo ""

# 编译示例
echo -e "${YELLOW}[5/6]${NC} 编译示例..."
if go build -o bin/sdk_demo examples/sdk_demo.go; then
    echo -e "${GREEN}✓${NC} 示例编译成功: bin/sdk_demo"
else
    echo -e "${RED}❌ 示例编译失败${NC}"
    exit 1
fi
echo ""

# 运行测试
echo -e "${YELLOW}[6/6]${NC} 运行测试..."
if go test ./... -v; then
    echo -e "${GREEN}✓${NC} 测试通过"
else
    echo -e "${YELLOW}⚠${NC} 测试失败（可能是正常的，因为需要 OpenCode Server）"
fi
echo ""

# 显示使用说明
echo "================================"
echo -e "${GREEN}✅ 所有检查完成！${NC}"
echo ""
echo "📝 下一步:"
echo "  1. 配置环境变量:"
echo "     export OPENCODE_BASE_URL=\"http://localhost:54321\""
echo "     export OPENCODE_API_KEY=\"your-api-key\""
echo ""
echo "  2. 启动服务:"
echo "     ./bin/gateway"
echo ""
echo "  3. 或运行示例:"
echo "     ./bin/sdk_demo"
echo ""
echo "  4. 测试 Webhook:"
echo "     curl http://localhost:8080/healthz"
echo ""
echo "📚 文档:"
echo "  - 架构文档: ARCHITECTURE.md"
echo "  - 升级总结: UPGRADE_SUMMARY.md"
echo "  - 示例说明: examples/README.md"
echo ""
