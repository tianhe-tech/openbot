# TOT - ClawHub Skill Manager for OpenCode

自动搜索、安装、适配 ClawHub skills 到 OpenCode 的命令行工具。

## 功能

- 🔍 **搜索** - 从 ClawHub/GitHub 搜索 skills
- ℹ️ **查看** - 查看 skill 详情和 SKILL.md 预览
- 📦 **安装** - 自动克隆、格式转换、权限配置
- 📋 **列表** - 列出已安装的 skills
- 🗑️ **卸载** - 移除已安装的 skill

## 安装

```bash
# 克隆并安装
git clone https://github.com/yourname/thopenbot-install.git
cd thopenbot-install
chmod +x tot
sudo ln -sf $(pwd)/tot /usr/local/bin/tot
```

## 使用方法

### 搜索 skills

```bash
# 搜索 Kubernetes 相关 skills
tot search kubernetes

# 搜索 DevOps 相关
tot search devops

# 限制结果数量
tot search kubernetes -l 10
```

### 查看 skill 详情

```bash
tot info kube-medic
```

### 安装 skills

```bash
# 通过名称安装
tot install kube-medic

# 通过 GitHub URL 安装
tot install https://github.com/user/skill-name.git

# 安装 skills 集合（会自动安装所有子 skills）
tot install https://github.com/cacheforge-ai/cacheforge-skills.git
```

### 列出已安装 skills

```bash
tot list
```

### 卸载 skills

```bash
tot uninstall kube-medic
```

## 格式转换

TOT 自动将 ClawHub skill 格式转换为 OpenCode 兼容格式：

### ClawHub 格式
```yaml
---
name: kube-medic
version: 1.0.2
description: "Kubernetes diagnostics"
author: Anvil AI
tags:
  - kubernetes
  - k8s
tools:
  - name: kube_medic
    command: "bash scripts/kube-medic.sh"
dependencies:
  - kubectl
  - jq
---
```

### OpenCode 格式（自动转换）
```yaml
---
name: kube-medic
description: Kubernetes diagnostics
license: MIT
compatibility: opencode
metadata:
  audience: all-users
  workflow: kubernetes-ops
  timeout: 30000
  dependency: kubectl, jq
---
```

## 权限管理

TOT 自动更新 `~/.config/opencode/opencode.json` 中的权限配置：

```json
{
  "permission": {
    "skill": {
      "kube-medic": "allow",
      "prom-query": "allow",
      "*": "deny"
    }
  }
}
```

## 目录结构

```
~/.config/opencode/
├── opencode.json          # OpenCode 配置
└── skills/                 # Skills 目录
    ├── kube-medic/
    │   ├── SKILL.md       # 已转换的格式
    │   └── scripts/
    │       └── kube-medic.sh
    ├── prom-query/
    └── ...
```

## 环境变量

- `GITHUB_TOKEN` - GitHub API token（提高 API 速率限制）

## 注意事项

1. 需要 `git` 命令来克隆仓库
2. 需要 Python 3.6+
3. 部分 skills 可能需要额外的系统依赖（如 kubectl、jq 等）

## License

MIT
