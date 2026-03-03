#!/usr/bin/env python3
"""
TOT - ClawHub Skill Manager for OpenCode
自动搜索、安装、适配 ClawHub skills 到 OpenCode

用法:
  tot search <keyword>     - 搜索 skills
  tot info <skill-name>    - 查看 skill 详情
  tot install <skill-name> - 安装 skill（自动适配格式）
  tot list                 - 列出已安装 skills
  tot uninstall <skill>    - 卸载 skill
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

# 配置
GITHUB_API = "https://api.github.com"
OPENCODE_CONFIG_DIR = Path.home() / ".config" / "opencode"
OPENCODE_SKILLS_DIR = OPENCODE_CONFIG_DIR / "skills"
OPENCODE_CONFIG_FILE = OPENCODE_CONFIG_DIR / "opencode.json"
TOT_CACHE_DIR = Path.home() / ".cache" / "tot"


def log(msg: str, level: str = "info"):
    """日志输出"""
    colors = {
        "info": "\033[36m",    # cyan
        "success": "\033[32m", # green
        "warn": "\033[33m",    # yellow
        "error": "\033[31m",   # red
    }
    reset = "\033[0m"
    color = colors.get(level, "")
    print(f"{color}{msg}{reset}")


def github_api(endpoint: str, params: dict = None) -> Optional[dict]:
    """调用 GitHub API"""
    url = f"{GITHUB_API}{endpoint}"
    if params:
        from urllib.parse import urlencode
        query = urlencode(params)
        url = f"{url}?{query}"
    
    headers = {"Accept": "application/vnd.github.v3+json"}
    
    # 如果有 GitHub token，使用它
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"token {token}"
    
    try:
        req = Request(url, headers=headers)
        with urlopen(req, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as e:
        if e.code == 403:
            log("GitHub API rate limit exceeded. Set GITHUB_TOKEN env var.", "error")
        else:
            log(f"GitHub API error: {e.code}", "error")
        return None
    except URLError as e:
        log(f"Network error: {e.reason}", "error")
        return None


def search_skills(keyword: str, limit: int = 20) -> list[dict]:
    """搜索 ClawHub skills"""
    log(f"Searching for '{keyword}'...", "info")
    
    # 搜索 GitHub 仓库
    result = github_api("/search/repositories", {
        "q": f"topic:skill {keyword}",
        "per_page": str(limit)
    })
    
    if not result or "items" not in result:
        return []
    
    skills = []
    for item in result["items"]:
        # 获取 SKILL.md 文件信息
        skill_info = {
            "name": item["name"],
            "full_name": item["full_name"],
            "description": item.get("description", ""),
            "url": item["html_url"],
            "clone_url": item["clone_url"],
            "stars": item.get("stargazers_count", 0),
            "updated": item.get("updated_at", "")[:10]
        }
        skills.append(skill_info)
    
    return skills


def get_skill_readme(full_name: str) -> Optional[str]:
    """获取 skill 的 README 或 SKILL.md 内容"""
    # 尝试获取 SKILL.md
    result = github_api(f"/repos/{full_name}/contents/SKILL.md")
    if result and "content" in result:
        import base64
        return base64.b64decode(result["content"]).decode("utf-8")
    
    # 尝试获取 README.md
    result = github_api(f"/repos/{full_name}/contents/README.md")
    if result and "content" in result:
        import base64
        return base64.b64decode(result["content"]).decode("utf-8")
    
    return None


def show_skill_info(skill_name: str) -> bool:
    """显示 skill 详情"""
    # 先搜索找到这个 skill
    result = github_api("/search/repositories", {
        "q": f"topic:skill {skill_name}",
        "per_page": "5"
    })
    
    if not result or not result.get("items"):
        log(f"Skill '{skill_name}' not found", "error")
        return False
    
    # 找最匹配的
    skill = None
    for item in result["items"]:
        if item["name"].lower() == skill_name.lower():
            skill = item
            break
    if not skill:
        skill = result["items"][0]
    
    print()
    log(f"📦 {skill['name']}", "success")
    print(f"   {skill.get('description', 'No description')}")
    print()
    print(f"   URL:     {skill['html_url']}")
    print(f"   Stars:   {skill.get('stargazers_count', 0)}")
    print(f"   Updated: {skill.get('updated_at', '')[:10]}")
    print()
    
    # 显示 SKILL.md 内容摘要
    readme = get_skill_readme(skill["full_name"])
    if readme:
        lines = readme.split("\n")
        # 显示前 30 行
        log("   SKILL.md preview:", "info")
        print("-" * 60)
        for line in lines[:30]:
            print(f"   {line}")
        if len(lines) > 30:
            print(f"   ... ({len(lines) - 30} more lines)")
        print("-" * 60)
    
    return True


def convert_skill_to_opencode(skill_dir: Path, skill_name: str) -> bool:
    """转换 ClawHub skill 为 OpenCode 兼容格式"""
    skill_md = skill_dir / "SKILL.md"
    
    if not skill_md.exists():
        # 查找可能的 skill 文件
        for pattern in ["*.md", "skill.md", "SKILL*"]:
            matches = list(skill_dir.glob(pattern))
            if matches:
                skill_md = matches[0]
                break
    
    if not skill_md.exists():
        log(f"No SKILL.md found in {skill_dir}", "warn")
        # 创建基本的 SKILL.md
        create_basic_skill_md(skill_md, skill_name)
        return True
    
    # 读取内容
    content = skill_md.read_text(encoding="utf-8")
    
    # 解析 frontmatter
    if content.startswith("---"):
        parts = content.split("---", 2)
        if len(parts) >= 3:
            frontmatter = parts[1].strip()
            body = parts[2]
            
            # 转换 frontmatter
            new_frontmatter = convert_frontmatter(frontmatter, skill_name)
            
            # 写回文件
            new_content = f"---\n{new_frontmatter}\n---{body}"
            skill_md.write_text(new_content, encoding="utf-8")
            log(f"Converted SKILL.md to OpenCode format", "success")
            return True
    
    return True


def convert_frontmatter(original: str, skill_name: str) -> str:
    """转换 YAML frontmatter 为 OpenCode 格式"""
    try:
        import yaml
        fields = yaml.safe_load(original)
    except ImportError:
        # Fallback to simple parsing if PyYAML not available
        fields = parse_yaml_simple(original)
    
    # OpenCode 必需字段
    new_lines = []
    new_lines.append(f"name: {fields.get('name', skill_name)}")
    
    # 描述 - 处理多行描述
    desc = fields.get('description', 'A skill from ClawHub')
    if isinstance(desc, str):
        # 如果是多行，取第一行或前100字符
        desc = desc.strip().split('\n')[0][:100]
    elif isinstance(desc, list):
        desc = str(desc[0]) if desc else 'A skill from ClawHub'
    new_lines.append(f"description: {desc}")
    
    # license - 可能在 metadata 中
    license_val = fields.get('license')
    if not license_val and 'metadata' in fields and isinstance(fields['metadata'], dict):
        license_val = fields['metadata'].get('license', 'MIT')
    if not license_val:
        license_val = 'MIT'
    new_lines.append(f"license: {license_val}")
    
    new_lines.append("compatibility: opencode")
    
    # metadata
    new_lines.append("metadata:")
    new_lines.append("  audience: all-users")
    
    # 从 tags 推断 workflow
    tags = fields.get("tags", [])
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.strip('[]').split(',')]
    if isinstance(tags, list):
        tags_str = " ".join(str(t).lower() for t in tags)
    else:
        tags_str = str(tags).lower()
    
    if "kubernetes" in tags_str or "k8s" in tags_str:
        new_lines.append("  workflow: kubernetes-ops")
    elif "devops" in tags_str:
        new_lines.append("  workflow: devops")
    elif "security" in tags_str:
        new_lines.append("  workflow: security")
    elif "log" in tags_str or "monitoring" in tags_str or "prometheus" in tags_str:
        new_lines.append("  workflow: observability")
    elif "api" in tags_str:
        new_lines.append("  workflow: api-development")
    else:
        new_lines.append("  workflow: general")
    
    new_lines.append("  timeout: 30000")
    
    # 从 dependencies 推断
    deps = fields.get("dependencies", [])
    if isinstance(deps, str):
        deps = [d.strip() for d in deps.strip('[]').split(',')]
    if isinstance(deps, list) and deps:
        new_lines.append(f"  dependency: {', '.join(str(d) for d in deps if d)}")
    
    return "\n".join(new_lines)


def parse_yaml_simple(yaml_str: str) -> dict:
    """简单的 YAML 解析器（不需要 PyYAML）"""
    fields = {}
    current_key = None
    current_value = []
    in_list = False
    in_multiline = False
    multiline_key = None
    
    for line in yaml_str.split("\n"):
        stripped = line.strip()
        
        # 跳过空行
        if not stripped:
            if in_multiline and multiline_key:
                current_value.append("")
            continue
        
        # 处理多行值（以 | 或 > 开头）
        if in_multiline:
            if line.startswith("  ") or line.startswith("\t"):
                current_value.append(stripped)
                continue
            else:
                # 多行结束
                fields[multiline_key] = " ".join(current_value)
                in_multiline = False
                multiline_key = None
                current_value = []
        
        # 检查是否是列表项
        if stripped.startswith("- "):
            if current_key:
                if current_key not in fields:
                    fields[current_key] = []
                if isinstance(fields[current_key], list):
                    fields[current_key].append(stripped[2:].strip())
            continue
        
        # 内联列表 [a, b, c]
        if stripped.startswith("[") and stripped.endswith("]"):
            if current_key:
                items = [i.strip() for i in stripped[1:-1].split(",")]
                fields[current_key] = items
                current_key = None
            continue
        
        # 检查是否是键值对
        if ":" in line and not line.startswith(" "):
            key, value = line.split(":", 1)
            key = key.strip()
            value = value.strip()
            
            # 处理引号包裹的值
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            elif value.startswith("'") and value.endswith("'"):
                value = value[1:-1]
            
            # 检查多行值
            if value in ("|", ">"):
                in_multiline = True
                multiline_key = key
                current_value = []
                current_key = key
                continue
            
            if value:
                fields[key] = value
                current_key = key
            else:
                fields[key] = []
                current_key = key
        elif line.startswith(" ") and current_key:
            # 嵌套值
            if ":" in stripped:
                # 嵌套键值对，跳过
                continue
            current_value.append(stripped)
    
    # 处理最后一个多行值
    if in_multiline and multiline_key and current_value:
        fields[multiline_key] = " ".join(current_value)
    
    return fields


def create_basic_skill_md(skill_md: Path, skill_name: str):
    """创建基本的 SKILL.md"""
    content = f"""---
name: {skill_name}
description: A skill from ClawHub
license: MIT
compatibility: opencode
metadata:
  audience: all-users
  workflow: general
  timeout: 30000
---

# {skill_name}

This skill was imported from ClawHub.

## Usage

Refer to the scripts in the `scripts/` directory for available commands.
"""
    skill_md.write_text(content, encoding="utf-8")


def ensure_opencode_config():
    """确保 OpenCode 配置文件存在且有 skills 权限配置"""
    OPENCODE_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    OPENCODE_SKILLS_DIR.mkdir(parents=True, exist_ok=True)
    
    if not OPENCODE_CONFIG_FILE.exists():
        # 创建基本配置
        config = {
            "$schema": "https://opencode.ai/config.json",
            "permission": {
                "skill": {
                    "*": "allow"
                }
            }
        }
        OPENCODE_CONFIG_FILE.write_text(json.dumps(config, indent=2), encoding="utf-8")
        log(f"Created {OPENCODE_CONFIG_FILE}", "success")
    else:
        # 检查并更新权限配置
        config = json.loads(OPENCODE_CONFIG_FILE.read_text(encoding="utf-8"))
        
        if "permission" not in config:
            config["permission"] = {}
        if "skill" not in config["permission"]:
            config["permission"]["skill"] = {"*": "allow"}
        elif "*" not in config["permission"]["skill"]:
            # 不覆盖已有的特定权限
            pass
        
        OPENCODE_CONFIG_FILE.write_text(json.dumps(config, indent=2), encoding="utf-8")


def update_opencode_permission(skill_name: str, action: str = "add"):
    """更新 OpenCode 权限配置"""
    if not OPENCODE_CONFIG_FILE.exists():
        return
    
    config = json.loads(OPENCODE_CONFIG_FILE.read_text(encoding="utf-8"))
    
    if "permission" not in config:
        config["permission"] = {}
    if "skill" not in config["permission"]:
        config["permission"]["skill"] = {}
    
    if action == "add":
        config["permission"]["skill"][skill_name] = "allow"
        log(f"Added permission for '{skill_name}'", "success")
    elif action == "remove" and skill_name in config["permission"]["skill"]:
        del config["permission"]["skill"][skill_name]
        log(f"Removed permission for '{skill_name}'", "success")
    
    OPENCODE_CONFIG_FILE.write_text(json.dumps(config, indent=2), encoding="utf-8")


def install_skill(skill_name: str, clone_url: str = None) -> bool:
    """安装 skill 到 OpenCode"""
    ensure_opencode_config()
    
    # 如果没有 clone_url，先搜索
    if not clone_url:
        result = github_api("/search/repositories", {
            "q": f"topic:skill {skill_name}",
            "per_page": "5"
        })
        
        if not result or not result.get("items"):
            log(f"Skill '{skill_name}' not found", "error")
            return False
        
        # 找最匹配的
        skill = None
        for item in result["items"]:
            if item["name"].lower() == skill_name.lower():
                skill = item
                break
        if not skill:
            skill = result["items"][0]
        
        clone_url = skill["clone_url"]
        skill_name = skill["name"]
    
    target_dir = OPENCODE_SKILLS_DIR / skill_name
    
    # 检查是否已安装
    if target_dir.exists():
        log(f"Skill '{skill_name}' already installed. Use 'tot uninstall {skill_name}' first.", "warn")
        return False
    
    log(f"Installing '{skill_name}'...", "info")
    
    # 克隆仓库
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)
        
        try:
            subprocess.run(
                ["git", "clone", "--depth", "1", clone_url, str(tmpdir / skill_name)],
                capture_output=True,
                check=True
            )
        except subprocess.CalledProcessError as e:
            log(f"Failed to clone: {e.stderr.decode()}", "error")
            return False
        
        source_dir = tmpdir / skill_name
        
        # 检查是否是 skills 集合（包含 skills/ 子目录）
        skills_subdir = source_dir / "skills"
        if skills_subdir.exists() and skills_subdir.is_dir():
            # 这是一个 skills 集合，安装每个子 skill
            installed_count = 0
            for skill_subdir in skills_subdir.iterdir():
                if skill_subdir.is_dir() and (skill_subdir / "SKILL.md").exists():
                    sub_skill_name = skill_subdir.name
                    sub_target = OPENCODE_SKILLS_DIR / sub_skill_name
                    
                    if sub_target.exists():
                        log(f"  Skill '{sub_skill_name}' already installed, skipping", "warn")
                        continue
                    
                    # 转换并安装
                    convert_skill_to_opencode(skill_subdir, sub_skill_name)
                    shutil.copytree(skill_subdir, sub_target)
                    
                    # 删除 .git 目录
                    git_dir = sub_target / ".git"
                    if git_dir.exists():
                        shutil.rmtree(git_dir)
                    
                    update_opencode_permission(sub_skill_name, "add")
                    log(f"  ✓ Installed sub-skill '{sub_skill_name}'", "success")
                    installed_count += 1
            
            if installed_count > 0:
                log(f"✓ Installed {installed_count} skills from collection", "success")
            else:
                log("No skills found in collection", "warn")
            return True
        
        # 单个 skill
        convert_skill_to_opencode(source_dir, skill_name)
        shutil.copytree(source_dir, target_dir)
        
        # 删除 .git 目录
        git_dir = target_dir / ".git"
        if git_dir.exists():
            shutil.rmtree(git_dir)
    
    # 更新权限
    update_opencode_permission(skill_name, "add")
    
    log(f"✓ Installed '{skill_name}' to {target_dir}", "success")
    log(f"  You can now use it in OpenCode!", "success")
    
    return True


def list_installed_skills() -> list[str]:
    """列出已安装的 skills"""
    if not OPENCODE_SKILLS_DIR.exists():
        return []
    
    skills = []
    for item in OPENCODE_SKILLS_DIR.iterdir():
        if item.is_dir() and (item / "SKILL.md").exists():
            skills.append(item.name)
    
    return sorted(skills)


def uninstall_skill(skill_name: str) -> bool:
    """卸载 skill"""
    skill_dir = OPENCODE_SKILLS_DIR / skill_name
    
    if not skill_dir.exists():
        log(f"Skill '{skill_name}' is not installed", "error")
        return False
    
    # 删除目录
    shutil.rmtree(skill_dir)
    
    # 更新权限
    update_opencode_permission(skill_name, "remove")
    
    log(f"✓ Uninstalled '{skill_name}'", "success")
    return True


def show_installed():
    """显示已安装的 skills"""
    skills = list_installed_skills()
    
    if not skills:
        log("No skills installed", "info")
        log("Use 'tot search <keyword>' to find skills", "info")
        return
    
    print()
    log(f"Installed skills ({len(skills)}):", "success")
    print()
    
    for skill in skills:
        skill_md = OPENCODE_SKILLS_DIR / skill / "SKILL.md"
        if skill_md.exists():
            content = skill_md.read_text(encoding="utf-8")
            # 提取描述
            desc = ""
            if content.startswith("---"):
                parts = content.split("---", 2)
                if len(parts) >= 2:
                    for line in parts[1].split("\n"):
                        if line.startswith("description:"):
                            desc = line.split(":", 1)[1].strip()
                            break
            
            print(f"  📦 {skill}")
            if desc:
                print(f"     {desc}")
            print()


def main():
    parser = argparse.ArgumentParser(
        description="TOT - ClawHub Skill Manager for OpenCode",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  tot search kubernetes     - Search for Kubernetes-related skills
  tot info kube-medic       - Show details about a skill
  tot install kube-medic    - Install a skill
  tot list                  - List installed skills
  tot uninstall kube-medic  - Uninstall a skill
"""
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # search
    search_parser = subparsers.add_parser("search", help="Search ClawHub skills")
    search_parser.add_argument("keyword", help="Search keyword")
    search_parser.add_argument("-l", "--limit", type=int, default=20, help="Max results")
    
    # info
    info_parser = subparsers.add_parser("info", help="Show skill details")
    info_parser.add_argument("skill", help="Skill name")
    
    # install
    install_parser = subparsers.add_parser("install", help="Install a skill")
    install_parser.add_argument("skill", help="Skill name or clone URL")
    
    # uninstall
    uninstall_parser = subparsers.add_parser("uninstall", help="Uninstall a skill")
    uninstall_parser.add_argument("skill", help="Skill name")
    
    # list
    subparsers.add_parser("list", help="List installed skills")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    if args.command == "search":
        skills = search_skills(args.keyword, args.limit)
        if skills:
            print()
            log(f"Found {len(skills)} skills:", "success")
            print()
            for s in skills:
                print(f"  📦 {s['name']}")
                print(f"     {s['description'][:60]}..." if len(s.get('description', '')) > 60 else f"     {s.get('description', 'No description')}")
                print(f"     ⭐ {s['stars']} | {s['updated']} | {s['url']}")
                print()
        else:
            log("No skills found", "warn")
    
    elif args.command == "info":
        show_skill_info(args.skill)
    
    elif args.command == "install":
        # 检查是否是 URL
        if args.skill.startswith("http"):
            install_skill(args.skill.split("/")[-1].replace(".git", ""), args.skill)
        else:
            install_skill(args.skill)
    
    elif args.command == "uninstall":
        uninstall_skill(args.skill)
    
    elif args.command == "list":
        show_installed()


if __name__ == "__main__":
    main()
