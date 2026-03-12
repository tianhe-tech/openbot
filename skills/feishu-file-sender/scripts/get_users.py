#!/usr/bin/env python3
"""
获取飞书用户列表

使用方法:
  python3 get_users.py [部门ID]

参数:
  [部门ID] - 部门ID（可选，默认0表示根部门）

环境变量（需在/root/.env中配置）:
  FEISHU_APP_ID       - 飞书应用的App ID
  FEISHU_APP_SECRET   - 飞书应用的App Secret
"""

import os
import sys
import requests
from pathlib import Path

BASE_URL = 'https://open.feishu.cn/open-apis'


def get_tenant_access_token(app_id, app_secret):
    """获取tenant_access_token"""
    url = f'{BASE_URL}/auth/v3/tenant_access_token/internal'
    payload = {'app_id': app_id, 'app_secret': app_secret}
    
    response = requests.post(url, json=payload)
    response.raise_for_status()
    data = response.json()
    
    if data.get('code') == 0:
        return data['tenant_access_token']
    else:
        raise Exception(f"获取token失败: {data.get('msg', data)}")


def get_department_users(access_token, department_id='0'):
    """获取部门下的用户列表"""
    users = []
    page_token = None
    
    while True:
        # 使用旧版API，不需要额外权限
        url = f'{BASE_URL}/contact/v3/users/find_by_department'
        params = {
            'department_id': department_id,
            'page_size': 50
        }
        if page_token:
            params['page_token'] = page_token
        
        headers = {'Authorization': f'Bearer {access_token}'}
        response = requests.get(url, params=params, headers=headers)
        
        # 如果失败，尝试获取会话/群组列表
        if response.status_code != 200:
            return get_chat_members(access_token)
        
        response.raise_for_status()
        data = response.json()
        
        if data.get('code') != 0:
            print(f"[✗] 获取用户失败: {data.get('msg', data)}")
            # 尝试获取群组
            return get_chat_members(access_token)
        
        items = data.get('data', {}).get('userlist', [])
        for user in items:
            users.append({
                'open_id': user.get('open_id'),
                'user_id': user.get('user_id'),
                'name': user.get('name', 'N/A'),
                'mobile': user.get('mobile', 'N/A'),
                'email': user.get('email', 'N/A')
            })
        
        page_token = data.get('data', {}).get('page_token')
        has_more = data.get('data', {}).get('has_more', False)
        
        if not has_more or not page_token:
            break
    
    return users


def get_chat_members(access_token):
    """获取机器人所在的群组列表"""
    # 先获取群组列表
    url = f'{BASE_URL}/im/v1/chats'
    headers = {'Authorization': f'Bearer {access_token}'}
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data.get('code') != 0:
            print(f"[✗] 获取群组失败: {data.get('msg', data)}")
            return []
        
        items = data.get('data', {}).get('items', [])
        
        if items:
            print(f"\n[*] 找到 {len(items)} 个群组，可以使用群组ID发送文件:\n")
            for chat in items:
                print(f"  群组: {chat.get('name', 'N/A')}")
                print(f"  Chat ID: {chat.get('chat_id')}")
                print(f"  ---")
            print()
        
        return []
    except Exception as e:
        print(f"[✗] 获取群组失败: {e}")
        return []


def main():
    try:
        # 加载环境变量
        env_file = Path('/root/.env')
        if env_file.exists():
            with open(env_file) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ[key] = value.strip('"\'')
        
        app_id = os.environ.get('FEISHU_APP_ID')
        app_secret = os.environ.get('FEISHU_APP_SECRET')
        
        if not app_id or not app_secret:
            print('[✗] 请在 /root/.env 中配置 FEISHU_APP_ID 和 FEISHU_APP_SECRET')
            sys.exit(1)
        
        department_id = sys.argv[1] if len(sys.argv) > 1 else '0'
        
        print(f'\n[*] 正在获取部门 {department_id} 的用户列表...\n')
        
        access_token = get_tenant_access_token(app_id, app_secret)
        users = get_department_users(access_token, department_id)
        
        if not users:
            print('[!] 未找到用户（可能是应用可见范围未设置）')
            print('请前往飞书开放平台 -> 应用详情 -> 权限管理 -> 可见范围设置')
            sys.exit(0)
        
        print(f'找到 {len(users)} 个用户:\n')
        print(f'{"Name":<15} {"Open ID":<30} {"User ID":<20} {"Mobile":<15} {"Email"}')
        print('-' * 100)
        
        for user in users:
            print(f"{user['name']:<15} {user['open_id']:<30} {user['user_id']:<20} {user['mobile']:<15} {user['email']}")
        
        print('\n[*] 提示: 您可以将任意用户的 Open ID 添加到 /root/.env:')
        print(f"export FEISHU_DEFAULT_OPEN_ID=\"{users[0]['open_id']}\"")
        
    except Exception as e:
        print(f'\n[✗] 出错: {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()
