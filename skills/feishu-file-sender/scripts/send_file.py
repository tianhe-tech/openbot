#!/usr/bin/env python3
"""
飞书文件发送脚本

使用方法:
  python3 send_file.py <文件路径> [用户ID]

参数:
  <文件路径> - 要发送的文件完整路径（必需）
  [用户ID]   - 接收者的飞书Open ID（可选，默认从环境变量读取）

环境变量（需在/root/.env中配置）:
  FEISHU_APP_ID           - 飞书应用的App ID
  FEISHU_APP_SECRET       - 飞书应用的App Secret
  FEISHU_DEFAULT_OPEN_ID  - 默认接收用户的Open ID

示例:
  python3 send_file.py /tmp/report.docx
  python3 send_file.py /tmp/data.xlsx ou_1234567890abcdef
"""

import os
import sys
import requests
from requests.exceptions import HTTPError
from pathlib import Path


# 配置
APP_ID = os.environ.get('FEISHU_APP_ID')
APP_SECRET = os.environ.get('FEISHU_APP_SECRET')
DEFAULT_OPEN_ID = os.environ.get('FEISHU_DEFAULT_OPEN_ID')

# 飞书API基础URL
BASE_URL = 'https://open.feishu.cn/open-apis'


def get_tenant_access_token():
    """获取飞书tenant_access_token"""
    try:
        url = f'{BASE_URL}/auth/v3/tenant_access_token/internal'
        payload = {
            'app_id': APP_ID,
            'app_secret': APP_SECRET
        }
        
        response = requests.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        
        if data.get('code') == 0 and data.get('tenant_access_token'):
            print('[✓] 获取访问令牌成功')
            return data['tenant_access_token']
        else:
            raise Exception(f"获取访问令牌失败: {data.get('msg', data)}")
    except Exception as error:
        print(f'[✗] 获取访问令牌出错: {error}')
        raise


def upload_file(access_token, file_path):
    """上传文件到飞书"""
    try:
        url = f'{BASE_URL}/im/v1/files'
        
        with open(file_path, 'rb') as f:
            files = {
                'file': (Path(file_path).name, f, 'application/octet-stream')
            }
            data = {
                'file_type': 'stream',
                'file_name': Path(file_path).name
            }
            headers = {
                'Authorization': f'Bearer {access_token}'
            }
            
            response = requests.post(url, files=files, data=data, headers=headers)
        
        response.raise_for_status()
        data = response.json()
        
        if data.get('code') == 0 and data.get('data', {}).get('file_key'):
            file_key = data['data']['file_key']
            print(f"[✓] 文件上传成功，file_key: {file_key}")
            return file_key
        else:
            raise Exception(f"文件上传失败: {data.get('msg', data)}")
    except HTTPError as error:
        print(f'[✗] 文件上传出错: {error}')
        if error.response is not None:
            print(f"    响应数据: {error.response.text}")
        raise
    except Exception as error:
        print(f'[✗] 文件上传出错: {error}')
        raise


def send_file_message(access_token, open_id, file_key, file_name):
    """发送文件消息"""
    try:
        url = f'{BASE_URL}/im/v1/messages?receive_id_type=open_id'
        
        payload = {
            'receive_id': open_id,
            'msg_type': 'file',
            'content': {
                'file_key': file_key,
                'file_name': file_name
            }
        }
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data.get('code') == 0:
            print('[✓] 文件消息发送成功')
            return data
        else:
            raise Exception(f"发送消息失败: {data.get('msg', data)}")
    except HTTPError as error:
        print(f'[✗] 发送消息出错: {error}')
        if error.response is not None:
            print(f"    响应数据: {error.response.text}")
        raise
    except Exception as error:
        print(f'[✗] 发送消息出错: {error}')
        raise


def validate_environment():
    """验证环境变量"""
    missing = []
    if not APP_ID:
        missing.append('FEISHU_APP_ID')
    if not APP_SECRET:
        missing.append('FEISHU_APP_SECRET')
    
    if missing:
        print('[✗] 缺少必要的环境变量:')
        for v in missing:
            print(f'    - {v}')
        print('\n请在 /root/.env 文件中配置这些变量')
        sys.exit(1)


def main():
    try:
        # 解析命令行参数
        args = sys.argv[1:]
        if not args:
            print('用法: python3 send_file.py <文件路径> [用户ID]')
            sys.exit(1)
        
        file_path = args[0]
        open_id = args[1] if len(args) > 1 else DEFAULT_OPEN_ID
        
        if not Path(file_path).exists():
            print(f'[✗] 文件不存在: {file_path}')
            sys.exit(1)
        
        if not open_id:
            print('[✗] 未指定用户ID，且环境变量 FEISHU_DEFAULT_OPEN_ID 未设置')
            sys.exit(1)
        
        # 验证环境变量
        validate_environment()
        
        file_name = Path(file_path).name
        print(f'\n开始发送文件: {file_name}')
        print(f'接收用户: {open_id}\n')
        
        # 1. 获取访问令牌
        access_token = get_tenant_access_token()
        
        # 2. 上传文件
        file_key = upload_file(access_token, file_path)
        
        # 3. 发送消息
        send_file_message(access_token, open_id, file_key, file_name)
        
        print('\n[✓] 文件发送完成！')
    except Exception as error:
        print(f'\n[✗] 发送失败: {error}')
        sys.exit(1)


if __name__ == '__main__':
    main()
