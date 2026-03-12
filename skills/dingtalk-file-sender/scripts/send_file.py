#!/usr/bin/env python3
"""
钉钉文件发送脚本

使用方法:
  python3 send_file.py <文件路径> [用户ID]

参数:
  <文件路径> - 要发送的文件完整路径（必需）
  [用户ID]   - 接收者的钉钉UserID（可选，默认从环境变量读取）

环境变量（需在/root/.env中配置）:
  DINGTALK_CLIENT_ID      - 钉钉应用的Client ID
  DINGTALK_CLIENT_SECRET  - 钉钉应用的Client Secret
  DINGTALK_AGENT_ID       - 钉钉应用的Agent ID
  DINGTALK_OWNER_USERID   - 默认接收用户的UserID

示例:
  python3 send_file.py /tmp/report.docx
  python3 send_file.py /tmp/data.xlsx 1601434517956472
"""

import os
import sys
import requests
from requests.exceptions import HTTPError
from pathlib import Path


# 配置
CLIENT_ID = os.environ.get('DINGTALK_CLIENT_ID')
CLIENT_SECRET = os.environ.get('DINGTALK_CLIENT_SECRET')
AGENT_ID = os.environ.get('DINGTALK_AGENT_ID')
DEFAULT_USER_ID = os.environ.get('DINGTALK_OWNER_USERID')


def get_access_token():
    """获取钉钉访问令牌"""
    try:
        response = requests.get(
            'https://oapi.dingtalk.com/gettoken',
            params={
                'appkey': CLIENT_ID,
                'appsecret': CLIENT_SECRET
            }
        )
        response.raise_for_status()
        data = response.json()
        
        if data.get('access_token'):
            print('[✓] 获取访问令牌成功')
            return data['access_token']
        else:
            raise Exception(f'获取访问令牌失败: {data}')
    except Exception as error:
        print(f'[✗] 获取访问令牌出错: {error}')
        raise


def upload_file(access_token, file_path):
    """上传媒体文件"""
    try:
        with open(file_path, 'rb') as f:
            files = {'media': f}
            response = requests.post(
                f'https://oapi.dingtalk.com/media/upload?access_token={access_token}&type=file',
                files=files
            )
        
        response.raise_for_status()
        data = response.json()
        
        if data.get('media_id'):
            print(f"[✓] 文件上传成功，media_id: {data['media_id']}")
            return data['media_id']
        else:
            raise Exception(f'文件上传失败: {data}')
    except HTTPError as error:
        print(f'[✗] 文件上传出错: {error}')
        if error.response is not None:
            print(f"    响应数据: {error.response.json()}")
        raise
    except Exception as error:
        print(f'[✗] 文件上传出错: {error}')
        raise


def send_file_message(access_token, user_id, media_id):
    """发送文件消息"""
    try:
        message = {
            'touser': user_id,
            'agentid': AGENT_ID,
            'msgtype': 'file',
            'file': {
                'media_id': media_id
            }
        }
        
        response = requests.post(
            f'https://oapi.dingtalk.com/message/send?access_token={access_token}',
            json=message
        )
        
        response.raise_for_status()
        data = response.json()
        
        if data.get('errcode') == 0:
            print('[✓] 文件消息发送成功')
            return data
        else:
            raise Exception(f'发送消息失败: {data}')
    except HTTPError as error:
        print(f'[✗] 发送消息出错: {error}')
        if error.response is not None:
            print(f"    响应数据: {error.response.json()}")
        raise
    except Exception as error:
        print(f'[✗] 发送消息出错: {error}')
        raise


def validate_environment():
    """验证环境变量"""
    missing = []
    if not CLIENT_ID:
        missing.append('DINGTALK_CLIENT_ID')
    if not CLIENT_SECRET:
        missing.append('DINGTALK_CLIENT_SECRET')
    if not AGENT_ID:
        missing.append('DINGTALK_AGENT_ID')
    
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
        user_id = args[1] if len(args) > 1 else DEFAULT_USER_ID
        
        if not Path(file_path).exists():
            print(f'[✗] 文件不存在: {file_path}')
            sys.exit(1)
        
        if not user_id:
            print('[✗] 未指定用户ID，且环境变量 DINGTALK_OWNER_USERID 未设置')
            sys.exit(1)
        
        # 验证环境变量
        validate_environment()
        
        file_name = Path(file_path).name
        print(f'\n开始发送文件: {file_name}')
        print(f'接收用户: {user_id}')
        print(f'AgentID: {AGENT_ID}\n')
        
        # 1. 获取访问令牌
        access_token = get_access_token()
        
        # 2. 上传文件
        media_id = upload_file(access_token, file_path)
        
        # 3. 发送消息
        send_file_message(access_token, user_id, media_id)
        
        print('\n[✓] 文件发送完成！')
    except Exception as error:
        print(f'\n[✗] 发送失败: {error}')
        sys.exit(1)


if __name__ == '__main__':
    main()
