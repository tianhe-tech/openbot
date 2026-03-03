"""
批量获取钉钉用户userid
根据手机号批量查询钉钉用户ID

用法:
    python batch_get_userid.py <excel_file>
    
环境变量:
    DINGTALK_CLIENT_ID: 钉钉应用Key
    DINGTALK_CLIENT_SECRET: 钉钉应用Secret
"""

import os
import sys
import pandas as pd
import requests
import time
from typing import Dict, List, Optional
from dotenv import load_dotenv


class DingtalkUserFetcher:
    def __init__(self):
        load_dotenv('.env.ai_daily')
        self.client_id = os.getenv('DINGTALK_CLIENT_ID')
        self.client_secret = os.getenv('DINGTALK_CLIENT_SECRET')
        self.access_token = None
        self.token_expire_time = 0
        self.base_url = "https://api.dingtalk.com"
        
    def _get_access_token(self) -> str:
        """获取访问令牌"""
        if self.access_token and time.time() < self.token_expire_time:
            return self.access_token
        
        if not self.client_id or not self.client_secret:
            raise ValueError("缺少钉钉密钥，请在.env.ai_daily中配置 DINGTALK_CLIENT_ID 和 DINGTALK_CLIENT_SECRET")
        
        url = f"{self.base_url}/v1.0/oauth2/accessToken"
        #url = "https://api.dingtalk.com/v1.0/oauth2/accessToken"
        headers = {"Content-Type": "application/json"}
        data = {
            "appKey": self.client_id,
            "appSecret": self.client_secret
        }
        
        response = requests.post(url, headers=headers, json=data)
        result = response.json()
        print(f"result: {result}")

        if response.status_code > 300:
            raise Exception(f"获取access_token失败: {result.get('errmsg', '未知错误')}")
        
        self.access_token = result.get('accessToken')
        self.token_expire_time = time.time() + 7000
       
        print(f"self.access_token: {self.access_token}")
        return self.access_token
    
    def get_userid_by_phones(self, phones: List[str]) -> Dict[str, Optional[str]]:
        """
        批量根据手机号查询用户ID
        
        Args:
            phones: 手机号列表
            
        Returns:
            {手机号: userid} 的字典
        """
        access_token = self._get_access_token()
        
        phone_to_userid = {}
        url = f"https://oapi.dingtalk.com/topapi/v2/user/getbymobile?access_token={access_token}"
        
        for phone in phones:
            data = {
                "mobile": phone,
                "support_exclusive_account_search": True
            }
            
            try:
                response = requests.post(url, json=data)
                result = response.json()
                
                if result.get('errcode') == 0 and result.get('result', {}).get('userid'):
                    phone_to_userid[phone] = result['result']['userid']
                else:
                    phone_to_userid[phone] = None
                    
                time.sleep(0.05)
            except Exception as e:
                print(f"  查询 {phone} 失败: {e}")
                phone_to_userid[phone] = None
        
        return phone_to_userid
    
    def process_excel(self, input_file: str, output_file: str = None) -> pd.DataFrame:
        """
        处理Excel文件，批量获取userid
        
        Args:
            input_file: 输入Excel文件路径
            output_file: 输出Excel文件路径（可选）
            
        Returns:
            包含userid的DataFrame
        """
        print(f"📂 读取文件: {input_file}")
        
        try:
            df = pd.read_csv(input_file, sep='\t')
        except Exception:
            try:
                df = pd.read_excel(input_file, engine='openpyxl')
            except Exception:
                df = pd.read_excel(input_file, engine='xlrd')
        
        if '手机号' not in df.columns:
            raise ValueError("Excel文件必须包含'手机号'列")
        
        phones = df['手机号'].astype(str).tolist()
        unique_phones = list(set(phones))
        
        print(f"📱 共 {len(phones)} 条记录，{len(unique_phones)} 个唯一手机号")
        
        phone_to_userid = {}
        batch_size = 50
        
        for i in range(0, len(unique_phones), batch_size):
            batch = unique_phones[i:i+batch_size]
            print(f"  查询 {i+1}-{min(i+batch_size, len(unique_phones))}...")
            
            try:
                result = self.get_userid_by_phones(batch)
                phone_to_userid.update(result)
                time.sleep(0.1)
            except Exception as e:
                print(f"  ⚠️ 批次查询失败: {e}")
                for p in batch:
                    phone_to_userid[p] = None
        
        df['userid'] = df['手机号'].astype(str).map(phone_to_userid)
        
        found_count = df['userid'].notna().sum()
        print(f"✅ 查询完成: {found_count}/{len(df)} 条记录找到userid")
        
        if output_file:
            df.to_excel(output_file, index=False)
            print(f"💾 结果已保存到: {output_file}")
        else:
            output_file = input_file.replace('.xlsx', '_with_userid.xlsx')
            df.to_excel(output_file, index=False)
            print(f"💾 结果已保存到: {output_file}")
        
        return df


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    if not os.path.exists(input_file):
        print(f"❌ 文件不存在: {input_file}")
        sys.exit(1)
    
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    fetcher = DingtalkUserFetcher()
    
    try:
        df = fetcher.process_excel(input_file, output_file)
        print("\n📋 预览结果:")
        print(df.head(10).to_string())
    except Exception as e:
        print(f"❌ 错误: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
