#!/usr/bin/env node
/**
 * 钉钉文件发送脚本
 * 
 * 使用方法:
 *   node send_file.js <文件路径> [用户ID]
 * 
 * 参数:
 *   <文件路径> - 要发送的文件完整路径（必需）
 *   [用户ID]   - 接收者的钉钉UserID（可选，默认从环境变量读取）
 * 
 * 环境变量（需在/root/.env中配置）:
 *   DINGTALK_CLIENT_ID      - 钉钉应用的Client ID
 *   DINGTALK_CLIENT_SECRET  - 钉钉应用的Client Secret
 *   DINGTALK_AGENT_ID       - 钉钉应用的Agent ID
 *   DINGTALK_OWNER_USERID   - 默认接收用户的UserID
 * 
 * 示例:
 *   node send_file.js /tmp/report.docx
 *   node send_file.js /tmp/data.xlsx 1601434517956472
 */

const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs');
const path = require('path');

// 配置
const CLIENT_ID = process.env.DINGTALK_CLIENT_ID;
const CLIENT_SECRET = process.env.DINGTALK_CLIENT_SECRET;
const AGENT_ID = process.env.DINGTALK_AGENT_ID;
const DEFAULT_USER_ID = process.env.DINGTALK_OWNER_USERID;

// 获取钉钉访问令牌
async function getAccessToken() {
  try {
    const response = await axios.get('https://oapi.dingtalk.com/gettoken', {
      params: {
        appkey: CLIENT_ID,
        appsecret: CLIENT_SECRET
      }
    });
    
    if (response.data && response.data.access_token) {
      console.log('[✓] 获取访问令牌成功');
      return response.data.access_token;
    } else {
      throw new Error('获取访问令牌失败: ' + JSON.stringify(response.data));
    }
  } catch (error) {
    console.error('[✗] 获取访问令牌出错:', error.message);
    throw error;
  }
}

// 上传媒体文件
async function uploadFile(accessToken, filePath) {
  try {
    const form = new FormData();
    form.append('media', fs.createReadStream(filePath));
    
    const response = await axios.post(
      `https://oapi.dingtalk.com/media/upload?access_token=${accessToken}&type=file`,
      form,
      {
        headers: {
          ...form.getHeaders()
        }
      }
    );
    
    if (response.data && response.data.media_id) {
      console.log('[✓] 文件上传成功，media_id:', response.data.media_id);
      return response.data.media_id;
    } else {
      throw new Error('文件上传失败: ' + JSON.stringify(response.data));
    }
  } catch (error) {
    console.error('[✗] 文件上传出错:', error.message);
    if (error.response) {
      console.error('    响应数据:', error.response.data);
    }
    throw error;
  }
}

// 发送文件消息
async function sendFileMessage(accessToken, userId, mediaId) {
  try {
    const message = {
      touser: userId,
      agentid: AGENT_ID,
      msgtype: 'file',
      file: {
        media_id: mediaId
      }
    };
    
    const response = await axios.post(
      `https://oapi.dingtalk.com/message/send?access_token=${accessToken}`,
      message
    );
    
    if (response.data && response.data.errcode === 0) {
      console.log('[✓] 文件消息发送成功');
      return response.data;
    } else {
      throw new Error('发送消息失败: ' + JSON.stringify(response.data));
    }
  } catch (error) {
    console.error('[✗] 发送消息出错:', error.message);
    if (error.response) {
      console.error('    响应数据:', error.response.data);
    }
    throw error;
  }
}

// 验证环境变量
function validateEnvironment() {
  const missing = [];
  if (!CLIENT_ID) missing.push('DINGTALK_CLIENT_ID');
  if (!CLIENT_SECRET) missing.push('DINGTALK_CLIENT_SECRET');
  if (!AGENT_ID) missing.push('DINGTALK_AGENT_ID');
  
  if (missing.length > 0) {
    console.error('[✗] 缺少必要的环境变量:');
    missing.forEach(v => console.error(`    - ${v}`));
    console.error('\n请在 /root/.env 文件中配置这些变量');
    process.exit(1);
  }
}

// 主函数
async function main() {
  try {
    // 解析命令行参数
    const args = process.argv.slice(2);
    const filePath = args[0];
    const userId = args[1] || DEFAULT_USER_ID;
    
    if (!filePath) {
      console.error('用法: node send_file.js <文件路径> [用户ID]');
      process.exit(1);
    }
    
    if (!fs.existsSync(filePath)) {
      console.error(`[✗] 文件不存在: ${filePath}`);
      process.exit(1);
    }
    
    if (!userId) {
      console.error('[✗] 未指定用户ID，且环境变量 DINGTALK_OWNER_USERID 未设置');
      process.exit(1);
    }
    
    // 验证环境变量
    validateEnvironment();
    
    const fileName = path.basename(filePath);
    console.log(`\n开始发送文件: ${fileName}`);
    console.log(`接收用户: ${userId}`);
    console.log(`AgentID: ${AGENT_ID}\n`);
    
    // 1. 获取访问令牌
    const accessToken = await getAccessToken();
    
    // 2. 上传文件
    const mediaId = await uploadFile(accessToken, filePath);
    
    // 3. 发送消息
    await sendFileMessage(accessToken, userId, mediaId);
    
    console.log('\n[✓] 文件发送完成！');
  } catch (error) {
    console.error('\n[✗] 发送失败:', error.message);
    process.exit(1);
  }
}

main();
