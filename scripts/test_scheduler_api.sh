#!/bin/bash

# 任务调度系统API测试脚本
# 使用前请确保服务已启动在 http://localhost:8080

BASE_URL="http://localhost:8080"

echo "========================================"
echo "任务调度系统 API 测试"
echo "========================================"
echo ""

# 1. 健康检查
echo "1. 健康检查"
curl -s "$BASE_URL/health" | jq .
echo ""
echo ""

# 2. 提交一个简单任务
echo "2. 提交任务"
TASK_RESPONSE=$(curl -s -X POST "$BASE_URL/api/tasks/submit" \
  -H "Content-Type: application/json" \
  -d '{
    "type": "message",
    "adapter_type": "dingtalk",
    "user_id": "test_user",
    "channel": "test_channel",
    "content": "测试任务：请帮我分析这段代码",
    "priority": 10
  }')
echo "$TASK_RESPONSE" | jq .
TASK_ID=$(echo "$TASK_RESPONSE" | jq -r .task_id)
echo "任务ID: $TASK_ID"
echo ""
echo ""

# 3. 查询任务状态
echo "3. 查询任务状态 (等待2秒)"
sleep 2
curl -s "$BASE_URL/api/tasks/$TASK_ID" | jq .
echo ""
echo ""

# 4. 创建定时任务
echo "4. 创建定时任务 - 每小时执行"
SCHEDULED_RESPONSE=$(curl -s -X POST "$BASE_URL/api/scheduled-tasks" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "测试定时任务",
    "description": "每小时检查一次系统状态",
    "type": "monitoring",
    "cron_expr": "0 0 * * * *",
    "enabled": true,
    "adapter_type": "dingtalk",
    "channel": "test_channel",
    "content": "检查系统状态并报告",
    "agent": "system_monitor"
  }')
echo "$SCHEDULED_RESPONSE" | jq .
SCHEDULED_TASK_ID=$(echo "$SCHEDULED_RESPONSE" | jq -r .task_id)
echo "定时任务ID: $SCHEDULED_TASK_ID"
echo ""
echo ""

# 5. 查看所有定时任务
echo "5. 查看所有定时任务"
curl -s "$BASE_URL/api/scheduled-tasks" | jq .
echo ""
echo ""

# 6. 查看活跃任务
echo "6. 查看活跃任务"
curl -s "$BASE_URL/api/tasks/active" | jq .
echo ""
echo ""

# 7. 查看系统统计
echo "7. 查看系统统计"
curl -s "$BASE_URL/api/tasks/stats" | jq .
echo ""
echo ""

# 8. 禁用定时任务
echo "8. 禁用定时任务"
curl -s -X POST "$BASE_URL/api/scheduled-tasks/disable/$SCHEDULED_TASK_ID" | jq .
echo ""
echo ""

# 9. 查看任务历史
echo "9. 查看任务历史"
curl -s "$BASE_URL/api/tasks/history" | jq .
echo ""
echo ""

# 10. 删除定时任务
echo "10. 删除定时任务"
curl -s -X DELETE "$BASE_URL/api/scheduled-tasks/$SCHEDULED_TASK_ID" | jq .
echo ""
echo ""

echo "========================================"
echo "测试完成！"
echo "========================================"
echo ""
echo "提示："
echo "- 提交的任务ID: $TASK_ID"
echo "- 创建的定时任务ID: $SCHEDULED_TASK_ID (已删除)"
echo ""
echo "更多API文档请查看: docs/SCHEDULER_GUIDE.md"
