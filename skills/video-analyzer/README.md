# Video Analyzer Skill

智能视频分析技能，通过提取关键帧、使用视觉AI理解帧内容，并回答关于视频的问题。

## 功能特性

- 智能关键帧提取（基于场景变化检测）
- 支持多种视频格式（MP4, AVI, MOV, MKV, WebM等）
- 使用视觉AI理解帧内容
- 结构化JSON输出
- 自动视频文件验证

## 依赖要求

### 必需
- Python 3.7+
- ffmpeg（用于视频处理）

### 可选
- OpenCV (cv2) - 当ffmpeg不可用时的备选方案
- NumPy - 配合OpenCV使用

## 安装依赖

### 安装 ffmpeg

**Ubuntu/Debian:**
```bash
sudo apt-get update
sudo apt-get install ffmpeg
```

**macOS:**
```bash
brew install ffmpeg
```

**CentOS/RHEL:**
```bash
sudo yum install ffmpeg
```

### 安装 Python 依赖（可选）

如果要使用OpenCV作为备选方案：
```bash
pip install opencv-python numpy
```

## 使用方法

### 基本用法

用户只需提及视频相关问题，skill会自动触发：

```
"帮我分析这个视频 /path/to/video.mp4 里发生了什么？"
"视频中谁在说话？"
"这个视频的主要内容是什么？"
```

### 命令行使用帧提取脚本

```bash
# 基本用法
python scripts/extract_frames.py /path/to/video.mp4

# 指定输出目录
python scripts/extract_frames.py /path/to/video.mp4 --output-dir ./frames

# 限制最大帧数
python scripts/extract_frames.py /path/to/video.mp4 --max-frames 30

# 调整场景变化阈值
python scripts/extract_frames.py /path/to/video.mp4 --min-scene-change 0.4

# JSON格式输出
python scripts/extract_frames.py /path/to/video.mp4 --json
```

### 参数说明

- `video_path`: 视频文件路径（必需）
- `--output-dir`: 帧输出目录（可选，默认临时目录）
- `--max-frames`: 最大提取帧数（默认: 20）
- `--min-scene-change`: 场景变化阈值 0-1（默认: 0.3）
- `--sample-interval`: 采样间隔秒数（默认: 2.0）
- `--json`: JSON格式输出

## 工作流程

1. **视频验证**: 检查视频文件是否存在且格式支持
2. **关键帧提取**: 使用智能算法提取代表性帧
3. **帧内容分析**: 使用视觉AI理解每帧内容
4. **信息综合**: 结合所有帧的分析结果
5. **生成答案**: 根据用户问题提供结构化输出

## 输出格式

```json
{
  "video_analysis": {
    "summary": "视频整体内容摘要",
    "duration_info": {
      "total_frames_analyzed": 15,
      "extraction_method": "intelligent_keyframe",
      "sampling_strategy": "scene_change_detection"
    },
    "timeline": [
      {
        "frame_number": 1,
        "timestamp": "00:00:00.000",
        "description": "帧内容描述",
        "key_elements": ["元素1", "元素2"],
        "actions": ["动作1", "动作2"]
      }
    ],
    "answer": {
      "question": "用户问题",
      "response": "基于视频分析的详细回答",
      "confidence": "high",
      "evidence": ["帧N显示...", "帧M表明..."]
    },
    "insights": {
      "main_subjects": ["主体1", "主体2"],
      "activities": ["活动1", "活动2"],
      "environment": "环境描述",
      "notable_changes": "帧间显著变化"
    }
  }
}
```

## 智能关键帧提取算法

该技能使用场景变化检测来智能选择关键帧：

1. **场景变化检测**: 计算相邻帧的感知差异
2. **优先级排序**: 优先选择变化显著的帧
3. **数量控制**: 在max_frames限制内选择最具代表性的帧
4. **时间分布**: 确保帧在时间上合理分布

## 示例场景

### 分析演示视频
```
用户: "这个演示视频 /videos/demo.mp4 讲了什么？"
助手会：
1. 验证视频文件
2. 提取关键帧
3. 分析每帧内容（演示幻灯片、演讲者等）
4. 总结演示主题和内容
```

### 监控视频分析
```
用户: "监控录像 /security/cam1.avi 中有人经过吗？"
助手会：
1. 提取关键帧
2. 识别帧中的人物
3. 追踪人物移动轨迹
4. 回答是否有人以及经过的时间点
```

## 注意事项

- 对于长视频，会智能采样以保持在max_frames限制内
- 帧提取质量取决于视频清晰度和编码
- 所有处理在本地进行，保护隐私
- 如果视频文件损坏或不支持，会返回明确的错误信息

## 故障排除

### ffmpeg未安装
```
错误: "ffmpeg not found"
解决: 安装ffmpeg（参见上方安装说明）
```

### 视频文件损坏
```
错误: "Cannot open video"
解决: 使用视频修复工具或尝试其他播放器验证文件
```

### 内存不足
```
错误: 内存错误
解决: 减小max-frames参数值
```
