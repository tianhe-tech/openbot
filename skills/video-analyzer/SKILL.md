---
name: video-analyzer
description: Analyze video files by extracting keyframes, understanding frame content through vision AI, and answering questions about the video. Use this skill whenever the user mentions video analysis, video understanding, video questions, or has a video file they want to analyze. Supports MP4, AVI, MOV, MKV, WebM, and other common video formats. The skill extracts intelligent keyframes based on scene changes and content differences, then uses vision AI to understand each frame and provides structured JSON output with comprehensive analysis.
---

# Video Analyzer Skill

This skill analyzes video files by extracting keyframes, understanding their content through vision AI, and providing structured answers to user questions about the video.

## When to Use This Skill

Trigger this skill when:
- User asks questions about video content
- User wants to understand what happens in a video
- User mentions analyzing, describing, or summarizing a video
- User provides a video file path and asks anything about it
- User wants to extract information from video frames

## Workflow

### Step 1: Validate Video File

First, check if the video file exists:

```bash
# Check if file exists and get basic info
ls -la <video_path>
file <video_path>
```

Supported formats: MP4, AVI, MOV, MKV, WebM, FLV, WMV, M4V, 3GP, MPEG, MPG

### Step 2: Extract Keyframes

Use the `scripts/extract_frames.py` script to intelligently extract keyframes:

```bash
python /root/.config/opencode/skills/video-analyzer/scripts/extract_frames.py <video_path> [options]
```

**Options:**
- `--output-dir <dir>`: Output directory for frames (default: temp directory)
- `--max-frames <n>`: Maximum frames to extract (default: 20)
- `--min-scene-change <threshold>`: Scene change threshold 0-1 (default: 0.3)
- `--sample-interval <seconds>`: Base sampling interval (default: 2.0)

The script uses scene detection to extract meaningful frames:
- Detects scene changes based on frame differences
- Avoids duplicate or near-duplicate frames
- Prioritizes frames with significant visual changes
- Respects the maximum frame limit

**Output:** JSON with extracted frame paths and timestamps

### Step 3: Analyze Each Frame

For each extracted frame, use the Read tool to view the image and understand its content. The Read tool supports image files and will provide visual understanding.

Read each frame systematically:
- What objects, people, or entities are visible?
- What actions or events are occurring?
- What text, symbols, or graphics are present?
- What is the scene context or environment?

### Step 4: Synthesize and Answer

After analyzing all frames:
1. Combine insights from all frames chronologically
2. Identify the overall narrative or sequence of events
3. Extract specific information requested by the user
4. Consider temporal relationships between frames

### Step 5: Generate Structured Output

Provide output in this JSON structure:

```json
{
  "video_analysis": {
    "summary": "Brief overall summary of the video content",
    "duration_info": {
      "total_frames_analyzed": <number>,
      "extraction_method": "intelligent_keyframe",
      "sampling_strategy": "scene_change_detection"
    },
    "timeline": [
      {
        "frame_number": <int>,
        "timestamp": "<HH:MM:SS.mmm>",
        "description": "What happens in this frame",
        "key_elements": ["element1", "element2"],
        "actions": ["action1", "action2"]
      }
    ],
    "answer": {
      "question": "<user's question>",
      "response": "<detailed answer based on video analysis>",
      "confidence": "<high|medium|low>",
      "evidence": ["frame N shows...", "frame M indicates..."]
    },
    "insights": {
      "main_subjects": ["subject1", "subject2"],
      "activities": ["activity1", "activity2"],
      "environment": "description of setting",
      "notable_changes": "significant changes across frames"
    }
  }
}
```

## Important Considerations

1. **Frame Selection Quality**: The intelligent extraction prioritizes frames with:
   - Scene changes and transitions
   - Significant visual differences from previous frames
   - Key moments of interest

2. **Context Awareness**: When analyzing frames:
   - Consider temporal sequence and causality
   - Note transitions and scene changes
   - Track objects/entities across frames
   - Understand context progression

3. **User Question Alignment**: 
   - Focus the analysis on answering the user's specific question
   - If the question is general, provide comprehensive coverage
   - If specific, prioritize relevant details

4. **Confidence Levels**:
   - **High**: Clear evidence from multiple frames
   - **Medium**: Inferred from visual cues with some uncertainty
   - **Low**: Limited visual evidence or ambiguous content

5. **Error Handling**:
   - If video file doesn't exist, inform user clearly
   - If extraction fails, report the error and suggest alternatives
   - If frames are unclear, note this in confidence level

## Example Usage

**User:** "What happens in the video at /path/to/demo.mp4?"

**Process:**
1. Validate: Check if /path/to/demo.mp4 exists
2. Extract: Run extract_frames.py to get keyframes
3. Analyze: Read each frame image and understand content
4. Synthesize: Combine frame insights into coherent narrative
5. Output: Return structured JSON with analysis and answer

**User:** "In the video /videos/meeting.avi, who is speaking and what are they presenting?"

**Process:**
1. Validate video file
2. Extract frames focusing on speaker and presentation content
3. Analyze frames for people, presentations, text on slides
4. Provide detailed answer about speaker and presentation content
5. Include timeline showing progression of the presentation

## Notes

- For very long videos, the extraction script will sample intelligently to stay within max_frames limit
- Frame extraction requires ffmpeg to be installed
- The skill adapts to different video qualities and lengths
- All analysis respects user privacy - frames are processed locally
