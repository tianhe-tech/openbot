#!/usr/bin/env python3
"""
Intelligent video keyframe extraction script.

Extracts keyframes from video files using scene change detection.
Supports MP4, AVI, MOV, MKV, WebM, and other common formats.
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import List, Dict, Any, Optional
import hashlib


def check_ffmpeg() -> bool:
    """Check if ffmpeg is available."""
    try:
        subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


def get_video_info(video_path: str) -> Dict[str, Any]:
    """Get video metadata using ffprobe or OpenCV fallback."""
    cmd = [
        'ffprobe', '-v', 'quiet', '-print_format', 'json',
        '-show_format', '-show_streams', video_path
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        return json.loads(result.stdout)
    except Exception:
        try:
            import cv2
            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                raise RuntimeError(f"Cannot open video: {video_path}")
            
            fps = cap.get(cv2.CAP_PROP_FPS)
            width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
            height = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
            duration = cap.get(cv2.CAP_PROP_FRAME_COUNT) / fps if fps > 0 else 0
            
            cap.release()
            
            return {
                "streams": [{
                    "codec_type": "video",
                    "width": width,
                    "height": height,
                    "r_frame_rate": f"{int(fps)}/1",
                    "avg_frame_rate": f"{int(fps)}/1"
                }],
                "format": {
                    "filename": video_path,
                    "duration": str(duration),
                    "size": "0"
                }
            }
        except ImportError:
            raise RuntimeError("Neither ffprobe nor OpenCV is available")


def extract_all_frames(video_path: str, output_dir: str, fps: float = 1.0) -> List[str]:
    """Extract frames at specified FPS for analysis using ffmpeg or OpenCV fallback."""
    output_pattern = os.path.join(output_dir, 'frame_%04d.jpg')
    
    cmd = [
        'ffmpeg', '-i', video_path,
        '-vf', f'fps={fps}',
        '-q:v', '2',
        '-y', output_pattern
    ]
    
    try:
        subprocess.run(cmd, capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        try:
            import cv2
            cap = cv2.VideoCapture(video_path)
            if not cap.isOpened():
                raise RuntimeError(f"Cannot open video: {video_path}")
            
            video_fps = cap.get(cv2.CAP_PROP_FPS)
            if video_fps <= 0:
                video_fps = fps
            
            frame_interval = int(round(video_fps / fps))
            if frame_interval < 1:
                frame_interval = 1
            
            frame_count = 0
            saved_count = 0
            
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                
                if frame_count % frame_interval == 0:
                    frame_path = os.path.join(output_dir, f'frame_{saved_count:04d}.jpg')
                    cv2.imwrite(frame_path, frame)
                    saved_count += 1
                
                frame_count += 1
            
            cap.release()
        except ImportError:
            raise RuntimeError("Neither ffmpeg nor OpenCV is available")
    
    frames = sorted([f for f in os.listdir(output_dir) if f.startswith('frame_') and f.endswith('.jpg')])
    return [os.path.join(output_dir, f) for f in frames]


def calculate_frame_difference(frame1_path: str, frame2_path: str) -> float:
    """Calculate perceptual difference between two frames using ffmpeg."""
    cmd = [
        'ffmpeg', '-i', frame1_path, '-i', frame2_path,
        '-lavfi', 'ssim', '-f', 'null', '-'
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        output = result.stderr
        
        if 'SSIM' in output:
            import re
            match = re.search(r'All:([0-9.]+)', output)
            if match:
                ssim = float(match.group(1))
                return 1.0 - ssim
        
        return 0.0
    except Exception:
        return 0.0


def select_keyframes_intelligent(
    frames: List[str],
    max_frames: int,
    min_scene_change: float = 0.3
) -> List[Dict[str, Any]]:
    """
    Intelligently select keyframes based on scene changes.
    
    Strategy:
    1. Always include first frame
    2. Detect scene changes based on frame differences
    3. Prioritize frames with significant changes
    4. Respect max_frames limit
    """
    if not frames:
        return []
    
    selected = []
    
    if len(frames) <= max_frames:
        for i, frame_path in enumerate(frames):
            selected.append({
                'frame_number': i,
                'frame_path': frame_path,
                'timestamp': calculate_timestamp(i, len(frames)),
                'change_score': 1.0 if i == 0 else 0.5
            })
        return selected
    
    scores = []
    
    scores.append({
        'frame_number': 0,
        'frame_path': frames[0],
        'change_score': 1.0
    })
    
    for i in range(1, len(frames)):
        diff = calculate_frame_difference(frames[i-1], frames[i])
        scores.append({
            'frame_number': i,
            'frame_path': frames[i],
            'change_score': diff
        })
    
    scores.sort(key=lambda x: x['change_score'], reverse=True)
    
    top_frames = scores[:max_frames]
    top_frames.sort(key=lambda x: x['frame_number'])
    
    for frame in top_frames:
        frame['timestamp'] = calculate_timestamp(frame['frame_number'], len(frames))
    
    return top_frames


def calculate_timestamp(frame_number: int, total_frames: int) -> str:
    """Calculate timestamp for a frame (approximate)."""
    seconds_per_frame = 2.0
    total_seconds = frame_number * seconds_per_frame
    
    hours = int(total_seconds // 3600)
    minutes = int((total_seconds % 3600) // 60)
    seconds = total_seconds % 60
    
    return f"{hours:02d}:{minutes:02d}:{seconds:06.3f}"


def extract_keyframes_opencv(video_path: str, output_dir: str, max_frames: int, 
                             min_scene_change: float) -> List[Dict[str, Any]]:
    """Fallback extraction using OpenCV if ffmpeg fails."""
    try:
        import cv2
        import numpy as np
    except ImportError:
        raise RuntimeError("Neither ffmpeg nor OpenCV is available")
    
    cap = cv2.VideoCapture(video_path)
    
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open video: {video_path}")
    
    fps = cap.get(cv2.CAP_PROP_FPS)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    
    if fps <= 0:
        fps = 24.0
    
    frames_data = []
    prev_frame = None
    frame_idx = 0
    
    sample_interval = max(1, total_frames // (max_frames * 3))
    
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        
        if frame_idx % sample_interval != 0:
            frame_idx += 1
            continue
        
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        gray = cv2.resize(gray, (160, 90))
        
        change_score = 1.0 if prev_frame is None else 0.0
        
        if prev_frame is not None:
            diff = cv2.absdiff(gray, prev_frame)
            change_score = np.mean(diff) / 255.0
        
        frame_path = os.path.join(output_dir, f'frame_{len(frames_data):04d}.jpg')
        cv2.imwrite(frame_path, frame)
        
        timestamp_sec = frame_idx / fps
        hours = int(timestamp_sec // 3600)
        minutes = int((timestamp_sec % 3600) // 60)
        seconds = timestamp_sec % 60
        timestamp = f"{hours:02d}:{minutes:02d}:{seconds:06.3f}"
        
        frames_data.append({
            'frame_number': frame_idx,
            'frame_path': frame_path,
            'change_score': change_score,
            'timestamp': timestamp
        })
        
        prev_frame = gray
        frame_idx += 1
    
    cap.release()
    
    if len(frames_data) > max_frames:
        frames_data.sort(key=lambda x: x['change_score'], reverse=True)
        frames_data = frames_data[:max_frames]
        frames_data.sort(key=lambda x: x['frame_number'])
    
    return frames_data


def main():
    parser = argparse.ArgumentParser(
        description='Extract keyframes from video using intelligent scene detection'
    )
    parser.add_argument('video_path', help='Path to video file')
    parser.add_argument('--output-dir', help='Output directory for frames', default=None)
    parser.add_argument('--max-frames', type=int, default=20, help='Maximum frames to extract')
    parser.add_argument('--min-scene-change', type=float, default=0.3, 
                        help='Minimum scene change threshold (0-1)')
    parser.add_argument('--sample-interval', type=float, default=2.0,
                        help='Base sampling interval in seconds')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.video_path):
        print(json.dumps({
            'success': False,
            'error': f'Video file not found: {args.video_path}'
        }))
        sys.exit(1)
    
    if args.output_dir:
        output_dir = args.output_dir
        os.makedirs(output_dir, exist_ok=True)
    else:
        output_dir = tempfile.mkdtemp(prefix='video_frames_')
    
    try:
        video_info = get_video_info(args.video_path)
        
        duration = float(video_info.get('format', {}).get('duration', 0))
        
        use_ffmpeg = check_ffmpeg()
        
        if use_ffmpeg:
            fps = 1.0 / args.sample_interval
            all_frames = extract_all_frames(args.video_path, output_dir, fps)
            keyframes = select_keyframes_intelligent(
                all_frames, args.max_frames, args.min_scene_change
            )
        else:
            keyframes = extract_keyframes_opencv(
                args.video_path, output_dir, args.max_frames, args.min_scene_change
            )
        
        result = {
            'success': True,
            'video_path': args.video_path,
            'output_dir': output_dir,
            'total_frames_analyzed': len(keyframes),
            'video_duration_seconds': duration,
            'extraction_method': 'intelligent_keyframe',
            'frames': [
                {
                    'frame_number': kf['frame_number'],
                    'frame_path': kf['frame_path'],
                    'timestamp': kf['timestamp'],
                    'change_score': round(kf['change_score'], 4)
                }
                for kf in keyframes
            ]
        }
        
        if args.json:
            print(json.dumps(result, indent=2))
        else:
            print(f"Successfully extracted {len(keyframes)} keyframes")
            print(f"Output directory: {output_dir}")
            for kf in keyframes:
                print(f"  Frame {kf['frame_number']}: {kf['timestamp']} (score: {kf['change_score']:.3f})")
        
    except Exception as e:
        error_result = {
            'success': False,
            'error': str(e),
            'video_path': args.video_path
        }
        if args.json:
            print(json.dumps(error_result, indent=2))
        else:
            print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
