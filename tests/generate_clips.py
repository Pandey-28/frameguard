import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import cv2

from shared.ffmpeg_utils import get_ffmpeg_binary

CLIPS_DIR = Path(__file__).parent / "clips"


def generate_clean_30fps(output_path: Path) -> Path:
    ff = get_ffmpeg_binary()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        ff, "-y",
        "-f", "lavfi",
        "-i", "testsrc2=duration=30:size=640x360:rate=30",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        str(output_path),
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    return output_path


def generate_clean_24fps(output_path: Path) -> Path:
    ff = get_ffmpeg_binary()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        ff, "-y",
        "-f", "lavfi",
        "-i", "testsrc2=duration=30:size=640x360:rate=24",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        str(output_path),
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    return output_path


def generate_invalid_file(output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        # Write random pseudo-header and garbage bytes
        f.write(b"\x00\x00\x00\x18ftypisom\x00\x00\x02\x00" + os.urandom(1024 * 100))
    return output_path


def generate_impaired_clip(
    output_path: Path,
    sidecar_path: Path,
    clean_30fps_path: Optional[Path] = None,
) -> Path:
    ff = get_ffmpeg_binary()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    sidecar_path.parent.mkdir(parents=True, exist_ok=True)

    # 1. Use clean 30fps as source
    source_clean = clean_30fps_path or (output_path.parent / "temp_clean_30.mp4")
    if not source_clean.exists():
        generate_clean_30fps(source_clean)

    # 2. Freeze frames 600..630 using OpenCV
    temp_frozen = output_path.parent / "temp_frozen_600_630.mp4"
    cap = cv2.VideoCapture(str(source_clean))
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fourcc = cv2.VideoWriter_fourcc(*"mp4v")
    out = cv2.VideoWriter(str(temp_frozen), fourcc, fps, (w, h))

    idx = 0
    freeze_frame = None
    while True:
        ret, frame = cap.read()
        if not ret:
            break
        if 600 <= idx <= 630:
            if freeze_frame is None:
                freeze_frame = frame.copy()
            out.write(freeze_frame)
        else:
            out.write(frame)
        idx += 1
    cap.release()
    out.release()

    # 3. Drop frames 100..103 using ffmpeg select filter with fps_mode passthrough
    # to maintain true container PTS gaps
    cmd = [
        ff, "-y",
        "-i", str(temp_frozen),
        "-vf", r"select=not(between(n\,100\,103))",
        "-fps_mode", "passthrough",
        "-c:v", "libx264",
        "-pix_fmt", "yuv420p",
        str(output_path),
    ]
    subprocess.run(cmd, check=True, capture_output=True)

    if temp_frozen.exists():
        temp_frozen.unlink()
    if clean_30fps_path is None and source_clean.exists():
        source_clean.unlink()

    # 4. Generate matching sidecar.json
    # Frame 100 at 30 fps is at timestamp 100 / 30 = 3.3333s
    sidecar_data = {
        "device_id": "sensor_hw_cam0",
        "nominal_fps": 30,
        "dropped_frames": [
            {
                "timestamp": round(100.0 / 30.0, 4),
                "pts": round(100.0 / 30.0, 4),
                "frame_index": 100,
                "count": 4,
                "reason": "buffer_overflow",
            }
        ],
    }
    with open(sidecar_path, "w", encoding="utf-8") as f:
        json.dump(sidecar_data, f, indent=2)

    return output_path


def generate_all_clips(target_dir: Optional[Path] = None):
    dir_path = target_dir or CLIPS_DIR
    dir_path.mkdir(parents=True, exist_ok=True)

    print("Generating clean 30 FPS video (30s)...")
    clean_30 = generate_clean_30fps(dir_path / "clean_30fps.mp4")

    print("Generating clean 24 FPS video (30s)...")
    generate_clean_24fps(dir_path / "clean_24fps.mp4")

    print("Generating invalid video file...")
    generate_invalid_file(dir_path / "invalid.mp4")

    print("Generating impaired video & sidecar (drop 100-103, freeze 600-630)...")
    generate_impaired_clip(
        dir_path / "impaired.mp4",
        dir_path / "impaired_sidecar.json",
        clean_30fps_path=clean_30,
    )
    print("All test clips generated in", dir_path)


if __name__ == "__main__":
    generate_all_clips()
