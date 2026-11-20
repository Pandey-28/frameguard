import os
import shutil
from pathlib import Path
from typing import Optional

KNOWN_FFMPEG_PATHS = [
    Path(r"C:\Users\ASUS\AppData\Local\Microsoft\WinGet\Packages\Gyan.FFmpeg_Microsoft.Winget.Source_8wekyb3d8bbwe\ffmpeg-9.0.2-full_build\bin"),
    Path(r"C:\ffmpeg\bin"),
    Path(r"C:\tools\ffmpeg\bin"),
]


def ensure_ffmpeg_in_path() -> None:
    """Ensure ffmpeg and ffprobe are in os.environ['PATH']."""
    if shutil.which("ffmpeg") and shutil.which("ffprobe"):
        return

    for p in KNOWN_FFMPEG_PATHS:
        if p.exists() and (p / "ffmpeg.exe").exists():
            os.environ["PATH"] = str(p) + os.pathsep + os.environ.get("PATH", "")
            break


def get_ffmpeg_binary() -> str:
    ensure_ffmpeg_in_path()
    path = shutil.which("ffmpeg")
    if not path:
        raise RuntimeError("ffmpeg executable not found in PATH or known install locations.")
    return path


def get_ffprobe_binary() -> str:
    ensure_ffmpeg_in_path()
    path = shutil.which("ffprobe")
    if not path:
        raise RuntimeError("ffprobe executable not found in PATH or known install locations.")
    return path
