import hashlib
import os
import shutil
from pathlib import Path
from typing import Optional, Tuple

DATA_DIR = Path(os.environ.get("FRAMEGUARD_DATA_DIR", "./data"))
QUARANTINE_DIR = DATA_DIR / "quarantine"


def get_upload_dir(upload_id: str) -> Path:
    return DATA_DIR / upload_id


def get_chunks_dir(upload_id: str) -> Path:
    return get_upload_dir(upload_id) / "chunks"


def get_chunk_path(upload_id: str, idx: int) -> Path:
    return get_chunks_dir(upload_id) / f"{idx}.part"


def get_assembled_video_path(upload_id: str) -> Path:
    return get_upload_dir(upload_id) / "video.mp4"


def get_sidecar_path(upload_id: str) -> Path:
    return get_upload_dir(upload_id) / "sidecar.json"


def save_chunk_bytes(upload_id: str, idx: int, data: bytes) -> Path:
    chunks_dir = get_chunks_dir(upload_id)
    chunks_dir.mkdir(parents=True, exist_ok=True)
    chunk_path = get_chunk_path(upload_id, idx)
    temp_path = chunk_path.with_suffix(".tmp")
    with open(temp_path, "wb") as f:
        f.write(data)
    temp_path.replace(chunk_path)
    return chunk_path


def save_sidecar_bytes(upload_id: str, data: bytes) -> Path:
    upload_dir = get_upload_dir(upload_id)
    upload_dir.mkdir(parents=True, exist_ok=True)
    sidecar_path = get_sidecar_path(upload_id)
    with open(sidecar_path, "wb") as f:
        f.write(data)
    return sidecar_path


def assemble_chunks(
    upload_id: str, n_chunks: int, buffer_size: int = 256 * 1024
) -> Tuple[Path, str, int]:
    """
    Stream-assemble chunks 0 .. n_chunks - 1 into video.mp4 while computing
    a single-pass SHA-256 and total bytes.
    Returns (video_path, computed_sha256, total_bytes).
    """
    upload_dir = get_upload_dir(upload_id)
    upload_dir.mkdir(parents=True, exist_ok=True)
    video_path = get_assembled_video_path(upload_id)
    temp_video_path = video_path.with_suffix(".assembling")

    hasher = hashlib.sha256()
    total_bytes = 0

    with open(temp_video_path, "wb") as outfile:
        for idx in range(n_chunks):
            chunk_file = get_chunk_path(upload_id, idx)
            if not chunk_file.exists():
                if temp_video_path.exists():
                    temp_video_path.unlink()
                raise FileNotFoundError(f"Missing chunk file: {chunk_file}")

            with open(chunk_file, "rb") as infile:
                while True:
                    buf = infile.read(buffer_size)
                    if not buf:
                        break
                    hasher.update(buf)
                    total_bytes += len(buf)
                    outfile.write(buf)

    if video_path.exists():
        video_path.unlink()
    temp_video_path.replace(video_path)

    computed_sha256 = hasher.hexdigest()
    return video_path, computed_sha256, total_bytes


def quarantine_upload(upload_id: str) -> Path:
    """Move an upload directory to the quarantine folder."""
    upload_dir = get_upload_dir(upload_id)
    target_dir = QUARANTINE_DIR / upload_id
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    if target_dir.exists():
        shutil.rmtree(target_dir)
    if upload_dir.exists():
        shutil.move(str(upload_dir), str(target_dir))
    return target_dir
