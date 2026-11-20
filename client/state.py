import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field

from shared.hashing import CHUNK_SIZE, compute_file_sha256, compute_sha256


class LocalStateData(BaseModel):
    file_path: str
    sidecar_path: Optional[str] = None
    total_bytes: int
    chunk_size: int
    n_chunks: int
    file_sha256: str
    sidecar_sha256: Optional[str] = None
    upload_id: Optional[str] = None
    chunk_hashes: List[str] = Field(default_factory=list)
    acked_chunks: Dict[str, bool] = Field(default_factory=dict)
    receipt: Optional[Dict[str, Any]] = None


class LocalUploadManager:
    def __init__(self, state_file: Path, data: LocalStateData):
        self.state_file = Path(state_file)
        self.data = data

    @classmethod
    def load_or_create(
        cls,
        video_path: Path,
        sidecar_path: Optional[Path] = None,
        state_file: Optional[Path] = None,
        chunk_size: int = CHUNK_SIZE,
    ) -> "LocalUploadManager":
        video_path = Path(video_path).resolve()
        if not video_path.exists():
            raise FileNotFoundError(f"Video file not found: {video_path}")

        if state_file is None:
            state_file = video_path.with_name(f"{video_path.name}.manifest.json")
        else:
            state_file = Path(state_file).resolve()

        if state_file.exists():
            try:
                with open(state_file, "r", encoding="utf-8") as f:
                    content = json.load(f)
                loaded_data = LocalStateData(**content)
                # Verify it matches target file
                if loaded_data.file_path == str(video_path):
                    return cls(state_file, loaded_data)
            except Exception:
                # If corrupted or mismatched, recompute
                pass

        # Compute single-pass hashes
        chunk_hashes: List[str] = []
        hasher = compute_sha256  # helper
        import hashlib
        whole_hasher = hashlib.sha256()
        total_bytes = 0

        with open(video_path, "rb") as f:
            while True:
                buf = f.read(chunk_size)
                if not buf:
                    break
                whole_hasher.update(buf)
                chunk_hashes.append(hasher(buf))
                total_bytes += len(buf)

        whole_sha256 = whole_hasher.hexdigest()
        n_chunks = len(chunk_hashes)

        sidecar_sha = None
        sidecar_str = None
        if sidecar_path and Path(sidecar_path).exists():
            sidecar_path = Path(sidecar_path).resolve()
            sidecar_str = str(sidecar_path)
            sidecar_sha = compute_file_sha256(sidecar_path)

        data = LocalStateData(
            file_path=str(video_path),
            sidecar_path=sidecar_str,
            total_bytes=total_bytes,
            chunk_size=chunk_size,
            n_chunks=n_chunks,
            file_sha256=whole_sha256,
            sidecar_sha256=sidecar_sha,
            upload_id=None,
            chunk_hashes=chunk_hashes,
            acked_chunks={},
            receipt=None,
        )

        manager = cls(state_file, data)
        manager.save_atomically()
        return manager

    def save_atomically(self) -> None:
        """Write to temp file and atomic rename for crash safety."""
        temp_file = self.state_file.with_suffix(".tmp")
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(temp_file, "w", encoding="utf-8") as f:
            f.write(self.data.model_dump_json(indent=2))
        temp_file.replace(self.state_file)

    def set_upload_id(self, upload_id: str) -> None:
        self.data.upload_id = upload_id
        self.save_atomically()

    def mark_acked(self, chunk_idx: int) -> None:
        self.data.acked_chunks[str(chunk_idx)] = True
        self.save_atomically()

    def is_acked(self, chunk_idx: int) -> bool:
        return self.data.acked_chunks.get(str(chunk_idx), False)

    def sync_server_acks(self, received_indices: List[int]) -> None:
        for idx in received_indices:
            self.data.acked_chunks[str(idx)] = True
        self.save_atomically()

    def set_receipt(self, receipt_dict: Dict[str, Any]) -> None:
        self.data.receipt = receipt_dict
        self.save_atomically()

    def get_chunk_bytes(self, chunk_idx: int) -> bytes:
        offset = chunk_idx * self.data.chunk_size
        with open(self.data.file_path, "rb") as f:
            f.seek(offset)
            return f.read(self.data.chunk_size)
