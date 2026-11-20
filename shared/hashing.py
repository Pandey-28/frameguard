import hashlib
from pathlib import Path
from typing import Generator, Tuple, Union

CHUNK_SIZE = 256 * 1024  # 256 KiB = 262,144 bytes


def compute_sha256(data: bytes) -> str:
    """Compute the SHA-256 hexadecimal digest of raw bytes."""
    return hashlib.sha256(data).hexdigest()


def compute_file_sha256(file_path: Union[str, Path], buffer_size: int = CHUNK_SIZE) -> str:
    """Stream-compute the SHA-256 hexadecimal digest of a file on disk."""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(buffer_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def chunk_file(
    file_path: Union[str, Path], chunk_size: int = CHUNK_SIZE
) -> Generator[Tuple[int, bytes, str], None, None]:
    """
    Split a file into fixed-size chunks.
    Yields (chunk_index, chunk_bytes, chunk_sha256).
    """
    idx = 0
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            chunk_hash = compute_sha256(chunk)
            yield idx, chunk, chunk_hash
            idx += 1
