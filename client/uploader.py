import argparse
import asyncio
import logging
import random
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import httpx

from backend.receipts import verify_receipt
from client.state import LocalUploadManager
from shared.models import Receipt, UploadManifest

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("frameguard-client")


class ResilientUploader:
    def __init__(
        self,
        base_url: str,
        video_path: Path,
        sidecar_path: Optional[Path] = None,
        timeout: float = 12.0,
        max_retries: int = 10,
        stop_after_chunks: Optional[int] = None,
        secret_key: Optional[str] = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.video_path = Path(video_path).resolve()
        self.sidecar_path = Path(sidecar_path).resolve() if sidecar_path else None
        self.timeout = timeout
        self.max_retries = max_retries
        self.stop_after_chunks = stop_after_chunks
        self.secret_key = secret_key

        self.manager = LocalUploadManager.load_or_create(
            self.video_path, self.sidecar_path
        )
        self.upload_id = self.manager.data.upload_id
        self.window_w = 2  # AIMD window: 1 to 4
        self.clean_acks_count = 0
        self.uploaded_in_this_session = 0

    async def init_session(self, client: httpx.AsyncClient) -> str:
        """Create new upload session or resume existing session."""
        if self.upload_id:
            # Check server status to resume
            try:
                resp = await client.get(f"{self.base_url}/v1/uploads/{self.upload_id}")
                if resp.status_code == 200:
                    status_data = resp.json()
                    logger.info(
                        f"Resuming existing session {self.upload_id} (Server has {len(status_data['received_chunks'])}/{status_data['n_chunks']} chunks)"
                    )
                    self.manager.sync_server_acks(status_data["received_chunks"])
                    return self.upload_id
            except Exception as e:
                logger.warning(f"Failed to resume session {self.upload_id}: {e}. Creating new session.")

        # Create new session
        manifest = UploadManifest(
            file_name=self.video_path.name,
            total_bytes=self.manager.data.total_bytes,
            chunk_size=self.manager.data.chunk_size,
            n_chunks=self.manager.data.n_chunks,
            file_sha256=self.manager.data.file_sha256,
            sidecar_sha256=self.manager.data.sidecar_sha256,
        )
        resp = await client.post(
            f"{self.base_url}/v1/uploads",
            json=manifest.model_dump(),
            timeout=10.0,
        )
        resp.raise_for_status()
        session_data = resp.json()
        self.upload_id = session_data["upload_id"]
        self.manager.set_upload_id(self.upload_id)
        logger.info(f"Opened new upload session: {self.upload_id} for {self.video_path.name}")
        return self.upload_id

    async def upload_sidecar(self, client: httpx.AsyncClient) -> None:
        if not self.sidecar_path or not self.sidecar_path.exists():
            return
        sidecar_bytes = self.sidecar_path.read_bytes()
        headers = {
            "Content-Type": "application/json",
            "X-Sidecar-SHA256": self.manager.data.sidecar_sha256 or "",
        }
        resp = await client.put(
            f"{self.base_url}/v1/uploads/{self.upload_id}/sidecar",
            content=sidecar_bytes,
            headers=headers,
            timeout=10.0,
        )
        resp.raise_for_status()
        logger.info("Sidecar metadata uploaded successfully.")

    async def upload(self, client: Optional[httpx.AsyncClient] = None) -> Optional[Receipt]:
        if client is not None:
            return await self._run_upload_loop(client)
        else:
            limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
            async with httpx.AsyncClient(limits=limits, timeout=self.timeout) as auto_client:
                return await self._run_upload_loop(auto_client)

    async def _run_upload_loop(self, client: httpx.AsyncClient) -> Optional[Receipt]:
        await self.init_session(client)

        if self.sidecar_path:
            await self.upload_sidecar(client)

        # Determine initial missing chunks
        n_chunks = self.manager.data.n_chunks
        queue: asyncio.Queue = asyncio.Queue()

        for i in range(n_chunks):
            if not self.manager.is_acked(i):
                queue.put_nowait((i, 0))  # (chunk_idx, attempt)

        if queue.empty():
            logger.info("All chunks already ACKed locally. Requesting completion...")
        else:
            logger.info(
                f"Starting resilient upload: {queue.qsize()}/{n_chunks} chunks remaining. Initial W={self.window_w}"
            )

        crashed = False

        upload_lock = asyncio.Lock()

        # Run adaptive upload worker loop
        async def worker():
            nonlocal crashed
            while True:
                if crashed:
                    break

                async with upload_lock:
                    if self.stop_after_chunks is not None and self.uploaded_in_this_session >= self.stop_after_chunks:
                        crashed = True
                        break
                    try:
                        chunk_idx, attempt = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

                success = await self._upload_single_chunk(client, chunk_idx, attempt)
                if success:
                    self.manager.mark_acked(chunk_idx)
                    async with upload_lock:
                        self.uploaded_in_this_session += 1
                        if self.stop_after_chunks is not None and self.uploaded_in_this_session >= self.stop_after_chunks:
                            crashed = True
                    self.clean_acks_count += 1
                    # AIMD: If clean window batch, increase W
                    if self.clean_acks_count >= self.window_w:
                        old_w = self.window_w
                        self.window_w = min(4, self.window_w + 1)
                        if self.window_w != old_w:
                            logger.debug(f"AIMD Congestion Window increased: W={self.window_w}")
                        self.clean_acks_count = 0
                else:
                    # AIMD: Error/timeout -> cut window
                    self.window_w = max(1, self.window_w // 2)
                    self.clean_acks_count = 0
                    logger.debug(f"AIMD Congestion Window decreased: W={self.window_w}")

                    if attempt < self.max_retries:
                        backoff = min(15.0, (0.2 * (2 ** attempt)) + random.uniform(0.0, 1.0))
                        logger.warning(
                            f"Chunk {chunk_idx} failed (attempt {attempt + 1}/{self.max_retries}). Backing off {backoff:.2f}s..."
                        )
                        await asyncio.sleep(backoff)
                        queue.put_nowait((chunk_idx, attempt + 1))
                    else:
                        raise RuntimeError(
                            f"Chunk {chunk_idx} exceeded max retries ({self.max_retries}). Aborting."
                        )

                queue.task_done()

        while not queue.empty() and not crashed:
            if self.stop_after_chunks is not None and self.uploaded_in_this_session >= self.stop_after_chunks:
                break
            # Launch workers up to window_w
            current_concurrency = self.window_w
            workers = [asyncio.create_task(worker()) for _ in range(current_concurrency)]
            await asyncio.gather(*workers)

        if self.stop_after_chunks is not None and self.uploaded_in_this_session >= self.stop_after_chunks:
            logger.info(f"Simulated client crash/stop after {self.uploaded_in_this_session} chunks.")
            return None

        # Finalize upload
        return await self._finalize_upload(client)

    async def _upload_single_chunk(
        self, client: httpx.AsyncClient, chunk_idx: int, attempt: int
    ) -> bool:
        chunk_bytes = self.manager.get_chunk_bytes(chunk_idx)
        chunk_hash = self.manager.data.chunk_hashes[chunk_idx]
        headers = {
            "Content-Type": "application/octet-stream",
            "X-Chunk-SHA256": chunk_hash,
        }
        url = f"{self.base_url}/v1/uploads/{self.upload_id}/chunks/{chunk_idx}"

        try:
            resp = await client.put(url, content=chunk_bytes, headers=headers)
            if resp.status_code == 200:
                logger.info(
                    f"[ACK] Chunk {chunk_idx + 1}/{self.manager.data.n_chunks} verified ({len(chunk_bytes)} B)"
                )
                return True
            elif resp.status_code == 422:
                logger.warning(f"Chunk {chunk_idx} rejected by server with 422 (corrupt).")
                return False
            elif resp.status_code in (500, 502, 503, 504):
                logger.warning(f"Server error {resp.status_code} for chunk {chunk_idx}.")
                return False
            else:
                logger.warning(f"Unexpected status {resp.status_code} for chunk {chunk_idx}.")
                return False
        except (httpx.TimeoutException, httpx.NetworkError) as e:
            logger.warning(f"Network error on chunk {chunk_idx}: {e}")
            return False

    async def _finalize_upload(self, client: httpx.AsyncClient) -> Receipt:
        logger.info("Requesting completion and signed receipt from server...")
        resp = await client.post(
            f"{self.base_url}/v1/uploads/{self.upload_id}/complete",
            timeout=30.0,
        )

        if resp.status_code == 409:
            body = resp.json()
            missing = body.get("missing", [])
            if missing:
                logger.warning(f"Server reported missing chunks: {missing}. Re-uploading...")
                for idx in missing:
                    self.manager.data.acked_chunks.pop(str(idx), None)
                self.manager.save_atomically()
                return await self.upload()

        resp.raise_for_status()
        receipt_dict = resp.json()
        receipt = Receipt(**receipt_dict)

        # G1 / G3 Verification:
        if receipt.file_sha256.lower() != self.manager.data.file_sha256.lower():
            raise ValueError(
                f"Receipt SHA-256 {receipt.file_sha256} does not match local {self.manager.data.file_sha256}!"
            )

        if not verify_receipt(receipt, secret_key=self.secret_key):
            raise ValueError("Receipt signature verification failed!")

        self.manager.set_receipt(receipt_dict)
        print("DELIVERED")
        logger.info(
            f"Successfully delivered and verified! Receipt Sig: {receipt.receipt_sig[:16]}..."
        )
        return receipt


def main():
    parser = argparse.ArgumentParser(description="Reliable Video Upload CLI")
    parser.add_argument("--file", "-f", required=True, help="Path to 30s video file")
    parser.add_argument("--sidecar", "-s", default=None, help="Optional path to sidecar.json")
    parser.add_argument("--url", "-u", default="http://127.0.0.1:8000", help="Backend API base URL")
    parser.add_argument("--timeout", "-t", type=float, default=12.0, help="Per-chunk timeout (seconds)")
    parser.add_argument("--key", "-k", default=None, help="HMAC secret key for receipt verification (defaults to FRAMEGUARD_SECRET_KEY env var)")
    args = parser.parse_args()

    uploader = ResilientUploader(
        base_url=args.url,
        video_path=Path(args.file),
        sidecar_path=Path(args.sidecar) if args.sidecar else None,
        timeout=args.timeout,
        secret_key=args.key,
    )
    asyncio.run(uploader.upload())


if __name__ == "__main__":
    main()
