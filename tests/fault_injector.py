import asyncio
import os
import random
import time
from typing import Callable, Dict, Optional
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


class FaultProfile:
    def __init__(self):
        self.enabled: bool = False
        self.latency_seconds: float = 0.0
        self.latency_chunk_indices: list[int] = []
        self.disconnect_active: bool = False
        self.disconnect_start_time: float = 0.0
        self.disconnect_duration_sec: float = 0.0
        self.bandwidth_kbps: float = 0.0  # 0 means unthrottled
        self.bit_flip_chunk_indices: list[int] = []
        self.fail_chunk_indices: Dict[int, int] = {}  # chunk_index -> status_code

    def reset(self):
        self.enabled = False
        self.latency_seconds = 0.0
        self.latency_chunk_indices = []
        self.disconnect_active = False
        self.disconnect_start_time = 0.0
        self.disconnect_duration_sec = 0.0
        self.bandwidth_kbps = 0.0
        self.bit_flip_chunk_indices = []
        self.fail_chunk_indices = {}


ACTIVE_FAULT_PROFILE = FaultProfile()


class FaultInjectionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        profile = ACTIVE_FAULT_PROFILE
        if not profile.enabled and os.environ.get("FRAMEGUARD_FAULT_INJECTION") != "1":
            return await call_next(request)

        path = request.url.path
        now = time.time()

        # Check disconnect window
        if profile.disconnect_active:
            if now < profile.disconnect_start_time + profile.disconnect_duration_sec:
                return JSONResponse(
                    status_code=503,
                    content={"detail": "Network fault injection: service temporarily disconnected"},
                )
            else:
                profile.disconnect_active = False

        # Check chunk endpoints
        if "/chunks/" in path and request.method == "PUT":
            try:
                parts = path.rstrip("/").split("/")
                chunk_idx = int(parts[-1])
            except (ValueError, IndexError):
                chunk_idx = -1

            # Check explicit fail status code for chunk
            if chunk_idx in profile.fail_chunk_indices:
                code = profile.fail_chunk_indices[chunk_idx]
                return JSONResponse(
                    status_code=code,
                    content={"detail": f"Network fault injection: forced status {code}"},
                )

            # Check latency injection
            if profile.latency_seconds > 0:
                if not profile.latency_chunk_indices or chunk_idx in profile.latency_chunk_indices:
                    await asyncio.sleep(profile.latency_seconds)

            # Check bandwidth throttling
            if profile.bandwidth_kbps > 0:
                body = await request.body()
                chunk_size_bytes = len(body)
                sleep_sec = chunk_size_bytes / (profile.bandwidth_kbps * 1024.0)
                await asyncio.sleep(sleep_sec)

            # Check bit flip corruption
            if chunk_idx in profile.bit_flip_chunk_indices:
                body = await request.body()
                if len(body) > 0:
                    corrupted = bytearray(body)
                    corrupted[0] ^= 0xFF
                    # We override receive to return corrupted body
                    async def custom_receive():
                        return {"type": "http.request", "body": bytes(corrupted), "more_body": False}
                    request._receive = custom_receive

        return await call_next(request)
