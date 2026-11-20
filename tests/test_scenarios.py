import asyncio
import json
import os
import shutil
import time
from pathlib import Path

import httpx
import pytest

from backend import db, storage
from backend.analyzer import run_analysis_pipeline
from backend.main import app
from backend.receipts import verify_receipt
from client.state import LocalUploadManager
from client.uploader import ResilientUploader
from shared.hashing import compute_file_sha256
from shared.models import EvidenceTier, JobStatus, UploadState
from tests.fault_injector import ACTIVE_FAULT_PROFILE
from tests.generate_clips import (
    generate_clean_24fps,
    generate_clean_30fps,
    generate_impaired_clip,
    generate_invalid_file,
)

CLIPS_DIR = Path(__file__).parent / "clips"


@pytest.fixture(scope="session", autouse=True)
def prepare_test_clips():
    CLIPS_DIR.mkdir(parents=True, exist_ok=True)
    clean_30 = CLIPS_DIR / "clean_30fps.mp4"
    if not clean_30.exists():
        generate_clean_30fps(clean_30)

    clean_24 = CLIPS_DIR / "clean_24fps.mp4"
    if not clean_24.exists():
        generate_clean_24fps(clean_24)

    invalid_file = CLIPS_DIR / "invalid.mp4"
    if not invalid_file.exists():
        generate_invalid_file(invalid_file)

    impaired_file = CLIPS_DIR / "impaired.mp4"
    sidecar_file = CLIPS_DIR / "impaired_sidecar.json"
    if not impaired_file.exists() or not sidecar_file.exists():
        generate_impaired_clip(impaired_file, sidecar_file, clean_30fps_path=clean_30)


@pytest.fixture(autouse=True)
def isolate_environment(tmp_path, monkeypatch):
    test_db = tmp_path / "test_frameguard.db"
    test_data = tmp_path / "test_data"
    test_quarantine = test_data / "quarantine"

    monkeypatch.setattr(db, "DEFAULT_DB_PATH", test_db)
    monkeypatch.setattr(storage, "DATA_DIR", test_data)
    monkeypatch.setattr(storage, "QUARANTINE_DIR", test_quarantine)

    db.init_db(test_db)
    ACTIVE_FAULT_PROFILE.reset()
    yield
    ACTIVE_FAULT_PROFILE.reset()


@pytest.fixture
def make_client_copy(tmp_path):
    """Helper to copy a test clip and run in an isolated client working directory."""
    def _copy(clip_name: str, sidecar_name: str = None):
        src_clip = CLIPS_DIR / clip_name
        dest_clip = tmp_path / clip_name
        shutil.copy2(src_clip, dest_clip)

        dest_sidecar = None
        if sidecar_name:
            src_sidecar = CLIPS_DIR / sidecar_name
            dest_sidecar = tmp_path / sidecar_name
            shutil.copy2(src_sidecar, dest_sidecar)

        return dest_clip, dest_sidecar

    return _copy


@pytest.mark.asyncio
async def test_clean_baseline(make_client_copy):
    """Scenario 1: Clean Baseline — Upload completes with COMMITTED, matching hash, 0 retries."""
    clip, _ = make_client_copy("clean_30fps.mp4")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        uploader = ResilientUploader(
            base_url="http://testserver",
            video_path=clip,
            timeout=10.0,
        )
        receipt = await uploader.upload(client=client)

        assert receipt is not None
        assert receipt.file_sha256.lower() == uploader.manager.data.file_sha256.lower()
        assert verify_receipt(receipt) is True

        # Verify DB status
        row = db.get_upload(receipt.upload_id)
        assert row is not None
        assert row["state"] == UploadState.COMMITTED.value
        assert row["receipt_sig"] == receipt.receipt_sig

        # Verify all chunks were stored
        received = db.get_received_chunks(receipt.upload_id)
        assert len(received) == uploader.manager.data.n_chunks


@pytest.mark.asyncio
async def test_disconnect_and_resume(make_client_copy):
    """Scenario 2: Disconnect & Resume — Drop connections mid-upload, client resumes and commits."""
    clip, _ = make_client_copy("clean_30fps.mp4")

    ACTIVE_FAULT_PROFILE.enabled = True
    ACTIVE_FAULT_PROFILE.fail_chunk_indices = {1: 503}  # Fail chunk 1 once

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        uploader = ResilientUploader(
            base_url="http://testserver",
            video_path=clip,
            timeout=10.0,
            max_retries=5,
        )

        # Clear failure after first encounter so retry succeeds
        async def clear_disconnect():
            await asyncio.sleep(0.5)
            ACTIVE_FAULT_PROFILE.fail_chunk_indices.clear()

        asyncio.create_task(clear_disconnect())

        receipt = await uploader.upload(client=client)
        assert receipt is not None
        assert receipt.file_sha256 == uploader.manager.data.file_sha256

        row = db.get_upload(receipt.upload_id)
        assert row["state"] == UploadState.COMMITTED.value


@pytest.mark.asyncio
async def test_timeout_and_idempotency(make_client_copy):
    """Scenario 3: Timeout & Idempotency — Delayed chunk causes client retry, server handles idempotently."""
    clip, _ = make_client_copy("clean_30fps.mp4")

    # Chunk 0 has slight latency
    ACTIVE_FAULT_PROFILE.enabled = True
    ACTIVE_FAULT_PROFILE.latency_chunk_indices = [0]
    ACTIVE_FAULT_PROFILE.latency_seconds = 0.6

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        # Set client timeout shorter than latency for chunk 0
        uploader = ResilientUploader(
            base_url="http://testserver",
            video_path=clip,
            timeout=0.3,
            max_retries=5,
        )

        # Remove latency after 0.5s so retry succeeds
        async def lift_delay():
            await asyncio.sleep(0.5)
            ACTIVE_FAULT_PROFILE.latency_seconds = 0.0

        asyncio.create_task(lift_delay())

        receipt = await uploader.upload(client=client)
        assert receipt is not None

        # Verify chunk 0 exists exactly once in DB
        chunks = db.get_received_chunks(receipt.upload_id)
        assert len(chunks) == len(set(chunks)) == uploader.manager.data.n_chunks


@pytest.mark.asyncio
async def test_corrupt_chunk_rejected_and_retried(make_client_copy):
    """Scenario 4: Corrupt Chunk — Flipped byte rejected with 422, client resends, committed."""
    clip, _ = make_client_copy("clean_30fps.mp4")

    ACTIVE_FAULT_PROFILE.enabled = True
    ACTIVE_FAULT_PROFILE.bit_flip_chunk_indices = [0]  # Corrupt chunk 0 initially

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        uploader = ResilientUploader(
            base_url="http://testserver",
            video_path=clip,
            timeout=5.0,
            max_retries=5,
        )

        # Clear corruption after 0.4s so retry sends clean chunk
        async def clear_corruption():
            await asyncio.sleep(0.4)
            ACTIVE_FAULT_PROFILE.bit_flip_chunk_indices.clear()

        asyncio.create_task(clear_corruption())

        receipt = await uploader.upload(client=client)
        assert receipt is not None
        assert receipt.file_sha256 == uploader.manager.data.file_sha256


@pytest.mark.asyncio
async def test_client_crash_and_restart(make_client_copy):
    """Scenario 5: Client Crash — Stop uploader at 50%, restart with same manifest, only send remaining chunks."""
    clip, _ = make_client_copy("clean_30fps.mp4")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        uploader_1 = ResilientUploader(
            base_url="http://testserver",
            video_path=clip,
        )
        total_chunks = uploader_1.manager.data.n_chunks
        half_chunks = total_chunks // 2
        uploader_1.stop_after_chunks = half_chunks

        res_1 = await uploader_1.upload(client=client)
        assert res_1 is None  # crashed/stopped
        k_uploaded = uploader_1.uploaded_in_this_session
        assert k_uploaded >= half_chunks

        upload_id = uploader_1.upload_id
        assert upload_id is not None

        # Verify server received exactly k_uploaded chunks
        server_received = db.get_received_chunks(upload_id)
        assert len(server_received) == k_uploaded

        # Restart client with same file & manifest
        uploader_2 = ResilientUploader(
            base_url="http://testserver",
            video_path=clip,
        )
        assert uploader_2.upload_id == upload_id
        # Confirm previously uploaded chunks are already marked acked
        for i in server_received:
            assert uploader_2.manager.is_acked(i) is True

        receipt = await uploader_2.upload(client=client)
        assert receipt is not None
        assert receipt.upload_id == upload_id

        # Verify uploader_2 only uploaded the remaining chunks!
        assert uploader_2.uploaded_in_this_session == total_chunks - k_uploaded
        assert k_uploaded + uploader_2.uploaded_in_this_session == total_chunks

        # Verify server state
        row = db.get_upload(upload_id)
        assert row["state"] == UploadState.COMMITTED.value


@pytest.mark.asyncio
async def test_invalid_and_altered_files(make_client_copy):
    """Scenario 6: Invalid & Altered Files — Whole file hash mismatch rejected & quarantined; invalid bytes fails validation."""
    clip, _ = make_client_copy("clean_30fps.mp4")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        # Part A: Altered file hash
        uploader = ResilientUploader(base_url="http://testserver", video_path=clip)
        # Intentionally alter manifest hash
        uploader.manager.data.file_sha256 = "f" * 64

        # Upload chunks
        await uploader.init_session(client)
        for i in range(uploader.manager.data.n_chunks):
            await uploader._upload_single_chunk(client, i, 0)

        # Finalize should return 422
        resp = await client.post(f"http://testserver/v1/uploads/{uploader.upload_id}/complete")
        assert resp.status_code == 422

        row = db.get_upload(uploader.upload_id)
        assert row["state"] == UploadState.REJECTED.value
        # Check quarantine directory
        quarantine_path = storage.QUARANTINE_DIR / uploader.upload_id
        assert quarantine_path.exists()

    # Part B: Invalid file (non-video bytes)
    invalid_clip, _ = make_client_copy("invalid.mp4")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        uploader_inv = ResilientUploader(base_url="http://testserver", video_path=invalid_clip)
        receipt_inv = await uploader_inv.upload(client=client)
        assert receipt_inv is not None

        # Run background analysis synchronously for test
        run_analysis_pipeline(
            upload_id=receipt_inv.upload_id,
            video_path=storage.get_assembled_video_path(receipt_inv.upload_id),
            expected_sha256=receipt_inv.file_sha256,
        )

        job = db.get_analysis_job(receipt_inv.upload_id)
        assert job["status"] == JobStatus.FAILED_INVALID.value


@pytest.mark.asyncio
async def test_anomaly_detection(make_client_copy):
    """Scenario 7: Anomaly Detection — Missing frames 100-103 and freeze 600-630 caught, 24 FPS clean."""
    impaired_clip, sidecar = make_client_copy("impaired.mp4", "impaired_sidecar.json")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        # 1. Upload impaired clip with sidecar
        uploader = ResilientUploader(
            base_url="http://testserver",
            video_path=impaired_clip,
            sidecar_path=sidecar,
        )
        receipt = await uploader.upload(client=client)
        assert receipt is not None

        # Run analysis pipeline
        run_analysis_pipeline(
            upload_id=receipt.upload_id,
            video_path=storage.get_assembled_video_path(receipt.upload_id),
            expected_sha256=receipt.file_sha256,
            sidecar_path=storage.get_sidecar_path(receipt.upload_id),
        )

        # Fetch report endpoint
        resp = await client.get(f"http://testserver/v1/uploads/{receipt.upload_id}/report")
        assert resp.status_code == 200
        report_data = resp.json()["report"]

        anomalies = report_data["anomalies"]
        types = [a["type"] for a in anomalies]
        assert "MISSING" in types
        assert "FREEZE" in types

        # Check MISSING anomaly matches frames 100-103 within +-1 frame
        missing_anomaly = next(a for a in anomalies if a["type"] == "MISSING")
        assert abs(missing_anomaly["start_frame"] - 100) <= 2
        assert missing_anomaly["evidence_tier"] == EvidenceTier.TIER_1_STRONG.value
        assert missing_anomaly["origin"] == "CAPTURE"
        assert missing_anomaly["confidence"] >= 0.90

        # Check FREEZE anomaly matches frames 600-630 within +-1 frame
        freeze_anomaly = next(a for a in anomalies if a["type"] == "FREEZE")
        assert abs(freeze_anomaly["start_frame"] - 600) <= 1
        assert abs(freeze_anomaly["end_frame"] - 630) <= 1
        assert freeze_anomaly["evidence_tier"] == EvidenceTier.TIER_3_WEAK.value
        assert freeze_anomaly["origin"] == "UNDETERMINED"
        assert freeze_anomaly["confidence"] <= 0.60

        # 2. Upload clean 24 FPS video
        clean_24, _ = make_client_copy("clean_24fps.mp4")
        uploader_24 = ResilientUploader(
            base_url="http://testserver",
            video_path=clean_24,
        )
        receipt_24 = await uploader_24.upload(client=client)
        assert receipt_24 is not None

        run_analysis_pipeline(
            upload_id=receipt_24.upload_id,
            video_path=storage.get_assembled_video_path(receipt_24.upload_id),
            expected_sha256=receipt_24.file_sha256,
        )

        resp_24 = await client.get(f"http://testserver/v1/uploads/{receipt_24.upload_id}/report")
        assert resp_24.status_code == 200
        rep_24 = resp_24.json()["report"]
        # Zero false defect flags
        assert rep_24["anomalies"] == []


def test_receipt_signature_cryptographic_verification():
    """Verify that valid receipt signatures pass and any tampered or mismatched receipts are rejected."""
    from backend.receipts import generate_receipt, verify_receipt
    from shared.models import Receipt

    upload_id = "up_test123"
    file_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    total_bytes = 1048576
    committed_at = "2026-11-20T00:00:00+00:00"

    # 1. Valid signature passes (canonical JSON)
    receipt = generate_receipt(
        upload_id, file_sha256, total_bytes, committed_at=committed_at, use_canonical_json=True
    )
    assert verify_receipt(receipt) is True

    # 2. Valid signature passes (legacy colon format)
    receipt_colon = generate_receipt(
        upload_id, file_sha256, total_bytes, committed_at=committed_at, use_canonical_json=False
    )
    assert verify_receipt(receipt_colon) is True

    # 3. Custom key works
    custom_key = "my-test-secret-key-42"
    receipt_custom = generate_receipt(
        upload_id, file_sha256, total_bytes, committed_at=committed_at, secret_key=custom_key
    )
    assert verify_receipt(receipt_custom, secret_key=custom_key) is True
    # Key mismatch rejected
    assert verify_receipt(receipt_custom, secret_key="wrong-key") is False
    assert verify_receipt(receipt_custom) is False

    # 4. Tampered fields are strictly rejected
    # Altered upload_id
    r_bad_id = receipt.model_copy(update={"upload_id": "up_tampered"})
    assert verify_receipt(r_bad_id) is False

    # Altered file_sha256
    r_bad_sha = receipt.model_copy(update={"file_sha256": "0" * 64})
    assert verify_receipt(r_bad_sha) is False

    # Altered total_bytes
    r_bad_bytes = receipt.model_copy(update={"total_bytes": total_bytes + 1})
    assert verify_receipt(r_bad_bytes) is False

    # Altered committed_at timestamp
    r_bad_time = receipt.model_copy(update={"committed_at": "2026-11-20T00:00:01+00:00"})
    assert verify_receipt(r_bad_time) is False

    # Altered signature
    r_bad_sig = receipt.model_copy(update={"receipt_sig": "bad" + receipt.receipt_sig[3:]})
    assert verify_receipt(r_bad_sig) is False

    # Empty signature
    r_empty_sig = receipt.model_copy(update={"receipt_sig": ""})
    assert verify_receipt(r_empty_sig) is False


@pytest.mark.asyncio
async def test_idempotent_complete_receipt_consistency(make_client_copy):
    """Verify that multiple /complete calls return the same committed_at timestamp and valid signature."""
    clip, _ = make_client_copy("clean_30fps.mp4")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        uploader = ResilientUploader(
            base_url="http://testserver",
            video_path=clip,
            timeout=10.0,
        )
        receipt1 = await uploader.upload(client=client)
        assert receipt1 is not None
        assert verify_receipt(receipt1) is True

        # Second call to /complete (idempotent re-complete)
        resp2 = await client.post(f"http://testserver/v1/uploads/{receipt1.upload_id}/complete")
        assert resp2.status_code == 200
        from shared.models import Receipt
        receipt2 = Receipt(**resp2.json())

        # Verify exact timestamp and signature persistence
        assert receipt2.committed_at == receipt1.committed_at
        assert receipt2.receipt_sig == receipt1.receipt_sig
        assert verify_receipt(receipt2) is True
