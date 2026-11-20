import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse

from backend import db, storage
from backend.analyzer import run_analysis_pipeline
from backend.receipts import generate_receipt
from shared.hashing import compute_sha256
from shared.models import (
    ChunkUploadResponse,
    IntegrityReport,
    MissingChunksResponse,
    Receipt,
    ReportResponse,
    UploadManifest,
    UploadSessionResponse,
    UploadState,
    UploadStatusResponse,
)
from tests.fault_injector import FaultInjectionMiddleware


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize SQLite schema
    db.init_db()
    yield


app = FastAPI(
    title="Reliable Video Upload & Frame Integrity Detection API",
    version="1.0.0",
    lifespan=lifespan,
)

# Add fault injection middleware for local testing
app.add_middleware(FaultInjectionMiddleware)


@app.post(
    "/v1/uploads",
    response_model=UploadSessionResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_upload_session(manifest: UploadManifest):
    """
    Open an upload session using the client's manifest.
    Returns upload_id and expires_at.
    """
    upload_id = f"up_{uuid.uuid4().hex[:12]}"
    expires_at = (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat()

    db.create_upload(
        upload_id=upload_id,
        total_bytes=manifest.total_bytes,
        chunk_size=manifest.chunk_size,
        n_chunks=manifest.n_chunks,
        file_sha256=manifest.file_sha256.lower(),
        sidecar_sha256=manifest.sidecar_sha256.lower() if manifest.sidecar_sha256 else None,
        expires_at=expires_at,
    )

    return UploadSessionResponse(
        upload_id=upload_id,
        state=UploadState.CREATED,
        expires_at=expires_at,
        n_chunks=manifest.n_chunks,
    )


@app.put(
    "/v1/uploads/{upload_id}/chunks/{chunk_index}",
    response_model=ChunkUploadResponse,
)
async def upload_chunk(
    upload_id: str,
    chunk_index: int,
    request: Request,
    x_chunk_sha256: Optional[str] = Header(None, alias="X-Chunk-SHA256"),
):
    """
    Accept raw bytes + X-Chunk-SHA256 header.
    - If hash does not match, return HTTP 422 and discard.
    - If chunk already exists with same hash, return HTTP 200 (idempotent).
    - If chunk exists with different hash, return HTTP 409 (conflict).
    - Otherwise save to disk and record in DB.
    """
    upload = db.get_upload(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload session not found")

    if upload["state"] in [UploadState.COMMITTED.value, UploadState.REJECTED.value, UploadState.EXPIRED.value]:
        raise HTTPException(
            status_code=409,
            detail=f"Upload session is already in terminal state: {upload['state']}",
        )

    if chunk_index < 0 or chunk_index >= upload["n_chunks"]:
        raise HTTPException(
            status_code=400,
            detail=f"Chunk index {chunk_index} out of range [0, {upload['n_chunks'] - 1}]",
        )

    body = await request.body()
    computed_hash = compute_sha256(body).lower()

    if not x_chunk_sha256 or computed_hash != x_chunk_sha256.lower():
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Chunk SHA-256 does not match X-Chunk-SHA256 header",
        )

    # Check if chunk already recorded
    existing_chunk = db.get_chunk(upload_id, chunk_index)
    if existing_chunk:
        if existing_chunk["sha256"].lower() == computed_hash:
            return ChunkUploadResponse(
                upload_id=upload_id,
                chunk_index=chunk_index,
                received_bytes=len(body),
                sha256=computed_hash,
                status="idempotent_duplicate",
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Chunk {chunk_index} exists with conflicting SHA-256",
            )

    # Save to disk
    storage.save_chunk_bytes(upload_id, chunk_index, body)
    db.save_chunk(upload_id, chunk_index, computed_hash, len(body))

    if upload["state"] == UploadState.CREATED.value:
        db.update_upload_state(upload_id, UploadState.UPLOADING.value)

    return ChunkUploadResponse(
        upload_id=upload_id,
        chunk_index=chunk_index,
        received_bytes=len(body),
        sha256=computed_hash,
        status="received",
    )


@app.put("/v1/uploads/{upload_id}/sidecar")
async def upload_sidecar(
    upload_id: str,
    request: Request,
    x_sidecar_sha256: Optional[str] = Header(None, alias="X-Sidecar-SHA256"),
):
    """Optional endpoint to upload sidecar.json metadata."""
    upload = db.get_upload(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload session not found")

    body = await request.body()
    computed_hash = compute_sha256(body).lower()
    if x_sidecar_sha256 and computed_hash != x_sidecar_sha256.lower():
        raise HTTPException(status_code=422, detail="Sidecar SHA-256 mismatch")

    storage.save_sidecar_bytes(upload_id, body)
    return {"status": "sidecar_saved", "sha256": computed_hash}


@app.get(
    "/v1/uploads/{upload_id}",
    response_model=UploadStatusResponse,
)
async def get_upload_status(upload_id: str):
    """
    Return current state and the list of received and missing chunk indices.
    """
    upload = db.get_upload(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload session not found")

    received = set(db.get_received_chunks(upload_id))
    all_chunks = set(range(upload["n_chunks"]))
    missing = sorted(list(all_chunks - received))

    return UploadStatusResponse(
        upload_id=upload_id,
        state=UploadState(upload["state"]),
        total_bytes=upload["total_bytes"],
        chunk_size=upload["chunk_size"],
        n_chunks=upload["n_chunks"],
        file_sha256=upload["file_sha256"],
        sidecar_sha256=upload["sidecar_sha256"],
        received_chunks=sorted(list(received)),
        missing_chunks=missing,
        expires_at=upload["expires_at"],
    )


@app.post(
    "/v1/uploads/{upload_id}/complete",
    response_model=Receipt,
)
async def complete_upload(
    upload_id: str,
    background_tasks: BackgroundTasks,
):
    """
    - Verify all chunks 0 .. n_chunks - 1 exist. If any are missing, return HTTP 409 with missing list.
    - Stream-assemble chunks in order into ./data/{id}/video.mp4 with single-pass SHA-256.
    - If hash mismatches file_sha256, mark REJECTED, move to quarantine, return HTTP 422.
    - If match, mark COMMITTED, issue signed receipt, schedule analyzer via BackgroundTasks.
    """
    upload = db.get_upload(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload session not found")

    if upload["state"] == UploadState.COMMITTED.value and upload.get("receipt_sig"):
        # Idempotent re-complete: return the authentic signed receipt
        return Receipt(
            upload_id=upload_id,
            file_sha256=upload["file_sha256"],
            total_bytes=upload["total_bytes"],
            committed_at=upload.get("committed_at") or upload["created_at"],
            receipt_sig=upload["receipt_sig"],
        )

    n_chunks = upload["n_chunks"]
    received = set(db.get_received_chunks(upload_id))
    missing = [i for i in range(n_chunks) if i not in received]

    if missing:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"missing": missing, "detail": "Upload incomplete, missing chunks"},
        )

    db.update_upload_state(upload_id, UploadState.ASSEMBLING.value)

    try:
        video_path, assembled_hash, total_bytes = storage.assemble_chunks(
            upload_id, n_chunks
        )
    except FileNotFoundError as e:
        db.update_upload_state(upload_id, UploadState.UPLOADING.value)
        raise HTTPException(status_code=409, detail=f"Assembly error: {str(e)}")

    db.update_upload_state(upload_id, UploadState.VERIFYING.value)

    if assembled_hash.lower() != upload["file_sha256"].lower():
        # Quarantine corrupted file and reject
        db.update_upload_state(upload_id, UploadState.REJECTED.value)
        storage.quarantine_upload(upload_id)
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "message": "Whole-file SHA-256 mismatch against manifest.",
                "expected": upload["file_sha256"],
                "computed": assembled_hash,
            },
        )

    # File matches manifest perfectly! Issue receipt and commit
    committed_at = datetime.now(timezone.utc).isoformat()
    receipt = generate_receipt(
        upload_id=upload_id,
        file_sha256=assembled_hash,
        total_bytes=total_bytes,
        committed_at=committed_at,
    )
    db.update_upload_state(
        upload_id,
        UploadState.COMMITTED.value,
        receipt_sig=receipt.receipt_sig,
        committed_at=receipt.committed_at,
    )

    # Initialize analysis job
    job_id = f"job_{upload_id}"
    db.create_analysis_job(job_id, upload_id)

    sidecar_path = storage.get_sidecar_path(upload_id)
    background_tasks.add_task(
        run_analysis_pipeline,
        upload_id=upload_id,
        video_path=video_path,
        expected_sha256=assembled_hash,
        sidecar_path=sidecar_path if sidecar_path.exists() else None,
    )

    return receipt


@app.get("/v1/uploads/{upload_id}/report", response_model=ReportResponse)
async def get_report_endpoint(upload_id: str):
    """
    Return the analysis status or the completed JSON integrity report.
    """
    upload = db.get_upload(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload session not found")

    job = db.get_analysis_job(upload_id)
    if not job:
        return ReportResponse(
            upload_id=upload_id,
            status="PENDING",
            report=None,
            error_detail=None,
        )

    report_row = db.get_report(upload_id)
    if report_row:
        import json
        report_data = json.loads(report_row["report_json"])
        return ReportResponse(
            upload_id=upload_id,
            status=job["status"],
            report=IntegrityReport(**report_data),
            error_detail=job.get("error_detail"),
        )

    return ReportResponse(
        upload_id=upload_id,
        status=job["status"],
        report=None,
        error_detail=job.get("error_detail"),
    )
