from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class UploadState(str, Enum):
    CREATED = "CREATED"
    UPLOADING = "UPLOADING"
    ASSEMBLING = "ASSEMBLING"
    VERIFYING = "VERIFYING"
    COMMITTED = "COMMITTED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"


class JobStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED_INVALID = "FAILED_INVALID"
    FAILED_ERROR = "FAILED_ERROR"


class EvidenceTier(str, Enum):
    TIER_1_STRONG = "TIER_1_STRONG"
    TIER_2_MEDIUM = "TIER_2_MEDIUM"
    TIER_3_WEAK = "TIER_3_WEAK"


class AnomalyOrigin(str, Enum):
    TRANSFER = "TRANSFER"
    CAPTURE = "CAPTURE"
    ENCODING = "ENCODING"
    UNDETERMINED = "UNDETERMINED"


class UploadManifest(BaseModel):
    file_name: str
    total_bytes: int
    chunk_size: int = 256 * 1024
    n_chunks: int
    file_sha256: str
    sidecar_sha256: Optional[str] = None


class UploadSessionResponse(BaseModel):
    upload_id: str
    state: UploadState
    expires_at: str
    n_chunks: int


class UploadStatusResponse(BaseModel):
    upload_id: str
    state: UploadState
    total_bytes: int
    chunk_size: int
    n_chunks: int
    file_sha256: str
    sidecar_sha256: Optional[str] = None
    received_chunks: List[int]
    missing_chunks: List[int]
    expires_at: str


class ChunkUploadResponse(BaseModel):
    upload_id: str
    chunk_index: int
    received_bytes: int
    sha256: str
    status: str = "received"


class MissingChunksResponse(BaseModel):
    missing: List[int]


class Receipt(BaseModel):
    upload_id: str
    file_sha256: str
    total_bytes: int
    committed_at: str
    receipt_sig: str


class Anomaly(BaseModel):
    type: str  # MISSING, FREEZE, DUPLICATE, DECODE_ERROR, JUMP, etc.
    evidence_tier: EvidenceTier
    origin: AnomalyOrigin
    confidence: float
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    start_frame: Optional[int] = None
    end_frame: Optional[int] = None
    details: Dict[str, Any] = Field(default_factory=dict)


class IntegrityReport(BaseModel):
    upload_id: str
    status: JobStatus
    transfer: Dict[str, Any] = Field(default_factory=dict)
    validation: Dict[str, Any] = Field(default_factory=dict)
    frames: Dict[str, Any] = Field(default_factory=dict)
    anomalies: List[Anomaly] = Field(default_factory=list)
    limitations: List[str] = Field(default_factory=list)
    report_sha256: Optional[str] = None
    generated_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class ReportResponse(BaseModel):
    upload_id: str
    status: JobStatus
    report: Optional[IntegrityReport] = None
    error_detail: Optional[str] = None
