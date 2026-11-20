import hashlib
import hmac
import json
import os
from datetime import datetime, timezone
from typing import Optional
from shared.models import Receipt

DEFAULT_SECRET_KEY = os.environ.get(
    "FRAMEGUARD_SECRET_KEY", "frameguard-mvp-insecure-default-key-change-in-prod"
)


def get_canonical_json_payload(
    upload_id: str,
    file_sha256: str,
    total_bytes: int,
    committed_at: str,
) -> bytes:
    """
    Produce canonical JSON bytes with sorted keys,
    compact separators, and lowercased hex hash.
    """
    payload = {
        "committed_at": str(committed_at),
        "file_sha256": str(file_sha256).lower(),
        "total_bytes": int(total_bytes),
        "upload_id": str(upload_id),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def get_colon_payload(
    upload_id: str,
    file_sha256: str,
    total_bytes: int,
    committed_at: str,
) -> bytes:
    """Format string payload for backward compatibility."""
    return f"{upload_id}:{file_sha256.lower()}:{total_bytes}:{committed_at}".encode("utf-8")


def compute_signature(
    upload_id: str,
    file_sha256: str,
    total_bytes: int,
    committed_at: str,
    secret_key: Optional[str] = None,
    use_canonical_json: bool = True,
) -> str:
    key = secret_key or os.environ.get("FRAMEGUARD_SECRET_KEY", DEFAULT_SECRET_KEY)
    if use_canonical_json:
        payload = get_canonical_json_payload(upload_id, file_sha256, total_bytes, committed_at)
    else:
        payload = get_colon_payload(upload_id, file_sha256, total_bytes, committed_at)
    return hmac.new(key.encode("utf-8"), payload, hashlib.sha256).hexdigest()


def generate_receipt(
    upload_id: str,
    file_sha256: str,
    total_bytes: int,
    committed_at: Optional[str] = None,
    secret_key: Optional[str] = None,
    use_canonical_json: bool = True,
) -> Receipt:
    key = secret_key or os.environ.get("FRAMEGUARD_SECRET_KEY", DEFAULT_SECRET_KEY)
    timestamp = committed_at or datetime.now(timezone.utc).isoformat()
    sig = compute_signature(
        upload_id=upload_id,
        file_sha256=file_sha256,
        total_bytes=total_bytes,
        committed_at=timestamp,
        secret_key=key,
        use_canonical_json=use_canonical_json,
    )
    return Receipt(
        upload_id=upload_id,
        file_sha256=file_sha256.lower(),
        total_bytes=total_bytes,
        committed_at=timestamp,
        receipt_sig=sig,
    )


def verify_receipt(receipt: Receipt, secret_key: Optional[str] = None) -> bool:
    """
    Verify the receipt signature using HMAC-SHA256 with constant-time comparison.
    Supports canonical JSON serialization as well as colon-delimited format
    to preserve backward compatibility while upholding cryptographic security.
    """
    key = secret_key or os.environ.get("FRAMEGUARD_SECRET_KEY", DEFAULT_SECRET_KEY)
    if not receipt.receipt_sig:
        return False

    # Check canonical JSON signature
    expected_canonical_sig = compute_signature(
        upload_id=receipt.upload_id,
        file_sha256=receipt.file_sha256,
        total_bytes=receipt.total_bytes,
        committed_at=receipt.committed_at,
        secret_key=key,
        use_canonical_json=True,
    )
    if hmac.compare_digest(receipt.receipt_sig, expected_canonical_sig):
        return True

    # Check colon-delimited signature
    expected_colon_sig = compute_signature(
        upload_id=receipt.upload_id,
        file_sha256=receipt.file_sha256,
        total_bytes=receipt.total_bytes,
        committed_at=receipt.committed_at,
        secret_key=key,
        use_canonical_json=False,
    )
    return hmac.compare_digest(receipt.receipt_sig, expected_colon_sig)
