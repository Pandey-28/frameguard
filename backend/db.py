import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_DB_PATH = Path(os.environ.get("FRAMEGUARD_DB_PATH", "./data/frameguard.db"))


def get_connection(db_path: Optional[Path] = None) -> sqlite3.Connection:
    target_path = db_path or DEFAULT_DB_PATH
    target_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target_path), check_same_thread=False, timeout=15.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


@contextmanager
def get_db(db_path: Optional[Path] = None):
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Optional[Path] = None) -> None:
    target_path = db_path or DEFAULT_DB_PATH
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with get_db(target_path) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS uploads (
                upload_id TEXT PRIMARY KEY,
                state TEXT NOT NULL,
                total_bytes INTEGER NOT NULL,
                chunk_size INTEGER NOT NULL,
                n_chunks INTEGER NOT NULL,
                file_sha256 TEXT NOT NULL,
                sidecar_sha256 TEXT,
                receipt_sig TEXT,
                committed_at TEXT,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            );
        """)
        # Ensure committed_at column exists in pre-existing tables
        cursor = conn.execute("PRAGMA table_info(uploads)")
        columns = [row["name"] for row in cursor.fetchall()]
        if "committed_at" not in columns:
            conn.execute("ALTER TABLE uploads ADD COLUMN committed_at TEXT;")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS chunks (
                upload_id TEXT NOT NULL,
                idx INTEGER NOT NULL,
                sha256 TEXT NOT NULL,
                length INTEGER NOT NULL,
                received_at TEXT NOT NULL,
                PRIMARY KEY (upload_id, idx),
                FOREIGN KEY (upload_id) REFERENCES uploads(upload_id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS analysis_jobs (
                job_id TEXT PRIMARY KEY,
                upload_id TEXT NOT NULL,
                status TEXT NOT NULL,
                error_detail TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (upload_id) REFERENCES uploads(upload_id) ON DELETE CASCADE
            );
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS reports (
                upload_id TEXT PRIMARY KEY,
                report_json TEXT NOT NULL,
                report_sha256 TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (upload_id) REFERENCES uploads(upload_id) ON DELETE CASCADE
            );
        """)


def create_upload(
    upload_id: str,
    total_bytes: int,
    chunk_size: int,
    n_chunks: int,
    file_sha256: str,
    sidecar_sha256: Optional[str],
    expires_at: str,
    db_path: Optional[Path] = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_db(db_path) as conn:
        conn.execute(
            """
            INSERT INTO uploads (upload_id, state, total_bytes, chunk_size, n_chunks, file_sha256, sidecar_sha256, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                upload_id,
                "CREATED",
                total_bytes,
                chunk_size,
                n_chunks,
                file_sha256,
                sidecar_sha256,
                now,
                expires_at,
            ),
        )


def get_upload(upload_id: str, db_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    with get_db(db_path) as conn:
        cursor = conn.execute("SELECT * FROM uploads WHERE upload_id = ?", (upload_id,))
        row = cursor.fetchone()
        return dict(row) if row else None


def update_upload_state(
    upload_id: str,
    state: str,
    receipt_sig: Optional[str] = None,
    committed_at: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> None:
    with get_db(db_path) as conn:
        if receipt_sig is not None and committed_at is not None:
            conn.execute(
                "UPDATE uploads SET state = ?, receipt_sig = ?, committed_at = ? WHERE upload_id = ?",
                (state, receipt_sig, committed_at, upload_id),
            )
        elif receipt_sig is not None:
            conn.execute(
                "UPDATE uploads SET state = ?, receipt_sig = ? WHERE upload_id = ?",
                (state, receipt_sig, upload_id),
            )
        else:
            conn.execute(
                "UPDATE uploads SET state = ? WHERE upload_id = ?",
                (state, upload_id),
            )


def save_chunk(
    upload_id: str,
    idx: int,
    sha256: str,
    length: int,
    db_path: Optional[Path] = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_db(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO chunks (upload_id, idx, sha256, length, received_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (upload_id, idx, sha256, length, now),
        )


def get_chunk(
    upload_id: str, idx: int, db_path: Optional[Path] = None
) -> Optional[Dict[str, Any]]:
    with get_db(db_path) as conn:
        cursor = conn.execute(
            "SELECT * FROM chunks WHERE upload_id = ? AND idx = ?",
            (upload_id, idx),
        )
        row = cursor.fetchone()
        return dict(row) if row else None


def get_received_chunks(upload_id: str, db_path: Optional[Path] = None) -> List[int]:
    with get_db(db_path) as conn:
        cursor = conn.execute(
            "SELECT idx FROM chunks WHERE upload_id = ? ORDER BY idx ASC",
            (upload_id,),
        )
        return [row["idx"] for row in cursor.fetchall()]


def create_analysis_job(
    job_id: str, upload_id: str, db_path: Optional[Path] = None
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_db(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO analysis_jobs (job_id, upload_id, status, error_detail, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (job_id, upload_id, "PENDING", None, now, now),
        )


def update_analysis_job(
    job_id: str,
    status: str,
    error_detail: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_db(db_path) as conn:
        conn.execute(
            """
            UPDATE analysis_jobs
            SET status = ?, error_detail = ?, updated_at = ?
            WHERE job_id = ?
            """,
            (status, error_detail, now, job_id),
        )


def get_analysis_job(upload_id: str, db_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    with get_db(db_path) as conn:
        cursor = conn.execute(
            "SELECT * FROM analysis_jobs WHERE upload_id = ? ORDER BY created_at DESC LIMIT 1",
            (upload_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None


def save_report(
    upload_id: str, report_json: str, report_sha256: str, db_path: Optional[Path] = None
) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with get_db(db_path) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO reports (upload_id, report_json, report_sha256, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (upload_id, report_json, report_sha256, now),
        )


def get_report(upload_id: str, db_path: Optional[Path] = None) -> Optional[Dict[str, Any]]:
    with get_db(db_path) as conn:
        cursor = conn.execute(
            "SELECT * FROM reports WHERE upload_id = ?",
            (upload_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None
