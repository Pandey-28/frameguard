# FrameGuard

**Reliable Video Upload & Evidence-Tiered Frame Integrity Detection**

FrameGuard is a local pipeline built for reliably uploading 30-second videos over unstable networks and performing evidence-tiered frame integrity analysis. Everything runs natively using Python 3.11+, SQLite, and FFmpeg—no Docker or external cloud services required.

---

## Architecture Overview

```
reliable-video-mvp/
├── requirements.txt
├── pytest.ini
├── shared/
│   ├── hashing.py            # 256 KiB chunker & streaming SHA-256 helpers
│   ├── models.py             # Pydantic schemas (Manifest, Receipt, Report, Anomalies)
│   └── ffmpeg_utils.py       # FFmpeg/FFprobe binary path resolution
├── backend/
│   ├── main.py               # FastAPI app & the 5 REST endpoints
│   ├── db.py                 # SQLite schema & upload state machine
│   ├── storage.py            # Local disk chunk writer & file assembler
│   ├── receipts.py           # HMAC-SHA256 signed receipt generator
│   └── analyzer.py           # Validation, 30 FPS resampler, D1–D4 detectors
├── client/
│   ├── uploader.py           # Resilient CLI uploader (resume, retry, AIMD window)
│   └── state.py              # Crash-safe atomic manifest/ACK persistence
└── tests/
    ├── fault_injector.py     # Pure-Python middleware to simulate bad networks
    ├── generate_clips.py     # Creates synthetic 30 s test videos via ffmpeg
    └── test_scenarios.py     # Automated pytest suite for upload & analysis
```

---

## Core Guarantees

* **G1 — No Silent Acceptance:** A partial, truncated, or corrupted file can never reach `COMMITTED`. Chunks are individually verified against SHA-256 headers, and the reassembled file is verified in a single-pass stream against the manifest SHA-256 before committing.
* **G2 — No Restart From Zero:** The server's ACK list is the source of truth; reconnects, timeouts, and app crashes resume strictly from the missing chunks.
* **G3 — No Over-Claiming:** Transfer integrity is provable via SHA-256. Capture integrity is inferred. Without a capture sidecar, pixel-only anomalies are reported as low-confidence observations (capped at 0.60 confidence, origin `UNDETERMINED`).

---

## 4 Frame Anomaly Detectors

1. **D1 — Capture Sidecar Join (Tier 1, Strong):** Matches camera metadata callbacks (`dropped_frames`) against PTS gaps within $\pm 1$ frame, attributed to `CAPTURE` (confidence $0.90+$).
2. **D2 — PTS Cadence (Tier 2, Medium):** Evaluates presentation timestamp delta $\Delta \text{pts}_i$ against median $\tilde{\Delta}$. Identifies missing frames ($> 1.5 \tilde{\Delta}$) and non-monotonic/duplicate frames ($\le 0$).
3. **D3 — Decode Health (Tier 2, Medium):** Captures container/packet decode warnings from FFmpeg (`-f null -`).
4. **D4 — Pixel Content (Tier 3, Weak):** Resamples frames onto a uniform 30 FPS grid via Zero-Order Hold. Computes 64-bit dHash and mean pixel difference. Detects sustained freezes ($> 3$ frames) and outlier jumps ($z > 6$), while filtering out standard 24-to-30 FPS pull-down repeats.

---

## Quickstart & Local Execution

### 1. Install Dependencies
```powershell
pip install -r requirements.txt
```

### 2. Generate Test Videos
```powershell
python -m tests.generate_clips
```
This generates:
- `clean_30fps.mp4` (30s testsrc2 baseline)
- `clean_24fps.mp4` (30s testsrc2 24 FPS baseline)
- `invalid.mp4` (corrupted non-video file)
- `impaired.mp4` & `impaired_sidecar.json` (dropped frames 100–103, freeze 600–630)

### 3. Start the Backend API
```powershell
uvicorn backend.main:app --host 127.0.0.1 --port 8000 --reload
```

### 4. Upload via CLI
```powershell
# Upload clean video
python -m client.uploader --file tests/clips/clean_30fps.mp4 --url http://127.0.0.1:8000

# Upload with capture sidecar
python -m client.uploader --file tests/clips/impaired.mp4 --sidecar tests/clips/impaired_sidecar.json --url http://127.0.0.1:8000
```
On success, the CLI validates the cryptographic HMAC-SHA256 receipt and prints `DELIVERED`.

### 5. Fetch Integrity Report
```powershell
curl http://127.0.0.1:8000/v1/uploads/{upload_id}/report
```

### 6. Run the Test Suite
```powershell
pytest -v
```
All 7 test scenarios pass:
1. `test_clean_baseline`: 0 retries, full verification, receipt signed.
2. `test_disconnect_and_resume`: mid-upload connection drops, resumes without restart.
3. `test_timeout_and_idempotency`: chunk timeout retry, idempotent deduplication on server.
4. `test_corrupt_chunk_rejected_and_retried`: 422 rejected bit-flip, clean retry, hash match.
5. `test_client_crash_and_restart`: stopped halfway, restarted, only missing chunks uploaded.
6. `test_invalid_and_altered_files`: whole-file hash mismatch quarantined/rejected; non-video fails validation.
7. `test_anomaly_detection`: missing frames 100-103 & freeze 600-630 detected within $\pm 1$ frame; clean 24 FPS zero false defect flags.
