# Reliable Video Upload & Frame Integrity Detection — MVP Plan

## 1. What We Are Building (The MVP)

This MVP is a local, working first model for reliably uploading 30-second videos over unstable networks and analyzing their frame integrity afterward. Everything runs natively on your machine using **Python**, **SQLite**, and **FFmpeg**—no Docker or external cloud services required.

The system solves two problems:

1. **Reliable, Resumable Upload:** Videos are split into **256 KiB chunks**, each verified with **SHA-256**. Progress is saved on both the client and server so disconnects, timeouts, or app crashes resume from the first missing chunk instead of restarting from zero. An upload is only marked complete when the server returns a **signed receipt** proving the reassembled file matches the client's original hash.

2. **Evidence-Tiered Frame Analysis:** Once an upload is `COMMITTED`, the backend validates the video, decodes it, maps it onto a **30 FPS grid** (900 frames for a 30 s clip), and reports anomalies (missing frames, duplicates, freezes, decode errors). Each finding cites its **evidence tier** and attributes the cause to **Transfer**, **Capture**, **Encoding**, or **Undetermined**.

### Core Guarantees

* **G1 — No Silent Acceptance:** A partial, truncated, or corrupted file can never reach `COMMITTED`.

* **G2 — No Restart From Zero:** The server's ACK list is the source of truth; reconnects and crashes only send missing chunks.

* **G3 — No Over-Claiming:** Transfer integrity is provable via SHA-256, while capture integrity is inferred. Without a capture sidecar, pixel-only anomalies are reported as low-confidence observations.

## 2. Tech Stack (100% Local, No Docker)

* **Language:** Python 3.11+

* **Backend API:** `fastapi`, `uvicorn`, `sqlite3` (local file database)

* **Background Analysis:** FastAPI `BackgroundTasks`

* **Client:** Python CLI script using `httpx` (async HTTP) and a local JSON/SQLite state file

* **Video & Frame Analysis:** Local `ffmpeg` & `ffprobe` (installed on your OS), `opencv-python-headless`, `numpy`

* **Network Fault Simulation & Testing:** `pytest` + a lightweight **Python Fault-Injection Middleware** (built right into a test FastAPI wrapper to simulate latency, disconnects, throttling, and bit-flips locally)

## 3. Project Structure

```
reliable-video-mvp/
├── requirements.txt
├── shared/
│   ├── hashing.py            # 256 KiB chunker & streaming SHA-256 helpers
│   └── models.py             # Pydantic schemas (Manifest, Receipt, Report)
├── backend/
│   ├── main.py               # FastAPI app & the 5 REST endpoints
│   ├── db.py                 # SQLite schema & upload state machine
│   ├── storage.py            # Local disk chunk writer & file assembler
│   ├── receipts.py           # HMAC-SHA256 signed receipt generator
│   └── analyzer.py           # Validation, 30 FPS resampler, D1–D4 detectors
├── client/
│   ├── uploader.py           # Resilient CLI uploader (resume, retry, AIMD)
│   └── state.py              # Local crash-safe manifest/ACK persistence
└── tests/
    ├── fault_injector.py     # Pure-Python middleware to simulate bad networks
    ├── generate_clips.py     # Creates synthetic 30 s test videos via ffmpeg
    └── test_scenarios.py     # Automated pytest suite for upload & analysis
```

## 4. Step-by-Step Implementation Plan

### Phase 1: Backend Upload API & State Machine (Day 1 — *Easy*)

**Goal:** Accept chunked uploads, verify hashes, assemble files on local disk, and issue signed receipts.

1. **SQLite Schema (`backend/db.py`):**
   Create 4 simple tables:

   * `uploads`: `upload_id`, `state` (`CREATED`, `UPLOADING`, `ASSEMBLING`, `VERIFYING`, `COMMITTED`, `REJECTED`, `EXPIRED`), `total_bytes`, `chunk_size`, `n_chunks`, `file_sha256`, `sidecar_sha256`, `receipt_sig`, `expires_at`.

   * `chunks`: `(upload_id, idx)` primary key, `sha256`, `length`, `received_at`.

   * `analysis_jobs`: `job_id`, `upload_id`, `status`, `error_detail`.

   * `reports`: `upload_id`, `report_json`, `report_sha256`.

2. **The 5 REST Endpoints (`backend/main.py`):**

   * `POST /v1/uploads`: Open a session using the client's manifest. Return `upload_id` and `expires_at`.

   * `PUT /v1/uploads/{id}/chunks/{i}`: Accept raw bytes + `X-Chunk-SHA256` header.

     * Compute SHA-256 of the body. If it does not match the header, return **HTTP 422** and discard the bytes.

     * If chunk `i` already exists with the *same* hash, return **HTTP 200** (idempotent).

     * If chunk `i` exists with a *different* hash, return **HTTP 409** (conflict).

     * Otherwise, save to `./data/{id}/chunks/{i}.part` and record it in SQLite.

   * `GET /v1/uploads/{id}`: Return current state and the list of received chunk indices.

   * `POST /v1/uploads/{id}/complete`:

     * Verify all chunks `0 .. n_chunks - 1` exist. If any are missing, return **HTTP 409** with `{"missing": [...]}`.

     * Stream-assemble chunks in order into `./data/{id}/video.mp4` while computing a single-pass SHA-256.

     * If the hash mismatches `file_sha256`, mark `REJECTED`, move to a quarantine folder, and return **HTTP 422**.

     * If it matches, mark `COMMITTED`, generate an HMAC-SHA256 signed receipt, schedule the analyzer via `BackgroundTasks`, and return the receipt.

   * `GET /v1/uploads/{id}/report`: Return the analysis status or the completed JSON integrity report.

### Phase 2: Resilient Python CLI Uploader (Day 2 — *Easy–Medium*)

**Goal:** Build a command-line client that survives timeouts, corrupt chunks, disconnects, and process crashes.

1. **Chunking & Local State (`client/state.py`):**

   * Read the video (and optional `sidecar.json`) in 256 KiB slices.

   * Compute each chunk's SHA-256 and the whole-file SHA-256 in a single pass.

   * Persist a local `manifest.json` storing `upload_id`, chunk hashes, and an `acked` flag per chunk. Write updates atomically after every ACK so restarting after a crash picks up where it left off.

2. **Adaptive Upload Loop (`client/uploader.py`):**

   * Call `POST /v1/uploads` for a new file, or `GET /v1/uploads/{id}` if resuming an existing manifest, to get the missing chunks.

   * Use a simple **AIMD concurrency window** `W` (starts at `2`, clamped between `1` and `4`):

     * Clean batch of ACKs $\rightarrow$ `W = min(4, W + 1)`.

     * Timeout or network error $\rightarrow$ `W = max(1, W // 2)`.

   * Use `httpx.AsyncClient` with a per-chunk timeout (e.g., 8–15 s) to catch stalls.

   * On failure (timeout, disconnect, HTTP 422/5xx), re-queue the chunk with **exponential backoff + jitter** (`base * 2^attempt + random(0, 1)`).

   * Once all chunks are ACKed, call `POST /v1/uploads/{id}/complete`.

     * If `409` with `missing[]`, re-queue those chunks and continue.

     * Only print **"DELIVERED"** when the returned signed receipt matches the local `file_sha256`.

### Phase 3: Validation, 30 FPS Resampler & Frame Analyzer (Days 3–4 — *Medium*)

**Goal:** Validate committed videos, resample onto a 30 FPS grid, and run Tier 1–3 anomaly detectors.

1. **Gate, Validate & Decode (`backend/analyzer.py`):**

   * Re-hash `./data/{id}/video.mp4` to confirm it still matches `file_sha256`.

   * Run `ffprobe -v error -show_format -show_streams -show_frames -of json` via `subprocess.run(..., timeout=30)`. If it fails or finds no video stream, mark the job `FAILED_INVALID` and save `stderr`.

   * Run `ffmpeg -v error -i video.mp4 -f null -` with a timeout and capture `stderr`. Any decode warnings/errors (corrupt packets, truncated container) are recorded as **Tier 2 Decode Errors (D3)**.

2. **30 FPS Zero-Order Hold Resampler:**

   * Read the decoded source frame timestamps (`src_pts`) from `ffprobe`.

   * For each target index $k = 0 \dots N-1$ (expected $N = 900 \pm 1$ for 30 s), set $t_k = k / 30$ and select the source frame $j$ with the largest $\text{PTS}_j \le t_k$.

   * Track expected conversion cadence: if the source is 24 FPS, repeating 1 frame in 5 is marked as expected conversion (`ENCODING/CONVERSION`), not a defect.

3. **Run the Four Detectors:**

   * **D1 — Capture Sidecar Join (Tier 1, Strong):** If `sidecar.json` is provided, match its dropped-frame callbacks and monotonic timestamps against PTS gaps within $\pm 1$ frame. Matches are attributed to **CAPTURE** (high confidence, e.g., `0.90+`).

   * **D2 — PTS Cadence (Tier 2, Medium):** Compute consecutive differences $\Delta \text{pts}_i$ and median $\tilde{\Delta}$.

     * $\Delta \text{pts}_i > 1.5 \tilde{\Delta}$ $\rightarrow$ **MISSING** frames, count $\approx \text{round}(\Delta \text{pts}_i / \tilde{\Delta}) - 1$.

     * $\Delta \text{pts}_i \le 0$ $\rightarrow$ **DUPLICATE / NON-MONOTONIC**.

   * **D3 — Decode Health (Tier 2, Medium):** Include any `ffmpeg` decode errors captured in Step 1.

   * **D4 — Pixel Content (Tier 3, Weak):** Read frames with OpenCV, resize to $9 \times 8$ grayscale, and compute a **64-bit dHash** plus mean absolute pixel difference (`diff`) between adjacent frames:

     * Hamming distance $\le 2$ and low pixel diff for $> 3$ consecutive frames $\rightarrow$ **FREEZE**.

     * Spike where $(\text{diff} - \text{median}) / (1.4826 \cdot \text{MAD}) > 6$ $\rightarrow$ **JUMP** (possible gap).

     * *Rule:* Without Tier 1 or Tier 2 support, cap confidence at `0.60` and label origin **UNDETERMINED**.

4. **Store the JSON Report:**
   Build the structured JSON report (`transfer`, `validation`, `frames`, `anomalies`, `limitations`), hash it with SHA-256, and save it in SQLite.

### Phase 4: Local Synthetic Clips & Fault-Injection Testing (Day 5 — *Easy–Medium*)

**Goal:** Prove reliability and detection accuracy locally using `ffmpeg` and `pytest`.

1. **Generate Ground-Truth Videos (`tests/generate_clips.py`):**
   Use your local `ffmpeg` with `testsrc2` (includes a built-in frame counter):

   * **Clean 30 FPS (30 s):** `ffmpeg -f lavfi -i testsrc2=duration=30:size=640x360:rate=30 -c:v libx264 clean_30fps.mp4`

   * **Clean 24 FPS (30 s):** Same command with `rate=24` (verifies conversion repeats are not flagged as defects).

   * **Invalid File:** Random bytes or a truncated `.mp4` (verifies `FAILED_INVALID`).

   * **Impaired Clip:** Drop frames `100–103` and freeze frames `600–630` using `ffmpeg` filters (or OpenCV), and write a matching `sidecar.json` for the dropped frames.

2. **Pure-Python Network Fault Injector (`tests/fault_injector.py`):**
   Instead of external network proxies, add a small FastAPI middleware (enabled only during tests via an environment variable) that reads a fault profile and injects:

   * **Latency / Stalls:** `await asyncio.sleep(15)` on selected chunk requests.

   * **Disconnects:** Return `503` or drop requests for a 10-second window mid-upload.

   * **Bandwidth Cap:** Sleep proportionally to chunk size (e.g., simulating 200 KB/s).

   * **Bit-Flip Corruption:** Flip a byte in the incoming chunk payload before the hash check runs.

3. **Run Core Automated Scenarios (`tests/test_scenarios.py`):**

   * **Clean Baseline:** Upload completes with `COMMITTED`, matching hash, 0 retries.

   * **Disconnect & Resume:** Drop connections for 10 s mid-upload $\rightarrow$ client resumes and commits without restarting.

   * **Timeout & Idempotency:** Inject 15 s delay $\rightarrow$ client retries, server stores no duplicate chunks.

   * **Corrupt Chunk:** Flip a byte $\rightarrow$ server rejects with `422`, client resends, final hash matches.

   * **Client Crash:** Stop the uploader at 50%, restart with the same `manifest.json` $\rightarrow$ only remaining chunks are sent.

   * **Invalid & Altered Files:** Verify `REJECTED` on whole-file hash mismatch and `FAILED_INVALID` on non-video bytes.

   * **Anomaly Detection:** Verify missing frames `100–103` and freeze `600–630` are caught within $\pm 1$ frame, and clean 24 FPS produces zero false defect flags.

## 5. How to Run Locally (Definition of Done)

Your MVP is complete when you can run everything directly from your terminal:

1. Start the backend locally: `uvicorn backend.main:app --reload`

2. Upload a 30 s video via the CLI: `python -m client.uploader --file tests/clean_30fps.mp4`

3. Interrupt the CLI (`Ctrl+C`) halfway through, re-run the same command, and watch it resume from the missing chunks and finish with a verified signed receipt.

4. Fetch the report from `GET /v1/uploads/{id}/report` and confirm clean, impaired, 24 FPS, and invalid files all produce the expected statuses and evidence tiers.

5. Run `pytest` and have all fault-injection and anomaly-detection tests pass.