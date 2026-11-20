import hashlib
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import cv2
import numpy as np

from shared.ffmpeg_utils import ensure_ffmpeg_in_path, get_ffmpeg_binary, get_ffprobe_binary
from shared.hashing import compute_file_sha256, compute_sha256
from shared.models import (
    Anomaly,
    AnomalyOrigin,
    EvidenceTier,
    IntegrityReport,
    JobStatus,
)


def compute_dhash(image_gray_9x8: np.ndarray) -> int:
    """
    Compute 64-bit dHash from 9x8 grayscale image.
    Difference between adjacent columns: shape is (8, 8) -> 64 bits.
    """
    diff = image_gray_9x8[:, 1:] > image_gray_9x8[:, :-1]
    bits = diff.flatten()
    hash_int = 0
    for bit in bits:
        hash_int = (hash_int << 1) | int(bit)
    return hash_int


def hamming_distance(h1: int, h2: int) -> int:
    return bin(h1 ^ h2).count("1")


class VideoAnalyzer:
    def __init__(self, video_path: Path, expected_sha256: str, sidecar_path: Optional[Path] = None):
        self.video_path = Path(video_path)
        self.expected_sha256 = expected_sha256.lower()
        self.sidecar_path = Path(sidecar_path) if sidecar_path and Path(sidecar_path).exists() else None
        ensure_ffmpeg_in_path()

    def analyze(self, upload_id: str) -> IntegrityReport:
        transfer_info = {
            "file_sha256": self.expected_sha256,
            "file_size": self.video_path.stat().st_size if self.video_path.exists() else 0,
            "hash_verified": False,
        }
        validation_info: Dict[str, Any] = {}
        frames_info: Dict[str, Any] = {}
        anomalies: List[Anomaly] = []
        limitations: List[str] = [
            "Transfer integrity is strictly verified via end-to-end SHA-256.",
            "Capture integrity is inferred; without capture sidecar, pixel-only anomalies are capped at 0.60 confidence and attributed to UNDETERMINED.",
            "Analysis maps video timestamps onto a uniform 30 FPS zero-order hold grid.",
        ]

        # Step 1: Re-hash video
        actual_sha256 = compute_file_sha256(self.video_path)
        if actual_sha256.lower() != self.expected_sha256:
            transfer_info["actual_sha256"] = actual_sha256
            anomalies.append(
                Anomaly(
                    type="CHECKSUM_MISMATCH",
                    evidence_tier=EvidenceTier.TIER_1_STRONG,
                    origin=AnomalyOrigin.TRANSFER,
                    confidence=1.0,
                    details={"expected": self.expected_sha256, "actual": actual_sha256},
                )
            )
            report = IntegrityReport(
                upload_id=upload_id,
                status=JobStatus.FAILED_INVALID,
                transfer=transfer_info,
                validation={"error": "File checksum mismatch on disk."},
                frames=frames_info,
                anomalies=anomalies,
                limitations=limitations,
            )
            report.report_sha256 = compute_sha256(report.model_dump_json().encode("utf-8"))
            return report

        transfer_info["hash_verified"] = True

        # Step 2: Probe with ffprobe
        ffprobe_bin = get_ffprobe_binary()
        cmd = [
            ffprobe_bin,
            "-v", "error",
            "-show_format",
            "-show_streams",
            "-show_frames",
            "-of", "json",
            str(self.video_path),
        ]
        try:
            probe_proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=30
            )
        except Exception as e:
            report = IntegrityReport(
                upload_id=upload_id,
                status=JobStatus.FAILED_INVALID,
                transfer=transfer_info,
                validation={"error": f"ffprobe execution failed: {str(e)}"},
                frames=frames_info,
                anomalies=anomalies,
                limitations=limitations,
            )
            report.report_sha256 = compute_sha256(report.model_dump_json().encode("utf-8"))
            return report

        if probe_proc.returncode != 0:
            report = IntegrityReport(
                upload_id=upload_id,
                status=JobStatus.FAILED_INVALID,
                transfer=transfer_info,
                validation={"error": "ffprobe returned non-zero code", "stderr": probe_proc.stderr},
                frames=frames_info,
                anomalies=anomalies,
                limitations=limitations,
            )
            report.report_sha256 = compute_sha256(report.model_dump_json().encode("utf-8"))
            return report

        try:
            probe_data = json.loads(probe_proc.stdout)
        except json.JSONDecodeError as e:
            report = IntegrityReport(
                upload_id=upload_id,
                status=JobStatus.FAILED_INVALID,
                transfer=transfer_info,
                validation={"error": f"Failed to parse ffprobe JSON output: {str(e)}"},
                frames=frames_info,
                anomalies=anomalies,
                limitations=limitations,
            )
            report.report_sha256 = compute_sha256(report.model_dump_json().encode("utf-8"))
            return report

        streams = probe_data.get("streams", [])
        video_streams = [s for s in streams if s.get("codec_type") == "video"]
        if not video_streams:
            report = IntegrityReport(
                upload_id=upload_id,
                status=JobStatus.FAILED_INVALID,
                transfer=transfer_info,
                validation={"error": "No video stream found in container.", "stderr": probe_proc.stderr},
                frames=frames_info,
                anomalies=anomalies,
                limitations=limitations,
            )
            report.report_sha256 = compute_sha256(report.model_dump_json().encode("utf-8"))
            return report

        v_stream = video_streams[0]
        fmt = probe_data.get("format", {})
        validation_info = {
            "format_name": fmt.get("format_name"),
            "duration": float(fmt.get("duration", 0.0) or 0.0),
            "bit_rate": int(fmt.get("bit_rate", 0) or 0),
            "video_codec": v_stream.get("codec_name"),
            "width": int(v_stream.get("width", 0) or 0),
            "height": int(v_stream.get("height", 0) or 0),
            "r_frame_rate": v_stream.get("r_frame_rate"),
            "avg_frame_rate": v_stream.get("avg_frame_rate"),
        }

        # Step 3: Run ffmpeg decode check (D3 - Decode Health)
        ffmpeg_bin = get_ffmpeg_binary()
        decode_cmd = [
            ffmpeg_bin,
            "-v", "error",
            "-i", str(self.video_path),
            "-f", "null",
            "-",
        ]
        decode_proc = subprocess.run(decode_cmd, capture_output=True, text=True, timeout=30)
        decode_stderr = decode_proc.stderr.strip()
        if decode_stderr:
            anomalies.append(
                Anomaly(
                    type="DECODE_ERROR",
                    evidence_tier=EvidenceTier.TIER_2_MEDIUM,
                    origin=AnomalyOrigin.ENCODING,
                    confidence=0.85,
                    details={"stderr": decode_stderr},
                )
            )
            validation_info["decode_errors"] = decode_stderr

        # Step 4: Extract video frame timestamps
        raw_frames = probe_data.get("frames", [])
        video_frames = [f for f in raw_frames if f.get("media_type") == "video"]
        pts_list: List[float] = []
        for f in video_frames:
            pts_time = f.get("pkt_pts_time") or f.get("pts_time")
            if pts_time is not None:
                try:
                    pts_list.append(float(pts_time))
                except (ValueError, TypeError):
                    pass

        # Sort PTS list to ensure order
        pts_list.sort()
        n_src_frames = len(pts_list)
        duration = float(fmt.get("duration") or (pts_list[-1] - pts_list[0] if pts_list else 30.0))

        # Check nominal FPS
        nominal_fps = 30.0
        if v_stream.get("r_frame_rate"):
            try:
                num, den = v_stream["r_frame_rate"].split("/")
                nominal_fps = float(num) / float(den)
            except Exception:
                pass

        frames_info = {
            "total_source_frames": n_src_frames,
            "nominal_fps": round(nominal_fps, 2),
            "duration": round(duration, 3),
            "target_fps": 30,
        }

        # Load sidecar if available (D1)
        sidecar_dropped_events = []
        if self.sidecar_path and self.sidecar_path.exists():
            try:
                with open(self.sidecar_path, "r", encoding="utf-8") as sf:
                    sidecar_data = json.load(sf)
                    if isinstance(sidecar_data, dict):
                        sidecar_dropped_events = sidecar_data.get("dropped_frames", [])
                    elif isinstance(sidecar_data, list):
                        sidecar_dropped_events = sidecar_data
            except Exception as e:
                limitations.append(f"Failed to parse sidecar: {str(e)}")

        # Step 5: PTS Cadence Analysis (D2)
        if len(pts_list) > 1:
            deltas = np.diff(pts_list)
            median_delta = float(np.median(deltas))
            frames_info["median_delta_sec"] = round(median_delta, 5)

            for i, delta in enumerate(deltas):
                curr_pts = pts_list[i]
                next_pts = pts_list[i + 1]

                # Check missing frames
                if delta > 1.5 * median_delta:
                    missing_est = int(round(delta / median_delta)) - 1
                    # Check if sidecar matches this gap (D1)
                    matched_sidecar = None
                    for evt in sidecar_dropped_events:
                        evt_ts = evt.get("timestamp") or evt.get("pts")
                        evt_frame = evt.get("frame_index") or evt.get("frame")
                        if evt_ts is not None and abs(evt_ts - curr_pts) <= (1.5 * median_delta):
                            matched_sidecar = evt
                            break
                        if evt_frame is not None and abs(evt_frame - i) <= 2:
                            matched_sidecar = evt
                            break

                    if matched_sidecar:
                        anomalies.append(
                            Anomaly(
                                type="MISSING",
                                evidence_tier=EvidenceTier.TIER_1_STRONG,
                                origin=AnomalyOrigin.CAPTURE,
                                confidence=0.95,
                                start_time=round(curr_pts, 3),
                                end_time=round(next_pts, 3),
                                start_frame=i,
                                end_frame=i + missing_est,
                                details={
                                    "missing_frames_estimate": missing_est,
                                    "delta_pts": round(float(delta), 4),
                                    "sidecar_match": matched_sidecar,
                                },
                            )
                        )
                    else:
                        anomalies.append(
                            Anomaly(
                                type="MISSING",
                                evidence_tier=EvidenceTier.TIER_2_MEDIUM,
                                origin=AnomalyOrigin.ENCODING,
                                confidence=0.85,
                                start_time=round(curr_pts, 3),
                                end_time=round(next_pts, 3),
                                start_frame=i,
                                end_frame=i + missing_est,
                                details={
                                    "missing_frames_estimate": missing_est,
                                    "delta_pts": round(float(delta), 4),
                                    "median_delta": round(median_delta, 4),
                                },
                            )
                        )

                # Check duplicate / non-monotonic timestamps
                elif delta <= 0.0001:
                    anomalies.append(
                        Anomaly(
                            type="DUPLICATE",
                            evidence_tier=EvidenceTier.TIER_2_MEDIUM,
                            origin=AnomalyOrigin.ENCODING,
                            confidence=0.80,
                            start_time=round(curr_pts, 3),
                            end_time=round(next_pts, 3),
                            start_frame=i,
                            end_frame=i + 1,
                            details={"delta_pts": round(float(delta), 4)},
                        )
                    )

        # Step 6: 30 FPS Zero-Order Hold Resampler & Pixel Analysis (D4)
        target_fps = 30.0
        n_resampled = int(round(duration * target_fps))
        if n_resampled <= 0:
            n_resampled = 900
        frames_info["resampled_frames"] = n_resampled

        # Analyze pixel content via OpenCV
        pixel_anomalies = self._analyze_pixels(
            nominal_fps=nominal_fps,
            pts_list=pts_list,
            target_fps=target_fps,
            n_resampled=n_resampled,
            sidecar_dropped_events=sidecar_dropped_events,
        )
        # Filter out pixel anomalies that are artifacts of known PTS missing frame gaps
        missing_intervals = [
            (a.start_time, a.end_time)
            for a in anomalies
            if a.type == "MISSING" and a.start_time is not None and a.end_time is not None
        ]

        def is_inside_missing_gap(start_t: float, end_t: float) -> bool:
            for m_start, m_end in missing_intervals:
                # If the freeze is substantially inside the missing gap
                overlap_start = max(start_t, m_start)
                overlap_end = min(end_t, m_end)
                if overlap_end > overlap_start:
                    overlap_dur = overlap_end - overlap_start
                    total_dur = max(0.001, end_t - start_t)
                    if overlap_dur / total_dur > 0.6:
                        return True
            return False

        filtered_pixel_anomalies = []
        for a in pixel_anomalies:
            if a.type in ("FREEZE", "JUMP") and a.start_time is not None and a.end_time is not None:
                if is_inside_missing_gap(a.start_time, a.end_time):
                    continue
            filtered_pixel_anomalies.append(a)

        anomalies.extend(filtered_pixel_anomalies)

        report = IntegrityReport(
            upload_id=upload_id,
            status=JobStatus.COMPLETED,
            transfer=transfer_info,
            validation=validation_info,
            frames=frames_info,
            anomalies=anomalies,
            limitations=limitations,
        )
        report.report_sha256 = compute_sha256(report.model_dump_json().encode("utf-8"))
        return report

    def _analyze_pixels(
        self,
        nominal_fps: float,
        pts_list: List[float],
        target_fps: float,
        n_resampled: int,
        sidecar_dropped_events: List[Dict[str, Any]],
    ) -> List[Anomaly]:
        anomalies: List[Anomaly] = []
        cap = cv2.VideoCapture(str(self.video_path))
        if not cap.isOpened():
            return anomalies

        raw_frames = []
        dhashes = []

        try:
            while True:
                ret, frame = cap.read()
                if not ret:
                    break
                gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                small_9x8 = cv2.resize(gray, (9, 8), interpolation=cv2.INTER_AREA)
                h = compute_dhash(small_9x8)
                dhashes.append(h)
                # Store thumbnail (64x64 or 32x32) for fast mean difference
                thumb = cv2.resize(gray, (32, 32), interpolation=cv2.INTER_AREA)
                raw_frames.append(thumb)
        finally:
            cap.release()

        n_decoded = len(raw_frames)
        if n_decoded < 2:
            return anomalies

        # Resample onto 30 FPS grid via Zero-Order Hold
        # Map target times t_k = k / 30.0 to source frame index
        resampled_hashes: List[int] = []
        resampled_thumbs: List[np.ndarray] = []
        is_expected_conversion_repeat: List[bool] = []

        is_24_fps = 23.5 <= nominal_fps <= 24.5

        if pts_list and len(pts_list) == n_decoded:
            for k in range(n_resampled):
                t_k = k / target_fps
                # Find largest j with pts_list[j] <= t_k
                j = 0
                for idx, pts in enumerate(pts_list):
                    if pts <= t_k + 1e-4:
                        j = idx
                    else:
                        break
                j = min(j, n_decoded - 1)
                resampled_hashes.append(dhashes[j])
                resampled_thumbs.append(raw_frames[j])

                # Check expected 24->30 FPS cadence repeat
                if is_24_fps and k > 0:
                    prev_j = 0
                    t_prev = (k - 1) / target_fps
                    for idx, pts in enumerate(pts_list):
                        if pts <= t_prev + 1e-4:
                            prev_j = idx
                        else:
                            break
                    prev_j = min(prev_j, n_decoded - 1)
                    # If this repeat corresponds to 1 repeat in 5 frames
                    if j == prev_j:
                        is_expected_conversion_repeat.append(True)
                    else:
                        is_expected_conversion_repeat.append(False)
                else:
                    is_expected_conversion_repeat.append(False)
        else:
            # Fallback uniform index sampling
            for k in range(n_resampled):
                src_idx = min(int(round(k * (n_decoded / n_resampled))), n_decoded - 1)
                resampled_hashes.append(dhashes[src_idx])
                resampled_thumbs.append(raw_frames[src_idx])
                is_expected_conversion_repeat.append(False)

        # Detect Freezes (D4)
        # Spec: "Hamming distance <= 2 and low pixel diff for > 3 consecutive frames -> FREEZE"
        # If source is 24 FPS, ignore isolated 1-frame cadence repeats.
        current_freeze_start = None
        current_freeze_len = 0

        diffs: List[float] = []
        for i in range(1, len(resampled_thumbs)):
            d = float(np.mean(np.abs(resampled_thumbs[i].astype(float) - resampled_thumbs[i - 1].astype(float))))
            diffs.append(d)

        for i in range(1, len(resampled_hashes)):
            h_dist = hamming_distance(resampled_hashes[i], resampled_hashes[i - 1])
            p_diff = diffs[i - 1]
            is_frozen = (h_dist <= 2 and p_diff < 1.0)

            # Skip single-frame hold if expected conversion from 24 FPS
            if is_frozen and is_expected_conversion_repeat[i] and current_freeze_len == 0:
                continue

            if is_frozen:
                if current_freeze_start is None:
                    current_freeze_start = i - 1
                    current_freeze_len = 1
                else:
                    current_freeze_len += 1
            else:
                if current_freeze_start is not None and current_freeze_len > 3:
                    # Found a freeze anomaly
                    start_f = current_freeze_start
                    end_f = current_freeze_start + current_freeze_len
                    start_t = start_f / target_fps
                    end_t = end_f / target_fps

                    anomalies.append(
                        Anomaly(
                            type="FREEZE",
                            evidence_tier=EvidenceTier.TIER_3_WEAK,
                            origin=AnomalyOrigin.UNDETERMINED,
                            confidence=0.60,
                            start_time=round(start_t, 3),
                            end_time=round(end_t, 3),
                            start_frame=start_f,
                            end_frame=end_f,
                            details={
                                "duration_frames": current_freeze_len,
                                "duration_sec": round(end_t - start_t, 3),
                            },
                        )
                    )
                current_freeze_start = None
                current_freeze_len = 0

        if current_freeze_start is not None and current_freeze_len > 3:
            start_f = current_freeze_start
            end_f = current_freeze_start + current_freeze_len
            start_t = start_f / target_fps
            end_t = end_f / target_fps
            anomalies.append(
                Anomaly(
                    type="FREEZE",
                    evidence_tier=EvidenceTier.TIER_3_WEAK,
                    origin=AnomalyOrigin.UNDETERMINED,
                    confidence=0.60,
                    start_time=round(start_t, 3),
                    end_time=round(end_t, 3),
                    start_frame=start_f,
                    end_frame=end_f,
                    details={
                        "duration_frames": current_freeze_len,
                        "duration_sec": round(end_t - start_t, 3),
                    },
                )
            )

        # Detect Jumps / Gaps (D4)
        # Spec: "Spike where (diff - median) / (1.4826 * MAD) > 6 -> JUMP"
        if len(diffs) > 10:
            diff_arr = np.array(diffs)
            median_d = float(np.median(diff_arr))
            mad = float(np.median(np.abs(diff_arr - median_d)))
            if mad > 1e-4:
                z_scores = (diff_arr - median_d) / (1.4826 * mad)
                spike_indices = np.where(z_scores > 6.0)[0]
                for s_idx in spike_indices:
                    jump_frame = int(s_idx) + 1
                    jump_time = jump_frame / target_fps
                    anomalies.append(
                        Anomaly(
                            type="JUMP",
                            evidence_tier=EvidenceTier.TIER_3_WEAK,
                            origin=AnomalyOrigin.UNDETERMINED,
                            confidence=0.55,
                            start_time=round(jump_time, 3),
                            end_time=round(jump_time, 3),
                            start_frame=jump_frame,
                            end_frame=jump_frame,
                            details={
                                "pixel_diff": round(float(diff_arr[s_idx]), 4),
                                "z_score": round(float(z_scores[s_idx]), 2),
                            },
                        )
                    )

        return anomalies


def run_analysis_pipeline(upload_id: str, video_path: Path, expected_sha256: str, sidecar_path: Optional[Path] = None):
    from backend import db

    job = db.get_analysis_job(upload_id)
    job_id = job["job_id"] if job else f"job_{upload_id}"
    db.update_analysis_job(job_id, "RUNNING")

    try:
        analyzer = VideoAnalyzer(video_path, expected_sha256, sidecar_path)
        report = analyzer.analyze(upload_id)

        status_str = report.status.value
        db.update_analysis_job(
            job_id,
            status_str,
            error_detail=report.validation.get("error") if report.status != JobStatus.COMPLETED else None,
        )
        report_json_str = report.model_dump_json(indent=2)
        db.save_report(upload_id, report_json_str, report.report_sha256 or "")
    except Exception as e:
        db.update_analysis_job(job_id, "FAILED_ERROR", error_detail=str(e))
