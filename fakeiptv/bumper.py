"""
bumper.py — Bumper (loading screen) transcoding and HLS streaming.
"""
import logging
import os
import random
import re
import subprocess
import threading
import time
from typing import List, Optional

log = logging.getLogger(__name__)


class BumperStreamer:
    """Transcodes bumper file to MP4 cache, then remuxes to HLS segments."""

    def __init__(self, bumper_path: str, tmp_base: str, cache_dir: str):
        self._bumper_path = bumper_path
        name = os.path.splitext(os.path.basename(bumper_path))[0]
        self.bumper_id = re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")
        self.hls_dir = os.path.join(tmp_base, f"bumper_{self.bumper_id}")
        self._manifest_path = os.path.join(self.hls_dir, "video.m3u8")
        self._cache_path = os.path.join(cache_dir, f"{self.bumper_id}.mp4")
        self._meta_path = os.path.join(cache_dir, f"{self.bumper_id}.meta")
        self._process: Optional[subprocess.Popen] = None
        self._stop_event = threading.Event()
        self._ready_event = threading.Event()
        self._segments: List[str] = []
        self._seg_duration: float = 1.0

    def start(self):
        os.makedirs(self.hls_dir, exist_ok=True)
        try:
            with open(os.path.join(self.hls_dir, "empty.vtt"), "w", encoding="utf-8") as f:
                f.write("WEBVTT\n\n")
        except OSError:
            pass
        self._stop_event.clear()
        threading.Thread(
            target=self._run, daemon=True, name=f"bumper-{self.bumper_id}"
        ).start()
        log.info("BumperStreamer starting: %s", self.bumper_id)

    def stop(self):
        self._stop_event.set()
        proc = self._process
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    def is_ready(self) -> bool:
        return self._ready_event.is_set()

    def wait_ready(self, timeout: float = 10.0) -> bool:
        return self._ready_event.wait(timeout=timeout)

    def current_seq(self) -> int:
        return int(time.time())

    def manifest_content(self) -> str:
        n = len(self._segments)
        if n == 0:
            return ""
        now_seq = int(time.time())
        target = max(1, round(self._seg_duration))
        lines = [
            "#EXTM3U", "#EXT-X-VERSION:3",
            f"#EXT-X-TARGETDURATION:{target}",
            f"#EXT-X-MEDIA-SEQUENCE:{now_seq}",
        ]
        prev_idx = None
        for i in range(min(3, n)):
            idx = (now_seq + i) % n
            if prev_idx is not None and idx < prev_idx:
                lines.append("#EXT-X-DISCONTINUITY")
            prev_idx = idx
            lines += [
                f"#EXTINF:{self._seg_duration:.3f},",
                f"/hls/_loading/{self.bumper_id}/seg{now_seq + i}.ts",
            ]
        return "\n".join(lines) + "\n"

    def _run(self):
        if not self._ensure_cache():
            return
        if self._stop_event.is_set():
            return
        if not self._segment_to_hls():
            return
        segs, dur = self._parse_manifest()
        if not segs:
            log.error("BumperStreamer: no segments after HLS segmentation for %s", self.bumper_id)
            return
        self._segments = segs
        self._seg_duration = dur
        self._ready_event.set()
        log.info("BumperStreamer ready: %s (%d segs, %.2fs each)", self.bumper_id, len(segs), dur)

    def _ensure_cache(self) -> bool:
        try:
            src_mtime = str(os.path.getmtime(self._bumper_path))
        except OSError as exc:
            log.error("BumperStreamer: cannot stat %s: %s", self._bumper_path, exc)
            return False
        if os.path.exists(self._cache_path) and os.path.exists(self._meta_path):
            try:
                with open(self._meta_path) as f:
                    if f.read().strip() == src_mtime:
                        log.debug("BumperStreamer: cache hit for %s", self.bumper_id)
                        return True
            except OSError:
                pass
        log.info("BumperStreamer: transcoding %s -> cache (first use or source changed)",
                 self.bumper_id)
        os.makedirs(os.path.dirname(self._cache_path), exist_ok=True)
        tmp = self._cache_path + ".tmp"
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-i", self._bumper_path,
            "-c:v", "libx264", "-crf", "23",
            "-sc_threshold", "0",
            "-force_key_frames", "expr:gte(t,n_forced*1)",
            "-c:a", "aac", "-b:a", "128k", "-ac", "2",
            "-movflags", "+faststart",
            "-f", "mp4",
            tmp,
        ]
        log.debug("BumperStreamer transcode: %s", " ".join(cmd))
        proc = subprocess.Popen(
            cmd, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        self._process = proc
        proc.wait()
        if proc.returncode != 0 or self._stop_event.is_set():
            try:
                os.remove(tmp)
            except OSError:
                pass
            if proc.returncode != 0:
                log.error("BumperStreamer: transcode failed (rc=%d) for %s",
                          proc.returncode, self.bumper_id)
            return False
        os.replace(tmp, self._cache_path)
        try:
            with open(self._meta_path, "w") as f:
                f.write(src_mtime)
        except OSError as exc:
            log.warning("BumperStreamer: could not write meta for %s: %s", self.bumper_id, exc)
        return True

    def _segment_to_hls(self) -> bool:
        for fn in os.listdir(self.hls_dir):
            if fn.endswith((".ts", ".m3u8")):
                try:
                    os.remove(os.path.join(self.hls_dir, fn))
                except OSError:
                    pass
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-i", self._cache_path,
            "-c:v", "copy", "-c:a", "copy",
            "-f", "hls", "-hls_time", "1",
            "-hls_list_size", "0",
            "-hls_flags", "omit_endlist",
            "-hls_segment_filename", os.path.join(self.hls_dir, "seg%d.ts"),
            self._manifest_path,
        ]
        log.debug("BumperStreamer segment: %s", " ".join(cmd))
        proc = subprocess.Popen(
            cmd, stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        self._process = proc
        proc.wait()
        if proc.returncode != 0:
            log.error("BumperStreamer: HLS segmentation failed (rc=%d) for %s",
                      proc.returncode, self.bumper_id)
        return proc.returncode == 0 and not self._stop_event.is_set()

    def _parse_manifest(self):
        segs, durs = [], []
        try:
            with open(self._manifest_path) as f:
                lines = f.read().splitlines()
        except OSError:
            return [], 1.0
        for i, line in enumerate(lines):
            if line.startswith("#EXTINF:"):
                try:
                    durs.append(float(line[8:].rstrip(",")))
                except ValueError:
                    pass
                nxt = lines[i + 1].strip() if i + 1 < len(lines) else ""
                if nxt and not nxt.startswith("#"):
                    segs.append(os.path.basename(nxt))
        avg = sum(durs) / len(durs) if durs else 1.0
        return segs, avg


class BumperManager:
    """Manages one BumperStreamer per bumper file."""

    _VIDEO_EXTENSIONS = (".mp4", ".mkv", ".mov", ".avi", ".webm")

    def __init__(self, bumpers_path: str, tmp_base: str, cache_dir: str):
        self._bumpers_path = bumpers_path
        self._tmp_base = tmp_base
        self._cache_dir = os.path.join(cache_dir, "bumpers")
        self._bumpers: List[BumperStreamer] = []

    def start_all(self):
        if not os.path.isdir(self._bumpers_path):
            log.warning("BumperManager: bumpers_path %s not found — bumper feature disabled",
                        self._bumpers_path)
            return
        files = sorted(
            f for f in os.listdir(self._bumpers_path)
            if os.path.splitext(f)[1].lower() in self._VIDEO_EXTENSIONS
        )
        if not files:
            log.warning("BumperManager: no video files found in %s", self._bumpers_path)
            return
        os.makedirs(self._cache_dir, exist_ok=True)
        for filename in files:
            path = os.path.join(self._bumpers_path, filename)
            bs = BumperStreamer(path, self._tmp_base, self._cache_dir)
            bs.start()
            self._bumpers.append(bs)
        log.info("BumperManager: started %d bumper stream(s): %s",
                 len(self._bumpers), [b.bumper_id for b in self._bumpers])

    def stop_all(self):
        for b in self._bumpers:
            b.stop()
        self._bumpers.clear()

    def get_random_ready(self) -> Optional[BumperStreamer]:
        """Return a random ready BumperStreamer, or None if none are ready."""
        ready = [b for b in self._bumpers if b.is_ready()]
        if not ready:
            return None
        return random.choice(ready)

    def get_by_id(self, bumper_id: str) -> Optional[BumperStreamer]:
        for b in self._bumpers:
            if b.bumper_id == bumper_id:
                return b
        return None
