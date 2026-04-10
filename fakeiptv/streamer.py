"""
streamer.py — Manages one ffmpeg process per channel.

Each process reads a concat file (built from the deterministic schedule),
remuxes to MPEG-TS at real-time speed (-re -c copy), and outputs HLS
segments + manifest to {tmp_dir}/ch_{id}/.

The concat file covers ~4 hours ahead. When ffmpeg finishes that window
it is automatically restarted with a freshly calculated concat file.
"""
import logging
import os
import shutil
import subprocess
import threading
import time
from typing import Dict, Optional

from .scheduler import Channel, NowPlaying, get_now_playing

log = logging.getLogger(__name__)

HLS_SEGMENT_SECONDS = 5
HLS_LIST_SIZE = 6          # sliding window — keep 6 segments (~30s of buffer)
CONCAT_HOURS = 4           # how many hours to pre-build in each concat file
RESTART_DELAY = 2          # seconds to wait before restarting a dead process


class ChannelStreamer:
    """Manages the ffmpeg process for a single channel."""

    def __init__(self, channel: Channel, tmp_base: str):
        self.channel = channel
        self._tmp_base = tmp_base
        self._process: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self.hls_dir = os.path.join(tmp_base, f"ch_{channel.id}")
        self.manifest_path = os.path.join(self.hls_dir, "stream.m3u8")
        self.concat_path = os.path.join(self.hls_dir, "concat.txt")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self):
        os.makedirs(self.hls_dir, exist_ok=True)
        self._stop_event.clear()
        self._launch()
        self._monitor_thread = threading.Thread(
            target=self._monitor, daemon=True, name=f"monitor-{self.channel.id}"
        )
        self._monitor_thread.start()
        log.info("Started streamer for channel: %s", self.channel.name)

    def stop(self):
        self._stop_event.set()
        self._kill()
        if os.path.isdir(self.hls_dir):
            shutil.rmtree(self.hls_dir, ignore_errors=True)
        log.info("Stopped streamer for channel: %s", self.channel.name)

    def is_ready(self) -> bool:
        """True once the HLS manifest exists (ffmpeg has written at least one segment)."""
        return os.path.exists(self.manifest_path)

    # ------------------------------------------------------------------
    # Concat file
    # ------------------------------------------------------------------

    def _build_concat(self) -> bool:
        """
        Build a ffconcat file covering ~CONCAT_HOURS hours from now.
        Uses inpoint on the first file to seek to the current schedule position.
        Returns True on success.
        """
        now_playing: Optional[NowPlaying] = get_now_playing(self.channel)
        if now_playing is None:
            log.error("No now-playing for channel %s", self.channel.id)
            return False

        entries = self.channel.entries
        n = len(entries)
        if n == 0:
            return False

        total_seconds = CONCAT_HOURS * 3600
        accumulated = 0.0

        lines = ["ffconcat version 1.0\n"]

        idx = now_playing.entry_index
        offset = now_playing.offset_sec
        first = True

        while accumulated < total_seconds:
            entry = entries[idx % n]
            path = entry.path.replace("\\", "/")
            lines.append(f"file '{path}'\n")
            if first and offset > 0:
                lines.append(f"inpoint {offset:.3f}\n")
                accumulated += entry.duration_sec - offset
                first = False
            else:
                accumulated += entry.duration_sec
            idx += 1

        with open(self.concat_path, "w", encoding="utf-8") as f:
            f.writelines(lines)

        return True

    # ------------------------------------------------------------------
    # ffmpeg process
    # ------------------------------------------------------------------

    def _launch(self):
        with self._lock:
            if not self._build_concat():
                log.error("Cannot build concat for %s — no entries?", self.channel.id)
                return

            seg_pattern = os.path.join(self.hls_dir, "seg%d.ts")
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "error",
                "-re",
                "-f", "concat",
                "-safe", "0",
                "-i", self.concat_path,
                "-c", "copy",
                "-map", "0:v:0",
                "-map", "0:a:0",
                "-f", "hls",
                "-hls_time", str(HLS_SEGMENT_SECONDS),
                "-hls_list_size", str(HLS_LIST_SIZE),
                "-hls_flags", "delete_segments+omit_endlist+append_list",
                "-hls_segment_filename", seg_pattern,
                self.manifest_path,
            ]
            log.debug("ffmpeg cmd: %s", " ".join(cmd))
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )

    def _kill(self):
        with self._lock:
            if self._process and self._process.poll() is None:
                try:
                    self._process.terminate()
                    self._process.wait(timeout=5)
                except Exception:
                    try:
                        self._process.kill()
                    except Exception:
                        pass
            self._process = None

    # ------------------------------------------------------------------
    # Monitor thread — restart ffmpeg when it exits
    # ------------------------------------------------------------------

    def _monitor(self):
        while not self._stop_event.is_set():
            proc = self._process
            if proc is None:
                time.sleep(1)
                continue

            ret = proc.wait()
            if self._stop_event.is_set():
                break

            # Log any ffmpeg stderr output on abnormal exit
            if ret != 0:
                stderr_output = ""
                try:
                    stderr_output = proc.stderr.read().decode(errors="replace")
                except Exception:
                    pass
                if stderr_output:
                    log.warning(
                        "ffmpeg exited (code %d) for %s:\n%s",
                        ret, self.channel.id, stderr_output[:500]
                    )
                else:
                    log.info(
                        "ffmpeg exited (code %d) for %s — concat exhausted, restarting",
                        ret, self.channel.id
                    )

            if not self._stop_event.is_set():
                time.sleep(RESTART_DELAY)
                log.info("Restarting ffmpeg for channel: %s", self.channel.name)
                self._launch()


# ---------------------------------------------------------------------------
# StreamManager — owns all channel streamers
# ---------------------------------------------------------------------------

class StreamManager:
    def __init__(self, tmp_base: str = "/tmp/fakeiptv"):
        self._tmp_base = tmp_base
        self._streamers: Dict[str, ChannelStreamer] = {}
        self._lock = threading.Lock()

    def stop_all(self):
        with self._lock:
            for s in self._streamers.values():
                s.stop()
            self._streamers.clear()

    def restart_channel(self, ch_id: str):
        with self._lock:
            s = self._streamers.get(ch_id)
            if s:
                s.stop()
                s.start()

    def get_hls_dir(self, ch_id: str) -> Optional[str]:
        s = self._streamers.get(ch_id)
        return s.hls_dir if s else None

    def is_ready(self, ch_id: str) -> bool:
        s = self._streamers.get(ch_id)
        return s.is_ready() if s else False

    def reload(self, channels: Dict[str, Channel]):
        """
        Stop streamers for removed channels, start new ones,
        restart those whose entry list has changed.
        """
        with self._lock:
            new_ids = set(channels.keys())
            old_ids = set(self._streamers.keys())

            for ch_id in old_ids - new_ids:
                self._streamers[ch_id].stop()
                del self._streamers[ch_id]

            for ch_id in new_ids - old_ids:
                s = ChannelStreamer(channels[ch_id], self._tmp_base)
                s.start()
                self._streamers[ch_id] = s

            # Restart channels whose file list changed
            for ch_id in new_ids & old_ids:
                old_paths = [e.path for e in self._streamers[ch_id].channel.entries]
                new_paths = [e.path for e in channels[ch_id].entries]
                if old_paths != new_paths:
                    log.info("Entry list changed for %s — restarting", ch_id)
                    self._streamers[ch_id].stop()
                    s = ChannelStreamer(channels[ch_id], self._tmp_base)
                    s.start()
                    self._streamers[ch_id] = s
