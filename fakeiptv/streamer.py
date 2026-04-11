"""
streamer.py — Manages one ffmpeg process per channel.

Each process reads a concat file (built from the deterministic schedule),
remuxes to MPEG-TS at real-time speed (-re -c copy), and outputs HLS
segments + manifest to {tmp_dir}/ch_{id}/.

The concat file covers ~4 hours ahead. When ffmpeg finishes that window
it is automatically restarted with a freshly calculated concat file.

CatchupManager handles on-demand VOD sessions for past programmes.
"""
import logging
import os
import re
import shutil
import subprocess
import threading
import time
import uuid
from datetime import datetime
from typing import Dict, Optional, Tuple

from .scheduler import Channel, NowPlaying, ScheduleEntry, get_now_playing, get_playing_at

log = logging.getLogger(__name__)

HLS_SEGMENT_SECONDS = 4
HLS_LIST_SIZE = 8          # sliding window — keep 8 × 4s segments (~32s of buffer)
CONCAT_HOURS = 4           # how many hours to pre-build in each concat file
RESTART_DELAY = 1          # seconds to wait before restarting a dead process
IDLE_TIMEOUT = 600         # stop ffmpeg after 10 min with no client requests
IDLE_CHECK_INTERVAL = 60   # how often the reaper checks for idle channels


class ChannelStreamer:
    """Manages the ffmpeg process for a single channel."""

    def __init__(self, channel: Channel, tmp_base: str, subtitles: bool = True,
                 audio_copy: bool = True):
        self.channel = channel
        self._tmp_base = tmp_base
        self._subtitles = subtitles
        self._audio_copy = audio_copy   # False → transcode audio to AAC
        self._process: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._last_accessed: float = 0.0
        self._started_at: float = 0.0
        self.hls_dir = os.path.join(tmp_base, f"ch_{channel.id}")
        self.manifest_path = os.path.join(self.hls_dir, "stream.m3u8")
        self.concat_path = os.path.join(self.hls_dir, "concat.txt")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self):
        os.makedirs(self.hls_dir, exist_ok=True)
        self._stop_event.clear()
        self._last_accessed = time.time()
        self._started_at = time.time()
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

    def touch(self):
        """Record that a client just fetched something — resets the idle clock."""
        self._last_accessed = time.time()

    def is_idle(self) -> bool:
        return (time.time() - self._last_accessed) > IDLE_TIMEOUT

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
            # Forward slashes required by ffconcat; escape single quotes so
            # paths like "It's Always Sunny.mkv" don't break the format.
            # Use unquoted paths with backslash escaping.
            # Single-quoted ffconcat strings have no escape for ' itself.
            # Double-quoted paths aren't supported by all ffmpeg builds.
            # In unquoted mode, av_get_token() treats \ as an escape for the
            # next char, so \<space> and \' both work reliably.
            path = entry.path.replace("\\", "/")
            path = re.sub(r"([ \t'\"])", r"\\\1", path)
            lines.append(f"file {path}\n")
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
            # Remove stale HLS segments and manifest from any previous run so the
            # player doesn't get confused by timestamp mismatches on restart.
            for fname in os.listdir(self.hls_dir) if os.path.isdir(self.hls_dir) else []:
                if fname.endswith(".ts") or fname.endswith(".m3u8") or fname.endswith(".vtt"):
                    try:
                        os.remove(os.path.join(self.hls_dir, fname))
                    except OSError:
                        pass

            if not self._build_concat():
                log.error("Cannot build concat for %s — no entries?", self.channel.id)
                return

            seg_pattern = os.path.join(self.hls_dir, "seg%d.ts")
            audio_opts = ["-c:a", "copy"] if self._audio_copy else ["-c:a", "aac", "-b:a", "192k"]
            cmd = [
                "ffmpeg",
                "-hide_banner",
                "-loglevel", "error",
                # Regenerate PTS from scratch so inpoint-based seeks don't produce
                # non-monotonic timestamps across segment boundaries.
                "-fflags", "+genpts",
                # Shift timestamps so the stream always starts at t=0, preventing
                # players from seeing a large initial PTS value.
                "-avoid_negative_ts", "make_zero",
                "-re",
                "-f", "concat",
                "-safe", "0",
                "-i", self.concat_path,
                "-c:v", "copy",
                *audio_opts,
                "-map", "0:v:0",
                "-map", "0:a:0",
                # Subtitles: convert embedded SRT/ASS to WebVTT in-stream.
                # The '?' makes the map optional — no error if a file has no subs.
                # PGS (Blu-ray bitmap subs) are skipped; only text-based tracks work.
                *(["-map", "0:s:0?", "-c:s", "webvtt"] if self._subtitles else []),
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
                stdin=subprocess.DEVNULL,   # no terminal stdin — prevents tty state corruption
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                start_new_session=True,
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
                    # Bitmap subtitles (PGS/VOBSUB) can't be converted to WebVTT.
                    # Disable subtitle mapping for this channel and carry on.
                    if self._subtitles and "bitmap to bitmap" in stderr_output:
                        log.warning(
                            "Channel %s has bitmap subtitles — disabling subtitle "
                            "track for this channel", self.channel.id
                        )
                        self._subtitles = False

                    # Some audio codecs can't remux cleanly into MPEG-TS with -c copy:
                    #   - eac3: loses sample rate metadata
                    #   - dts / dts-hd: too-large packets or unsupported in TS
                    # Fall back to AAC transcoding for this channel.
                    _audio_errors = (
                        "unspecified sample rate",
                        "packet too large",
                        "invalid data found when processing input",
                        "dts-hd ma is not supported",
                        "no core found",
                    )
                    if self._audio_copy and any(e in stderr_output.lower() for e in _audio_errors):
                        log.warning(
                            "Channel %s has incompatible audio — "
                            "falling back to AAC transcoding", self.channel.id
                        )
                        self._audio_copy = False
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
    """
    Owns all channel streamers.  ffmpeg is started lazily — only when a client
    first requests /hls/<channel_id>/stream.m3u8 (via ensure_started).
    """

    def __init__(self, tmp_base: str = "/tmp/fakeiptv", subtitles: bool = True):
        self._tmp_base = tmp_base
        self._subtitles = subtitles
        # All known channels (running or not)
        self._channels: Dict[str, Channel] = {}
        # Only channels with an active ffmpeg process
        self._streamers: Dict[str, ChannelStreamer] = {}
        self._lock = threading.Lock()
        self._reaper = threading.Thread(
            target=self._reap_loop, daemon=True, name="stream-reaper"
        )
        self._reaper.start()

    def ensure_started(self, ch_id: str) -> bool:
        """
        Start the ffmpeg streamer for ch_id if it isn't already running.
        Returns False if the channel is unknown, True otherwise.
        Call this before waiting for is_ready().
        """
        with self._lock:
            if ch_id not in self._channels:
                return False
            if ch_id not in self._streamers:
                s = ChannelStreamer(self._channels[ch_id], self._tmp_base, self._subtitles)
                s.start()
                self._streamers[ch_id] = s
            return True

    def touch(self, ch_id: str):
        """Signal that a client is actively fetching this channel."""
        s = self._streamers.get(ch_id)
        if s:
            s.touch()

    def stop_all(self):
        with self._lock:
            for s in self._streamers.values():
                s.stop()
            self._streamers.clear()
            self._channels.clear()

    def get_hls_dir(self, ch_id: str) -> Optional[str]:
        s = self._streamers.get(ch_id)
        return s.hls_dir if s else None

    def is_ready(self, ch_id: str) -> bool:
        s = self._streamers.get(ch_id)
        return s.is_ready() if s else False

    def reload(self, channels: Dict[str, Channel]):
        """
        Update the channel registry.
        - Stops streamers for channels that were removed.
        - Restarts *running* streamers whose entry list changed.
        - New channels are registered but NOT started (lazy).
        """
        with self._lock:
            new_ids = set(channels.keys())
            old_ids = set(self._channels.keys())
            running_ids = set(self._streamers.keys())

            # Stop and remove streamers for channels that no longer exist
            for ch_id in old_ids - new_ids:
                if ch_id in running_ids:
                    self._streamers[ch_id].stop()
                    del self._streamers[ch_id]

            # Restart *running* channels whose file list changed
            for ch_id in new_ids & old_ids & running_ids:
                old_paths = [e.path for e in self._channels[ch_id].entries]
                new_paths = [e.path for e in channels[ch_id].entries]
                if old_paths != new_paths:
                    log.info("Entry list changed for %s — restarting", ch_id)
                    old_s = self._streamers[ch_id]
                    kept_subs = old_s._subtitles
                    kept_audio = old_s._audio_copy
                    old_s.stop()
                    s = ChannelStreamer(
                        channels[ch_id], self._tmp_base,
                        subtitles=kept_subs, audio_copy=kept_audio,
                    )
                    s.start()
                    self._streamers[ch_id] = s

            # Replace channel registry (new channels are NOT started here)
            self._channels = dict(channels)

    def _reap_loop(self):
        """Background thread: stop ffmpeg for channels with no recent client activity."""
        while True:
            time.sleep(IDLE_CHECK_INTERVAL)
            with self._lock:
                idle = [ch_id for ch_id, s in self._streamers.items() if s.is_idle()]
                for ch_id in idle:
                    log.info(
                        "Channel %s idle for >%ds — stopping ffmpeg", ch_id, IDLE_TIMEOUT
                    )
                    self._streamers[ch_id].stop()
                    del self._streamers[ch_id]


# ---------------------------------------------------------------------------
# CatchupManager — on-demand VOD sessions for past programmes
# ---------------------------------------------------------------------------

# How long (seconds) to keep a catchup session alive after last manifest request.
CATCHUP_SESSION_TTL = 7200   # 2 hours


class CatchupSession:
    """
    One temporary ffmpeg VOD process for a single catchup request.
    Serves HLS with #EXT-X-ENDLIST (seekable, not live).
    """

    def __init__(self, session_id: str, entry: ScheduleEntry, offset_sec: float,
                 duration_sec: float, session_dir: str, subtitles: bool):
        self.session_id = session_id
        self.entry = entry
        self.offset_sec = offset_sec
        self.duration_sec = duration_sec   # how much to serve (programme length - offset)
        self.session_dir = session_dir
        self.subtitles = subtitles
        self.manifest_path = os.path.join(session_dir, "stream.m3u8")
        self._process: Optional[subprocess.Popen] = None
        self._last_accessed = time.time()

    def touch(self):
        self._last_accessed = time.time()

    def is_expired(self) -> bool:
        return (time.time() - self._last_accessed) > CATCHUP_SESSION_TTL

    def start(self):
        os.makedirs(self.session_dir, exist_ok=True)
        seg_pattern = os.path.join(self.session_dir, "seg%d.ts")
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "error",
            # Seek within the source file to the programme start offset
            "-ss", str(self.offset_sec),
            # Limit output to the programme's remaining duration
            "-t", str(self.duration_sec),
            "-i", self.entry.path,
            "-c:v", "copy",
            "-c:a", "copy",
            "-map", "0:v:0",
            "-map", "0:a:0",
            *(["-map", "0:s:0?", "-c:s", "webvtt"] if self.subtitles else []),
            "-f", "hls",
            "-hls_time", str(HLS_SEGMENT_SECONDS),
            # No list_size limit — serve all segments (VOD)
            "-hls_list_size", "0",
            # No delete_segments, no omit_endlist — write #EXT-X-ENDLIST when done
            "-hls_segment_filename", seg_pattern,
            self.manifest_path,
        ]
        log.debug("Catchup ffmpeg: %s", " ".join(cmd))
        self._process = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )

    def stop(self):
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
        if os.path.isdir(self.session_dir):
            shutil.rmtree(self.session_dir, ignore_errors=True)

    def is_ready(self) -> bool:
        return os.path.exists(self.manifest_path)


class CatchupManager:
    """
    Creates and manages CatchupSession instances.
    A background thread evicts expired sessions.
    """

    def __init__(self, tmp_base: str, subtitles: bool = True):
        self._tmp_base = tmp_base
        self._subtitles = subtitles
        self._sessions: Dict[str, CatchupSession] = {}
        self._lock = threading.Lock()
        self._reaper = threading.Thread(
            target=self._reap_loop, daemon=True, name="catchup-reaper"
        )
        self._reaper.start()

    def get_or_create(
        self,
        channel: Channel,
        at: datetime,
    ) -> Optional[CatchupSession]:
        """
        Find or create a catchup session for `channel` at datetime `at`.
        Returns None if the channel has no content at that time.
        """
        result = get_playing_at(channel, at)
        if result is None:
            return None

        entry, offset_sec = result
        # Duration = remainder of the programme from the requested start
        duration_sec = entry.duration_sec - offset_sec

        # Session key: channel + unix timestamp rounded to nearest second
        ts = int(at.timestamp())
        session_id = f"{channel.id}_{ts}"

        with self._lock:
            if session_id in self._sessions:
                self._sessions[session_id].touch()
                return self._sessions[session_id]

            session_dir = os.path.join(self._tmp_base, "catchup", session_id)
            session = CatchupSession(
                session_id=session_id,
                entry=entry,
                offset_sec=offset_sec,
                duration_sec=duration_sec,
                session_dir=session_dir,
                subtitles=self._subtitles,
            )
            session.start()
            self._sessions[session_id] = session
            log.info(
                "Catchup session started: %s | %s @ %.0fs",
                session_id, entry.title, offset_sec
            )
            return session

    def get_session(self, session_id: str) -> Optional[CatchupSession]:
        with self._lock:
            s = self._sessions.get(session_id)
            if s:
                s.touch()
            return s

    def stop_all(self):
        with self._lock:
            for s in self._sessions.values():
                s.stop()
            self._sessions.clear()

    def _reap_loop(self):
        while True:
            time.sleep(300)  # check every 5 minutes
            with self._lock:
                expired = [sid for sid, s in self._sessions.items() if s.is_expired()]
                for sid in expired:
                    log.info("Expiring catchup session: %s", sid)
                    self._sessions[sid].stop()
                    del self._sessions[sid]
