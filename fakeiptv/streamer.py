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
from typing import Dict, List, Optional, Tuple

from .scheduler import Channel, NowPlaying, ScheduleEntry, get_now_playing, get_playing_at

log = logging.getLogger(__name__)

HLS_SEGMENT_SECONDS = 2
HLS_LIST_SIZE = 30         # sliding window — keep 30 × 2s segments (~60s of buffer)
CONCAT_HOURS = 4           # how many hours to pre-build in each concat file
RESTART_DELAY = 1          # seconds to wait before restarting a dead process
IDLE_TIMEOUT = 600         # stop ffmpeg after 10 min with no client requests (watched)
IDLE_TIMEOUT_PREWARM = 120 # default; overridden per-instance via StreamManager config
IDLE_CHECK_INTERVAL = 30   # how often the reaper checks for idle channels


def _srt_ts_to_sec(ts: str) -> float:
    """Convert SRT/VTT timestamp (HH:MM:SS,mmm or HH:MM:SS.mmm) to seconds."""
    ts = ts.strip().replace(",", ".")
    parts = ts.split(":")
    h, m, s = int(parts[0]), int(parts[1]), float(parts[2])
    return h * 3600 + m * 60 + s


def _sec_to_vtt_ts(sec: float) -> str:
    """Convert seconds to WebVTT timestamp (HH:MM:SS.mmm)."""
    sec = max(0.0, sec)
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:06.3f}"


_SRT_TS_RE = re.compile(
    r"(\d{1,2}:\d{2}:\d{2}[,\.]\d{3})\s*-->\s*(\d{1,2}:\d{2}:\d{2}[,\.]\d{3})"
)


def _read_srt(path: str) -> str:
    """Read SRT file using the first encoding that succeeds."""
    for enc in ("utf-8-sig", "utf-8", "cp1255", "iso-8859-8", "latin-1"):
        try:
            with open(path, encoding=enc) as f:
                return f.read()
        except (UnicodeDecodeError, LookupError):
            continue
    return ""


def _parse_srt_cues(text: str):
    """Parse SRT text, return list of (start_sec, end_sec, cue_text)."""
    cues = []
    for block in re.split(r"\n\s*\n", text.strip()):
        lines = block.strip().splitlines()
        for i, line in enumerate(lines):
            m = _SRT_TS_RE.match(line.strip())
            if m:
                start = _srt_ts_to_sec(m.group(1))
                end = _srt_ts_to_sec(m.group(2))
                text_part = "\n".join(lines[i + 1:]).strip()
                if text_part:
                    cues.append((start, end, text_part))
                break
    return cues


class SubtitleStreamer:
    """
    Generates a static WebVTT subtitle file + HLS playlist for one language.

    Reads SRT files directly in Python (handling any encoding), adjusts cue
    timestamps so t=0 matches the video stream's start, and writes:
      sub_{lang}.vtt   — combined WebVTT for the ~4h concat window
      sub_{lang}.m3u8  — VOD-style single-entry HLS playlist

    No ffmpeg process.  Instant generation.  Regenerates on channel restart.
    """

    def __init__(self, channel: Channel, lang: str, hls_dir: str):
        self._channel = channel
        self.lang = lang
        self.hls_dir = hls_dir
        lang_label = lang or "und"
        self.vtt_path = os.path.join(hls_dir, f"sub_{lang_label}.vtt")
        self.manifest_path = os.path.join(hls_dir, f"sub_{lang_label}.m3u8")
        self._ok = False

    def is_running(self) -> bool:
        """True when the subtitle files were successfully generated."""
        return self._ok

    def start(self, _empty_srt: str = ""):
        try:
            self._generate()
            self._ok = True
        except Exception:
            log.exception(
                "SubtitleStreamer (%s, %s): generation failed",
                self._channel.id, self.lang or "und",
            )
            self._ok = False

    def stop(self):
        self._ok = False

    def _generate(self):
        now_playing: Optional[NowPlaying] = get_now_playing(self._channel)
        if now_playing is None:
            raise RuntimeError("no now-playing")

        entries = self._channel.entries
        n = len(entries)
        total_seconds = CONCAT_HOURS * 3600

        cue_lines: list = []
        stream_pos = 0.0          # seconds into the HLS stream
        inpoint = now_playing.offset_sec   # where we start within the first entry
        idx = now_playing.entry_index
        cue_count = 0

        while stream_pos < total_seconds:
            entry = entries[idx % n]
            srt_path = entry.subtitle_paths.get(self.lang, "")

            if srt_path and os.path.exists(srt_path):
                raw = _read_srt(srt_path)
                for start, end, text in _parse_srt_cues(raw):
                    # Shift cue into the stream timeline
                    s_adj = start - inpoint + stream_pos
                    e_adj = end   - inpoint + stream_pos
                    if e_adj <= 0:
                        continue   # cue entirely before our start
                    s_adj = max(0.0, s_adj)
                    if s_adj >= total_seconds:
                        break      # past the window
                    cue_lines.append(
                        f"{_sec_to_vtt_ts(s_adj)} --> {_sec_to_vtt_ts(e_adj)}\n"
                        f"{text}\n\n"
                    )
                    cue_count += 1

            remaining = entry.duration_sec - inpoint
            stream_pos += remaining
            inpoint = 0.0
            idx += 1

        # WebVTT file
        # X-TIMESTAMP-MAP aligns VTT time 0 with MPEG-TS PTS 0
        # (video stream starts at PTS 0 due to -avoid_negative_ts make_zero)
        with open(self.vtt_path, "w", encoding="utf-8") as f:
            f.write("WEBVTT\n")
            f.write("X-TIMESTAMP-MAP=MPEGTS:0,LOCAL:00:00:00.000\n\n")
            f.writelines(cue_lines)

        # VOD-style HLS playlist — single segment pointing to the VTT file.
        # The player downloads the full file and seeks within it.
        vtt_name = os.path.basename(self.vtt_path)
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            f.write(
                "#EXTM3U\n"
                "#EXT-X-TARGETDURATION:14400\n"
                "#EXT-X-VERSION:3\n"
                "#EXT-X-MEDIA-SEQUENCE:0\n"
                f"#EXTINF:{total_seconds:.1f},\n"
                f"{vtt_name}\n"
                "#EXT-X-ENDLIST\n"
            )

        log.info(
            "Subtitle track %s (%s): %d cues written",
            self.lang or "und", self._channel.id, cue_count,
        )


class ChannelStreamer:
    """Manages the ffmpeg process for a single channel."""

    def __init__(self, channel: Channel, tmp_base: str, subtitles: bool = True,
                 audio_copy: bool = True, prewarm_timeout: int = IDLE_TIMEOUT_PREWARM,
                 ready_segments: int = 3):
        self.channel = channel
        self._tmp_base = tmp_base
        self._subtitles = subtitles
        self._audio_copy = audio_copy   # False → transcode audio to AAC
        self._prewarm_timeout = prewarm_timeout
        self._ready_segments = ready_segments
        self._process: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._ready_event = threading.Event()   # set when manifest first appears
        self._monitor_thread: Optional[threading.Thread] = None
        self._last_accessed: float = 0.0
        self._started_at: float = 0.0
        self._ever_watched: bool = False  # True once a client touches this channel
        self.hls_dir = os.path.join(tmp_base, f"ch_{channel.id}")
        # Video manifest — named "video.m3u8" so the server can serve a master
        # playlist at "stream.m3u8" when subtitle tracks are present.
        self.manifest_path = os.path.join(self.hls_dir, "video.m3u8")
        self.concat_path = os.path.join(self.hls_dir, "concat.txt")
        self._subtitle_streamers: Dict[str, SubtitleStreamer] = {}

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
        for sub in self._subtitle_streamers.values():
            sub.stop()
        self._subtitle_streamers.clear()
        if os.path.isdir(self.hls_dir):
            shutil.rmtree(self.hls_dir, ignore_errors=True)
        log.info("Stopped streamer for channel: %s", self.channel.name)

    def touch(self):
        """Record that a client just fetched something — resets the idle clock."""
        self._last_accessed = time.time()
        self._ever_watched = True

    def is_idle(self) -> bool:
        if self._ever_watched:
            return (time.time() - self._last_accessed) > IDLE_TIMEOUT
        if self._prewarm_timeout == 0:
            return False  # 0 = never reap pre-warmed channels
        return (time.time() - self._last_accessed) > self._prewarm_timeout

    def is_ready(self) -> bool:
        """True once the HLS manifest exists (ffmpeg has written at least one segment)."""
        return self._ready_event.is_set()

    def wait_ready(self, timeout: float = 20.0) -> bool:
        """
        Block until the manifest is ready or timeout expires.
        Efficient: all concurrent callers wait on the same Event — no spinning,
        no per-request polling loops.  Returns True if ready within timeout.
        """
        return self._ready_event.wait(timeout=timeout)

    def _watch_ready(self):
        """Background thread: set _ready_event once enough segments are buffered.
        Waiting for MIN_READY_SEGMENTS before signalling avoids startup stutter —
        if we fire on the first segment the client plays it and then has to wait
        for the next one to be written, causing 1-2 visible stutters on channel switch.
        """
        while not self._stop_event.is_set():
            if os.path.exists(self.manifest_path):
                seg_count = sum(
                    1 for f in os.listdir(self.hls_dir) if f.endswith(".ts")
                )
                if seg_count >= self._ready_segments:
                    self._ready_event.set()
                    return
            time.sleep(0.2)

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

    def _get_subtitle_langs(self):
        """Collect all external SRT languages available across this channel's entries."""
        langs = set()
        for entry in self.channel.entries:
            langs.update(entry.subtitle_paths.keys())
        return sorted(langs)

    def _launch(self):
        self._ready_event.clear()
        threading.Thread(
            target=self._watch_ready, daemon=True, name=f"ready-{self.channel.id}"
        ).start()
        with self._lock:
            # Stop any running subtitle processes before cleaning up the directory
            for sub in self._subtitle_streamers.values():
                sub.stop()
            self._subtitle_streamers.clear()

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

            # Always transcode audio to AAC so any source codec (DTS, EAC3,
            # AC3, TrueHD, …) works transparently without per-channel detection.
            # On a modern CPU the overhead is negligible (~1-3% per core per channel).
            audio_opts = ["-c:a", "aac", "-b:a", "192k", "-ac", "2"]

            # --- Subtitles ---
            subtitle_langs = self._get_subtitle_langs()
            if subtitle_langs:
                # External SRT files found: generate WebVTT + playlist in Python.
                for lang in subtitle_langs:
                    sub = SubtitleStreamer(self.channel, lang, self.hls_dir)
                    sub.start()
                    self._subtitle_streamers[lang] = sub
                ready = [l for l, s in self._subtitle_streamers.items() if s.is_running()]
                log.info(
                    "Channel %s: subtitle tracks ready: %s",
                    self.channel.id, ready or "none",
                )
                # No embedded sub mapping — external SRTs cover subtitles
                sub_opts = []
            else:
                # No external SRTs: try embedded text subtitles via the video process.
                sub_opts = ["-map", "0:s:0?", "-c:s", "webvtt"] if self._subtitles else []

            seg_pattern = os.path.join(self.hls_dir, "seg%d.ts")
            cmd = [
                "nice", "-n", "10",   # lower CPU priority — yields to other processes
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
                # Only used when no external SRT files are available.
                # The '?' makes the map optional — no error if a file has no subs.
                *sub_opts,
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

    def _get_manifest_mtime(self) -> float:
        try:
            return os.path.getmtime(self.manifest_path)
        except OSError:
            return 0.0

    def _monitor(self):
        STALL_TIMEOUT = 30  # kill ffmpeg if no new segment for 30s (gives NAS time to recover)
        while not self._stop_event.is_set():
            proc = self._process
            if proc is None:
                time.sleep(1)
                continue

            last_mtime = self._get_manifest_mtime()
            last_check = time.time()
            ret = None

            while not self._stop_event.is_set():
                ret = proc.poll()
                if ret is not None:
                    break
                now = time.time()
                if self._ready_event.is_set() and now - last_check >= STALL_TIMEOUT:
                    mtime = self._get_manifest_mtime()
                    if mtime == last_mtime:
                        log.warning(
                            "Channel %s stalled — no new segments in %ds, restarting ffmpeg",
                            self.channel.id, STALL_TIMEOUT
                        )
                        self._kill()
                        break
                    last_mtime = mtime
                    last_check = now
                time.sleep(1)

            if self._stop_event.is_set():
                break

            # ret is None when we killed ffmpeg ourselves (stall detector) — just restart
            if ret is None:
                pass
            # Log any ffmpeg stderr output on abnormal exit
            elif ret != 0:
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

    def __init__(self, tmp_base: str = "/tmp/fakeiptv", subtitles: bool = True,
                 audio_copy: bool = True, prewarm_timeout: int = IDLE_TIMEOUT_PREWARM,
                 ready_segments: int = 3, session_mode: bool = False,
                 prewarm_adjacent: int = 0):
        self._tmp_base = tmp_base
        self._subtitles = subtitles
        self._audio_copy = audio_copy
        self._prewarm_timeout = prewarm_timeout
        self._ready_segments = ready_segments
        self._session_mode = session_mode
        self._prewarm_adjacent = prewarm_adjacent
        self._last_global_touch: float = time.time()
        # All known channels (running or not)
        self._channels: Dict[str, Channel] = {}
        # Ordered channel list — mirrors playlist order, used for adjacency
        self._channel_order: List[str] = []
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
                s = ChannelStreamer(self._channels[ch_id], self._tmp_base, self._subtitles,
                                   audio_copy=self._audio_copy,
                                   prewarm_timeout=self._prewarm_timeout,
                                   ready_segments=self._ready_segments)
                s.start()
                self._streamers[ch_id] = s
            if self._session_mode:
                self._last_global_touch = time.time()
            return True

    def touch(self, ch_id: str):
        """Signal that a client is actively fetching this channel."""
        s = self._streamers.get(ch_id)
        if s:
            s.touch()
            if self._session_mode:
                self._last_global_touch = time.time()

        if self._prewarm_adjacent > 0:
            order = self._channel_order  # snapshot — avoids holding lock during iteration
            if ch_id in order:
                idx = order.index(ch_id)
                for offset in range(-self._prewarm_adjacent, self._prewarm_adjacent + 1):
                    if offset == 0:
                        continue
                    adj_idx = idx + offset
                    if 0 <= adj_idx < len(order):
                        self.ensure_started(order[adj_idx])

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

    def wait_ready(self, ch_id: str, timeout: float = 20.0) -> bool:
        """Block until the channel manifest is ready or timeout expires."""
        s = self._streamers.get(ch_id)
        return s.wait_ready(timeout) if s else False

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
                    # Preserve per-channel audio fallback state, but always respect
                    # the global audio_copy setting (False = always transcode).
                    kept_audio = old_s._audio_copy and self._audio_copy
                    old_s.stop()
                    s = ChannelStreamer(
                        channels[ch_id], self._tmp_base,
                        subtitles=kept_subs, audio_copy=kept_audio,
                        prewarm_timeout=self._prewarm_timeout,
                        ready_segments=self._ready_segments,
                    )
                    s.start()
                    self._streamers[ch_id] = s

            # Replace channel registry (new channels are NOT started here)
            self._channels = dict(channels)
            self._channel_order = list(channels.keys())

    def get_subtitle_languages(self, ch_id: str):
        """Return sorted list of subtitle language codes whose ffmpeg is still running."""
        s = self._streamers.get(ch_id)
        if s is None:
            return []
        return sorted(
            lang for lang, sub in s._subtitle_streamers.items() if sub.is_running()
        )

    def has_active_streamers(self) -> bool:
        """True if any channel is currently running."""
        return bool(self._streamers)

    def _reap_loop(self):
        """Background thread: stop ffmpeg for channels with no recent client activity."""
        while True:
            time.sleep(IDLE_CHECK_INTERVAL)
            with self._lock:
                if self._session_mode:
                    # Session mode: keep all channels alive together; stop all at once
                    # when no channel has been touched for prewarm_timeout seconds.
                    if self._streamers and (time.time() - self._last_global_touch) > self._prewarm_timeout:
                        log.info(
                            "Session idle for >%ds — stopping all %d channels",
                            self._prewarm_timeout, len(self._streamers)
                        )
                        for s in self._streamers.values():
                            s.stop()
                        self._streamers.clear()
                else:
                    idle = [ch_id for ch_id, s in self._streamers.items() if s.is_idle()]
                    for ch_id in idle:
                        timeout = IDLE_TIMEOUT if self._streamers[ch_id]._ever_watched else IDLE_TIMEOUT_PREWARM
                        log.info(
                            "Channel %s idle for >%ds — stopping ffmpeg", ch_id, timeout
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
        duration_sec = entry.duration_sec - offset_sec
        ts = int(at.timestamp())

        with self._lock:
            # Reuse an existing session for the same channel within 60s tolerance.
            # Televizo (shift mode) increments utc by a few seconds each manifest poll,
            # which would otherwise spawn a new ffmpeg process on every request.
            REUSE_TOLERANCE = 60
            prefix = channel.id + "_"
            for sid, s in self._sessions.items():
                if sid.startswith(prefix):
                    try:
                        existing_ts = int(sid.rsplit("_", 1)[1])
                    except (ValueError, IndexError):
                        continue
                    if abs(existing_ts - ts) <= REUSE_TOLERANCE:
                        s.touch()
                        return s

            session_id = f"{channel.id}_{ts}"
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
