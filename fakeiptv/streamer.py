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
import random
import re
import shutil
import subprocess
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from .models import Channel, NowPlaying, ScheduleEntry
from .scheduler import get_now_playing, get_playing_at
from .subtitle_streamer import SubtitleStreamer
from .live_subtitle import LiveSubtitleWriter
from .bumper import BumperStreamer, BumperManager
from .catchup import CatchupManager
from .ffprobe_utils import (
    _nas_prewarm, _nas_prewarm_header, _probe_keyframe_inpoint, _probe_gop_size,
    _probe_audio_stream_index, _probe_subtitle_stream_indices,
    _probe_segment_start_pts, _probe_stream_start_time,
)
from .subtitle_utils import (
    _read_srt, _parse_srt_cues, _sec_to_vtt_ts, _srt_ts_to_sec,
    _text_has_hebrew, _he_bidi_fix, _extract_embedded_srt,
)

log = logging.getLogger(__name__)

HLS_SEGMENT_SECONDS = 2
HLS_LIST_SIZE = 15
CONCAT_HOURS = 4
RESTART_DELAY = 1
IDLE_TIMEOUT = 600
IDLE_TIMEOUT_PREWARM = 120
IDLE_CHECK_INTERVAL = 30
CONCAT_PREWARM_LEAD = 60   # seconds before episode transition to prewarm next file
GLOBAL_PREWARM_INTERVAL = 600  # seconds between all-channel NAS warmup sweeps

class ChannelStreamer:
    """Manages the ffmpeg process for a single channel."""

    def __init__(self, channel: Channel, tmp_base: str, subtitles: bool = True,
                 audio_copy: bool = True, prewarm_timeout: int = IDLE_TIMEOUT_PREWARM,
                 ready_segments: int = 3, preferred_audio_language: str = "eng",
                 hls_start_number: int = 0, subtitle_background: bool = True):
        self.channel = channel
        self._tmp_base = tmp_base
        self._subtitles = subtitles
        self._subtitle_background = subtitle_background
        self._audio_copy = audio_copy   # False → transcode audio to AAC
        self._preferred_audio_language = preferred_audio_language
        self._prewarm_timeout = prewarm_timeout
        self._ready_segments = ready_segments
        # MEDIA-SEQUENCE offset applied when serving video.m3u8 to the player.
        # Set to bumper_seq + headroom when a bumper covers the cold-start gap,
        # so the channel's declared sequence number is HIGHER than the bumper's.
        # The player treats a forward jump as new content and downloads immediately
        # rather than treating real segments as "already seen" (which caused a
        # 6-10s stall waiting for a master-playlist re-poll to reset tracking).
        # Segment filenames on disk stay as seg1.ts, seg2.ts, etc. — only the
        # #EXT-X-MEDIA-SEQUENCE header in the served manifest is adjusted.
        self._seq_offset = hls_start_number
        self._process: Optional[subprocess.Popen] = None
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._ready_event = threading.Event()   # set when manifest first appears
        self._monitor_thread: Optional[threading.Thread] = None
        self._last_accessed: float = 0.0
        self._started_at: float = 0.0
        self._last_launch_wall_time: float = 0.0  # wall time when _launch() last ran
        self._ever_watched: bool = False  # True once a client touches this channel
        self.hls_dir = os.path.join(tmp_base, f"ch_{channel.id}")
        # Video manifest — named "video.m3u8" so the server can serve a master
        # playlist at "stream.m3u8" when subtitle tracks are present.
        self.manifest_path = os.path.join(self.hls_dir, "video.m3u8")
        self.concat_path = os.path.join(self.hls_dir, "concat.txt")
        self._subtitle_streamers: Dict[str, SubtitleStreamer] = {}
        # Set once subtitle VTTs have been written with the correct TIMESTAMP-MAP.
        # The server waits on this before serving the master playlist so the player
        # always gets a VTT that has the right MPEGTS anchor (never MPEGTS:0).
        self._subtitle_ready_event = threading.Event()
        # Langs whose subs are written as SRT side outputs by the main ffmpeg process.
        # Populated in _launch() for the current entry; cleared on each restart.
        self._live_srt_langs: set = set()
        self._live_srt_indices: Dict[str, int] = {}
        self._codec_disc_pending = False   # set by _build_concat when codec/res changes at boundary

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self):
        os.makedirs(self.hls_dir, exist_ok=True)
        self._stop_event.clear()
        self._last_accessed = time.time()
        self._started_at = time.time()
        self._launch()
        self._codec_disc_pending = False  # no prior stream; don't inject disc on initial start
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

    def wait_subtitle_ready(self, timeout: float = 10.0) -> bool:
        """
        Block until subtitle VTTs have been written (or timeout expires).
        Returns True if ready.  Always returns True for channels without SRTs
        (event is set immediately in _launch()).  Called after wait_ready() so
        the extra wait is typically ~0.5s (subtitle write finishes at ~2.5s,
        wait_ready fires at ~2s on the first segment).
        """
        return self._subtitle_ready_event.wait(timeout=timeout)

    def regenerate_segment(self, seg_num: int) -> bool:
        """
        Recreate a live segment that was deleted by ffmpeg's sliding-window
        cleanup.  Uses get_playing_at() with the approximate wall-clock time
        when that segment was produced to find the source file and offset.
        Returns True if the segment file was successfully written.
        """
        if self._last_launch_wall_time == 0.0:
            return False
        approx_ts = self._last_launch_wall_time + seg_num * HLS_SEGMENT_SECONDS
        result = get_playing_at(self.channel, datetime.fromtimestamp(approx_ts))
        if result is None:
            return False
        entry, offset_sec = result
        seg_path = os.path.join(self.hls_dir, f"seg{seg_num}.ts")
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-ss", str(offset_sec),
            "-t", str(HLS_SEGMENT_SECONDS),
            "-i", entry.path,
            "-c:v", "copy",
            "-c:a", "aac", "-b:a", "192k", "-ac", "2",
            "-map", "0:v:0", "-map", "0:a:0",
            "-f", "mpegts", seg_path,
        ]
        try:
            log.debug("Live regen seg%d for %s: %s", seg_num, self.channel.id, " ".join(cmd))
            subprocess.run(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=15,
            )
            return os.path.exists(seg_path) and os.path.getsize(seg_path) > 0
        except Exception:
            log.exception("Live regen failed for %s seg%d", self.channel.id, seg_num)
            return False

    def _watch_ready(self):
        """Background thread: set _ready_event once the HLS manifest exists and
        at least _ready_segments segments have been written.  Defaults to 1 so
        the manifest is served as soon as the first segment is available (~2s),
        matching the original design.  Players buffer ahead so there is no stutter.
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
        prev_codec = ""
        prev_width = 0
        prev_height = 0
        at_least_one = False  # True once first entry has been added to concat

        while accumulated < total_seconds:
            entry = entries[idx % n]
            # Detect codec or resolution change between consecutive entries.
            # Truncate the concat here so ffmpeg restarts at the boundary and the
            # server can inject #EXT-X-DISCONTINUITY for a clean decoder reset.
            cur_codec = entry.video_codec
            cur_w = entry.video_width
            cur_h = entry.video_height
            if at_least_one and prev_codec and cur_codec and (
                cur_codec != prev_codec
                or (cur_w and prev_width and cur_w != prev_width)
                or (cur_h and prev_height and cur_h != prev_height)
            ):
                self._codec_disc_pending = True
                log.debug(
                    "Channel %s: codec/res change at boundary (%s %dx%d → %s %dx%d) — "
                    "truncating concat, will inject DISCONTINUITY on restart",
                    self.channel.id, prev_codec, prev_width, prev_height,
                    cur_codec, cur_w, cur_h,
                )
                break
            if cur_codec:
                prev_codec = cur_codec
            if cur_w:
                prev_width = cur_w
            if cur_h:
                prev_height = cur_h
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
                if offset >= entry.duration_sec:
                    log.warning(
                        "Channel %s: inpoint %.1fs >= cached duration %.1fs for '%s' — "
                        "duration cache may be stale (file shorter than expected); "
                        "stream may skip to next entry",
                        self.channel.id, offset, entry.duration_sec, entry.title,
                    )
                lines.append(f"inpoint {offset:.3f}\n")
                accumulated += entry.duration_sec - offset
                first = False
            else:
                accumulated += entry.duration_sec
            idx += 1
            at_least_one = True

        with open(self.concat_path, "w", encoding="utf-8") as f:
            f.writelines(lines)

        return True

    # ------------------------------------------------------------------
    # ffmpeg process
    # ------------------------------------------------------------------

    def _get_subtitle_langs(self):
        """Collect named external SRT language codes available across this channel's entries.

        Excludes the empty-string key (unlabeled .srt files) — we can't know
        the language, so advertising it as a track is misleading and unhelpful.
        """
        langs = set()
        for entry in self.channel.entries:
            langs.update(k for k in entry.subtitle_paths.keys() if k)
        return sorted(langs)

    def _launch(self):
        self._last_launch_wall_time = time.time()
        self._ready_event.clear()
        with self._lock:
            # Stop any running subtitle processes before cleaning up the directory
            for sub in self._subtitle_streamers.values():
                sub.stop()
            self._subtitle_streamers.clear()

            # Remove stale HLS segments and manifest from any previous run so the
            # player doesn't get confused by timestamp mismatches on restart.
            # Also remove stale SRT side-output files so the new run starts fresh.
            for fname in os.listdir(self.hls_dir) if os.path.isdir(self.hls_dir) else []:
                if fname.endswith(".ts") or fname.endswith(".m3u8") \
                        or fname.endswith(".srt"):
                    try:
                        os.remove(os.path.join(self.hls_dir, fname))
                    except OSError:
                        pass

            # Start _watch_ready AFTER stale cleanup so it never sees old .ts files
            # and triggers a false-positive is_transition_ready() before ffmpeg starts.
            threading.Thread(
                target=self._watch_ready, daemon=True, name=f"ready-{self.channel.id}"
            ).start()

            if not self._build_concat():
                log.error("Cannot build concat for %s — no entries?", self.channel.id)
                return

            # Always transcode audio to AAC so any source codec (DTS, EAC3,
            # AC3, TrueHD, …) works transparently without per-channel detection.
            # On a modern CPU the overhead is negligible (~1-3% per core per channel).
            audio_opts = ["-c:a", "aac", "-b:a", "192k", "-ac", "2"]

            # HDR metadata stripping via hevc_metadata bitstream filter.
            # hevc_metadata is HEVC-only — applying it to H.264 data crashes ffmpeg.
            # Only enable when EVERY entry in the channel is flagged HDR, which
            # guarantees all entries are HEVC (scanner only sets is_hdr for HEVC).
            # Mixed HDR+SDR channels are skipped; HDR episodes appear normal on SDR
            # displays but at least the stream doesn't break.
            hdr_entries = [e for e in self.channel.entries if e.is_hdr]
            apply_hdr_bsf = bool(hdr_entries) and len(hdr_entries) == len(self.channel.entries)
            video_bsf_opts = (
                ["-bsf:v", "hevc_metadata=colour_primaries=1:transfer_characteristics=1:matrix_coefficients=1"]
                if apply_hdr_bsf else []
            )
            if hdr_entries and not apply_hdr_bsf:
                log.info(
                    "Channel %s: %d/%d entries are HDR — skipping BSF (mixed channel, "
                    "HEVC-only filter would break non-HEVC segments)",
                    self.channel.id, len(hdr_entries), len(self.channel.entries),
                )
            elif apply_hdr_bsf:
                log.info("Channel %s: all entries HDR — stripping HDR metadata via hevc_metadata BSF",
                         self.channel.id)

            # --- Subtitles ---
            subtitle_langs = self._get_subtitle_langs()
            self._subtitle_ready_event.clear()
            self._live_srt_langs = set()
            self._live_srt_indices = {}
            log.debug(
                "Channel %s: subtitle langs from entries: %s",
                self.channel.id, subtitle_langs or "none",
            )

            # Write placeholder VTTs before NAS prewarm so hls_sub_manifest returns
            # an empty stub (not 404) the instant the master playlist declares the track.
            if subtitle_langs:
                os.makedirs(self.hls_dir, exist_ok=True)
                for _lang in subtitle_langs:
                    _lang_label = _lang or "und"
                    _vtt_path = os.path.join(self.hls_dir, f"sub_{_lang_label}.vtt")
                    try:
                        with open(_vtt_path, "w", encoding="utf-8") as _f:
                            _f.write("WEBVTT\nX-TIMESTAMP-MAP=MPEGTS:0,LOCAL:00:00:00.000\n\n")
                    except OSError:
                        pass

            # Get now-playing early so we can probe the current entry for embedded subs.
            np = get_now_playing(self.channel)

            # Pre-warm the NAS disk cache FIRST — puts the file header, Cues element,
            # and seek cluster into NAS RAM.  Running this before any ffprobe call
            # means subtitle and audio probes hit warm cache (< 200ms) instead of
            # cold disk (2-10s each), which is the main driver of slow cold-start.
            if np and np.offset_sec > 0 and np.entry:
                _nas_prewarm(np.entry.path, np.offset_sec, np.entry.duration_sec)

            # Snap inpoint to the actual keyframe landing point to eliminate
            # real-time pre-inpoint decode delay under -re. NAS is now warm.
            actual_inpoint = np.offset_sec if np else 0.0
            launch_actual_inpoint_override = -1.0
            if np and np.offset_sec > 0 and np.entry:
                try:
                    snapped = _probe_keyframe_inpoint(
                        np.entry.path, np.offset_sec, np.entry.duration_sec, timeout=5
                    )
                    gop_size = _probe_gop_size(np.entry.path)
                    expected_fallback = max(0.0, np.offset_sec - gop_size)
                    is_real_keyframe = abs(snapped - expected_fallback) > 0.3
                    if is_real_keyframe and snapped < np.offset_sec - 0.1:
                        actual_inpoint = snapped
                        launch_actual_inpoint_override = snapped
                        with open(self.concat_path, 'r', encoding='utf-8') as _f:
                            _ct = _f.read()
                        _ct = re.sub(
                            r'(?m)^inpoint \S+$',
                            f'inpoint {snapped:.3f}',
                            _ct,
                            count=1,
                        )
                        with open(self.concat_path, 'w', encoding='utf-8') as _f:
                            _f.write(_ct)
                        log.info(
                            "Channel %s: inpoint snapped %.3fs → %.3fs (Δ=%.3fs, saving ~%.1fs bumper)",
                            self.channel.id, np.offset_sec, snapped,
                            np.offset_sec - snapped, np.offset_sec - snapped,
                        )
                except Exception:
                    pass

            if subtitle_langs:
                # For langs that have no external SRT on the *current* entry, probe
                # for an embedded subtitle stream.  The main ffmpeg process will write
                # it as an SRT side-output (piggyback — zero extra NAS I/O), and a
                # background watcher thread will update the VTT progressively.
                # External SRT always wins: only probe langs with no file on disk.
                current_entry = np.entry if np else None
                if current_entry:
                    embedded_for_probe = [
                        l for l in subtitle_langs
                        if not (current_entry.subtitle_paths.get(l)
                                and os.path.exists(current_entry.subtitle_paths[l]))
                    ]
                    if embedded_for_probe:
                        self._live_srt_indices = _probe_subtitle_stream_indices(
                            current_entry.path, embedded_for_probe
                        )
                        self._live_srt_langs = set(self._live_srt_indices.keys())
                        if self._live_srt_langs:
                            log.info(
                                "Channel %s: ffmpeg SRT side outputs for langs=%s",
                                self.channel.id, sorted(self._live_srt_langs),
                            )

                # Register SubtitleStreamers immediately (no SRT I/O here).
                # SRT reading (build_cues) runs in the async thread in parallel
                # with ffmpeg startup, so _launch() returns without any NAS reads.
                for lang in subtitle_langs:
                    sub = SubtitleStreamer(self.channel, lang, self.hls_dir,
                                          subtitle_background=self._subtitle_background)
                    sub.write_placeholder()
                    if lang in self._live_srt_langs:
                        sub.has_ffmpeg_srt = True
                    self._subtitle_streamers[lang] = sub

                # Async thread: read SRTs, wait for first TS segment, probe
                # start_pts, write VTT files, set _subtitle_ready_event.
                _writer = LiveSubtitleWriter(
                    channel_id=self.channel.id,
                    hls_dir=self.hls_dir,
                    stop_event=self._stop_event,
                    subtitle_streamers=self._subtitle_streamers,
                    subtitle_ready_event=self._subtitle_ready_event,
                    live_srt_langs=self._live_srt_langs,
                    get_launch_time=lambda: self._last_launch_wall_time,
                    subtitle_background=self._subtitle_background,
                )
                threading.Thread(
                    target=_writer.write_subtitle_files_async,
                    args=(subtitle_langs,),
                    kwargs={
                        "launch_inpoint": np.offset_sec if np else 0.0,
                        "launch_actual_inpoint": launch_actual_inpoint_override,
                        "launch_entry_path": np.entry.path if (np and np.entry) else None,
                        "launch_entry_duration": np.entry.duration_sec if (np and np.entry) else 0.0,
                    },
                    daemon=True,
                    name=f"sub-write-{self.channel.id}",
                ).start()
                sub_opts = []
            else:
                # No external SRTs: try embedded text subtitles via the video process.
                self._subtitle_ready_event.set()   # no subs → immediately ready
                # Map ALL subtitle streams (not just s:0) so players can choose
                # language.  Bitmap subs (PGS/VOBSUB) trigger the monitor's
                # "bitmap to bitmap" detection and disable subs cleanly on restart.
                sub_opts = ["-map", "0:s?", "-c:s", "webvtt"] if self._subtitles else []

            # Probe the current file for the preferred audio track.  Files with
            # multiple audio languages (e.g. French + English) often store the
            # non-English track first; probing ensures we select the right one.
            # NAS prewarm already ran above, so the file header is cached.
            audio_idx = 0
            if np and np.entry:
                audio_idx = _probe_audio_stream_index(
                    np.entry.path, self._preferred_audio_language
                )

            seg_pattern = os.path.join(self.hls_dir, "seg%d.ts")
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
                *video_bsf_opts,
                "-map", "0:v:0",
                "-map", f"0:a:{audio_idx}",
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
            # SRT side outputs: one per embedded-subtitle lang, written at -re rate.
            # Piggybacks on the main ffmpeg read — zero extra NAS I/O.
            # -flush_packets 1: force packet-level flushing so the watcher thread
            # sees data immediately rather than waiting for the 32 KB avio buffer.
            for lang, idx in self._live_srt_indices.items():
                lang_label = lang or "und"
                srt_out = os.path.join(self.hls_dir, f"sub_{lang_label}.srt")
                cmd += ["-vn", "-an", "-map", f"0:s:{idx}",
                        "-flush_packets", "1", "-c:s", "srt", srt_out]
            log.debug("ffmpeg cmd: %s", " ".join(cmd))
            self._process = subprocess.Popen(
                cmd,
                stdin=subprocess.DEVNULL,   # no terminal stdin — prevents tty state corruption
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                start_new_session=True,
            )

        if np:
            self._start_concat_lookahead_prewarm(np, self._last_launch_wall_time)

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
    # Concat lookahead prewarm thread
    # ------------------------------------------------------------------

    def _start_concat_lookahead_prewarm(self, np: NowPlaying, launch_time: float) -> None:
        """Spawn a daemon thread that prewarms upcoming concat entries into NAS RAM
        ~CONCAT_PREWARM_LEAD seconds before ffmpeg opens each one."""
        threading.Thread(
            target=self._concat_prewarm_worker,
            args=(np, launch_time),
            daemon=True,
            name=f"prewarm-{self.channel.id}",
        ).start()

    def _concat_prewarm_worker(self, np: NowPlaying, launch_time: float) -> None:
        entries = self.channel.entries
        n = len(entries)
        if n == 0:
            return

        # Time until ffmpeg finishes the first (current) entry from its inpoint
        accumulated = np.entry.duration_sec - np.offset_sec
        idx = (np.entry_index + 1) % n

        while not self._stop_event.is_set():
            # Bail if _launch() was called again (stall-restart or concat refresh)
            if self._last_launch_wall_time != launch_time:
                return

            # Stop after the concat window — nothing beyond this is in the concat file
            if accumulated > CONCAT_HOURS * 3600:
                return

            entry = entries[idx]
            wake_time = launch_time + accumulated - CONCAT_PREWARM_LEAD
            sleep_secs = wake_time - time.time()
            if sleep_secs > 0:
                self._stop_event.wait(timeout=sleep_secs)
                if self._stop_event.is_set():
                    return
                if self._last_launch_wall_time != launch_time:
                    return

            log.debug(
                "Concat lookahead prewarm: channel=%s entry=%s",
                self.channel.id, entry.title,
            )
            _nas_prewarm_header(entry.path)

            accumulated += entry.duration_sec
            idx = (idx + 1) % n

    # ------------------------------------------------------------------
    # Monitor thread — restart ffmpeg when it exits
    # ------------------------------------------------------------------

    def _get_manifest_mtime(self) -> float:
        try:
            return os.path.getmtime(self.manifest_path)
        except OSError:
            return 0.0

    def _monitor(self):
        # Two-tier stall detection:
        #   STARTUP_TIMEOUT — how long to wait for the FIRST segment.  Files with a
        #     proper seek index (MKV Cues / MP4 moov) start in 2-3s.  60s allows
        #     long-GOP HEVC content (GOP up to ~50s observed in cartoon encodes) to
        #     reach the first keyframe boundary before the segment is written.
        #     Genuinely broken files (ffmpeg exits with error) are caught by the
        #     ret != 0 path, not by this timeout, so slow recovery is not a concern.
        #   RUNNING_TIMEOUT — how long to wait for the NEXT segment once the channel is
        #     already running.  A 30s gap mid-stream is a genuine stall.
        STARTUP_TIMEOUT = 60
        RUNNING_TIMEOUT = 30
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
                # Use the longer startup timeout until the manifest exists (first segment written)
                timeout = RUNNING_TIMEOUT if os.path.exists(self.manifest_path) else STARTUP_TIMEOUT
                if now - last_check >= timeout:
                    mtime = self._get_manifest_mtime()
                    if mtime == last_mtime:
                        if not os.path.exists(self.manifest_path):
                            log.warning(
                                "Channel %s stalled with no output after %ds — restarting ffmpeg",
                                self.channel.id, timeout,
                            )
                        else:
                            log.warning(
                                "Channel %s stalled — no new segments in %ds, restarting ffmpeg",
                                self.channel.id, RUNNING_TIMEOUT,
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
# BumperStreamer / BumperManager — always-on loading screen streams
# ---------------------------------------------------------------------------



class StreamManager:
    """
    Owns all channel streamers.  ffmpeg is started lazily — only when a client
    first requests /hls/<channel_id>/stream.m3u8 (via ensure_started).
    """

    def __init__(self, tmp_base: str = "/tmp/fakeiptv", subtitles: bool = True,
                 audio_copy: bool = True, prewarm_timeout: int = IDLE_TIMEOUT_PREWARM,
                 ready_segments: int = 3, session_mode: bool = False,
                 prewarm_adjacent: int = 0, preferred_audio_language: str = "eng",
                 bumpers_path: str = "", bumpers_cache_dir: str = "",
                 subtitle_background: bool = True):
        self._tmp_base = tmp_base
        self._subtitles = subtitles
        self._subtitle_background = subtitle_background
        self._audio_copy = audio_copy
        self._preferred_audio_language = preferred_audio_language
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
        # Channels that have been watched at least once this session.
        # Persists across streamer deletions so recreated streamers get the
        # 600s watched timeout instead of the 120s prewarm timeout.
        self._watched_channels: set = set()
        self._lock = threading.Lock()
        self._reaper = threading.Thread(
            target=self._reap_loop, daemon=True, name="stream-reaper"
        )
        self._reaper.start()
        threading.Thread(
            target=self._global_prewarm_loop, daemon=True, name="global-prewarm"
        ).start()
        # Bumper loading screens — started once at init if bumpers_path is set
        self._bumper_manager: Optional[BumperManager] = None
        if bumpers_path:
            self._bumper_manager = BumperManager(
                bumpers_path, tmp_base, bumpers_cache_dir or tmp_base
            )
            self._bumper_manager.start_all()

    def ensure_started(self, ch_id: str, background: bool = False, is_prewarm: bool = False) -> bool:
        """
        Start the ffmpeg streamer for ch_id if it isn't already running.
        Returns False if the channel is unknown, True otherwise.
        Call this before waiting for is_ready().

        s.start() is called OUTSIDE the lock because _launch() reads SRT files
        from NAS (2-3s per channel).  Holding the lock during that would
        serialise all concurrent prewarm/ensure_started calls, causing 30s+ delays.
        The streamer is registered in _streamers before start() so a second
        concurrent call for the same ch_id sees it and skips.

        background=True: run start() in a daemon thread so NAS probes and prewarm
        don't block the Flask request.  The streamer is still registered
        synchronously, so subsequent ensure_started() calls are no-ops.
        Use when a bumper is available to fill the loading gap.
        """
        # When a bumper will cover the cold-start gap, read the bumper's current
        # MEDIA-SEQUENCE BEFORE taking the lock (file I/O outside critical section).
        # We start the channel's HLS at a higher sequence number so the player's
        # tracker sees a forward jump (bumper→channel) rather than a backward one.
        # A backward jump makes the player think it already saw real segments and
        # stall 6-10s for a master-playlist re-poll to reset its tracking.
        hls_start = 0
        if background and self._bumper_manager is not None:
            bumper = self._bumper_manager.get_random_ready()
            if bumper is not None:
                hls_start = bumper.current_seq() + 100

        new_streamer = None
        with self._lock:
            if ch_id not in self._channels:
                return False
            if ch_id not in self._streamers:
                new_streamer = ChannelStreamer(self._channels[ch_id], self._tmp_base,
                                              self._subtitles,
                                              audio_copy=self._audio_copy,
                                              prewarm_timeout=self._prewarm_timeout,
                                              ready_segments=self._ready_segments,
                                              preferred_audio_language=self._preferred_audio_language,
                                              hls_start_number=hls_start,
                                              subtitle_background=self._subtitle_background)
                if not is_prewarm and ch_id in self._watched_channels:
                    new_streamer._ever_watched = True  # restore 600s timeout after idle stop
                self._streamers[ch_id] = new_streamer  # register before start()
            elif hls_start > 0:
                # Channel already running (prewarmed). Apply seq_offset now so the
                # bumper→real manifest hand-off doesn't produce a backward
                # MEDIA-SEQUENCE jump.  A backward jump makes ExoPlayer think it
                # already saw those segments and it stalls until its ABR re-check
                # fires (~30s), which is exactly the "4 loops" symptom.
                existing = self._streamers[ch_id]
                if existing._seq_offset == 0:
                    existing._seq_offset = hls_start
            if self._session_mode:
                self._last_global_touch = time.time()
        if new_streamer is not None:
            if background:
                threading.Thread(
                    target=new_streamer.start, daemon=True,
                    name=f"start-{ch_id}",
                ).start()
            else:
                new_streamer.start()  # outside lock — NAS I/O in _launch() runs in parallel
        return True

    def touch(self, ch_id: str):
        """Signal that a client is actively fetching this channel."""
        self._watched_channels.add(ch_id)
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
        if self._bumper_manager:
            self._bumper_manager.stop_all()

    def get_random_bumper(self) -> Optional[BumperStreamer]:
        """Return a random ready BumperStreamer, or None if unavailable."""
        if self._bumper_manager is None:
            return None
        return self._bumper_manager.get_random_ready()

    def get_bumper_by_id(self, bumper_id: str) -> Optional[BumperStreamer]:
        if self._bumper_manager is None:
            return None
        return self._bumper_manager.get_by_id(bumper_id)

    def get_hls_dir(self, ch_id: str) -> Optional[str]:
        s = self._streamers.get(ch_id)
        return s.hls_dir if s else None

    def get_seq_offset(self, ch_id: str) -> int:
        """Return the MEDIA-SEQUENCE offset to add when serving video.m3u8."""
        s = self._streamers.get(ch_id)
        return s._seq_offset if s else 0

    def pop_codec_disc(self, ch_id: str) -> bool:
        """Return True (and clear the flag) if a codec/resolution change restart just occurred."""
        s = self._streamers.get(ch_id)
        if s and s._codec_disc_pending:
            s._codec_disc_pending = False
            return True
        return False

    def is_ready(self, ch_id: str) -> bool:
        s = self._streamers.get(ch_id)
        return s.is_ready() if s else False

    def is_transition_ready(self, ch_id: str, min_segments: int = 2) -> bool:
        """
        True when the channel has at least min_segments TS files on disk.

        2 segments (4s) is enough for ExoPlayer to start smoothly after the
        #EXT-X-DISCONTINUITY that separates the bumper from real content.
        """
        s = self._streamers.get(ch_id)
        if not s or not s.is_ready():
            return False
        try:
            count = sum(1 for f in os.listdir(s.hls_dir) if f.endswith(".ts"))
            return count >= min_segments
        except OSError:
            return False

    def is_subtitle_ready(self, ch_id: str) -> bool:
        """True once subtitle VTTs have been written with a correct MPEGTS anchor.

        Returns True for channels with no subtitle tracks (event is set immediately
        in _launch() when subtitle_langs is empty).
        """
        s = self._streamers.get(ch_id)
        if s is None:
            return True
        return s._subtitle_ready_event.is_set()

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
                        preferred_audio_language=self._preferred_audio_language,
                        subtitle_background=self._subtitle_background,
                    )
                    s.start()
                    self._streamers[ch_id] = s
                else:
                    # Paths unchanged but metadata (durations, ratings, etc.) may
                    # have been re-probed at refresh time.  Update the channel
                    # reference so the next _build_concat() uses correct durations
                    # and stays in sync with the EPG.  The running ffmpeg process
                    # is unaffected — it continues from its current concat file.
                    self._streamers[ch_id].channel = channels[ch_id]

            # Replace channel registry (new channels are NOT started here)
            self._channels = dict(channels)
            self._channel_order = list(channels.keys())

    def get_subtitle_languages(self, ch_id: str):
        """
        Return sorted list of subtitle language codes for ch_id.

        Returns all languages registered for the channel (i.e. present in at
        least one entry) — not just ones whose VTT has been written yet.  The
        VTT endpoint independently waits up to 15s for the file to appear, so
        it is safe to include a language in the master playlist before its VTT
        is written.  This removes the need to block the manifest response on
        subtitle readiness.
        """
        s = self._streamers.get(ch_id)
        if s is None:
            return []
        # Prefer the registered _subtitle_streamers (populated in _launch)
        # since they reflect the actual langs for the current concat window.
        # Fall back to _get_subtitle_langs() if the streamer hasn't launched yet.
        if s._subtitle_streamers:
            return sorted(k for k in s._subtitle_streamers.keys() if k)
        return s._get_subtitle_langs()

    def wait_subtitle_ready(self, ch_id: str, timeout: float = 10.0) -> bool:
        """
        Block until subtitle files are written for ch_id.
        Returns True if ready within timeout.  Safe to call even if the
        channel has no subtitle tracks (event is set immediately in that case).
        """
        s = self._streamers.get(ch_id)
        if s is None:
            return True
        return s.wait_subtitle_ready(timeout)

    def has_active_streamers(self) -> bool:
        """True if any channel is currently running."""
        return bool(self._streamers)

    def regenerate_segment(self, ch_id: str, seg_num: int) -> bool:
        """Re-create a deleted live segment on demand. Returns True if successful."""
        s = self._streamers.get(ch_id)
        return s.regenerate_segment(seg_num) if s else False

    def _global_prewarm_loop(self):
        """Background thread: periodically warm all channels' upcoming entries into NAS RAM.

        Runs every GLOBAL_PREWARM_INTERVAL seconds. Warms the current seek position
        plus the next 2 entries for every channel, regardless of whether ffmpeg is
        running. Active channels also have per-episode _concat_prewarm_worker threads
        for precise timing; this sweep provides coverage for inactive channels.
        """
        # Wait for reload() to populate _channels before first sweep
        while not self._channels:
            time.sleep(5)
        self._global_prewarm_once()
        while True:
            time.sleep(GLOBAL_PREWARM_INTERVAL)
            self._global_prewarm_once()

    def _global_prewarm_once(self):
        channels = list(self._channels.values())  # snapshot outside lock
        log.info("Global NAS prewarm: sweeping %d channels", len(channels))
        warmed = 0
        for ch in channels:
            np = get_now_playing(ch)
            if not np or not np.entry:
                continue
            entries = ch.entries
            n = len(entries)
            if n == 0:
                continue
            # Warm current entry at seek offset — same read _launch() does,
            # so cold-start finds it already cached.
            _nas_prewarm(np.entry.path, np.offset_sec, np.entry.duration_sec)
            # Warm next 2 upcoming entries (opened from start by ffconcat).
            for i in range(1, 3):
                _nas_prewarm_header(entries[(np.entry_index + i) % n].path)
            warmed += 1
            time.sleep(0.5)  # stagger to avoid NAS burst
        log.info("Global NAS prewarm: done (%d/%d channels warmed)", warmed, len(channels))

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
CATCHUP_FFMPEG_IDLE = 120    # stop ffmpeg after 120s with no segment fetches

# Trailing segment buffer for rolling delete.  Segments behind HWM - KEEP are
# deleted immediately; on-demand regen recreates them if the player rewinds.
CATCHUP_KEEP_SEGMENTS = 30   # 30 × 2s = 60s trailing buffer


