"""
catchup.py — Catchup VOD (on-demand playback) sessions with subtitle handling.
"""
import logging
import os
import shutil
import subprocess
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, Optional

from .models import Channel, ScheduleEntry
from .scheduler import get_playing_at

HLS_SEGMENT_SECONDS = 2
from .ffprobe_utils import (
    _nas_prewarm, _probe_audio_stream_index, _probe_subtitle_stream_indices,
    _probe_segment_start_pts,
)
from .subtitle_utils import (
    _read_srt, _parse_srt_cues, _sec_to_vtt_ts, _he_bidi_fix,
    _extract_embedded_srt,
)

log = logging.getLogger(__name__)

CATCHUP_SESSION_TTL = 2 * 3600  # 2 hours
CATCHUP_FFMPEG_IDLE = 30  # seconds
CATCHUP_KEEP_SEGMENTS = 15  # rolling delete window


class CatchupSession:
    """One temporary ffmpeg VOD process for a single catchup request."""

    _LANG_NAMES = {
        "he": "Hebrew", "en": "English", "es": "Spanish", "fr": "French",
        "de": "German", "ar": "Arabic", "ru": "Russian", "pt": "Portuguese",
        "it": "Italian", "nl": "Dutch", "pl": "Polish", "cs": "Czech",
        "ja": "Japanese", "ko": "Korean", "zh": "Chinese", "": "Subtitles",
    }

    _ALWAYS_SUBTITLE_LANGS = ["he", "en"]

    def __init__(self, session_id: str, entry: ScheduleEntry, offset_sec: float,
                 duration_sec: float, session_dir: str, subtitles: bool,
                 preferred_audio_language: str = "eng", is_seek: bool = False,
                 subtitle_background: bool = True):
        self.session_id = session_id
        self.entry = entry
        self.offset_sec = offset_sec
        self.duration_sec = duration_sec
        self.session_dir = session_dir
        self.subtitles = subtitles
        self._preferred_audio_language = preferred_audio_language
        self.is_seek = is_seek
        self._subtitle_background = subtitle_background
        self.manifest_path = os.path.join(session_dir, "stream.m3u8")
        self._process: Optional[subprocess.Popen] = None
        self._last_accessed = time.time()
        self._last_fetch_time = time.time()
        self._audio_idx: int = 0
        self._hwm: int = -1
        self._last_deleted: int = -1
        self._regen_events: Dict[int, threading.Event] = {}
        self._regen_lock = threading.Lock()
        self._subs_ready = threading.Event()

    def touch(self):
        self._last_accessed = time.time()

    def is_expired(self) -> bool:
        return (time.time() - self._last_accessed) > CATCHUP_SESSION_TTL

    def is_ffmpeg_idle(self) -> bool:
        """True if ffmpeg is running but no segment has been fetched recently."""
        return (self._process is not None
                and self._process.poll() is None
                and (time.time() - self._last_fetch_time) > CATCHUP_FFMPEG_IDLE)

    def stop_ffmpeg(self):
        """Terminate the ffmpeg process without deleting session files."""
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
        log.info("Catchup %s: ffmpeg idle — process stopped, session kept", self.session_id)

    def start(self):
        os.makedirs(self.session_dir, exist_ok=True)
        _nas_prewarm(self.entry.path, self.offset_sec, self.entry.duration_sec)
        audio_idx = _probe_audio_stream_index(self.entry.path, self._preferred_audio_language)
        self._audio_idx = audio_idx
        seg_pattern = os.path.join(self.session_dir, "seg%d.ts")
        video_manifest = os.path.join(self.session_dir, "video.m3u8")

        # Determine subtitle languages to write as VTT sidecar files.
        # VTTs are written asynchronously in _write_subs_and_master() after
        # the first segment is produced (so start_pts can be probed for
        # correct X-TIMESTAMP-MAP alignment).
        self._sub_langs = []
        if self.subtitles:
            candidate_langs = list(self._ALWAYS_SUBTITLE_LANGS)
            for lang in self.entry.subtitle_paths:
                if lang not in candidate_langs:
                    candidate_langs.append(lang)
            # External SRTs
            for lang in candidate_langs:
                if (self.entry.subtitle_paths.get(lang)
                        and os.path.exists(self.entry.subtitle_paths[lang])):
                    self._sub_langs.append(lang)
            # Fallback: embedded subtitle streams when no external SRTs
            if not self._sub_langs:
                no_srt_langs = [l for l in candidate_langs if l]
                embedded = _probe_subtitle_stream_indices(self.entry.path, no_srt_langs)
                self._sub_langs = sorted(embedded.keys())

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "error",
            "-ss", str(self.offset_sec),
            "-re",
            "-avoid_negative_ts", "make_zero",
            "-i", self.entry.path,
            "-c:v", "copy",
            "-c:a", "aac", "-b:a", "192k", "-ac", "2",
            "-map", "0:v:0",
            "-map", f"0:a:{audio_idx}",
            "-t", str(self.duration_sec),
            "-f", "hls",
            "-hls_time", str(HLS_SEGMENT_SECONDS),
            "-hls_list_size", "0",
            "-hls_segment_filename", seg_pattern,
            video_manifest,
        ]
        log.debug("Catchup ffmpeg: %s", " ".join(cmd))
        self._process = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        threading.Thread(
            target=self._monitor_stderr,
            daemon=True,
            name=f"catchup-stderr-{self.session_id}",
        ).start()
        threading.Thread(
            target=self._write_subs_and_master,
            daemon=True,
            name=f"catchup-subs-{self.session_id}",
        ).start()

    def _monitor_stderr(self):
        """Read ffmpeg stderr and log warnings/errors."""
        proc = self._process
        if not proc:
            return
        stderr_lines = []
        try:
            for raw in proc.stderr:
                line = raw.decode(errors="replace").rstrip()
                if line:
                    stderr_lines.append(line)
        except Exception:
            pass
        ret = proc.wait()
        if ret != 0 or stderr_lines:
            level = log.warning if ret != 0 else log.debug
            for line in stderr_lines:
                level("Catchup %s ffmpeg: %s", self.session_id, line)
            if ret != 0:
                log.warning(
                    "Catchup %s ffmpeg exited with code %d", self.session_id, ret
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

    def _write_subs_and_master(self):
        """Background thread: probe start_pts, write per-language VTTs, write master."""
        seg0 = os.path.join(self.session_dir, "seg0.ts")
        deadline = time.time() + 35
        while not os.path.exists(seg0):
            if time.time() > deadline or (
                self._process and self._process.poll() is not None
            ):
                break
            time.sleep(0.2)

        start_pts = 0
        if os.path.exists(seg0):
            start_pts = _probe_segment_start_pts(seg0) or 0
            log.info("Catchup %s: subtitle TIMESTAMP-MAP → MPEGTS:%d",
                     self.session_id, start_pts)

        for lang in self._sub_langs:
            n = self._write_vtt_for_lang(lang, start_pts)
            log.info("Catchup %s: wrote %s VTT, %d cues (MPEGTS:%d)",
                     self.session_id, lang or "und", n, start_pts)

        self._write_master(self._sub_langs)
        self._subs_ready.set()
        log.info("Catchup %s: master written for langs=%s",
                 self.session_id, self._sub_langs or "none")

    def _write_vtt_for_lang(self, lang: str, start_pts: int) -> int:
        """Write a WebVTT file for one subtitle language. Returns cue count."""
        lang_label = lang or "und"
        vtt_path = os.path.join(self.session_dir, f"sub_{lang_label}.vtt")
        srt_path = self.entry.subtitle_paths.get(lang, "")
        is_rtl = (lang == "he")

        raw = ""
        inpoint = self.offset_sec
        srt_offset = 0.0
        if srt_path and os.path.exists(srt_path):
            raw = _read_srt(srt_path)
            cues_tmp = _parse_srt_cues(raw)
            if cues_tmp:
                first = min(s for s, e, t in cues_tmp)
                srt_offset = first if first > 300.0 else 0.0
        else:
            raw = _extract_embedded_srt(
                self.entry.path, lang, inpoint, self.duration_sec, timeout=20
            )
            inpoint = 0.0  # embedded extraction already starts from inpoint

        cue_lines = []
        if raw:
            for cue_start, cue_end, text in _parse_srt_cues(raw):
                s_adj = (cue_start - srt_offset) - inpoint
                e_adj = (cue_end - srt_offset) - inpoint
                if e_adj <= 0 or s_adj < 0:
                    continue
                if s_adj >= self.duration_sec:
                    break
                if is_rtl:
                    text = "\n".join(_he_bidi_fix(l) for l in text.split("\n"))
                cue_lines.append(
                    f"{_sec_to_vtt_ts(s_adj)} --> {_sec_to_vtt_ts(e_adj)}\n"
                    f"{text}\n\n"
                )

        with open(vtt_path, "w", encoding="utf-8") as f:
            f.write("WEBVTT\n")
            f.write(f"X-TIMESTAMP-MAP=MPEGTS:{start_pts},LOCAL:00:00:00.000\n\n")
            if not self._subtitle_background:
                f.write("STYLE\n::cue {\n  background-color: rgba(0,0,0,0.6);\n}\n\n")
            f.writelines(cue_lines)
        return len(cue_lines)

    def _write_master(self, sub_langs):
        """Write stream.m3u8 master playlist pointing to video.m3u8."""
        lines = ["#EXTM3U\n"]
        if sub_langs:
            for lang in sub_langs:
                lang_label = lang or "und"
                name = self._LANG_NAMES.get(lang, lang.upper() or "Subtitles")
                lines.append(
                    f'#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",'
                    f'LANGUAGE="{lang_label}",NAME="{name}",'
                    f'DEFAULT=NO,AUTOSELECT=NO,'
                    f'URI="sub_{lang_label}.m3u8"\n'
                )
            lines.append('#EXT-X-STREAM-INF:BANDWIDTH=8000000,SUBTITLES="subs"\n')
        else:
            lines.append("#EXT-X-STREAM-INF:BANDWIDTH=8000000\n")
        lines.append("video.m3u8\n")
        with open(self.manifest_path, "w", encoding="utf-8") as f:
            f.writelines(lines)

    def is_ready(self) -> bool:
        return os.path.exists(self.manifest_path)

    def is_failed(self) -> bool:
        """True if ffmpeg exited without producing the manifest."""
        return (
            self._process is not None
            and self._process.poll() is not None
            and not os.path.exists(self.manifest_path)
        )

    def is_done(self) -> bool:
        """True once ffmpeg has finished and video.m3u8 has EXT-X-ENDLIST."""
        if self._process is None or self._process.poll() is None:
            return False
        video_path = os.path.join(self.session_dir, "video.m3u8")
        try:
            with open(video_path) as f:
                return "#EXT-X-ENDLIST" in f.read()
        except Exception:
            return False

    def has_been_watched(self) -> bool:
        """True if the player has fetched at least one segment."""
        return self._hwm >= 0

    def mark_fetched(self, seg_num: int):
        """Called after a .ts segment is served. Advances the high-water mark."""
        self._last_fetch_time = time.time()
        with self._regen_lock:
            if seg_num > self._hwm:
                self._hwm = seg_num
            delete_before = self._hwm - CATCHUP_KEEP_SEGMENTS
            if delete_before > self._last_deleted:
                for n in range(self._last_deleted + 1, delete_before + 1):
                    try:
                        os.remove(os.path.join(self.session_dir, f"seg{n}.ts"))
                    except OSError:
                        pass
                self._last_deleted = delete_before

    def regenerate_segment(self, seg_num: int) -> bool:
        """Recreate a deleted segment on demand (player rewound)."""
        with self._regen_lock:
            if seg_num in self._regen_events:
                evt = self._regen_events[seg_num]
                is_initiator = False
            else:
                evt = threading.Event()
                self._regen_events[seg_num] = evt
                is_initiator = True

        if not is_initiator:
            evt.wait(timeout=15)
            seg_path = os.path.join(self.session_dir, f"seg{seg_num}.ts")
            return os.path.exists(seg_path) and os.path.getsize(seg_path) > 0

        seg_path = os.path.join(self.session_dir, f"seg{seg_num}.ts")
        try:
            start_sec = self.offset_sec + seg_num * HLS_SEGMENT_SECONDS
            cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-ss", str(start_sec),
                "-t", str(HLS_SEGMENT_SECONDS),
                "-i", self.entry.path,
                "-c:v", "copy", "-c:a", "aac", "-b:a", "192k", "-ac", "2",
                "-map", "0:v:0", "-map", f"0:a:{self._audio_idx}",
                "-f", "mpegts", seg_path,
            ]
            log.debug("Catchup regen seg%d: %s", seg_num, " ".join(cmd))
            subprocess.run(
                cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=15,
            )
            result = os.path.exists(seg_path) and os.path.getsize(seg_path) > 0
            if result:
                log.debug("Catchup %s: regenerated seg%d", self.session_id, seg_num)
            else:
                log.warning("Catchup %s: regen failed for seg%d", self.session_id, seg_num)
            return result
        except Exception:
            log.exception("Catchup %s: regen exception for seg%d", self.session_id, seg_num)
            return False
        finally:
            evt.set()
            with self._regen_lock:
                self._regen_events.pop(seg_num, None)


class CatchupManager:
    """Creates and manages CatchupSession instances."""

    def __init__(self, tmp_base: str, subtitles: bool = True,
                 preferred_audio_language: str = "eng", subtitle_background: bool = True):
        self._tmp_base = tmp_base
        self._subtitles = subtitles
        self._preferred_audio_language = preferred_audio_language
        self._subtitle_background = subtitle_background
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
        """Find or create a catchup session for `channel` at datetime `at`."""
        result = get_playing_at(channel, at)
        if result is None:
            return None

        entry, offset_sec = result
        remaining = entry.duration_sec - offset_sec

        now = datetime.now()
        if at > now:
            log.info(
                "Catchup %s: utc %s is in the future (now=%s) — treating as start-over",
                channel.id, at.isoformat(), now.isoformat(),
            )
            live_result = get_playing_at(channel, now)
            if live_result is None:
                return None
            entry, _ = live_result
            offset_sec = 0.0
            remaining = entry.duration_sec

        SNAP_THRESHOLD = 2.0
        if remaining < SNAP_THRESHOLD:
            next_result = get_playing_at(channel, at + timedelta(seconds=remaining + 0.5))
            if next_result is not None:
                next_entry, next_off = next_result
                if next_off < SNAP_THRESHOLD:
                    next_off = 0.0
                entry, offset_sec = next_entry, next_off
            else:
                offset_sec = 0.0
            log.info(
                "Catchup %s: utc near boundary (%.3fs remaining in prev) — snapping forward to '%s'",
                channel.id, remaining, entry.title,
            )
            remaining = entry.duration_sec - offset_sec

        duration_sec = max(remaining, 5.0)
        ts = int(at.timestamp())

        with self._lock:
            REUSE_TOLERANCE = 60
            prefix = channel.id + "_"
            for sid, s in self._sessions.items():
                if sid.startswith(prefix):
                    try:
                        existing_ts = int(sid.rsplit("_", 1)[1])
                    except (ValueError, IndexError):
                        continue
                    if abs(existing_ts - ts) <= REUSE_TOLERANCE and not s.has_been_watched():
                        s.touch()
                        return s

            stale = [
                sid for sid, s in self._sessions.items()
                if sid.startswith(prefix) and (
                    abs(int(sid.rsplit("_", 1)[1]) - ts) > REUSE_TOLERANCE
                    or s.has_been_watched()
                )
            ]
            is_seek = any(
                s.entry.path == entry.path
                for sid, s in self._sessions.items()
                if sid.startswith(prefix)
            )
            for sid in stale:
                log.info("Evicting stale catchup session %s (new session for same channel)", sid)
                self._sessions[sid].stop()
                del self._sessions[sid]

            session_id = f"{channel.id}_{ts}"
            session_dir = os.path.join(self._tmp_base, "catchup", session_id)
            session = CatchupSession(
                session_id=session_id,
                entry=entry,
                offset_sec=offset_sec,
                duration_sec=duration_sec,
                session_dir=session_dir,
                subtitles=self._subtitles,
                preferred_audio_language=self._preferred_audio_language,
                is_seek=is_seek,
                subtitle_background=self._subtitle_background,
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
            time.sleep(60)
            with self._lock:
                expired = [sid for sid, s in self._sessions.items() if s.is_expired()]
                for sid in expired:
                    log.info("Expiring catchup session: %s", sid)
                    self._sessions[sid].stop()
                    del self._sessions[sid]
