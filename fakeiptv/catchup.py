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
    _probe_segment_start_pts, _probe_keyframe_inpoint, _probe_stream_start_time
)
from .subtitle_utils import (
    _read_srt, _parse_srt_cues, _sec_to_vtt_ts, _text_has_hebrew, _he_bidi_fix,
    _extract_embedded_srt
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

        self._sub_stream_indices: dict = {}
        if self.subtitles:
            always_langs = list(self._ALWAYS_SUBTITLE_LANGS)
            for lang in self.entry.subtitle_paths:
                if lang not in always_langs:
                    always_langs.append(lang)
            embedded_langs = [
                l for l in always_langs
                if l and not (self.entry.subtitle_paths.get(l) and
                              os.path.exists(self.entry.subtitle_paths[l]))
            ]
            if embedded_langs:
                self._sub_stream_indices = _probe_subtitle_stream_indices(
                    self.entry.path, embedded_langs
                )
                if self._sub_stream_indices:
                    log.debug(
                        "Catchup %s: SRT side outputs for langs=%s",
                        self.session_id, list(self._sub_stream_indices),
                    )

        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "error",
            "-ss", str(self.offset_sec),
            "-re",
            "-avoid_negative_ts", "make_zero",
            "-i", self.entry.path,
            "-t", str(self.duration_sec),
            "-c:v", "copy",
            "-c:a", "aac", "-b:a", "192k", "-ac", "2",
            "-map", "0:v:0",
            "-map", f"0:a:{audio_idx}",
            "-f", "hls",
            "-hls_time", str(HLS_SEGMENT_SECONDS),
            "-hls_list_size", "0",
            "-hls_segment_filename", seg_pattern,
            video_manifest,
        ]
        for lang, idx in self._sub_stream_indices.items():
            lang_label = lang or "und"
            srt_out = os.path.join(self.session_dir, f"sub_{lang_label}.srt")
            cmd += ["-vn", "-an", "-map", f"0:s:{idx}",
                    "-flush_packets", "1", "-c:s", "srt", srt_out]
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
        """Background thread: write HLS master playlist and subtitle VTTs."""
        seg0 = os.path.join(self.session_dir, "seg0.ts")
        deadline = time.time() + 35
        while not os.path.exists(seg0):
            if time.time() > deadline or (
                self._process and self._process.poll() is not None
            ):
                if self.subtitles:
                    self._write_placeholder_vtts_and_master(
                        self._ALWAYS_SUBTITLE_LANGS, 0
                    )
                else:
                    self._write_master([])
                self._subs_ready.set()
                return
            time.sleep(0.2)

        start_pts = _probe_segment_start_pts(seg0) or 0

        if not self.subtitles:
            self._write_master([])
            self._subs_ready.set()
            return

        langs = list(self._ALWAYS_SUBTITLE_LANGS)
        for lang in self.entry.subtitle_paths:
            if lang not in langs:
                langs.append(lang)

        self._write_placeholder_vtts_and_master(langs, start_pts)

        actual_start_sec = _probe_keyframe_inpoint(
            self.entry.path, self.offset_sec, self.entry.duration_sec
        )

        video_start_time = _probe_stream_start_time(self.entry.path, "v:0")
        sub_pts_corrections = {}
        for lang, idx in self._sub_stream_indices.items():
            sub_start = _probe_stream_start_time(self.entry.path, f"s:{idx}")
            correction = video_start_time - sub_start
            sub_pts_corrections[lang] = correction
            if abs(correction) > 0.05:
                log.info(
                    "Catchup %s: sub stream %s (idx %d) start_time=%.3fs vs "
                    "video start_time=%.3fs → correction=%.3fs",
                    self.session_id, lang or "und", idx, sub_start,
                    video_start_time, correction,
                )

        log.info(
            "Catchup %s subtitle timing: offset_sec=%.3f actual_start=%.3f "
            "start_pts=%d (%.3fs) pts_minus_actual=%.3fs",
            self.session_id, self.offset_sec, actual_start_sec,
            start_pts, start_pts / 90000.0,
            start_pts / 90000.0 - (self.offset_sec - actual_start_sec),
        )

        def _write_vtt(lang, lang_label, cue_lines, is_rtl=False):
            vtt_path = os.path.join(self.session_dir, f"sub_{lang_label}.vtt")
            try:
                with open(vtt_path, "w", encoding="utf-8") as f:
                    f.write("WEBVTT\n")
                    f.write(f"X-TIMESTAMP-MAP=MPEGTS:{start_pts},LOCAL:00:00:00.000\n\n")
                    if not self._subtitle_background:
                        f.write("STYLE\n::cue {\n  background-color: transparent;\n}\n\n")
                    f.writelines(cue_lines)
            except OSError as exc:
                log.error("Catchup %s: VTT write failed lang=%s: %s",
                          self.session_id, lang_label, exc)

        def _bump_manifest(lang_label, seq, endlist=False):
            sub_m3u8 = os.path.join(self.session_dir, f"sub_{lang_label}.m3u8")
            try:
                with open(sub_m3u8, "w", encoding="utf-8") as f:
                    f.write(
                        "#EXTM3U\n"
                        "#EXT-X-TARGETDURATION:99999\n"
                        "#EXT-X-VERSION:3\n"
                        f"#EXT-X-MEDIA-SEQUENCE:{seq}\n"
                        f"#EXTINF:{self.duration_sec:.1f},\n"
                        f"sub_{lang_label}.vtt\n"
                    )
                    if endlist:
                        f.write("#EXT-X-ENDLIST\n")
            except OSError:
                pass

        def _build_cues_from_raw(raw, lang, offset):
            cue_lines = []
            is_rtl = lang == "he" or (lang == "" and _text_has_hebrew(raw))
            try:
                for cue_start, cue_end, text in _parse_srt_cues(raw):
                    s = cue_start - offset
                    e = cue_end - offset
                    if e <= 0:
                        continue
                    s = max(0.0, s)
                    if is_rtl:
                        text = "\n".join(_he_bidi_fix(l) for l in text.split("\n"))
                    cue_lines.append(
                        f"{_sec_to_vtt_ts(s)} --> {_sec_to_vtt_ts(e)}\n{text}\n\n"
                    )
            except Exception:
                log.exception("Catchup %s: cue parse failed lang=%s",
                              self.session_id, lang or "und")
            return cue_lines, is_rtl

        def _extract_one(lang):
            lang_label = lang or "und"
            srt_path = self.entry.subtitle_paths.get(lang, "")
            raw = ""
            if srt_path and os.path.exists(srt_path):
                try:
                    raw = _read_srt(srt_path)
                except Exception:
                    log.exception("Catchup %s: SRT read failed lang=%s",
                                  self.session_id, lang_label)
            elif lang and lang not in self._sub_stream_indices:
                raw = _extract_embedded_srt(self.entry.path, lang, actual_start_sec,
                                             self.duration_sec, timeout=120)
            cue_lines, is_rtl = _build_cues_from_raw(raw, lang, actual_start_sec) if raw else ([], False)
            log.debug("Catchup %s: wrote %d cues lang=%s (external/fallback)",
                      self.session_id, len(cue_lines), lang_label)
            _write_vtt(lang, lang_label, cue_lines, is_rtl)

        def _poll_ffmpeg_srt(lang):
            lang_label = lang or "und"
            srt_path = os.path.join(self.session_dir, f"sub_{lang_label}.srt")
            seq = 0
            last_size = 0

            while True:
                for _ in range(4):
                    time.sleep(0.5)
                    if self._process and self._process.poll() is not None:
                        break

                if not os.path.exists(srt_path):
                    if self._process and self._process.poll() is not None:
                        break
                    continue

                size = os.path.getsize(srt_path)
                if size == last_size:
                    if self._process and self._process.poll() is not None:
                        break
                    continue
                last_size = size

                try:
                    raw = _read_srt(srt_path)
                except Exception:
                    if self._process and self._process.poll() is not None:
                        break
                    continue

                effective_offset = actual_start_sec - self.offset_sec
                cue_lines, is_rtl = _build_cues_from_raw(raw, lang, effective_offset)
                if not cue_lines:
                    if self._process and self._process.poll() is not None:
                        break
                    continue

                if seq == 0:
                    try:
                        first_cues = list(_parse_srt_cues(raw))
                        if first_cues:
                            raw_first = first_cues[0][0]
                            log.info(
                                "Catchup %s SRT first cue: raw_ts=%.3fs "
                                "(no offset subtracted, start_pts=%.3fs) lang=%s",
                                self.session_id, raw_first,
                                start_pts / 90000.0, lang_label,
                            )
                    except Exception:
                        pass

                seq += 1
                done = (self._process and self._process.poll() is not None)
                _write_vtt(lang, lang_label, cue_lines, is_rtl)
                _bump_manifest(lang_label, seq, endlist=done)
                log.debug(
                    "Catchup %s: SRT poll — %d cues lang=%s seq=%d%s",
                    self.session_id, len(cue_lines), lang_label, seq,
                    " [final]" if done else "",
                )
                if done:
                    return

        ext_threads = [
            threading.Thread(target=_extract_one, args=(lang,), daemon=True)
            for lang in langs
        ]
        for t in ext_threads:
            t.start()

        for lang in langs:
            if lang in self._sub_stream_indices:
                threading.Thread(
                    target=_poll_ffmpeg_srt, args=(lang,), daemon=True,
                    name=f"srt-poll-{self.session_id}-{lang or 'und'}",
                ).start()

        for t in ext_threads:
            t.join()

        for lang in langs:
            if lang not in self._sub_stream_indices:
                _bump_manifest(lang or "und", seq=1, endlist=True)

        self._subs_ready.set()
        log.info("Catchup %s: subtitle extraction complete — VTTs ready for langs=%s",
                 self.session_id, langs)

    def _write_placeholder_vtts_and_master(self, langs, start_pts):
        """Write empty VTTs + sub manifests + stream.m3u8 so is_ready() fires immediately."""
        for lang in langs:
            lang_label = lang or "und"
            vtt_path = os.path.join(self.session_dir, f"sub_{lang_label}.vtt")
            sub_m3u8 = os.path.join(self.session_dir, f"sub_{lang_label}.m3u8")
            with open(vtt_path, "w", encoding="utf-8") as f:
                f.write("WEBVTT\n")
                f.write(f"X-TIMESTAMP-MAP=MPEGTS:{start_pts},LOCAL:00:00:00.000\n\n")
            with open(sub_m3u8, "w", encoding="utf-8") as f:
                f.write(
                    "#EXTM3U\n"
                    "#EXT-X-TARGETDURATION:99999\n"
                    "#EXT-X-VERSION:3\n"
                    "#EXT-X-MEDIA-SEQUENCE:0\n"
                    f"#EXTINF:{self.duration_sec:.1f},\n"
                    f"sub_{lang_label}.vtt\n"
                )
        log.debug("Catchup %s: placeholder VTTs written for langs=%s — awaiting extraction",
                  self.session_id, langs)
        self._write_master(langs)

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
