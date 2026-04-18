"""
streamer.py — Manages one ffmpeg process per channel.

Each process reads a concat file (built from the deterministic schedule),
remuxes to MPEG-TS at real-time speed (-re -c copy), and outputs HLS
segments + manifest to {tmp_dir}/ch_{id}/.

The concat file covers ~4 hours ahead. When ffmpeg finishes that window
it is automatically restarted with a freshly calculated concat file.

CatchupManager handles on-demand VOD sessions for past programmes.
"""
import json
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
from typing import Dict, List, Optional, Tuple

from .scheduler import Channel, NowPlaying, ScheduleEntry, get_now_playing, get_playing_at

log = logging.getLogger(__name__)

# Limit concurrent NAS keyframe probes so simultaneous channel starts
# (prewarm) don't thrash the NAS with 30+ ffprobe processes at once.
_keyframe_probe_sem = threading.Semaphore(3)

# Per-file GOP size cache: path → max keyframe interval in seconds, probed
# from the first 10 s of the file (position 0, no seeking — always fast).
# Used as the fallback compensation when the inpoint probe fails.
_gop_size_cache: Dict[str, float] = {}
_DEFAULT_GOP_SEC = 5.0   # used when GOP probe itself fails

HLS_SEGMENT_SECONDS = 2
HLS_LIST_SIZE = 15         # sliding window — keep 15 × 2s segments (~30s of buffer)
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
def _probe_segment_start_pts(seg_path: str) -> Optional[int]:
    """
    Return the MPEG-TS start_pts (90kHz units) of the first video stream in seg_path.

    With -c:v copy, ffmpeg preserves source PTS values rather than resetting to 0,
    so the segments' starting PTS is non-zero.  This value is needed to write a
    correct X-TIMESTAMP-MAP so WebVTT cues align with the player's timeline.
    """
    try:
        r = subprocess.run([
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-select_streams", "v:0",
            seg_path,
        ], capture_output=True, text=True, timeout=10)
        data = json.loads(r.stdout)
        streams = data.get("streams", [])
        if streams:
            pts = streams[0].get("start_pts")
            if pts is not None:
                return int(pts)
    except Exception as exc:
        log.debug("_probe_segment_start_pts failed for %s: %s", seg_path, exc)
    return None


def _probe_stream_start_time(path: str, stream_spec: str) -> float:
    """
    Return the start_time (seconds) of the first stream matching stream_spec.

    Used to detect cross-stream PTS offsets: some MKVs have video PTS starting
    at e.g. +3s while the subtitle stream starts at 0s.  The difference must be
    added as a correction when building VTT cues from ffmpeg SRT side-output, so
    that cues align with the video rather than the subtitle track's origin.

    Returns 0.0 on any error or when start_time is not reported.
    """
    try:
        r = subprocess.run([
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-select_streams", stream_spec,
            "-show_entries", "stream=start_time",
            path,
        ], capture_output=True, text=True, timeout=10)
        data = json.loads(r.stdout)
        streams = data.get("streams", [])
        if streams:
            st = streams[0].get("start_time")
            if st not in (None, "N/A"):
                return float(st)
    except Exception as exc:
        log.debug("_probe_stream_start_time(%s, %s) failed: %s", path, stream_spec, exc)
    return 0.0


def _nas_prewarm(path: str, inpoint: float, entry_duration: float) -> None:
    """
    Prime the NAS SMB disk cache for a seek to `inpoint` in `path`.

    MKV seeking requires three disk regions that are cold on first access:
      1. The file header — ffmpeg reads this first when opening any container.
      2. The Cues element — stored near the end of the file after mkvmerge.
      3. The cluster at the target timestamp — proportional to inpoint/duration.

    Reading these regions is ~100ms on a warm LAN connection and puts all areas
    into the NAS RAM cache.  Subsequent accesses by ffmpeg and ffprobe then
    complete in <100ms instead of the 2-10s each would take from cold disk.

    Called once from _launch() (before Popen) and once from _probe_keyframe_inpoint
    (before ffprobe); the second call is nearly free because the cache is already warm.
    """
    if inpoint <= 0 or entry_duration <= 0:
        return
    try:
        file_size = os.path.getsize(path)
        HEAD = 65536        # 64 KB — file header + EBML SeekHead
        WARM = 512 * 1024   # 512 KB per seek region (wider window for estimation error)
        with open(path, 'rb') as f:
            # Region 1: file header — ffmpeg reads this to detect the container format
            # and parse the SeekHead.  Not otherwise prewarmed.
            f.read(HEAD)
            # Region 2: file tail — Cues element lives here after mkvmerge
            f.seek(max(HEAD, file_size - WARM))
            f.read(WARM)
            # Region 3: estimated cluster for inpoint.
            # bytes_per_sec derived from known duration and file size.
            # Read WARM bytes centred on the estimate to absorb bitrate variance.
            bps = file_size / entry_duration
            cluster_pos = max(HEAD, int(inpoint * bps) - WARM // 2)
            if cluster_pos < file_size - WARM * 2:
                f.seek(cluster_pos)
                f.read(WARM)
    except Exception:
        pass  # non-fatal — ffmpeg/ffprobe will just be slower on cold cache


def _probe_gop_size(path: str) -> float:
    """
    Return the max keyframe interval (GOP size) for the file, in seconds.

    Probes only the first 10 s of the file (read_intervals 0%10), so no
    seeking is required — fast even for UHD MKV files without a Cues element.
    Result is cached in _gop_size_cache (in-memory, per container start).
    Falls back to _DEFAULT_GOP_SEC if the probe fails.
    """
    if path in _gop_size_cache:
        return _gop_size_cache[path]
    try:
        r = subprocess.run([
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-select_streams", "v:0",
            "-show_entries", "packet=pts_time,flags",
            "-read_intervals", "0%10",
            path,
        ], capture_output=True, text=True, timeout=8)
        packets = json.loads(r.stdout).get("packets", [])
        kf_times = sorted([
            float(p["pts_time"]) for p in packets
            if p.get("flags", "").startswith("K")
            and p.get("pts_time") not in (None, "N/A")
        ])
        if len(kf_times) >= 2:
            gop = max(b - a for a, b in zip(kf_times, kf_times[1:]))
            log.debug("GOP probe %s: %.2fs (from %d keyframes)",
                      os.path.basename(path), gop, len(kf_times))
            _gop_size_cache[path] = gop
            return gop
    except Exception as exc:
        log.debug("GOP probe failed for %s: %s", os.path.basename(path), exc)
    _gop_size_cache[path] = _DEFAULT_GOP_SEC
    return _DEFAULT_GOP_SEC


def _probe_keyframe_inpoint(path: str, inpoint: float,
                            entry_duration: float = 0.0,
                            timeout: int = 15) -> float:
    """
    Return the timestamp of the last video keyframe at or before `inpoint`.

    When ffmpeg uses an ffconcat `inpoint` with -c:v copy it can only start
    at a keyframe boundary.  If the nearest keyframe is N seconds before the
    nominal inpoint, the video stream will start N seconds earlier than our
    subtitle timings expect — causing subtitles to drift by up to the GOP
    interval (can be 30 s on long-GOP HEVC content).

    Called after the first HLS segment appears, so the NAS SMB cache is
    already warm (ffmpeg has been reading the file for ~2s).  ffprobe
    completes in <1s even for files without a seek index.

    Falls back to GOP-based compensation if the probe fails.
    """
    if inpoint <= 0:
        return 0.0

    # GOP-based fallback: probe the first 10 s of the file (no seeking, always
    # fast) to get the actual keyframe interval for this file.  Used when the
    # main probe below fails — ensures compensation matches the file's own GOP
    # rather than a hardcoded constant.
    gop_size = _probe_gop_size(path)
    fallback = max(0.0, inpoint - gop_size)

    # Serialise concurrent probes to avoid thrashing the NAS when many
    # channels start simultaneously (prewarm).  Max 3 probes at a time.
    with _keyframe_probe_sem:
        try:
            # Search back only as far as needed: gop_size*2 guarantees ≥1 full
            # GOP while staying within the NAS SMB cache range.  ffmpeg has
            # been reading from ~inpoint for ~2s, so the cache covers roughly
            # [inpoint-gop_size, inpoint+ε].  The old fixed 60s window reached
            # well outside that range on H.264 content (gop≈5s), forcing a
            # cold NAS fetch → timeout → GOP fallback → subtitle drift.
            # For HEVC (gop_size≈30s) this stays at 60s — same as before.
            start = max(0.0, inpoint - max(gop_size * 2, 10.0))
            r = subprocess.run([
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-select_streams", "v:0",
                "-show_entries", "packet=pts_time,flags",
                "-read_intervals", f"{start:.3f}%{inpoint + 0.5:.3f}",
                path,
            ], capture_output=True, text=True, timeout=timeout)
            packets = json.loads(r.stdout).get("packets", [])
            kf_times = [
                float(p["pts_time"])
                for p in packets
                if p.get("flags", "").startswith("K")
                and p.get("pts_time") not in (None, "N/A")
            ]
            candidates = [k for k in kf_times if k <= inpoint]
            if candidates:
                actual = max(candidates)
                if abs(actual - inpoint) > 0.1:
                    log.info(
                        "Subtitle keyframe snap: %.3fs → %.3fs (Δ=%.3fs) for %s",
                        inpoint, actual, inpoint - actual,
                        os.path.basename(path),
                    )
                return actual
            log.warning(
                "Subtitle keyframe probe: no keyframes found near %.3fs in %s "
                "— applying %.2fs GOP compensation (file may lack seek index)",
                inpoint, os.path.basename(path), gop_size,
            )
        except Exception as exc:
            log.warning(
                "Subtitle keyframe probe failed for %s @ %.3fs: %s "
                "— applying %.2fs GOP compensation",
                os.path.basename(path), inpoint, exc, gop_size,
            )
    return fallback


# ISO 639-1 (2-letter) → ISO 639-2 (3-letter) map for common languages.
# Used when matching ffprobe language tags (which may be 2- or 3-letter) against
# config.preferred_audio_language and subtitle lang codes.
_LANG2_TO_LANG3: Dict[str, str] = {
    "en": "eng", "he": "heb", "fr": "fra", "de": "deu",
    "es": "spa", "ar": "ara", "ru": "rus", "pt": "por",
    "it": "ita", "nl": "nld", "pl": "pol", "cs": "ces",
    "ja": "jpn", "ko": "kor", "zh": "zho",
}
_LANG3_TO_LANG2: Dict[str, str] = {v: k for k, v in _LANG2_TO_LANG3.items()}


def _lang_matches(tag: str, preferred: str) -> bool:
    """Return True if ffprobe language tag `tag` matches `preferred` (2- or 3-letter)."""
    tag = tag.lower().strip()
    preferred = preferred.lower().strip()
    if not tag:
        return False
    if tag == preferred:
        return True
    # Normalise both to 3-letter and compare
    tag3 = _LANG2_TO_LANG3.get(tag, tag)
    pref3 = _LANG2_TO_LANG3.get(preferred, preferred)
    return tag3 == pref3


def _probe_audio_stream_index(path: str, preferred_lang: str = "eng") -> int:
    """
    Return the index of the first audio stream whose language tag matches
    preferred_lang (accepts both 2- and 3-letter ISO codes).  Returns 0 if no
    match is found or the probe fails.
    """
    try:
        r = subprocess.run([
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-select_streams", "a", path,
        ], capture_output=True, text=True, timeout=10)
        streams = json.loads(r.stdout).get("streams", [])
        for i, s in enumerate(streams):
            lang = s.get("tags", {}).get("language", "")
            if _lang_matches(lang, preferred_lang):
                if i != 0:
                    log.info(
                        "Audio track probe: using stream %d (%s) for %s",
                        i, lang, os.path.basename(path),
                    )
                return i
    except Exception as exc:
        log.debug("Audio stream probe failed for %s: %s", os.path.basename(path), exc)
    return 0


def _probe_subtitle_stream_indices(path: str, langs: list) -> dict:
    """
    Quick ffprobe to map language codes to subtitle stream indices.
    Returns {lang: stream_idx} for text subtitle streams only (skips bitmap).
    Used to build ffmpeg SRT side-output arguments.
    """
    try:
        r = subprocess.run([
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-select_streams", "s", path,
        ], capture_output=True, text=True, timeout=10)
        streams = json.loads(r.stdout).get("streams", [])
    except Exception as exc:
        log.debug("Subtitle stream probe failed for %s: %s", os.path.basename(path), exc)
        return {}
    result = {}
    for lang in langs:
        for i, s in enumerate(streams):
            if _lang_matches(s.get("tags", {}).get("language", ""), lang):
                codec = s.get("codec_name", "").lower()
                if codec not in ("hdmv_pgs_subtitle", "dvd_subtitle", "vobsub"):
                    result[lang] = i
                break
    return result


def _extract_embedded_srt(path: str, lang: str, start_sec: float = 0.0,
                          duration_sec: float = 0.0, timeout: int = 30) -> str:
    """
    Extract the subtitle stream matching `lang` (2- or 3-letter ISO code) from
    a video file and return its content as SRT text.  Returns "" on any failure
    (no matching stream, bitmap sub, extraction error, etc.).

    start_sec: input-seek to this position before extracting.  Output timestamps
    are relative to the seek point and shifted back to absolute below.
    duration_sec: if > 0, stop reading after this many seconds (output -t).
    Limits how much data ffmpeg reads on large files — critical for catchup on
    4K files where reading to end-of-file over NFS exceeds the 30s timeout.
    """
    try:
        r = subprocess.run([
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-select_streams", "s", path,
        ], capture_output=True, text=True, timeout=10)
        streams = json.loads(r.stdout).get("streams", [])
        stream_idx = None
        for i, s in enumerate(streams):
            slang = s.get("tags", {}).get("language", "")
            if _lang_matches(slang, lang):
                # Skip bitmap subtitle formats — ffmpeg can't export them as SRT
                codec = s.get("codec_name", "").lower()
                if codec in ("hdmv_pgs_subtitle", "dvd_subtitle", "vobsub"):
                    log.debug(
                        "Embedded sub extraction: skipping bitmap codec %s for %s [%s]",
                        codec, os.path.basename(path), lang,
                    )
                    return ""
                stream_idx = i
                break
        if stream_idx is None:
            return ""

        cmd = ["ffmpeg", "-v", "quiet"]
        if start_sec > 1.0:
            # Input seek: jump near the inpoint so ffmpeg doesn't scan the
            # entire file from byte 0.  Without -copyts, output timestamps are
            # relative to the seek point — we shift them back to absolute below.
            cmd += ["-ss", f"{start_sec:.3f}"]
        cmd += ["-i", path]
        if duration_sec > 0:
            # Limit output to the session duration — stops ffmpeg from reading
            # the rest of the file over NFS, which can easily exceed 30s for
            # large 4K files even when a seek index is present.
            cmd += ["-t", f"{duration_sec:.3f}"]
        cmd += ["-map", f"0:s:{stream_idx}", "-f", "srt", "pipe:1"]

        r2 = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        raw = r2.stdout
        if r2.returncode == 0 and raw.strip():
            if start_sec > 1.0:
                # Timestamps are relative to seek point; add start_sec back so
                # callers receive absolute file-position timestamps as always.
                def _shift(m):
                    def p(ts):
                        h, mn, s = ts.split(":")
                        s, ms = s.split(",")
                        return int(h)*3600 + int(mn)*60 + int(s) + int(ms)/1000
                    def f(sec):
                        sec = max(0.0, sec)
                        h = int(sec // 3600)
                        mn = int((sec % 3600) // 60)
                        s = int(sec % 60)
                        ms = int(round((sec % 1) * 1000))
                        return f"{h:02d}:{mn:02d}:{s:02d},{ms:03d}"
                    return f"{f(p(m.group(1)) + start_sec)} --> {f(p(m.group(2)) + start_sec)}"
                raw = re.sub(
                    r"(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})",
                    _shift, raw,
                )
            log.debug(
                "Embedded sub extraction: got %d bytes from %s [%s] (seek=%.1fs)",
                len(raw), os.path.basename(path), lang, start_sec,
            )
        return raw
    except Exception as exc:
        log.debug(
            "Embedded SRT extraction failed for %s [%s]: %s",
            os.path.basename(path), lang, exc,
        )
    return ""


_LATIN_RUN_RE = re.compile(r'[A-Za-z][A-Za-z0-9]*(?:[ \'-][A-Za-z][A-Za-z0-9]*)*')
# Matches an optional run of leading HTML tags, then terminal/continuation
# punctuation that Hebrew SRT authors place at logical position 0 to compensate
# for LTR-rendering players (it appears at visual-left = end, by accident).
_HTML_TAG_RE = re.compile(r'(<[^>]*>)')
_LEADING_PUNCT_RE = re.compile(r'^((?:<[^>]*>)*)([.!?,\u2026]+)')


def _he_bidi_fix(line: str) -> str:
    """
    Fix Hebrew RTL display in ExoPlayer-based players (Televizo, Stremio Android).

    1. Move leading terminal/continuation punctuation to logical end.
       Hebrew SRT files store . ! ? , … at logical position 0 to compensate
       for broken LTR rendering (the punctuation lands at visual-left by
       accident, which is correct for RTL end-of-sentence).  With proper RTL
       rendering via RLI the same character would appear at visual-right
       (wrong — looks like it's at the start).  Moving it to the logical end
       lets RLI put it at visual-left where it belongs.

    2. Wrap line with U+2067 … U+2069 (RLI … PDI, Right-to-Left Isolate).
       Explicit RTL isolate — cannot be overridden by surrounding view context.

    3. Wrap each Latin run (in text nodes only, not inside HTML tags) with
       U+2066 … U+2069 (LRI … PDI).  Prevents ExoPlayer from breaking lines at
       LTR/RTL bidi run boundaries when English words appear inside Hebrew text.
       Processing only text nodes avoids corrupting <i> / <b> tag names.

    4. Append U+200F (RLM) before closing PDI — anchors genuinely trailing
       neutral characters (e.g. a comma at logical end) to RTL via N1.
    """
    RLI = '\u2067'
    LRI = '\u2066'
    RLM = '\u200F'
    PDI = '\u2069'

    # Step 1: relocate leading terminal/continuation punctuation to logical end.
    m = _LEADING_PUNCT_RE.match(line)
    if m:
        leading_tags = m.group(1)  # e.g. '<i>' or ''
        punct = m.group(2)         # e.g. '.', '!', ',', '...'
        rest = line[m.end():]      # remainder after the punctuation
        # If there's a trailing dialogue dash, swap it to logical-first so that
        # RTL rendering gives:  - sentence ?  (not  sentence ?-  or  sentence -?)
        trail = rest.rstrip()
        if trail.endswith('-'):
            rest_body = rest[:rest.rfind('-')].strip()
            rest = '-' + rest_body + punct
        else:
            rest = rest + punct
        line = leading_tags + rest

    # Step 2: wrap Latin runs in text nodes only (tags are passed through raw).
    parts = _HTML_TAG_RE.split(line)
    result = ''
    for i, part in enumerate(parts):
        if i % 2 == 0:  # text node
            result += _LATIN_RUN_RE.sub(lambda mo: LRI + mo.group() + PDI, part)
        else:           # HTML tag — preserve unchanged
            result += part

    return RLI + result + RLM + PDI


class SubtitleStreamer:
    """
    Generates a static WebVTT subtitle file + HLS playlist for one language.

    Two-phase design to eliminate a race condition where the player downloads
    the VTT before X-TIMESTAMP-MAP is corrected:

      Phase 1 — build_cues(inpoint):
        Pure in-memory SRT parsing.  Returns (cue_lines, cue_count).
        No file I/O.  Completes in < 100ms.  Called synchronously in _launch().

      Phase 2 — write_files(cue_lines, cue_count, start_pts):
        Writes sub_{lang}.vtt with the correct X-TIMESTAMP-MAP=MPEGTS:{start_pts}
        and sub_{lang}.m3u8.  Called from an async thread once the first HLS
        segment has been probed for its start PTS (typically ~2 s after launch).

    The server waits for a _subtitle_ready_event (set after Phase 2) before
    serving the master playlist.  wait_ready blocks for ~2s (1 segment), and
    subtitle write finishes at ~2.5s, so the net extra wait is minimal (~0.5s).
    """

    def __init__(self, channel: Channel, lang: str, hls_dir: str):
        self._channel = channel
        self.lang = lang
        self.hls_dir = hls_dir
        lang_label = lang or "und"
        self.vtt_path = os.path.join(hls_dir, f"sub_{lang_label}.vtt")
        self.manifest_path = os.path.join(hls_dir, f"sub_{lang_label}.m3u8")
        self._ok = False
        # Set True by ChannelStreamer when ffmpeg writes an SRT side-output for
        # this lang — causes _generate() to skip blocking embedded extraction.
        self.has_ffmpeg_srt = False

    def write_placeholder(self):
        """
        Write an empty VTT immediately so hls_sub_manifest can return without
        blocking.  Overwritten by write_files() once extraction completes.
        """
        os.makedirs(self.hls_dir, exist_ok=True)
        with open(self.vtt_path, "w", encoding="utf-8") as f:
            f.write("WEBVTT\nX-TIMESTAMP-MAP=MPEGTS:0,LOCAL:00:00:00.000\n\n")
        log.debug("Subtitle %s (%s): placeholder VTT written — awaiting extraction",
                  self.lang or "und", self._channel.id)

    def is_running(self) -> bool:
        """True when the subtitle files have been written with a correct TIMESTAMP-MAP."""
        return self._ok

    def build_cues(self, inpoint: float):
        """
        Phase 1: parse SRT files, return (cue_lines, cue_count).
        No file writes.  Safe to call synchronously in _launch().
        """
        return self._generate(inpoint)

    def write_files(self, cue_lines: list, cue_count: int, start_pts: int):
        """
        Phase 2: write VTT + manifest to disk with the correct TIMESTAMP-MAP.
        Called after the first HLS segment's start_pts is known.
        """
        total_seconds = CONCAT_HOURS * 3600
        with open(self.vtt_path, "w", encoding="utf-8") as f:
            f.write("WEBVTT\n")
            f.write(f"X-TIMESTAMP-MAP=MPEGTS:{start_pts},LOCAL:00:00:00.000\n\n")
            # Per-track CSS: transparent background + 1px outline stroke.
            # Four-direction zero-blur text-shadow creates a clean outline.
            cue_style = (
                "  background-color: transparent;\n"
                "  text-shadow: 1px 0 0 #000, -1px 0 0 #000, 0 1px 0 #000, 0 -1px 0 #000;\n"
            )
            if self.lang == "he":
                # Explicit RTL direction for ExoPlayer versions that respect
                # ::cue CSS — belt-and-suspenders alongside the RLM prepend in
                # _he_bidi_fix.
                cue_style += "  direction: rtl;\n  unicode-bidi: isolate;\n"
            f.write(f"STYLE\n::cue {{\n{cue_style}}}\n\n")
            f.writelines(cue_lines)

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

        # Always mark the track as valid — even with 0 cues for the current
        # 4h window, the VTT file is written (just empty) so the master
        # playlist can declare the language.  The player sees the track option
        # and subs appear automatically once the channel reaches an episode
        # that has them.
        self._ok = True
        if cue_count > 0:
            log.info(
                "Subtitle %s (%s): placeholder replaced — %d cues written (MPEGTS:%d)",
                self.lang or "und", self._channel.id, cue_count, start_pts,
            )
        else:
            log.info(
                "Subtitle %s (%s): placeholder replaced — no cues in current window",
                self.lang or "und", self._channel.id,
            )

    def stop(self):
        self._ok = False

    def _generate(self, inpoint: float = None):
        """
        Parse SRT / embedded subtitle streams and return (cue_lines, cue_count).
        Called by build_cues().
        """
        now_playing: Optional[NowPlaying] = get_now_playing(self._channel)
        if now_playing is None:
            raise RuntimeError("no now-playing")

        entries = self._channel.entries
        n = len(entries)
        total_seconds = CONCAT_HOURS * 3600

        cue_lines: list = []
        stream_pos = 0.0
        if inpoint is None:
            inpoint = now_playing.offset_sec
        idx = now_playing.entry_index
        cue_count = 0
        entries_with_subs = 0
        entries_without_subs = 0
        is_current_entry = True   # only True for the first (currently playing) entry

        log.debug(
            "SubtitleStreamer._generate: channel=%s lang=%s entry_index=%d inpoint=%.3fs",
            self._channel.id, self.lang or "und", idx, inpoint,
        )

        while stream_pos < total_seconds:
            entry = entries[idx % n]
            srt_path = entry.subtitle_paths.get(self.lang, "")

            if srt_path and os.path.exists(srt_path):
                entries_with_subs += 1
                raw = _read_srt(srt_path)
                cues = _parse_srt_cues(raw)
                # Disc-rip SRTs may use absolute disc timestamps (e.g. first cue at
                # 02:30:04 because the episode starts at chapter 02:30:00 on the disc).
                # Only subtract the offset when the first cue is clearly out-of-file
                # (> 5 minutes).  Normal SRTs with a first cue at e.g. 50s must NOT
                # be normalised — subtracting 50s would shift every subtitle 50s early.
                first_cue_time = min((s for s, e, t in cues), default=0.0)
                srt_offset = first_cue_time if first_cue_time > 300.0 else 0.0
                entry_cues_added = 0
                entry_cues_skipped = 0
                for start, end, text in cues:
                    s_adj = (start - srt_offset) - inpoint + stream_pos
                    e_adj = (end   - srt_offset) - inpoint + stream_pos
                    if e_adj <= 0 or s_adj < 0:
                        entry_cues_skipped += 1
                        continue
                    if s_adj >= total_seconds:
                        break
                    if self.lang == "he":
                        text = "\n".join(_he_bidi_fix(l) for l in text.split("\n"))
                    cue_lines.append(
                        f"{_sec_to_vtt_ts(s_adj)} --> {_sec_to_vtt_ts(e_adj)}\n"
                        f"{text}\n\n"
                    )
                    cue_count += 1
                    entry_cues_added += 1
                if entries_with_subs <= 3:
                    log.debug(
                        "  [%s] %s: srt_offset=%.3fs, %d cues raw, %d added, %d skipped (stream_pos=%.1fs)",
                        self.lang or "und", os.path.basename(srt_path),
                        srt_offset, len(cues), entry_cues_added, entry_cues_skipped, stream_pos,
                    )
            else:
                if not srt_path:
                    log.debug(
                        "  [%s] entry %d (%s): no srt for this lang — trying embedded",
                        self.lang or "und", idx % n, entry.title,
                    )
                elif not os.path.exists(srt_path):
                    log.warning(
                        "  [%s] entry %d (%s): srt path missing on disk: %s — trying embedded",
                        self.lang or "und", idx % n, entry.title, srt_path,
                    )
                # Fall back to embedded subtitle stream — only for the currently
                # playing entry.  Future entries in the concat window are on cold
                # NAS pages; running ffmpeg on them blocks _subtitle_ready_event
                # for several seconds per file.  They'll be extracted when they
                # become the current entry on the next concat rebuild.
                if is_current_entry and self.has_ffmpeg_srt:
                    # ffmpeg is writing an SRT side-output for this lang (piggyback
                    # on the live ffmpeg read — zero extra NAS I/O).  Skip blocking
                    # extraction here; _watch_live_srt() will update the VTT async.
                    log.debug(
                        "  [%s] entry %d (%s): ffmpeg SRT piggyback active"
                        " — skipping blocking embedded extraction",
                        self.lang or "und", idx % n, entry.title,
                    )
                    raw = ""
                else:
                    remaining = max(0.0, entry.duration_sec - inpoint)
                    raw = _extract_embedded_srt(entry.path, self.lang, inpoint,
                                                duration_sec=remaining) if is_current_entry else ""
                if raw:
                    entries_with_subs += 1
                    cues = _parse_srt_cues(raw)
                    entry_cues_added = 0
                    for start, end, text in cues:
                        s_adj = start - inpoint + stream_pos
                        e_adj = end   - inpoint + stream_pos
                        if e_adj <= 0 or s_adj < 0:
                            continue
                        if s_adj >= total_seconds:
                            break
                        if self.lang == "he":
                            text = "\n".join(_he_bidi_fix(l) for l in text.split("\n"))
                        cue_lines.append(
                            f"{_sec_to_vtt_ts(s_adj)} --> {_sec_to_vtt_ts(e_adj)}\n"
                            f"{text}\n\n"
                        )
                        cue_count += 1
                        entry_cues_added += 1
                    log.debug(
                        "  [%s] entry %d (%s): embedded sub — %d cues added",
                        self.lang or "und", idx % n, entry.title, entry_cues_added,
                    )
                else:
                    entries_without_subs += 1
                    log.debug(
                        "  [%s] entry %d (%s): no srt and no embedded sub",
                        self.lang or "und", idx % n, entry.title,
                    )

            remaining = entry.duration_sec - inpoint
            stream_pos += remaining
            inpoint = 0.0
            idx += 1
            is_current_entry = False

        log.info(
            "Subtitle track %s (%s): %d cues built "
            "(entries with subs: %d, without: %d)",
            self.lang or "und", self._channel.id, cue_count,
            entries_with_subs, entries_without_subs,
        )
        return cue_lines, cue_count


class ChannelStreamer:
    """Manages the ffmpeg process for a single channel."""

    def __init__(self, channel: Channel, tmp_base: str, subtitles: bool = True,
                 audio_copy: bool = True, prewarm_timeout: int = IDLE_TIMEOUT_PREWARM,
                 ready_segments: int = 3, preferred_audio_language: str = "eng",
                 hls_start_number: int = 0):
        self.channel = channel
        self._tmp_base = tmp_base
        self._subtitles = subtitles
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
                    sub = SubtitleStreamer(self.channel, lang, self.hls_dir)
                    sub.write_placeholder()
                    if lang in self._live_srt_langs:
                        sub.has_ffmpeg_srt = True
                    self._subtitle_streamers[lang] = sub

                # Async thread: read SRTs, wait for first TS segment, probe
                # start_pts, write VTT files, set _subtitle_ready_event.
                threading.Thread(
                    target=self._write_subtitle_files_async,
                    args=(subtitle_langs,),
                    kwargs={
                        "launch_inpoint": np.offset_sec if np else 0.0,
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


    def _write_subtitle_files_async(
        self,
        subtitle_langs: list,
        launch_inpoint: float = 0.0,
        launch_entry_path: Optional[str] = None,
        launch_entry_duration: float = 0.0,
    ):
        """
        Combined subtitle build + write thread (runs entirely in background).

        Steps:
          1. Wait for the first HLS TS segment (~2s). By then ffmpeg has opened
             the file and the NAS SMB cache is warm.
          2. Probe start_pts from the segment (corrects TIMESTAMP-MAP for
             HEVC B-frame streams where PTS doesn't start at 0).
          3. Probe actual keyframe inpoint — NAS warm, completes in <1s.
             Corrects subtitle timing when ffmpeg snaps to a keyframe before
             nominal_inpoint (can be 30s off on long-GOP HEVC content).
          4. Build cue lists from SRT files on NAS (2-3s, I/O bound).
          5. Write VTT + m3u8 files; set _subtitle_ready_event.

        Probing after the segment appears (rather than speculatively before)
        gives accurate inpoint data on the first try, without the cold-NAS
        timeout/retry complexity of the previous approach.
        """
        nominal_inpoint = launch_inpoint
        entry_path = launch_entry_path
        entry_duration = launch_entry_duration

        # Step 1 — wait for the first TS segment (up to 30s)
        deadline = time.time() + 30
        seg_path = None
        while time.time() < deadline and not self._stop_event.is_set():
            if os.path.isdir(self.hls_dir):
                ts_files = [
                    os.path.join(self.hls_dir, f)
                    for f in os.listdir(self.hls_dir)
                    if f.endswith(".ts")
                ]
                if ts_files:
                    seg_path = min(ts_files, key=os.path.getmtime)
                    break
            time.sleep(0.1)

        # Step 2 — probe start_pts
        if seg_path is None:
            log.warning("_write_subtitle_files_async: no TS segment for %s — using MPEGTS:0",
                        self.channel.id)
            start_pts = 0
        else:
            start_pts = _probe_segment_start_pts(seg_path) or 0
            log.info(
                "Channel %s: subtitle TIMESTAMP-MAP → MPEGTS:%d (%.3fs)",
                self.channel.id, start_pts, start_pts / 90000,
            )

        # Step 3 — probe keyframe inpoint. NAS is warm (ffmpeg has been reading
        # the file since the segment appeared), so ffprobe completes in <1s.
        actual_inpoint = nominal_inpoint
        if nominal_inpoint > 0 and entry_path:
            actual_inpoint = _probe_keyframe_inpoint(
                entry_path, nominal_inpoint, entry_duration
            )

        # Step 3b — probe video stream start_time.
        # Some MKVs (disc rips, certain encodes) have a non-zero video start_time
        # (e.g. 3s) while external SRT timestamps are content-relative (0 = first
        # video frame).  _probe_keyframe_inpoint returns container PTS time, so we
        # must subtract video_start_time to convert actual_inpoint to content time
        # before building subtitle cues.  No-op for the common case of start_time=0.
        video_start_time = 0.0
        if entry_path:
            video_start_time = _probe_stream_start_time(entry_path, "v:0") or 0.0
            if abs(video_start_time) > 0.05:
                log.info(
                    "Channel %s: video start_time=%.3fs — adjusting subtitle inpoint "
                    "(%.3fs → %.3fs)",
                    self.channel.id, video_start_time,
                    actual_inpoint, actual_inpoint - video_start_time,
                )
        corrected_inpoint = actual_inpoint - video_start_time

        log.info(
            "Channel %s: subtitle inpoint nominal=%.3fs actual=%.3fs "
            "(snap=%.3fs, video_start=%.3fs, entry=%s)",
            self.channel.id, nominal_inpoint, actual_inpoint,
            nominal_inpoint - actual_inpoint, video_start_time,
            os.path.basename(entry_path) if entry_path else "?",
        )

        # Step 4 — build cue lists with the correct inpoint (parallel, one thread per lang)
        pending: Dict[str, tuple] = {}
        pending_lock = threading.Lock()

        def _build_one(lang):
            sub = self._subtitle_streamers.get(lang)
            if sub is None:
                return
            try:
                cue_lines, cue_count = sub.build_cues(corrected_inpoint)
            except Exception:
                log.exception("SubtitleStreamer build_cues failed (%s, %s)",
                              self.channel.id, lang or "und")
                cue_lines, cue_count = [], 0
            with pending_lock:
                pending[lang] = (cue_lines, cue_count)

        build_threads = [
            threading.Thread(target=_build_one, args=(lang,), daemon=True)
            for lang in subtitle_langs
        ]
        for t in build_threads:
            t.start()
        for t in build_threads:
            t.join()

        # Step 5 — write VTT files (with whatever cues we have from external SRTs)
        # and start live SRT watcher threads for embedded langs.
        ready_langs = []
        launch_time = self._last_launch_wall_time
        for lang, (cue_lines, cue_count) in pending.items():
            sub = self._subtitle_streamers.get(lang)
            if sub is None:
                continue
            sub.write_files(cue_lines, cue_count, start_pts)
            if sub.is_running():
                ready_langs.append(lang)

            # For langs where ffmpeg writes an SRT side-output, start a watcher
            # thread that polls the growing file and updates the VTT progressively.
            # The player keeps re-fetching the VTT (live sub playlist has no
            # EXT-X-ENDLIST) and picks up new cues on each poll.
            if lang in self._live_srt_langs:
                lang_label = lang or "und"
                srt_path = os.path.join(self.hls_dir, f"sub_{lang_label}.srt")
                threading.Thread(
                    target=self._watch_live_srt,
                    args=(lang, srt_path, sub.vtt_path, start_pts,
                          cue_lines, launch_time),
                    daemon=True,
                    name=f"live-srt-{self.channel.id}-{lang_label}",
                ).start()
                log.info("Channel %s: started live SRT watcher for [%s]",
                         self.channel.id, lang_label)

        log.info(
            "Channel %s: subtitle tracks ready: %s (no cues: %s)",
            self.channel.id, ready_langs or "none",
            [l for l in pending if l not in ready_langs] or "none",
        )
        self._subtitle_ready_event.set()

    def _watch_live_srt(self, lang: str, srt_path: str, vtt_path: str,
                        start_pts: int, existing_cue_lines: list,
                        launch_time: float):
        """
        Poll the ffmpeg SRT side-output file as it grows and rewrite the VTT.

        ffmpeg writes the SRT at -re (realtime) rate.  We poll every 2s and
        overwrite the VTT with:
          - SRT cues from ffmpeg (current entry, timestamps start at 0 = LOCAL:0)
          - Existing cues from external SRTs (future entries in the concat window)

        The live subtitle manifest (served by hls_sub_manifest) has no
        EXT-X-ENDLIST, so the player keeps polling and picks up the updated VTT
        on its next manifest cycle (~4s for Televizo).

        Stops when:
          - The channel is stopped (_stop_event set).
          - A newer _launch() runs (launch_time differs from _last_launch_wall_time).
          - The SRT file doesn't grow for 30 × 2s = 60s (e.g. entry has no subs).
        """
        lang_label = lang or "und"
        cue_style = (
            "  background-color: transparent;\n"
            "  text-shadow: 1px 0 0 #000, -1px 0 0 #000,"
            " 0 1px 0 #000, 0 -1px 0 #000;\n"
        )
        if lang == "he":
            cue_style += "  direction: rtl;\n  unicode-bidi: isolate;\n"

        last_size = 0
        stall_count = 0

        while not self._stop_event.is_set():
            # Check if a newer _launch() superseded this watcher.
            if self._last_launch_wall_time != launch_time:
                return

            time.sleep(2)

            if self._stop_event.is_set() or self._last_launch_wall_time != launch_time:
                return

            if not os.path.exists(srt_path):
                # SRT not yet created — entry may have no embedded subs, or
                # ffmpeg hasn't started writing yet.  Keep waiting; a future
                # entry in the concat window may produce one.
                continue

            size = os.path.getsize(srt_path)
            if size <= last_size:
                # No growth — SRT hasn't changed.  Don't exit: a future entry
                # in the same concat window may start writing cues later.
                # Only log occasionally so we know the watcher is alive.
                stall_count += 1
                if stall_count % 150 == 0:  # every ~5 min
                    log.debug(
                        "live SRT watcher %s [%s]: no SRT growth for %ds",
                        self.channel.id, lang_label, stall_count * 2,
                    )
                continue

            stall_count = 0
            last_size = size

            try:
                raw = _read_srt(srt_path)
                cues = _parse_srt_cues(raw)
                if not cues:
                    continue

                # Timestamps from ffmpeg SRT (avoid_negative_ts make_zero applied)
                # start at 0 = LOCAL:00:00:00.000.  X-TIMESTAMP-MAP=MPEGTS:{start_pts}
                # maps LOCAL:0 to the correct MPEGTS offset.  No adjustment needed.
                ffmpeg_cue_lines = []
                for cue_start, cue_end, text in cues:
                    if lang == "he":
                        text = "\n".join(_he_bidi_fix(_l) for _l in text.split("\n"))
                    ffmpeg_cue_lines.append(
                        f"{_sec_to_vtt_ts(cue_start)} --> {_sec_to_vtt_ts(cue_end)}\n"
                        f"{text}\n\n"
                    )

                # Atomic write: write to tmp then rename so the player never
                # reads a partially-written VTT.
                tmp_path = vtt_path + ".tmp"
                with open(tmp_path, "w", encoding="utf-8") as f:
                    f.write("WEBVTT\n")
                    f.write(f"X-TIMESTAMP-MAP=MPEGTS:{start_pts},LOCAL:00:00:00.000\n\n")
                    f.write(f"STYLE\n::cue {{\n{cue_style}}}\n\n")
                    # Current-entry cues first (stream start = 0)
                    f.writelines(ffmpeg_cue_lines)
                    # Future-entry cues from external SRTs (non-zero stream_pos timestamps)
                    f.writelines(existing_cue_lines)
                os.replace(tmp_path, vtt_path)

                log.info(
                    "live SRT watcher %s [%s]: updated VTT — %d ffmpeg cues"
                    " + %d external cues",
                    self.channel.id, lang_label,
                    len(ffmpeg_cue_lines), len(existing_cue_lines),
                )
            except Exception as exc:
                log.debug(
                    "live SRT watcher %s [%s]: error: %s",
                    self.channel.id, lang_label, exc,
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

class BumperStreamer:
    """
    Keeps a single ffmpeg HLS loop running for one bumper file.
    Starts at container startup; monitor thread restarts it on exit.
    Segments live in {tmp_base}/bumper_{bumper_id}/.
    """

    def __init__(self, bumper_path: str, tmp_base: str):
        self._bumper_path = bumper_path
        name = os.path.splitext(os.path.basename(bumper_path))[0]
        # Slugify: lowercase, replace non-alphanumeric (except hyphen) with hyphen
        self.bumper_id = re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")
        self.hls_dir = os.path.join(tmp_base, f"bumper_{self.bumper_id}")
        self._manifest_path = os.path.join(self.hls_dir, "video.m3u8")
        self._process: Optional[subprocess.Popen] = None
        self._stop_event = threading.Event()
        self._ready_event = threading.Event()
        self._monitor_thread: Optional[threading.Thread] = None

    def start(self):
        os.makedirs(self.hls_dir, exist_ok=True)
        # Write a permanent empty VTT so the subtitle manifest can mirror
        # the bumper playlist with a valid (empty) subtitle segment reference.
        try:
            with open(os.path.join(self.hls_dir, "empty.vtt"), "w", encoding="utf-8") as f:
                f.write("WEBVTT\n\n")
        except OSError:
            pass
        self._stop_event.clear()
        self._launch()
        self._monitor_thread = threading.Thread(
            target=self._monitor, daemon=True, name=f"bumper-monitor-{self.bumper_id}"
        )
        self._monitor_thread.start()
        log.info("BumperStreamer started: %s", self.bumper_id)

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

    def _launch(self):
        # Delete stale segments to avoid timestamp discontinuity
        if os.path.isdir(self.hls_dir):
            for f in os.listdir(self.hls_dir):
                if f.endswith((".ts", ".m3u8")):
                    try:
                        os.remove(os.path.join(self.hls_dir, f))
                    except OSError:
                        pass

        seg_pattern = os.path.join(self.hls_dir, "seg%d.ts")
        cmd = [
            "ffmpeg", "-hide_banner", "-loglevel", "error",
            "-stream_loop", "-1",
            "-re",
            "-i", self._bumper_path,
            # Transcode video with forced 2-second keyframes so -hls_time 2 is
            # honoured.  -c:v copy is unreliable here: if the source has large
            # GOP intervals (e.g. 10 s), ffmpeg can only cut at existing keyframes
            # and produces 10-second segments regardless of -hls_time.  Long
            # segments mean the player buffers many seconds of bumper and cannot
            # be interrupted promptly when the real channel becomes ready.
            # ultrafast + crf 28 is imperceptible for a loading animation and
            # costs negligible CPU (single short clip on a server-class machine).
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
            "-sc_threshold", "0",                   # no extra cuts at scene changes
            "-force_key_frames", "expr:gte(t,n_forced*1)",  # I-frame every 1 s
            "-c:a", "aac", "-b:a", "128k", "-ac", "2",
            "-f", "hls",
            "-hls_time", "1",
            "-hls_list_size", "3",
            "-hls_flags", "delete_segments+omit_endlist+append_list",
            "-hls_segment_filename", seg_pattern,
            self._manifest_path,
        ]
        log.debug("BumperStreamer launch: %s", " ".join(cmd))
        self._process = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        # Start ready-watcher thread
        t = threading.Thread(
            target=self._watch_ready, daemon=True,
            name=f"bumper-ready-{self.bumper_id}"
        )
        t.start()

    def _watch_ready(self):
        while not self._stop_event.is_set():
            if os.path.exists(self._manifest_path):
                ts_count = sum(1 for f in os.listdir(self.hls_dir) if f.endswith(".ts"))
                if ts_count >= 1:
                    self._ready_event.set()
                    return
            time.sleep(0.2)

    def _monitor(self):
        while not self._stop_event.is_set():
            proc = self._process
            if proc is not None:
                proc.wait()
            if self._stop_event.is_set():
                break
            log.warning("BumperStreamer %s exited — restarting", self.bumper_id)
            time.sleep(2)
            self._ready_event.clear()
            self._launch()


class BumperManager:
    """
    Manages one BumperStreamer per bumper file found in bumpers_path.
    All streams start at container startup and run continuously.
    """

    _VIDEO_EXTENSIONS = (".mp4", ".mkv", ".mov", ".avi", ".webm")

    def __init__(self, bumpers_path: str, tmp_base: str):
        self._bumpers_path = bumpers_path
        self._tmp_base = tmp_base
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
        for filename in files:
            path = os.path.join(self._bumpers_path, filename)
            bs = BumperStreamer(path, self._tmp_base)
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
                 prewarm_adjacent: int = 0, preferred_audio_language: str = "eng",
                 bumpers_path: str = ""):
        self._tmp_base = tmp_base
        self._subtitles = subtitles
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
        # Bumper loading screens — started once at init if bumpers_path is set
        self._bumper_manager: Optional[BumperManager] = None
        if bumpers_path:
            self._bumper_manager = BumperManager(bumpers_path, tmp_base)
            self._bumper_manager.start_all()

    def ensure_started(self, ch_id: str, background: bool = False) -> bool:
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
                try:
                    with open(os.path.join(bumper.hls_dir, "video.m3u8")) as f:
                        for line in f:
                            if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
                                hls_start = int(line.strip().split(":")[1]) + 100
                                break
                except (OSError, ValueError):
                    pass

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
                                              hls_start_number=hls_start)
                if ch_id in self._watched_channels:
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


class CatchupSession:
    """
    One temporary ffmpeg VOD process for a single catchup request.
    Serves HLS with #EXT-X-ENDLIST (seekable, not live).
    """

    def __init__(self, session_id: str, entry: ScheduleEntry, offset_sec: float,
                 duration_sec: float, session_dir: str, subtitles: bool,
                 preferred_audio_language: str = "eng", is_seek: bool = False):
        self.session_id = session_id
        self.entry = entry
        self.offset_sec = offset_sec
        self.duration_sec = duration_sec   # how much to serve (programme length - offset)
        self.session_dir = session_dir
        self.subtitles = subtitles
        self._preferred_audio_language = preferred_audio_language
        # True when the new session was created because of a seek within the same
        # source file (another session for the same channel+file existed).
        # The bumper loading screen is suppressed for seeks so the viewer doesn't
        # see a flash of the loading animation when scrubbing within an episode.
        self.is_seek = is_seek
        self.manifest_path = os.path.join(session_dir, "stream.m3u8")
        self._process: Optional[subprocess.Popen] = None
        self._last_accessed = time.time()
        self._last_fetch_time = time.time()  # updated only when a .ts segment is served
        self._audio_idx: int = 0          # probed in start(), reused by regenerate_segment
        self._hwm: int = -1               # highest segment number fetched by a client
        self._last_deleted: int = -1      # highest segment number already deleted
        self._regen_events: Dict[int, threading.Event] = {}
        self._regen_lock = threading.Lock()
        self._subs_ready = threading.Event()  # set after VTTs are written with real content

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

        # Probe subtitle stream indices for langs that have no external SRT.
        # These will be written as SRT side outputs by the main ffmpeg process
        # (piggyback on the -re read — zero extra NAS I/O).
        # External SRT always wins: skip any lang that already has a file on disk.
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

        # ffmpeg produces video+audio + optional SRT side outputs.
        # Subtitles as WebVTT are handled separately by _write_subs_and_master.
        cmd = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel", "error",
            "-ss", str(self.offset_sec),
            # Real-time rate limiting: write segments at playback speed so
            # ffmpeg never gets more than a few segments ahead of the player.
            # Without -re, ffmpeg writes all segments instantly (full NAS I/O
            # speed), filling the tmpfs before the rolling delete can keep up.
            # Must be an input option (before -i), not output.
            "-re",
            "-avoid_negative_ts", "make_zero",
            "-i", self.entry.path,
            "-t", str(self.duration_sec),
            "-c:v", "copy",
            "-c:a", "copy",
            "-map", "0:v:0",
            "-map", f"0:a:{audio_idx}",
            "-f", "hls",
            "-hls_time", str(HLS_SEGMENT_SECONDS),
            "-hls_list_size", "0",
            "-hls_segment_filename", seg_pattern,
            video_manifest,
        ]
        # SRT side outputs: one per embedded-subtitle lang, written at -re rate.
        # No extra NAS reads — ffmpeg is already reading the file for video.
        # -flush_packets 1: force packet-level flushing so the watcher sees data
        # immediately rather than waiting for the 32 KB avio write buffer.
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
        """Read ffmpeg stderr and log it; detect bitmap subtitle errors."""
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

    # Language display names for the HLS master playlist SUBTITLES group.
    _LANG_NAMES = {
        "he": "Hebrew", "en": "English", "es": "Spanish", "fr": "French",
        "de": "German", "ar": "Arabic", "ru": "Russian", "pt": "Portuguese",
        "it": "Italian", "nl": "Dutch", "pl": "Polish", "cs": "Czech",
        "ja": "Japanese", "ko": "Korean", "zh": "Chinese", "": "Subtitles",
    }

    # Subtitle languages always declared in the catchup master playlist,
    # even when no SRT exists (empty VTT keeps the track option visible).
    _ALWAYS_SUBTITLE_LANGS = ["he", "en"]

    def _write_subs_and_master(self):
        """
        Background thread: write HLS master playlist and subtitle VTTs.

        Non-blocking design — mirrors the live channel approach:
          1. Wait for seg0.ts (ffmpeg started writing).
          2. Probe start_pts + actual keyframe inpoint.
          3. Write placeholder VTTs + stream.m3u8 immediately → is_ready() fires,
             server redirects the player, video starts.
          4. Fire per-lang threads to extract/parse subs, overwrite VTTs.
             Player fetches sub_he.vtt via catchup_segment which waits on
             _subs_ready so the final content is served on first fetch.
        """
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

        # Probe the actual first video PTS from seg0.ts (90kHz units).
        # With avoid_negative_ts make_zero + B-frame encoding, the minimum DTS
        # (video DTS, which precedes PTS for B-frames) is shifted to 0, so the
        # first video PTS lands at start_pts > 0 (~B-frame DTS offset).
        # This value must be used in two places:
        #   1. TIMESTAMP-MAP MPEGTS:{start_pts}=LOCAL:0  (player alignment)
        #   2. Path B cue offset = start_pts/90000  (cancel the extra shift
        #      that make_zero adds to the SRT output timestamps)
        # Path A (external SRT) already uses actual_start_sec which is correct.
        start_pts = _probe_segment_start_pts(seg0) or 0

        if not self.subtitles:
            self._write_master([])
            self._subs_ready.set()
            return

        # Collect all languages to declare
        langs = list(self._ALWAYS_SUBTITLE_LANGS)
        for lang in self.entry.subtitle_paths:
            if lang not in langs:
                langs.append(lang)

        # Step 3 — placeholder VTTs + master BEFORE the keyframe probe so
        # is_ready() fires immediately after seg0.ts appears (~0.5s after
        # ffmpeg starts).  The keyframe probe takes ~2s and is only needed
        # for subtitle timing; video playback is unaffected by it.
        self._write_placeholder_vtts_and_master(langs, start_pts)

        # Step 3b — keyframe probe (now runs without blocking the manifest).
        # actual_start_sec is used only by subtitle extraction below.
        actual_start_sec = _probe_keyframe_inpoint(
            self.entry.path, self.offset_sec, self.entry.duration_sec
        )

        # Probe cross-stream PTS offset: some MKVs have video PTS starting at
        # e.g. +3s while subtitle streams start at 0s.  The delta is added to
        # the cue-offset when building VTTs from ffmpeg SRT side-output (Path B),
        # so cues align with the video track rather than the subtitle origin.
        video_start_time = _probe_stream_start_time(self.entry.path, "v:0")
        sub_pts_corrections = {}  # lang → correction_sec (add to effective offset)
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

        # Step 4 — extract real cues per lang, overwrite VTTs.
        #
        # Two paths per language:
        #   A) External SRT or no-SRT-side-output: _extract_one (blocking, may call
        #      _extract_embedded_srt as last resort).
        #   B) Ffmpeg SRT side output available: _poll_ffmpeg_srt — daemon thread
        #      that reads the growing SRT every 15s and updates VTT + manifest.
        #      Zero extra NAS I/O — ffmpeg is already reading the file for video.

        def _cue_style(lang):
            s = (
                "  background-color: transparent;\n"
                "  text-shadow: 1px 0 0 #000, -1px 0 0 #000,"
                " 0 1px 0 #000, 0 -1px 0 #000;\n"
            )
            if lang == "he":
                s += "  direction: rtl;\n  unicode-bidi: isolate;\n"
            return s

        def _write_vtt(lang, lang_label, cue_lines):
            vtt_path = os.path.join(self.session_dir, f"sub_{lang_label}.vtt")
            try:
                with open(vtt_path, "w", encoding="utf-8") as f:
                    f.write("WEBVTT\n")
                    f.write(f"X-TIMESTAMP-MAP=MPEGTS:{start_pts},LOCAL:00:00:00.000\n\n")
                    f.write(f"STYLE\n::cue {{\n{_cue_style(lang)}}}\n\n")
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
            """Parse SRT raw text and return VTT cue lines. offset subtracted from timestamps."""
            cue_lines = []
            try:
                for cue_start, cue_end, text in _parse_srt_cues(raw):
                    s = cue_start - offset
                    e = cue_end - offset
                    if e <= 0:
                        continue
                    s = max(0.0, s)
                    if lang == "he":
                        text = "\n".join(_he_bidi_fix(l) for l in text.split("\n"))
                    cue_lines.append(
                        f"{_sec_to_vtt_ts(s)} --> {_sec_to_vtt_ts(e)}\n{text}\n\n"
                    )
            except Exception:
                log.exception("Catchup %s: cue parse failed lang=%s",
                              self.session_id, lang or "und")
            return cue_lines

        def _extract_one(lang):
            """Path A: external SRT (instant) or subprocess fallback (blocking)."""
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
                # No ffmpeg SRT side output — fall back to standalone subprocess.
                raw = _extract_embedded_srt(self.entry.path, lang, actual_start_sec,
                                             self.duration_sec, timeout=120)
            cue_lines = _build_cues_from_raw(raw, lang, actual_start_sec) if raw else []
            log.debug("Catchup %s: wrote %d cues lang=%s (external/fallback)",
                      self.session_id, len(cue_lines), lang_label)
            _write_vtt(lang, lang_label, cue_lines)

        def _poll_ffmpeg_srt(lang):
            """Path B: read the SRT written by the main ffmpeg every 15s, update VTT."""
            lang_label = lang or "und"
            srt_path = os.path.join(self.session_dir, f"sub_{lang_label}.srt")
            seq = 0
            last_size = 0

            while True:
                # Sleep 2 s (in 0.5s steps so we notice process exit quickly).
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
                    continue  # no new data yet
                last_size = size

                try:
                    raw = _read_srt(srt_path)
                except Exception:
                    if self._process and self._process.poll() is not None:
                        break
                    continue

                # The HLS video output and the SRT side-output each have their
                # own make_zero anchor.
                #
                # HLS output make_zero: anchored to the video stream's minimum
                #   DTS (the B-frame DTS preceding the keyframe).
                #   Result: first video PTS = start_pts = B_frame_lead * 90k
                #
                # SRT output make_zero: per-output, anchored to offset_sec
                #   (the -ss seek point), because the subtitle stream has no
                #   packet at the video keyframe position and ffmpeg uses the
                #   seek timestamp as the normalization reference.
                #   Result: SRT_ts = T_source_sub - offset_sec
                #
                # To convert SRT_ts to a LOCAL time that the player can match
                # against video PTS (using X-TIMESTAMP-MAP):
                #   LOCAL = T_source_sub - actual_start_sec
                #         = (SRT_ts + offset_sec) - actual_start_sec
                #         = SRT_ts - (actual_start_sec - offset_sec)
                #
                # effective_offset = actual_start_sec - offset_sec  (negative
                # when the keyframe is before the requested seek point, as usual)
                effective_offset = actual_start_sec - self.offset_sec
                cue_lines = _build_cues_from_raw(raw, lang, effective_offset)
                if not cue_lines:
                    if self._process and self._process.poll() is not None:
                        break
                    continue

                if seq == 0:
                    # Log first cue raw SRT timestamp for timing diagnosis
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
                _write_vtt(lang, lang_label, cue_lines)
                _bump_manifest(lang_label, seq, endlist=done)
                log.debug(
                    "Catchup %s: SRT poll — %d cues lang=%s seq=%d%s",
                    self.session_id, len(cue_lines), lang_label, seq,
                    " [final]" if done else "",
                )
                if done:
                    return

        # Launch Path-A threads (external SRT or subprocess fallback).
        ext_threads = [
            threading.Thread(target=_extract_one, args=(lang,), daemon=True)
            for lang in langs
        ]
        for t in ext_threads:
            t.start()

        # Launch Path-B daemon threads (ffmpeg SRT polling) — do not join.
        for lang in langs:
            if lang in self._sub_stream_indices:
                threading.Thread(
                    target=_poll_ffmpeg_srt, args=(lang,), daemon=True,
                    name=f"srt-poll-{self.session_id}-{lang or 'und'}",
                ).start()

        # Wait for Path-A threads, then bump their manifests.
        for t in ext_threads:
            t.join()

        # Bump manifest for langs handled by Path A (external SRT / subprocess).
        # Path B manages its own manifest bumps from within _poll_ffmpeg_srt.
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
                    # No EXT-X-ENDLIST — player keeps polling so it picks up
                    # the updated VTT once async extraction completes.
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
        """True if ffmpeg exited without producing the manifest (crashed or errored)."""
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
        """
        Called after a .ts segment is served.  Advances the high-water mark and
        deletes segments more than CATCHUP_KEEP_SEGMENTS behind the HWM so
        4K VOD sessions don't fill the tmpfs.

        Deleted segments are regenerated on demand if the player rewinds
        (see regenerate_segment).
        """
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
        """
        Recreate a deleted segment on demand (player rewound past rolling-delete
        window).  Deduplicates concurrent requests for the same segment number.
        Returns True if the segment file exists and is non-empty after regen.
        """
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
                "-c:v", "copy", "-c:a", "copy",
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
    """
    Creates and manages CatchupSession instances.
    A background thread evicts expired sessions.
    """

    def __init__(self, tmp_base: str, subtitles: bool = True,
                 preferred_audio_language: str = "eng"):
        self._tmp_base = tmp_base
        self._subtitles = subtitles
        self._preferred_audio_language = preferred_audio_language
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
        remaining = entry.duration_sec - offset_sec

        # Detect "start over": utc is in the future, meaning Televizo sent the
        # stop_time of the currently-airing episode instead of its start_time.
        # In that case, find what's actually playing NOW and start it from 0.
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

        # If utc lands within the last 2 seconds of an episode, we're at a programme
        # boundary. This happens because EPG timestamps are truncated to integer seconds
        # but schedule boundaries are floating-point — Televizo sends back the integer
        # start of show B, which falls fractionally inside show A's last second.
        # Snap FORWARD past the boundary to start the intended next episode.
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
                    if abs(existing_ts - ts) <= REUSE_TOLERANCE and not s.has_been_watched():
                        s.touch()
                        return s

            # Evict any old sessions for this channel outside the reuse window.
            # Without this, skipping forward/backward spawns a new session per
            # seek while the old (potentially multi-GB) sessions sit idle until
            # the 2h reaper runs.
            stale = [
                sid for sid, s in self._sessions.items()
                if sid.startswith(prefix) and abs(int(sid.rsplit("_", 1)[1]) - ts) > REUSE_TOLERANCE
            ]
            # Before evicting, check if any stale session uses the same source file.
            # If so, this is likely a seek within the same episode — suppress the
            # bumper loading screen so the viewer doesn't see a flash mid-scrub.
            is_seek = any(
                s.entry.path == entry.path and not s.has_been_watched()
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
            time.sleep(60)  # check every minute
            with self._lock:
                expired = [sid for sid, s in self._sessions.items() if s.is_expired()]
                for sid in expired:
                    log.info("Expiring catchup session: %s", sid)
                    self._sessions[sid].stop()
                    del self._sessions[sid]
                idle = [sid for sid, s in self._sessions.items()
                        if sid not in expired and s.is_ffmpeg_idle()]
                for sid in idle:
                    self._sessions[sid].stop_ffmpeg()
