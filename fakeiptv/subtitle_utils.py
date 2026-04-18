"""
subtitle_utils.py — SRT/VTT parsing and Hebrew RTL display fixes.
"""
import logging
import os
import re
import subprocess
from typing import Dict, Optional

log = logging.getLogger(__name__)

_LANG_CODES = {"he", "en", "es", "fr", "de", "ar", "ru", "pt", "it", "nl", "pl", "cs", "ja", "ko", "zh"}

_LATIN_RUN_RE = re.compile(r'[A-Za-z][A-Za-z0-9]*(?:[ \'-][A-Za-z][A-Za-z0-9]*)*')
_HTML_TAG_RE = re.compile(r'(<[^>]*>)')
_LEADING_PUNCT_RE = re.compile(r'^((?:<[^>]*>)*)([.!?,\u2026]+)')
_BRACKET_FLIP = str.maketrans('()[]{}', ')(][}{')

_SRT_TS_RE = re.compile(
    r"(\d{1,2}:\d{2}:\d{2}[,\.]\d{3})\s*-->\s*(\d{1,2}:\d{2}:\d{2}[,\.]\d{3})"
)


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


def _text_has_hebrew(text: str) -> bool:
    return any('\u0590' <= c <= '\u05FF' for c in text)


def _he_bidi_fix(line: str) -> str:
    """Fix Hebrew RTL display in ExoPlayer-based players."""
    RLI = '\u2067'
    LRI = '\u2066'
    RLM = '\u200F'
    PDI = '\u2069'

    m = _LEADING_PUNCT_RE.match(line)
    if m:
        leading_tags = m.group(1)
        punct = m.group(2)
        rest = line[m.end():]
        trail = rest.rstrip()
        if trail.endswith('-'):
            rest_body = rest[:rest.rfind('-')].strip()
            rest = '-' + rest_body + punct
        else:
            rest = rest + punct
        line = leading_tags + rest

    parts = _HTML_TAG_RE.split(line)
    result = ''
    for i, part in enumerate(parts):
        if i % 2 == 0:
            mirrored = part.translate(_BRACKET_FLIP)
            result += _LATIN_RUN_RE.sub(lambda mo: LRI + mo.group() + PDI, mirrored)
        else:
            result += part

    return RLI + result + RLM + PDI


def _is_likely_hebrew(path: str) -> bool:
    """Return True if the file contains Hebrew Unicode characters (U+0590–U+05FF)."""
    try:
        with open(path, "rb") as f:
            raw = f.read(2048)
        text = raw.decode("utf-8", errors="ignore")
        return any('\u0590' <= c <= '\u05FF' for c in text)
    except Exception:
        return False


def _find_subtitle_files(video_path: str) -> Dict[str, str]:
    """
    Return {lang: path} for external subtitle files alongside the video file.
    Matches:
      <basename>.<lang>.srt  e.g.  Show.S01E01.he.srt  → {"he": "..."}
      <basename>.srt         (unlabeled)                → {"": "..."}
    """
    base = os.path.splitext(video_path)[0]
    result: Dict[str, str] = {}
    for lang in sorted(_LANG_CODES):
        for candidate in [f"{base}.{lang}.srt", f"{base}.{lang}.hi.srt"]:
            if os.path.exists(candidate):
                result[lang] = candidate
                break
    plain = f"{base}.srt"
    if os.path.exists(plain):
        if result and "he" not in result and _is_likely_hebrew(plain):
            result["he"] = plain
        elif not result:
            result[""] = plain
    if result:
        log.debug("Subtitle files found for %s: %s", os.path.basename(video_path), list(result.keys()))
    return result


def _extract_embedded_srt(path: str, lang: str, start_sec: float = 0.0,
                          duration_sec: float = 0.0, timeout: int = 30) -> str:
    """
    Extract the subtitle stream matching `lang` from a video file and return SRT text.
    Returns "" on any failure.
    """
    try:
        r = subprocess.run([
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-select_streams", "s", path,
        ], capture_output=True, text=True, timeout=10)
        streams = __import__('json').loads(r.stdout).get("streams", [])
        stream_idx = None
        from .ffprobe_utils import _lang_matches
        for i, s in enumerate(streams):
            slang = s.get("tags", {}).get("language", "")
            if _lang_matches(slang, lang):
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
            cmd += ["-ss", f"{start_sec:.3f}"]
        cmd += ["-i", path]
        if duration_sec > 0:
            cmd += ["-t", f"{duration_sec:.3f}"]
        cmd += ["-map", f"0:s:{stream_idx}", "-f", "srt", "pipe:1"]

        r2 = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        raw = r2.stdout
        if r2.returncode == 0 and raw.strip():
            if start_sec > 1.0:
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
