"""
subtitle_streamer.py — Generates WebVTT subtitle files + HLS playlists per language.
"""
import logging
import os
from typing import Optional

from .models import Channel, NowPlaying
from .scheduler import get_now_playing, CONCAT_HOURS
from .subtitle_utils import (
    _read_srt, _parse_srt_cues, _sec_to_vtt_ts, _text_has_hebrew, _he_bidi_fix,
    _extract_embedded_srt
)

log = logging.getLogger(__name__)


class SubtitleStreamer:
    """Generates a static WebVTT subtitle file + HLS playlist for one language."""

    def __init__(self, channel: Channel, lang: str, hls_dir: str):
        self._channel = channel
        self.lang = lang
        self._is_rtl = (lang == "he")
        self.hls_dir = hls_dir
        lang_label = lang or "und"
        self.vtt_path = os.path.join(hls_dir, f"sub_{lang_label}.vtt")
        self.manifest_path = os.path.join(hls_dir, f"sub_{lang_label}.m3u8")
        self._ok = False
        self.has_ffmpeg_srt = False

    def write_placeholder(self):
        """Write an empty VTT immediately so hls_sub_manifest can return without blocking."""
        os.makedirs(self.hls_dir, exist_ok=True)
        with open(self.vtt_path, "w", encoding="utf-8") as f:
            f.write("WEBVTT\nX-TIMESTAMP-MAP=MPEGTS:0,LOCAL:00:00:00.000\n\n")
        log.debug("Subtitle %s (%s): placeholder VTT written — awaiting extraction",
                  self.lang or "und", self._channel.id)

    def is_running(self) -> bool:
        """True when the subtitle files have been written with a correct TIMESTAMP-MAP."""
        return self._ok

    def build_cues(self, inpoint: float):
        """Phase 1: parse SRT files, return (cue_lines, cue_count)."""
        return self._generate(inpoint)

    def write_files(self, cue_lines: list, cue_count: int, start_pts: int):
        """Phase 2: write VTT + manifest to disk with the correct TIMESTAMP-MAP."""
        total_seconds = CONCAT_HOURS * 3600
        with open(self.vtt_path, "w", encoding="utf-8") as f:
            f.write("WEBVTT\n")
            f.write(f"X-TIMESTAMP-MAP=MPEGTS:{start_pts},LOCAL:00:00:00.000\n\n")
            cue_style = (
                "  background-color: transparent;\n"
                "  text-shadow: 1px 0 0 #000, -1px 0 0 #000, 0 1px 0 #000, 0 -1px 0 #000;\n"
            )
            if self._is_rtl:
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
        """Parse SRT / embedded subtitle streams and return (cue_lines, cue_count)."""
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
        is_current_entry = True

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
                if not self._is_rtl and self.lang == "" and _text_has_hebrew(raw):
                    self._is_rtl = True
                cues = _parse_srt_cues(raw)
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
                    if self._is_rtl:
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
                if is_current_entry and self.has_ffmpeg_srt:
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
                        if self._is_rtl:
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
