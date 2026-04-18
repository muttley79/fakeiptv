"""
live_subtitle.py — Async subtitle writer and live SRT watcher for ChannelStreamer.

Extracted from streamer.py to keep ChannelStreamer focused on ffmpeg lifecycle.
"""
import logging
import os
import threading
import time
from typing import Callable, Dict, Optional, Set

from .ffprobe_utils import (
    _probe_segment_start_pts,
    _probe_keyframe_inpoint,
    _probe_stream_start_time,
)
from .subtitle_utils import _read_srt, _parse_srt_cues, _sec_to_vtt_ts, _he_bidi_fix

log = logging.getLogger(__name__)


class LiveSubtitleWriter:
    """
    Handles the two-phase async subtitle pipeline for a running ChannelStreamer:

    Phase 1 (write_subtitle_files_async):
      Wait for first TS segment → probe start_pts → write stub VTTs →
      probe keyframe inpoint → build cue lists → overwrite VTTs with real cues →
      start _watch_live_srt threads for ffmpeg SRT side-outputs.

    Phase 2 (_watch_live_srt):
      Poll the ffmpeg SRT side-output file as it grows and rewrite the VTT
      progressively until the channel stops or a newer _launch() supersedes this run.
    """

    def __init__(
        self,
        channel_id: str,
        hls_dir: str,
        stop_event: threading.Event,
        subtitle_streamers: dict,
        subtitle_ready_event: threading.Event,
        live_srt_langs: Set[str],
        get_launch_time: Callable[[], float],
    ):
        self._channel_id = channel_id
        self._hls_dir = hls_dir
        self._stop_event = stop_event
        self._subtitle_streamers = subtitle_streamers
        self._subtitle_ready_event = subtitle_ready_event
        self._live_srt_langs = live_srt_langs
        self._get_launch_time = get_launch_time

    def write_subtitle_files_async(
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
            if os.path.isdir(self._hls_dir):
                ts_files = [
                    os.path.join(self._hls_dir, f)
                    for f in os.listdir(self._hls_dir)
                    if f.endswith(".ts")
                ]
                if ts_files:
                    seg_path = min(ts_files, key=os.path.getmtime)
                    break
            time.sleep(0.1)

        # Step 2 — probe start_pts
        if seg_path is None:
            log.warning("_write_subtitle_files_async: no TS segment for %s — using MPEGTS:0",
                        self._channel_id)
            start_pts = 0
        else:
            start_pts = _probe_segment_start_pts(seg_path) or 0
            log.info(
                "Channel %s: subtitle TIMESTAMP-MAP → MPEGTS:%d (%.3fs)",
                self._channel_id, start_pts, start_pts / 90000,
            )

        # Step 2.5 — write empty VTT stubs with the correct MPEGTS anchor and
        # set _subtitle_ready_event immediately.  The player gets valid (empty)
        # VTTs right away; cues are filled in during step 5 below.  This keeps
        # subtitle requests non-blocking even when the keyframe probe is slow
        # (e.g. UHD REMUXes without a seek index that need linear scanning).
        for _lang in subtitle_langs:
            _sub = self._subtitle_streamers.get(_lang)
            if _sub:
                _sub.write_files([], 0, start_pts)
        self._subtitle_ready_event.set()
        log.info("Channel %s: subtitle stubs ready (MPEGTS:%d) — cue build pending",
                 self._channel_id, start_pts)

        # Step 3 — probe keyframe inpoint. NAS is warm (ffmpeg has been reading
        # the file since the segment appeared).  Use a longer timeout (60s) than
        # the default 15s: the player is no longer blocked (stubs written above)
        # and files without a seek index need time for linear scanning.
        actual_inpoint = nominal_inpoint
        if nominal_inpoint > 0 and entry_path:
            actual_inpoint = _probe_keyframe_inpoint(
                entry_path, nominal_inpoint, entry_duration, timeout=60
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
                    self._channel_id, video_start_time,
                    actual_inpoint, actual_inpoint - video_start_time,
                )
        corrected_inpoint = actual_inpoint - video_start_time

        log.info(
            "Channel %s: subtitle inpoint nominal=%.3fs actual=%.3fs "
            "(snap=%.3fs, video_start=%.3fs, entry=%s)",
            self._channel_id, nominal_inpoint, actual_inpoint,
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
                              self._channel_id, lang or "und")
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
        launch_time = self._get_launch_time()
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
                srt_path = os.path.join(self._hls_dir, f"sub_{lang_label}.srt")
                threading.Thread(
                    target=self._watch_live_srt,
                    args=(lang, srt_path, sub.vtt_path, start_pts,
                          cue_lines, launch_time),
                    daemon=True,
                    name=f"live-srt-{self._channel_id}-{lang_label}",
                ).start()
                log.info("Channel %s: started live SRT watcher for [%s]",
                         self._channel_id, lang_label)

        log.info(
            "Channel %s: subtitle tracks ready: %s (no cues: %s)",
            self._channel_id, ready_langs or "none",
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
            if self._get_launch_time() != launch_time:
                return

            time.sleep(2)

            if self._stop_event.is_set() or self._get_launch_time() != launch_time:
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
                        self._channel_id, lang_label, stall_count * 2,
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
                    self._channel_id, lang_label,
                    len(ffmpeg_cue_lines), len(existing_cue_lines),
                )
            except Exception as exc:
                log.debug(
                    "live SRT watcher %s [%s]: error: %s",
                    self._channel_id, lang_label, exc,
                )
