"""
server.py — Flask HTTP server.

Endpoints:
  GET /playlist.m3u8                        IPTV channel list
  GET /epg.xml                              XMLTV EPG (past + future window)
  GET /hls/<channel_id>/stream.m3u8         Live HLS manifest
  GET /hls/<channel_id>/<segment>           HLS TS segment / subtitle file
  GET /catchup/<channel_id>                 Catchup VOD manifest (returns redirect)
  GET /catchup/<channel_id>/<session_id>/stream.m3u8    Catchup VOD manifest
  GET /catchup/<channel_id>/<session_id>/<segment>      Catchup VOD segment
  GET /refresh                              Trigger library rescan
  GET /status                               JSON status
"""
import gzip
import logging
import os
import re
import threading
import time
from datetime import datetime
from typing import TYPE_CHECKING

from flask import Flask, Response, abort, jsonify, redirect, request, send_from_directory

from .hls_utils import _build_master_playlist, _inject_discontinuity, _parse_media_sequence, _bumper_manifest_content, _bumper_response

if TYPE_CHECKING:
    from .app import FakeIPTV

log = logging.getLogger(__name__)

app = Flask(__name__)
_app_instance: "FakeIPTV" = None  # set by app.py before server starts

@app.before_request
def _start_timer():
    request._start_time = time.time()

@app.after_request
def _log_request(response):
    elapsed_ms = (time.time() - request._start_time) * 1000
    log.debug("%s %s %d %.0fms", request.method, request.full_path.rstrip("?"), response.status_code, elapsed_ms)
    return response

_prewarm_done = False              # pre-warm all channels on first manifest request
_bumper_served_channels: set = set()   # live channels/catchup sessions that last received a bumper manifest
_channel_bumper: dict = {}             # channel_id → pinned bumper for this loading period
_discontinuity_pending: set = set()    # channels: inject #EXT-X-DISCONTINUITY into next video.m3u8
_catchup_seq_offsets: dict = {}        # session_id → seq_offset to apply to stream.m3u8

# Matches HLS segment filenames like "seg42.ts" → group(1) = "42"
_SEG_RE = re.compile(r"^seg(\d+)\.ts$")


def set_app(instance: "FakeIPTV"):
    global _app_instance
    _app_instance = instance


# ---------------------------------------------------------------------------
# Channel list + EPG
# ---------------------------------------------------------------------------

@app.route("/playlist.m3u8")
def playlist():
    content = _app_instance.get_playlist()
    return Response(content, mimetype="application/x-mpegurl")


@app.route("/epg.xml")
def epg():
    content = _app_instance.get_epg()
    return Response(content, mimetype="application/xml")


@app.route("/epg.xml.gz")
def epg_gz():
    content = _app_instance.get_epg().encode("utf-8")
    compressed = gzip.compress(content)
    return Response(compressed, mimetype="application/x-gzip")


# ---------------------------------------------------------------------------
# HLS stream
# ---------------------------------------------------------------------------



@app.route("/hls/<channel_id>/stream.m3u8")
def hls_manifest(channel_id: str):
    global _prewarm_done
    if channel_id not in _app_instance.channels:
        abort(404)

    # catchup="shift" — Televizo appends ?utc=TIMESTAMP to the stream URL
    utc_str = request.args.get("utc") or request.args.get("start")
    if utc_str and "{" not in utc_str:
        try:
            at = datetime.fromtimestamp(int(utc_str))
        except (ValueError, TypeError):
            abort(400)
        log.info("Catchup: channel=%s utc=%s → local=%s", channel_id, utc_str, at.isoformat())
        channel = _app_instance.channels[channel_id]
        session = _app_instance.catchup_manager.get_or_create(channel, at)
        if session is None:
            abort(404)
        log.info("Catchup: resolved entry='%s' offset=%.1fs session=%s",
                 session.entry.title, session.offset_sec, session.session_id)
        # If a bumper is available, redirect immediately — catchup_manifest() will
        # serve the bumper while the session warms up.  Without a bumper, keep the
        # original behaviour: wait here so the player always gets a valid manifest.
        if _app_instance.stream_manager.get_random_bumper() is None:
            deadline = time.time() + 30
            while not session.is_ready():
                if session.is_failed() or time.time() > deadline:
                    abort(503)
                time.sleep(0.5)
        return redirect(f"/catchup/{channel_id}/{session.session_id}/stream.m3u8")

    # Pre-warm all channels on first request of each "session" (if enabled via FAKEIPTV_PREWARM).
    # _prewarm_done resets when all channels have gone idle (nobody watching),
    # so the next viewer triggers a fresh pre-warm.
    if _app_instance.config.server.prewarm or _app_instance.config.server.prewarm_session:
        global _prewarm_done
        if not _prewarm_done:
            _prewarm_done = True
            _app_instance.prewarm_channels()
        elif not _app_instance.stream_manager.has_active_streamers():
            # All channels went idle since last pre-warm — reset so next session warms up
            _prewarm_done = False

    # Check bumper availability before starting the channel so we can use
    # background=True when a bumper is ready — this lets start() run its NAS
    # probes and prewarm in a daemon thread while the bumper is served immediately.
    # Pin a bumper for this loading period so all subsequent video.m3u8 polls
    # serve the same bumper (avoids alternating segments when multiple bumpers exist).
    if channel_id not in _channel_bumper:
        _channel_bumper[channel_id] = _app_instance.stream_manager.get_random_bumper()
    bumper = _channel_bumper[channel_id]
    has_bumper = bumper is not None and bumper.is_ready()

    # Start ffmpeg lazily on first client request for this channel.
    # background=True when a bumper covers the gap: the streamer is registered
    # synchronously (instant) but start() — including NAS prewarm — runs in a
    # daemon thread so ffmpeg warmup happens in parallel with bumper display.
    if not _app_instance.stream_manager.ensure_started(channel_id, background=has_bumper):
        abort(404)

    _app_instance.stream_manager.touch(channel_id)

    # If the channel isn't ready yet, serve the bumper loading screen.
    # 3 segments (6s) pre-buffered before the discontinuity fires: the
    # player's download thread fills the buffer during the flush — no visible stall.
    subtitle_langs = _app_instance.stream_manager.get_subtitle_languages(channel_id)
    if not _app_instance.stream_manager.is_transition_ready(channel_id, min_segments=3):
        if has_bumper:
            # Channel warming — return master pointing to video.m3u8 immediately.
            # hls_segment() serves bumper content from video.m3u8 while not ready,
            # then real segments once is_transition_ready().
            # The variant URL (video.m3u8) NEVER changes in the master, so Televizo
            # keeps polling video.m3u8 on its ~4s live-variant cycle.  The switch
            # happens within one poll — no 20s master ABR re-check delay.
            # Subtitle tracks ARE included during bumper so ExoPlayer registers the
            # rendition from the start.  hls_sub_manifest returns a stub playlist
            # (not 503) when video.m3u8 hasn't been written yet, so no stall.
            content = _build_master_playlist(subtitle_langs)
            resp = Response(content, mimetype="application/x-mpegurl")
            resp.headers["Cache-Control"] = "no-cache, no-store"
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp
        # No bumper — block until the channel has enough segments to play.
        if not _app_instance.stream_manager.wait_ready(channel_id, timeout=60):
            abort(503)

    _channel_bumper.pop(channel_id, None)  # clear pin; next cold start picks fresh bumper
    log.debug("hls_manifest %s: subtitle_langs=%s", channel_id, subtitle_langs or "none")
    content = _build_master_playlist(subtitle_langs)
    resp = Response(content, mimetype="application/x-mpegurl")
    resp.headers["Cache-Control"] = "no-cache, no-store"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/hls/<channel_id>/sub_<lang>.m3u8")
def hls_sub_manifest(channel_id: str, lang: str):
    """
    Dynamic subtitle manifest — mirrors the video playlist's live media-sequence
    so the player can match subtitle segments to video segments by sequence number.

    Serves the same single VTT file for every sequence number.  The VTT uses
    X-TIMESTAMP-MAP so players that support it align cues to video PTS.
    Players that don't use TIMESTAMP-MAP still get cues with correct LOCAL
    timestamps because we offset them from stream start (LOCAL=0).

    No EXT-X-ENDLIST → player treats this as a live playlist and polls it,
    matching the live behaviour of video.m3u8.
    """
    if channel_id not in _app_instance.channels:
        abort(404)

    hls_dir = _app_instance.stream_manager.get_hls_dir(channel_id)
    if not hls_dir:
        abort(404)

    vtt_name = f"sub_{lang}.vtt"
    vtt_path = os.path.join(hls_dir, vtt_name)

    # Mirror the video manifest exactly — same EXTINF durations, same media-sequence,
    # but replace each segment filename with the VTT file.  This satisfies the HLS
    # spec (TARGETDURATION ≥ all EXTINF values) and gives Televizo a proper live
    # sliding-window subtitle playlist that maps 1:1 with video segments.
    video_manifest = os.path.join(hls_dir, "video.m3u8")
    out = []
    try:
        with open(video_manifest, "r") as f:
            video_lines = f.readlines()
        if not os.path.exists(vtt_path):
            # video.m3u8 exists but VTT not yet written (extraction running).
            # Return empty stub — the player keeps polling and gets real cues shortly.
            stub = (
                "#EXTM3U\n"
                "#EXT-X-VERSION:3\n"
                "#EXT-X-TARGETDURATION:2\n"
                "#EXT-X-MEDIA-SEQUENCE:0\n"
            )
            resp = Response(stub, mimetype="application/x-mpegurl")
            resp.headers["Cache-Control"] = "no-cache, no-store"
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp
        replace_next = False
        for line in video_lines:
            if line.startswith("#EXT-X-ENDLIST"):
                continue  # no ENDLIST — keep live
            elif line.startswith("#EXTINF:"):
                out.append(line)
                replace_next = True
            elif replace_next:
                out.append(f"{vtt_name}\n")
                replace_next = False
            else:
                out.append(line)
    except FileNotFoundError:
        # video.m3u8 not written yet (channel warming — bumper is playing).
        # Mirror the bumper's video.m3u8 with empty.vtt so the subtitle manifest
        # has the same MEDIA-SEQUENCE as the bumper video track.  Without this,
        # a selected subtitle track causes ExoPlayer to stall: it tries to buffer
        # subtitle seq N to match bumper video seq N but the sub manifest is at
        # seq 0 with no segments.
        bumper = _channel_bumper.get(channel_id)
        if bumper is not None and bumper.is_ready():
            raw = bumper.manifest_content()
            if raw:
                # Mirror bumper video manifest but replace .ts segments with empty.vtt
                # so the subtitle track has the same MEDIA-SEQUENCE as the video track.
                out_lines = []
                for line in raw.splitlines(keepends=True):
                    if line.strip().endswith(".ts"):
                        out_lines.append(
                            f"/hls/_loading/{bumper.bumper_id}/empty.vtt\n"
                        )
                    else:
                        out_lines.append(line)
                resp = Response("".join(out_lines), mimetype="application/x-mpegurl")
                resp.headers["Cache-Control"] = "no-cache, no-store"
                resp.headers["Access-Control-Allow-Origin"] = "*"
                return resp
        stub = (
            "#EXTM3U\n"
            "#EXT-X-VERSION:3\n"
            "#EXT-X-TARGETDURATION:2\n"
            "#EXT-X-MEDIA-SEQUENCE:0\n"
        )
        resp = Response(stub, mimetype="application/x-mpegurl")
        resp.headers["Cache-Control"] = "no-cache, no-store"
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    except Exception:
        abort(503)

    content = "".join(out)

    seq_offset = _app_instance.stream_manager.get_seq_offset(channel_id)
    if seq_offset:
        content = re.sub(
            r"(#EXT-X-MEDIA-SEQUENCE:)(\d+)",
            lambda m: m.group(1) + str(int(m.group(2)) + seq_offset),
            content,
        )

    resp = Response(content, mimetype="application/x-mpegurl")
    resp.headers["Cache-Control"] = "no-cache, no-store"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/hls/<channel_id>/<path:segment>")
def hls_segment(channel_id: str, segment: str):
    if channel_id not in _app_instance.channels:
        abort(404)

    hls_dir = _app_instance.stream_manager.get_hls_dir(channel_id)
    if not hls_dir:
        abort(404)

    # video.m3u8 doubles as the bumper delivery vehicle: while the channel is warming
    # up, serve bumper content from this URL so the variant URL declared in the master
    # never changes.  The player polls video.m3u8 every ~4s; real segments appear on
    # the next poll after is_transition_ready() — no 20s master ABR re-check delay.
    # Must run BEFORE seg_path / file-existence checks so we can serve bumper content
    # even when video.m3u8 doesn't exist on disk yet (ffmpeg still starting up).
    if segment == "video.m3u8":
        if not _app_instance.stream_manager.is_transition_ready(channel_id, min_segments=3):
            bumper = _channel_bumper.get(channel_id)
            if bumper is not None and bumper.is_ready():
                content = _bumper_manifest_content(bumper)
                if content:
                    _bumper_served_channels.add(channel_id)
                    resp = Response(content, mimetype="application/x-mpegurl")
                    resp.headers["Cache-Control"] = "no-cache, no-store"
                    resp.headers["Access-Control-Allow-Origin"] = "*"
                    _app_instance.stream_manager.touch(channel_id)
                    return resp
            # No bumper or unreadable — fall through; video.m3u8 may not exist yet
            # which will 404 cleanly below.
        elif channel_id in _bumper_served_channels:
            # First real video.m3u8 after bumper: inject DISCONTINUITY so ExoPlayer
            # resets all TimestampAdjusters.  Without this, the subtitle adjuster keeps
            # the bumper's accumulated PTS offset, causing subtitles to appear ~3s early.
            # The previous 18s stall was caused by the sub-sync stall inside the bumper
            # (503 / empty stub) delaying DISCONTINUITY processing; with no subtitle
            # tracks declared during bumper phase (fix in hls_manifest), DISCONTINUITY
            # is processed immediately and triggers a fast stream.m3u8 re-check.
            _bumper_served_channels.discard(channel_id)
            _channel_bumper.pop(channel_id, None)
            _discontinuity_pending.add(channel_id)

    seg_path = os.path.join(hls_dir, segment)

    if not os.path.exists(seg_path):
        # On-demand regen: live .ts segment was deleted by ffmpeg's sliding window.
        # Reconstruct it from the source file using get_playing_at().
        if segment.endswith(".ts"):
            m = _SEG_RE.match(segment)
            if m and _app_instance.stream_manager.regenerate_segment(channel_id, int(m.group(1))):
                pass  # file now exists, fall through to serve
            else:
                abort(404)
        else:
            abort(404)

    if segment == "video.m3u8":
        needs_disc = channel_id in _discontinuity_pending
        needs_disc = needs_disc or _app_instance.stream_manager.pop_codec_disc(channel_id)
        seq_offset = _app_instance.stream_manager.get_seq_offset(channel_id)
        if needs_disc or seq_offset:
            if needs_disc:
                _discontinuity_pending.discard(channel_id)
            try:
                with open(seg_path, "r") as f:
                    content = f.read()
                if seq_offset:
                    content = re.sub(
                        r"(#EXT-X-MEDIA-SEQUENCE:)(\d+)",
                        lambda m: m.group(1) + str(int(m.group(2)) + seq_offset),
                        content,
                    )
                if needs_disc:
                    content = _inject_discontinuity(content)
                resp = Response(content, mimetype="application/x-mpegurl")
                resp.headers["Cache-Control"] = "no-cache, no-store"
                resp.headers["Access-Control-Allow-Origin"] = "*"
                _app_instance.stream_manager.touch(channel_id)
                return resp
            except OSError:
                pass  # fall through to send_from_directory on read error

    _app_instance.stream_manager.touch(channel_id)
    resp = send_from_directory(hls_dir, segment)
    resp.headers["Cache-Control"] = "no-cache, no-store"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/hls/_loading/<bumper_id>/<path:segment>")
def bumper_segment(bumper_id: str, segment: str):
    """Serve segments for the bumper loading screen stream."""
    bumper = _app_instance.stream_manager.get_bumper_by_id(bumper_id)
    if bumper is None:
        abort(404)
    # Manifest uses virtual seq-based names (seg{N}.ts) to guarantee unique URLs
    # across loop cycles.  Map back to the actual on-disk file seg{N%n}.ts.
    actual = segment
    m = re.match(r"seg(\d+)\.ts$", segment)
    if m and bumper._segments:
        actual = bumper._segments[int(m.group(1)) % len(bumper._segments)]
    resp = send_from_directory(bumper.hls_dir, actual)
    resp.headers["Cache-Control"] = "no-cache, no-store"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp



# ---------------------------------------------------------------------------
# Control endpoints
# ---------------------------------------------------------------------------

@app.route("/catchup/<channel_id>")
def catchup_start(channel_id: str):
    """
    Televizo calls this with ?utc=<unix_ts>&utcend=<unix_ts>.
    We create (or reuse) a catchup session and redirect to its manifest.
    """
    if channel_id not in _app_instance.channels:
        abort(404)

    # Accept any common timestamp parameter name
    utc_str = (request.args.get("utc") or request.args.get("start")
               or request.args.get("t") or request.args.get("timestamp")
               or request.args.get("begin"))
    log.debug("Catchup request for %s — args: %s", channel_id, dict(request.args))

    # If the player sent the literal template (substitution failed), log it clearly
    if not utc_str or "{" in str(utc_str):
        log.warning(
            "Catchup for %s received unsubstituted URL — player did not fill in timestamp. "
            "Full args: %s", channel_id, dict(request.args)
        )
        abort(400)

    try:
        at = datetime.fromtimestamp(int(utc_str))
    except (ValueError, TypeError):
        log.warning("Catchup for %s — bad utc value: %r", channel_id, utc_str)
        abort(400)
    log.info("Catchup: channel=%s utc=%s → local=%s", channel_id, utc_str, at.isoformat())

    channel = _app_instance.channels[channel_id]
    session = _app_instance.catchup_manager.get_or_create(channel, at)
    if session is None:
        abort(404)
    log.info("Catchup: resolved entry='%s' offset=%.1fs session=%s",
             session.entry.title, session.offset_sec, session.session_id)

    # If a bumper is available, redirect immediately — catchup_manifest() will
    # serve the bumper while the session warms up.  Without a bumper, keep the
    # original behaviour: wait here so the player always gets a valid manifest.
    if _app_instance.stream_manager.get_random_bumper() is None:
        deadline = time.time() + 30
        while not session.is_ready():
            if session.is_failed() or time.time() > deadline:
                abort(503)
            time.sleep(0.5)
    return redirect(f"/catchup/{channel_id}/{session.session_id}/stream.m3u8")


@app.route("/catchup/<channel_id>/<session_id>/sub_<lang>.m3u8")
def catchup_sub_manifest(channel_id: str, session_id: str, lang: str):
    """
    Dynamic subtitle manifest for catchup sessions.

    Mirrors video.m3u8 entry-by-entry (EXTINF:2.0 per segment, MEDIA-SEQUENCE=0,
    no EXT-X-ENDLIST), replacing every segment filename with sub_{lang}.vtt.
    As ffmpeg writes new video segments the list grows and the player downloads
    sub_{lang}.vtt again, picking up the latest cues.  The VTT contains all cues
    with X-TIMESTAMP-MAP so the player shows the right ones at any playback position
    regardless of how far it has buffered ahead.
    """
    session = _app_instance.catchup_manager.get_session(session_id)
    if session is None:
        abort(404)

    vtt_name = f"sub_{lang}.vtt"
    vtt_path = os.path.join(session.session_dir, vtt_name)
    if not os.path.exists(vtt_path):
        abort(404)

    video_manifest = os.path.join(session.session_dir, "video.m3u8")
    try:
        with open(video_manifest, "r") as f:
            video_lines = f.readlines()
    except Exception:
        abort(503)

    # Mirror video.m3u8 header + all EXTINF entries, replacing filenames with
    # the VTT.  Omit EXT-X-ENDLIST so Televizo keeps polling and re-fetches the
    # VTT as new cues are written.
    out = []
    replace_next = False
    for line in video_lines:
        if line.startswith("#EXT-X-ENDLIST"):
            continue
        elif line.startswith("#EXTINF:"):
            out.append(line)
            replace_next = True
        elif replace_next:
            out.append(f"{vtt_name}\n")
            replace_next = False
        else:
            out.append(line)

    content = "".join(out)
    resp = Response(content, mimetype="application/x-mpegurl")
    resp.headers["Cache-Control"] = "no-cache, no-store"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/catchup/<channel_id>/<session_id>/stream.m3u8")
def catchup_manifest(channel_id: str, session_id: str):
    session = _app_instance.catchup_manager.get_session(session_id)
    if session is None:
        abort(404)

    if not session.is_ready():
        if session.is_failed():
            abort(503)
        # Show bumper loading screen while catchup ffmpeg warms up, unless this
        # is a seek within the same episode (is_seek=True) — in that case return
        # 404 so the player retries in ~2s without a bumper flash.
        if not session.is_seek:
            if session_id not in _channel_bumper:
                _channel_bumper[session_id] = _app_instance.stream_manager.get_random_bumper()
            bumper = _channel_bumper[session_id]
            if bumper and bumper.is_ready():
                # Return a minimal master pointing to video.m3u8; catchup_segment()
                # will serve the bumper media playlist from that URL while loading.
                # Never serve bumper media directly from stream.m3u8 — stream.m3u8 must
                # always be a master playlist (serving media causes player type confusion).
                content = _build_master_playlist([], variant_uri="video.m3u8")
                resp = Response(content, mimetype="application/x-mpegurl")
                resp.headers["Cache-Control"] = "no-cache, no-store"
                resp.headers["Access-Control-Allow-Origin"] = "*"
                return resp
        abort(404)

    # Once catchup ffmpeg has finished AND the player has fetched at least one
    # segment, redirect back to live so Televizo seamlessly resumes.
    # Guard: only redirect if a segment was actually fetched (hwm >= 0) to
    # avoid firing before the player has received any content.
    if session.is_done() and session.has_been_watched():
        log.debug("Catchup %s done — redirecting to live channel %s",
                  session_id, channel_id)
        _catchup_seq_offsets.pop(session_id, None)
        _bumper_served_channels.discard(session_id)
        return redirect(f"/hls/{channel_id}/stream.m3u8", code=302)

    _channel_bumper.pop(session_id, None)
    resp = send_from_directory(session.session_dir, "stream.m3u8")
    resp.headers["Cache-Control"] = "no-cache, no-store"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp



@app.route("/catchup/<channel_id>/<session_id>/<path:segment>")
def catchup_segment(channel_id: str, session_id: str, segment: str):
    session = _app_instance.catchup_manager.get_session(session_id)
    if session is None:
        abort(404)

    # video.m3u8 is the variant media playlist — same role as video.m3u8 for live
    # channels.  Serve bumper media here while the catchup ffmpeg warms up, then
    # inject DISCONTINUITY + seq_offset on the first real serve so ExoPlayer resets
    # its PTS adjuster and the media-sequence doesn't jump backward.
    if segment == "video.m3u8":
        if not session.is_ready():
            bumper = _channel_bumper.get(session_id)
            if bumper and bumper.is_ready():
                content = _bumper_manifest_content(bumper)
                if content:
                    _bumper_served_channels.add(session_id)
                    if session_id not in _catchup_seq_offsets:
                        _catchup_seq_offsets[session_id] = _parse_media_sequence(content) + 100
                    resp = Response(content, mimetype="application/x-mpegurl")
                    resp.headers["Cache-Control"] = "no-cache, no-store"
                    resp.headers["Access-Control-Allow-Origin"] = "*"
                    return resp
            abort(404)
        elif session_id in _bumper_served_channels:
            # First real video.m3u8 after bumper: flag for DISCONTINUITY injection
            _bumper_served_channels.discard(session_id)
            _channel_bumper.pop(session_id, None)
            _discontinuity_pending.add(session_id)

        needs_disc = session_id in _discontinuity_pending
        seq_offset = _catchup_seq_offsets.get(session_id, 0)
        if needs_disc or seq_offset:
            if needs_disc:
                _discontinuity_pending.discard(session_id)
            try:
                with open(os.path.join(session.session_dir, "video.m3u8"), "r") as f:
                    content = f.read()
                if seq_offset:
                    content = re.sub(
                        r"(#EXT-X-MEDIA-SEQUENCE:)(\d+)",
                        lambda m: m.group(1) + str(int(m.group(2)) + seq_offset),
                        content,
                    )
                if needs_disc:
                    content = _inject_discontinuity(content)
                resp = Response(content, mimetype="application/x-mpegurl")
                resp.headers["Cache-Control"] = "no-cache, no-store"
                resp.headers["Access-Control-Allow-Origin"] = "*"
                return resp
            except OSError:
                pass

    seg_path = os.path.join(session.session_dir, segment)

    # VTT files may not exist immediately (async extraction).
    # Wait up to 15s so the player gets real cues on first fetch.
    if segment.endswith(".vtt") and not os.path.exists(seg_path):
        deadline = time.time() + 15
        while not os.path.exists(seg_path) and time.time() < deadline:
            time.sleep(0.2)

    if not os.path.exists(seg_path) and segment.endswith(".ts"):
        # On-demand regen: segment was deleted by rolling cleanup (player rewound).
        m = _SEG_RE.match(segment)
        if not m or not session.regenerate_segment(int(m.group(1))):
            abort(404)
    elif not os.path.exists(seg_path):
        abort(404)

    # Advance rolling-delete high-water mark after successful serve.
    if segment.endswith(".ts"):
        m = _SEG_RE.match(segment)
        if m:
            session.mark_fetched(int(m.group(1)))

    resp = send_from_directory(session.session_dir, segment)
    resp.headers["Cache-Control"] = "no-cache, no-store"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/refresh")
def refresh():
    threading.Thread(target=lambda: _app_instance.refresh(force=True), daemon=True).start()
    return jsonify({"status": "ok", "message": "Library refresh started"})


@app.route("/status")
def status():
    from .scheduler import get_now_playing

    channels_status = []
    for ch_id, channel in _app_instance.channels.items():
        np = get_now_playing(channel)
        channels_status.append({
            "id": ch_id,
            "name": channel.name,
            "group": channel.group,
            "entries": len(channel.entries),
            "total_duration_hours": round(channel.total_duration / 3600, 1),
            "ready": _app_instance.stream_manager.is_ready(ch_id),
            "now_playing": {
                "title": np.entry.title,
                "subtitle": np.entry.subtitle,
                "offset_sec": round(np.offset_sec),
                "duration_sec": round(np.entry.duration_sec),
                "path": np.entry.path,
                "subtitle_paths": np.entry.subtitle_paths,
            } if np else None,
        })

    return jsonify({
        "uptime_sec": round(time.time() - _app_instance.start_time),
        "channels": channels_status,
        "library": {
            "shows": len(_app_instance.library.shows),
            "movies": len(_app_instance.library.movies),
        },
    })
