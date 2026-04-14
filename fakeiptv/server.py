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
import time
from datetime import datetime
from typing import TYPE_CHECKING

from flask import Flask, Response, abort, jsonify, redirect, request, send_from_directory

if TYPE_CHECKING:
    from .app import FakeIPTV

log = logging.getLogger(__name__)

app = Flask(__name__)
_app_instance: "FakeIPTV" = None  # set by app.py before server starts
_prewarm_done = False              # pre-warm all channels on first manifest request


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

_LANG_NAMES = {
    "he": "Hebrew", "en": "English", "es": "Spanish", "fr": "French",
    "de": "German", "ar": "Arabic", "ru": "Russian", "pt": "Portuguese",
    "it": "Italian", "nl": "Dutch", "pl": "Polish", "cs": "Czech",
    "ja": "Japanese", "ko": "Korean", "zh": "Chinese", "": "Subtitles",
}


def _build_master_playlist(subtitle_langs) -> str:
    """
    Build an HLS master playlist that references the video stream (video.m3u8)
    and one subtitle track per language (sub_{lang}.m3u8).
    DEFAULT=NO / AUTOSELECT=NO so a missing or slow subtitle track never
    blocks video playback — the player will load subs opportunistically.
    """
    lines = [
        "#EXTM3U\n",
        "#EXT-X-START:TIME-OFFSET=-4.0,PRECISE=NO\n",
    ]
    for lang in subtitle_langs:
        name = _LANG_NAMES.get(lang, lang.upper() or "Subtitles")
        lang_label = lang or "und"
        lines.append(
            f'#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID="subs",'
            f'LANGUAGE="{lang_label}",NAME="{name}",'
            f'DEFAULT=NO,AUTOSELECT=NO,'
            f'URI="sub_{lang_label}.m3u8"\n'
        )
    lines.append('#EXT-X-STREAM-INF:BANDWIDTH=8000000,SUBTITLES="subs"\n')
    lines.append("video.m3u8\n")
    return "".join(lines)


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
        channel = _app_instance.channels[channel_id]
        session = _app_instance.catchup_manager.get_or_create(channel, at)
        if session is None:
            abort(404)
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

    # Start ffmpeg lazily on first client request for this channel
    if not _app_instance.stream_manager.ensure_started(channel_id):
        abort(404)

    _app_instance.stream_manager.touch(channel_id)

    # Wait (up to 30s) for ffmpeg to produce enough segments (READY_SEGMENTS).
    # Uses threading.Event — all concurrent requests for the same channel share
    # one event, so this does not create one thread per request.
    if not _app_instance.stream_manager.wait_ready(channel_id, timeout=60):
        abort(503)

    hls_dir = _app_instance.stream_manager.get_hls_dir(channel_id)

    # Build master playlist if the channel has subtitle tracks.
    # We do NOT wait for VTT files to be written here — that would add 5-8s
    # of mandatory startup delay.  Instead we declare all channel subtitle
    # languages immediately (they're known from the channel entries), and let
    # the /hls/<id>/<lang>.vtt endpoint wait up to 15s for each file to
    # appear.  This gives the player the master playlist as soon as the first
    # HLS segment is ready (~2s cold start), while subtitles catch up async.
    subtitle_langs = _app_instance.stream_manager.get_subtitle_languages(channel_id)
    log.debug("hls_manifest %s: subtitle_langs=%s", channel_id, subtitle_langs or "none")

    log.debug("hls_manifest %s: serving %s playlist",
              channel_id, "master" if subtitle_langs else "simple video")
    if subtitle_langs:
        content = _build_master_playlist(subtitle_langs)
    else:
        manifest_path = os.path.join(hls_dir, "video.m3u8")
        with open(manifest_path, "r") as f:
            content = f.read()
        # Tell the player to start near the live edge instead of the oldest segment
        # in the sliding window, preventing replay of recently-watched content on
        # channel switch-back. TIME-OFFSET=-4.0 = 2 segments before live edge.
        content = content.replace(
            "#EXTM3U\n",
            "#EXTM3U\n#EXT-X-START:TIME-OFFSET=-4.0,PRECISE=NO\n",
            1,
        )

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

    # Wait up to 15s for the VTT to be written by the async subtitle thread
    if not os.path.exists(vtt_path):
        deadline = time.time() + 15
        while not os.path.exists(vtt_path) and time.time() < deadline:
            time.sleep(0.2)

    if not os.path.exists(vtt_path):
        abort(404)

    # Mirror the video manifest exactly — same EXTINF durations, same media-sequence,
    # but replace each segment filename with the VTT file.  This satisfies the HLS
    # spec (TARGETDURATION ≥ all EXTINF values) and gives Televizo a proper live
    # sliding-window subtitle playlist that maps 1:1 with video segments.
    video_manifest = os.path.join(hls_dir, "video.m3u8")
    out = []
    try:
        with open(video_manifest, "r") as f:
            video_lines = f.readlines()
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
    except Exception:
        abort(503)

    content = "".join(out)

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

    seg_path = os.path.join(hls_dir, segment)

    # VTT files may not be written yet when the player first requests them
    # (async subtitle thread is still running).  Wait up to 15s for the file
    # to appear before returning 404 — covers the ~6s subtitle generation time.
    if segment.endswith(".vtt") and not os.path.exists(seg_path):
        deadline = time.time() + 15
        while not os.path.exists(seg_path) and time.time() < deadline:
            time.sleep(0.2)

    if not os.path.exists(seg_path):
        abort(404)

    _app_instance.stream_manager.touch(channel_id)
    resp = send_from_directory(hls_dir, segment)
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

    channel = _app_instance.channels[channel_id]
    session = _app_instance.catchup_manager.get_or_create(channel, at)
    if session is None:
        abort(404)

    # Wait up to 30s for the first segment (cold NAS can take >15s to open a file)
    deadline = time.time() + 30
    while not session.is_ready():
        if session.is_failed() or time.time() > deadline:
            abort(503)
        time.sleep(0.5)

    manifest_url = f"/catchup/{channel_id}/{session.session_id}/stream.m3u8"
    return redirect(manifest_url)


@app.route("/catchup/<channel_id>/<session_id>/stream.m3u8")
def catchup_manifest(channel_id: str, session_id: str):
    session = _app_instance.catchup_manager.get_session(session_id)
    if session is None or not session.is_ready():
        abort(404)

    resp = send_from_directory(session.session_dir, "stream.m3u8")
    resp.headers["Cache-Control"] = "no-cache, no-store"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/catchup/<channel_id>/<session_id>/<path:segment>")
def catchup_segment(channel_id: str, session_id: str, segment: str):
    session = _app_instance.catchup_manager.get_session(session_id)
    if session is None:
        abort(404)

    seg_path = os.path.join(session.session_dir, segment)
    if not os.path.exists(seg_path):
        abort(404)

    resp = send_from_directory(session.session_dir, segment)
    resp.headers["Cache-Control"] = "no-cache, no-store"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


@app.route("/refresh")
def refresh():
    _app_instance.refresh()
    return jsonify({"status": "ok", "message": "Library refreshed"})


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
