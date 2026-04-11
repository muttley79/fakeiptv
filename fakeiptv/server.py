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


def set_app(instance: "FakeIPTV"):
    global _app_instance
    _app_instance = instance


# ---------------------------------------------------------------------------
# Channel list + EPG
# ---------------------------------------------------------------------------

@app.route("/playlist.m3u8")
def playlist():
    content = _app_instance.get_playlist()
    # Pre-warm all channels in the background so they're ready before the user
    # taps one.  Channels that are already running are skipped immediately.
    _app_instance.prewarm_channels()
    return Response(content, mimetype="application/x-mpegurl")


@app.route("/epg.xml")
def epg():
    content = _app_instance.get_epg()
    return Response(content, mimetype="application/xml")


# ---------------------------------------------------------------------------
# HLS stream
# ---------------------------------------------------------------------------

@app.route("/hls/<channel_id>/stream.m3u8")
def hls_manifest(channel_id: str):
    if channel_id not in _app_instance.channels:
        abort(404)

    # Start ffmpeg lazily on first client request for this channel
    if not _app_instance.stream_manager.ensure_started(channel_id):
        abort(404)

    # Wait up to 15 seconds for ffmpeg to write the first manifest
    deadline = time.time() + 15
    while not _app_instance.stream_manager.is_ready(channel_id):
        if time.time() > deadline:
            abort(503)
        time.sleep(0.5)

    _app_instance.stream_manager.touch(channel_id)
    hls_dir = _app_instance.stream_manager.get_hls_dir(channel_id)
    resp = send_from_directory(hls_dir, "stream.m3u8")
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

    utc_str = request.args.get("utc") or request.args.get("start")
    if not utc_str:
        abort(400)

    try:
        at = datetime.utcfromtimestamp(int(utc_str))
        # Convert to local time to match our EPOCH-based schedule
        import time as _time
        local_offset = _time.timezone if (_time.daylight == 0) else _time.altzone
        at = datetime.fromtimestamp(int(utc_str) - local_offset)
    except (ValueError, TypeError):
        abort(400)

    channel = _app_instance.channels[channel_id]
    session = _app_instance.catchup_manager.get_or_create(channel, at)
    if session is None:
        abort(404)

    # Wait up to 15s for the first segment
    deadline = time.time() + 15
    while not session.is_ready():
        if time.time() > deadline:
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
