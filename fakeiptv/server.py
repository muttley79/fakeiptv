"""
server.py — Flask HTTP server.

Endpoints:
  GET /playlist.m3u8                    IPTV channel list
  GET /epg.xml                          XMLTV EPG (24h window)
  GET /hls/<channel_id>/stream.m3u8     Live HLS manifest
  GET /hls/<channel_id>/<segment>.ts    HLS TS segment
  GET /refresh                          Trigger library rescan
  GET /status                           JSON status
"""
import logging
import os
import time
from typing import TYPE_CHECKING

from flask import Flask, Response, abort, jsonify, send_from_directory

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

    hls_dir = _app_instance.stream_manager.get_hls_dir(channel_id)
    if not hls_dir:
        abort(404)

    # Wait up to 15 seconds for ffmpeg to write the first manifest
    deadline = time.time() + 15
    while not _app_instance.stream_manager.is_ready(channel_id):
        if time.time() > deadline:
            abort(503)
        time.sleep(0.5)

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

    resp = send_from_directory(hls_dir, segment)
    resp.headers["Cache-Control"] = "no-cache, no-store"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


# ---------------------------------------------------------------------------
# Control endpoints
# ---------------------------------------------------------------------------

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
