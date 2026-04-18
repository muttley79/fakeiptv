"""
hls_utils.py — HLS manifest building and manipulation helpers.
"""
import re

_LANG_NAMES = {
    "he": "Hebrew", "en": "English", "es": "Spanish", "fr": "French",
    "de": "German", "ar": "Arabic", "ru": "Russian", "pt": "Portuguese",
    "it": "Italian", "nl": "Dutch", "pl": "Polish", "cs": "Czech",
    "ja": "Japanese", "ko": "Korean", "zh": "Chinese", "": "Subtitles",
}


def _bumper_manifest_content(bumper) -> str:
    """Get bumper manifest content."""
    return bumper.manifest_content()


def _parse_media_sequence(content: str) -> int:
    """Extract #EXT-X-MEDIA-SEQUENCE number from HLS manifest."""
    m = re.search(r"#EXT-X-MEDIA-SEQUENCE:(\d+)", content)
    return int(m.group(1)) if m else 0


def _inject_discontinuity(content: str) -> str:
    """Insert #EXT-X-DISCONTINUITY before the first #EXTINF line."""
    lines = content.splitlines(keepends=True)
    for i, line in enumerate(lines):
        if line.startswith("#EXTINF:"):
            lines.insert(i, "#EXT-X-DISCONTINUITY\n")
            break
    return "".join(lines)


def _bumper_response(channel_id, bumper, _bumper_served_channels=None):
    """Return a Flask Response serving the bumper manifest."""
    from flask import Response
    content = _bumper_manifest_content(bumper)
    if not content:
        return None
    if channel_id is not None and _bumper_served_channels is not None:
        _bumper_served_channels.add(channel_id)
    resp = Response(content, mimetype="application/x-mpegurl")
    resp.headers["Cache-Control"] = "no-cache, no-store"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


def _build_master_playlist(subtitle_langs, variant_uri="video.m3u8") -> str:
    """Build an HLS master playlist with subtitle tracks."""
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
    if subtitle_langs:
        lines.append(f'#EXT-X-STREAM-INF:BANDWIDTH=8000000,SUBTITLES="subs"\n')
    else:
        lines.append("#EXT-X-STREAM-INF:BANDWIDTH=8000000\n")
    lines.append(f"{variant_uri}\n")
    return "".join(lines)
