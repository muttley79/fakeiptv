"""
playlist.py — Generates the M3U8 channel list that IPTV clients import.
This is NOT an HLS manifest — it's the Kodi/Televizo-style playlist that
lists all channels with their stream URLs and EPG IDs.
"""
from typing import Dict

from .scheduler import Channel


def build_m3u8(
    channels: Dict[str, Channel],
    base_url: str,
    epg_url: str,
) -> str:
    """
    base_url: e.g. "http://192.168.1.100:8080"
    epg_url:  e.g. "http://192.168.1.100:8080/epg.xml"
    """
    lines = [f'#EXTM3U x-tvg-url="{epg_url}"\n']

    # Sort: Shows first, then Genre Mix, then Movies
    group_order = {"Shows": 0, "Genre Mix": 1, "Movies": 2}
    sorted_channels = sorted(
        channels.values(),
        key=lambda c: (group_order.get(c.group, 9), c.name.lower()),
    )

    for channel in sorted_channels:
        logo = channel.poster_url or ""
        stream_url = f"{base_url}/hls/{channel.id}/stream.m3u8"
        lines.append(
            f'#EXTINF:-1 tvg-id="{channel.id}" tvg-name="{channel.name}" '
            f'tvg-logo="{logo}" group-title="{channel.group}",{channel.name}\n'
        )
        lines.append(f"{stream_url}\n")

    return "".join(lines)
