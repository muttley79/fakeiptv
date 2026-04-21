"""
playlist.py — Generates the M3U8 channel list that IPTV clients import.
This is NOT an HLS manifest — it's the Kodi/Televizo-style playlist that
lists all channels with their stream URLs, EPG IDs, and catchup metadata.
"""
from datetime import datetime
from typing import Dict

from .models import Channel


def build_m3u8(
    channels: Dict[str, Channel],
    base_url: str,
    epg_url: str,
    catchup_days: int = 0,
) -> str:
    """
    base_url:     e.g. "http://192.168.1.100:8080"
    epg_url:      e.g. "http://192.168.1.100:8080/epg.xml"
    catchup_days: if > 0, adds catchup attributes to each channel entry
    """
    generation_date = datetime.now().strftime("%Y.%m.%d %H:%M:%S")
    lines = [f'#EXTM3U generation-date="{generation_date}" url-tvg="{epg_url}"\n']

    # Sort: Shows first, then Genre Mix, then Movies
    group_order = {"Shows": 0, "Genre Mix": 1, "Movies": 2}
    sorted_channels = sorted(
        channels.values(),
        key=lambda c: (group_order.get(c.group, 9), c.name.lower()),
    )

    for chno, channel in enumerate(sorted_channels, start=1):
        logo = f"{base_url}/logos/{channel.id}.png" if not channel.poster_url else channel.poster_url
        stream_url = f"{base_url}/hls/{channel.id}/stream.m3u8"

        catchup_attrs = ""
        if catchup_days > 0:
            catchup_url = f"{base_url}/catchup/{channel.id}?utc={{utc}}&utcend={{utcend}}"
            catchup_attrs = (
                f' catchup="shift"'
                f' catchup-days="{catchup_days}"'
                f' catchup-source="{catchup_url}"'
            )

        lines.append(
            f'#EXTINF:0 tvg-id="{channel.id}" tvg-name="{channel.name}" '
            f'tvg-chno="{chno}" '
            f'tvg-logo="{logo}" group-title="{channel.group}"{catchup_attrs},{channel.name}\n'
        )
        lines.append(f"{stream_url}\n")

    return "".join(lines)
