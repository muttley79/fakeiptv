"""
epg.py — Generates XMLTV-format EPG XML from a 24-hour schedule window.
"""
import time as _time
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from .models import Channel, ScheduleEntry


def _local_offset_sec() -> int:
    """Return local UTC offset in seconds (positive = east of UTC)."""
    if _time.daylight and _time.localtime().tm_isdst:
        return -_time.altzone
    return -_time.timezone


def _to_utc(dt: datetime) -> datetime:
    """Convert a naive local datetime to UTC."""
    return dt - timedelta(seconds=_local_offset_sec())


def _fmt_xmltv_time(dt: datetime) -> str:
    """Format a local datetime as XMLTV UTC timestamp."""
    return _to_utc(dt).strftime("%Y%m%d%H%M%S") + " +0000"


def _esc(text: str) -> str:
    """Escape XML special characters."""
    return (
        text
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def build_xmltv(
    channels: Dict[str, Channel],
    schedule: Dict[str, List[Tuple[datetime, datetime, ScheduleEntry]]],
) -> str:
    """Return XMLTV XML string compatible with Televizo and other IPTV clients."""
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<!DOCTYPE tv SYSTEM "https://raw.githubusercontent.com/XMLTV/xmltv/master/xmltv.dtd">',
        "<tv>",
    ]

    # --- Channel definitions ---
    for ch_id, channel in channels.items():
        lines.append(f'  <channel id="{_esc(ch_id)}">')
        lines.append(f"    <display-name>{_esc(channel.name)}</display-name>")
        if channel.poster_url:
            lines.append(f'    <icon src="{_esc(channel.poster_url)}" />')
        lines.append("  </channel>")

    # --- Programme entries ---
    for ch_id, slots in schedule.items():
        for start, end, entry in slots:
            start_s = _fmt_xmltv_time(start)
            stop_s = _fmt_xmltv_time(end)
            lines.append(
                f'  <programme channel="{_esc(ch_id)}" start="{start_s}" stop="{stop_s}">'
            )
            lines.append(f"    <title>{_esc(entry.title)}</title>")

            subtitle = entry.subtitle if entry.subtitle != entry.title else ""
            if entry.season and entry.episode:
                ep_tag = f"S{entry.season:02d}E{entry.episode:02d}"
                subtitle = f"{ep_tag} - {subtitle}" if subtitle else ep_tag
            if subtitle:
                lines.append(f"    <sub-title>{_esc(subtitle)}</sub-title>")

            if entry.plot:
                lines.append(f"    <desc>{_esc(entry.plot)}</desc>")
            if entry.year:
                lines.append(f"    <date>{entry.year}</date>")
            for genre in entry.genres:
                lines.append(f"    <category>{_esc(genre)}</category>")
            if entry.season and entry.episode:
                lines.append(
                    f'    <episode-num system="onscreen">S{entry.season:02d} E{entry.episode:02d}</episode-num>'
                )
                lines.append(
                    f'    <episode-num system="xmltv_ns">{entry.season - 1}.{entry.episode - 1}.0/1</episode-num>'
                )
            if entry.poster_url:
                lines.append(f'    <icon src="{_esc(entry.poster_url)}" />')
            lines.append("  </programme>")

    lines.append("</tv>")
    return "\n".join(lines) + "\n"
