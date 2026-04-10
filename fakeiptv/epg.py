"""
epg.py — Generates XMLTV-format EPG XML from a 24-hour schedule window.
"""
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Tuple

from .scheduler import Channel, ScheduleEntry


def _fmt_xmltv_time(dt: datetime) -> str:
    """Format datetime as XMLTV timestamp: 20260410222615 +0000"""
    return dt.strftime("%Y%m%d%H%M%S") + " +0000"


def build_xmltv(
    channels: Dict[str, Channel],
    schedule: Dict[str, List[Tuple[datetime, datetime, ScheduleEntry]]],
) -> str:
    """Return XMLTV XML string."""
    root = ET.Element("tv", attrib={
        "generator-info-name": "fakeiptv",
        "source-info-name": "fakeiptv",
    })

    # --- Channel definitions ---
    for ch_id, channel in channels.items():
        ch_el = ET.SubElement(root, "channel", id=ch_id)
        ET.SubElement(ch_el, "display-name").text = channel.name
        if channel.poster_url:
            ET.SubElement(ch_el, "icon", src=channel.poster_url)

    # --- Programme entries ---
    for ch_id, slots in schedule.items():
        for start, end, entry in slots:
            prog = ET.SubElement(root, "programme", attrib={
                "start": _fmt_xmltv_time(start),
                "stop": _fmt_xmltv_time(end),
                "channel": ch_id,
            })
            ET.SubElement(prog, "title", lang="en").text = entry.title
            if entry.subtitle and entry.subtitle != entry.title:
                ET.SubElement(prog, "sub-title", lang="en").text = entry.subtitle
            if entry.plot:
                ET.SubElement(prog, "desc", lang="en").text = entry.plot
            if entry.season and entry.episode:
                # XMLTV episode-num system="onscreen": S01 E03
                ET.SubElement(prog, "episode-num", system="onscreen").text = (
                    f"S{entry.season:02d} E{entry.episode:02d}"
                )
                # XMLTV episode-num system="xmltv_ns": season.episode.part (0-indexed)
                ET.SubElement(prog, "episode-num", system="xmltv_ns").text = (
                    f"{entry.season - 1}.{entry.episode - 1}.0/1"
                )
            if entry.poster_url:
                ET.SubElement(prog, "icon", src=entry.poster_url)

    return '<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(
        root, encoding="unicode", xml_declaration=False
    )
