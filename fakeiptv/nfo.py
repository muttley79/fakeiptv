"""
nfo.py — Kodi/Jellyfin NFO metadata parser.
"""
import logging
import xml.etree.ElementTree as ET

log = logging.getLogger(__name__)


def _nfo_text(root: ET.Element, tag: str) -> str:
    el = root.find(tag)
    return (el.text or "").strip() if el is not None else ""


def _nfo_int(root: ET.Element, tag: str) -> int:
    val = _nfo_text(root, tag)
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def parse_nfo(nfo_path: str) -> dict:
    """Parse a Kodi/Jellyfin .nfo file. Returns a dict of known fields."""
    try:
        tree = ET.parse(nfo_path)
        root = tree.getroot()
    except Exception as e:
        log.debug("Failed to parse NFO %s: %s", nfo_path, e)
        return {}

    genres = [el.text.strip() for el in root.findall("genre") if el.text]

    tmdb_id = ""
    for uid in root.findall("uniqueid"):
        if uid.get("type") == "tmdb" and uid.text:
            tmdb_id = uid.text.strip()
    if not tmdb_id:
        tmdb_id = _nfo_text(root, "tmdbid")

    runtime_min = _nfo_int(root, "runtime")

    rating = 0.0
    rating_str = _nfo_text(root, "rating")
    if not rating_str:
        ratings_el = root.find("ratings")
        if ratings_el is not None:
            val_el = ratings_el.find(".//value")
            if val_el is not None:
                rating_str = (val_el.text or "").strip()
    try:
        rating = float(rating_str) if rating_str else 0.0
    except ValueError:
        rating = 0.0

    return {
        "title": _nfo_text(root, "title"),
        "plot": _nfo_text(root, "plot"),
        "year": _nfo_int(root, "year"),
        "season": _nfo_int(root, "season"),
        "episode": _nfo_int(root, "episode"),
        "runtime_sec": runtime_min * 60 if runtime_min else 0,
        "genres": genres,
        "tmdb_id": tmdb_id,
        "poster": _nfo_text(root, "thumb") or _nfo_text(root, "poster"),
        "rating": rating,
    }
