"""
scanner.py — Walks the NAS, parses NFO metadata, falls back to TMDB,
probes durations via ffprobe, and returns a MediaLibrary.
"""
import json
import logging
import os
import re
import sqlite3
import subprocess
import threading
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import requests

log = logging.getLogger(__name__)

TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".m4v", ".mov"}


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class Episode:
    path: str
    title: str
    show_title: str
    season: int
    episode: int
    duration_sec: float
    plot: str = ""
    genres: List[str] = field(default_factory=list)
    year: int = 0
    poster_url: str = ""
    tmdb_id: str = ""
    rating: float = 0.0   # show-level rating (0–10), propagated to all episodes
    audio_codec: str = ""
    subtitle_paths: Dict[str, str] = field(default_factory=dict)
    has_embedded_subs: bool = False
    is_hdr: bool = False
    video_width: int = 0
    video_height: int = 0
    video_codec: str = ""


@dataclass
class Movie:
    path: str
    title: str
    duration_sec: float
    plot: str = ""
    genres: List[str] = field(default_factory=list)
    year: int = 0
    poster_url: str = ""
    tmdb_id: str = ""
    rating: float = 0.0   # 0–10 scale
    audio_codec: str = ""
    subtitle_paths: Dict[str, str] = field(default_factory=dict)
    has_embedded_subs: bool = False
    is_hdr: bool = False
    video_width: int = 0
    video_height: int = 0
    video_codec: str = ""

@dataclass
class Show:
    name: str
    episodes: List[Episode] = field(default_factory=list)
    genres: List[str] = field(default_factory=list)
    poster_url: str = ""
    rating: float = 0.0   # 0–10 scale, from Sonarr/TMDB/NFO


@dataclass
class MediaLibrary:
    shows: Dict[str, Show] = field(default_factory=dict)
    movies: List[Movie] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Slug helper
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


# ---------------------------------------------------------------------------
# Shared SQLite connection
# ---------------------------------------------------------------------------

def _open_db(db_path: str) -> sqlite3.Connection:
    """Open (or create) the cache DB with WAL mode for safe concurrent access."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


# ---------------------------------------------------------------------------
# Duration cache
# ---------------------------------------------------------------------------

class DurationCache:
    def __init__(self, db_path: str):
        self._conn = _open_db(db_path)
        self._lock = threading.Lock()
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS durations (
                key               TEXT PRIMARY KEY,
                duration          REAL NOT NULL,
                audio_codec       TEXT NOT NULL DEFAULT '',
                has_embedded_subs INTEGER NOT NULL DEFAULT -1,
                is_hdr            INTEGER NOT NULL DEFAULT -1,
                video_width       INTEGER NOT NULL DEFAULT 0,
                video_height      INTEGER NOT NULL DEFAULT 0,
                video_codec       TEXT NOT NULL DEFAULT ''
            )
        """)
        # Migrate older databases that lack the new columns
        for col_def in [
            "ADD COLUMN audio_codec TEXT NOT NULL DEFAULT ''",
            "ADD COLUMN has_embedded_subs INTEGER NOT NULL DEFAULT -1",
            "ADD COLUMN is_hdr INTEGER NOT NULL DEFAULT -1",
            "ADD COLUMN slow_seek INTEGER NOT NULL DEFAULT -1",  # kept for schema compat, unused
            "ADD COLUMN video_width INTEGER NOT NULL DEFAULT 0",
            "ADD COLUMN video_height INTEGER NOT NULL DEFAULT 0",
            "ADD COLUMN video_codec TEXT NOT NULL DEFAULT ''",
        ]:
            try:
                self._conn.execute(f"ALTER TABLE durations {col_def}")
            except Exception:
                pass  # column already exists
        self._conn.commit()

    def _key(self, path: str) -> str:
        try:
            mtime = str(os.path.getmtime(path))
        except OSError:
            mtime = "0"
        return f"{path}|{mtime}"

    def get_info(self, path: str) -> Optional[tuple]:
        """Return (duration, audio_codec, has_embedded_subs, is_hdr, video_width, video_height, video_codec) or None if not cached."""
        row = self._conn.execute(
            "SELECT duration, audio_codec, has_embedded_subs, is_hdr, video_width, video_height, video_codec FROM durations WHERE key = ?",
            (self._key(path),)
        ).fetchone()
        if row is None:
            return None
        duration, audio_codec, has_embedded_subs_int, is_hdr_int, video_width, video_height, video_codec = row
        if has_embedded_subs_int < 0 or is_hdr_int < 0 or video_width == 0 or not video_codec:
            return None  # legacy entry lacking fields — re-probe
        return duration, audio_codec, bool(has_embedded_subs_int), bool(is_hdr_int), video_width, video_height, video_codec

    def set_info(self, path: str, duration: float, audio_codec: str,
                 has_embedded_subs: bool, is_hdr: bool, video_width: int = 0, video_height: int = 0,
                 video_codec: str = ""):
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO durations "
                "(key, duration, audio_codec, has_embedded_subs, is_hdr, video_width, video_height, video_codec) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (self._key(path), duration, audio_codec,
                 int(has_embedded_subs), int(is_hdr), video_width, video_height, video_codec),
            )
            self._conn.commit()

    # Legacy accessors kept for any external callers
    def get(self, path: str) -> Optional[float]:
        info = self.get_info(path)
        return info[0] if info is not None else None

    def set(self, path: str, duration: float):
        self.set_info(path, duration, "", False, False)


# ---------------------------------------------------------------------------
# TMDB cache
# ---------------------------------------------------------------------------

class TMDBCache:
    def __init__(self, db_path: str, api_key: str):
        self._conn = _open_db(db_path)
        self._lock = threading.Lock()
        self._api_key = api_key
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS tmdb (
                key  TEXT PRIMARY KEY,
                data TEXT NOT NULL
            )
        """)
        self._conn.commit()

    def _get(self, url: str, params: dict) -> Optional[dict]:
        if not self._api_key:
            return None
        cache_key = url + json.dumps(params, sort_keys=True)

        row = self._conn.execute(
            "SELECT data FROM tmdb WHERE key = ?", (cache_key,)
        ).fetchone()
        if row:
            return json.loads(row[0])

        try:
            params["api_key"] = self._api_key
            resp = requests.get(url, params=params, timeout=10)
            resp.raise_for_status()
            result = resp.json()
            with self._lock:
                self._conn.execute(
                    "INSERT OR REPLACE INTO tmdb (key, data) VALUES (?, ?)",
                    (cache_key, json.dumps(result)),
                )
                self._conn.commit()
            return result
        except Exception as e:
            log.warning("TMDB request failed %s: %s", url, e)
            return None

    def fetch_movie(self, tmdb_id: str) -> Optional[dict]:
        return self._get(f"{TMDB_BASE}/movie/{tmdb_id}", {})

    def fetch_show(self, tmdb_id: str) -> Optional[dict]:
        return self._get(f"{TMDB_BASE}/tv/{tmdb_id}", {})

    def fetch_episode(self, tmdb_id: str, season: int, episode: int) -> Optional[dict]:
        return self._get(f"{TMDB_BASE}/tv/{tmdb_id}/season/{season}/episode/{episode}", {})

    def search_movie(self, title: str, year: int = 0) -> Optional[dict]:
        params = {"query": title}
        if year:
            params["year"] = year
        result = self._get(f"{TMDB_BASE}/search/movie", params)
        if result and result.get("results"):
            return result["results"][0]
        return None

    def search_show(self, title: str) -> Optional[dict]:
        result = self._get(f"{TMDB_BASE}/search/tv", {"query": title})
        if result and result.get("results"):
            return result["results"][0]
        return None


# ---------------------------------------------------------------------------
# NFO parser
# ---------------------------------------------------------------------------

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

    # TMDB ID — check uniqueid type="tmdb" or tmdbid tag
    tmdb_id = ""
    for uid in root.findall("uniqueid"):
        if uid.get("type") == "tmdb" and uid.text:
            tmdb_id = uid.text.strip()
    if not tmdb_id:
        tmdb_id = _nfo_text(root, "tmdbid")

    runtime_min = _nfo_int(root, "runtime")

    # Rating: try <rating>, then <ratings><rating><value>, then <userrating>
    rating = 0.0
    rating_str = _nfo_text(root, "rating")
    if not rating_str:
        # Kodi nested format: <ratings><rating name="imdb"><value>8.5</value></rating></ratings>
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


# ---------------------------------------------------------------------------
# ffprobe helpers
# ---------------------------------------------------------------------------

# Bitmap subtitle codecs that cannot be converted to WebVTT
_BITMAP_SUB_CODECS = {"hdmv_pgs_subtitle", "dvd_subtitle", "dvb_subtitle"}

# Known language code suffixes for external SRT files
_LANG_CODES = {"he", "en", "es", "fr", "de", "ar", "ru", "pt", "it", "nl", "pl", "cs", "ja", "ko", "zh"}


# HDR transfer functions that indicate HDR10 / HLG content
_HDR_TRANSFERS = {"smpte2084", "arib-std-b67", "smpte428"}


def probe_file_info(path: str):
    """
    Return (duration_sec, audio_codec, has_text_embedded_subs, is_hdr) via ffprobe.
    Falls back to (0.0, "", False, False) on any error.
    """
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_format", "-show_streams",
                path,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        data = json.loads(result.stdout)
        duration = float(data["format"]["duration"])
        audio_codec = ""
        has_embedded_subs = False
        is_hdr = False
        video_width = 0
        video_height = 0
        video_codec = ""
        for stream in data.get("streams", []):
            ctype = stream.get("codec_type", "")
            if ctype == "video" and not is_hdr:
                if not video_width:
                    video_width = stream.get("width") or 0
                    video_height = stream.get("height") or 0
                    video_codec = stream.get("codec_name", "").lower()
                transfer = stream.get("color_transfer", "")
                if transfer in _HDR_TRANSFERS:
                    is_hdr = True
                    log.debug("HDR detected in %s (transfer=%s)", path, transfer)
            elif ctype == "audio" and not audio_codec:
                audio_codec = stream.get("codec_name", "").lower()
            elif ctype == "subtitle":
                if stream.get("codec_name", "") not in _BITMAP_SUB_CODECS:
                    has_embedded_subs = True
        return duration, audio_codec, has_embedded_subs, is_hdr, video_width, video_height, video_codec
    except Exception as e:
        log.warning("ffprobe failed for %s: %s", path, e)
        return 0.0, "", False, False, 0, 0, ""


def _is_likely_hebrew(path: str) -> bool:
    """Return True if the file contains Hebrew Unicode characters (U+0590–U+05FF)."""
    try:
        with open(path, "rb") as f:
            raw = f.read(2048)
        text = raw.decode("utf-8", errors="ignore")
        return any('\u0590' <= c <= '\u05FF' for c in text)
    except Exception:
        return False


def _find_subtitle_files(video_path: str) -> Dict[str, str]:
    """
    Return {lang: path} for external subtitle files alongside the video file.
    Matches:
      <basename>.<lang>.srt  e.g.  Show.S01E01.he.srt  → {"he": "..."}
      <basename>.srt         (unlabeled)                → {"": "..."}
    """
    base = os.path.splitext(video_path)[0]
    result: Dict[str, str] = {}
    for lang in sorted(_LANG_CODES):
        # Match <base>.<lang>.srt and <base>.<lang>.<tag>.srt (e.g. .he.hi.srt)
        for candidate in [f"{base}.{lang}.srt", f"{base}.{lang}.hi.srt"]:
            if os.path.exists(candidate):
                result[lang] = candidate
                break
    # Unlabeled .srt handling:
    # If any lang-specific .srt was found alongside it (e.g. .en.srt), treat
    # the bare .srt as Hebrew — this is the common Israeli release pattern.
    # Otherwise keep it unlabeled ("") as a fallback.
    plain = f"{base}.srt"
    if os.path.exists(plain):
        if result and "he" not in result and _is_likely_hebrew(plain):
            result["he"] = plain
        elif not result:
            result[""] = plain
    if result:
        log.debug("Subtitle files found for %s: %s", os.path.basename(video_path), list(result.keys()))
    return result


# Keep old name as alias so any external callers don't break
def probe_duration(path: str) -> float:
    return probe_file_info(path)[0]


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

class Scanner:
    def __init__(self, shows_path: str, movies_path: str, cache_dir: str,
                 tmdb_api_key: str = "", ignore_patterns: List[str] = None,
                 sonarr_url: str = "", sonarr_api_key: str = "",
                 radarr_url: str = "", radarr_api_key: str = ""):
        from .arrclient import SonarrClient, RadarrClient
        self.shows_path = shows_path
        self.movies_path = movies_path
        self._ignore = ignore_patterns or []
        db_path = os.path.join(cache_dir, "cache.db")
        self._dur_cache = DurationCache(db_path)
        self._tmdb = TMDBCache(db_path, tmdb_api_key)
        self._sonarr = SonarrClient(sonarr_url, sonarr_api_key) if sonarr_url and sonarr_api_key else None
        self._radarr = RadarrClient(radarr_url, radarr_api_key) if radarr_url and radarr_api_key else None

    def _is_ignored(self, path: str) -> bool:
        """Return True if the path should be skipped."""
        import fnmatch
        # Always skip hidden directories/files (e.g. .@__thumb, .DS_Store)
        parts = path.replace("\\", "/").split("/")
        if any(p.startswith(".") for p in parts):
            return True
        # User-defined patterns matched against the full path
        for pattern in self._ignore:
            if fnmatch.fnmatch(path, pattern) or fnmatch.fnmatch(os.path.basename(path), pattern):
                return True
        return False

    def scan(self) -> MediaLibrary:
        library = MediaLibrary()
        if os.path.isdir(self.shows_path):
            self._scan_shows(library)
        else:
            log.warning("Shows path not found: %s", self.shows_path)

        if os.path.isdir(self.movies_path):
            self._scan_movies(library)
        else:
            log.warning("Movies path not found: %s", self.movies_path)

        return library

    # ------------------------------------------------------------------
    # Shows
    # ------------------------------------------------------------------

    def _scan_shows(self, library: MediaLibrary):
        for show_name in sorted(os.listdir(self.shows_path)):
            show_dir = os.path.join(self.shows_path, show_name)
            if not os.path.isdir(show_dir):
                continue

            show = Show(name=show_name)
            episodes = []

            for root_dir, dirs, files in os.walk(show_dir):
                # Prune ignored dirs in-place so os.walk won't descend into them
                dirs[:] = sorted(d for d in dirs if not self._is_ignored(os.path.join(root_dir, d)))
                for fname in sorted(files):
                    ext = os.path.splitext(fname)[1].lower()
                    if ext not in VIDEO_EXTS:
                        continue
                    fpath = os.path.join(root_dir, fname)
                    if self._is_ignored(fpath):
                        continue
                    ep = self._make_episode(fpath, show_name)
                    if ep:
                        episodes.append(ep)

            if not episodes:
                continue

            # Sort by season then episode number
            episodes.sort(key=lambda e: (e.season, e.episode))
            show.episodes = episodes

            # Aggregate genres and rating from episodes
            genre_counts: Dict[str, int] = {}
            for ep in episodes:
                for g in ep.genres:
                    genre_counts[g] = genre_counts.get(g, 0) + 1
            show.genres = sorted(genre_counts, key=lambda g: -genre_counts[g])

            # All episodes share the same show-level rating; take the first non-zero value
            ratings = [ep.rating for ep in episodes if ep.rating]
            show.rating = ratings[0] if ratings else 0.0

            if episodes and episodes[0].poster_url:
                show.poster_url = episodes[0].poster_url

            library.shows[show_name] = show
            log.info("Scanned show: %s (%d episodes, rating: %.1f)",
                     show_name, len(episodes), show.rating)

    def _make_episode(self, path: str, show_name: str) -> Optional[Episode]:
        nfo_path = os.path.splitext(path)[0] + ".nfo"
        nfo = parse_nfo(nfo_path) if os.path.exists(nfo_path) else {}

        dur_probed, audio_codec, has_embedded_subs, is_hdr, video_width, video_height, video_codec = self._get_file_info(path)
        duration = nfo.get("runtime_sec") or dur_probed
        if not duration:
            log.warning("Could not determine duration for %s, skipping", path)
            return None
        subtitle_paths = _find_subtitle_files(path)

        title = nfo.get("title") or self._clean_filename_title(os.path.basename(path))
        season = nfo.get("season") or self._guess_season(path)
        episode_num = nfo.get("episode") or self._guess_episode(path)
        genres = nfo.get("genres") or []
        plot = nfo.get("plot", "")
        tmdb_id = nfo.get("tmdb_id") or ""
        poster_url = nfo.get("poster", "")
        rating = float(nfo.get("rating") or 0)
        meta_source = "nfo" if nfo else "filename"

        # --- Sonarr (middle tier: fills gaps left by NFO) ---
        if self._sonarr and (not genres or not plot or not poster_url or not rating):
            show_meta = self._sonarr.get_show_metadata(show_name)
            if show_meta:
                if not genres and show_meta.get("genres"):
                    genres = show_meta["genres"]
                    meta_source = "sonarr"
                if not plot and show_meta.get("plot"):
                    plot = show_meta["plot"]
                    meta_source = "sonarr"
                poster_url = poster_url or show_meta.get("poster_url", "")
                rating = rating or float(show_meta.get("rating") or 0)
            ep_meta = self._sonarr.get_episode_metadata(show_name, season, episode_num)
            if ep_meta:
                # Prefer Sonarr's episode title over the filename-derived one
                if not nfo.get("title") and ep_meta.get("title"):
                    title = ep_meta["title"]
                plot = plot or ep_meta.get("plot", "")

        # --- TMDB (last resort) ---
        if self._tmdb._api_key and (not genres or not plot or not rating):
            if tmdb_id:
                show_data = self._tmdb.fetch_show(tmdb_id)
            else:
                show_data = self._tmdb.search_show(show_name)
            if show_data:
                if not genres:
                    genres = [g["name"] for g in show_data.get("genres", [])]
                    meta_source = "tmdb"
                poster_url = poster_url or (
                    TMDB_IMAGE_BASE + show_data["poster_path"]
                    if show_data.get("poster_path") else ""
                )
                rating = rating or float(show_data.get("vote_average") or 0)

        log.debug("S%02dE%02d %s — metadata from: %s (rating: %.1f)",
                  season, episode_num, os.path.basename(path), meta_source, rating)

        return Episode(
            path=path,
            title=title,
            show_title=show_name,
            season=season,
            episode=episode_num,
            duration_sec=duration,
            plot=plot,
            genres=genres,
            year=nfo.get("year", 0),
            poster_url=poster_url,
            tmdb_id=tmdb_id,
            rating=rating,
            audio_codec=audio_codec,
            subtitle_paths=subtitle_paths,
            has_embedded_subs=has_embedded_subs,
            is_hdr=is_hdr,
            video_width=video_width,
            video_height=video_height,
            video_codec=video_codec,
        )

    # ------------------------------------------------------------------
    # Movies
    # ------------------------------------------------------------------

    def _scan_movies(self, library: MediaLibrary):
        for entry in sorted(os.listdir(self.movies_path)):
            movie_dir = os.path.join(self.movies_path, entry)
            if self._is_ignored(movie_dir):
                continue

            # Support both flat files and one-folder-per-movie layouts
            if os.path.isfile(movie_dir):
                candidates = [movie_dir] if os.path.splitext(entry)[1].lower() in VIDEO_EXTS else []
            else:
                candidates = [
                    os.path.join(movie_dir, f)
                    for f in sorted(os.listdir(movie_dir))
                    if os.path.splitext(f)[1].lower() in VIDEO_EXTS
                    and not self._is_ignored(os.path.join(movie_dir, f))
                ]

            for path in candidates:
                movie = self._make_movie(path)
                if movie:
                    library.movies.append(movie)
                    log.info("Scanned movie: %s", movie.title)

    def _make_movie(self, path: str) -> Optional[Movie]:
        nfo_path = os.path.splitext(path)[0] + ".nfo"
        nfo = parse_nfo(nfo_path) if os.path.exists(nfo_path) else {}

        # For the lookup title: use NFO title if available, otherwise clean the filename.
        # The cleaned filename (dots→spaces, quality markers stripped) improves Radarr matching.
        raw_filename_title = self._clean_filename_title(os.path.basename(path))
        title = nfo.get("title") or raw_filename_title
        lookup_title = title  # what we actually send to Radarr/TMDB
        genres = nfo.get("genres") or []
        plot = nfo.get("plot", "")
        tmdb_id = nfo.get("tmdb_id") or ""
        poster_url = nfo.get("poster") or ""
        year = nfo.get("year", 0)
        rating = float(nfo.get("rating") or 0)
        meta_source = "nfo" if nfo else "filename"

        # --- Radarr (middle tier) ---
        if self._radarr and (not genres or not plot or not poster_url or not rating):
            meta = self._radarr.get_movie_metadata(lookup_title, year)
            if meta:
                # Use Radarr's canonical title (clean, properly capitalised)
                if not nfo.get("title") and meta.get("title"):
                    title = meta["title"]
                if not genres and meta.get("genres"):
                    genres = meta["genres"]
                    meta_source = "radarr"
                if not plot and meta.get("plot"):
                    plot = meta["plot"]
                    meta_source = "radarr"
                poster_url = poster_url or meta.get("poster_url", "")
                year = year or meta.get("year", 0)
                rating = rating or float(meta.get("rating") or 0)
                if not nfo.get("runtime_sec") and meta.get("runtime_sec"):
                    nfo["runtime_sec"] = meta["runtime_sec"]

        dur_probed, audio_codec, has_embedded_subs, is_hdr, video_width, video_height, video_codec = self._get_file_info(path)
        duration = nfo.get("runtime_sec") or dur_probed
        if not duration:
            log.warning("Could not determine duration for %s, skipping", path)
            return None
        subtitle_paths = _find_subtitle_files(path)

        # --- TMDB (last resort) ---
        if self._tmdb._api_key and (not genres or not plot or not rating):
            if tmdb_id:
                data = self._tmdb.fetch_movie(tmdb_id)
            else:
                data = self._tmdb.search_movie(title, year)
            if data:
                if not genres:
                    genres = [g["name"] for g in data.get("genres", [])]
                    meta_source = "tmdb"
                if not poster_url and data.get("poster_path"):
                    poster_url = TMDB_IMAGE_BASE + data["poster_path"]
                rating = rating or float(data.get("vote_average") or 0)

        log.debug("%s — metadata from: %s (rating: %.1f)",
                  os.path.basename(path), meta_source, rating)

        return Movie(
            path=path,
            title=title,
            duration_sec=duration,
            plot=plot,
            genres=genres,
            year=year,
            poster_url=poster_url,
            tmdb_id=tmdb_id,
            rating=rating,
            audio_codec=audio_codec,
            subtitle_paths=subtitle_paths,
            has_embedded_subs=has_embedded_subs,
            is_hdr=is_hdr,
            video_width=video_width,
            video_height=video_height,
            video_codec=video_codec,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_file_info(self, path: str):
        """Return (duration, audio_codec, has_embedded_subs, is_hdr, video_width, video_height, video_codec), using cache when available."""
        info = self._dur_cache.get_info(path)
        if info is not None:
            return info
        log.info("Probing: %s", os.path.basename(path))
        dur, audio_codec, has_embedded_subs, is_hdr, video_width, video_height, video_codec = probe_file_info(path)
        if dur:
            self._dur_cache.set_info(path, dur, audio_codec, has_embedded_subs, is_hdr, video_width, video_height, video_codec)
        return dur, audio_codec, has_embedded_subs, is_hdr, video_width, video_height, video_codec

    @staticmethod
    def _clean_filename_title(filename: str) -> str:
        """
        Turn a raw filename into a human-readable title for Radarr/Sonarr lookup.
        Strips extension, dots/underscores, year, and common quality/codec markers.
        e.g. "Harry.Potter.and.the.Chamber.of.Secrets.2002.Bluray-1080p.mkv"
             → "Harry Potter and the Chamber of Secrets"
        """
        # Remove extension
        name = os.path.splitext(filename)[0]
        # Replace dots and underscores with spaces
        name = name.replace(".", " ").replace("_", " ")
        # Truncate at year (4-digit number 1900-2099) or quality markers
        quality_re = re.compile(
            r"\s*\b("
            r"(19|20)\d{2}"           # year
            r"|[0-9]{3,4}p"           # resolution: 720p, 1080p, 2160p
            r"|(?:blu.?ray|bluray|bdrip|brrip|dvdrip|webrip|web.?dl|hdtv|hdrip|uhd)"
            r"|(?:x264|x265|h264|h265|xvid|divx|hevc|avc)"
            r"|(?:aac|ac3|dts|truehd|atmos|eac3|dd5?(?:\.1)?)"
            r"|(?:hdr|hdr10|dv|dolby)"
            r"|(?:extended|theatrical|remastered|proper|repack)"
            r")\b.*$",
            re.IGNORECASE,
        )
        name = quality_re.sub("", name).strip()
        # Collapse multiple spaces
        name = re.sub(r"\s+", " ", name).strip()
        return name

    @staticmethod
    def _guess_season(path: str) -> int:
        m = re.search(r"[Ss](\d{1,2})[Ee]\d", path)
        if m:
            return int(m.group(1))
        m = re.search(r"[Ss]eason\s*(\d+)", path, re.IGNORECASE)
        if m:
            return int(m.group(1))
        return 1

    @staticmethod
    def _guess_episode(path: str) -> int:
        m = re.search(r"[Ss]\d{1,2}[Ee](\d{1,2})", path)
        if m:
            return int(m.group(1))
        m = re.search(r"[Ee](\d{1,3})", path)
        if m:
            return int(m.group(1))
        return 0
