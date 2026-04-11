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


@dataclass
class Show:
    name: str
    episodes: List[Episode] = field(default_factory=list)
    genres: List[str] = field(default_factory=list)
    poster_url: str = ""


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
                key  TEXT PRIMARY KEY,
                duration REAL NOT NULL
            )
        """)
        self._conn.commit()

    def _key(self, path: str) -> str:
        try:
            mtime = str(os.path.getmtime(path))
        except OSError:
            mtime = "0"
        return f"{path}|{mtime}"

    def get(self, path: str) -> Optional[float]:
        row = self._conn.execute(
            "SELECT duration FROM durations WHERE key = ?", (self._key(path),)
        ).fetchone()
        return row[0] if row else None

    def set(self, path: str, duration: float):
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO durations (key, duration) VALUES (?, ?)",
                (self._key(path), duration),
            )
            self._conn.commit()


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
    }


# ---------------------------------------------------------------------------
# ffprobe duration
# ---------------------------------------------------------------------------

def probe_duration(path: str) -> float:
    """Return duration in seconds via ffprobe. Returns 0.0 on failure."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_format",
                path,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        data = json.loads(result.stdout)
        return float(data["format"]["duration"])
    except Exception as e:
        log.warning("ffprobe failed for %s: %s", path, e)
        return 0.0


# ---------------------------------------------------------------------------
# Scanner
# ---------------------------------------------------------------------------

class Scanner:
    def __init__(self, shows_path: str, movies_path: str, cache_dir: str, tmdb_api_key: str = ""):
        self.shows_path = shows_path
        self.movies_path = movies_path
        db_path = os.path.join(cache_dir, "cache.db")
        self._dur_cache = DurationCache(db_path)
        self._tmdb = TMDBCache(db_path, tmdb_api_key)

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
                dirs.sort()
                for fname in sorted(files):
                    ext = os.path.splitext(fname)[1].lower()
                    if ext not in VIDEO_EXTS:
                        continue
                    fpath = os.path.join(root_dir, fname)
                    ep = self._make_episode(fpath, show_name)
                    if ep:
                        episodes.append(ep)

            if not episodes:
                continue

            # Sort by season then episode number
            episodes.sort(key=lambda e: (e.season, e.episode))
            show.episodes = episodes

            # Aggregate genres from episodes
            genre_counts: Dict[str, int] = {}
            for ep in episodes:
                for g in ep.genres:
                    genre_counts[g] = genre_counts.get(g, 0) + 1
            show.genres = sorted(genre_counts, key=lambda g: -genre_counts[g])

            if episodes and episodes[0].poster_url:
                show.poster_url = episodes[0].poster_url

            library.shows[show_name] = show
            log.info("Scanned show: %s (%d episodes)", show_name, len(episodes))

    def _make_episode(self, path: str, show_name: str) -> Optional[Episode]:
        nfo_path = os.path.splitext(path)[0] + ".nfo"
        nfo = parse_nfo(nfo_path) if os.path.exists(nfo_path) else {}

        duration = nfo.get("runtime_sec") or self._get_duration(path)
        if not duration:
            log.warning("Could not determine duration for %s, skipping", path)
            return None

        title = nfo.get("title") or os.path.splitext(os.path.basename(path))[0]
        season = nfo.get("season") or self._guess_season(path)
        episode_num = nfo.get("episode") or self._guess_episode(path)
        genres = nfo.get("genres") or []
        tmdb_id = nfo.get("tmdb_id") or ""
        poster_url = ""

        # TMDB fallback for missing metadata
        if self._tmdb._api_key and tmdb_id and (not genres or not nfo.get("plot")):
            ep_data = self._tmdb.fetch_episode(tmdb_id, season, episode_num)
            show_data = self._tmdb.fetch_show(tmdb_id)
            if show_data:
                genres = genres or [g["name"] for g in show_data.get("genres", [])]
                if show_data.get("poster_path"):
                    poster_url = TMDB_IMAGE_BASE + show_data["poster_path"]
        elif self._tmdb._api_key and not tmdb_id and not genres:
            show_data = self._tmdb.search_show(show_name)
            if show_data:
                genres = [g["name"] for g in self._tmdb.fetch_show(
                    str(show_data["id"])) .get("genres", [])] if show_data else []
                if show_data.get("poster_path"):
                    poster_url = TMDB_IMAGE_BASE + show_data["poster_path"]

        return Episode(
            path=path,
            title=title,
            show_title=show_name,
            season=season,
            episode=episode_num,
            duration_sec=duration,
            plot=nfo.get("plot", ""),
            genres=genres,
            year=nfo.get("year", 0),
            poster_url=nfo.get("poster") or poster_url,
            tmdb_id=tmdb_id,
        )

    # ------------------------------------------------------------------
    # Movies
    # ------------------------------------------------------------------

    def _scan_movies(self, library: MediaLibrary):
        for entry in sorted(os.listdir(self.movies_path)):
            movie_dir = os.path.join(self.movies_path, entry)

            # Support both flat files and one-folder-per-movie layouts
            if os.path.isfile(movie_dir):
                candidates = [movie_dir] if os.path.splitext(entry)[1].lower() in VIDEO_EXTS else []
            else:
                candidates = [
                    os.path.join(movie_dir, f)
                    for f in sorted(os.listdir(movie_dir))
                    if os.path.splitext(f)[1].lower() in VIDEO_EXTS
                ]

            for path in candidates:
                movie = self._make_movie(path)
                if movie:
                    library.movies.append(movie)
                    log.info("Scanned movie: %s", movie.title)

    def _make_movie(self, path: str) -> Optional[Movie]:
        nfo_path = os.path.splitext(path)[0] + ".nfo"
        nfo = parse_nfo(nfo_path) if os.path.exists(nfo_path) else {}

        duration = nfo.get("runtime_sec") or self._get_duration(path)
        if not duration:
            log.warning("Could not determine duration for %s, skipping", path)
            return None

        title = nfo.get("title") or os.path.splitext(os.path.basename(path))[0]
        genres = nfo.get("genres") or []
        tmdb_id = nfo.get("tmdb_id") or ""
        poster_url = nfo.get("poster") or ""

        if self._tmdb._api_key and not genres:
            if tmdb_id:
                data = self._tmdb.fetch_movie(tmdb_id)
            else:
                data = self._tmdb.search_movie(title, nfo.get("year", 0))
            if data:
                genres = [g["name"] for g in data.get("genres", [])]
                if not poster_url and data.get("poster_path"):
                    poster_url = TMDB_IMAGE_BASE + data["poster_path"]

        return Movie(
            path=path,
            title=title,
            duration_sec=duration,
            plot=nfo.get("plot", ""),
            genres=genres,
            year=nfo.get("year", 0),
            poster_url=poster_url,
            tmdb_id=tmdb_id,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_duration(self, path: str) -> float:
        cached = self._dur_cache.get(path)
        if cached is not None:
            return cached
        dur = probe_duration(path)
        if dur:
            self._dur_cache.set(path, dur)
        return dur

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
