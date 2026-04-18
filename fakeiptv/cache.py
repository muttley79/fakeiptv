"""
cache.py — SQLite-backed caching for durations and TMDB metadata.
"""
import json
import logging
import os
import sqlite3
import threading
from typing import Optional

import requests

log = logging.getLogger(__name__)

TMDB_BASE = "https://api.themoviedb.org/3"


def _open_db(db_path: str) -> sqlite3.Connection:
    """Open (or create) the cache DB with WAL mode for safe concurrent access."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


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
        for col_def in [
            "ADD COLUMN audio_codec TEXT NOT NULL DEFAULT ''",
            "ADD COLUMN has_embedded_subs INTEGER NOT NULL DEFAULT -1",
            "ADD COLUMN is_hdr INTEGER NOT NULL DEFAULT -1",
            "ADD COLUMN slow_seek INTEGER NOT NULL DEFAULT -1",
            "ADD COLUMN video_width INTEGER NOT NULL DEFAULT 0",
            "ADD COLUMN video_height INTEGER NOT NULL DEFAULT 0",
            "ADD COLUMN video_codec TEXT NOT NULL DEFAULT ''",
        ]:
            try:
                self._conn.execute(f"ALTER TABLE durations {col_def}")
            except Exception:
                pass
        self._conn.commit()

    def _key(self, path: str) -> str:
        try:
            mtime = str(os.path.getmtime(path))
        except OSError:
            mtime = "0"
        return f"{path}|{mtime}"

    def get_info(self, path: str) -> Optional[tuple]:
        """Return (duration, audio_codec, has_embedded_subs, is_hdr, video_width, video_height, video_codec) or None."""
        row = self._conn.execute(
            "SELECT duration, audio_codec, has_embedded_subs, is_hdr, video_width, video_height, video_codec FROM durations WHERE key = ?",
            (self._key(path),)
        ).fetchone()
        if row is None:
            return None
        duration, audio_codec, has_embedded_subs_int, is_hdr_int, video_width, video_height, video_codec = row
        if has_embedded_subs_int < 0 or is_hdr_int < 0 or video_width == 0 or not video_codec:
            return None
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

    def get(self, path: str) -> Optional[float]:
        """Legacy accessor for duration only."""
        info = self.get_info(path)
        return info[0] if info is not None else None

    def set(self, path: str, duration: float):
        """Legacy setter for duration only."""
        self.set_info(path, duration, "", False, False)


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
