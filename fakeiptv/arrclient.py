"""
arrclient.py — Optional Sonarr and Radarr metadata sources.

Paths from Arr are intentionally ignored — the Pi sees different mount
paths than Radarr/Sonarr. Only metadata fields are used:
  genres, poster_url, plot, year, runtime_sec, title.

Each client fetches all series/movies in one API call at scan time and
builds an in-memory lookup dict keyed by normalised title for fast matching.
"""
import logging
import re
from typing import Dict, List, Optional

import requests

log = logging.getLogger(__name__)

TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"


def _normalise(title: str) -> str:
    """Lowercase, treat dots/underscores as spaces, strip punctuation — for fuzzy matching."""
    title = title.lower()
    title = title.replace(".", " ").replace("_", " ")  # filename separators → spaces
    title = re.sub(r"[^\w\s]", "", title)              # strip remaining punctuation
    title = re.sub(r"\s+", " ", title).strip()
    return title


# ---------------------------------------------------------------------------
# Sonarr
# ---------------------------------------------------------------------------

class SonarrClient:
    def __init__(self, base_url: str, api_key: str):
        self._base = base_url.rstrip("/")
        self._key = api_key
        self._headers = {"X-Api-Key": api_key}
        # Populated on first use: {normalised_title: series_dict}
        self._series_by_title: Dict[str, dict] = {}
        # {series_id: {(season, episode): episode_dict}}
        self._episodes: Dict[int, Dict[tuple, dict]] = {}
        self._loaded = False

    def _load(self):
        if self._loaded:
            return
        try:
            resp = requests.get(
                f"{self._base}/api/v3/series",
                headers=self._headers,
                timeout=10,
            )
            resp.raise_for_status()
            for series in resp.json():
                key = _normalise(series.get("title", ""))
                if key:
                    self._series_by_title[key] = series
            log.info("Sonarr: loaded %d series", len(self._series_by_title))
            self._loaded = True
        except Exception as e:
            log.warning("Sonarr unavailable: %s", e)

    def _load_episodes(self, series_id: int):
        if series_id in self._episodes:
            return
        try:
            resp = requests.get(
                f"{self._base}/api/v3/episode",
                headers=self._headers,
                params={"seriesId": series_id},
                timeout=10,
            )
            resp.raise_for_status()
            ep_map: Dict[tuple, dict] = {}
            for ep in resp.json():
                key = (ep.get("seasonNumber", 0), ep.get("episodeNumber", 0))
                ep_map[key] = ep
            self._episodes[series_id] = ep_map
        except Exception as e:
            log.warning("Sonarr: failed to load episodes for series %d: %s", series_id, e)
            self._episodes[series_id] = {}

    def _find_series(self, show_name: str) -> Optional[dict]:
        self._load()
        key = _normalise(show_name)
        # Exact match first
        if key in self._series_by_title:
            return self._series_by_title[key]
        # Prefix / substring match as fallback
        for title, series in self._series_by_title.items():
            if key in title or title in key:
                return series
        return None

    def get_show_metadata(self, show_name: str) -> Optional[dict]:
        """Return show-level metadata: genres, poster_url, year, plot."""
        series = self._find_series(show_name)
        if not series:
            return None
        poster_url = ""
        for img in series.get("images", []):
            if img.get("coverType") == "poster":
                # Sonarr returns a local /MediaCover URL — use remotePoster instead
                poster_url = series.get("remotePoster", "") or img.get("remoteUrl", "")
                break
        # Sonarr v3: ratings = {"votes": N, "value": 8.5}
        sonarr_ratings = series.get("ratings", {})
        rating = float(sonarr_ratings.get("value", 0) or 0)
        return {
            "title": series.get("title", ""),
            "genres": series.get("genres", []),
            "poster_url": poster_url,
            "year": series.get("year", 0),
            "plot": series.get("overview", ""),
            "rating": rating,
        }

    def get_episode_metadata(self, show_name: str, season: int, episode: int) -> Optional[dict]:
        """Return episode-level metadata: title, plot."""
        series = self._find_series(show_name)
        if not series:
            return None
        series_id = series["id"]
        self._load_episodes(series_id)
        ep = self._episodes.get(series_id, {}).get((season, episode))
        if not ep:
            return None
        return {
            "title": ep.get("title", ""),
            "plot": ep.get("overview", ""),
        }

    def reload(self):
        """Force re-fetch on next access (called at midnight rescan)."""
        self._loaded = False
        self._series_by_title.clear()
        self._episodes.clear()


# ---------------------------------------------------------------------------
# Radarr
# ---------------------------------------------------------------------------

class RadarrClient:
    def __init__(self, base_url: str, api_key: str):
        self._base = base_url.rstrip("/")
        self._key = api_key
        self._headers = {"X-Api-Key": api_key}
        self._movies_by_title: Dict[str, dict] = {}
        self._loaded = False

    def _load(self):
        if self._loaded:
            return
        try:
            resp = requests.get(
                f"{self._base}/api/v3/movie",
                headers=self._headers,
                timeout=10,
            )
            resp.raise_for_status()
            for movie in resp.json():
                key = _normalise(movie.get("title", ""))
                if key:
                    self._movies_by_title[key] = movie
                # Also index by originalTitle if different
                orig = movie.get("originalTitle", "")
                if orig:
                    self._movies_by_title.setdefault(_normalise(orig), movie)
            log.info("Radarr: loaded %d movies", len(self._movies_by_title))
            self._loaded = True
        except Exception as e:
            log.warning("Radarr unavailable: %s", e)

    def get_movie_metadata(self, title: str, year: int = 0) -> Optional[dict]:
        """Return movie metadata: genres, poster_url, year, plot, runtime_sec."""
        self._load()
        key = _normalise(title)
        movie = self._movies_by_title.get(key)
        if not movie:
            # Try substring match
            for t, m in self._movies_by_title.items():
                if key in t or t in key:
                    # If year is known, use it to disambiguate
                    if year and m.get("year") and abs(m["year"] - year) > 1:
                        continue
                    movie = m
                    break
        if not movie:
            return None

        poster_url = movie.get("remotePoster", "")
        runtime_min = movie.get("runtime", 0)
        # Radarr v3: ratings = {"imdb": {"votes": N, "value": 7.5}, "tmdb": {...}}
        # Prefer IMDb, fall back to TMDB, then Rotten Tomatoes (scale differs — skip RT)
        radarr_ratings = movie.get("ratings", {})
        rating = float(
            (radarr_ratings.get("imdb") or {}).get("value")
            or (radarr_ratings.get("tmdb") or {}).get("value")
            or 0
        )
        return {
            "title": movie.get("title", ""),
            "genres": movie.get("genres", []),
            "poster_url": poster_url,
            "year": movie.get("year", 0),
            "plot": movie.get("overview", ""),
            "runtime_sec": runtime_min * 60 if runtime_min else 0,
            "rating": rating,
        }

    def reload(self):
        self._loaded = False
        self._movies_by_title.clear()
