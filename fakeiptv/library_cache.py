"""
library_cache.py — Serialize/deserialize MediaLibrary to disk for fast startup.

On cache hit, startup skips Scanner.scan() entirely (saves ~25-30s NAS walk).
Cache is invalidated by: TTL expiry, config change, or NAS top-level dir change.
"""
import dataclasses
import hashlib
import json
import logging
import os
import time
from typing import Dict, List, Optional, Tuple

from .models import Episode, MediaLibrary, Movie, Show

log = logging.getLogger(__name__)

_LIBRARY_JSON = "library.json"
_META_JSON = "library_meta.json"


class LibraryCache:
    def __init__(self, config):
        self._config = config
        self._cache_dir = config.metadata.cache_dir

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save(self, library: MediaLibrary) -> None:
        os.makedirs(self._cache_dir, exist_ok=True)
        file_count, mtime_hash = self._check_nas_state()
        meta = {
            "saved_at": time.time(),
            "config_hash": self._compute_config_hash(),
            "file_count": file_count,
            "mtime_hash": mtime_hash,
        }
        lib_path = os.path.join(self._cache_dir, _LIBRARY_JSON)
        meta_path = os.path.join(self._cache_dir, _META_JSON)
        lib_tmp = lib_path + ".tmp"
        meta_tmp = meta_path + ".tmp"
        try:
            with open(lib_tmp, "w", encoding="utf-8") as f:
                json.dump(dataclasses.asdict(library), f)
            with open(meta_tmp, "w", encoding="utf-8") as f:
                json.dump(meta, f)
            os.replace(lib_tmp, lib_path)
            os.replace(meta_tmp, meta_path)
            log.info("Library cache saved (%d shows, %d movies)", len(library.shows), len(library.movies))
        except Exception:
            log.exception("Failed to save library cache")
            for p in (lib_tmp, meta_tmp):
                try:
                    os.remove(p)
                except OSError:
                    pass

    def load(self) -> Optional[MediaLibrary]:
        if not self._config.metadata.startup_cache:
            log.debug("Startup cache disabled by config")
            return None
        if not self.is_fresh():
            return None
        lib_path = os.path.join(self._cache_dir, _LIBRARY_JSON)
        try:
            with open(lib_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            library = _library_from_dict(data)
            log.info("Startup cache hit: %d shows, %d movies", len(library.shows), len(library.movies))
            return library
        except Exception:
            log.warning("Failed to load library cache; will full-scan", exc_info=True)
            return None

    def is_fresh(self) -> bool:
        meta = self._load_meta()
        if meta is None:
            log.debug("No library cache meta found")
            return False

        max_age = self._config.metadata.startup_cache_max_age_hours * 3600
        age = time.time() - meta.get("saved_at", 0)
        if age > max_age:
            log.info("Library cache expired (age=%.1fh, max=%dh)", age / 3600,
                     self._config.metadata.startup_cache_max_age_hours)
            return False

        if meta.get("config_hash") != self._compute_config_hash():
            log.info("Library cache invalidated: config changed")
            return False

        current_count, current_mtime_hash = self._check_nas_state()
        if meta.get("file_count") != current_count or meta.get("mtime_hash") != current_mtime_hash:
            log.info("Library cache invalidated: NAS changed (was %d entries, now %d)",
                     meta.get("file_count"), current_count)
            return False

        return True

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _load_meta(self) -> Optional[dict]:
        meta_path = os.path.join(self._cache_dir, _META_JSON)
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return None

    def _compute_config_hash(self) -> str:
        cfg = self._config
        relevant = {
            "shows_path": cfg.media.shows_path,
            "movies_path": cfg.media.movies_path,
            "ignore_patterns": sorted(cfg.media.ignore_patterns),
            "sonarr_url": cfg.metadata.sonarr_url,
            "sonarr_api_key": cfg.metadata.sonarr_api_key,
            "radarr_url": cfg.metadata.radarr_url,
            "radarr_api_key": cfg.metadata.radarr_api_key,
            "tmdb_api_key": cfg.metadata.tmdb_api_key,
            "goldies_before": cfg.channels.goldies_before,
            "hits_rating": cfg.channels.hits_rating,
            "disabled": sorted(cfg.channels.disabled),
            "rename": sorted(cfg.channels.rename.items()),
        }
        blob = json.dumps(relevant, sort_keys=True).encode()
        return hashlib.sha256(blob).hexdigest()

    def _check_nas_state(self) -> Tuple[int, str]:
        """Non-recursive scandir of shows and movies root dirs.
        Detects folder-level adds/removes/renames without recursing into NAS.
        """
        entries = []
        for root in (self._config.media.shows_path, self._config.media.movies_path):
            if not os.path.isdir(root):
                continue
            try:
                with os.scandir(root) as it:
                    for entry in it:
                        try:
                            st = entry.stat(follow_symlinks=False)
                            entries.append((entry.name, st.st_mtime))
                        except OSError:
                            pass
            except OSError as e:
                log.warning("Cannot scandir %s: %s", root, e)
        entries.sort()
        blob = json.dumps(entries).encode()
        return len(entries), hashlib.sha256(blob).hexdigest()


# ------------------------------------------------------------------
# Serialization helpers (module-level, no self needed)
# ------------------------------------------------------------------

def _episode(d: dict) -> Episode:
    return Episode(
        path=d["path"],
        title=d["title"],
        show_title=d["show_title"],
        season=d["season"],
        episode=d["episode"],
        duration_sec=d["duration_sec"],
        plot=d.get("plot", ""),
        genres=d.get("genres", []),
        year=d.get("year", 0),
        poster_url=d.get("poster_url", ""),
        tmdb_id=d.get("tmdb_id", ""),
        rating=d.get("rating", 0.0),
        audio_codec=d.get("audio_codec", ""),
        subtitle_paths=d.get("subtitle_paths", {}),
        has_embedded_subs=d.get("has_embedded_subs", False),
        is_hdr=d.get("is_hdr", False),
        video_width=d.get("video_width", 0),
        video_height=d.get("video_height", 0),
        video_codec=d.get("video_codec", ""),
    )


def _movie(d: dict) -> Movie:
    return Movie(
        path=d["path"],
        title=d["title"],
        duration_sec=d["duration_sec"],
        plot=d.get("plot", ""),
        genres=d.get("genres", []),
        year=d.get("year", 0),
        poster_url=d.get("poster_url", ""),
        tmdb_id=d.get("tmdb_id", ""),
        rating=d.get("rating", 0.0),
        audio_codec=d.get("audio_codec", ""),
        subtitle_paths=d.get("subtitle_paths", {}),
        has_embedded_subs=d.get("has_embedded_subs", False),
        is_hdr=d.get("is_hdr", False),
        video_width=d.get("video_width", 0),
        video_height=d.get("video_height", 0),
        video_codec=d.get("video_codec", ""),
    )


def _show(d: dict) -> Show:
    return Show(
        name=d["name"],
        episodes=[_episode(e) for e in d.get("episodes", [])],
        genres=d.get("genres", []),
        poster_url=d.get("poster_url", ""),
        rating=d.get("rating", 0.0),
    )


def _library_from_dict(data: dict) -> MediaLibrary:
    return MediaLibrary(
        shows={k: _show(v) for k, v in data.get("shows", {}).items()},
        movies=[_movie(m) for m in data.get("movies", [])],
    )
