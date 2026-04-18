"""
scanner.py — Walks the NAS, parses NFO metadata, falls back to TMDB,
probes durations via ffprobe, and returns a MediaLibrary.
"""
import json
import logging
import os
import re
import subprocess
import threading
from typing import Dict, List, Optional

from .models import Episode, Movie, Show, MediaLibrary  # noqa: F401
from .cache import DurationCache, TMDBCache
from .nfo import parse_nfo
from .ffprobe_utils import probe_file_info, probe_duration
from .subtitle_utils import _find_subtitle_files, _is_likely_hebrew

log = logging.getLogger(__name__)

TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/w500"

VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".m4v", ".mov"}


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
