"""
scheduler.py — Builds deterministic per-channel schedules anchored to a
fixed local-time epoch, calculates the current playback position, and
generates XMLTV EPG data for a configurable window (past + future).
"""
import hashlib
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from .scanner import Episode, MediaLibrary, Movie, Show, slugify

log = logging.getLogger(__name__)

# Fixed epoch — all schedules are offset from this local datetime.
# Changing this will shift every channel's schedule.
EPOCH = datetime(2024, 1, 1, 0, 0, 0)

# Minimum number of movies of a genre to create a dedicated movie channel
MOVIE_GENRE_MIN = 3

# Minimum number of shows sharing a genre to create a genre channel
SHOW_GENRE_MIN = 3

# Maximum fraction of a genre channel's episodes one show may contribute.
# If a single show would own more than this share, the channel is skipped.
SHOW_GENRE_MAX_DOMINANCE = 0.6

# Each show's episode contribution is capped at this multiple of the
# smallest show's episode count, so no show runs away in the mix.
SHOW_GENRE_EPISODE_CAP_FACTOR = 3

# Minimum number of qualifying shows to create a "Goldies" channel
GOLDIES_MIN = 2


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------

@dataclass
class ScheduleEntry:
    """One slot in a channel's repeating schedule."""
    path: str
    title: str
    subtitle: str        # episode title or movie title
    duration_sec: float
    plot: str
    poster_url: str
    season: int = 0
    episode: int = 0
    year: int = 0
    genres: List[str] = field(default_factory=list)
    audio_codec: str = ""
    subtitle_paths: Dict[str, str] = field(default_factory=dict)
    has_embedded_subs: bool = False


@dataclass
class Channel:
    id: str              # slug, e.g. "breaking-bad"
    name: str
    group: str           # "Shows", "Movies", "Genre Mix"
    entries: List[ScheduleEntry] = field(default_factory=list)
    poster_url: str = ""

    @property
    def total_duration(self) -> float:
        return sum(e.duration_sec for e in self.entries)


@dataclass
class NowPlaying:
    channel_id: str
    entry: ScheduleEntry
    offset_sec: float      # how far into the entry we currently are
    entry_index: int       # index in channel.entries


# ---------------------------------------------------------------------------
# Channel builder
# ---------------------------------------------------------------------------

def build_channels(
    library: MediaLibrary,
    disabled: List[str] = None,
    rename: Dict[str, str] = None,
    goldies_before: int = 2010,
    hits_rating: float = 8.0,
) -> Dict[str, Channel]:
    """
    Auto-discover channels from the media library.

    A show may appear in multiple channels (e.g. Friends in both Comedy and
    Hits). Simultaneous airing is prevented by _channel_offset_sec(), which
    gives each channel a unique time offset so their loops are staggered.

    Channel types:
      - "Primetime"      — all shows interleaved
      - "{Genre}"        — per-genre mix (≥ SHOW_GENRE_MIN shows)
      - "Goldies"        — shows with known year < goldies_before
      - "Hits"           — shows with rating ≥ hits_rating
      - "{Genre} Movies" — genre movie mix (≥ MOVIE_GENRE_MIN); movies are exclusive
      - "Movies"         — remaining movies not in any genre channel
    """
    disabled = disabled or []
    rename = rename or {}
    channels: Dict[str, Channel] = {}

    all_shows = [s for s in library.shows.values() if s.episodes]

    def _add_show_channel(ch_id: str, default_name: str, shows: List[Show],
                          group: str = "Shows") -> None:
        if ch_id in disabled or not shows:
            return
        entries = _interleave_shows(shows)
        if entries:
            channels[ch_id] = Channel(
                id=ch_id, name=rename.get(ch_id, default_name),
                group=group, entries=entries,
            )

    # --- Primetime — every show ---
    _add_show_channel("primetime", "Primetime", all_shows)

    # --- Genre channels ---
    genre_shows: Dict[str, List[Show]] = {}
    for show in all_shows:
        for g in show.genres:
            genre_shows.setdefault(g, []).append(show)

    for genre, shows in sorted(genre_shows.items()):
        if len(shows) < SHOW_GENRE_MIN:
            continue
        # Skip if one show would dominate the channel (too few other shows
        # to provide real variety even though the threshold was met).
        total_eps = sum(len(s.episodes) for s in shows)
        if total_eps == 0:
            continue
        top_share = max(len(s.episodes) for s in shows) / total_eps
        if top_share > SHOW_GENRE_MAX_DOMINANCE:
            log.debug("Skipping genre '%s' — one show owns %.0f%% of episodes", genre, top_share * 100)
            continue
        _add_show_channel(slugify(genre), genre, shows)

    # --- Goldies ---
    goldies = [s for s in all_shows if _show_year(s) and _show_year(s) < goldies_before]
    if len(goldies) >= GOLDIES_MIN:
        _add_show_channel("goldies", "Goldies", goldies)

    # --- Hits ---
    hits = [s for s in all_shows if s.rating >= hits_rating]
    if len(hits) >= GOLDIES_MIN:
        _add_show_channel("hits", "Hits", hits)
        log.info("Hits channel: %d shows with rating ≥ %.1f", len(hits), hits_rating)

    # --- Movies (exclusive: each movie in one channel only) ---
    if library.movies:
        claimed_movies: set = set()

        genre_movies: Dict[str, List[Movie]] = {}
        for movie in library.movies:
            if movie.genres:
                genre_movies.setdefault(movie.genres[0], []).append(movie)

        for genre, movies in sorted(genre_movies.items()):
            unclaimed = [m for m in movies if m.title not in claimed_movies]
            if len(unclaimed) < MOVIE_GENRE_MIN:
                continue
            ch_id = slugify(genre + "-movies")
            if ch_id in disabled:
                continue
            entries = [_movie_to_entry(m) for m in unclaimed]
            channels[ch_id] = Channel(
                id=ch_id, name=rename.get(ch_id, genre + " Movies"),
                group="Movies", entries=entries,
            )
            claimed_movies.update(m.title for m in unclaimed)

        remaining = [m for m in library.movies if m.title not in claimed_movies]
        if remaining and "movies" not in disabled:
            entries = [_movie_to_entry(m) for m in remaining]
            channels["movies"] = Channel(
                id="movies", name=rename.get("movies", "Movies"),
                group="Movies", entries=entries,
            )

    log.info("Built %d channels", len(channels))
    return channels


def _show_year(show: Show) -> int:
    """Return the first known year from the show's episodes, or 0."""
    for ep in show.episodes:
        if ep.year:
            return ep.year
    return 0


def _episode_to_entry(ep: Episode) -> ScheduleEntry:
    return ScheduleEntry(
        path=ep.path,
        title=ep.show_title,
        subtitle=ep.title,
        duration_sec=ep.duration_sec,
        plot=ep.plot,
        poster_url=ep.poster_url,
        season=ep.season,
        episode=ep.episode,
        year=ep.year,
        genres=ep.genres,
        audio_codec=ep.audio_codec,
        subtitle_paths=ep.subtitle_paths,
        has_embedded_subs=ep.has_embedded_subs,
    )


def _movie_to_entry(movie: Movie) -> ScheduleEntry:
    return ScheduleEntry(
        path=movie.path,
        title=movie.title,
        subtitle=movie.title,
        duration_sec=movie.duration_sec,
        plot=movie.plot,
        poster_url=movie.poster_url,
        year=movie.year,
        genres=movie.genres,
        audio_codec=movie.audio_codec,
        subtitle_paths=movie.subtitle_paths,
        has_embedded_subs=movie.has_embedded_subs,
    )


def _interleave_shows(shows: List[Show]) -> List[ScheduleEntry]:
    """
    Round-robin episodes across shows so the mix alternates between them.

    Each show's contribution is capped at SHOW_GENRE_EPISODE_CAP_FACTOR ×
    the smallest show's episode count so that a show with many seasons
    doesn't run uninterrupted after shorter shows are exhausted.
    """
    if not shows:
        return []
    episode_counts = [len(s.episodes) for s in shows if s.episodes]
    if not episode_counts:
        return []
    min_eps = min(episode_counts)
    cap = min_eps * SHOW_GENRE_EPISODE_CAP_FACTOR

    iterators = [iter(show.episodes[:cap]) for show in shows]
    entries = []
    while True:
        added = False
        new_iters = []
        for it in iterators:
            ep = next(it, None)
            if ep is not None:
                entries.append(_episode_to_entry(ep))
                new_iters.append(it)
                added = True
        iterators = new_iters
        if not added:
            break
    return entries


# ---------------------------------------------------------------------------
# Core schedule position logic
# ---------------------------------------------------------------------------

def _channel_offset_sec(channel_id: str) -> float:
    """
    Deterministic per-channel schedule offset (0 – 7 days in seconds).
    Derived from the channel slug so it's stable across restarts.
    Spreads channels that share content apart in time so the same show
    is very unlikely to be airing simultaneously on two channels.
    """
    h = int(hashlib.md5(channel_id.encode()).hexdigest()[:8], 16)
    return float(h % (7 * 24 * 3600))   # 0..604799 s  (up to 7 days)


def _position_at(channel: Channel, at: datetime) -> Tuple[int, float]:
    """
    Return (entry_index, offset_sec) for what is playing on `channel` at
    the given datetime. Pure calculation — no side effects.
    Each channel gets a deterministic time offset so channels with shared
    content are staggered and unlikely to air the same thing simultaneously.
    """
    elapsed = (at - EPOCH).total_seconds() + _channel_offset_sec(channel.id)
    pos = elapsed % channel.total_duration
    for i, entry in enumerate(channel.entries):
        if pos < entry.duration_sec:
            return i, pos
        pos -= entry.duration_sec
    return 0, 0.0  # fallback (shouldn't happen with valid inputs)


# ---------------------------------------------------------------------------
# Live position
# ---------------------------------------------------------------------------

def get_now_playing(channel: Channel) -> Optional[NowPlaying]:
    """Return what is currently airing on the channel."""
    if not channel.entries or channel.total_duration == 0:
        return None
    idx, offset = _position_at(channel, datetime.now())
    return NowPlaying(
        channel_id=channel.id,
        entry=channel.entries[idx],
        offset_sec=offset,
        entry_index=idx,
    )


def get_playing_at(channel: Channel, at: datetime) -> Optional[Tuple[ScheduleEntry, float]]:
    """
    Return (entry, offset_sec) for what was/will be playing at `at`.
    Used by the catchup endpoint to locate the right file and seek position.
    Returns None if the channel has no entries.
    """
    if not channel.entries or channel.total_duration == 0:
        return None
    idx, offset = _position_at(channel, at)
    return channel.entries[idx], offset


# ---------------------------------------------------------------------------
# EPG — build a window: hours_back in the past + hours_forward into future
# ---------------------------------------------------------------------------

def build_epg_window(
    channels: Dict[str, Channel],
    hours_back: int = 0,
    hours_forward: int = 24,
) -> Dict[str, List[Tuple[datetime, datetime, ScheduleEntry]]]:
    """
    Returns {channel_id: [(start, end, entry), ...]} covering
    [now - hours_back ... now + hours_forward].

    Past entries are complete programme slots (full duration even if they
    started before the window start — clipped to window_start for the
    returned start time, but the actual air time is used for EPG so
    catch-up clients can match by timestamp).
    """
    now = datetime.now()
    window_start = now - timedelta(hours=hours_back)
    window_end = now + timedelta(hours=hours_forward)

    result: Dict[str, List[Tuple[datetime, datetime, ScheduleEntry]]] = {}

    for ch_id, channel in channels.items():
        if not channel.entries or channel.total_duration == 0:
            continue

        slots = []

        # Find the entry playing at window_start
        idx, offset = _position_at(channel, window_start)

        # The programme that straddles window_start began before it
        programme_start = window_start - timedelta(seconds=offset)
        programme_end = programme_start + timedelta(seconds=channel.entries[idx].duration_sec)

        # Walk forward until we pass window_end
        while programme_start < window_end:
            entry = channel.entries[idx]
            slots.append((programme_start, programme_end, entry))
            idx = (idx + 1) % len(channel.entries)
            programme_start = programme_end
            programme_end = programme_start + timedelta(seconds=channel.entries[idx].duration_sec)

        result[ch_id] = slots

    return result
