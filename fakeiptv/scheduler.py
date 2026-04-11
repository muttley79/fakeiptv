"""
scheduler.py — Builds deterministic per-channel schedules anchored to a
fixed local-time epoch, calculates the current playback position, and
generates XMLTV EPG data for a configurable window (past + future).
"""
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
SHOW_GENRE_MIN = 2

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

    Each show and movie is assigned to exactly ONE channel so the same
    content never plays simultaneously on two channels.

    Show assignment priority (first match wins):
      1. "Hits"      — shows with rating ≥ hits_rating
      2. "Goldies"   — shows with known year < goldies_before
      3. "{Genre}"   — dominant genre channel (≥ SHOW_GENRE_MIN shows)
      4. "Primetime" — all remaining unclaimed shows

    Movie assignment priority:
      1. "{Genre} Movies" — primary genre channel (≥ MOVIE_GENRE_MIN movies)
      2. "Movies"         — all remaining unclaimed movies
    """
    disabled = disabled or []
    rename = rename or {}
    channels: Dict[str, Channel] = {}

    all_shows = [s for s in library.shows.values() if s.episodes]
    claimed: set = set()   # show names already assigned to a channel

    def _make_show_channel(ch_id: str, default_name: str, shows: List[Show],
                           group: str = "Shows") -> None:
        if ch_id in disabled or not shows:
            return
        ch_name = rename.get(ch_id, default_name)
        entries = _interleave_shows(shows)
        if entries:
            channels[ch_id] = Channel(id=ch_id, name=ch_name, group=group, entries=entries)
            claimed.update(s.name for s in shows)

    # --- Pass 1: Hits ---
    hits_shows = [s for s in all_shows if s.rating >= hits_rating]
    if len(hits_shows) >= GOLDIES_MIN:
        _make_show_channel("hits", "Hits", hits_shows)
        log.info("Hits channel: %d shows with rating ≥ %.1f", len(hits_shows), hits_rating)

    # --- Pass 2: Goldies ---
    goldies_shows = [s for s in all_shows
                     if s.name not in claimed and _show_year(s) and _show_year(s) < goldies_before]
    if len(goldies_shows) >= GOLDIES_MIN:
        _make_show_channel("goldies", "Goldies", goldies_shows)

    # --- Pass 3: Genre channels ---
    # Build genre→shows map from unclaimed shows only.
    # Each show is assigned to its dominant genre (first genre in its list).
    dominant_genre: Dict[str, str] = {}   # show_name → genre
    genre_shows: Dict[str, List[Show]] = {}
    for show in all_shows:
        if show.name in claimed or not show.genres:
            continue
        g = show.genres[0]   # dominant genre (sorted by frequency in scanner)
        dominant_genre[show.name] = g
        genre_shows.setdefault(g, []).append(show)

    for genre, shows in sorted(genre_shows.items()):
        # Filter again — some shows may have been claimed by an earlier genre pass
        unclaimed_shows = [s for s in shows if s.name not in claimed]
        if len(unclaimed_shows) < SHOW_GENRE_MIN:
            continue
        ch_id = slugify(genre)
        _make_show_channel(ch_id, genre, unclaimed_shows)

    # --- Pass 4: Primetime — all remaining unclaimed shows ---
    remaining_shows = [s for s in all_shows if s.name not in claimed]
    if remaining_shows:
        _make_show_channel("primetime", "Primetime", remaining_shows)

    # --- Movies: genre-first, then remainder ---
    claimed_movies: set = set()

    def _make_movie_channel(ch_id: str, default_name: str, movies: List[Movie],
                             group: str = "Movies") -> None:
        if ch_id in disabled or not movies:
            return
        ch_name = rename.get(ch_id, default_name)
        entries = [_movie_to_entry(m) for m in movies]
        channels[ch_id] = Channel(id=ch_id, name=ch_name, group=group, entries=entries)
        claimed_movies.update(m.title for m in movies)

    if library.movies:
        # Primary genre per movie (first genre)
        movie_genre: Dict[str, List[Movie]] = {}
        for movie in library.movies:
            if movie.genres:
                movie_genre.setdefault(movie.genres[0], []).append(movie)

        for genre, movies in sorted(movie_genre.items()):
            unclaimed = [m for m in movies if m.title not in claimed_movies]
            if len(unclaimed) < MOVIE_GENRE_MIN:
                continue
            ch_id = slugify(genre + "-movies")
            _make_movie_channel(ch_id, genre + " Movies", unclaimed)

        # Remaining movies → "Movies"
        remaining_movies = [m for m in library.movies if m.title not in claimed_movies]
        if remaining_movies:
            _make_movie_channel("movies", "Movies", remaining_movies)

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
    )


def _movie_to_entry(movie: Movie) -> ScheduleEntry:
    return ScheduleEntry(
        path=movie.path,
        title=movie.title,
        subtitle=movie.title,
        duration_sec=movie.duration_sec,
        plot=movie.plot,
        poster_url=movie.poster_url,
    )


def _interleave_shows(shows: List[Show]) -> List[ScheduleEntry]:
    """Round-robin episodes across shows so the mix alternates between them."""
    iterators = [iter(show.episodes) for show in shows]
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

def _position_at(channel: Channel, at: datetime) -> Tuple[int, float]:
    """
    Return (entry_index, offset_sec) for what is playing on `channel` at
    the given datetime. Pure calculation — no side effects.
    """
    elapsed = (at - EPOCH).total_seconds()
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
