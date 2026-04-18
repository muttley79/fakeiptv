"""
models.py — Shared dataclasses for scanner, scheduler, and streaming.
"""
from dataclasses import dataclass, field
from typing import Dict, List


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
    rating: float = 0.0
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
    rating: float = 0.0
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
    rating: float = 0.0


@dataclass
class MediaLibrary:
    shows: Dict[str, Show] = field(default_factory=dict)
    movies: List[Movie] = field(default_factory=list)


@dataclass
class ScheduleEntry:
    """One slot in a channel's repeating schedule."""
    path: str
    title: str
    subtitle: str
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
    is_hdr: bool = False
    video_width: int = 0
    video_height: int = 0
    video_codec: str = ""


@dataclass
class Channel:
    id: str
    name: str
    group: str
    entries: List[ScheduleEntry] = field(default_factory=list)
    poster_url: str = ""

    @property
    def total_duration(self) -> float:
        return sum(e.duration_sec for e in self.entries)


@dataclass
class NowPlaying:
    channel_id: str
    entry: ScheduleEntry
    offset_sec: float
    entry_index: int
