"""
config.py — Loads configuration from config.yaml, overridden by environment
variables (set directly or via .env file loaded in run.py).

Environment variable precedence:  .env / shell env > config.yaml > defaults
"""
import os
import yaml
from dataclasses import dataclass, field
from typing import List


@dataclass
class MediaConfig:
    shows_path: str = "/mnt/nas/Shows"
    movies_path: str = "/mnt/nas/Movies"
    ignore_patterns: List[str] = field(default_factory=list)


@dataclass
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080
    rpi_ip: str = "127.0.0.1"
    tmp_dir: str = "/dev/shm/fakeiptv"
    subtitles: bool = True
    catchup_days: int = 7


@dataclass
class MetadataConfig:
    tmdb_api_key: str = ""
    cache_dir: str = "~/.fakeiptv/"
    sonarr_url: str = ""
    sonarr_api_key: str = ""
    radarr_url: str = ""
    radarr_api_key: str = ""


@dataclass
class ChannelsConfig:
    disabled: List[str] = field(default_factory=list)
    rename: dict = field(default_factory=dict)
    goldies_before: int = 2010   # shows with year < this go into "Goldies"
    hits_rating: float = 8.0     # minimum rating (0–10) for "Hits" channel


@dataclass
class AppConfig:
    media: MediaConfig = field(default_factory=MediaConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    metadata: MetadataConfig = field(default_factory=MetadataConfig)
    channels: ChannelsConfig = field(default_factory=ChannelsConfig)


def _env(key: str, fallback: str) -> str:
    return os.environ.get(key, fallback)


def _env_int(key: str, fallback: int) -> int:
    val = os.environ.get(key)
    if val is not None:
        try:
            return int(val)
        except ValueError:
            pass
    return fallback


def _env_bool(key: str, fallback: bool) -> bool:
    val = os.environ.get(key)
    if val is not None:
        return val.strip().lower() not in ("0", "false", "no", "off")
    return fallback


def load_config(path: str = "config.yaml") -> AppConfig:
    raw: dict = {}
    if os.path.exists(path):
        with open(path, "r") as f:
            raw = yaml.safe_load(f) or {}

    media_raw = raw.get("media", {})
    server_raw = raw.get("server", {})
    meta_raw = raw.get("metadata", {})
    ch_raw = raw.get("channels", {}) or {}

    media = MediaConfig(
        shows_path=_env("FAKEIPTV_SHOWS_PATH", media_raw.get("shows_path", "/mnt/nas/Shows")),
        movies_path=_env("FAKEIPTV_MOVIES_PATH", media_raw.get("movies_path", "/mnt/nas/Movies")),
        ignore_patterns=media_raw.get("ignore_patterns") or [],
    )
    server = ServerConfig(
        host=_env("FAKEIPTV_HOST", server_raw.get("host", "0.0.0.0")),
        port=_env_int("FAKEIPTV_PORT", int(server_raw.get("port", 8080))),
        rpi_ip=_env("FAKEIPTV_RPI_IP", server_raw.get("rpi_ip", "127.0.0.1")),
        tmp_dir=_env("FAKEIPTV_TMP_DIR", server_raw.get("tmp_dir", "/tmp/fakeiptv")),
        subtitles=_env_bool("FAKEIPTV_SUBTITLES", server_raw.get("subtitles", True)),
        catchup_days=_env_int("FAKEIPTV_CATCHUP_DAYS", int(server_raw.get("catchup_days", 7))),
    )
    metadata = MetadataConfig(
        tmdb_api_key=_env("FAKEIPTV_TMDB_API_KEY", meta_raw.get("tmdb_api_key", "")),
        cache_dir=os.path.expanduser(
            _env("FAKEIPTV_CACHE_DIR", meta_raw.get("cache_dir", "~/.fakeiptv/"))
        ),
        sonarr_url=_env("FAKEIPTV_SONARR_URL", meta_raw.get("sonarr_url", "")),
        sonarr_api_key=_env("FAKEIPTV_SONARR_API_KEY", meta_raw.get("sonarr_api_key", "")),
        radarr_url=_env("FAKEIPTV_RADARR_URL", meta_raw.get("radarr_url", "")),
        radarr_api_key=_env("FAKEIPTV_RADARR_API_KEY", meta_raw.get("radarr_api_key", "")),
    )
    channels = ChannelsConfig(
        disabled=ch_raw.get("disabled") or [],
        rename=ch_raw.get("rename") or {},
        goldies_before=int(ch_raw.get("goldies_before", 2010)),
        hits_rating=float(ch_raw.get("hits_rating", 8.0)),
    )

    return AppConfig(media=media, server=server, metadata=metadata, channels=channels)
