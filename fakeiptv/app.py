"""
app.py — FakeIPTV application core.
Wires together scanner, scheduler, streamer, EPG, and playlist.
Also owns the daily refresh timer.
"""
import logging
import threading
import time
from datetime import datetime, timedelta
from typing import Dict

from .config import AppConfig
from .epg import build_xmltv
from .playlist import build_m3u8
from .scanner import MediaLibrary, Scanner
from .scheduler import Channel, build_channels, build_epg_window
from .streamer import StreamManager

log = logging.getLogger(__name__)


class FakeIPTV:
    def __init__(self, config: AppConfig):
        self.config = config
        self.start_time = time.time()

        self.library: MediaLibrary = MediaLibrary()
        self.channels: Dict[str, Channel] = {}
        self._playlist_cache: str = ""
        self._epg_cache: str = ""
        self._cache_lock = threading.Lock()

        self.stream_manager = StreamManager(
            tmp_base=config.server.tmp_dir,
            subtitles=config.server.subtitles,
        )
        self._refresh_timer: threading.Timer = None

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def start(self):
        log.info("FakeIPTV starting up...")
        self.refresh()
        self._schedule_midnight_refresh()
        log.info("FakeIPTV ready on http://%s:%d", self.config.server.rpi_ip, self.config.server.port)

    def stop(self):
        if self._refresh_timer:
            self._refresh_timer.cancel()
        self.stream_manager.stop_all()
        log.info("FakeIPTV stopped.")

    # ------------------------------------------------------------------
    # Refresh
    # ------------------------------------------------------------------

    def refresh(self):
        log.info("Refreshing media library...")
        scanner = Scanner(
            shows_path=self.config.media.shows_path,
            movies_path=self.config.media.movies_path,
            cache_dir=self.config.metadata.cache_dir,
            tmdb_api_key=self.config.metadata.tmdb_api_key,
        )
        library = scanner.scan()
        channels = build_channels(
            library,
            disabled=self.config.channels.disabled,
            rename=self.config.channels.rename,
        )

        self.library = library
        self.channels = channels

        self.stream_manager.reload(channels)
        self._rebuild_cache()
        log.info(
            "Refresh complete: %d shows, %d movies, %d channels",
            len(library.shows),
            len(library.movies),
            len(channels),
        )

    def _rebuild_cache(self):
        base_url = f"http://{self.config.server.rpi_ip}:{self.config.server.port}"
        epg_url = f"{base_url}/epg.xml"

        schedule = build_epg_window(self.channels, hours=24)
        epg_xml = build_xmltv(self.channels, schedule)
        playlist = build_m3u8(self.channels, base_url, epg_url)

        with self._cache_lock:
            self._epg_cache = epg_xml
            self._playlist_cache = playlist

    # ------------------------------------------------------------------
    # Cache accessors (called by Flask routes)
    # ------------------------------------------------------------------

    def get_playlist(self) -> str:
        with self._cache_lock:
            return self._playlist_cache

    def get_epg(self) -> str:
        with self._cache_lock:
            return self._epg_cache

    # ------------------------------------------------------------------
    # Midnight refresh timer
    # ------------------------------------------------------------------

    def _schedule_midnight_refresh(self):
        now = datetime.now()
        tomorrow_midnight = (now + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        delay = (tomorrow_midnight - now).total_seconds()
        log.info("Next auto-refresh in %.0f seconds (midnight local time)", delay)
        self._refresh_timer = threading.Timer(delay, self._midnight_refresh)
        self._refresh_timer.daemon = True
        self._refresh_timer.start()

    def _midnight_refresh(self):
        log.info("Midnight auto-refresh triggered")
        self.refresh()
        self._schedule_midnight_refresh()
