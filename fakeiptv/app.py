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
from .library_cache import LibraryCache
from .playlist import build_m3u8
from .models import Channel, MediaLibrary
from .scanner import Scanner
from .scheduler import build_channels, build_epg_window
from .streamer import StreamManager
from .catchup import CatchupManager

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
            audio_copy=config.server.audio_copy,
            prewarm_timeout=config.server.prewarm_timeout,
            ready_segments=config.server.ready_segments,
            session_mode=config.server.prewarm_session,
            prewarm_adjacent=config.server.prewarm_adjacent,
            preferred_audio_language=config.server.preferred_audio_language,
            bumpers_path=config.server.bumpers_path,
            bumpers_cache_dir=config.metadata.cache_dir,
            subtitle_background=config.server.subtitle_background,
        )
        self.catchup_manager = CatchupManager(
            tmp_base=config.server.tmp_dir,
            subtitles=config.server.subtitles,
            preferred_audio_language=config.server.preferred_audio_language,
            subtitle_background=config.server.subtitle_background,
        )
        self._refresh_timer: threading.Timer = None
        self._epg_timer: threading.Timer = None

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    def start(self):
        log.info("FakeIPTV starting up...")
        self.refresh()
        self._schedule_midnight_refresh()
        self._schedule_hourly_epg()
        log.info("FakeIPTV ready on http://%s:%d", self.config.server.host_ip, self.config.server.port)

    def stop(self):
        if self._refresh_timer:
            self._refresh_timer.cancel()
        if self._epg_timer:
            self._epg_timer.cancel()
        self.stream_manager.stop_all()
        self.catchup_manager.stop_all()
        log.info("FakeIPTV stopped.")

    # ------------------------------------------------------------------
    # Refresh
    # ------------------------------------------------------------------

    def refresh(self, force: bool = False):
        cache = LibraryCache(self.config)

        if not force:
            cached_library = cache.load()
            if cached_library is not None:
                channels = build_channels(
                    cached_library,
                    disabled=self.config.channels.disabled,
                    rename=self.config.channels.rename,
                    goldies_before=self.config.channels.goldies_before,
                    hits_rating=self.config.channels.hits_rating,
                )
                self.library = cached_library
                self.channels = channels
                self.stream_manager.reload(channels)
                self._rebuild_cache()
                log.info("Startup complete (from cache): %d channels", len(channels))
                return

        log.info("Refreshing media library (full scan)...")
        scanner = Scanner(
            shows_path=self.config.media.shows_path,
            movies_path=self.config.media.movies_path,
            cache_dir=self.config.metadata.cache_dir,
            tmdb_api_key=self.config.metadata.tmdb_api_key,
            ignore_patterns=self.config.media.ignore_patterns,
            sonarr_url=self.config.metadata.sonarr_url,
            sonarr_api_key=self.config.metadata.sonarr_api_key,
            radarr_url=self.config.metadata.radarr_url,
            radarr_api_key=self.config.metadata.radarr_api_key,
        )
        library = scanner.scan()
        channels = build_channels(
            library,
            disabled=self.config.channels.disabled,
            rename=self.config.channels.rename,
            goldies_before=self.config.channels.goldies_before,
            hits_rating=self.config.channels.hits_rating,
        )

        self.library = library
        self.channels = channels

        self.stream_manager.reload(channels)
        self._rebuild_cache()
        cache.save(library)
        log.info(
            "Refresh complete: %d shows, %d movies, %d channels",
            len(library.shows),
            len(library.movies),
            len(channels),
        )

    def _rebuild_cache(self):
        base_url = f"http://{self.config.server.host_ip}:{self.config.server.port}"
        epg_url = f"{base_url}/epg.xml.gz"
        catchup_days = self.config.server.catchup_days

        schedule = build_epg_window(
            self.channels,
            hours_back=catchup_days * 24,
            hours_forward=24,
        )
        epg_xml = build_xmltv(self.channels, schedule)
        playlist = build_m3u8(self.channels, base_url, epg_url, catchup_days=catchup_days)

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
    # Channel pre-warming
    # ------------------------------------------------------------------

    def prewarm_channels(self):
        """Start ffmpeg for every channel in parallel background threads.

        Each channel's _launch() reads SRT files from NAS (2-3s).  Running them
        in parallel cuts total prewarm time from ~N×3s (serial) to ~3s (parallel).
        Threads are staggered by 0.1s to avoid a thundering-herd NAS hit.
        """
        channels = list(self.channels.keys())
        if not channels:
            return

        def _warm():
            log.info("Pre-warming %d channels (parallel)...", len(channels))
            threads = []
            for ch_id in channels:
                def _start(cid=ch_id):
                    try:
                        self.stream_manager.ensure_started(cid, is_prewarm=True)
                    except Exception:
                        log.exception("Pre-warm failed for channel %s", cid)
                t = threading.Thread(target=_start, daemon=True, name=f"prewarm-{ch_id}")
                t.start()
                threads.append(t)
                time.sleep(0.1)  # small stagger — avoids NAS thundering herd
            for t in threads:
                t.join()
            log.info("Pre-warm complete: %d channels started", len(channels))

        threading.Thread(target=_warm, daemon=True, name="prewarm").start()


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
        self.refresh(force=True)
        self._schedule_midnight_refresh()

    # ------------------------------------------------------------------
    # Hourly EPG rolling window
    # ------------------------------------------------------------------

    def _schedule_hourly_epg(self):
        self._epg_timer = threading.Timer(3600, self._hourly_epg)
        self._epg_timer.daemon = True
        self._epg_timer.start()

    def _hourly_epg(self):
        log.debug("Hourly EPG rebuild — rolling forward window")
        self._rebuild_cache()
        self._schedule_hourly_epg()
