#!/usr/bin/env python3
"""
FakeIPTV — entry point.
"""
import logging
import os
import signal
import sys

# Load .env before importing anything else so env vars are available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv is optional; fall back to config.yaml / defaults

from fakeiptv.app import FakeIPTV
from fakeiptv.config import load_config
from fakeiptv.server import app as flask_app, set_app

_log_level = getattr(logging, os.environ.get("FAKEIPTV_LOG_LEVEL", "INFO").upper(), logging.INFO)
_log_format = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"
_log_datefmt = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(
    level=_log_level,
    format=_log_format,
    datefmt=_log_datefmt,
)

# Optional file logging — set FAKEIPTV_LOG_FILE to enable
_log_file = os.environ.get("FAKEIPTV_LOG_FILE", "")
if _log_file:
    os.makedirs(os.path.dirname(_log_file), exist_ok=True)
    _fh = logging.FileHandler(_log_file)
    _fh.setLevel(_log_level)
    _fh.setFormatter(logging.Formatter(_log_format, datefmt=_log_datefmt))
    logging.getLogger().addHandler(_fh)
log = logging.getLogger("fakeiptv")


def main():
    config_path = os.environ.get("FAKEIPTV_CONFIG", "config.yaml")
    config = load_config(config_path)

    fake_iptv = FakeIPTV(config)
    set_app(fake_iptv)

    # Graceful shutdown
    def _shutdown(sig, frame):
        log.info("Shutting down...")
        fake_iptv.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    fake_iptv.start()

    flask_app.run(
        host=config.server.host,
        port=config.server.port,
        threaded=True,
        use_reloader=False,
    )


if __name__ == "__main__":
    main()
