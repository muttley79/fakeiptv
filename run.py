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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
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
