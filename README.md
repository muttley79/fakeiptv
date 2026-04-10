# FakeIPTV

Turns a NAS full of TV shows and movies into fake live IPTV channels.
Generates a proper HLS live stream (sliding-window M3U8 + MPEG-TS segments) and XMLTV EPG for each channel.
No transcoding — just fast remux via `ffmpeg -c copy`.

## How it works

- The NAS is scanned for shows and movies at startup and once a day at midnight (local time).
- Each show gets its own channel that plays episodes in order, looping forever.
- Genre mix channels (e.g. "Drama Mix") interleave episodes from multiple shows.
- A "Movies" channel rotates through all movies; genre-specific movie channels are created if there are ≥ 5 movies in a genre.
- Every channel's schedule is **deterministic** — anchored to a fixed epoch. If you restart the server, channels resume exactly where they would have been.
- One `ffmpeg -re -c copy` process per channel outputs HLS segments to a temp directory.
- Flask serves the segments and the XMLTV EPG.

---

## Requirements

**Raspberry Pi (or any Linux machine):**
- Python 3.9+
- `ffmpeg` and `ffprobe` (system packages)
- NAS mounted (e.g. via SMB/CIFS at `/mnt/nas`)

```bash
sudo apt install ffmpeg
```

---

## Installation

```bash
# 1. Clone / copy to the Pi
git clone <repo> /opt/fakeiptv
cd /opt/fakeiptv

# 2. Create virtualenv
python3 -m venv venv
venv/bin/pip install -r requirements.txt

# 3. Configure
cp .env.example .env
nano .env          # set FAKEIPTV_RPI_IP and paths

# 4. Run (dev mode)
venv/bin/python run.py
```

---

## Configuration

All settings can be set in **`.env`** (preferred) or **`config.yaml`**.
Environment variables always take precedence over `config.yaml`.

### `.env` (copy from `.env.example`)

| Variable | Default | Description |
|---|---|---|
| `FAKEIPTV_SHOWS_PATH` | `/mnt/nas/Shows` | Path to TV shows on the NAS |
| `FAKEIPTV_MOVIES_PATH` | `/mnt/nas/Movies` | Path to movies on the NAS |
| `FAKEIPTV_RPI_IP` | `127.0.0.1` | **LAN IP of this machine** (used in stream URLs) |
| `FAKEIPTV_PORT` | `8080` | HTTP port to listen on |
| `FAKEIPTV_TMP_DIR` | `/tmp/fakeiptv` | Where HLS segments are written |
| `FAKEIPTV_TMDB_API_KEY` | _(empty)_ | Optional TMDB API key for metadata fallback |
| `FAKEIPTV_CACHE_DIR` | `~/.fakeiptv/` | Duration + TMDB cache directory |

### NAS layout expected

```
/mnt/nas/
├── Shows/
│   ├── Breaking Bad/
│   │   ├── Season 01/
│   │   │   ├── Breaking.Bad.S01E01.mkv
│   │   │   └── Breaking.Bad.S01E01.nfo   ← optional sidecar
│   │   └── ...
│   └── ...
└── Movies/
    ├── Inception (2010)/
    │   ├── Inception.mkv
    │   └── Inception.nfo
    └── ...
```

NFO files follow the **Kodi / Jellyfin XML format**.
If an NFO is missing or incomplete, metadata is fetched from TMDB (requires API key).

### Disabling or renaming channels (`config.yaml`)

```yaml
channels:
  disabled:
    - some-show-slug    # auto-derived: lowercase, spaces → hyphens
  rename:
    breaking-bad: Breaking Bad 24/7
```

---

## Running as a systemd service

```bash
# Copy unit file
sudo cp /opt/fakeiptv/fakeiptv.service /etc/systemd/system/

# Enable + start
sudo systemctl daemon-reload
sudo systemctl enable fakeiptv
sudo systemctl start fakeiptv

# Check logs
journalctl -u fakeiptv -f
```

---

## Adding to Televizo

1. Open Televizo → **Add Playlist** → enter:
   ```
   http://<rpi-ip>:8080/playlist.m3u8
   ```
2. **Add EPG source** → enter:
   ```
   http://<rpi-ip>:8080/epg.xml
   ```
3. Select any channel — it will start mid-show, just like real TV.

---

## API endpoints

| Endpoint | Description |
|---|---|
| `GET /playlist.m3u8` | IPTV channel list (import into Televizo) |
| `GET /epg.xml` | XMLTV EPG, 24-hour window |
| `GET /hls/<channel_id>/stream.m3u8` | Live HLS manifest for a channel |
| `GET /hls/<channel_id>/<seg>.ts` | HLS MPEG-TS segment |
| `GET /refresh` | Trigger immediate library rescan |
| `GET /status` | JSON: channels, now-playing, uptime |

---

## Troubleshooting

**Channel shows "loading" forever in Televizo**
- Check `journalctl -u fakeiptv` for ffmpeg errors.
- Make sure the NAS is mounted and files are readable.
- Some MKV files with exotic audio (DTS, TrueHD) may fail to remux. ffmpeg stderr will say so.

**Wrong episode playing / schedule seems off**
- The schedule epoch is `2024-01-01 00:00:00` local time. All offsets are computed from that.
- To "reset" a channel's schedule, edit `EPOCH` in `fakeiptv/scheduler.py`.

**TMDB metadata not loading**
- Set `FAKEIPTV_TMDB_API_KEY` in `.env`.
- TMDB results are cached in `FAKEIPTV_CACHE_DIR/tmdb_cache.json` — delete this file to force a re-fetch.

**Duration shows as 0 / episode skipped**
- ffprobe couldn't read the file. Check the file isn't corrupt and ffprobe is installed (`ffprobe -version`).
- Duration results are cached in `FAKEIPTV_CACHE_DIR/duration_cache.json`.
