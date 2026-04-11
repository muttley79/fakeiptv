# FakeIPTV

Turns a NAS full of TV shows and movies into fake live IPTV channels.
Generates a proper HLS live stream (sliding-window M3U8 + MPEG-TS segments) and XMLTV EPG for each channel.
No transcoding — just fast remux via `ffmpeg -c copy`.

## How it works

- The NAS is scanned for shows and movies at startup and once a day at midnight (local time).
- Channels are auto-discovered from your library — no per-show channels, only curated mixes:
  - **Primetime** — all shows interleaved
  - **{Genre}** — per-genre mix (e.g. Drama, Comedy) if ≥ 2 shows share the genre
  - **Goldies** — shows older than a configurable year
  - **Hits** — shows with a rating above a configurable threshold (from Sonarr/Radarr/TMDB/NFO)
  - **Movies** — all movies; genre-specific movie channels if ≥ 3 movies share a genre
- Every channel's schedule is **deterministic** — anchored to a fixed epoch. Restarting the server picks up exactly where it would have been.
- One `ffmpeg -re -c copy` process per channel outputs 2-second HLS segments to a tmpfs directory.
- All channels are **pre-warmed at startup** so channel switching is near-instant.
- Flask serves the segments and the XMLTV EPG directly from tmpfs.

---

## Deployment — Docker (recommended)

### Requirements

- Docker + Docker Compose on the host (Raspberry Pi or any Linux machine)
- NAS mounted on the host (e.g. via SMB/CIFS at `/mnt/nas`)

### Setup

```bash
# 1. Clone the repo
git clone <repo> /opt/fakeiptv
cd /opt/fakeiptv

# 2. Configure
cp .env.example .env
nano .env          # set FAKEIPTV_RPI_IP, paths, API keys

# 3. Build and start
docker compose up -d

# 4. Watch logs
docker compose logs -f
```

### Updating

```bash
git pull
docker compose up -d --build
```

No `down` needed — compose stops, rebuilds, and restarts in one step.

### Useful commands

```bash
docker logs fakeiptv          # view logs
docker exec -it fakeiptv bash # shell into container
docker compose restart        # restart without rebuild
```

---

## Deployment — systemd (alternative)

```bash
# Install system deps
sudo apt install ffmpeg python3-venv

# Set up virtualenv
python3 -m venv venv
venv/bin/pip install -r requirements.txt

cp .env.example .env && nano .env

# Install and start the service
sudo cp fakeiptv.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now fakeiptv
journalctl -u fakeiptv -f
```

---

## Configuration

All settings live in **`.env`** (preferred) or **`config.yaml`**.
Environment variables always take precedence over `config.yaml`.

| Variable | Default | Description |
|---|---|---|
| `FAKEIPTV_RPI_IP` | `127.0.0.1` | **LAN IP of this machine** — used in stream/EPG URLs |
| `FAKEIPTV_PORT` | `8080` | HTTP port |
| `FAKEIPTV_SHOWS_PATH` | `/mnt/nas/Shows` | Host path to TV shows |
| `FAKEIPTV_MOVIES_PATH` | `/mnt/nas/Movies` | Host path to movies |
| `FAKEIPTV_TMP_DIR` | `/tmp/fakeiptv` | Where HLS segments are written (use tmpfs) |
| `FAKEIPTV_CACHE_DIR` | `~/.fakeiptv/` | SQLite cache for durations + TMDB metadata |
| `FAKEIPTV_SUBTITLES` | `true` | Include subtitle tracks (SRT/ASS→WebVTT) |
| `FAKEIPTV_CATCHUP_DAYS` | `7` | Days of past programming available for catch-up |
| `FAKEIPTV_TMDB_API_KEY` | _(empty)_ | Optional — TMDB metadata fallback |
| `FAKEIPTV_SONARR_URL` | _(empty)_ | Optional — Sonarr integration for ratings/metadata |
| `FAKEIPTV_SONARR_API_KEY` | _(empty)_ | |
| `FAKEIPTV_RADARR_URL` | _(empty)_ | Optional — Radarr integration for ratings/metadata |
| `FAKEIPTV_RADARR_API_KEY` | _(empty)_ | |
| `FAKEIPTV_TMPFS_SIZE` | `1073741824` | tmpfs size in bytes for HLS segments (Docker only) |

### NAS layout expected

```
Shows/
└── Breaking Bad/
    └── Season 01/
        ├── Breaking.Bad.S01E01.mkv
        └── Breaking.Bad.S01E01.nfo   ← optional Kodi/Jellyfin sidecar

Movies/
└── Inception (2010)/
    ├── Inception.mkv
    └── Inception.nfo
```

### Disabling or renaming channels (`config.yaml`)

```yaml
channels:
  goldies_before: 2010     # shows older than this go into "Goldies"
  hits_rating: 8.0         # minimum rating (0–10) for "Hits"
  disabled:
    - goldies
  rename:
    primetime: Prime Time
```

---

## Adding to Televizo

1. **Add Playlist** → `http://<rpi-ip>:<port>/playlist.m3u8`
2. **Add EPG** → `http://<rpi-ip>:<port>/epg.xml`
3. Select any channel — it starts mid-show, just like real TV.

---

## API endpoints

| Endpoint | Description |
|---|---|
| `GET /playlist.m3u8` | IPTV channel list |
| `GET /epg.xml` | XMLTV EPG (7 days back + 1 day forward) |
| `GET /hls/<channel_id>/stream.m3u8` | Live HLS manifest |
| `GET /hls/<channel_id>/<seg>.ts` | HLS MPEG-TS segment |
| `GET /catchup/<channel_id>?utc=<ts>` | Catch-up VOD session |
| `GET /refresh` | Trigger immediate library rescan |
| `GET /status` | JSON: channels, now-playing, ready state, uptime |

---

## Troubleshooting

**Channel shows "loading" forever**
- Check `docker logs fakeiptv` for ffmpeg errors.
- Verify the NAS is mounted and paths in `.env` are correct.
- Check `/status` — `"ready": false` means ffmpeg hasn't produced its first segment yet.

**Bitmap subtitle crash loop (PGS/VOBSUB)**
- The monitor thread detects "bitmap to bitmap" in ffmpeg stderr and automatically disables subtitles for that channel. It self-heals after one restart.

**eac3 / unspecified sample rate error**
- Some MKV files lose eac3 codec parameters when muxed into MPEG-TS with `-c copy`. The monitor detects this and automatically falls back to `-c:a aac -b:a 192k` for that channel.

**Wrong episode playing / schedule seems off**
- The schedule epoch is `2024-01-01 00:00:00` local time. All offsets are computed from that. Consistent across restarts and container rebuilds.

**Cache re-probing everything after Docker setup**
- The SQLite cache is keyed by file path. If the in-container path (`/shows/...`) differs from the original path (`/mnt/nas/Shows/...`), all files are re-probed once. This is normal on first Docker run.
- Mount `~/.fakeiptv` as the cache volume to persist across rebuilds.
