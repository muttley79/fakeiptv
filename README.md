# FakeIPTV

Turns a NAS full of TV shows and movies into fake live IPTV channels.
Generates a proper HLS live stream (sliding-window M3U8 + MPEG-TS segments), XMLTV EPG, and catch-up TV for each channel.
No transcoding — just fast remux via `ffmpeg -c copy`.

## How it works

- The NAS is scanned for shows and movies at startup and once a day at midnight (local time).
- Channels are auto-discovered from your library — no per-show channels, only curated mixes:
  - **Primetime** — all shows interleaved
  - **{Genre}** — per-genre mix (e.g. Drama, Comedy) if ≥ 3 shows share the genre
  - **Goldies** — shows older than a configurable year
  - **Hits** — shows with a rating above a configurable threshold (from Sonarr/Radarr/TMDB/NFO)
  - **Movies** — all movies; genre-specific movie channels if ≥ 3 movies share a genre
- Every channel's schedule is **deterministic** — anchored to a fixed epoch. Restarting the server picks up exactly where it would have been.
- One `ffmpeg -re -c copy` process per channel outputs 2-second HLS segments to a tmpfs directory.
- All channels are **pre-warmed on first client connect** so channel switching is near-instant.
- Flask serves the segments, XMLTV EPG, and catch-up manifests directly from tmpfs.

---

## Deployment — Docker (recommended)

### Requirements

- Docker + Docker Compose on the host (Raspberry Pi 4 or any Linux machine)
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
docker logs -f fakeiptv
```

### Updating

```bash
git pull && docker compose up -d --build
```

No `down` needed — compose stops, rebuilds, and restarts in one step.

### Useful commands

```bash
docker logs fakeiptv            # view logs
docker logs -f fakeiptv         # follow logs
docker exec -it fakeiptv bash   # shell into container
docker compose restart          # restart without rebuild
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
| `FAKEIPTV_PREWARM` | `false` | Start all channels on first client connect (smoother channel switching, higher CPU spike) |
| `FAKEIPTV_PREWARM_TIMEOUT` | `120` | Seconds before a pre-warmed-but-never-watched channel is stopped (`0` = never stop) |
| `FAKEIPTV_READY_SEGMENTS` | `3` | HLS segments buffered before a channel is declared ready (~2s each; higher = less startup stutter, more channel-switch delay) |
| `FAKEIPTV_CATCHUP_DAYS` | `7` | Days of past programming available for catch-up |
| `FAKEIPTV_TMDB_API_KEY` | _(empty)_ | Optional — TMDB metadata fallback |
| `FAKEIPTV_SONARR_URL` | _(empty)_ | Optional — Sonarr integration for ratings/metadata |
| `FAKEIPTV_SONARR_API_KEY` | _(empty)_ | |
| `FAKEIPTV_RADARR_URL` | _(empty)_ | Optional — Radarr integration for ratings/metadata |
| `FAKEIPTV_RADARR_API_KEY` | _(empty)_ | |
| `FAKEIPTV_TMPFS_SIZE` | `1073741824` | tmpfs size in bytes for HLS segments (Docker only) |
| `TZ` | `UTC` | Timezone for schedule and EPG (e.g. `Asia/Jerusalem`) |

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

1. **Add Playlist** → `http://<ip>:<port>/playlist.m3u8`
2. The EPG URL is embedded in the playlist automatically — Televizo picks it up from `url-tvg=`
3. Select any channel — it starts mid-show, just like real TV
4. **Catch-up**: open the EPG guide, select a past programme, press play

---

## EPG

- Available at `/epg.xml` (plain) and `/epg.xml.gz` (gzip — what Televizo fetches)
- Covers 7 days back + 1 day forward
- Timestamps are always UTC (`+0000`) for maximum player compatibility
- Regenerates automatically every hour

---

## Catch-up

Catch-up uses `catchup="shift"` mode (required for Televizo — `catchup="default"` does not work).

When a past programme is selected in Televizo's EPG:
1. Televizo calls the live stream URL with `?utc=TIMESTAMP` appended
2. The server detects the timestamp and spins up a temporary VOD ffmpeg session
3. The session serves a finite HLS playlist (`#EXT-X-ENDLIST`) starting at the requested offset
4. Sessions are reused for up to 60 seconds to handle Televizo's polling behaviour
5. Sessions expire after 2 hours of inactivity

---

## API endpoints

| Endpoint | Description |
|---|---|
| `GET /playlist.m3u8` | IPTV channel list |
| `GET /epg.xml` | XMLTV EPG (plain XML) |
| `GET /epg.xml.gz` | XMLTV EPG (gzip) |
| `GET /hls/<channel_id>/stream.m3u8` | Live HLS manifest (also catch-up entry point with `?utc=`) |
| `GET /hls/<channel_id>/<seg>.ts` | HLS MPEG-TS segment |
| `GET /catchup/<channel_id>/<session_id>/stream.m3u8` | Catch-up VOD manifest |
| `GET /catchup/<channel_id>/<session_id>/<seg>.ts` | Catch-up VOD segment |
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

**EPG not showing in Televizo**
- Make sure the playlist was re-imported after changes (Televizo caches the playlist).
- The EPG URL is auto-embedded via `url-tvg=` in the playlist header — no manual EPG URL needed in Televizo.

**Catch-up not working**
- Catch-up requires `catchup="shift"` mode — `catchup="default"` does not substitute timestamps in Televizo.
- EPG must be loaded and working first — Televizo needs programme times to trigger catch-up.
- Check `/status` to confirm the channel exists and has entries.

**Wrong episode playing / schedule seems off**
- The schedule epoch is `2024-01-01 00:00:00` local time. All offsets are computed from that. Consistent across restarts and container rebuilds.

**Cache re-probing everything after Docker setup**
- The SQLite cache is keyed by file path. If the in-container path (`/shows/...`) differs from the original path (`/mnt/nas/Shows/...`), all files are re-probed once. This is normal on first Docker run.
- Mount `~/.fakeiptv` as the cache volume to persist across rebuilds.

**Docker build not picking up code changes**
- If `docker compose up -d --build` uses cached layers, `touch` the changed `.py` files first, or run `docker compose build --no-cache && docker compose up -d --force-recreate`.
