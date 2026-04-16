# FakeIPTV

Turns a NAS full of TV shows and movies into fake live IPTV channels.
Generates a proper HLS live stream (sliding-window M3U8 + MPEG-TS segments), XMLTV EPG, and catch-up TV — no transcoding, just fast remux via `ffmpeg -c copy`.

## How it works

- The NAS is scanned for shows and movies at startup and once a day at midnight (local time).
- Channels are auto-discovered from your library — no per-show channels, only curated mixes:
  - **Primetime** and **Mix 1–5** — all shows interleaved (six variants with distinct shuffles)
  - **{Genre}** — per-genre mix (e.g. Drama, Comedy) if ≥ 3 shows share the genre
  - **Goldies** — shows older than a configurable year threshold
  - **Hits** — shows with a rating above a configurable threshold (from Sonarr/Radarr/TMDB/NFO)
  - **{Genre} Movies**, **Movie Hits**, **Movies** — movie channels, genre-exclusive where possible
- Every channel's schedule is **deterministic** — anchored to a fixed epoch (`2024-01-01 00:00:00` local). Restarting the server picks up exactly where it would have been.
- One `ffmpeg -re -c copy` process per channel outputs 2-second HLS segments to a tmpfs directory.
- A **bumper loading screen** plays while a channel's ffmpeg is warming up, so switching channels feels instant instead of showing a spinner.
- Flask serves the segments, XMLTV EPG, and catch-up manifests directly from tmpfs.

---

## Deployment — Docker (recommended)

### Requirements

- Docker + Docker Compose on the host
- NAS accessible over NFS or SMB from the host

### Setup

```bash
# 1. Clone the repo
git clone <repo> /opt/fakeiptv
cd /opt/fakeiptv

# 2. Configure
cp .env.example .env
nano .env          # set FAKEIPTV_HOST_IP, paths, API keys

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

> **Windows / Docker Desktop note:** The container mounts the NAS as a Docker named NFS volume. In-container `mount.cifs` fails on Docker Desktop (errno 95); NFS works natively via the Linux VM kernel.

---

## Configuration

All settings live in **`.env`** (preferred) or **`config.yaml`**. Environment variables always take precedence.

| Variable | Default | Description |
|---|---|---|
| `FAKEIPTV_HOST_IP` | `127.0.0.1` | **LAN IP of this machine** — used in all stream/EPG URLs |
| `FAKEIPTV_PORT` | `8080` | HTTP port |
| `FAKEIPTV_SHOWS_PATH` | `/mnt/nas/Shows` | Path to TV shows (inside container) |
| `FAKEIPTV_MOVIES_PATH` | `/mnt/nas/Movies` | Path to movies (inside container) |
| `FAKEIPTV_TMP_DIR` | `/tmp/fakeiptv` | Where HLS segments are written (use tmpfs) |
| `FAKEIPTV_CACHE_DIR` | `~/.fakeiptv/` | SQLite cache for durations + TMDB metadata |
| `FAKEIPTV_SUBTITLES` | `true` | Include subtitle tracks (SRT → WebVTT) |
| `FAKEIPTV_AUDIO_COPY` | `true` | Copy audio codec; `false` = transcode to AAC 192k stereo |
| `FAKEIPTV_PREFERRED_AUDIO_LANGUAGE` | `eng` | ISO 639-1 or 639-2 code for preferred audio track |
| `FAKEIPTV_CATCHUP_DAYS` | `7` | Days of past programming available for catch-up |
| `FAKEIPTV_PREWARM` | `false` | Start all channels on first client connect |
| `FAKEIPTV_PREWARM_SESSION` | `false` | Keep all channels alive together (session mode) |
| `FAKEIPTV_PREWARM_ADJACENT` | `0` | Also warm N channels on each side of the watched channel |
| `FAKEIPTV_PREWARM_TIMEOUT` | `120` | Seconds before a prewarm-only channel is stopped |
| `FAKEIPTV_READY_SEGMENTS` | `1` | HLS segments buffered before channel is declared ready |
| `FAKEIPTV_BUMPERS_PATH` | `/app/bumpers` | Directory of bumper video files (baked into the image); set to empty to disable |
| `FAKEIPTV_TMDB_API_KEY` | _(empty)_ | Optional — TMDB metadata fallback |
| `FAKEIPTV_SONARR_URL` | _(empty)_ | Optional — Sonarr integration for ratings/metadata |
| `FAKEIPTV_SONARR_API_KEY` | _(empty)_ | |
| `FAKEIPTV_RADARR_URL` | _(empty)_ | Optional — Radarr integration for ratings/metadata |
| `FAKEIPTV_RADARR_API_KEY` | _(empty)_ | |
| `FAKEIPTV_TMPFS_SIZE` | `1073741824` | tmpfs size in bytes for HLS segments (Docker only) |
| `TZ` | `UTC` | Timezone for schedule and midnight refresh (e.g. `Asia/Jerusalem`) |

### Bumper loading screen

Drop any number of video files (`.mp4`, `.mkv`, `.mov`, `.avi`, `.webm`) into the `bumpers/` directory at the repo root before building. They are baked into the Docker image at `/app/bumpers`. When a channel starts cold, a randomly-chosen bumper loops until the real stream has buffered enough segments to play, then the player switches seamlessly.

To disable bumpers entirely, set `FAKEIPTV_BUMPERS_PATH=` (empty) in `.env`.

### Expected NAS layout

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

External subtitles: `<video_basename>.<lang>.srt` or `<video_basename>.srt` alongside the video file.

### Disabling or renaming channels (`config.yaml`)

```yaml
channels:
  goldies_before: 2010     # shows older than this year go into "Goldies"
  hits_rating: 8.0         # minimum rating (0–10) for "Hits"
  disabled:
    - goldies
  rename:
    primetime: Prime Time
```

---

## Adding to Televizo

1. **Add Playlist** → `http://<ip>:<port>/playlist.m3u8`
2. The EPG URL is embedded in the playlist automatically via `url-tvg=` — Televizo picks it up on import
3. Select any channel — it starts mid-show, just like real TV
4. **Catch-up**: open the EPG guide, select a past programme, press play

---

## EPG

- Available at `/epg.xml` (plain) and `/epg.xml.gz` (gzip — what Televizo fetches)
- Covers `catchup_days` days back + 1 day forward
- Timestamps are always UTC (`+0000`) — required for Televizo's catch-up timestamp substitution
- Regenerates automatically every hour

---

## Catch-up

Catch-up uses `catchup="shift"` mode (required for Televizo — `catchup="default"` does not work).

When a past programme is selected in Televizo's EPG:
1. Televizo calls the live stream URL with `?utc=TIMESTAMP` appended
2. The server detects the timestamp and creates a temporary VOD ffmpeg session
3. The session serves a finite HLS playlist (`-hls_list_size 0`) starting at the requested offset
4. Sessions are reused for 60 seconds to handle Televizo's polling behaviour
5. Sessions expire after 2 hours of inactivity

---

## API endpoints

| Endpoint | Description |
|---|---|
| `GET /playlist.m3u8` | IPTV channel list |
| `GET /epg.xml` | XMLTV EPG (plain XML) |
| `GET /epg.xml.gz` | XMLTV EPG (gzip) |
| `GET /hls/<channel_id>/stream.m3u8` | Live HLS master manifest (also catch-up entry point with `?utc=`) |
| `GET /hls/<channel_id>/video.m3u8` | Live HLS video-only manifest (serves bumper content while channel warms up) |
| `GET /hls/<channel_id>/sub_<lang>.m3u8` | Live HLS subtitle manifest |
| `GET /hls/<channel_id>/<seg>.ts` | HLS MPEG-TS segment |
| `GET /hls/_loading/<bumper_id>/<seg>.ts` | Bumper loading screen segment |
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
- The monitor thread detects "bitmap to bitmap" in ffmpeg stderr and automatically disables subtitles for that channel. Self-heals after one restart.

**eac3 / unspecified sample rate**
- Some MKV files lose eac3 codec parameters when muxed into MPEG-TS with `-c copy`. The monitor detects this and automatically falls back to `-c:a aac -b:a 192k` for that channel.

**EPG not showing in Televizo**
- Re-import the playlist in Televizo (it caches the playlist and EPG URL).
- The EPG URL is auto-embedded via `url-tvg=` in the playlist header.

**Catch-up not working**
- EPG must be loaded first — Televizo needs programme times to trigger catch-up.
- Check that `catchup="shift"` is in use (not `"default"`).
- Verify EPG timestamps are UTC — any local offset breaks Televizo's timestamp parsing.

**Channel shows bumper forever / never switches to real content**
- Check `docker logs fakeiptv` for ffmpeg errors on the channel.
- If bumpers were updated, rebuild the image — they are baked in, not mounted.
- If `FAKEIPTV_BUMPERS_PATH` points to an empty or missing directory, bumpers are disabled and channels fall back to blocking startup.

**Wrong episode playing / schedule seems off**
- The schedule epoch is `2024-01-01 00:00:00` local time. Consistent across restarts and rebuilds.

**All files re-probed after first Docker run**
- Normal on first run: the SQLite cache is keyed by file path. If the in-container mount path differs from the original cache path, everything re-probes once.

**Docker build not picking up code changes**
- `touch` the changed `.py` files before rebuilding, or run:
  ```bash
  docker compose build --no-cache && docker compose up -d --force-recreate
  ```
