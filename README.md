# FakeIPTV

Turns a NAS full of TV shows and movies into fake live IPTV channels. Generates proper HLS live streams (sliding-window M3U8 + MPEG-TS segments), a XMLTV EPG, subtitle tracks, and catch-up TV — all without transcoding. Just fast remux via `ffmpeg -c copy`.

Designed for **Televizo** on a local network, deployed via Docker on Windows 11 with Docker Desktop.

---

## Features

- **Auto-channel discovery** — no manual per-show configuration:
  - **Primetime** and **Mix 1–5** — all shows interleaved (six variants with distinct shuffles)
  - **{Genre}** — per-genre mix (Drama, Comedy, etc.) if ≥ 3 shows share the genre
  - **Goldies** — shows older than a configurable year (default: pre-2010)
  - **Hits** — shows with a rating above a configurable threshold
  - **{Genre} Movies**, **Movie Hits**, **Movies** — movie channels, genre-exclusive where possible
- **Deterministic schedule** — anchored to a fixed epoch (`2024-01-01 00:00:00` local time). Restarting picks up exactly where it would have been with no state stored.
- **No transcoding** — `ffmpeg -re -c:v copy` remuxes to MPEG-TS. 2-second HLS segments, 15-segment sliding window. Audio optionally copied or transcoded to AAC 192k.
- **Bumper loading screen** — a video loops while a channel's ffmpeg warms up. Switching channels feels instant instead of showing a spinner. Bumper is suppressed when scrubbing catch-up (no mid-scrub flash).
- **Catch-up TV** — select any past programme in Televizo's EPG guide and it plays from the beginning. Uses `catchup="shift"` mode with 60-second session reuse and 2-hour expiry.
- **XMLTV EPG** — UTC timestamps (required for Televizo), auto-embedded in the playlist via `url-tvg=`, regenerated hourly. Covers `catchup_days` days back + 1 day forward.
- **Subtitles** — external `.srt` files (any language) converted to WebVTT with correct MPEG-TS timestamp anchoring. External SRT takes priority over embedded tracks. Hebrew RTL bidi supported. Bitmap subtitles (PGS/VOBSUB) auto-detected and skipped.
- **HDR handling** — `hevc_metadata` bitstream filter strips HDR colour metadata on all-HDR HEVC channels so SDR players don't show a green screen.
- **Audio language selection** — picks the preferred audio track by ISO 639 language code. Automatic fallback to AAC 192k stereo if eac3/DTS loses parameters during remux.
- **NAS disk pre-warming** — reads header, tail, and estimated seek cluster of each MKV before ffmpeg touches it, putting pages in the NAS RAM cache to avoid cold-seek stalls.
- **Metadata integration** — ratings, genres, and posters from NFO sidecars, Sonarr, Radarr, or TMDB (cascade in that order).
- **SQLite duration cache** — probes each file once; keyed by path + mtime. Survives restarts.
- **Daily refresh** — library rescan at midnight local time. Also available at `GET /refresh`.
- **`/status` endpoint** — JSON with all channels, now-playing, ready state, and uptime.

---

## Requirements

- **Docker** and **Docker Compose** (Docker Desktop on Windows, or Docker Engine on Linux)
- **NAS accessible via NFS** from the Docker host
  - On Docker Desktop for Windows, use a named NFS volume (see `docker-compose.yml`). In-container `mount.cifs` fails with errno 95; NFS works natively via the Linux VM kernel.
- No other host dependencies — ffmpeg, Python, and all Python packages run inside the container.

---

## Installation

### 1. Clone the repo

```bash
git clone <repo> /opt/fakeiptv
cd /opt/fakeiptv
```

### 2. Configure

```bash
cp .env.example .env
```

Edit `.env` — at minimum set:

```
FAKEIPTV_HOST_IP=192.168.1.100   # LAN IP of this machine
TZ=Asia/Jerusalem                 # your local timezone
```

### 3. Configure the NAS volume

Edit `docker-compose.yml` — find the `nas_multimedia` volume at the bottom and update:

```yaml
volumes:
  nas_multimedia:
    driver: local
    driver_opts:
      type: nfs
      o: "addr=192.168.1.200,hard,rsize=1048576,wsize=1048576,nfsvers=3,proto=tcp,ro"
      device: ":/share/Multimedia"
```

Also update `FAKEIPTV_SHOWS_PATH` and `FAKEIPTV_MOVIES_PATH` in the `environment:` block to match where shows and movies sit under the NFS share's mount point inside the container (`/multimedia` by default).

### 4. Add bumpers (optional)

Drop any video files (`.mp4`, `.mkv`, `.mov`, `.avi`) into `bumpers/` at the repo root. They are baked into the Docker image at build time. To disable bumpers entirely, set `FAKEIPTV_BUMPERS_PATH=` (empty) in `.env`.

### 5. Build and start

```bash
docker compose up -d --build
docker logs -f fakeiptv
```

Wait for the log line `Startup scan complete — N shows, M movies`. First run probes every file for duration and may take a few minutes depending on library size.

### 6. Add to Televizo

1. **Add Playlist** → `http://<host_ip>:<port>/playlist.m3u8`
2. The EPG URL is embedded automatically via `url-tvg=` — Televizo picks it up on import.
3. Select a channel — it starts mid-show, just like real TV.
4. **Catch-up**: open the EPG guide, select a past programme, press play.

---

## Updating

```bash
git pull && docker compose up -d --build
```

No `down` needed — Compose stops, rebuilds, and restarts in one step.

---

## NAS layout

```
Shows/
└── Breaking Bad/
    └── Season 01/
        ├── Breaking.Bad.S01E01.mkv
        ├── Breaking.Bad.S01E01.nfo       ← optional Kodi/Jellyfin sidecar
        ├── Breaking.Bad.S01E01.en.srt    ← external English subtitles
        └── Breaking.Bad.S01E01.he.srt    ← external Hebrew subtitles

Movies/
└── Inception (2010)/
    ├── Inception.mkv
    ├── Inception.nfo
    └── Inception.he.srt
```

Subtitle sidecar naming: `<video_basename>.<lang>.srt` or `<video_basename>.srt` (language defaults to `und`).

---

## Configuration

All settings live in **`.env`** (preferred) or **`config.yaml`**. Environment variables always take precedence.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `FAKEIPTV_HOST_IP` | `127.0.0.1` | **LAN IP of this machine** — used in all stream/EPG URLs. Critical. |
| `FAKEIPTV_PORT` | `8080` | HTTP port |
| `FAKEIPTV_SHOWS_PATH` | `/mnt/nas/Shows` | Path to TV shows inside the container |
| `FAKEIPTV_MOVIES_PATH` | `/mnt/nas/Movies` | Path to movies inside the container |
| `FAKEIPTV_TMP_DIR` | `/tmp/fakeiptv` | HLS segment directory (use tmpfs — see docker-compose.yml) |
| `FAKEIPTV_CACHE_DIR` | `~/.fakeiptv/` | SQLite cache for durations and TMDB metadata |
| `FAKEIPTV_SUBTITLES` | `true` | Enable subtitle tracks (SRT → WebVTT) |
| `FAKEIPTV_AUDIO_COPY` | `true` | Copy audio codec; `false` = always transcode to AAC 192k stereo |
| `FAKEIPTV_PREFERRED_AUDIO_LANGUAGE` | `eng` | ISO 639-1 or 639-2 code for preferred audio track |
| `FAKEIPTV_CATCHUP_DAYS` | `7` | Days of past programming available for catch-up |
| `FAKEIPTV_PREWARM` | `false` | Start all channels when the first client connects |
| `FAKEIPTV_PREWARM_SESSION` | `false` | Keep all channels alive together; stop all after idle timeout |
| `FAKEIPTV_PREWARM_ADJACENT` | `0` | Also warm N channels above and below the watched channel |
| `FAKEIPTV_PREWARM_TIMEOUT` | `120` | Seconds before a prewarm-only channel is stopped |
| `FAKEIPTV_READY_SEGMENTS` | `1` | HLS segments buffered before channel is declared ready |
| `FAKEIPTV_BUMPERS_PATH` | `/app/bumpers` | Directory of bumper video files (baked into image); set to empty to disable |
| `FAKEIPTV_TMDB_API_KEY` | _(empty)_ | Optional TMDB metadata fallback |
| `FAKEIPTV_SONARR_URL` | _(empty)_ | Optional Sonarr endpoint for ratings/metadata |
| `FAKEIPTV_SONARR_API_KEY` | _(empty)_ | |
| `FAKEIPTV_RADARR_URL` | _(empty)_ | Optional Radarr endpoint for ratings/metadata |
| `FAKEIPTV_RADARR_API_KEY` | _(empty)_ | |
| `FAKEIPTV_TMPFS_SIZE` | `1073741824` | tmpfs size in bytes for HLS segments (1 GB default; ~25 MB per active channel) |
| `TZ` | `UTC` | Timezone for schedule epoch and midnight refresh (e.g. `Asia/Jerusalem`) |

### Pre-warm modes

Only one pre-warm mode should be active at a time:

| Mode | When to use |
|---|---|
| `FAKEIPTV_PREWARM_ADJACENT=1` | Warm 1 channel above and below the watched one. Best for typical zapping. |
| `FAKEIPTV_PREWARM_SESSION=true` | Keep all channels alive as long as anyone is watching. High RAM / tmpfs usage. |
| `FAKEIPTV_PREWARM=true` | Start all channels on first connect, each with its own idle timeout. |

### config.yaml — channel overrides

```yaml
channels:
  goldies_before: 2010     # shows older than this year go into Goldies
  hits_rating: 8.0         # minimum rating (0–10) for Hits channel
  disabled:
    - goldies
    - mix-3
  rename:
    primetime: Prime Time
    hits: Top Rated
```

Channel IDs are slugified names (lowercase, spaces→hyphens). Check `/status` to see all channel IDs.

---

## Bumper loading screen

Drop video files (`.mp4`, `.mkv`, etc.) into `bumpers/` before building the image. When a channel starts cold, a random bumper loops until the real stream has buffered enough segments, then the player switches seamlessly via `#EXT-X-DISCONTINUITY`.

To update bumpers, edit the `bumpers/` directory and rebuild:
```bash
docker compose up -d --build
```

To disable bumpers: `FAKEIPTV_BUMPERS_PATH=` (empty string) in `.env`.

---

## API endpoints

| Endpoint | Description |
|---|---|
| `GET /playlist.m3u8` | IPTV channel list with catch-up metadata |
| `GET /epg.xml` | XMLTV EPG (plain XML) |
| `GET /epg.xml.gz` | XMLTV EPG (gzip — what Televizo fetches) |
| `GET /hls/<channel_id>/stream.m3u8` | Live HLS master manifest; also catch-up entry with `?utc=` |
| `GET /hls/<channel_id>/video.m3u8` | Live HLS video manifest (serves bumper while warming) |
| `GET /hls/<channel_id>/sub_<lang>.m3u8` | Live HLS subtitle manifest |
| `GET /hls/<channel_id>/<seg>.ts` | HLS MPEG-TS segment |
| `GET /hls/_loading/<bumper_id>/<seg>` | Bumper loading screen segments |
| `GET /catchup/<channel_id>/<session_id>/stream.m3u8` | Catch-up VOD manifest |
| `GET /catchup/<channel_id>/<session_id>/<seg>.ts` | Catch-up VOD segment |
| `GET /refresh` | Trigger immediate library rescan |
| `GET /status` | JSON: channels, now-playing, ready states, uptime |

---

## Troubleshooting

**Channel shows "loading" forever**
- Check `docker logs fakeiptv | grep -i error` for ffmpeg errors.
- Verify the NAS is mounted and paths in `.env` match what's inside the container.
- Check `/status` — `"ready": false` means ffmpeg hasn't produced its first segment yet.

**Bitmap subtitle crash loop (PGS/VOBSUB)**
- The monitor thread detects "bitmap to bitmap" in ffmpeg stderr and automatically disables subtitles for that channel. Self-heals after one restart cycle.

**eac3 / "unspecified sample rate"**
- Some MKV files lose eac3 codec parameters when muxed into MPEG-TS with `-c copy`. The monitor detects this and automatically falls back to AAC 192k for that channel. Self-heals after one restart.

**Subtitles not appearing**
- Subtitle VTT files are written asynchronously ~2s after channel start. If they disappear after switching episodes, that's expected — the channel restarts and re-writes them.
- Check that the `.srt` file is alongside the video and named correctly (see NAS layout above).

**EPG not showing in Televizo**
- Re-import the playlist in Televizo (the EPG URL is embedded via `url-tvg=` and cached on import).
- The EPG URL is `http://<host_ip>:<port>/epg.xml.gz`.

**Catch-up not working**
- EPG must be loaded in Televizo first — it needs programme times to trigger catch-up.
- Verify EPG timestamps are UTC — any local offset breaks Televizo's timestamp parsing.
- Only `catchup="shift"` mode works in Televizo; `catchup="default"` does not substitute timestamps.

**Channel shows bumper forever / never switches to real content**
- Check `docker logs fakeiptv | grep -i "channel_id"` for ffmpeg errors on the specific channel.
- If bumpers were updated, rebuild the image — they are baked in at build time, not mounted.
- If `FAKEIPTV_BUMPERS_PATH` points to an empty or missing directory, bumpers are disabled.

**Green or washed-out video on an HDR channel**
- The `hevc_metadata` BSF strips HDR colour metadata for all-HDR HEVC channels. If the channel has mixed HDR/SDR content, the BSF is not applied (would crash ffmpeg on H.264 segments).

**Wrong episode playing / schedule seems off**
- The schedule is deterministic: `EPOCH = 2024-01-01 00:00:00` local time. It's consistent across restarts. If `TZ` is wrong, the epoch shifts — set `TZ` correctly in `.env`.

**All files re-probed after a Docker rebuild or path change**
- Normal: the SQLite cache is keyed by the full path inside the container. If the mount path changes, everything re-probes once and the new paths are cached.

**Docker build not picking up code changes**
- `touch` the changed `.py` files before rebuilding, or force a full rebuild:
  ```bash
  docker compose build --no-cache && docker compose up -d --force-recreate
  ```

**Useful commands**

```bash
docker logs fakeiptv | grep -i error     # errors only
docker logs -f fakeiptv                  # follow live
docker exec -it fakeiptv bash            # shell into container
curl http://localhost:8080/status        # channel status JSON
curl http://localhost:8080/refresh       # trigger library rescan
```
