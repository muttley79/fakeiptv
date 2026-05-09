# FakeIPTV

Turns a NAS full of TV shows and movies into fake live IPTV channels with HLS streams, XMLTV EPG, and catch-up TV. Everything is **deterministic** (same content at the same wall-clock time across restarts), **no transcoding** (just fast ffmpeg remux), and designed for **Televizo** on a local LAN.

Deployed via Docker on Windows 11 with Docker Desktop (or Linux Docker Engine).

---

## Features

- **Auto-discovered channels** — no manual per-show configuration:
  - **Primetime** — all shows shuffled round-robin (seed 0)
  - **Mix 1–5** — same show pool, different shuffle seeds so simultaneous viewers see different content
  - **{Genre}** — per-genre mix (Drama, Comedy, etc.) if ≥ 3 shows share the primary genre and no show dominates
  - **Goldies** — shows older than a configurable year (default: pre-2010)
  - **Hits** — shows with rating ≥ configurable threshold (default: 8.0)
  - **{Genre} Movies** — per-genre movie channel (exclusive: each movie in one genre only)
  - **Movie Hits** — high-rated movies (non-exclusive)
  - **Movies** — all remaining movies

- **Deterministic schedule** — anchored to `2024-01-01 00:00:00` local time. Restarting picks up exactly where it would have been with no persistent state.

- **Fast HLS remux** — `ffmpeg -c:v copy` (no transcoding). 4-second segments, 15-segment sliding window. Audio transcoded to AAC 192k stereo (handles DTS, EAC3, TrueHD transparently). Optional audio codec copy if compatible.

- **Bumper loading screen** — a video loops while a channel's ffmpeg warms up. Switching feels instant (no spinner). Bumper transcoded once and cached; suppressed when scrubbing catch-up (no mid-scrub flash). Seamless transition via `#EXT-X-DISCONTINUITY`.

- **Catch-up TV** — select any past programme in Televizo's EPG and play from the beginning. Sessions are reused within 60 seconds (same timestamp), expire after 2 hours. Subtitles follow from live.

- **XMLTV EPG** — UTC timestamps (required for Televizo), embedded in playlist via `url-tvg=`, regenerated every hour. Covers `catchup_days` history + 1 day forward.

- **Subtitles** — external `.srt` files (any language) converted to WebVTT with correct MPEG-TS timestamp anchoring. External SRT takes priority over embedded tracks. Hebrew RTL BiDi supported (RLM + Unicode isolates, not the old pre-inversion hack). Bitmap subtitles (PGS/VOBSUB) auto-detected and skipped (one crash cycle, then stable).

- **HDR support** — `hevc_metadata` BSF strips HDR colour metadata on all-HDR HEVC channels (prevents green screen on SDR players). Automatically disabled for mixed or H.264 content (would crash ffmpeg).

- **Audio language selection** — picks the preferred audio track by ISO 639-1/2 code. Auto-fallback to AAC 192k stereo if eac3/DTS loses parameters during remux.

- **NAS disk pre-warming** — reads header (64 KB) + tail (512 KB) + estimated seek cluster before ffmpeg touches the file, putting pages in NAS RAM cache to avoid 2–10s cold-seek stalls. Three-tier strategy: first pass before probes, second pass before ffmpeg, concat lookahead (60s before episode transition), and global 10-minute sweep.

- **Fast keyframe indexing** — MKV Cues-based probing (EBML parser) for instant subtitle snap-to-keyframe seeks. Fallback to ffprobe full-packet scan for files without Cues. Max 3 concurrent probes via semaphore.

- **Metadata cascade** — Ratings, genres, and posters from: NFO sidecars (Kodi/Jellyfin XML) → Sonarr/Radarr API → TMDB API. Each source fills gaps left by the previous one.

- **SQLite caches** — Duration probes (path + mtime keyed), TMDB responses, keyframe indices, SRT content. All survive restarts.

- **Startup library cache** — Serialized `library.json` cached for 24h (configurable). Invalidated by config hash change, NAS top-level folder change, or explicit `force=True` refresh. Fast startup: skips 25–30s NAS scan on cache hit.

- **Idle channel reaping** — Unwatched channels stop after 120s, watched channels after 600s. Configurable via prewarm modes (`FAKEIPTV_PREWARM`, `FAKEIPTV_ALWAYS_ON`, etc.).

- **Daily midnight refresh** — library rescan at local midnight. Also available on-demand at `/refresh`.

- **Status endpoint** — `GET /status` returns JSON: all channels (id, name, entries, total_duration, ready, now_playing with title/offset/path), library totals, uptime.

---

## Requirements

- **Docker + Docker Compose** (Docker Desktop on Windows, Docker Engine on Linux)
- **NAS accessible via NFS** from the Docker host
  - Windows Docker Desktop: use a named NFS volume (see `docker-compose.yml`). In-container CIFS fails with errno 95; NFS works natively via the embedded Linux kernel.
  - Linux: NFS or local bind-mount of the host's NAS mount point.
- **No other host dependencies** — ffmpeg, Python, and all packages run in the container.

---

## Quick Start

### 1. Clone

```bash
git clone <repo> /opt/fakeiptv
cd /opt/fakeiptv
```

### 2. Configure

```bash
cp .env.example .env
```

Edit `.env` — at minimum:

```
FAKEIPTV_HOST_IP=192.168.1.100   # LAN IP of this machine (critical)
TZ=Asia/Jerusalem                 # your local timezone (critical for schedule)
```

### 3. Configure NAS mount

Edit `docker-compose.yml` — update the `nas_multimedia` volume:

```yaml
volumes:
  nas_multimedia:
    driver: local
    driver_opts:
      type: nfs
      o: "addr=192.168.1.200,hard,rsize=1048576,wsize=1048576,nfsvers=3,proto=tcp,ro"
      device: ":/share/Multimedia"
```

Also update `FAKEIPTV_SHOWS_PATH` and `FAKEIPTV_MOVIES_PATH` in the `environment:` block (e.g., `/multimedia/TV Shows` and `/multimedia/Movies`).

### 4. Add bumpers (optional)

Drop video files (`.mp4`, `.mkv`, `.mov`, `.avi`, `.webm`) into `bumpers/` at the repo root. They are baked into the image at build time. To disable: set `FAKEIPTV_BUMPERS_PATH=` (empty) in `.env`.

### 5. Build and start

```bash
docker compose up -d --build
docker logs -f fakeiptv
```

Wait for: `Startup scan complete — N shows, M movies`. First run probes every file for duration (takes a few minutes depending on library size and NAS latency).

### 6. Add to Televizo

1. **Add Playlist** → `http://<host_ip>:<port>/playlist.m3u8`
2. The EPG URL (`/epg.xml.gz`) is embedded automatically via `url-tvg=`.
3. Select a channel — it starts mid-show, just like real TV.
4. **Catch-up**: open EPG, select a past programme, press play.

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

**Subtitle naming**: `{video_basename}.{lang}.srt` or `{video_basename}.srt` (no language tag defaults to `und` — unlabeled).

**Supported video formats**: `.mkv`, `.mp4`, `.avi`, `.m4v`, `.mov`.

**Supported subtitle formats**: SRT sidecar files (recommended), or embedded SRT/WebVTT/ASS tracks in the video file.

---

## Configuration reference

All settings via **`.env`** (preferred) or **`config.yaml`**. Env vars always override config file.

### Server and paths

| Var | Default | Notes |
|---|---|---|
| `FAKEIPTV_HOST_IP` | `127.0.0.1` | **LAN IP** — used in all stream/EPG URLs. **Critical.** |
| `FAKEIPTV_PORT` | `8080` | HTTP listen port |
| `FAKEIPTV_HOST` | `0.0.0.0` | HTTP listen address (all interfaces by default) |
| `FAKEIPTV_SHOWS_PATH` | `/mnt/nas/Shows` | Path to TV shows inside container |
| `FAKEIPTV_MOVIES_PATH` | `/mnt/nas/Movies` | Path to movies inside container |
| `FAKEIPTV_TMP_DIR` | `/tmp/fakeiptv` | HLS segment directory (should be tmpfs) |
| `FAKEIPTV_CACHE_DIR` | `~/.fakeiptv/` | SQLite + library.json cache dir |
| `FAKEIPTV_BUMPERS_PATH` | `/app/bumpers` | Bumper video files dir (baked in image) |
| `FAKEIPTV_ASSETS_DIR` | `/assets` | Channel logos dir |
| `TZ` | `UTC` | **Timezone** — used for EPOCH and midnight refresh. Set to your local timezone (e.g., `Asia/Jerusalem`). |

### Subtitles and audio

| Var | Default | Notes |
|---|---|---|
| `FAKEIPTV_SUBTITLES` | `true` | Enable subtitle tracks (SRT → WebVTT) |
| `FAKEIPTV_SUBTITLE_BACKGROUND` | `false` | `false` = semi-transparent CSS bg (ExoPlayer CSS rendering). `true` = solid black box (native rendering). |
| `FAKEIPTV_AUDIO_COPY` | `true` | `true` = try `-c:a copy` (falls back to AAC on eac3 error). `false` = always transcode to AAC 192k stereo. |
| `FAKEIPTV_PREFERRED_AUDIO_LANGUAGE` | `eng` | ISO 639-1 or 639-2 code (e.g., `eng`, `he`, `fra`). |

### Catch-up and EPG

| Var | Default | Notes |
|---|---|---|
| `FAKEIPTV_CATCHUP_DAYS` | `7` | Days of past programming available for catch-up. Set to `0` to disable. |

### Channel warming modes (pick one)

| Var | Default | Behavior |
|---|---|---|
| `FAKEIPTV_PREWARM` | `false` | Start all channels at boot (or on first manifest request if `false`). Each channel idles independently. |
| `FAKEIPTV_PREWARM_SESSION` | `false` | Keep all channels alive together; stop all after global idle timeout. |
| `FAKEIPTV_PREWARM_ADJACENT` | `0` | Warm N channels above/below the watched one. E.g., `2` = next 2 up, next 2 down. |
| `FAKEIPTV_ALWAYS_ON` | `"false"` | `"false"` (default) = on-demand start/stop. `"true"` = boot at startup, never stop. `"connected"` = session-mode global idle. |
| `FAKEIPTV_PREWARM_TIMEOUT` | `120` | Seconds before a prewarmed-only channel is stopped. |
| `FAKEIPTV_READY_SEGMENTS` | `1` | HLS segments buffered before channel is declared ready (manifest returned to player). |

### Metadata sources

| Var | Default | Notes |
|---|---|---|
| `FAKEIPTV_TMDB_API_KEY` | _(empty)_ | Optional TMDB API key for metadata fallback. |
| `FAKEIPTV_SONARR_URL` | _(empty)_ | Optional Sonarr URL (e.g., `http://192.168.1.50:8989`). |
| `FAKEIPTV_SONARR_API_KEY` | _(empty)_ | Sonarr API key. |
| `FAKEIPTV_RADARR_URL` | _(empty)_ | Optional Radarr URL. |
| `FAKEIPTV_RADARR_API_KEY` | _(empty)_ | Radarr API key. |

### Startup cache

| Var | Default | Notes |
|---|---|---|
| `FAKEIPTV_STARTUP_CACHE` | `true` | Use `library.json` startup cache. |
| `FAKEIPTV_STARTUP_CACHE_MAX_AGE_HOURS` | `24` | Cache TTL in hours. |

### Logging and sizing

| Var | Default | Notes |
|---|---|---|
| `FAKEIPTV_LOG_LEVEL` | `INFO` | Log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |
| `FAKEIPTV_LOG_FILE` | _(empty)_ | Optional log file path. |
| `FAKEIPTV_TMPFS_SIZE` | `1073741824` | tmpfs size for HLS segments (1 GB = 1073741824 bytes, ~25 MB per active channel). |

### Channel customization (config.yaml only)

```yaml
channels:
  goldies_before: 2010     # shows older than this year → Goldies channel
  hits_rating: 8.0         # minimum rating (0–10) for Hits channel
  disabled:
    - goldies              # disable specific channels
    - mix-3
  rename:
    primetime: Prime Time   # rename channel display name
    hits: Top Rated
```

Channel IDs are slugified (lowercase, spaces → hyphens). View all IDs in `/status` output.

---

## How it works

**Schedule**: Every channel has a deterministic schedule anchored to `EPOCH = 2024-01-01 00:00:00` local time. Position is computed as: `(now - EPOCH + channel_offset) % channel.total_duration`. Same content airs at the same wall-clock time across restarts. Offsets are staggered to prevent simultaneous identical content (Primetime and Mix 1–5 are 131 hours apart; other channels use MD5-based 0–7 day offsets).

**Streaming**: Each channel runs one ffmpeg process per host instance. ffmpeg reads a pre-built `concat.txt` covering ~4 hours ahead (fetched from the live schedule), remuxes to HLS segments (4s each, 15-segment window), and outputs to tmpfs. No transcoding.

**Bumpers**: When a channel starts cold, `BumperStreamer` picks a random bumper video and loops it as HLS with a fake live MEDIA-SEQUENCE (based on Unix timestamp). The real channel starts in a background thread. Once buffered, the player switches smoothly via `#EXT-X-DISCONTINUITY`. Bumpers are suppressed during catch-up scrubbing.

**Subtitles**: Two-phase async process:
1. Phase 1 (sync at startup): Parse SRT files for all future entries, build cue lists, write placeholder VTTs immediately (player not blocked).
2. Phase 2 (async thread): Wait for first `.ts` segment, probe its MPEG-TS start_pts, probe actual keyframe inpoint, rewrite VTTs with correct timestamp anchoring. Optionally watch ffmpeg SRT side-output (for languages without external SRT).

**Catch-up**: Sessions are created on-demand when a player requests a past timestamp. Sessions reuse within 60s (same timestamp), expire after 2h. Kept in session-specific directories with rolling 60-second segment retention (not persistent).

---

## API endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/playlist.m3u8` | GET | IPTV channel list with catch-up metadata. Also `/playlist_local.m3u8` (uses `localhost`). |
| `/epg.xml` | GET | XMLTV EPG (plain XML). Also `/epg.xml.gz` (gzip). |
| `/logos/{filename}` | GET | Static PNG channel logos from `FAKEIPTV_ASSETS_DIR`. |
| `/hls/{ch_id}/stream.m3u8` | GET | Live HLS master playlist. Query param `?utc=` redirects to catchup. |
| `/hls/{ch_id}/video.m3u8` | GET | Live HLS video manifest (serves bumper content while channel warms). |
| `/hls/{ch_id}/sub_{lang}.m3u8` | GET | Live HLS subtitle manifest for a language. |
| `/hls/{ch_id}/{segment}` | GET | HLS segment (`.ts` file, video/subtitle manifest, or bumper content). |
| `/catchup/{ch_id}` | GET | Catchup entry point (accepts `?utc=`, `?start=`, `?t=`, etc.). Redirects to session URL. |
| `/catchup/{ch_id}/{sid}/stream.m3u8` | GET | Catchup VOD master playlist. |
| `/catchup/{ch_id}/{sid}/{segment}` | GET | Catchup VOD segment or manifest. |
| `/refresh` | GET | Trigger library rescan (returns `{"status": "ok"}`). |
| `/status` | GET | JSON: channels, now_playing, ready states, uptime. |

---

## Troubleshooting

**Channel won't start / "loading" forever**
- Check `docker logs fakeiptv | grep -i error` for ffmpeg errors or missing file paths.
- Verify NAS is mounted: `docker exec fakeiptv ls /multimedia` (or your configured path).
- Check `/status` endpoint: `"ready": false` means ffmpeg hasn't produced its first segment yet (check logs for ffmpeg stall).
- For ffmpeg errors starting a specific channel, note the channel ID and search logs for that channel's PID.

**eac3 codec / "unspecified sample rate" crash loop**
- Some MKVs lose eac3 codec parameters when muxed into MPEG-TS via `-c:a copy`. The monitor detects this and automatically falls back to AAC 192k stereo. **Self-heals after one restart cycle.**
- Set `FAKEIPTV_AUDIO_COPY=false` to skip the copy attempt (always transcode, safer but slower).

**Bitmap subtitle crash loop (PGS/VOBSUB)**
- Monitor detects "bitmap to bitmap" in ffmpeg stderr and auto-disables subtitles for that channel. **Self-heals after one restart cycle.**
- No manual action needed — the channel will restart and work without embedded subtitles.

**Subtitles not appearing**
- VTT files are written asynchronously ~2s after channel start. If they disappear after an episode switch, that's expected — the channel restarts and rewrites them.
- Check that the `.srt` file is alongside the video and named correctly: `{basename}.{lang}.srt` or `{basename}.srt`.
- If you renamed a file, the old cache entry might be stale. Delete the `srt_content` table in `~/.fakeiptv/cache.db` or use `/refresh` to force a full scan.

**EPG not showing in Televizo**
- Re-import the playlist in Televizo — the EPG URL is embedded via `url-tvg=` and cached on first import.
- The EPG URL is `http://{FAKEIPTV_HOST_IP}:{FAKEIPTV_PORT}/epg.xml.gz`.

**Catch-up not working**
- EPG must be loaded in Televizo first (needs programme times to trigger catch-up).
- Verify EPG timestamps are UTC — any local offset breaks Televizo's parsing. Check `/epg.xml.gz` for ` +0000` at the end of timestamps.
- Only `catchup="shift"` mode works in Televizo. `catchup="default"` does not substitute placeholders.

**Channel shows bumper forever / never transitions to real content**
- Check `docker logs fakeiptv | grep -E "(ffmpeg|channel_id)"` for errors on that specific channel.
- If bumpers were updated, rebuild the image (they are baked in at build time, not mounted).
- If `FAKEIPTV_BUMPERS_PATH` is empty or points to a missing dir, bumpers are disabled (set `FAKEIPTV_READY_SEGMENTS=1` and check if manifest appears immediately).

**Wrong episode playing / schedule off by hours**
- The schedule is deterministic: `EPOCH = 2024-01-01 00:00:00` local time. If `TZ` is wrong, the epoch shifts by hours (UTC shift). **Set `TZ` correctly** in `.env` (e.g., `Asia/Jerusalem`, not `UTC`).
- Restart the container after changing `TZ`.

**Duration cache stale (re-probing all files)**
- Cache key is `{path}|{mtime}`. If a file is replaced in-place without mtime change, the old duration is cached. Solution: delete the `durations` table in SQLite or run `/refresh?force=true` (non-standard endpoint).
- Container path change (e.g., `/mnt/nas` → `/multimedia`): re-probes once on first run (new paths cached). Normal.

**Docker build doesn't pick up code changes**
- Force rebuild: `docker compose build --no-cache && docker compose up -d --force-recreate`.
- Or: `touch` the changed files before `docker compose up -d --build`.

**Useful commands**

```bash
docker logs fakeiptv                         # view logs
docker logs -f fakeiptv                      # follow live
docker logs fakeiptv | grep -i error         # errors only
docker logs fakeiptv | grep -i hdmi          # debug HDMI/audio issues
docker logs fakeiptv | grep -i "channel_id"  # specific channel errors
docker exec -it fakeiptv bash                # shell into container
curl http://localhost:8080/status            # channel status JSON
curl http://localhost:8080/refresh           # trigger library rescan
sqlite3 ~/.fakeiptv/cache.db                 # inspect SQLite cache
```

---

## Updating

```bash
git pull
docker compose up -d --build
```

No `down` needed — Compose stops, rebuilds, and restarts automatically. Persistent cache (SQLite + library.json) survives restarts.

---

## Development

See `CLAUDE.md` for detailed architecture, design decisions, and ffmpeg command skeletons.
