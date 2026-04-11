# FakeIPTV — Claude context

## What this project is

A Python service that reads a NAS media library (SMB-mounted) and exposes it as fake live IPTV channels. Each channel produces a proper HLS live stream (sliding-window `.m3u8` + MPEG-TS segments) and a XMLTV EPG.

Primary consumer: **Televizo** IPTV player on LAN.
Deployed as: **Docker container** (via `docker compose`), running on a Raspberry Pi.

## Deployment

```
docker compose up -d          # build image + start container
docker compose up -d --build  # rebuild after code changes (no down needed)
docker logs fakeiptv          # view logs
```

All ffmpeg processes run inside the container — not visible on the host `ps`. HLS segments live in a tmpfs volume (`/tmp/fakeiptv` inside the container). The SQLite cache is in a named or host-mounted volume (`/cache`).

## Key design decisions

- **No transcoding by default.** `ffmpeg -re -c copy` remuxes to MPEG-TS only. Extremely light on CPU.
- **Automatic audio fallback.** If ffmpeg reports "unspecified sample rate" (eac3 in some MKVs), the monitor thread sets `_audio_copy = False` and restarts with `-c:a aac -b:a 192k` for that channel only.
- **Bitmap subtitle auto-disable.** PGS/VOBSUB subs can't convert to WebVTT. Monitor detects "bitmap to bitmap" in stderr and disables the subtitle map for that channel, then restarts cleanly.
- **Deterministic schedule.** Anchored to `EPOCH = datetime(2024, 1, 1, 0, 0, 0)` (local time). No state stored — position always computed from `(now - EPOCH + channel_offset) % total_duration`. Consistent across restarts and container rebuilds.
- **Per-channel epoch offset.** Each channel gets a deterministic MD5-derived offset (0–7 days) so channels sharing the same show don't air the same episode simultaneously.
- **One ffmpeg process per channel.** Each writes HLS segments to `{tmp_dir}/ch_{id}/`. A monitor thread restarts ffmpeg automatically when the concat window (~4h) is exhausted or on crash.
- **2-second HLS segments, list size 15** (~30s sliding window). Smaller segments reduce cold-start wait vs. the previous 5s default.
- **Stale segment cleanup on restart.** `_launch()` deletes all `.ts/.m3u8/.vtt` files before starting a new ffmpeg process to prevent timestamp discontinuity artifacts.
- **Timestamp normalization.** `-fflags +genpts -avoid_negative_ts make_zero` ensure monotonic PTS from t=0, fixing "3s replay" artifacts from `inpoint`-based seeks.
- **Pre-warm all channels at startup.** `prewarm_channels()` is called at boot and on each `/playlist.m3u8` fetch. Starts all channels staggered 0.3s apart so the Pi isn't spiked. Channels are ready before the user selects one.
- **Idle timeout: 10 minutes.** Channels with no client requests for 600s are stopped by the reaper. Pre-warmed channels stay alive long enough for the user to browse and select.
- **Lazy ffmpeg startup** (for channels not yet started). `ensure_started()` is called on manifest request; waits up to 15s for the first segment.
- **Concat demuxer + inpoint.** Seeking into the current file at startup uses the `ffconcat` `inpoint` directive, not `-ss` (which doesn't work with concat).
- **Unquoted paths with backslash escaping** in ffconcat files. Single-quoted strings can't escape `'`; double-quoted strings aren't supported by all ffmpeg builds. `re.sub(r"([ \t'\"])", r"\\\1", path)` is the reliable approach.
- **Flask serves segments directly** from tmpfs via `send_from_directory`. No proxying.
- **EPG covers 7 days back + 1 day forward.** Local timezone offset computed at runtime via `time.timezone`/`time.altzone`. Regenerates hourly.
- **Catch-up**: Deterministic schedule means past programmes can always be reconstructed. `GET /catchup/{channel_id}?utc={ts}` spins up a temporary VOD ffmpeg process (no `-re`), outputs HLS with `#EXT-X-ENDLIST`. Sessions expire after 2h of inactivity.
- **Daily refresh at midnight (local device time).** Uses `threading.Timer`. Also callable via `GET /refresh`.
- **`start_new_session=True` + `stdin=DEVNULL`** on all Popen calls — isolates ffmpeg from the terminal/container process group so SIGINT doesn't propagate and doesn't corrupt the TTY.

## Module map

| File | Role |
|---|---|
| `fakeiptv/config.py` | Loads config.yaml + env vars. `AppConfig` dataclass. |
| `fakeiptv/scanner.py` | Walks NAS, parses NFO XML, probes duration via ffprobe, fetches TMDB/Sonarr/Radarr. Returns `MediaLibrary`. |
| `fakeiptv/arrclient.py` | Sonarr/Radarr API client for ratings and metadata. |
| `fakeiptv/scheduler.py` | Builds `Channel` objects, calculates `NowPlaying`, generates EPG window. |
| `fakeiptv/streamer.py` | `ChannelStreamer` (one ffmpeg process) + `StreamManager` (all channels) + `CatchupManager`. |
| `fakeiptv/epg.py` | Renders XMLTV XML from schedule window with correct local timezone offset. |
| `fakeiptv/playlist.py` | Renders M3U8 channel list for Televizo. |
| `fakeiptv/server.py` | Flask app. Routes call into the `FakeIPTV` app instance. |
| `fakeiptv/app.py` | `FakeIPTV` class — wires everything, owns refresh + prewarm lifecycle. |
| `run.py` | Entry point. Loads `.env`, config, starts app + Flask. |
| `Dockerfile` | `python:3.9-slim` + `ffmpeg` apt package + pip install. |
| `docker-compose.yml` | Port mapping, tmpfs for segments, named volume for cache, read-only NAS mounts. |
| `.env.example` | Template for all `FAKEIPTV_*` env vars. |

## Configuration hierarchy

`.env` overrides `config.yaml` overrides defaults.
All path-like config values are in `.env` / `config.yaml` — nothing is hardcoded in source.

Key env vars: `FAKEIPTV_SHOWS_PATH`, `FAKEIPTV_MOVIES_PATH`, `FAKEIPTV_RPI_IP`, `FAKEIPTV_TMP_DIR`, `FAKEIPTV_CACHE_DIR`, `FAKEIPTV_TMDB_API_KEY`, `FAKEIPTV_PORT`, `FAKEIPTV_SUBTITLES`, `FAKEIPTV_CATCHUP_DAYS`, `FAKEIPTV_SONARR_URL`, `FAKEIPTV_SONARR_API_KEY`, `FAKEIPTV_RADARR_URL`, `FAKEIPTV_RADARR_API_KEY`, `FAKEIPTV_TMPFS_SIZE`.

## Channel auto-discovery rules

- No per-show channels — every channel is a curated mix.
- **Primetime** — all shows interleaved (round-robin by show).
- **{Genre}** — per-genre mix if ≥ 2 shows share the genre (threshold: `SHOW_GENRE_MIN = 2`).
- **Goldies** — shows with known year < `goldies_before` (default 2010).
- **Hits** — shows with rating ≥ `hits_rating` (default 8.0, from Sonarr/Radarr/TMDB/NFO).
- **Movies** — all remaining movies not claimed by a genre channel.
- **{Genre} Movies** — genre movie mix if ≥ 3 movies share the genre (threshold: `MOVIE_GENRE_MIN = 3`). Movies are exclusive — each movie appears in one channel only.
- Channel ID = `slugify(name)` (lowercase, spaces→hyphens, special chars stripped).
- Overrides in `config.yaml` under `channels.disabled` and `channels.rename`.

## ffmpeg command (live channel)

```bash
ffmpeg -hide_banner -loglevel error \
  -fflags +genpts -avoid_negative_ts make_zero \
  -re -f concat -safe 0 -i {tmp_dir}/ch_{id}/concat.txt \
  -c:v copy -c:a copy \          # or -c:a aac -b:a 192k if audio fallback triggered
  -map 0:v:0 -map 0:a:0 \
  [-map 0:s:0? -c:s webvtt] \    # optional, omitted if subtitles disabled or bitmap detected
  -f hls \
  -hls_time 2 \
  -hls_list_size 15 \
  -hls_flags delete_segments+omit_endlist+append_list \
  -hls_segment_filename {tmp_dir}/ch_{id}/seg%d.ts \
  {tmp_dir}/ch_{id}/stream.m3u8
```

## ffmpeg command (catch-up VOD)

```bash
ffmpeg -hide_banner -loglevel error \
  -ss {offset_sec} -t {duration_sec} \
  -i {entry.path} \
  -c:v copy -c:a copy \
  -map 0:v:0 -map 0:a:0 \
  [-map 0:s:0? -c:s webvtt] \
  -f hls -hls_time 2 -hls_list_size 0 \
  -hls_segment_filename {session_dir}/seg%d.ts \
  {session_dir}/stream.m3u8
```

## Known edge cases / caveats

- **eac3 audio**: some MKVs lose sample rate metadata when muxed into MPEG-TS. Monitor auto-detects and falls back to AAC. One crash cycle per affected channel, then stable.
- **Bitmap subtitles** (PGS/VOBSUB): can't convert to WebVTT. Monitor auto-detects and disables subtitle map. One crash cycle, then stable.
- **inpoint + keyframes**: `-c copy` can only cut at keyframe boundaries, so the first segment after an `inpoint` seek may be shorter than `hls_time`. `-fflags +genpts` prevents the resulting timestamp gap from confusing players.
- **Path escaping in ffconcat**: unquoted paths with `re.sub(r"([ \t'\"])", r"\\\1", path)`. Single-quoted ffconcat strings can't escape `'`; double-quoted paths aren't supported by all ffmpeg versions on Pi.
- **Duration cache** is keyed by `path + mtime`. If a file is replaced in-place without changing mtime, the cached duration may be stale. Delete the `durations` table in the SQLite cache to force re-probe.
- **Container path mismatch**: the cache is keyed by the path ffprobe sees (e.g. `/shows/...`). If the mount point changes, all files are re-probed once automatically.
- **tmpfs sizing**: ~25 MB per active channel (15 segments × ~1–2 MB each). Default Docker tmpfs is 1 GB — raise `FAKEIPTV_TMPFS_SIZE` if you have many simultaneous channels.

## User preferences

- Python 3.9.2 (no match statements, no 3.10+ syntax)
- Paths externalized to `.env` / `config.yaml` — nothing hardcoded
- Daily refresh at midnight **local device time** (not UTC)
- Direct stream only, no re-encoding by default
- LAN use only, no auth, no SSL
- Deployed via Docker on Raspberry Pi
