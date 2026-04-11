# FakeIPTV — Claude context

## What this project is

A Python service that reads a NAS media library (SMB-mounted) and exposes it as fake live IPTV channels. Each channel produces a proper HLS live stream (sliding-window `.m3u8` + MPEG-TS segments) and a XMLTV EPG.

Primary consumer: **Televizo** IPTV player on LAN.
Deployed as: **Docker container** (via `docker compose`), running on a NAS or Raspberry Pi.

## Deployment

```
git pull && docker compose up -d --build   # pull latest + rebuild + restart (no down needed)
docker logs fakeiptv                        # view logs
docker logs -f fakeiptv                     # follow logs
```

All ffmpeg processes run inside the container — not visible on the host `ps`. HLS segments live in a tmpfs volume (`/tmp/fakeiptv` inside the container). The SQLite cache is in a host-mounted volume (`~/.fakeiptv:/cache`).

If Docker build uses cached layers and changes aren't picked up, `touch` the changed `.py` files before rebuilding. `docker compose build --no-cache` also forces a full rebuild.

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
- **Pre-warm all channels on first client connect.** `prewarm_channels()` is triggered by the first `/hls/*/stream.m3u8` request of each session. Starts all channels staggered 0.3s apart. Resets when all channels go idle so next session pre-warms again.
- **Two-tier idle timeout.** Channels watched by a real client: 600s timeout. Channels only pre-warmed (never watched): 120s timeout. Keeps Pi CPU low when nobody is browsing.
- **Lazy ffmpeg startup.** `ensure_started()` is called on manifest request; waits up to 20s via `threading.Event` for the first segment.
- **Concat demuxer + inpoint.** Seeking into the current file at startup uses the `ffconcat` `inpoint` directive, not `-ss` (which doesn't work with concat).
- **Unquoted paths with backslash escaping** in ffconcat files. Single-quoted strings can't escape `'`; double-quoted strings aren't supported by all ffmpeg builds. `re.sub(r"([ \t'\"])", r"\\\1", path)` is the reliable approach.
- **Flask serves segments directly** from tmpfs via `send_from_directory`. No proxying.
- **EPG covers 7 days back + 1 day forward.** Output in **UTC (`+0000`)** — local timezone offsets break Televizo's catchup timestamp substitution. Regenerates hourly.
- **EPG served as gzip** at `/epg.xml.gz`. Televizo requires gzip. Plain XML also available at `/epg.xml`.
- **Catch-up via `catchup="shift"` mode.** `catchup="default"` does NOT substitute `{utc}` in Televizo. With `shift`, Televizo appends `?utc=TIMESTAMP&lutc=CURRENT_TIME` to the live HLS stream URL. The manifest handler detects the `utc` param and creates/reuses a catchup session.
- **Catchup session reuse within 60s.** Televizo increments `utc` by a few seconds on each manifest poll in shift mode. Sessions are reused if the timestamp is within 60s of an existing session for the same channel, preventing runaway ffmpeg spawning.
- **Catchup sessions expire after 2h** of inactivity.
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
| `fakeiptv/epg.py` | Renders XMLTV XML in UTC. String-based (not xml.etree) for full control of formatting. |
| `fakeiptv/playlist.py` | Renders M3U8 channel list for Televizo with catchup="shift" attributes. |
| `fakeiptv/server.py` | Flask app. HLS manifest handler doubles as catchup router for shift-mode requests. |
| `fakeiptv/app.py` | `FakeIPTV` class — wires everything, owns refresh + prewarm lifecycle. |
| `run.py` | Entry point. Loads `.env`, config, starts app + Flask. |
| `Dockerfile` | `python:3.9-slim` + `ffmpeg` apt package + pip install. |
| `docker-compose.yml` | Port mapping, tmpfs for segments, host-mounted cache, read-only NAS mounts, `TZ=Asia/Jerusalem`. |
| `.env.example` | Template for all `FAKEIPTV_*` env vars. |

## Configuration hierarchy

`.env` overrides `config.yaml` overrides defaults.
All path-like config values are in `.env` / `config.yaml` — nothing is hardcoded in source.

Key env vars: `FAKEIPTV_SHOWS_PATH`, `FAKEIPTV_MOVIES_PATH`, `FAKEIPTV_RPI_IP`, `FAKEIPTV_TMP_DIR`, `FAKEIPTV_CACHE_DIR`, `FAKEIPTV_TMDB_API_KEY`, `FAKEIPTV_PORT`, `FAKEIPTV_SUBTITLES`, `FAKEIPTV_CATCHUP_DAYS`, `FAKEIPTV_SONARR_URL`, `FAKEIPTV_SONARR_API_KEY`, `FAKEIPTV_RADARR_URL`, `FAKEIPTV_RADARR_API_KEY`, `FAKEIPTV_TMPFS_SIZE`, `TZ`.

## Channel auto-discovery rules

- No per-show channels — every channel is a curated mix.
- **Primetime** — all shows interleaved (round-robin by show).
- **{Genre}** — per-genre mix if ≥ 3 shows share the genre (`SHOW_GENRE_MIN = 3`). Skipped if one show dominates > 60% of episodes (`SHOW_GENRE_MAX_DOMINANCE = 0.6`). Each show's contribution capped at 3× the smallest show's episode count (`SHOW_GENRE_EPISODE_CAP_FACTOR = 3`).
- **Goldies** — shows with known year < `goldies_before` (default 2010).
- **Hits** — shows with rating ≥ `hits_rating` (default 8.0, from Sonarr/Radarr/TMDB/NFO).
- **Movies** — all remaining movies not claimed by a genre channel.
- **{Genre} Movies** — genre movie mix if ≥ 3 movies share the genre (`MOVIE_GENRE_MIN = 3`). Movies are exclusive — each movie appears in one channel only.
- Channel ID = `slugify(name)` (lowercase, spaces→hyphens, special chars stripped).
- Overrides in `config.yaml` under `channels.disabled` and `channels.rename`.

## EPG format (Televizo-compatible)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE tv SYSTEM "https://raw.githubusercontent.com/XMLTV/xmltv/master/xmltv.dtd">
<tv>
  <channel id="primetime">
    <display-name>Primetime</display-name>
  </channel>
  <programme channel="primetime" start="20260411160000 +0000" stop="20260411163530 +0000">
    <title>The IT Crowd</title>
    <sub-title>S01E02 - Calamity Jen</sub-title>
    <desc>...</desc>
    <category>Comedy</category>
    <episode-num system="onscreen">S01 E02</episode-num>
    <episode-num system="xmltv_ns">0.1.0/1</episode-num>
  </programme>
</tv>
```

Key requirements:
- Timestamps **must be UTC (`+0000`)** — local offsets break Televizo catchup
- No `lang=` attributes on child elements
- No extra attributes on `<tv>` tag
- DOCTYPE declaration required
- `url-tvg=` (not only `x-tvg-url`) in the M3U8 header

## Catchup flow (Televizo shift mode)

1. Playlist has `catchup="shift" catchup-days="7"` on each channel entry
2. User selects a past programme in Televizo's EPG guide
3. Televizo calls `GET /hls/{channel_id}/stream.m3u8?utc=TIMESTAMP&lutc=CURRENT_TIME`
4. Server detects `utc` param → calls `CatchupManager.get_or_create(channel, datetime.fromtimestamp(utc))`
5. Returns 302 → `/catchup/{channel_id}/{session_id}/stream.m3u8`
6. Catchup session: `ffmpeg -ss {offset} -t {duration} -i {file} ... -f hls -hls_list_size 0 ... stream.m3u8`
7. Subsequent polls reuse the same session (60s tolerance window)

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
- **Catchup `catchup="default"` doesn't work in Televizo** — it sends literal `{utc}` without substitution. Must use `catchup="shift"`.
- **EPG local timezone breaks catchup** — EPG timestamps must be UTC. If `+0300` or similar is used, Televizo fails to extract timestamps for catchup URL substitution.
- **Docker build cache**: if `COPY fakeiptv/` step is cached after editing Python files, `touch` the changed files or use `--no-cache` to force rebuild.

## User preferences

- Python 3.9.2 (no match statements, no 3.10+ syntax)
- Paths externalized to `.env` / `config.yaml` — nothing hardcoded
- Daily refresh at midnight **local device time** (not UTC)
- Direct stream only, no re-encoding by default
- LAN use only, no auth, no SSL
- Deployed via Docker on Raspberry Pi 4
