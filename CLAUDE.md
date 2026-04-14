# FakeIPTV — Claude context

## What this project is

A Python service that reads a NAS media library and exposes it as fake live IPTV channels. Each channel produces a proper HLS live stream (sliding-window `.m3u8` + MPEG-TS segments) and a XMLTV EPG.

Primary consumer: **Televizo** IPTV player on LAN.
Deployed as: **Docker container** (via `docker compose`), running on Windows 11 with Docker Desktop. NAS is accessed over NFS inside the container.

## Deployment

```
git pull && docker compose up -d --build   # pull latest + rebuild + restart (no down needed)
docker logs fakeiptv                        # view logs
docker logs -f fakeiptv                     # follow logs
```

All ffmpeg processes run inside the container — not visible on the host `ps`. HLS segments live in a tmpfs volume (`/tmp/fakeiptv` inside the container). The SQLite cache is in a host-mounted volume (`./fakeiptv-cache:/cache`).

**Docker Desktop / NFS note:** The container uses a Docker named NFS volume (`nas_multimedia`) pointing at `192.168.9.106`. `mount.cifs` inside a Docker Desktop container fails (errno 95, operation not permitted); NFS works natively via the Docker Desktop Linux VM kernel.

If Docker build uses cached layers and changes aren't picked up, `touch` the changed `.py` files before rebuilding. `docker compose build --no-cache` also forces a full rebuild.

## Module map

| File | Role |
|---|---|
| `fakeiptv/config.py` | Loads `config.yaml` + env vars. `AppConfig` dataclass. |
| `fakeiptv/scanner.py` | Walks NAS, parses NFO XML, probes duration/codec/HDR via ffprobe, fetches TMDB/Sonarr/Radarr. Returns `MediaLibrary`. |
| `fakeiptv/arrclient.py` | Sonarr/Radarr API clients for ratings and metadata. |
| `fakeiptv/scheduler.py` | Builds `Channel` objects, computes `NowPlaying`, generates EPG window. |
| `fakeiptv/streamer.py` | `ChannelStreamer` (one ffmpeg process) + `StreamManager` (all channels) + `CatchupManager`. |
| `fakeiptv/epg.py` | Renders XMLTV XML in UTC. String-based for full formatting control. |
| `fakeiptv/playlist.py` | Renders M3U8 channel list with `catchup="shift"` attributes. |
| `fakeiptv/server.py` | Flask app. HLS manifest handler doubles as catchup router for shift-mode requests. |
| `fakeiptv/app.py` | `FakeIPTV` class — wires everything, owns refresh + prewarm lifecycle. |
| `run.py` | Entry point. Loads `.env`, config, starts app + Flask. |
| `Dockerfile` | `python:3.9-slim` + ffmpeg apt package + pip install. |
| `docker-compose.yml` | Port mapping, tmpfs for segments, host-mounted cache, NFS volume for NAS. |
| `.env.example` | Template for all `FAKEIPTV_*` env vars. |

## Key design decisions

- **No transcoding by default.** `ffmpeg -re -c:v copy` remuxes to MPEG-TS. When `audio_copy=true`, audio is also copied; otherwise re-encoded to AAC 192k.
- **Automatic audio fallback.** If ffmpeg reports "unspecified sample rate" (eac3 in some MKVs), the monitor thread sets `_audio_copy = False` and restarts with `-c:a aac -b:a 192k -ac 2` for that channel only.
- **Bitmap subtitle auto-disable.** PGS/VOBSUB subs can't convert to WebVTT. Monitor detects "bitmap to bitmap" in stderr and disables the subtitle map, then restarts cleanly.
- **HDR HEVC metadata strip.** If every entry in a channel is HDR HEVC, the `hevc_metadata` bitstream filter strips colour/transfer metadata. Not applied to mixed or H.264 channels.
- **Deterministic schedule.** Anchored to `EPOCH = datetime(2024, 1, 1, 0, 0, 0)` (local time). No state stored — position always computed from `(now - EPOCH + channel_offset) % total_duration`. Consistent across restarts.
- **Per-channel epoch offset.** Primetime and Mix 1–5 use evenly spaced offsets (0, 5, 10, 15, 20, 25 days). Other channels get an MD5-derived offset in 0–7 days.
- **One ffmpeg process per channel.** Each writes HLS segments to `{tmp_dir}/ch_{id}/`. A monitor thread restarts ffmpeg automatically when the concat window (~4h) is exhausted or on crash.
- **2-second HLS segments, list size 15** (~30s sliding window).
- **Stale segment cleanup on restart.** `_launch()` deletes all `.ts/.m3u8/.vtt` files before starting a new ffmpeg process to prevent timestamp discontinuity artifacts.
- **Timestamp normalization.** `-fflags +genpts -avoid_negative_ts make_zero` ensure monotonic PTS from t=0.
- **NAS disk cache warming.** Before launching ffmpeg (and before catchup), `_nas_prewarm()` explicitly pre-reads three regions of the MKV: header (64 KB), tail (512 KB), and the estimated cluster at the seek point. Puts those pages in the NAS RAM cache before ffmpeg tries to seek over SMB.
- **Keyframe snapping for subtitles.** `_probe_keyframe_inpoint()` finds the actual ffmpeg cut keyframe (which may be up to one GOP earlier than the nominal `inpoint`). The delta is applied as a cue offset to prevent subtitle drift. Max 3 concurrent probes (semaphore) to avoid NAS thrashing on prewarm.
- **Concat demuxer + inpoint.** Seeking into the current file uses the `ffconcat` `inpoint` directive, not `-ss`.
- **Unquoted paths with backslash escaping** in ffconcat files. `re.sub(r"([ \t'\"])", r"\\\1", path)`. Single-quoted ffconcat strings can't escape `'`; double-quoted strings aren't supported on all ffmpeg builds.
- **Flask serves segments directly** from tmpfs via `send_from_directory`. No proxying.
- **EPG covers `catchup_days * 24` hours back + 24h forward.** Output always **UTC (`+0000`)** — local timezone offsets break Televizo's catchup timestamp substitution. Regenerates hourly.
- **EPG served as gzip** at `/epg.xml.gz`. Televizo requires gzip. Plain XML also available at `/epg.xml`.
- **Catch-up via `catchup="shift"` mode.** Televizo appends `?utc=TIMESTAMP&lutc=CURRENT_TIME` to the live HLS stream URL. The manifest handler detects the `utc` param and creates/reuses a catchup session.
- **Catchup session reuse within 60s.** Televizo increments `utc` by a few seconds on each manifest poll. Sessions are reused if the timestamp is within 60s of an existing session for the same channel.
- **Catchup sessions expire after 2h** of inactivity.
- **Daily refresh at midnight (local device time).** Uses `threading.Timer`. Also callable via `GET /refresh`.
- **`start_new_session=True` + `stdin=DEVNULL`** on all Popen calls — isolates ffmpeg from the container process group.
- **Preferred audio language.** `_probe_audio_stream_index()` probes all audio streams and picks the one matching `preferred_audio_language` (ISO 639-1 or 639-2). Falls back to stream 0.
- **Async subtitle thread.** After ffmpeg starts, a separate thread: waits for first TS segment (NAS warms), probes start PTS + actual keyframe inpoint, generates WebVTT from SRT, writes files. Channel declared "subtitle-ready" via event.

## Configuration hierarchy

`.env` overrides `config.yaml` overrides built-in defaults. Nothing is hardcoded in source.

Key env vars:

| Variable | Default | Description |
|---|---|---|
| `FAKEIPTV_HOST_IP` | `127.0.0.1` | LAN IP used in stream/EPG URLs (critical) |
| `FAKEIPTV_PORT` | `8080` | HTTP port |
| `FAKEIPTV_SHOWS_PATH` | `/mnt/nas/Shows` | Path to TV shows (inside container) |
| `FAKEIPTV_MOVIES_PATH` | `/mnt/nas/Movies` | Path to movies (inside container) |
| `FAKEIPTV_TMP_DIR` | `/tmp/fakeiptv` | HLS segment tmpfs location |
| `FAKEIPTV_CACHE_DIR` | `~/.fakeiptv/` | SQLite cache for durations + metadata |
| `FAKEIPTV_SUBTITLES` | `true` | Enable subtitle tracks |
| `FAKEIPTV_AUDIO_COPY` | `true` | Copy audio codec; false = transcode to AAC 192k |
| `FAKEIPTV_PREFERRED_AUDIO_LANGUAGE` | `eng` | ISO 639-1 or 639-2 audio track preference |
| `FAKEIPTV_CATCHUP_DAYS` | `7` | Days of past programming for catch-up |
| `FAKEIPTV_PREWARM` | `false` | Start all channels on first client connect |
| `FAKEIPTV_PREWARM_SESSION` | `false` | Keep all channels alive as a group (session mode) |
| `FAKEIPTV_PREWARM_ADJACENT` | `0` | Warm N channels on each side of watched channel |
| `FAKEIPTV_PREWARM_TIMEOUT` | `120` | Idle timeout (sec) for prewarm-only channels |
| `FAKEIPTV_READY_SEGMENTS` | `1` | Segments buffered before channel declared ready |
| `FAKEIPTV_TMDB_API_KEY` | _(empty)_ | Optional TMDB metadata fallback |
| `FAKEIPTV_SONARR_URL` | _(empty)_ | Optional Sonarr endpoint |
| `FAKEIPTV_SONARR_API_KEY` | _(empty)_ | |
| `FAKEIPTV_RADARR_URL` | _(empty)_ | Optional Radarr endpoint |
| `FAKEIPTV_RADARR_API_KEY` | _(empty)_ | |
| `FAKEIPTV_TMPFS_SIZE` | `1073741824` | tmpfs size in bytes (Docker only) |
| `TZ` | `UTC` | Timezone for schedule/midnight refresh (e.g. `Asia/Jerusalem`) |

`config.yaml` also exposes:

```yaml
channels:
  goldies_before: 2010     # shows older than this year → Goldies channel
  hits_rating: 8.0         # min rating (0–10) for Hits channel
  disabled:
    - goldies
  rename:
    primetime: Prime Time
```

## Channel auto-discovery rules

No per-show channels — every channel is a curated mix.

- **Primetime** — all shows, shuffled round-robin interleave, seed 0.
- **Mix 1–5** — same full show pool as Primetime, distinct seeds (5 additional channels for variety).
- **{Genre}** — per-genre mix if ≥ `SHOW_GENRE_MIN` (3) shows share that genre. Skipped if one show dominates > 60% of episodes. Each show capped at 8% of total channel episodes (min 4 eps).
- **Goldies** — shows with known year < `goldies_before` (default 2010); min 2 shows required.
- **Hits** — shows with rating ≥ `hits_rating` (default 8.0); min 2 shows required.
- **{Genre} Movies** — per-genre movie mix if ≥ `MOVIE_GENRE_MIN` (3) movies share the genre. Movies are exclusive — each movie in one channel only (primary genre wins).
- **Movie Hits** — movies with rating ≥ `hits_rating`. Non-exclusive (can overlap genre channels).
- **Movies** — all movies not claimed by a genre channel.
- Channel ID = `slugify(name)` (lowercase, spaces→hyphens, special chars stripped).
- Overrides: `config.yaml` under `channels.disabled` and `channels.rename`.

**Interleave algorithm:** shuffled round-robin (one episode per show per round), 25% chance of immediate second episode (TV double effect), per-show rotation offset from MD5(channel_id + show_name), seeded RNG from MD5(channel_id).

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

1. Playlist has `catchup="shift" catchup-days="N"` on each channel entry
2. User selects a past programme in Televizo's EPG guide
3. Televizo calls `GET /hls/{channel_id}/stream.m3u8?utc=TIMESTAMP&lutc=CURRENT_TIME`
4. Server detects `utc` param → calls `CatchupManager.get_or_create(channel, datetime.fromtimestamp(utc))`
5. Returns 302 → `/catchup/{channel_id}/{session_id}/stream.m3u8`
6. Catchup session: `ffmpeg -ss {offset} -t {duration} -i {file} ... -f hls -hls_list_size 0`
7. Subsequent polls reuse the same session if within 60s tolerance window

## ffmpeg command (live channel)

```bash
ffmpeg -hide_banner -loglevel error \
  -fflags +genpts -avoid_negative_ts make_zero \
  -re -f concat -safe 0 -i {tmp_dir}/ch_{id}/concat.txt \
  -c:v copy \
  [-c:a aac -b:a 192k -ac 2 | -c:a copy] \         # controlled by audio_copy config
  [-bsf:v hevc_metadata=colour_primaries=1:...] \   # HDR strip, HEVC-only, all-HDR channels
  -map 0:v:0 -map 0:a:{audio_idx} \
  [-map 0:s? -c:s webvtt] \                         # omitted if subtitles=false or bitmap detected
  -f hls -hls_time 2 -hls_list_size 15 \
  -hls_flags delete_segments+omit_endlist+append_list \
  -hls_segment_filename {tmp_dir}/ch_{id}/seg%d.ts \
  {tmp_dir}/ch_{id}/video.m3u8
```

## ffmpeg command (catch-up VOD)

```bash
ffmpeg -hide_banner -loglevel error \
  -ss {offset_sec} -t {duration_sec} \
  -i {entry.path} \
  -c:v copy -c:a copy \
  -map 0:v:0 -map 0:a:{audio_idx} \
  [-map 0:s? -c:s webvtt] \
  -f hls -hls_time 2 -hls_list_size 0 \
  -hls_segment_filename {session_dir}/seg%d.ts \
  {session_dir}/stream.m3u8
```

## Known edge cases / caveats

- **eac3 audio**: some MKVs lose sample rate metadata when muxed into MPEG-TS. Monitor auto-detects and falls back to AAC. One crash cycle per affected channel, then stable.
- **Bitmap subtitles** (PGS/VOBSUB): can't convert to WebVTT. Monitor auto-detects and disables subtitle map. One crash cycle, then stable.
- **Keyframe / inpoint drift**: `-c:v copy` can only cut at keyframe boundaries. `_probe_keyframe_inpoint()` finds the actual cut point and applies the delta as a subtitle cue offset to prevent drift.
- **Path escaping in ffconcat**: unquoted paths with `re.sub(r"([ \t'\"])", r"\\\1", path)`. Single-quoted ffconcat strings can't escape `'`; double-quoted paths aren't supported by all ffmpeg versions.
- **Duration cache** is keyed by `path + mtime`. If a file is replaced in-place without changing mtime, the cached duration may be stale. Delete the `durations` table in the SQLite cache to force re-probe.
- **Container path mismatch**: cache is keyed by the path ffprobe sees (e.g. `/multimedia/...`). If the mount point changes, all files are re-probed once automatically.
- **tmpfs sizing**: ~25 MB per active channel (15 segments × ~1–2 MB). Default Docker tmpfs is 1 GB — raise `FAKEIPTV_TMPFS_SIZE` if needed.
- **`catchup="default"` doesn't work in Televizo** — sends literal `{utc}` without substitution. Must use `catchup="shift"`.
- **EPG local timezone breaks catchup** — EPG timestamps must be UTC. Any non-UTC offset causes Televizo to fail catchup URL substitution.
- **Docker build cache**: if `COPY fakeiptv/` step is cached after editing Python files, `touch` the changed files or use `--no-cache` to force rebuild.
- **MKVs without a seek index (Cues element)**: seeks take 2–10s on cold NAS. `_nas_prewarm()` mitigates this; real fix is mkvmerge/ffmpeg remux with seek index.
- **Hebrew RTL subtitles**: use `_he_bidi_fix()` (RLM + RLI/PDI + LRI/PDI Unicode bidi controls). Do not use the old pre-inversion hack — it breaks bidirectional runs.

## User / dev preferences

- Python 3.9 — no `match` statements, no 3.10+ syntax
- Paths externalized to `.env` / `config.yaml` — nothing hardcoded in source
- Daily refresh at midnight **local device time** (not UTC)
- Direct stream only; re-encoding only as a fallback, never by default
- LAN use only, no auth, no SSL
- Deployed via Docker Desktop on Windows 11; container targets Linux
- CPU is not a bottleneck — NAS SMB/NFS latency is the primary concern
- Do not restart the container without asking
- Do not commit without explicit instruction
