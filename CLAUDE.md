# FakeIPTV — Claude context

## What this project is

A Python service that reads a NAS media library (SMB-mounted) and exposes it as fake live IPTV channels. Each channel produces a proper HLS live stream (sliding-window `.m3u8` + MPEG-TS segments) and a XMLTV EPG.

Primary consumer: **Televizo** IPTV player on LAN.
Runs on: **Raspberry Pi** (Python 3.9.2), deployed as a **systemd service**.

## Key design decisions

- **No transcoding.** `ffmpeg -re -c copy` remuxes to MPEG-TS only. This is extremely light on CPU.
- **Deterministic schedule.** Anchored to `EPOCH = datetime(2024, 1, 1, 0, 0, 0)` (local time). No state stored — position is always computed from `(now - EPOCH) % total_duration`. Consistent across restarts.
- **One ffmpeg process per channel.** Each writes HLS segments to `{tmp_dir}/ch_{id}/`. A monitor thread restarts ffmpeg automatically when the concat window is exhausted (~4h).
- **Concat demuxer + inpoint.** Seeking into the current file at startup uses the `ffconcat` `inpoint` directive, not `-ss` (which doesn't work with concat).
- **Flask serves segments directly** from `/tmp` via `send_from_directory`. No proxying.
- **EPG is a 24-hour window only.** No catch-up. EPG regenerates at each refresh.
- **Daily refresh at midnight (local device time).** Uses `threading.Timer`. Also callable via `GET /refresh`.

## Module map

| File | Role |
|---|---|
| `fakeiptv/config.py` | Loads config.yaml + env vars. `AppConfig` dataclass. |
| `fakeiptv/scanner.py` | Walks NAS, parses NFO XML, probes duration via ffprobe, fetches TMDB. Returns `MediaLibrary`. |
| `fakeiptv/scheduler.py` | Builds `Channel` objects, calculates `NowPlaying`, generates 24h EPG window. |
| `fakeiptv/streamer.py` | `ChannelStreamer` (one ffmpeg process) + `StreamManager` (all channels). |
| `fakeiptv/epg.py` | Renders XMLTV XML from schedule window. |
| `fakeiptv/playlist.py` | Renders M3U8 channel list for Televizo. |
| `fakeiptv/server.py` | Flask app. Routes call into the `FakeIPTV` app instance. |
| `fakeiptv/app.py` | `FakeIPTV` class — wires everything, owns refresh lifecycle. |
| `run.py` | Entry point. Loads `.env`, config, starts app + Flask. |

## Configuration hierarchy

`.env` overrides `config.yaml` overrides defaults.
All path-like config values are in `.env` / `config.yaml` — nothing is hardcoded in source.

Key env vars: `FAKEIPTV_SHOWS_PATH`, `FAKEIPTV_MOVIES_PATH`, `FAKEIPTV_RPI_IP`, `FAKEIPTV_TMP_DIR`, `FAKEIPTV_CACHE_DIR`, `FAKEIPTV_TMDB_API_KEY`, `FAKEIPTV_PORT`.

## Channel auto-discovery rules

- One channel per show folder under `Shows/`.
- Genre mix channels (e.g. "Drama Mix") created if ≥ 3 shows share a genre.
- One "Movies" channel for all movies.
- Genre-specific movie channels if ≥ 5 movies share a genre.
- Channel ID = `slugify(name)` (lowercase, spaces→hyphens, special chars stripped).
- Overrides in `config.yaml` under `channels.disabled` and `channels.rename`.

## ffmpeg command (per channel)

```bash
ffmpeg -hide_banner -loglevel error \
  -re -f concat -safe 0 -i {tmp_dir}/ch_{id}/concat.txt \
  -c copy -map 0:v:0 -map 0:a:0 \
  -f hls \
  -hls_time 5 \
  -hls_list_size 6 \
  -hls_flags delete_segments+omit_endlist+append_list \
  -hls_segment_filename {tmp_dir}/ch_{id}/seg%d.ts \
  {tmp_dir}/ch_{id}/stream.m3u8
```

## Known edge cases / caveats

- Files with DTS or TrueHD audio won't remux cleanly to MPEG-TS with `-c copy`. ffmpeg stderr will report the error. If this comes up, add a fallback: `-c:v copy -c:a aac -b:a 192k`.
- The `inpoint` directive in ffconcat handles the seek into the first file. Do not use `-ss` before `-i` with concat — it doesn't work as expected.
- Duration cache is keyed by `path + mtime`. If a file is replaced in-place without changing mtime, the cached duration may be stale. Delete `duration_cache.json` to force re-probe.
- EPG times are in local device time. XMLTV timestamps use `+0000` offset but represent local time — this is a deliberate simplification for home LAN use.

## User preferences

- Python 3.9.2 (no match statements, no 3.10+ syntax)
- Paths externalized to `.env` / `config.yaml` — nothing hardcoded
- Daily refresh at midnight **local device time** (not UTC)
- Direct stream only, no re-encoding
- LAN use only, no auth, no SSL
