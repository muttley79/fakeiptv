# FakeIPTV — Claude context

## What this project is

A Python service that reads a NAS media library and exposes it as fake live IPTV channels with HLS streams, XMLTV EPG, and catch-up TV. Primary consumer: **Televizo** IPTV player on LAN.

## Deployment

```
git pull && docker compose up -d --build   # rebuild + restart
docker logs fakeiptv                        # view logs
docker logs -f fakeiptv                     # follow logs
```

Container runs on Windows 11 / Docker Desktop. NAS via NFS named volume (`nas_multimedia`). HLS segments in tmpfs (`/tmp/fakeiptv`). SQLite cache in host-mounted `./fakeiptv-cache:/cache`.

## Module map

| File | Role |
|---|---|
| `fakeiptv/config.py` | `AppConfig` dataclass. Loads `config.yaml` + env vars. |
| `fakeiptv/scanner.py` | Walks NAS, parses NFO XML, probes via ffprobe, fetches TMDB/Sonarr/Radarr. Returns `MediaLibrary`. |
| `fakeiptv/arrclient.py` | Sonarr/Radarr API clients. |
| `fakeiptv/scheduler.py` | Builds `Channel` objects, computes `NowPlaying`, generates EPG window. |
| `fakeiptv/streamer.py` | `ChannelStreamer` (one ffmpeg) + `StreamManager` + `CatchupManager` + `BumperStreamer/Manager`. |
| `fakeiptv/epg.py` | Renders XMLTV XML (UTC). |
| `fakeiptv/playlist.py` | Renders M3U8 channel list. |
| `fakeiptv/server.py` | Flask app. HLS manifest handler doubles as catchup router. |
| `fakeiptv/app.py` | `FakeIPTV` — wires everything, owns refresh + prewarm lifecycle. |
| `run.py` | Entry point. Loads `.env`, config, starts app + waitress server. |

## Key design decisions (non-obvious)

- **Deterministic schedule** anchored to `EPOCH = datetime(2024,1,1)` local time. Position = `(now - EPOCH + offset) % total_duration`. No state stored.
- **Per-channel epoch offset** prevents simultaneous airing of the same content across channels. Primetime/Mix use fixed offsets (0,5,10,15,20,25 days); others use MD5-derived 0–7 day offset.
- **ffconcat + inpoint** for seeking into current file (not `-ss`). Covers ~4h ahead.
- **Unquoted paths with backslash escaping** in ffconcat: `re.sub(r"([ \t'\"])", r"\\\1", path)`. Single-quoted strings can't escape `'`; double-quoted not universally supported.
- **Stale segment cleanup** in `_launch()` deletes `.ts/.m3u8/.vtt` before new ffmpeg to prevent PTS discontinuity.
- **NAS prewarm** before ffmpeg seeks: reads header (64 KB) + tail (512 KB) + estimated cluster. Puts pages in NAS RAM cache before ffmpeg touches file.
- **Keyframe snapping for subtitles**: `_probe_keyframe_inpoint()` finds actual ffmpeg cut point (≤1 GOP before nominal). Delta applied as cue offset to prevent subtitle drift. Max 3 concurrent probes (semaphore).
- **Two-phase subtitle**: Phase 1 (sync in `_launch()`) parses SRTs, builds cue lists. Phase 2 (async thread) waits for first TS segment, probes `start_pts`, writes VTT with correct `X-TIMESTAMP-MAP=MPEGTS:{start_pts},LOCAL:00:00:00.000`. MPEGTS:0 is wrong — HEVC B-frames start at PTS ≈ 134k, not 0.
- **External SRT always wins over embedded** subs. ffmpeg SRT side-output only for langs with no external SRT.
- **`subtitle_background`** (`config.yaml` / `FAKEIPTV_SUBTITLE_BACKGROUND`): `true` (default) = no STYLE block in VTT → ExoPlayer uses native `CaptionStyleCompat` rendering (black background box, player outline option works). `false` = injects `::cue { background-color: transparent }` STYLE block → transparent background but outline option in Televizo is suppressed. Mutually exclusive in ExoPlayer: any STYLE block forces CSS rendering path which ignores `EDGE_TYPE_OUTLINE`.
- **Hebrew bidi fix**: RLM + RLI/PDI/LRI Unicode isolates. Not the old pre-inversion hack (breaks bidi runs).
- **Bumper loading screen**: `BumperStreamer` runs continuous looped HLS. When channel is cold, `hls_manifest()` returns master playlist immediately; `hls_segment(video.m3u8)` serves bumper until `is_transition_ready()`. Channel ffmpeg starts in daemon thread (background=True) — no blocking wait.
- **Bumper→channel handoff**: `seq_offset` bumps channel's `EXT-X-MEDIA-SEQUENCE` above bumper's (bumper_seq + 100) to prevent player stall from backward jump. `#EXT-X-DISCONTINUITY` injected on first real manifest after bumper.
- **Bumper suppressed for scrubbing**: `CatchupSession.is_seek=True` when user seeks within same episode — bumper flash avoided.
- **Bumper transcoding** uses `-c:v libx264 -preset ultrafast -crf 28 -force_key_frames expr:gte(t,n_forced*2)` — guarantees 2s segments regardless of source GOP.
- **HDR HEVC strip**: `hevc_metadata` BSF strips colour/transfer metadata only when ALL channel entries are HDR HEVC. Never on mixed or H.264 — applying it to H.264 crashes ffmpeg.
- **Audio fallback**: monitor detects "unspecified sample rate" (eac3 in some MKVs), sets `_audio_copy=False`, restarts with `-c:a aac -b:a 192k -ac 2` for that channel only.
- **Bitmap subtitle auto-disable**: monitor detects "bitmap to bitmap" in stderr, disables subtitle map, restarts. One crash cycle, then stable.
- **Codec change discontinuity**: `#EXT-X-DISCONTINUITY` injected at concat boundaries where video codec changes (e.g. H.264 → HEVC).
- **Catchup session reuse within 60s**: Televizo increments `utc` by a few seconds on each poll. Sessions expire after 2h inactivity.
- **EPG timestamps must be UTC (`+0000`)**: any local offset breaks Televizo's catchup URL timestamp substitution.
- **`catchup="shift"` required**: `catchup="default"` sends literal `{utc}` in Televizo — no substitution.
- **`start_new_session=True` + `stdin=DEVNULL`** on all Popen — isolates ffmpeg from container process group.

## Configuration hierarchy

`.env` → `config.yaml` → built-in defaults. Nothing hardcoded in source. All paths externalized.

## ffmpeg commands (skeletons)

**Live channel:**
```
ffmpeg -re -f concat -safe 0 -i concat.txt
  -c:v copy [-c:a aac -b:a 192k -ac 2 | -c:a copy]
  [-bsf:v hevc_metadata=colour_primaries=1:...]   # HDR-only channels
  -map 0:v:0 -map 0:a:{idx} [-map 0:s? -c:s webvtt]
  -f hls -hls_time 2 -hls_list_size 15
  -hls_flags delete_segments+omit_endlist+append_list
```

**Bumper loop:**
```
ffmpeg -stream_loop -1 -re -i {file}
  -c:v libx264 -preset ultrafast -crf 28
  -force_key_frames expr:gte(t,n_forced*2)
  -c:a aac -b:a 128k -ac 2
  -f hls -hls_time 2 -hls_list_size 15
```

**Catchup VOD:**
```
ffmpeg -ss {offset} -t {duration} -i {file}
  -c:v copy -c:a copy
  -map 0:v:0 -map 0:a:{idx} [-map 0:s? -c:s webvtt]
  -f hls -hls_time 2 -hls_list_size 0
```

## Channel auto-discovery rules

- **Primetime** — all shows, shuffled round-robin interleave, seed 0
- **Mix 1–5** — same pool, seeds 1–5
- **{Genre}** — ≥3 shows share genre, no show dominates >60% of episodes, each show capped at 8% (min 4 eps)
- **Goldies** — year < `goldies_before` (default 2010), ≥2 shows
- **Hits** — rating ≥ `hits_rating` (default 8.0), ≥2 shows
- **{Genre} Movies** — ≥3 movies per genre (exclusive: primary genre wins)
- **Movie Hits** — rating ≥ hits_rating (non-exclusive)
- **Movies** — remaining movies

Interleave: shuffled round-robin, 25% chance of immediate second episode (double effect).

## Known edge cases

- eac3: loses sample rate in MPEG-TS → auto-fallback to AAC. One crash cycle per channel, then stable.
- Bitmap subs (PGS/VOBSUB): can't convert to WebVTT → auto-disabled. One crash cycle, then stable.
- MKVs without seek index: NAS seeks 2–10s cold. `_nas_prewarm()` mitigates.
- Duration cache keyed by `path|mtime`. File replaced in-place without mtime change → stale cache. Delete `durations` table in SQLite to force re-probe.
- Container path mismatch: cache re-probes once on first run if mount path changed.
- tmpfs: ~25 MB per active channel. Default 1 GB covers ~40 channels.

## Dev constraints

- Python 3.9 — no `match`, no 3.10+ syntax
- Never restart the container without asking the user
- Never commit without explicit instruction
- LAN only, no auth, no SSL
- CPU is not a bottleneck — NAS NFS latency is the primary concern
