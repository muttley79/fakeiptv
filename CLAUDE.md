# FakeIPTV — Claude Context

## What this project is

A Python service that reads a NAS media library and exposes it as fake live IPTV channels with HLS streams, XMLTV EPG, and catch-up TV. The stream is deterministic (same content at the same wall-clock time across restarts) and designed for the **Televizo** IPTV player on a local LAN. No transcoding for live playback — only a fast ffmpeg remux.

## Deployment

```bash
git pull && docker compose up -d --build   # rebuild + restart
docker logs fakeiptv                        # view logs
docker logs -f fakeiptv                     # follow logs
```

Container runs on Windows 11 / Docker Desktop. NAS is mounted via NFS named volume (`nas_multimedia`). HLS segments are stored in tmpfs (`/tmp/fakeiptv`). SQLite cache and library JSON are persisted in a host-mounted volume (`./fakeiptv-cache:/cache`).

## Module map

| File | Role |
|---|---|
| `run.py` | Entry point. Loads `.env`, config, installs signal handlers, starts Waitress server. |
| `fakeiptv/config.py` | `AppConfig` dataclasses. Loads `config.yaml` + env var overrides. All settings externalized. |
| `fakeiptv/app.py` | `FakeIPTV` class — application core. Owns `StreamManager`, `CatchupManager`, refresh/EPG timers, library cache. |
| `fakeiptv/server.py` | Flask HTTP server. All endpoints: playlist, EPG, HLS manifest/segments, catchup, logos, status, refresh. |
| `fakeiptv/models.py` | Dataclasses: `Episode`, `Movie`, `Show`, `MediaLibrary`, `ScheduleEntry`, `Channel`, `NowPlaying`. |
| `fakeiptv/scheduler.py` | Channel builder + schedule engine. `build_channels()`, deterministic position math (EPOCH, offsets), EPG window builder. |
| `fakeiptv/scanner.py` | NAS walker. Parses NFO XML, fetches Sonarr/Radarr/TMDB metadata, probes via ffprobe, returns `MediaLibrary`. |
| `fakeiptv/arrclient.py` | Sonarr and Radarr API clients. Lazy-loaded, in-memory caches, fuzzy title matching. |
| `fakeiptv/streamer.py` | `ChannelStreamer` (one ffmpeg per channel) + `StreamManager` (owns all streamers, bumper integration, idle reaping, NAS prewarm). |
| `fakeiptv/bumper.py` | `BumperStreamer` (transcoded bumper looped as HLS) + `BumperManager` (owns all bumpers). |
| `fakeiptv/catchup.py` | `CatchupSession` (VOD ffmpeg) + `CatchupManager` (owns all sessions, reuse within 60s, TTL 2h). |
| `fakeiptv/epg.py` | XMLTV rendering. Timestamps in UTC (+0000 zone). |
| `fakeiptv/playlist.py` | M3U8 channel list rendering. Includes catchup URLs with `{utc}` template for Televizo substitution. |
| `fakeiptv/hls_utils.py` | HLS manifest helpers: master playlist builder, sequence injection, discontinuity insertion. |
| `fakeiptv/nfo.py` | Kodi/Jellyfin `.nfo` XML sidecar parser. Extracts title, plot, season/episode, genres, TMDB ID, poster, rating, runtime. |
| `fakeiptv/cache.py` | SQLite WAL-mode caches: `DurationCache`, `TMDBCache`, `KeyframeCache`, `SRTCache`. Thread-safe per-instance. |
| `fakeiptv/library_cache.py` | Startup library cache: `library.json` + `library_meta.json`. Invalidated by TTL, config hash, NAS top-level change. |
| `fakeiptv/ffprobe_utils.py` | ffprobe wrappers. Probes duration, codecs, HDR, keyframes. EBML parser for fast MKV Cues reading. NAS prewarm helpers. |
| `fakeiptv/subtitle_utils.py` | SRT/VTT parsing. Hebrew RTL BiDi fix (RLM + LRI/PDI isolates, bracket mirroring). Subtitle file discovery. |
| `fakeiptv/subtitle_streamer.py` | `SubtitleStreamer` — builds VTT cue lists for all entries in one language for one channel. |
| `fakeiptv/live_subtitle.py` | `LiveSubtitleWriter` — async two-phase VTT writer. Phase 1 (in `_launch`): sync build. Phase 2 (async): probe start_pts, write with correct MPEGTS anchor, watch ffmpeg SRT output. |

## Key design decisions (non-obvious)

### Deterministic schedule
All playback is anchored to `EPOCH = datetime(2024, 1, 1, 0, 0, 0)` local time. Schedule position is pure calculation: `elapsed = (now - EPOCH).total_seconds() + channel_offset_sec`, then `pos = elapsed % channel.total_duration`. Same content plays at the same wall-clock time across restarts. No persistent state — the schedule is derived from the library on demand.

### Per-channel epoch offsets
Prevents simultaneous airing of identical content across channels (e.g., all Mix channels would start at the same time otherwise).
- **Primetime + Mix 1–5**: evenly spaced at multiples of 131 hours (5d 11h apart, deliberately not 24h-aligned). Ensures six all-shows channels always play different content simultaneously.
- **All other channels**: `MD5(channel_id)[:8] % (7 * 24 * 3600)` — hash-based offset in 0–7 days, stable per channel ID.

### ffconcat + inpoint (not -ss)
The ffmpeg input is a pre-built ffconcat file covering ~4 hours ahead. The first entry uses `inpoint {offset:.3f}` to seek to the correct position in the first file. This avoids the slow startup latency of `-ss` seeking before remuxing begins.

### Unquoted backslash-escaped paths in ffconcat
ffconcat escaping: forward slashes required, spaces/quotes escaped as `\<char>`. Single quotes cannot be escaped; double quotes aren't universally supported. Paths are escaped via `re.sub(r"([ \t'\"])", r"\\\1", path)`.

### Stale segment cleanup in `_launch()`
Before launching a new ffmpeg, all stale `.ts`, `.m3u8`, `.srt`, `.vtt` files are deleted from the HLS directory. This prevents PTS discontinuity errors when the old manifest is still being served while new segments are being written.

### NAS prewarm (two-pass, concat lookahead, global sweep)
Three-tier warm strategy to eliminate 2–10s cold-disk seek latency:
1. **First pass** (`_nas_prewarm` before ffprobe calls): reads file header (64 KB) + tail (512 KB) + estimated seek cluster.
2. **Second pass** (just before `Popen`): re-warms in case pages were evicted during ffprobe calls.
3. **Concat lookahead** (`_concat_prewarm_worker`): wakes 60s before ffmpeg opens each upcoming entry, warms the file header.
4. **Global sweep** (every 10 minutes): `_global_prewarm_loop` warms current + next 2 entries for every channel (covers inactive channels).

### Keyframe snapping for subtitles
`_probe_keyframe_inpoint()` finds the actual ffmpeg cut point (the nearest preceding keyframe ≤ nominal offset). For MKV with Cues index: binary-search on the in-memory `KeyframeCache` (0.1ms latency). Fallback: ffprobe full-packet scan (slower). Max 3 concurrent probes via semaphore. Delta between nominal and real inpoint is applied as cue offset in `LiveSubtitleWriter` to prevent subtitle drift.

### Two-phase subtitles
1. **Phase 1 (sync in `_launch()`)**: Parse SRTs for all entries, build cue lists, collect language codes, create `SubtitleStreamer` instances, write placeholder VTTs immediately (player is no longer blocked waiting for subtitle build).
2. **Phase 2 (async in background thread)**: Wait for first `.ts` segment, probe its `start_pts` (90kHz MPEG-TS anchor), probe actual keyframe inpoint, then rewrite VTTs with correct `X-TIMESTAMP-MAP=MPEGTS:{start_pts},LOCAL:00:00:00.000`. Probe start_time of the video to handle disc rips with non-zero video.start_time. Optionally watch ffmpeg SRT side-output and integrate live cues.

### External SRT always wins over embedded
- External `.srt` files (named `{basename}.{lang}.srt` or `{basename}.srt`) are preferred.
- ffmpeg SRT side-output (`-f srt pipe:1`) is only used for languages with no external SRT.
- Embedded subtitle extraction via ffmpeg only fills gaps.
- Result: maximum flexibility — external subtitles override everything.

### `subtitle_background` behavior
`FAKEIPTV_SUBTITLE_BACKGROUND=false` (default):
- Injects `::cue { background-color: rgba(0, 0, 0, 0.6); }` STYLE block into VTT.
- Forces ExoPlayer's CSS rendering path (ignores native `CaptionStyleCompat` settings).
- Player's outline toggle is suppressed (ExoPlayer WebVTT CSS does not support `text-shadow`).

`FAKEIPTV_SUBTITLE_BACKGROUND=true`:
- No STYLE block → native `CaptionStyleCompat` rendering.
- Black background box, player outline/edge options work.

These modes are mutually exclusive in ExoPlayer: any STYLE block forces CSS rendering, which ignores native edge settings.

### Hebrew BiDi fix
RTL text rendering in ExoPlayer/Televizo requires:
- Wrap the entire line in RLI (Right-to-Left Isolate) + RLM (Right-to-Left Mark) + PDI (Pop Directional Isolate): `RLI + text + RLM + PDI`.
- Wrap Latin runs in LRI...PDI (prevents Latin words from being reordered by RTL context).
- Mirror brackets: `()[]{}` ↔ `)(][}{`.
- Move leading punctuation to the right (RTL display correction).

Do NOT use the old pre-inversion hack (reversing the text). Unicode isolates work correctly in modern players.

### Bumper loading screen
`BumperStreamer` manages one transcoded bumper video looped as live HLS:
- On first use or source mtime change: transcode via `libx264 -preset ultrafast -crf 28 -force_key_frames expr:gte(t,n_forced*1)` (1-second keyframes for fine-grained segmentation).
- `manifest_content()` is called on every request and uses `int(time.time())` as the MEDIA-SEQUENCE. This makes the bumper look like a live stream with an ever-advancing sequence number.
- Segment URLs are virtual (`seg{seq}.ts`) and mapped back to on-disk files via modulo of bumper segment count, enabling seamless looping.
- `BumperManager` scans `bumpers_path` at startup, creates one streamer per video file, and exposes `get_random_ready()` for channel cold-start.

### Bumper → channel handoff
The critical problem: when a cold channel becomes ready, the player's MEDIA-SEQUENCE jumps from the bumper's current sequence (e.g., 1234567890) to the channel's first sequence (e.g., 1 or 2). This backward jump triggers ExoPlayer's slow-poll mode (~10s catch-up delay).

Solution:
- Channel streamer gets `hls_start = bumper.current_seq() + 100` (a 100-segment gap = ~400s).
- Channel's `#EXT-X-MEDIA-SEQUENCE` is bumped by this offset when served.
- First `video.m3u8` manifest after bumper triggers `_recalculate_seq_offset()` to shrink the gap to exactly `+1` (one segment forward).
- `#EXT-X-DISCONTINUITY` is injected at the transition point to signal the codec/stream change.

Result: the player sees a strict forward MEDIA-SEQUENCE jump without stall.

### Bumper suppressed for scrubbing
When a user seeks within the same episode (e.g., rewinding to replay a scene), `CatchupSession.is_seek=True` and the `_already_has_bumper` flag is set. The next manifest request suppresses bumper output. This avoids flashing a loading screen mid-scrub.

### Bumper transcoding parameters
`libx264 -preset ultrafast -crf 28 -force_key_frames "expr:gte(t,n_forced*1)"`:
- `ultrafast`: minimal encoding delay, ~2–3 Mbps bitrate.
- `crf 28`: visually lossless for low-motion video.
- `force_key_frames expr:gte(t,n_forced*1)`: guarantee keyframes every 1 second (not dependent on GOP structure or motion).

### HDR HEVC BSF strip
The ffmpeg `-bsf:v hevc_metadata=colour_primaries=1:transfer_characteristics=1:matrix_coefficients=1` bitstream filter strips HDR colour metadata from HEVC streams. **Only applied when ALL entries in a channel are flagged `is_hdr=True`**. Mixed channels or H.264-only channels skip this filter (applying it to non-HEVC crashes ffmpeg).

### Audio codec handling
**Live channels: always transcode audio to AAC 192k stereo** via `-c:a aac -b:a 192k -ac 2`. This handles DTS, EAC3, TrueHD, and other complex codecs transparently.

`FAKEIPTV_AUDIO_COPY=true` (default): try `-c:a copy` first. If ffmpeg detects "unspecified sample rate" in stderr (eac3 in some MKVs), fallback to AAC transcoding and restart the channel (one crash cycle, then stable).

`FAKEIPTV_AUDIO_COPY=false`: always transcode (safest, but slower).

### Bitmap subtitle auto-disable
When ffmpeg outputs "bitmap to bitmap" in stderr (attempting to convert PGS/VOBSUB to WebVTT), the monitor thread disables the subtitle map for this channel (`self._subtitles = False`) and restarts ffmpeg (one crash cycle, then stable).

### Codec change discontinuity
`_build_concat()` detects when consecutive entries differ in `video_codec`, `video_width`, or `video_height`. The concat is truncated at that boundary and `_codec_disc_pending=True`. On ffmpeg restart, `#EXT-X-DISCONTINUITY` is injected before the restart point. This signals the player that a stream property changed.

### Catchup session reuse within 60s
`CatchupManager.get_or_create()` looks for an existing session for the same channel within 60 seconds of the requested timestamp and that hasn't been watched yet (`has_been_watched()==False`). If found, it reuses the session (avoiding duplicate ffmpeg processes). Session TTL is 2 hours; sessions older than that are reaped automatically.

### Catchup rolling delete
`CatchupSession` keeps the last 15 segments (60s of trailing buffer) and deletes older segments as the high-water mark advances (via `mark_fetched()`). This conserves disk space for long-running catchup sessions.

### EPG timestamps in UTC
All XMLTV timestamps are in UTC (`+0000` zone). This is critical for Televizo's catchup URL timestamp substitution — local offsets break the substitution logic. The conversion is done in `epg.py` via `_local_offset_sec()` and `_to_utc()`.

### Catchup URL format: `catchup="shift"`
The M3U8 catchup source template must use `catchup="shift"` (not `catchup="default"`). Televizo only substitutes `{utc}` and `{utcend}` placeholders when the catchup type is `"shift"`. With `"default"`, Televizo sends literal `{utc}` to the server (no substitution).

### `start_new_session=True` + `stdin=DEVNULL` on all Popen
Every `subprocess.Popen` call for ffmpeg uses:
- `start_new_session=True` (creates a new process group, isolates ffmpeg from container process group, ensures kill signals don't cascade).
- `stdin=subprocess.DEVNULL` (ffmpeg is not connected to container stdin, preventing interactive input).

### Library startup cache (`library.json`)
Serialized at `{cache_dir}/library.json` + `{cache_dir}/library_meta.json`. Invalidated by:
1. Age: `startup_cache_max_age_hours` (default 24h).
2. Config hash: any change to media paths, ignore patterns, Arr URLs/keys, TMDB key, channel settings.
3. NAS top-level change: non-recursive `os.scandir` of shows/movies roots — detects adds/removes/renames of top-level folders without recursing (expensive scan avoided).

On cache hit, `refresh()` returns in seconds (skips `Scanner.scan()`, which takes 25–30s). On cache miss or `force=True`, full scan is performed.

### `always_on` modes
`FAKEIPTV_ALWAYS_ON` can be:
- `"false"` (default): channels are started on-demand and stopped after idle timeout (600s for watched, 120s for prewarm-only).
- `"true"`: boot all channels at startup and never stop them. No idle reaping. Useful for always-on playback without interruption.
- `"connected"`: start channels on first manifest request and stop all together after global idle timeout (useful for multi-room setups with shared playback state). Rarely used.

## Configuration hierarchy

`.env` → `config.yaml` → built-in defaults. Environment variables always override config file values.

**Loading order:**
1. `python-dotenv` loads `.env` (optional, silently fails if missing).
2. `FAKEIPTV_CONFIG` env var specifies config path (default `"config.yaml"`).
3. `load_config(path)` reads YAML and applies env var overrides via `_env()`, `_env_int()`, `_env_bool()` helpers.

**Key env vars and defaults:**

**Media paths:**
- `FAKEIPTV_SHOWS_PATH` — default `/mnt/nas/Shows`
- `FAKEIPTV_MOVIES_PATH` — default `/mnt/nas/Movies`

**Server:**
- `FAKEIPTV_HOST` — default `0.0.0.0`
- `FAKEIPTV_PORT` — default `8080`
- `FAKEIPTV_HOST_IP` — LAN IP for URL generation (critical)
- `FAKEIPTV_TMP_DIR` — HLS segments dir, use tmpfs — default `/tmp/fakeiptv`
- `FAKEIPTV_SUBTITLES` — boolean, default `true`
- `FAKEIPTV_SUBTITLE_BACKGROUND` — boolean, default `false` (semi-transparent CSS)
- `FAKEIPTV_AUDIO_COPY` — boolean, default `true`
- `FAKEIPTV_PREFERRED_AUDIO_LANGUAGE` — ISO 639-1/2 code, default `eng`
- `FAKEIPTV_CATCHUP_DAYS` — days of EPG/catchup history, default `7`
- `FAKEIPTV_READY_SEGMENTS` — HLS segments to buffer before ready, default `1`
- `FAKEIPTV_ALWAYS_ON` — `"false"` / `"true"` / `"connected"`, default `"false"`
- `FAKEIPTV_BUMPERS_PATH` — bumper videos dir, default `/app/bumpers`
- `FAKEIPTV_ASSETS_DIR` — logos/assets dir, default `/assets`

**Prewarm modes (pick one):**
- `FAKEIPTV_PREWARM` — start all channels at startup (simple on/off).
- `FAKEIPTV_PREWARM_SESSION` — start all channels on first playlist request, stop after idle timeout.
- `FAKEIPTV_PREWARM_ADJACENT` — start adjacent channels when a channel is touched (value is count, e.g., `2` = next 2 channels).
- `FAKEIPTV_PREWARM_TIMEOUT` — seconds before prewarmed-only channels are stopped, default `120`.

**Metadata:**
- `FAKEIPTV_TMDB_API_KEY` — TMDB API key (optional)
- `FAKEIPTV_SONARR_URL` — Sonarr URL (optional)
- `FAKEIPTV_SONARR_API_KEY` — Sonarr API key (optional)
- `FAKEIPTV_RADARR_URL` — Radarr URL (optional)
- `FAKEIPTV_RADARR_API_KEY` — Radarr API key (optional)
- `FAKEIPTV_CACHE_DIR` — metadata cache dir, default `~/.fakeiptv/`

**Cache:**
- `FAKEIPTV_STARTUP_CACHE` — boolean, default `true`
- `FAKEIPTV_STARTUP_CACHE_MAX_AGE_HOURS` — cache TTL, default `24`

**Logging:**
- `FAKEIPTV_LOG_LEVEL` — logging level, default `INFO`
- `FAKEIPTV_LOG_FILE` — optional log file path

## ffmpeg commands (skeletons)

### Live channel
```
ffmpeg -fflags +genpts -avoid_negative_ts make_zero -re \
  -f concat -safe 0 -i concat.txt \
  -c:v copy \
  [-bsf:v hevc_metadata=colour_primaries=1:transfer_characteristics=1:matrix_coefficients=1] \
  -c:a aac -b:a 192k -ac 2 \
  -map 0:v:0 -map 0:a:{audio_idx} \
  [-map 0:s? -c:s webvtt] \
  -f hls -hls_time 4 -hls_list_size 15 \
  -hls_flags delete_segments+omit_endlist+append_list \
  -hls_segment_filename {hls_dir}/seg%d.ts \
  {hls_dir}/video.m3u8
```

**Notes:**
- `-fflags +genpts -avoid_negative_ts make_zero`: regenerate PTS from scratch, starting at 0. Eliminates timestamp discontinuities from inpoint-based seeks.
- `-c:v copy`: no video transcoding.
- `-c:a aac -b:a 192k -ac 2`: always transcode audio to stereo AAC (safe, handles DTS/EAC3/TrueHD).
- `[-bsf:v hevc_metadata=...]`: only applied when all channel entries are HDR HEVC.
- `-c:s webvtt`: external subtitle output only (ffmpeg SRT side-output for languages without external SRT).
- `concat.txt` covers ~4 hours ahead, first entry uses `inpoint {offset:.3f}` for seeking.

### Bumper loop
```
ffmpeg -stream_loop -1 -re -i {bumper_file} \
  -c:v libx264 -preset ultrafast -crf 28 \
  -force_key_frames "expr:gte(t,n_forced*1)" \
  -c:a aac -b:a 128k -ac 2 \
  -f hls -hls_time 2 -hls_list_size 15 \
  -hls_flags delete_segments+omit_endlist \
  -hls_segment_filename {bumper_dir}/seg%d.ts \
  {bumper_dir}/video.m3u8
```

**Notes:**
- `-stream_loop -1`: infinite loop (the bumper runs indefinitely).
- `-preset ultrafast -crf 28`: minimal encoding delay, visually lossless.
- `-force_key_frames "expr:gte(t,n_forced*1)"`: 1-second keyframes for fine-grained segmentation.
- Output is segmented to tmpfs, not persisted.

### Catchup VOD
```
ffmpeg -ss {offset_sec} -re -avoid_negative_ts make_zero \
  -i {file} \
  -c:v copy -c:a aac -b:a 192k -ac 2 \
  -map 0:v:0 -map 0:a:{audio_idx} \
  [-map 0:s? -c:s webvtt] \
  -t {remaining_duration_sec} \
  -f hls -hls_list_size 0 \
  -hls_flags omit_endlist+append_list \
  -hls_segment_filename {session_dir}/seg%d.ts \
  {session_dir}/video.m3u8
```

**Notes:**
- `-ss {offset_sec}`: seek to the requested position.
- `-t {duration}`: limit output to the remaining duration of the episode (ensures VOD ends correctly).
- `-hls_list_size 0`: all segments retained (no rolling window). Allows rewinding.
- `-hls_flags omit_endlist`: manifest does not contain `#EXT-X-ENDLIST` initially (added after ffmpeg exits).

## Channel auto-discovery rules

Channels are auto-discovered from the library by `build_channels()` in this order:

1. **Primetime** — all shows shuffled round-robin interleave (seed 0). Channel ID: `primetime`.
2. **Per-genre show channels** — groups shows by primary genre. Creates `{genre}` channel if ≥ 3 shows share the genre AND no single show owns > 60% of episodes.
3. **Goldies** — shows with `year < goldies_before` (config, default 2010). Requires ≥ 2 shows. Channel ID: `goldies`.
4. **Hits** — shows with `rating >= hits_rating` (config, default 8.0). Requires ≥ 2 shows. Channel ID: `hits`.
5. **Mix 1–5** — five additional all-shows channels with different shuffle seeds. Channel IDs: `mix-1` … `mix-5`. Combined with distinct time offsets (131h apart), these always air different content.
6. **Genre movie channels** — for each primary genre with ≥ 3 movies, creates `{genre}-movies` channel. **Exclusive**: each movie claimed by one genre channel only.
7. **Movie Hits** — high-rated movies (`rating >= hits_rating`). **Non-exclusive**, can overlap genre channels.
8. **Movies** — all remaining unclaimed movies. Channel ID: `movies`.

Channel IDs can be overridden via config `channels.rename: {old_id: new_name}`. Channels can be disabled via config `channels.disabled: [id1, id2, ...]`.

**Interleave algorithm** (used for all show channels):
1. **Shuffle seed**: `int(MD5(channel_id)[:8], 16)` — stable per channel.
2. **Per-show episode cap**: max `max(int(total_raw_episodes * 0.08), 4)` episodes per show (prevents dominance).
3. **Per-show rotation**: first episode index = `MD5(channel_id + show.name)[:8] % len(episodes)` (same show starts at different point in each channel).
4. **Round-robin loop**: shuffle the show queue, pop one episode per show, 25% chance of immediate double episode. Repeat until all shows exhausted.
5. **Result**: flat list of `ScheduleEntry`, deterministic, well-balanced.

## Known edge cases

**eac3 audio codec** — Some MKVs encode eac3 with "unspecified sample rate" which ffmpeg detects as an error in stderr. Monitor thread triggers fallback to AAC transcoding and restarts the channel (one crash cycle, then stable).

**Bitmap subtitles** — PGS and VOBSUB formats cannot be converted to WebVTT. Monitor detects "bitmap to bitmap" in stderr and disables subtitle mapping for the channel (one crash cycle, then stable).

**MKVs without seek index** — Files without an MKV Cues index require ffprobe full-packet scan, which can take 10–30 seconds. The keyframe cache and NAS prewarm mitigate the impact.

**Duration cache staleness** — Cache key is `{path}|{mtime}`. If a file is modified in-place without mtime change, the cache entry is stale. Solution: delete the `durations` table in SQLite or use `force=True` refresh.

**Container path mismatch** — If the NAS mount path changed (e.g., `/mnt/nas` → `/multimedia`), the cache re-probes all durations on first run (one-time cost). Subsequent runs use the cached values.

**tmpfs sizing** — Each active channel uses ~25 MB of tmpfs (HLS segments + subtitle files). Default 1 GB covers ~40 channels. Configure via `FAKEIPTV_TMPFS_SIZE` env var (bytes).

## Dev constraints

- **Python 3.9** — no `match` statement, no 3.10+ syntax (string union types, etc.). Stick to dataclasses, type hints via `typing.List`/`Dict`.
- **Never restart container without asking** — always ask the user before running `docker compose up -d --build`.
- **Never commit without explicit instruction** — ask before running `git add/commit/push`.
- **LAN only, no auth, no SSL** — this is a local network tool, not internet-facing.
- **Thread safety** — use locks for shared state (`_lock` on `ChannelStreamer`, `StreamManager`, per-instance on cache classes).
- **Daemon threads** — background workers are daemon threads so they don't block process exit.
