# Session Notes — 2026-04-13

Branch: `feature/subtitles`

## What was built

### Subtitle support (external SRT files)
Scanner detects `.he.srt`, `.en.srt`, `.srt` files next to each video.  
Scheduler propagates `subtitle_paths: Dict[str, str]` and `has_embedded_subs: bool` through `ScheduleEntry`.  
Streamer generates WebVTT + HLS playlists at channel start.  
Server builds an HLS master playlist (`stream.m3u8`) referencing `video.m3u8` + `sub_{lang}.m3u8` when subtitles are available.

### Subtitle generation — two-phase design (latest fix)

**Phase 1 (synchronous, < 100ms in `_launch()`)**:  
Parse all SRT files for the 4h concat window. Build cue lists in memory.  
`_subtitle_streamers` dict is populated immediately (no files written yet).  
`SubtitleStreamer.build_cues(inpoint)` → returns `(cue_lines, cue_count)`.

**Phase 2 (async thread `_write_subtitle_files_async`)**:  
Waits for first TS segment (~2s). Probes `start_pts` from it via ffprobe.  
Writes `sub_{lang}.vtt` with `X-TIMESTAMP-MAP=MPEGTS:{start_pts},LOCAL:00:00:00.000`.  
Writes `sub_{lang}.m3u8` (VOD-style, single-entry, `#EXT-X-ENDLIST`).  
Sets `_subtitle_ready_event` when done.

**Server (`hls_manifest`)**:  
After `wait_ready()` (~6s for 3 segments), calls `wait_subtitle_ready(10s)`.  
Since Phase 2 finishes at ~2.5s, this adds zero net startup delay.  
`get_subtitle_languages()` only returns langs where `sub.is_running() == True`  
(i.e. VTT has been written with correct TIMESTAMP-MAP).

**Why MPEGTS:0 was wrong**: With `-c:v copy` + HEVC B-frames, stream starts at  
PTS ≈ 133950 (1.488s), not 0. Using MPEGTS:0 caused all cues to appear at  
wrong times. Now we probe the real start_pts and use it.

### HDR metadata stripping (hevc_metadata BSF)
`is_hdr: bool` added to `Episode`, `Movie`, `ScheduleEntry`.  
Scanner detects HDR via `color_transfer` in ffprobe (`smpte2084`, `arib-std-b67`, `smpte428`).  
BSF `hevc_metadata=colour_primaries=1:transfer_characteristics=1:matrix_coefficients=1`  
strips HDR color metadata so SDR players don't show green-screen.

**Safety fix**: BSF is HEVC-only. Applied only when ALL entries in channel have  
`is_hdr=True` (guarantees all-HEVC). Mixed HDR+SDR channels skip it — applying  
it to H.264 segments crashes ffmpeg and causes "jumpy" video from restarts.

### Audio
Always transcodes to AAC (`-c:a aac -b:a 192k -ac 2`). Handles DTS, EAC3, TrueHD etc.

### Embedded subtitle detection
`has_embedded_subs` probed via ffprobe (text subs only; PGS/VOBSUB excluded).  
Embedded subs via ffmpeg (`-map 0:s:0? -c:s webvtt`) used as fallback when no external SRTs.  
Bitmap sub auto-disable still works (monitor detects "bitmap to bitmap" in stderr).

## Key commits
```
9a34abd  Fix subtitle race condition and HDR BSF safety
60295e6  Fix subtitle TIMESTAMP-MAP race: write VTT only after probing start_pts
```

## Known issues / TODO

- **Subtitle fix is inconsistent** — sometimes works, sometimes doesn't.  
  Needs logs: TIMESTAMP-MAP write timing vs player VTT fetch timing.

- **Subtitle timing off by a few seconds** — not yet retested with two-phase fix.  
  Remaining drift from nominal vs actual keyframe inpoint (keyframe probe removed  
  from critical path to avoid 5-15s NAS latency).

- **Catchup is broken** — was working before subtitle/HDR work. Cause unknown.  
  Check `CatchupManager` and catchup routes in `server.py`.

- **HDR channels** — BSF now only applies when ALL entries are HDR. Needs test  
  on a pure-HDR channel to confirm green-screen fix works.

- **Jumpy HEVC (BT.709)** — likely BSF crash-restart on mixed channel (now fixed).  
  If it persists after rebuild, may be a source file or large-GOP alignment issue.

- **video_codec not tracked** — `is_hdr` used as HEVC proxy. Future: add  
  `video_codec: str` to `Episode`/`Movie` for more precise BSF gating.

## Architecture notes
- `DEFAULT=NO, AUTOSELECT=NO` on subtitle tracks — user must select manually in Televizo.
- SRT normalization: `srt_offset = min(cue start times)` handles disc-absolute timestamps.
- Segment cleanup in `_launch()` deletes `.vtt` files alongside `.ts`/`.m3u8`.
- `_subtitle_ready_event` is cleared in `_launch()` and set in `_write_subtitle_files_async()`.
- Channels without SRTs set the event immediately so `wait_subtitle_ready()` never blocks them.
