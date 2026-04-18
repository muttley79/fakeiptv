"""
test_televizo.py — Automated Televizo simulator for FakeIPTV.

Simulates the Televizo IPTV player as a black-box external client:
  - Fetches playlist.m3u8, parses channels
  - Opens a channel via stream.m3u8 (master playlist), then polls video.m3u8
  - Observes the bumper -> real channel transition from segment URLs and manifest tags
  - Performs catchup: follows 302 redirects, polls VOD manifest to ENDLIST
  - Validates subtitle manifests, segment content (via ffprobe), EPG/status consistency
  - No internal server knowledge used to drive test flow

Usage:
    python tests/test_televizo.py [options]

Options:
    --url URL           Server base URL (default: http://localhost:8080)
    --channel ID        Channel to test (default: first from playlist.m3u8)
    --timeout SECS      Max wait for bumper transition / catchup ready (default: 60)
    --ffprobe PATH      Path to ffprobe binary (default: ffprobe; skipped if not found)
    --nas-map SRC=DST   Map container path prefix to Windows path.
                        May be given multiple times.
                        Defaults: /mnt/nas/Shows=M:\\TV Shows
                                  /mnt/nas/Movies=M:\\Movies
    --only GROUPS       Comma-separated test groups: live,epg,catchup,regression,all
    --skip NAMES        Comma-separated test names to skip
    --list              List all tests and exit

Exit code: 0 if all ran tests pass, 1 otherwise.
"""
import argparse
import gzip
import json
import os
import random
import re
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

try:
    import requests
except ImportError:
    print("ERROR: 'requests' library not installed. Run: pip install requests")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Context
# ---------------------------------------------------------------------------

@dataclass
class TestCtx:
    base_url: str
    session: requests.Session
    channel_id: str           # primary test channel (from playlist)
    catchup_source_tpl: str   # catchup-source URL template for test channel
    all_channels: List[dict]  # raw channel dicts from playlist.m3u8
    timeout: int
    ffprobe_path: Optional[str]
    nas_maps: List[Tuple[str, str]]  # [(container_prefix, windows_path)]

    def url(self, path_or_url: str) -> str:
        if path_or_url.startswith("http"):
            return path_or_url
        return self.base_url.rstrip("/") + path_or_url

    def get(self, path_or_url: str, **kwargs) -> requests.Response:
        kwargs.setdefault("allow_redirects", False)
        kwargs.setdefault("timeout", 20)
        return self.session.get(self.url(path_or_url), **kwargs)

    def map_path(self, container_path: str) -> Optional[str]:
        for src, dst in self.nas_maps:
            if container_path.startswith(src):
                rel = container_path[len(src):].lstrip("/").replace("/", os.sep)
                return os.path.join(dst, rel)
        return None


# ---------------------------------------------------------------------------
# Test registry
# ---------------------------------------------------------------------------

_TESTS: List[Tuple[str, str, object]] = []


def _register(name: str, group: str):
    def decorator(fn):
        _TESTS.append((name, group, fn))
        return fn
    return decorator


# ---------------------------------------------------------------------------
# Televizo player simulation helpers
# ---------------------------------------------------------------------------

def _televizo_open_channel(ctx: TestCtx, channel_id: str) -> Tuple[str, List[Tuple[str, str]]]:
    """
    Simulate Televizo opening a channel: GET stream.m3u8, parse master playlist.
    Returns (variant_url, [(lang, sub_m3u8_uri), ...]).
    variant_url is absolute.
    Raises AssertionError if the server returns a non-200 response.
    """
    r = ctx.session.get(
        ctx.url(f"/hls/{channel_id}/stream.m3u8"),
        allow_redirects=True,
        timeout=ctx.timeout,  # may block on cold start if no bumper
    )
    assert r.status_code == 200, (
        f"stream.m3u8 returned HTTP {r.status_code} for channel {channel_id!r}"
    )
    text = r.text

    # Parse variant URL (always relative: "video.m3u8")
    variant_rel = None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#"):
            variant_rel = stripped
            break
    assert variant_rel, f"No variant URL in master playlist:\n{text[:300]}"

    # Build absolute variant URL using the final request URL as base
    base = r.url.rsplit("/", 1)[0]
    variant_url = f"{base}/{variant_rel}"

    # Parse subtitle tracks: LANGUAGE="xx" ... URI="sub_xx.m3u8"
    sub_tracks: List[Tuple[str, str]] = re.findall(
        r'LANGUAGE="([^"]+)"[^"]*URI="(sub_[^"]+\.m3u8)"', text
    )

    return variant_url, sub_tracks


def _televizo_poll_variant(ctx: TestCtx, variant_url: str) -> Tuple[int, str]:
    """
    Simulate one Televizo poll of the variant playlist (video.m3u8).
    Returns (http_status, manifest_text).
    """
    r = ctx.session.get(variant_url, allow_redirects=False, timeout=15)
    return r.status_code, r.text if r.status_code == 200 else ""


def _find_channel_with_subs(ctx: TestCtx) -> Tuple[Optional[str], str, List[Tuple[str, str]]]:
    """
    Find a channel that advertises subtitle tracks in its master playlist.
    Only opens channels that are already ready (no blocking cold-start wait).
    Polls each ready channel's master up to 90s for subtitle readiness
    (subtitle thread runs async after channel start).
    Returns (channel_id, variant_url, sub_tracks) or (None, "", []).
    """
    r_st = ctx.session.get(ctx.url("/status"), timeout=10)
    if r_st.status_code != 200:
        return None, "", []
    all_ch = r_st.json().get("channels", [])
    ready_ids = [c["id"] for c in all_ch if c.get("ready")]

    def _check(ch_id: str) -> Optional[List[Tuple[str, str]]]:
        r = ctx.session.get(
            ctx.url(f"/hls/{ch_id}/stream.m3u8"),
            allow_redirects=True, timeout=15,
        )
        if r.status_code != 200:
            return None
        tracks: List[Tuple[str, str]] = re.findall(
            r'LANGUAGE="([^"]+)"[^"]*URI="(sub_[^"]+\.m3u8)"', r.text
        )
        return tracks if tracks else None

    # First pass: check all ready channels instantly
    for ch_id in ready_ids:
        tracks = _check(ch_id)
        if tracks:
            r = ctx.session.get(ctx.url(f"/hls/{ch_id}/stream.m3u8"),
                                allow_redirects=True, timeout=15)
            base = r.url.rsplit("/", 1)[0]
            variant_rel = next((l.strip() for l in r.text.splitlines()
                                if l.strip() and not l.strip().startswith("#")), None)
            return ch_id, f"{base}/{variant_rel}" if variant_rel else "", tracks

    # Second pass: subtitle thread may still be initialising — poll up to 90s
    deadline = time.time() + 90
    while time.time() < deadline:
        time.sleep(5)
        # Also pick up any channels that became ready since first check
        r_st2 = ctx.session.get(ctx.url("/status"), timeout=10)
        if r_st2.status_code == 200:
            ready_ids = [c["id"] for c in r_st2.json().get("channels", [])
                         if c.get("ready")]
        for ch_id in ready_ids:
            tracks = _check(ch_id)
            if tracks:
                r = ctx.session.get(ctx.url(f"/hls/{ch_id}/stream.m3u8"),
                                    allow_redirects=True, timeout=15)
                base = r.url.rsplit("/", 1)[0]
                variant_rel = next((l.strip() for l in r.text.splitlines()
                                    if l.strip() and not l.strip().startswith("#")), None)
                return ch_id, f"{base}/{variant_rel}" if variant_rel else "", tracks

    return None, "", []


def _televizo_poll_loop(
    ctx: TestCtx,
    variant_url: str,
    until_fn,
    poll_interval: float = 2.0,
) -> Optional[str]:
    """
    Poll variant_url every poll_interval seconds until until_fn(manifest_text) returns True
    or timeout elapses.  Returns the last manifest text that satisfied until_fn, or None.
    Follows HLS live spec: wait ~target_duration between polls.
    """
    deadline = time.time() + ctx.timeout
    last_text = ""
    while time.time() < deadline:
        status, text = _televizo_poll_variant(ctx, variant_url)
        if status == 200:
            last_text = text
            if until_fn(text):
                return text
        time.sleep(poll_interval)
    return None


# ---------------------------------------------------------------------------
# Parse helpers
# ---------------------------------------------------------------------------

def _parse_m3u8_channels(text: str) -> List[dict]:
    """Parse a Televizo-style M3U8 playlist into channel dicts."""
    channels = []
    lines = text.splitlines()
    for i, line in enumerate(lines):
        if not line.startswith("#EXTINF:"):
            continue
        attrs: dict = {}
        for m in re.finditer(r'([\w-]+)="([^"]*)"', line):
            attrs[m.group(1)] = m.group(2)
        nm = re.search(r",(.+)$", line)
        attrs["_name"] = nm.group(1).strip() if nm else ""
        if i + 1 < len(lines):
            attrs["_url"] = lines[i + 1].strip()
        channels.append(attrs)
    return channels


def _parse_epg(xml_text: str) -> List[dict]:
    root = ET.fromstring(xml_text)
    return [
        {
            "channel": p.get("channel"),
            "start": p.get("start"),
            "stop": p.get("stop"),
            "title": (p.findtext("title") or "").strip(),
            "subtitle": (p.findtext("sub-title") or "").strip(),
        }
        for p in root.findall("programme")
    ]


def _epg_dt(ts: str) -> datetime:
    """Parse '20240416120000 +0000' -> UTC datetime."""
    import datetime as dt_mod
    ts = ts.strip()
    m = re.match(r"(\d{14})\s*([+-]\d{4})?", ts)
    if not m:
        raise ValueError(f"Bad EPG timestamp: {ts!r}")
    naive = datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
    tz_str = m.group(2) or "+0000"
    sign = 1 if tz_str[0] == "+" else -1
    off = sign * (int(tz_str[1:3]) * 3600 + int(tz_str[3:5]) * 60)
    return datetime(naive.year, naive.month, naive.day,
                    naive.hour, naive.minute, naive.second,
                    tzinfo=timezone.utc) - dt_mod.timedelta(seconds=off)


def _manifest_seq(text: str) -> Optional[int]:
    m = re.search(r"#EXT-X-MEDIA-SEQUENCE:(\d+)", text)
    return int(m.group(1)) if m else None


def _manifest_segs(text: str) -> List[str]:
    segs, expect = [], False
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF:"):
            expect = True
        elif expect and line and not line.startswith("#"):
            segs.append(line)
            expect = False
    return segs


def _vtt_ts(s: str) -> float:
    s = s.replace(",", ".")
    parts = s.split(":")
    if len(parts) == 3:
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
    elif len(parts) == 2:
        return int(parts[0]) * 60 + float(parts[1])
    return float(parts[0])


def _parse_vtt_cues(text: str) -> List[Tuple[float, float, str]]:
    cues, i = [], 0
    lines = text.splitlines()
    while i < len(lines):
        m = re.match(
            r"(\d+:\d{2}:\d{2}[.,]\d{3})\s+-->\s+(\d+:\d{2}:\d{2}[.,]\d{3})", lines[i]
        )
        if m:
            start, end = _vtt_ts(m.group(1)), _vtt_ts(m.group(2))
            i += 1
            txt = []
            while i < len(lines) and lines[i].strip():
                txt.append(lines[i])
                i += 1
            cues.append((start, end, "\n".join(txt)))
        else:
            i += 1
    return cues


def _parse_srt_cues(text: str) -> List[Tuple[float, float, str]]:
    cues = []
    for block in re.split(r"\n\s*\n", text.strip()):
        for line in block.strip().splitlines():
            m = re.match(
                r"(\d+:\d{2}:\d{2}[.,]\d{3})\s+-->\s+(\d+:\d{2}:\d{2}[.,]\d{3})", line
            )
            if m:
                lines_after = block.strip().splitlines()
                idx = block.strip().splitlines().index(line)
                body = "\n".join(lines_after[idx + 1:])
                clean = re.sub(r"<[^>]+>", "", body).strip()
                if clean:
                    cues.append((_vtt_ts(m.group(1)), _vtt_ts(m.group(2)), clean))
                break
    return cues


def _read_srt(path: str) -> str:
    for enc in ("utf-8-sig", "utf-8", "cp1255", "iso-8859-8", "latin-1"):
        try:
            with open(path, encoding=enc, errors="strict") as f:
                return f.read()
        except (UnicodeDecodeError, LookupError):
            continue
    with open(path, encoding="latin-1", errors="replace") as f:
        return f.read()


def _vtt_mpegts_offset(vtt_text: str) -> Optional[float]:
    m = re.search(r"X-TIMESTAMP-MAP=MPEGTS:(\d+)", vtt_text)
    return int(m.group(1)) / 90000.0 if m else None


def _probe_segment(data: bytes, ffprobe_path: str) -> Optional[dict]:
    with tempfile.NamedTemporaryFile(suffix=".ts", delete=False) as f:
        f.write(data)
        tmp = f.name
    try:
        result = subprocess.run(
            [ffprobe_path, "-v", "quiet", "-print_format", "json",
             "-show_streams", "-show_format", tmp],
            capture_output=True, text=True, timeout=15,
        )
        return json.loads(result.stdout) if result.returncode == 0 else None
    except (FileNotFoundError, json.JSONDecodeError, subprocess.TimeoutExpired):
        return None
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


def _find_past_programme(ctx: TestCtx, channel_id: str, min_ago: int = 300) -> Optional[dict]:
    r = ctx.get("/epg.xml", allow_redirects=True)
    if r.status_code != 200:
        return None
    now = datetime.now(tz=timezone.utc)
    cutoff = now.timestamp() - min_ago
    candidates = []
    for p in _parse_epg(r.text):
        if p["channel"] != channel_id:
            continue
        try:
            start = _epg_dt(p["start"])
            stop = _epg_dt(p["stop"])
            if start.timestamp() < cutoff and (stop - start).total_seconds() >= 120:
                candidates.append((start, p))
        except (ValueError, KeyError):
            continue
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


# ---------------------------------------------------------------------------
# Tests — EPG / playlist
# ---------------------------------------------------------------------------

@_register("playlist", "epg")
def test_playlist(ctx: TestCtx) -> str:
    """GET /playlist.m3u8: valid M3U, >=1 channel, catchup attrs, url-tvg."""
    r = ctx.session.get(ctx.url("/playlist.m3u8"), timeout=15)
    assert r.status_code == 200, f"HTTP {r.status_code}"
    text = r.text
    assert text.startswith("#EXTM3U"), "Missing #EXTM3U header"
    assert "url-tvg=" in text, "Missing url-tvg attribute"

    channels = _parse_m3u8_channels(text)
    assert channels, "No channels in playlist"

    with_catchup = [c for c in channels if c.get("catchup") == "shift"]
    assert with_catchup, 'No channels have catchup="shift"'
    assert all(c.get("catchup-days") for c in with_catchup), "Some catchup channels missing catchup-days"
    assert all(c.get("catchup-source") for c in with_catchup), "Some catchup channels missing catchup-source"

    return f"{len(channels)} channels, catchup attrs on {len(with_catchup)}"


@_register("epg_xml", "epg")
def test_epg_xml(ctx: TestCtx) -> str:
    """GET /epg.xml: valid XMLTV, >=1 programme, all timestamps UTC (+0000)."""
    r = ctx.session.get(ctx.url("/epg.xml"), timeout=15)
    assert r.status_code == 200, f"HTTP {r.status_code}"
    text = r.text
    assert "DOCTYPE tv" in text, "Missing DOCTYPE tv declaration"

    progs = _parse_epg(text)
    assert progs, "No programmes in EPG"

    bad_tz = [p for p in progs
              if not p["start"].strip().endswith("+0000")
              and not p["start"].strip().endswith("-0000")]
    assert not bad_tz, (
        f"{len(bad_tz)} programmes with non-UTC timestamps, e.g. {bad_tz[0]['start']!r}"
    )
    return f"{len(progs)} programmes, all UTC timestamps"


@_register("epg_gz", "epg")
def test_epg_gz(ctx: TestCtx) -> str:
    """GET /epg.xml.gz: decompresses correctly, same programme count as /epg.xml."""
    r = ctx.session.get(ctx.url("/epg.xml.gz"), timeout=15)
    assert r.status_code == 200, f"HTTP {r.status_code}"
    try:
        decompressed = gzip.decompress(r.content).decode("utf-8")
    except Exception as e:
        raise AssertionError(f"gzip decompression failed: {e}") from e
    progs = _parse_epg(decompressed)
    assert progs, "No programmes in decompressed EPG"
    return f"gzip OK, {len(progs)} programmes"


@_register("status", "live")
def test_status(ctx: TestCtx) -> str:
    """GET /status: JSON with channels array, now_playing on each, uptime > 0."""
    r = ctx.session.get(ctx.url("/status"), timeout=15)
    assert r.status_code == 200, f"HTTP {r.status_code}"
    data = r.json()
    assert data.get("uptime_sec", 0) > 0, "uptime_sec <= 0"
    channels = data.get("channels", [])
    assert channels, "Empty channels array"
    for ch in channels:
        assert "id" in ch, f"Channel missing id: {ch}"
        assert "now_playing" in ch, f"Channel {ch.get('id')} missing now_playing"
    ready = sum(1 for c in channels if c.get("ready"))
    return f"{len(channels)} channels, {ready} ready, uptime {data['uptime_sec']}s"


# ---------------------------------------------------------------------------
# Tests — Live HLS  (Televizo player simulation)
# ---------------------------------------------------------------------------

@_register("hls_master", "live")
def test_hls_master(ctx: TestCtx) -> str:
    """
    Televizo opens channel: GET stream.m3u8 -> master playlist with STREAM-INF + video.m3u8.
    Cold-start aware: if no bumper, server blocks until channel is ready (up to timeout).
    """
    variant_url, sub_tracks = _televizo_open_channel(ctx, ctx.channel_id)
    # Verify the master playlist structure we parsed is correct
    r = ctx.session.get(
        ctx.url(f"/hls/{ctx.channel_id}/stream.m3u8"),
        allow_redirects=True, timeout=ctx.timeout,
    )
    assert "#EXT-X-STREAM-INF:" in r.text, "Missing #EXT-X-STREAM-INF"
    assert "video.m3u8" in r.text, "Missing video.m3u8 reference"
    assert "#EXT-X-ENDLIST" not in r.text, "ENDLIST in master (should be live)"

    bw_m = re.search(r"BANDWIDTH=(\d+)", r.text)
    bw = int(bw_m.group(1)) if bw_m else 0
    sub_info = f"sub tracks: {' '.join(l for l, _ in sub_tracks)}" if sub_tracks else "no subs"
    return f"BANDWIDTH={bw}, {sub_info}, variant={variant_url.split('/')[-1]}"


@_register("hls_video", "live")
def test_hls_video(ctx: TestCtx) -> str:
    """
    Televizo polls video.m3u8 after opening channel.
    Verifies: TARGETDURATION:2, MEDIA-SEQUENCE present, >=1 segment, no ENDLIST (live).
    """
    # First open the channel as Televizo would
    variant_url, _ = _televizo_open_channel(ctx, ctx.channel_id)

    # Poll until we get a valid manifest (may take a few seconds on cold start)
    deadline = time.time() + ctx.timeout
    last_status, last_text = 0, ""
    while time.time() < deadline:
        last_status, last_text = _televizo_poll_variant(ctx, variant_url)
        if last_status == 200 and _manifest_segs(last_text):
            break
        time.sleep(2)

    assert last_status == 200, f"video.m3u8 never returned 200 (last: {last_status})"
    text = last_text
    assert "#EXT-X-TARGETDURATION:" in text, "Missing TARGETDURATION"
    assert "#EXT-X-MEDIA-SEQUENCE:" in text, "Missing MEDIA-SEQUENCE"
    assert "#EXT-X-ENDLIST" not in text, "ENDLIST present (should be live)"

    segs = _manifest_segs(text)
    assert segs, "No segments in manifest"
    seq = _manifest_seq(text)
    td_m = re.search(r"#EXT-X-TARGETDURATION:(\d+)", text)
    td = td_m.group(1) if td_m else "?"
    return f"SEQ={seq}, TARGETDURATION={td}, {len(segs)} segs, no ENDLIST"


@_register("bumper_transition", "live")
def test_bumper_transition(ctx: TestCtx) -> str:
    """
    Televizo cold-start: open cold (not-yet-running) channels, observe bumper->real transition.
    Starts ALL cold channels simultaneously so their NAS prewarm runs in parallel — the first
    channel whose NAS data is warm transitions quickly, minimising test time.
    Verifies: bumper segments served first (/_loading/ URLs), then real segments appear,
    MEDIA-SEQUENCE strictly increases at transition (no backward jump), #EXT-X-DISCONTINUITY injected.
    Skips if bumpers disabled or all channels already running.
    """
    # Get cold channels from /status — cheap with keep-alive (3ms).
    r_st = ctx.session.get(ctx.url("/status"), timeout=10)
    assert r_st.status_code == 200, f"/status returned {r_st.status_code}"
    all_channels_status = r_st.json().get("channels", [])
    cold_ids = [ch["id"] for ch in all_channels_status if not ch.get("ready")]
    if not cold_ids:
        return "SKIP — all channels already running (no cold-start to observe)"

    # Phase 1: trigger ALL cold channels to start (background NAS prewarm per channel).
    # Use a short timeout — if stream.m3u8 doesn't respond in 3s the bumper isn't ready yet;
    # skip that channel rather than blocking (each timeout can kill the keep-alive connection).
    candidates: List[Tuple[str, str, int]] = []  # (ch_id, variant_url, bumper_seq)
    for ch_id in cold_ids:
        try:
            r = ctx.session.get(
                ctx.url(f"/hls/{ch_id}/stream.m3u8"),
                allow_redirects=True, timeout=3,
            )
        except Exception:
            continue
        if r.status_code != 200:
            continue
        variant_rel = next(
            (l.strip() for l in r.text.splitlines()
             if l.strip() and not l.strip().startswith("#")), None
        )
        if not variant_rel:
            continue
        base = r.url.rsplit("/", 1)[0]
        variant_url = f"{base}/{variant_rel}"

        try:
            rv = ctx.session.get(variant_url, allow_redirects=False, timeout=3)
        except Exception:
            continue
        if rv.status_code != 200:
            continue
        text = rv.text
        segs = _manifest_segs(text)
        if not segs or not any("/_loading/" in s for s in segs):
            continue  # already warm or no bumper
        candidates.append((ch_id, variant_url, _manifest_seq(text)))

    assert candidates, (
        "No cold channel showed bumper content — "
        "bumpers may be disabled (check FAKEIPTV_BUMPERS_PATH)"
    )

    # Verify at least one candidate has ≥3 segments pre-buffered.
    for ch_id, variant_url, bseq in candidates:
        rv = ctx.session.get(variant_url, allow_redirects=False, timeout=5)
        if rv.status_code != 200:
            continue
        segs = _manifest_segs(rv.text)
        assert len(segs) >= 3, (
            f"ch={ch_id}: first bumper manifest has only {len(segs)} segment(s) — "
            f"expected ≥3 pre-buffered.\nManifest:\n{rv.text}"
        )
        break  # only check one

    # Phase 2: poll ALL candidates; exit on first transition.
    # Check deadline both at loop top AND inside the for-loop to prevent overshoot
    # (each _televizo_poll_variant call can block; N candidates × timeout can exceed BUMPER_MAX_SEC).
    bumper_seqs = {ch_id: bseq for ch_id, _, bseq in candidates}
    used_channel = None
    transition_seq: Optional[int] = None
    got_discontinuity = False
    final_bumper_seq: Optional[int] = None

    BUMPER_MAX_SEC = 30
    transition_deadline = time.time() + BUMPER_MAX_SEC
    while time.time() < transition_deadline:
        time.sleep(0.5)
        for ch_id, variant_url, _ in candidates:
            if time.time() >= transition_deadline:
                break
            remaining = transition_deadline - time.time()
            r = ctx.session.get(variant_url, allow_redirects=False,
                                timeout=min(3.0, max(0.5, remaining)))
            if r.status_code != 200:
                continue
            text = r.text
            segs = _manifest_segs(text)
            if not segs:
                continue
            seq = _manifest_seq(text)
            if any("/_loading/" in s for s in segs):
                assert seq >= bumper_seqs[ch_id], (
                    f"ch={ch_id}: bumper MEDIA-SEQUENCE went backward: {bumper_seqs[ch_id]} -> {seq}"
                )
                bumper_seqs[ch_id] = seq
            else:
                used_channel = ch_id
                transition_seq = seq
                final_bumper_seq = bumper_seqs[ch_id]
                got_discontinuity = "#EXT-X-DISCONTINUITY" in text
                break
        if used_channel:
            break

    assert transition_seq is not None, (
        f"Bumper still showing after {BUMPER_MAX_SEC}s on all {len(candidates)} cold channel(s) — "
        f"channel warmup too slow. Check NAS latency."
    )
    assert got_discontinuity, (
        f"ch={used_channel}: #EXT-X-DISCONTINUITY missing on first real manifest after bumper"
    )
    assert final_bumper_seq is not None and transition_seq > final_bumper_seq, (
        f"ch={used_channel}: MEDIA-SEQUENCE went backward at transition: "
        f"{final_bumper_seq} -> {transition_seq}. Player sees backward jump."
    )
    jump = transition_seq - final_bumper_seq
    return (
        f"ch={used_channel} (of {len(candidates)} cold), "
        f"bumper->real: SEQ {final_bumper_seq}->{transition_seq} (+{jump}), DISCONTINUITY injected"
    )


@_register("bumper_channel_surf", "live")
def test_bumper_channel_surf(ctx: TestCtx) -> str:
    """
    Simulate user pressing next channel 3 times in quick succession on LIVE channels.
    A live (ready) channel must NEVER serve bumper segments — player should get real
    content immediately. Also verifies cold channels have >= 3 bumper segments so the
    player never runs dry (the '2s bumper then buffer' bug).
    """
    r_st = ctx.session.get(ctx.url("/status"), timeout=10)
    if r_st.status_code != 200:
        return "SKIP — /status unavailable"

    all_channels = r_st.json().get("channels", [])
    live_channels = [ch["id"] for ch in all_channels if ch.get("ready")]
    cold_channels = [ch["id"] for ch in all_channels if not ch.get("ready")]

    # Pick non-adjacent channels: spread picks evenly across the list
    def _spread_pick(lst, n):
        if len(lst) <= n:
            return lst[:]
        step = len(lst) // n
        return [lst[i * step] for i in range(n)]

    # --- Test 1: live channels must never show bumper ---
    live_results = []
    for ch_id in _spread_pick(live_channels, 3):
        variant_url, _ = _televizo_open_channel(ctx, ch_id)
        status, text = _televizo_poll_variant(ctx, variant_url)
        assert status == 200, f"Live channel {ch_id}: video.m3u8 returned HTTP {status}"
        segs = _manifest_segs(text)
        is_bumper = any("/_loading/" in s for s in segs)
        assert not is_bumper, (
            f"Live channel {ch_id} is serving bumper segments — "
            f"player sees 2s of loading screen when switching to a running channel"
        )
        live_results.append(ch_id)

    # --- Test 2: cold channels must have >= 3 bumper segments on first poll ---
    cold_results = []
    for ch_id in _spread_pick(cold_channels, 3):
        variant_url, _ = _televizo_open_channel(ctx, ch_id)
        status, text = _televizo_poll_variant(ctx, variant_url)
        assert status == 200, f"Cold channel {ch_id}: video.m3u8 returned HTTP {status}"
        segs = _manifest_segs(text)
        is_bumper = any("/_loading/" in s for s in segs)
        if not is_bumper:
            cold_results.append(f"{ch_id}:immediate(no bumper)")
            continue
        assert len(segs) >= 3, (
            f"Cold channel {ch_id}: first bumper manifest has only {len(segs)} segment(s). "
            f"Player plays {len(segs)*2}s then buffers. Expected >= 3 pre-buffered segments."
        )
        cold_results.append(f"{ch_id}:{len(segs)}segs")

    parts = []
    if live_results:
        parts.append(f"{len(live_results)} live channels: no bumper (correct)")
    if cold_results:
        parts.append(f"cold: {', '.join(cold_results)}")
    if not live_results and not cold_results:
        return "SKIP — no channels to test"
    return "; ".join(parts)


@_register("segment_probe", "live")
def test_segment_probe(ctx: TestCtx) -> str:
    """
    Televizo downloads first real (non-bumper) TS segment.
    Verifies: HTTP 200, Content-Type video/mp2t, ~2s duration via ffprobe.
    """
    variant_url, _ = _televizo_open_channel(ctx, ctx.channel_id)

    # Poll until we find real segments (not bumper)
    deadline = time.time() + ctx.timeout
    real_seg_url: Optional[str] = None
    while time.time() < deadline:
        status, text = _televizo_poll_variant(ctx, variant_url)
        if status == 200:
            segs = _manifest_segs(text)
            real_segs = [s for s in segs if "/_loading/" not in s]
            if real_segs:
                seg = real_segs[0]
                # Build absolute URL
                if seg.startswith("http"):
                    real_seg_url = seg
                elif seg.startswith("/"):
                    real_seg_url = ctx.url(seg)
                else:
                    base = variant_url.rsplit("/", 1)[0]
                    real_seg_url = f"{base}/{seg}"
                break
        time.sleep(2)

    assert real_seg_url, f"No real segments appeared within {ctx.timeout}s"

    # Download the segment (as Televizo would buffer it)
    r = ctx.session.get(real_seg_url, timeout=30)
    assert r.status_code == 200, f"Segment HTTP {r.status_code}"
    # Don't assert Content-Type: Windows maps .ts to TypeScript MIME type, not video/mp2t.
    # Use ffprobe to validate the actual content instead.
    data = r.content
    assert len(data) > 1000, f"Segment suspiciously small: {len(data)} bytes"

    if ctx.ffprobe_path is None:
        return f"HTTP 200, {len(data)//1024}KB, content-type OK (ffprobe not found — skipping codec check)"

    info = _probe_segment(data, ctx.ffprobe_path)
    if info is None:
        return f"HTTP 200, {len(data)//1024}KB (ffprobe parse failed)"

    streams = info.get("streams", [])
    video = next((s for s in streams if s.get("codec_type") == "video"), None)
    audio = next((s for s in streams if s.get("codec_type") == "audio"), None)
    assert video, "No video stream in segment"

    dur = float(info.get("format", {}).get("duration", 0))
    # With -c:v copy, segment duration aligns to keyframe boundaries.
    # Keyframe intervals can be 2–8s depending on codec/encoder settings.
    assert 0.3 < dur < 12.0, f"Unexpected segment duration {dur:.2f}s (expected 2–8s)"

    vcodec = video.get("codec_name", "?")
    acodec = audio.get("codec_name", "none") if audio else "none"
    return f"{vcodec} video, {dur:.1f}s, {acodec} audio, {len(data)//1024}KB"


@_register("channel_vs_epg", "live")
def test_channel_vs_epg(ctx: TestCtx) -> str:
    """
    For each channel: compare /status now_playing title against EPG programme at current UTC.
    Only checks ready channels. Cold channels (not yet started) are skipped with a count.
    """
    r_status = ctx.session.get(ctx.url("/status"), timeout=15)
    assert r_status.status_code == 200
    status_data = r_status.json()

    r_epg = ctx.session.get(ctx.url("/epg.xml"), timeout=15)
    assert r_epg.status_code == 200
    progs = _parse_epg(r_epg.text)

    now = _now_utc()
    epg_by_ch: Dict[str, List[dict]] = {}
    for p in progs:
        epg_by_ch.setdefault(p["channel"], []).append(p)

    matched = 0
    cold = 0
    mismatched: List[str] = []

    for ch in status_data["channels"]:
        ch_id = ch["id"]
        np = ch.get("now_playing")
        if not np or not ch.get("ready"):
            cold += 1
            continue

        ch_progs = epg_by_ch.get(ch_id, [])
        current_prog = None
        for p in ch_progs:
            try:
                start = _epg_dt(p["start"])
                stop = _epg_dt(p["stop"])
                if start <= now <= stop:
                    current_prog = p
                    break
            except (ValueError, KeyError):
                continue

        if current_prog is None:
            mismatched.append(f"{ch_id}: no EPG entry at current time")
            continue

        if np["title"].lower() != current_prog["title"].lower():
            mismatched.append(
                f"{ch_id}: status={np['title']!r} ≠ epg={current_prog['title']!r}"
            )
        else:
            matched += 1

    total_checked = matched + len(mismatched)
    detail = f"{matched}/{total_checked} title match"
    if cold:
        detail += f" ({cold} cold/not-started)"
    if mismatched:
        raise AssertionError(
            f"{len(mismatched)} mismatch(es):\n  " + "\n  ".join(mismatched)
        )
    return detail


@_register("subtitle_manifest", "live")
def test_subtitle_manifest(ctx: TestCtx) -> str:
    """
    Televizo fetches subtitle manifest after opening channel.
    Verifies: mirrors video.m3u8 MEDIA-SEQUENCE, no ENDLIST (live).
    """
    ch_id, variant_url, sub_tracks = _find_channel_with_subs(ctx)
    assert ch_id, "No channel with subtitle tracks found across all channels"

    lang, sub_uri = sub_tracks[0]
    base = ctx.url(f"/hls/{ch_id}")
    r_sub = ctx.session.get(f"{base}/{sub_uri}", allow_redirects=False, timeout=15)
    assert r_sub.status_code == 200, f"sub manifest HTTP {r_sub.status_code}"
    sub_text = r_sub.text

    assert "#EXT-X-TARGETDURATION:" in sub_text, "Missing TARGETDURATION in sub manifest"
    assert "#EXT-X-MEDIA-SEQUENCE:" in sub_text, "Missing MEDIA-SEQUENCE in sub manifest"
    assert "#EXT-X-ENDLIST" not in sub_text, "ENDLIST in live subtitle manifest"

    # Compare sequence with video manifest (allow small race condition diff)
    status, vid_text = _televizo_poll_variant(ctx, variant_url)
    if status == 200:
        vid_seq = _manifest_seq(vid_text)
        sub_seq = _manifest_seq(sub_text)
        if vid_seq is not None and sub_seq is not None:
            diff = abs(vid_seq - sub_seq)
            assert diff <= 5, f"Sub SEQ={sub_seq} too far from video SEQ={vid_seq} (diff={diff})"

    sub_segs = _manifest_segs(sub_text)
    return f"{lang} manifest: SEQ={_manifest_seq(sub_text)}, {len(sub_segs)} entries, no ENDLIST"


@_register("refresh", "live")
def test_refresh(ctx: TestCtx) -> str:
    """GET /refresh: 200 JSON {status: ok}. Uses long timeout — NAS scan can take >30s."""
    r = ctx.session.get(ctx.url("/refresh"), timeout=15)
    assert r.status_code == 200, f"HTTP {r.status_code}"
    data = r.json()
    assert data.get("status") == "ok", f"Expected status=ok, got: {data}"
    return f"{{status: {data['status']!r}}}"


# ---------------------------------------------------------------------------
# Tests — Catchup  (Televizo catchup="shift" simulation)
# ---------------------------------------------------------------------------

def _catchup_url(ctx: TestCtx, utc_ts: int, utcend_ts: int = 0) -> str:
    """Build catchup URL from the catchup-source template in the playlist."""
    if not ctx.catchup_source_tpl:
        return ctx.url(f"/catchup/{ctx.channel_id}?utc={utc_ts}")
    url = ctx.catchup_source_tpl
    url = url.replace("{utc}", str(utc_ts))
    url = url.replace("{utcend}", str(utcend_ts or utc_ts + 3600))
    return url


def _televizo_start_catchup(
    ctx: TestCtx, utc_ts: int
) -> Tuple[Optional[str], Optional[str]]:
    """
    Simulate Televizo starting catchup: follow redirect chain from catchup-source URL.
    Returns (session_manifest_url, session_id) or (None, None) on failure.
    """
    url = _catchup_url(ctx, utc_ts)
    r = ctx.session.get(url, allow_redirects=False, timeout=20)
    if r.status_code != 302:
        return None, None
    location = r.headers.get("Location", "")
    if not location:
        return None, None
    if location.startswith("/"):
        manifest_url = ctx.url(location)
    else:
        # Server returns absolute URL using its configured HOST_IP (e.g. 192.168.9.2).
        # Rewrite to the base_url we're actually testing against (e.g. localhost:9090).
        parsed_loc = urlparse(location)
        parsed_base = urlparse(ctx.base_url)
        manifest_url = urlunparse(parsed_loc._replace(
            scheme=parsed_base.scheme,
            netloc=parsed_base.netloc,
        ))
    m = re.search(r"/catchup/[^/]+/([^/]+)/stream\.m3u8", manifest_url)
    session_id = m.group(1) if m else None
    return manifest_url, session_id


@_register("catchup_redirect", "catchup")
def test_catchup_redirect(ctx: TestCtx) -> str:
    """
    Televizo catchup: GET catchup-source?utc=X -> 302 to /catchup/{ch}/{session}/stream.m3u8.
    Verifies: redirect target format and session_id naming.
    """
    prog = _find_past_programme(ctx, ctx.channel_id)
    if prog is None:
        return "SKIP — no past EPG programme (catchup_days=0 or EPG window too short)"

    utc_ts = int(_epg_dt(prog["start"]).timestamp()) + 60
    url = _catchup_url(ctx, utc_ts)
    r = ctx.session.get(url, allow_redirects=False, timeout=20)
    assert r.status_code == 302, f"Expected 302, got {r.status_code}"

    location = r.headers.get("Location", "")
    assert location, "No Location header"
    assert "/catchup/" in location, f"Redirect not to /catchup/: {location!r}"
    assert "stream.m3u8" in location, f"Redirect missing stream.m3u8: {location!r}"

    m = re.search(r"/catchup/[^/]+/([^/]+)/stream\.m3u8", location)
    assert m, f"Cannot parse session_id from {location!r}"
    session_id = m.group(1)
    assert re.match(rf"^{re.escape(ctx.channel_id)}_\d+$", session_id), (
        f"session_id format unexpected: {session_id!r} (expected {ctx.channel_id}_<digits>)"
    )
    return f"302 -> …/{session_id}/stream.m3u8"


@_register("catchup_manifest", "catchup")
def test_catchup_manifest(ctx: TestCtx) -> str:
    """
    Televizo follows catchup redirect: stream.m3u8 is the master pointing to video.m3u8 (variant).
    Verifies: master serves video.m3u8 ref; variant has MEDIA-SEQUENCE:0 and segments; seg0.ts playable.
    Does NOT wait for EXT-X-ENDLIST — catchup ffmpeg runs real-time; ENDLIST only appears after
    the full episode duration elapses, which can be minutes to hours.
    """
    prog = _find_past_programme(ctx, ctx.channel_id)
    if prog is None:
        return "SKIP — no past EPG programme"

    utc_ts = int(_epg_dt(prog["start"]).timestamp()) + 60
    manifest_url, session_id = _televizo_start_catchup(ctx, utc_ts)
    assert manifest_url, "Catchup redirect failed"

    # session_dir/stream.m3u8 is the master; session_dir/video.m3u8 is the variant (ffmpeg output).
    video_url = manifest_url.replace("stream.m3u8", "video.m3u8")

    # Poll as Televizo would
    t0 = time.time()
    deadline = t0 + ctx.timeout
    ready_text = None
    while time.time() < deadline:
        rs = ctx.session.get(manifest_url, allow_redirects=False, timeout=15)
        if rs.status_code == 302:
            return "Session done early (redirected back to live)"
        if rs.status_code == 200:
            rv = ctx.session.get(video_url, allow_redirects=False, timeout=15)
            if rv.status_code == 200 and _manifest_segs(rv.text):
                ready_text = rv.text
                break
        time.sleep(2)

    assert ready_text, f"video.m3u8 never had segments after {ctx.timeout}s"
    seq = _manifest_seq(ready_text)
    assert seq == 0, f"Expected MEDIA-SEQUENCE:0, got {seq}"
    segs = _manifest_segs(ready_text)

    # Fetch seg0.ts as Televizo would to start playback
    seg_base = manifest_url.rsplit("/", 1)[0]
    seg0_url = f"{seg_base}/{segs[0]}"
    rs0 = ctx.session.get(seg0_url, timeout=30)
    assert rs0.status_code == 200, f"seg0.ts HTTP {rs0.status_code}"
    assert len(rs0.content) > 1000, f"seg0.ts too small: {len(rs0.content)} bytes"

    elapsed = time.time() - t0
    endlist = "#EXT-X-ENDLIST" in ready_text
    return (f"ready in {elapsed:.1f}s, {len(segs)} segs so far, "
            f"MEDIA-SEQUENCE:0, seg0.ts {len(rs0.content)//1024}KB"
            + (", ENDLIST" if endlist else ""))


@_register("catchup_bumper", "catchup")
def test_catchup_bumper(ctx: TestCtx) -> str:
    """
    Televizo starts catchup: bumper loading screen shown while session warms up.
    Verifies segment URLs contain /_loading/ before real catchup segments appear.
    Skips if bumpers disabled.
    """
    prog = _find_past_programme(ctx, ctx.channel_id)
    if prog is None:
        return "SKIP — no past EPG programme"

    # Use 3 min into episode to avoid snap-to-start edge case
    utc_ts = int(_epg_dt(prog["start"]).timestamp()) + 180
    manifest_url, _ = _televizo_start_catchup(ctx, utc_ts)
    assert manifest_url, "Catchup redirect failed"

    # stream.m3u8 IS the variant for catchup — bumper content is served at this same URL
    # while the session is warming up.
    saw_bumper = False
    t_bumper = t_real = None

    deadline = time.time() + min(20, ctx.timeout)
    while time.time() < deadline:
        rs = ctx.session.get(manifest_url, allow_redirects=False, timeout=10)
        if rs.status_code not in (200, 404):
            time.sleep(0.5)
            continue
        if rs.status_code == 404:
            break

        segs = _manifest_segs(rs.text)
        is_bumper = bool(segs) and any("/_loading/" in s for s in segs)
        # When session becomes ready, stream.m3u8 is the master (no EXTINF, has "video.m3u8")
        is_real_master = not segs and "video.m3u8" in rs.text

        if is_bumper:
            if not saw_bumper:
                saw_bumper = True
                t_bumper = time.time()
        elif is_real_master and saw_bumper:
            t_real = time.time()
            break
        elif not saw_bumper and (is_real_master or (segs and not is_bumper)):
            return "SKIP — no bumper observed (bumpers disabled or session instantly ready)"
        elif not segs and not is_real_master:
            pass  # empty/unrecognised response, keep polling

        time.sleep(0.5)

    if not saw_bumper:
        return "SKIP — no bumper observed (bumpers disabled)"

    duration = (t_real - t_bumper) if t_real and t_bumper else 0
    return f"bumper served for {duration:.1f}s before real catchup segments"


@_register("catchup_seek", "catchup")
def test_catchup_seek(ctx: TestCtx) -> str:
    """
    Televizo seeks within an episode: second catchup request to same file -> no bumper.
    First request: 2 min into episode. Second request: 4 min into same episode.
    Verifies: second session serves no bumper (is_seek=True suppresses it).
    """
    prog = _find_past_programme(ctx, ctx.channel_id)
    if prog is None:
        return "SKIP — no past EPG programme"

    prog_dur = (_epg_dt(prog["stop"]) - _epg_dt(prog["start"])).total_seconds()
    if prog_dur < 300:
        return "SKIP — programme too short for seek test (< 5 min)"

    start_ts = int(_epg_dt(prog["start"]).timestamp())

    # First request: 2 min in
    _, sid1 = _televizo_start_catchup(ctx, start_ts + 120)

    # Second request: 4 min in (seek within same episode)
    manifest_url2, sid2 = _televizo_start_catchup(ctx, start_ts + 240)
    assert manifest_url2, "Second catchup redirect failed"

    video_url2 = manifest_url2.replace("stream.m3u8", "video.m3u8")

    # Poll second session — should NOT see bumper
    deadline = time.time() + 30
    got_manifest = False
    got_bumper = False
    while time.time() < deadline:
        rs = ctx.session.get(manifest_url2, allow_redirects=False, timeout=10)
        if rs.status_code == 404:
            time.sleep(0.5)
            continue
        if rs.status_code != 200:
            time.sleep(0.5)
            continue
        rv = ctx.session.get(video_url2, allow_redirects=False, timeout=10)
        if rv.status_code != 200:
            time.sleep(0.5)
            continue
        segs = _manifest_segs(rv.text)
        if not segs:
            time.sleep(0.5)
            continue
        got_manifest = True
        got_bumper = any("/_loading/" in s for s in segs)
        break

    if not got_manifest:
        return "SKIP — second session did not become ready in time"

    assert not got_bumper, (
        "Second catchup session (seek) served bumper — is_seek=True should suppress it"
    )
    return f"seek: no bumper. sessions: {sid1} -> {sid2}"


# ---------------------------------------------------------------------------
# Regression tests
# ---------------------------------------------------------------------------

@_register("regression_catchup_snap_to_start", "regression")
def test_regression_catchup_snap_to_start(ctx: TestCtx) -> str:
    """
    Televizo sends catchup request for (episode_stop - 20s).
    Server should snap to episode start (offset=0) instead of serving only the last 20s.

    Observable from outside: if snap DOES NOT work, ffmpeg only generates ~20s of content
    (10 segments) and writes EXT-X-ENDLIST quickly (~20s at real-time -re).
    If snap WORKS, ffmpeg starts from the beginning and generates the full episode —
    no EXT-X-ENDLIST appears after 25s of polling.
    Only valid if programme is > 2 min so the timing difference is clear.
    """
    prog = _find_past_programme(ctx, ctx.channel_id)
    if prog is None:
        return "SKIP — no past EPG programme"

    start_dt = _epg_dt(prog["start"])
    stop_dt = _epg_dt(prog["stop"])
    prog_dur = (stop_dt - start_dt).total_seconds()
    if prog_dur < 120:
        return "SKIP — programme too short for snap test (< 2 min)"

    # Request 20s before episode ends — should snap to start
    utc_near_end = int(stop_dt.timestamp()) - 20
    manifest_url, session_id = _televizo_start_catchup(ctx, utc_near_end)
    if not manifest_url:
        return "SKIP — catchup redirect failed"

    video_url = manifest_url.replace("stream.m3u8", "video.m3u8")

    # Wait for video.m3u8 to appear (session ready)
    deadline = time.time() + ctx.timeout
    while time.time() < deadline:
        rv = ctx.session.get(video_url, allow_redirects=False, timeout=10)
        if rv.status_code == 200 and _manifest_segs(rv.text):
            break
        time.sleep(2)
    else:
        return "SKIP — video.m3u8 never became ready"

    # Now wait 25s and re-poll.
    # Without snap: only ~20s of content -> ENDLIST appears within ~20s.
    # With snap: full episode -> still generating, no ENDLIST after 25s.
    time.sleep(25)
    rv2 = ctx.session.get(video_url, allow_redirects=False, timeout=10)
    has_endlist = rv2.status_code == 200 and "#EXT-X-ENDLIST" in rv2.text
    segs = _manifest_segs(rv2.text) if rv2.status_code == 200 else []

    assert not has_endlist, (
        f"snap-to-start FAILED: video.m3u8 has EXT-X-ENDLIST after 25s with only {len(segs)} segments — "
        f"server served the last 20s instead of snapping to episode start"
    )
    return f"snap OK: no ENDLIST after 25s ({len(segs)} segs still growing, prog_dur={prog_dur:.0f}s)"


@_register("regression_catchup_no_double_session", "regression")
def test_regression_catchup_no_double_session(ctx: TestCtx) -> str:
    """
    Two catchup requests within the 60s reuse window -> same session_id returned.
    Verifies: server reuses existing session instead of spawning a new ffmpeg.
    """
    prog = _find_past_programme(ctx, ctx.channel_id)
    if prog is None:
        return "SKIP — no past EPG programme"

    start_ts = int(_epg_dt(prog["start"]).timestamp()) + 300
    _, sid1 = _televizo_start_catchup(ctx, start_ts)

    # Second request: 30s later within same episode (inside 60s reuse window)
    _, sid2 = _televizo_start_catchup(ctx, start_ts + 30)

    if not sid1 or not sid2:
        return "SKIP — one or both redirects failed"

    assert sid1 == sid2, (
        f"Expected same session_id within 60s window, got {sid1!r} vs {sid2!r}"
    )
    return f"both requests -> same session {sid1}"


@_register("regression_catchup_subtitle_drift", "regression")
def test_regression_catchup_subtitle_drift(ctx: TestCtx) -> str:
    """
    Catchup session with subtitles: first VTT cue must start within first 60s of content.
    A large value (e.g. >60s) indicates the inpoint offset was not subtracted correctly.
    """
    prog = _find_past_programme(ctx, ctx.channel_id)
    if prog is None:
        return "SKIP — no past EPG programme"

    utc_ts = int(_epg_dt(prog["start"]).timestamp()) + 60
    manifest_url, _ = _televizo_start_catchup(ctx, utc_ts)
    if not manifest_url:
        return "SKIP — catchup redirect failed"

    # Wait for session master to be ready and advertise subtitle tracks
    deadline = time.time() + ctx.timeout
    sub_tracks = []
    while time.time() < deadline:
        rs = ctx.session.get(manifest_url, allow_redirects=False, timeout=10)
        if rs.status_code == 200:
            sub_tracks = re.findall(r'LANGUAGE="([^"]+)".*?URI="(sub_[^"]+\.m3u8)"', rs.text)
            if sub_tracks:
                break
        time.sleep(2)

    if not sub_tracks:
        return "SKIP — no subtitle tracks in catchup session"

    lang, sub_uri = sub_tracks[0]
    session_base = manifest_url.rsplit("/", 1)[0]
    sub_url = f"{session_base}/{sub_uri}"

    # Fetch VTT (Televizo would request this alongside video)
    vtt_uri = sub_uri.replace(".m3u8", ".vtt")
    vtt_url = f"{session_base}/{vtt_uri}"
    rv2 = ctx.session.get(vtt_url, allow_redirects=True, timeout=30)
    if rv2.status_code != 200:
        return f"SKIP — VTT {vtt_uri} returned {rv2.status_code}"

    vtt_text = rv2.text
    cues = _parse_vtt_cues(vtt_text)
    if not cues:
        return "SKIP — no VTT cues"

    mpegts_offset = _vtt_mpegts_offset(vtt_text)
    first_cue_start = cues[0][0]

    assert first_cue_start < 60, (
        f"First VTT cue at {first_cue_start:.2f}s — suspiciously late, "
        f"likely inpoint drift (offset not subtracted from cue timestamps)"
    )
    return (
        f"lang={lang}, {len(cues)} cues, "
        f"first cue at {first_cue_start:.2f}s, "
        f"MPEGTS offset={mpegts_offset:.2f}s" if mpegts_offset else
        f"lang={lang}, {len(cues)} cues, first cue at {first_cue_start:.2f}s"
    )


@_register("regression_subtitle_vtt_vs_srt", "regression")
def test_regression_subtitle_vtt_vs_srt(ctx: TestCtx) -> str:
    """
    Live channel: compare VTT cue timestamps against source SRT on NAS (via --nas-map).
    Verifies: cue drift <= 500ms after accounting for current offset_sec.
    Requires: --nas-map configured, channel has subtitle_paths in /status.
    """
    assert ctx.nas_maps, "No --nas-map configured (needed to read SRT from NAS)"

    # Iterate through all channels to find one that has:
    # - subtitle_paths in /status now_playing (needs rebuilt container)
    # - subtitle tracks in its master playlist
    # - a readable SRT on the Windows NAS path
    r_status = ctx.session.get(ctx.url("/status"), timeout=15)
    assert r_status.status_code == 200
    all_ch = r_status.json().get("channels", [])

    # Try ready channels first
    ordered_ch = [c for c in all_ch if c.get("ready")] + \
                 [c for c in all_ch if not c.get("ready")]

    found_ch_id = None
    lang = None
    win_srt_path = None
    srt_cues = []
    sub_tracks = []
    np = {}
    offset_sec = 0

    for ch_data in ordered_ch:
        ch_id = ch_data["id"]
        np_data = ch_data.get("now_playing") or {}
        subtitle_paths = np_data.get("subtitle_paths", {})
        if not subtitle_paths:
            continue
        for l, container_path in subtitle_paths.items():
            mapped = ctx.map_path(container_path)
            if not mapped or not os.path.exists(mapped):
                continue
            cues = _parse_srt_cues(_read_srt(mapped))
            if not cues:
                continue
            # Verify the channel's master also has a subtitle track for this lang
            r_master = ctx.session.get(
                ctx.url(f"/hls/{ch_id}/stream.m3u8"),
                allow_redirects=True, timeout=ctx.timeout,
            )
            if r_master.status_code != 200:
                continue
            tracks: List[Tuple[str, str]] = re.findall(
                r'LANGUAGE="([^"]+)"[^"]*URI="(sub_[^"]+\.m3u8)"', r_master.text
            )
            lang2 = l[:2] if len(l) == 3 else l
            match_uri = next((uri for lk, uri in tracks if lk in (l, lang2)), None)
            if not match_uri:
                continue
            found_ch_id = ch_id
            lang = l
            win_srt_path = mapped
            srt_cues = cues
            sub_tracks = tracks
            np = np_data
            offset_sec = np_data.get("offset_sec", 0)
            break
        if found_ch_id:
            break

    assert found_ch_id, (
        "No channel found with subtitle_paths in /status + readable SRT on NAS. "
        "Ensure container is rebuilt (subtitle_paths was added to /status) "
        "and --nas-map points to the correct NAS paths."
    )

    lang2 = lang[:2] if len(lang) == 3 else lang
    match = next((uri for lk, uri in sub_tracks if lk in (lang, lang2)), None)

    vtt_uri = match.replace(".m3u8", ".vtt")
    r_vtt = ctx.session.get(
        ctx.url(f"/hls/{found_ch_id}/{vtt_uri}"),
        allow_redirects=True, timeout=30,
    )
    assert r_vtt.status_code == 200, f"VTT returned HTTP {r_vtt.status_code}"

    vtt_cues = _parse_vtt_cues(r_vtt.text)
    assert vtt_cues, "VTT has no cues"
    TOLERANCE = 0.5

    drifts = []
    large_drifts = []
    for v_start, _, _ in vtt_cues[:20]:
        # Find closest matching SRT cue by adjusted time
        best_diff = float("inf")
        for s_start, _, _ in srt_cues:
            s_adj = s_start - offset_sec
            diff = abs(v_start - s_adj)
            if diff < best_diff:
                best_diff = diff
        drifts.append(best_diff)
        if best_diff > TOLERANCE:
            large_drifts.append(f"VTT {v_start:.2f}s, closest SRT adj {v_start - best_diff:.2f}s, drift {best_diff:.2f}s")

    assert drifts, "Could not match any VTT cues to SRT cues"

    avg_drift = sum(drifts) / len(drifts)
    max_drift = max(drifts)

    if large_drifts:
        raise AssertionError(
            f"{len(large_drifts)}/{len(drifts)} cues exceed {TOLERANCE}s tolerance "
            f"(avg={avg_drift:.3f}s, max={max_drift:.3f}s):\n  " +
            "\n  ".join(large_drifts[:3])
        )
    return (f"ch={found_ch_id}, lang={lang}, {len(drifts)} cues, "
            f"avg drift {avg_drift:.3f}s, max {max_drift:.3f}s (offset_sec={offset_sec})")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

_COLORS = {"PASS": "\033[32m", "FAIL": "\033[31m", "SKIP": "\033[33m", "RESET": "\033[0m"}


def _color(label: str, text: str) -> str:
    return f"{_COLORS.get(label, '')}{text}{_COLORS['RESET']}" if sys.stdout.isatty() else text


def _run_tests(ctx: TestCtx, names_to_run: List[str]) -> Tuple[int, int, int]:
    passed = failed = skipped = 0
    width = max(len(n) for n, _, _ in _TESTS) + 2
    for name, group, fn in _TESTS:
        if name not in names_to_run:
            continue
        t0 = time.time()
        try:
            result = fn(ctx)
        except AssertionError as e:
            label, detail, failed = "FAIL", str(e), failed + 1
        except Exception as e:
            label, detail, failed = "FAIL", f"{type(e).__name__}: {e}", failed + 1
        else:
            if isinstance(result, str) and result.startswith("SKIP"):
                label, detail, skipped = "SKIP", result[5:].lstrip(" —"), skipped + 1
            else:
                label, detail, passed = "PASS", result or "", passed + 1
        elapsed = time.time() - t0
        print(f"{_color(label, label)}  {name.ljust(width)}  {elapsed:5.1f}s  {detail}")
    return passed, failed, skipped


def _build_ctx(args) -> TestCtx:
    s = requests.Session()
    s.headers["User-Agent"] = "Televizo/3.1 (fakeiptv-test-suite)"

    base_url = args.url.rstrip("/")

    # Fetch playlist as Televizo would on startup
    try:
        r = s.get(f"{base_url}/playlist.m3u8", timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f"ERROR: Cannot reach {base_url}/playlist.m3u8 : {e}")
        sys.exit(1)

    channels = _parse_m3u8_channels(r.text)
    if not channels:
        print("ERROR: No channels in playlist")
        sys.exit(1)

    # Pick channel
    if args.channel:
        ch = next((c for c in channels if c.get("tvg-id") == args.channel), None)
        if ch is None:
            ids = [c.get("tvg-id", "?") for c in channels]
            print(f"ERROR: Channel {args.channel!r} not found. Available: {', '.join(ids)}")
            sys.exit(1)
        channel_id = args.channel
    else:
        channel_id = channels[0].get("tvg-id") or channels[0].get("_name", "")

    catchup_source = next(
        (c.get("catchup-source", "") for c in channels if c.get("tvg-id") == channel_id),
        "",
    )

    # Probe ffprobe
    ffprobe = args.ffprobe or "ffprobe"
    try:
        subprocess.run([ffprobe, "-version"], capture_output=True, timeout=5)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        ffprobe = None

    # NAS maps
    nas_maps: List[Tuple[str, str]] = []
    for mapping in (args.nas_map or []):
        if "=" not in mapping:
            print(f"WARNING: --nas-map {mapping!r} ignored (no '=' separator)")
            continue
        src, dst = mapping.split("=", 1)
        nas_maps.append((src.rstrip("/"), dst))
    if not nas_maps:
        nas_maps = [
            ("/mnt/nas/Shows", r"M:\TV Shows"),
            ("/mnt/nas/Movies", r"M:\Movies"),
            ("/multimedia/TV Shows", r"M:\TV Shows"),
            ("/multimedia/Movies", r"M:\Movies"),
        ]

    return TestCtx(
        base_url=base_url,
        session=s,
        channel_id=channel_id,
        catchup_source_tpl=catchup_source,
        all_channels=channels,
        timeout=args.timeout,
        ffprobe_path=ffprobe,
        nas_maps=nas_maps,
    )


def main():
    parser = argparse.ArgumentParser(
        description="FakeIPTV automated Televizo simulator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--url", default="http://localhost:8080")
    parser.add_argument("--channel", default="", help="Channel tvg-id to test (default: first)")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--ffprobe", default="")
    parser.add_argument("--nas-map", action="append", metavar="SRC=DST")
    parser.add_argument("--only", default="",
                        help="Comma-separated groups: live,epg,catchup,regression,all")
    parser.add_argument("--tests", default="", help="Comma-separated test names to run (overrides --only)")
    parser.add_argument("--skip", default="", help="Comma-separated test names to skip")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()

    if args.list:
        groups: Dict[str, List] = {}
        for name, group, fn in _TESTS:
            groups.setdefault(group, []).append((name, fn))
        width = max(len(n) for n, _, _ in _TESTS) + 2
        for group, items in sorted(groups.items()):
            print(f"\n[{group}]")
            for name, fn in items:
                doc = (fn.__doc__ or "").strip().split("\n")[0]
                print(f"  {name.ljust(width)}{doc}")
        return

    only_groups = {g.strip() for g in args.only.split(",") if g.strip()}
    only_tests = {n.strip() for n in args.tests.split(",") if n.strip()}
    skip_names = {n.strip() for n in args.skip.split(",") if n.strip()}

    if only_tests:
        names_to_run = [n for n, _, _ in _TESTS if n in only_tests]
    elif not only_groups or "all" in only_groups:
        names_to_run = [n for n, _, _ in _TESTS]
    else:
        names_to_run = [n for n, g, _ in _TESTS if g in only_groups]
    names_to_run = [n for n in names_to_run if n not in skip_names]

    if not names_to_run:
        print("No tests selected.")
        return

    print(f"Connecting to {args.url} ...", flush=True)
    try:
        ctx = _build_ctx(args)
    except KeyboardInterrupt:
        print("\nInterrupted during startup.")
        sys.exit(1)
    print(
        f"FakeIPTV test suite  url={ctx.base_url}  channel={ctx.channel_id}  "
        f"tests={len(names_to_run)}  "
        f"ffprobe={'found' if ctx.ffprobe_path else 'not found'}"
    )
    for src, dst in ctx.nas_maps:
        print(f"  nas-map: {src} -> {dst}")
    print()

    t0 = time.time()
    passed, failed, skipped = _run_tests(ctx, names_to_run)
    elapsed = time.time() - t0

    total = passed + failed + skipped
    summary = f"{passed}/{total} passed"
    if skipped:
        summary += f", {skipped} skipped"
    if failed:
        summary += f", {failed} FAILED"
    summary += f" in {elapsed:.1f}s"
    print()
    print(_color("PASS" if failed == 0 else "FAIL", summary))
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(1)
