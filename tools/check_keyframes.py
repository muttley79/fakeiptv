"""
check_keyframes.py — Scan a media library and report keyframe seek-index status.

For each video file:
  FAST   — MKV Cues index found and parsed (no ffprobe needed)
  SLOW   — no Cues / non-MKV, ffprobe full-packet scan required
  FAILED — ffprobe timed out or crashed

Run from anywhere on Windows:
    python tools/check_keyframes.py
    python tools/check_keyframes.py "M:\\Movies" "M:\\TV Shows"
"""
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Tuple

PATHS = [r"M:\Movies", r"M:\TV Shows"]
VIDEO_EXTS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts", ".wmv"}
WORKERS = 6
FFPROBE_TIMEOUT = 60  # seconds per file

# ---------------------------------------------------------------------------
# Minimal inline EBML reader (mirrors ffprobe_utils.py — no package import)
# ---------------------------------------------------------------------------

def _ebml_read_id(data: bytes, pos: int) -> Tuple[Optional[int], int]:
    if pos >= len(data):
        return None, pos
    b = data[pos]
    if b >= 0x80:
        return b, pos + 1
    if b >= 0x40:
        if pos + 2 > len(data): return None, pos
        return (b << 8) | data[pos + 1], pos + 2
    if b >= 0x20:
        if pos + 3 > len(data): return None, pos
        return (b << 16) | (data[pos + 1] << 8) | data[pos + 2], pos + 3
    if b >= 0x10:
        if pos + 4 > len(data): return None, pos
        return (b << 24) | (data[pos + 1] << 16) | (data[pos + 2] << 8) | data[pos + 3], pos + 4
    return None, pos + 1


def _ebml_read_size(data: bytes, pos: int) -> Tuple[Optional[int], int]:
    UNKNOWN = -1
    if pos >= len(data):
        return None, pos
    b = data[pos]
    if b >= 0x80: return b & 0x7F, pos + 1
    if b >= 0x40:
        if pos + 2 > len(data): return None, pos
        return ((b & 0x3F) << 8) | data[pos + 1], pos + 2
    if b >= 0x20:
        if pos + 3 > len(data): return None, pos
        return ((b & 0x1F) << 16) | (data[pos + 1] << 8) | data[pos + 2], pos + 3
    if b >= 0x10:
        if pos + 4 > len(data): return None, pos
        return ((b & 0x0F) << 24) | (data[pos + 1] << 16) | (data[pos + 2] << 8) | data[pos + 3], pos + 4
    if b >= 0x08:
        if pos + 5 > len(data): return None, pos
        v = ((b & 0x07) << 32) | (data[pos + 1] << 24) | (data[pos + 2] << 16) | (data[pos + 3] << 8) | data[pos + 4]
        return (UNKNOWN if v == 0x7FFFFFFFF else v), pos + 5
    if b >= 0x04:
        if pos + 6 > len(data): return None, pos
        v = ((b & 0x03) << 40) | (data[pos + 1] << 32) | (data[pos + 2] << 24) | (data[pos + 3] << 16) | (data[pos + 4] << 8) | data[pos + 5]
        return (UNKNOWN if v == 0x3FFFFFFFFFF else v), pos + 6
    if b >= 0x02:
        if pos + 7 > len(data): return None, pos
        v = ((b & 0x01) << 48) | (data[pos + 1] << 40) | (data[pos + 2] << 32) | (data[pos + 3] << 24) | (data[pos + 4] << 16) | (data[pos + 5] << 8) | data[pos + 6]
        return (UNKNOWN if v == 0x1FFFFFFFFFFFF else v), pos + 7
    if b == 0x01:
        if pos + 8 > len(data): return None, pos
        v = (data[pos + 1] << 48) | (data[pos + 2] << 40) | (data[pos + 3] << 32) | (data[pos + 4] << 24) | (data[pos + 5] << 16) | (data[pos + 6] << 8) | data[pos + 7]
        return (UNKNOWN if v == 0xFFFFFFFFFFFFFF else v), pos + 8
    return None, pos + 1


_ID_EBML_HEADER    = 0x1A45DFA3
_ID_SEGMENT        = 0x18538067
_ID_SEGMENT_INFO   = 0x1549A966
_ID_TIMESTAMP_SCALE= 0x2AD7B1
_ID_SEEKHEAD       = 0x114D9B74
_ID_SEEK           = 0x4DBB
_ID_SEEK_ID        = 0x53AB
_ID_SEEK_POS       = 0x53AC
_ID_CUES           = 0x1C53BB6B
_ID_CUE_POINT      = 0xBB
_ID_CUE_TIME       = 0xB3
_ID_CLUSTER        = 0x1F43B675


def _mkv_cues_count(path: str) -> Optional[int]:
    """Return number of Cue entries in the MKV Cues index, or None if absent/broken."""
    try:
        file_size = os.path.getsize(path)
        if file_size < 65536:
            return None

        with open(path, "rb") as f:
            head = f.read(65536)

        pos = 0
        eid, pos = _ebml_read_id(head, pos)
        esz, pos = _ebml_read_size(head, pos)
        if eid != _ID_EBML_HEADER or esz is None:
            return None
        if esz != -1:
            pos += esz

        eid, pos = _ebml_read_id(head, pos)
        esz, pos = _ebml_read_size(head, pos)
        if eid != _ID_SEGMENT:
            return None
        seg_body_abs = pos

        cues_seek_pos = None
        timestamp_scale_ns = 1_000_000

        seg_pos = pos
        while seg_pos < len(head) - 4:
            eid, next_pos = _ebml_read_id(head, seg_pos)
            esz, next_pos = _ebml_read_size(head, next_pos)
            if eid is None or esz is None:
                break
            if eid == _ID_CLUSTER:
                break
            if eid == _ID_SEEKHEAD:
                sh_end = min(next_pos + esz, len(head))
                sh_pos = next_pos
                while sh_pos < sh_end - 2:
                    seek_id, sh_pos = _ebml_read_id(head, sh_pos)
                    seek_sz, sh_pos = _ebml_read_size(head, sh_pos)
                    if seek_id is None or seek_sz is None:
                        break
                    if seek_id == _ID_SEEK:
                        se_end = min(sh_pos + seek_sz, len(head))
                        entry_id = None
                        entry_pos = None
                        se_pos = sh_pos
                        while se_pos < se_end - 2:
                            sub_id, se_pos = _ebml_read_id(head, se_pos)
                            sub_sz, se_pos = _ebml_read_size(head, se_pos)
                            if sub_id is None or sub_sz is None:
                                break
                            if sub_id == _ID_SEEK_ID:
                                entry_id, _ = _ebml_read_id(head, se_pos)
                            elif sub_id == _ID_SEEK_POS:
                                val = 0
                                for i in range(min(sub_sz, 8)):
                                    val = (val << 8) | head[se_pos + i]
                                entry_pos = val
                            se_pos += sub_sz
                        if entry_id == _ID_CUES and entry_pos is not None:
                            cues_seek_pos = entry_pos
                    sh_pos += seek_sz
            elif eid == _ID_SEGMENT_INFO:
                info_end = min(next_pos + esz, len(head))
                info_pos = next_pos
                while info_pos < info_end - 2:
                    info_id, info_pos = _ebml_read_id(head, info_pos)
                    info_sz, info_pos = _ebml_read_size(head, info_pos)
                    if info_id is None or info_sz is None:
                        break
                    if info_id == _ID_TIMESTAMP_SCALE:
                        val = 0
                        for i in range(min(info_sz, 4)):
                            val = (val << 8) | head[info_pos + i]
                        timestamp_scale_ns = val
                    info_pos += info_sz
            if esz == -1 or esz < 0:
                break
            seg_pos = next_pos + esz

        if cues_seek_pos is None:
            return None

        abs_cues = seg_body_abs + cues_seek_pos
        if abs_cues >= file_size:
            return None

        with open(path, "rb") as f:
            f.seek(abs_cues)
            cues_hdr = f.read(16)
        eid, hdr_pos = _ebml_read_id(cues_hdr, 0)
        esz, hdr_pos = _ebml_read_size(cues_hdr, hdr_pos)
        if eid != _ID_CUES or esz is None or esz <= 0 or esz > 32 * 1024 * 1024:
            return None

        with open(path, "rb") as f:
            f.seek(abs_cues + hdr_pos)
            cues_data = f.read(min(esz, 32 * 1024 * 1024))

        count = 0
        cues_pos = 0
        while cues_pos < len(cues_data) - 2:
            cp_id, cues_pos = _ebml_read_id(cues_data, cues_pos)
            cp_sz, cues_pos = _ebml_read_size(cues_data, cues_pos)
            if cp_id is None or cp_sz is None:
                break
            if cp_id == _ID_CUE_POINT:
                count += 1
            cues_pos += cp_sz

        return count if count > 0 else None

    except Exception:
        return None


def _ffprobe_keyframe_count(path: str) -> Optional[int]:
    """Count video keyframes via ffprobe full-packet scan. Returns None on failure."""
    try:
        r = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-select_streams", "v:0",
                "-show_entries", "packet=flags",
                path,
            ],
            capture_output=True, text=True, timeout=FFPROBE_TIMEOUT,
        )
        packets = json.loads(r.stdout).get("packets", [])
        return sum(1 for p in packets if p.get("flags", "").startswith("K"))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Result type: ("fast"|"slow"|"failed", keyframe_count_or_None)
# ---------------------------------------------------------------------------

def check_file(path: str) -> Tuple[str, str, Optional[int]]:
    """Return (path, status, kf_count)."""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".mkv":
        count = _mkv_cues_count(path)
        if count is not None:
            return path, "fast", count
        # Cues absent or broken — fall through to ffprobe
        count = _ffprobe_keyframe_count(path)
        if count is not None:
            return path, "slow", count
        return path, "failed", None
    else:
        count = _ffprobe_keyframe_count(path)
        if count is not None:
            return path, "slow", count
        return path, "failed", None


def collect_files(roots: List[str]) -> List[str]:
    files = []
    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if not d.startswith(".")]
            for fn in filenames:
                if os.path.splitext(fn)[1].lower() in VIDEO_EXTS:
                    files.append(os.path.join(dirpath, fn))
    return sorted(files)


def main():
    roots = sys.argv[1:] if len(sys.argv) > 1 else PATHS
    print(f"Scanning: {', '.join(roots)}")
    files = collect_files(roots)
    total = len(files)
    print(f"Found {total} video files. Probing with {WORKERS} workers...\n")

    fast_files   = []
    slow_files   = []
    failed_files = []
    done = 0

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(check_file, p): p for p in files}
        for fut in as_completed(futures):
            path, status, count = fut.result()
            done += 1
            name = os.path.relpath(path, roots[0] if len(roots) == 1 else "")
            if status == "fast":
                fast_files.append((path, count))
            elif status == "slow":
                slow_files.append((path, count))
                print(f"  [SLOW]   {name}  ({count} kf)")
            else:
                failed_files.append(path)
                print(f"  [FAILED] {name}")
            # Progress ticker every 50 files
            if done % 50 == 0 or done == total:
                pct = done * 100 // total
                print(f"  ... {done}/{total} ({pct}%)")

    print("\n" + "=" * 72)
    print(f"RESULTS: {total} files total")
    print(f"  FAST   (MKV Cues index): {len(fast_files)}")
    print(f"  SLOW   (ffprobe scan)  : {len(slow_files)}")
    print(f"  FAILED (probe error)   : {len(failed_files)}")

    if slow_files:
        print("\n--- SLOW files (no Cues index — consider remuxing with mkvmerge) ---")
        for path, count in sorted(slow_files):
            print(f"  {path}  ({count} kf)")

    if failed_files:
        print("\n--- FAILED files ---")
        for path in sorted(failed_files):
            print(f"  {path}")

    print()


if __name__ == "__main__":
    main()
