"""
ffprobe_utils.py — Media probing, NAS optimization, and EBML parsing for seeking.
"""
import json
import logging
import os
import subprocess
import threading
from typing import Dict, Optional, Tuple

log = logging.getLogger(__name__)

_BITMAP_SUB_CODECS = {"hdmv_pgs_subtitle", "dvd_subtitle", "dvb_subtitle"}
_HDR_TRANSFERS = {"smpte2084", "arib-std-b67", "smpte428"}

_gop_size_cache: Dict[str, float] = {}
_DEFAULT_GOP_SEC = 5.0
_keyframe_probe_sem = threading.Semaphore(3)

_LANG2_TO_LANG3: Dict[str, str] = {
    "en": "eng", "he": "heb", "fr": "fra", "de": "deu",
    "es": "spa", "ar": "ara", "ru": "rus", "pt": "por",
    "it": "ita", "nl": "nld", "pl": "pol", "cs": "ces",
    "ja": "jpn", "ko": "kor", "zh": "zho",
}
_LANG3_TO_LANG2: Dict[str, str] = {v: k for k, v in _LANG2_TO_LANG3.items()}

_EBML_ID_EBML_HEADER     = 0x1A45DFA3
_EBML_ID_SEGMENT         = 0x18538067
_EBML_ID_SEGMENT_INFO    = 0x1549A966
_EBML_ID_TIMESTAMP_SCALE = 0x2AD7B1
_EBML_ID_SEEKHEAD        = 0x114D9B74
_EBML_ID_SEEK            = 0x4DBB
_EBML_ID_SEEK_ID         = 0x53AB
_EBML_ID_SEEK_POS        = 0x53AC
_EBML_ID_CUES            = 0x1C53BB6B
_EBML_ID_CUE_POINT       = 0xBB
_EBML_ID_CUE_TIME        = 0xB3
_EBML_ID_CUE_TRACK_POSITIONS = 0xB7
_EBML_ID_CUE_CLUSTER_POSITION = 0xF1
_EBML_ID_CLUSTER         = 0x1F43B675


def probe_file_info(path: str):
    """Return (duration_sec, audio_codec, has_text_embedded_subs, is_hdr, video_width, video_height, video_codec)."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-show_format", "-show_streams",
                path,
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        data = json.loads(result.stdout)
        duration = float(data["format"]["duration"])
        audio_codec = ""
        has_embedded_subs = False
        is_hdr = False
        video_width = 0
        video_height = 0
        video_codec = ""
        for stream in data.get("streams", []):
            ctype = stream.get("codec_type", "")
            if ctype == "video" and not is_hdr:
                if not video_width:
                    video_width = stream.get("width") or 0
                    video_height = stream.get("height") or 0
                    video_codec = stream.get("codec_name", "").lower()
                transfer = stream.get("color_transfer", "")
                if transfer in _HDR_TRANSFERS:
                    is_hdr = True
                    log.debug("HDR detected in %s (transfer=%s)", path, transfer)
            elif ctype == "audio" and not audio_codec:
                audio_codec = stream.get("codec_name", "").lower()
            elif ctype == "subtitle":
                if stream.get("codec_name", "") not in _BITMAP_SUB_CODECS:
                    has_embedded_subs = True
        return duration, audio_codec, has_embedded_subs, is_hdr, video_width, video_height, video_codec
    except Exception as e:
        log.warning("ffprobe failed for %s: %s", path, e)
        return 0.0, "", False, False, 0, 0, ""


def probe_duration(path: str) -> float:
    """Alias for backward compatibility."""
    return probe_file_info(path)[0]


def _probe_segment_start_pts(seg_path: str) -> Optional[int]:
    """Return the MPEG-TS start_pts (90kHz units) of the first video stream."""
    try:
        r = subprocess.run([
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-select_streams", "v:0",
            seg_path,
        ], capture_output=True, text=True, timeout=10)
        data = json.loads(r.stdout)
        streams = data.get("streams", [])
        if streams:
            pts = streams[0].get("start_pts")
            if pts is not None:
                return int(pts)
    except Exception as exc:
        log.debug("_probe_segment_start_pts failed for %s: %s", seg_path, exc)
    return None


def _probe_stream_start_time(path: str, stream_spec: str) -> float:
    """Return the start_time (seconds) of the first stream matching stream_spec."""
    try:
        r = subprocess.run([
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-select_streams", stream_spec,
            "-show_entries", "stream=start_time",
            path,
        ], capture_output=True, text=True, timeout=10)
        data = json.loads(r.stdout)
        streams = data.get("streams", [])
        if streams:
            st = streams[0].get("start_time")
            if st not in (None, "N/A"):
                return float(st)
    except Exception as exc:
        log.debug("_probe_stream_start_time(%s, %s) failed: %s", path, stream_spec, exc)
    return 0.0


def _nas_prewarm(path: str, inpoint: float, entry_duration: float) -> None:
    """Prime the NAS SMB disk cache for a seek to `inpoint` in `path`."""
    if inpoint <= 0 or entry_duration <= 0:
        return
    try:
        file_size = os.path.getsize(path)
        HEAD = 65536
        WARM = 512 * 1024
        with open(path, 'rb') as f:
            f.read(HEAD)
            f.seek(max(HEAD, file_size - WARM))
            f.read(WARM)
            cluster_pos = None
            if path.lower().endswith(".mkv"):
                exact = _mkv_cues_cluster_pos(path, inpoint)
                if exact is not None:
                    cluster_pos = max(HEAD, exact - WARM // 4)
                    log.debug(f"NAS prewarm MKV {path.split('/')[-1]}: exact_offset={exact}, cluster_pos={cluster_pos}")
                else:
                    log.debug(f"NAS prewarm MKV {path.split('/')[-1]}: Cues not found, using linear estimate")
            if cluster_pos is None:
                bps = file_size / entry_duration
                cluster_pos = max(HEAD, int(inpoint * bps) - WARM // 2)
            if cluster_pos < file_size - WARM * 2:
                f.seek(cluster_pos)
                f.read(WARM)
    except Exception as e:
        log.debug(f"NAS prewarm error for {path}: {e}")


def _nas_prewarm_header(path: str) -> None:
    """Prime the NAS cache for opening a file from the start (concat transition).

    Reads the container header and the tail where MKV stores its Cues index.
    No seek-cluster read needed — ffmpeg opens the file at position 0.
    """
    HEAD = 65536        # 64 KB — container header + codec params
    TAIL = 524288       # 512 KB — MKV Cues / MP4 moov index (often at end)
    try:
        file_size = os.path.getsize(path)
        with open(path, 'rb') as f:
            f.read(HEAD)
            if file_size > HEAD + TAIL:
                f.seek(file_size - TAIL)
                f.read(TAIL)
    except Exception:
        pass


def _ebml_read_id(data: bytes, pos: int) -> Tuple[Optional[int], int]:
    """Read EBML variable-length element ID. Returns (id_int, new_pos)."""
    if pos >= len(data):
        return None, pos
    b = data[pos]
    if b >= 0x80:
        return b, pos + 1
    if b >= 0x40:
        if pos + 2 > len(data):
            return None, pos
        return (b << 8) | data[pos + 1], pos + 2
    if b >= 0x20:
        if pos + 3 > len(data):
            return None, pos
        return (b << 16) | (data[pos + 1] << 8) | data[pos + 2], pos + 3
    if b >= 0x10:
        if pos + 4 > len(data):
            return None, pos
        return (b << 24) | (data[pos + 1] << 16) | (data[pos + 2] << 8) | data[pos + 3], pos + 4
    return None, pos + 1


def _ebml_read_size(data: bytes, pos: int) -> Tuple[Optional[int], int]:
    """Read EBML variable-length size. Returns (size_int, new_pos)."""
    UNKNOWN = -1
    if pos >= len(data):
        return None, pos
    b = data[pos]
    if b >= 0x80:
        return b & 0x7F, pos + 1
    if b >= 0x40:
        if pos + 2 > len(data):
            return None, pos
        return ((b & 0x3F) << 8) | data[pos + 1], pos + 2
    if b >= 0x20:
        if pos + 3 > len(data):
            return None, pos
        return ((b & 0x1F) << 16) | (data[pos + 1] << 8) | data[pos + 2], pos + 3
    if b >= 0x10:
        if pos + 4 > len(data):
            return None, pos
        return ((b & 0x0F) << 24) | (data[pos + 1] << 16) | (data[pos + 2] << 8) | data[pos + 3], pos + 4
    if b >= 0x08:
        if pos + 5 > len(data):
            return None, pos
        v = ((b & 0x07) << 32) | (data[pos + 1] << 24) | (data[pos + 2] << 16) | (data[pos + 3] << 8) | data[pos + 4]
        return (UNKNOWN if v == 0x7FFFFFFFF else v), pos + 5
    if b >= 0x04:
        if pos + 6 > len(data):
            return None, pos
        v = ((b & 0x03) << 40) | (data[pos + 1] << 32) | (data[pos + 2] << 24) | (data[pos + 3] << 16) | (data[pos + 4] << 8) | data[pos + 5]
        return (UNKNOWN if v == 0x3FFFFFFFFFF else v), pos + 6
    if b >= 0x02:
        if pos + 7 > len(data):
            return None, pos
        v = ((b & 0x01) << 48) | (data[pos + 1] << 40) | (data[pos + 2] << 32) | (data[pos + 3] << 24) | (data[pos + 4] << 16) | (data[pos + 5] << 8) | data[pos + 6]
        return (UNKNOWN if v == 0x1FFFFFFFFFFFF else v), pos + 7
    if b == 0x01:
        if pos + 8 > len(data):
            return None, pos
        v = (data[pos + 1] << 48) | (data[pos + 2] << 40) | (data[pos + 3] << 32) | (data[pos + 4] << 24) | (data[pos + 5] << 16) | (data[pos + 6] << 8) | data[pos + 7]
        return (UNKNOWN if v == 0xFFFFFFFFFFFFFF else v), pos + 8
    return None, pos + 1


def _mkv_cues_keyframe_inpoint(path: str, inpoint: float) -> Optional[float]:
    """Read MKV Cues element directly to find the keyframe ≤ inpoint."""
    try:
        file_size = os.path.getsize(path)
        if file_size < 65536:
            return None

        with open(path, "rb") as f:
            head = f.read(65536)

        pos = 0
        UNKNOWN = -1

        eid, pos = _ebml_read_id(head, pos)
        esz, pos = _ebml_read_size(head, pos)
        if eid != _EBML_ID_EBML_HEADER or esz is None:
            return None
        if esz != UNKNOWN:
            pos += esz

        eid, pos = _ebml_read_id(head, pos)
        esz, pos = _ebml_read_size(head, pos)
        if eid != _EBML_ID_SEGMENT:
            return None
        seg_body_abs = pos

        cues_seek_pos = None
        timestamp_scale_ns = 1000000

        seg_pos = pos
        while seg_pos < len(head) - 4:
            eid, next_pos = _ebml_read_id(head, seg_pos)
            esz, next_pos = _ebml_read_size(head, next_pos)
            if eid is None or esz is None:
                break
            if eid == _EBML_ID_CLUSTER:
                break

            if eid == _EBML_ID_SEEKHEAD:
                sh_end = min(next_pos + esz, len(head))
                sh_pos = next_pos
                while sh_pos < sh_end - 2:
                    seek_id, sh_pos = _ebml_read_id(head, sh_pos)
                    seek_sz, sh_pos = _ebml_read_size(head, sh_pos)
                    if seek_id is None or seek_sz is None:
                        break
                    if seek_id == _EBML_ID_SEEK:
                        seek_entry_end = min(sh_pos + seek_sz, len(head))
                        seek_entry_id = None
                        seek_entry_pos = None
                        se_pos = sh_pos
                        while se_pos < seek_entry_end - 2:
                            sub_id, se_pos = _ebml_read_id(head, se_pos)
                            sub_sz, se_pos = _ebml_read_size(head, se_pos)
                            if sub_id is None or sub_sz is None:
                                break
                            if sub_id == _EBML_ID_SEEK_ID:
                                seek_entry_id, _ = _ebml_read_id(head, se_pos)
                            elif sub_id == _EBML_ID_SEEK_POS:
                                val = 0
                                for i in range(min(sub_sz, 8)):
                                    val = (val << 8) | head[se_pos + i]
                                seek_entry_pos = val
                            se_pos += sub_sz
                        if seek_entry_id == _EBML_ID_CUES and seek_entry_pos is not None:
                            cues_seek_pos = seek_entry_pos
                    sh_pos += seek_sz

            elif eid == _EBML_ID_SEGMENT_INFO:
                info_end = min(next_pos + esz, len(head))
                info_pos = next_pos
                while info_pos < info_end - 2:
                    info_id, info_pos = _ebml_read_id(head, info_pos)
                    info_sz, info_pos = _ebml_read_size(head, info_pos)
                    if info_id is None or info_sz is None:
                        break
                    if info_id == _EBML_ID_TIMESTAMP_SCALE:
                        val = 0
                        for i in range(min(info_sz, 4)):
                            val = (val << 8) | head[info_pos + i]
                        timestamp_scale_ns = val
                    info_pos += info_sz

            if esz == UNKNOWN or esz < 0:
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
        if eid != _EBML_ID_CUES or esz is None or esz <= 0 or esz > 8 * 1024 * 1024:
            return None

        with open(path, "rb") as f:
            f.seek(abs_cues + hdr_pos)
            cues_data = f.read(min(esz, 8 * 1024 * 1024))

        best_time = None
        cues_pos = 0
        while cues_pos < len(cues_data) - 2:
            cp_id, cues_pos = _ebml_read_id(cues_data, cues_pos)
            cp_sz, cues_pos = _ebml_read_size(cues_data, cues_pos)
            if cp_id is None or cp_sz is None:
                break
            if cp_id == _EBML_ID_CUE_POINT:
                cp_end = min(cues_pos + cp_sz, len(cues_data))
                cp_pos = cues_pos
                cue_time = None
                while cp_pos < cp_end - 2:
                    cue_id, cp_pos = _ebml_read_id(cues_data, cp_pos)
                    cue_sz, cp_pos = _ebml_read_size(cues_data, cp_pos)
                    if cue_id is None or cue_sz is None:
                        break
                    if cue_id == _EBML_ID_CUE_TIME:
                        val = 0
                        for i in range(min(cue_sz, 8)):
                            val = (val << 8) | cues_data[cp_pos + i]
                        cue_time = val * timestamp_scale_ns / 1e9
                        if cue_time <= inpoint:
                            best_time = cue_time
                    cp_pos += cue_sz
            cues_pos += cp_sz

        return best_time

    except Exception:
        return None


def _mkv_cues_cluster_pos(path: str, inpoint: float) -> Optional[int]:
    """Read MKV Cues element to find the absolute byte offset of the cluster containing inpoint.

    Returns the byte offset (relative to file start) of the cluster, or None on failure.
    """
    try:
        file_size = os.path.getsize(path)
        if file_size < 65536:
            return None

        with open(path, "rb") as f:
            head = f.read(65536)

        pos = 0
        UNKNOWN = -1

        eid, pos = _ebml_read_id(head, pos)
        esz, pos = _ebml_read_size(head, pos)
        if eid != _EBML_ID_EBML_HEADER or esz is None:
            return None
        if esz != UNKNOWN:
            pos += esz

        eid, pos = _ebml_read_id(head, pos)
        esz, pos = _ebml_read_size(head, pos)
        if eid != _EBML_ID_SEGMENT:
            return None
        seg_body_abs = pos

        cues_seek_pos = None
        timestamp_scale_ns = 1000000

        seg_pos = pos
        while seg_pos < len(head) - 4:
            eid, next_pos = _ebml_read_id(head, seg_pos)
            esz, next_pos = _ebml_read_size(head, next_pos)
            if eid is None or esz is None:
                break
            if eid == _EBML_ID_CLUSTER:
                break

            if eid == _EBML_ID_SEEKHEAD:
                sh_end = min(next_pos + esz, len(head))
                sh_pos = next_pos
                while sh_pos < sh_end - 2:
                    seek_id, sh_pos = _ebml_read_id(head, sh_pos)
                    seek_sz, sh_pos = _ebml_read_size(head, sh_pos)
                    if seek_id is None or seek_sz is None:
                        break
                    if seek_id == _EBML_ID_SEEK:
                        seek_entry_end = min(sh_pos + seek_sz, len(head))
                        seek_entry_id = None
                        seek_entry_pos = None
                        se_pos = sh_pos
                        while se_pos < seek_entry_end - 2:
                            sub_id, se_pos = _ebml_read_id(head, se_pos)
                            sub_sz, se_pos = _ebml_read_size(head, se_pos)
                            if sub_id is None or sub_sz is None:
                                break
                            if sub_id == _EBML_ID_SEEK_ID:
                                seek_entry_id, _ = _ebml_read_id(head, se_pos)
                            elif sub_id == _EBML_ID_SEEK_POS:
                                val = 0
                                for i in range(min(sub_sz, 8)):
                                    val = (val << 8) | head[se_pos + i]
                                seek_entry_pos = val
                            se_pos += sub_sz
                        if seek_entry_id == _EBML_ID_CUES and seek_entry_pos is not None:
                            cues_seek_pos = seek_entry_pos
                    sh_pos += seek_sz

            elif eid == _EBML_ID_SEGMENT_INFO:
                info_end = min(next_pos + esz, len(head))
                info_pos = next_pos
                while info_pos < info_end - 2:
                    info_id, info_pos = _ebml_read_id(head, info_pos)
                    info_sz, info_pos = _ebml_read_size(head, info_pos)
                    if info_id is None or info_sz is None:
                        break
                    if info_id == _EBML_ID_TIMESTAMP_SCALE:
                        val = 0
                        for i in range(min(info_sz, 4)):
                            val = (val << 8) | head[info_pos + i]
                        timestamp_scale_ns = val
                    info_pos += info_sz

            if esz == UNKNOWN or esz < 0:
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
        if eid != _EBML_ID_CUES or esz is None or esz <= 0 or esz > 8 * 1024 * 1024:
            return None

        with open(path, "rb") as f:
            f.seek(abs_cues + hdr_pos)
            cues_data = f.read(min(esz, 8 * 1024 * 1024))

        best_cluster_pos = None
        cues_pos = 0
        while cues_pos < len(cues_data) - 2:
            cp_id, cues_pos = _ebml_read_id(cues_data, cues_pos)
            cp_sz, cues_pos = _ebml_read_size(cues_data, cues_pos)
            if cp_id is None or cp_sz is None:
                break
            if cp_id == _EBML_ID_CUE_POINT:
                cp_end = min(cues_pos + cp_sz, len(cues_data))
                cp_pos = cues_pos
                cue_time = None
                cue_cluster_pos = None
                while cp_pos < cp_end - 2:
                    cue_id, cp_pos = _ebml_read_id(cues_data, cp_pos)
                    cue_sz, cp_pos = _ebml_read_size(cues_data, cp_pos)
                    if cue_id is None or cue_sz is None:
                        break
                    if cue_id == _EBML_ID_CUE_TIME:
                        val = 0
                        for i in range(min(cue_sz, 8)):
                            val = (val << 8) | cues_data[cp_pos + i]
                        cue_time = val * timestamp_scale_ns / 1e9
                    elif cue_id == _EBML_ID_CUE_TRACK_POSITIONS:
                        ctp_end = min(cp_pos + cue_sz, len(cues_data))
                        ctp_pos = cp_pos
                        while ctp_pos < ctp_end - 2:
                            ctp_id, ctp_pos = _ebml_read_id(cues_data, ctp_pos)
                            ctp_sz, ctp_pos = _ebml_read_size(cues_data, ctp_pos)
                            if ctp_id is None or ctp_sz is None:
                                break
                            if ctp_id == _EBML_ID_CUE_CLUSTER_POSITION:
                                val = 0
                                for i in range(min(ctp_sz, 8)):
                                    val = (val << 8) | cues_data[ctp_pos + i]
                                cue_cluster_pos = val
                            ctp_pos += ctp_sz
                    cp_pos += cue_sz
                if cue_time is not None and cue_time <= inpoint and cue_cluster_pos is not None:
                    best_cluster_pos = seg_body_abs + cue_cluster_pos
            cues_pos += cp_sz

        return best_cluster_pos

    except Exception:
        return None


def _probe_gop_size(path: str) -> float:
    """Return the max keyframe interval (GOP size) in seconds, from first 10s of file."""
    if path in _gop_size_cache:
        return _gop_size_cache[path]
    try:
        r = subprocess.run([
            "ffprobe", "-v", "quiet",
            "-print_format", "json",
            "-select_streams", "v:0",
            "-show_entries", "packet=pts_time,flags",
            "-read_intervals", "0%10",
            path,
        ], capture_output=True, text=True, timeout=30)
        packets = json.loads(r.stdout).get("packets", [])
        kf_times = sorted([
            float(p["pts_time"]) for p in packets
            if p.get("flags", "").startswith("K")
            and p.get("pts_time") not in (None, "N/A")
        ])
        if len(kf_times) >= 2:
            gop = max(b - a for a, b in zip(kf_times, kf_times[1:]))
            log.debug("GOP probe %s: %.2fs (from %d keyframes)",
                      os.path.basename(path), gop, len(kf_times))
            _gop_size_cache[path] = gop
            return gop
    except Exception as exc:
        log.debug("GOP probe failed for %s: %s", os.path.basename(path), exc)
    _gop_size_cache[path] = _DEFAULT_GOP_SEC
    return _DEFAULT_GOP_SEC


def _probe_keyframe_inpoint(path: str, inpoint: float,
                            entry_duration: float = 0.0,
                            timeout: int = 15) -> float:
    """Return the timestamp of the last video keyframe at or before `inpoint`."""
    if inpoint <= 0:
        return 0.0

    # MKV Cues hint where to start the packet scan, but they can be inaccurate
    # (e.g. stale mux where CueTime doesn't match any real keyframe). Always
    # verify with a real ffprobe packet scan; use the Cues result only to
    # narrow the scan window.
    cues_hint = None
    if path.lower().endswith(".mkv"):
        cues_hint = _mkv_cues_keyframe_inpoint(path, inpoint)

    gop_size = _probe_gop_size(path)
    fallback = max(0.0, inpoint - gop_size)

    with _keyframe_probe_sem:
        try:
            # If Cues gave a hint, scan from 2 GOPs before it (catches inaccurate Cues);
            # otherwise scan from 2 GOPs before the inpoint.
            hint = cues_hint if cues_hint is not None else inpoint
            start = max(0.0, hint - max(gop_size * 2, 10.0))
            r = subprocess.run([
                "ffprobe", "-v", "quiet",
                "-print_format", "json",
                "-select_streams", "v:0",
                "-show_entries", "packet=pts_time,flags",
                "-read_intervals", f"{start:.3f}%{inpoint + 0.5:.3f}",
                path,
            ], capture_output=True, text=True, timeout=timeout)
            packets = json.loads(r.stdout).get("packets", [])
            kf_times = [
                float(p["pts_time"])
                for p in packets
                if p.get("flags", "").startswith("K")
                and p.get("pts_time") not in (None, "N/A")
            ]
            candidates = [k for k in kf_times if k <= inpoint]
            if candidates:
                actual = max(candidates)
                if abs(actual - inpoint) > 0.1:
                    log.info(
                        "Subtitle keyframe snap: %.3fs → %.3fs (Δ=%.3fs) for %s",
                        inpoint, actual, inpoint - actual,
                        os.path.basename(path),
                    )
                return actual
            log.warning(
                "Subtitle keyframe probe: no keyframes found near %.3fs in %s "
                "— applying %.2fs GOP compensation (file may lack seek index)",
                inpoint, os.path.basename(path), gop_size,
            )
        except Exception as exc:
            log.warning(
                "Subtitle keyframe probe failed for %s @ %.3fs: %s "
                "— applying %.2fs GOP compensation",
                os.path.basename(path), inpoint, exc, gop_size,
            )
    return fallback


def _lang_matches(tag: str, preferred: str) -> bool:
    """Return True if ffprobe language tag `tag` matches `preferred` (2- or 3-letter)."""
    tag = tag.lower().strip()
    preferred = preferred.lower().strip()
    if not tag:
        return False
    if tag == preferred:
        return True
    tag3 = _LANG2_TO_LANG3.get(tag, tag)
    pref3 = _LANG2_TO_LANG3.get(preferred, preferred)
    return tag3 == pref3


def _probe_audio_stream_index(path: str, preferred_lang: str = "eng") -> int:
    """Return the index of the first audio stream matching preferred_lang."""
    try:
        r = subprocess.run([
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-select_streams", "a", path,
        ], capture_output=True, text=True, timeout=10)
        streams = json.loads(r.stdout).get("streams", [])
        for i, s in enumerate(streams):
            lang = s.get("tags", {}).get("language", "")
            if _lang_matches(lang, preferred_lang):
                if i != 0:
                    log.info(
                        "Audio track probe: using stream %d (%s) for %s",
                        i, lang, os.path.basename(path),
                    )
                return i
    except Exception as exc:
        log.debug("Audio stream probe failed for %s: %s", os.path.basename(path), exc)
    return 0


def _probe_subtitle_stream_indices(path: str, langs: list) -> dict:
    """Quick ffprobe to map language codes to subtitle stream indices."""
    try:
        r = subprocess.run([
            "ffprobe", "-v", "quiet", "-print_format", "json",
            "-show_streams", "-select_streams", "s", path,
        ], capture_output=True, text=True, timeout=10)
        streams = json.loads(r.stdout).get("streams", [])
    except Exception as exc:
        log.debug("Subtitle stream probe failed for %s: %s", os.path.basename(path), exc)
        return {}
    result = {}
    for lang in langs:
        for i, s in enumerate(streams):
            if _lang_matches(s.get("tags", {}).get("language", ""), lang):
                codec = s.get("codec_name", "").lower()
                if codec not in ("hdmv_pgs_subtitle", "dvd_subtitle", "vobsub"):
                    result[lang] = i
                break
    return result
