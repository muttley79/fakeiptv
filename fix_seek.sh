#!/bin/bash
# fix_seek.sh — scan MKV/MP4 files for missing seek index, then offer to fix.
#
# MKV: mkvmerge rewrites container with proper Cue entries.
# MP4: ffmpeg -movflags faststart moves moov atom to front.
#
# Workflow: scan → confirm → fix.
# Run on WSL: bash fix_seek.sh

set -euo pipefail

SEARCH_DIRS=(
    "/mnt/m/Movies"
    "/mnt/m/TV Shows"
)

# ---------------------------------------------------------------------------
# Phase 1 — Scan (same EBML/MP4 check logic as check_seek.sh)
# ---------------------------------------------------------------------------

check_file() {
python3 - "$1" "$2" <<'PYEOF'
import sys, struct, os

path = sys.argv[1]
ext  = sys.argv[2].lower()

def read_id(data, pos):
    if pos >= len(data): return None, pos
    b = data[pos]
    if b >= 0x80: return b, pos + 1
    if b >= 0x40:
        if pos + 2 > len(data): return None, pos
        return (b << 8) | data[pos+1], pos + 2
    if b >= 0x20:
        if pos + 3 > len(data): return None, pos
        return (b << 16) | (data[pos+1] << 8) | data[pos+2], pos + 3
    if b >= 0x10:
        if pos + 4 > len(data): return None, pos
        return (b << 24) | (data[pos+1] << 16) | (data[pos+2] << 8) | data[pos+3], pos + 4
    return None, pos + 1

def read_size(data, pos):
    UNKNOWN = -1
    if pos >= len(data): return None, pos
    b = data[pos]
    if b >= 0x80: return b & 0x7F, pos + 1
    if b >= 0x40:
        if pos + 2 > len(data): return None, pos
        return ((b & 0x3F) << 8) | data[pos+1], pos + 2
    if b >= 0x20:
        if pos + 3 > len(data): return None, pos
        return ((b & 0x1F) << 16) | (data[pos+1] << 8) | data[pos+2], pos + 3
    if b >= 0x10:
        if pos + 4 > len(data): return None, pos
        return ((b & 0x0F) << 24) | (data[pos+1] << 16) | (data[pos+2] << 8) | data[pos+3], pos + 4
    if b >= 0x08:
        if pos + 5 > len(data): return None, pos
        v = ((b & 0x07) << 32) | (data[pos+1] << 24) | (data[pos+2] << 16) | (data[pos+3] << 8) | data[pos+4]
        return (UNKNOWN if v == 0x7FFFFFFFF else v), pos + 5
    if b >= 0x04:
        if pos + 6 > len(data): return None, pos
        v = ((b & 0x03) << 40) | (data[pos+1] << 32) | (data[pos+2] << 24) | (data[pos+3] << 16) | (data[pos+4] << 8) | data[pos+5]
        return (UNKNOWN if v == 0x3FFFFFFFFFF else v), pos + 6
    if b >= 0x02:
        if pos + 7 > len(data): return None, pos
        v = ((b & 0x01) << 48) | (data[pos+1] << 40) | (data[pos+2] << 32) | (data[pos+3] << 24) | (data[pos+4] << 16) | (data[pos+5] << 8) | data[pos+6]
        return (UNKNOWN if v == 0x1FFFFFFFFFFFF else v), pos + 7
    if b == 0x01:
        if pos + 8 > len(data): return None, pos
        v = (data[pos+1] << 48) | (data[pos+2] << 40) | (data[pos+3] << 32) | (data[pos+4] << 24) | (data[pos+5] << 16) | (data[pos+6] << 8) | data[pos+7]
        return (UNKNOWN if v == 0xFFFFFFFFFFFFFF else v), pos + 8
    return None, pos + 1

ID_EBML_HEADER = 0x1A45DFA3
ID_SEGMENT     = 0x18538067
ID_SEEKHEAD    = 0x114D9B74
ID_SEEK        = 0x4DBB
ID_SEEK_ID     = 0x53AB
ID_SEEK_POS    = 0x53AC
ID_CUES        = 0x1C53BB6B
ID_CLUSTER     = 0x1F43B675

def seekhead_has_valid_cues(data, start, end, seg_body_abs, file_size):
    pos = start
    while pos < end - 2:
        eid, pos = read_id(data, pos)
        esz, pos = read_size(data, pos)
        if eid is None or esz is None or esz < 0:
            break
        if eid == ID_SEEK:
            seek_id_val = None
            seek_pos_val = None
            se_pos = pos
            se_end = min(pos + esz, len(data))
            while se_pos < se_end - 2:
                sub_id, se_pos = read_id(data, se_pos)
                sub_sz, se_pos = read_size(data, se_pos)
                if sub_id is None or sub_sz is None:
                    break
                if sub_id == ID_SEEK_ID:
                    ref_id, _ = read_id(data, se_pos)
                    seek_id_val = ref_id
                elif sub_id == ID_SEEK_POS:
                    val = 0
                    for i in range(min(sub_sz, 8)):
                        val = (val << 8) | data[se_pos + i]
                    seek_pos_val = val
                se_pos += sub_sz
            if seek_id_val == ID_CUES and seek_pos_val is not None:
                abs_pos = seg_body_abs + seek_pos_val
                if abs_pos < file_size:
                    return True
        pos += esz
    return False

def mkv_has_cues(data, file_size, data_file_offset=0):
    UNKNOWN = -1
    pos = 0
    if data_file_offset > 0:
        while pos < len(data) - 4:
            eid, npos = read_id(data, pos)
            esz, npos = read_size(data, npos)
            if eid is None or esz is None:
                pos += 1
                continue
            if eid == ID_SEEKHEAD:
                end = len(data) if esz == UNKNOWN else min(npos + esz, len(data))
                if seekhead_has_valid_cues(data, npos, end, data_file_offset, file_size):
                    return True
            if esz == UNKNOWN or esz < 0:
                pos += 1
            else:
                pos = npos + esz
        return False
    eid, pos = read_id(data, pos)
    esz, pos = read_size(data, pos)
    if eid != ID_EBML_HEADER or esz is None:
        return False
    if esz != UNKNOWN:
        pos += esz
    eid, pos = read_id(data, pos)
    esz, pos = read_size(data, pos)
    if eid != ID_SEGMENT:
        return False
    seg_body_abs = pos
    seg_pos = pos
    while seg_pos < len(data) - 4:
        eid, next_pos = read_id(data, seg_pos)
        esz, next_pos = read_size(data, next_pos)
        if eid is None or esz is None:
            break
        if eid == ID_CLUSTER:
            break
        if eid == ID_SEEKHEAD:
            end = len(data) if esz == UNKNOWN else min(next_pos + esz, len(data))
            if seekhead_has_valid_cues(data, next_pos, end, seg_body_abs, file_size):
                return True
        if esz == UNKNOWN or esz < 0:
            break
        seg_pos = next_pos + esz
    return False

def mp4_is_faststart(data):
    pos = 0
    moov_pos = None
    mdat_pos = None
    while pos + 8 <= len(data):
        box_size = struct.unpack('>I', data[pos:pos+4])[0]
        box_type = data[pos+4:pos+8]
        if box_type == b'moov':
            moov_pos = pos
        elif box_type == b'mdat':
            mdat_pos = pos
        if box_size == 0:
            break
        if box_size < 8:
            break
        pos += box_size
    if moov_pos is not None and (mdat_pos is None or moov_pos < mdat_pos):
        return True
    return False

try:
    size = os.path.getsize(path)
    chunk = 131072
    tail_offset = max(0, size - chunk)
    with open(path, 'rb') as f:
        head = f.read(chunk)
        if size > chunk * 2:
            f.seek(tail_offset)
            tail = f.read(chunk)
        else:
            tail = b''
            tail_offset = 0
except Exception as e:
    print(f'ERROR: {e}')
    sys.exit(0)

if ext == 'mkv':
    if mkv_has_cues(head, size) or mkv_has_cues(tail, size, tail_offset):
        print('OK')
    else:
        print('SLOW')
elif ext == 'mp4':
    if mp4_is_faststart(head):
        print('OK')
    else:
        print('SLOW')
else:
    print('OK')
PYEOF
}

echo "=== Scanning for missing seek index ==="
echo ""

total=$(find "${SEARCH_DIRS[@]}" -type f \( -name "*.mkv" -o -name "*.mp4" \) 2>/dev/null | wc -l)
checked=0
slow_mkv=()
slow_mp4=()

while IFS= read -r -d '' file; do
    checked=$((checked + 1))
    ext="${file##*.}"
    printf "\r[%d/%d] Checking..." "$checked" "$total"

    result=$(check_file "$file" "$ext")

    if [ "$result" = "SLOW" ]; then
        if [ "$ext" = "mkv" ]; then
            slow_mkv+=("$file")
        else
            slow_mp4+=("$file")
        fi
    fi

done < <(find "${SEARCH_DIRS[@]}" -type f \( -name "*.mkv" -o -name "*.mp4" \) -print0 2>/dev/null | sort -z)

printf "\r%*s\r" 60 ""   # clear the progress line

total_slow=$(( ${#slow_mkv[@]} + ${#slow_mp4[@]} ))

echo "Scanned $total files — found $total_slow needing repair:"
echo ""

if [ ${#slow_mkv[@]} -gt 0 ]; then
    echo "  MKV (mkvmerge):"
    for f in "${slow_mkv[@]}"; do
        echo "    $(basename "$f")"
    done
fi

if [ ${#slow_mp4[@]} -gt 0 ]; then
    echo "  MP4 (ffmpeg faststart):"
    for f in "${slow_mp4[@]}"; do
        echo "    $(basename "$f")"
    done
fi

if [ "$total_slow" -eq 0 ]; then
    echo "  Nothing to fix."
    exit 0
fi

echo ""

# ---------------------------------------------------------------------------
# Phase 2 — Confirm
# ---------------------------------------------------------------------------
read -r -p "Proceed with fixes? [y/N] " answer
echo ""
case "$answer" in
    [yY][eE][sS]|[yY]) ;;
    *)
        echo "Aborted."
        exit 0
        ;;
esac

# ---------------------------------------------------------------------------
# Phase 3 — Fix
# ---------------------------------------------------------------------------
ok=0
failed=0
total_fix=$(( ${#slow_mkv[@]} + ${#slow_mp4[@]} ))
step=0

status() {
    step=$(( step + 1 ))
    printf "[%d/%d] %s\n" "$step" "$total_fix" "$1"
}

fix_mkv() {
    local src="$1"
    local tmp="${src}.tmp_fix.mkv"
    if mkvmerge -o "$tmp" "$src" > /dev/null 2>&1; then
        mv "$tmp" "$src"
        echo "       OK"
        ok=$(( ok + 1 ))
    else
        rm -f "$tmp"
        echo "       FAILED"
        failed=$(( failed + 1 ))
    fi
}

fix_mp4() {
    local src="$1"
    local tmp="${src}.tmp_fix.mp4"
    if ffmpeg -hide_banner -loglevel error -i "$src" -c copy -movflags faststart "$tmp" 2>/dev/null; then
        mv "$tmp" "$src"
        echo "       OK"
        ok=$(( ok + 1 ))
    else
        rm -f "$tmp"
        echo "       FAILED"
        failed=$(( failed + 1 ))
    fi
}

if [ ${#slow_mkv[@]} -gt 0 ]; then
    echo "=== Fixing MKV files (mkvmerge) ==="
    for f in "${slow_mkv[@]}"; do
        status "MKV: $(basename "$f")"
        fix_mkv "$f"
    done
    echo ""
fi

if [ ${#slow_mp4[@]} -gt 0 ]; then
    echo "=== Fixing MP4 files (ffmpeg faststart) ==="
    for f in "${slow_mp4[@]}"; do
        status "MP4: $(basename "$f")"
        fix_mp4 "$f"
    done
    echo ""
fi

echo "=============================="
printf "Done. OK: %d   Failed: %d\n" "$ok" "$failed"
