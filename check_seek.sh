#!/bin/bash
# check_seek.sh — scan MKV/MP4 files for missing seek index.
#
# MKV: parses the EBML SeekHead structure to check if Cues element is referenced.
#      Does NOT rely on raw byte search (which gives false results on video data).
# MP4: parses the box structure to check if moov comes before mdat (faststart).
#
# Run on WSL: bash check_seek.sh

SEARCH_DIRS=(
    "/mnt/m/Movies"
    "/mnt/m/TV Shows"
)

slow_files=()
ok_count=0
slow_count=0
checked=0

total=$(find "${SEARCH_DIRS[@]}" -type f \( -name "*.mkv" -o -name "*.mp4" \) 2>/dev/null | wc -l)
echo "Checking $total files..."
echo ""

check_file() {
    python3 - "$1" "$2" <<'PYEOF'
import sys, struct, os

path = sys.argv[1]
ext  = sys.argv[2].lower()

# ---------------------------------------------------------------------------
# EBML helpers (MKV)
# ---------------------------------------------------------------------------

def read_id(data, pos):
    """Read EBML variable-length element ID. Returns (id_int, new_pos)."""
    if pos >= len(data):
        return None, pos
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
    """Read EBML variable-length element size. Returns (size_int, new_pos).
    Returns (None, pos) on error. Unknown size (all data bits set) returns (UNKNOWN, new_pos)."""
    UNKNOWN = -1  # sentinel for EBML unknown-size marker
    if pos >= len(data):
        return None, pos
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

# Known EBML element IDs
ID_EBML_HEADER = 0x1A45DFA3
ID_SEGMENT     = 0x18538067
ID_SEEKHEAD    = 0x114D9B74
ID_SEEK        = 0x4DBB
ID_SEEK_ID     = 0x53AB      # SeekID: contains the ID of the referenced element
ID_SEEK_POS    = 0x53AC      # SeekPosition: byte offset of the referenced element
ID_CUES        = 0x1C53BB6B
ID_VOID        = 0xEC
ID_CLUSTER     = 0x1F43B675  # marks end of header area in segment

def seekhead_has_valid_cues(data, start, end, seg_body_abs, file_size):
    """
    Parse a SeekHead block and return True if it references the Cues element
    AND the referenced position is actually within the file.
    """
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
                    return True  # Cues reference exists and points inside the file
        pos += esz
    return False

def mkv_has_cues(data, file_size, data_file_offset=0):
    """
    Parse MKV EBML structure to check if a SeekHead references the Cues element
    at a position that actually exists within the file.
    data_file_offset: byte offset where `data` starts within the file (0 for head, tail_offset for tail).
    """
    UNKNOWN = -1
    pos = 0

    # If this is a tail buffer we're re-scanning, look for SeekHead directly
    # (skip the EBML header / Segment preamble search for tail chunks)
    if data_file_offset > 0:
        # Scan for SeekHead anywhere in the tail chunk
        while pos < len(data) - 4:
            eid, npos = read_id(data, pos)
            esz, npos = read_size(data, npos)
            if eid is None or esz is None:
                pos += 1
                continue
            if eid == ID_SEEKHEAD:
                end = len(data) if esz == UNKNOWN else min(npos + esz, len(data))
                seg_body_abs = data_file_offset  # approximate; good enough for offset check
                if seekhead_has_valid_cues(data, npos, end, seg_body_abs, file_size):
                    return True
            if esz == UNKNOWN or esz < 0:
                pos += 1
            else:
                pos = npos + esz
        return False

    # Head chunk: parse properly from the beginning
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

    seg_body_abs = pos  # absolute file offset of Segment body start

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

# ---------------------------------------------------------------------------
# MP4 box structure check
# ---------------------------------------------------------------------------

def mp4_is_faststart(data):
    """
    Parse MP4 box structure to check if moov comes before mdat.
    moov-after-mdat means the player must seek to end-of-file before playback.
    """
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
            break  # box extends to EOF
        if box_size < 8:
            break  # corrupt
        pos += box_size
    # moov before mdat = faststart; moov not found in first chunk = likely at end
    if moov_pos is not None and (mdat_pos is None or moov_pos < mdat_pos):
        return True
    return False

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

try:
    size = os.path.getsize(path)
    chunk = 131072  # 128KB
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

while IFS= read -r -d '' file; do
    checked=$((checked + 1))
    name=$(basename "$file")
    ext="${file##*.}"

    printf "[%d/%d] Checking: %s" "$checked" "$total" "$name"

    result=$(check_file "$file" "$ext")

    if [ "$result" = "SLOW" ]; then
        slow_count=$((slow_count + 1))
        slow_files+=("$file")
        printf "\r[%d/%d] NO INDEX: %s\n" "$checked" "$total" "$name"
    elif [ "$result" = "OK" ]; then
        ok_count=$((ok_count + 1))
        printf "\r[%d/%d] OK:       %s\n" "$checked" "$total" "$name"
    else
        ok_count=$((ok_count + 1))
        printf "\r[%d/%d] %-10s%s\n" "$checked" "$total" "$result" "$name"
    fi

done < <(find "${SEARCH_DIRS[@]}" -type f \( -name "*.mkv" -o -name "*.mp4" \) -print0 2>/dev/null | sort -z)

echo ""
echo "=============================="
printf "OK: %d   NO INDEX: %d   Total: %d\n" "$ok_count" "$slow_count" "$total"
echo ""

if [ ${#slow_files[@]} -gt 0 ]; then
    echo "Files that need fixing:"
    for f in "${slow_files[@]}"; do
        echo "  $f"
    done
    echo ""
    echo "Fix commands:"
    for f in "${slow_files[@]}"; do
        dir=$(dirname "$f")
        base=$(basename "$f")
        name_no_ext="${base%.*}"
        ext="${base##*.}"
        echo "  mkvmerge -o \"${dir}/${name_no_ext}_fixed.${ext}\" \"${f}\""
    done
fi
