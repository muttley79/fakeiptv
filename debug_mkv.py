#!/usr/bin/env python3
"""
Debug script: parse MKV SeekHead and report what elements are indexed,
then verify the Cues element actually exists at the referenced position.

Usage: python3 debug_mkv.py "/mnt/m/Movies/Men in Black (1997)/Men.in.Black.1997.NORDiC.1080p.WEB-DL.H.264.DD5.1-TWA-xpost.mkv"
"""
import sys, struct, os

path = sys.argv[1]

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
ID_VOID        = 0xEC

NAMES = {
    0x1A45DFA3: "EBMLHeader",
    0x18538067: "Segment",
    0x114D9B74: "SeekHead",
    0x4DBB:     "Seek",
    0x53AB:     "SeekID",
    0x53AC:     "SeekPosition",
    0x1C53BB6B: "Cues",
    0x1F43B675: "Cluster",
    0x1549A966: "Info",
    0x1654AE6B: "Tracks",
    0xEC:       "Void",
}

file_size = os.path.getsize(path)
chunk = 131072

with open(path, 'rb') as f:
    head = f.read(chunk)
    f.seek(max(0, file_size - chunk))
    tail = f.read(chunk)
    tail_offset = max(0, file_size - chunk)

print(f"File size: {file_size:,} bytes ({file_size / 1024 / 1024:.1f} MB)")
print(f"Checking first {len(head):,} bytes and last {len(tail):,} bytes")
print()

def parse_seekhead(data, start, end, seg_body_offset, label):
    print(f"  [{label}] SeekHead at offset {seg_body_offset + start:,}:")
    pos = start
    while pos < end - 2:
        eid, npos = read_id(data, pos)
        esz, npos = read_size(data, npos)
        if eid is None or esz is None or esz < 0:
            break
        if eid == ID_SEEK:
            seek_id = None
            seek_pos = None
            sp = npos
            sp_end = npos + esz
            while sp < sp_end - 2:
                sid, snpos = read_id(data, sp)
                ssz, snpos = read_size(data, snpos)
                if sid is None or ssz is None or ssz < 0:
                    break
                if sid == ID_SEEK_ID:
                    ref_id, _ = read_id(data, snpos)
                    seek_id = ref_id
                elif sid == ID_SEEK_POS:
                    val = 0
                    for i in range(min(ssz, 8)):
                        val = (val << 8) | data[snpos + i]
                    seek_pos = val
                sp = snpos + ssz
            name = NAMES.get(seek_id, f"0x{seek_id:X}" if seek_id else "?")
            print(f"    Seek: {name} at segment offset {seek_pos:,}" if seek_pos is not None else f"    Seek: {name}")
            if seek_id == ID_CUES and seek_pos is not None:
                # Try to verify Cues element actually exists at that position
                abs_pos = seg_body_offset + seek_pos
                print(f"    --> Cues absolute file offset: {abs_pos:,}")
                # Check if it falls in our tail buffer
                if abs_pos >= tail_offset and abs_pos < tail_offset + len(tail):
                    local = abs_pos - tail_offset
                    cid, _ = read_id(tail, local)
                    print(f"    --> Byte at that position: 0x{tail[local]:02X} (element ID: 0x{cid:X if cid else 0})")
                    if cid == ID_CUES:
                        print(f"    --> CONFIRMED: real Cues element present")
                    elif cid == ID_VOID:
                        print(f"    --> WARNING: Void element at Cues position — Cues NOT written!")
                    else:
                        print(f"    --> WARNING: Unexpected element at Cues position")
                elif abs_pos >= len(head):
                    print(f"    --> Position outside both head and tail buffers — cannot verify")
        pos = npos + esz

# Parse head
UNKNOWN = -1
pos = 0
eid, pos = read_id(head, pos)
esz, pos = read_size(head, pos)
print(f"EBML Header: ID=0x{eid:X}, size={esz}")
if esz != UNKNOWN and esz is not None:
    pos += esz

eid, pos = read_id(head, pos)
esz, pos = read_size(head, pos)
print(f"Segment: ID=0x{eid:X}, size={'unknown' if esz == UNKNOWN else esz}")
print()

seg_body_start = pos
seg_pos = pos
while seg_pos < len(head) - 4:
    eid, next_pos = read_id(head, seg_pos)
    esz, next_pos = read_size(head, next_pos)
    if eid is None or esz is None:
        break
    name = NAMES.get(eid, f"0x{eid:08X}")
    size_str = 'unknown' if esz == UNKNOWN else str(esz)
    print(f"  Element: {name} (size={size_str}) at file offset {seg_pos:,}")
    if eid == ID_CLUSTER:
        print("  (reached Cluster — stopping head scan)")
        break
    if eid == ID_SEEKHEAD:
        parse_seekhead(head, next_pos, min(next_pos + esz, len(head)), seg_body_start, "head")
    if esz == UNKNOWN or esz < 0:
        break
    seg_pos = next_pos + esz

# Also scan tail for a SeekHead (some files have one at the end)
print()
print(f"Scanning tail (last {len(tail):,} bytes, file offset {tail_offset:,})...")
tpos = 0
while tpos < len(tail) - 4:
    eid, tnext = read_id(tail, tpos)
    esz, tnext = read_size(tail, tnext)
    if eid is None or esz is None:
        tpos += 1
        continue
    if eid == ID_SEEKHEAD:
        parse_seekhead(tail, tnext, min(tnext + esz, len(tail)), tail_offset, "tail")
        tpos = tnext + (0 if esz < 0 else esz)
    elif eid == ID_CUES:
        print(f"  Found Cues element in tail at file offset {tail_offset + tpos:,}")
        tpos = tnext + (0 if esz < 0 else esz)
    else:
        tpos += 1
