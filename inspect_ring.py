#!/usr/bin/env python3
"""
inspect_ring.py

Usage:
    python3 inspect_ring.py rings/binance_book_chunk0 --list
    python3 inspect_ring.py rings/binance_book_chunk0 --dump-json --limit 50
    python3 inspect_ring.py rings/binance_book_chunk0 --seq-range 1000 1100

This tool reads the .meta and .data files produced by your Rust MmapRing.
It parses:
 - meta header: magic, data_capacity, tail_offset, seq_counter, index_slots
 - index slots: (off:u64, len:u32, seq:u64, kind:u8)
 - data records: (payload_len:u32, kind:u8, 7 bytes pad, seq:u64, payload bytes)
"""
import argparse
import json
import os
import struct
import mmap
from typing import List, Tuple, Optional

META_MAGIC = b"RINGV1\x00\x00"
META_HEADER_LEN = 40
INDEX_SLOT_SIZE = 24
RECORD_HEADER_FMT = "<IB7xQ"  # payload_len:u32, kind:u8, pad7, seq:u64
RECORD_HEADER_LEN = struct.calcsize(RECORD_HEADER_FMT)  # should be 20
INDEX_SLOT_FMT = "<Q I Q B 3x"  # off:u64, len:u32, seq:u64, kind:u8, pad3
INDEX_SLOT_LEN = struct.calcsize(INDEX_SLOT_FMT)  # should be 24

def open_mmap(path: str, for_write: bool=False):
    f = open(path, "r+b" if for_write else "rb")
    mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE if for_write else mmap.ACCESS_READ)
    return f, mm

def read_meta_header(meta_mm: mmap.mmap):
    magic = meta_mm[0:8]
    if magic != META_MAGIC:
        raise RuntimeError(f"meta magic mismatch: {magic!r}")
    data_capacity = struct.unpack_from("<Q", meta_mm, 8)[0]
    tail_offset = struct.unpack_from("<Q", meta_mm, 16)[0]
    seq_counter = struct.unpack_from("<Q", meta_mm, 24)[0]
    index_slots = struct.unpack_from("<Q", meta_mm, 32)[0]
    return {
        "data_capacity": data_capacity,
        "tail_offset": tail_offset,
        "seq_counter": seq_counter,
        "index_slots": index_slots,
    }

def read_index_slot(meta_mm: mmap.mmap, idx: int) -> Tuple[int,int,int,int]:
    """
    Returns (off:u64, ln:u32, seq:u64, kind:u8)
    """
    base = META_HEADER_LEN + idx * INDEX_SLOT_SIZE
    off, ln, seq, kind = struct.unpack_from(INDEX_SLOT_FMT, meta_mm, base)
    return int(off), int(ln), int(seq), int(kind)

def read_from_data(data_mm: mmap.mmap, data_capacity: int, off: int, n: int) -> bytes:
    """Read n bytes starting at off, handling wrap-around"""
    off = off % data_capacity
    if off + n <= data_capacity:
        return data_mm[off:off+n]
    else:
        first = data_capacity - off
        return data_mm[off:off+first] + data_mm[0:n-first]

def parse_record_at(data_mm: mmap.mmap, data_capacity: int, off: int):
    """Parse record header + payload from data mmap at offset (wrap-aware)"""
    # read header first
    header_bytes = read_from_data(data_mm, data_capacity, off, RECORD_HEADER_LEN)
    payload_len, kind, seq = struct.unpack(RECORD_HEADER_FMT, header_bytes)
    total_len = RECORD_HEADER_LEN + payload_len
    raw = read_from_data(data_mm, data_capacity, off, total_len)
    # parse payload bytes slice
    payload_bytes = raw[RECORD_HEADER_LEN:RECORD_HEADER_LEN+payload_len]
    # try decode as utf-8 JSON
    payload = None
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception:
        # keep raw bytes if not JSON
        payload = payload_bytes
    return {
        "payload_len": payload_len,
        "kind": kind,
        "seq": seq,
        "payload": payload,
        "raw_payload_bytes": payload_bytes,
        "total_len": total_len,
        "raw_header": header_bytes
    }

def inspect_ring(prefix: str, list_only=False, dump_json=False, limit: Optional[int]=None,
                 seq_from: Optional[int]=None, seq_to: Optional[int]=None):
    meta_path = f"{prefix}.meta"
    data_path = f"{prefix}.data"
    if not os.path.exists(meta_path) or not os.path.exists(data_path):
        raise FileNotFoundError(f"meta or data not found for prefix {prefix}")

    meta_f, meta_mm = open_mmap(meta_path)
    data_f, data_mm = open_mmap(data_path)

    try:
        header = read_meta_header(meta_mm)
    except Exception as e:
        meta_mm.close(); meta_f.close(); data_mm.close(); data_f.close()
        raise

    data_capacity = header["data_capacity"]
    tail_offset = header["tail_offset"]
    seq_counter = header["seq_counter"]
    index_slots = header["index_slots"]

    print(f"RING: {prefix}")
    print(f" data_capacity: {data_capacity}")
    print(f" tail_offset:   {tail_offset}")
    print(f" seq_counter:   {seq_counter}")
    print(f" index_slots:   {index_slots}")
    print("")

    # collect index slots
    slots = []
    for i in range(index_slots):
        off, ln, seq, kind = read_index_slot(meta_mm, i)
        if seq == 0:
            continue
        slots.append((i, off, ln, seq, kind))
    if not slots:
        print("no indexed records found (all seq==0)")
        meta_mm.close(); meta_f.close(); data_mm.close(); data_f.close()
        return

    # sort by seq asc (older -> newer)
    slots.sort(key=lambda x: x[3])

    # filter by seq range
    if seq_from is not None:
        slots = [s for s in slots if s[3] >= seq_from]
    if seq_to is not None:
        slots = [s for s in slots if s[3] <= seq_to]

    if list_only:
        print("index_slots with seq (sorted by seq):")
        for i, off, ln, seq, kind in slots[:limit]:
            print(f" idx={i:6d} seq={seq:12d} kind={kind} off={off:8d} len={ln:8d}")
        meta_mm.close(); meta_f.close(); data_mm.close(); data_f.close()
        return

    out = []
    count = 0
    for i, off, ln, seq, kind in slots:
        if limit is not None and count >= limit:
            break
        try:
            rec = parse_record_at(data_mm, data_capacity, off)
        except Exception as e:
            print(f"failed to parse record at slot {i} (off={off}): {e}")
            continue
        # basic sanity: seq must match index slot seq
        if int(rec["seq"]) != int(seq):
            print(f"warning: slot seq {seq} != record seq {rec['seq']} (slot idx={i}, off={off})")
        summary = {
            "slot_idx": i,
            "slot_off": off,
            "slot_len": ln,
            "slot_seq": seq,
            "record_seq": rec["seq"],
            "kind": rec["kind"],
            "payload_len": rec["payload_len"],
        }
        if dump_json:
            # include full payload (if JSON) or base64 of raw bytes
            payload = rec["payload"]
            if isinstance(payload, (bytes, bytearray)):
                # not JSON; present hex preview
                try:
                    payload_preview = payload.decode("utf-8", errors="replace")[:200]
                except Exception:
                    payload_preview = payload.hex()[:200]
                summary["payload_preview"] = payload_preview
                summary["payload_raw_hex"] = payload.hex()
            else:
                summary["payload"] = payload
            out.append(summary)
        else:
            # print one-line preview
            payload_preview = None
            if isinstance(rec["payload"], (bytes, bytearray)):
                try:
                    payload_preview = rec["payload"].decode("utf-8", errors="replace")[:200]
                except Exception:
                    payload_preview = rec["raw_payload_bytes"].hex()[:200]
            else:
                payload_preview = json.dumps(rec["payload"], ensure_ascii=False)[:200]
            print(f"slot={i:6d} seq={seq:12d} kind={rec['kind']:2d} len={rec['payload_len']:6d} off={off:8d} preview={payload_preview}")
        count += 1

    if dump_json:
        # print JSON array
        print(json.dumps(out, indent=2, ensure_ascii=False))

    meta_mm.close(); meta_f.close(); data_mm.close(); data_f.close()

def parse_args():
    p = argparse.ArgumentParser(description="Inspect mmap ring (.meta/.data) files")
    p.add_argument("prefix", help="ring prefix, e.g. rings/binance_book_chunk0")
    p.add_argument("--list", dest="list_only", action="store_true", help="list index slots (no payload parsing)")
    p.add_argument("--dump-json", dest="dump_json", action="store_true", help="dump parsed payloads as JSON")
    p.add_argument("--limit", type=int, default=None, help="limit number of results")
    p.add_argument("--seq-range", nargs=2, type=int, metavar=("FROM","TO"), default=None, help="filter by seq range (inclusive)")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()
    seq_from = seq_to = None
    if args.seq_range:
        seq_from, seq_to = args.seq_range
    inspect_ring(args.prefix, list_only=args.list_only, dump_json=args.dump_json,
                 limit=args.limit, seq_from=seq_from, seq_to=seq_to)
