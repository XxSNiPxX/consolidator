#!/usr/bin/env python3
"""
inspect_all_chunks.py

- Scans rings/ for `*_book_chunk*.{meta,data}` and `*_trade_chunk*.{meta,data}`
- Optionally prints a map of symbol -> chunks
- Optionally follows all chunks and prints records filtered by --symbol

Usage:
  # list symbols per chunk
  python3 inspect_all_chunks.py --list

  # list which chunk contains a symbol
  python3 inspect_all_chunks.py --find BTCUSDT

  # follow all book chunks and print JSON records for BTCUSDT
  python3 inspect_all_chunks.py --follow --symbol BTCUSDT

Notes:
- This assumes the same meta/data layout used by your Rust MmapRing.
- The script polls meta.seq for new records and reads new index slots as they appear.
"""
import argparse
import glob
import json
import os
import struct
import time
import threading
import mmap
from typing import Dict, List, Tuple, Optional

# --- layout constants (must match your Rust code) ---
META_MAGIC = b"RINGV1\x00\x00"
META_HEADER_LEN = 40
INDEX_SLOT_SIZE = 24
INDEX_SLOT_FMT = "<Q I Q B 3x"   # off:u64, len:u32, seq:u64, kind:u8, pad3
INDEX_SLOT_LEN = struct.calcsize(INDEX_SLOT_FMT)
RECORD_HEADER_FMT = "<I B 7x Q"  # payload_len:u32, kind:u8, pad7, seq:u64
RECORD_HEADER_LEN = struct.calcsize(RECORD_HEADER_FMT)

# --- helpers to open mmaps ---
def open_mmap_read(path: str):
    f = open(path, "r+b")
    mm = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
    return f, mm

def read_meta_header(meta_mm: mmap.mmap):
    magic = meta_mm[0:8]
    if magic != META_MAGIC:
        raise RuntimeError(f"bad meta magic: {magic!r}")
    data_capacity = struct.unpack_from("<Q", meta_mm, 8)[0]
    tail_offset = struct.unpack_from("<Q", meta_mm, 16)[0]
    seq_counter = struct.unpack_from("<Q", meta_mm, 24)[0]
    index_slots = struct.unpack_from("<Q", meta_mm, 32)[0]
    return {"data_capacity": data_capacity, "tail_offset": tail_offset, "seq_counter": seq_counter, "index_slots": index_slots}

def read_index_slot(meta_mm: mmap.mmap, idx: int):
    base = META_HEADER_LEN + idx * INDEX_SLOT_SIZE
    off, ln, seq, kind = struct.unpack_from(INDEX_SLOT_FMT, meta_mm, base)
    return int(off), int(ln), int(seq), int(kind)

def read_from_data(data_mm: mmap.mmap, data_capacity: int, off:int, n:int) -> bytes:
    off = off % data_capacity
    if off + n <= data_capacity:
        return data_mm[off:off+n]
    else:
        first = data_capacity - off
        return data_mm[off:off+first] + data_mm[0:n-first]

def parse_record_at(data_mm: mmap.mmap, data_capacity: int, off: int):
    header = read_from_data(data_mm, data_capacity, off, RECORD_HEADER_LEN)
    payload_len, kind, seq = struct.unpack(RECORD_HEADER_FMT, header)
    total_len = RECORD_HEADER_LEN + payload_len
    raw = read_from_data(data_mm, data_capacity, off, total_len)
    payload_bytes = raw[RECORD_HEADER_LEN:RECORD_HEADER_LEN+payload_len]
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception:
        payload = None
    return {"payload_len": payload_len, "kind": kind, "seq": seq, "payload": payload, "raw": payload_bytes, "total_len": total_len}

# --- scanning rings ---
def find_chunks(rings_dir: str = "rings", kind_filter: str = "book"):
    patterns = [
        os.path.join(rings_dir, f"*_{kind_filter}_chunk*")
    ]
    prefixes = set()
    for pat in patterns:
        for p in glob.glob(pat + ".meta"):
            prefixes.add(p[:-5])  # strip .meta
    # also include .data-only ones if .meta missing? ignore
    return sorted(prefixes)

def build_symbol_index(prefixes: List[str]) -> Dict[str, List[Tuple[str,int,int,int]]]:
    """Return dict: symbol -> list of (prefix, slot_idx, seq, offset)"""
    idx = {}
    for prefix in prefixes:
        meta_path = prefix + ".meta"
        data_path = prefix + ".data"
        try:
            f_meta, mm_meta = open_mmap_read(meta_path)
            f_data, mm_data = open_mmap_read(data_path)
        except Exception as e:
            print(f"skip {prefix}: open error {e}")
            continue
        try:
            h = read_meta_header(mm_meta)
            data_capacity = h["data_capacity"]
            index_slots = int(h["index_slots"])
            # iterate slots and parse payload JSON to extract 'asset' or symbol
            for si in range(index_slots):
                off, ln, seq, kind = read_index_slot(mm_meta, si)
                if seq == 0:
                    continue
                try:
                    rec = parse_record_at(mm_data, data_capacity, off)
                except Exception:
                    continue
                payload = rec.get("payload")
                if isinstance(payload, dict):
                    # snapshot payloads in your code include asset as "asset" or "s"
                    symbol = payload.get("asset") or payload.get("s") or payload.get("symbol")
                    if symbol:
                        symbol = symbol.upper()
                        idx.setdefault(symbol, []).append((prefix, si, rec["seq"], off))
        finally:
            mm_meta.close(); f_meta.close()
            mm_data.close(); f_data.close()
    return idx

# --- tailing all chunks concurrently and filtering by symbol ---
class ChunkFollower(threading.Thread):
    def __init__(self, prefix: str, symbol_filter: Optional[str], callback, poll_ms: int = 200):
        super().__init__(daemon=True)
        self.prefix = prefix
        self.symbol_filter = symbol_filter.upper() if symbol_filter else None
        self.callback = callback
        self.poll_ms = poll_ms
        self.stop_requested = False
        # will be init in run
        self.meta_mm = None
        self.data_mm = None
        self.data_capacity = None
        self.last_seen_seq = 0

    def stop(self):
        self.stop_requested = True

    def run(self):
        meta_path = self.prefix + ".meta"
        data_path = self.prefix + ".data"
        try:
            f_meta, self.meta_mm = open_mmap_read(meta_path)
            f_data, self.data_mm = open_mmap_read(data_path)
        except Exception as e:
            print(f"[{self.prefix}] open failed: {e}")
            return
        try:
            header = read_meta_header(self.meta_mm)
            self.data_capacity = header["data_capacity"]
            self.last_seen_seq = header["seq_counter"]  # start from current
        except Exception as e:
            print(f"[{self.prefix}] header read failed: {e}")
            return

        # poll loop
        while not self.stop_requested:
            try:
                header = read_meta_header(self.meta_mm)
                seq_counter = header["seq_counter"]
                if seq_counter > self.last_seen_seq:
                    # new sequences available; find slots with seq > last_seen_seq
                    # We iterate all slots and pick those with seq > last_seen_seq
                    index_slots = int(header["index_slots"])
                    new_recs = []
                    for si in range(index_slots):
                        off, ln, seq, kind = read_index_slot(self.meta_mm, si)
                        if seq > self.last_seen_seq:
                            new_recs.append((si, off, ln, seq, kind))
                    # sort by seq asc
                    new_recs.sort(key=lambda x: x[3])
                    for si, off, ln, seq, kind in new_recs:
                        try:
                            rec = parse_record_at(self.data_mm, self.data_capacity, off)
                        except Exception as e:
                            print(f"[{self.prefix}] parse failure slot={si} off={off}: {e}")
                            continue
                        payload = rec.get("payload")
                        if isinstance(payload, dict):
                            # extract symbol (try keys your producer uses)
                            symbol = (payload.get("asset") or payload.get("s") or payload.get("symbol"))
                            if symbol:
                                symbol = symbol.upper()
                                if (self.symbol_filter is None) or (symbol == self.symbol_filter):
                                    out = {
                                        "prefix": self.prefix,
                                        "slot": si,
                                        "seq": rec["seq"],
                                        "kind": rec["kind"],
                                        "symbol": symbol,
                                        "payload": payload
                                    }
                                    self.callback(out)
                        else:
                            # non-json payload, send raw if no filter
                            if self.symbol_filter is None:
                                out = {
                                    "prefix": self.prefix,
                                    "slot": si,
                                    "seq": rec["seq"],
                                    "kind": rec["kind"],
                                    "payload_raw": rec["raw"]
                                }
                                self.callback(out)
                        self.last_seen_seq = max(self.last_seen_seq, rec["seq"])
                time.sleep(self.poll_ms / 1000.0)
            except Exception as e:
                print(f"[{self.prefix}] poll exception: {e}")
                time.sleep(self.poll_ms / 1000.0)

# --- CLI and main ---
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--rings-dir", default="rings", help="directory with ring files")
    p.add_argument("--kind", default="book", choices=["book", "trade"], help="which chunk kind to scan")
    p.add_argument("--list", action="store_true", help="list symbols per chunk")
    p.add_argument("--find", default=None, help="find which chunks contain this symbol")
    p.add_argument("--follow", action="store_true", help="follow all chunks and print filtered records")
    p.add_argument("--symbol", default=None, help="symbol to filter (e.g. BTCUSDT)")
    p.add_argument("--poll-ms", type=int, default=150, help="poll interval ms for follow mode")
    return p.parse_args()

def pretty_print_map(sym_map: Dict[str, List[Tuple[str,int,int,int]]]):
    for sym, lst in sorted(sym_map.items()):
        print(f"{sym}:")
        for (prefix, slot, seq, off) in lst:
            print(f"  - {prefix} slot={slot} seq={seq} off={off}")
    print(f"TOTAL symbols: {len(sym_map)}")

def main():
    args = parse_args()
    prefixes = find_chunks(args.rings_dir, kind_filter=args.kind)
    if not prefixes:
        print("no chunk prefixes found")
        return

    if args.list or args.find:
        sym_map = build_symbol_index(prefixes)
        if args.list:
            pretty_print_map(sym_map)
        if args.find:
            s = args.find.upper()
            if s in sym_map:
                print(f"{s} found in:")
                for item in sym_map[s]:
                    print(" ", item)
            else:
                print(f"{s} not found in scanned chunks")
        return

    if args.follow:
        # start a follower thread per prefix
        symbol_filter = args.symbol.upper() if args.symbol else None
        followers = []
        lock = threading.Lock()
        def callback(record):
            with lock:
                # simple stdout JSON line
                print(json.dumps(record, ensure_ascii=False))
        for p in prefixes:
            f = ChunkFollower(p, symbol_filter, callback, poll_ms=args.poll_ms)
            f.start()
            followers.append(f)
        try:
            while True:
                time.sleep(1.0)
        except KeyboardInterrupt:
            print("stopping followers...")
            for f in followers:
                f.stop()
            time.sleep(0.2)
        return

    # default: scan and print a summary map
    sym_map = build_symbol_index(prefixes)
    pretty_print_map(sym_map)

if __name__ == "__main__":
    main()
