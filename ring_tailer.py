#!/usr/bin/env python3
# ring_tailer.py
"""
Tail ring files for a given asset. Supports two modes:
- fast path: uses index slots stored in .meta (if present)
- fallback: full data-region scan (resynchronizing heuristics) if index not present

Usage:
    python3 ring_tailer.py find BTCUSDT
    python3 ring_tailer.py follow BTCUSDT
    python3 ring_tailer.py follow BTCUSDT --prefix rings/binance_book_chunk0
"""

import os
import mmap
import struct
import json
import time
import argparse
from typing import Optional, Tuple, List, Generator

META_HEADER_SIZE = 32
INDEX_SLOT_SIZE = 24

META_OFF_MAGIC = 0
META_OFF_DATA_CAP = 8
META_OFF_TAIL = 16
META_OFF_SEQ = 24
META_OFF_INDEX_SLOTS_TRY = 32  # some implementations write index_slots at offset 32

# Index slot layout (24 bytes):
# 0: off u64
# 8: ln u32
# 12: seq u64
# 20: kind u8

class RingReader:
    def __init__(self, prefix: str):
        self.prefix = prefix
        self.meta_path = f"{prefix}.meta"
        self.data_path = f"{prefix}.data"

        if not os.path.exists(self.meta_path) or not os.path.exists(self.data_path):
            raise FileNotFoundError(f"Missing meta/data for prefix {prefix}")

        self._meta_f = open(self.meta_path, "rb")
        self._data_f = open(self.data_path, "rb")
        self.meta = mmap.mmap(self._meta_f.fileno(), 0, access=mmap.ACCESS_READ)
        self.data = mmap.mmap(self._data_f.fileno(), 0, access=mmap.ACCESS_READ)
        self.data_capacity = self.data.size()

        if self.meta.size() < META_HEADER_SIZE:
            raise RuntimeError(f"Meta file too small: {self.meta.size()} < {META_HEADER_SIZE}")

        self.magic = self._read_bytes(META_OFF_MAGIC, 8)
        self.data_cap_from_meta = self._read_u64(META_OFF_DATA_CAP)
        self.tail = self._read_u64(META_OFF_TAIL)
        self.seq = self._read_u64(META_OFF_SEQ)

        # attempt to read index_slots from offset 32
        self.index_slots = None
        if self.meta.size() >= (META_OFF_INDEX_SLOTS_TRY + 8):
            try:
                maybe_slots = self._read_u64(META_OFF_INDEX_SLOTS_TRY)
                if 4 <= maybe_slots <= 2_000_000:
                    self.index_slots = int(maybe_slots)
            except Exception:
                self.index_slots = None

    def _read_bytes(self, off: int, ln: int) -> bytes:
        return self.meta[off: off + ln]

    def _read_u64(self, off: int) -> int:
        return struct.unpack_from("<Q", self.meta, off)[0]

    def _read_u32(self, off: int) -> int:
        return struct.unpack_from("<I", self.meta, off)[0]

    def refresh_header(self):
        self.tail = self._read_u64(META_OFF_TAIL)
        self.seq = self._read_u64(META_OFF_SEQ)
        if self.index_slots is None and self.meta.size() >= (META_OFF_INDEX_SLOTS_TRY + 8):
            try:
                maybe_slots = self._read_u64(META_OFF_INDEX_SLOTS_TRY)
                if 4 <= maybe_slots <= 2_000_000:
                    self.index_slots = int(maybe_slots)
            except Exception:
                pass

    # ----- index-slot helpers -----
    def index_slot_count(self) -> Optional[int]:
        return self.index_slots

    def _read_index_slot(self, index_pos: int, slot_count: int) -> Tuple[int, int, int, int]:
        slot_base = META_HEADER_SIZE + (index_pos % slot_count) * INDEX_SLOT_SIZE
        if slot_base + INDEX_SLOT_SIZE > self.meta.size():
            raise RuntimeError("index slot out of bounds")
        off = struct.unpack_from("<Q", self.meta, slot_base)[0]
        ln = struct.unpack_from("<I", self.meta, slot_base + 8)[0]
        seq = struct.unpack_from("<Q", self.meta, slot_base + 12)[0]
        kind = struct.unpack_from("<B", self.meta, slot_base + 20)[0]
        return int(off), int(ln), int(seq), int(kind)

    def _read_data_region(self, off: int, ln: int) -> bytes:
        if ln == 0:
            return b""
        if off + ln <= self.data_capacity:
            return self.data[off: off + ln]
        first = self.data_capacity - off
        return self.data[off:] + self.data[:(ln - first)]

    def decode_record_from_data(self, raw: bytes) -> Optional[dict]:
        # Layout used by writer: [len:u32][kind:u8][pad:7][seq:u64][payload...]
        if len(raw) < 20:
            return None
        try:
            payload_len = struct.unpack_from("<I", raw, 0)[0]
            kind = struct.unpack_from("<B", raw, 4)[0]
            seq = struct.unpack_from("<Q", raw, 12)[0]
            payload_bytes = raw[20:20 + payload_len] if 20 + payload_len <= len(raw) else raw[20:]
            try:
                payload = json.loads(payload_bytes.decode("utf-8"))
            except Exception:
                payload = {"_raw": payload_bytes.hex(), "_decode_error": True}
            return {"seq": int(seq), "kind": int(kind), "payload_len": int(payload_len), "payload": payload}
        except Exception:
            return None

    # ----- fast index scan -----
    def scan_for_asset_indexed(self, asset: str, max_slots_check: Optional[int] = None) -> List[Tuple[int, int, int, int]]:
        self.refresh_header()
        if self.index_slots is None:
            raise RuntimeError("index_slots not available")

        found = []
        slot_count = self.index_slots
        to_check = slot_count if max_slots_check is None else min(slot_count, max_slots_check)
        asset_u = asset.upper()

        for idx in range(0, to_check):
            try:
                off, ln, seq, kind = self._read_index_slot(idx, slot_count)
                if ln == 0:
                    continue
                raw = self._read_data_region(off, ln)
                rec = self.decode_record_from_data(raw)
                if rec is None:
                    continue
                pl = rec.get("payload")
                if isinstance(pl, dict):
                    payload_asset = (pl.get("asset") or pl.get("symbol") or pl.get("s") or "").upper()
                    if payload_asset == asset_u:
                        found.append((idx, off, ln, rec["seq"]))
            except Exception:
                continue
        return found

    # ----- fallback: full data-region scan -----
    def scan_all_records(self, max_records: Optional[int] = None) -> List[Tuple[int, int, int, dict]]:
        """
        Scan entire data region and attempt to parse records. Returns list of tuples:
          (offset, total_len, seq, rec_dict)
        This uses a resyncing strategy: try to read header at current offset; if header looks invalid,
        advance by 1 byte and retry. If header valid, consume total_len bytes and continue.
        """
        results = []
        cap = self.data_capacity
        off = 0
        scanned = 0
        max_scan_bytes = cap  # scan full buffer once

        while scanned < max_scan_bytes:
            # read 4 bytes payload_len at off (wrap-aware)
            try:
                # read 20 bytes header region (may wrap)
                header = self._read_data_region(off, 20)
                if len(header) < 20:
                    # can't parse; advance
                    off = (off + 1) % cap
                    scanned += 1
                    continue
                payload_len = struct.unpack_from("<I", header, 0)[0]
                # sanity checks on payload_len
                if payload_len > cap - 20:
                    # ridiculously large -> likely not a valid header here
                    off = (off + 1) % cap
                    scanned += 1
                    continue
                total_len = 20 + payload_len
                # now read full record bytes
                rec_bytes = self._read_data_region(off, total_len)
                rec = self.decode_record_from_data(rec_bytes)
                if rec is None:
                    off = (off + 1) % cap
                    scanned += 1
                    continue
                # basic seq sanity: seq > 0 and not absurd (> current meta seq)
                if rec["seq"] == 0 or rec["seq"] > max(1, self.seq + 1):
                    # Could be stale/uninitialized; resync
                    off = (off + 1) % cap
                    scanned += 1
                    continue
                # Accept record
                results.append((off, total_len, rec["seq"], rec))
                off = (off + total_len) % cap
                scanned += total_len
                if max_records and len(results) >= max_records:
                    break
            except Exception:
                off = (off + 1) % cap
                scanned += 1
                continue

        return results

    def scan_for_asset_fallback(self, asset: str) -> List[Tuple[int, int, int, dict]]:
        asset_u = asset.upper()
        recs = self.scan_all_records()
        out = []
        for off, total_len, seq, rec in recs:
            pl = rec.get("payload")
            if isinstance(pl, dict):
                payload_asset = (pl.get("asset") or pl.get("symbol") or pl.get("s") or "").upper()
                if payload_asset == asset_u:
                    out.append((off, total_len, seq, rec))
        return out

    # follow_asset combines both methods: tries indexed fast path, falls back to full scan
    def follow_asset(self, asset: str, start_after_seq: Optional[int] = None, poll_interval: float = 0.05) -> Generator[dict, None, None]:
        last_seen = start_after_seq or -1
        asset_u = asset.upper()

        # If index available, use index scanning which is much faster
        if self.index_slots is not None:
            slot_count = self.index_slots
            while True:
                try:
                    self.refresh_header()
                    # scan slots; yield records with seq > last_seen
                    for idx in range(0, slot_count):
                        try:
                            off, ln, seq, kind = self._read_index_slot(idx, slot_count)
                            if ln == 0 or seq <= last_seen:
                                continue
                            raw = self._read_data_region(off, ln)
                            rec = self.decode_record_from_data(raw)
                            if rec is None:
                                continue
                            pl = rec.get("payload")
                            if isinstance(pl, dict):
                                payload_asset = (pl.get("asset") or pl.get("symbol") or pl.get("s") or "").upper()
                                if payload_asset == asset_u:
                                    last_seen = max(last_seen, rec["seq"])
                                    yield rec
                        except Exception:
                            continue
                    time.sleep(poll_interval)
                except KeyboardInterrupt:
                    break
                except Exception:
                    time.sleep(poll_interval)
            return

        # Fallback: no index present — full-scan per poll
        while True:
            try:
                self.refresh_header()
                found = self.scan_for_asset_fallback(asset)
                # sort by seq and yield those > last_seen
                found_sorted = sorted(found, key=lambda t: t[2])
                for off, total_len, seq, rec in found_sorted:
                    if seq > last_seen:
                        last_seen = seq
                        yield rec
                time.sleep(poll_interval)
            except KeyboardInterrupt:
                break
            except Exception:
                time.sleep(poll_interval)

# discovery utilities
def discover_ring_prefixes(rings_dir: str = "rings") -> List[str]:
    if not os.path.isdir(rings_dir):
        return []
    files = os.listdir(rings_dir)
    prefixes = set()
    for fn in files:
        if fn.endswith(".meta"):
            prefixes.add(os.path.join(rings_dir, fn[:-5]))
        elif fn.endswith(".data"):
            prefixes.add(os.path.join(rings_dir, fn[:-5]))
    return sorted(prefixes)

def find_asset_in_rings(asset: str, rings_dir: str = "rings", max_slots_check: Optional[int] = None) -> List[Tuple[str, int, int, int]]:
    prefixes = discover_ring_prefixes(rings_dir)
    matches = []
    for p in prefixes:
        try:
            rr = RingReader(p)
        except Exception:
            continue
        try:
            if rr.index_slots is not None:
                found = rr.scan_for_asset_indexed(asset, max_slots_check=max_slots_check)
                for idx, off, ln, seq in found:
                    matches.append((p, idx, off, seq))
            else:
                # fallback cheap check: scan_all_records but limit to ~1000 records
                found_fb = rr.scan_for_asset_fallback(asset)
                for off, ln, seq, rec in found_fb:
                    matches.append((p, -1, off, seq))
        except Exception:
            continue
    return matches

def main():
    parser = argparse.ArgumentParser(description="Find and follow asset records in rings.")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_find = sub.add_parser("find", help="Find which ring prefixes contain the asset")
    p_find.add_argument("asset")
    p_find.add_argument("--rings-dir", default="rings")
    p_find.add_argument("--max-slots", type=int, default=None)

    p_follow = sub.add_parser("follow", help="Find and follow asset; follows first matching prefix unless --prefix given")
    p_follow.add_argument("asset")
    p_follow.add_argument("--rings-dir", default="rings")
    p_follow.add_argument("--prefix", default=None)
    p_follow.add_argument("--poll", type=float, default=0.05)

    args = parser.parse_args()

    if args.cmd == "find":
        matches = find_asset_in_rings(args.asset, rings_dir=args.rings_dir, max_slots_check=args.max_slots)
        if not matches:
            print(f"No matches for asset {args.asset} in rings under {args.rings_dir}")
            return
        print(f"Found {len(matches)} candidate(s):")
        for p, idx, off, seq in matches:
            idx_str = f"slot={idx}" if idx >= 0 else "slot=FALLBACK"
            print(f"  prefix={p} {idx_str} offset={off} seq={seq}")
        return

    if args.cmd == "follow":
        chosen_prefix = args.prefix
        if chosen_prefix is None:
            matches = find_asset_in_rings(args.asset, rings_dir=args.rings_dir)
            if not matches:
                print(f"No matches for asset {args.asset} in rings under {args.rings_dir}")
                return
            # pick highest seq
            matches.sort(key=lambda t: t[3], reverse=True)
            chosen_prefix = matches[0][0]
            print(f"Following prefix {chosen_prefix} (best match seq={matches[0][3]})")

        rr = RingReader(chosen_prefix)
        try:
            for rec in rr.follow_asset(args.asset, poll_interval=args.poll):
                seq = rec.get("seq")
                kind = rec.get("kind")
                pl = rec.get("payload")
                ts = None
                if isinstance(pl, dict):
                    ts = pl.get("recv_ts_ms") or pl.get("exchange_ts_ms") or pl.get("ts") or None
                ts_str = f"{ts}" if ts else "-"
                print(f"{time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(time.time()))} seq={seq} kind={kind} ts={ts_str} payload={json.dumps(pl)}")
        finally:
            rr.meta.close()
            rr.data.close()

if __name__ == "__main__":
    main()
