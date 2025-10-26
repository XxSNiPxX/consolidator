#!/usr/bin/env python3
# ring_reader_noindex.py
"""
Read/Follow Rust MmapRing files (no index in meta).

Assumptions (match your Rust writer):
- meta layout (32 bytes):
  0..8   : magic bytes (RINGV1\0\0)
  8..16  : data_capacity (u64 LE)
 16..24  : tail_offset (u64 LE)   <-- writer's next-write offset (mod data_capacity)
 24..32  : seq_counter (u64 LE)   <-- last issued seq

- data layout: records packed densely (may wrap)
  record header (20 bytes):
    [0..4)   payload_len: u32 LE
    [4]      kind: u8
    [5..12)  padding (7 bytes)
    [12..20) seq: u64 LE
  record payload: payload_len bytes (utf-8 JSON)

This reader:
- scans the whole data file with resync heuristics to extract valid records
- builds seq->record mapping and returns records sorted by seq
- used to find which ring contains an asset and to follow it continuously

Usage:
    python3 ring_reader_noindex.py find BTCUSDT
    python3 ring_reader_noindex.py follow BTCUSDT
    python3 ring_reader_noindex.py follow BTCUSDT --prefix rings/binance_book_chunk0
"""

import os
import mmap
import struct
import json
import time
import argparse
from typing import List, Dict, Tuple, Optional

# Meta and record constants
META_HEADER_SIZE = 32
RECORD_HEADER_SIZE = 20

META_OFF_MAGIC = 0
META_OFF_DATA_CAP = 8
META_OFF_TAIL = 16
META_OFF_SEQ = 24

MAGIC_BYTES = b"RINGV1\x00\x00"


class RingNoIndex:
    def __init__(self, prefix: str):
        self.prefix = prefix
        self.meta_path = f"{prefix}.meta"
        self.data_path = f"{prefix}.data"
        if not os.path.exists(self.meta_path):
            raise FileNotFoundError(self.meta_path)
        if not os.path.exists(self.data_path):
            raise FileNotFoundError(self.data_path)

        self._meta_f = open(self.meta_path, "rb")
        self._data_f = open(self.data_path, "rb")
        self.meta = mmap.mmap(self._meta_f.fileno(), 0, access=mmap.ACCESS_READ)
        self.data = mmap.mmap(self._data_f.fileno(), 0, access=mmap.ACCESS_READ)

        if self.meta.size() < META_HEADER_SIZE:
            raise RuntimeError("meta file too small")

        # read meta fields
        magic = self.meta[META_OFF_MAGIC:META_OFF_MAGIC + 8]
        if magic != MAGIC_BYTES:
            # Not fatal — some older versions may use different magic; warn
            # but continue reading fields.
            # raise RuntimeError(f"unexpected magic {magic!r}")
            pass

        self.data_capacity = struct.unpack_from("<Q", self.meta, META_OFF_DATA_CAP)[0]
        self.tail_offset = struct.unpack_from("<Q", self.meta, META_OFF_TAIL)[0]
        self.seq_counter = struct.unpack_from("<Q", self.meta, META_OFF_SEQ)[0]

    def refresh_meta(self):
        self.tail_offset = struct.unpack_from("<Q", self.meta, META_OFF_TAIL)[0]
        self.seq_counter = struct.unpack_from("<Q", self.meta, META_OFF_SEQ)[0]

    def _read_data_region(self, off: int, ln: int) -> bytes:
        """Read ln bytes from data mmap with wrap support."""
        cap = self.data_capacity
        if ln == 0:
            return b""
        if off + ln <= cap:
            return self.data[off: off + ln]
        first = cap - off
        return self.data[off: cap] + self.data[0: ln - first]

    def _try_decode_at(self, off: int) -> Optional[Tuple[int, dict]]:
        """
        Try to decode a record starting at offset `off`.
        If valid, return (total_len, record_dict).
        record_dict contains keys: seq (int), kind (int), payload (dict or raw), payload_len (int)
        """
        cap = self.data_capacity
        # need RECORD_HEADER_SIZE bytes available (wrap-aware)
        header = self._read_data_region(off, RECORD_HEADER_SIZE)
        if len(header) < RECORD_HEADER_SIZE:
            return None
        payload_len = struct.unpack_from("<I", header, 0)[0]
        kind = struct.unpack_from("<B", header, 4)[0]
        seq = struct.unpack_from("<Q", header, 12)[0]

        # basic sanity checks
        if payload_len > cap - RECORD_HEADER_SIZE:
            return None
        total_len = RECORD_HEADER_SIZE + payload_len
        # read full record
        raw = self._read_data_region(off, total_len)
        if len(raw) < total_len:
            return None
        payload_bytes = raw[RECORD_HEADER_SIZE:RECORD_HEADER_SIZE + payload_len]
        # attempt JSON decode
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except Exception:
            # keep raw bytes if not JSON
            payload = {"_raw": payload_bytes.hex(), "_decode_error": True}

        # more sanity: seq should be > 0 and not unreasonably huge
        if seq == 0:
            return None
        # seq can be larger than current meta.seq (writer may update meta later), accept for now

        rec = {
            "seq": int(seq),
            "kind": int(kind),
            "payload_len": int(payload_len),
            "payload": payload,
            "off": int(off),
            "total_len": int(total_len),
        }
        return total_len, rec

    def scan_entire_buffer(self, max_bytes: Optional[int] = None) -> Dict[int, dict]:
        """
        Scan the full data buffer and try to extract valid records.
        Returns map seq -> record (latest occurrence kept if duplicate seq).
        This uses resync: try to parse at offset; if invalid advance by 1 byte.
        max_bytes: limit total scanned bytes (None => full buffer)
        """
        cap = self.data_capacity
        mapped = {}
        scanned = 0
        off = 0
        max_scan = cap if max_bytes is None else min(max_bytes, cap)
        while scanned < max_scan:
            res = self._try_decode_at(off)
            if res is None:
                off = (off + 1) % cap
                scanned += 1
                continue
            total_len, rec = res
            seq = rec["seq"]
            # store record (will overwrite earlier dup seqs with later occurrences)
            mapped[seq] = rec
            off = (off + total_len) % cap
            scanned += total_len
        return mapped

    def find_asset(self, asset: str) -> List[dict]:
        """
        Return list of records for given asset found in the buffer (sorted by seq).
        This scans the entire buffer.
        """
        seq_map = self.scan_entire_buffer()
        out = []
        asset_u = asset.upper()
        for seq in sorted(seq_map.keys()):
            rec = seq_map[seq]
            pl = rec.get("payload")
            if isinstance(pl, dict):
                # common fields to check for symbol
                payload_asset = (pl.get("asset") or pl.get("symbol") or pl.get("s") or "").upper()
                if payload_asset == asset_u:
                    out.append(rec)
        return out

    def follow_asset(self, asset: str, start_after_seq: Optional[int] = None, poll: float = 0.05):
        """
        Generator: yields new records for `asset` as they appear (polling).
        This implementation rescans entire buffer on each poll (safe but heavier).
        """
        last_seen = start_after_seq or 0
        asset_u = asset.upper()
        while True:
            try:
                self.refresh_meta()
                seq_map = self.scan_entire_buffer()
                # convert to sorted list
                for seq in sorted(seq_map.keys()):
                    if seq <= last_seen:
                        continue
                    rec = seq_map[seq]
                    pl = rec.get("payload")
                    payload_asset = ""
                    if isinstance(pl, dict):
                        payload_asset = (pl.get("asset") or pl.get("symbol") or pl.get("s") or "").upper()
                    if payload_asset == asset_u:
                        last_seen = max(last_seen, rec["seq"])
                        yield rec
                time.sleep(poll)
            except KeyboardInterrupt:
                break
            except Exception:
                # transient error; sleep a bit
                time.sleep(poll)

    def close(self):
        try:
            self.meta.close()
            self.data.close()
            self._meta_f.close()
            self._data_f.close()
        except Exception:
            pass


# Utilities
def discover_prefixes(rings_dir: str = "rings") -> List[str]:
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


def find_asset_across_rings(asset: str, rings_dir: str = "rings") -> List[Tuple[str, int]]:
    """
    Scan each ring and return list of (prefix, latest_seq_for_asset) for rings that contain the asset.
    """
    prefixes = discover_prefixes(rings_dir)
    matches = []
    for p in prefixes:
        try:
            r = RingNoIndex(p)
            recs = r.find_asset(asset)
            if recs:
                # choose latest seq
                latest = max(r["seq"] for r in recs)
                matches.append((p, int(latest)))
            r.close()
        except Exception:
            continue
    return matches


def main():
    parser = argparse.ArgumentParser(description="Ring no-index reader (scan entire buffer).")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_find = sub.add_parser("find", help="Find which rings contain an asset")
    p_find.add_argument("asset")
    p_find.add_argument("--rings-dir", default="rings")

    p_follow = sub.add_parser("follow", help="Follow an asset (rescan buffer each poll)")
    p_follow.add_argument("asset")
    p_follow.add_argument("--prefix", default=None, help="explicit ring prefix to follow")
    p_follow.add_argument("--rings-dir", default="rings")
    p_follow.add_argument("--poll", type=float, default=0.05)

    args = parser.parse_args()

    if args.cmd == "find":
        matches = find_asset_across_rings(args.asset, rings_dir=args.rings_dir)
        if not matches:
            print("No matches")
            return
        print("Matches:")
        for p, seq in matches:
            print(f"  {p} latest_seq={seq}")
        return

    if args.cmd == "follow":
        prefix = args.prefix
        if not prefix:
            matches = find_asset_across_rings(args.asset, rings_dir=args.rings_dir)
            if not matches:
                print("No matches anywhere")
                return
            # pick ring with highest seq
            matches.sort(key=lambda t: t[1], reverse=True)
            prefix = matches[0][0]
            print(f"Following {args.asset} in {prefix} (best seq {matches[0][1]})")
        r = RingNoIndex(prefix)
        try:
            for rec in r.follow_asset(args.asset, poll=args.poll):
                # pretty-print
                ts = None
                pl = rec.get("payload", {})
                if isinstance(pl, dict):
                    ts = pl.get("recv_ts_ms") or pl.get("exchange_ts_ms") or pl.get("ts") or None
                print(f"{time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(time.time()))} seq={rec['seq']} off={rec['off']} len={rec['total_len']} ts={ts} payload={json.dumps(pl)}")
        finally:
            r.close()


if __name__ == "__main__":
    main()
