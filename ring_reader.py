# ring_reader.py
import mmap
import os
import struct
import json
import time
from typing import List, Optional, Dict, Any


META_HEADER_SIZE = 32
INDEX_SLOT_SIZE = 24

# meta header offsets (LE u64)
META_OFF_LATEST_SEQ = 0         # u64
META_OFF_LATEST_INDEX_POS = 8   # u64
META_OFF_TAIL = 16              # u64
META_OFF_INDEX_SLOTS = 24       # u64


class RingReader:
    """
    Reader for the dense byte-tail circular mmap ring produced by the Rust MmapRing.
    The ring uses two files: <prefix>.meta and <prefix>.data
    """

    def __init__(self, prefix: str):
        """
        prefix: path prefix the rust writer used, e.g. "rings/binance_book_chunk0"
        - meta file: prefix.meta
        - data file: prefix.data
        """
        self.prefix = prefix
        self.meta_path = f"{prefix}.meta"
        self.data_path = f"{prefix}.data"

        if not os.path.exists(self.meta_path):
            raise FileNotFoundError(self.meta_path)
        if not os.path.exists(self.data_path):
            raise FileNotFoundError(self.data_path)

        # open and memory-map (read-only)
        self._meta_f = open(self.meta_path, "rb")
        self._data_f = open(self.data_path, "rb")

        self.meta = mmap.mmap(self._meta_f.fileno(), 0, access=mmap.ACCESS_READ)
        self.data = mmap.mmap(self._data_f.fileno(), 0, access=mmap.ACCESS_READ)
        self.data_capacity = self.data.size()

        # read index_slots from meta header (u64 little endian)
        self.index_slots = self._read_u64_from_meta(META_OFF_INDEX_SLOTS)
        if self.index_slots == 0:
            raise RuntimeError("index_slots == 0 in meta; ring may be uninitialized")

    # -------------------------
    # low-level helpers
    # -------------------------
    def _read_u64_from_meta(self, offset: int) -> int:
        return struct.unpack_from("<Q", self.meta, offset)[0]

    def _read_u32_from_meta(self, offset: int) -> int:
        return struct.unpack_from("<I", self.meta, offset)[0]

    def _read_index_slot(self, index_pos: int):
        """
        Returns tuple (data_offset: int, length: int, seq: int, kind: int)
        If the slot is empty (length == 0) returns (None, 0, 0, 0)
        """
        slot_base = META_HEADER_SIZE + (index_pos % self.index_slots) * INDEX_SLOT_SIZE
        off = struct.unpack_from("<Q", self.meta, slot_base)[0]
        ln = struct.unpack_from("<I", self.meta, slot_base + 8)[0]
        seq = struct.unpack_from("<Q", self.meta, slot_base + 12)[0]
        kind = struct.unpack_from("<B", self.meta, slot_base + 20)[0]
        if ln == 0:
            return (None, 0, seq, kind)
        return (int(off), int(ln), int(seq), int(kind))

    def _read_data_region(self, off: int, ln: int) -> bytes:
        """
        Read ln bytes starting at off in the circular buffer, handling wrap-around.
        off is an offset inside [0, data_capacity)
        """
        if ln == 0:
            return b""
        if off + ln <= self.data_capacity:
            return self.data[off: off + ln]
        # wrap
        first = self.data_capacity - off
        return self.data[off:] + self.data[:(ln - first)]

    # -------------------------
    # record decoding
    # -------------------------
    def _decode_record(self, raw: bytes) -> Optional[Dict[str, Any]]:
        """
        Record layout written by Rust:
        - [0..8)   : seq (u64 little)
        - [8..16)  : ts_ms (u64 little)  -- writer's write timestamp
        - [16..20) : payload_len (u32 little)
        - [20]     : kind (u8)
        - [21..24] : padding (3 bytes)
        - [24..]   : payload bytes (payload_len)
        """
        if len(raw) < 24:
            return None
        seq = struct.unpack_from("<Q", raw, 0)[0]
        ts_ms = struct.unpack_from("<Q", raw, 8)[0]
        payload_len = struct.unpack_from("<I", raw, 16)[0]
        kind = struct.unpack_from("<B", raw, 20)[0]
        if 24 + payload_len > len(raw):
            # malformed / truncated
            payload_bytes = raw[24:]
        else:
            payload_bytes = raw[24:24 + payload_len]

        try:
            payload_text = payload_bytes.decode("utf-8")
            payload = json.loads(payload_text)
        except Exception:
            # return raw if JSON decode fails
            payload = {"_raw": payload_bytes.hex(), "_decode_error": True}

        return {
            "seq": int(seq),
            "writer_ts_ms": int(ts_ms),
            "payload_len": int(payload_len),
            "kind": int(kind),
            "payload": payload,
        }

    # -------------------------
    # high-level readers
    # -------------------------
    def read_latest(self, n: int = 1) -> List[Dict[str, Any]]:
        """
        Return up to `n` newest records (newest-first).
        """
        if n <= 0:
            return []

        latest_index_pos = self._read_u64_from_meta(META_OFF_LATEST_INDEX_POS)
        latest_index_pos = int(latest_index_pos)
        out = []
        pos = latest_index_pos
        got = 0

        while got < n and got < self.index_slots:
            # wrap-around of pos (pos may be signed in Rust but stored as u64)
            idx = pos % self.index_slots
            off, ln, seq, kind = self._read_index_slot(idx)
            if not off or ln == 0:
                # empty slot (uninitialized)
                break
            raw = self._read_data_region(off, ln)
            rec = self._decode_record(raw)
            if rec is None:
                break
            out.append(rec)
            pos = pos - 1
            got += 1

        return out

    def read_latest_for_asset(self, asset: str, n: int = 1) -> List[Dict[str, Any]]:
        """
        Return up to `n` newest records whose payload 'asset' equals the provided asset.
        Works by scanning back through index slots. This is O(index_slots) in worst-case.
        """
        if n <= 0:
            return []

        latest_index_pos = self._read_u64_from_meta(META_OFF_LATEST_INDEX_POS)
        latest_index_pos = int(latest_index_pos)
        out = []
        pos = latest_index_pos
        scanned = 0

        while len(out) < n and scanned < self.index_slots:
            idx = pos % self.index_slots
            off, ln, seq, kind = self._read_index_slot(idx)
            if off and ln > 0:
                raw = self._read_data_region(off, ln)
                rec = self._decode_record(raw)
                if rec and isinstance(rec["payload"], dict):
                    # payload is expected to be normalized object (e.g. OrderBookSnapshot)
                    p = rec["payload"]
                    # normalize key names (some payloads may use 'asset' or 'symbol')
                    payload_asset = p.get("asset") or p.get("symbol") or p.get("s")
                    if payload_asset and payload_asset.upper() == asset.upper():
                        out.append(rec)
            pos -= 1
            scanned += 1

        return out

    def read_by_seq(self, seq: int) -> Optional[Dict[str, Any]]:
        """
        Attempt to find a record by sequence number by scanning the index ring.
        Returns the first matching record (there may be multiple if seq wrapped).
        """
        for i in range(self.index_slots):
            off, ln, s, kind = self._read_index_slot(i)
            if ln > 0 and s == seq:
                raw = self._read_data_region(off, ln)
                return self._decode_record(raw)
        return None

    def close(self):
        try:
            self.meta.close()
            self.data.close()
            self._meta_f.close()
            self._data_f.close()
        except Exception:
            pass


# -------------------------
# Command-line example usage
# -------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Read ring buffer files created by consolidator (Rust).")
    parser.add_argument("prefix", help="File prefix (without .meta/.data), e.g. rings/binance_book_chunk0")
    parser.add_argument("--asset", help="Filter to this asset symbol (e.g. BTCUSDT)", default=None)
    parser.add_argument("-n", "--num", help="How many records to return", type=int, default=5)
    parser.add_argument("--tail", help="Continuously poll and print new records", action="store_true")
    args = parser.parse_args()

    rr = RingReader(args.prefix)
    try:
        if args.tail:
            printed = set()
            while True:
                recs = rr.read_latest(args.num)
                # print newest-first
                for r in recs:
                    key = (r["seq"], r["writer_ts_ms"])
                    if key in printed:
                        continue
                    printed.add(key)
                    print(json.dumps(r, indent=None, ensure_ascii=False))
                time.sleep(0.2)
        else:
            if args.asset:
                recs = rr.read_latest_for_asset(args.asset, args.num)
            else:
                recs = rr.read_latest(args.num)
            for r in recs:
                # pretty-print payload and some meta
                payload = r["payload"]
                ts = r["writer_ts_ms"]
                print(f"seq={r['seq']} kind={r['kind']} ts_ms={ts} payload={json.dumps(payload)}")
    finally:
        rr.close()
