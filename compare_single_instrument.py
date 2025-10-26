#!/usr/bin/env python3
"""
compare_single_instrument.py

Purpose:
 - For one instrument (e.g. BTCUSDT), find which ring chunk contains recent records by scanning ring files.
 - Open websocket to Binance for that instrument (bookTicker).
 - When a WS message arrives, search the chunk ring (scan backwards up to max_scan slots) for the matching symbol record.
 - Append a CSV row with comparison fields.
 - Also prints light diagnostics.

Usage:
    python3 compare_single_instrument.py BTCUSDT

Notes:
 - Requires `websockets` and `aiofiles` packages:
     pip install websockets aiofiles
 - Assumes ring files live under `rings/` and are named like `rings/binance_book_chunk{N}.meta` / `.data`
"""

import asyncio
import aiofiles
import csv
import glob
import json
import mmap
import os
import struct
import time
from typing import Optional, Dict, Any

import websockets

# Ring layout constants (must match your Rust writer)
META_HEADER_SIZE = 32
INDEX_SLOT_SIZE = 24
META_OFF_LATEST_SEQ = 0
META_OFF_LATEST_INDEX_POS = 8
META_OFF_INDEX_SLOTS = 24

RINGS_DIR = "rings"
CSV_OUT = "cmp_single.csv"
CSV_HEADER = [
    "utc_ts",
    "instrument",
    "ws_recv_ts_ms",
    "ws_exchange_ts_ms",
    "ws_bid",
    "ws_ask",
    "ws_mid",
    "ring_read_ts_ms",
    "ring_writer_ts_ms",
    "ring_exchange_ts_ms",
    "ring_bid",
    "ring_ask",
    "ring_mid",
    "abs_price_diff",
    "pct_price_diff",
    "latency_ws_to_ring_ms",
    "latency_ring_write_to_read_ms",
    "ring_seq",
    "ring_slot_idx",
    "ring_slot_ln",
]

# -------------------------
# ring utilities (lightweight)
# -------------------------
class RingReader:
    def __init__(self, prefix: str):
        self.prefix = prefix
        self.meta_path = f"{prefix}.meta"
        self.data_path = f"{prefix}.data"
        if not os.path.exists(self.meta_path) or not os.path.exists(self.data_path):
            raise FileNotFoundError(f"missing ring files for prefix: {prefix}")
        self._meta_f = open(self.meta_path, "rb")
        self._data_f = open(self.data_path, "rb")
        self.meta = mmap.mmap(self._meta_f.fileno(), 0, access=mmap.ACCESS_READ)
        self.data = mmap.mmap(self._data_f.fileno(), 0, access=mmap.ACCESS_READ)
        self.data_capacity = self.data.size()
        self.index_slots = struct.unpack_from("<Q", self.meta, META_OFF_INDEX_SLOTS)[0]

    def close(self):
        try:
            self.meta.close()
            self.data.close()
            self._meta_f.close()
            self._data_f.close()
        except Exception:
            pass

    def read_meta_u64(self, off: int) -> int:
        return struct.unpack_from("<Q", self.meta, off)[0]

    def read_index_slot(self, index_pos: int):
        slot_base = META_HEADER_SIZE + (index_pos % self.index_slots) * INDEX_SLOT_SIZE
        off = struct.unpack_from("<Q", self.meta, slot_base)[0]
        ln = struct.unpack_from("<I", self.meta, slot_base + 8)[0]
        seq = struct.unpack_from("<Q", self.meta, slot_base + 12)[0]
        kind = struct.unpack_from("<B", self.meta, slot_base + 20)[0]
        return int(off), int(ln), int(seq), int(kind)

    def read_data_region(self, off: int, ln: int) -> bytes:
        if ln == 0:
            return b""
        if off + ln <= self.data_capacity:
            return self.data[off: off + ln]
        first = self.data_capacity - off
        return self.data[off:] + self.data[:(ln - first)]

    def decode_record(self, raw: bytes) -> Optional[Dict[str, Any]]:
        if len(raw) < 24:
            return None
        seq = struct.unpack_from("<Q", raw, 0)[0]
        writer_ts = struct.unpack_from("<Q", raw, 8)[0]
        payload_len = struct.unpack_from("<I", raw, 16)[0]
        kind = struct.unpack_from("<B", raw, 20)[0]
        payload_bytes = raw[24:24 + payload_len] if 24 + payload_len <= len(raw) else raw[24:]
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except Exception:
            payload = {"_raw": payload_bytes.hex(), "_json_err": True}
        return {"seq": int(seq), "writer_ts": int(writer_ts), "payload": payload, "kind": int(kind)}

    def find_most_recent_for_symbol(self, symbol: str, max_scan: Optional[int] = None):
        """
        Scan backward from latest_index_pos up to max_scan slots (default = index_slots)
        until a record's payload asset/symbol matches `symbol`. Return (rec, slot_idx, ln) or (None, None, None).
        """
        latest_index_pos = struct.unpack_from("<Q", self.meta, META_OFF_LATEST_INDEX_POS)[0]
        if max_scan is None:
            max_scan = self.index_slots
        scanned = 0
        pos = latest_index_pos
        sym_up = symbol.upper()
        while scanned < max_scan:
            idx = pos % self.index_slots
            off, ln, seq, kind = self.read_index_slot(idx)
            if ln and off is not None and ln > 0:
                raw = self.read_data_region(off, ln)
                rec = self.decode_record(raw)
                if rec and isinstance(rec["payload"], dict):
                    p = rec["payload"]
                    payload_asset = (p.get("asset") or p.get("symbol") or p.get("s") or "").upper()
                    if payload_asset == sym_up:
                        return rec, idx, ln
            pos -= 1
            scanned += 1
        return None, None, None

# -------------------------
# find ring prefix for a single symbol (fast scan)
# -------------------------
def find_prefix_for_symbol(symbol: str, rings_dir: str = RINGS_DIR, max_scan_per_chunk: int = 1024):
    """
    Scan all ring prefixes under `rings_dir` and quickly look for `symbol`.
    max_scan_per_chunk reduces work (scan only latest N slots per chunk).
    Returns first prefix found or None.
    """
    metas = sorted(glob.glob(os.path.join(rings_dir, "*.meta")))
    sym_up = symbol.upper()
    for meta in metas:
        prefix = meta[:-5]
        try:
            rr = RingReader(prefix)
        except Exception:
            continue
        try:
            rec, idx, ln = rr.find_most_recent_for_symbol(sym_up, max_scan=max_scan_per_chunk)
            rr.close()
            if rec:
                return prefix
        except Exception:
            rr.close()
            continue
    return None

# -------------------------
# simple ring health diagnostic
# -------------------------
def ring_health(prefix: str, scan_slots: int = 256):
    """
    Quick health checks on ring:
     - prints index_slots, latest_seq, latest_index_pos
     - scans latest `scan_slots` slots for seq progression and duplicate payloads
    """
    try:
        rr = RingReader(prefix)
    except Exception as e:
        print(f"[health] cannot open ring {prefix}: {e}")
        return
    index_slots = rr.index_slots
    latest_seq = rr.read_meta_u64(META_OFF_LATEST_SEQ)
    latest_index_pos = rr.read_meta_u64(META_OFF_LATEST_INDEX_POS)
    print(f"[health] prefix={prefix} index_slots={index_slots} latest_seq={latest_seq} latest_index_pos={latest_index_pos}")
    # scan
    seen_seqs = set()
    seen_payloads = {}
    dup_payload_count = 0
    monotonic = True
    prev_seq = None
    pos = latest_index_pos
    for i in range(min(scan_slots, index_slots)):
        idx = pos % index_slots
        off, ln, seq, kind = rr.read_index_slot(idx)
        if ln and off is not None and ln > 0:
            raw = rr.read_data_region(off, ln)
            rec = rr.decode_record(raw)
            if rec:
                s = rec["seq"]
                if prev_seq is not None and s > prev_seq:
                    monotonic = False
                prev_seq = s
                if s in seen_seqs:
                    pass
                seen_seqs.add(s)
                payload_key = json.dumps(rec["payload"], sort_keys=True)
                cnt = seen_payloads.get(payload_key, 0) + 1
                seen_payloads[payload_key] = cnt
                if cnt > 1:
                    dup_payload_count += 1
        pos -= 1
    print(f"[health] scanned {min(scan_slots,index_slots)} slots: unique_seqs={len(seen_seqs)}, duplicate_payloads={dup_payload_count}, monotonic_seq_lookback_ok={monotonic}")
    rr.close()

# -------------------------
# comparator: open WS for single symbol and compare on each message
# -------------------------
async def compare_one(symbol: str, max_scan_per_chunk: int = 4096, poll_find_prefix_seconds: int = 3):
    # 1) find prefix quickly (scan last N slots per chunk)
    prefix = find_prefix_for_symbol(symbol, max_scan_per_chunk=max_scan_per_chunk)
    start_time = time.time()
    while prefix is None and (time.time() - start_time) < 30:
        print(f"[{symbol}] prefix not found yet; retrying in {poll_find_prefix_seconds}s")
        await asyncio.sleep(poll_find_prefix_seconds)
        prefix = find_prefix_for_symbol(symbol, max_scan_per_chunk=max_scan_per_chunk)
    if prefix is None:
        print(f"[{symbol}] ERROR: could not locate ring prefix containing {symbol}")
        return
    print(f"[{symbol}] located prefix: {prefix}")

    # open ring reader
    rr = RingReader(prefix)

    # prepare CSV
    header_needed = not os.path.exists(CSV_OUT)
    async with aiofiles.open(CSV_OUT, mode="a") as af:
        if header_needed:
            await af.write(",".join(CSV_HEADER) + "\n")

    # open WS
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@bookTicker"
    print(f"[{symbol}] connecting websocket {url}")
    async with websockets.connect(url, max_size=2 ** 22) as ws:
        async for raw in ws:
            ws_recv_ts = int(time.time() * 1000)
            try:
                j = json.loads(raw)
            except Exception:
                continue
            data = j
            ws_ex_ts = data.get("E")
            try:
                ws_bid = float(data.get("b") or 0.0)
                ws_ask = float(data.get("a") or 0.0)
                ws_mid = 0.5 * (ws_bid + ws_ask) if ws_bid and ws_ask else None
            except Exception:
                ws_bid = ws_ask = ws_mid = None

            # read latest record for symbol (scan up to max_scan_per_chunk slots)
            ring_rec, slot_idx, ln = rr.find_most_recent_for_symbol(symbol, max_scan=max_scan_per_chunk)
            ring_read_ts = int(time.time() * 1000)
            ring_writer_ts = None
            ring_ex_ts = None
            ring_bid = None
            ring_ask = None
            ring_mid = None
            abs_price_diff = None
            pct_price_diff = None
            latency_ws_to_ring = None
            latency_ring_write_to_read = None
            ring_seq = ""
            if ring_rec:
                ring_seq = ring_rec.get("seq")
                ring_writer_ts = ring_rec.get("writer_ts")
                payload = ring_rec.get("payload", {})
                ring_ex_ts = payload.get("exchange_ts_ms") or payload.get("E")
                # attempt common bid/ask extraction
                ring_bid = _extract_bid(payload)
                ring_ask = _extract_ask(payload)
                if ring_bid is not None and ring_ask is not None:
                    ring_mid = 0.5 * (ring_bid + ring_ask)
                if ws_mid is not None and ring_mid is not None:
                    abs_price_diff = abs(ws_mid - ring_mid)
                    pct_price_diff = (abs_price_diff / ring_mid * 100.0) if ring_mid != 0 else None
                if ring_writer_ts is not None:
                    latency_ws_to_ring = ws_recv_ts - ring_writer_ts
                    latency_ring_write_to_read = ring_read_ts - ring_writer_ts
                elif ring_ex_ts is not None:
                    latency_ws_to_ring = (ws_recv_ts - ring_ex_ts) if ws_recv_ts and ring_ex_ts else None
                    latency_ring_write_to_read = (ring_read_ts - ring_ex_ts) if ring_read_ts and ring_ex_ts else None

            row = [
                _utc_now_iso(),
                symbol,
                _fmt(ws_recv_ts),
                _fmt(ws_ex_ts),
                _fmt(ws_bid),
                _fmt(ws_ask),
                _fmt(ws_mid),
                _fmt(ring_read_ts),
                _fmt(ring_writer_ts),
                _fmt(ring_ex_ts),
                _fmt(ring_bid),
                _fmt(ring_ask),
                _fmt(ring_mid),
                _fmt(abs_price_diff),
                _fmt(pct_price_diff),
                _fmt(latency_ws_to_ring),
                _fmt(latency_ring_write_to_read),
                str(ring_seq),
                str(slot_idx),
                str(ln),
            ]
            line = ",".join([str(x) for x in row]) + "\n"
            async with aiofiles.open(CSV_OUT, mode="a") as af:
                await af.write(line)

    rr.close()

# helpers
def _utc_now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _fmt(x):
    return "" if x is None else str(x)

def _extract_bid(payload: Dict[str, Any]):
    for k in ("bid", "b", "bestBid", "bidPrice"):
        v = payload.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass
    if isinstance(payload.get("data"), dict):
        return _extract_bid(payload["data"])
    return None

def _extract_ask(payload: Dict[str, Any]):
    for k in ("ask", "a", "bestAsk", "askPrice"):
        v = payload.get(k)
        if v is not None:
            try:
                return float(v)
            except Exception:
                pass
    if isinstance(payload.get("data"), dict):
        return _extract_ask(payload["data"])
    return None

# CLI
if __name__ == "__main__":
    import sys, argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("symbol", help="instrument symbol (e.g. BTCUSDT)")
    parser.add_argument("--max-scan", type=int, default=4096, help="how many index slots to scan backward to find symbol in chunk")
    args = parser.parse_args()
    sym = args.symbol.upper()
    try:
        asyncio.run(compare_one(sym, max_scan_per_chunk=args.max_scan))
    except KeyboardInterrupt:
        print("Interrupted")
