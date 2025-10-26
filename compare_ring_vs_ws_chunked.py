#!/usr/bin/env python3
"""
compare_ring_vs_ws_chunked.py

- Deterministically compute symbol -> chunk using Binance REST /api/v3/exchangeInfo
- For each chunk that contains at least one requested symbol, open a multiplexed websocket:
    wss://stream.binance.com:9443/stream?streams=s1@bookTicker/s2@bookTicker/...
- On each incoming message, if it's for an instrument we're monitoring, read the ring for that symbol
  (search backwards until the slot seq matches the message event or the symbol) and write CSV comparison.
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
from typing import Dict, List, Optional, Tuple, Any, Set
import aiohttp
import websockets

# constants
RINGS_DIR = "rings"
CSV_OUT = "comparisons.csv"
BINANCE_WS_BASE = "wss://stream.binance.com:9443/stream?streams="
BINANCE_REST_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"

META_HEADER_SIZE = 32
INDEX_SLOT_SIZE = 24
META_OFF_LATEST_SEQ = 0
META_OFF_LATEST_INDEX_POS = 8
META_OFF_INDEX_SLOTS = 24

CSV_HEADER = [
    "timestamp_utc",
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
]

# ---------------------------
# Helpers: deterministic chunk selection (same as Rust)
# ---------------------------
async def fetch_all_binance_symbols() -> List[str]:
    async with aiohttp.ClientSession() as sess:
        async with sess.get(BINANCE_REST_EXCHANGE_INFO, timeout=10) as resp:
            resp.raise_for_status()
            data = await resp.json()
    syms = [s["symbol"].upper() for s in data.get("symbols", []) if s.get("status", "TRADING") == "TRADING"]
    syms.sort()
    return syms

def compute_chunk_for_symbol(sorted_symbols: List[str], symbol: str, chunk_size: int) -> int:
    symbol = symbol.upper()
    try:
        idx = sorted_symbols.index(symbol)
    except ValueError:
        raise KeyError(f"symbol {symbol} not found in exchangeInfo")
    return idx // chunk_size

# ---------------------------
# Ring reader (lightweight)
# ---------------------------
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

    def _read_index_slot(self, index_pos: int) -> Tuple[Optional[int], int, int, int]:
        slot_base = META_HEADER_SIZE + (index_pos % self.index_slots) * INDEX_SLOT_SIZE
        off = struct.unpack_from("<Q", self.meta, slot_base)[0]
        ln = struct.unpack_from("<I", self.meta, slot_base + 8)[0]
        seq = struct.unpack_from("<Q", self.meta, slot_base + 12)[0]
        kind = struct.unpack_from("<B", self.meta, slot_base + 20)[0]
        if ln == 0:
            return None, 0, seq, kind
        return int(off), int(ln), int(seq), int(kind)

    def _read_data_region(self, off: int, ln: int) -> bytes:
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
        payload_bytes = raw[24:24+payload_len] if 24+payload_len <= len(raw) else raw[24:]
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except Exception:
            payload = {"_raw": payload_bytes.hex()}
        return {"seq": int(seq), "writer_ts": int(writer_ts), "payload": payload}

    def read_latest_for_symbol_search(self, symbol: str, max_scan: int = None) -> Optional[Dict[str, Any]]:
        """
        Scan backwards from latest_index_pos in the chunk until we find a record with payload.symbol == symbol.
        Returns the first match (most recent).
        max_scan: optional cap on number of slots to scan (defaults to index_slots).
        """
        latest_index_pos = struct.unpack_from("<Q", self.meta, META_OFF_LATEST_INDEX_POS)[0]
        if max_scan is None:
            max_scan = self.index_slots
        s_upper = symbol.upper()
        scanned = 0
        pos = latest_index_pos
        while scanned < max_scan:
            idx = pos % self.index_slots
            off, ln, seq, kind = self._read_index_slot(idx)
            if off and ln > 0:
                raw = self._read_data_region(off, ln)
                rec = self.decode_record(raw)
                if rec and isinstance(rec["payload"], dict):
                    p = rec["payload"]
                    payload_asset = (p.get("asset") or p.get("symbol") or p.get("s") or "").upper()
                    if payload_asset == s_upper:
                        return rec
            pos -= 1
            scanned += 1
        return None

# ---------------------------
# build per-chunk subscriptions and tasks
# ---------------------------
async def run_chunk_monitor(chunk_id: int, symbols_in_chunk: List[str], chunk_prefix: str, csv_lock: asyncio.Lock):
    """
    Open multiplexed websocket for chunk (streams joined with '/'), handle incoming events.
    symbols_in_chunk: list of symbols (uppercase) that we are interested in within this chunk.
    chunk_prefix: the ring prefix for this chunk (e.g. rings/binance_book_chunk3)
    """
    # open ring reader
    rr = RingReader(chunk_prefix)
    # build multiplexed stream string but limit length (Binance allows long URLs, but keep safe)
    streams = "/".join([f"{s.lower()}@bookTicker" for s in symbols_in_chunk])
    url = BINANCE_WS_BASE + streams
    print(f"[chunk {chunk_id}] connecting to ws: {url}")
    try:
        async with websockets.connect(url, max_size=2**22) as ws:
            async for raw in ws:
                now_ms = int(time.time() * 1000)
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                # multiplexed stream envelope: {"stream":"<s>@bookTicker","data": {...}}
                data = msg.get("data") if "data" in msg else msg
                if not isinstance(data, dict):
                    continue
                s = data.get("s") or data.get("symbol")
                if not s:
                    continue
                s = s.upper()
                # Only handle symbols we care about
                if s not in symbols_in_chunk:
                    continue
                # compute WS fields
                ws_recv_ts = now_ms
                ws_ex_ts = data.get("E")
                try:
                    ws_bid = float(data.get("b") or data.get("bestBid") or 0.0)
                    ws_ask = float(data.get("a") or data.get("bestAsk") or 0.0)
                    ws_mid = 0.5*(ws_bid + ws_ask) if (ws_bid and ws_ask) else None
                except Exception:
                    ws_bid = ws_ask = ws_mid = None

                # read ring latest record for this symbol (scan)
                ring_rec = None
                try:
                    ring_rec = rr.read_latest_for_symbol_search(s, max_scan=rr.index_slots)
                except Exception:
                    ring_rec = None

                # prepare csv row
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

                if ring_rec:
                    ring_writer_ts = ring_rec.get("writer_ts")
                    payload = ring_rec.get("payload", {})
                    ring_ex_ts = payload.get("exchange_ts_ms") or payload.get("E")
                    ring_bid = _extract_bid(payload)
                    ring_ask = _extract_ask(payload)
                    if ring_bid is not None and ring_ask is not None:
                        ring_mid = 0.5*(ring_bid + ring_ask)
                    if ws_mid is not None and ring_mid is not None:
                        abs_price_diff = abs(ws_mid - ring_mid)
                        pct_price_diff = (abs_price_diff / ring_mid * 100.0) if ring_mid != 0 else None
                    if ring_writer_ts is not None:
                        latency_ws_to_ring = (ws_recv_ts - ring_writer_ts)
                        latency_ring_write_to_read = (ring_read_ts - ring_writer_ts)
                    elif ring_ex_ts is not None:
                        latency_ws_to_ring = (ws_recv_ts - ring_ex_ts) if ws_recv_ts and ring_ex_ts else None
                        latency_ring_write_to_read = (ring_read_ts - ring_ex_ts) if ring_read_ts and ring_ex_ts else None

                row = [
                    _utc_now_iso(),
                    s,
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
                ]
                line = ",".join(row) + "\n"
                # write CSV
                async with csv_lock:
                    async with aiofiles.open(CSV_OUT, mode="a") as af:
                        await af.write(line)
    except Exception as e:
        print(f"[chunk {chunk_id}] ws/monitor error: {e}")
    finally:
        rr.close()

# ---------------------------
# utilities
# ---------------------------
def _utc_now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _fmt(x):
    return "" if x is None else str(x)

def _extract_bid(payload: Dict[str, Any]) -> Optional[float]:
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

def _extract_ask(payload: Dict[str, Any]) -> Optional[float]:
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

# ---------------------------
# main orchestration
# ---------------------------
async def main(instruments: List[str], chunk_size: int = 800):
    # fetch symbols list
    print("fetching exchangeInfo...")
    sorted_syms = await fetch_all_binance_symbols()
    print(f"fetched {len(sorted_syms)} symbols")
    # determine chunk -> symbols mapping for requested instruments
    chunk_to_symbols: Dict[int, List[str]] = {}
    for inst in instruments:
        inst = inst.upper()
        try:
            chunk_id = compute_chunk_for_symbol(sorted_syms, inst, chunk_size)
        except KeyError:
            print(f"instrument {inst} not found in exchangeInfo; skipping")
            continue
        chunk_to_symbols.setdefault(chunk_id, []).append(inst)

    if not chunk_to_symbols:
        print("no instruments resolved to chunks; exiting")
        return

    # create rings dir prefix for each chunk and spawn monitor
    csv_lock = asyncio.Lock()
    # create CSV header if needed
    if not os.path.exists(CSV_OUT):
        async with aiofiles.open(CSV_OUT, mode="w") as af:
            await af.write(",".join(CSV_HEADER) + "\n")

    tasks = []
    for chunk_id, syms in chunk_to_symbols.items():
        prefix = os.path.join(RINGS_DIR, f"binance_book_chunk{chunk_id}")
        # sanity: if files don't exist yet, warn
        if not os.path.exists(prefix + ".meta") or not os.path.exists(prefix + ".data"):
            print(f"[chunk {chunk_id}] WARNING: ring files not found for prefix {prefix}; monitor will still try to connect")
        t = asyncio.create_task(run_chunk_monitor(chunk_id, syms, prefix, csv_lock))
        tasks.append(t)

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python3 compare_ring_vs_ws_chunked.py BTCUSDT [ETHUSDT ...]")
        sys.exit(1)
    insts = sys.argv[1:]
    try:
        asyncio.run(main(insts, chunk_size=800))
    except KeyboardInterrupt:
        print("interrupted")
