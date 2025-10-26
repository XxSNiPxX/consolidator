#!/usr/bin/env python3
"""
stream_and_ws_noplot.py

- watches mmap chunks under `rings/`
- dynamic filters in filters.json (symbols/exchanges)
- for each filtered symbol, opens Binance websocket(s) (bookTicker + trade)
- writes rows to CSV per symbol (no plotting)
"""
import argparse
import asyncio
import json
import math
import mmap
import os
import queue
import struct
import threading
import time
import traceback
from datetime import datetime
from typing import Dict

import pandas as pd  # optional (kept for possible CSV handling), can be removed if not needed
import websockets
import csv
from collections import defaultdict, deque

# --- constants matching Rust mmap layout ---
META_MAGIC = b"RINGV1\x00\x00"
META_HEADER_LEN = 40
INDEX_SLOT_SIZE = 24
INDEX_SLOT_FMT = "<Q I Q B 3x"
RECORD_HEADER_FMT = "<I B 7x Q"
RECORD_HEADER_LEN = struct.calcsize(RECORD_HEADER_FMT)

def iso_now():
    return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

# --- mmap helpers ---
def read_meta_header(mm):
    magic = mm[:8]
    if magic != META_MAGIC:
        raise RuntimeError("bad meta magic")
    data_capacity = struct.unpack_from("<Q", mm, 8)[0]
    seq_counter = struct.unpack_from("<Q", mm, 24)[0]
    slots = struct.unpack_from("<Q", mm, 32)[0]
    return {"cap": int(data_capacity), "seq": int(seq_counter), "slots": int(slots)}

def read_index_slot(mm, i):
    off, ln, seq, kind = struct.unpack_from(INDEX_SLOT_FMT, mm, META_HEADER_LEN + i*INDEX_SLOT_SIZE)
    return int(off), int(ln), int(seq), int(kind)

def read_from_data(mm, cap, off, n):
    off %= cap
    if off + n <= cap:
        return mm[off:off+n]
    first = cap - off
    return mm[off:off+first] + mm[:n-first]

def parse_record(data_mm, cap, off):
    hdr = read_from_data(data_mm, cap, off, RECORD_HEADER_LEN)
    if len(hdr) < RECORD_HEADER_LEN:
        return None
    try:
        plen, kind, seq = struct.unpack(RECORD_HEADER_FMT, hdr)
    except Exception:
        return None
    total = RECORD_HEADER_LEN + plen
    raw = read_from_data(data_mm, cap, off, total)
    body = raw[RECORD_HEADER_LEN:RECORD_HEADER_LEN+plen]
    payload = None
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        payload = None
    return {"seq": int(seq), "kind": int(kind), "payload": payload}

# --- chunk watcher thread ---
import glob
class ChunkWatcher(threading.Thread):
    def __init__(self, prefix, out_q, poll_ms=50):
        super().__init__(daemon=True)
        self.prefix = prefix
        self.out_q = out_q
        self.poll_ms = poll_ms
        self.stop_evt = threading.Event()
        self.meta_mm = self.data_mm = None
        self.meta_f = self.data_f = None
        self.last_seq = 0
        self.cap = 0
        self.slots = 0

    def reopen(self):
        self.close()
        mfile, dfile = self.prefix + '.meta', self.prefix + '.data'
        if not (os.path.exists(mfile) and os.path.exists(dfile)):
            return
        try:
            self.meta_f = open(mfile, 'r+b'); self.meta_mm = mmap.mmap(self.meta_f.fileno(), 0, access=mmap.ACCESS_READ)
            self.data_f = open(dfile, 'r+b'); self.data_mm = mmap.mmap(self.data_f.fileno(), 0, access=mmap.ACCESS_READ)
            hdr = read_meta_header(self.meta_mm)
            self.cap = hdr["cap"]; self.slots = hdr["slots"]; self.last_seq = hdr["seq"]
            print(f"{iso_now()} [+] watching {self.prefix} capacity={self.cap} slots={self.slots} seq={self.last_seq}")
        except Exception:
            self.close()

    def close(self):
        for m in (self.meta_mm, self.data_mm):
            try:
                if m: m.close()
            except Exception:
                pass
        for f in (self.meta_f, self.data_f):
            try:
                if f: f.close()
            except Exception:
                pass
        self.meta_mm = self.data_mm = self.meta_f = self.data_f = None

    def run(self):
        self.reopen()
        while not self.stop_evt.is_set():
            if not self.meta_mm or not self.data_mm:
                self.reopen()
                time.sleep(0.2)
                continue
            try:
                hdr = read_meta_header(self.meta_mm)
            except Exception:
                self.close(); time.sleep(0.2); continue
            seq_now = hdr["seq"]
            if seq_now < self.last_seq:
                self.last_seq = 0
            if seq_now > self.last_seq:
                for i in range(hdr["slots"]):
                    try:
                        off, ln, seq, kind = read_index_slot(self.meta_mm, i)
                    except Exception:
                        continue
                    if seq > self.last_seq:
                        rec = parse_record(self.data_mm, self.cap, off)
                        if rec and rec["seq"] == seq and isinstance(rec["payload"], dict):
                            rec["prefix"] = self.prefix
                            self.out_q.put(("mmap", rec))
                self.last_seq = seq_now
            time.sleep(self.poll_ms/1000.0)

    def stop(self):
        self.stop_evt.set()

# --- Binance websocket monitor (async + background thread) ---
class BinanceWSMonitor:
    def __init__(self, symbol, out_q):
        self.symbol = symbol.lower()
        self.out_q = out_q
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self):
        asyncio.run(self._ws_main())

    async def _ws_main(self):
        base = "wss://stream.binance.com:9443"
        streams = f"{self.symbol}@bookTicker/{self.symbol}@trade"
        url = f"{base}/stream?streams={streams}"
        backoff = 0.1
        while not self._stop.is_set():
            try:
                async with websockets.connect(url, max_size=2**24) as ws:
                    backoff = 0.1
                    async for msg in ws:
                        try:
                            obj = json.loads(msg)
                        except Exception:
                            continue
                        data = obj.get("data") if isinstance(obj, dict) and "data" in obj else obj
                        if data is None:
                            continue
                        # trade detection
                        if data.get("e") == "trade" or ("p" in data and "q" in data):
                            out = {"symbol": data.get("s"), "px": safe_float(data.get("p")), "sz": safe_float(data.get("q")), "side_raw": data.get("m"), "ws_ts": data.get("E")}
                            self.out_q.put(("ws_trade", out))
                        elif "b" in data and "a" in data:
                            out = {"symbol": data.get("s"), "bid": safe_float(data.get("b")), "ask": safe_float(data.get("a")), "ws_ts": data.get("E")}
                            if out["bid"] is not None and out["ask"] is not None:
                                out["mid"] = (out["bid"] + out["ask"]) / 2.0
                            self.out_q.put(("ws_book", out))
                        else:
                            pass
            except Exception as e:
                print(f"{iso_now()} [ws:{self.symbol}] error: {e}; reconnecting")
                await asyncio.sleep(backoff)
                backoff = min(5.0, backoff * 1.8)

    def stop(self):
        self._stop.set()

# --- CSV writer and compare engine (no plotting) ---
class CompareEngine:
    def __init__(self, outdir="out"):
        self.outdir = outdir
        os.makedirs(self.outdir, exist_ok=True)
        self.queues = queue.Queue()
        self.symbol_windows = defaultdict(lambda: {"mmap": deque(maxlen=2000), "ws_book": deque(maxlen=2000), "ws_trade": deque(maxlen=2000)})
        self.csv_handles = {}
        self.csv_files = {}
        self.lock = threading.Lock()
        self._stop = threading.Event()
        self.writer_thread = threading.Thread(target=self._writer_loop, daemon=True)
        self.writer_thread.start()

    def enqueue(self, tag, rec):
        self.queues.put((tag, rec))

    def _ensure_csv(self, symbol):
        symbol = symbol.upper()
        if symbol in self.csv_handles:
            return
        fname = os.path.join(self.outdir, f"{symbol}.csv")
        f = open(fname, "a", newline="")
        w = csv.writer(f)
        if os.stat(fname).st_size == 0:
            w.writerow([
                "ts_iso","symbol","source","prefix_or_stream","kind","seq","bid","ask","mid","px","sz","side","recv_ts","ws_ts","latency_ms","mismatch"
            ])
        self.csv_handles[symbol] = w
        self.csv_files[symbol] = f

    def _writer_loop(self):
        while not self._stop.is_set():
            try:
                tag, rec = self.queues.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                if tag == "mmap":
                    payload = rec["payload"]
                    symbol = (payload.get("asset") or payload.get("s") or payload.get("symbol") or "").upper()
                    kind = rec["kind"]
                    seq = rec["seq"]
                    prefix = rec.get("prefix")
                    bid = safe_float(payload.get("bid"))
                    ask = safe_float(payload.get("ask"))
                    mid = safe_float(payload.get("mid")) if payload.get("mid") is not None else ((bid+ask)/2.0 if bid is not None and ask is not None else None)
                    recv_ts = payload.get("recv_ts_ms") or payload.get("exchange_ts_ms")
                    self.symbol_windows[symbol]["mmap"].append({"ts": int(time.time()*1000), "seq": seq, "prefix": prefix, "bid": bid, "ask": ask, "mid": mid, "recv_ts": recv_ts})
                    self._ensure_csv(symbol)
                    row = [iso_now(), symbol, "mmap", prefix, kind, seq, bid, ask, mid, None, None, None, recv_ts, None, None, False]
                    with self.lock:
                        self.csv_handles[symbol].writerow(row)
                elif tag == "ws_book":
                    symbol = (rec.get("symbol") or "").upper()
                    bid = rec.get("bid"); ask = rec.get("ask"); mid = rec.get("mid")
                    ws_ts = rec.get("ws_ts") or int(time.time()*1000)
                    self.symbol_windows[symbol]["ws_book"].append({"ts": int(time.time()*1000), "bid": bid, "ask": ask, "mid": mid, "ws_ts": ws_ts})
                    latest_mmap = self.symbol_windows[symbol]["mmap"][-1] if self.symbol_windows[symbol]["mmap"] else None
                    mismatch = False
                    latency = None
                    if latest_mmap and latest_mmap.get("mid") is not None and mid is not None:
                        diff = abs(latest_mmap["mid"] - mid)
                        mismatch = diff > 1e-9 and diff / max(1.0, mid) > 1e-6
                    if latest_mmap and latest_mmap.get("recv_ts") and ws_ts:
                        try:
                            latency = int(time.time()*1000) - int(ws_ts)
                        except Exception:
                            latency = None
                    self._ensure_csv(symbol)
                    row = [iso_now(), symbol, "ws_book", "binance_bookTicker", 0, None, bid, ask, mid, None, None, None, None, ws_ts, latency, mismatch]
                    with self.lock:
                        self.csv_handles[symbol].writerow(row)
                elif tag == "ws_trade":
                    symbol = (rec.get("symbol") or "").upper()
                    px = rec.get("px"); sz = rec.get("sz"); side = rec.get("side_raw")
                    ws_ts = rec.get("ws_ts") or int(time.time()*1000)
                    self.symbol_windows[symbol]["ws_trade"].append({"ts": int(time.time()*1000), "px": px, "sz": sz, "side": side, "ws_ts": ws_ts})
                    self._ensure_csv(symbol)
                    row = [iso_now(), symbol, "ws_trade", "binance_trade", 1, None, None, None, None, px, sz, side, None, ws_ts, None, False]
                    with self.lock:
                        self.csv_handles[symbol].writerow(row)
            except Exception as e:
                print(f"{iso_now()} [compare] writer error: {e}\n{traceback.format_exc()}")

    def stop(self):
        self._stop.set()
        self.writer_thread.join(timeout=1)
        for f in list(self.csv_files.values()):
            try: f.close()
            except: pass

# --- Controller coordinating watchers & websockets ---
class Controller:
    def __init__(self, rings_dir="rings", poll_ms=50, filter_file="filters.json", outdir="out"):
        self.rings_dir = rings_dir
        self.poll_ms = poll_ms
        self.filter_file = filter_file
        self.outdir = outdir

        self.mmap_q = queue.Queue()
        self.watchers = {}
        self.ws_monitors = {}
        self.compare = CompareEngine(outdir=self.outdir)
        self.filters = {"symbols": [], "exchanges": []}
        self._stop = threading.Event()

    def load_filters(self):
        if not os.path.exists(self.filter_file):
            return
        try:
            d = json.load(open(self.filter_file))
            syms = [s.upper() for s in d.get("symbols", []) if isinstance(s, str)]
            exs = [e.upper() for e in d.get("exchanges", []) if isinstance(e, str)]
            self.filters["symbols"] = syms
            self.filters["exchanges"] = exs
        except Exception as e:
            print(f"{iso_now()} [controller] load_filters error: {e}")

    def discover_and_spawn_watchers(self):
        prefixes = set()
        for m in glob.glob(os.path.join(self.rings_dir, "*_chunk*.meta")):
            prefixes.add(m[:-5])
        for p in sorted(prefixes):
            if p not in self.watchers:
                w = ChunkWatcher(p, self.mmap_q, poll_ms=self.poll_ms)
                w.start()
                self.watchers[p] = w

    def start_ws_for_symbol(self, symbol):
        symbol = symbol.upper()
        if symbol in self.ws_monitors:
            return
        w = BinanceWSMonitor(symbol, out_q_all)
        self.ws_monitors[symbol] = w
        w.start()
        print(f"{iso_now()} [controller] started WS monitor for {symbol}")

    def stop_ws_for_symbol(self, symbol):
        s = symbol.upper()
        if s not in self.ws_monitors:
            return
        try:
            self.ws_monitors[s].stop()
        except:
            pass
        self.ws_monitors.pop(s, None)

    def run(self):
        self.discover_and_spawn_watchers()
        last_scan = 0
        last_filter_load = 0
        while not self._stop.is_set():
            try:
                now = time.time()
                if now - last_scan > 1.0:
                    self.discover_and_spawn_watchers()
                    last_scan = now
                if now - last_filter_load > 1.0:
                    self.load_filters()
                    last_filter_load = now
                    for sym in self.filters["symbols"]:
                        self.start_ws_for_symbol(sym)
                    for s in list(self.ws_monitors.keys()):
                        if s not in self.filters["symbols"]:
                            self.stop_ws_for_symbol(s)
                try:
                    item = self.mmap_q.get(timeout=0.5)
                except queue.Empty:
                    item = None
                if item:
                    tag, rec = item
                    payload = rec.get("payload", {})
                    symbol = (payload.get("asset") or payload.get("s") or payload.get("symbol") or "").upper()
                    if self.filters["symbols"] and symbol not in self.filters["symbols"]:
                        pass
                    else:
                        self.compare.enqueue("mmap", rec)
                # drain ws queue
                try:
                    ws_item = out_q_all.get_nowait()
                    tag, rec = ws_item
                    symbol = (rec.get("symbol") or "").upper()
                    if self.filters["symbols"] and symbol not in self.filters["symbols"]:
                        pass
                    else:
                        self.compare.enqueue(tag, rec)
                except queue.Empty:
                    pass
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"{iso_now()} [controller] error: {e}\n{traceback.format_exc()}")
        # shutdown
        print(f"{iso_now()} stopping...")
        for w in list(self.watchers.values()):
            try: w.stop()
            except: pass
        for ws in list(self.ws_monitors.values()):
            try: ws.stop()
            except: pass
        self.compare.stop()

# global WS output queue
out_q_all = queue.Queue()

# --- CLI ---
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--rings-dir", default="rings")
    p.add_argument("--poll-ms", type=int, default=50)
    p.add_argument("--filter-file", default="filters.json")
    p.add_argument("--outdir", default="out")
    return p.parse_args()

def main():
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    c = Controller(rings_dir=args.rings_dir, poll_ms=args.poll_ms, filter_file=args.filter_file, outdir=args.outdir)
    try:
        c.run()
    except KeyboardInterrupt:
        print("exiting")
    finally:
        pass

if __name__ == "__main__":
    main()
