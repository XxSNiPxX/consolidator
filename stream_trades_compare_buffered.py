#!/usr/bin/env python3
"""
stream_and_compare_trades_only.py

Watches mmap chunk rings for trade records (kind=1 only),
spawns Binance WS trade monitors for symbols in filters.json, writes per-symbol CSVs
containing both mmap and WS trade rows, and emits comparison rows when a WS/mmap pair
is matched within a time tolerance.

Usage:
    python3 stream_and_compare_trades_only.py --rings-dir rings --filter-file filters.json --outdir out --poll-ms 50 --match-ms 500

CSV format (columns):
 ts_iso, symbol, source, subsource, kind, seq_or_id, px, sz, bid, ask, mid,
 recv_ts, ws_ts, arrival_ms, price_diff, matched_with, note

- source: "mmap" | "ws" | "compare"
- subsource: prefix for mmap or "trade" for ws
- NOTE: bid/ask/mid are unused for trades and will be None.
"""
from __future__ import annotations
import argparse, os, time, json, glob, mmap, struct, threading, queue, csv, traceback, asyncio
from collections import defaultdict, deque
from datetime import datetime
from typing import Dict, Any, Optional
import websockets

# --- constants matching Rust mmap layout ---
META_MAGIC = b"RINGV1\x00\x00"
META_HEADER_LEN = 40
INDEX_SLOT_SIZE = 24
INDEX_SLOT_FMT = "<Q I Q B 3x"
RECORD_HEADER_FMT = "<I B 7x Q"
RECORD_HEADER_LEN = struct.calcsize(RECORD_HEADER_FMT)

# --- helpers ---
def iso_now():
    return datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None

# --- mmap parse helpers ---
def read_meta_header(mm: mmap.mmap):
    magic = mm[:8]
    if magic != META_MAGIC:
        raise RuntimeError("bad meta magic")
    data_capacity = struct.unpack_from("<Q", mm, 8)[0]
    seq_counter = struct.unpack_from("<Q", mm, 24)[0]
    slots = struct.unpack_from("<Q", mm, 32)[0]
    return {"cap": int(data_capacity), "seq": int(seq_counter), "slots": int(slots)}

def read_index_slot(mm: mmap.mmap, i: int):
    off, ln, seq, kind = struct.unpack_from(INDEX_SLOT_FMT, mm, META_HEADER_LEN + i*INDEX_SLOT_SIZE)
    return int(off), int(ln), int(seq), int(kind)

def read_from_data(mm: mmap.mmap, cap: int, off: int, n: int) -> bytes:
    off %= cap
    if off + n <= cap:
        return mm[off:off+n]
    first = cap - off
    return mm[off:off+first] + mm[:n-first]

def parse_record(data_mm: mmap.mmap, cap: int, off: int) -> Optional[Dict[str,Any]]:
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
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        payload = None
    return {"seq": int(seq), "kind": int(kind), "payload": payload}

# --- ChunkWatcher: emits mmap_trade (kind==1) only ---
class ChunkWatcher(threading.Thread):
    def __init__(self, prefix: str, out_q: queue.Queue, poll_ms: int = 50, filter_ref: dict = None):
        super().__init__(daemon=True)
        self.prefix = prefix
        self.out_q = out_q
        self.poll_ms = poll_ms
        self.filter_ref = filter_ref
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
        for mm in (self.meta_mm, self.data_mm):
            try:
                if mm: mm.close()
            except: pass
        for f in (self.meta_f, self.data_f):
            try:
                if f: f.close()
            except: pass
        self.meta_mm = self.data_mm = self.meta_f = self.data_f = None

    def run(self):
        self.reopen()
        while not self.stop_evt.is_set():
            if not self.meta_mm or not self.data_mm:
                self.reopen(); time.sleep(0.2); continue
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
                    # trades only
                    if seq > self.last_seq and kind == 1:
                        rec = parse_record(self.data_mm, self.cap, off)
                        if rec and rec["seq"] == seq and isinstance(rec["payload"], dict):
                            payload = rec["payload"]
                            sym = (payload.get("asset") or payload.get("s") or payload.get("symbol") or "").upper()

                            # filtering by symbol if provided
                            allowed = True
                            if self.filter_ref and self.filter_ref.get("symbols"):
                                allowed = (sym in self.filter_ref.get("symbols"))
                            if not allowed:
                                continue

                            rec["prefix"] = self.prefix
                            self.out_q.put(("mmap_trade", rec))

                            px = payload.get("px") or payload.get("p") or payload.get("price")
                            sz = payload.get("sz") or payload.get("q") or payload.get("size")
                            print(f"{iso_now()} [mmap-trade] prefix={self.prefix} seq={seq} symbol={sym} px={px} sz={sz}")
                self.last_seq = seq_now
            time.sleep(self.poll_ms/1000.0)

    def stop(self):
        self.stop_evt.set()

# --- Binance websocket monitor subscribing to trade only ---
class BinanceWSMonitor:
    def __init__(self, symbol: str, out_q: queue.Queue):
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
        streams = f"{self.symbol}@trade"
        url = f"{base}/stream?streams={streams}"
        backoff = 0.1
        while not self._stop.is_set():
            try:
                async with websockets.connect(url, max_size=2**24) as ws:
                    backoff = 0.1
                    async for msg in ws:
                        if self._stop.is_set(): break
                        try:
                            obj = json.loads(msg)
                        except Exception:
                            continue
                        data = obj.get("data") if isinstance(obj, dict) and "data" in obj else obj
                        if not data: continue
                        # trade message
                        if "p" in data and "q" in data:
                            sym = (data.get("s") or "").upper()
                            px = safe_float(data.get("p"))
                            sz = safe_float(data.get("q"))
                            ws_ts = data.get("E") or int(time.time()*1000)
                            trade_id = data.get("t")
                            payload = {"symbol": sym, "px": px, "sz": sz, "ws_ts": ws_ts, "trade_id": trade_id}
                            self.out_q.put(("ws_trade", payload))
                            print(f"{iso_now()} [ws-trade] symbol={sym} px={px} sz={sz} trade_id={trade_id}")
                        else:
                            continue
            except Exception as e:
                print(f"{iso_now()} [ws:{self.symbol}] connection error: {e}; reconnect in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(5.0, backoff * 1.8)

    def stop(self):
        self._stop.set()

# --- Buffered CSV writer & compare engine (trades only) ---
class TradeCompareEngine:
    def __init__(self, outdir="out", buffer_rows=50, buffer_sec=5, fsync=False, match_ms=500):
        self.outdir = outdir; os.makedirs(self.outdir, exist_ok=True)
        self.buffer_rows = int(buffer_rows); self.buffer_sec = float(buffer_sec); self.fsync = bool(fsync)
        self.match_ms = int(match_ms)
        self.q = queue.Queue()
        self.windows = defaultdict(lambda: {"mmap": deque(maxlen=5000), "ws": deque(maxlen=5000)})
        self.buffers = defaultdict(list)
        self.last_flush = defaultdict(lambda: time.time())
        self.csv_handles = {}
        self.csv_writers = {}
        self.lock = threading.Lock()
        self._stop = threading.Event()
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def enqueue(self, tag, rec):
        self.q.put((tag, rec))

    def _ensure_csv(self, symbol):
        s = symbol.upper()
        if s in self.csv_writers: return
        fname = os.path.join(self.outdir, f"{s}.csv")
        first_time = not os.path.exists(fname) or os.stat(fname).st_size == 0
        f = open(fname, "a", newline="")
        w = csv.writer(f)
        if first_time:
            w.writerow([
                "ts_iso","symbol","source","subsource","kind",
                "seq_or_tradeid","px","sz","bid","ask","mid",
                "recv_ts","ws_ts","arrival_ms","price_diff","matched_with","note"
            ])
            if self.fsync:
                f.flush(); os.fsync(f.fileno())
        self.csv_handles[s] = f
        self.csv_writers[s] = w

    def _buffer_row(self, symbol, row):
        s = symbol.upper()
        self.buffers[s].append(row)
        if len(self.buffers[s]) >= self.buffer_rows:
            self._flush_symbol(s)
        else:
            now = time.time()
            if now - self.last_flush[s] >= self.buffer_sec:
                self._flush_symbol(s)

    def _flush_symbol(self, symbol):
        s = symbol.upper()
        if not self.buffers.get(s):
            return
        self._ensure_csv(s)
        with self.lock:
            f = self.csv_handles[s]; w = self.csv_writers[s]
            for r in self.buffers[s]:
                w.writerow(r)
            if self.fsync:
                f.flush(); os.fsync(f.fileno())
            else:
                f.flush()
        self.buffers[s].clear()
        self.last_flush[s] = time.time()
        print(f"{iso_now()} [flush] wrote rows for {s}")

    def _match_and_emit(self, symbol: str):
        win = self.windows[symbol]
        mmap_win = win["mmap"]
        ws_win = win["ws"]
        if not mmap_win or not ws_win:
            return
        m = mmap_win[-1]
        w = ws_win[-1]
        m_ts = m.get("recv_ts") or m.get("arrival_ms") or 0
        w_ts = w.get("ws_ts") or w.get("arrival_ms") or 0
        if not m_ts or not w_ts:
            return
        dt = abs(int(w_ts) - int(m_ts))
        if dt <= self.match_ms:
            m_px = m.get("px")
            w_px = w.get("px")
            price_diff = None
            if m_px is not None and w_px is not None:
                try:
                    price_diff = float(m_px) - float(w_px)
                except Exception:
                    price_diff = None
            symbol_u = symbol.upper()
            crow = [
                iso_now(), symbol_u, "compare", f"{m.get('prefix','mmap')}|ws",
                2,  # kind=2 for compare
                m.get("seq") or w.get("trade_id"),
                m_px, m.get("sz"), None, None, None,  # bid/ask/mid unused
                m_ts, w_ts, int(time.time()*1000), price_diff, w.get("trade_id") or m.get("seq"), f"dt_ms={dt}"
            ]
            self._buffer_row(symbol_u, crow)
            print(f"{iso_now()} [compare] {symbol_u} dt_ms={dt} price_diff={price_diff}")

    def _loop(self):
        while not self._stop.is_set():
            try:
                tag, rec = self.q.get(timeout=0.5)
            except queue.Empty:
                for s in list(self.buffers.keys()):
                    if self.buffers[s] and (time.time() - self.last_flush[s]) >= self.buffer_sec:
                        self._flush_symbol(s)
                continue
            try:
                now_ms = int(time.time()*1000)
                if tag == "mmap_trade":
                    seq = rec.get("seq")
                    payload = rec.get("payload") or {}
                    prefix = rec.get("prefix")
                    symbol = (payload.get("asset") or payload.get("s") or payload.get("symbol") or "").upper()
                    if not symbol:
                        continue
                    px = safe_float(payload.get("px") or payload.get("p") or payload.get("price"))
                    sz = safe_float(payload.get("sz") or payload.get("q") or payload.get("size"))
                    recv_ts = payload.get("recv_ts_ms") or payload.get("exchange_ts_ms") or None

                    self.windows[symbol]["mmap"].append({
                        "seq": seq, "px": px, "sz": sz, "recv_ts": recv_ts,
                        "prefix": prefix, "arrival_ms": now_ms
                    })

                    row = [
                        iso_now(), symbol, "mmap", prefix, 1,
                        seq, px, sz, None, None, None,
                        recv_ts, None, now_ms, None, None, ""
                    ]
                    self._buffer_row(symbol, row)
                    self._match_and_emit(symbol)

                elif tag == "ws_trade":
                    symbol = (rec.get("symbol") or "").upper()
                    px = safe_float(rec.get("px")); sz = safe_float(rec.get("sz"))
                    ws_ts = rec.get("ws_ts") or int(time.time()*1000)
                    trade_id = rec.get("trade_id")
                    self.windows[symbol]["ws"].append({"trade_id": trade_id, "px": px, "sz": sz, "ws_ts": ws_ts, "arrival_ms": now_ms})
                    row = [iso_now(), symbol, "ws", "trade", 1, trade_id, px, sz, None, None, None, None, ws_ts, now_ms, None, None, ""]
                    self._buffer_row(symbol, row)
                    self._match_and_emit(symbol)
                    print(f"{iso_now()} [record] ws_trade symbol={symbol} trade_id={trade_id} px={px} sz={sz}")

                else:
                    pass

            except Exception as e:
                print(f"{iso_now()} [engine] exception: {e}\n{traceback.format_exc()}")

    def stop(self):
        self._stop.set()
        for s in list(self.buffers.keys()):
            if self.buffers[s]:
                self._flush_symbol(s)
        self.thread.join(timeout=1)
        for f in list(self.csv_handles.values()):
            try: f.close()
            except: pass

# --- Controller orchestration ---
class Controller:
    def __init__(self, rings_dir="rings", poll_ms=50, filter_file="filters.json", outdir="out", buffer_rows=50, buffer_sec=5, fsync=False, match_ms=500):
        self.rings_dir = rings_dir; self.poll_ms = poll_ms; self.filter_file = filter_file; self.outdir = outdir
        self.mmap_q = queue.Queue()
        self.watchers: Dict[str, ChunkWatcher] = {}
        self.ws_monitors: Dict[str, BinanceWSMonitor] = {}
        self.engine = TradeCompareEngine(outdir=self.outdir, buffer_rows=buffer_rows, buffer_sec=buffer_sec, fsync=fsync, match_ms=match_ms)
        self.filters = {"symbols": set(), "exchanges": set()}
        self._stop = threading.Event()

    def load_filters(self):
        if not os.path.exists(self.filter_file):
            self.filters = {"symbols": set(), "exchanges": set()}
            return
        try:
            d = json.load(open(self.filter_file))
            syms = {s.upper() for s in d.get("symbols", []) if isinstance(s, str)}
            exs = {e.upper() for e in d.get("exchanges", []) if isinstance(e, str)}
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
                w = ChunkWatcher(p, self.mmap_q, poll_ms=self.poll_ms, filter_ref=self.filters)
                w.start(); self.watchers[p] = w

    def start_ws_for_symbol(self, symbol: str):
        s = symbol.upper()
        if s in self.ws_monitors: return
        w = BinanceWSMonitor(s, out_q_all)
        self.ws_monitors[s] = w; w.start()
        print(f"{iso_now()} [controller] started WS monitor for {s}")

    def stop_ws_for_symbol(self, symbol: str):
        s = symbol.upper()
        if s not in self.ws_monitors: return
        try: self.ws_monitors[s].stop()
        except: pass
        self.ws_monitors.pop(s, None)

    def run(self):
        self.discover_and_spawn_watchers()
        last_scan = 0; last_filter_load = 0
        try:
            while not self._stop.is_set():
                now = time.time()
                if now - last_scan > 1.0:
                    self.discover_and_spawn_watchers(); last_scan = now
                if now - last_filter_load > 1.0:
                    self.load_filters(); last_filter_load = now
                    for sym in self.filters["symbols"]:
                        self.start_ws_for_symbol(sym)
                    for s in list(self.ws_monitors.keys()):
                        if s not in self.filters["symbols"]:
                            self.stop_ws_for_symbol(s)

                # drain mmap queue
                try:
                    item = self.mmap_q.get(timeout=0.5)
                except queue.Empty:
                    item = None
                if item:
                    tag, rec = item
                    payload = rec.get("payload", {}) if isinstance(rec, dict) else {}
                    symbol = (payload.get("asset") or payload.get("s") or payload.get("symbol") or "").upper()
                    if self.filters["symbols"] and symbol and symbol not in self.filters["symbols"]:
                        continue
                    self.engine.enqueue(tag, rec)

                # drain global ws queue
                try:
                    tag, rec = out_q_all.get_nowait()
                    symbol = (rec.get("symbol") or "").upper()
                    if self.filters["symbols"] and symbol and symbol not in self.filters["symbols"]:
                        continue
                    self.engine.enqueue(tag, rec)
                except queue.Empty:
                    pass

        except KeyboardInterrupt:
            pass
        finally:
            print(f"{iso_now()} stopping controller...")
            for w in self.watchers.values():
                try: w.stop()
                except: pass
            for ws in self.ws_monitors.values():
                try: ws.stop()
                except: pass
            self.engine.stop()

# global WS queue
out_q_all = queue.Queue()

# --- CLI and main ---
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--rings-dir", default="rings")
    p.add_argument("--poll-ms", type=int, default=50)
    p.add_argument("--filter-file", default="filters.json")
    p.add_argument("--outdir", default="out")
    p.add_argument("--buffer-rows", type=int, default=50)
    p.add_argument("--buffer-sec", type=float, default=5.0)
    p.add_argument("--fsync", action="store_true", help="call fsync after each buffer flush")
    p.add_argument("--match-ms", type=int, default=500, help="max ms difference to match mmap<>ws trades")
    return p.parse_args()

def main():
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    c = Controller(rings_dir=args.rings_dir, poll_ms=args.poll_ms, filter_file=args.filter_file,
                   outdir=args.outdir, buffer_rows=args.buffer_rows, buffer_sec=args.buffer_sec,
                   fsync=args.fsync, match_ms=args.match_ms)
    try:
        c.run()
    except KeyboardInterrupt:
        print("exiting")

if __name__ == "__main__":
    main()
