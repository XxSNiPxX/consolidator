#!/usr/bin/env python3
"""
dynamic_tail.py

Dynamically discover and tail a symbol across all chunk rings.

Usage:
  python3 dynamic_tail.py BTCUSDT \
      --rings-dir rings --kind book --poll-ms 80 --rescan-sec 5 --replay 3

Options:
  symbol        Symbol to follow (e.g. BTCUSDT)
  --rings-dir   Directory containing ring files (default "rings")
  --kind        "book" or "trade" or "both" (default "book")
  --poll-ms     Poll interval per tailer in milliseconds (default 100)
  --rescan-sec  How often to rescan all rings to discover new prefixes (default 10)
  --replay N    On discovering a new chunk containing the symbol, replay last N historical records (default 0)
  --verbose     More logging
"""
import argparse
import glob
import json
import mmap
import os
import queue
import struct
import threading
import time
from typing import Dict, List, Optional, Tuple

# constants (must match Rust layout)
META_MAGIC = b"RINGV1\x00\x00"
META_HEADER_LEN = 40
INDEX_SLOT_SIZE = 24
INDEX_SLOT_FMT = "<Q I Q B 3x"   # off:u64, len:u32, seq:u64, kind:u8, pad3
RECORD_HEADER_FMT = "<I B 7x Q"  # payload_len:u32, kind:u8, pad7, seq:u64
INDEX_SLOT_LEN = struct.calcsize(INDEX_SLOT_FMT)
RECORD_HEADER_LEN = struct.calcsize(RECORD_HEADER_FMT)

# ------------------------------------------------------------
# small helper functions
# ------------------------------------------------------------
def iso_now():
    t = time.gmtime()
    ms = int(time.time() * 1000) % 1000
    return time.strftime("%Y-%m-%dT%H:%M:%S", t) + f".{ms:03d}Z"

def list_prefixes(rings_dir: str, kind: str) -> List[str]:
    """Return sorted list of ring prefixes found for kind (book/trade/both)."""
    patterns = []
    if kind in ("book", "both"):
        patterns.append(os.path.join(rings_dir, "*_book_chunk*"))
    if kind in ("trade", "both"):
        patterns.append(os.path.join(rings_dir, "*_trade_chunk*"))
    prefixes = set()
    for pat in patterns:
        for m in glob.glob(pat + ".meta"):
            prefixes.add(m[:-5])  # strip .meta
    return sorted(prefixes)

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
    return {
        "data_capacity": int(data_capacity),
        "tail_offset": int(tail_offset),
        "seq_counter": int(seq_counter),
        "index_slots": int(index_slots),
    }

def read_index_slot(meta_mm: mmap.mmap, idx: int) -> Tuple[int, int, int, int]:
    base = META_HEADER_LEN + idx * INDEX_SLOT_SIZE
    off, ln, seq, kind = struct.unpack_from(INDEX_SLOT_FMT, meta_mm, base)
    return int(off), int(ln), int(seq), int(kind)

def read_from_data(data_mm: mmap.mmap, data_capacity: int, off: int, n: int) -> bytes:
    off = off % data_capacity
    if off + n <= data_capacity:
        return data_mm[off:off+n]
    else:
        first = data_capacity - off
        return data_mm[off:off+first] + data_mm[0:n-first]

def parse_record_at(data_mm: mmap.mmap, data_capacity: int, off: int):
    hdr = read_from_data(data_mm, data_capacity, off, RECORD_HEADER_LEN)
    if len(hdr) < RECORD_HEADER_LEN:
        raise RuntimeError("short header")
    payload_len, kind, seq = struct.unpack(RECORD_HEADER_FMT, hdr)
    total_len = RECORD_HEADER_LEN + payload_len
    raw = read_from_data(data_mm, data_capacity, off, total_len)
    payload_bytes = raw[RECORD_HEADER_LEN:RECORD_HEADER_LEN+payload_len]
    payload = None
    try:
        payload = json.loads(payload_bytes.decode("utf-8"))
    except Exception:
        payload = None
    return {"payload_len": int(payload_len), "kind": int(kind), "seq": int(seq), "payload": payload, "raw": payload_bytes}

# ------------------------------------------------------------
# Tailer class: resilient single-prefix tailer (runs in background thread)
# ------------------------------------------------------------
class ChunkTailer(threading.Thread):
    def __init__(self, prefix: str, symbol: str, poll_ms: int = 100, replay: int = 0, verbose: bool = False):
        super().__init__(daemon=True)
        self.prefix = prefix
        self.symbol = symbol.upper()
        self.poll_ms = poll_ms
        self.replay = int(replay)
        self.verbose = verbose

        # file/mmap state
        self.meta_mm: Optional[mmap.mmap] = None
        self.meta_f = None
        self.data_mm: Optional[mmap.mmap] = None
        self.data_f = None
        self.data_capacity: Optional[int] = None
        self.index_slots: Optional[int] = None

        # state
        self.last_seen_seq = 0
        self.stopped = threading.Event()
        self.open_attempts = 0

        # internal queue to emit records back to main controller
        self.emitter = None  # set by controller: a queue.Queue

    def open_once(self):
        meta_path = self.prefix + ".meta"
        data_path = self.prefix + ".data"
        if not os.path.exists(meta_path) or not os.path.exists(data_path):
            raise FileNotFoundError(f"missing files {meta_path} or {data_path}")
        self.meta_f = open(meta_path, "r+b")
        self.data_f = open(data_path, "r+b")
        self.meta_mm = mmap.mmap(self.meta_f.fileno(), 0, access=mmap.ACCESS_READ)
        self.data_mm = mmap.mmap(self.data_f.fileno(), 0, access=mmap.ACCESS_READ)
        hdr = read_meta_header(self.meta_mm)
        self.data_capacity = hdr["data_capacity"]
        self.index_slots = hdr["index_slots"]
        # start from current seq so we only get new records by default
        self.last_seen_seq = hdr["seq_counter"]
        if self.verbose:
            print(f"{iso_now()} [tailer] opened {self.prefix} seq={self.last_seen_seq} slots={self.index_slots}")

    def open_with_backoff(self):
        backoff = 0.05
        while not self.stopped.is_set():
            try:
                self.open_once()
                self.open_attempts = 0
                return
            except Exception as e:
                self.open_attempts += 1
                if self.open_attempts <= 5 or self.open_attempts % 20 == 0:
                    print(f"{iso_now()} [tailer] open failed {self.prefix}: {e} (attempt {self.open_attempts})")
                time.sleep(backoff)
                backoff = min(5.0, backoff * 1.8)

    def close_mmaps(self):
        try:
            if self.meta_mm:
                self.meta_mm.close()
        except Exception:
            pass
        try:
            if self.data_mm:
                self.data_mm.close()
        except Exception:
            pass
        try:
            if self.meta_f:
                self.meta_f.close()
        except Exception:
            pass
        try:
            if self.data_f:
                self.data_f.close()
        except Exception:
            pass
        self.meta_mm = None
        self.data_mm = None
        self.meta_f = None
        self.data_f = None

    def stop(self):
        self.stopped.set()

    def replay_historical(self):
        """If replay>0, scan index slots, collect matching seqs, and emit last N in order."""
        try:
            hdr = read_meta_header(self.meta_mm)
            slots = []
            for si in range(self.index_slots):
                off, ln, seq, kind = read_index_slot(self.meta_mm, si)
                if seq == 0:
                    continue
                try:
                    rec = parse_record_at(self.data_mm, self.data_capacity, off)
                except Exception:
                    continue
                if rec["seq"] != seq:
                    continue
                payload = rec.get("payload")
                if isinstance(payload, dict):
                    s = (payload.get("asset") or payload.get("s") or payload.get("symbol"))
                    if s and s.upper() == self.symbol:
                        slots.append((rec["seq"], rec))
            if not slots:
                return
            slots.sort(key=lambda x: x[0])
            to_emit = slots[-self.replay:] if self.replay > 0 else []
            for seq, rec in to_emit:
                # emit to controller queue
                if self.emitter:
                    self.emitter.put((self.prefix, seq, rec["payload"]))
            # set last_seen_seq to the last replayed seq so we don't re-emit
            if to_emit:
                self.last_seen_seq = to_emit[-1][0]
        except Exception as e:
            if self.verbose:
                print(f"{iso_now()} [tailer] replay error {self.prefix}: {e}")

    def poll_once_collect(self) -> List[Tuple[int, dict]]:
        """
        Collect matching records since last_seen_seq and return list of (seq,payload).
        If seq_counter decreases (reinit/wrap) we reset last_seen_seq to 0 and attempt reading available slots.
        """
        if not self.meta_mm:
            raise RuntimeError("meta not open")
        hdr = read_meta_header(self.meta_mm)
        seq_counter = hdr["seq_counter"]

        if seq_counter < self.last_seen_seq:
            # reinit or wrap: re-scan from 0
            if self.verbose:
                print(f"{iso_now()} [tailer] seq_counter decreased {self.prefix} {seq_counter} < {self.last_seen_seq}; resetting last_seen_seq=0")
            self.last_seen_seq = 0

        if seq_counter == self.last_seen_seq:
            return []

        found = []
        for si in range(self.index_slots):
            off, ln, seq, kind = read_index_slot(self.meta_mm, si)
            if seq > self.last_seen_seq and seq <= seq_counter:
                try:
                    rec = parse_record_at(self.data_mm, self.data_capacity, off)
                except Exception:
                    continue
                if rec["seq"] != seq:
                    # possible partial overwrite, skip
                    continue
                payload = rec.get("payload")
                if isinstance(payload, dict):
                    s = (payload.get("asset") or payload.get("s") or payload.get("symbol"))
                    if s and s.upper() == self.symbol:
                        found.append((rec["seq"], payload))
        found.sort(key=lambda x: x[0])
        if found:
            self.last_seen_seq = max(self.last_seen_seq, found[-1][0])
        else:
            self.last_seen_seq = max(self.last_seen_seq, seq_counter)
        return found

    def run(self):
        # open mmaps (retry loop)
        self.open_with_backoff()
        # optional replay
        if self.replay > 0:
            self.replay_historical()

        while not self.stopped.is_set():
            try:
                recs = self.poll_once_collect()
                for seq, payload in recs:
                    if self.emitter:
                        self.emitter.put((self.prefix, seq, payload))
                time.sleep(self.poll_ms / 1000.0)
            except Exception as e:
                # any error -> close and reopen with backoff
                if self.verbose:
                    print(f"{iso_now()} [tailer] poll error {self.prefix}: {e}; reopening")
                self.close_mmaps()
                self.open_with_backoff()

# ------------------------------------------------------------
# Controller: discovers prefixes and manages tailers
# ------------------------------------------------------------
class DynamicTailController:
    def __init__(self, symbol: str, rings_dir: str = "rings", kind: str = "book",
                 poll_ms: int = 100, rescan_sec: int = 10, replay: int = 0, verbose: bool = False):
        self.symbol = symbol.upper()
        self.rings_dir = rings_dir
        self.kind = kind
        self.poll_ms = poll_ms
        self.rescan_sec = rescan_sec
        self.replay = replay
        self.verbose = verbose

        self.tailers: Dict[str, ChunkTailer] = {}  # prefix -> tailer
        self.last_seen_active: Dict[str, float] = {}  # prefix -> last time we saw symbol
        self.emitter = queue.Queue()  # collect records from tailers

        # parameters for removal: if no symbol seen in a prefix for this long, remove tailer
        self.deactivate_after = max(30, int(rescan_sec * 4))

        # controller's stop flag
        self.stopped = threading.Event()

    def discover_prefixes(self) -> List[str]:
        return list_prefixes(self.rings_dir, self.kind)

    def prefix_contains_symbol(self, prefix: str) -> bool:
        """Scan index slots (lightweight) and return True if we find the symbol in any payload."""
        try:
            meta_f, meta_mm = open_mmap_read(prefix + ".meta")
            data_f, data_mm = open_mmap_read(prefix + ".data")
        except Exception:
            return False
        try:
            hdr = read_meta_header(meta_mm)
            data_capacity = hdr["data_capacity"]
            index_slots = hdr["index_slots"]
            found = False
            # iterate slots; stop as soon as we find symbol
            for si in range(index_slots):
                off, ln, seq, kind = read_index_slot(meta_mm, si)
                if seq == 0:
                    continue
                try:
                    rec = parse_record_at(data_mm, data_capacity, off)
                except Exception:
                    continue
                payload = rec.get("payload")
                if isinstance(payload, dict):
                    s = (payload.get("asset") or payload.get("s") or payload.get("symbol"))
                    if s and s.upper() == self.symbol:
                        found = True
                        break
            return found
        finally:
            try:
                meta_mm.close(); meta_f.close()
                data_mm.close(); data_f.close()
            except Exception:
                pass

    def ensure_tailers(self, prefixes: List[str]):
        """
        Called periodically: discover current prefixes and start tailers for prefixes
        where the symbol is present. Remove tailers that have not shown the symbol
        for longer than deactivate_after seconds.
        """
        now = time.time()
        # scan prefixes for presence (this is the dynamic discovery step)
        for prefix in prefixes:
            # skip if already have a tailer running
            if prefix in self.tailers:
                continue
            # cheap check: does this prefix have the symbol now?
            try:
                if self.prefix_contains_symbol(prefix):
                    # start tailer
                    t = ChunkTailer(prefix, self.symbol, poll_ms=self.poll_ms, replay=self.replay, verbose=self.verbose)
                    t.emitter = self.emitter
                    t.start()
                    self.tailers[prefix] = t
                    self.last_seen_active[prefix] = now
                    if self.verbose:
                        print(f"{iso_now()} [controller] started tailer for {prefix}")
            except Exception as e:
                if self.verbose:
                    print(f"{iso_now()} [controller] discovery error {prefix}: {e}")
                continue

        # cleanup: remove tailers that haven't had the symbol for a while
        to_remove = []
        for prefix, t in list(self.tailers.items()):
            last = self.last_seen_active.get(prefix, 0)
            # if this tailer hasn't emitted anything for a while AND the prefix doesn't currently contain symbol, stop it
            still_has = False
            try:
                still_has = self.prefix_contains_symbol(prefix)
            except Exception:
                still_has = False
            if not still_has:
                age = now - last
                if age > self.deactivate_after:
                    to_remove.append(prefix)

        for prefix in to_remove:
            t = self.tailers.pop(prefix)
            if self.verbose:
                print(f"{iso_now()} [controller] stopping tailer for {prefix} (inactive)")
            t.stop()
            self.last_seen_active.pop(prefix, None)

    def start(self):
        # main loop: rescan periodically and forward emitter output to stdout
        try:
            while not self.stopped.is_set():
                prefixes = self.discover_prefixes()
                if self.verbose:
                    print(f"{iso_now()} [controller] discovered {len(prefixes)} prefixes")
                self.ensure_tailers(prefixes)
                # process emitted messages until next rescan time
                deadline = time.time() + self.rescan_sec
                while time.time() < deadline:
                    try:
                        prefix, seq, payload = self.emitter.get(timeout=0.2)
                        # print a succinct line (you can customize)
                        bid = payload.get("bid")
                        ask = payload.get("ask")
                        mid = payload.get("mid")
                        if mid is None and isinstance(bid, (int, float)) and isinstance(ask, (int, float)):
                            mid = (bid + ask) / 2.0
                        ts = payload.get("recv_ts_ms") or payload.get("exchange_ts_ms") or int(time.time()*1000)
                        print(f"{iso_now()} {prefix} seq={seq} symbol={self.symbol} ts={ts} bid={bid} ask={ask} mid={mid}", flush=True)
                        # record that this prefix emitted data recently
                        self.last_seen_active[prefix] = time.time()
                    except queue.Empty:
                        # nothing to process now; continue waiting until deadline
                        pass
                # loop and rescan prefixes
            # end main loop
        except KeyboardInterrupt:
            print("stopping dynamic tail on keyboard interrupt")
        finally:
            self.stop_all()

    def stop_all(self):
        for p, t in self.tailers.items():
            try:
                t.stop()
            except Exception:
                pass
        self.tailers.clear()

# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("symbol", help="symbol to follow (e.g. BTCUSDT)")
    p.add_argument("--rings-dir", default="rings", help="directory containing ring files")
    p.add_argument("--kind", choices=["book","trade","both"], default="book", help="what chunk kinds to scan")
    p.add_argument("--poll-ms", type=int, default=100, help="poll interval per tailer (ms)")
    p.add_argument("--rescan-sec", type=int, default=10, help="how often to rescan all prefixes (seconds)")
    p.add_argument("--replay", type=int, default=0, help="replay last N historical records when a new prefix is added")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()

def main():
    args = parse_args()
    controller = DynamicTailController(symbol=args.symbol,
                                      rings_dir=args.rings_dir,
                                      kind=args.kind,
                                      poll_ms=args.poll_ms,
                                      rescan_sec=args.rescan_sec,
                                      replay=args.replay,
                                      verbose=args.verbose)
    controller.start()

if __name__ == "__main__":
    main()
