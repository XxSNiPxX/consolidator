#!/usr/bin/env python3
"""
stream_all_chunks_v2.py

Continuously watches all mmap chunk rings and prints nicely formatted lines
for snapshots (kind=0) and trades (kind=1). Filters are loaded from a JSON
file that can be edited live.

Usage:
  python3 stream_all_chunks_v2.py --rings-dir rings --poll-ms 50 --filter-file filters.json --kinds both

filters.json example:
{
  "symbols": ["BTCUSDT"],
  "exchanges": ["BINANCE"]
}
"""
import os, sys, time, mmap, json, glob, struct, threading, queue, argparse

META_MAGIC = b"RINGV1\x00\x00"
META_HEADER_LEN = 40
INDEX_SLOT_SIZE = 24
INDEX_SLOT_FMT = "<Q I Q B 3x"
RECORD_HEADER_FMT = "<I B 7x Q"
RECORD_HEADER_LEN = struct.calcsize(RECORD_HEADER_FMT)

def iso_now():
    t = time.gmtime()
    return time.strftime("%Y-%m-%dT%H:%M:%S", t) + f".{int(time.time()*1000)%1000:03d}Z"

def read_meta_header(mm):
    magic = mm[:8]
    if magic != META_MAGIC:
        raise RuntimeError(f"bad meta magic {magic!r}")
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
        payload = json.loads(body.decode('utf-8'))
    except Exception:
        payload = None
    return {"seq": int(seq), "kind": int(kind), "payload": payload}

class ChunkWatcher(threading.Thread):
    def __init__(self, prefix, out_q, poll_ms):
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
            # print once
            print(f"{iso_now()} [+] watching {self.prefix} capacity={self.cap} slots={self.slots} seq={self.last_seq}")
        except Exception as e:
            self.close()
            # swallow; caller will retry
            return

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
                # try reopen
                self.reopen()
                time.sleep(0.5)
                continue
            try:
                hdr = read_meta_header(self.meta_mm)
            except Exception:
                # file changed or truncated; reopen
                self.close()
                time.sleep(0.2)
                self.reopen()
                continue
            seq_now = hdr["seq"]
            if seq_now < self.last_seq:
                # reinit/wrap: reset
                self.last_seq = 0
            if seq_now > self.last_seq:
                # scan index slots looking for seq > last_seq
                for i in range(hdr["slots"]):
                    try:
                        off, ln, seq, kind = read_index_slot(self.meta_mm, i)
                    except Exception:
                        continue
                    if seq > self.last_seq:
                        rec = parse_record(self.data_mm, self.cap, off)
                        if rec and rec["seq"] == seq and isinstance(rec["payload"], dict):
                            rec["prefix"] = self.prefix
                            self.out_q.put(rec)
                self.last_seq = seq_now
            time.sleep(self.poll_ms / 1000.0)

    def stop(self):
        self.stop_evt.set()

class FilterManager:
    def __init__(self, path=None):
        self.path = path
        self.symbols = []
        self.exchanges = []
        self.last_load = 0
        self.reload_int = 1.5

    def reload(self):
        if not self.path or not os.path.exists(self.path):
            return
        if time.time() - self.last_load < self.reload_int:
            return
        try:
            with open(self.path,'r') as f:
                obj = json.load(f)
            self.symbols = [s.upper() for s in obj.get("symbols", []) if isinstance(s, str)]
            self.exchanges = [e.upper() for e in obj.get("exchanges", []) if isinstance(e, str)]
            self.last_load = time.time()
        except Exception as e:
            # ignore parse errors, don't crash
            print(f"{iso_now()} [filter] failed to load {self.path}: {e}")

    def match(self, payload):
        if not isinstance(payload, dict):
            return False
        sym = str(payload.get("asset") or payload.get("s") or payload.get("symbol") or "").upper()
        exch = str(payload.get("exchange") or "").upper()
        if self.symbols and sym not in self.symbols:
            return False
        if self.exchanges and exch not in self.exchanges:
            return False
        return True

def discover_chunks(rdir):
    prefixes = set()
    for m in glob.glob(os.path.join(rdir, "*_chunk*.meta")):
        prefixes.add(m[:-5])
    return sorted(prefixes)

def format_and_print(rec, kind):
    prefix = rec.get("prefix")
    payload = rec.get("payload") or {}
    if kind == 0:
        bid = payload.get("bid")
        ask = payload.get("ask")
        mid = payload.get("mid")
        recv_ts = payload.get("recv_ts_ms") or payload.get("exchange_ts_ms")
        print(f"{iso_now()} {prefix} SNAP {payload.get('asset')} bid={bid} ask={ask} mid={mid} ts={recv_ts}", flush=True)
    elif kind == 1:
        px = payload.get("px") or payload.get("price") or payload.get("p")
        sz = payload.get("sz") or payload.get("size") or payload.get("q")
        side = payload.get("side") or payload.get("m") or payload.get("maker") or payload.get("t")
        recv_ts = payload.get("recv_ts_ms") or payload.get("exchange_ts_ms")
        # normalize side representation
        if isinstance(side, bool):
            # Binance uses 'm' boolean where true = maker (sell). Represent as "sell"/"buy"
            side = "sell" if side else "buy"
        print(f"{iso_now()} {prefix} TRADE {payload.get('asset') or payload.get('s')} px={px} sz={sz} side={side} ts={recv_ts}", flush=True)
    else:
        # unknown kind: dump JSON
        print(f"{iso_now()} {prefix} KIND{kind} payload={json.dumps(payload, ensure_ascii=False)}", flush=True)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rings-dir", default="rings")
    ap.add_argument("--poll-ms", type=int, default=50)
    ap.add_argument("--filter-file", help="JSON file with filters (symbols/exchanges)")
    ap.add_argument("--kinds", choices=["snap","trade","both"], default="both", help="which kinds to print")
    args = ap.parse_args()

    out_q = queue.Queue()
    watchers = {}
    printed = set()  # (prefix, kind, seq) to avoid duplicates
    filters = FilterManager(args.filter_file)
    last_scan = 0

    try:
        while True:
            # discover prefixes periodically (every 1s)
            now = time.time()
            if now - last_scan > 1.0:
                prefixes = discover_chunks(args.rings_dir)
                # start new watchers
                for p in prefixes:
                    if p not in watchers:
                        w = ChunkWatcher(p, out_q, args.poll_ms)
                        w.start()
                        watchers[p] = w
                # remove watchers for deleted prefixes
                removed = [p for p in list(watchers.keys()) if p not in prefixes]
                for r in removed:
                    try:
                        watchers[r].stop()
                    except Exception:
                        pass
                    watchers.pop(r, None)
                last_scan = now

            # reload filters
            filters.reload()

            # process up to some messages quickly, then loop (prevents blocking)
            try:
                rec = out_q.get(timeout=0.5)
            except queue.Empty:
                continue

            if not isinstance(rec, dict):
                continue
            payload = rec.get("payload")
            if not payload:
                continue

            # match filters
            if not filters.match(payload):
                continue

            kind = rec.get("kind", 0)
            # kind filtering
            if args.kinds == "snap" and kind != 0:
                continue
            if args.kinds == "trade" and kind != 1:
                continue

            key = (rec.get("prefix"), kind, rec.get("seq"))
            if key in printed:
                # duplicate, skip
                continue
            printed.add(key)
            # cleanup printed set occasionally to avoid growth: keep last 100k entries
            if len(printed) > 200000:
                # simple trim by recreating from last 100k elements
                printed = set(list(printed)[-100000:])

            # nice formatting depending on kind
            format_and_print(rec, kind)

    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        for w in list(watchers.values()):
            try:
                w.stop()
            except:
                pass

if __name__ == "__main__":
    main()
