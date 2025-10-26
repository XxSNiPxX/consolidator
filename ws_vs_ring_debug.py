#!/usr/bin/env python3
# ws_vs_ring_debug.py
import asyncio, websockets, json, time, mmap, struct, os
from typing import Optional

META_HEADER_SIZE = 32
INDEX_SLOT_SIZE = 24
META_OFF_LATEST_INDEX_POS = 8

def read_latest_record_from_ring(prefix: str) -> Optional[dict]:
    meta_path = prefix + ".meta"
    data_path = prefix + ".data"
    if not os.path.exists(meta_path) or not os.path.exists(data_path):
        return None
    with open(meta_path, "rb") as mf, open(data_path, "rb") as df:
        meta = mmap.mmap(mf.fileno(), 0, access=mmap.ACCESS_READ)
        data = mmap.mmap(df.fileno(), 0, access=mmap.ACCESS_READ)
        try:
            index_slots = struct.unpack_from("<Q", meta, 24)[0]
            latest_index_pos = struct.unpack_from("<Q", meta, META_OFF_LATEST_INDEX_POS)[0]
            slot_base = META_HEADER_SIZE + (latest_index_pos % index_slots) * INDEX_SLOT_SIZE
            off = struct.unpack_from("<Q", meta, slot_base)[0]
            ln = struct.unpack_from("<I", meta, slot_base + 8)[0]
            seq = struct.unpack_from("<Q", meta, slot_base + 12)[0]
            kind = struct.unpack_from("<B", meta, slot_base + 20)[0]
            if ln == 0:
                return None
            data_capacity = data.size()
            if off + ln <= data_capacity:
                raw = data[off: off + ln]
            else:
                first = data_capacity - off
                raw = data[off:] + data[:(ln - first)]
            # decode as in our reader
            seq = struct.unpack_from("<Q", raw, 0)[0]
            writer_ts = struct.unpack_from("<Q", raw, 8)[0]
            payload_len = struct.unpack_from("<I", raw, 16)[0]
            payload_bytes = raw[24:24+payload_len] if 24+payload_len <= len(raw) else raw[24:]
            try:
                payload = json.loads(payload_bytes.decode("utf-8"))
            except Exception:
                payload = {"_raw": payload_bytes.hex()}
            return {"seq": int(seq), "writer_ts": int(writer_ts), "payload": payload}
        finally:
            meta.close()
            data.close()

async def ws_printer(symbol: str, ring_prefix: str):
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@bookTicker"
    print("CONNECT", url)
    async with websockets.connect(url, max_size=2**22) as ws:
        async for msg in ws:
            now_ms = int(time.time()*1000)
            try:
                j = json.loads(msg)
            except Exception:
                j = {"_raw": msg}
            ring = read_latest_record_from_ring(ring_prefix)
            print("WS:", now_ms, "|", j)
            if ring:
                print("RING:", ring["seq"], ring["writer_ts"], "|", ring["payload"])
            else:
                print("RING: <no record yet>")
            print("-"*80)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python3 ws_vs_ring_debug.py BTCUSDT rings/binance_book_chunk0")
        sys.exit(1)
    sym = sys.argv[1]
    prefix = sys.argv[2]
    asyncio.run(ws_printer(sym, prefix))
