# 🧩 Market Data Consolidator

A **high-performance market data mirror** implemented in **Rust**, using **memory-mapped ring buffers (mmap)** for near-zero-latency data persistence.

It provides real-time ingestion from **Binance** and **Deribit**, persisting all trades into structured `.meta` and `.data` files, and supports validation and replay using Python or C++ tools.

---

## ⚙️ Architecture Overview

[Exchange WS]
│
▼
[Async Rust Ingestor]
│
▼
[Chunk Writer → mmap ring (.meta/.data)]
│
├── Metrics Aggregator → metrics.csv
├── Python Live Reader → CSV or stdout
└── C++ Comparator → WS vs mmap diff


Each connector (Binance, Deribit) runs independently and writes its own ring files.

Example file layout:

connectors/bin-binance/src/ingest/rings/binance_trade_chunk0.{meta,data}
connectors/bin-deribit/src/ingest/rings/deribit_trade_chunk0.{meta,data}


---

## 🧩 Components

| Component | Language | Description |
|------------|-----------|--------------|
| `core/` | Rust | mmap ring implementation, writers, metrics |
| `connectors/bin-binance/` | Rust | Binance WebSocket → mmap |
| `connectors/bin-deribit/` | Rust | Deribit authenticated WS → mmap |
| `mmap_trade_compare.cpp` | C++ | Compare WS feed vs mmap |
| `live_mmap_tail.py`, `deribit_map_reader.py` | Python | Real-time readers & CSV exporters |

---

## 🧰 System Requirements

### OS
Linux (tested on Pop!\_OS / Ubuntu 22.04+)

### Dependencies
```bash
sudo apt update
sudo apt install -y build-essential libssl-dev libboost-all-dev pkg-config python3 python3-pip

Install Rust:

curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup default stable

🔧 Building
1. Rust

cargo build --release

Or run specific connectors in debug mode:
Binance

RUST_LOG=debug cargo run -p bin-binance

Deribit

RUST_LOG=debug cargo run -p bin-deribit

2. C++ Comparator

g++ -std=c++17 -O2 -pthread mmap_trade_compare.cpp -o mmap_trade_compare \
    -lboost_system -lboost_filesystem -lssl -lcrypto

🚀 Running
Binance Ingest

# from project root
RUST_LOG=debug cargo run -p bin-binance

This will:

    Fetch Binance symbols via REST

    Split them into 400-symbol chunks

    Subscribe to combined WebSocket trade streams

    Write to connectors/bin-binance/src/ingest/rings/

Deribit Ingest
Credentials handling

In sources_deribit.rs the client credentials are loaded as:

let client_id = std::env::var("DERIBIT_CLIENT_ID").unwrap_or_else(|_| {
    // fallback placeholder - replace with your real ID or set env DERIBIT_CLIENT_ID
    "".to_string()
});
let client_secret = std::env::var("DERIBIT_CLIENT_SECRET").unwrap_or_else(|_| "".to_string());

You can either:

    Use environment variables (recommended):

export DERIBIT_CLIENT_ID="your_id"
export DERIBIT_CLIENT_SECRET="your_secret"
RUST_LOG=debug cargo run -p bin-deribit

Hardcode credentials for local testing (not secure):
Replace those two lines with:

    let client_id = "jUJDR2_r".to_string();
    let client_secret = "sUIzI72kafD68j7RdTKSN2RcmErbLGPONsLB7Vc_A80".to_string();

        ⚠️ Security Note: Never commit or push these credentials.
        Add the source file to .gitignore or revert before pushing.

Deribit writes to:

connectors/bin-deribit/src/ingest/rings/deribit_trade_chunkN.{meta,data}

🧠 Reading Data (Python)
1. Dump all trades into CSV

cd connectors/bin-deribit/src/ingest
./deribit_map_reader.py --rings ./rings --out out --poll-ms 100 --verbose --ensure-out

2. Live stream selected symbols

python3 live_mmap_tail.py --rings ./connectors/bin-deribit/src/ingest/rings --filter BTC-PERPETUAL --poll-ms 100

Example output:

2025-11-08 20:12:11 BTC-PERPETUAL  Sell px=102049.5 sz=1280.0
2025-11-08 20:12:16 BTC-PERPETUAL  Buy  px=102029.0 sz=90.0

🧮 Data Structure

Each record in .data is serialized JSON of a TradePrint:

{
  "exchange": "DERIBIT",
  "asset": "BTC_USDC-PERPETUAL",
  "kind": "Spot",
  "px": 101872.0,
  "sz": 0.0001,
  "side": "Sell",
  "exchange_trade_id": 2758805,
  "exchange_ts_ms": 1762631812784,
  "writer_ts_ms": 1762631812992,
  "recv_ts_ms": 1762631812992,
  "parse_ts_ms": 1762631812992,
  "enqueue_ts_ms": 1762631812992
}

Timing Fields
Field	Description
exchange_ts_ms	Timestamp from exchange feed
recv_ts_ms	When WS message was received
parse_ts_ms	When message was parsed
enqueue_ts_ms	When queued for writer
writer_ts_ms	When serialized/written
persist_ts_ms	When actually persisted
🧩 Comparator (C++)

Run the comparator after writing some data:

./mmap_trade_compare --exchange deribit --rings ./connectors/bin-deribit/src/ingest/rings --filters ./filters.json --out out

Example filters.json:

{
  "symbols": ["BTC-PERPETUAL", "ETH-PERPETUAL", "SOLUSDT"],
  "exchanges": ["BINANCE", "DERIBIT"]
}

This tool:

    Connects to the exchange WS

    Reads the mmap rings concurrently

    Matches trades by timestamp & price

    Outputs per-symbol CSVs into out/

🧾 metrics.csv Overview

Example fields logged every second:
Field	Description
trade_count	trades per second
trade_bytes	serialized size written
recv_to_write_ms_avg	avg latency
recv_to_write_ms_max	worst latency
writer_seq	current sequence number
dedupe_size	deduplication window size

Example values:

2025-11-08T20:04:12Z,trade_count=650,trade_bytes=82000,recv_to_write_ms_avg=48

🧩 Reliability Summary
Aspect	Status	Confidence
Feed correctness	✅ Verified by comparator
Timing alignment	✅ ±2 ms offset typical
Disk persistence	✅ Preallocated (posix_fallocate)
Data integrity	✅ CRC validated on read
Small trades visibility	✅ Confirmed visible (no Deribit filtering)
Deribit auth reliability	✅ with client_credentials
Ring writer	✅ Confirmed writing sequentially
⚠️ Security Notes

    Do not commit credentials.
    Always store Deribit or Binance secrets as environment variables or local .env files excluded from version control.

    Do not run production writers as root.
    Use a dedicated low-privilege user to isolate file permissions.

    File storage isolation:
    Each exchange’s ring directory is sandboxed. Never point both connectors to the same path.

    Persistence safety:
    .meta and .data files are preallocated with posix_fallocate to prevent fragmentation or partial writes.
    Use SSDs with sync_writes = false (async flusher enabled).

    System tuning:

        Increase file descriptor limits: ulimit -n 65535

        Increase locked memory for mmap: ulimit -l unlimited

        Disable power-saving CPU scaling (for latency consistency).

        Optionally use taskset or cset shield to isolate CPU cores.

    Network tuning:

        Use wired connections and disable Nagle’s algorithm (already handled in WS layer).

        Keep NIC timestamping enabled if supported.

        Synchronize time with chronyd or systemd-timesyncd.

    Security hygiene:

        Ensure Cargo.toml dependencies are up-to-date.

        Do not expose ring files or internal metrics over public endpoints.

        Use a dedicated user for service daemons.

🧠 Troubleshooting
Symptom	Likely Cause	Fix
CSV empty	Wrong --rings path	Point to correct connector folder
raw_subscriptions_not_available_for_unauthorized	Missing Deribit auth	Set or hardcode credentials
Only few trades visible	Filters applied	Remove --filter or edit filters.json
mmap seq stays 0	Writer not writing	Check for panic logs
Comparator never writes	One source missing	Wait for both ws_seen + mmap_seen
Permission denied	Non-writable path	Fix folder ownership or chmod
🧩 Git Workflow

git checkout main
git pull origin main
git checkout -b feat/update-readme
git add README.md
git commit -m "docs: update README for binance/deribit + mmap comparator + security notes"
git push -u origin feat/update-readme

Then open a PR to merge feat/update-readme → main.
📜 License

MIT License © 2025 — StringMeta Project
