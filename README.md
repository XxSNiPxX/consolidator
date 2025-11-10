# 🧩 Market Data Consolidator

A high-performance market data mirror built in **Rust**, designed for **ultra-low-latency ingestion**, **persistence**, and **comparison** of exchange tick data.

Supports both **Binance** and **Deribit**.
Each connector subscribes to exchange WebSocket feeds, groups instruments into chunks, and writes them to **memory-mapped ring files** for durable, replayable local storage.

---

## ⚙️ Overview

The system continuously ingests trades (and optionally order books) from exchanges and writes to `.meta` / `.data` ring files with constant-time appends.

Key features:
- ✅ Chunked async WebSocket ingestion (scalable to 1000+ instruments)
- ✅ Memory-mapped ring persistence (`memmap2`)
- ✅ Fully preallocated files (no sparse growth)
- ✅ Per-second metrics via `metrics.csv`
- ✅ Python live-tail utilities for analysis
- ✅ C++ comparator for WS↔mmap validation

---

## 🧠 Architecture

[Exchange WS]
│
▼
[Ingestor (Rust)]
│
▼
[Chunk Writer → mmap ring (.meta/.data)]
│
├── Metrics Aggregator → metrics.csv
└── Python/C++ Consumers → CSV / plots / realtime diff
