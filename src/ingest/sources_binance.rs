// src/ingest/sources_binance.rs
//! Binance ingestion: per-chunk websocket readers + per-chunk mmap ring writers.
//! - computes meta_size with MmapRing::compute_meta_size for index_slots
//! - preallocates data and meta files (posix_fallocate on unix when available)

use crate::metrics::MetricsAggregator;
use crate::storage::mmap_ring::{MmapRing, MmapRingConfig, DEFAULT_META_SIZE};
use crate::types::{
    AssetKind, ExchangeStatus, ExchangeStatusKind, OrderBookSnapshot, TradePrint, TradeSide,
};
use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use reqwest::Client;
use serde_json::Value;
use std::collections::HashMap;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc::UnboundedReceiver, mpsc::UnboundedSender, watch};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

#[derive(Clone, Debug)]
pub struct BinanceSources {
    pub name: String,
    pub ws_url: String,
    pub books: Vec<String>,
    pub trades: Vec<String>,
    pub chunk_size: usize,
    pub chunk_data_capacity: usize,
    pub chunk_index_slots: usize,
    pub chunk_sync_writes: bool,
    pub chunk_dedupe_size: usize, // new: per-chunk dedupe size
}

pub fn crate_level_binance_struct_from_parts(
    name: String,
    ws_url: String,
    books: Vec<String>,
    trades: Vec<String>,
) -> BinanceSources {
    BinanceSources {
        name,
        ws_url,
        books,
        trades,
        chunk_size: 400,
        chunk_data_capacity: 512 * 1024 * 1024, // 512 MiB
        chunk_index_slots: 32_768,
        chunk_sync_writes: false,
        chunk_dedupe_size: 200_000, // default dedupe window
    }
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(serde::Deserialize)]
struct ExchangeInfo {
    symbols: Vec<SymbolInfo>,
}
#[derive(serde::Deserialize)]
struct SymbolInfo {
    symbol: String,
    status: Option<String>,
}

async fn fetch_all_binance_symbols(client: &Client) -> Result<Vec<String>> {
    let url = "https://api.binance.com/api/v3/exchangeInfo";
    let resp = client.get(url).send().await?;
    let info: ExchangeInfo = resp.json().await?;
    let mut syms: Vec<String> = info
        .symbols
        .into_iter()
        .filter(|s| s.status.as_deref().unwrap_or("TRADING") == "TRADING")
        .map(|s| s.symbol.to_uppercase())
        .collect();
    syms.sort();
    Ok(syms)
}

fn chunks<T: Clone>(vec: &[T], chunk_size: usize) -> Vec<Vec<T>> {
    if chunk_size == 0 {
        return vec![vec![]];
    }
    vec.chunks(chunk_size).map(|c| c.to_vec()).collect()
}

fn backoff_ms_with_jitter(base_ms: u64, attempt: u32, cap_ms: u64) -> u64 {
    let mut rng = rand::thread_rng();
    let exp_pow = std::cmp::min(attempt, 63);
    let multiplier = 1u64.checked_shl(exp_pow).unwrap_or(u64::MAX);
    let mut exp = base_ms.saturating_mul(multiplier);
    if exp > cap_ms {
        exp = cap_ms;
    }
    rng.gen_range(0..=exp)
}

/// preallocate file (create parent directories if needed)
fn preallocate_file(path: &PathBuf, size: u64) -> Result<(), anyhow::Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let f = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
    #[cfg(unix)]
    {
        // attempt posix_fallocate to avoid sparse file if available
        unsafe {
            // Use posix_fallocate; ignore errno usage, just check return code
            let fd = f.as_raw_fd();
            let r = libc::posix_fallocate(fd, 0, size as libc::off_t);
            if r == 0 {
                return Ok(());
            } else {
                tracing::warn!("posix_fallocate returned {} - falling back to set_len", r);
            }
        }
    }
    f.set_len(size)?;
    Ok(())
}

/// Spawn binance connectors by chunking symbol lists and creating per-chunk ring writers.
pub async fn spawn_binance_from_config(
    bin_cfg: &BinanceSources,
    tx_snap: UnboundedSender<OrderBookSnapshot>,
    tx_tr: UnboundedSender<TradePrint>,
    status_tx: watch::Sender<ExchangeStatus>,
    metrics: Arc<MetricsAggregator>,
) -> Result<()> {
    let client = Client::new();

    // resolve books/trades lists (support "ALL")
    let books: Vec<String> = if bin_cfg.books.len() == 1 && bin_cfg.books[0].to_uppercase() == "ALL"
    {
        info!(exchange=%bin_cfg.name, "querying Binance exchangeInfo for all symbols");
        match fetch_all_binance_symbols(&client).await {
            Ok(v) => {
                info!(exchange=%bin_cfg.name, "fetched {} symbols from exchangeInfo", v.len());
                v
            }
            Err(e) => {
                warn!(exchange=%bin_cfg.name, "failed to fetch symbols via REST: {:?}", e);
                bin_cfg.books.clone()
            }
        }
    } else {
        bin_cfg.books.clone()
    };

    let trades: Vec<String> = if bin_cfg.trades.is_empty() {
        books.clone()
    } else if bin_cfg.trades.len() == 1 && bin_cfg.trades[0].to_uppercase() == "ALL" {
        match fetch_all_binance_symbols(&client).await {
            Ok(v) => v,
            Err(_) => bin_cfg.trades.clone(),
        }
    } else {
        bin_cfg.trades.clone()
    };

    // chunking
    let chunk_size = if bin_cfg.chunk_size == 0 {
        400
    } else {
        bin_cfg.chunk_size
    };
    let book_chunks = chunks(&books, chunk_size);
    let trade_chunks = chunks(&trades, chunk_size);

    // local clones
    let exchange_name = bin_cfg.name.clone();
    let ws_url = bin_cfg.ws_url.clone();

    // spawn book chunk writers/readers
    for (i, chunk_symbols) in book_chunks.into_iter().enumerate() {
        if chunk_symbols.is_empty() {
            continue;
        }

        // per-iteration exchange clone (fix borrow/move issues)
        let ex = exchange_name.clone();
        let ring_prefix = format!("rings/{}_book_chunk{}", ex.to_lowercase(), i);
        let data_path = PathBuf::from(format!("{}.data", ring_prefix));
        if let Err(e) = preallocate_file(&data_path, bin_cfg.chunk_data_capacity as u64) {
            warn!(exchange=%ex, chunk=i, "preallocate data failed: {:?}", e);
        }

        // compute meta_size required for index_slots
        let meta_size = std::cmp::max(
            DEFAULT_META_SIZE,
            MmapRing::compute_meta_size(bin_cfg.chunk_index_slots)?,
        );

        let meta_path = PathBuf::from(format!("{}.meta", ring_prefix));
        if let Err(e) = preallocate_file(&meta_path, meta_size as u64) {
            warn!(exchange=%ex, chunk=i, "preallocate meta failed: {:?}", e);
        }

        let ring_cfg = MmapRingConfig {
            path_prefix: ring_prefix.clone(),
            data_capacity: bin_cfg.chunk_data_capacity,
            index_slots: bin_cfg.chunk_index_slots,
            meta_size,
            create: true,
            force_reinit: true,
            sync_writes: bin_cfg.chunk_sync_writes,
            flusher_interval_ms: 50, // default; tune down for HFT testing
        };
        let ring = Arc::new(MmapRing::open(&ring_cfg)?);

        // per-chunk channel
        let (chunk_tx, mut chunk_rx): (
            UnboundedSender<OrderBookSnapshot>,
            UnboundedReceiver<OrderBookSnapshot>,
        ) = tokio::sync::mpsc::unbounded_channel();

        // writer task: dedupe simple cache by (bid, ask) per asset
        let ring_w = ring.clone();
        let metrics_w = metrics.clone();
        tokio::spawn(async move {
            let mut last_vals: HashMap<String, (f64, f64)> = HashMap::new();
            while let Some(mut snap) = chunk_rx.recv().await {
                let key = snap.asset.clone();
                let should_write = match last_vals.get(&key) {
                    Some(&(b, a)) => !(b == snap.bid && a == snap.ask),
                    None => true,
                };
                if should_write {
                    if let Ok(payload) = serde_json::to_vec(&snap) {
                        if let Ok(_seq) = ring_w.write_record(0u8, &payload) {
                            // populate writer timestamp for local batch write
                            snap.writer_ts_ms = Some(now_ms());
                            last_vals.insert(key, (snap.bid, snap.ask));
                            // metrics - compute hop timings
                            let recv_ts = snap.recv_ts_ms as u64;
                            if let Some(ex_ts) = snap.exchange_ts_ms.map(|v| v as u64) {
                                let write_ts = snap.writer_ts_ms.unwrap() as u64;
                                let hop_ex_recv = recv_ts.saturating_sub(ex_ts);
                                let hop_recv_write = (write_ts as u64).saturating_sub(recv_ts);
                                let total = (write_ts as u64).saturating_sub(ex_ts);
                                metrics_w.record_snapshot_latency(
                                    hop_ex_recv,
                                    hop_recv_write,
                                    total,
                                    payload.len() as u64,
                                );
                            } else {
                                let write_ts = snap.writer_ts_ms.unwrap() as u64;
                                let hop_recv_write = (write_ts as u64).saturating_sub(recv_ts);
                                metrics_w.record_snapshot_latency(
                                    0,
                                    hop_recv_write,
                                    hop_recv_write,
                                    payload.len() as u64,
                                );
                            }
                        } else {
                            warn!(exchange=%ex, chunk=i, "chunk ring.write_record failed");
                        }
                    }
                }
            }
        });

        // health counters (per-chunk)
        let recv_msgs = Arc::new(AtomicU64::new(0));
        let recv_bytes = Arc::new(AtomicU64::new(0));
        let reconnects = Arc::new(AtomicU64::new(0));
        let last_exchange_ts_ms = Arc::new(AtomicI64::new(0));
        let ex = exchange_name.clone();
        // spawn websocket reader for this chunk (use per-iteration ex clone)
        let ex_clone = ex.clone();
        let url_clone = ws_url.clone();
        let tx_snap_clone = tx_snap.clone();
        let chunk_tx_clone = chunk_tx.clone();
        let status_tx_clone = status_tx.clone();

        let recv_msgs_c = recv_msgs.clone();
        let recv_bytes_c = recv_bytes.clone();
        let reconnects_c = reconnects.clone();
        let last_exchange_ts_c = last_exchange_ts_ms.clone();

        tokio::spawn(async move {
            if let Err(e) = run_binance_book_stream_chunk(
                &ex_clone,
                &url_clone,
                i,
                chunk_symbols,
                tx_snap_clone,
                chunk_tx_clone,
                status_tx_clone,
                recv_msgs_c,
                recv_bytes_c,
                reconnects_c,
                last_exchange_ts_c,
            )
            .await
            {
                warn!(exchange=%ex_clone, chunk=i, "book chunk task error: {:?}", e);
            }
        });

        info!(exchange=%ex, chunk=i, "spawned book chunk writer and reader; ring={}", ring_prefix);
    }

    // spawn trade chunks (same pattern as book chunks)
    for (i, chunk_symbols) in trade_chunks.into_iter().enumerate() {
        if chunk_symbols.is_empty() {
            continue;
        }

        let ex = exchange_name.clone();
        let ring_prefix = format!("rings/{}_trade_chunk{}", ex.to_lowercase(), i);
        let data_path = PathBuf::from(format!("{}.data", ring_prefix));
        if let Err(e) = preallocate_file(&data_path, bin_cfg.chunk_data_capacity as u64) {
            warn!(exchange=%ex, chunk=i, "preallocate data failed: {:?}", e);
        }

        let meta_size = std::cmp::max(
            DEFAULT_META_SIZE,
            MmapRing::compute_meta_size(bin_cfg.chunk_index_slots)?,
        );

        let meta_path = PathBuf::from(format!("{}.meta", ring_prefix));
        if let Err(e) = preallocate_file(&meta_path, meta_size as u64) {
            warn!(exchange=%ex, chunk=i, "preallocate meta failed: {:?}", e);
        }

        let ring_cfg = MmapRingConfig {
            path_prefix: ring_prefix.clone(),
            data_capacity: bin_cfg.chunk_data_capacity,
            index_slots: bin_cfg.chunk_index_slots,
            meta_size,
            create: true,
            force_reinit: true,
            sync_writes: bin_cfg.chunk_sync_writes,
            flusher_interval_ms: 50, // batch-flush interval for background flusher
        };
        let ring = Arc::new(MmapRing::open(&ring_cfg)?);

        let (chunk_tx, mut chunk_rx): (UnboundedSender<TradePrint>, UnboundedReceiver<TradePrint>) =
            tokio::sync::mpsc::unbounded_channel();

        let ring_w = ring.clone();
        let metrics_w = metrics.clone();
        tokio::spawn(async move {
            // for trades we typically always write
            while let Some(mut tp) = chunk_rx.recv().await {
                if let Ok(payload) = serde_json::to_vec(&tp) {
                    if let Ok(_seq) = ring_w.write_record(1u8, &payload) {
                        tp.writer_ts_ms = Some(now_ms());
                        metrics_w.record_trade_latency(0, 0, 0, payload.len() as u64);
                    } else {
                        warn!(exchange=%ex, chunk=i, "trade chunk ring.write_record failed");
                    }
                }
            }
        });

        let recv_msgs = Arc::new(AtomicU64::new(0));
        let recv_bytes = Arc::new(AtomicU64::new(0));
        let reconnects = Arc::new(AtomicU64::new(0));
        let last_exchange_ts_ms = Arc::new(AtomicI64::new(0));
        let ex = exchange_name.clone();
        let ex_for_task = ex.clone(); // one copy per task
        let url_clone = ws_url.clone();
        let tx_tr_clone = tx_tr.clone();
        let chunk_tx_clone = chunk_tx.clone();
        let status_tx_clone = status_tx.clone();

        let recv_msgs_c = recv_msgs.clone();
        let recv_bytes_c = recv_bytes.clone();
        let reconnects_c = reconnects.clone();
        let last_exchange_ts_c = last_exchange_ts_ms.clone();

        tokio::spawn(async move {
            if let Err(e) = run_binance_trade_stream_chunk(
                &ex_for_task,
                &url_clone,
                i,
                chunk_symbols,
                tx_tr_clone,
                chunk_tx_clone,
                status_tx_clone,
                recv_msgs_c,
                recv_bytes_c,
                reconnects_c,
                last_exchange_ts_c,
            )
            .await
            {
                warn!(exchange=%ex_for_task, chunk=i, "trade chunk task error: {:?}", e);
            }
        });

        info!(exchange=%ex, chunk=i, "spawned trade chunk writer and reader; ring={}", ring_prefix);
    }

    Ok(())
}

// ---------------------------------------------------------------------
// chunked websocket readers
// ---------------------------------------------------------------------
#[allow(clippy::too_many_arguments)]
async fn run_binance_book_stream_chunk(
    exchange: &str,
    ws_url: &str,
    chunk_id: usize,
    symbols: Vec<String>,
    tx_snap: UnboundedSender<OrderBookSnapshot>,
    chunk_writer_tx: UnboundedSender<OrderBookSnapshot>,
    status_tx: watch::Sender<ExchangeStatus>,
    recv_msgs: Arc<AtomicU64>,
    recv_bytes: Arc<AtomicU64>,
    reconnects: Arc<AtomicU64>,
    last_exchange_ts_ms: Arc<AtomicI64>,
) -> Result<()> {
    let streams_joined = streams_for_symbols(&symbols, "bookTicker");

    if streams_joined.len() > 60_000 {
        warn!(exchange=%exchange, chunk=chunk_id, streams_len=%streams_joined.len(), "constructed stream list is very large; consider reducing chunk_size");
    }

    let base = {
        let mut s = ws_url.trim_end_matches('/').to_string();
        if s.ends_with("/ws") {
            s.truncate(s.len() - "/ws".len());
        } else if s.ends_with("/stream") {
            s.truncate(s.len() - "/stream".len());
        }
        s
    };

    let base_ms = 250u64;
    let cap_ms = 30_000u64;
    let mut attempt: u32 = 0;

    loop {
        let url = format!("{}/stream?streams={}", base, streams_joined);
        info!(exchange=%exchange, chunk=chunk_id, "connecting book stream: {}", url);

        let _ = status_tx.send(ExchangeStatus {
            exchange: exchange.to_string(),
            kind: ExchangeStatusKind::Reconnecting,
            detail: Some(format!(
                "connecting attempt {} (chunk {})",
                attempt + 1,
                chunk_id
            )),
            ts_ms: now_ms(),
        });

        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws_stream, _resp)) => {
                info!(exchange=%exchange, chunk=chunk_id, "book stream connected");
                let _ = status_tx.send(ExchangeStatus {
                    exchange: exchange.to_string(),
                    kind: ExchangeStatusKind::Connected,
                    detail: Some(format!("chunk {}", chunk_id)),
                    ts_ms: now_ms(),
                });

                attempt = 0;
                let (mut write, mut read) = ws_stream.split();
                let mut ping_interval = tokio::time::interval(Duration::from_secs(15));

                loop {
                    tokio::select! {
                        _ = ping_interval.tick() => {
                            if let Err(e) = write.send(Message::Ping(Vec::new())).await {
                                warn!(exchange=%exchange, chunk=chunk_id,"book ping failed: {:?}", e);
                                break;
                            }
                        }
                        msg_opt = read.next() => {
                            let Some(msg) = msg_opt else {
                                warn!(exchange=%exchange, chunk=chunk_id,"book chunk stream ended, reconnecting");
                                break;
                            };
                            match msg {
                                Ok(Message::Text(txt)) => {
                                    let recv_ts = now_ms();
                                    recv_msgs.fetch_add(1, Ordering::Relaxed);
                                    recv_bytes.fetch_add(txt.len() as u64, Ordering::Relaxed);

                                    if let Ok(v) = serde_json::from_str::<Value>(&txt) {
                                        if let Some(data) = v.get("data").cloned().or(Some(v.clone())) {
                                            if let Some(sym) = data.get("s").and_then(|x| x.as_str()) {
                                                let bid = data.get("b")
                                                    .and_then(|x| x.as_str())
                                                    .and_then(|s| s.parse::<f64>().ok())
                                                    .unwrap_or(f64::NAN);
                                                let ask = data.get("a")
                                                    .and_then(|x| x.as_str())
                                                    .and_then(|s| s.parse::<f64>().ok())
                                                    .unwrap_or(f64::NAN);
                                                let ts_opt = data.get("E").and_then(|x| x.as_i64());
                                                if let Some(tsv) = ts_opt {
                                                    last_exchange_ts_ms.store(tsv, Ordering::Relaxed);
                                                }
                                                let event_u = data.get("u").and_then(|x| x.as_u64());
                                                if bid.is_finite() && ask.is_finite() && ask >= bid {
                                                    let mid = 0.5*(bid+ask);
                                                    let snap = OrderBookSnapshot {
                                                        exchange: exchange.to_string(),
                                                        asset: sym.to_uppercase(),
                                                        kind: AssetKind::Spot,
                                                        bid,
                                                        ask,
                                                        mid,
                                                        exchange_ts_ms: ts_opt,
                                                        event_u,
                                                        recv_ts_ms: recv_ts,
                                                        writer_ts_ms: None,
                                                    };
                                                    // send to central consumers
                                                    if tx_snap.send(snap.clone()).is_err() {
                                                        warn!(exchange=%exchange, chunk=chunk_id,"tx_snap closed; stopping book chunk stream");
                                                        break;
                                                    }
                                                    // send to chunk local writer, best-effort
                                                    let _ = chunk_writer_tx.send(snap);
                                                }
                                            }
                                        }
                                    } else {
                                        debug!(exchange=%exchange, chunk=chunk_id,"book text not JSON: {}", txt);
                                    }
                                }
                                Ok(Message::Close(_)) => {
                                    warn!(exchange=%exchange, chunk=chunk_id,"book close received, reconnecting");
                                    break;
                                }
                                Err(e) => {
                                    warn!(exchange=%exchange, chunk=chunk_id,"book recv error: {:?}", e);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(e) => {
                warn!(exchange=%exchange, chunk=chunk_id, "failed to connect book stream: {:?}; will backoff", e);
                reconnects.fetch_add(1, Ordering::Relaxed);
                let _ = status_tx.send(ExchangeStatus {
                    exchange: exchange.to_string(),
                    kind: ExchangeStatusKind::Reconnecting,
                    detail: Some(format!("connect failed: {:?}", e)),
                    ts_ms: now_ms(),
                });
                let backoff = backoff_ms_with_jitter(base_ms, attempt, cap_ms);
                attempt = attempt.saturating_add(1);
                tokio::time::sleep(Duration::from_millis(backoff)).await;
                continue;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_binance_trade_stream_chunk(
    exchange: &str,
    ws_url: &str,
    chunk_id: usize,
    symbols: Vec<String>,
    tx_tr: UnboundedSender<TradePrint>,
    chunk_writer_tx: UnboundedSender<TradePrint>,
    status_tx: watch::Sender<ExchangeStatus>,
    recv_msgs: Arc<AtomicU64>,
    recv_bytes: Arc<AtomicU64>,
    reconnects: Arc<AtomicU64>,
    last_exchange_ts_ms: Arc<AtomicI64>,
) -> Result<()> {
    let streams_joined = streams_for_symbols(&symbols, "trade");

    if streams_joined.len() > 60_000 {
        warn!(exchange=%exchange, chunk=chunk_id, streams_len=%streams_joined.len(), "constructed stream list is very large; consider reducing chunk_size");
    }

    let base = {
        let mut s = ws_url.trim_end_matches('/').to_string();
        if s.ends_with("/ws") {
            s.truncate(s.len() - "/ws".len());
        } else if s.ends_with("/stream") {
            s.truncate(s.len() - "/stream".len());
        }
        s
    };

    let base_ms = 250u64;
    let cap_ms = 30_000u64;
    let mut attempt: u32 = 0;

    loop {
        let url = format!("{}/stream?streams={}", base, streams_joined);
        info!(exchange=%exchange, chunk=chunk_id, "connecting trades stream: {}", url);

        let _ = status_tx.send(ExchangeStatus {
            exchange: exchange.to_string(),
            kind: ExchangeStatusKind::Reconnecting,
            detail: Some(format!(
                "connecting attempt {} (chunk {})",
                attempt + 1,
                chunk_id
            )),
            ts_ms: now_ms(),
        });

        match tokio_tungstenite::connect_async(&url).await {
            Ok((ws_stream, _resp)) => {
                info!(exchange=%exchange, chunk=chunk_id, "trades stream connected");
                let _ = status_tx.send(ExchangeStatus {
                    exchange: exchange.to_string(),
                    kind: ExchangeStatusKind::Connected,
                    detail: Some(format!("chunk {}", chunk_id)),
                    ts_ms: now_ms(),
                });

                attempt = 0;
                let (mut write, mut read) = ws_stream.split();
                let mut ping_interval = tokio::time::interval(Duration::from_secs(15));

                loop {
                    tokio::select! {
                        _ = ping_interval.tick() => {
                            if let Err(e) = write.send(Message::Ping(Vec::new())).await {
                                warn!(exchange=%exchange, chunk=chunk_id,"trade ping failed: {:?}", e);
                                break;
                            }
                        }
                        msg_opt = read.next() => {
                            let Some(msg) = msg_opt else {
                                warn!(exchange=%exchange, chunk=chunk_id,"trade chunk stream ended, reconnecting");
                                break;
                            };
                            match msg {
                                Ok(Message::Text(txt)) => {
                                    let recv_ts = now_ms();
                                    recv_msgs.fetch_add(1, Ordering::Relaxed);
                                    recv_bytes.fetch_add(txt.len() as u64, Ordering::Relaxed);

                                    if let Ok(v) = serde_json::from_str::<Value>(&txt) {
                                        if let Some(data) = v.get("data").cloned().or(Some(v.clone())) {
                                            if let Some(arr) = data.as_array() {
                                                for tr in arr {
                                                    let px = tr.get("p").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(f64::NAN);
                                                    let sz = tr.get("q").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                                    let ts = tr.get("E").and_then(|x| x.as_i64());
                                                    if let Some(tsv) = ts { last_exchange_ts_ms.store(tsv, Ordering::Relaxed); }
                                                    let event_u = tr.get("u").and_then(|x| x.as_u64());
                                                    let sym = tr.get("s").and_then(|x| x.as_str()).unwrap_or("").to_uppercase();
                                                    let side = tr.get("m").and_then(|x| x.as_bool()).map(|is_maker| if is_maker { TradeSide::Sell } else { TradeSide::Buy }).unwrap_or(TradeSide::Unknown);
                                                    if px.is_finite() && sz > 0.0 {
                                                        let tp = TradePrint {
                                                            exchange: exchange.to_string(),
                                                            asset: sym.clone(),
                                                            kind: AssetKind::Spot,
                                                            px,
                                                            sz,
                                                            side,
                                                            exchange_ts_ms: ts,
                                                            event_u,
                                                            recv_ts_ms: recv_ts,
                                                            writer_ts_ms: None,
                                                        };
                                                        // send to central consumer
                                                        if tx_tr.send(tp.clone()).is_err() {
                                                            warn!(exchange=%exchange, chunk=chunk_id,"tx_tr closed; stopping trade chunk stream");
                                                            break;
                                                        }
                                                        // local chunk writer
                                                        let _ = chunk_writer_tx.send(tp);
                                                    }
                                                }
                                            } else {
                                                if let Some(sym) = data.get("s").and_then(|x| x.as_str()) {
                                                    let px = data.get("p").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(f64::NAN);
                                                    let sz = data.get("q").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                                    let ts = data.get("E").and_then(|x| x.as_i64());
                                                    if let Some(tsv) = ts { last_exchange_ts_ms.store(tsv, Ordering::Relaxed); }
                                                    let event_u = data.get("u").and_then(|x| x.as_u64());
                                                    let side = data.get("m").and_then(|x| x.as_bool()).map(|is_maker| if is_maker { TradeSide::Sell } else { TradeSide::Buy }).unwrap_or(TradeSide::Unknown);
                                                    if px.is_finite() && sz > 0.0 {
                                                        let tp = TradePrint {
                                                            exchange: exchange.to_string(),
                                                            asset: sym.to_uppercase(),
                                                            kind: AssetKind::Spot,
                                                            px,
                                                            sz,
                                                            side,
                                                            exchange_ts_ms: ts,
                                                            event_u,
                                                            recv_ts_ms: recv_ts,
                                                            writer_ts_ms: None,
                                                        };
                                                        if tx_tr.send(tp.clone()).is_err() {
                                                            warn!(exchange=%exchange, chunk=chunk_id,"tx_tr closed; stopping trade chunk stream");
                                                            break;
                                                        }
                                                        let _ = chunk_writer_tx.send(tp);
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        debug!(exchange=%exchange, chunk=chunk_id,"trade text not JSON: {}", txt);
                                    }
                                }
                                Ok(Message::Close(_)) => {
                                    warn!(exchange=%exchange, chunk=chunk_id,"trade close received, reconnecting");
                                    break;
                                }
                                Err(e) => {
                                    warn!(exchange=%exchange, chunk=chunk_id,"trade recv error: {:?}", e);
                                    break;
                                }
                                _ => {}
                            }
                        }
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(e) => {
                warn!(exchange=%exchange, chunk=chunk_id, "failed to connect trades stream: {:?}; will backoff", e);
                reconnects.fetch_add(1, Ordering::Relaxed);
                let _ = status_tx.send(ExchangeStatus {
                    exchange: exchange.to_string(),
                    kind: ExchangeStatusKind::Reconnecting,
                    detail: Some(format!("connect failed: {:?}", e)),
                    ts_ms: now_ms(),
                });
                let backoff = backoff_ms_with_jitter(base_ms, attempt, cap_ms);
                attempt = attempt.saturating_add(1);
                tokio::time::sleep(Duration::from_millis(backoff)).await;
                continue;
            }
        }
    }
}

fn streams_for_symbols(symbols: &[String], suffix: &str) -> String {
    let mut parts: Vec<String> = Vec::with_capacity(symbols.len());
    for s in symbols {
        parts.push(format!("{}@{}", s.to_lowercase(), suffix));
    }
    parts.join("/")
}
