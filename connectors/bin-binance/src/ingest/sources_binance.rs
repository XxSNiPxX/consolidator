//! Binance trade-only ingestion (chunked websocket readers -> per-chunk mmap ring writers).
//! Optional simd-json hot path behind feature "use_simd_json".

use anyhow::Result;
use core::metrics::MetricsAggregator;
use core::types::{AssetKind, ExchangeStatus, ExchangeStatusKind, TradePrint, TradeSide};
use core::writer::{spawn_trade_chunk_writer, TradeChunkWriterConfig};
use futures_util::SinkExt;
use futures_util::StreamExt;
#[cfg(unix)]
use libc;
use rand::Rng;
use reqwest::Client;
use serde_json::Value as SerdeValue;
#[cfg(feature = "use_simd_json")]
use simd_json::owned::value::Value as SimdValue;
#[cfg(feature = "use_simd_json")]
use simd_json::ValueAccess;
use std::collections::HashMap;
use std::os::fd::AsRawFd;
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
    pub trades: Vec<String>,
    pub chunk_size: usize,
    pub chunk_data_capacity: usize,
    pub chunk_index_slots: usize,
    pub chunk_sync_writes: bool,
    pub chunk_dedupe_size: usize,
}

pub fn crate_level_binance_struct_from_parts(
    name: String,
    ws_url: String,
    trades: Vec<String>,
) -> BinanceSources {
    BinanceSources {
        name,
        ws_url,
        trades,
        chunk_size: 400,
        chunk_data_capacity: 512 * 1024 * 1024, // 512 MiB
        chunk_index_slots: 32_768,
        chunk_sync_writes: false,
        chunk_dedupe_size: 200_000,
    }
}

/// Make now_ms public for this crate.
pub fn now_ms() -> i64 {
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
        unsafe {
            let fd = f.as_raw_fd();
            let r = libc::posix_fallocate(fd, 0, size as libc::off_t);
            if r == 0 {
                return Ok(());
            } else {
                tracing::warn!("posix_fallocate failed ({}), falling back to set_len", r);
            }
        }
    }
    f.set_len(size)?;
    Ok(())
}

/// Spawn per-chunk trade writers and websocket readers.
/// Each chunk creates its own MmapRing for data+meta and a local writer task.
pub async fn spawn_binance_from_config(
    bin_cfg: &BinanceSources,
    status_tx: watch::Sender<ExchangeStatus>,
    metrics: Arc<MetricsAggregator>,
) -> Result<()> {
    let client = Client::new();

    // resolve symbols if "ALL"
    let trades: Vec<String> =
        if bin_cfg.trades.len() == 1 && bin_cfg.trades[0].to_uppercase() == "ALL" {
            info!(exchange=%bin_cfg.name, "querying Binance exchangeInfo for all symbols");
            match fetch_all_binance_symbols(&client).await {
                Ok(v) => {
                    info!(exchange=%bin_cfg.name, "fetched {} symbols from exchangeInfo", v.len());
                    v
                }
                Err(e) => {
                    warn!(exchange=%bin_cfg.name, "failed to fetch symbols via REST: {:?}", e);
                    bin_cfg.trades.clone()
                }
            }
        } else {
            bin_cfg.trades.clone()
        };

    let chunk_size = if bin_cfg.chunk_size == 0 {
        400
    } else {
        bin_cfg.chunk_size
    };
    let trade_chunks = chunks(&trades, chunk_size);

    let exchange_name = bin_cfg.name.clone();
    let ws_url = bin_cfg.ws_url.clone();

    // spawn trade chunks
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

        // compute correct meta size for the requested index_slots (no override)
        let meta_size =
            core::storage::mmap_ring::MmapRing::compute_meta_size(bin_cfg.chunk_index_slots)?;
        info!(exchange=%ex, chunk=i, index_slots=%bin_cfg.chunk_index_slots, meta_size=%meta_size, "computed meta size");
        let meta_path = PathBuf::from(format!("{}.meta", ring_prefix));
        if let Err(e) = preallocate_file(&meta_path, meta_size as u64) {
            warn!(exchange=%ex, chunk=i, "preallocate meta failed: {:?}", e);
        }

        // builder for the chunk writer config
        let writer_cfg = TradeChunkWriterConfig {
            path_prefix: ring_prefix.clone(),
            data_capacity: bin_cfg.chunk_data_capacity,
            index_slots: bin_cfg.chunk_index_slots,
            meta_size,
            create: true,
            force_reinit: false,
            sync_writes: bin_cfg.chunk_sync_writes,
            flusher_interval_ms: 50,
            periodic_log_every: 10_000,
            dedupe_size: bin_cfg.chunk_dedupe_size,
        };

        // create channel for this chunk
        let (chunk_tx, chunk_rx): (UnboundedSender<TradePrint>, UnboundedReceiver<TradePrint>) =
            tokio::sync::mpsc::unbounded_channel();

        // spawn the chunk-local writer via core writer helper
        let metrics_clone = metrics.clone();
        let ring = spawn_trade_chunk_writer(writer_cfg.clone(), chunk_rx, metrics_clone).await?;
        info!(exchange=%ex, chunk=i, "opened ring for chunk: {}", ring_prefix);

        // health counters (kept for metrics / monitoring)
        let recv_msgs = Arc::new(AtomicU64::new(0));
        let recv_bytes = Arc::new(AtomicU64::new(0));
        let reconnects = Arc::new(AtomicU64::new(0));
        let last_exchange_ts_ms = Arc::new(AtomicI64::new(0));

        // spawn websocket reader task for trades
        let ex_for_task = ex.clone();
        let url_clone = ws_url.clone();
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

#[allow(clippy::too_many_arguments)]
async fn run_binance_trade_stream_chunk(
    exchange: &str,
    ws_url: &str,
    chunk_id: usize,
    symbols: Vec<String>,
    chunk_writer_tx: UnboundedSender<TradePrint>,
    status_tx: watch::Sender<ExchangeStatus>,
    recv_msgs: Arc<AtomicU64>,
    recv_bytes: Arc<AtomicU64>,
    reconnects: Arc<AtomicU64>,
    last_exchange_ts_ms: Arc<AtomicI64>,
) -> Result<()> {
    let streams_joined = streams_for_symbols(&symbols, "trade");

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

                                    // parse trades (serde branch, optional simd branch for feature)
                                    #[cfg(feature = "use_simd_json")]
                                    {
                                        if let Ok(v) = simd_json::to_owned_value(txt.as_bytes()) {
                                            let parse_ts = now_ms();
                                            let data = v.get("data").cloned().unwrap_or(v.clone());
                                            if let Some(arr) = data.as_array() {
                                                for tr in arr {
                                                    // attempt to extract fields defensively
                                                    let px = tr.get("p").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(f64::NAN);
                                                    let sz = tr.get("q").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                                    let exchange_trade_id = tr.get("t").and_then(|x| x.as_u64());
                                                    let ts_opt = tr.get("T").and_then(|x| x.as_i64()).or_else(|| tr.get("E").and_then(|x| x.as_i64()));
                                                    let exchange_ts_ms = ts_opt.map(|ts| if ts < 1_000_000_000_000_i64 { ts * 1000 } else { ts });
                                                    if let Some(tsv) = exchange_ts_ms { last_exchange_ts_ms.store(tsv, Ordering::Relaxed); }
                                                    let event_u = tr.get("u").and_then(|x| x.as_u64());
                                                    let sym = tr.get("s").and_then(|x| x.as_str()).unwrap_or("").to_uppercase();
                                                    let side = tr.get("m").and_then(|x| x.as_bool()).map(|is_maker| if is_maker { TradeSide::Sell } else { TradeSide::Buy }).unwrap_or(TradeSide::Unknown);
                                                    if px.is_finite() && sz > 0.0 {
                                                        let mut tp = TradePrint {
                                                            exchange: exchange.to_string(),
                                                            asset: sym.clone(),
                                                            kind: AssetKind::Spot,
                                                            px,
                                                            sz,
                                                            side,
                                                            exchange_trade_id,
                                                            exchange_ts_ms,
                                                            event_u,
                                                            writer_seq: None,
                                                            writer_ts_ms: None,
                                                            recv_ts_ms: recv_ts,
                                                            parse_ts_ms: Some(parse_ts),
                                                            enqueue_ts_ms: None,
                                                            persist_ts_ms: None,
                                                        };
                                                        tp.enqueue_ts_ms = Some(now_ms());
                                                        if let Err(e) = chunk_writer_tx.send(tp) {
                                                            warn!(exchange=%exchange, chunk=chunk_id, "chunk_writer_tx send failed: {:?}", e);
                                                        }
                                                    }
                                                }
                                            }
                                        } else {
                                            debug!(exchange=%exchange, chunk=chunk_id,"simd-json trade parse failed");
                                        }
                                    }

                                    #[cfg(not(feature = "use_simd_json"))]
                                    {
                                        if let Ok(v) = serde_json::from_str::<SerdeValue>(&txt) {
                                            let parse_ts = now_ms();
                                            if let Some(data) = v.get("data").cloned().or(Some(v.clone())) {
                                                if let Some(arr) = data.as_array() {
                                                    for tr in arr {
                                                        let px = tr.get("p").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(f64::NAN);
                                                        let sz = tr.get("q").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);

                                                        let exchange_trade_id = tr.get("t").and_then(|x| x.as_u64());
                                                        let ts_opt = tr.get("T").and_then(|x| x.as_i64()).or_else(|| tr.get("E").and_then(|x| x.as_i64()));
                                                        let exchange_ts_ms = ts_opt.map(|ts| if ts < 1_000_000_000_000_i64 { ts * 1000 } else { ts });

                                                        if let Some(tsv) = exchange_ts_ms {
                                                            last_exchange_ts_ms.store(tsv, Ordering::Relaxed);
                                                        }

                                                        let event_u = tr.get("u").and_then(|x| x.as_u64());
                                                        let sym = tr.get("s").and_then(|x| x.as_str()).unwrap_or("").to_uppercase();
                                                        let side = tr.get("m").and_then(|x| x.as_bool()).map(|is_maker| if is_maker { TradeSide::Sell } else { TradeSide::Buy }).unwrap_or(TradeSide::Unknown);

                                                        if px.is_finite() && sz > 0.0 {
                                                            let mut tp = TradePrint {
                                                                exchange: exchange.to_string(),
                                                                asset: sym.clone(),
                                                                kind: AssetKind::Spot,
                                                                px,
                                                                sz,
                                                                side,
                                                                exchange_trade_id,
                                                                exchange_ts_ms,
                                                                event_u,
                                                                writer_seq: None,
                                                                writer_ts_ms: None,
                                                                recv_ts_ms: recv_ts,
                                                                parse_ts_ms: Some(parse_ts),
                                                                enqueue_ts_ms: None,
                                                                persist_ts_ms: None,
                                                            };
                                                            tp.enqueue_ts_ms = Some(now_ms());
                                                            if let Err(e) = chunk_writer_tx.send(tp) {
                                                                warn!(exchange=%exchange, chunk=chunk_id, "chunk_writer_tx send failed: {:?}", e);
                                                            }
                                                        }
                                                    }
                                                } else {
                                                    // single object trade message case - attempt extraction (rare)
                                                    if let Some(sym) = data.get("s").and_then(|x| x.as_str()) {
                                                        let px = data.get("p").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(f64::NAN);
                                                        let sz = data.get("q").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                                        let exchange_trade_id = data.get("t").and_then(|x| x.as_u64());
                                                        let ts_opt = data.get("T").and_then(|x| x.as_i64()).or_else(|| data.get("E").and_then(|x| x.as_i64()));
                                                        let exchange_ts_ms = ts_opt.map(|ts| if ts < 1_000_000_000_000_i64 { ts * 1000 } else { ts });
                                                        let side = data.get("m").and_then(|x| x.as_bool()).map(|is_maker| if is_maker { TradeSide::Sell } else { TradeSide::Buy }).unwrap_or(TradeSide::Unknown);

                                                        if px.is_finite() && sz > 0.0 {
                                                            let mut tp = TradePrint {
                                                                exchange: exchange.to_string(),
                                                                asset: sym.to_uppercase(),
                                                                kind: AssetKind::Spot,
                                                                px,
                                                                sz,
                                                                side,
                                                                exchange_trade_id,
                                                                exchange_ts_ms,
                                                                event_u: data.get("u").and_then(|x| x.as_u64()),
                                                                writer_seq: None,
                                                                writer_ts_ms: None,
                                                                recv_ts_ms: recv_ts,
                                                                parse_ts_ms: Some(parse_ts),
                                                                enqueue_ts_ms: None,
                                                                persist_ts_ms: None,
                                                            };
                                                            tp.enqueue_ts_ms = Some(now_ms());
                                                            if let Err(e) = chunk_writer_tx.send(tp) {
                                                                warn!(exchange=%exchange, chunk=chunk_id, "chunk_writer_tx send failed: {:?}", e);
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        } else {
                                            debug!(exchange=%exchange, chunk=chunk_id,"trade text not JSON: {}", txt);
                                        }
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

                tokio::time::sleep(Duration::from_millis(250)).await;
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
