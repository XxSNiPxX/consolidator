//! Deribit trade-only ingestion (chunked websocket readers -> per-chunk mmap ring writers).
//! Uses public REST + websocket JSON-RPC v2.
//! See Deribit docs: https://docs.deribit.com (ws base: wss://www.deribit.com/ws/api/v2)
//!
//! This version authenticates (public/auth client_credentials) before subscribing to
//! raw channels (trades.*.raw) because raw subscriptions require authorization.

use anyhow::{anyhow, Result};
use core::metrics::MetricsAggregator;
use core::types::{AssetKind, ExchangeStatus, ExchangeStatusKind, TradePrint, TradeSide};
use core::writer::{spawn_trade_chunk_writer, TradeChunkWriterConfig};
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde_json::Value as SerdeValue;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc::UnboundedReceiver, mpsc::UnboundedSender, watch};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};

#[derive(Clone, Debug)]
pub struct DeribitSources {
    pub name: String,
    pub rest_base: String, // https://www.deribit.com or https://test.deribit.com
    pub ws_url: String,    // wss://www.deribit.com/ws/api/v2
    pub currencies: Vec<String>, // e.g. vec!["BTC", "ETH"] or ["ALL"]
    pub kind: String,      // kind filter: "future", "option", "any"
    pub chunk_size: usize,
    pub chunk_data_capacity: usize,
    pub chunk_index_slots: usize,
    pub chunk_sync_writes: bool,
    pub chunk_dedupe_size: usize,
}

pub fn crate_level_deribit_struct_from_parts(
    name: String,
    rest_base: String,
    ws_url: String,
    currencies: Vec<String>,
) -> DeribitSources {
    DeribitSources {
        name,
        rest_base,
        ws_url,
        currencies,
        kind: "any".to_string(),
        chunk_size: 400,
        chunk_data_capacity: 512 * 1024 * 1024,
        chunk_index_slots: 32_768,
        chunk_sync_writes: false,
        chunk_dedupe_size: 200_000,
    }
}

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Fetch instruments for a given currency via RESTv2.
async fn fetch_instruments_for_currency(
    client: &Client,
    rest_base: &str,
    currency: &str,
    kind: &str,
) -> Result<Vec<String>> {
    let url = format!(
        "{}/api/v2/public/get_instruments",
        rest_base.trim_end_matches('/')
    );
    let resp = client
        .get(&url)
        .query(&[("currency", currency), ("kind", kind), ("expired", "false")])
        .send()
        .await?
        .error_for_status()?;
    let v: SerdeValue = resp.json().await?;
    let mut out = Vec::new();
    if let Some(arr) = v.get("result").and_then(|r| r.as_array()) {
        for item in arr {
            if let Some(instr) = item.get("instrumentName").and_then(|s| s.as_str()) {
                out.push(instr.to_string());
            } else if let Some(instr) = item.get("instrument_name").and_then(|s| s.as_str()) {
                out.push(instr.to_string());
            }
        }
    }
    out.sort();
    out.dedup();
    Ok(out)
}

fn expanded_currencies(cfg: &DeribitSources) -> Vec<String> {
    if cfg.currencies.len() == 1 && cfg.currencies[0].to_uppercase() == "ALL" {
        vec![
            "BTC".into(),
            "ETH".into(),
            "SOL".into(),
            "USDC".into(),
            "USDT".into(),
        ]
    } else {
        cfg.currencies.clone()
    }
}

fn chunks<T: Clone>(vec: &[T], chunk_size: usize) -> Vec<Vec<T>> {
    if chunk_size == 0 {
        return vec![vec![]];
    }
    vec.chunks(chunk_size).map(|c| c.to_vec()).collect()
}

/// Authenticate on the websocket by sending public/auth (client credentials).
/// We send the login request (id == auth_id) and then read until a response with the same id
/// appears (or timeout). On success returns true.
async fn ws_authenticate(
    write: &mut futures_util::stream::SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    read: &mut futures_util::stream::SplitStream<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    >,
    client_id: &str,
    client_secret: &str,
    timeout_ms: u64,
) -> Result<()> {
    // id used to match response
    let auth_id = 123456_i64;
    let login = serde_json::json!({
        "jsonrpc": "2.0",
        "id": auth_id,
        "method": "public/auth",
        "params": {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret
        }
    });

    write.send(Message::Text(login.to_string())).await?;
    info!("sent public/auth request (id={})", auth_id);

    // wait for response with same id or timeout
    let mut elapsed = 0u64;
    let step = 50u64;
    loop {
        // attempt to read a message non-blocking with a small timeout
        let recv = tokio::time::timeout(Duration::from_millis(step), read.next()).await;
        match recv {
            Ok(Some(Ok(msg))) => {
                if let Message::Text(txt) = msg {
                    debug!("ws_authenticate got text: {}", txt);
                    if let Ok(v) = serde_json::from_str::<SerdeValue>(&txt) {
                        if let Some(idv) = v.get("id") {
                            if idv.as_i64() == Some(auth_id) {
                                if v.get("error").is_some() {
                                    return Err(anyhow!("auth error: {:?}", v.get("error")));
                                }
                                if let Some(res) = v.get("result") {
                                    if let Some(token) =
                                        res.get("access_token").and_then(|t| t.as_str())
                                    {
                                        info!(
                                            "ws auth success; access_token present (len={})",
                                            token.len()
                                        );
                                        // success - return
                                        return Ok(());
                                    } else {
                                        warn!("ws auth response missing access_token: {:?}", res);
                                        return Err(anyhow!("no access_token in auth response"));
                                    }
                                }
                            } else {
                                // other replies: log debug and continue
                                debug!("ws_authenticate: unrelated id reply: {}", idv);
                            }
                        } else {
                            // notifications etc - ignore
                            debug!("ws_authenticate: notification/other before auth: {}", txt);
                        }
                    } else {
                        debug!("ws_authenticate: non-json reply: {}", txt);
                    }
                } else if let Message::Binary(b) = msg {
                    debug!("ws_authenticate: binary msg len={}", b.len());
                } else if msg.is_close() {
                    return Err(anyhow!("ws closed during auth"));
                }
            }
            Ok(Some(Err(e))) => {
                return Err(anyhow!("ws read error during auth: {:?}", e));
            }
            Ok(None) => {
                return Err(anyhow!("ws stream ended during auth"));
            }
            Err(_) => {
                // timeout step elapsed - increment elapsed and loop
                elapsed += step;
                if elapsed >= timeout_ms {
                    return Err(anyhow!("auth timeout after {} ms", timeout_ms));
                }
                continue;
            }
        }
    }
}

/// Main spawn: create per-chunk rings, writer tasks, and websocket reader per chunk.
/// This mirrors the spawn_binance_from_config pattern.
pub async fn spawn_deribit_from_config(
    dcfg: &DeribitSources,
    status_tx: watch::Sender<ExchangeStatus>,
    metrics: Arc<MetricsAggregator>,
) -> Result<()> {
    let client = Client::new();

    // resolve instruments across currencies
    let mut all_instruments: Vec<String> = Vec::new();
    let currencies = expanded_currencies(dcfg);
    for cur in currencies {
        match fetch_instruments_for_currency(&client, &dcfg.rest_base, &cur, &dcfg.kind).await {
            Ok(mut v) => {
                info!(exchange=%dcfg.name, currency=%cur, found=%v.len(), "fetched instruments");
                all_instruments.append(&mut v);
            }
            Err(e) => {
                warn!(exchange=%dcfg.name, currency=%cur, "get_instruments failed: {:?}", e);
            }
        }
    }

    if all_instruments.is_empty() {
        warn!(exchange=%dcfg.name, "no instruments resolved; aborting spawn");
        return Ok(());
    }

    // chunk them
    let trade_chunks = chunks(&all_instruments, dcfg.chunk_size);

    // NOTE: supply client_id/client_secret here via env or config
    // TODO: move these to config instead of hardcoding
    let client_id = std::env::var("DERIBIT_CLIENT_ID").unwrap_or_else(|_| {
        // fallback placeholder - replace with your real ID or set env DERIBIT_CLIENT_ID
        "".to_string()
    });
    let client_secret = std::env::var("DERIBIT_CLIENT_SECRET").unwrap_or_else(|_| "".to_string());

    for (i, chunk_symbols) in trade_chunks.into_iter().enumerate() {
        if chunk_symbols.is_empty() {
            continue;
        }

        let ex = dcfg.name.clone();

        // IMPORTANT: create rings under connectors/bin-deribit/src/ingest/rings/
        let ring_prefix = format!(
            "connectors/bin-deribit/src/ingest/rings/{}_trade_chunk{}",
            ex.to_lowercase(),
            i
        );
        let data_path = PathBuf::from(format!("{}.data", ring_prefix));

        // ensure directory exists
        let _ = std::fs::create_dir_all(
            data_path
                .parent()
                .unwrap_or_else(|| std::path::Path::new(".")),
        );

        // preallocate .data
        let _ = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&data_path)
            .and_then(|f| f.set_len(dcfg.chunk_data_capacity as u64));

        // meta size from MmapRing helper
        let meta_size =
            core::storage::mmap_ring::MmapRing::compute_meta_size(dcfg.chunk_index_slots)?;
        let meta_path = PathBuf::from(format!("{}.meta", ring_prefix));
        let _ = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&meta_path)
            .and_then(|f| f.set_len(meta_size as u64));

        let writer_cfg = TradeChunkWriterConfig {
            path_prefix: ring_prefix.clone(),
            data_capacity: dcfg.chunk_data_capacity,
            index_slots: dcfg.chunk_index_slots,
            meta_size,
            create: true,
            force_reinit: false,
            sync_writes: dcfg.chunk_sync_writes,
            flusher_interval_ms: 50,
            periodic_log_every: 10_000,
            dedupe_size: dcfg.chunk_dedupe_size,
        };

        let (chunk_tx, chunk_rx): (UnboundedSender<TradePrint>, UnboundedReceiver<TradePrint>) =
            tokio::sync::mpsc::unbounded_channel();

        let metrics_clone = metrics.clone();
        let _ring = spawn_trade_chunk_writer(writer_cfg.clone(), chunk_rx, metrics_clone).await?;
        info!(exchange=%ex, chunk=i, "opened ring for chunk: {}", ring_prefix);

        // health counters
        let recv_msgs = Arc::new(AtomicU64::new(0));
        let recv_bytes = Arc::new(AtomicU64::new(0));
        let reconnects = Arc::new(AtomicU64::new(0));
        let last_exchange_ts_ms = Arc::new(AtomicI64::new(0));

        // capture vars for task
        let ex_for_task = ex.clone();
        let url_clone = dcfg.ws_url.clone();
        let chunk_tx_clone = chunk_tx.clone();
        let status_tx_clone = status_tx.clone();
        let symbols_clone = chunk_symbols.clone();
        let client_id_clone = client_id.clone();
        let client_secret_clone = client_secret.clone();

        let recv_msgs_c = recv_msgs.clone();
        let recv_bytes_c = recv_bytes.clone();
        let reconnects_c = reconnects.clone();
        let last_exchange_ts_c = last_exchange_ts_ms.clone();

        tokio::spawn(async move {
            if let Err(e) = run_deribit_trade_stream_chunk(
                &ex_for_task,
                &url_clone,
                i,
                symbols_clone,
                chunk_tx_clone,
                status_tx_clone,
                recv_msgs_c,
                recv_bytes_c,
                reconnects_c,
                last_exchange_ts_c,
                &client_id_clone,
                &client_secret_clone,
            )
            .await
            {
                warn!(exchange=%ex_for_task, chunk=i, "deribit chunk task error: {:?}", e);
            }
        });

        info!(exchange=%ex, chunk=i, "spawned deribit trade chunk writer and reader; ring={}", ring_prefix);
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_deribit_trade_stream_chunk(
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
    client_id: &str,
    client_secret: &str,
) -> Result<()> {
    use serde_json::json;

    // build channels array for instruments e.g. trades.BTC-PERPETUAL.raw
    let channels: Vec<String> = symbols
        .iter()
        .map(|s| format!("trades.{}.raw", s))
        .collect();

    let base_ms = 250u64;
    let cap_ms = 30_000u64;
    let mut attempt: u32 = 0;

    loop {
        info!(exchange=%exchange, chunk=chunk_id, "connecting deribit ws: {}", ws_url);
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

        match connect_async(ws_url).await {
            Ok((ws_stream, _resp)) => {
                info!(exchange=%exchange, chunk=chunk_id, "deribit ws connected");
                let _ = status_tx.send(ExchangeStatus {
                    exchange: exchange.to_string(),
                    kind: ExchangeStatusKind::Connected,
                    detail: Some(format!("chunk {}", chunk_id)),
                    ts_ms: now_ms(),
                });

                attempt = 0;

                let (mut write, mut read) = ws_stream.split();

                // 1) Authenticate with client credentials (necessary for raw feeds).
                match ws_authenticate(&mut write, &mut read, client_id, client_secret, 5000).await {
                    Ok(_) => {
                        info!(exchange=%exchange, chunk=chunk_id, "ws auth ok, proceeding to subscribe raw channels");
                    }
                    Err(e) => {
                        warn!(exchange=%exchange, chunk=chunk_id, "ws auth failed: {:?}", e);
                        // server will reject raw subs if unauthenticated; backoff and retry
                        let backoff = backoff_ms_with_jitter(base_ms, attempt, cap_ms);
                        attempt = attempt.saturating_add(1);
                        reconnects.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(backoff)).await;
                        continue;
                    }
                }

                // 2) Send subscribe request (bulk)
                let req = json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "public/subscribe",
                    "params": {
                        "channels": channels
                    }
                });
                if let Err(e) = write.send(Message::Text(req.to_string())).await {
                    warn!(exchange=%exchange, chunk=chunk_id, "subscribe send failed: {:?}", e);
                } else {
                    info!(exchange=%exchange, chunk=chunk_id, "sent public/subscribe ({} channels)", channels.len());
                }

                let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
                // small sample counter for raw message dumps
                let mut sample_counter: usize = 0;

                loop {
                    tokio::select! {
                        _ = ping_interval.tick() => {
                            // keepalive
                            let ping = json!({"jsonrpc":"2.0","id": 9999, "method":"public/ping"});
                            if let Err(e) = write.send(Message::Text(ping.to_string())).await {
                                warn!(exchange=%exchange, chunk=chunk_id,"ping failed: {:?}", e);
                                break;
                            }
                        }
                        msg_opt = read.next() => {
                            let Some(msg) = msg_opt else {
                                warn!(exchange=%exchange, chunk=chunk_id,"deribit stream ended, reconnecting");
                                break;
                            };
                            match msg {
                                Ok(Message::Text(txt)) => {
                                    let recv_ts = now_ms();
                                    recv_msgs.fetch_add(1, Ordering::Relaxed);
                                    recv_bytes.fetch_add(txt.len() as u64, Ordering::Relaxed);

                                    // sample a couple of raw messages to debug authentication/subscription
                                    sample_counter = sample_counter.saturating_add(1);
                                    if sample_counter <= 4 {
                                        debug!(exchange=%exchange, chunk=chunk_id, sample_idx=sample_counter, raw=txt.len(), "raw deribit msg sample: {}", txt);
                                    }

                                    // try parse json
                                    if let Ok(v) = serde_json::from_str::<SerdeValue>(&txt) {
                                        // if the message is a subscription notification -> "params" usually contains "data"
                                        if let Some(obj) = v.as_object() {
                                            if let Some(params) = obj.get("params") {
                                                let data_opt = params.get("data").cloned().or_else(|| params.get("result").cloned());
                                                if let Some(data) = data_opt {
                                                    // handle arrays of trade objects
                                                    if let Some(arr) = data.as_array() {
                                                        for tr in arr {
                                                            debug!(exchange=%exchange, chunk=chunk_id, "raw_trade: {}", tr);

                                                            // defensive extraction
                                                            let px = tr.get("price")
                                                                .and_then(|x| x.as_f64())
                                                                .or_else(|| tr.get("trade_price").and_then(|x| x.as_f64()))
                                                                .unwrap_or(f64::NAN);

                                                            let sz = tr.get("amount")
                                                                .and_then(|x| x.as_f64())
                                                                .or_else(|| tr.get("size").and_then(|x| x.as_f64()))
                                                                .unwrap_or(0.0);

                                                            let exchange_trade_id = tr.get("trade_seq")
                                                                .and_then(|x| x.as_u64())
                                                                .or_else(|| tr.get("trade_id").and_then(|x| x.as_u64()));

                                                            let ts_opt = tr.get("timestamp")
                                                                .and_then(|x| x.as_i64())
                                                                .or_else(|| tr.get("time").and_then(|x| x.as_i64()));

                                                            let exchange_ts_ms = ts_opt.map(|ts| if ts < 1_000_000_000_000_i64 { ts * 1000 } else { ts });
                                                            if let Some(tsv) = exchange_ts_ms { last_exchange_ts_ms.store(tsv, Ordering::Relaxed); }

                                                            let instr = tr.get("instrument_name")
                                                                .and_then(|x| x.as_str())
                                                                .unwrap_or("")
                                                                .to_string();

                                                            let side = tr.get("direction")
                                                                .and_then(|x| x.as_str())
                                                                .map(|s| if s.eq_ignore_ascii_case("buy") { TradeSide::Buy } else { TradeSide::Sell })
                                                                .or_else(|| tr.get("label").and_then(|x| x.as_str()).map(|s| if s.eq_ignore_ascii_case("buy") { TradeSide::Buy } else { TradeSide::Sell }))
                                                                .unwrap_or(TradeSide::Unknown);

                                                            if !px.is_finite() {
                                                                debug!(exchange=%exchange, chunk=chunk_id, "dropping trade: px invalid for instr={} data={}", instr, tr);
                                                                continue;
                                                            }
                                                            if sz <= 0.0 {
                                                                debug!(exchange=%exchange, chunk=chunk_id, "dropping trade: size <=0 for instr={} data={}", instr, tr);
                                                                continue;
                                                            }

                                                            let mut tp = TradePrint {
                                                                exchange: exchange.to_string(),
                                                                asset: instr.clone(),
                                                                // keep Spot for now; change to derivatives kind if present in core::types
                                                                kind: AssetKind::Spot,
                                                                px,
                                                                sz,
                                                                side,
                                                                exchange_trade_id,
                                                                exchange_ts_ms,
                                                                event_u: None,
                                                                writer_seq: None,
                                                                writer_ts_ms: None,
                                                                recv_ts_ms: recv_ts,
                                                                parse_ts_ms: Some(now_ms()),
                                                                enqueue_ts_ms: None,
                                                                persist_ts_ms: None,
                                                            };
                                                            tp.enqueue_ts_ms = Some(now_ms());

                                                            debug!(exchange=%exchange, chunk=chunk_id, instr=%instr, px=%tp.px, sz=%tp.sz, "sending TradePrint to chunk writer");

                                                            match chunk_writer_tx.send(tp) {
                                                                Ok(_) => {
                                                                    // record successful enqueue
                                                                    recv_msgs.fetch_add(1, Ordering::Relaxed);
                                                                    debug!(exchange=%exchange, chunk=chunk_id, "enqueued TradePrint ok for instr={}", instr);
                                                                }
                                                                Err(e) => {
                                                                    warn!(exchange=%exchange, chunk=chunk_id, "chunk_writer_tx send failed: {:?}", e);
                                                                }
                                                            }
                                                        }
                                                    } else if data.is_object() {
                                                        // sometimes data contains an object with "trades" array
                                                        if let Some(arr) = data.get("trades").and_then(|a| a.as_array()) {
                                                            for tr in arr {
                                                                debug!(exchange=%exchange, chunk=chunk_id, "raw_trade(container): {}", tr);

                                                                let px = tr.get("price")
                                                                    .and_then(|x| x.as_f64())
                                                                    .or_else(|| tr.get("trade_price").and_then(|x| x.as_f64()))
                                                                    .unwrap_or(f64::NAN);
                                                                let sz = tr.get("amount")
                                                                    .and_then(|x| x.as_f64())
                                                                    .or_else(|| tr.get("size").and_then(|x| x.as_f64()))
                                                                    .unwrap_or(0.0);

                                                                if !px.is_finite() || sz <= 0.0 {
                                                                    debug!(exchange=%exchange, chunk=chunk_id, "dropping trade (container) px={} sz={}", px, sz);
                                                                    continue;
                                                                }

                                                                let exchange_trade_id = tr.get("trade_seq")
                                                                    .and_then(|x| x.as_u64())
                                                                    .or_else(|| tr.get("trade_id").and_then(|x| x.as_u64()));

                                                                let instr = tr.get("instrument_name")
                                                                    .and_then(|x| x.as_str())
                                                                    .unwrap_or("")
                                                                    .to_string();

                                                                let ts_opt = tr.get("timestamp")
                                                                    .and_then(|x| x.as_i64())
                                                                    .or_else(|| tr.get("time").and_then(|x| x.as_i64()));
                                                                let exchange_ts_ms = ts_opt.map(|ts| if ts < 1_000_000_000_000_i64 { ts * 1000 } else { ts });
                                                                if let Some(tsv) = exchange_ts_ms { last_exchange_ts_ms.store(tsv, Ordering::Relaxed); }

                                                                let mut tp = TradePrint {
                                                                    exchange: exchange.to_string(),
                                                                    asset: instr.clone(),
                                                                    kind: AssetKind::Spot,
                                                                    px,
                                                                    sz,
                                                                    side: TradeSide::Unknown,
                                                                    exchange_trade_id,
                                                                    exchange_ts_ms,
                                                                    event_u: None,
                                                                    writer_seq: None,
                                                                    writer_ts_ms: None,
                                                                    recv_ts_ms: recv_ts,
                                                                    parse_ts_ms: Some(now_ms()),
                                                                    enqueue_ts_ms: None,
                                                                    persist_ts_ms: None,
                                                                };
                                                                tp.enqueue_ts_ms = Some(now_ms());
                                                                if let Err(e) = chunk_writer_tx.send(tp) {
                                                                    warn!(exchange=%exchange, chunk=chunk_id, "chunk_writer_tx send failed: {:?}", e);
                                                                } else {
                                                                    recv_msgs.fetch_add(1, Ordering::Relaxed);
                                                                }
                                                            }
                                                        }
                                                    }
                                                } // data_opt present
                                            } // params present
                                        } // is object
                                    } else {
                                        debug!(exchange=%exchange, chunk=chunk_id, "text not json: {}", txt);
                                    }
                                } // Message::Text
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
                    } // select
                } // inner read loop

                tokio::time::sleep(Duration::from_millis(250)).await;
                continue;
            } // connected
            Err(e) => {
                warn!(exchange=%exchange, chunk=chunk_id, "failed to connect deribit ws: {:?}; will backoff", e);
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
        } // match connect
    } // reconnect loop
}

fn backoff_ms_with_jitter(base_ms: u64, attempt: u32, cap_ms: u64) -> u64 {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let exp_pow = std::cmp::min(attempt, 63);
    let multiplier = 1u64.checked_shl(exp_pow).unwrap_or(u64::MAX);
    let mut exp = base_ms.saturating_mul(multiplier);
    if exp > cap_ms {
        exp = cap_ms;
    }
    rng.gen_range(0..=exp)
}
