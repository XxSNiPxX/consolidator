mod ingest;
mod metrics;
mod storage;
mod types;
mod writer;

use crate::ingest::sources_binance::{
    crate_level_binance_struct_from_parts, spawn_binance_from_config,
};
use crate::metrics::MetricsAggregator;
use crate::storage::mmap_ring::{MmapRing, MmapRingConfig};
use crate::types::{ExchangeStatus, OrderBookSnapshot, TradePrint};
use crate::writer::{snapshot_writer_task, trade_writer_task};
use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, watch};

use tracing_subscriber::filter::EnvFilter;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    // --- config
    let exchange = "BINANCE".to_string();
    let ws_url = "wss://stream.binance.com:9443".to_string(); // base
    let bin_cfg = crate_level_binance_struct_from_parts(
        exchange.clone(),
        ws_url.clone(),
        vec!["ALL".to_string()],
        vec!["ALL".to_string()],
    );

    // central channels for snapshots and trades
    let (tx_snap, rx_snap) = mpsc::unbounded_channel::<OrderBookSnapshot>();
    let (tx_tr, rx_tr) = mpsc::unbounded_channel::<TradePrint>();

    // exchange status
    let (status_tx, _status_rx) = watch::channel(ExchangeStatus {
        exchange: exchange.clone(),
        kind: crate::types::ExchangeStatusKind::Disconnected,
        detail: None,
        ts_ms: now_ms(),
    });

    // metrics aggregator (CSV writer)
    let metrics = Arc::new(MetricsAggregator::new("metrics.csv"));
    let metrics_clone = metrics.clone();
    tokio::spawn(async move {
        metrics_clone.run().await;
    });

    // --- configure global rings only if you actually need them.
    // The per-chunk rings are created in spawn_binance_from_config and will compute meta_size correctly.
    // If you need global rings here, compute meta_size with compute_meta_size(index_slots).

    // Spawn snapshot writer task (central)
    // Spawn snapshot writer task (central)
    // Spawn snapshot writer task (central) — no global ring
    let metrics_cl = metrics.clone();
    tokio::spawn(async move {
        snapshot_writer_task(rx_snap, None, metrics_cl).await;
        tracing::info!("central snapshot_writer_task exited");
    });

    // Spawn trade writer task (central) — no global ring, provide dedupe size
    let metrics_cl2 = metrics.clone();
    let dedupe_size = bin_cfg.chunk_dedupe_size;
    tokio::spawn(async move {
        trade_writer_task(rx_tr, None, metrics_cl2, dedupe_size).await;
        tracing::info!("central trade_writer_task exited");
    });

    // Now spawn the binance connectors which will create per-chunk rings and writer tasks
    spawn_binance_from_config(
        &bin_cfg,
        tx_snap.clone(),
        tx_tr.clone(),
        status_tx.clone(),
        metrics.clone(),
    )
    .await?;

    // keep main alive (connectors run in background)
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
