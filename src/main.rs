// src/main.rs
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
    tracing_subscriber::fmt().init();
    // --- config
    let exchange = "BINANCE".to_string();
    let ws_url = "wss://stream.binance.com:9443".to_string();
    let bin_cfg = crate_level_binance_struct_from_parts(
        exchange.clone(),
        ws_url.clone(),
        vec!["ALL".to_string()],
        vec!["ALL".to_string()],
    );

    // central channels
    let (tx_snap, rx_snap) = mpsc::unbounded_channel::<OrderBookSnapshot>();
    let (tx_tr, rx_tr) = mpsc::unbounded_channel::<TradePrint>();

    // exchange status
    let (status_tx, _status_rx) = watch::channel(ExchangeStatus {
        exchange: exchange.clone(),
        kind: crate::types::ExchangeStatusKind::Disconnected,
        detail: None,
        ts_ms: crate::ingest::sources_binance::now_ms(),
    });

    // metrics aggregator (CSV writer)
    let metrics = Arc::new(MetricsAggregator::new("metrics.csv"));
    {
        // spawn metrics runner (clone Arc)
        let metrics_runner = metrics.clone();
        tokio::spawn(async move {
            metrics_runner.run().await;
        });
    }

    // Spawn snapshot writer task (central)
    let metrics_cl = metrics.clone();
    tokio::spawn(async move {
        snapshot_writer_task(rx_snap, None, metrics_cl).await;
        tracing::info!("central snapshot_writer_task exited");
    });

    // Spawn trade writer task (central)
    let metrics_cl2 = metrics.clone();
    tokio::spawn(async move {
        trade_writer_task(rx_tr, None, metrics_cl2, 200_000).await;
        tracing::info!("central trade_writer_task exited");
    });

    // spawn binance connectors which create per-chunk rings
    spawn_binance_from_config(
        &bin_cfg,
        tx_snap.clone(),
        tx_tr.clone(),
        status_tx.clone(),
        metrics.clone(),
    )
    .await?;

    // keep main alive
    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}
