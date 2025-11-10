mod ingest;

use anyhow::Result;
use core::metrics::MetricsAggregator;
use core::types::{ExchangeStatus, ExchangeStatusKind};
use std::sync::Arc;
use tokio::sync::watch;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    // initialize tracing subscriber so tracing::info!/warn! prints to stdout
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    tracing::info!("Starting bin-binance (trade-only)");

    // metrics aggregator
    let metrics = Arc::new(MetricsAggregator::new("metrics.csv"));
    let metrics_runner = metrics.clone();
    tokio::spawn(async move {
        metrics_runner.run().await;
    });

    // exchange configuration
    let exchange = "BINANCE".to_string();
    let ws_url = "wss://stream.binance.com:9443".to_string();

    // exchange status channel
    let (status_tx, _status_rx) = watch::channel(ExchangeStatus {
        exchange: exchange.clone(),
        kind: ExchangeStatusKind::Disconnected,
        detail: None,
        ts_ms: crate::ingest::sources_binance::now_ms(),
    });

    // build sources config
    let bin_cfg = crate::ingest::sources_binance::crate_level_binance_struct_from_parts(
        exchange.clone(),
        ws_url.clone(),
        vec!["ALL".to_string()],
    );

    // spawn trade ingestion
    crate::ingest::sources_binance::spawn_binance_from_config(
        &bin_cfg,
        status_tx.clone(),
        metrics.clone(),
    )
    .await?;

    // keep running
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}
