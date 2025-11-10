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

    tracing::info!("Starting bin-deribit (trade-only)");

    // metrics aggregator
    let metrics = Arc::new(MetricsAggregator::new("metrics.csv"));
    let metrics_runner = metrics.clone();
    tokio::spawn(async move {
        metrics_runner.run().await;
    });

    // exchange configuration (Deribit)
    let exchange = "DERIBIT".to_string();
    let rest_base = "https://www.deribit.com".to_string(); // or testnet: https://test.deribit.com
    let ws_url = "wss://www.deribit.com/ws/api/v2".to_string(); // ws json-rpc v2

    // exchange status channel
    let (status_tx, _status_rx) = watch::channel(ExchangeStatus {
        exchange: exchange.clone(),
        kind: ExchangeStatusKind::Disconnected,
        detail: None,
        ts_ms: crate::ingest::sources_deribit::now_ms(),
    });

    // build sources config — note: 4 args: name, rest_base, ws_url, currencies
    let deribit_cfg = crate::ingest::sources_deribit::crate_level_deribit_struct_from_parts(
        exchange.clone(),
        rest_base.clone(),
        ws_url.clone(),
        vec!["ALL".to_string()],
    );

    // spawn trade ingestion
    crate::ingest::sources_deribit::spawn_deribit_from_config(
        &deribit_cfg,
        status_tx.clone(),
        metrics.clone(),
    )
    .await?;

    // keep running
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}
