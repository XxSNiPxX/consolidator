use crate::metrics::MetricsAggregator;
use crate::types::{OrderBookSnapshot, TradePrint};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;

/// Central snapshot writer task: no ring here, just metrics or forwarding.
pub async fn snapshot_writer_task(
    mut rx: UnboundedReceiver<OrderBookSnapshot>,
    metrics: Arc<MetricsAggregator>,
) {
    while let Some(snap) = rx.recv().await {
        // do light work / metrics / forwarding — keep cheap to avoid blocking ingestion
        metrics.record_snapshot_latency(0, 0, 0, snap.asset.len() as u64);
    }
}

pub async fn trade_writer_task(
    mut rx: UnboundedReceiver<TradePrint>,
    metrics: Arc<MetricsAggregator>,
) {
    while let Some(tp) = rx.recv().await {
        metrics.record_trade_latency(0, 0, 0, tp.asset.len() as u64);
    }
}
