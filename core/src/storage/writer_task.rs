// core/src/storage/writer_task.rs

use crate::metrics::MetricsAggregator;
use crate::storage::mmap_ring::MmapRing;
use crate::types::TradePrint;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;

/// Simple async task that consumes trade prints from `rx` and writes them into `ring`.
/// This is a minimal helper; prefer `crate::writer::spawn_trade_chunk_writer` which opens
/// the ring and spawns the task for you.
pub async fn trade_writer_task(
    mut rx: UnboundedReceiver<TradePrint>,
    ring: Arc<MmapRing>,
    metrics: Arc<MetricsAggregator>,
    prefix: String,
    periodic_log_every: u64,
) {
    let mut local_count: u64 = 0;
    while let Some(mut tp) = rx.recv().await {
        local_count = local_count.saturating_add(1);
        tp.writer_ts_ms = Some(chrono::Utc::now().timestamp_millis());

        match serde_json::to_vec(&tp) {
            Ok(payload) => match ring.write_record(1u8, &payload) {
                Ok(seq) => {
                    tp.persist_ts_ms = Some(chrono::Utc::now().timestamp_millis());
                    tp.writer_seq = Some(seq);

                    metrics.record_trade(crate::metrics::MetricsRecord::Trade {
                        ts_iso: chrono::Utc::now().to_rfc3339(),
                        exchange: tp.exchange.clone(),
                        asset: tp.asset.clone(),
                        recv: tp.recv_ts_ms,
                        parse: tp.parse_ts_ms,
                        enqueue: tp.enqueue_ts_ms,
                        writer: tp.writer_ts_ms,
                        persist: tp.persist_ts_ms,
                        bytes: payload.len(),
                        writer_seq: tp.writer_seq,
                    });

                    if periodic_log_every > 0 && (local_count % periodic_log_every == 0) {
                        tracing::info!(
                            "trade chunk writer '{}' wrote {} messages, last_seq={}",
                            prefix,
                            local_count,
                            seq
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "trade chunk writer '{}' failed write (msg #{}): {:?}",
                        prefix,
                        local_count,
                        e
                    );
                }
            },
            Err(e) => {
                tracing::warn!(
                    "trade chunk writer '{}' failed to serialize (msg #{}): {:?}",
                    prefix,
                    local_count,
                    e
                );
            }
        }
    }
    tracing::info!(
        "trade chunk writer '{}' exiting after {} messages",
        prefix,
        local_count
    );
}
