// src/writer.rs
use crate::metrics::MetricsAggregator;
use crate::storage::mmap_ring::MmapRing;
use crate::types::{OrderBookSnapshot, TradePrint};
use serde_json;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;

/// snapshot writer task
/// - rx: receives central snapshots
/// - ring: optional global ring (central); None if you don't use a global ring
/// - metrics: metrics aggregator
pub async fn snapshot_writer_task(
    mut rx: UnboundedReceiver<OrderBookSnapshot>,
    ring: Option<Arc<MmapRing>>,
    metrics: Arc<MetricsAggregator>,
) {
    while let Some(mut snap) = rx.recv().await {
        // writer timestamp
        let write_ts = crate::ingest::sources_binance::now_ms();
        snap.writer_ts_ms = Some(write_ts);

        if let Some(r) = &ring {
            if let Ok(payload) = serde_json::to_vec(&snap) {
                let _ = r.write_record(0u8, &payload);
                // record metrics
                let recv_ts = snap.recv_ts_ms as u64;
                if let Some(ex_ts) = snap.exchange_ts_ms.map(|v| v as u64) {
                    let hop_ex_recv = recv_ts.saturating_sub(ex_ts);
                    let hop_recv_write = (write_ts as u64).saturating_sub(recv_ts);
                    let total = (write_ts as u64).saturating_sub(ex_ts);
                    metrics.record_snapshot_latency(
                        hop_ex_recv,
                        hop_recv_write,
                        total,
                        payload.len() as u64,
                    );
                } else {
                    let hop_recv_write = (write_ts as u64).saturating_sub(recv_ts);
                    metrics.record_snapshot_latency(
                        0,
                        hop_recv_write,
                        hop_recv_write,
                        payload.len() as u64,
                    );
                }
            }
        } else {
            // no ring: still record metrics with zeros except hop recv->write
            metrics.record_snapshot_latency(0, 0, 0, snap.asset.len() as u64);
        }
    }
}

/// trade writer task
/// - dedupe_size: LRU dedupe window
/// - ring: optional per-central ring
pub async fn trade_writer_task(
    mut rx: UnboundedReceiver<TradePrint>,
    ring: Option<Arc<MmapRing>>,
    metrics: Arc<MetricsAggregator>,
    dedupe_size: usize,
) {
    use lru::LruCache;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use std::num::NonZeroUsize;

    let cap = NonZeroUsize::new(dedupe_size.max(1)).unwrap();
    let mut dedupe: LruCache<u64, ()> = LruCache::new(cap);
    while let Some(mut tp) = rx.recv().await {
        let mut h = DefaultHasher::new();
        tp.asset.hash(&mut h);
        if let Some(ev) = tp.event_u {
            ev.hash(&mut h);
        } else {
            (tp.recv_ts_ms as i64).hash(&mut h);
            ((tp.px * 1e6) as i64).hash(&mut h);
            ((tp.sz * 1e6) as i64).hash(&mut h);
        }
        let key = h.finish();
        if dedupe.contains(&key) {
            continue;
        }
        dedupe.put(key, ());
        let write_ts = crate::ingest::sources_binance::now_ms();
        tp.writer_ts_ms = Some(write_ts);

        let recv_ts = tp.recv_ts_ms as u64;
        if let Some(ex_ts_i) = tp.exchange_ts_ms {
            let ex_ts = ex_ts_i as u64;
            let hop_ex_recv = recv_ts.saturating_sub(ex_ts);
            let hop_recv_write = (write_ts as u64).saturating_sub(recv_ts);
            let total = (write_ts as u64).saturating_sub(ex_ts);
            if let Some(r) = &ring {
                if let Ok(payload) = serde_json::to_vec(&tp) {
                    let _ = r.write_record(1u8, &payload);
                    metrics.record_trade_latency(
                        hop_ex_recv,
                        hop_recv_write,
                        total,
                        payload.len() as u64,
                    );
                }
            } else {
                metrics.record_trade_latency(
                    hop_ex_recv,
                    hop_recv_write,
                    total,
                    tp.asset.len() as u64,
                );
            }
        } else {
            let hop_recv_write = (write_ts as u64).saturating_sub(recv_ts);
            if let Some(r) = &ring {
                if let Ok(payload) = serde_json::to_vec(&tp) {
                    let _ = r.write_record(1u8, &payload);
                    metrics.record_trade_latency(
                        0,
                        hop_recv_write,
                        hop_recv_write,
                        payload.len() as u64,
                    );
                }
            } else {
                metrics.record_trade_latency(
                    0,
                    hop_recv_write,
                    hop_recv_write,
                    tp.asset.len() as u64,
                );
            }
        }
    }
}
