// src/writer.rs
use crate::metrics::{MetricsAggregator, MetricsRecord};
use crate::storage::mmap_ring::MmapRing;
use crate::types::{OrderBookSnapshot, TradePrint};
use serde_json;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;

pub async fn snapshot_writer_task(
    mut rx: UnboundedReceiver<OrderBookSnapshot>,
    ring: Option<Arc<MmapRing>>,
    metrics: Arc<MetricsAggregator>,
) {
    while let Some(mut snap) = rx.recv().await {
        let write_ts = crate::ingest::sources_binance::now_ms();
        snap.writer_ts_ms = Some(write_ts);
        if let Some(r) = &ring {
            if let Ok(payload) = serde_json::to_vec(&snap) {
                let _ = r.write_record(0u8, &payload);
                snap.persist_ts_ms = Some(crate::ingest::sources_binance::now_ms());
                metrics.record_snapshot(MetricsRecord::Snapshot {
                    ts_iso: chrono::Utc::now().to_rfc3339(),
                    exchange: snap.exchange.clone(),
                    asset: snap.asset.clone(),
                    recv: snap.recv_ts_ms,
                    parse: snap.parse_ts_ms,
                    enqueue: snap.enqueue_ts_ms,
                    writer: snap.writer_ts_ms,
                    persist: snap.persist_ts_ms,
                    bytes: payload.len(),
                });
            }
        } else {
            metrics.record_snapshot(MetricsRecord::Snapshot {
                ts_iso: chrono::Utc::now().to_rfc3339(),
                exchange: snap.exchange.clone(),
                asset: snap.asset.clone(),
                recv: snap.recv_ts_ms,
                parse: snap.parse_ts_ms,
                enqueue: snap.enqueue_ts_ms,
                writer: snap.writer_ts_ms,
                persist: snap.persist_ts_ms,
                bytes: 0,
            });
        }
    }
}

pub async fn trade_writer_task(
    mut rx: UnboundedReceiver<TradePrint>,
    ring: Option<Arc<MmapRing>>,
    metrics: Arc<MetricsAggregator>,
    dedupe_size: usize,
) {
    use lru::LruCache;
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let nz = NonZeroUsize::new(dedupe_size.max(1)).unwrap();
    let mut dedupe: LruCache<u64, ()> = LruCache::new(nz);
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

        if let Some(r) = &ring {
            if let Ok(payload) = serde_json::to_vec(&tp) {
                let _ = r.write_record(1u8, &payload);
                tp.persist_ts_ms = Some(crate::ingest::sources_binance::now_ms());
                metrics.record_trade(MetricsRecord::Trade {
                    ts_iso: chrono::Utc::now().to_rfc3339(),
                    exchange: tp.exchange.clone(),
                    asset: tp.asset.clone(),
                    recv: tp.recv_ts_ms,
                    parse: tp.parse_ts_ms,
                    enqueue: tp.enqueue_ts_ms,
                    writer: tp.writer_ts_ms,
                    persist: tp.persist_ts_ms,
                    bytes: payload.len(),
                });
            }
        } else {
            metrics.record_trade(MetricsRecord::Trade {
                ts_iso: chrono::Utc::now().to_rfc3339(),
                exchange: tp.exchange.clone(),
                asset: tp.asset.clone(),
                recv: tp.recv_ts_ms,
                parse: tp.parse_ts_ms,
                enqueue: tp.enqueue_ts_ms,
                writer: tp.writer_ts_ms,
                persist: tp.persist_ts_ms,
                bytes: 0,
            });
        }
    }
}
