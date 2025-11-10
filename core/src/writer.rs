// core/src/writer.rs

use crate::metrics::MetricsAggregator;
use crate::storage::mmap_ring::{MmapRing, MmapRingConfig};
use crate::types::TradePrint;
use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;

use lru::LruCache;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;

/// Minimal trade-chunk writer configuration.
///
/// - `dedupe_size` controls the LRU dedupe window (number of keys to remember).
///   If 0 => deduping disabled.
#[derive(Clone, Debug)]
pub struct TradeChunkWriterConfig {
    /// prefix for ring files, e.g. "rings/binance_trade_chunk0"
    pub path_prefix: String,
    /// data capacity in bytes for the .data file
    pub data_capacity: usize,
    /// number of index slots for meta
    pub index_slots: usize,
    /// explicit meta size; set to 0 to auto-compute from index_slots
    pub meta_size: usize,
    /// create files if missing
    pub create: bool,
    /// if true, force reinitialization of meta (zeroes meta!)
    pub force_reinit: bool,
    /// durable per-write flush when true
    pub sync_writes: bool,
    /// background flusher interval (ms) when sync_writes == false
    pub flusher_interval_ms: u64,
    /// how many messages between periodic info logs (0 = no periodic logs)
    pub periodic_log_every: u64,
    /// dedupe cache capacity (0 disables dedupe)
    pub dedupe_size: usize,
}

impl Default for TradeChunkWriterConfig {
    fn default() -> Self {
        TradeChunkWriterConfig {
            path_prefix: "rings/trade_chunk".to_string(),
            data_capacity: 512 * 1024 * 1024,
            index_slots: 32_768,
            meta_size: 0,
            create: true,
            force_reinit: false,
            sync_writes: false,
            flusher_interval_ms: 50,
            periodic_log_every: 10_000,
            dedupe_size: 0,
        }
    }
}

/// Create/open the ring and spawn a tokio task that consumes `rx` and writes trades to the ring.
/// Returns the Arc<MmapRing> so callers can inspect tail/seq or read back if needed.
///
/// New behaviors:
/// - Optional dedupe window (LRU) keyed by (asset + exchange_trade_id) when available,
///   otherwise a fallback hash of (asset, px, sz, side, exchange_ts_ms).
/// - Defensive timestamp normalization: if `exchange_ts_ms` looks like seconds, convert to ms.
/// - After write_record() returns seq, the task sets `tp.writer_seq` and `tp.persist_ts_ms`
///   and includes `writer_seq` in the metrics record (so CSV/metrics can correlate exactly).
pub async fn spawn_trade_chunk_writer(
    cfg: TradeChunkWriterConfig,
    mut rx: UnboundedReceiver<TradePrint>,
    metrics: Arc<MetricsAggregator>,
) -> Result<Arc<MmapRing>> {
    // prepare ring config
    let ring_cfg = MmapRingConfig {
        path_prefix: cfg.path_prefix.clone(),
        data_capacity: cfg.data_capacity,
        index_slots: cfg.index_slots,
        meta_size: if cfg.meta_size == 0 {
            // compute required meta size based on index slots
            MmapRing::compute_meta_size(cfg.index_slots)?
        } else {
            cfg.meta_size
        },
        create: cfg.create,
        force_reinit: cfg.force_reinit,
        sync_writes: cfg.sync_writes,
        flusher_interval_ms: cfg.flusher_interval_ms,
    };

    // open ring
    let ring = Arc::new(MmapRing::open(&ring_cfg)?);

    // spawn writer task
    let ring_clone = ring.clone();
    let metrics_clone = metrics.clone();
    let prefix = cfg.path_prefix.clone();
    let periodic = cfg.periodic_log_every;
    let dedupe_capacity = cfg.dedupe_size;

    tokio::spawn(async move {
        // LRU dedupe cache (only created if dedupe_capacity > 0)
        let mut dedupe: Option<LruCache<u64, ()>> = if dedupe_capacity > 0 {
            Some(LruCache::new(NonZeroUsize::new(dedupe_capacity).unwrap()))
        } else {
            None
        };

        let mut local_count: u64 = 0;
        while let Some(mut tp) = rx.recv().await {
            local_count = local_count.saturating_add(1);

            // Defensive normalization of exchange_ts_ms: if looks like seconds (small),
            // convert to milliseconds.
            if let Some(ts) = tp.exchange_ts_ms {
                if ts < 1_000_000_000_000_i64 {
                    tp.exchange_ts_ms = Some(ts * 1000);
                }
            }

            // build dedupe key
            let key = {
                // prefer (asset + exchange_trade_id)
                if let Some(ex_id) = tp.exchange_trade_id {
                    let mut s = DefaultHasher::new();
                    tp.asset.hash(&mut s);
                    ex_id.hash(&mut s);
                    s.finish()
                } else {
                    // fallback: hash asset, px, sz, side, exchange_ts_ms (if any)
                    let mut s = DefaultHasher::new();
                    tp.asset.hash(&mut s);
                    // float -> bits
                    let px_bits: u64 = tp.px.to_bits();
                    px_bits.hash(&mut s);
                    let sz_bits: u64 = tp.sz.to_bits();
                    sz_bits.hash(&mut s);
                    format!("{:?}", tp.side).hash(&mut s);
                    tp.exchange_ts_ms.unwrap_or(0i64).hash(&mut s);
                    s.finish()
                }
            };

            // dedupe check (if enabled)
            let mut is_dup = false;
            if let Some(cache) = dedupe.as_mut() {
                if cache.contains(&key) {
                    is_dup = true;
                } else {
                    cache.put(key, ());
                }
            }

            if is_dup {
                // skip write for duplicate
                if periodic > 0 && (local_count % periodic == 0) {
                    tracing::info!(
                        "trade chunk writer '{}' skipped {} duplicate messages so far",
                        prefix,
                        local_count
                    );
                }
                continue;
            }

            // populate writer timestamp (before write)
            tp.writer_ts_ms = Some(chrono::Utc::now().timestamp_millis());

            // serialize
            match serde_json::to_vec(&tp) {
                Ok(payload) => {
                    // attempt write; write kind 1 == trade
                    match ring_clone.write_record(1u8, &payload) {
                        Ok(seq) => {
                            tp.persist_ts_ms = Some(chrono::Utc::now().timestamp_millis());
                            tp.writer_seq = Some(seq);

                            // record metrics (non-blocking) — note: metrics now includes writer_seq
                            metrics_clone.record_trade(crate::metrics::MetricsRecord::Trade {
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

                            if periodic > 0 && (local_count % periodic == 0) {
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
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "trade chunk writer '{}' failed to serialize trade (msg #{}): {:?}",
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
    });

    Ok(ring)
}
