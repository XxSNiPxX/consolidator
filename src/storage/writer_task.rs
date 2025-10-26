// src/storage/writer_task.rs (or inside storage/mmap_ring.rs for demo)
use crate::types::{OrderBookSnapshot, TradePrint};
use tokio::sync::mpsc::UnboundedReceiver;

pub async fn snapshot_writer_task(
    mut rx: UnboundedReceiver<OrderBookSnapshot>,
    ring: Arc<MmapRing>,
) {
    while let Some(snap) = rx.recv().await {
        // serialize a compact payload (JSON or msgpack). JSON example:
        if let Ok(payload) = serde_json::to_vec(&snap) {
            // kind 0 = snapshot
            if let Err(e) = ring.write_record(0u8, &payload) {
                tracing::warn!("ring write snapshot failed: {:?}", e);
            }
        }
    }
}

pub async fn trade_writer_task(mut rx: UnboundedReceiver<TradePrint>, ring: Arc<MmapRing>) {
    while let Some(tp) = rx.recv().await {
        if let Ok(payload) = serde_json::to_vec(&tp) {
            if let Err(e) = ring.write_record(1u8, &payload) {
                tracing::warn!("ring write trade failed: {:?}", e);
            }
        }
    }
}
