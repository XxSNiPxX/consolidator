// core/src/metrics.rs

use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex};
use tracing::warn;

/// Simple CSV metrics aggregator (trade-focused).
///
/// Designed for low-dependency usage inside `core`.
/// - Single-process append is expected (concurrent multi-process header handling
///   is non-deterministic; avoid multiple processes writing the same metrics file).
#[derive(Clone)]
pub struct MetricsAggregator {
    out_path: String,
    inner: Arc<ArcInner>,
}

struct ArcInner {
    file: Mutex<std::fs::File>,
}

impl MetricsAggregator {
    /// Create or open metrics file. If file is empty, write CSV header once.
    pub fn new(path: &str) -> Self {
        // file must be mutable for writeln!/flush()
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("metrics file open");
        // write header if file empty
        if file.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
            // added writer_seq column
            let _ = writeln!(
                &mut file,
                "ts_iso,kind,exchange,asset,recv,parse,enqueue,writer,persist,bytes,writer_seq"
            );
            let _ = file.flush();
        }
        MetricsAggregator {
            out_path: path.to_string(),
            inner: Arc::new(ArcInner {
                file: Mutex::new(file),
            }),
        }
    }

    /// Background CSV flush loop (no-op for now; kept for future extension).
    /// Call once with a cloned aggregator if you want periodic flushing or rotation logic.
    pub async fn run(&self) {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            // Intentionally minimal: no extra logging. Could add rotation/flush here.
        }
    }

    pub fn record_trade(&self, rec: MetricsRecord) {
        let mut f = self.inner.file.lock().unwrap();
        let line = match rec {
            MetricsRecord::Trade {
                ts_iso,
                exchange,
                asset,
                recv,
                parse,
                enqueue,
                writer,
                persist,
                bytes,
                writer_seq,
            } => format!(
                "{},{},{},{},{:?},{:?},{:?},{:?},{:?},{},{}\n",
                ts_iso,
                "trade",
                exchange,
                asset,
                recv,
                parse,
                enqueue,
                writer,
                persist,
                bytes,
                // writer_seq: Option<u64> -> print as empty or number
                writer_seq
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| "".to_string())
            ),
        };
        if let Err(e) = f.write_all(line.as_bytes()) {
            warn!("failed to write metrics line: {:?}", e);
        }
    }
}

/// Slimmed set of metric records focused on trades (snapshots removed).
pub enum MetricsRecord {
    Trade {
        ts_iso: String,
        exchange: String,
        asset: String,
        recv: i64,
        parse: Option<i64>,
        enqueue: Option<i64>,
        writer: Option<i64>,
        persist: Option<i64>,
        bytes: usize,
        writer_seq: Option<u64>,
    },
}
