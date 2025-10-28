// src/metrics.rs
use crate::types::{OrderBookSnapshot, TradePrint};
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct MetricsAggregator {
    // simple CSV writer guarded by Mutex for simplicity (single writer thread spawns run()).
    out_path: String,
    inner: Arc<ArcInner>,
}

struct ArcInner {
    file: Mutex<std::fs::File>,
}

impl MetricsAggregator {
    pub fn new(path: &str) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("metrics file open");
        // write header if file empty
        if file.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
            let _ = writeln!(
                &file,
                "ts_iso,kind,exchange,asset,recv,parse,enqueue,writer,persist,bytes"
            );
        }
        MetricsAggregator {
            out_path: path.to_string(),
            inner: Arc::new(ArcInner {
                file: Mutex::new(file),
            }),
        }
    }

    /// background CSV flush loop (called once)
    pub async fn run(&self) {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    }

    pub fn record_snapshot(&self, rec: MetricsRecord) {
        let mut f = self.inner.file.lock().unwrap();
        let line = match rec {
            MetricsRecord::Snapshot {
                ts_iso,
                exchange,
                asset,
                recv,
                parse,
                enqueue,
                writer,
                persist,
                bytes,
            } => format!(
                "{},{},{},{},{:?},{:?},{:?},{:?},{:?},{}\n",
                ts_iso, "snapshot", exchange, asset, recv, parse, enqueue, writer, persist, bytes
            ),
            _ => String::new(),
        };
        let _ = f.write_all(line.as_bytes());
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
            } => format!(
                "{},{},{},{},{:?},{:?},{:?},{:?},{:?},{}\n",
                ts_iso, "trade", exchange, asset, recv, parse, enqueue, writer, persist, bytes
            ),
            _ => String::new(),
        };
        let _ = f.write_all(line.as_bytes());
    }
}

pub enum MetricsRecord {
    Snapshot {
        ts_iso: String,
        exchange: String,
        asset: String,
        recv: i64,
        parse: Option<i64>,
        enqueue: Option<i64>,
        writer: Option<i64>,
        persist: Option<i64>,
        bytes: usize,
    },
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
    },
}
