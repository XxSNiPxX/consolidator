// src/metrics.rs
use parking_lot::Mutex;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::interval;

#[derive(Default, Clone)]
struct PerSecondStats {
    snap_count: u64,
    snap_bytes: u64,
    snap_ex_recv_sum: u128,
    snap_recv_write_sum: u128,
    snap_max_ex_recv: u64,
    snap_max_recv_write: u64,

    trade_count: u64,
    trade_bytes: u64,
    trade_ex_recv_sum: u128,
    trade_recv_write_sum: u128,
    trade_max_ex_recv: u64,
    trade_max_recv_write: u64,
}

#[derive(Clone)]
pub struct MetricsSnapshot {
    pub snap_count: u64,
    pub snap_bytes: u64,
    pub snap_avg_ex_recv_ms: u64,
    pub snap_avg_recv_write_ms: u64,
    pub snap_max_ex_recv_ms: u64,
    pub snap_max_recv_write_ms: u64,

    pub trade_count: u64,
    pub trade_bytes: u64,
    pub trade_avg_ex_recv_ms: u64,
    pub trade_avg_recv_write_ms: u64,
    pub trade_max_ex_recv_ms: u64,
    pub trade_max_recv_write_ms: u64,
}

#[derive(Clone)]
pub struct MetricsAggregator {
    inner: Arc<Mutex<PerSecondStats>>,
    csv_path: String,
}

impl MetricsAggregator {
    pub fn new(csv_path: &str) -> Self {
        MetricsAggregator {
            inner: Arc::new(Mutex::new(PerSecondStats::default())),
            csv_path: csv_path.to_string(),
        }
    }

    pub fn record_snapshot_latency(&self, ex_recv: u64, recv_write: u64, _total: u64, bytes: u64) {
        let mut s = self.inner.lock();
        s.snap_count += 1;
        s.snap_bytes += bytes;
        s.snap_ex_recv_sum += ex_recv as u128;
        s.snap_recv_write_sum += recv_write as u128;
        if ex_recv > s.snap_max_ex_recv {
            s.snap_max_ex_recv = ex_recv;
        }
        if recv_write > s.snap_max_recv_write {
            s.snap_max_recv_write = recv_write;
        }
    }

    pub fn record_trade_latency(&self, ex_recv: u64, recv_write: u64, _total: u64, bytes: u64) {
        let mut s = self.inner.lock();
        s.trade_count += 1;
        s.trade_bytes += bytes;
        s.trade_ex_recv_sum += ex_recv as u128;
        s.trade_recv_write_sum += recv_write as u128;
        if ex_recv > s.trade_max_ex_recv {
            s.trade_max_ex_recv = ex_recv;
        }
        if recv_write > s.trade_max_recv_write {
            s.trade_max_recv_write = recv_write;
        }
    }

    /// background task: runs forever and writes per-second CSV rows
    pub async fn run(self: Arc<Self>) {
        // open CSV and write header
        let mut file = File::create(&self.csv_path).expect("create metrics.csv");
        let header = "ts, snap_count, snap_bytes, snap_avg_ex_recv_ms, snap_avg_recv_write_ms, snap_max_ex_recv_ms, snap_max_recv_write_ms, trade_count, trade_bytes, trade_avg_ex_recv_ms, trade_avg_recv_write_ms, trade_max_ex_recv_ms, trade_max_recv_write_ms\n";
        file.write_all(header.as_bytes()).unwrap();
        file.flush().unwrap();

        let mut ticker = interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;
            let mut s = self.inner.lock();
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            // compute averages
            let snap_avg_ex_recv = if s.snap_count > 0 {
                (s.snap_ex_recv_sum / (s.snap_count as u128)) as u64
            } else {
                0
            };
            let snap_avg_recv_write = if s.snap_count > 0 {
                (s.snap_recv_write_sum / (s.snap_count as u128)) as u64
            } else {
                0
            };
            let trade_avg_ex_recv = if s.trade_count > 0 {
                (s.trade_ex_recv_sum / (s.trade_count as u128)) as u64
            } else {
                0
            };
            let trade_avg_recv_write = if s.trade_count > 0 {
                (s.trade_recv_write_sum / (s.trade_count as u128)) as u64
            } else {
                0
            };

            let row = format!(
                "{}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}\n",
                ts,
                s.snap_count,
                s.snap_bytes,
                snap_avg_ex_recv,
                snap_avg_recv_write,
                s.snap_max_ex_recv,
                s.snap_max_recv_write,
                s.trade_count,
                s.trade_bytes,
                trade_avg_ex_recv,
                trade_avg_recv_write,
                s.trade_max_ex_recv,
                s.trade_max_recv_write
            );
            file.write_all(row.as_bytes()).unwrap();
            file.flush().unwrap();

            // reset counters for next second
            *s = PerSecondStats::default();
        }
    }

    /// Return a snapshot of current aggregated stats and zero them (for health logging).
    /// This is intended for periodic health logs (not the per-second CSV which runs separately).
    pub fn snapshot_and_reset(&self) -> MetricsSnapshot {
        let mut s = self.inner.lock();
        let snap_count = s.snap_count;
        let snap_bytes = s.snap_bytes;
        let snap_avg_ex_recv_ms = if s.snap_count > 0 {
            (s.snap_ex_recv_sum / (s.snap_count as u128)) as u64
        } else {
            0
        };
        let snap_avg_recv_write_ms = if s.snap_count > 0 {
            (s.snap_recv_write_sum / (s.snap_count as u128)) as u64
        } else {
            0
        };
        let snap_max_ex_recv_ms = s.snap_max_ex_recv;
        let snap_max_recv_write_ms = s.snap_max_recv_write;

        let trade_count = s.trade_count;
        let trade_bytes = s.trade_bytes;
        let trade_avg_ex_recv_ms = if s.trade_count > 0 {
            (s.trade_ex_recv_sum / (s.trade_count as u128)) as u64
        } else {
            0
        };
        let trade_avg_recv_write_ms = if s.trade_count > 0 {
            (s.trade_recv_write_sum / (s.trade_count as u128)) as u64
        } else {
            0
        };
        let trade_max_ex_recv_ms = s.trade_max_ex_recv;
        let trade_max_recv_write_ms = s.trade_max_recv_write;

        // reset
        *s = PerSecondStats::default();

        MetricsSnapshot {
            snap_count,
            snap_bytes,
            snap_avg_ex_recv_ms,
            snap_avg_recv_write_ms,
            snap_max_ex_recv_ms,
            snap_max_recv_write_ms,
            trade_count,
            trade_bytes,
            trade_avg_ex_recv_ms,
            trade_avg_recv_write_ms,
            trade_max_ex_recv_ms,
            trade_max_recv_write_ms,
        }
    }
}
