// core/src/types.rs

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AssetKind {
    Spot,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TradePrint {
    pub exchange: String,
    pub asset: String,
    pub kind: AssetKind,
    pub px: f64,
    pub sz: f64,
    pub side: TradeSide,

    /// canonical exchange trade id (Binance `t`) when available
    pub exchange_trade_id: Option<u64>,

    /// canonical exchange timestamp in milliseconds (Binance `T` or `E`) normalized to ms
    pub exchange_ts_ms: Option<i64>,

    /// optional event-level u field (if you still use it)
    pub event_u: Option<u64>,

    /// ring writer sequence returned by write_record() (populated by writer task
    /// after write_record completes). NOTE: this field may NOT be present inside
    /// the exact bytes that were just written (see further explanation).
    pub writer_seq: Option<u64>,

    /// writer timestamp (set before or after write depending on flow)
    pub writer_ts_ms: Option<i64>,

    // per-hop timing
    pub recv_ts_ms: i64,
    pub parse_ts_ms: Option<i64>,
    pub enqueue_ts_ms: Option<i64>,
    pub persist_ts_ms: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ExchangeStatusKind {
    Connected,
    Reconnecting,
    Disconnected,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExchangeStatus {
    pub exchange: String,
    pub kind: ExchangeStatusKind,
    pub detail: Option<String>,
    pub ts_ms: i64,
}
