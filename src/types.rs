// src/types.rs
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AssetKind {
    Spot,
    // extend if needed
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TradeSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub exchange: String,
    pub asset: String,
    pub kind: AssetKind,
    pub bid: f64,
    pub ask: f64,
    pub mid: f64,
    // timestamps / event ids
    pub exchange_ts_ms: Option<i64>, // from exchange message
    pub event_u: Option<u64>,
    pub recv_ts_ms: i64,           // when we received / parsed
    pub writer_ts_ms: Option<i64>, // when writer persisted (populated by writer)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TradePrint {
    pub exchange: String,
    pub asset: String,
    pub kind: AssetKind,
    pub px: f64,
    pub sz: f64,
    pub side: TradeSide,
    pub exchange_ts_ms: Option<i64>,
    pub event_u: Option<u64>,
    pub recv_ts_ms: i64,
    pub writer_ts_ms: Option<i64>, // when writer persisted
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
