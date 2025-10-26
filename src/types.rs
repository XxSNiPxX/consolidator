// src/types.rs
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AssetKind {
    Spot,
    Derivative,
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
    pub exchange_ts_ms: Option<i64>, // parsed from "E"
    pub event_u: Option<u64>,        // parsed from "u" if present
    pub recv_ts_ms: i64,             // local recv time
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
