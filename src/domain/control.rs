use super::{DomainResult, EngineSeq, ProtocolErrorEvent, RecvTs, RejectReason};
use crate::domain::Price;
use crate::domain::trading::order::{AccountId, OrderId, Side};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RequestId(u32);

impl RequestId {
    pub const fn raw(self) -> u32 {
        self.0
    }

    pub const fn new(value: u32) -> DomainResult<Self> {
        Ok(Self(value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeartbeatSeq(u32);

impl HeartbeatSeq {
    pub const fn raw(self) -> u32 {
        self.0
    }

    pub const fn new(value: u32) -> DomainResult<Self> {
        Ok(Self(value))
    }
}

/// Kill-switch control action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KillSwitchAction {
    HaltTrading,
    Shutdown,
    ResumeTrading,
}

/// Control-plane request result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlResult {
    Ok,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlRequest {
    KillSwitch(KillSwitch),
    Snapshot(Snapshot),
    Heartbeat(Heartbeat),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlEvent {
    ControlResponse(ControlResponse),
    HeartbeatStatus(HeartbeatStatus),
    SnapshotBegin(SnapshotBegin),
    SnapshotOrder(SnapshotOrder),
    SnapshotEnd(SnapshotEnd),
    ProtocolError(ProtocolErrorEvent),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KillSwitch {
    pub request_id: RequestId,
    pub recv_ts: RecvTs,
    pub action: KillSwitchAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snapshot {
    pub request_id: RequestId,
    pub recv_ts: RecvTs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Heartbeat {
    pub heartbeat_seq: HeartbeatSeq,
    pub recv_ts: RecvTs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlResponse {
    pub engine_seq: EngineSeq,
    pub request_id: RequestId,
    pub recv_ts: RecvTs,
    pub result: ControlResult,
    pub reason: RejectReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatStatus {
    pub engine_seq: EngineSeq,
    pub heartbeat_seq: HeartbeatSeq,
    pub recv_ts: RecvTs,
    pub result: ControlResult,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotBegin {
    pub engine_seq: EngineSeq,
    pub request_id: RequestId,
    pub recv_ts: RecvTs,
    pub bid_price: Option<Price>,
    pub bid_qty: u64,
    pub ask_price: Option<Price>,
    pub ask_qty: u64,
    pub resting_order_count: u64,
    pub last_md_seq: u64,
    pub next_trade_id: u64,
    pub risk_reference_price: Price,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotOrder {
    pub engine_seq: EngineSeq,
    pub request_id: RequestId,
    pub row_index: u64,
    pub side: Side,
    pub client_order_id: OrderId,
    pub account_id: AccountId,
    pub price: Price,
    pub remaining_qty: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotEnd {
    pub engine_seq: EngineSeq,
    pub request_id: RequestId,
    pub recv_ts: RecvTs,
    pub row_count: u64,
    pub result: ControlResult,
    pub reason: RejectReason,
}
