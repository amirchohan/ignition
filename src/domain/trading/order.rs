use crate::domain::{
    DomainResult, EngineSeq, RecvTs, RejectReason,
    rd::{Price, Qty},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OrderId(u64);

impl OrderId {
    pub const fn raw(self) -> u64 {
        self.0
    }

    pub const fn new(value: u64) -> DomainResult<Self> {
        Ok(Self(value))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AccountId(u32);

impl AccountId {
    pub const fn raw(self) -> u32 {
        self.0
    }

    pub const fn new(value: u32) -> DomainResult<Self> {
        Ok(Self(value))
    }
}

/// Client supplied timestamp, carried opaquely through normalized order entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientTs(u64);

impl ClientTs {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// Buy/sell side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Buy,
    Sell,
}

/// Limit or market order type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Limit,
    Market,
}

/// Supported time-in-force policy values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce {
    Gtc,
    Ioc,
    PostOnly,
}

/// Execution-report state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecState {
    Accepted,
    Rejected,
    PartiallyFilled,
    Filled,
    Cancelled,
    Replaced,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderEntry {
    NewOrder(NewOrder),
    CancelOrder(CancelOrder),
    CancelReplace(CancelReplace),
    MassCancel(MassCancel),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewOrder {
    pub client_order_id: OrderId,
    pub account_id: AccountId,
    pub client_ts: ClientTs,
    pub recv_ts: RecvTs,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub price: Option<Price>,
    pub qty: Qty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelOrder {
    pub client_order_id: OrderId,
    pub account_id: AccountId,
    pub orig_client_order_id: OrderId,
    pub client_ts: ClientTs,
    pub recv_ts: RecvTs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelReplace {
    pub client_order_id: OrderId,
    pub account_id: AccountId,
    pub orig_client_order_id: OrderId,
    pub client_ts: ClientTs,
    pub recv_ts: RecvTs,
    pub price: Price,
    pub qty: Qty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MassCancel {
    pub client_order_id: OrderId,
    pub account_id: AccountId,
    pub client_ts: ClientTs,
    pub recv_ts: RecvTs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionReport {
    pub engine_seq: EngineSeq,
    pub client_order_id: OrderId,
    pub account_id: AccountId,
    pub recv_ts: RecvTs,
    pub state: ExecState,
    pub reject_reason: RejectReason,
    pub fill_price: Option<Price>,
    pub fill_qty: u64,
    pub remaining_qty: u64,
}
