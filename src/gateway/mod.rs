//! Gateway boundary.
//!
//! The gateway owns frame/session ingress and egress. It may decode protocol
//! frames and forward typed control requests or order entries, but it must not
//! make matching, risk, or market-data decisions.

use std::collections::VecDeque;

use crate::domain::trading::{TradingEvent, order::OrderEntry};
use crate::protocol::{ProtocolFrame, ProtocolResult};

/// Buffer-oriented frame decoder used by future transports and clients.
pub trait FrameDecoder {
    /// Decode a complete input frame header from `input`.
    fn decode_frame(&self, input: &[u8]) -> ProtocolResult<ProtocolFrame>;
}

/// Result of one gateway ingress poll phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewayPoll {
    /// Whether this poll produced at least one order-entry message.
    pub made_progress: bool,
    /// Whether this gateway adapter has no more input.
    pub closed: bool,
}

/// Typed in-process gateway adapter used by runtime tests and future replay clients.
#[derive(Debug, Default)]
pub struct GatewayRuntime {
    orders: VecDeque<OrderEntry>,
    published_trading: Vec<TradingEvent>,
    closed_when_drained: bool,
}

impl GatewayRuntime {
    /// Create a finite typed gateway from already-normalized order entries.
    pub fn from_orders(orders: impl IntoIterator<Item = OrderEntry>) -> Self {
        Self {
            orders: orders.into_iter().collect(),
            published_trading: Vec::new(),
            closed_when_drained: true,
        }
    }

    /// Poll order-entry input into the caller-owned order buffer.
    pub fn poll(&mut self, orders: &mut Vec<OrderEntry>) -> GatewayPoll {
        let start_len = orders.len();
        while let Some(order) = self.orders.pop_front() {
            orders.push(order);
        }

        GatewayPoll {
            made_progress: orders.len() != start_len,
            closed: self.closed_when_drained && self.orders.is_empty(),
        }
    }

    /// Publish trading events to this adapter's outbound sink.
    pub fn publish_trading(&mut self, events: &[TradingEvent]) {
        self.published_trading.extend_from_slice(events);
    }

    /// Return all trading events published through this gateway.
    pub fn published_trading(&self) -> &[TradingEvent] {
        &self.published_trading
    }
}
