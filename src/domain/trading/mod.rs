pub mod md;
pub mod order;

use super::ProtocolErrorEvent;
use md::{L1Update, L2Update, TradePrint};
use order::ExecutionReport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingEvent {
    ExecutionReport(ExecutionReport),
    TradePrint(TradePrint),
    L1Update(L1Update),
    L2Update(L2Update),
    ProtocolError(ProtocolErrorEvent),
}
