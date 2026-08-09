pub mod md;
pub mod order;

use super::ProtocolErrorEvent;
use md::{BookUpdate, TradePrint};
use order::ExecutionReport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingEvent {
    ExecutionReport(ExecutionReport),
    TradePrint(TradePrint),
    BookUpdate(BookUpdate),
    ProtocolError(ProtocolErrorEvent),
}
