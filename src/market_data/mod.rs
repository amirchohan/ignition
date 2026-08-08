//! Market-data boundary.
//!
//! Market data owns outbound trade and book-update shaping. It observes engine
//! events and produces sequenced outbound messages, but it must not make
//! matching decisions.

/// Market-data runtime shell for future milestones.
#[derive(Debug, Default)]
pub struct MarketDataRuntime;
