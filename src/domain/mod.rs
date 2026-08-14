//! Protocol-agnostic domain model for the deterministic engine core.
//!
//! These types carry validated integer-scaled data through matching, risk,
//! market-data, replay, and benchmark paths. They deliberately do not contain
//! symbols, asset labels, generated protocol enums, wall-clock reads, or
//! randomness.

pub mod control;
pub mod rd;
pub mod trading;

/// Result type for validated domain construction and normalization.
pub type DomainResult<T> = Result<T, DomainError>;

/// Positive, tick-aligned integer price.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(u64);

impl Price {
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Construct a price from a value already proven positive and tick-aligned.
    pub(crate) const fn new_unchecked(value: u64) -> Self {
        Self(value)
    }

    pub fn new(value: u64, instrument: &rd::InstrumentSpec) -> DomainResult<Self> {
        if value == 0 || !value.is_multiple_of(instrument.price_tick) {
            return Err(DomainError::InvalidPrice);
        }

        Ok(Self(value))
    }
}

/// Positive, lot-aligned integer quantity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Qty(u64);

impl Qty {
    pub const fn raw(self) -> u64 {
        self.0
    }

    pub fn new(value: u64, instrument: &rd::InstrumentSpec) -> DomainResult<Self> {
        if value == 0 || !value.is_multiple_of(instrument.qty_lot) {
            return Err(DomainError::InvalidQuantity);
        }

        Ok(Self(value))
    }
}

/// Raw notional in `price_integer * qty_integer` units.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RawNotional(u128);

impl RawNotional {
    pub fn from_price_qty(price: Price, qty: Qty) -> Self {
        Self(u128::from(price.raw()) * u128::from(qty.raw()))
    }

    pub const fn raw(self) -> u128 {
        self.0
    }
}

/// Deterministic receive timestamp injected by runtime/replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecvTs(u64);

impl RecvTs {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// Monotonic sequence for execution, control, and protocol-error streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineSeq(u64);

impl EngineSeq {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// Deterministic sequence allocator for execution/control streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sequencer {
    next: u64,
}

impl Default for Sequencer {
    fn default() -> Self {
        Self { next: 1 }
    }
}

impl Sequencer {
    pub const fn new(next: u64) -> Self {
        Self { next }
    }

    pub fn next_engine_seq(&mut self) -> DomainResult<EngineSeq> {
        let current = self.next;
        self.next = self.next.checked_add(1).ok_or(DomainError::SequenceOverflow)?;
        Ok(EngineSeq(current))
    }
}

/// Reject taxonomy shared by risk, matching, and protocol output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    None,
    Malformed,
    UnknownOrder,
    InvalidPrice,
    InvalidQuantity,
    RiskOpenOrderLimit,
    RiskNotionalLimit,
    PriceBand,
    SelfTradePrevention,
    KillSwitchActive,
    HeartbeatLost,
}

/// Protocol-level outbound error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    DecodeError,
    UnsupportedTemplate,
    UnsupportedVersion,
    InternalError,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolErrorEvent {
    pub engine_seq: EngineSeq,
    pub template_id: u16,
    pub recv_ts: RecvTs,
    pub error_code: ErrorCode,
}

/// Typed domain failures. Order-entry validation failures carry reject reasons
/// so the runtime can later convert them into deterministic execution reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainError {
    InvalidInstrumentSpec,
    InvalidPrice,
    InvalidQuantity,
    Malformed,
    UnsupportedMessage,
    SequenceOverflow,
}

impl DomainError {
    /// Convert a validation error into the execution-report reject taxonomy.
    pub const fn reject_reason(self) -> RejectReason {
        match self {
            Self::InvalidPrice => RejectReason::InvalidPrice,
            Self::InvalidQuantity => RejectReason::InvalidQuantity,
            Self::Malformed | Self::InvalidInstrumentSpec | Self::UnsupportedMessage | Self::SequenceOverflow => RejectReason::Malformed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequencer_is_monotonic() {
        let mut sequencer = Sequencer::default();

        assert_eq!(sequencer.next_engine_seq().expect("seq").raw(), 1);
        assert_eq!(sequencer.next_engine_seq().expect("seq").raw(), 2);
    }

    #[test]
    fn sequencer_reports_overflow() {
        let mut sequencer = Sequencer::new(u64::MAX - 1);
        assert_eq!(sequencer.next_engine_seq().expect("penultimate seq").raw(), u64::MAX - 1);
        assert_eq!(sequencer.next_engine_seq(), Err(DomainError::SequenceOverflow));
    }
}
