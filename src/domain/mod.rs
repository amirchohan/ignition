//! Protocol-agnostic domain model for the deterministic engine core.
//!
//! These types carry validated integer-scaled data into later matching, risk,
//! market-data, and replay milestones. They deliberately do not contain symbols,
//! asset labels, generated protocol enums, wall-clock reads, or randomness.

pub mod control;
pub mod rd;
pub mod trading;

/// Result type for validated domain construction and normalization.
pub type DomainResult<T> = Result<T, DomainError>;

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
pub struct EngineSequencer {
    next: u64,
}

impl Default for EngineSequencer {
    fn default() -> Self {
        Self { next: 1 }
    }
}

impl EngineSequencer {
    pub const fn new(next: u64) -> Self {
        Self { next }
    }

    pub fn next_engine_seq(&mut self) -> DomainResult<EngineSeq> {
        let current = self.next;
        self.next = self
            .next
            .checked_add(1)
            .ok_or(DomainError::SequenceOverflow)?;
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
            Self::Malformed
            | Self::InvalidInstrumentSpec
            | Self::UnsupportedMessage
            | Self::SequenceOverflow => RejectReason::Malformed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_sequencer_is_monotonic() {
        let mut engine = EngineSequencer::default();

        assert_eq!(engine.next_engine_seq().expect("seq").raw(), 1);
        assert_eq!(engine.next_engine_seq().expect("seq").raw(), 2);
    }

    #[test]
    fn engine_sequencer_reports_overflow() {
        let mut engine = EngineSequencer::new(u64::MAX - 1);
        assert_eq!(
            engine.next_engine_seq().expect("penultimate seq").raw(),
            u64::MAX - 1
        );
        assert_eq!(engine.next_engine_seq(), Err(DomainError::SequenceOverflow));
    }
}
