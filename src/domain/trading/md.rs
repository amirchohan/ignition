use crate::domain::{
    DomainError, DomainResult, EngineSeq, Price, Qty,
    trading::order::{OrderId, Side},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MdSeq(u64);

impl MdSeq {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn raw(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradeId(u64);

impl TradeId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn raw(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TradePrint {
    pub engine_seq: EngineSeq,
    pub md_seq: MdSeq,
    pub trade_id: TradeId,
    pub maker_order_id: OrderId,
    pub taker_order_id: OrderId,
    pub price: Price,
    pub qty: Qty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L1Update {
    pub engine_seq: EngineSeq,
    pub md_seq: MdSeq,
    pub prev_md_seq: MdSeq,
    pub bid_price: Option<Price>,
    pub bid_qty: u64,
    pub ask_price: Option<Price>,
    pub ask_qty: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2Update {
    pub engine_seq: EngineSeq,
    pub md_seq: MdSeq,
    pub prev_md_seq: MdSeq,
    pub side: Side,
    pub price: Price,
    pub qty: u64,
}

/// Market-data output emitted by deterministic book mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketDataEvent {
    TradePrint(TradePrint),
    L1Update(L1Update),
    L2Update(L2Update),
}

/// Deterministic sequence allocator for market-data streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarketDataSequencer {
    next_md_seq: u64,
    next_trade_id: u64,
}

impl Default for MarketDataSequencer {
    fn default() -> Self {
        Self {
            next_md_seq: 1,
            next_trade_id: 1,
        }
    }
}

impl MarketDataSequencer {
    pub const fn new(next_md_seq: u64, next_trade_id: u64) -> Self {
        Self {
            next_md_seq,
            next_trade_id,
        }
    }

    pub fn next_md_seq(&mut self) -> DomainResult<MdSeq> {
        let current = self.next_md_seq;
        self.next_md_seq = self.next_md_seq.checked_add(1).ok_or(DomainError::SequenceOverflow)?;
        Ok(MdSeq(current))
    }

    pub fn previous_md_seq(&self) -> MdSeq {
        MdSeq(self.next_md_seq.saturating_sub(1))
    }

    pub const fn next_trade_id_value(&self) -> TradeId {
        TradeId(self.next_trade_id)
    }

    pub fn next_trade_id(&mut self) -> DomainResult<TradeId> {
        let current = self.next_trade_id;
        self.next_trade_id = self.next_trade_id.checked_add(1).ok_or(DomainError::SequenceOverflow)?;
        Ok(TradeId(current))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn market_data_sequencer_is_monotonic() {
        let mut md = MarketDataSequencer::default();

        assert_eq!(md.previous_md_seq().raw(), 0);
        assert_eq!(md.next_md_seq().expect("md seq").raw(), 1);
        assert_eq!(md.previous_md_seq().raw(), 1);
        assert_eq!(md.next_trade_id().expect("trade id").raw(), 1);
        assert_eq!(md.next_md_seq().expect("md seq").raw(), 2);
    }

    #[test]
    fn market_data_sequencer_reports_overflow() {
        let mut md = MarketDataSequencer::new(u64::MAX - 1, u64::MAX - 1);
        assert_eq!(md.next_md_seq().expect("penultimate md").raw(), u64::MAX - 1);
        assert_eq!(md.next_md_seq(), Err(DomainError::SequenceOverflow));
        assert_eq!(md.next_trade_id().expect("penultimate trade").raw(), u64::MAX - 1);
        assert_eq!(md.next_trade_id(), Err(DomainError::SequenceOverflow));
    }

    proptest! {
        /// Checks that generated market-data and trade-id allocations always
        /// move forward one step at a time. This enforces the outbound stream
        /// monotonicity contract used by gap detection and replay comparison.
        #[test]
        fn property_market_data_sequences_are_strictly_monotonic(md_count in 1_usize..256, trade_count in 1_usize..256) {
            let mut md = MarketDataSequencer::default();
            let mut last_md_seq = md.previous_md_seq().raw();
            for expected in 1..=md_count as u64 {
                let next = md.next_md_seq().expect("md seq").raw();
                prop_assert!(next > last_md_seq);
                prop_assert_eq!(next, expected);
                prop_assert_eq!(md.previous_md_seq().raw(), next);
                last_md_seq = next;
            }

            let mut last_trade_id = 0;
            for expected in 1..=trade_count as u64 {
                let next = md.next_trade_id().expect("trade id").raw();
                prop_assert!(next > last_trade_id);
                prop_assert_eq!(next, expected);
                last_trade_id = next;
            }
        }
    }
}
