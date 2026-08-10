//! Market-data boundary.
//!
//! Market data owns outbound trade, L1, and L2 update shaping. It observes
//! engine mutations and produces sequenced outbound messages, but it must not
//! make matching decisions.

use crate::domain::{
    DomainError, DomainResult, Price, Qty,
    rd::InstrumentSpec,
    trading::{
        TradingEvent,
        md::{L1Update, L2Update, MarketDataSequencer, TradePrint},
        order::{OrderId, Side},
    },
};

/// Market-data event shaper and deterministic sequence owner.
#[derive(Debug, Default)]
pub struct MarketDataRuntime {
    sequencer: MarketDataSequencer,
    last_l1: Option<L1Update>,
}

impl MarketDataRuntime {
    /// Emit one trade print with a fresh market-data sequence and trade ID.
    #[allow(clippy::too_many_arguments)]
    pub fn publish_trade(
        &mut self,
        maker_order_id: OrderId,
        taker_order_id: OrderId,
        price: Price,
        fill_qty: u64,
        instrument: &InstrumentSpec,
        events: &mut Vec<TradingEvent>,
    ) -> DomainResult<()> {
        let qty = Qty::new(fill_qty, instrument).map_err(|_| DomainError::InvalidQuantity)?;
        let md_seq = self.sequencer.next_md_seq()?;
        events.push(TradingEvent::TradePrint(TradePrint {
            md_seq,
            trade_id: self.sequencer.next_trade_id()?,
            maker_order_id,
            taker_order_id,
            price,
            qty,
        }));
        Ok(())
    }

    /// Emit one absolute L2 aggregate level update.
    pub fn publish_l2_update(
        &mut self,
        side: Side,
        price: Price,
        qty: u64,
        events: &mut Vec<TradingEvent>,
    ) -> DomainResult<()> {
        let prev_md_seq = self.sequencer.previous_md_seq();
        let md_seq = self.sequencer.next_md_seq()?;
        events.push(TradingEvent::L2Update(L2Update {
            md_seq,
            prev_md_seq,
            side,
            price,
            qty,
        }));
        Ok(())
    }

    /// Emit one L1 top-of-book update only when the top state changed.
    pub fn publish_l1_update_if_changed(
        &mut self,
        best_bid: Option<(Price, u64)>,
        best_ask: Option<(Price, u64)>,
        events: &mut Vec<TradingEvent>,
    ) -> DomainResult<()> {
        let bid_price = best_bid.map(|(price, _)| price);
        let bid_qty = best_bid.map_or(0, |(_, qty)| qty);
        let ask_price = best_ask.map(|(price, _)| price);
        let ask_qty = best_ask.map_or(0, |(_, qty)| qty);

        if self.last_l1.is_some_and(|last| {
            last.bid_price == bid_price
                && last.bid_qty == bid_qty
                && last.ask_price == ask_price
                && last.ask_qty == ask_qty
        }) {
            return Ok(());
        }

        let prev_md_seq = self.sequencer.previous_md_seq();
        let md_seq = self.sequencer.next_md_seq()?;
        let event = L1Update {
            md_seq,
            prev_md_seq,
            bid_price,
            bid_qty,
            ask_price,
            ask_qty,
        };
        events.push(TradingEvent::L1Update(event));
        self.last_l1 = Some(event);
        Ok(())
    }
}
