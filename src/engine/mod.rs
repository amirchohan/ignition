//! Deterministic matching-engine boundary.
//!
//! The engine owns order-book state, price-time priority mutations, and
//! matching behavior. It accepts protocol-agnostic domain commands and emits
//! typed order-entry and market-data outputs; gateway and protocol code stay
//! outside this module.

mod accounts;
mod book;
mod ladder;
mod orders;

use crate::domain::{
    DomainError, DomainResult, EngineSequencer, Price, RejectReason,
    rd::InstrumentSpec,
    trading::order::{AccountId, ExecutionReport, OrderEntry, OrderId, Side},
};
use crate::market_data::MarketDataRuntime;

use book::OrderBook;

/// Matching-engine storage and capacity settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineConfig {
    /// Validated numeric reference data for the single instrument.
    instrument: InstrumentSpec,
    /// Lowest integer price addressable by the book ladder.
    storage_min_price: Price,
    /// Highest integer price addressable by the book ladder.
    storage_max_price: Price,
    /// Number of price ticks in each ladder page.
    page_size: u32,
    /// Maximum number of orders allowed to rest simultaneously.
    max_resting_orders: u32,
    /// Highest account ID accepted by dense account indexing.
    max_account_id: u32,
    /// Maximum number of active price pages per side.
    max_active_pages: u32,
}

impl EngineConfig {
    /// Build validated matching-engine settings from startup configuration.
    pub fn new(
        instrument: InstrumentSpec,
        storage_min_price: u64,
        storage_max_price: u64,
        page_size: u32,
        max_resting_orders: u32,
        max_account_id: u32,
        max_active_pages: u32,
    ) -> DomainResult<Self> {
        let storage_min_price = Price::new(storage_min_price, &instrument)?;
        let storage_max_price = Price::new(storage_max_price, &instrument)?;

        if storage_min_price > storage_max_price
            || page_size == 0
            || !page_size.is_power_of_two()
            || max_resting_orders == 0
            || max_account_id == 0
            || max_active_pages == 0
        {
            return Err(DomainError::InvalidInstrumentSpec);
        }

        Ok(Self {
            instrument,
            storage_min_price,
            storage_max_price,
            page_size,
            max_resting_orders,
            max_account_id,
            max_active_pages,
        })
    }

    /// Return the validated single-instrument reference data.
    pub const fn instrument(self) -> InstrumentSpec {
        self.instrument
    }

    /// Return the lowest price representable by this book instance.
    pub const fn storage_min_price(self) -> Price {
        self.storage_min_price
    }

    /// Return the highest price representable by this book instance.
    pub const fn storage_max_price(self) -> Price {
        self.storage_max_price
    }

    /// Return the number of price ticks stored in each ladder page.
    pub const fn page_size(self) -> u32 {
        self.page_size
    }

    /// Return the maximum number of orders that may rest at once.
    pub const fn max_resting_orders(self) -> u32 {
        self.max_resting_orders
    }

    /// Return the highest account ID accepted by the dense account table.
    pub const fn max_account_id(self) -> u32 {
        self.max_account_id
    }

    /// Return the maximum number of ladder pages that may be active at once.
    pub const fn max_active_pages(self) -> u32 {
        self.max_active_pages
    }
}

/// Engine construction or execution failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineError {
    InvalidConfig,
    SequenceOverflow,
}

/// Single-symbol deterministic matching engine.
#[derive(Debug)]
pub struct Engine {
    /// In-memory book state and matching indexes.
    book: OrderBook,
}

impl Engine {
    /// Create an empty deterministic matching engine with preallocated state.
    pub fn new(config: EngineConfig) -> Result<Self, EngineError> {
        Ok(Self {
            book: OrderBook::new(config).map_err(|_| EngineError::InvalidConfig)?,
        })
    }

    /// Execute one normalized order-entry command into caller-owned output sinks.
    pub fn execute(
        &mut self,
        entry: OrderEntry,
        sequencer: &mut EngineSequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> Result<(), EngineError> {
        self.book
            .execute(entry, sequencer, market_data, emit_report)
            .map_err(|error| match error {
                DomainError::SequenceOverflow => EngineError::SequenceOverflow,
                _ => EngineError::InvalidConfig,
            })
    }

    /// Return the current best bid price and aggregate quantity.
    pub fn best_bid(&self) -> Option<(Price, u64)> {
        self.book.best_bid()
    }

    /// Return the current best ask price and aggregate quantity.
    pub fn best_ask(&self) -> Option<(Price, u64)> {
        self.book.best_ask()
    }

    /// Return the number of currently resting orders.
    pub fn resting_order_count(&self) -> usize {
        self.book.resting_order_count()
    }

    /// Return whether this dense account ID is valid for this engine instance.
    pub fn account_exists(&self, account_id: AccountId) -> bool {
        self.book.account_exists(account_id)
    }

    /// Return the number of currently resting orders for one account.
    pub fn account_open_order_count(&self, account_id: AccountId) -> Option<u32> {
        self.book.account_open_order_count(account_id)
    }

    /// Return raw open resting notional for one account.
    pub fn account_open_notional(&self, account_id: AccountId) -> Option<u128> {
        self.book.account_open_notional(account_id)
    }

    /// Return a read-only view of one resting order for risk projection.
    pub fn resting_order(
        &self,
        account_id: AccountId,
        order_id: OrderId,
    ) -> Option<RestingOrderView> {
        self.book.resting_order(account_id, order_id)
    }

    /// Project how much of an incoming order would execute or rest without mutation.
    pub fn project_order(
        &self,
        account_id: AccountId,
        side: Side,
        limit_price: Option<Price>,
        qty: u64,
    ) -> OrderProjection {
        self.book.project_order(account_id, side, limit_price, qty)
    }

    /// Append resting orders in deterministic snapshot order.
    pub fn snapshot_orders(&self, out: &mut Vec<RestingOrderView>) {
        self.book.snapshot_orders(out);
    }

    /// Cancel every resting order in deterministic book order.
    pub fn drain_all(
        &mut self,
        sequencer: &mut EngineSequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> Result<(), EngineError> {
        self.book
            .drain_all(sequencer, market_data, emit_report)
            .map_err(|error| match error {
                DomainError::SequenceOverflow => EngineError::SequenceOverflow,
                _ => EngineError::InvalidConfig,
            })
    }
}

/// Read-only resting-order view exposed across the engine boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestingOrderView {
    pub order_id: OrderId,
    pub account_id: AccountId,
    pub side: Side,
    pub price: Price,
    pub remaining_qty: u64,
}

/// Read-only no-mutation projection of an incoming order against visible book liquidity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderProjection {
    pub executable_qty: u64,
    pub residual_qty: u64,
    pub low_execution_price: Option<Price>,
    pub high_execution_price: Option<Price>,
    pub self_trade: bool,
}

pub(crate) fn structural_reject_reason(error: DomainError) -> RejectReason {
    match error {
        DomainError::InvalidPrice => RejectReason::InvalidPrice,
        DomainError::InvalidQuantity => RejectReason::InvalidQuantity,
        _ => RejectReason::Malformed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        EngineSeq, Price, Qty, RecvTs,
        rd::InstrumentSpec,
        trading::{
            md::{L1Update, L2Update, MdSeq, TradeId, TradePrint},
            order::{
                AccountId, ClientTs, ExecType, ExecutionReport, NewOrder, OrdStatus, OrderId,
                OrderType, Side, TimeInForce,
            },
        },
    };
    use crate::market_data::MarketDataRuntime;
    use crate::protocol::{
        MessageKind, SBE_HEADER_LEN, SCHEMA_VERSION, decode_header,
        generated::codecs::{
            ReadBuf, book_side, l1_update_codec, l2_update_codec, trade_print_codec,
        },
    };

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TradingEvent {
        ExecutionReport(ExecutionReport),
        TradePrint(TradePrint),
        L1Update(L1Update),
        L2Update(L2Update),
    }

    fn instrument() -> InstrumentSpec {
        InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument")
    }

    fn config() -> EngineConfig {
        EngineConfig::new(instrument(), 100, 10_000, 1024, 128, 100, 16).expect("config")
    }

    fn price(raw: u64) -> Price {
        Price::new(raw, &instrument()).expect("price")
    }

    fn qty(raw: u64) -> Qty {
        Qty::new(raw, &instrument()).expect("qty")
    }

    #[allow(clippy::too_many_arguments)]
    fn new_order(
        id: u64,
        account: u32,
        side: Side,
        order_type: OrderType,
        tif: TimeInForce,
        price_raw: Option<u64>,
        qty_raw: u64,
        recv_ts: u64,
    ) -> OrderEntry {
        OrderEntry::NewOrder(NewOrder {
            client_order_id: OrderId::new(id).expect("order"),
            account_id: AccountId::new(account).expect("account"),
            client_ts: ClientTs::new(recv_ts),
            recv_ts: RecvTs::new(recv_ts),
            side,
            order_type,
            time_in_force: tif,
            price: price_raw.map(price),
            qty: qty(qty_raw),
        })
    }

    fn cancel(id: u64, account: u32, orig: u64) -> OrderEntry {
        OrderEntry::CancelOrder(crate::domain::trading::order::CancelOrder {
            client_order_id: OrderId::new(id).expect("order"),
            account_id: AccountId::new(account).expect("account"),
            orig_client_order_id: OrderId::new(orig).expect("orig"),
            client_ts: ClientTs::new(1),
            recv_ts: RecvTs::new(1),
        })
    }

    fn replace(id: u64, account: u32, orig: u64, price_raw: u64, qty_raw: u64) -> OrderEntry {
        OrderEntry::CancelReplace(crate::domain::trading::order::CancelReplace {
            client_order_id: OrderId::new(id).expect("order"),
            account_id: AccountId::new(account).expect("account"),
            orig_client_order_id: OrderId::new(orig).expect("orig"),
            client_ts: ClientTs::new(1),
            recv_ts: RecvTs::new(1),
            price: price(price_raw),
            qty: qty(qty_raw),
        })
    }

    fn mass_cancel(id: u64, account: u32) -> OrderEntry {
        OrderEntry::MassCancel(crate::domain::trading::order::MassCancel {
            client_order_id: OrderId::new(id).expect("order"),
            account_id: AccountId::new(account).expect("account"),
            client_ts: ClientTs::new(1),
            recv_ts: RecvTs::new(1),
        })
    }

    fn exec_reports(
        events: &[TradingEvent],
    ) -> Vec<crate::domain::trading::order::ExecutionReport> {
        events
            .iter()
            .filter_map(|event| match event {
                TradingEvent::ExecutionReport(report) => Some(*report),
                _ => None,
            })
            .collect()
    }

    fn trade_makers(events: &[TradingEvent]) -> Vec<u64> {
        events
            .iter()
            .filter_map(|event| match event {
                TradingEvent::TradePrint(trade) => Some(trade.maker_order_id.raw()),
                _ => None,
            })
            .collect()
    }

    fn l1_updates(events: &[TradingEvent]) -> Vec<L1Update> {
        events
            .iter()
            .filter_map(|event| match event {
                TradingEvent::L1Update(update) => Some(*update),
                _ => None,
            })
            .collect()
    }

    fn l2_updates(events: &[TradingEvent]) -> Vec<L2Update> {
        events
            .iter()
            .filter_map(|event| match event {
                TradingEvent::L2Update(update) => Some(*update),
                _ => None,
            })
            .collect()
    }

    fn md_seqs(events: &[TradingEvent]) -> Vec<u64> {
        events
            .iter()
            .filter_map(|event| match event {
                TradingEvent::TradePrint(trade) => Some(trade.md_seq.raw()),
                TradingEvent::L1Update(update) => Some(update.md_seq.raw()),
                TradingEvent::L2Update(update) => Some(update.md_seq.raw()),
                _ => None,
            })
            .collect()
    }

    fn market_event_count(events: &[TradingEvent]) -> usize {
        events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    TradingEvent::TradePrint(_)
                        | TradingEvent::L1Update(_)
                        | TradingEvent::L2Update(_)
                )
            })
            .count()
    }

    fn reports_for_order(
        events: &[TradingEvent],
        order_id: u64,
    ) -> Vec<crate::domain::trading::order::ExecutionReport> {
        exec_reports(events)
            .into_iter()
            .filter(|report| report.client_order_id.raw() == order_id)
            .collect()
    }

    fn decode_market_data_event(frame: &[u8]) -> TradingEvent {
        match decode_header(frame).expect("market-data header").kind {
            MessageKind::TradePrint => {
                let decoder = trade_print_codec::TradePrintDecoder::default().wrap(
                    ReadBuf::new(frame),
                    SBE_HEADER_LEN,
                    trade_print_codec::SBE_BLOCK_LENGTH,
                    SCHEMA_VERSION,
                );
                TradingEvent::TradePrint(TradePrint {
                    md_seq: MdSeq::new(decoder.md_seq()),
                    trade_id: TradeId::new(decoder.trade_id()),
                    maker_order_id: OrderId::new(decoder.maker_order_id()).expect("maker"),
                    taker_order_id: OrderId::new(decoder.taker_order_id()).expect("taker"),
                    price: price(decoder.price()),
                    qty: qty(decoder.qty()),
                })
            }
            MessageKind::L1Update => {
                let decoder = l1_update_codec::L1UpdateDecoder::default().wrap(
                    ReadBuf::new(frame),
                    SBE_HEADER_LEN,
                    l1_update_codec::SBE_BLOCK_LENGTH,
                    SCHEMA_VERSION,
                );
                TradingEvent::L1Update(L1Update {
                    md_seq: MdSeq::new(decoder.md_seq()),
                    prev_md_seq: MdSeq::new(decoder.prev_md_seq()),
                    bid_price: (decoder.bid_price() != 0).then(|| price(decoder.bid_price())),
                    bid_qty: decoder.bid_qty(),
                    ask_price: (decoder.ask_price() != 0).then(|| price(decoder.ask_price())),
                    ask_qty: decoder.ask_qty(),
                })
            }
            MessageKind::L2Update => {
                let decoder = l2_update_codec::L2UpdateDecoder::default().wrap(
                    ReadBuf::new(frame),
                    SBE_HEADER_LEN,
                    l2_update_codec::SBE_BLOCK_LENGTH,
                    SCHEMA_VERSION,
                );
                TradingEvent::L2Update(L2Update {
                    md_seq: MdSeq::new(decoder.md_seq()),
                    prev_md_seq: MdSeq::new(decoder.prev_md_seq()),
                    side: match decoder.side() {
                        book_side::BookSide::Bid => Side::Buy,
                        book_side::BookSide::Ask => Side::Sell,
                        book_side::BookSide::NullVal => panic!("null book side"),
                    },
                    price: price(decoder.price()),
                    qty: decoder.qty(),
                })
            }
            kind => panic!("unexpected market-data frame: {kind:?}"),
        }
    }

    struct TestEngine {
        engine: Engine,
        sequencer: EngineSequencer,
        market_data: MarketDataRuntime,
    }

    impl TestEngine {
        fn new() -> Self {
            Self {
                engine: Engine::new(config()).expect("engine"),
                sequencer: EngineSequencer::default(),
                market_data: MarketDataRuntime::default(),
            }
        }

        fn best_bid(&self) -> Option<(Price, u64)> {
            self.engine.best_bid()
        }

        fn best_ask(&self) -> Option<(Price, u64)> {
            self.engine.best_ask()
        }

        fn resting_order_count(&self) -> usize {
            self.engine.resting_order_count()
        }

        fn account_open_order_count(&self, account: u32) -> Option<u32> {
            self.engine
                .account_open_order_count(AccountId::new(account).expect("account"))
        }

        fn account_open_notional(&self, account: u32) -> Option<u128> {
            self.engine
                .account_open_notional(AccountId::new(account).expect("account"))
        }

        fn execute(&mut self, entry: OrderEntry, events: &mut Vec<TradingEvent>, label: &str) {
            events.clear();
            self.market_data.clear_queued_frames();
            let mut emit_report = |report| {
                events.push(TradingEvent::ExecutionReport(report));
            };
            self.engine
                .execute(
                    entry,
                    &mut self.sequencer,
                    &mut self.market_data,
                    &mut emit_report,
                )
                .expect(label);
            for frame in self.market_data.queued_frames() {
                events.push(decode_market_data_event(frame));
            }
        }
    }

    fn execute(
        engine: &mut TestEngine,
        entry: OrderEntry,
        events: &mut Vec<TradingEvent>,
        label: &str,
    ) {
        engine.execute(entry, events, label);
    }

    fn assert_prev_links(events: &[TradingEvent]) {
        let mut previous = MdSeq::new(0);
        for event in events {
            match event {
                TradingEvent::TradePrint(trade) => {
                    assert_eq!(trade.md_seq.raw(), previous.raw() + 1);
                    previous = trade.md_seq;
                }
                TradingEvent::L2Update(update) => {
                    assert_eq!(update.prev_md_seq, previous);
                    assert_eq!(update.md_seq.raw(), previous.raw() + 1);
                    previous = update.md_seq;
                }
                TradingEvent::L1Update(update) => {
                    assert_eq!(update.prev_md_seq, previous);
                    assert_eq!(update.md_seq.raw(), previous.raw() + 1);
                    previous = update.md_seq;
                }
                TradingEvent::ExecutionReport(_) => {}
            }
        }
    }

    #[test]
    fn passive_orders_rest_and_update_top_of_book() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "execute",
        );

        assert_eq!(engine.best_bid(), Some((price(1000), 10)));
        assert_eq!(engine.best_ask(), None);
        assert_eq!(engine.resting_order_count(), 1);
        assert_eq!(engine.account_open_order_count(1), Some(1));
        assert_eq!(engine.account_open_notional(1), Some(10_000));
    }

    #[test]
    fn passive_add_emits_l2_then_l1_update() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "execute",
        );

        let l2 = l2_updates(&events);
        let l1 = l1_updates(&events);
        assert_eq!(l2.len(), 1);
        assert_eq!(l2[0].md_seq.raw(), 1);
        assert_eq!(l2[0].prev_md_seq.raw(), 0);
        assert_eq!(l2[0].side, Side::Buy);
        assert_eq!(l2[0].price, price(1000));
        assert_eq!(l2[0].qty, 10);
        assert_eq!(l1.len(), 1);
        assert_eq!(l1[0].md_seq.raw(), 2);
        assert_eq!(l1[0].prev_md_seq.raw(), 1);
        assert_eq!(l1[0].bid_price, Some(price(1000)));
        assert_eq!(l1[0].bid_qty, 10);
        assert_eq!(l1[0].ask_price, None);
        assert_eq!(l1[0].ask_qty, 0);
        assert_prev_links(&events);
    }

    #[test]
    fn behind_best_add_emits_l2_without_l1() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "best",
        );
        execute(
            &mut engine,
            new_order(
                2,
                2,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(999),
                20,
                2,
            ),
            &mut events,
            "behind",
        );

        let l2 = l2_updates(&events);
        let l1 = l1_updates(&events);
        assert_eq!(l2.len(), 1);
        assert_eq!(l2[0].md_seq.raw(), 3);
        assert_eq!(l2[0].side, Side::Buy);
        assert_eq!(l2[0].price, price(999));
        assert_eq!(l2[0].qty, 20);
        assert_eq!(l1.len(), 0);
    }

    #[test]
    fn improving_best_add_emits_l2_and_l1() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "best",
        );
        execute(
            &mut engine,
            new_order(
                2,
                2,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1001),
                20,
                2,
            ),
            &mut events,
            "improve",
        );

        let l2 = l2_updates(&events);
        let l1 = l1_updates(&events);
        assert_eq!(l2.len(), 1);
        assert_eq!(l2[0].md_seq.raw(), 3);
        assert_eq!(l2[0].side, Side::Buy);
        assert_eq!(l2[0].price, price(1001));
        assert_eq!(l2[0].qty, 20);
        assert_eq!(l1.len(), 1);
        assert_eq!(l1[0].md_seq.raw(), 4);
        assert_eq!(l1[0].prev_md_seq.raw(), 3);
        assert_eq!(l1[0].bid_price, Some(price(1001)));
        assert_eq!(l1[0].bid_qty, 20);
    }

    #[test]
    fn best_level_qty_increase_emits_l1() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "best",
        );
        execute(
            &mut engine,
            new_order(
                2,
                2,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                5,
                2,
            ),
            &mut events,
            "same best",
        );

        let l2 = l2_updates(&events);
        let l1 = l1_updates(&events);
        assert_eq!(l2.len(), 1);
        assert_eq!(l2[0].md_seq.raw(), 3);
        assert_eq!(l2[0].price, price(1000));
        assert_eq!(l2[0].qty, 15);
        assert_eq!(l1.len(), 1);
        assert_eq!(l1[0].md_seq.raw(), 4);
        assert_eq!(l1[0].bid_price, Some(price(1000)));
        assert_eq!(l1[0].bid_qty, 15);
    }

    #[test]
    fn behind_best_cancel_emits_l2_without_l1() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "best",
        );
        execute(
            &mut engine,
            new_order(
                2,
                2,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(999),
                20,
                2,
            ),
            &mut events,
            "behind",
        );

        execute(&mut engine, cancel(20, 2, 2), &mut events, "cancel behind");

        let l2 = l2_updates(&events);
        let l1 = l1_updates(&events);
        assert_eq!(l2.len(), 1);
        assert_eq!(l2[0].md_seq.raw(), 4);
        assert_eq!(l2[0].price, price(999));
        assert_eq!(l2[0].qty, 0);
        assert_eq!(l1.len(), 0);
    }

    #[test]
    fn execute_clears_caller_owned_event_buffer() {
        let mut engine = TestEngine::new();
        let mut events = vec![TradingEvent::ExecutionReport(
            crate::domain::trading::order::ExecutionReport {
                engine_seq: EngineSeq::new(99),
                client_order_id: OrderId::new(99).expect("order"),
                account_id: AccountId::new(99).expect("account"),
                exec_type: ExecType::Rejected,
                ord_status: OrdStatus::Rejected,
                reject_reason: RejectReason::Malformed,
                fill_price: None,
                fill_qty: 0,
                remaining_qty: 0,
            },
        )];

        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Buy,
                OrderType::Market,
                TimeInForce::Ioc,
                None,
                10,
                1,
            ),
            &mut events,
            "execute",
        );

        let reports = exec_reports(&events);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].client_order_id.raw(), 1);
    }

    #[test]
    fn matching_preserves_fifo_at_price_level() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        for id in [1, 2] {
            execute(
                &mut engine,
                new_order(
                    id,
                    id as u32,
                    Side::Sell,
                    OrderType::Limit,
                    TimeInForce::Gtc,
                    Some(1000),
                    10,
                    id,
                ),
                &mut events,
                "rest",
            );
        }

        execute(
            &mut engine,
            new_order(
                3,
                3,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Ioc,
                Some(1000),
                15,
                3,
            ),
            &mut events,
            "match",
        );

        assert_eq!(trade_makers(&events), vec![1, 2]);
        let taker_reports = reports_for_order(&events, 3);
        assert_eq!(taker_reports.len(), 2);
        assert_eq!(taker_reports[0].exec_type, ExecType::Trade);
        assert_eq!(taker_reports[0].ord_status, OrdStatus::PartiallyFilled);
        assert_eq!(taker_reports[0].fill_price, Some(price(1000)));
        assert_eq!(taker_reports[0].fill_qty, 10);
        assert_eq!(taker_reports[0].remaining_qty, 5);
        assert_eq!(taker_reports[1].exec_type, ExecType::Trade);
        assert_eq!(taker_reports[1].ord_status, OrdStatus::Filled);
        assert_eq!(taker_reports[1].fill_price, Some(price(1000)));
        assert_eq!(taker_reports[1].fill_qty, 5);
        assert_eq!(taker_reports[1].remaining_qty, 0);
        assert_eq!(engine.best_ask(), Some((price(1000), 5)));
    }

    #[test]
    fn fill_emits_trade_l2_delete_and_l1_after_unlink() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Sell,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "rest",
        );

        execute(
            &mut engine,
            new_order(
                2,
                2,
                Side::Buy,
                OrderType::Market,
                TimeInForce::Ioc,
                None,
                10,
                2,
            ),
            &mut events,
            "match",
        );

        assert_eq!(md_seqs(&events), vec![3, 4, 5]);
        assert_eq!(engine.account_open_order_count(1), Some(0));
        assert_eq!(engine.account_open_notional(1), Some(0));
        let l2 = l2_updates(&events);
        let l1 = l1_updates(&events);
        assert_eq!(l2.len(), 1);
        assert_eq!(l2[0].side, Side::Sell);
        assert_eq!(l2[0].price, price(1000));
        assert_eq!(l2[0].qty, 0);
        assert_eq!(l1.len(), 1);
        assert_eq!(l1[0].ask_price, None);
        assert_eq!(l1[0].ask_qty, 0);
        assert_eq!(engine.best_ask(), None);
    }

    #[test]
    fn cancel_unlinks_head_middle_and_tail_without_scanning() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        for id in [1, 2, 3] {
            execute(
                &mut engine,
                new_order(
                    id,
                    id as u32,
                    Side::Sell,
                    OrderType::Limit,
                    TimeInForce::Gtc,
                    Some(1000),
                    10,
                    id,
                ),
                &mut events,
                "rest",
            );
        }

        execute(&mut engine, cancel(20, 2, 2), &mut events, "cancel middle");
        execute(&mut engine, cancel(21, 1, 1), &mut events, "cancel head");
        execute(
            &mut engine,
            new_order(
                4,
                4,
                Side::Buy,
                OrderType::Market,
                TimeInForce::Ioc,
                None,
                10,
                4,
            ),
            &mut events,
            "match tail",
        );

        assert_eq!(trade_makers(&events), vec![3]);
        assert_eq!(engine.resting_order_count(), 0);
    }

    #[test]
    fn partial_fill_reduces_account_open_notional() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Sell,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "rest",
        );
        execute(
            &mut engine,
            new_order(
                2,
                2,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Ioc,
                Some(1000),
                4,
                2,
            ),
            &mut events,
            "partial fill",
        );

        assert_eq!(engine.account_open_order_count(1), Some(1));
        assert_eq!(engine.account_open_notional(1), Some(6_000));
    }

    #[test]
    fn cancel_at_shared_price_emits_remaining_l2_qty() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "rest first",
        );
        execute(
            &mut engine,
            new_order(
                2,
                2,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                5,
                2,
            ),
            &mut events,
            "rest second",
        );

        execute(&mut engine, cancel(20, 1, 1), &mut events, "cancel first");

        let l2 = l2_updates(&events);
        let l1 = l1_updates(&events);
        assert_eq!(l2.len(), 1);
        assert_eq!(l2[0].side, Side::Buy);
        assert_eq!(l2[0].price, price(1000));
        assert_eq!(l2[0].qty, 5);
        assert_eq!(l1.len(), 1);
        assert_eq!(l1[0].bid_price, Some(price(1000)));
        assert_eq!(l1[0].bid_qty, 5);
    }

    #[test]
    fn mass_cancel_only_removes_target_account_orders() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                7,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "rest",
        );
        execute(
            &mut engine,
            new_order(
                2,
                8,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1001),
                10,
                2,
            ),
            &mut events,
            "rest",
        );

        execute(&mut engine, mass_cancel(50, 7), &mut events, "mass cancel");

        assert_eq!(engine.resting_order_count(), 1);
        assert_eq!(engine.best_bid(), Some((price(1001), 10)));
    }

    #[test]
    fn mass_cancel_at_shared_price_emits_remaining_l2_qty() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                7,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "target account",
        );
        execute(
            &mut engine,
            new_order(
                2,
                8,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                5,
                2,
            ),
            &mut events,
            "other account",
        );

        execute(&mut engine, mass_cancel(50, 7), &mut events, "mass cancel");

        let l2 = l2_updates(&events);
        let l1 = l1_updates(&events);
        assert_eq!(l2.len(), 1);
        assert_eq!(l2[0].side, Side::Buy);
        assert_eq!(l2[0].price, price(1000));
        assert_eq!(l2[0].qty, 5);
        assert_eq!(l1.len(), 1);
        assert_eq!(l1[0].bid_price, Some(price(1000)));
        assert_eq!(l1[0].bid_qty, 5);
    }

    #[test]
    fn mass_cancel_emits_l1_only_when_top_changes() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                7,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "rest best",
        );
        execute(
            &mut engine,
            new_order(
                2,
                7,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(999),
                20,
                2,
            ),
            &mut events,
            "rest behind",
        );

        execute(&mut engine, mass_cancel(50, 7), &mut events, "mass cancel");

        let l2 = l2_updates(&events);
        let l1 = l1_updates(&events);
        assert_eq!(l2.len(), 2);
        assert_eq!(l1.len(), 2);
        assert_eq!(l2[0].side, Side::Buy);
        assert_eq!(l2[0].price, price(1000));
        assert_eq!(l2[0].qty, 0);
        assert_eq!(l1[0].bid_price, Some(price(999)));
        assert_eq!(l1[0].bid_qty, 20);
        assert_eq!(l2[1].side, Side::Buy);
        assert_eq!(l2[1].price, price(999));
        assert_eq!(l2[1].qty, 0);
        assert_eq!(l1[1].bid_price, None);
        assert_eq!(l1[1].bid_qty, 0);
        assert_eq!(md_seqs(&events), vec![4, 5, 6, 7]);
    }

    #[test]
    fn same_price_qty_reduction_replace_preserves_priority() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Sell,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "rest",
        );
        execute(
            &mut engine,
            new_order(
                2,
                2,
                Side::Sell,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                2,
            ),
            &mut events,
            "rest",
        );
        execute(
            &mut engine,
            replace(11, 1, 1, 1000, 5),
            &mut events,
            "replace",
        );

        execute(
            &mut engine,
            new_order(
                3,
                3,
                Side::Buy,
                OrderType::Market,
                TimeInForce::Ioc,
                None,
                5,
                3,
            ),
            &mut events,
            "match",
        );

        assert_eq!(trade_makers(&events), vec![11]);
        assert_eq!(engine.best_ask(), Some((price(1000), 10)));
        assert_eq!(engine.account_open_order_count(1), Some(0));
        assert_eq!(engine.account_open_notional(1), Some(0));
    }

    #[test]
    fn same_price_qty_reduction_replace_emits_l2_and_l1() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Sell,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "rest",
        );

        execute(
            &mut engine,
            replace(11, 1, 1, 1000, 5),
            &mut events,
            "replace",
        );

        let l2 = l2_updates(&events);
        let l1 = l1_updates(&events);
        assert_eq!(l2.len(), 1);
        assert_eq!(l2[0].md_seq.raw(), 3);
        assert_eq!(l2[0].side, Side::Sell);
        assert_eq!(l2[0].price, price(1000));
        assert_eq!(l2[0].qty, 5);
        assert_eq!(l1.len(), 1);
        assert_eq!(l1[0].md_seq.raw(), 4);
        assert_eq!(l1[0].ask_price, Some(price(1000)));
        assert_eq!(l1[0].ask_qty, 5);
    }

    #[test]
    fn same_price_same_qty_replace_does_not_emit_market_data() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Sell,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "rest",
        );

        execute(
            &mut engine,
            replace(11, 1, 1, 1000, 10),
            &mut events,
            "replace",
        );

        assert_eq!(market_event_count(&events), 0);
        assert_eq!(exec_reports(&events).len(), 1);
    }

    #[test]
    fn price_change_replace_loses_priority() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Sell,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "rest",
        );
        execute(
            &mut engine,
            new_order(
                2,
                2,
                Side::Sell,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                2,
            ),
            &mut events,
            "rest",
        );
        execute(
            &mut engine,
            replace(11, 1, 1, 1001, 10),
            &mut events,
            "replace",
        );

        execute(
            &mut engine,
            new_order(
                3,
                3,
                Side::Buy,
                OrderType::Market,
                TimeInForce::Ioc,
                None,
                10,
                3,
            ),
            &mut events,
            "match",
        );

        assert_eq!(trade_makers(&events), vec![2]);
    }

    #[test]
    fn storage_envelope_rejects_unrepresentable_prices() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(10_001),
                10,
                1,
            ),
            &mut events,
            "execute",
        );
        let reports = exec_reports(&events);

        assert_eq!(reports[0].exec_type, ExecType::Rejected);
        assert_eq!(reports[0].ord_status, OrdStatus::Rejected);
        assert_eq!(reports[0].reject_reason, RejectReason::InvalidPrice);
        assert_eq!(engine.resting_order_count(), 0);
        assert_eq!(market_event_count(&events), 0);
    }

    #[test]
    fn post_only_crossing_order_cancels_without_resting() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Sell,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "rest",
        );
        execute(
            &mut engine,
            new_order(
                2,
                2,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::PostOnly,
                Some(1000),
                10,
                2,
            ),
            &mut events,
            "post only",
        );
        let reports = exec_reports(&events);

        assert_eq!(reports[0].exec_type, ExecType::Canceled);
        assert_eq!(reports[0].ord_status, OrdStatus::Canceled);
        assert_eq!(engine.best_ask(), Some((price(1000), 10)));
    }

    #[test]
    fn market_no_fill_expires_without_fill_price() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Buy,
                OrderType::Market,
                TimeInForce::Ioc,
                None,
                10,
                1,
            ),
            &mut events,
            "execute",
        );
        let reports = reports_for_order(&events, 1);

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].exec_type, ExecType::Expired);
        assert_eq!(reports[0].ord_status, OrdStatus::Expired);
        assert_eq!(reports[0].fill_price, None);
        assert_eq!(reports[0].fill_qty, 0);
        assert_eq!(reports[0].remaining_qty, 0);
    }

    #[test]
    fn market_partial_fill_expires_residual_on_final_trade_report() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Sell,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "rest",
        );

        execute(
            &mut engine,
            new_order(
                2,
                2,
                Side::Buy,
                OrderType::Market,
                TimeInForce::Ioc,
                None,
                15,
                2,
            ),
            &mut events,
            "execute",
        );
        let reports = reports_for_order(&events, 2);

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].exec_type, ExecType::Trade);
        assert_eq!(reports[0].ord_status, OrdStatus::Expired);
        assert_eq!(reports[0].fill_price, Some(price(1000)));
        assert_eq!(reports[0].fill_qty, 10);
        assert_eq!(reports[0].remaining_qty, 0);
    }

    #[test]
    fn gtc_partial_fill_keeps_live_remaining_qty_on_trade_report() {
        let mut engine = TestEngine::new();
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(
                1,
                1,
                Side::Sell,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                10,
                1,
            ),
            &mut events,
            "rest",
        );

        execute(
            &mut engine,
            new_order(
                2,
                2,
                Side::Buy,
                OrderType::Limit,
                TimeInForce::Gtc,
                Some(1000),
                15,
                2,
            ),
            &mut events,
            "execute",
        );
        let reports = reports_for_order(&events, 2);

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].exec_type, ExecType::Trade);
        assert_eq!(reports[0].ord_status, OrdStatus::PartiallyFilled);
        assert_eq!(reports[0].fill_price, Some(price(1000)));
        assert_eq!(reports[0].fill_qty, 10);
        assert_eq!(reports[0].remaining_qty, 5);
        assert_eq!(engine.best_bid(), Some((price(1000), 5)));
    }
}
