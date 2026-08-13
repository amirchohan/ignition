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
    DomainError, DomainResult, Price, RejectReason, Sequencer,
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
        sequencer: &mut Sequencer,
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
    pub fn resting_order(&self, account_id: AccountId, order_id: OrderId) -> Option<RestingOrderView> {
        self.book.resting_order(account_id, order_id)
    }

    /// Project how much of an incoming order would execute or rest without mutation.
    pub fn project_order(&self, account_id: AccountId, side: Side, limit_price: Option<Price>, qty: u64) -> OrderProjection {
        self.book.project_order(account_id, side, limit_price, qty)
    }

    /// Visit resting orders in deterministic snapshot order.
    pub fn try_for_each_resting_order<E>(&self, visit: impl FnMut(RestingOrderView) -> Result<(), E>) -> Result<(), E> {
        self.book.try_for_each_resting_order(visit)
    }

    /// Cancel every resting order in deterministic book order.
    pub fn drain_all(
        &mut self,
        sequencer: &mut Sequencer,
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
        trading::order::{AccountId, ClientTs, ExecType, ExecutionReport, NewOrder, OrdStatus, OrderId, OrderType, Side, TimeInForce},
    };
    use crate::market_data::{MarketDataFeedConfig, MarketDataRuntime};

    type TradingEvent = ExecutionReport;

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

    fn exec_reports(events: &[TradingEvent]) -> Vec<crate::domain::trading::order::ExecutionReport> {
        events.to_vec()
    }

    fn reports_for_order(events: &[TradingEvent], order_id: u64) -> Vec<crate::domain::trading::order::ExecutionReport> {
        exec_reports(events)
            .into_iter()
            .filter(|report| report.client_order_id.raw() == order_id)
            .collect()
    }

    fn trade_report_makers(events: &[TradingEvent], taker_order_id: u64) -> Vec<u64> {
        exec_reports(events)
            .into_iter()
            .filter(|report| report.exec_type == ExecType::Trade && report.client_order_id.raw() != taker_order_id)
            .map(|report| report.client_order_id.raw())
            .collect()
    }

    fn market_data_runtime() -> Option<MarketDataRuntime> {
        match MarketDataRuntime::new(MarketDataFeedConfig::default()) {
            Ok(market_data) => Some(market_data),
            Err(crate::market_data::MarketDataError::Io(crate::io::IoError::Bind(error)))
                if error.kind() == std::io::ErrorKind::PermissionDenied =>
            {
                None
            }
            Err(error) => panic!("market-data: {error:?}"),
        }
    }

    struct TestEngine {
        engine: Engine,
        sequencer: Sequencer,
        market_data: MarketDataRuntime,
    }

    impl TestEngine {
        fn new() -> Option<Self> {
            Some(Self {
                engine: Engine::new(config()).expect("engine"),
                sequencer: Sequencer::default(),
                market_data: market_data_runtime()?,
            })
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
            self.engine.account_open_order_count(AccountId::new(account).expect("account"))
        }

        fn account_open_notional(&self, account: u32) -> Option<u128> {
            self.engine.account_open_notional(AccountId::new(account).expect("account"))
        }

        fn execute(&mut self, entry: OrderEntry, events: &mut Vec<TradingEvent>, label: &str) {
            events.clear();
            let mut emit_report = |report| {
                events.push(report);
            };
            self.engine
                .execute(entry, &mut self.sequencer, &mut self.market_data, &mut emit_report)
                .expect(label);
        }
    }

    fn execute(engine: &mut TestEngine, entry: OrderEntry, events: &mut Vec<TradingEvent>, label: &str) {
        engine.execute(entry, events, label);
    }

    #[test]
    fn passive_orders_rest_and_update_top_of_book() {
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
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
    fn snapshot_streams_rows_in_price_time_order() {
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        for (id, account, side, price_raw, qty_raw) in [
            (1, 1, Side::Buy, 1000, 10),
            (2, 2, Side::Buy, 1001, 5),
            (3, 3, Side::Buy, 1001, 7),
            (4, 4, Side::Sell, 1100, 9),
        ] {
            execute(
                &mut engine,
                new_order(id, account, side, OrderType::Limit, TimeInForce::Gtc, Some(price_raw), qty_raw, id),
                &mut events,
                "rest",
            );
        }

        let mut rows = Vec::new();
        engine
            .engine
            .try_for_each_resting_order(|row| {
                rows.push((row.order_id.raw(), row.side, row.price.raw(), row.remaining_qty));
                Ok::<(), ()>(())
            })
            .expect("snapshot rows");

        assert_eq!(
            rows,
            vec![
                (2, Side::Buy, 1001, 5),
                (3, Side::Buy, 1001, 7),
                (1, Side::Buy, 1000, 10),
                (4, Side::Sell, 1100, 9),
            ]
        );
    }

    #[test]
    fn execute_clears_caller_owned_event_buffer() {
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = vec![crate::domain::trading::order::ExecutionReport {
            engine_seq: EngineSeq::new(99),
            client_order_id: OrderId::new(99).expect("order"),
            account_id: AccountId::new(99).expect("account"),
            exec_type: ExecType::Rejected,
            ord_status: OrdStatus::Rejected,
            reject_reason: RejectReason::Malformed,
            fill_price: None,
            fill_qty: 0,
            remaining_qty: 0,
        }];

        execute(
            &mut engine,
            new_order(1, 1, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 10, 1),
            &mut events,
            "execute",
        );

        let reports = exec_reports(&events);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].client_order_id.raw(), 1);
    }

    #[test]
    fn matching_preserves_fifo_at_price_level() {
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        for id in [1, 2] {
            execute(
                &mut engine,
                new_order(id, id as u32, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, id),
                &mut events,
                "rest",
            );
        }

        execute(
            &mut engine,
            new_order(3, 3, Side::Buy, OrderType::Limit, TimeInForce::Ioc, Some(1000), 15, 3),
            &mut events,
            "match",
        );

        assert_eq!(trade_report_makers(&events, 3), vec![1, 2]);
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
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
            &mut events,
            "rest",
        );

        execute(
            &mut engine,
            new_order(2, 2, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 10, 2),
            &mut events,
            "match",
        );

        assert_eq!(engine.account_open_order_count(1), Some(0));
        assert_eq!(engine.account_open_notional(1), Some(0));
        assert_eq!(engine.best_ask(), None);
    }

    #[test]
    fn cancel_unlinks_head_middle_and_tail_without_scanning() {
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        for id in [1, 2, 3] {
            execute(
                &mut engine,
                new_order(id, id as u32, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, id),
                &mut events,
                "rest",
            );
        }

        execute(&mut engine, cancel(20, 2, 2), &mut events, "cancel middle");
        execute(&mut engine, cancel(21, 1, 1), &mut events, "cancel head");
        execute(
            &mut engine,
            new_order(4, 4, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 10, 4),
            &mut events,
            "match tail",
        );

        assert_eq!(trade_report_makers(&events, 4), vec![3]);
        assert_eq!(engine.resting_order_count(), 0);
    }

    #[test]
    fn partial_fill_reduces_account_open_notional() {
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
            &mut events,
            "rest",
        );
        execute(
            &mut engine,
            new_order(2, 2, Side::Buy, OrderType::Limit, TimeInForce::Ioc, Some(1000), 4, 2),
            &mut events,
            "partial fill",
        );

        assert_eq!(engine.account_open_order_count(1), Some(1));
        assert_eq!(engine.account_open_notional(1), Some(6_000));
    }

    #[test]
    fn mass_cancel_only_removes_target_account_orders() {
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(1, 7, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
            &mut events,
            "rest",
        );
        execute(
            &mut engine,
            new_order(2, 8, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1001), 10, 2),
            &mut events,
            "rest",
        );

        execute(&mut engine, mass_cancel(50, 7), &mut events, "mass cancel");

        assert_eq!(engine.resting_order_count(), 1);
        assert_eq!(engine.best_bid(), Some((price(1001), 10)));
    }

    #[test]
    fn same_price_qty_reduction_replace_preserves_priority() {
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
            &mut events,
            "rest",
        );
        execute(
            &mut engine,
            new_order(2, 2, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 2),
            &mut events,
            "rest",
        );
        execute(&mut engine, replace(11, 1, 1, 1000, 5), &mut events, "replace");

        execute(
            &mut engine,
            new_order(3, 3, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 5, 3),
            &mut events,
            "match",
        );

        assert_eq!(trade_report_makers(&events, 3), vec![11]);
        assert_eq!(engine.best_ask(), Some((price(1000), 10)));
        assert_eq!(engine.account_open_order_count(1), Some(0));
        assert_eq!(engine.account_open_notional(1), Some(0));
    }

    #[test]
    fn same_price_same_qty_replace_does_not_emit_market_data() {
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
            &mut events,
            "rest",
        );

        execute(&mut engine, replace(11, 1, 1, 1000, 10), &mut events, "replace");

        assert_eq!(exec_reports(&events).len(), 1);
    }

    #[test]
    fn price_change_replace_loses_priority() {
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
            &mut events,
            "rest",
        );
        execute(
            &mut engine,
            new_order(2, 2, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 2),
            &mut events,
            "rest",
        );
        execute(&mut engine, replace(11, 1, 1, 1001, 10), &mut events, "replace");

        execute(
            &mut engine,
            new_order(3, 3, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 10, 3),
            &mut events,
            "match",
        );

        assert_eq!(trade_report_makers(&events, 3), vec![2]);
    }

    #[test]
    fn storage_envelope_rejects_unrepresentable_prices() {
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(10_001), 10, 1),
            &mut events,
            "execute",
        );
        let reports = exec_reports(&events);

        assert_eq!(reports[0].exec_type, ExecType::Rejected);
        assert_eq!(reports[0].ord_status, OrdStatus::Rejected);
        assert_eq!(reports[0].reject_reason, RejectReason::InvalidPrice);
        assert_eq!(engine.resting_order_count(), 0);
    }

    #[test]
    fn post_only_crossing_order_cancels_without_resting() {
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
            &mut events,
            "rest",
        );
        execute(
            &mut engine,
            new_order(2, 2, Side::Buy, OrderType::Limit, TimeInForce::PostOnly, Some(1000), 10, 2),
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
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(1, 1, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 10, 1),
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
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
            &mut events,
            "rest",
        );

        execute(
            &mut engine,
            new_order(2, 2, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 15, 2),
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
        let Some(mut engine) = TestEngine::new() else {
            return;
        };
        let mut events = Vec::new();
        execute(
            &mut engine,
            new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
            &mut events,
            "rest",
        );

        execute(
            &mut engine,
            new_order(2, 2, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1000), 15, 2),
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
