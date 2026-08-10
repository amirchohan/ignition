//! Runtime orchestration boundary.
//!
//! Runtime owns the poll-driven phase order. It does not parse wire bytes or
//! own book internals; it moves typed messages between control, gateway, risk,
//! matching, market-data, and egress in a deterministic loop.

use crate::{
    control::ControlRuntime,
    domain::{
        EngineSequencer, RejectReason,
        trading::{
            TradingEvent,
            order::{ExecType, ExecutionReport, OrdStatus, OrderEntry},
        },
    },
    engine::{Engine, EngineConfig, EngineError},
    gateway::GatewayRuntime,
    market_data::MarketDataRuntime,
    risk::{RiskConfig, RiskDecision, RiskRuntime},
};

/// Runtime orchestration failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeError {
    Engine(EngineError),
}

impl From<EngineError> for RuntimeError {
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}

/// Summary of work completed by a runtime loop.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeSummary {
    pub iterations: u64,
    pub processed_orders: u64,
    pub risk_rejects: u64,
    pub emitted_trading_events: u64,
}

/// Result of one runtime poll iteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimePoll {
    pub made_progress: bool,
    pub closed: bool,
    pub processed_orders: u64,
    pub risk_rejects: u64,
    pub emitted_trading_events: u64,
}

/// Poll-driven runtime orchestrator.
#[derive(Debug)]
pub struct Runtime {
    /// Control-plane state and ingress.
    control: ControlRuntime,
    /// Order-entry ingress and trading egress.
    gateway: GatewayRuntime,
    /// Pre-match risk gate and reference-price state.
    risk: RiskRuntime,
    /// Matching engine state.
    engine: Engine,
    /// Market-data shaper and market-data sequencer.
    market_data: MarketDataRuntime,
    /// Shared deterministic execution/control sequencer.
    sequencer: EngineSequencer,
    /// Reusable per-iteration order buffer.
    orders: Vec<OrderEntry>,
    /// Reusable per-order event buffer.
    events: Vec<TradingEvent>,
}

impl Runtime {
    /// Create a runtime from prebuilt control and gateway adapters.
    pub fn new(
        engine_config: EngineConfig,
        risk_config: RiskConfig,
        control: ControlRuntime,
        gateway: GatewayRuntime,
    ) -> Result<Self, RuntimeError> {
        Ok(Self {
            control,
            gateway,
            risk: RiskRuntime::new(risk_config),
            engine: Engine::new(engine_config)?,
            market_data: MarketDataRuntime::default(),
            sequencer: EngineSequencer::default(),
            orders: Vec::new(),
            events: Vec::new(),
        })
    }

    /// Poll until all finite adapters are closed and no buffered work remains.
    pub fn run(&mut self) -> Result<RuntimeSummary, RuntimeError> {
        let mut summary = RuntimeSummary::default();
        loop {
            let poll = self.poll_once()?;
            summary.iterations += 1;
            summary.processed_orders += poll.processed_orders;
            summary.risk_rejects += poll.risk_rejects;
            summary.emitted_trading_events += poll.emitted_trading_events;

            if poll.closed {
                return Ok(summary);
            }
            if !poll.made_progress {
                std::thread::yield_now();
            }
        }
    }

    /// Execute one bounded runtime iteration.
    pub fn poll_once(&mut self) -> Result<RuntimePoll, RuntimeError> {
        let control_poll = self.control.poll();
        self.orders.clear();
        let gateway_poll = self.gateway.poll(&mut self.orders);

        let mut processed_orders = 0;
        let mut risk_rejects = 0;
        let mut emitted_trading_events = 0;
        for index in 0..self.orders.len() {
            let order = self.orders[index];
            processed_orders += 1;
            let rejected = self.process_order(order)?;
            risk_rejects += u64::from(rejected);
            emitted_trading_events += self.events.len() as u64;
            self.gateway.publish_trading(&self.events);
        }
        self.orders.clear();

        Ok(RuntimePoll {
            made_progress: control_poll.made_progress
                || gateway_poll.made_progress
                || processed_orders > 0,
            closed: control_poll.closed && gateway_poll.closed && self.orders.is_empty(),
            processed_orders,
            risk_rejects,
            emitted_trading_events,
        })
    }

    /// Return the runtime-owned matching engine.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Return the runtime-owned risk state.
    pub fn risk(&self) -> &RiskRuntime {
        &self.risk
    }

    /// Return the runtime-owned gateway.
    pub fn gateway(&self) -> &GatewayRuntime {
        &self.gateway
    }

    fn process_order(&mut self, entry: OrderEntry) -> Result<bool, RuntimeError> {
        self.events.clear();
        match self.risk.evaluate(entry, &self.engine) {
            RiskDecision::Accept => {
                self.engine.execute(
                    entry,
                    &mut self.sequencer,
                    &mut self.market_data,
                    &mut self.events,
                )?;
                self.risk.observe_events(&self.events);
                Ok(false)
            }
            RiskDecision::Reject(reason) => {
                self.reject(entry, reason)?;
                Ok(true)
            }
        }
    }

    fn reject(&mut self, entry: OrderEntry, reason: RejectReason) -> Result<(), RuntimeError> {
        let (client_order_id, account_id, remaining_qty) = match entry {
            OrderEntry::NewOrder(order) => {
                (order.client_order_id, order.account_id, order.qty.raw())
            }
            OrderEntry::CancelReplace(replace) => (
                replace.client_order_id,
                replace.account_id,
                replace.qty.raw(),
            ),
            OrderEntry::CancelOrder(cancel) => (cancel.client_order_id, cancel.account_id, 0),
            OrderEntry::MassCancel(cancel) => (cancel.client_order_id, cancel.account_id, 0),
        };

        let engine_seq = self
            .sequencer
            .next_engine_seq()
            .map_err(|_| RuntimeError::Engine(EngineError::SequenceOverflow))?;
        self.events
            .push(TradingEvent::ExecutionReport(ExecutionReport {
                engine_seq,
                client_order_id,
                account_id,
                exec_type: ExecType::Rejected,
                ord_status: OrdStatus::Rejected,
                reject_reason: reason,
                fill_price: None,
                fill_qty: 0,
                remaining_qty,
            }));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::trading::order::CancelReplace;
    use crate::domain::{
        Price, Qty, RecvTs,
        rd::InstrumentSpec,
        trading::order::{AccountId, ClientTs, NewOrder, OrderId, OrderType, Side, TimeInForce},
    };

    fn instrument() -> InstrumentSpec {
        InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument")
    }

    fn price(raw: u64) -> Price {
        Price::new(raw, &instrument()).expect("price")
    }

    fn qty(raw: u64) -> Qty {
        Qty::new(raw, &instrument()).expect("qty")
    }

    fn engine_config() -> EngineConfig {
        EngineConfig::new(instrument(), 100, 10_000, 1024, 128, 100, 16).expect("config")
    }

    fn risk_config(max_orders: u32, max_notional: u128) -> RiskConfig {
        RiskConfig::new(max_orders, max_notional, price(1_000), 100).expect("risk config")
    }

    #[allow(clippy::too_many_arguments)]
    fn new_order(
        id: u64,
        account: u32,
        side: Side,
        tif: TimeInForce,
        price_raw: u64,
        qty_raw: u64,
    ) -> OrderEntry {
        OrderEntry::NewOrder(NewOrder {
            client_order_id: OrderId::new(id).expect("order"),
            account_id: AccountId::new(account).expect("account"),
            client_ts: ClientTs::new(id),
            recv_ts: RecvTs::new(id),
            side,
            order_type: OrderType::Limit,
            time_in_force: tif,
            price: Some(price(price_raw)),
            qty: qty(qty_raw),
        })
    }

    fn replace(id: u64, account: u32, orig: u64, price_raw: u64, qty_raw: u64) -> OrderEntry {
        OrderEntry::CancelReplace(CancelReplace {
            client_order_id: OrderId::new(id).expect("order"),
            account_id: AccountId::new(account).expect("account"),
            orig_client_order_id: OrderId::new(orig).expect("orig"),
            client_ts: ClientTs::new(id),
            recv_ts: RecvTs::new(id),
            price: price(price_raw),
            qty: qty(qty_raw),
        })
    }

    fn runtime_with_orders(
        orders: impl IntoIterator<Item = OrderEntry>,
        risk_config: RiskConfig,
    ) -> Runtime {
        Runtime::new(
            engine_config(),
            risk_config,
            ControlRuntime,
            GatewayRuntime::from_orders(orders),
        )
        .expect("runtime")
    }

    fn reports(events: &[TradingEvent]) -> Vec<ExecutionReport> {
        events
            .iter()
            .filter_map(|event| match event {
                TradingEvent::ExecutionReport(report) => Some(*report),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn poll_once_processes_gateway_orders_fifo_through_engine() {
        let mut runtime = runtime_with_orders(
            [
                new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
                new_order(2, 2, Side::Buy, TimeInForce::Gtc, 1_001, 5),
            ],
            risk_config(10, 1_000_000),
        );

        let poll = runtime.poll_once().expect("poll");

        assert_eq!(poll.processed_orders, 2);
        assert_eq!(poll.risk_rejects, 0);
        assert_eq!(runtime.engine().best_bid(), Some((price(1_001), 5)));
        let published = runtime.gateway().published_trading();
        let reports = reports(published);
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].client_order_id.raw(), 1);
        assert_eq!(reports[1].client_order_id.raw(), 2);
    }

    #[test]
    fn risk_reject_uses_shared_engine_sequence_and_publishes() {
        let mut runtime = runtime_with_orders(
            [
                new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
                new_order(2, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
            ],
            risk_config(1, 1_000_000),
        );

        let poll = runtime.poll_once().expect("poll");
        let reports = reports(runtime.gateway().published_trading());

        assert_eq!(poll.processed_orders, 2);
        assert_eq!(poll.risk_rejects, 1);
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].engine_seq.raw(), 1);
        assert_eq!(reports[1].engine_seq.raw(), 2);
        assert_eq!(reports[1].reject_reason, RejectReason::RiskOpenOrderLimit);
        assert_eq!(runtime.engine().resting_order_count(), 1);
    }

    #[test]
    fn risk_reject_does_not_mutate_book() {
        let mut runtime = runtime_with_orders(
            [new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10)],
            risk_config(10, 9_999),
        );

        let poll = runtime.poll_once().expect("poll");

        assert_eq!(poll.risk_rejects, 1);
        assert_eq!(runtime.engine().resting_order_count(), 0);
        assert_eq!(
            runtime
                .engine()
                .account_open_notional(AccountId::new(1).expect("account")),
            Some(0)
        );
        assert_eq!(
            reports(runtime.gateway().published_trading())[0].reject_reason,
            RejectReason::RiskNotionalLimit
        );
    }

    #[test]
    fn duplicate_order_id_reaches_engine_structural_reject() {
        let mut runtime = runtime_with_orders(
            [
                new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
                new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
            ],
            risk_config(10, 1_000_000),
        );

        let poll = runtime.poll_once().expect("poll");
        let reports = reports(runtime.gateway().published_trading());

        assert_eq!(poll.processed_orders, 2);
        assert_eq!(poll.risk_rejects, 0);
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[1].client_order_id.raw(), 1);
        assert_eq!(reports[1].reject_reason, RejectReason::Malformed);
        assert_eq!(runtime.engine().resting_order_count(), 1);
    }

    #[test]
    fn unknown_replace_reaches_engine_unknown_order_reject() {
        let mut runtime =
            runtime_with_orders([replace(2, 1, 404, 1_000, 15)], risk_config(10, 1_000_000));

        let poll = runtime.poll_once().expect("poll");
        let reports = reports(runtime.gateway().published_trading());

        assert_eq!(poll.processed_orders, 1);
        assert_eq!(poll.risk_rejects, 0);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].client_order_id.raw(), 2);
        assert_eq!(reports[0].reject_reason, RejectReason::UnknownOrder);
        assert_eq!(runtime.engine().resting_order_count(), 0);
    }

    #[test]
    fn run_terminates_for_closed_control_and_finite_gateway() {
        let mut runtime = runtime_with_orders(
            [new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10)],
            risk_config(10, 1_000_000),
        );

        let summary = runtime.run().expect("run");

        assert_eq!(summary.iterations, 1);
        assert_eq!(summary.processed_orders, 1);
        assert_eq!(summary.risk_rejects, 0);
        assert_eq!(runtime.engine().resting_order_count(), 1);
    }

    #[test]
    fn empty_runtime_poll_is_closed_without_progress() {
        let mut runtime = runtime_with_orders([], risk_config(10, 1_000_000));

        let poll = runtime.poll_once().expect("poll");

        assert!(!poll.made_progress);
        assert!(poll.closed);
        assert_eq!(poll.processed_orders, 0);
    }
}
