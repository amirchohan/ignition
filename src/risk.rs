//! Risk boundary.
//!
//! Risk owns pre-match policy checks such as account limits, price bands, and
//! self-trade prevention. It does not mutate the book or emit protocol frames.

use crate::{
    domain::{
        DomainError, DomainResult, Price, RejectReason,
        trading::order::{AccountId, CancelReplace, NewOrder, OrderEntry, OrderType, TimeInForce},
    },
    engine::{Engine, OrderProjection, RestingOrderView},
};

/// Immutable risk settings validated at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RiskConfig {
    pub max_open_orders_per_account: u32,
    pub max_open_notional_per_account: u128,
    pub startup_reference_price: Price,
    pub price_band_bps: u32,
}

impl RiskConfig {
    /// Build validated risk settings.
    pub fn new(
        max_open_orders_per_account: u32,
        max_open_notional_per_account: u128,
        startup_reference_price: Price,
        price_band_bps: u32,
    ) -> DomainResult<Self> {
        if max_open_orders_per_account == 0 || max_open_notional_per_account == 0 || price_band_bps > 10_000 {
            return Err(DomainError::InvalidInstrumentSpec);
        }

        Ok(Self {
            max_open_orders_per_account,
            max_open_notional_per_account,
            startup_reference_price,
            price_band_bps,
        })
    }
}

/// Pre-match risk decision for one order-entry command.
///
/// `Accept` means risk does not block the command. It does not mean the command
/// is structurally valid; the engine still owns duplicate IDs, unknown orders,
/// invalid accounts, storage-envelope checks, and final order lifecycle rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskDecision {
    Accept,
    Reject(RejectReason),
}

/// Deterministic risk runtime state.
///
/// This component owns only policy checks that can reject before matching:
/// price bands, self-trade prevention, open-order limits, and open-notional
/// limits. It reads book-derived state through `Engine` views but never mutates
/// the book and never emits execution reports directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RiskRuntime {
    config: RiskConfig,
    reference_price: Price,
}

impl RiskRuntime {
    /// Create a risk runtime with its configured startup reference price.
    pub fn new(config: RiskConfig) -> Self {
        Self {
            reference_price: config.startup_reference_price,
            config,
        }
    }

    /// Return the current price-band reference.
    pub const fn reference_price(&self) -> Price {
        self.reference_price
    }

    /// Evaluate one order-entry command against current book-derived state.
    ///
    /// Risk rejects only when it can prove a risk-policy violation. Commands
    /// outside risk's ownership are accepted through to the engine so the
    /// engine can apply structural validation and matching semantics.
    pub fn evaluate(&self, entry: OrderEntry, engine: &Engine) -> RiskDecision {
        match entry {
            OrderEntry::NewOrder(order) => self.evaluate_new(order, engine),
            OrderEntry::CancelReplace(replace) => self.evaluate_replace(replace, engine),
            OrderEntry::CancelOrder(_) | OrderEntry::MassCancel(_) => RiskDecision::Accept,
        }
    }

    /// Observe the latest deterministic trade price emitted by market data.
    pub fn observe_trade_price(&mut self, price: Price) {
        self.reference_price = price;
    }

    /// Evaluate risk policy for a new order.
    ///
    /// Duplicate order IDs, invalid accounts, missing limit prices, and
    /// storage-envelope violations are deliberately not checked here; the
    /// engine owns those structural rejects after risk passes the command on.
    fn evaluate_new(&self, order: NewOrder, engine: &Engine) -> RiskDecision {
        // Submitted limit prices are checked directly against the active band.
        // Market orders have no submitted price, so they are checked through
        // projected executable prices below.
        if let Some(reason) = self.check_limit_price(order.order_type, order.price) {
            return RiskDecision::Reject(reason);
        }

        // The projection is read-only: it walks visible executable liquidity to
        // determine STP, executable price-band exposure, and any quantity that
        // would remain resting after matching.
        let projection = engine.project_order(order.account_id, order.side, order.price, order.qty.raw());
        if let Some(reason) = self.check_projection(order.time_in_force, projection) {
            return RiskDecision::Reject(reason);
        }

        // Open-order and open-notional limits apply only to projected resting
        // residual. IOC and market residuals expire and therefore add no open
        // exposure.
        let residual_qty = resting_residual(order.order_type, order.time_in_force, projection);
        self.check_account_limits(engine, order.account_id, None, order.price, residual_qty)
    }

    /// Evaluate risk policy for cancel-replace.
    ///
    /// Risk needs the current resting order to know the replacement side and to
    /// subtract old exposure from the projected account state. If the original
    /// order is missing or belongs to another account, risk does not reject;
    /// the engine will produce the correct `UnknownOrder` lifecycle report.
    fn evaluate_replace(&self, replace: CancelReplace, engine: &Engine) -> RiskDecision {
        let Some(current) = engine.resting_order(replace.account_id, replace.orig_client_order_id) else {
            return RiskDecision::Accept;
        };
        // Cancel-replace always submits a limit replacement price, so the new
        // price itself must be inside the band before projection.
        if let Some(reason) = self.check_limit_price(OrderType::Limit, Some(replace.price)) {
            return RiskDecision::Reject(reason);
        }

        // Project the replacement as a fresh GTC limit order after removing the
        // original exposure in `check_account_limits`.
        let projection = engine.project_order(replace.account_id, current.side, Some(replace.price), replace.qty.raw());
        if let Some(reason) = self.check_projection(TimeInForce::Gtc, projection) {
            return RiskDecision::Reject(reason);
        }

        self.check_account_limits(
            engine,
            replace.account_id,
            Some(current),
            Some(replace.price),
            projection.residual_qty,
        )
    }

    /// Check the submitted limit price against the configured/latest reference band.
    ///
    /// This does not validate whether a required limit price exists; protocol
    /// normalization and engine structural checks own malformed order shape.
    fn check_limit_price(&self, order_type: OrderType, price: Option<Price>) -> Option<RejectReason> {
        if order_type == OrderType::Limit && price.is_some_and(|price| !self.price_in_band(price)) {
            return Some(RejectReason::PriceBand);
        }
        None
    }

    /// Check read-only projected executions for STP and executable price-band breaches.
    ///
    /// STP is cancel-aggressive: if any executable maker belongs to the taker
    /// account, risk rejects the incoming command before the book mutates.
    fn check_projection(&self, time_in_force: TimeInForce, projection: OrderProjection) -> Option<RejectReason> {
        // Post-only orders that would cross are canceled by the engine without
        // execution, so they do not create a self-trade risk event here.
        if time_in_force != TimeInForce::PostOnly && projection.self_trade {
            return Some(RejectReason::SelfTradePrevention);
        }
        // The projection records the min/max executable prices. Checking both
        // endpoints is enough because executable price levels are monotonic as
        // the aggressive order walks the opposite side.
        if projection.low_execution_price.is_some_and(|price| !self.price_in_band(price))
            || projection.high_execution_price.is_some_and(|price| !self.price_in_band(price))
        {
            return Some(RejectReason::PriceBand);
        }
        None
    }

    /// Check projected per-account open-order count and raw open notional.
    ///
    /// Account aggregates are book-derived state. If the engine has no account
    /// state for the ID, risk does not invent a structural rejection; the engine
    /// remains responsible for invalid-account handling.
    fn check_account_limits(
        &self,
        engine: &Engine,
        account_id: AccountId,
        replaced_order: Option<RestingOrderView>,
        residual_price: Option<Price>,
        residual_qty: u64,
    ) -> RiskDecision {
        let Some(open_count) = engine.account_open_order_count(account_id) else {
            return RiskDecision::Accept;
        };
        let Some(open_notional) = engine.account_open_notional(account_id) else {
            return RiskDecision::Accept;
        };
        // For replace, subtract the current resting order first, then add only
        // the projected residual that would rest after matching.
        let removed_count = u32::from(replaced_order.is_some());
        let removed_notional = replaced_order.map_or(0, |order| raw_notional(order.price, order.remaining_qty));
        let added_count = u32::from(residual_qty > 0);
        let added_notional = residual_price.map_or(0, |price| raw_notional(price, residual_qty));

        let projected_count = open_count - removed_count + added_count;
        if projected_count > self.config.max_open_orders_per_account {
            return RiskDecision::Reject(RejectReason::RiskOpenOrderLimit);
        }

        let projected_notional = open_notional - removed_notional + added_notional;
        if projected_notional > self.config.max_open_notional_per_account {
            return RiskDecision::Reject(RejectReason::RiskNotionalLimit);
        }

        RiskDecision::Accept
    }

    /// Return whether `price` is inside the inclusive reference-price band.
    fn price_in_band(&self, price: Price) -> bool {
        let reference = u128::from(self.reference_price.raw());
        let band = u128::from(self.config.price_band_bps);
        let lower = reference * (10_000 - band) / 10_000;
        let upper = reference * (10_000 + band) / 10_000;
        u128::from(price.raw()) >= lower && u128::from(price.raw()) <= upper
    }
}

fn resting_residual(order_type: OrderType, time_in_force: TimeInForce, projection: OrderProjection) -> u64 {
    match (order_type, time_in_force) {
        (OrderType::Limit, TimeInForce::Gtc) => projection.residual_qty,
        (OrderType::Limit, TimeInForce::PostOnly) if projection.executable_qty == 0 => projection.residual_qty,
        _ => 0,
    }
}

fn raw_notional(price: Price, qty: u64) -> u128 {
    u128::from(price.raw()) * u128::from(qty)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            Qty, RecvTs, Sequencer,
            rd::InstrumentSpec,
            trading::order::{ClientTs, ExecutionReport, OrderId, Side},
        },
        engine::{Engine, EngineConfig},
        market_data::{MarketDataFeedConfig, MarketDataRuntime},
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
        order_type: OrderType,
        tif: TimeInForce,
        price_raw: Option<u64>,
        qty_raw: u64,
    ) -> NewOrder {
        NewOrder {
            client_order_id: OrderId::new(id).expect("order"),
            account_id: AccountId::new(account).expect("account"),
            client_ts: ClientTs::new(id),
            recv_ts: RecvTs::new(id),
            side,
            order_type,
            time_in_force: tif,
            price: price_raw.map(price),
            qty: qty(qty_raw),
        }
    }

    fn replace(id: u64, account: u32, orig: u64, price_raw: u64, qty_raw: u64) -> CancelReplace {
        CancelReplace {
            client_order_id: OrderId::new(id).expect("order"),
            account_id: AccountId::new(account).expect("account"),
            orig_client_order_id: OrderId::new(orig).expect("orig"),
            client_ts: ClientTs::new(id),
            recv_ts: RecvTs::new(id),
            price: price(price_raw),
            qty: qty(qty_raw),
        }
    }

    fn execute(engine: &mut Engine, entry: OrderEntry) -> Option<Vec<ExecutionReport>> {
        let mut sequencer = Sequencer::default();
        let mut market_data = match MarketDataRuntime::new(MarketDataFeedConfig::default()) {
            Ok(market_data) => market_data,
            Err(crate::market_data::MarketDataError::Io(crate::io::IoError::Bind(error)))
                if error.kind() == std::io::ErrorKind::PermissionDenied =>
            {
                return None;
            }
            Err(error) => panic!("market-data: {error:?}"),
        };
        let mut reports = Vec::new();
        let mut emit_report = |report| reports.push(report);
        engine
            .execute(entry, &mut sequencer, &mut market_data, &mut emit_report)
            .expect("execute");
        Some(reports)
    }

    #[test]
    fn open_order_limit_rejects_projected_resting_order() {
        let mut engine = Engine::new(engine_config()).expect("engine");
        let Some(_) = execute(
            &mut engine,
            OrderEntry::NewOrder(new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1_000), 10)),
        ) else {
            return;
        };

        let risk = RiskRuntime::new(risk_config(1, 1_000_000));
        let decision = risk.evaluate(
            OrderEntry::NewOrder(new_order(2, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1_000), 10)),
            &engine,
        );

        assert_eq!(decision, RiskDecision::Reject(RejectReason::RiskOpenOrderLimit));
    }

    #[test]
    fn notional_limit_rejects_projected_open_exposure() {
        let engine = Engine::new(engine_config()).expect("engine");
        let risk = RiskRuntime::new(risk_config(10, 9_999));
        let decision = risk.evaluate(
            OrderEntry::NewOrder(new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1_000), 10)),
            &engine,
        );

        assert_eq!(decision, RiskDecision::Reject(RejectReason::RiskNotionalLimit));
    }

    #[test]
    fn price_band_rejects_limit_price_outside_reference_band() {
        let engine = Engine::new(engine_config()).expect("engine");
        let risk = RiskRuntime::new(risk_config(10, 1_000_000));
        let decision = risk.evaluate(
            OrderEntry::NewOrder(new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1_011), 10)),
            &engine,
        );

        assert_eq!(decision, RiskDecision::Reject(RejectReason::PriceBand));
    }

    #[test]
    fn stp_cancel_aggressive_rejects_before_matching() {
        let mut engine = Engine::new(engine_config()).expect("engine");
        let Some(_) = execute(
            &mut engine,
            OrderEntry::NewOrder(new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1_000), 10)),
        ) else {
            return;
        };

        let risk = RiskRuntime::new(risk_config(10, 1_000_000));
        let decision = risk.evaluate(
            OrderEntry::NewOrder(new_order(2, 1, Side::Buy, OrderType::Limit, TimeInForce::Ioc, Some(1_000), 10)),
            &engine,
        );

        assert_eq!(decision, RiskDecision::Reject(RejectReason::SelfTradePrevention));
        assert_eq!(engine.resting_order_count(), 1);
    }

    #[test]
    fn replace_projection_removes_original_order_exposure() {
        let mut engine = Engine::new(engine_config()).expect("engine");
        let Some(_) = execute(
            &mut engine,
            OrderEntry::NewOrder(new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1_000), 10)),
        ) else {
            return;
        };

        let risk = RiskRuntime::new(risk_config(1, 15_000));
        let decision = risk.evaluate(OrderEntry::CancelReplace(replace(2, 1, 1, 1_000, 15)), &engine);

        assert_eq!(decision, RiskDecision::Accept);
    }

    #[test]
    fn duplicate_new_order_id_is_not_a_risk_reject() {
        let mut engine = Engine::new(engine_config()).expect("engine");
        let Some(_) = execute(
            &mut engine,
            OrderEntry::NewOrder(new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1_000), 10)),
        ) else {
            return;
        };

        let risk = RiskRuntime::new(risk_config(10, 1_000_000));
        let decision = risk.evaluate(
            OrderEntry::NewOrder(new_order(1, 1, Side::Buy, OrderType::Limit, TimeInForce::Gtc, Some(1_000), 10)),
            &engine,
        );

        assert_eq!(decision, RiskDecision::Accept);
    }

    #[test]
    fn unknown_replace_original_is_not_a_risk_reject() {
        let engine = Engine::new(engine_config()).expect("engine");
        let risk = RiskRuntime::new(risk_config(10, 1_000_000));
        let decision = risk.evaluate(OrderEntry::CancelReplace(replace(2, 1, 404, 1_000, 15)), &engine);

        assert_eq!(decision, RiskDecision::Accept);
    }

    #[test]
    fn observes_trade_price_as_new_reference_price() {
        let mut risk = RiskRuntime::new(risk_config(10, 1_000_000));

        risk.observe_trade_price(price(1_010));

        assert_eq!(risk.reference_price(), price(1_010));
    }
}
