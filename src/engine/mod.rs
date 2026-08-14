//! Deterministic matching-engine boundary.
//!
//! The engine owns order-book state, price-time priority mutations, and
//! matching behavior. It accepts protocol-agnostic domain commands and emits
//! typed order-entry and market-data outputs; gateway and protocol code stay
//! outside this module.

use std::collections::HashMap;

mod accounts;
mod ladder;
mod orders;

use crate::domain::{
    DomainError, DomainResult, Price, RejectReason, Sequencer,
    rd::InstrumentSpec,
    trading::order::{
        AccountId, CancelOrder, CancelReplace, ExecType, ExecutionReport, MassCancel, NewOrder, OrdStatus, OrderEntry, OrderId, OrderType,
        Side, TimeInForce,
    },
};
use crate::market_data::MarketDataRuntime;

use self::{
    accounts::AccountTable,
    ladder::SideLadder,
    orders::{OrderArena, OrderHandle, RestingOrder},
};

/// Single-symbol deterministic matching engine.
#[derive(Debug)]
pub struct Engine {
    /// Validated engine configuration.
    config: EngineConfig,
    /// Buy side ladder.
    bids: SideLadder,
    /// Sell side ladder.
    asks: SideLadder,
    /// Fixed-capacity arena for resting orders.
    orders: OrderArena,
    /// Client order ID lookup into arena handles.
    order_index: HashMap<OrderId, OrderHandle>,
    /// Dense account table for active-order chains.
    accounts: AccountTable,
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

fn engine_error(error: DomainError) -> EngineError {
    match error {
        DomainError::SequenceOverflow => EngineError::SequenceOverflow,
        _ => EngineError::InvalidConfig,
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct MatchOutcome {
    /// Total taker quantity filled during the aggressive walk.
    filled_qty: u64,
    /// Last taker report is held until the caller knows whether residual expires.
    pending_taker_report: Option<ReportSpec>,
}

impl Engine {
    /// Create an empty deterministic matching engine with preallocated state.
    pub fn new(config: EngineConfig) -> Result<Self, EngineError> {
        Ok(Self {
            config,
            bids: SideLadder::new(Side::Buy, config).map_err(|_| EngineError::InvalidConfig)?,
            asks: SideLadder::new(Side::Sell, config).map_err(|_| EngineError::InvalidConfig)?,
            orders: OrderArena::with_capacity(config.max_resting_orders()),
            order_index: std::collections::HashMap::with_capacity(config.max_resting_orders() as usize),
            accounts: AccountTable::new(config.max_account_id()),
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
        let result = match entry {
            OrderEntry::NewOrder(order) => self.on_new_order(order, sequencer, market_data, emit_report),
            OrderEntry::CancelOrder(cancel) => self.on_cancel_order(cancel, sequencer, market_data, emit_report),
            OrderEntry::CancelReplace(replace) => self.on_cancel_replace(replace, sequencer, market_data, emit_report),
            OrderEntry::MassCancel(cancel) => self.on_mass_cancel(cancel, sequencer, market_data, emit_report),
        };
        result.map_err(engine_error)
    }

    /// Return the current best bid price and aggregate quantity.
    pub fn best_bid(&self) -> Option<(Price, u64)> {
        self.bids.top()
    }

    /// Return the current best ask price and aggregate quantity.
    pub fn best_ask(&self) -> Option<(Price, u64)> {
        self.asks.top()
    }

    /// Return the number of currently resting orders.
    pub fn resting_order_count(&self) -> usize {
        self.orders.active()
    }

    /// Return whether this dense account ID is valid for this engine instance.
    pub fn account_exists(&self, account_id: AccountId) -> bool {
        self.accounts.contains(account_id)
    }

    /// Return the number of currently resting orders for one account.
    pub fn account_open_order_count(&self, account_id: AccountId) -> Option<u32> {
        self.accounts.get(account_id).map(|account| account.open_order_count)
    }

    /// Return raw open resting notional for one account.
    pub fn account_open_notional(&self, account_id: AccountId) -> Option<u128> {
        self.accounts.get(account_id).map(|account| account.open_notional)
    }

    /// Return a read-only view of one resting order for risk projection.
    pub fn resting_order(&self, account_id: AccountId, order_id: OrderId) -> Option<RestingOrderView> {
        let handle = self.order_index.get(&order_id).copied()?;
        let order = self.orders.get(handle).copied()?;
        (order.account_id == account_id).then_some(RestingOrderView {
            order_id: order.order_id,
            account_id: order.account_id,
            side: order.side,
            price: order.price,
            remaining_qty: order.remaining_qty,
        })
    }

    /// Project an incoming order against visible opposite liquidity without mutation.
    pub fn project_order(&self, account_id: AccountId, side: Side, limit_price: Option<Price>, qty: u64) -> OrderProjection {
        let mut remaining_qty = qty;
        let mut executable_qty = 0;
        let mut low_execution_price = None;
        let mut high_execution_price = None;
        let mut self_trade = false;
        let opposite_side = best_side(side);
        let mut cursor = self.side(opposite_side).best_level().map(|(key, _)| key);

        while remaining_qty > 0 {
            let Some(key) = cursor else {
                break;
            };
            if !Self::is_executable(side, limit_price, key.price) {
                break;
            }

            if let Some(level) = self.side(opposite_side).get_level(key) {
                let mut current = level.head;
                while remaining_qty > 0 {
                    let Some(handle) = current else {
                        break;
                    };
                    let Some(order) = self.orders.get(handle).copied() else {
                        break;
                    };
                    let fill_qty = remaining_qty.min(order.remaining_qty);
                    remaining_qty -= fill_qty;
                    executable_qty += fill_qty;
                    low_execution_price = Some(low_execution_price.map_or(order.price, |price: Price| price.min(order.price)));
                    high_execution_price = Some(high_execution_price.map_or(order.price, |price: Price| price.max(order.price)));
                    self_trade |= order.account_id == account_id;
                    current = order.level_next;
                }
            }

            cursor = self.next_occupied_key(opposite_side, key);
        }

        OrderProjection {
            executable_qty,
            residual_qty: remaining_qty,
            low_execution_price,
            high_execution_price,
            self_trade,
        }
    }

    /// Visit resting orders in deterministic book order.
    pub fn try_for_each_resting_order<E>(&self, mut visit: impl FnMut(RestingOrderView) -> Result<(), E>) -> Result<(), E> {
        self.try_for_each_side_order(Side::Buy, &mut visit)?;
        self.try_for_each_side_order(Side::Sell, &mut visit)
    }

    /// Cancel every resting order in deterministic book order.
    pub fn drain_all(
        &mut self,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> Result<(), EngineError> {
        self.drain_all_inner(sequencer, market_data, emit_report).map_err(engine_error)
    }

    fn drain_all_inner(
        &mut self,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> DomainResult<()> {
        let mut handles = Vec::with_capacity(self.resting_order_count());
        self.collect_side_handles(Side::Buy, &mut handles);
        self.collect_side_handles(Side::Sell, &mut handles);

        for handle in handles {
            let Some(removed) = self.unlink_and_free(handle) else {
                continue;
            };
            let level_qty = self.current_level_qty(removed.side, removed.price);
            self.publish_book_update(removed.side, removed.price, level_qty, sequencer, market_data)?;
            self.push_report(
                sequencer,
                emit_report,
                ReportSpec {
                    client_order_id: removed.order_id,
                    account_id: removed.account_id,
                    exec_type: ExecType::Canceled,
                    ord_status: OrdStatus::Canceled,
                    reject_reason: RejectReason::None,
                    fill_price: None,
                    fill_qty: 0,
                    remaining_qty: removed.remaining_qty,
                },
            )?;
        }

        Ok(())
    }

    /// Handle a new order, including matching and optional resting.
    fn on_new_order(
        &mut self,
        order: NewOrder,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> DomainResult<()> {
        if !self.accounts.contains(order.account_id) {
            // Account IDs outside the configured dense table are structural rejects.
            return self.reject_new(order, RejectReason::Malformed, sequencer, emit_report);
        }
        if self.order_index.contains_key(&order.client_order_id) {
            // Client order IDs must be unique while an order is live.
            return self.reject_new(order, RejectReason::Malformed, sequencer, emit_report);
        }

        // ensure valid price
        if matches!(order.order_type, OrderType::Limit) {
            let Some(price) = order.price else {
                return self.reject_new(order, RejectReason::InvalidPrice, sequencer, emit_report);
            };
            if !self.price_in_storage(price) {
                return self.reject_new(order, RejectReason::InvalidPrice, sequencer, emit_report);
            }
        }

        // if it's a PostOnly order ensure that it doesn't cross the book
        if order.time_in_force == TimeInForce::PostOnly && self.would_cross(order.side, order.price) {
            self.push_report(
                sequencer,
                emit_report,
                ReportSpec {
                    client_order_id: order.client_order_id,
                    account_id: order.account_id,
                    exec_type: ExecType::Canceled,
                    ord_status: OrdStatus::Canceled,
                    reject_reason: RejectReason::None,
                    fill_price: None,
                    fill_qty: 0,
                    remaining_qty: order.qty.raw(),
                },
            )?;
            return Ok(());
        }

        // execute as much of the order as possible
        let mut remaining_qty = order.qty.raw();
        let match_outcome = self.match_incoming(
            order.client_order_id,
            order.account_id,
            order.side,
            order.price,
            &mut remaining_qty,
            sequencer,
            market_data,
            emit_report,
        )?;

        // order completely filled - happy path
        if remaining_qty == 0 {
            self.emit_pending_taker_report(sequencer, emit_report, match_outcome.pending_taker_report)?;
            return Ok(());
        }

        match (order.order_type, order.time_in_force) {
            (OrderType::Market, _) | (_, TimeInForce::Ioc) => {
                // market orders and IOC residuals never rest on the book.
                if match_outcome.filled_qty > 0 {
                    return self.expire_taker_residual(sequencer, emit_report, match_outcome);
                }

                // nothing filled and the order cannot rest, so it expires immediately.
                self.push_report(
                    sequencer,
                    emit_report,
                    ReportSpec {
                        client_order_id: order.client_order_id,
                        account_id: order.account_id,
                        exec_type: ExecType::Expired,
                        ord_status: OrdStatus::Expired,
                        reject_reason: RejectReason::None,
                        fill_price: None,
                        fill_qty: 0,
                        remaining_qty: 0,
                    },
                )
            }
            (OrderType::Limit, TimeInForce::Gtc | TimeInForce::PostOnly) => {
                // rest any unfilled passive quantity after the aggressive walk.
                let price = order.price.ok_or(DomainError::InvalidPrice)?;
                let level_qty = self.rest_order(order.client_order_id, order.account_id, order.side, price, remaining_qty)?;
                self.publish_book_update(order.side, price, level_qty, sequencer, market_data)?;
                // distinguish a pure add from an order that traded before resting.
                if match_outcome.filled_qty == 0 {
                    self.push_report(
                        sequencer,
                        emit_report,
                        ReportSpec {
                            client_order_id: order.client_order_id,
                            account_id: order.account_id,
                            exec_type: ExecType::New,
                            ord_status: OrdStatus::New,
                            reject_reason: RejectReason::None,
                            fill_price: None,
                            fill_qty: 0,
                            remaining_qty,
                        },
                    )
                } else {
                    self.emit_pending_taker_report(sequencer, emit_report, match_outcome.pending_taker_report)
                }
            }
        }
    }

    /// Handle an order cancel by unlinking the original resting order.
    fn on_cancel_order(
        &mut self,
        cancel: CancelOrder,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> DomainResult<()> {
        let Some(handle) = self.order_index.get(&cancel.orig_client_order_id).copied() else {
            return self.reject_cancel(cancel, RejectReason::UnknownOrder, sequencer, emit_report);
        };
        if self.orders.get(handle).map(|order| order.account_id) != Some(cancel.account_id) {
            return self.reject_cancel(cancel, RejectReason::UnknownOrder, sequencer, emit_report);
        }

        let removed = self.unlink_and_free(handle);
        if let Some(order) = removed {
            let level_qty = self.current_level_qty(order.side, order.price);
            self.publish_book_update(order.side, order.price, level_qty, sequencer, market_data)?;
        }
        self.push_report(
            sequencer,
            emit_report,
            ReportSpec {
                client_order_id: cancel.orig_client_order_id,
                account_id: cancel.account_id,
                exec_type: ExecType::Canceled,
                ord_status: OrdStatus::Canceled,
                reject_reason: RejectReason::None,
                fill_price: None,
                fill_qty: 0,
                remaining_qty: removed.map_or(0, |order| order.remaining_qty),
            },
        )
    }

    /// Handle cancel-replace with exchange-style priority rules.
    fn on_cancel_replace(
        &mut self,
        replace: CancelReplace,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> DomainResult<()> {
        if !self.price_in_storage(replace.price) {
            return self.reject_replace(replace, RejectReason::InvalidPrice, sequencer, emit_report);
        }
        if self.order_index.contains_key(&replace.client_order_id) && replace.client_order_id != replace.orig_client_order_id {
            return self.reject_replace(replace, RejectReason::Malformed, sequencer, emit_report);
        }

        let Some(handle) = self.order_index.get(&replace.orig_client_order_id).copied() else {
            return self.reject_replace(replace, RejectReason::UnknownOrder, sequencer, emit_report);
        };
        let Some(current) = self.orders.get(handle).copied() else {
            return self.reject_replace(replace, RejectReason::UnknownOrder, sequencer, emit_report);
        };
        if current.account_id != replace.account_id {
            return self.reject_replace(replace, RejectReason::UnknownOrder, sequencer, emit_report);
        }

        if current.price == replace.price && replace.qty.raw() <= current.remaining_qty {
            // Same-price reductions stay in the existing FIFO position.
            self.preserve_priority_replace(handle, replace, current.remaining_qty)?;
            if replace.qty.raw() != current.remaining_qty {
                self.publish_book_update(current.side, current.price, replace.qty.raw(), sequencer, market_data)?;
            }
            self.push_report(
                sequencer,
                emit_report,
                ReportSpec {
                    client_order_id: replace.client_order_id,
                    account_id: replace.account_id,
                    exec_type: ExecType::Replaced,
                    ord_status: OrdStatus::Replaced,
                    reject_reason: RejectReason::None,
                    fill_price: None,
                    fill_qty: 0,
                    remaining_qty: replace.qty.raw(),
                },
            )?;
            return Ok(());
        }

        // Price changes and quantity increases lose priority and re-enter as new.
        if let Some(removed) = self.unlink_and_free(handle) {
            let level_qty = self.current_level_qty(removed.side, removed.price);
            self.publish_book_update(removed.side, removed.price, level_qty, sequencer, market_data)?;
        }
        self.push_report(
            sequencer,
            emit_report,
            ReportSpec {
                client_order_id: replace.orig_client_order_id,
                account_id: replace.account_id,
                exec_type: ExecType::Canceled,
                ord_status: OrdStatus::Canceled,
                reject_reason: RejectReason::None,
                fill_price: None,
                fill_qty: 0,
                remaining_qty: 0,
            },
        )?;

        self.on_new_order(
            NewOrder {
                client_order_id: replace.client_order_id,
                account_id: replace.account_id,
                client_ts: replace.client_ts,
                recv_ts: replace.recv_ts,
                side: current.side,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
                price: Some(replace.price),
                qty: replace.qty,
            },
            sequencer,
            market_data,
            emit_report,
        )
    }

    /// Cancel all active orders for one account.
    fn on_mass_cancel(
        &mut self,
        cancel: MassCancel,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> DomainResult<()> {
        if !self.accounts.contains(cancel.account_id) {
            return self.push_report(
                sequencer,
                emit_report,
                ReportSpec::new_reject(cancel.client_order_id, cancel.account_id, RejectReason::Malformed, 0),
            );
        }

        let mut current = self.accounts.get(cancel.account_id).and_then(|account| account.head);
        while let Some(handle) = current {
            // Capture the next account link before unlinking frees this slot.
            current = self.orders.get(handle).and_then(|order| order.account_next);
            if let Some(removed) = self.unlink_and_free(handle) {
                let level_qty = self.current_level_qty(removed.side, removed.price);
                self.publish_book_update(removed.side, removed.price, level_qty, sequencer, market_data)?;
                self.push_report(
                    sequencer,
                    emit_report,
                    ReportSpec {
                        client_order_id: removed.order_id,
                        account_id: removed.account_id,
                        exec_type: ExecType::Canceled,
                        ord_status: OrdStatus::Canceled,
                        reject_reason: RejectReason::None,
                        fill_price: None,
                        fill_qty: 0,
                        remaining_qty: removed.remaining_qty,
                    },
                )?;
            }
        }

        self.push_report(
            sequencer,
            emit_report,
            ReportSpec {
                client_order_id: cancel.client_order_id,
                account_id: cancel.account_id,
                exec_type: ExecType::Canceled,
                ord_status: OrdStatus::Canceled,
                reject_reason: RejectReason::None,
                fill_price: None,
                fill_qty: 0,
                remaining_qty: 0,
            },
        )
    }

    /// Match an aggressive order against the opposite best levels.
    #[allow(clippy::too_many_arguments)]
    fn match_incoming(
        &mut self,
        taker_order_id: OrderId,
        taker_account_id: AccountId,
        side: Side,
        limit_price: Option<Price>,
        remaining_qty: &mut u64,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> DomainResult<MatchOutcome> {
        let mut outcome = MatchOutcome::default();
        while *remaining_qty > 0 {
            // stop when the opposite side has no visible liquidity left.
            let Some((best_key, level_head)) = self.best_opposite(side) else {
                break;
            };
            // limit orders can only trade at prices at or better than their limit.
            if !Self::is_executable(side, limit_price, best_key.price) {
                break;
            }

            // empty or stale level heads should not persist in the best-price bitmap.
            let Some(maker_handle) = level_head else {
                self.side_mut(best_side(side)).mark_empty(best_key);
                continue;
            };
            let Some(maker) = self.orders.get(maker_handle).copied() else {
                self.side_mut(best_side(side)).mark_empty(best_key);
                continue;
            };

            self.emit_pending_taker_report(sequencer, emit_report, outcome.pending_taker_report.take())?;
            // one execution trades the smaller of taker leaves quantity and maker leaves quantity.
            let fill_qty = (*remaining_qty).min(maker.remaining_qty);
            *remaining_qty -= fill_qty;
            outcome.filled_qty += fill_qty;
            // apply book state first so all following events describe post-fill state.
            self.apply_fill(maker_handle, best_key, fill_qty)?;
            let maker_remaining = self.orders.get(maker_handle).map_or(0, |order| order.remaining_qty);
            if maker_remaining == 0 {
                // fully filled makers leave both intrusive lists and the ID index now.
                self.unlink_and_free(maker_handle);
            }

            // market-data output is sequenced immediately after the deterministic state change.
            market_data.publish_trade(
                sequencer,
                maker.order_id,
                taker_order_id,
                maker.price,
                fill_qty,
                &self.config.instrument(),
            )?;
            let level_qty = self.current_level_qty(maker.side, maker.price);
            self.publish_book_update(maker.side, maker.price, level_qty, sequencer, market_data)?;

            // maker report reflects this fill and the maker's remaining book quantity.
            self.push_report(
                sequencer,
                emit_report,
                ReportSpec {
                    client_order_id: maker.order_id,
                    account_id: maker.account_id,
                    exec_type: ExecType::Trade,
                    ord_status: if maker_remaining == 0 {
                        OrdStatus::Filled
                    } else {
                        OrdStatus::PartiallyFilled
                    },
                    reject_reason: RejectReason::None,
                    fill_price: Some(maker.price),
                    fill_qty,
                    remaining_qty: maker_remaining,
                },
            )?;

            // taker gets a separate execution report for the same fill price and quantity.
            outcome.pending_taker_report = Some(ReportSpec {
                client_order_id: taker_order_id,
                account_id: taker_account_id,
                exec_type: ExecType::Trade,
                ord_status: if *remaining_qty == 0 {
                    OrdStatus::Filled
                } else {
                    OrdStatus::PartiallyFilled
                },
                reject_reason: RejectReason::None,
                fill_price: Some(maker.price),
                fill_qty,
                remaining_qty: *remaining_qty,
            });
        }

        Ok(outcome)
    }

    /// Mark the final taker fill report terminal when residual liquidity expires.
    fn expire_taker_residual(
        &mut self,
        sequencer: &mut Sequencer,
        emit_report: &mut impl FnMut(ExecutionReport),
        outcome: MatchOutcome,
    ) -> DomainResult<()> {
        let Some(mut spec) = outcome.pending_taker_report else {
            return Ok(());
        };
        spec.ord_status = OrdStatus::Expired;
        spec.remaining_qty = 0;
        self.push_report(sequencer, emit_report, spec)
    }

    fn emit_pending_taker_report(
        &mut self,
        sequencer: &mut Sequencer,
        emit_report: &mut impl FnMut(ExecutionReport),
        spec: Option<ReportSpec>,
    ) -> DomainResult<()> {
        let Some(spec) = spec else {
            return Ok(());
        };
        self.push_report(sequencer, emit_report, spec)
    }

    /// Apply a fill to one maker order and its price-level aggregate.
    fn apply_fill(&mut self, maker_handle: OrderHandle, key: ladder::LevelKey, fill_qty: u64) -> DomainResult<()> {
        let filled_notional = self.orders.get(maker_handle).map_or(0, |order| raw_notional(order.price, fill_qty));
        if let Some(order) = self.orders.get_mut(maker_handle) {
            order.remaining_qty -= fill_qty;
        }
        let side = self.orders.get(maker_handle).map_or(Side::Sell, |order| order.side);
        if let Some(order) = self.orders.get(maker_handle).copied()
            && let Some(account) = self.accounts.get_mut(order.account_id)
        {
            account.open_notional -= filled_notional;
        }
        let level = self.side_mut(side).get_level_mut_or_activate(key)?;
        level.total_qty -= fill_qty;
        Ok(())
    }

    /// Insert an order into the resting indexes.
    fn rest_order(&mut self, order_id: OrderId, account_id: AccountId, side: Side, price: Price, remaining_qty: u64) -> DomainResult<u64> {
        let key = self.side(side).key_for_price(price)?;
        let order = RestingOrder {
            order_id,
            account_id,
            side,
            price,
            remaining_qty,
            level_prev: None,
            level_next: None,
            account_prev: None,
            account_next: None,
        };
        let handle = self.orders.allocate(order).ok_or(DomainError::InvalidInstrumentSpec)?;

        // a resting order is visible through price, account, and client-order indexes.
        let level_qty = self.append_to_level(side, key, handle)?;
        self.append_to_account(account_id, handle)?;
        self.order_index.insert(order_id, handle);
        Ok(level_qty)
    }

    /// Append an order to the FIFO chain at a price level.
    fn append_to_level(&mut self, side: Side, key: ladder::LevelKey, handle: OrderHandle) -> DomainResult<u64> {
        let tail = {
            let level = self.side_mut(side).get_level_mut_or_activate(key)?;
            level.tail
        };

        // append at the tail to preserve FIFO within the price level.
        if let Some(tail) = tail {
            self.orders.get_mut(tail).expect("tail").level_next = Some(handle);
            self.orders.get_mut(handle).expect("order").level_prev = Some(tail);
        }

        let remaining_qty = self.orders.get(handle).expect("order").remaining_qty;
        let level = self.side_mut(side).get_level_mut_or_activate(key)?;
        if level.head.is_none() {
            level.head = Some(handle);
        }
        level.tail = Some(handle);
        level.total_qty += remaining_qty;
        let level_qty = level.total_qty;
        level.order_count += 1;
        self.side_mut(side).mark_occupied(key);
        Ok(level_qty)
    }

    /// Append an order to its account's active-order chain.
    fn append_to_account(&mut self, account_id: AccountId, handle: OrderHandle) -> DomainResult<()> {
        let order_notional = self
            .orders
            .get(handle)
            .map(|order| raw_notional(order.price, order.remaining_qty))
            .ok_or(DomainError::Malformed)?;
        let tail = {
            let account = self.accounts.get_mut(account_id).ok_or(DomainError::Malformed)?;
            let tail = account.tail;
            if account.head.is_none() {
                account.head = Some(handle);
            }
            account.tail = Some(handle);
            account.open_order_count += 1;
            account.open_notional += order_notional;
            tail
        };

        // Keep a second intrusive list so mass cancel never scans unrelated accounts.
        if let Some(tail) = tail {
            self.orders.get_mut(tail).expect("account tail").account_next = Some(handle);
            self.orders.get_mut(handle).expect("order").account_prev = Some(tail);
        }
        Ok(())
    }

    /// Update an existing order in place without changing FIFO priority.
    fn preserve_priority_replace(&mut self, handle: OrderHandle, replace: CancelReplace, old_qty: u64) -> DomainResult<()> {
        let current = self.orders.get(handle).copied().ok_or(DomainError::Malformed)?;
        let key = self.side(current.side).key_for_price(current.price)?;
        let order = self.orders.get_mut(handle).ok_or(DomainError::Malformed)?;
        order.order_id = replace.client_order_id;
        order.remaining_qty = replace.qty.raw();
        // The order keeps its handle; only the externally visible ID changes.
        self.order_index.remove(&replace.orig_client_order_id);
        self.order_index.insert(replace.client_order_id, handle);

        let level = self.side_mut(current.side).get_level_mut_or_activate(key)?;
        level.total_qty -= old_qty - replace.qty.raw();
        if let Some(account) = self.accounts.get_mut(current.account_id) {
            account.open_notional -= raw_notional(current.price, old_qty - replace.qty.raw());
        }
        Ok(())
    }

    /// Remove one resting order from every index and free its arena slot.
    fn unlink_and_free(&mut self, handle: OrderHandle) -> Option<RestingOrder> {
        let order = self.orders.get(handle).copied()?;
        // Unlink every index before returning the arena slot to the freelist.
        self.unlink_from_level(handle, order);
        self.unlink_from_account(handle, order);
        self.order_index.remove(&order.order_id);
        self.orders.free(handle)
    }

    /// Remove one resting order from its price-level FIFO chain.
    fn unlink_from_level(&mut self, handle: OrderHandle, order: RestingOrder) {
        // Patch neighboring FIFO links so arbitrary cancels stay constant-time.
        if let Some(prev) = order.level_prev
            && let Some(prev_order) = self.orders.get_mut(prev)
        {
            prev_order.level_next = order.level_next;
        }
        if let Some(next) = order.level_next
            && let Some(next_order) = self.orders.get_mut(next)
        {
            next_order.level_prev = order.level_prev;
        }

        let key = self.side(order.side).key_for_price(order.price).expect("stored price");
        let emptied = {
            // Update level aggregates before possibly clearing ladder occupancy.
            let level = self.side_mut(order.side).get_level_mut_or_activate(key).expect("stored level");
            if level.head == Some(handle) {
                level.head = order.level_next;
            }
            if level.tail == Some(handle) {
                level.tail = order.level_prev;
            }
            level.total_qty -= order.remaining_qty;
            level.order_count -= 1;
            level.is_empty()
        };
        if emptied {
            self.side_mut(order.side).mark_empty(key);
        }
    }

    /// Remove one resting order from its account active-order chain.
    fn unlink_from_account(&mut self, handle: OrderHandle, order: RestingOrder) {
        // Patch account-list neighbors independently from price-level FIFO links.
        if let Some(prev) = order.account_prev
            && let Some(prev_order) = self.orders.get_mut(prev)
        {
            prev_order.account_next = order.account_next;
        }
        if let Some(next) = order.account_next
            && let Some(next_order) = self.orders.get_mut(next)
        {
            next_order.account_prev = order.account_prev;
        }

        if let Some(account) = self.accounts.get_mut(order.account_id) {
            if account.head == Some(handle) {
                account.head = order.account_next;
            }
            if account.tail == Some(handle) {
                account.tail = order.account_prev;
            }
            account.open_order_count -= 1;
            account.open_notional -= raw_notional(order.price, order.remaining_qty);
        }
    }

    /// Return the next occupied price key behind `current` on one side of the book.
    fn next_occupied_key(&self, side: Side, current: ladder::LevelKey) -> Option<ladder::LevelKey> {
        match side {
            Side::Buy => self.side(side).occupied_before(current),
            Side::Sell => self.side(side).occupied_after(current),
        }
    }

    /// Return the opposite side's current best key and FIFO head.
    fn best_opposite(&self, side: Side) -> Option<(ladder::LevelKey, Option<OrderHandle>)> {
        let (key, level) = self.side(best_side(side)).best_level()?;
        Some((key, level.head))
    }

    /// Return whether a post-only order would cross the current opposite best.
    fn would_cross(&self, side: Side, price: Option<Price>) -> bool {
        let Some(price) = price else {
            return true;
        };
        match side {
            Side::Buy => self.best_ask().is_some_and(|(ask, _)| price >= ask),
            Side::Sell => self.best_bid().is_some_and(|(bid, _)| price <= bid),
        }
    }

    /// Return whether an opposite price is executable for this incoming order.
    fn is_executable(side: Side, limit_price: Option<Price>, opposite_price: Price) -> bool {
        match (side, limit_price) {
            (_, None) => true,
            (Side::Buy, Some(limit)) => limit >= opposite_price,
            (Side::Sell, Some(limit)) => limit <= opposite_price,
        }
    }

    /// Return whether a price is representable by this book instance.
    fn price_in_storage(&self, price: Price) -> bool {
        price >= self.config.storage_min_price() && price <= self.config.storage_max_price()
    }

    /// Emit market data for a visible book-level mutation.
    fn publish_book_update(
        &mut self,
        side: Side,
        price: Price,
        level_qty: u64,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
    ) -> DomainResult<()> {
        let best_bid = self.best_bid();
        let best_ask = self.best_ask();
        market_data.publish_l2_update(sequencer, side, price, level_qty)?;
        market_data.publish_l1_update_if_changed(sequencer, best_bid, best_ask)
    }

    /// Return the aggregate quantity currently visible at a level after mutation.
    fn current_level_qty(&self, side: Side, price: Price) -> u64 {
        let Ok(key) = self.side(side).key_for_price(price) else {
            return 0;
        };
        self.side(side).get_level(key).map_or(0, |level| level.total_qty)
    }

    /// Visit one side in price-time priority order.
    fn try_for_each_side_order<E>(&self, side: Side, visit: &mut impl FnMut(RestingOrderView) -> Result<(), E>) -> Result<(), E> {
        let mut cursor = self.side(side).best_level().map(|(key, _)| key);
        while let Some(key) = cursor {
            if let Some(level) = self.side(side).get_level(key) {
                let mut current = level.head;
                while let Some(handle) = current {
                    let Some(order) = self.orders.get(handle) else {
                        break;
                    };
                    visit(RestingOrderView {
                        order_id: order.order_id,
                        account_id: order.account_id,
                        side: order.side,
                        price: order.price,
                        remaining_qty: order.remaining_qty,
                    })?;
                    current = order.level_next;
                }
            }
            cursor = self.next_occupied_key(side, key);
        }
        Ok(())
    }

    /// Capture handles before drain mutation so traversal remains stable.
    fn collect_side_handles(&self, side: Side, out: &mut Vec<OrderHandle>) {
        let mut cursor = self.side(side).best_level().map(|(key, _)| key);
        while let Some(key) = cursor {
            if let Some(level) = self.side(side).get_level(key) {
                let mut current = level.head;
                while let Some(handle) = current {
                    let Some(order) = self.orders.get(handle) else {
                        break;
                    };
                    out.push(handle);
                    current = order.level_next;
                }
            }
            cursor = self.next_occupied_key(side, key);
        }
    }

    /// Return immutable state for one side of the book.
    fn side(&self, side: Side) -> &SideLadder {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    /// Return mutable state for one side of the book.
    fn side_mut(&mut self, side: Side) -> &mut SideLadder {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    /// Emit a rejected new-order report.
    fn reject_new(
        &mut self,
        order: NewOrder,
        reason: RejectReason,
        sequencer: &mut Sequencer,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> DomainResult<()> {
        self.push_report(
            sequencer,
            emit_report,
            ReportSpec::new_reject(order.client_order_id, order.account_id, reason, order.qty.raw()),
        )
    }

    /// Emit a rejected cancel report.
    fn reject_cancel(
        &mut self,
        cancel: CancelOrder,
        reason: RejectReason,
        sequencer: &mut Sequencer,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> DomainResult<()> {
        self.push_report(
            sequencer,
            emit_report,
            ReportSpec::new_reject(cancel.client_order_id, cancel.account_id, reason, 0),
        )
    }

    /// Emit a rejected cancel-replace report.
    fn reject_replace(
        &mut self,
        replace: CancelReplace,
        reason: RejectReason,
        sequencer: &mut Sequencer,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> DomainResult<()> {
        self.push_report(
            sequencer,
            emit_report,
            ReportSpec::new_reject(replace.client_order_id, replace.account_id, reason, replace.qty.raw()),
        )
    }

    /// Emit one deterministic execution report to the output sink.
    fn push_report(
        &mut self,
        sequencer: &mut Sequencer,
        emit_report: &mut impl FnMut(ExecutionReport),
        spec: ReportSpec,
    ) -> DomainResult<()> {
        emit_report(ExecutionReport {
            engine_seq: sequencer.next_engine_seq()?,
            client_order_id: spec.client_order_id,
            account_id: spec.account_id,
            exec_type: spec.exec_type,
            ord_status: spec.ord_status,
            reject_reason: spec.reject_reason,
            fill_price: spec.fill_price,
            fill_qty: spec.fill_qty,
            remaining_qty: spec.remaining_qty,
        });
        Ok(())
    }
}

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

#[derive(Debug, Clone, Copy)]
struct ReportSpec {
    /// Order ID carried on the report.
    client_order_id: OrderId,
    /// Account ID carried on the report.
    account_id: AccountId,
    /// Event type that caused this execution report.
    exec_type: ExecType,
    /// Order status after applying this report.
    ord_status: OrdStatus,
    /// Reject reason, or `None` for non-reject reports.
    reject_reason: RejectReason,
    /// Fill price for trade reports.
    fill_price: Option<Price>,
    /// Quantity filled by this report.
    fill_qty: u64,
    /// Live leaves quantity after this report.
    remaining_qty: u64,
}

impl ReportSpec {
    const fn new_reject(client_order_id: OrderId, account_id: AccountId, reject_reason: RejectReason, remaining_qty: u64) -> Self {
        Self {
            client_order_id,
            account_id,
            exec_type: ExecType::Rejected,
            ord_status: OrdStatus::Rejected,
            reject_reason,
            fill_price: None,
            fill_qty: 0,
            remaining_qty,
        }
    }
}

/// Return the side that provides liquidity to a taker on `taker_side`.
const fn best_side(taker_side: Side) -> Side {
    match taker_side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

fn raw_notional(price: Price, qty: u64) -> u128 {
    u128::from(price.raw()) * u128::from(qty)
}

#[allow(dead_code)]
const _: fn(DomainError) -> RejectReason = structural_reject_reason;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        EngineSeq, Price, Qty, RecvTs, Sequencer,
        rd::InstrumentSpec,
        trading::order::{
            AccountId, ClientTs, ExecType, ExecutionReport, NewOrder, OrdStatus, OrderEntry, OrderId, OrderType, Side, TimeInForce,
        },
    };
    use crate::{
        io::IOConfig,
        market_data::{MarketDataFeedConfig, MarketDataRuntime},
        protocol::{
            MessageKind, SBE_HEADER_LEN, SCHEMA_VERSION, decode_header,
            generated::codecs::{ReadBuf, book_side, l1_update_codec, l2_update_codec, trade_print_codec},
        },
    };
    use proptest::prelude::*;
    use std::{
        collections::{HashMap, HashSet},
        io::Read,
        path::{Path, PathBuf},
    };

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
        match MarketDataRuntime::new(market_data_config()) {
            Ok(market_data) => Some(market_data),
            Err(crate::market_data::MarketDataError::Io(crate::io::IoError::Bind(error)))
                if error.kind() == std::io::ErrorKind::PermissionDenied =>
            {
                None
            }
            Err(error) => panic!("market-data: {error:?}"),
        }
    }

    fn market_data_config() -> MarketDataFeedConfig {
        MarketDataFeedConfig {
            io: IOConfig::Udp {
                bind_addr: "0.0.0.0:0".to_owned(),
                group_addr: "239.255.0.1:19002".to_owned(),
                multicast_ttl: 1,
                multicast_loop_v4: true,
                max_datagram_len: 4096,
                max_pending_datagram_bytes: 1 << 20,
            },
        }
    }

    fn market_data_file_config(path: PathBuf) -> MarketDataFeedConfig {
        MarketDataFeedConfig {
            io: IOConfig::File {
                input_path: None,
                output_path: Some(path),
                max_frame_len: 4096,
            },
        }
    }

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ignition-engine-{name}-{}-{}.bin", std::process::id(), unique_suffix()))
    }

    fn unique_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum CapturedMarketData {
        Trade {
            engine_seq: u64,
            md_seq: u64,
            trade_id: u64,
            maker_order_id: u64,
            taker_order_id: u64,
            price: u64,
            qty: u64,
        },
        L1 {
            engine_seq: u64,
            md_seq: u64,
            prev_md_seq: u64,
            bid_price: u64,
            bid_qty: u64,
            ask_price: u64,
            ask_qty: u64,
        },
        L2 {
            engine_seq: u64,
            md_seq: u64,
            prev_md_seq: u64,
            side: Side,
            price: u64,
            qty: u64,
        },
    }

    impl CapturedMarketData {
        const fn md_seq(self) -> u64 {
            match self {
                Self::Trade { md_seq, .. } | Self::L1 { md_seq, .. } | Self::L2 { md_seq, .. } => md_seq,
            }
        }
        const fn engine_seq(self) -> u64 {
            match self {
                Self::Trade { engine_seq, .. } | Self::L1 { engine_seq, .. } | Self::L2 { engine_seq, .. } => engine_seq,
            }
        }
    }

    fn read_market_data(path: &Path) -> Vec<CapturedMarketData> {
        let mut file = std::fs::File::open(path).expect("market-data output");
        let mut events = Vec::new();
        loop {
            let mut len = [0_u8; 4];
            match file.read_exact(&mut len) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(error) => panic!("read market-data frame len: {error:?}"),
            }
            let mut frame = vec![0_u8; u32::from_le_bytes(len) as usize];
            file.read_exact(&mut frame).expect("read market-data frame");
            let header = decode_header(&frame).expect("market-data header");
            let read_buf = ReadBuf::new(&frame);
            events.push(match header.kind {
                MessageKind::TradePrint => {
                    let decoder = trade_print_codec::TradePrintDecoder::default().wrap(
                        read_buf,
                        SBE_HEADER_LEN,
                        trade_print_codec::SBE_BLOCK_LENGTH,
                        SCHEMA_VERSION,
                    );
                    CapturedMarketData::Trade {
                        engine_seq: decoder.engine_seq(),
                        md_seq: decoder.md_seq(),
                        trade_id: decoder.trade_id(),
                        maker_order_id: decoder.maker_order_id(),
                        taker_order_id: decoder.taker_order_id(),
                        price: decoder.price(),
                        qty: decoder.qty(),
                    }
                }
                MessageKind::L1Update => {
                    let decoder = l1_update_codec::L1UpdateDecoder::default().wrap(
                        read_buf,
                        SBE_HEADER_LEN,
                        l1_update_codec::SBE_BLOCK_LENGTH,
                        SCHEMA_VERSION,
                    );
                    CapturedMarketData::L1 {
                        engine_seq: decoder.engine_seq(),
                        md_seq: decoder.md_seq(),
                        prev_md_seq: decoder.prev_md_seq(),
                        bid_price: decoder.bid_price(),
                        bid_qty: decoder.bid_qty(),
                        ask_price: decoder.ask_price(),
                        ask_qty: decoder.ask_qty(),
                    }
                }
                MessageKind::L2Update => {
                    let decoder = l2_update_codec::L2UpdateDecoder::default().wrap(
                        read_buf,
                        SBE_HEADER_LEN,
                        l2_update_codec::SBE_BLOCK_LENGTH,
                        SCHEMA_VERSION,
                    );
                    CapturedMarketData::L2 {
                        engine_seq: decoder.engine_seq(),
                        md_seq: decoder.md_seq(),
                        prev_md_seq: decoder.prev_md_seq(),
                        side: match decoder.side() {
                            book_side::BookSide::Bid => Side::Buy,
                            book_side::BookSide::Ask => Side::Sell,
                            book_side::BookSide::NullVal => panic!("null book side"),
                        },
                        price: decoder.price(),
                        qty: decoder.qty(),
                    }
                }
                kind => panic!("unexpected market-data frame: {kind:?}"),
            });
        }
        events
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

        fn new_noop() -> Self {
            Self {
                engine: Engine::new(config()).expect("engine"),
                sequencer: Sequencer::default(),
                market_data: MarketDataRuntime::new(MarketDataFeedConfig { io: IOConfig::Noop }).expect("market data"),
            }
        }

        fn new_file_market_data(path: PathBuf) -> Self {
            Self {
                engine: Engine::new(config()).expect("engine"),
                sequencer: Sequencer::default(),
                market_data: MarketDataRuntime::new(market_data_file_config(path)).expect("market data"),
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

        fn flush_market_data(&mut self) {
            self.market_data.flush_pending().expect("flush market data");
        }
    }

    fn execute(engine: &mut TestEngine, entry: OrderEntry, events: &mut Vec<TradingEvent>, label: &str) {
        engine.execute(entry, events, label);
    }

    fn resting_rows(engine: &TestEngine) -> Vec<(u64, u32, Side, u64, u64)> {
        let mut rows = Vec::new();
        engine
            .engine
            .try_for_each_resting_order(|row| {
                rows.push((
                    row.order_id.raw(),
                    row.account_id.raw(),
                    row.side,
                    row.price.raw(),
                    row.remaining_qty,
                ));
                Ok::<(), ()>(())
            })
            .expect("resting rows");
        rows
    }

    fn assert_account_aggregates_match_rows(engine: &TestEngine) {
        let rows = resting_rows(engine);
        let mut expected: HashMap<u32, (u32, u128)> = HashMap::new();
        for (_, account, _, price_raw, qty_raw) in rows {
            let entry = expected.entry(account).or_insert((0, 0));
            entry.0 += 1;
            entry.1 += u128::from(price_raw) * u128::from(qty_raw);
        }
        for account in 1..=config().max_account_id() {
            let expected = expected.get(&account).copied().unwrap_or((0, 0));
            assert_eq!(
                engine.account_open_order_count(account),
                Some(expected.0),
                "account {account} open count"
            );
            assert_eq!(
                engine.account_open_notional(account),
                Some(expected.1),
                "account {account} notional"
            );
        }
    }

    fn side_order(side: Side, price_raw: u64) -> std::cmp::Reverse<u64> {
        match side {
            Side::Buy => std::cmp::Reverse(price_raw),
            Side::Sell => std::cmp::Reverse(u64::MAX - price_raw),
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum GeneratedCommand {
        New {
            order_id: u64,
            account: u32,
            buy: bool,
            market: bool,
            tif: u8,
            price_raw: u64,
            qty_raw: u64,
        },
        Cancel {
            order_id: u64,
            account: u32,
            orig_order_id: u64,
        },
        Replace {
            order_id: u64,
            account: u32,
            orig_order_id: u64,
            price_raw: u64,
            qty_raw: u64,
        },
        MassCancel {
            order_id: u64,
            account: u32,
        },
    }

    impl GeneratedCommand {
        fn into_entry(self, recv_ts: u64) -> OrderEntry {
            match self {
                Self::New {
                    order_id,
                    account,
                    buy,
                    market,
                    tif,
                    price_raw,
                    qty_raw,
                } => new_order(
                    order_id,
                    account,
                    if buy { Side::Buy } else { Side::Sell },
                    if market { OrderType::Market } else { OrderType::Limit },
                    match tif {
                        0 => TimeInForce::Gtc,
                        1 => TimeInForce::Ioc,
                        _ => TimeInForce::PostOnly,
                    },
                    (!market).then_some(price_raw),
                    qty_raw,
                    recv_ts,
                ),
                Self::Cancel {
                    order_id,
                    account,
                    orig_order_id,
                } => cancel(order_id, account, orig_order_id),
                Self::Replace {
                    order_id,
                    account,
                    orig_order_id,
                    price_raw,
                    qty_raw,
                } => replace(order_id, account, orig_order_id, price_raw, qty_raw),
                Self::MassCancel { order_id, account } => mass_cancel(order_id, account),
            }
        }
    }

    fn generated_command_strategy() -> impl Strategy<Value = GeneratedCommand> {
        prop_oneof![
            5 => (1_u64..40, 1_u32..10, any::<bool>(), any::<bool>(), 0_u8..3, 1_u64..11_000, 1_u64..25)
                .prop_map(|(order_id, account, buy, market, tif, price_raw, qty_raw)| GeneratedCommand::New {
                    order_id,
                    account,
                    buy,
                    market,
                    tif,
                    price_raw,
                    qty_raw,
                }),
            2 => (100_u64..160, 1_u32..10, 1_u64..40)
                .prop_map(|(order_id, account, orig_order_id)| GeneratedCommand::Cancel {
                    order_id,
                    account,
                    orig_order_id,
                }),
            3 => (1_u64..50, 1_u32..10, 1_u64..40, 1_u64..11_000, 1_u64..35)
                .prop_map(|(order_id, account, orig_order_id, price_raw, qty_raw)| GeneratedCommand::Replace {
                    order_id,
                    account,
                    orig_order_id,
                    price_raw,
                    qty_raw,
                }),
            1 => (200_u64..240, 1_u32..10).prop_map(|(order_id, account)| GeneratedCommand::MassCancel {
                order_id,
                account,
            }),
        ]
    }

    fn assert_reports_are_strictly_sequenced(events: &[TradingEvent], last_engine_seq: &mut u64) {
        for report in events {
            assert!(
                report.engine_seq.raw() > *last_engine_seq,
                "engine seq {} did not advance after {}",
                report.engine_seq.raw(),
                *last_engine_seq
            );
            *last_engine_seq = report.engine_seq.raw();
        }
    }

    fn assert_trade_reports_are_paired(events: &[TradingEvent]) {
        let trades = events
            .iter()
            .filter(|report| report.exec_type == ExecType::Trade)
            .collect::<Vec<_>>();
        assert_eq!(trades.len() % 2, 0, "trade reports must be maker/taker pairs: {trades:?}");
        for pair in trades.chunks_exact(2) {
            assert_ne!(pair[0].client_order_id, pair[1].client_order_id);
            assert_eq!(pair[0].fill_price, pair[1].fill_price);
            assert_eq!(pair[0].fill_qty, pair[1].fill_qty);
            assert!(pair[0].fill_price.is_some());
            assert!(pair[0].fill_qty > 0);
        }
    }

    fn assert_rows_are_in_book_order(rows: &[(u64, u32, Side, u64, u64)]) {
        let first_sell = rows.iter().position(|(_, _, side, _, _)| *side == Side::Sell).unwrap_or(rows.len());
        assert!(rows[..first_sell].iter().all(|(_, _, side, _, _)| *side == Side::Buy));
        assert!(rows[first_sell..].iter().all(|(_, _, side, _, _)| *side == Side::Sell));

        for pair in rows[..first_sell].windows(2) {
            assert!(pair[0].3 >= pair[1].3, "bids not descending: {rows:?}");
        }
        for pair in rows[first_sell..].windows(2) {
            assert!(pair[0].3 <= pair[1].3, "asks not ascending: {rows:?}");
        }
    }

    fn assert_best_quotes_match_rows(engine: &TestEngine, rows: &[(u64, u32, Side, u64, u64)]) {
        let best_bid = rows
            .iter()
            .filter(|(_, _, side, _, _)| *side == Side::Buy)
            .map(|(_, _, _, price_raw, _)| *price_raw)
            .max()
            .map(|best_price| {
                (
                    price(best_price),
                    rows.iter()
                        .filter(|(_, _, side, price_raw, _)| *side == Side::Buy && *price_raw == best_price)
                        .map(|(_, _, _, _, qty_raw)| *qty_raw)
                        .sum(),
                )
            });
        let best_ask = rows
            .iter()
            .filter(|(_, _, side, _, _)| *side == Side::Sell)
            .map(|(_, _, _, price_raw, _)| *price_raw)
            .min()
            .map(|best_price| {
                (
                    price(best_price),
                    rows.iter()
                        .filter(|(_, _, side, price_raw, _)| *side == Side::Sell && *price_raw == best_price)
                        .map(|(_, _, _, _, qty_raw)| *qty_raw)
                        .sum(),
                )
            });

        assert_eq!(engine.best_bid(), best_bid);
        assert_eq!(engine.best_ask(), best_ask);
        if let (Some((bid, _)), Some((ask, _))) = (best_bid, best_ask) {
            assert!(bid < ask, "book must not remain crossed");
        }
    }

    fn assert_core_book_invariants(engine: &TestEngine) {
        let rows = resting_rows(engine);
        assert_eq!(engine.resting_order_count(), rows.len());
        assert!(
            rows.iter()
                .all(|(_, _, _, price_raw, qty_raw)| (100..=10_000).contains(price_raw) && *qty_raw > 0)
        );
        assert_eq!(
            rows.iter().map(|(order_id, _, _, _, _)| *order_id).collect::<HashSet<_>>().len(),
            rows.len(),
            "resting order ids must be unique"
        );
        assert_rows_are_in_book_order(&rows);
        assert_best_quotes_match_rows(engine, &rows);
        assert_account_aggregates_match_rows(engine);
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
    fn market_data_and_execution_reports_share_engine_sequence() {
        let path = temp_path("global-outbound-sequence");
        let mut engine = TestEngine::new_file_market_data(path.clone());
        let mut events = Vec::new();

        execute(
            &mut engine,
            new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1000), 10, 1),
            &mut events,
            "rest maker",
        );
        assert_eq!(
            exec_reports(&events).iter().map(|event| event.engine_seq.raw()).collect::<Vec<_>>(),
            vec![3]
        );

        execute(
            &mut engine,
            new_order(2, 2, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 4, 2),
            &mut events,
            "take liquidity",
        );
        assert_eq!(
            exec_reports(&events).iter().map(|event| event.engine_seq.raw()).collect::<Vec<_>>(),
            vec![7, 8]
        );

        engine.flush_market_data();
        let captured = read_market_data(&path);
        let _ = std::fs::remove_file(path);

        assert_eq!(
            captured.iter().map(|event| event.engine_seq()).collect::<Vec<_>>(),
            vec![1, 2, 4, 5, 6]
        );
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

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]

        /// Builds a generated opposite-side book, sends one aggressive market
        /// IOC order through the real engine, and checks price-time priority.
        /// It also checks maker/taker report pairing, strictly increasing
        /// engine sequence numbers, nonzero resting quantities, total resting
        /// quantity conservation, and per-account aggregate consistency.
        #[test]
        fn property_market_ioc_respects_price_time_priority_and_conserves_book(
            taker_buy in any::<bool>(),
            maker_specs in prop::collection::vec((0_u64..5, 1_u64..25), 1..24),
            taker_qty in 1_u64..500,
        ) {
            let mut engine = TestEngine::new_noop();
            let mut events = Vec::new();
            let taker_side = if taker_buy { Side::Buy } else { Side::Sell };
            let maker_side = if taker_buy { Side::Sell } else { Side::Buy };

            for (index, (price_offset, qty_raw)) in maker_specs.iter().copied().enumerate() {
                execute(
                    &mut engine,
                    new_order(
                        index as u64 + 1,
                        index as u32 + 1,
                        maker_side,
                        OrderType::Limit,
                        TimeInForce::Gtc,
                        Some(1_000 + price_offset),
                        qty_raw,
                        index as u64 + 1,
                    ),
                    &mut events,
                    "rest maker",
                );
            }

            let mut expected_priority: Vec<_> = maker_specs
                .iter()
                .copied()
                .enumerate()
                .map(|(index, (price_offset, qty_raw))| (index as u64 + 1, 1_000 + price_offset, qty_raw, index))
                .collect();
            expected_priority.sort_by_key(|(_, price_raw, _, index)| (side_order(maker_side, *price_raw), *index));

            execute(
                &mut engine,
                new_order(10_000, 99, taker_side, OrderType::Market, TimeInForce::Ioc, None, taker_qty, 10_000),
                &mut events,
                "take liquidity",
            );

            let mut remaining_taker = taker_qty;
            let mut expected_fills = Vec::new();
            for (maker_id, price_raw, maker_qty, _) in expected_priority {
                if remaining_taker == 0 {
                    break;
                }
                let fill_qty = remaining_taker.min(maker_qty);
                expected_fills.push((maker_id, price_raw, fill_qty));
                remaining_taker -= fill_qty;
            }

            let maker_reports: Vec<_> = events
                .iter()
                .filter(|report| report.exec_type == ExecType::Trade && report.client_order_id.raw() != 10_000)
                .map(|report| (report.client_order_id.raw(), report.fill_price.expect("maker fill price").raw(), report.fill_qty))
                .collect();
            let taker_reports: Vec<_> = events
                .iter()
                .filter(|report| report.exec_type == ExecType::Trade && report.client_order_id.raw() == 10_000)
                .map(|report| (report.fill_price.expect("taker fill price").raw(), report.fill_qty))
                .collect();

            prop_assert_eq!(maker_reports, expected_fills.clone());
            prop_assert_eq!(
                taker_reports,
                expected_fills
                    .iter()
                    .map(|(_, price_raw, fill_qty)| (*price_raw, *fill_qty))
                    .collect::<Vec<_>>()
            );
            prop_assert!(events.windows(2).all(|pair| pair[0].engine_seq.raw() < pair[1].engine_seq.raw()));

            let total_resting: u64 = maker_specs.iter().map(|(_, qty_raw)| *qty_raw).sum();
            let total_filled = taker_qty.min(total_resting);
            let rows = resting_rows(&engine);
            prop_assert!(rows.iter().all(|(_, _, _, _, qty_raw)| *qty_raw > 0));
            prop_assert_eq!(rows.iter().map(|(_, _, _, _, qty_raw)| *qty_raw).sum::<u64>(), total_resting - total_filled);
            prop_assert_eq!(engine.resting_order_count(), rows.len());
            assert_account_aggregates_match_rows(&engine);
        }

        /// Uses file-backed market-data output so the property checks the
        /// actual encoded SBE feed, not only execution reports. It builds a
        /// generated one-sided book, executes one aggressive market IOC, then
        /// asserts that TradePrints carry the expected maker, taker, price, and
        /// quantity, L2 updates publish absolute level quantities after every
        /// add/fill, L1 ends at the final top of book, and market-data sequence
        /// numbers plus prev links are monotonic.
        #[test]
        fn property_market_data_feed_matches_generated_matching_semantics(
            taker_buy in any::<bool>(),
            maker_specs in prop::collection::vec((0_u64..5, 1_u64..25), 1..24),
            taker_qty in 1_u64..500,
        ) {
            let path = temp_path("property-market-data");
            let mut engine = TestEngine::new_file_market_data(path.clone());
            let mut events = Vec::new();
            let taker_side = if taker_buy { Side::Buy } else { Side::Sell };
            let maker_side = if taker_buy { Side::Sell } else { Side::Buy };
            let mut expected_l2 = Vec::new();
            let mut level_qty: HashMap<u64, u64> = HashMap::new();

            for (index, (price_offset, qty_raw)) in maker_specs.iter().copied().enumerate() {
                let price_raw = 1_000 + price_offset;
                execute(
                    &mut engine,
                    new_order(
                        index as u64 + 1,
                        index as u32 + 1,
                        maker_side,
                        OrderType::Limit,
                        TimeInForce::Gtc,
                        Some(price_raw),
                        qty_raw,
                        index as u64 + 1,
                    ),
                    &mut events,
                    "rest maker",
                );
                let qty = level_qty.entry(price_raw).or_insert(0);
                *qty += qty_raw;
                expected_l2.push((maker_side, price_raw, *qty));
            }

            let mut expected_priority: Vec<_> = maker_specs
                .iter()
                .copied()
                .enumerate()
                .map(|(index, (price_offset, qty_raw))| (index as u64 + 1, 1_000 + price_offset, qty_raw, index))
                .collect();
            expected_priority.sort_by_key(|(_, price_raw, _, index)| (side_order(maker_side, *price_raw), *index));

            execute(
                &mut engine,
                new_order(10_000, 99, taker_side, OrderType::Market, TimeInForce::Ioc, None, taker_qty, 10_000),
                &mut events,
                "take liquidity",
            );

            let mut remaining_taker = taker_qty;
            let mut expected_trades = Vec::new();
            for (maker_id, price_raw, maker_qty, _) in expected_priority {
                if remaining_taker == 0 {
                    break;
                }
                let fill_qty = remaining_taker.min(maker_qty);
                expected_trades.push((maker_id, 10_000, price_raw, fill_qty));
                remaining_taker -= fill_qty;
                let qty = level_qty.get_mut(&price_raw).expect("level qty");
                *qty -= fill_qty;
                expected_l2.push((maker_side, price_raw, *qty));
            }

            engine.flush_market_data();
            let captured = read_market_data(&path);
            let _ = std::fs::remove_file(path);

            prop_assert_eq!(
                captured.iter().map(|event| event.md_seq()).collect::<Vec<_>>(),
                (1..=captured.len() as u64).collect::<Vec<_>>()
            );
            prop_assert!(captured.windows(2).all(|pair| pair[0].engine_seq() < pair[1].engine_seq()));
            for event in &captured {
                match *event {
                    CapturedMarketData::L1 { md_seq, prev_md_seq, .. } | CapturedMarketData::L2 { md_seq, prev_md_seq, .. } => {
                        prop_assert_eq!(prev_md_seq, md_seq - 1);
                    }
                    CapturedMarketData::Trade { .. } => {}
                }
            }

            let trades = captured
                .iter()
                .filter_map(|event| match *event {
                    CapturedMarketData::Trade {
                        trade_id,
                        maker_order_id,
                        taker_order_id,
                        price,
                        qty,
                        ..
                    } => Some((trade_id, maker_order_id, taker_order_id, price, qty)),
                    CapturedMarketData::L1 { .. } | CapturedMarketData::L2 { .. } => None,
                })
                .collect::<Vec<_>>();
            prop_assert_eq!(
                trades,
                expected_trades
                    .iter()
                    .enumerate()
                    .map(|(index, (maker_order_id, taker_order_id, price, qty))| {
                        (index as u64 + 1, *maker_order_id, *taker_order_id, *price, *qty)
                    })
                    .collect::<Vec<_>>()
            );

            let l2_updates = captured
                .iter()
                .filter_map(|event| match *event {
                    CapturedMarketData::L2 { side, price, qty, .. } => Some((side, price, qty)),
                    CapturedMarketData::Trade { .. } | CapturedMarketData::L1 { .. } => None,
                })
                .collect::<Vec<_>>();
            prop_assert_eq!(l2_updates, expected_l2);

            let last_l1 = captured.iter().rev().find_map(|event| match *event {
                CapturedMarketData::L1 {
                    bid_price,
                    bid_qty,
                    ask_price,
                    ask_qty,
                    ..
                } => Some((bid_price, bid_qty, ask_price, ask_qty)),
                CapturedMarketData::Trade { .. } | CapturedMarketData::L2 { .. } => None,
            });
            if let Some((bid_price, bid_qty, ask_price, ask_qty)) = last_l1 {
                let final_bid = engine.best_bid().map_or((0, 0), |(price, qty)| (price.raw(), qty));
                let final_ask = engine.best_ask().map_or((0, 0), |(price, qty)| (price.raw(), qty));
                prop_assert_eq!((bid_price, bid_qty), final_bid);
                prop_assert_eq!((ask_price, ask_qty), final_ask);
            }
        }

        /// Builds a generated one-sided book across several accounts, runs a
        /// mass cancel for one account, and checks that only that account's
        /// resting orders disappear. The remaining rows must keep deterministic
        /// book order and the account open-order/notional aggregates must still
        /// match the visible book.
        #[test]
        fn property_mass_cancel_removes_only_target_account(
            target_account in 1_u32..8,
            order_specs in prop::collection::vec((1_u32..8, 0_u64..5, 1_u64..25), 1..32),
        ) {
            let mut engine = TestEngine::new_noop();
            let mut events = Vec::new();
            let mut expected = Vec::new();

            for (index, (account, price_offset, qty_raw)) in order_specs.iter().copied().enumerate() {
                let side = Side::Buy;
                let id = index as u64 + 1;
                execute(
                    &mut engine,
                    new_order(id, account, side, OrderType::Limit, TimeInForce::Gtc, Some(1_000 + price_offset), qty_raw, id),
                    &mut events,
                    "rest",
                );
                expected.push((id, account, side, 1_000 + price_offset, qty_raw));
            }

            execute(&mut engine, mass_cancel(10_000, target_account), &mut events, "mass cancel");
            expected.retain(|(_, account, _, _, _)| *account != target_account);
            expected.sort_by_key(|(id, _, side, price_raw, _)| (*side == Side::Sell, side_order(*side, *price_raw), *id));

            prop_assert_eq!(resting_rows(&engine), expected);
            prop_assert!(resting_rows(&engine).iter().all(|(_, account, _, _, _)| *account != target_account));
            assert_account_aggregates_match_rows(&engine);
        }

        /// Runs a generated mixed stream of adds, cancels, replaces, mass
        /// cancels, duplicate IDs, unknown order references, market orders, IOC
        /// orders, GTC residuals, post-only orders, and storage-envelope
        /// rejects through one persistent engine. After every command it checks
        /// report sequencing, maker/taker trade pairing, no crossed book,
        /// unique and positive resting rows, deterministic book ordering, best
        /// quote aggregates, and per-account open count/notional consistency.
        #[test]
        fn property_mixed_command_stream_preserves_core_book_invariants(
            commands in prop::collection::vec(generated_command_strategy(), 1..96),
        ) {
            let mut engine = TestEngine::new_noop();
            let mut events = Vec::new();
            let mut last_engine_seq = 0;

            for (index, command) in commands.into_iter().enumerate() {
                execute(
                    &mut engine,
                    command.into_entry(index as u64 + 1),
                    &mut events,
                    "generated mixed command",
                );
                assert_reports_are_strictly_sequenced(&events, &mut last_engine_seq);
                assert_trade_reports_are_paired(&events);
                assert_core_book_invariants(&engine);
            }
        }

        /// Checks the documented cancel-replace priority rules with generated
        /// quantities. Same-price quantity reductions must keep the order's
        /// FIFO position, while price changes and quantity increases must lose
        /// priority against orders that were already resting at the destination
        /// price level.
        #[test]
        fn property_cancel_replace_priority_rules_match_documented_semantics(
            first_qty in 2_u64..40,
            reduced_qty in 1_u64..20,
            competing_qty in 1_u64..40,
            increase_delta in 1_u64..20,
        ) {
            let reduced_qty = reduced_qty.min(first_qty);

            let mut reduction_engine = TestEngine::new_noop();
            let mut reduction_events = Vec::new();
            execute(&mut reduction_engine, new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1_000), first_qty, 1), &mut reduction_events, "rest first");
            execute(&mut reduction_engine, new_order(2, 2, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1_000), competing_qty, 2), &mut reduction_events, "rest second");
            execute(&mut reduction_engine, replace(11, 1, 1, 1_000, reduced_qty), &mut reduction_events, "reduce");
            execute(&mut reduction_engine, new_order(3, 3, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, reduced_qty, 3), &mut reduction_events, "take reduced");
            prop_assert_eq!(trade_report_makers(&reduction_events, 3), vec![11]);
            assert_account_aggregates_match_rows(&reduction_engine);

            let mut price_change_engine = TestEngine::new_noop();
            let mut price_change_events = Vec::new();
            execute(&mut price_change_engine, new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1_001), competing_qty, 1), &mut price_change_events, "rest competing destination");
            execute(&mut price_change_engine, new_order(2, 2, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1_000), first_qty, 2), &mut price_change_events, "rest original");
            execute(&mut price_change_engine, replace(22, 2, 2, 1_001, first_qty), &mut price_change_events, "price change");
            execute(&mut price_change_engine, new_order(3, 3, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 1, 3), &mut price_change_events, "take changed");
            prop_assert_eq!(trade_report_makers(&price_change_events, 3), vec![1]);
            assert_account_aggregates_match_rows(&price_change_engine);

            let mut increase_engine = TestEngine::new_noop();
            let mut increase_events = Vec::new();
            execute(&mut increase_engine, new_order(1, 1, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1_000), first_qty, 1), &mut increase_events, "rest original");
            execute(&mut increase_engine, new_order(2, 2, Side::Sell, OrderType::Limit, TimeInForce::Gtc, Some(1_000), competing_qty, 2), &mut increase_events, "rest competing");
            execute(&mut increase_engine, replace(11, 1, 1, 1_000, first_qty + increase_delta), &mut increase_events, "increase");
            execute(&mut increase_engine, new_order(3, 3, Side::Buy, OrderType::Market, TimeInForce::Ioc, None, 1, 3), &mut increase_events, "take increased");
            prop_assert_eq!(trade_report_makers(&increase_events, 3), vec![2]);
            assert_account_aggregates_match_rows(&increase_engine);
        }
    }
}
