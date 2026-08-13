use std::collections::HashMap;

use crate::{
    domain::{
        DomainError, DomainResult, Price, RejectReason, Sequencer,
        trading::order::{
            AccountId, CancelOrder, CancelReplace, ExecType, ExecutionReport, MassCancel, NewOrder, OrdStatus, OrderEntry, OrderId,
            OrderType, Side, TimeInForce,
        },
    },
    market_data::MarketDataRuntime,
};

use super::{
    EngineConfig, OrderProjection, RestingOrderView, accounts::AccountTable, ladder::SideLadder, orders::OrderArena, orders::OrderHandle,
    orders::RestingOrder, structural_reject_reason,
};

/// Single-symbol order book with side ladders and resting-order indexes.
#[derive(Debug)]
pub(crate) struct OrderBook {
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

#[derive(Debug, Default, Clone, Copy)]
struct MatchOutcome {
    /// Total taker quantity filled during the aggressive walk.
    filled_qty: u64,
    /// Last taker report is held until the caller knows whether residual expires.
    pending_taker_report: Option<ReportSpec>,
}

impl OrderBook {
    /// Create an empty order book using prevalidated engine configuration.
    pub(crate) fn new(config: EngineConfig) -> DomainResult<Self> {
        Ok(Self {
            config,
            bids: SideLadder::new(Side::Buy, config)?,
            asks: SideLadder::new(Side::Sell, config)?,
            orders: OrderArena::with_capacity(config.max_resting_orders()),
            order_index: HashMap::with_capacity(config.max_resting_orders() as usize),
            accounts: AccountTable::new(config.max_account_id()),
        })
    }

    /// Execute one normalized order-entry command against the book.
    pub(crate) fn execute(
        &mut self,
        entry: OrderEntry,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> DomainResult<()> {
        match entry {
            OrderEntry::NewOrder(order) => self.on_new_order(order, sequencer, market_data, emit_report),
            OrderEntry::CancelOrder(cancel) => self.on_cancel_order(cancel, sequencer, market_data, emit_report),
            OrderEntry::CancelReplace(replace) => self.on_cancel_replace(replace, sequencer, market_data, emit_report),
            OrderEntry::MassCancel(cancel) => self.on_mass_cancel(cancel, sequencer, market_data, emit_report),
        }
    }

    /// Return the current best bid and aggregate quantity.
    pub(crate) fn best_bid(&self) -> Option<(Price, u64)> {
        self.bids.top()
    }

    /// Return the current best ask and aggregate quantity.
    pub(crate) fn best_ask(&self) -> Option<(Price, u64)> {
        self.asks.top()
    }

    /// Return the number of active resting orders.
    pub(crate) fn resting_order_count(&self) -> usize {
        self.orders.active()
    }

    /// Return whether an account ID can be addressed by this book.
    pub(crate) fn account_exists(&self, account_id: AccountId) -> bool {
        self.accounts.contains(account_id)
    }

    /// Return the account's current open resting order count.
    pub(crate) fn account_open_order_count(&self, account_id: AccountId) -> Option<u32> {
        self.accounts.get(account_id).map(|account| account.open_order_count)
    }

    /// Return the account's current raw open resting notional.
    pub(crate) fn account_open_notional(&self, account_id: AccountId) -> Option<u128> {
        self.accounts.get(account_id).map(|account| account.open_notional)
    }

    /// Return a read-only view of one resting order if it belongs to the account.
    pub(crate) fn resting_order(&self, account_id: AccountId, order_id: OrderId) -> Option<RestingOrderView> {
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
    pub(crate) fn project_order(&self, account_id: AccountId, side: Side, limit_price: Option<Price>, qty: u64) -> OrderProjection {
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
    pub(crate) fn try_for_each_resting_order<E>(&self, mut visit: impl FnMut(RestingOrderView) -> Result<(), E>) -> Result<(), E> {
        self.try_for_each_side_order(Side::Buy, &mut visit)?;
        self.try_for_each_side_order(Side::Sell, &mut visit)
    }

    /// Cancel every resting order in deterministic book order.
    pub(crate) fn drain_all(
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
            self.publish_book_update(removed.side, removed.price, level_qty, market_data)?;
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
            // todo - provide better rejection
            return self.reject_new(order, RejectReason::Malformed, sequencer, emit_report);
        }
        if self.order_index.contains_key(&order.client_order_id) {
            // todo - provide better rejection
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
                self.publish_book_update(order.side, price, level_qty, market_data)?;
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
            self.publish_book_update(order.side, order.price, level_qty, market_data)?;
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
                self.publish_book_update(current.side, current.price, replace.qty.raw(), market_data)?;
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
            self.publish_book_update(removed.side, removed.price, level_qty, market_data)?;
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
                self.publish_book_update(removed.side, removed.price, level_qty, market_data)?;
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
            market_data.publish_trade(maker.order_id, taker_order_id, maker.price, fill_qty, &self.config.instrument())?;
            self.publish_book_update(maker.side, maker.price, maker_remaining, market_data)?;

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
    fn apply_fill(&mut self, maker_handle: OrderHandle, key: super::ladder::LevelKey, fill_qty: u64) -> DomainResult<()> {
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
    fn append_to_level(&mut self, side: Side, key: super::ladder::LevelKey, handle: OrderHandle) -> DomainResult<u64> {
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
    fn next_occupied_key(&self, side: Side, current: super::ladder::LevelKey) -> Option<super::ladder::LevelKey> {
        match side {
            Side::Buy => self.side(side).occupied_before(current),
            Side::Sell => self.side(side).occupied_after(current),
        }
    }

    /// Return the opposite side's current best key and FIFO head.
    fn best_opposite(&self, side: Side) -> Option<(super::ladder::LevelKey, Option<OrderHandle>)> {
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
    fn publish_book_update(&mut self, side: Side, price: Price, level_qty: u64, market_data: &mut MarketDataRuntime) -> DomainResult<()> {
        let best_bid = self.best_bid();
        let best_ask = self.best_ask();
        market_data.publish_l2_update(side, price, level_qty)?;
        market_data.publish_l1_update_if_changed(best_bid, best_ask)
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
