use std::collections::HashMap;

use crate::{
    domain::{
        DomainError, DomainResult, EngineSequencer, Price, RejectReason,
        trading::{
            TradingEvent,
            order::{
                AccountId, CancelOrder, CancelReplace, ExecType, ExecutionReport, MassCancel,
                NewOrder, OrdStatus, OrderEntry, OrderId, OrderType, Side, TimeInForce,
            },
        },
    },
    market_data::MarketDataRuntime,
};

use super::{
    EngineConfig, accounts::AccountTable, ladder::SideLadder, orders::OrderArena,
    orders::OrderHandle, orders::RestingOrder, structural_reject_reason,
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

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct MatchOutcome {
    /// Total taker quantity filled during the aggressive walk.
    filled_qty: u64,
    /// Index of the most recent taker execution report in the event buffer.
    last_taker_report_index: Option<usize>,
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
        sequencer: &mut EngineSequencer,
        market_data: &mut MarketDataRuntime,
        events: &mut Vec<TradingEvent>,
    ) -> DomainResult<()> {
        match entry {
            OrderEntry::NewOrder(order) => self.on_new_order(order, sequencer, market_data, events),
            OrderEntry::CancelOrder(cancel) => {
                self.on_cancel_order(cancel, sequencer, market_data, events)
            }
            OrderEntry::CancelReplace(replace) => {
                self.on_cancel_replace(replace, sequencer, market_data, events)
            }
            OrderEntry::MassCancel(cancel) => {
                self.on_mass_cancel(cancel, sequencer, market_data, events)
            }
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

    /// Handle a new order, including matching and optional resting.
    fn on_new_order(
        &mut self,
        order: NewOrder,
        sequencer: &mut EngineSequencer,
        market_data: &mut MarketDataRuntime,
        events: &mut Vec<TradingEvent>,
    ) -> DomainResult<()> {
        if !self.accounts.contains(order.account_id) {
            // todo - provide better rejection
            return self.reject_new(order, RejectReason::Malformed, sequencer, events);
        }
        if self.order_index.contains_key(&order.client_order_id) {
            // todo - provide better rejection
            return self.reject_new(order, RejectReason::Malformed, sequencer, events);
        }

        // ensure valid price
        if matches!(order.order_type, OrderType::Limit) {
            let Some(price) = order.price else {
                return self.reject_new(order, RejectReason::InvalidPrice, sequencer, events);
            };
            if !self.price_in_storage(price) {
                return self.reject_new(order, RejectReason::InvalidPrice, sequencer, events);
            }
        }

        // if it's a PostOnly order ensure that it doesn't cross the book
        if order.time_in_force == TimeInForce::PostOnly && self.would_cross(order.side, order.price)
        {
            self.push_report(
                sequencer,
                events,
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
            events,
        )?;

        // order completely filled - happy path
        if remaining_qty == 0 {
            return Ok(());
        }

        match (order.order_type, order.time_in_force) {
            (OrderType::Market, _) | (_, TimeInForce::Ioc) => {
                // market orders and IOC residuals never rest on the book.
                if match_outcome.filled_qty > 0 {
                    self.expire_taker_residual(events, match_outcome);
                    return Ok(());
                }

                // nothing filled and the order cannot rest, so it expires immediately.
                self.push_report(
                    sequencer,
                    events,
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
                let level_qty = self.rest_order(
                    order.client_order_id,
                    order.account_id,
                    order.side,
                    price,
                    remaining_qty,
                )?;
                self.publish_book_update(order.side, price, level_qty, market_data, events)?;
                // distinguish a pure add from an order that traded before resting.
                if match_outcome.filled_qty == 0 {
                    self.push_report(
                        sequencer,
                        events,
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
                    Ok(())
                }
            }
        }
    }

    /// Handle an order cancel by unlinking the original resting order.
    fn on_cancel_order(
        &mut self,
        cancel: CancelOrder,
        sequencer: &mut EngineSequencer,
        market_data: &mut MarketDataRuntime,
        events: &mut Vec<TradingEvent>,
    ) -> DomainResult<()> {
        let Some(handle) = self.order_index.get(&cancel.orig_client_order_id).copied() else {
            return self.reject_cancel(cancel, RejectReason::UnknownOrder, sequencer, events);
        };
        if self.orders.get(handle).map(|order| order.account_id) != Some(cancel.account_id) {
            return self.reject_cancel(cancel, RejectReason::UnknownOrder, sequencer, events);
        }

        let removed = self.unlink_and_free(handle);
        if let Some(order) = removed {
            let level_qty = self.current_level_qty(order.side, order.price);
            self.publish_book_update(order.side, order.price, level_qty, market_data, events)?;
        }
        self.push_report(
            sequencer,
            events,
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
        sequencer: &mut EngineSequencer,
        market_data: &mut MarketDataRuntime,
        events: &mut Vec<TradingEvent>,
    ) -> DomainResult<()> {
        if !self.price_in_storage(replace.price) {
            return self.reject_replace(replace, RejectReason::InvalidPrice, sequencer, events);
        }
        if self.order_index.contains_key(&replace.client_order_id)
            && replace.client_order_id != replace.orig_client_order_id
        {
            return self.reject_replace(replace, RejectReason::Malformed, sequencer, events);
        }

        let Some(handle) = self.order_index.get(&replace.orig_client_order_id).copied() else {
            return self.reject_replace(replace, RejectReason::UnknownOrder, sequencer, events);
        };
        let Some(current) = self.orders.get(handle).copied() else {
            return self.reject_replace(replace, RejectReason::UnknownOrder, sequencer, events);
        };
        if current.account_id != replace.account_id {
            return self.reject_replace(replace, RejectReason::UnknownOrder, sequencer, events);
        }

        if current.price == replace.price && replace.qty.raw() <= current.remaining_qty {
            // Same-price reductions stay in the existing FIFO position.
            self.preserve_priority_replace(handle, replace, current.remaining_qty)?;
            if replace.qty.raw() != current.remaining_qty {
                self.publish_book_update(
                    current.side,
                    current.price,
                    replace.qty.raw(),
                    market_data,
                    events,
                )?;
            }
            self.push_report(
                sequencer,
                events,
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
            self.publish_book_update(removed.side, removed.price, level_qty, market_data, events)?;
        }
        self.push_report(
            sequencer,
            events,
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
            events,
        )
    }

    /// Cancel all active orders for one account.
    fn on_mass_cancel(
        &mut self,
        cancel: MassCancel,
        sequencer: &mut EngineSequencer,
        market_data: &mut MarketDataRuntime,
        events: &mut Vec<TradingEvent>,
    ) -> DomainResult<()> {
        if !self.accounts.contains(cancel.account_id) {
            return self.push_report(
                sequencer,
                events,
                ReportSpec {
                    client_order_id: cancel.client_order_id,
                    account_id: cancel.account_id,
                    exec_type: ExecType::Rejected,
                    ord_status: OrdStatus::Rejected,
                    reject_reason: RejectReason::Malformed,
                    fill_price: None,
                    fill_qty: 0,
                    remaining_qty: 0,
                },
            );
        }

        let mut current = self
            .accounts
            .get(cancel.account_id)
            .and_then(|account| account.head);
        while let Some(handle) = current {
            // Capture the next account link before unlinking frees this slot.
            current = self.orders.get(handle).and_then(|order| order.account_next);
            if let Some(removed) = self.unlink_and_free(handle) {
                let level_qty = self.current_level_qty(removed.side, removed.price);
                self.publish_book_update(
                    removed.side,
                    removed.price,
                    level_qty,
                    market_data,
                    events,
                )?;
                self.push_report(
                    sequencer,
                    events,
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
            events,
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
        sequencer: &mut EngineSequencer,
        market_data: &mut MarketDataRuntime,
        events: &mut Vec<TradingEvent>,
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

            // one execution trades the smaller of taker leaves quantity and maker leaves quantity.
            let fill_qty = (*remaining_qty).min(maker.remaining_qty);
            *remaining_qty -= fill_qty;
            outcome.filled_qty += fill_qty;
            // apply book state first so all following events describe post-fill state.
            self.apply_fill(maker_handle, best_key, fill_qty);
            let maker_remaining = self
                .orders
                .get(maker_handle)
                .map_or(0, |order| order.remaining_qty);
            if maker_remaining == 0 {
                // fully filled makers leave both intrusive lists and the ID index now.
                self.unlink_and_free(maker_handle);
            }

            // market-data output is sequenced immediately after the deterministic state change.
            market_data.publish_trade(
                maker.order_id,
                taker_order_id,
                maker.price,
                fill_qty,
                &self.config.instrument(),
                events,
            )?;
            self.publish_book_update(
                maker.side,
                maker.price,
                maker_remaining,
                market_data,
                events,
            )?;

            // maker report reflects this fill and the maker's remaining book quantity.
            self.push_report(
                sequencer,
                events,
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
            let taker_report_index = events.len();
            self.push_report(
                sequencer,
                events,
                ReportSpec {
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
                },
            )?;
            // keep the last taker report address so IOC/market expiry can mark it terminal.
            outcome.last_taker_report_index = Some(taker_report_index);
        }

        Ok(outcome)
    }

    /// Mark the final taker fill report terminal when residual liquidity expires.
    fn expire_taker_residual(&self, events: &mut [TradingEvent], outcome: MatchOutcome) {
        let Some(index) = outcome.last_taker_report_index else {
            return;
        };
        let Some(TradingEvent::ExecutionReport(report)) = events.get_mut(index) else {
            return;
        };
        report.ord_status = OrdStatus::Expired;
        report.remaining_qty = 0;
    }

    /// Apply a fill to one maker order and its price-level aggregate.
    fn apply_fill(
        &mut self,
        maker_handle: OrderHandle,
        key: super::ladder::LevelKey,
        fill_qty: u64,
    ) {
        if let Some(order) = self.orders.get_mut(maker_handle) {
            order.remaining_qty -= fill_qty;
        }
        let side = self
            .orders
            .get(maker_handle)
            .map_or(Side::Sell, |order| order.side);
        let level = self
            .side_mut(side)
            .get_level_mut_or_activate(key)
            .expect("maker level exists");
        level.total_qty -= fill_qty;
    }

    /// Insert an order into the resting indexes.
    fn rest_order(
        &mut self,
        order_id: OrderId,
        account_id: AccountId,
        side: Side,
        price: Price,
        remaining_qty: u64,
    ) -> DomainResult<u64> {
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
        let handle = self
            .orders
            .allocate(order)
            .ok_or(DomainError::InvalidInstrumentSpec)?;

        // a resting order is visible through price, account, and client-order indexes.
        let level_qty = self.append_to_level(side, key, handle)?;
        self.append_to_account(account_id, handle)?;
        self.order_index.insert(order_id, handle);
        Ok(level_qty)
    }

    /// Append an order to the FIFO chain at a price level.
    fn append_to_level(
        &mut self,
        side: Side,
        key: super::ladder::LevelKey,
        handle: OrderHandle,
    ) -> DomainResult<u64> {
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
    fn append_to_account(
        &mut self,
        account_id: AccountId,
        handle: OrderHandle,
    ) -> DomainResult<()> {
        let account = self
            .accounts
            .get_mut(account_id)
            .ok_or(DomainError::Malformed)?;
        let tail = account.tail;
        // Keep a second intrusive list so mass cancel never scans unrelated accounts.
        if let Some(tail) = tail {
            self.orders
                .get_mut(tail)
                .expect("account tail")
                .account_next = Some(handle);
            self.orders.get_mut(handle).expect("order").account_prev = Some(tail);
        }
        if account.head.is_none() {
            account.head = Some(handle);
        }
        account.tail = Some(handle);
        account.open_order_count += 1;
        Ok(())
    }

    /// Update an existing order in place without changing FIFO priority.
    fn preserve_priority_replace(
        &mut self,
        handle: OrderHandle,
        replace: CancelReplace,
        old_qty: u64,
    ) -> DomainResult<()> {
        let current = self
            .orders
            .get(handle)
            .copied()
            .ok_or(DomainError::Malformed)?;
        let key = self.side(current.side).key_for_price(current.price)?;
        let order = self.orders.get_mut(handle).ok_or(DomainError::Malformed)?;
        order.order_id = replace.client_order_id;
        order.remaining_qty = replace.qty.raw();
        // The order keeps its handle; only the externally visible ID changes.
        self.order_index.remove(&replace.orig_client_order_id);
        self.order_index.insert(replace.client_order_id, handle);

        let level = self.side_mut(current.side).get_level_mut_or_activate(key)?;
        level.total_qty -= old_qty - replace.qty.raw();
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

        let key = self
            .side(order.side)
            .key_for_price(order.price)
            .expect("stored price");
        let emptied = {
            // Update level aggregates before possibly clearing ladder occupancy.
            let level = self
                .side_mut(order.side)
                .get_level_mut_or_activate(key)
                .expect("stored level");
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
    fn publish_book_update(
        &mut self,
        side: Side,
        price: Price,
        level_qty: u64,
        market_data: &mut MarketDataRuntime,
        events: &mut Vec<TradingEvent>,
    ) -> DomainResult<()> {
        let best_bid = self.best_bid();
        let best_ask = self.best_ask();
        market_data.publish_l2_update(side, price, level_qty, events)?;
        market_data.publish_l1_update_if_changed(best_bid, best_ask, events)
    }

    /// Return the aggregate quantity currently visible at a level after mutation.
    fn current_level_qty(&self, side: Side, price: Price) -> u64 {
        let Ok(key) = self.side(side).key_for_price(price) else {
            return 0;
        };
        self.side(side)
            .get_level(key)
            .map_or(0, |level| level.total_qty)
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
        sequencer: &mut EngineSequencer,
        events: &mut Vec<TradingEvent>,
    ) -> DomainResult<()> {
        self.push_report(
            sequencer,
            events,
            ReportSpec {
                client_order_id: order.client_order_id,
                account_id: order.account_id,
                exec_type: ExecType::Rejected,
                ord_status: OrdStatus::Rejected,
                reject_reason: reason,
                fill_price: None,
                fill_qty: 0,
                remaining_qty: order.qty.raw(),
            },
        )
    }

    /// Emit a rejected cancel report.
    fn reject_cancel(
        &mut self,
        cancel: CancelOrder,
        reason: RejectReason,
        sequencer: &mut EngineSequencer,
        events: &mut Vec<TradingEvent>,
    ) -> DomainResult<()> {
        self.push_report(
            sequencer,
            events,
            ReportSpec {
                client_order_id: cancel.client_order_id,
                account_id: cancel.account_id,
                exec_type: ExecType::Rejected,
                ord_status: OrdStatus::Rejected,
                reject_reason: reason,
                fill_price: None,
                fill_qty: 0,
                remaining_qty: 0,
            },
        )
    }

    /// Emit a rejected cancel-replace report.
    fn reject_replace(
        &mut self,
        replace: CancelReplace,
        reason: RejectReason,
        sequencer: &mut EngineSequencer,
        events: &mut Vec<TradingEvent>,
    ) -> DomainResult<()> {
        self.push_report(
            sequencer,
            events,
            ReportSpec {
                client_order_id: replace.client_order_id,
                account_id: replace.account_id,
                exec_type: ExecType::Rejected,
                ord_status: OrdStatus::Rejected,
                reject_reason: reason,
                fill_price: None,
                fill_qty: 0,
                remaining_qty: replace.qty.raw(),
            },
        )
    }

    /// Append one deterministic execution report to the output buffer.
    fn push_report(
        &mut self,
        sequencer: &mut EngineSequencer,
        events: &mut Vec<TradingEvent>,
        spec: ReportSpec,
    ) -> DomainResult<()> {
        events.push(TradingEvent::ExecutionReport(ExecutionReport {
            engine_seq: sequencer.next_engine_seq()?,
            client_order_id: spec.client_order_id,
            account_id: spec.account_id,
            exec_type: spec.exec_type,
            ord_status: spec.ord_status,
            reject_reason: spec.reject_reason,
            fill_price: spec.fill_price,
            fill_qty: spec.fill_qty,
            remaining_qty: spec.remaining_qty,
        }));
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

/// Return the side that provides liquidity to a taker on `taker_side`.
const fn best_side(taker_side: Side) -> Side {
    match taker_side {
        Side::Buy => Side::Sell,
        Side::Sell => Side::Buy,
    }
}

#[allow(dead_code)]
const _: fn(DomainError) -> RejectReason = structural_reject_reason;
