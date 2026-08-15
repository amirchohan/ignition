//! Order-book queries and cross-index resting-order mutations.
//!
//! This module is the coordination layer for the engine's live book. It
//! provides read-only queries, inserts and removes resting orders, traverses
//! orders in deterministic price-time order, and publishes market data after a
//! visible level changes.
//!
//! `book.rs` does not own a separate data structure. Its methods operate on the
//! components already owned by [`Engine`]:
//! - `SideLadder` owns price-level metadata for bids and asks. Each level stores
//!   its FIFO endpoints, aggregate visible quantity, and resting-order count.
//! - `OrderArena` owns the actual `RestingOrder` nodes. Each node contains links
//!   for both its price-level FIFO chain and its account's active-order chain.
//! - `AccountTable` owns per-account totals and maps
//!   `(account_id, client_order_id)` to the corresponding arena handle.
//!
//! # Main responsibilities
//!
//! - The public query methods expose top of book, account totals, individual
//!   resting orders, and deterministic book traversal without exposing mutable
//!   engine storage.
//! - The linking methods make a new resting order visible through its price
//!   level, account chain, and account-scoped order lookup.
//! - The unlinking methods remove an order from all of those views before its
//!   arena slot can be reused.
//! - The traversal helpers follow ladder occupancy and FIFO links instead of
//!   scanning unused prices or arena slots.
//! - The market-data helper publishes the changed L2 level followed by an L1
//!   snapshot derived from the already-mutated book.
//!
//! # Important invariants
//!
//! A live resting order must agree across every index: its ladder level, arena
//! node, account chain, account totals, and account-scoped lookup must all
//! describe the same order. Orders append at the tail of a price level to
//! preserve FIFO priority. Removal unlinks every index before freeing the arena
//! slot so a generation-checked handle cannot be reused while it is still
//! reachable. All visible state changes happen before their market-data events
//! are emitted.
//!
//! Lifecycle and matching code call these helpers to mutate the book.
//! `ladder.rs`, `orders.rs`, and `accounts.rs` provide the underlying storage;
//! `capacity.rs` checks bounded resources before an insertion reaches this
//! module.

use super::*;

impl Engine {
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
        // resolve through the account-scoped key first, then use the
        // generation-checked handle to read the arena slot.
        let handle = self.accounts.order_handle(account_id, order_id)?;
        let order = self.orders.get(handle).copied()?;
        // the composite lookup must never resolve an order owned by another
        // account.
        debug_assert_eq!(order.account_id, account_id);
        Some(RestingOrderView {
            order_id: order.order_id,
            account_id: order.account_id,
            side: order.side,
            price: order.price,
            remaining_qty: order.remaining_qty,
        })
    }

    /// Visit resting orders in deterministic book order.
    pub fn try_for_each_resting_order<E>(&self, mut visit: impl FnMut(RestingOrderView) -> Result<(), E>) -> Result<(), E> {
        // snapshots and risk iteration use one stable ordering: bids from best
        // to worst first, then asks from best to worst.
        self.try_for_each_side_order(Side::Buy, &mut visit)?;
        self.try_for_each_side_order(Side::Sell, &mut visit)
    }

    /// Insert an order into the resting indexes.
    #[inline]
    pub(super) fn rest_order(
        &mut self,
        order_id: OrderId,
        account_id: AccountId,
        side: Side,
        price: Price,
        remaining_qty: u64,
    ) -> DomainResult<u64> {
        let key = self.side(side).key_for_price(price)?;
        // check both bounded resources before reserving either one. the engine
        // is single-writer, so availability cannot change before insertion.
        if !self.orders.has_free_slot() || !self.side(side).can_rest_at(key) {
            return Err(DomainError::InvalidInstrumentSpec);
        }
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

        // make the allocated order reachable through its price, account, and
        // client-order indexes before returning it to the caller.
        let level_qty = self.append_to_level(side, key, handle)?;
        self.append_to_account(account_id, handle)?;
        self.accounts.insert_order(account_id, order_id, handle);
        Ok(level_qty)
    }

    /// Append an order to the FIFO chain at a price level.
    pub(super) fn append_to_level(&mut self, side: Side, key: ladder::LevelKey, handle: OrderHandle) -> DomainResult<u64> {
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
        // the first order initializes both ends; later orders only advance the
        // tail because their neighbor links were patched above.
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
    pub(super) fn append_to_account(&mut self, account_id: AccountId, handle: OrderHandle) -> DomainResult<()> {
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

        // keep a second intrusive list so mass cancel never scans unrelated
        // accounts or allocates separate list nodes.
        if let Some(tail) = tail {
            self.orders.get_mut(tail).expect("account tail").account_next = Some(handle);
            self.orders.get_mut(handle).expect("order").account_prev = Some(tail);
        }
        Ok(())
    }

    /// Update an existing order in place without changing FIFO priority.
    #[inline]
    pub(super) fn preserve_priority_replace(&mut self, handle: OrderHandle, replace: CancelReplace, old_qty: u64) -> DomainResult<()> {
        let current = self.orders.get(handle).copied().ok_or(DomainError::Malformed)?;
        let key = self.side(current.side).key_for_price(current.price)?;
        let order = self.orders.get_mut(handle).ok_or(DomainError::Malformed)?;
        order.order_id = replace.client_order_id;
        order.remaining_qty = replace.qty.raw();
        // the order keeps its arena slot and FIFO position; only the externally
        // visible ID and remaining quantity change.
        self.accounts
            .replace_order_id(replace.account_id, replace.orig_client_order_id, replace.client_order_id, handle);

        let level = self.side_mut(current.side).get_level_mut_or_activate(key)?;
        // this path only permits a same-price quantity reduction, so both
        // visible aggregates decrease by the removed quantity.
        level.total_qty -= old_qty - replace.qty.raw();
        if let Some(account) = self.accounts.get_mut(current.account_id) {
            account.open_notional -= raw_notional(current.price, old_qty - replace.qty.raw());
        }
        Ok(())
    }

    /// Remove one resting order from every index and free its arena slot.
    #[inline]
    pub(super) fn unlink_and_free(&mut self, handle: OrderHandle) -> Option<RestingOrder> {
        let order = self.orders.get(handle).copied()?;
        // unlink every index before returning the arena slot to the freelist so
        // no live path can retain a handle whose slot may be reused.
        self.unlink_from_level(handle, order);
        self.unlink_from_account(handle, order);
        let removed_handle = self.accounts.remove_order(order.account_id, order.order_id);
        debug_assert_eq!(removed_handle, Some(handle));
        self.orders.free(handle)
    }

    /// Remove one resting order from its price-level FIFO chain.
    pub(super) fn unlink_from_level(&mut self, handle: OrderHandle, order: RestingOrder) {
        // patch neighboring FIFO links so arbitrary cancels stay constant-time.
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
            // update level aggregates before possibly clearing ladder occupancy.
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
    pub(super) fn unlink_from_account(&mut self, handle: OrderHandle, order: RestingOrder) {
        // patch account-list neighbors independently from price-level FIFO links.
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
    pub(super) fn next_occupied_key(&self, side: Side, current: ladder::LevelKey) -> Option<ladder::LevelKey> {
        match side {
            Side::Buy => self.side(side).occupied_before(current),
            Side::Sell => self.side(side).occupied_after(current),
        }
    }

    /// Return whether a post-only order would cross the current opposite best.
    #[inline]
    pub(super) fn would_cross(&self, side: Side, price: Option<Price>) -> bool {
        // an absent limit cannot be placed passively and is therefore treated
        // as crossing for the post-only decision.
        let Some(price) = price else {
            return true;
        };
        match side {
            Side::Buy => self.best_ask().is_some_and(|(ask, _)| price >= ask),
            Side::Sell => self.best_bid().is_some_and(|(bid, _)| price <= bid),
        }
    }

    /// Return whether an opposite price is executable for this incoming order.
    #[inline]
    pub(super) fn is_executable(side: Side, limit_price: Option<Price>, opposite_price: Price) -> bool {
        // no limit represents a marketable order; otherwise buys may trade at
        // or below their limit and sells at or above theirs.
        match (side, limit_price) {
            (_, None) => true,
            (Side::Buy, Some(limit)) => limit >= opposite_price,
            (Side::Sell, Some(limit)) => limit <= opposite_price,
        }
    }

    /// Return whether a price is representable by this book instance.
    #[inline]
    pub(super) fn price_in_storage(&self, price: Price) -> bool {
        price >= self.config.storage_min_price() && price <= self.config.storage_max_price()
    }

    /// Emit market data for a visible book-level mutation.
    #[inline]
    pub(super) fn publish_book_update(
        &mut self,
        side: Side,
        price: Price,
        level_qty: u64,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
    ) -> DomainResult<()> {
        // read the new top of book after mutation so L1 describes the same
        // state as the absolute L2 update.
        let best_bid = self.best_bid();
        let best_ask = self.best_ask();
        // preserve deterministic event ordering: changed depth first, then the
        // resulting top-of-book snapshot.
        market_data.publish_l2_update(sequencer, side, price, level_qty)?;
        market_data.publish_l1_update(sequencer, best_bid, best_ask)
    }

    /// Return the aggregate quantity currently visible at a level after mutation.
    #[inline]
    pub(super) fn current_level_qty(&self, side: Side, price: Price) -> u64 {
        // an unrepresentable or inactive level has no visible book quantity.
        let Ok(key) = self.side(side).key_for_price(price) else {
            return 0;
        };
        self.side(side).get_level(key).map_or(0, |level| level.total_qty)
    }

    /// Visit one side in price-time priority order.
    pub(super) fn try_for_each_side_order<E>(
        &self,
        side: Side,
        visit: &mut impl FnMut(RestingOrderView) -> Result<(), E>,
    ) -> Result<(), E> {
        // ladder occupancy jumps directly between active prices in the side's
        // best-to-worst direction.
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

    /// Collect resting-order handles for one side in price-time order.
    pub(super) fn collect_side_handles(&self, side: Side, out: &mut Vec<OrderHandle>) {
        // drain cannot follow links after freeing their arena slots, so capture
        // the stable handle order before any mutation begins.
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
    #[inline]
    pub(super) fn side(&self, side: Side) -> &SideLadder {
        match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        }
    }

    /// Return mutable state for one side of the book.
    #[inline]
    pub(super) fn side_mut(&mut self, side: Side) -> &mut SideLadder {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }
}
