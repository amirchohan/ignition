//! Read-only preflight checks for fixed-capacity engine resources.
//!
//! The engine uses bounded, preallocated storage instead of growing its main
//! book structures on demand. Before a command mutates the book, this module
//! answers one narrow question: would the command need a resting-order slot or
//! side-local ladder page that cannot be made available?
//!
//! A capacity result is not general order validation. Invalid accounts,
//! duplicate IDs, missing orders, invalid prices, and commands that never rest
//! are left to the normal lifecycle handlers. This distinction prevents a full
//! resource from hiding the command's real rejection reason.
//!
//! # Main components
//!
//! This file defines no storage types of its own. Its methods inspect the
//! components owned by [`Engine`]:
//!
//! - `OrderArena` reports whether a new resting-order slot is immediately free.
//! - `SideLadder` reports whether a price already has an active page or its
//!   side-local page pool can activate one.
//! - `AccountTable` distinguishes structurally valid, account-scoped live
//!   orders from commands that will be rejected before consuming capacity.
//! - [`OrderProjection`] reports how much of an incoming order would execute
//!   and how much would remain to rest, without changing the book.
//! - [`CapacityResource`] identifies the bounded resource that prevents the
//!   command from resting.
//!
//! # Command flow
//!
//! [`Engine::execute`] calls `capacity_pressure` before dispatching to
//! `lifecycle.rs`. Cancels and mass cancels always pass because they release or
//! preserve capacity. New orders and replacements use separate checks because
//! they make resources reusable in different ways: matching can free a maker's
//! arena slot for a new order, while a replacement first removes its own order
//! and may also release that order's ladder page.
//!
//! # Important invariants
//!
//! These checks never mutate or allocate. They only report pressure when a
//! structurally processable command would leave residual quantity that must
//! rest. A fully executable order, a non-resting order, or a command that will
//! be rejected for another reason must not be reported as a capacity failure.
//! The preflight must also account for resources deterministically released by
//! the command itself so a full book can still make forward progress.

use super::*;

impl Engine {
    /// Return the bounded resource that would prevent this command from resting.
    #[inline]
    pub(super) fn capacity_pressure(&self, entry: OrderEntry) -> Result<Option<CapacityResource>, EngineError> {
        match entry {
            OrderEntry::NewOrder(order) => self.new_order_capacity_pressure(order),
            OrderEntry::CancelReplace(replace) => self.replace_capacity_pressure(replace),
            // cancellation never needs a new resting slot or ladder page and
            // must remain available when the book is full.
            OrderEntry::CancelOrder(_) | OrderEntry::MassCancel(_) => Ok(None),
        }
    }

    /// Return capacity pressure for a new order that may leave resting quantity.
    #[inline]
    pub(super) fn new_order_capacity_pressure(&self, order: NewOrder) -> Result<Option<CapacityResource>, EngineError> {
        // malformed account ownership and duplicate account-scoped IDs are
        // structural rejections, so they cannot consume resting capacity.
        if !self.accounts.contains(order.account_id) || self.accounts.contains_order(order.account_id, order.client_order_id) {
            return Ok(None);
        }
        // market and IOC orders never place residual quantity on the book.
        if order.order_type != OrderType::Limit || order.time_in_force == TimeInForce::Ioc {
            return Ok(None);
        }
        // a missing price is rejected by lifecycle validation rather than
        // reported as capacity pressure.
        let Some(price) = order.price else { return Ok(None) };
        // an unrepresentable price is structurally invalid, while a crossing
        // post-only order is canceled without resting.
        if !self.price_in_storage(price)
            || (order.time_in_force == TimeInForce::PostOnly && self.would_cross(order.side, order.price).map_err(engine_error)?)
        {
            return Ok(None);
        }

        let projection = self.project_order(order.account_id, order.side, order.price, order.qty.raw())?;
        // an order that would fill completely does not need resting resources.
        if projection.residual_qty == 0 {
            return Ok(None);
        }

        // a residual after any execution means every encountered maker was
        // fully removed, so at least one shared arena slot will be reusable.
        if !self.orders.has_free_slot() && projection.executable_qty == 0 {
            return Ok(Some(CapacityResource::RestingOrders));
        }

        let key = self.side(order.side).key_for_price(price).map_err(engine_error)?;
        Ok((!self.side(order.side).can_rest_at(key)).then_some(CapacityResource::ActivePages))
    }

    /// Return capacity pressure for a replacement that may re-enter the book.
    #[inline]
    pub(super) fn replace_capacity_pressure(&self, replace: CancelReplace) -> Result<Option<CapacityResource>, EngineError> {
        // invalid prices and duplicate replacement IDs are rejected before the
        // replacement can consume a resource.
        if !self.price_in_storage(replace.price)
            || (self.accounts.contains_order(replace.account_id, replace.client_order_id)
                && replace.client_order_id != replace.orig_client_order_id)
        {
            return Ok(None);
        }
        // an unknown or stale original order reaches the lifecycle handler so
        // it can produce the correct rejection instead of a capacity failure.
        let Some(handle) = self.accounts.order_handle(replace.account_id, replace.orig_client_order_id) else {
            return Ok(None);
        };
        let current = self.orders.get(handle).copied().ok_or(EngineError::InvariantViolation)?;
        if current.account_id != replace.account_id || current.order_id != replace.orig_client_order_id {
            return Err(EngineError::InvariantViolation);
        }
        // a same-price quantity reduction stays in its existing arena slot and
        // ladder position, so it cannot require new capacity.
        if current.price == replace.price && replace.qty.raw() <= current.remaining_qty {
            return Ok(None);
        }

        let projection = self.project_order(replace.account_id, current.side, Some(replace.price), replace.qty.raw())?;
        // a replacement that would fill completely needs no destination page.
        if projection.residual_qty == 0 {
            return Ok(None);
        }

        // the original order is removed before re-entry, so its arena slot is
        // always reusable; only the side-local destination page can be blocked.
        let target_key = self.side(current.side).key_for_price(replace.price).map_err(engine_error)?;
        if self.side(current.side).can_rest_at(target_key) {
            return Ok(None);
        }
        // when the page pool is full, moving the only order from its source page
        // releases that page in time to activate the destination page.
        let source_key = self.side(current.side).key_for_price(current.price).map_err(engine_error)?;
        Ok((!self.side(current.side).page_released_after_removing_order(source_key)).then_some(CapacityResource::ActivePages))
    }
}
