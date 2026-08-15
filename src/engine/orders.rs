//! Resting-order nodes, generation-checked handles, and fixed-capacity storage.
//!
//! This module owns the engine's actual resting-order records. The arena is
//! allocated to its configured maximum size at startup and reuses those slots
//! for the lifetime of the engine. Adding or removing a resting order therefore
//! does not grow storage on the order-entry hot path.
//!
//! # Main components
//!
//! - [`OrderHandle`] identifies one arena slot and the generation of the order
//!   that occupied it. The generation prevents a handle retained from an older
//!   order from resolving after that slot has been reused.
//! - [`RestingOrder`] stores the order's visible state and two independent sets
//!   of intrusive links: one for FIFO order at its price level and one for the
//!   owning account's active-order chain.
//! - `OrderSlot` is the physical arena entry. An occupied slot contains an
//!   order; a free slot instead carries the next index in the freelist.
//! - [`OrderArena`] owns every slot, the freelist head, and the current number
//!   of occupied slots.
//!
//! # Allocation and reuse
//!
//! At construction, every slot is linked into a deterministic freelist.
//! `allocate` pops its head, stores an order, and returns a handle containing the
//! slot's current generation. `free` removes the order, advances the generation,
//! and pushes the slot back onto the freelist. Both operations are constant-time
//! and use only preallocated memory.
//!
//! # Intrusive links
//!
//! Price levels and accounts store only the head and tail `OrderHandle` values
//! for their chains. The previous and next links live in `RestingOrder`, avoiding
//! separate list-node allocations. Price-level links preserve FIFO matching;
//! account links let mass cancel visit one account without scanning the arena.
//!
//! # Important invariants
//!
//! A valid handle must have an in-range index and a generation equal to the
//! current slot generation. An occupied slot contains an order and is not part
//! of the freelist. A free slot contains no order and is reachable from
//! `free_head`. `active` equals the number of occupied slots, and a slot's
//! generation is never zero.
//!
//! `book.rs` allocates, links, unlinks, and frees orders. `ladder.rs` stores
//! price-level chain endpoints, `accounts.rs` stores account-chain endpoints and
//! account-scoped lookup handles, `matching.rs` updates maker leaves quantity,
//! and `capacity.rs` checks whether the freelist has a slot available.

use crate::domain::trading::order::{AccountId, OrderId, Side};

/// Stable reference to one generation of an arena slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct OrderHandle {
    /// Physical slot index inside `OrderArena`.
    index: u32,
    /// Slot generation captured when the order was allocated.
    generation: u32,
}

/// Resting order node linked into price-level and account chains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RestingOrder {
    /// Client-assigned order ID for the resting order.
    pub(crate) order_id: OrderId,
    /// Account that owns the resting order.
    pub(crate) account_id: AccountId,
    /// Side where this order rests.
    pub(crate) side: Side,
    /// Limit price where this order rests.
    pub(crate) price: crate::domain::Price,
    /// Leaves quantity still visible and available to match.
    pub(crate) remaining_qty: u64,
    /// Older FIFO neighbor at the same price level.
    pub(crate) level_prev: Option<OrderHandle>,
    /// Newer FIFO neighbor at the same price level.
    pub(crate) level_next: Option<OrderHandle>,
    /// Previous live order in the owning account's chain.
    pub(crate) account_prev: Option<OrderHandle>,
    /// Next live order in the owning account's chain.
    pub(crate) account_next: Option<OrderHandle>,
}

/// One physical slot, represented either as an order or a freelist node.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderSlot {
    /// Generation required for a handle to resolve this slot.
    generation: u32,
    /// Resting order stored in this slot, if occupied.
    order: Option<RestingOrder>,
    /// Next freelist index when this slot is unoccupied.
    next_free: Option<u32>,
}

/// Fixed-capacity owner of all resting-order slots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrderArena {
    /// Preallocated storage for every possible resting order.
    slots: Vec<OrderSlot>,
    /// First reusable slot, or `None` when the arena is full.
    free_head: Option<u32>,
    /// Number of slots currently containing resting orders.
    active: usize,
}

impl OrderArena {
    /// Allocate a fixed number of reusable order slots.
    pub(crate) fn with_capacity(capacity: u32) -> Self {
        let mut slots = Vec::with_capacity(capacity as usize);
        for index in 0..capacity {
            // initial slots form one ascending chain from zero to capacity - 1.
            let next_free = (index + 1 < capacity).then_some(index + 1);
            slots.push(OrderSlot {
                generation: 1,
                order: None,
                next_free,
            });
        }

        // the deterministic freelist covers every slot exactly once.
        Self {
            slots,
            free_head: (capacity > 0).then_some(0),
            active: 0,
        }
    }

    /// Store a resting order in the next free slot.
    pub(crate) fn allocate(&mut self, order: RestingOrder) -> Option<OrderHandle> {
        let index = self.free_head?;
        let slot = &mut self.slots[index as usize];
        // detach the slot from the freelist before making its handle visible.
        self.free_head = slot.next_free;
        slot.next_free = None;
        slot.order = Some(order);
        self.active += 1;

        Some(OrderHandle {
            index,
            generation: slot.generation,
        })
    }

    /// Remove an occupied slot and return it to the freelist.
    pub(crate) fn free(&mut self, handle: OrderHandle) -> Option<RestingOrder> {
        // capture the old head before borrowing the slot so the released slot
        // can be pushed onto the front of the freelist.
        let free_head = self.free_head;
        let slot = self.valid_slot_mut(handle)?;
        let order = slot.order.take()?;
        // advance the generation before reuse so every outstanding old handle
        // becomes invalid; reserve zero as a non-generation after wraparound.
        slot.generation = slot.generation.wrapping_add(1).max(1);
        slot.next_free = free_head;
        self.free_head = Some(handle.index);
        self.active -= 1;
        Some(order)
    }

    /// Return an order for a currently valid handle.
    pub(crate) fn get(&self, handle: OrderHandle) -> Option<&RestingOrder> {
        self.valid_slot(handle)?.order.as_ref()
    }

    /// Return a mutable order for a currently valid handle.
    pub(crate) fn get_mut(&mut self, handle: OrderHandle) -> Option<&mut RestingOrder> {
        self.valid_slot_mut(handle)?.order.as_mut()
    }

    /// Return the number of occupied arena slots.
    pub(crate) fn active(&self) -> usize {
        self.active
    }

    /// Return whether another resting order can be stored without growing the arena.
    pub(crate) const fn has_free_slot(&self) -> bool {
        self.free_head.is_some()
    }

    /// Return a slot only when the handle generation still matches.
    fn valid_slot(&self, handle: OrderHandle) -> Option<&OrderSlot> {
        // bounds and generation are both part of handle validity.
        let slot = self.slots.get(handle.index as usize)?;
        (slot.generation == handle.generation).then_some(slot)
    }

    /// Return a mutable slot only when the handle generation still matches.
    fn valid_slot_mut(&mut self, handle: OrderHandle) -> Option<&mut OrderSlot> {
        // perform the same validation before granting exclusive slot access.
        let slot = self.slots.get_mut(handle.index as usize)?;
        (slot.generation == handle.generation).then_some(slot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Price, rd::InstrumentSpec, trading::order::AccountId};

    fn order(id: u64) -> RestingOrder {
        let instrument = InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument");
        RestingOrder {
            order_id: OrderId::new(id).expect("order"),
            account_id: AccountId::new(1).expect("account"),
            side: Side::Buy,
            price: Price::new(100, &instrument).expect("price"),
            remaining_qty: 10,
            level_prev: None,
            level_next: None,
            account_prev: None,
            account_next: None,
        }
    }

    #[test]
    fn arena_rejects_stale_handles_after_free() {
        let mut arena = OrderArena::with_capacity(1);
        let handle = arena.allocate(order(1)).expect("slot");
        assert_eq!(arena.get(handle).expect("order").order_id.raw(), 1);
        arena.free(handle).expect("freed");
        assert!(arena.get(handle).is_none());

        let next = arena.allocate(order(2)).expect("reused slot");
        assert_ne!(handle, next);
        assert_eq!(arena.get(next).expect("order").order_id.raw(), 2);
    }
}
