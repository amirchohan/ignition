use crate::domain::trading::order::{AccountId, OrderId, Side};

/// Generation-checked index into the resting-order arena.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct OrderHandle {
    /// Arena slot index.
    index: u32,
    /// Slot generation used to reject stale handles after reuse.
    generation: u32,
}

impl OrderHandle {
    /// Return the arena slot index portion of this handle.
    pub(crate) const fn index(self) -> usize {
        self.index as usize
    }
}

/// Resting order node linked into price-level and account FIFO chains.
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
    /// Quantity still visible on the book.
    pub(crate) remaining_qty: u64,
    /// Previous order at the same price level.
    pub(crate) level_prev: Option<OrderHandle>,
    /// Next order at the same price level.
    pub(crate) level_next: Option<OrderHandle>,
    /// Previous active order for the same account.
    pub(crate) account_prev: Option<OrderHandle>,
    /// Next active order for the same account.
    pub(crate) account_next: Option<OrderHandle>,
}

/// One arena slot, either occupied by an order or linked into the freelist.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OrderSlot {
    /// Current generation for handles pointing at this slot.
    generation: u32,
    /// Resting order stored in this slot, if occupied.
    order: Option<RestingOrder>,
    /// Next free slot index when this slot is on the freelist.
    next_free: Option<u32>,
}

/// Fixed-capacity resting-order arena with generation-checked handles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OrderArena {
    /// Fixed storage for all resting-order slots.
    slots: Vec<OrderSlot>,
    /// Head of the slot freelist.
    free_head: Option<u32>,
    /// Number of occupied slots.
    active: usize,
}

impl OrderArena {
    /// Allocate a fixed number of reusable order slots.
    pub(crate) fn with_capacity(capacity: u32) -> Self {
        let mut slots = Vec::with_capacity(capacity as usize);
        for index in 0..capacity {
            let next_free = (index + 1 < capacity).then_some(index + 1);
            slots.push(OrderSlot {
                generation: 1,
                order: None,
                next_free,
            });
        }

        // Initial freelist order is deterministic and covers every slot exactly once.
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
        // Pop this slot from the freelist before exposing its handle.
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
        let free_head = self.free_head;
        let slot = self.valid_slot_mut(handle)?;
        let order = slot.order.take()?;
        // Bump generation so stale handles cannot address the reused slot.
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
        let slot = self.slots.get(handle.index())?;
        (slot.generation == handle.generation).then_some(slot)
    }

    /// Return a mutable slot only when the handle generation still matches.
    fn valid_slot_mut(&mut self, handle: OrderHandle) -> Option<&mut OrderSlot> {
        let slot = self.slots.get_mut(handle.index())?;
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
