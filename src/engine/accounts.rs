//! Account-owned book state and account-scoped active-order lookup.
//!
//! This module answers two different kinds of question for the matching
//! engine:
//!
//! 1. "What book state belongs to this account?" This is answered by indexing
//!    directly into the dense `accounts` array with an account ID.
//! 2. "Where is this account's live order?" This is answered by looking up the
//!    pair `(account_id, client_order_id)` in `active_orders`.
//!
//! The account ID must be part of the order lookup because a client order ID is
//! only unique within its account. Account 10 and account 20 may both have a
//! live order called 500, and those orders must remain completely independent.
//!
//! # Main components
//!
//! - `AccountOrderKey` is the internal key for one live order. It packs the
//!   account ID into the upper 64 bits of a `u128` and the client order ID into
//!   the lower 64 bits. No identifier bits are discarded, so two distinct
//!   `(account_id, client_order_id)` pairs cannot become the same key before
//!   the hash map hashes them.

//! - [`AccountBookState`] is the small amount of state maintained for one
//!   account. It stores the ends of that account's active-order chain plus its
//!   current resting-order count and raw open notional.

//! - [`AccountTable`] owns all account state and the active-order lookup. The
//!   account array is dense because account IDs are bounded at startup; the
//!   order lookup is pre-sized to the engine's maximum resting-order count.
//!
//! # Relationship to order storage
//!
//! This module does not store the resting orders themselves. Orders live in
//! `OrderArena`, and this module stores `OrderHandle` values that point into
//! that arena. The account chain is intrusive: its previous and next links live
//! inside each resting order. `AccountBookState` only needs the head and tail.
//! This lets mass cancel walk one account's orders without scanning the whole
//! book or allocating a separate list node for every order.

use crate::domain::trading::order::{AccountId, OrderId};
use hashbrown::HashMap;

use super::orders::OrderHandle;

/// Internal identity of one currently resting order within its owning account.
/// This is a packed value rather than a tuple so the hash map handles one small,
/// copyable integer key. Packing is lossless: the account occupies the upper
/// half and the client order ID occupies the lower half.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct AccountOrderKey(
    /// Account ID in the upper 64 bits and client order ID in the lower 64 bits.
    u128,
);

impl AccountOrderKey {
    /// Pack both identifiers without losing any bits.
    const fn new(account_id: AccountId, client_order_id: OrderId) -> Self {
        // distinct ID pairs produce distinct packed values because every bit of
        // both wire identifiers is preserved. the hash map still performs normal
        // hashing and collision resolution on this value.
        Self((account_id.raw() as u128) << u64::BITS | client_order_id.raw() as u128)
    }
}

/// Book state maintained for one configured account.
///
/// The chain endpoints allow the engine to reach only this account's orders.
/// Counts and notional are updated alongside book mutations so risk and control
/// queries do not need to scan the order book.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AccountBookState {
    /// Oldest reachable live order in this account's intrusive order chain.
    pub(crate) head: Option<OrderHandle>,
    /// Newest reachable live order in this account's intrusive order chain.
    pub(crate) tail: Option<OrderHandle>,
    /// Number of this account's orders that are currently resting on the book.
    pub(crate) open_order_count: u32,
    /// Sum of raw `price * remaining_qty` across this account's resting orders.
    pub(crate) open_notional: u128,
}

/// Owner of per-account book state and account-scoped live-order lookup.
///
/// Account state uses direct array indexing, while order resolution uses a hash
/// map keyed by both account ID and client order ID. These structures serve
/// different access patterns but are mutated together by the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AccountTable {
    /// Dense account slots indexed directly by raw account ID; slot zero is unused.
    accounts: Vec<AccountBookState>,
    /// Pre-sized mapping from an account-scoped order identity to its arena handle.
    active_orders: HashMap<AccountOrderKey, OrderHandle>,
}

impl AccountTable {
    /// Create empty account state for IDs in `1..=max_account_id`.
    pub(crate) fn new(max_account_id: u32, max_resting_orders: u32) -> Self {
        Self {
            // keep index zero unused so each valid raw account ID can be used
            // directly as its vector index.
            accounts: vec![AccountBookState::default(); max_account_id as usize + 1],
            // the engine cannot have more lookup entries than resting orders.
            active_orders: HashMap::with_capacity(max_resting_orders as usize),
        }
    }

    /// Return whether an account ID names a configured dense-array slot.
    pub(crate) fn contains(&self, account_id: AccountId) -> bool {
        // account zero is deliberately invalid even though its unused slot
        // exists to preserve direct indexing for all valid accounts.
        account_id.raw() != 0 && (account_id.raw() as usize) < self.accounts.len()
    }

    /// Return read-only book state for a valid account ID.
    pub(crate) fn get(&self, account_id: AccountId) -> Option<&AccountBookState> {
        // validate before indexing so zero and out-of-range IDs return None.
        self.contains(account_id).then(|| &self.accounts[account_id.raw() as usize])
    }

    /// Return mutable book state for a valid account ID.
    pub(crate) fn get_mut(&mut self, account_id: AccountId) -> Option<&mut AccountBookState> {
        // linking, unlinking, and fills all update the same per-account slot.
        // validate first so an invalid ID never reaches the vector index.
        self.contains(account_id).then(|| &mut self.accounts[account_id.raw() as usize])
    }

    /// Return whether this account already has a live order with this client ID.
    #[inline]
    pub(crate) fn contains_order(&self, account_id: AccountId, client_order_id: OrderId) -> bool {
        // including the account in the key means another account may reuse the
        // same client order ID without being treated as a duplicate.
        self.active_orders.contains_key(&AccountOrderKey::new(account_id, client_order_id))
    }

    /// Resolve one account-scoped client order to its arena handle.
    #[inline]
    pub(crate) fn order_handle(&self, account_id: AccountId, client_order_id: OrderId) -> Option<OrderHandle> {
        // only the exact account and client-order pair identifies a live entry.
        self.active_orders.get(&AccountOrderKey::new(account_id, client_order_id)).copied()
    }

    /// Register a newly resting order in the account-scoped lookup.
    #[inline]
    pub(crate) fn insert_order(&mut self, account_id: AccountId, client_order_id: OrderId, handle: OrderHandle) {
        // resting storage assigns the arena handle before the order becomes
        // reachable through this lookup.
        let previous = self.active_orders.insert(AccountOrderKey::new(account_id, client_order_id), handle);
        // structural validation must reject a duplicate exact key before this
        // method is called.
        debug_assert!(previous.is_none(), "live account order must be unique");
    }

    /// Remove an account order from the live-order lookup.
    #[inline]
    pub(crate) fn remove_order(&mut self, account_id: AccountId, client_order_id: OrderId) -> Option<OrderHandle> {
        // the map returns the registered handle, or None if this exact
        // account-scoped order is not live.
        self.active_orders.remove(&AccountOrderKey::new(account_id, client_order_id))
    }

    /// Change a live order's client ID while preserving its arena handle.
    #[inline]
    pub(crate) fn replace_order_id(
        &mut self,
        account_id: AccountId,
        old_client_order_id: OrderId,
        new_client_order_id: OrderId,
        handle: OrderHandle,
    ) {
        // a same-ID replacement needs no map mutation. confirm that the
        // existing key still points to the order being replaced.
        if old_client_order_id == new_client_order_id {
            debug_assert_eq!(self.order_handle(account_id, old_client_order_id), Some(handle));
            return;
        }

        // a priority-preserving cancel-replace keeps its arena slot but changes
        // the externally visible key used to find that slot.
        let removed = self.remove_order(account_id, old_client_order_id);
        debug_assert_eq!(removed, Some(handle), "replacement must rekey its original live order");
        self.insert_order(account_id, new_client_order_id, handle);
    }
}
