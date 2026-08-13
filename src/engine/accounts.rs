use crate::domain::trading::order::AccountId;

use super::orders::OrderHandle;

/// Per-account resting-order chain used by the matching engine.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AccountBookState {
    /// First active order in this account's intrusive order chain.
    pub(crate) head: Option<OrderHandle>,
    /// Last active order in this account's intrusive order chain.
    pub(crate) tail: Option<OrderHandle>,
    /// Number of currently resting orders for this account.
    pub(crate) open_order_count: u32,
    /// Raw open resting notional for this account.
    pub(crate) open_notional: u128,
}

/// Dense account table keyed directly by bounded account ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AccountTable {
    /// Dense account slots indexed directly by raw account ID.
    accounts: Vec<AccountBookState>,
}

impl AccountTable {
    /// Create account slots for IDs in `1..=max_account_id`.
    pub(crate) fn new(max_account_id: u32) -> Self {
        Self {
            accounts: vec![AccountBookState::default(); max_account_id as usize + 1],
        }
    }

    /// Return whether an account ID can be indexed without hashing.
    pub(crate) fn contains(&self, account_id: AccountId) -> bool {
        account_id.raw() != 0 && (account_id.raw() as usize) < self.accounts.len()
    }

    /// Return immutable matching-engine state for a valid account ID.
    pub(crate) fn get(&self, account_id: AccountId) -> Option<&AccountBookState> {
        self.contains(account_id).then(|| &self.accounts[account_id.raw() as usize])
    }

    /// Return mutable matching-engine state for a valid account ID.
    pub(crate) fn get_mut(&mut self, account_id: AccountId) -> Option<&mut AccountBookState> {
        self.contains(account_id).then(|| &mut self.accounts[account_id.raw() as usize])
    }
}
