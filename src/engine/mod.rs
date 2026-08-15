//! The deterministic, single-symbol matching engine.
//!
//! This module owns the live order book. It accepts already-decoded order
//! commands, changes the book in price-time order, and emits execution reports
//! and market-data events. Networking, wire decoding, risk checks, and runtime
//! control stay outside the engine.
//!
//! # Main components
//!
//! - [`Engine`] ties the components together. It owns one bid ladder, one ask
//!   ladder, the resting-order storage, the account state, and the validated
//!   limits that bound all of them.

//! - `SideLadder` represents one side of the book: bids or asks. It finds the
//!   best occupied price quickly and stores the FIFO head, FIFO tail, total
//!   quantity, and order count for each active price level. The orders
//!   themselves live in `OrderArena`, not in the ladder.

//! - `OrderArena` is fixed-capacity storage for resting orders. It reuses
//!   preallocated slots and returns generation-checked `OrderHandle` values, so
//!   the hot path does not allocate per order and an old handle cannot silently
//!   refer to a newer order that reused the same slot.

//! - `AccountTable` owns the book state associated with each account. It keeps
//!   per-account order counts and open notional, links each account's live
//!   orders for efficient mass cancel, and resolves the pair
//!   `(account_id, client_order_id)` to an arena handle. Client order IDs are
//!   therefore unique within an account, not across the whole engine.
//!
//! # Files in this module
//!
//! - `accounts.rs` contains `AccountTable` and the per-account book state. It
//!   owns account-scoped order lookup and the linked list used by mass cancel.

//! - `book.rs` contains book queries and the code that links or unlinks resting
//!   orders across price levels, accounts, and the active-order lookup. It also
//!   publishes market data after visible book changes.

//! - `capacity.rs` performs a read-only capacity check before mutation. It
//!   predicts whether a new or replacement order could need an unavailable
//!   arena slot or ladder page.

//! - `ladder.rs` implements the paged price ladder for one side of the book. It
//!   uses occupancy bitmaps to find prices and a fixed page pool so a wide
//!   configured price range does not require every price level to be active.

//! - `lifecycle.rs` handles new, cancel, replace, mass-cancel, and full-drain
//!   commands. It applies lifecycle rules and creates execution reports.

//! - `matching.rs` projects executable liquidity and runs the aggressive
//!   matching loop. It applies fills in price-time order and emits the related
//!   trades, book updates, and maker/taker reports.

//! - `orders.rs` defines resting-order nodes, stable handles, and the
//!   fixed-capacity arena that stores those nodes.

mod accounts;
mod book;
mod capacity;
mod ladder;
mod lifecycle;
mod matching;
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
    /// Dense account table and account-scoped active-order lookup.
    accounts: AccountTable,
}

impl Engine {
    /// Create an empty deterministic matching engine with preallocated state.
    pub fn new(config: EngineConfig) -> Result<Self, EngineError> {
        Ok(Self {
            config,
            bids: SideLadder::new(Side::Buy, config).map_err(|_| EngineError::InvalidConfig)?,
            asks: SideLadder::new(Side::Sell, config).map_err(|_| EngineError::InvalidConfig)?,
            orders: OrderArena::with_capacity(config.max_resting_orders()),
            accounts: AccountTable::new(config.max_account_id(), config.max_resting_orders()),
        })
    }

    /// Execute one normalized order-entry command into caller-owned output sinks.
    pub fn execute(
        &mut self,
        entry: OrderEntry,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> Result<EngineExecution, EngineError> {
        // Step 1: Check capacity without changing the book. New and replacement
        // orders are projected against current liquidity to see whether any
        // residual would need a resting-order slot or ladder page that is full.
        // If so, reject the command before changing any state.
        if let Some(resource) = self.capacity_pressure(entry)? {
            let rejected = match entry {
                OrderEntry::NewOrder(order) => self.reject_new(order, RejectReason::KillSwitchActive, sequencer, emit_report),
                OrderEntry::CancelReplace(replace) => self.reject_replace(replace, RejectReason::KillSwitchActive, sequencer, emit_report),
                OrderEntry::CancelOrder(_) | OrderEntry::MassCancel(_) => return Err(EngineError::InvariantViolation),
            };
            rejected.map_err(engine_error)?;
            return Ok(EngineExecution::CapacityExceeded(resource));
        }

        // Step 2: Process the command. The selected lifecycle handler performs
        // matching and book mutations, then emits sequenced execution reports
        // and market-data events.
        let result = match entry {
            OrderEntry::NewOrder(order) => self.on_new_order(order, sequencer, market_data, emit_report),
            OrderEntry::CancelOrder(cancel) => self.on_cancel_order(cancel, sequencer, market_data, emit_report),
            OrderEntry::CancelReplace(replace) => self.on_cancel_replace(replace, sequencer, market_data, emit_report),
            OrderEntry::MassCancel(cancel) => self.on_mass_cancel(cancel, sequencer, market_data, emit_report),
        };
        result.map_err(engine_error)?;
        Ok(EngineExecution::Processed)
    }
}

/// Result of one engine command needed by the owning runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EngineExecution {
    /// Command completed without exhausting a bounded book resource.
    Processed,
    /// Command was rejected before mutation because a bounded resource was full.
    CapacityExceeded(CapacityResource),
}

/// Fixed-capacity resource that prevented an order from resting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CapacityResource {
    /// Shared resting-order arena.
    RestingOrders,
    /// Side-local active ladder-page pool.
    ActivePages,
}

/// Read-only resting-order view exposed across the engine boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestingOrderView {
    /// Client-assigned ID of the resting order.
    pub order_id: OrderId,
    /// Account that owns the resting order.
    pub account_id: AccountId,
    /// Side of the book on which the order rests.
    pub side: Side,
    /// Limit price of the resting order.
    pub price: Price,
    /// Quantity that is still visible and available to trade.
    pub remaining_qty: u64,
}

/// Read-only no-mutation projection of an incoming order against visible book liquidity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderProjection {
    /// Quantity that would trade against currently visible liquidity.
    pub executable_qty: u64,
    /// Quantity that would remain after all possible executions.
    pub residual_qty: u64,
    /// Lowest price at which any projected execution would occur.
    pub low_execution_price: Option<Price>,
    /// Highest price at which any projected execution would occur.
    pub high_execution_price: Option<Price>,
    /// Whether any projected execution would trade with the same account.
    pub self_trade: bool,
}

/// Translate a domain validation error into the rejection reason sent to a client.
pub(crate) fn structural_reject_reason(error: DomainError) -> RejectReason {
    match error {
        DomainError::InvalidPrice => RejectReason::InvalidPrice,
        DomainError::InvalidQuantity => RejectReason::InvalidQuantity,
        _ => RejectReason::Malformed,
    }
}

/// Translate an internal domain failure into the engine's public error type.
fn engine_error(error: DomainError) -> EngineError {
    match error {
        DomainError::SequenceOverflow => EngineError::SequenceOverflow,
        _ => EngineError::InvariantViolation,
    }
}

/// Summary of fills produced while matching one incoming order.
#[derive(Debug, Default, Clone, Copy)]
struct MatchOutcome {
    /// Total taker quantity filled during the aggressive walk.
    filled_qty: u64,
    /// Last taker report is held until the caller knows whether residual expires.
    pending_taker_report: Option<ReportSpec>,
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
    /// Startup configuration was invalid.
    InvalidConfig,
    /// Live engine indexes or aggregates disagreed with one another.
    InvariantViolation,
    /// A monotonic outbound sequence number could not be incremented.
    SequenceOverflow,
}

/// Values needed to build one sequenced execution report.
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
    /// Build the common report shape used when an order command is rejected.
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

/// Multiply raw integer price and quantity without narrowing the result.
fn raw_notional(price: Price, qty: u64) -> u128 {
    u128::from(price.raw()) * u128::from(qty)
}

#[allow(dead_code)]
const _: fn(DomainError) -> RejectReason = structural_reject_reason;

#[cfg(test)]
mod tests;
