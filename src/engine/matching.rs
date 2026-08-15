//! Read-only liquidity projection and the mutating price-time matching loop.
//!
//! This module has two related jobs. `project_order` answers what an incoming
//! order would do against the current book without changing state. The mutating
//! path then performs those executions in price-time order and emits the market
//! data and execution reports caused by each fill.
//!
//! # Main components
//!
//! This file adds methods to [`Engine`] and owns no standalone storage:
//!
//! - [`OrderProjection`] summarizes executable quantity, residual quantity,
//!   execution-price bounds, and whether visible matching would involve the
//!   incoming account. Risk and capacity checks consume this read-only result.
//! - `MatchOutcome` carries total taker fills and the final delayed taker report
//!   back to `lifecycle.rs`, where residual quantity is known to rest or expire.
//! - `SideLadder` supplies the best opposite price, the next occupied price, and
//!   each price level's FIFO head.
//! - `OrderArena` supplies maker nodes and their FIFO links.
//! - `AccountTable` holds open notional that must decrease with every maker
//!   fill.
//! - `MarketDataRuntime`, `ReportSpec`, and the shared `Sequencer` turn each
//!   state transition into deterministically ordered trade, book, and execution
//!   events.
//!
//! # Price-time matching
//!
//! Matching starts at the opposite best price. It stops when the taker is full,
//! the opposite book is empty, or the next price is outside the taker's limit.
//! Within a price level, makers are visited from the FIFO head. Each execution
//! uses the maker's resting price and the smaller of maker leaves quantity and
//! taker leaves quantity.
//!
//! Projection follows the same visible price and FIFO traversal but only updates
//! local counters. It does not allocate, mutate the book, consume sequence
//! numbers, or emit events.
//!
//! # Event ordering
//!
//! For each fill, book state changes first. A trade is then published, followed
//! by L2 and L1 updates and the maker execution report. The corresponding taker
//! report is held temporarily. Before another fill it can be emitted as partial;
//! after the final fill, lifecycle code can mark it filled, leave it partial
//! while quantity rests, or mark it expired when an IOC or market residual is
//! discarded.
//!
//! # Important invariants
//!
//! Price-time priority and quantity conservation must hold for projection and
//! real matching. A maker fill reduces the order's leaves quantity, the owning
//! account's open notional, and the price level's aggregate by the same fill.
//! Fully filled makers leave every book index before post-fill book data is
//! published. All outputs describe already-applied state and use the shared
//! monotonic engine sequence.
//!
//! `risk.rs` and `capacity.rs` use projection. `lifecycle.rs` starts matching and
//! decides the final taker outcome. `book.rs` supplies traversal, unlinking, and
//! book-update helpers.

use super::*;

impl Engine {
    /// Project an incoming order against visible opposite liquidity without mutation.
    pub fn project_order(
        &self,
        account_id: AccountId,
        side: Side,
        limit_price: Option<Price>,
        qty: u64,
    ) -> Result<OrderProjection, EngineError> {
        let mut remaining_qty = qty;
        let mut executable_qty: u64 = 0;
        let mut low_execution_price = None;
        let mut high_execution_price = None;
        let mut self_trade = false;
        let opposite_side = best_side(side);
        // begin at the economically best maker price, then follow occupied
        // levels in the side's best-to-worst direction.
        let mut cursor = self.side(opposite_side).best_level().map_err(engine_error)?.map(|(key, _)| key);

        while remaining_qty > 0 {
            let Some(key) = cursor else {
                break;
            };
            if !Self::is_executable(side, limit_price, key.price) {
                break;
            }

            let level = self.side(opposite_side).get_level(key).ok_or(EngineError::InvariantViolation)?;
            // each level's intrusive chain defines FIFO order without
            // allocating a projection-specific collection.
            let mut current = level.head.ok_or(EngineError::InvariantViolation).map(Some)?;
            let mut visited_count = 0_u32;
            let mut visited_qty = 0_u64;
            while remaining_qty > 0 {
                let Some(handle) = current else {
                    break;
                };
                let order = self.orders.get(handle).copied().ok_or(EngineError::InvariantViolation)?;
                if order.remaining_qty == 0 || order.side != opposite_side || order.price != key.price {
                    return Err(EngineError::InvariantViolation);
                }
                let fill_qty = remaining_qty.min(order.remaining_qty);
                visited_count = visited_count.checked_add(1).ok_or(EngineError::InvariantViolation)?;
                visited_qty = visited_qty
                    .checked_add(order.remaining_qty)
                    .ok_or(EngineError::InvariantViolation)?;
                remaining_qty -= fill_qty;
                executable_qty = executable_qty.checked_add(fill_qty).ok_or(EngineError::InvariantViolation)?;
                low_execution_price = Some(low_execution_price.map_or(order.price, |price: Price| price.min(order.price)));
                high_execution_price = Some(high_execution_price.map_or(order.price, |price: Price| price.max(order.price)));
                // projection records potential self-trade for the risk layer;
                // matching itself does not apply self-trade policy.
                self_trade |= order.account_id == account_id;
                current = order.level_next;
            }
            if remaining_qty > 0 && current.is_none() && (visited_count != level.order_count || visited_qty != level.total_qty) {
                return Err(EngineError::InvariantViolation);
            }

            cursor = self.next_occupied_key(opposite_side, key);
        }

        Ok(OrderProjection {
            executable_qty,
            residual_qty: remaining_qty,
            low_execution_price,
            high_execution_price,
            self_trade,
        })
    }

    /// Match an aggressive order against the opposite best levels.
    #[allow(clippy::too_many_arguments)]
    #[inline]
    pub(super) fn match_incoming(
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
            // an empty opposite side leaves any residual decision to lifecycle.
            let Some((best_key, best_level)) = self.side(best_side(side)).best_level()? else {
                break;
            };
            let level_head = best_level.head;
            // reaching a non-executable best price also makes every worse price
            // non-executable on this ordered ladder.
            if !Self::is_executable(side, limit_price, best_key.price) {
                break;
            }

            let maker_handle = level_head.ok_or(DomainError::Malformed)?;
            let maker = self.orders.get(maker_handle).copied().ok_or(DomainError::Malformed)?;
            if maker.remaining_qty == 0 || maker.side != best_side(side) || maker.price != best_key.price {
                return Err(DomainError::Malformed);
            }

            // once another fill is certain, the previous taker report can no
            // longer be the order's terminal report and is safe to emit.
            self.emit_pending_taker_report(sequencer, emit_report, outcome.pending_taker_report.take())?;
            // using the smaller leaves quantity conserves quantity and ensures
            // at least one side of every execution reaches zero.
            let fill_qty = (*remaining_qty).min(maker.remaining_qty);
            *remaining_qty -= fill_qty;
            outcome.filled_qty = outcome.filled_qty.checked_add(fill_qty).ok_or(DomainError::Malformed)?;
            // apply book state first so all following events describe post-fill state.
            self.apply_fill(maker_handle, best_key, fill_qty)?;
            let maker_remaining = self.orders.get(maker_handle).ok_or(DomainError::Malformed)?.remaining_qty;
            if maker_remaining == 0 {
                // fully filled makers leave both intrusive lists and the
                // account-scoped ID lookup before book data is observed.
                self.unlink_and_free(maker_handle)?;
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
            let level_qty = self.current_level_qty(maker.side, maker.price)?;
            self.publish_book_update(maker.side, maker.price, level_qty, sequencer, market_data)?;

            // maker status is final only when this fill exhausted its leaves
            // quantity; otherwise the maker remains at the same FIFO priority.
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

            // delay the taker report until lifecycle knows whether this is its
            // final fill and what happens to any remaining quantity.
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
    pub(super) fn expire_taker_residual(
        &mut self,
        sequencer: &mut Sequencer,
        emit_report: &mut impl FnMut(ExecutionReport),
        outcome: MatchOutcome,
    ) -> DomainResult<()> {
        // no pending fill means the order expired without trading and lifecycle
        // emits the standalone expiry report instead.
        let Some(mut spec) = outcome.pending_taker_report else {
            return Ok(());
        };
        spec.ord_status = OrdStatus::Expired;
        spec.remaining_qty = 0;
        self.push_report(sequencer, emit_report, spec)
    }

    /// Emit a delayed taker fill report when one is waiting to be finalized.
    #[inline]
    pub(super) fn emit_pending_taker_report(
        &mut self,
        sequencer: &mut Sequencer,
        emit_report: &mut impl FnMut(ExecutionReport),
        spec: Option<ReportSpec>,
    ) -> DomainResult<()> {
        // callers may have no delayed report when matching produced no fill.
        let Some(spec) = spec else {
            return Ok(());
        };
        self.push_report(sequencer, emit_report, spec)
    }

    /// Apply a fill to one maker order and its price-level aggregate.
    #[inline]
    pub(super) fn apply_fill(&mut self, maker_handle: OrderHandle, key: ladder::LevelKey, fill_qty: u64) -> DomainResult<()> {
        // capture notional before taking mutable borrows; the matching loop has
        // already validated that this generation-checked handle is live.
        let maker = self.orders.get(maker_handle).copied().ok_or(DomainError::Malformed)?;
        let filled_notional = raw_notional(maker.price, fill_qty);
        let order = self.orders.get_mut(maker_handle).ok_or(DomainError::Malformed)?;
        order.remaining_qty = order.remaining_qty.checked_sub(fill_qty).ok_or(DomainError::Malformed)?;
        let side = maker.side;
        // account exposure tracks only quantity still resting on the book.
        let account = self.accounts.get_mut(maker.account_id).ok_or(DomainError::Malformed)?;
        account.open_notional = account.open_notional.checked_sub(filled_notional).ok_or(DomainError::Malformed)?;
        // order count and FIFO links remain unchanged here; unlink_and_free
        // removes them only when the maker reaches zero leaves quantity.
        let level = self.side_mut(side).get_level_mut_or_activate(key)?;
        level.total_qty = level.total_qty.checked_sub(fill_qty).ok_or(DomainError::Malformed)?;
        Ok(())
    }
}
