//! Order lifecycle policy and deterministic execution-report creation.
//!
//! This module decides the outcome of each normalized order-entry command. It
//! validates the command against current engine state, chooses whether quantity
//! trades, rests, expires, or is rejected, and emits the execution reports that
//! describe that outcome.
//!
//! `lifecycle.rs` owns no standalone book storage. Its methods coordinate the
//! components already owned by [`Engine`]:
//!
//! - `AccountTable` validates account-scoped order identity and provides the
//!   per-account chain used by mass cancel.
//! - `OrderArena` resolves live handles and stores the order state needed by
//!   cancel and replacement.
//! - `book.rs` links and unlinks resting orders, maintains aggregates, and
//!   publishes visible book changes.
//! - `matching.rs` walks opposite liquidity, applies fills, and returns the
//!   taker outcome needed to decide whether residual quantity rests or expires.
//! - `ReportSpec` holds an unsequenced execution-report description, while
//!   `push_report` obtains the next engine sequence and sends the final
//!   [`ExecutionReport`] to the caller-owned sink.
//! - [`MarketDataRuntime`] publishes book and trade events through the same
//!   engine sequencer, preserving one deterministic outbound order.
//!
//! # Command behavior
//!
//! - A new order first passes structural checks. A crossing post-only order is
//!   canceled without matching. Other orders match first; market and IOC
//!   residuals expire, while eligible limit residuals rest on the book.
//! - A cancel resolves the original order by
//!   `(account_id, orig_client_order_id)`, removes it from every book index, and
//!   then reports the cancellation.
//! - A same-price replacement that does not increase quantity keeps its FIFO
//!   position. A price change or quantity increase loses priority: the original
//!   order is canceled and the replacement re-enters through the new-order
//!   path as a GTC limit order.
//! - A mass cancel follows one account's intrusive order chain and does not scan
//!   unrelated accounts.
//! - A full drain captures deterministic book order before mutation, then
//!   removes every order. This path is used during controlled shutdown.
//!
//! # Important invariants
//!
//! Rejected commands do not mutate the book. Visible state is changed before
//! its market-data event is published. Every execution report receives exactly
//! one monotonic engine sequence immediately before emission. Client order IDs
//! are resolved within an account, never globally. Replacement priority rules
//! are applied before the replacement is routed back through normal new-order
//! handling.

use super::*;

impl Engine {
    /// Cancel every resting order in deterministic book order.
    pub fn drain_all(
        &mut self,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> Result<(), EngineError> {
        self.drain_all_inner(sequencer, market_data, emit_report).map_err(engine_error)
    }

    /// Remove every resting order and emit its book update and cancel report.
    pub(super) fn drain_all_inner(
        &mut self,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> DomainResult<()> {
        // collect before mutation because freeing an arena slot invalidates the
        // links needed to continue traversal. this shutdown path may allocate;
        // normal order-entry processing does not use it.
        let mut handles = Vec::with_capacity(self.resting_order_count());
        self.collect_side_handles(Side::Buy, &mut handles)?;
        self.collect_side_handles(Side::Sell, &mut handles)?;

        for handle in handles {
            let removed = self.unlink_and_free(handle)?;
            // unlinking happens before publication so both L2 and L1 describe
            // the post-cancel book.
            let level_qty = self.current_level_qty(removed.side, removed.price)?;
            self.publish_book_update(removed.side, removed.price, level_qty, sequencer, market_data)?;
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
    #[inline]
    pub(super) fn on_new_order(
        &mut self,
        order: NewOrder,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> DomainResult<()> {
        if !self.accounts.contains(order.account_id) {
            // account IDs outside the configured dense table are structural rejects.
            return self.reject_new(order, RejectReason::Malformed, sequencer, emit_report);
        }
        if self.accounts.contains_order(order.account_id, order.client_order_id) {
            // client order IDs must be unique within an account while an order is live.
            return self.reject_new(order, RejectReason::Malformed, sequencer, emit_report);
        }

        // limit orders need a representable resting price; market-order prices
        // are irrelevant to matching and are ignored.
        if matches!(order.order_type, OrderType::Limit) {
            let Some(price) = order.price else {
                return self.reject_new(order, RejectReason::InvalidPrice, sequencer, emit_report);
            };
            if !self.price_in_storage(price) {
                return self.reject_new(order, RejectReason::InvalidPrice, sequencer, emit_report);
            }
        }

        // post-only liquidity must never execute immediately, so a crossing
        // order is canceled before entering the matching loop.
        if order.time_in_force == TimeInForce::PostOnly && self.would_cross(order.side, order.price)? {
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

        // matching always precedes the residual decision because a marketable
        // limit may trade first and rest only its remaining quantity.
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

        // matching holds the final taker fill report until lifecycle knows
        // whether the order is filled, expires, or leaves resting quantity.
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
                self.publish_book_update(order.side, price, level_qty, sequencer, market_data)?;
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
    #[inline]
    pub(super) fn on_cancel_order(
        &mut self,
        cancel: CancelOrder,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> DomainResult<()> {
        let Some(handle) = self.accounts.order_handle(cancel.account_id, cancel.orig_client_order_id) else {
            // cancellation identity is account-scoped; the same client order ID
            // on another account is not the requested original order.
            return self.push_report(
                sequencer,
                emit_report,
                ReportSpec::new_reject(cancel.client_order_id, cancel.account_id, RejectReason::UnknownOrder, 0),
            );
        };
        if self.orders.get(handle).map(|order| order.account_id) != Some(cancel.account_id) {
            return Err(DomainError::Malformed);
        }

        // remove the order before publishing the level change and cancellation
        // report so every output observes post-cancel state.
        let removed = self.unlink_and_free(handle)?;
        let level_qty = self.current_level_qty(removed.side, removed.price)?;
        self.publish_book_update(removed.side, removed.price, level_qty, sequencer, market_data)?;
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
                remaining_qty: removed.remaining_qty,
            },
        )
    }

    /// Handle cancel-replace with exchange-style priority rules.
    #[inline]
    pub(super) fn on_cancel_replace(
        &mut self,
        replace: CancelReplace,
        sequencer: &mut Sequencer,
        market_data: &mut MarketDataRuntime,
        emit_report: &mut impl FnMut(ExecutionReport),
    ) -> DomainResult<()> {
        // invalid destination prices are rejected before resolving or mutating
        // the original order.
        if !self.price_in_storage(replace.price) {
            return self.reject_replace(replace, RejectReason::InvalidPrice, sequencer, emit_report);
        }
        if self.accounts.contains_order(replace.account_id, replace.client_order_id)
            && replace.client_order_id != replace.orig_client_order_id
        {
            // a replacement may retain its original ID, but cannot take the ID
            // of another live order owned by the same account.
            return self.reject_replace(replace, RejectReason::Malformed, sequencer, emit_report);
        }

        let Some(handle) = self.accounts.order_handle(replace.account_id, replace.orig_client_order_id) else {
            return self.reject_replace(replace, RejectReason::UnknownOrder, sequencer, emit_report);
        };
        let current = self.orders.get(handle).copied().ok_or(DomainError::Malformed)?;
        if current.account_id != replace.account_id || current.order_id != replace.orig_client_order_id {
            return Err(DomainError::Malformed);
        }

        if current.price == replace.price && replace.qty.raw() <= current.remaining_qty {
            // same-price reductions stay in the existing FIFO position.
            let level_qty = self.preserve_priority_replace(handle, replace, current.remaining_qty)?;
            if replace.qty.raw() != current.remaining_qty {
                self.publish_book_update(current.side, current.price, level_qty, sequencer, market_data)?;
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

        // price changes and quantity increases lose priority and re-enter as new.
        let removed = self.unlink_and_free(handle)?;
        let level_qty = self.current_level_qty(removed.side, removed.price)?;
        self.publish_book_update(removed.side, removed.price, level_qty, sequencer, market_data)?;
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
    #[inline]
    pub(super) fn on_mass_cancel(
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
            // capture the next account link before unlinking frees this slot.
            current = Some(self.orders.get(handle).ok_or(DomainError::Malformed)?.account_next).flatten();
            let removed = self.unlink_and_free(handle)?;
            let level_qty = self.current_level_qty(removed.side, removed.price)?;
            self.publish_book_update(removed.side, removed.price, level_qty, sequencer, market_data)?;
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

    /// Emit a rejected new-order report.
    pub(super) fn reject_new(
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

    /// Emit a rejected cancel-replace report.
    pub(super) fn reject_replace(
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
    #[inline]
    pub(super) fn push_report(
        &mut self,
        sequencer: &mut Sequencer,
        emit_report: &mut impl FnMut(ExecutionReport),
        spec: ReportSpec,
    ) -> DomainResult<()> {
        // sequence immediately before emission so reports cannot reserve numbers
        // before earlier market-data or execution events are published.
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
