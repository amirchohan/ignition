//! Runtime coordination for one normalized order-entry command.
//!
//! This module connects the gateway's decoded [`OrderEntry`] values to control
//! gating, pre-match risk, the matching engine, execution-report publication,
//! and market-data publication. It owns coordination only; policy remains in
//! `control` and `risk`, while book mutation remains in `engine`.
//!
//! # Main components
//!
//! - [`OrderProcessor`] applies the fixed validation and execution sequence for
//!   one command received during the runtime gateway phase.
//! - [`OrderProcessOutcome`] tells the outer runtime whether processing may
//!   continue or fixed-capacity exhaustion requires a graceful shutdown.
//!
//! # Data flow and invariants
//!
//! Risk-increasing commands observe the control gate before risk evaluation.
//! Cancels remain available while trading is halted so exposure can be reduced.
//! Every reject receives the shared engine sequence, accepted commands delegate
//! mutation to `Engine`, and deferred market-data errors are surfaced before the
//! next command is accepted.

use crate::{
    control::ControlRuntime,
    domain::{
        RecvTs, RejectReason, Sequencer,
        trading::order::{ExecType, ExecutionReport, OrdStatus, OrderEntry},
    },
    engine::{CapacityResource, Engine, EngineError, EngineExecution},
    gateway::GatewayPublisher,
    market_data::MarketDataRuntime,
    risk::{RiskDecision, RiskRuntime},
};

use super::RuntimeError;

/// Order-entry processor for the runtime gateway phase.
pub(super) struct OrderProcessor;

impl OrderProcessor {
    /// Apply control gating, risk checks, matching, and report publication for one order entry.
    pub(super) fn process(
        control: &ControlRuntime,
        risk: &mut RiskRuntime,
        engine: &mut Engine,
        sequencer: &mut Sequencer,
        reports: &mut GatewayPublisher<'_, '_>,
        market_data: &mut MarketDataRuntime,
        entry: OrderEntry,
    ) -> Result<OrderProcessOutcome, RuntimeError> {
        let mut emit_report = |report| reports.emit_execution_report(report);

        // only new orders and replaces can add risk. cancels must keep working
        // while the market is halted so users can reduce exposure.
        if Self::is_risk_increasing(entry) {
            let reason = control.reject_reason_for(Self::entry_recv_ts(entry));
            if reason != RejectReason::None {
                Self::reject_order_entry(sequencer, &mut emit_report, entry, reason)?;
                market_data.check_deferred_error()?;
                return Ok(OrderProcessOutcome::Continue);
            }
        }

        let outcome = match risk.evaluate(entry, engine)? {
            RiskDecision::Accept => {
                // accepted orders are handed to the book, which publishes
                // execution reports and any market-data updates it creates.
                let outcome = engine.execute(entry, sequencer, market_data, &mut emit_report)?;
                if let Some(price) = market_data.take_reference_price_update() {
                    risk.observe_trade_price(price);
                }
                match outcome {
                    EngineExecution::Processed => OrderProcessOutcome::Continue,
                    EngineExecution::CapacityExceeded(resource) => OrderProcessOutcome::GracefulShutdown(resource),
                }
            }
            RiskDecision::Reject(reason) => {
                // risk rejects still get a sequenced execution report.
                Self::reject_order_entry(sequencer, &mut emit_report, entry, reason)?;
                OrderProcessOutcome::Continue
            }
        };

        // surface any deferred market-data write failure before the next order.
        market_data.check_deferred_error()?;
        Ok(outcome)
    }

    /// Publish a sequenced reject report without changing book state.
    fn reject_order_entry(
        sequencer: &mut Sequencer,
        emit_report: &mut impl FnMut(ExecutionReport),
        entry: OrderEntry,
        reason: RejectReason,
    ) -> Result<(), RuntimeError> {
        let (client_order_id, account_id, remaining_qty) = match entry {
            OrderEntry::NewOrder(order) => (order.client_order_id, order.account_id, order.qty.raw()),
            OrderEntry::CancelReplace(replace) => (replace.client_order_id, replace.account_id, replace.qty.raw()),
            OrderEntry::CancelOrder(cancel) => (cancel.client_order_id, cancel.account_id, 0),
            OrderEntry::MassCancel(cancel) => (cancel.client_order_id, cancel.account_id, 0),
        };

        let engine_seq = sequencer
            .next_engine_seq()
            .map_err(|_| RuntimeError::Engine(EngineError::SequenceOverflow))?;

        // rejected orders do not trade, so fill fields stay empty and the
        // remaining quantity is the original requested quantity where one exists.
        emit_report(ExecutionReport {
            engine_seq,
            client_order_id,
            account_id,
            exec_type: ExecType::Rejected,
            ord_status: OrdStatus::Rejected,
            reject_reason: reason,
            fill_price: None,
            fill_qty: 0,
            remaining_qty,
        });
        Ok(())
    }

    /// Return whether the order could increase open exposure.
    const fn is_risk_increasing(entry: OrderEntry) -> bool {
        matches!(entry, OrderEntry::NewOrder(_) | OrderEntry::CancelReplace(_))
    }

    /// Return the receive timestamp carried by any order-entry variant.
    const fn entry_recv_ts(entry: OrderEntry) -> RecvTs {
        match entry {
            OrderEntry::NewOrder(order) => order.recv_ts,
            OrderEntry::CancelReplace(replace) => replace.recv_ts,
            OrderEntry::CancelOrder(cancel) => cancel.recv_ts,
            OrderEntry::MassCancel(cancel) => cancel.recv_ts,
        }
    }
}

/// Runtime action produced by processing one normalized order command.
pub(super) enum OrderProcessOutcome {
    /// Continue accepting commands from the current gateway batch.
    Continue,
    /// Stop ingress and drain after the named fixed-capacity resource is exhausted.
    GracefulShutdown(CapacityResource),
}
