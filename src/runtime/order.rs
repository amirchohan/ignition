use crate::{
    control::ControlRuntime,
    domain::{
        RecvTs, RejectReason, Sequencer,
        trading::order::{ExecType, ExecutionReport, OrdStatus, OrderEntry},
    },
    engine::{Engine, EngineError},
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
    ) -> Result<(), RuntimeError> {
        let mut emit_report = |report| reports.emit_execution_report(report);

        // Only new orders and replaces can add risk. Cancels must keep working
        // while the market is halted so users can reduce exposure.
        if Self::is_risk_increasing(entry) {
            let reason = control.reject_reason_for(Self::entry_recv_ts(entry));
            if reason != RejectReason::None {
                Self::reject_order_entry(sequencer, &mut emit_report, entry, reason)?;
                market_data.check_deferred_error()?;
                return Ok(());
            }
        }

        match risk.evaluate(entry, engine) {
            RiskDecision::Accept => {
                // Accepted orders are handed to the book, which publishes
                // execution reports and any market-data updates it creates.
                engine.execute(entry, sequencer, market_data, &mut emit_report)?;
                if let Some(price) = market_data.take_reference_price_update() {
                    risk.observe_trade_price(price);
                }
            }
            RiskDecision::Reject(reason) => {
                // Risk rejects still get a sequenced execution report.
                Self::reject_order_entry(sequencer, &mut emit_report, entry, reason)?;
            }
        }

        // surface any deferred market-data write failure before the next order.
        market_data.check_deferred_error()?;
        Ok(())
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
