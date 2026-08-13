use crate::{
    control::{ControlError, ControlPublisher},
    domain::{
        RejectReason, Sequencer,
        control::{ControlEvent, ControlResult, Snapshot, SnapshotBegin, SnapshotEnd, SnapshotOrder},
    },
    engine::{Engine, RestingOrderView},
    market_data::MarketDataRuntime,
    risk::RiskRuntime,
};

/// Snapshot publisher for the runtime control phase.
pub(super) struct SnapshotProcessor;

impl SnapshotProcessor {
    /// Publish a complete begin/rows/end snapshot batch for one control request.
    pub(super) fn process(
        engine: &Engine,
        risk: &RiskRuntime,
        market_data: &MarketDataRuntime,
        sequencer: &mut Sequencer,
        publisher: &mut ControlPublisher<'_, '_>,
        request: Snapshot,
    ) -> Result<(), ControlError> {
        Self::publish_begin(engine, risk, market_data, sequencer, publisher, request)?;

        // Rows are emitted straight from the book traversal. This avoids a
        // second snapshot-sized buffer while preserving the engine's row order.
        let mut row_count = 0;
        engine.try_for_each_resting_order(|row| {
            Self::publish_row(sequencer, publisher, request, row_count, row)?;
            row_count += 1;
            Ok::<(), ControlError>(())
        })?;

        Self::publish_end(sequencer, publisher, request, row_count)
    }

    /// Publish point-in-time metadata before any resting-order rows.
    fn publish_begin(
        engine: &Engine,
        risk: &RiskRuntime,
        market_data: &MarketDataRuntime,
        sequencer: &mut Sequencer,
        publisher: &mut ControlPublisher<'_, '_>,
        request: Snapshot,
    ) -> Result<(), ControlError> {
        let best_bid = engine.best_bid();
        let best_ask = engine.best_ask();
        let market_data = market_data.snapshot();

        // The begin message captures state that is not repeated on each row.
        publisher.publish(ControlEvent::SnapshotBegin(SnapshotBegin {
            engine_seq: sequencer.next_engine_seq()?,
            request_id: request.request_id,
            recv_ts: request.recv_ts,
            bid_price: best_bid.map(|(price, _)| price),
            bid_qty: best_bid.map_or(0, |(_, qty)| qty),
            ask_price: best_ask.map(|(price, _)| price),
            ask_qty: best_ask.map_or(0, |(_, qty)| qty),
            resting_order_count: engine.resting_order_count() as u64,
            last_md_seq: market_data.last_md_seq,
            next_trade_id: market_data.next_trade_id,
            risk_reference_price: risk.reference_price(),
        }))
    }

    /// Publish one resting-order row with its deterministic row index.
    fn publish_row(
        sequencer: &mut Sequencer,
        publisher: &mut ControlPublisher<'_, '_>,
        request: Snapshot,
        row_index: u64,
        row: RestingOrderView,
    ) -> Result<(), ControlError> {
        publisher.publish(ControlEvent::SnapshotOrder(SnapshotOrder {
            engine_seq: sequencer.next_engine_seq()?,
            request_id: request.request_id,
            row_index,
            side: row.side,
            client_order_id: row.order_id,
            account_id: row.account_id,
            price: row.price,
            remaining_qty: row.remaining_qty,
        }))
    }

    /// Publish the terminal marker with the number of rows actually emitted.
    fn publish_end(
        sequencer: &mut Sequencer,
        publisher: &mut ControlPublisher<'_, '_>,
        request: Snapshot,
        row_count: u64,
    ) -> Result<(), ControlError> {
        // End closes the batch and lets readers detect truncated snapshots.
        publisher.publish(ControlEvent::SnapshotEnd(SnapshotEnd {
            engine_seq: sequencer.next_engine_seq()?,
            request_id: request.request_id,
            recv_ts: request.recv_ts,
            row_count,
            result: ControlResult::Ok,
            reason: RejectReason::None,
        }))
    }
}
