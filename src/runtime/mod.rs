//! Runtime orchestration boundary.
//!
//! Runtime owns the poll-driven phase order. It does not parse wire bytes or
//! own book internals; it moves typed messages between control, gateway, risk,
//! matching, market-data, and egress in a deterministic loop.

use crate::{
    control::{ControlError, ControlPublisher, ControlRuntime},
    domain::{
        EngineSequencer, RecvTs, RejectReason,
        control::{ControlEvent, ControlResult, SnapshotBegin, SnapshotEnd, SnapshotOrder},
        trading::order::{ExecType, ExecutionReport, OrdStatus, OrderEntry},
    },
    engine::{Engine, EngineConfig, EngineError, RestingOrderView},
    gateway::{GatewayError, GatewayPollError, GatewayPublisher, GatewayRuntime},
    market_data::{MarketDataFeedError, MarketDataRuntime},
    risk::{RiskConfig, RiskDecision, RiskRuntime},
};

/// Runtime orchestration failure.
#[derive(Debug)]
pub enum RuntimeError {
    Engine(EngineError),
    Control(ControlError),
    Gateway(GatewayError),
    MarketDataFeed(MarketDataFeedError),
}

impl From<EngineError> for RuntimeError {
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}

impl From<ControlError> for RuntimeError {
    fn from(error: ControlError) -> Self {
        Self::Control(error)
    }
}

impl From<GatewayError> for RuntimeError {
    fn from(error: GatewayError) -> Self {
        Self::Gateway(error)
    }
}

impl From<MarketDataFeedError> for RuntimeError {
    fn from(error: MarketDataFeedError) -> Self {
        Self::MarketDataFeed(error)
    }
}

/// Summary of work completed by a runtime loop.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeSummary {
    pub iterations: u64,
}

/// Poll-driven runtime orchestrator.
#[derive(Debug)]
pub struct Runtime {
    /// Control-plane state and ingress.
    control: ControlRuntime,
    /// Order-entry ingress and trading egress.
    gateway: GatewayRuntime,
    /// Market-data multicast egress.
    market_data: MarketDataRuntime,
    /// Pre-match risk gate and reference-price state.
    risk: RiskRuntime,
    /// Matching engine state.
    engine: Engine,
    /// Shared deterministic execution/control sequencer.
    sequencer: EngineSequencer,
    /// Reusable snapshot row buffer.
    snapshot_orders: Vec<RestingOrderView>,
    /// Set after deterministic drain-and-shutdown completes.
    terminal: bool,
}

impl Runtime {
    /// Create a runtime from prebuilt control and gateway adapters.
    pub fn new(
        engine_config: EngineConfig,
        risk_config: RiskConfig,
        control: ControlRuntime,
        gateway: GatewayRuntime,
        market_data: MarketDataRuntime,
    ) -> Result<Self, RuntimeError> {
        Ok(Self {
            control,
            gateway,
            market_data,
            risk: RiskRuntime::new(risk_config),
            engine: Engine::new(engine_config)?,
            sequencer: EngineSequencer::default(),
            snapshot_orders: Vec::new(),
            terminal: false,
        })
    }

    /// Poll until all finite adapters are closed and no buffered work remains.
    pub fn run(&mut self) -> Result<RuntimeSummary, RuntimeError> {
        let mut summary = RuntimeSummary::default();
        loop {
            let made_progress = self.poll_once()?;
            summary.iterations += 1;

            if self.terminal {
                return Ok(summary);
            }
            if !made_progress {
                std::thread::yield_now();
            }
        }
    }

    /// Execute one bounded runtime iteration.
    pub fn poll_once(&mut self) -> Result<bool, RuntimeError> {
        if self.terminal {
            return Ok(false);
        }

        let mut shutdown_requested = false;
        let mut handled_control_request = false;

        let Runtime {
            control,
            gateway,
            market_data,
            risk,
            engine,
            sequencer,
            snapshot_orders,
            terminal,
        } = self;

        let control_progress = control.poll(sequencer, |request, publisher, sequencer| {
            handled_control_request = true;
            match request {
                crate::domain::control::ControlRequest::KillSwitch(request) => {
                    if request.action == crate::domain::control::KillSwitchAction::Shutdown {
                        shutdown_requested = true;
                    }
                }
                crate::domain::control::ControlRequest::Heartbeat(_) => {}
                crate::domain::control::ControlRequest::Snapshot(snapshot) => {
                    let mut snapshot_emitter = SnapshotEmitter {
                        engine,
                        risk,
                        market_data,
                        sequencer,
                        snapshot_orders,
                        publisher,
                    };
                    snapshot_emitter.emit(snapshot.request_id, snapshot.recv_ts)?;
                }
            }
            Ok(())
        })?;

        if shutdown_requested {
            {
                let mut emit_report = |report| gateway.emit_execution_report(report);
                engine.drain_all(sequencer, market_data, &mut emit_report)?;
                if let Some(price) = market_data.take_reference_price_update() {
                    risk.observe_trade_price(price);
                }
            }
            gateway.finish()?;
            market_data.finish()?;
            gateway.flush_pending()?;
            market_data.flush_pending()?;
            *terminal = true;
            return Ok(true);
        }

        let mut handled_order = false;
        let gateway_progress = gateway
            .poll(sequencer, |order, publisher, sequencer| {
                handled_order = true;
                let mut processor = OrderProcessor {
                    control,
                    risk,
                    engine,
                    sequencer,
                    reports: publisher,
                    market_data,
                };
                processor.process(order)?;
                market_data.finish()?;
                Ok::<(), RuntimeError>(())
            })
            .map_err(|error| match error {
                GatewayPollError::Gateway(error) => RuntimeError::Gateway(error),
                GatewayPollError::Handler(error) => error,
            })?;

        let market_data_progress = market_data.flush_pending()?;

        Ok(control_progress
            || gateway_progress
            || market_data_progress
            || handled_control_request
            || handled_order)
    }

    /// Return the runtime-owned matching engine.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    fn reject(
        sequencer: &mut EngineSequencer,
        emit_report: &mut impl FnMut(ExecutionReport),
        entry: OrderEntry,
        reason: RejectReason,
    ) -> Result<(), RuntimeError> {
        let (client_order_id, account_id, remaining_qty) = match entry {
            OrderEntry::NewOrder(order) => {
                (order.client_order_id, order.account_id, order.qty.raw())
            }
            OrderEntry::CancelReplace(replace) => (
                replace.client_order_id,
                replace.account_id,
                replace.qty.raw(),
            ),
            OrderEntry::CancelOrder(cancel) => (cancel.client_order_id, cancel.account_id, 0),
            OrderEntry::MassCancel(cancel) => (cancel.client_order_id, cancel.account_id, 0),
        };

        let engine_seq = sequencer
            .next_engine_seq()
            .map_err(|_| RuntimeError::Engine(EngineError::SequenceOverflow))?;
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
}

struct OrderProcessor<'a, 'b, 'c> {
    control: &'a ControlRuntime,
    risk: &'a mut RiskRuntime,
    engine: &'a mut Engine,
    sequencer: &'a mut EngineSequencer,
    reports: &'a mut GatewayPublisher<'b, 'c>,
    market_data: &'a mut MarketDataRuntime,
}

impl OrderProcessor<'_, '_, '_> {
    fn process(&mut self, entry: OrderEntry) -> Result<(), RuntimeError> {
        let mut emit_report = |report| self.reports.emit_execution_report(report);
        if is_risk_increasing(entry) {
            let reason = self.control.reject_reason_for(entry_recv_ts(entry));
            if reason != RejectReason::None {
                Runtime::reject(self.sequencer, &mut emit_report, entry, reason)?;
                return Ok(());
            }
        }

        match self.risk.evaluate(entry, self.engine) {
            RiskDecision::Accept => {
                self.engine
                    .execute(entry, self.sequencer, self.market_data, &mut emit_report)?;
                if let Some(price) = self.market_data.take_reference_price_update() {
                    self.risk.observe_trade_price(price);
                }
                Ok(())
            }
            RiskDecision::Reject(reason) => {
                Runtime::reject(self.sequencer, &mut emit_report, entry, reason)?;
                Ok(())
            }
        }
    }
}

struct SnapshotEmitter<'a, 'b, 'c> {
    engine: &'a Engine,
    risk: &'a RiskRuntime,
    market_data: &'a MarketDataRuntime,
    sequencer: &'a mut EngineSequencer,
    snapshot_orders: &'a mut Vec<RestingOrderView>,
    publisher: &'a mut ControlPublisher<'b, 'c>,
}

impl SnapshotEmitter<'_, '_, '_> {
    fn emit(
        &mut self,
        request_id: crate::domain::control::RequestId,
        recv_ts: RecvTs,
    ) -> Result<(), ControlError> {
        self.snapshot_orders.clear();
        self.engine.snapshot_orders(self.snapshot_orders);
        let best_bid = self.engine.best_bid();
        let best_ask = self.engine.best_ask();
        let market_data = self.market_data.snapshot();

        self.publisher
            .publish(ControlEvent::SnapshotBegin(SnapshotBegin {
                engine_seq: self.sequencer.next_engine_seq()?,
                request_id,
                recv_ts,
                bid_price: best_bid.map(|(price, _)| price),
                bid_qty: best_bid.map_or(0, |(_, qty)| qty),
                ask_price: best_ask.map(|(price, _)| price),
                ask_qty: best_ask.map_or(0, |(_, qty)| qty),
                resting_order_count: self.engine.resting_order_count() as u64,
                last_md_seq: market_data.last_md_seq,
                next_trade_id: market_data.next_trade_id,
                risk_reference_price: self.risk.reference_price(),
            }))?;

        for (row_index, row) in self.snapshot_orders.iter().copied().enumerate() {
            self.publisher
                .publish(ControlEvent::SnapshotOrder(SnapshotOrder {
                    engine_seq: self.sequencer.next_engine_seq()?,
                    request_id,
                    row_index: row_index as u64,
                    side: row.side,
                    client_order_id: row.order_id,
                    account_id: row.account_id,
                    price: row.price,
                    remaining_qty: row.remaining_qty,
                }))?;
        }

        self.publisher
            .publish(ControlEvent::SnapshotEnd(SnapshotEnd {
                engine_seq: self.sequencer.next_engine_seq()?,
                request_id,
                recv_ts,
                row_count: self.snapshot_orders.len() as u64,
                result: ControlResult::Ok,
                reason: RejectReason::None,
            }))?;
        Ok(())
    }
}

const fn is_risk_increasing(entry: OrderEntry) -> bool {
    matches!(
        entry,
        OrderEntry::NewOrder(_) | OrderEntry::CancelReplace(_)
    )
}

const fn entry_recv_ts(entry: OrderEntry) -> RecvTs {
    match entry {
        OrderEntry::NewOrder(order) => order.recv_ts,
        OrderEntry::CancelReplace(replace) => replace.recv_ts,
        OrderEntry::CancelOrder(cancel) => cancel.recv_ts,
        OrderEntry::MassCancel(cancel) => cancel.recv_ts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::ControlConfig;
    use crate::domain::control::{ControlResult, KillSwitchAction};
    use crate::domain::trading::order::{CancelOrder, CancelReplace};
    use crate::domain::{
        Price, Qty, RecvTs,
        rd::InstrumentSpec,
        trading::order::{AccountId, ClientTs, NewOrder, OrderId, OrderType, Side, TimeInForce},
    };
    use crate::market_data::{MarketDataFeedConfig, MarketDataRuntime};
    use crate::protocol::{
        MessageKind, SBE_HEADER_LEN, SCHEMA_VERSION, decode_header, encode_header,
        generated::codecs::{
            ReadBuf, WriteBuf, cancel_order_codec, cancel_replace_codec,
            control_result as wire_control_result, execution_report_codec, heartbeat_codec,
            kill_switch_action, kill_switch_codec, mass_cancel_codec, new_order_codec,
            order_type as wire_order_type, reject_reason as wire_reject_reason, side as wire_side,
            snapshot_begin_codec, snapshot_codec, snapshot_end_codec, snapshot_order_codec,
            time_in_force as wire_tif,
        },
    };
    use std::{
        io::{Read, Write},
        net::{SocketAddr, TcpListener, TcpStream},
        thread,
        time::Duration,
    };

    fn instrument() -> InstrumentSpec {
        InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument")
    }

    fn price(raw: u64) -> Price {
        Price::new(raw, &instrument()).expect("price")
    }

    fn qty(raw: u64) -> Qty {
        Qty::new(raw, &instrument()).expect("qty")
    }

    fn engine_config() -> EngineConfig {
        EngineConfig::new(instrument(), 100, 10_000, 1024, 128, 100, 16).expect("config")
    }

    fn risk_config(max_orders: u32, max_notional: u128) -> RiskConfig {
        RiskConfig::new(max_orders, max_notional, price(1_000), 100).expect("risk config")
    }

    #[allow(clippy::too_many_arguments)]
    fn new_order(
        id: u64,
        account: u32,
        side: Side,
        tif: TimeInForce,
        price_raw: u64,
        qty_raw: u64,
    ) -> OrderEntry {
        OrderEntry::NewOrder(NewOrder {
            client_order_id: OrderId::new(id).expect("order"),
            account_id: AccountId::new(account).expect("account"),
            client_ts: ClientTs::new(id),
            recv_ts: RecvTs::new(id),
            side,
            order_type: OrderType::Limit,
            time_in_force: tif,
            price: Some(price(price_raw)),
            qty: qty(qty_raw),
        })
    }

    fn replace(id: u64, account: u32, orig: u64, price_raw: u64, qty_raw: u64) -> OrderEntry {
        OrderEntry::CancelReplace(CancelReplace {
            client_order_id: OrderId::new(id).expect("order"),
            account_id: AccountId::new(account).expect("account"),
            orig_client_order_id: OrderId::new(orig).expect("orig"),
            client_ts: ClientTs::new(id),
            recv_ts: RecvTs::new(id),
            price: price(price_raw),
            qty: qty(qty_raw),
        })
    }

    fn cancel(id: u64, account: u32, orig: u64) -> OrderEntry {
        OrderEntry::CancelOrder(CancelOrder {
            client_order_id: OrderId::new(id).expect("order"),
            account_id: AccountId::new(account).expect("account"),
            orig_client_order_id: OrderId::new(orig).expect("orig"),
            client_ts: ClientTs::new(id),
            recv_ts: RecvTs::new(id),
        })
    }

    fn runtime_with_orders(
        orders: impl IntoIterator<Item = OrderEntry>,
        risk_config: RiskConfig,
    ) -> Option<(Runtime, TcpStream)> {
        let control = new_control()?;
        runtime_with_control_and_orders(control, orders, risk_config)
    }

    fn runtime_without_gateway_client(risk_config: RiskConfig) -> Option<Runtime> {
        let control = new_control()?;
        let market_data = new_market_data()?;
        let gateway_addr = reserve_loopback_addr()?;
        let gateway = match GatewayRuntime::new(
            crate::gateway::GatewayConfig {
                bind_addr: gateway_addr.to_string(),
                ..crate::gateway::GatewayConfig::default()
            },
            instrument(),
        ) {
            Ok(gateway) => gateway,
            Err(crate::gateway::GatewayError::Io(crate::io::IoError::Bind(error)))
                if error.kind() == std::io::ErrorKind::PermissionDenied =>
            {
                return None;
            }
            Err(error) => panic!("gateway: {error:?}"),
        };
        Some(
            Runtime::new(engine_config(), risk_config, control, gateway, market_data)
                .expect("runtime"),
        )
    }

    fn runtime_with_control_and_orders(
        control: ControlRuntime,
        orders: impl IntoIterator<Item = OrderEntry>,
        risk_config: RiskConfig,
    ) -> Option<(Runtime, TcpStream)> {
        let market_data = new_market_data()?;
        let gateway_addr = reserve_loopback_addr()?;
        let gateway_config = crate::gateway::GatewayConfig {
            bind_addr: gateway_addr.to_string(),
            ..crate::gateway::GatewayConfig::default()
        };
        let gateway = match GatewayRuntime::new(gateway_config, instrument()) {
            Ok(gateway) => gateway,
            Err(crate::gateway::GatewayError::Io(crate::io::IoError::Bind(error)))
                if error.kind() == std::io::ErrorKind::PermissionDenied =>
            {
                return None;
            }
            Err(error) => panic!("gateway: {error:?}"),
        };
        let mut client = TcpStream::connect(gateway_addr).expect("connect gateway");
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("gateway timeout");
        write_gateway_orders(&mut client, orders);
        Some(
            Runtime::new(engine_config(), risk_config, control, gateway, market_data)
                .expect("runtime"),
        )
        .map(|runtime| (runtime, client))
    }

    fn control_config() -> ControlConfig {
        ControlConfig {
            bind_addr: "127.0.0.1:0".to_owned(),
            ..ControlConfig::default()
        }
    }

    fn new_market_data() -> Option<MarketDataRuntime> {
        match MarketDataRuntime::new(MarketDataFeedConfig {
            bind_addr: "0.0.0.0:0".to_owned(),
            group_addr: "239.255.0.1:9002".to_owned(),
            multicast_loop_v4: true,
            ..MarketDataFeedConfig::default()
        }) {
            Ok(feed) => Some(feed),
            Err(crate::market_data::MarketDataFeedError::Io(crate::io::IoError::Bind(error)))
                if error.kind() == std::io::ErrorKind::PermissionDenied =>
            {
                None
            }
            Err(error) => panic!("market-data feed: {error:?}"),
        }
    }

    fn reserve_loopback_addr() -> Option<SocketAddr> {
        match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => Some(listener.local_addr().expect("addr")),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(error) => panic!("reserve control addr: {error:?}"),
        }
    }

    fn new_control() -> Option<ControlRuntime> {
        new_control_with_config(control_config())
    }

    fn new_control_with_config(config: ControlConfig) -> Option<ControlRuntime> {
        match ControlRuntime::new(config) {
            Ok(control) => Some(control),
            Err(crate::control::ControlError::Io(crate::io::IoError::Bind(error)))
                if error.kind() == std::io::ErrorKind::PermissionDenied =>
            {
                None
            }
            Err(error) => panic!("control: {error:?}"),
        }
    }

    fn runtime_with_control_client_and_orders(
        orders: impl IntoIterator<Item = OrderEntry>,
        risk_config: RiskConfig,
    ) -> Option<(Runtime, TcpStream, TcpStream)> {
        let bind_addr = reserve_loopback_addr()?;
        let control = new_control_with_config(ControlConfig {
            bind_addr: bind_addr.to_string(),
            ..ControlConfig::default()
        })?;
        let client = TcpStream::connect(bind_addr).expect("connect");
        client
            .set_read_timeout(Some(Duration::from_secs(1)))
            .expect("timeout");
        let (runtime, gateway_client) =
            runtime_with_control_and_orders(control, orders, risk_config)?;
        Some((runtime, client, gateway_client))
    }

    fn write_control(client: &mut TcpStream, frame: &[u8]) {
        client.write_all(frame).expect("write control");
    }

    fn write_gateway_orders(client: &mut TcpStream, orders: impl IntoIterator<Item = OrderEntry>) {
        for order in orders {
            client
                .write_all(&order_entry_frame(order))
                .expect("write gateway");
        }
    }

    fn order_entry_frame(entry: OrderEntry) -> Vec<u8> {
        match entry {
            OrderEntry::NewOrder(order) => {
                let mut bytes =
                    vec![0_u8; SBE_HEADER_LEN + new_order_codec::SBE_BLOCK_LENGTH as usize];
                encode_header(
                    &mut bytes,
                    new_order_codec::SBE_BLOCK_LENGTH,
                    MessageKind::NewOrder,
                )
                .expect("header");
                let write_buf = WriteBuf::new(&mut bytes);
                let mut encoder =
                    new_order_codec::NewOrderEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
                encoder
                    .client_order_id(order.client_order_id.raw())
                    .account_id(order.account_id.raw())
                    .client_ts(order.client_ts.raw())
                    .side(to_wire_side(order.side))
                    .order_type(match order.order_type {
                        OrderType::Limit => wire_order_type::OrderType::Limit,
                        OrderType::Market => wire_order_type::OrderType::Market,
                    })
                    .time_in_force(to_wire_tif(order.time_in_force))
                    .price(order.price.map_or(0, Price::raw))
                    .qty(order.qty.raw());
                bytes
            }
            OrderEntry::CancelOrder(cancel) => {
                let mut bytes =
                    vec![0_u8; SBE_HEADER_LEN + cancel_order_codec::SBE_BLOCK_LENGTH as usize];
                encode_header(
                    &mut bytes,
                    cancel_order_codec::SBE_BLOCK_LENGTH,
                    MessageKind::CancelOrder,
                )
                .expect("header");
                let write_buf = WriteBuf::new(&mut bytes);
                let mut encoder = cancel_order_codec::CancelOrderEncoder::default()
                    .wrap(write_buf, SBE_HEADER_LEN);
                encoder
                    .client_order_id(cancel.client_order_id.raw())
                    .account_id(cancel.account_id.raw())
                    .orig_client_order_id(cancel.orig_client_order_id.raw())
                    .client_ts(cancel.client_ts.raw());
                bytes
            }
            OrderEntry::CancelReplace(replace) => {
                let mut bytes =
                    vec![0_u8; SBE_HEADER_LEN + cancel_replace_codec::SBE_BLOCK_LENGTH as usize];
                encode_header(
                    &mut bytes,
                    cancel_replace_codec::SBE_BLOCK_LENGTH,
                    MessageKind::CancelReplace,
                )
                .expect("header");
                let write_buf = WriteBuf::new(&mut bytes);
                let mut encoder = cancel_replace_codec::CancelReplaceEncoder::default()
                    .wrap(write_buf, SBE_HEADER_LEN);
                encoder
                    .client_order_id(replace.client_order_id.raw())
                    .account_id(replace.account_id.raw())
                    .orig_client_order_id(replace.orig_client_order_id.raw())
                    .client_ts(replace.client_ts.raw())
                    .price(replace.price.raw())
                    .qty(replace.qty.raw());
                bytes
            }
            OrderEntry::MassCancel(cancel) => {
                let mut bytes =
                    vec![0_u8; SBE_HEADER_LEN + mass_cancel_codec::SBE_BLOCK_LENGTH as usize];
                encode_header(
                    &mut bytes,
                    mass_cancel_codec::SBE_BLOCK_LENGTH,
                    MessageKind::MassCancel,
                )
                .expect("header");
                let write_buf = WriteBuf::new(&mut bytes);
                let mut encoder =
                    mass_cancel_codec::MassCancelEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
                encoder
                    .client_order_id(cancel.client_order_id.raw())
                    .account_id(cancel.account_id.raw())
                    .client_ts(cancel.client_ts.raw());
                bytes
            }
        }
    }

    fn to_wire_side(side: Side) -> wire_side::Side {
        match side {
            Side::Buy => wire_side::Side::Buy,
            Side::Sell => wire_side::Side::Sell,
        }
    }

    fn to_wire_tif(tif: TimeInForce) -> wire_tif::TimeInForce {
        match tif {
            TimeInForce::Gtc => wire_tif::TimeInForce::Gtc,
            TimeInForce::Ioc => wire_tif::TimeInForce::Ioc,
            TimeInForce::PostOnly => wire_tif::TimeInForce::PostOnly,
        }
    }

    fn poll_until_control(runtime: &mut Runtime) {
        for _ in 0..100 {
            if runtime.poll_once().expect("poll") {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("expected control request");
    }

    fn kill_switch(action: KillSwitchAction) -> Vec<u8> {
        let mut bytes = vec![0_u8; SBE_HEADER_LEN + kill_switch_codec::SBE_BLOCK_LENGTH as usize];
        encode_header(
            &mut bytes,
            kill_switch_codec::SBE_BLOCK_LENGTH,
            MessageKind::KillSwitch,
        )
        .expect("header");
        let write_buf = WriteBuf::new(&mut bytes);
        let mut encoder =
            kill_switch_codec::KillSwitchEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
        encoder.request_id(1).action(match action {
            KillSwitchAction::HaltTrading => kill_switch_action::KillSwitchAction::HaltTrading,
            KillSwitchAction::Shutdown => kill_switch_action::KillSwitchAction::Shutdown,
            KillSwitchAction::ResumeTrading => kill_switch_action::KillSwitchAction::ResumeTrading,
        });
        bytes
    }

    fn heartbeat(seq: u32) -> Vec<u8> {
        let mut bytes = vec![0_u8; SBE_HEADER_LEN + heartbeat_codec::SBE_BLOCK_LENGTH as usize];
        encode_header(
            &mut bytes,
            heartbeat_codec::SBE_BLOCK_LENGTH,
            MessageKind::Heartbeat,
        )
        .expect("header");
        let write_buf = WriteBuf::new(&mut bytes);
        let mut encoder =
            heartbeat_codec::HeartbeatEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
        encoder.heartbeat_seq(seq);
        bytes
    }

    fn snapshot(request_id: u32) -> Vec<u8> {
        let mut bytes = vec![0_u8; SBE_HEADER_LEN + snapshot_codec::SBE_BLOCK_LENGTH as usize];
        encode_header(
            &mut bytes,
            snapshot_codec::SBE_BLOCK_LENGTH,
            MessageKind::Snapshot,
        )
        .expect("header");
        let write_buf = WriteBuf::new(&mut bytes);
        let mut encoder =
            snapshot_codec::SnapshotEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
        encoder.request_id(request_id);
        bytes
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum PublishedSnapshotEvent {
        Begin {
            bid_price: Option<Price>,
            bid_qty: u64,
            ask_price: Option<Price>,
            resting_order_count: u64,
            risk_reference_price: Price,
        },
        Order {
            row_index: u64,
            client_order_id: OrderId,
        },
        End {
            row_count: u64,
            result: ControlResult,
        },
    }

    fn read_control_frame(client: &mut TcpStream) -> Vec<u8> {
        let mut header = [0_u8; SBE_HEADER_LEN];
        client.read_exact(&mut header).expect("read control header");
        let frame_len = SBE_HEADER_LEN + u16::from_le_bytes([header[0], header[1]]) as usize;
        let mut frame = vec![0_u8; frame_len];
        frame[..SBE_HEADER_LEN].copy_from_slice(&header);
        client
            .read_exact(&mut frame[SBE_HEADER_LEN..])
            .expect("read control payload");
        frame
    }

    fn read_gateway_frame(client: &mut TcpStream) -> Vec<u8> {
        let mut header = [0_u8; SBE_HEADER_LEN];
        client.read_exact(&mut header).expect("read gateway header");
        let frame_len = SBE_HEADER_LEN + u16::from_le_bytes([header[0], header[1]]) as usize;
        let mut frame = vec![0_u8; frame_len];
        frame[..SBE_HEADER_LEN].copy_from_slice(&header);
        client
            .read_exact(&mut frame[SBE_HEADER_LEN..])
            .expect("read gateway payload");
        frame
    }

    fn read_execution_reports(client: &mut TcpStream, count: usize) -> Vec<ExecutionReport> {
        let mut reports = Vec::with_capacity(count);
        while reports.len() < count {
            let frame = read_gateway_frame(client);
            let header = decode_header(&frame).expect("gateway header");
            if header.kind != MessageKind::ExecutionReport {
                continue;
            }
            let decoder = execution_report_codec::ExecutionReportDecoder::default().wrap(
                ReadBuf::new(&frame),
                SBE_HEADER_LEN,
                execution_report_codec::SBE_BLOCK_LENGTH,
                SCHEMA_VERSION,
            );
            reports.push(ExecutionReport {
                engine_seq: crate::domain::EngineSeq::new(decoder.engine_seq()),
                client_order_id: OrderId::new(decoder.client_order_id()).expect("order"),
                account_id: AccountId::new(decoder.account_id()).expect("account"),
                exec_type: match decoder.exec_type() {
                    crate::protocol::generated::codecs::exec_type::ExecType::New => ExecType::New,
                    crate::protocol::generated::codecs::exec_type::ExecType::Trade => {
                        ExecType::Trade
                    }
                    crate::protocol::generated::codecs::exec_type::ExecType::Canceled => {
                        ExecType::Canceled
                    }
                    crate::protocol::generated::codecs::exec_type::ExecType::Replaced => {
                        ExecType::Replaced
                    }
                    crate::protocol::generated::codecs::exec_type::ExecType::Rejected => {
                        ExecType::Rejected
                    }
                    crate::protocol::generated::codecs::exec_type::ExecType::Expired => {
                        ExecType::Expired
                    }
                    crate::protocol::generated::codecs::exec_type::ExecType::NullVal => {
                        panic!("null exec type")
                    }
                },
                ord_status: match decoder.ord_status() {
                    crate::protocol::generated::codecs::ord_status::OrdStatus::New => {
                        OrdStatus::New
                    }
                    crate::protocol::generated::codecs::ord_status::OrdStatus::PartiallyFilled => {
                        OrdStatus::PartiallyFilled
                    }
                    crate::protocol::generated::codecs::ord_status::OrdStatus::Filled => {
                        OrdStatus::Filled
                    }
                    crate::protocol::generated::codecs::ord_status::OrdStatus::Canceled => {
                        OrdStatus::Canceled
                    }
                    crate::protocol::generated::codecs::ord_status::OrdStatus::Replaced => {
                        OrdStatus::Replaced
                    }
                    crate::protocol::generated::codecs::ord_status::OrdStatus::Rejected => {
                        OrdStatus::Rejected
                    }
                    crate::protocol::generated::codecs::ord_status::OrdStatus::Expired => {
                        OrdStatus::Expired
                    }
                    crate::protocol::generated::codecs::ord_status::OrdStatus::NullVal => {
                        panic!("null order status")
                    }
                },
                reject_reason: match decoder.reject_reason() {
                    wire_reject_reason::RejectReason::None => RejectReason::None,
                    wire_reject_reason::RejectReason::Malformed => RejectReason::Malformed,
                    wire_reject_reason::RejectReason::UnknownOrder => RejectReason::UnknownOrder,
                    wire_reject_reason::RejectReason::InvalidPrice => RejectReason::InvalidPrice,
                    wire_reject_reason::RejectReason::InvalidQuantity => {
                        RejectReason::InvalidQuantity
                    }
                    wire_reject_reason::RejectReason::RiskOpenOrderLimit => {
                        RejectReason::RiskOpenOrderLimit
                    }
                    wire_reject_reason::RejectReason::RiskNotionalLimit => {
                        RejectReason::RiskNotionalLimit
                    }
                    wire_reject_reason::RejectReason::PriceBand => RejectReason::PriceBand,
                    wire_reject_reason::RejectReason::SelfTradePrevention => {
                        RejectReason::SelfTradePrevention
                    }
                    wire_reject_reason::RejectReason::KillSwitchActive => {
                        RejectReason::KillSwitchActive
                    }
                    wire_reject_reason::RejectReason::HeartbeatLost => RejectReason::HeartbeatLost,
                    wire_reject_reason::RejectReason::NullVal => panic!("null reject reason"),
                },
                fill_price: if decoder.fill_price() == 0 {
                    None
                } else {
                    Some(price(decoder.fill_price()))
                },
                fill_qty: decoder.fill_qty(),
                remaining_qty: decoder.remaining_qty(),
            });
        }
        reports
    }

    fn assert_no_control_frame(client: &mut TcpStream) {
        let mut header = [0_u8; SBE_HEADER_LEN];
        let error = client
            .read_exact(&mut header)
            .expect_err("unexpected control frame");
        assert!(
            matches!(
                error.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ),
            "unexpected read error: {error:?}"
        );
    }

    fn optional_price(raw: u64) -> Option<Price> {
        (raw != 0).then(|| price(raw))
    }

    fn read_snapshot_events(client: &mut TcpStream, count: usize) -> Vec<PublishedSnapshotEvent> {
        let mut events = Vec::with_capacity(count);
        for _ in 0..count {
            let frame = read_control_frame(client);
            let header = decode_header(&frame).expect("control header");
            let read_buf = ReadBuf::new(&frame);
            match header.kind {
                MessageKind::SnapshotBegin => {
                    let decoder = snapshot_begin_codec::SnapshotBeginDecoder::default().wrap(
                        read_buf,
                        SBE_HEADER_LEN,
                        snapshot_begin_codec::SBE_BLOCK_LENGTH,
                        SCHEMA_VERSION,
                    );
                    events.push(PublishedSnapshotEvent::Begin {
                        bid_price: optional_price(decoder.bid_price()),
                        bid_qty: decoder.bid_qty(),
                        ask_price: optional_price(decoder.ask_price()),
                        resting_order_count: decoder.resting_order_count(),
                        risk_reference_price: price(decoder.risk_reference_price()),
                    });
                }
                MessageKind::SnapshotOrder => {
                    let decoder = snapshot_order_codec::SnapshotOrderDecoder::default().wrap(
                        read_buf,
                        SBE_HEADER_LEN,
                        snapshot_order_codec::SBE_BLOCK_LENGTH,
                        SCHEMA_VERSION,
                    );
                    assert_ne!(decoder.side(), wire_side::Side::NullVal);
                    events.push(PublishedSnapshotEvent::Order {
                        row_index: decoder.row_index(),
                        client_order_id: OrderId::new(decoder.client_order_id()).expect("order"),
                    });
                }
                MessageKind::SnapshotEnd => {
                    let decoder = snapshot_end_codec::SnapshotEndDecoder::default().wrap(
                        read_buf,
                        SBE_HEADER_LEN,
                        snapshot_end_codec::SBE_BLOCK_LENGTH,
                        SCHEMA_VERSION,
                    );
                    assert_eq!(decoder.reason(), wire_reject_reason::RejectReason::None);
                    events.push(PublishedSnapshotEvent::End {
                        row_count: decoder.row_count(),
                        result: match decoder.result() {
                            wire_control_result::ControlResult::Ok => ControlResult::Ok,
                            wire_control_result::ControlResult::Rejected => ControlResult::Rejected,
                            wire_control_result::ControlResult::NullVal => {
                                panic!("null snapshot result")
                            }
                        },
                    });
                }
                kind => panic!("unexpected control event: {kind:?}"),
            }
        }
        events
    }

    #[test]
    fn poll_once_processes_gateway_orders_fifo_through_engine() {
        let Some((mut runtime, mut gateway_client)) = runtime_with_orders(
            [
                new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
                new_order(2, 2, Side::Buy, TimeInForce::Gtc, 1_001, 5),
            ],
            risk_config(10, 1_000_000),
        ) else {
            return;
        };

        let made_progress = runtime.poll_once().expect("poll");

        assert!(made_progress);
        assert_eq!(runtime.engine().best_bid(), Some((price(1_001), 5)));
        let reports = read_execution_reports(&mut gateway_client, 2);
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].client_order_id.raw(), 1);
        assert_eq!(reports[1].client_order_id.raw(), 2);
    }

    #[test]
    fn risk_reject_uses_shared_engine_sequence_and_publishes() {
        let Some((mut runtime, mut gateway_client)) = runtime_with_orders(
            [
                new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
                new_order(2, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
            ],
            risk_config(1, 1_000_000),
        ) else {
            return;
        };

        let made_progress = runtime.poll_once().expect("poll");
        let reports = read_execution_reports(&mut gateway_client, 2);

        assert!(made_progress);
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].engine_seq.raw(), 1);
        assert_eq!(reports[1].engine_seq.raw(), 2);
        assert_eq!(reports[1].reject_reason, RejectReason::RiskOpenOrderLimit);
        assert_eq!(runtime.engine().resting_order_count(), 1);
    }

    #[test]
    fn risk_reject_does_not_mutate_book() {
        let Some((mut runtime, mut gateway_client)) = runtime_with_orders(
            [new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10)],
            risk_config(10, 9_999),
        ) else {
            return;
        };

        let made_progress = runtime.poll_once().expect("poll");

        assert!(made_progress);
        assert_eq!(runtime.engine().resting_order_count(), 0);
        assert_eq!(
            runtime
                .engine()
                .account_open_notional(AccountId::new(1).expect("account")),
            Some(0)
        );
        assert_eq!(
            read_execution_reports(&mut gateway_client, 1)[0].reject_reason,
            RejectReason::RiskNotionalLimit
        );
    }

    #[test]
    fn duplicate_order_id_reaches_engine_structural_reject() {
        let Some((mut runtime, mut gateway_client)) = runtime_with_orders(
            [
                new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
                new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
            ],
            risk_config(10, 1_000_000),
        ) else {
            return;
        };

        let made_progress = runtime.poll_once().expect("poll");
        let reports = read_execution_reports(&mut gateway_client, 2);

        assert!(made_progress);
        assert_eq!(reports.len(), 2);
        assert_eq!(reports[1].client_order_id.raw(), 1);
        assert_eq!(reports[1].reject_reason, RejectReason::Malformed);
        assert_eq!(runtime.engine().resting_order_count(), 1);
    }

    #[test]
    fn unknown_replace_reaches_engine_unknown_order_reject() {
        let Some((mut runtime, mut gateway_client)) =
            runtime_with_orders([replace(2, 1, 404, 1_000, 15)], risk_config(10, 1_000_000))
        else {
            return;
        };

        let made_progress = runtime.poll_once().expect("poll");
        let reports = read_execution_reports(&mut gateway_client, 1);

        assert!(made_progress);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].client_order_id.raw(), 2);
        assert_eq!(reports[0].reject_reason, RejectReason::UnknownOrder);
        assert_eq!(runtime.engine().resting_order_count(), 0);
    }

    #[test]
    fn poll_once_processes_gateway_without_closing_live_control() {
        let Some((mut runtime, mut gateway_client)) = runtime_with_orders(
            [new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10)],
            risk_config(10, 1_000_000),
        ) else {
            return;
        };

        let made_progress = runtime.poll_once().expect("poll");

        assert!(made_progress);
        assert_eq!(runtime.engine().resting_order_count(), 1);
        let _ = read_execution_reports(&mut gateway_client, 1);
    }

    #[test]
    fn empty_live_runtime_poll_is_open_without_progress() {
        let Some(mut runtime) = runtime_without_gateway_client(risk_config(10, 1_000_000)) else {
            return;
        };

        let made_progress = runtime.poll_once().expect("poll");

        assert!(!made_progress);
    }

    #[test]
    fn halt_trading_rejects_new_and_replace_before_risk() {
        let Some((mut runtime, mut client, mut gateway_client)) =
            runtime_with_control_client_and_orders(
                [
                    new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
                    replace(2, 1, 1, 1_001, 10),
                ],
                risk_config(10, 1_000_000),
            )
        else {
            return;
        };
        write_control(&mut client, &kill_switch(KillSwitchAction::HaltTrading));

        poll_until_control(&mut runtime);
        let reports = read_execution_reports(&mut gateway_client, 2);

        assert_eq!(reports.len(), 2);
        assert_eq!(runtime.engine().resting_order_count(), 0);
        assert!(
            reports
                .iter()
                .all(|report| report.reject_reason == RejectReason::KillSwitchActive)
        );
    }

    #[test]
    fn halt_trading_allows_cancel_flow_through_engine() {
        let Some((mut runtime, mut client, mut gateway_client)) =
            runtime_with_control_client_and_orders(
                [
                    new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
                    cancel(2, 1, 1),
                ],
                risk_config(10, 1_000_000),
            )
        else {
            return;
        };
        write_control(&mut client, &kill_switch(KillSwitchAction::HaltTrading));

        poll_until_control(&mut runtime);
        let reports = read_execution_reports(&mut gateway_client, 2);

        assert_eq!(reports.len(), 2);
        assert_eq!(reports[0].reject_reason, RejectReason::KillSwitchActive);
        assert_eq!(reports[1].reject_reason, RejectReason::UnknownOrder);
        assert_eq!(runtime.engine().resting_order_count(), 0);
    }

    #[test]
    fn heartbeat_timeout_starts_after_first_heartbeat() {
        let Some(bind_addr) = reserve_loopback_addr() else {
            return;
        };
        let Some(control) = new_control_with_config(ControlConfig {
            heartbeat_timeout_ns: 10,
            bind_addr: bind_addr.to_string(),
            ..ControlConfig::default()
        }) else {
            return;
        };
        let mut client = TcpStream::connect(bind_addr).expect("connect");
        let Some((mut runtime, mut gateway_client)) = runtime_with_control_and_orders(
            control,
            [new_order(
                1_000_000_000,
                1,
                Side::Buy,
                TimeInForce::Gtc,
                1_000,
                10,
            )],
            risk_config(10, 1_000_000),
        ) else {
            return;
        };
        write_control(&mut client, &heartbeat(1));

        poll_until_control(&mut runtime);
        let reports = read_execution_reports(&mut gateway_client, 1);

        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].reject_reason, RejectReason::HeartbeatLost);
        assert_eq!(runtime.engine().resting_order_count(), 0);
    }

    #[test]
    fn snapshot_emits_price_time_rows_and_state_metadata() {
        let Some((mut runtime, mut client, mut gateway_client)) =
            runtime_with_control_client_and_orders(
                [
                    new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
                    new_order(2, 2, Side::Buy, TimeInForce::Gtc, 1_001, 5),
                    new_order(3, 3, Side::Buy, TimeInForce::Gtc, 1_001, 7),
                    new_order(4, 4, Side::Sell, TimeInForce::Gtc, 1_100, 9),
                ],
                RiskConfig::new(10, 10_000_000, price(1_000), 2_000).expect("risk config"),
            )
        else {
            return;
        };
        runtime.poll_once().expect("seed book");
        let _ = read_execution_reports(&mut gateway_client, 4);
        write_control(&mut client, &snapshot(99));

        poll_until_control(&mut runtime);
        let events = read_snapshot_events(&mut client, 6);

        assert_eq!(events.len(), 6);
        let PublishedSnapshotEvent::Begin {
            bid_price,
            bid_qty,
            ask_price,
            resting_order_count,
            risk_reference_price,
        } = events[0]
        else {
            panic!("begin");
        };
        assert_eq!(bid_price, Some(price(1_001)));
        assert_eq!(bid_qty, 12);
        assert_eq!(ask_price, Some(price(1_100)));
        assert_eq!(resting_order_count, 4);
        assert_eq!(risk_reference_price, price(1_000));

        let rows: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                PublishedSnapshotEvent::Order {
                    row_index,
                    client_order_id,
                } => Some((*row_index, *client_order_id)),
                _ => None,
            })
            .collect();
        assert_eq!(
            rows.iter()
                .map(|(_, client_order_id)| client_order_id.raw())
                .collect::<Vec<_>>(),
            vec![2, 3, 1, 4]
        );
        assert_eq!(rows[0].0, 0);
        assert_eq!(rows[1].0, 1);

        let PublishedSnapshotEvent::End { row_count, result } = events[5] else {
            panic!("end");
        };
        assert_eq!(row_count, 4);
        assert_eq!(result, ControlResult::Ok);
    }

    #[test]
    fn shutdown_drains_book_and_closes_runtime() {
        let Some((mut runtime, mut client, mut gateway_client)) =
            runtime_with_control_client_and_orders(
                [new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10)],
                risk_config(10, 1_000_000),
            )
        else {
            return;
        };
        runtime.poll_once().expect("seed book");
        write_control(&mut client, &kill_switch(KillSwitchAction::Shutdown));

        poll_until_control(&mut runtime);
        let reports = read_execution_reports(&mut gateway_client, 2);

        assert!(runtime.terminal);
        assert_eq!(runtime.engine().resting_order_count(), 0);
        assert_eq!(
            reports.last().expect("drain cancel").reject_reason,
            RejectReason::None
        );
        assert_eq!(
            reports.last().expect("drain cancel").ord_status,
            OrdStatus::Canceled
        );
    }

    #[test]
    fn shutdown_stops_same_poll_control_processing() {
        let Some((mut runtime, mut client, _gateway_client)) =
            runtime_with_control_client_and_orders([], risk_config(10, 1_000_000))
        else {
            return;
        };
        let mut frames = kill_switch(KillSwitchAction::Shutdown);
        frames.extend_from_slice(&snapshot(99));
        write_control(&mut client, &frames);

        poll_until_control(&mut runtime);
        let frame = read_control_frame(&mut client);

        assert!(runtime.terminal);
        assert_eq!(
            decode_header(&frame).expect("control header").kind,
            MessageKind::ControlResponse
        );
        assert_no_control_frame(&mut client);
    }
}
