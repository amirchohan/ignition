//! Order-entry gateway runtime boundary.
//!
//! The gateway is the runtime's order-entry boundary. It owns the trading
//! listener and trading egress, while the main `Runtime` owns control gating,
//! risk checks, matching, market-data sequencing, and shutdown drains.
//! `Runtime` polls gateway after control so a halt or heartbeat failure is
//! visible before any new risk-increasing order reaches the book.
//!
//! Inbound flow is intentionally allocation-free once the listener is open:
//! `TcpFrameIo` lends each complete frame to `GatewayRuntime::poll`,
//! `wire::trading` decodes and normalizes it into a typed `OrderEntry`, and
//! runtime-owned matching work is invoked immediately through the poll
//! callback. Protocol decode errors and order-shape rejects are published
//! immediately through the borrowed `GatewayPublisher`.
//!
//! Runtime-owned work is handled by the callback passed to `poll`. The callback
//! sees one typed order-entry command and can use the borrowed publisher to
//! queue execution reports without staging orders or events in intermediate
//! vectors. Market-data output is emitted by the runtime-owned market-data feed.

use crate::{
    domain::{
        DomainError, ProtocolErrorEvent, RecvTs, RejectReason, Sequencer,
        rd::InstrumentSpec,
        trading::order::{AccountId, ExecType, ExecutionReport, OrdStatus, OrderEntry, OrderId},
    },
    io::{FramePollError, IoError, TcpFrameIo, TcpFramePublisher},
    protocol::{
        ProtocolError,
        wire::{
            encode_protocol_error, protocol_error_code, template_id,
            trading::{
                encode_execution_report,
                order::{
                    WireCancelOrder, WireCancelReplace, WireMassCancel, WireNewOrder, WireOrderEntry, decode_order_entry,
                    normalize_order_entry,
                },
            },
        },
    },
};

/// Trading gateway runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayConfig {
    /// TCP address for the trading listener.
    pub bind_addr: String,
    /// Maximum accepted SBE frame length including the SBE header.
    pub max_frame_len: usize,
    /// Maximum queued outbound bytes before disconnecting the active client.
    pub max_pending_write_bytes: usize,
    /// Maximum inbound frames processed in one runtime poll.
    pub max_messages_per_poll: usize,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9000".to_owned(),
            max_frame_len: 4096,
            max_pending_write_bytes: 1 << 20,
            max_messages_per_poll: 1024,
        }
    }
}

/// Borrowed trading egress handle for the current order-entry callback.
///
/// `GatewayPublisher` reuses `GatewayRuntime`'s encode buffer and the active
/// TCP writer ring. It exists only for the duration of processing one inbound
/// frame, which lets runtime-owned order work publish immediately without
/// taking ownership of the socket or allocating outbound frame storage.
pub struct GatewayPublisher<'a, 'b> {
    io: &'a mut TcpFramePublisher<'b>,
    encode_buf: &'a mut Vec<u8>,
    first_error: Option<GatewayError>,
}

impl GatewayPublisher<'_, '_> {
    /// Publish one execution report to this gateway's egress sink.
    ///
    /// The encoded bytes are valid only until the next publish because the
    /// backing buffer is reused. `TcpFrameIo` copies those bytes into its
    /// bounded write ring before this method returns.
    pub fn publish_execution_report(&mut self, event: ExecutionReport) -> Result<(), GatewayError> {
        let written = encode_execution_report(event, self.encode_buf)?;
        self.io.queue_frame(&self.encode_buf[..written])?;
        Ok(())
    }

    /// Return the first egress failure deferred by sink-style emission.
    pub fn check_deferred_error(&mut self) -> Result<(), GatewayError> {
        match self.first_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn publish_protocol_error_event(&mut self, event: ProtocolErrorEvent) -> Result<(), GatewayError> {
        let written = encode_protocol_error(event, self.encode_buf)?;
        self.io.queue_frame(&self.encode_buf[..written])?;
        Ok(())
    }
}

impl GatewayPublisher<'_, '_> {
    /// Queue one execution report and defer the first egress failure until checked.
    pub fn emit_execution_report(&mut self, report: ExecutionReport) {
        if self.first_error.is_some() {
            return;
        }
        if let Err(error) = self.publish_execution_report(report) {
            self.first_error = Some(error);
        }
    }
}

/// Live order-entry gateway runtime.
///
/// This type owns the trading listener, the reusable outbound encode buffer,
/// the configured instrument used for order-entry normalization, and the
/// per-poll frame budget. It does not own engine/book/risk state; valid orders
/// are forwarded to the runtime callback during `poll`.
#[derive(Debug)]
pub struct GatewayRuntime {
    io: TcpFrameIo,
    encode_buf: Vec<u8>,
    instrument: InstrumentSpec,
    max_messages_per_poll: usize,
    first_error: Option<GatewayError>,
}

impl GatewayRuntime {
    /// Open the configured live trading listener and preallocate buffers.
    ///
    /// All configurable transport bounds are applied here so the poll path only
    /// reads already-normalized state. The encode buffer is sized once from
    /// `max_frame_len` and reused for every trading event.
    pub fn new(config: GatewayConfig, instrument: InstrumentSpec) -> Result<Self, GatewayError> {
        let io = TcpFrameIo::bind(&config.bind_addr, config.max_frame_len, config.max_pending_write_bytes)?;
        Ok(Self {
            io,
            encode_buf: vec![0_u8; config.max_frame_len],
            instrument,
            max_messages_per_poll: config.max_messages_per_poll.max(1),
            first_error: None,
        })
    }

    /// Poll order-entry input and process each decoded command immediately.
    ///
    /// The method preserves the runtime phase contract: decode first, publish
    /// gateway-owned protocol or shape errors immediately, then invoke
    /// `on_order` for runtime-owned risk/matching side effects. The callback
    /// receives a `GatewayPublisher` so execution reports can be queued
    /// directly into the trading connection's write ring.
    ///
    /// The returned boolean means forward progress was made by the underlying
    /// transport or frame processing. It is deliberately not a count; callers
    /// should observe state or output streams for behavior-specific assertions.
    pub fn poll<F, E>(&mut self, sequencer: &mut Sequencer, mut on_order: F) -> Result<bool, GatewayPollError<E>>
    where
        F: FnMut(OrderEntry, &mut GatewayPublisher<'_, '_>, &mut Sequencer) -> Result<(), E>,
    {
        let mut session = GatewaySession {
            encode_buf: &mut self.encode_buf,
            instrument: self.instrument,
            sequencer,
            on_order: &mut on_order,
            handled_frame: false,
        };
        let io_progress = match self.io.poll_bounded(
            |io_publisher, recv_ts, bytes| session.on_frame(io_publisher, recv_ts, bytes),
            self.max_messages_per_poll,
        ) {
            Ok(progress) => progress,
            Err(FramePollError::Io(error)) => {
                return Err(GatewayPollError::Gateway(error.into()));
            }
            Err(FramePollError::Handler(error)) => return Err(error),
        };

        Ok(io_progress || session.handled_frame)
    }

    /// Flush queued order-entry egress outside normal polling.
    pub fn flush_pending(&mut self) -> Result<bool, GatewayError> {
        self.io.flush_pending().map_err(GatewayError::Io)
    }

    /// Return the first egress failure deferred by sink-style emission.
    pub fn check_deferred_error(&mut self) -> Result<(), GatewayError> {
        match self.first_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn queue_execution_report(&mut self, report: ExecutionReport) -> Result<(), GatewayError> {
        let written = encode_execution_report(report, &mut self.encode_buf)?;
        self.io.queue_frame(&self.encode_buf[..written])?;
        Ok(())
    }
}

impl GatewayRuntime {
    /// Queue one execution report and defer the first egress failure until checked.
    pub fn emit_execution_report(&mut self, report: ExecutionReport) {
        if self.first_error.is_some() {
            return;
        }
        if let Err(error) = self.queue_execution_report(report) {
            self.first_error = Some(error);
        }
    }
}

struct GatewaySession<'a, F> {
    encode_buf: &'a mut Vec<u8>,
    instrument: InstrumentSpec,
    sequencer: &'a mut Sequencer,
    on_order: &'a mut F,
    handled_frame: bool,
}

impl<F, E> GatewaySession<'_, F>
where
    F: FnMut(OrderEntry, &mut GatewayPublisher<'_, '_>, &mut Sequencer) -> Result<(), E>,
{
    fn on_frame(&mut self, io_publisher: &mut TcpFramePublisher<'_>, recv_ts: RecvTs, bytes: &[u8]) -> Result<(), GatewayPollError<E>> {
        self.handled_frame = true;
        let mut publisher = GatewayPublisher {
            io: io_publisher,
            encode_buf: self.encode_buf,
            first_error: None,
        };

        let result = match decode_order_entry(bytes) {
            Ok(wire) => match normalize_order_entry(wire, recv_ts, &self.instrument) {
                Ok(entry) => (self.on_order)(entry, &mut publisher, self.sequencer).map_err(GatewayPollError::Handler),
                Err(error) => publish_order_shape_reject(self.sequencer, wire, error.reject_reason(), &mut publisher),
            },
            Err(error) => publish_protocol_error(self.sequencer, error, bytes, recv_ts, &mut publisher),
        };
        result?;
        publisher.check_deferred_error().map_err(GatewayPollError::Gateway)
    }
}

fn publish_order_shape_reject<E>(
    sequencer: &mut Sequencer,
    wire: WireOrderEntry,
    reason: RejectReason,
    publisher: &mut GatewayPublisher<'_, '_>,
) -> Result<(), GatewayPollError<E>> {
    let reject = reject_report(sequencer, wire, reason)
        .map_err(GatewayError::Domain)
        .map_err(GatewayPollError::Gateway)?;
    publisher.publish_execution_report(reject).map_err(GatewayPollError::Gateway)?;
    Ok(())
}

fn publish_protocol_error<E>(
    sequencer: &mut Sequencer,
    error: ProtocolError,
    bytes: &[u8],
    recv_ts: RecvTs,
    publisher: &mut GatewayPublisher<'_, '_>,
) -> Result<(), GatewayPollError<E>> {
    let engine_seq = sequencer
        .next_engine_seq()
        .map_err(GatewayError::Domain)
        .map_err(GatewayPollError::Gateway)?;
    publisher
        .publish_protocol_error_event(ProtocolErrorEvent {
            engine_seq,
            template_id: template_id(bytes),
            recv_ts,
            error_code: protocol_error_code(error),
        })
        .map_err(GatewayPollError::Gateway)
}

fn reject_report(sequencer: &mut Sequencer, wire: WireOrderEntry, reason: RejectReason) -> Result<ExecutionReport, DomainError> {
    let (client_order_id, account_id, remaining_qty) = match wire {
        WireOrderEntry::NewOrder(WireNewOrder {
            client_order_id,
            account_id,
            qty,
            ..
        }) => (client_order_id, account_id, qty),
        WireOrderEntry::CancelOrder(WireCancelOrder {
            client_order_id,
            account_id,
            ..
        })
        | WireOrderEntry::MassCancel(WireMassCancel {
            client_order_id,
            account_id,
            ..
        }) => (client_order_id, account_id, 0),
        WireOrderEntry::CancelReplace(WireCancelReplace {
            client_order_id,
            account_id,
            qty,
            ..
        }) => (client_order_id, account_id, qty),
    };

    Ok(ExecutionReport {
        engine_seq: sequencer.next_engine_seq()?,
        client_order_id: OrderId::new(client_order_id)?,
        account_id: AccountId::new(account_id)?,
        exec_type: ExecType::Rejected,
        ord_status: OrdStatus::Rejected,
        reject_reason: reason,
        fill_price: None,
        fill_qty: 0,
        remaining_qty,
    })
}

/// Gateway-owned failure.
#[derive(Debug)]
pub enum GatewayError {
    Io(IoError),
    Encode(ProtocolError),
    Domain(DomainError),
}

impl From<IoError> for GatewayError {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

impl From<ProtocolError> for GatewayError {
    fn from(error: ProtocolError) -> Self {
        Self::Encode(error)
    }
}

impl From<DomainError> for GatewayError {
    fn from(error: DomainError) -> Self {
        Self::Domain(error)
    }
}

/// Gateway poll can fail from gateway mechanics or from the runtime callback.
#[derive(Debug)]
pub enum GatewayPollError<E> {
    Gateway(GatewayError),
    Handler(E),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{EngineSeq, trading::order::NewOrder},
        protocol::{
            MessageKind, SBE_HEADER_LEN, SCHEMA_VERSION, decode_header, encode_header,
            generated::codecs::{
                ReadBuf, WriteBuf, execution_report_codec, kill_switch_action, kill_switch_codec, new_order_codec,
                order_type as wire_order_type, reject_reason as wire_reject_reason, side as wire_side, time_in_force as wire_tif,
            },
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

    fn reserve_loopback_addr() -> Option<SocketAddr> {
        match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => Some(listener.local_addr().expect("addr")),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(error) => panic!("reserve gateway addr: {error:?}"),
        }
    }

    fn live_gateway() -> Option<(GatewayRuntime, SocketAddr)> {
        let bind_addr = reserve_loopback_addr()?;
        let config = GatewayConfig {
            bind_addr: bind_addr.to_string(),
            ..GatewayConfig::default()
        };
        match GatewayRuntime::new(config, instrument()) {
            Ok(gateway) => Some((gateway, bind_addr)),
            Err(GatewayError::Io(IoError::Bind(error))) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(error) => panic!("gateway: {error:?}"),
        }
    }

    fn new_order_frame(qty: u64) -> Vec<u8> {
        let mut bytes = vec![0_u8; SBE_HEADER_LEN + new_order_codec::SBE_BLOCK_LENGTH as usize];
        encode_header(&mut bytes, new_order_codec::SBE_BLOCK_LENGTH, MessageKind::NewOrder).expect("header");
        let write_buf = WriteBuf::new(&mut bytes);
        let mut encoder = new_order_codec::NewOrderEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
        encoder
            .client_order_id(11)
            .account_id(22)
            .client_ts(33)
            .side(wire_side::Side::Buy)
            .order_type(wire_order_type::OrderType::Limit)
            .time_in_force(wire_tif::TimeInForce::Gtc)
            .price(100)
            .qty(qty);
        bytes
    }

    fn kill_switch_frame() -> Vec<u8> {
        let mut bytes = vec![0_u8; SBE_HEADER_LEN + kill_switch_codec::SBE_BLOCK_LENGTH as usize];
        encode_header(&mut bytes, kill_switch_codec::SBE_BLOCK_LENGTH, MessageKind::KillSwitch).expect("header");
        let write_buf = WriteBuf::new(&mut bytes);
        let mut encoder = kill_switch_codec::KillSwitchEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
        encoder.request_id(1).action(kill_switch_action::KillSwitchAction::HaltTrading);
        bytes
    }

    fn read_gateway_frame(client: &mut TcpStream) -> Vec<u8> {
        let mut header = [0_u8; SBE_HEADER_LEN];
        client.read_exact(&mut header).expect("read header");
        let frame_len = SBE_HEADER_LEN + u16::from_le_bytes([header[0], header[1]]) as usize;
        let mut frame = vec![0_u8; frame_len];
        frame[..SBE_HEADER_LEN].copy_from_slice(&header);
        client.read_exact(&mut frame[SBE_HEADER_LEN..]).expect("read payload");
        frame
    }

    fn poll_until_progress<F>(gateway: &mut GatewayRuntime, sequencer: &mut Sequencer, f: F)
    where
        F: FnMut(OrderEntry, &mut GatewayPublisher<'_, '_>, &mut Sequencer) -> Result<(), GatewayError> + Copy,
    {
        for _ in 0..100 {
            if gateway.poll(sequencer, f).expect("poll") {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("expected gateway progress");
    }

    fn ack_report(order: NewOrder, engine_seq: EngineSeq) -> ExecutionReport {
        ExecutionReport {
            engine_seq,
            client_order_id: order.client_order_id,
            account_id: order.account_id,
            exec_type: ExecType::New,
            ord_status: OrdStatus::New,
            reject_reason: RejectReason::None,
            fill_price: None,
            fill_qty: 0,
            remaining_qty: order.qty.raw(),
        }
    }

    #[test]
    fn live_gateway_decodes_order_and_publishes_trading_event() {
        let Some((mut gateway, bind_addr)) = live_gateway() else {
            return;
        };
        let mut client = TcpStream::connect(bind_addr).expect("connect");
        client.set_read_timeout(Some(Duration::from_secs(1))).expect("timeout");
        client.write_all(&new_order_frame(10)).expect("write");
        let mut sequencer = Sequencer::default();

        poll_until_progress(&mut gateway, &mut sequencer, |entry, publisher, sequencer| {
            let OrderEntry::NewOrder(order) = entry else {
                panic!("new order");
            };
            publisher.publish_execution_report(ack_report(order, sequencer.next_engine_seq()?))
        });

        let frame = read_gateway_frame(&mut client);
        assert_eq!(decode_header(&frame).expect("header").kind, MessageKind::ExecutionReport);
        let decoder = execution_report_codec::ExecutionReportDecoder::default().wrap(
            ReadBuf::new(&frame),
            SBE_HEADER_LEN,
            execution_report_codec::SBE_BLOCK_LENGTH,
            SCHEMA_VERSION,
        );
        assert_eq!(decoder.client_order_id(), 11);
        assert_eq!(decoder.account_id(), 22);
        assert_eq!(decoder.remaining_qty(), 10);
    }

    #[test]
    fn live_gateway_rejects_malformed_order_payload_as_execution_report() {
        let Some((mut gateway, bind_addr)) = live_gateway() else {
            return;
        };
        let mut client = TcpStream::connect(bind_addr).expect("connect");
        client.set_read_timeout(Some(Duration::from_secs(1))).expect("timeout");
        client.write_all(&new_order_frame(0)).expect("write");
        let mut sequencer = Sequencer::default();
        let mut callback_seen = false;

        for _ in 0..100 {
            if gateway
                .poll(&mut sequencer, |_, _, _| {
                    callback_seen = true;
                    Ok::<(), GatewayError>(())
                })
                .expect("poll")
            {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }

        assert!(!callback_seen);
        let frame = read_gateway_frame(&mut client);
        assert_eq!(decode_header(&frame).expect("header").kind, MessageKind::ExecutionReport);
        let decoder = execution_report_codec::ExecutionReportDecoder::default().wrap(
            ReadBuf::new(&frame),
            SBE_HEADER_LEN,
            execution_report_codec::SBE_BLOCK_LENGTH,
            SCHEMA_VERSION,
        );
        assert_eq!(decoder.client_order_id(), 11);
        assert_eq!(decoder.reject_reason(), wire_reject_reason::RejectReason::InvalidQuantity);
    }

    #[test]
    fn live_gateway_rejects_wrong_endpoint_template_as_protocol_error() {
        let Some((mut gateway, bind_addr)) = live_gateway() else {
            return;
        };
        let mut client = TcpStream::connect(bind_addr).expect("connect");
        client.set_read_timeout(Some(Duration::from_secs(1))).expect("timeout");
        client.write_all(&kill_switch_frame()).expect("write");
        let mut sequencer = Sequencer::default();

        poll_until_progress(&mut gateway, &mut sequencer, |_, _, _| {
            panic!("wrong endpoint should not reach callback")
        });

        let frame = read_gateway_frame(&mut client);
        assert_eq!(decode_header(&frame).expect("header").kind, MessageKind::ProtocolError);
    }
}
