//! Control-plane runtime boundary.
//!
//! The control plane is the runtime's operational safety boundary. It owns
//! kill-switch state and heartbeat liveness, while the main `Runtime` owns
//! engine-dependent side effects such as snapshots and shutdown book drains.
//! `Runtime` polls control before gateway/risk/matching so a halt or heartbeat
//! failure is visible before any new risk-increasing order reaches the book.
//!
//! Inbound flow is intentionally allocation-free once the listener is open:
//! `IORuntime` lends each complete frame to `ControlRuntime::poll`,
//! `wire::control` decodes it into a typed `ControlRequest`, control-owned
//! state is updated immediately, and control-owned replies are published
//! immediately through the borrowed `ControlPublisher`.
//!
//! Runtime-owned work is handled by the callback passed to `poll`. The callback
//! sees the same typed request and can use the borrowed publisher to stream
//! control egress, for example snapshot begin/row/end messages, without
//! staging requests or events in intermediate vectors.

use crate::runtime::Mode;
use crate::{
    domain::{
        DomainError, ProtocolErrorEvent, RecvTs, RejectReason, Sequencer,
        control::{
            ControlEvent, ControlRequest, ControlResponse, ControlResult, Heartbeat, HeartbeatSeq, HeartbeatStatus, KillSwitch,
            KillSwitchAction,
        },
    },
    io::{Endpoint, FramePollError, IOConfig, IOPublisher, IORuntime, IoError},
    protocol::{
        ProtocolError,
        wire::control::{WireDecodeError, decode_control_request, encode_control_event},
    },
};

/// Control-plane runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlConfig {
    /// Nanoseconds allowed between heartbeats. Zero disables heartbeat gating.
    pub heartbeat_timeout_ns: u64,
    /// Endpoint IO backend.
    pub io: IOConfig,
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout_ns: 0,
            io: IOConfig::Tcp {
                bind_addr: "127.0.0.1:9001".to_owned(),
                max_frame_len: 4096,
                max_pending_write_bytes: 1 << 20,
                record_path: None,
            },
        }
    }
}
/// Borrowed control egress handle for the current poll callback.
///
/// `ControlPublisher` reuses `ControlRuntime`'s encode buffer and the active
/// TCP writer ring. It exists only for the duration of processing one inbound
/// frame, which lets runtime-owned control work publish immediately without
/// taking ownership of the socket or allocating outbound frame storage.
#[derive(Debug)]
pub struct ControlPublisher<'a, 'b> {
    io: &'a mut IOPublisher<'b>,
    encode_buf: &'a mut Vec<u8>,
}

impl ControlPublisher<'_, '_> {
    /// Encode and publish one control event through the active control socket.
    ///
    /// The encoded bytes are valid only until the next publish because the
    /// backing buffer is reused. `IORuntime` copies those bytes into its
    /// bounded write ring before this method returns.
    pub fn publish(&mut self, event: ControlEvent) -> Result<(), ControlError> {
        let written = encode_control_event(event, self.encode_buf)?;
        self.io.publish(&self.encode_buf[..written])?;
        Ok(())
    }
}

/// Control-plane runtime.
///
/// This type owns the control listener, the reusable outbound encode buffer,
/// and the operational state used by the order path. It does not own
/// engine/book state; requests that require engine data are forwarded to the
/// runtime callback during `poll`.
#[derive(Debug)]
pub struct ControlRuntime {
    io: IORuntime,
    encode_buf: Vec<u8>,
    state: ControlState,
}

impl ControlRuntime {
    /// Open the configured control I/O and preallocate runtime buffers.
    ///
    /// All configurable transport bounds are applied here so the poll path only
    /// reads already-normalized state. The encode buffer is sized once from
    /// `max_frame_len` and reused for every control response.
    pub fn new(config: ControlConfig) -> Result<Self, ControlError> {
        let encode_len = config.io.frame_capacity();
        let io = IORuntime::new(Endpoint::Control, config.io)?;
        Ok(Self {
            io,
            encode_buf: vec![0_u8; encode_len],
            state: ControlState::new(config.heartbeat_timeout_ns),
        })
    }

    /// Poll control input and handle every complete frame currently available.
    ///
    /// The method preserves the runtime phase contract: decode first, apply
    /// control-owned effects immediately, publish control-owned acknowledgments
    /// immediately, then invoke `on_request` for runtime-owned side effects.
    /// The callback receives a `ControlPublisher` so snapshot output can be
    /// streamed directly over the control connection.
    ///
    /// The returned boolean means forward progress was made by the underlying
    /// transport or frame processing. It is deliberately not a count; callers
    /// should observe state or output streams for behavior-specific assertions.
    pub fn poll<F>(&mut self, mode: &mut Mode, sequencer: &mut Sequencer, mut on_request: F) -> Result<bool, ControlError>
    where
        F: FnMut(ControlRequest, &mut ControlPublisher<'_, '_>, &mut Sequencer) -> Result<(), ControlError>,
    {
        let ControlRuntime { io, encode_buf, state } = self;
        let mut session = ControlPollSession {
            state,
            encode_buf,
            sequencer,
            on_request: &mut on_request,
            shutdown_seen: false,
        };
        let io_progress = match io.poll(mode, |io_publisher, recv_ts, bytes| session.on_frame(io_publisher, recv_ts, bytes)) {
            Ok(progress) => progress,
            Err(FramePollError::Io(error)) => return Err(error.into()),
            Err(FramePollError::Handler(error)) => return Err(error),
        };

        Ok(io_progress)
    }

    /// Return the current pre-risk reject reason for risk-increasing orders.
    ///
    /// Runtime calls this after control has been polled and before risk/match.
    /// Cancels and mass cancels are intentionally not gated by this reason.
    pub fn reject_reason_for(&self, recv_ts: RecvTs) -> RejectReason {
        self.state.reject_reason_for(recv_ts)
    }

    pub fn replay_input_exhausted(&mut self) -> Result<bool, ControlError> {
        self.io.replay_input_exhausted().map_err(ControlError::Io)
    }
}

#[derive(Debug)]
struct ControlState {
    halted: bool,
    shutdown_requested: bool,
    last_heartbeat_seq: Option<HeartbeatSeq>,
    last_heartbeat_recv_ts: Option<RecvTs>,
    heartbeat_timeout_ns: u64,
}

impl ControlState {
    const fn new(heartbeat_timeout_ns: u64) -> Self {
        Self {
            halted: false,
            shutdown_requested: false,
            last_heartbeat_seq: None,
            last_heartbeat_recv_ts: None,
            heartbeat_timeout_ns,
        }
    }

    fn reject_reason_for(&self, recv_ts: RecvTs) -> RejectReason {
        if self.shutdown_requested {
            return RejectReason::KillSwitchActive;
        }
        if self.halted {
            return RejectReason::KillSwitchActive;
        }

        // A zero timeout means heartbeat enforcement is disabled. With a
        // nonzero timeout, enforcement starts only after the first heartbeat
        // because runtime has no wall-clock startup timestamp.
        let Some(last_recv_ts) = self.last_heartbeat_recv_ts else {
            return RejectReason::None;
        };
        if self.heartbeat_timeout_ns != 0 && recv_ts.raw().saturating_sub(last_recv_ts.raw()) > self.heartbeat_timeout_ns {
            return RejectReason::HeartbeatLost;
        }

        RejectReason::None
    }

    fn apply_kill_switch(&mut self, request: KillSwitch) -> bool {
        match request.action {
            KillSwitchAction::HaltTrading => {
                self.halted = true;
                false
            }
            KillSwitchAction::ResumeTrading => {
                self.halted = false;
                false
            }
            KillSwitchAction::Shutdown => {
                self.shutdown_requested = true;
                true
            }
        }
    }

    fn apply_heartbeat(&mut self, heartbeat: Heartbeat) {
        self.last_heartbeat_seq = Some(heartbeat.heartbeat_seq);
        self.last_heartbeat_recv_ts = Some(heartbeat.recv_ts);
    }
}

struct ControlPollSession<'a, F> {
    state: &'a mut ControlState,
    encode_buf: &'a mut Vec<u8>,
    sequencer: &'a mut Sequencer,
    on_request: &'a mut F,
    shutdown_seen: bool,
}

impl<F> ControlPollSession<'_, F>
where
    F: FnMut(ControlRequest, &mut ControlPublisher<'_, '_>, &mut Sequencer) -> Result<(), ControlError>,
{
    /// Process one complete wire frame borrowed from `IORuntime`.
    ///
    /// This method is the per-frame hot path. It performs no allocation and
    /// does not retain `bytes`; decode errors are converted to protocol-error
    /// events immediately, while valid requests pass through state mutation and
    /// the runtime callback.
    fn on_frame(&mut self, io_publisher: &mut IOPublisher<'_>, recv_ts: RecvTs, bytes: &[u8]) -> Result<(), ControlError> {
        if self.shutdown_seen {
            // Shutdown is terminal for this poll batch. Frames already read
            // after it remain buffered by the transport state but produce no
            // same-iteration side effects.
            return Ok(());
        }

        let decoded = decode_control_request(bytes, recv_ts);
        let mut publisher = ControlPublisher {
            io: io_publisher,
            encode_buf: self.encode_buf,
        };
        match decoded {
            Ok(request) => {
                let shutdown = Self::apply_request(self.state, self.sequencer, request, &mut publisher)?;
                // Runtime owns engine-dependent side effects such as snapshots
                // and shutdown drains, so it observes every valid request
                // after control has applied its own state transition and
                // response.
                (self.on_request)(request, &mut publisher, self.sequencer)?;
                if shutdown {
                    self.shutdown_seen = true;
                }
                Ok(())
            }
            Err(error) => Self::publish_protocol_error(self.sequencer, error, &mut publisher),
        }
    }

    fn apply_request(
        state: &mut ControlState,
        sequencer: &mut Sequencer,
        request: ControlRequest,
        publisher: &mut ControlPublisher<'_, '_>,
    ) -> Result<bool, ControlError> {
        match request {
            ControlRequest::KillSwitch(request) => {
                let shutdown = state.apply_kill_switch(request);
                Self::push_ok_response(sequencer, request, publisher)?;
                Ok(shutdown)
            }
            ControlRequest::Heartbeat(heartbeat) => {
                state.apply_heartbeat(heartbeat);
                publisher.publish(ControlEvent::HeartbeatStatus(HeartbeatStatus {
                    engine_seq: sequencer.next_engine_seq()?,
                    heartbeat_seq: heartbeat.heartbeat_seq,
                    recv_ts: heartbeat.recv_ts,
                    result: ControlResult::Ok,
                }))?;
                Ok(false)
            }
            // Snapshot content comes from runtime-owned engine/risk/market
            // data state, so control only forwards the typed request.
            ControlRequest::Snapshot(_) => Ok(false),
        }
    }

    fn push_ok_response(
        sequencer: &mut Sequencer,
        request: KillSwitch,
        publisher: &mut ControlPublisher<'_, '_>,
    ) -> Result<(), ControlError> {
        publisher.publish(ControlEvent::ControlResponse(ControlResponse {
            engine_seq: sequencer.next_engine_seq()?,
            request_id: request.request_id,
            recv_ts: request.recv_ts,
            result: ControlResult::Ok,
            reason: RejectReason::None,
        }))?;
        Ok(())
    }

    fn publish_protocol_error(
        sequencer: &mut Sequencer,
        error: WireDecodeError,
        publisher: &mut ControlPublisher<'_, '_>,
    ) -> Result<(), ControlError> {
        publisher.publish(ControlEvent::ProtocolError(ProtocolErrorEvent {
            engine_seq: sequencer.next_engine_seq()?,
            template_id: error.template_id,
            recv_ts: error.recv_ts,
            error_code: error.error_code,
        }))
    }
}

/// Control runtime failure.
#[derive(Debug)]
pub enum ControlError {
    Io(IoError),
    Encode(ProtocolError),
    Domain(DomainError),
}

impl From<IoError> for ControlError {
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

impl From<ProtocolError> for ControlError {
    fn from(error: ProtocolError) -> Self {
        Self::Encode(error)
    }
}

impl From<DomainError> for ControlError {
    fn from(error: DomainError) -> Self {
        Self::Domain(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        MessageKind, SBE_HEADER_LEN, decode_header, encode_header,
        generated::codecs::{
            WriteBuf, heartbeat_codec, kill_switch_action, kill_switch_codec, new_order_codec, order_type as wire_order_type,
            side as wire_side, time_in_force as wire_tif,
        },
    };
    use std::{
        io::{Read, Write},
        net::{SocketAddr, TcpListener, TcpStream},
        thread,
        time::Duration,
    };

    fn reserve_loopback_addr() -> Option<SocketAddr> {
        match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => Some(listener.local_addr().expect("addr")),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(error) => panic!("reserve control addr: {error:?}"),
        }
    }

    fn test_control_with_addr() -> Option<(ControlRuntime, SocketAddr)> {
        let bind_addr = reserve_loopback_addr()?;
        let config = ControlConfig {
            io: IOConfig::Tcp {
                bind_addr: bind_addr.to_string(),
                max_frame_len: 4096,
                max_pending_write_bytes: 1 << 20,
                record_path: None,
            },
            ..ControlConfig::default()
        };
        match ControlRuntime::new(config) {
            Ok(control) => Some((control, bind_addr)),
            Err(ControlError::Io(IoError::Bind(error))) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(error) => panic!("control runtime: {error:?}"),
        }
    }

    fn heartbeat_frame(seq: u32) -> Vec<u8> {
        let mut bytes = vec![0_u8; SBE_HEADER_LEN + heartbeat_codec::SBE_BLOCK_LENGTH as usize];
        encode_header(&mut bytes, heartbeat_codec::SBE_BLOCK_LENGTH, MessageKind::Heartbeat).expect("header");
        let write_buf = WriteBuf::new(&mut bytes);
        let mut encoder = heartbeat_codec::HeartbeatEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
        encoder.heartbeat_seq(seq);
        bytes
    }

    fn kill_switch_frame(action: kill_switch_action::KillSwitchAction) -> Vec<u8> {
        let mut bytes = vec![0_u8; SBE_HEADER_LEN + kill_switch_codec::SBE_BLOCK_LENGTH as usize];
        encode_header(&mut bytes, kill_switch_codec::SBE_BLOCK_LENGTH, MessageKind::KillSwitch).expect("header");
        let write_buf = WriteBuf::new(&mut bytes);
        let mut encoder = kill_switch_codec::KillSwitchEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
        encoder.request_id(77).action(action);
        bytes
    }

    fn new_order_frame() -> Vec<u8> {
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
            .qty(10);
        bytes
    }

    fn poll_until_request(control: &mut ControlRuntime, out: &mut Vec<ControlRequest>) {
        let mut sequencer = Sequencer::default();
        let mut mode = crate::runtime::Mode::default();
        for _ in 0..100 {
            if control
                .poll(&mut mode, &mut sequencer, |request, _, _| {
                    out.push(request);
                    Ok(())
                })
                .expect("poll")
                && !out.is_empty()
            {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("expected control request");
    }

    fn read_control_frame(client: &mut TcpStream) -> Vec<u8> {
        let mut header = [0_u8; SBE_HEADER_LEN];
        client.read_exact(&mut header).expect("read header");
        let frame_len = SBE_HEADER_LEN + u16::from_le_bytes([header[0], header[1]]) as usize;
        let mut frame = vec![0_u8; frame_len];
        frame[..SBE_HEADER_LEN].copy_from_slice(&header);
        client.read_exact(&mut frame[SBE_HEADER_LEN..]).expect("read payload");
        frame
    }

    #[test]
    fn control_request_decode_normalizes_kill_switch_in_control() {
        let bytes = kill_switch_frame(kill_switch_action::KillSwitchAction::HaltTrading);
        let request = decode_control_request(&bytes, RecvTs::new(55)).expect("control request");

        let ControlRequest::KillSwitch(request) = request else {
            panic!("expected kill switch");
        };
        assert_eq!(request.request_id.raw(), 77);
        assert_eq!(request.recv_ts.raw(), 55);
        assert_eq!(request.action, KillSwitchAction::HaltTrading);
    }

    #[test]
    fn control_request_decode_rejects_wrong_endpoint_template() {
        let error = decode_control_request(&new_order_frame(), RecvTs::new(55)).expect_err("wrong endpoint");
        assert_eq!(error.template_id, 100);
        assert_eq!(error.error_code, crate::domain::ErrorCode::UnsupportedTemplate);
    }

    #[test]
    fn control_request_decode_rejects_null_action() {
        let bytes = kill_switch_frame(kill_switch_action::KillSwitchAction::NullVal);

        let error = decode_control_request(&bytes, RecvTs::new(55)).expect_err("null action should reject");
        assert_eq!(error.template_id, MessageKind::KillSwitch.template_id());
        assert_eq!(error.error_code, crate::domain::ErrorCode::DecodeError);
    }

    #[test]
    fn poll_returns_progress_boolean_from_tcp_frame() {
        let Some((mut control, bind_addr)) = test_control_with_addr() else {
            return;
        };
        let mut client = TcpStream::connect(bind_addr).expect("connect");
        client.write_all(&heartbeat_frame(1)).expect("write");
        let mut out = Vec::new();

        poll_until_request(&mut control, &mut out);

        let ControlRequest::Heartbeat(heartbeat) = out[0] else {
            panic!("heartbeat");
        };
        assert_eq!(heartbeat.heartbeat_seq.raw(), 1);
        out.clear();
        let mut sequencer = Sequencer::default();
        let mut mode = crate::runtime::Mode::default();
        assert!(
            !control
                .poll(&mut mode, &mut sequencer, |request, _, _| {
                    out.push(request);
                    Ok(())
                })
                .expect("second poll")
        );
    }

    #[test]
    fn protocol_errors_emit_during_poll_and_live_control_stays_not_drained() {
        let Some((mut control, bind_addr)) = test_control_with_addr() else {
            return;
        };
        let mut client = TcpStream::connect(bind_addr).expect("connect");
        client.write_all(&new_order_frame()).expect("write");
        let mut sequencer = Sequencer::default();
        let mut mode = crate::runtime::Mode::default();
        let mut requests = Vec::new();

        for _ in 0..100 {
            control
                .poll(&mut mode, &mut sequencer, |request, _, _| {
                    requests.push(request);
                    Ok(())
                })
                .expect("poll");
            if !requests.is_empty() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }

        assert!(requests.is_empty());
        assert_eq!(
            decode_header(&read_control_frame(&mut client)).expect("header").kind,
            MessageKind::ProtocolError
        );
    }

    #[test]
    fn kill_switch_response_sequences_and_updates_gate_during_poll() {
        let Some((mut control, bind_addr)) = test_control_with_addr() else {
            return;
        };
        let mut client = TcpStream::connect(bind_addr).expect("connect");
        client
            .write_all(&kill_switch_frame(kill_switch_action::KillSwitchAction::HaltTrading))
            .expect("write");
        let mut sequencer = Sequencer::default();
        let mut mode = crate::runtime::Mode::default();
        let mut requests = Vec::new();

        for _ in 0..100 {
            control
                .poll(&mut mode, &mut sequencer, |request, _, _| {
                    requests.push(request);
                    Ok(())
                })
                .expect("poll");
            if !requests.is_empty() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }

        assert!(matches!(requests[0], ControlRequest::KillSwitch(_)));
        assert_eq!(
            decode_header(&read_control_frame(&mut client)).expect("header").kind,
            MessageKind::ControlResponse
        );
        assert_eq!(control.reject_reason_for(RecvTs::new(2)), RejectReason::KillSwitchActive);
    }
}
