use crate::{
    domain::{
        DomainError, DomainResult, RecvTs,
        control::{
            ControlEvent, ControlRequest, ControlResponse, Heartbeat, HeartbeatSeq,
            HeartbeatStatus, KillSwitch, KillSwitchAction, RequestId, Snapshot,
        },
    },
    protocol::{
        MessageKind, ProtocolResult, SBE_HEADER_LEN, decode_header, encode_header,
        generated::codecs::{
            ReadBuf, WriteBuf, control_response_codec, heartbeat_codec, heartbeat_status_codec,
            kill_switch_action, kill_switch_codec, snapshot_codec,
        },
    },
};

use super::{
    InboundDecodeError, encode_protocol_error, ensure_complete, ensure_out, to_wire_control_result,
    to_wire_reject_reason, unexpected_template,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireControlRequest {
    KillSwitch(WireKillSwitch),
    Snapshot(WireSnapshot),
    Heartbeat(WireHeartbeat),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireKillSwitch {
    pub request_id: u32,
    pub action: kill_switch_action::KillSwitchAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireSnapshot {
    pub request_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireHeartbeat {
    pub heartbeat_seq: u32,
}

pub fn decode_control_request(input: &[u8]) -> ProtocolResult<WireControlRequest> {
    let frame = decode_header(input)?;
    ensure_complete(input, frame)?;

    if !frame.kind.is_control_request() {
        return Err(unexpected_template(frame.kind));
    }

    let read_buf = ReadBuf::new(input);
    match frame.kind {
        MessageKind::KillSwitch => {
            let decoder = kill_switch_codec::KillSwitchDecoder::default().wrap(
                read_buf,
                SBE_HEADER_LEN,
                frame.block_length,
                frame.version,
            );
            Ok(WireControlRequest::KillSwitch(WireKillSwitch {
                request_id: decoder.request_id(),
                action: decoder.action(),
            }))
        }
        MessageKind::Snapshot => {
            let decoder = snapshot_codec::SnapshotDecoder::default().wrap(
                read_buf,
                SBE_HEADER_LEN,
                frame.block_length,
                frame.version,
            );
            Ok(WireControlRequest::Snapshot(WireSnapshot {
                request_id: decoder.request_id(),
            }))
        }
        MessageKind::Heartbeat => {
            let decoder = heartbeat_codec::HeartbeatDecoder::default().wrap(
                read_buf,
                SBE_HEADER_LEN,
                frame.block_length,
                frame.version,
            );
            Ok(WireControlRequest::Heartbeat(WireHeartbeat {
                heartbeat_seq: decoder.heartbeat_seq(),
            }))
        }
        _ => unreachable!("control request kind checked above"),
    }
}

pub fn normalize_control_request(
    message: WireControlRequest,
    recv_ts: RecvTs,
) -> DomainResult<ControlRequest> {
    match message {
        WireControlRequest::KillSwitch(message) => Ok(ControlRequest::KillSwitch(KillSwitch {
            request_id: RequestId::new(message.request_id)?,
            recv_ts,
            action: normalize_kill_switch_action(message.action)?,
        })),
        WireControlRequest::Snapshot(message) => Ok(ControlRequest::Snapshot(Snapshot {
            request_id: RequestId::new(message.request_id)?,
            recv_ts,
        })),
        WireControlRequest::Heartbeat(message) => Ok(ControlRequest::Heartbeat(Heartbeat {
            heartbeat_seq: HeartbeatSeq::new(message.heartbeat_seq)?,
            recv_ts,
        })),
    }
}

pub fn decode_and_normalize_control_request(
    input: &[u8],
    recv_ts: RecvTs,
) -> Result<ControlRequest, InboundDecodeError> {
    let message = decode_control_request(input).map_err(InboundDecodeError::Protocol)?;
    normalize_control_request(message, recv_ts).map_err(InboundDecodeError::Domain)
}

pub fn encode_control_event(event: ControlEvent, out: &mut [u8]) -> ProtocolResult<usize> {
    match event {
        ControlEvent::ControlResponse(event) => encode_control_response(event, out),
        ControlEvent::HeartbeatStatus(event) => encode_heartbeat_status(event, out),
        ControlEvent::ProtocolError(event) => encode_protocol_error(event, out),
    }
}

fn encode_control_response(event: ControlResponse, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = control_response_codec::SBE_BLOCK_LENGTH;
    ensure_out(out, block_len)?;
    encode_header(out, block_len, MessageKind::ControlResponse)?;

    let write_buf = WriteBuf::new(out);
    let mut encoder =
        control_response_codec::ControlResponseEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .engine_seq(event.engine_seq.raw())
        .request_id(event.request_id.raw())
        .recv_ts(event.recv_ts.raw())
        .result(to_wire_control_result(event.result))
        .reason(to_wire_reject_reason(event.reason));
    Ok(SBE_HEADER_LEN + control_response_codec::SBE_BLOCK_LENGTH as usize)
}

fn encode_heartbeat_status(event: HeartbeatStatus, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = heartbeat_status_codec::SBE_BLOCK_LENGTH;
    ensure_out(out, block_len)?;
    encode_header(out, block_len, MessageKind::HeartbeatStatus)?;

    let write_buf = WriteBuf::new(out);
    let mut encoder =
        heartbeat_status_codec::HeartbeatStatusEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .engine_seq(event.engine_seq.raw())
        .heartbeat_seq(event.heartbeat_seq.raw())
        .recv_ts(event.recv_ts.raw())
        .result(to_wire_control_result(event.result));
    Ok(SBE_HEADER_LEN + heartbeat_status_codec::SBE_BLOCK_LENGTH as usize)
}

fn normalize_kill_switch_action(
    value: kill_switch_action::KillSwitchAction,
) -> DomainResult<KillSwitchAction> {
    match value {
        kill_switch_action::KillSwitchAction::HaltTrading => Ok(KillSwitchAction::HaltTrading),
        kill_switch_action::KillSwitchAction::Shutdown => Ok(KillSwitchAction::Shutdown),
        kill_switch_action::KillSwitchAction::ResumeTrading => Ok(KillSwitchAction::ResumeTrading),
        kill_switch_action::KillSwitchAction::NullVal => Err(DomainError::Malformed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            EngineSeq, ErrorCode, ProtocolErrorEvent,
            control::{ControlResult, HeartbeatStatus},
        },
        protocol::{
            ProtocolError,
            generated::codecs::{
                message_header_codec, new_order_codec, order_type as wire_order_type,
                protocol_error_codec, side as wire_side, time_in_force as wire_tif,
            },
        },
    };

    fn encode_kill_switch(action: kill_switch_action::KillSwitchAction) -> Vec<u8> {
        let len = SBE_HEADER_LEN + kill_switch_codec::SBE_BLOCK_LENGTH as usize;
        let mut bytes = vec![0_u8; len];
        encode_header(
            &mut bytes,
            kill_switch_codec::SBE_BLOCK_LENGTH,
            MessageKind::KillSwitch,
        )
        .expect("header");

        let write_buf = WriteBuf::new(&mut bytes);
        let mut encoder =
            kill_switch_codec::KillSwitchEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
        encoder.request_id(77).action(action);
        bytes
    }

    fn encode_new_order() -> Vec<u8> {
        let len = SBE_HEADER_LEN + new_order_codec::SBE_BLOCK_LENGTH as usize;
        let mut bytes = vec![0_u8; len];
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

    #[test]
    fn control_request_normalizes_kill_switch() {
        let bytes = encode_kill_switch(kill_switch_action::KillSwitchAction::HaltTrading);
        let request =
            decode_and_normalize_control_request(&bytes, RecvTs::new(55)).expect("control request");

        let ControlRequest::KillSwitch(request) = request else {
            panic!("expected kill switch");
        };

        assert_eq!(request.request_id.raw(), 77);
        assert_eq!(request.recv_ts.raw(), 55);
        assert_eq!(request.action, KillSwitchAction::HaltTrading);
    }

    #[test]
    fn control_request_rejects_wrong_endpoint_template() {
        assert_eq!(
            decode_control_request(&encode_new_order()),
            Err(ProtocolError::UnexpectedTemplate { template_id: 100 })
        );
    }

    #[test]
    fn control_request_rejects_null_action() {
        let bytes = encode_kill_switch(kill_switch_action::KillSwitchAction::NullVal);

        assert_eq!(
            decode_and_normalize_control_request(&bytes, RecvTs::new(55)),
            Err(InboundDecodeError::Domain(DomainError::Malformed))
        );
    }

    #[test]
    fn control_event_encodes_heartbeat_status_and_protocol_error() {
        let mut bytes = [0_u8; SBE_HEADER_LEN + heartbeat_status_codec::SBE_BLOCK_LENGTH as usize];
        let written = encode_control_event(
            ControlEvent::HeartbeatStatus(HeartbeatStatus {
                engine_seq: EngineSeq::new(7),
                heartbeat_seq: HeartbeatSeq::new(10).expect("heartbeat"),
                recv_ts: RecvTs::new(900),
                result: ControlResult::Ok,
            }),
            &mut bytes,
        )
        .expect("encode heartbeat status");
        assert_eq!(written, bytes.len());
        assert_eq!(
            decode_header(&bytes).expect("header").kind,
            MessageKind::HeartbeatStatus
        );

        let mut error_bytes =
            [0_u8; SBE_HEADER_LEN + protocol_error_codec::SBE_BLOCK_LENGTH as usize];
        encode_control_event(
            ControlEvent::ProtocolError(ProtocolErrorEvent {
                engine_seq: EngineSeq::new(8),
                template_id: 222,
                recv_ts: RecvTs::new(901),
                error_code: ErrorCode::UnsupportedTemplate,
            }),
            &mut error_bytes,
        )
        .expect("encode protocol error");

        let decoder = message_header_codec::MessageHeaderDecoder::default()
            .wrap(ReadBuf::new(&error_bytes), 0);
        assert_eq!(
            decoder.template_id(),
            MessageKind::ProtocolError.template_id()
        );
    }
}
