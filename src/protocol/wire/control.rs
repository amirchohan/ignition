use crate::{
    domain::{
        ErrorCode, RecvTs,
        control::{
            ControlEvent, ControlRequest, ControlResponse, Heartbeat, HeartbeatSeq,
            HeartbeatStatus, KillSwitch, KillSwitchAction, RequestId, Snapshot, SnapshotBegin,
            SnapshotEnd, SnapshotOrder,
        },
    },
    protocol::{
        MessageKind, ProtocolError, ProtocolResult, SBE_HEADER_LEN, decode_header, encode_header,
        generated::codecs::{
            ReadBuf, WriteBuf, control_response_codec, heartbeat_codec, heartbeat_status_codec,
            kill_switch_action, kill_switch_codec, snapshot_begin_codec, snapshot_codec,
            snapshot_end_codec, snapshot_order_codec,
        },
    },
};

use super::{
    encode_protocol_error, ensure_complete, ensure_out, protocol_error_code, template_id,
    to_wire_control_result, to_wire_reject_reason, to_wire_side, unexpected_template,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireDecodeError {
    pub template_id: u16,
    pub recv_ts: RecvTs,
    pub error_code: ErrorCode,
}

impl WireDecodeError {
    fn protocol(error: ProtocolError, bytes: &[u8], recv_ts: RecvTs) -> Self {
        Self {
            template_id: template_id(bytes),
            recv_ts,
            error_code: protocol_error_code(error),
        }
    }

    fn domain(bytes: &[u8], recv_ts: RecvTs) -> Self {
        Self {
            template_id: template_id(bytes),
            recv_ts,
            error_code: ErrorCode::DecodeError,
        }
    }
}

pub fn decode_control_request(
    input: &[u8],
    recv_ts: RecvTs,
) -> Result<ControlRequest, WireDecodeError> {
    let frame =
        decode_header(input).map_err(|error| WireDecodeError::protocol(error, input, recv_ts))?;
    ensure_complete(input, frame)
        .map_err(|error| WireDecodeError::protocol(error, input, recv_ts))?;

    if !frame.kind.is_control_request() {
        return Err(WireDecodeError::protocol(
            unexpected_template(frame.kind),
            input,
            recv_ts,
        ));
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
            Ok(ControlRequest::KillSwitch(KillSwitch {
                request_id: RequestId::new(decoder.request_id())
                    .map_err(|_| WireDecodeError::domain(input, recv_ts))?,
                recv_ts,
                action: match decoder.action() {
                    kill_switch_action::KillSwitchAction::HaltTrading => {
                        KillSwitchAction::HaltTrading
                    }
                    kill_switch_action::KillSwitchAction::Shutdown => KillSwitchAction::Shutdown,
                    kill_switch_action::KillSwitchAction::ResumeTrading => {
                        KillSwitchAction::ResumeTrading
                    }
                    kill_switch_action::KillSwitchAction::NullVal => {
                        return Err(WireDecodeError::domain(input, recv_ts));
                    }
                },
            }))
        }
        MessageKind::Snapshot => {
            let decoder = snapshot_codec::SnapshotDecoder::default().wrap(
                read_buf,
                SBE_HEADER_LEN,
                frame.block_length,
                frame.version,
            );
            Ok(ControlRequest::Snapshot(Snapshot {
                request_id: RequestId::new(decoder.request_id())
                    .map_err(|_| WireDecodeError::domain(input, recv_ts))?,
                recv_ts,
            }))
        }
        MessageKind::Heartbeat => {
            let decoder = heartbeat_codec::HeartbeatDecoder::default().wrap(
                read_buf,
                SBE_HEADER_LEN,
                frame.block_length,
                frame.version,
            );
            Ok(ControlRequest::Heartbeat(Heartbeat {
                heartbeat_seq: HeartbeatSeq::new(decoder.heartbeat_seq())
                    .map_err(|_| WireDecodeError::domain(input, recv_ts))?,
                recv_ts,
            }))
        }
        _ => unreachable!("control request kind checked above"),
    }
}

pub fn encode_control_event(event: ControlEvent, out: &mut [u8]) -> ProtocolResult<usize> {
    match event {
        ControlEvent::ControlResponse(event) => encode_control_response(event, out),
        ControlEvent::HeartbeatStatus(event) => encode_heartbeat_status(event, out),
        ControlEvent::SnapshotBegin(event) => encode_snapshot_begin(event, out),
        ControlEvent::SnapshotOrder(event) => encode_snapshot_order(event, out),
        ControlEvent::SnapshotEnd(event) => encode_snapshot_end(event, out),
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

fn encode_snapshot_begin(event: SnapshotBegin, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = snapshot_begin_codec::SBE_BLOCK_LENGTH;
    ensure_out(out, block_len)?;
    encode_header(out, block_len, MessageKind::SnapshotBegin)?;

    let write_buf = WriteBuf::new(out);
    let mut encoder =
        snapshot_begin_codec::SnapshotBeginEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .engine_seq(event.engine_seq.raw())
        .request_id(event.request_id.raw())
        .recv_ts(event.recv_ts.raw())
        .bid_price(event.bid_price.map_or(0, |price| price.raw()))
        .bid_qty(event.bid_qty)
        .ask_price(event.ask_price.map_or(0, |price| price.raw()))
        .ask_qty(event.ask_qty)
        .resting_order_count(event.resting_order_count)
        .last_md_seq(event.last_md_seq)
        .next_trade_id(event.next_trade_id)
        .risk_reference_price(event.risk_reference_price.raw());
    Ok(SBE_HEADER_LEN + snapshot_begin_codec::SBE_BLOCK_LENGTH as usize)
}

fn encode_snapshot_order(event: SnapshotOrder, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = snapshot_order_codec::SBE_BLOCK_LENGTH;
    ensure_out(out, block_len)?;
    encode_header(out, block_len, MessageKind::SnapshotOrder)?;

    let write_buf = WriteBuf::new(out);
    let mut encoder =
        snapshot_order_codec::SnapshotOrderEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .engine_seq(event.engine_seq.raw())
        .request_id(event.request_id.raw())
        .row_index(event.row_index)
        .side(to_wire_side(event.side))
        .client_order_id(event.client_order_id.raw())
        .account_id(event.account_id.raw())
        .price(event.price.raw())
        .remaining_qty(event.remaining_qty);
    Ok(SBE_HEADER_LEN + snapshot_order_codec::SBE_BLOCK_LENGTH as usize)
}

fn encode_snapshot_end(event: SnapshotEnd, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = snapshot_end_codec::SBE_BLOCK_LENGTH;
    ensure_out(out, block_len)?;
    encode_header(out, block_len, MessageKind::SnapshotEnd)?;

    let write_buf = WriteBuf::new(out);
    let mut encoder =
        snapshot_end_codec::SnapshotEndEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .engine_seq(event.engine_seq.raw())
        .request_id(event.request_id.raw())
        .recv_ts(event.recv_ts.raw())
        .row_count(event.row_count)
        .result(to_wire_control_result(event.result))
        .reason(to_wire_reject_reason(event.reason));
    Ok(SBE_HEADER_LEN + snapshot_end_codec::SBE_BLOCK_LENGTH as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            EngineSeq, ErrorCode, Price, ProtocolErrorEvent, RecvTs, RejectReason,
            control::{
                ControlResult, HeartbeatSeq, HeartbeatStatus, RequestId, SnapshotBegin,
                SnapshotEnd, SnapshotOrder,
            },
            rd::InstrumentSpec,
            trading::order::{AccountId, OrderId, Side},
        },
        protocol::{
            SCHEMA_VERSION, decode_header,
            generated::codecs::{
                ReadBuf, message_header_codec, protocol_error_codec, side as wire_side,
                snapshot_begin_codec, snapshot_end_codec, snapshot_order_codec,
            },
        },
    };

    fn instrument() -> InstrumentSpec {
        InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument")
    }

    fn price(raw: u64) -> Price {
        Price::new(raw, &instrument()).expect("price")
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

    #[test]
    fn control_event_encodes_snapshot_messages() {
        let request_id = RequestId::new(99).expect("request");

        let mut begin_bytes =
            [0_u8; SBE_HEADER_LEN + snapshot_begin_codec::SBE_BLOCK_LENGTH as usize];
        encode_control_event(
            ControlEvent::SnapshotBegin(SnapshotBegin {
                engine_seq: EngineSeq::new(1),
                request_id,
                recv_ts: RecvTs::new(100),
                bid_price: Some(price(1_001)),
                bid_qty: 12,
                ask_price: None,
                ask_qty: 0,
                resting_order_count: 3,
                last_md_seq: 44,
                next_trade_id: 55,
                risk_reference_price: price(1_000),
            }),
            &mut begin_bytes,
        )
        .expect("encode begin");
        assert_eq!(
            decode_header(&begin_bytes).expect("header").kind,
            MessageKind::SnapshotBegin
        );
        let begin = snapshot_begin_codec::SnapshotBeginDecoder::default().wrap(
            ReadBuf::new(&begin_bytes),
            SBE_HEADER_LEN,
            snapshot_begin_codec::SBE_BLOCK_LENGTH,
            SCHEMA_VERSION,
        );
        assert_eq!(begin.bid_price(), 1_001);
        assert_eq!(begin.ask_price(), 0);
        assert_eq!(begin.last_md_seq(), 44);

        let mut row_bytes =
            [0_u8; SBE_HEADER_LEN + snapshot_order_codec::SBE_BLOCK_LENGTH as usize];
        encode_control_event(
            ControlEvent::SnapshotOrder(SnapshotOrder {
                engine_seq: EngineSeq::new(2),
                request_id,
                row_index: 0,
                side: Side::Buy,
                client_order_id: OrderId::new(11).expect("order"),
                account_id: AccountId::new(22).expect("account"),
                price: price(1_001),
                remaining_qty: 9,
            }),
            &mut row_bytes,
        )
        .expect("encode row");
        let row = snapshot_order_codec::SnapshotOrderDecoder::default().wrap(
            ReadBuf::new(&row_bytes),
            SBE_HEADER_LEN,
            snapshot_order_codec::SBE_BLOCK_LENGTH,
            SCHEMA_VERSION,
        );
        assert_eq!(row.side(), wire_side::Side::Buy);
        assert_eq!(row.client_order_id(), 11);

        let mut end_bytes = [0_u8; SBE_HEADER_LEN + snapshot_end_codec::SBE_BLOCK_LENGTH as usize];
        encode_control_event(
            ControlEvent::SnapshotEnd(SnapshotEnd {
                engine_seq: EngineSeq::new(3),
                request_id,
                recv_ts: RecvTs::new(100),
                row_count: 1,
                result: ControlResult::Ok,
                reason: RejectReason::None,
            }),
            &mut end_bytes,
        )
        .expect("encode end");
        assert_eq!(
            decode_header(&end_bytes).expect("header").kind,
            MessageKind::SnapshotEnd
        );
    }
}
