use crate::protocol::{
    MessageKind, ProtocolError, ProtocolResult, SBE_HEADER_LEN, decode_header, encode_header,
    generated::codecs::{
        ReadBuf, WriteBuf, control_response_codec, heartbeat_codec, heartbeat_status_codec, kill_switch_action, kill_switch_codec,
        protocol_error_codec, snapshot_begin_codec, snapshot_codec, snapshot_end_codec, snapshot_order_codec,
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientControlFrame {
    KillSwitch(ClientKillSwitch),
    Snapshot(ClientSnapshot),
    Heartbeat(ClientHeartbeat),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientKillSwitch {
    pub request_id: u32,
    pub action: kill_switch_action::KillSwitchAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientSnapshot {
    pub request_id: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientHeartbeat {
    pub heartbeat_seq: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientControlEvent {
    ControlResponse(ClientControlResponse),
    HeartbeatStatus(ClientHeartbeatStatus),
    SnapshotBegin(ClientSnapshotBegin),
    SnapshotOrder(ClientSnapshotOrder),
    SnapshotEnd(ClientSnapshotEnd),
    ProtocolError(ClientProtocolError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientControlResponse {
    pub engine_seq: u64,
    pub request_id: u32,
    pub recv_ts: u64,
    pub result: crate::protocol::generated::codecs::control_result::ControlResult,
    pub reason: crate::protocol::generated::codecs::reject_reason::RejectReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientHeartbeatStatus {
    pub engine_seq: u64,
    pub heartbeat_seq: u32,
    pub recv_ts: u64,
    pub result: crate::protocol::generated::codecs::control_result::ControlResult,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientSnapshotBegin {
    pub engine_seq: u64,
    pub request_id: u32,
    pub recv_ts: u64,
    pub bid_price: u64,
    pub bid_qty: u64,
    pub ask_price: u64,
    pub ask_qty: u64,
    pub resting_order_count: u64,
    pub last_md_seq: u64,
    pub next_trade_id: u64,
    pub risk_reference_price: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientSnapshotOrder {
    pub engine_seq: u64,
    pub request_id: u32,
    pub row_index: u64,
    pub side: crate::protocol::generated::codecs::side::Side,
    pub client_order_id: u64,
    pub account_id: u32,
    pub price: u64,
    pub remaining_qty: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientSnapshotEnd {
    pub engine_seq: u64,
    pub request_id: u32,
    pub recv_ts: u64,
    pub row_count: u64,
    pub result: crate::protocol::generated::codecs::control_result::ControlResult,
    pub reason: crate::protocol::generated::codecs::reject_reason::RejectReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientProtocolError {
    pub engine_seq: u64,
    pub template_id: u16,
    pub recv_ts: u64,
    pub error_code: crate::protocol::generated::codecs::error_code::ErrorCode,
}

pub fn encode_control_frame(frame: ClientControlFrame, out: &mut [u8]) -> ProtocolResult<usize> {
    match frame {
        ClientControlFrame::KillSwitch(frame) => encode_kill_switch(frame, out),
        ClientControlFrame::Snapshot(frame) => encode_snapshot(frame, out),
        ClientControlFrame::Heartbeat(frame) => encode_heartbeat(frame, out),
    }
}

pub fn decode_control_event(input: &[u8]) -> ProtocolResult<ClientControlEvent> {
    let frame = decode_header(input)?;
    if input.len() < SBE_HEADER_LEN + frame.block_length as usize {
        return Err(ProtocolError::BufferTooShort);
    }
    let read_buf = ReadBuf::new(input);
    match frame.kind {
        MessageKind::ControlResponse => {
            let decoder =
                control_response_codec::ControlResponseDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(ClientControlEvent::ControlResponse(ClientControlResponse {
                engine_seq: decoder.engine_seq(),
                request_id: decoder.request_id(),
                recv_ts: decoder.recv_ts(),
                result: decoder.result(),
                reason: decoder.reason(),
            }))
        }
        MessageKind::HeartbeatStatus => {
            let decoder =
                heartbeat_status_codec::HeartbeatStatusDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(ClientControlEvent::HeartbeatStatus(ClientHeartbeatStatus {
                engine_seq: decoder.engine_seq(),
                heartbeat_seq: decoder.heartbeat_seq(),
                recv_ts: decoder.recv_ts(),
                result: decoder.result(),
            }))
        }
        MessageKind::SnapshotBegin => {
            let decoder =
                snapshot_begin_codec::SnapshotBeginDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(ClientControlEvent::SnapshotBegin(ClientSnapshotBegin {
                engine_seq: decoder.engine_seq(),
                request_id: decoder.request_id(),
                recv_ts: decoder.recv_ts(),
                bid_price: decoder.bid_price(),
                bid_qty: decoder.bid_qty(),
                ask_price: decoder.ask_price(),
                ask_qty: decoder.ask_qty(),
                resting_order_count: decoder.resting_order_count(),
                last_md_seq: decoder.last_md_seq(),
                next_trade_id: decoder.next_trade_id(),
                risk_reference_price: decoder.risk_reference_price(),
            }))
        }
        MessageKind::SnapshotOrder => {
            let decoder =
                snapshot_order_codec::SnapshotOrderDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(ClientControlEvent::SnapshotOrder(ClientSnapshotOrder {
                engine_seq: decoder.engine_seq(),
                request_id: decoder.request_id(),
                row_index: decoder.row_index(),
                side: decoder.side(),
                client_order_id: decoder.client_order_id(),
                account_id: decoder.account_id(),
                price: decoder.price(),
                remaining_qty: decoder.remaining_qty(),
            }))
        }
        MessageKind::SnapshotEnd => {
            let decoder =
                snapshot_end_codec::SnapshotEndDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(ClientControlEvent::SnapshotEnd(ClientSnapshotEnd {
                engine_seq: decoder.engine_seq(),
                request_id: decoder.request_id(),
                recv_ts: decoder.recv_ts(),
                row_count: decoder.row_count(),
                result: decoder.result(),
                reason: decoder.reason(),
            }))
        }
        MessageKind::ProtocolError => {
            let decoder =
                protocol_error_codec::ProtocolErrorDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(ClientControlEvent::ProtocolError(ClientProtocolError {
                engine_seq: decoder.engine_seq(),
                template_id: decoder.template_id(),
                recv_ts: decoder.recv_ts(),
                error_code: decoder.error_code(),
            }))
        }
        _ => Err(ProtocolError::UnexpectedTemplate {
            template_id: frame.kind.template_id(),
        }),
    }
}

fn encode_kill_switch(frame: ClientKillSwitch, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = kill_switch_codec::SBE_BLOCK_LENGTH;
    encode_header(out, block_len, MessageKind::KillSwitch)?;
    let write_buf = WriteBuf::new(out);
    let mut encoder = kill_switch_codec::KillSwitchEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder.request_id(frame.request_id).action(frame.action);
    Ok(SBE_HEADER_LEN + block_len as usize)
}

fn encode_snapshot(frame: ClientSnapshot, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = snapshot_codec::SBE_BLOCK_LENGTH;
    encode_header(out, block_len, MessageKind::Snapshot)?;
    let write_buf = WriteBuf::new(out);
    let mut encoder = snapshot_codec::SnapshotEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder.request_id(frame.request_id);
    Ok(SBE_HEADER_LEN + block_len as usize)
}

fn encode_heartbeat(frame: ClientHeartbeat, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = heartbeat_codec::SBE_BLOCK_LENGTH;
    encode_header(out, block_len, MessageKind::Heartbeat)?;
    let write_buf = WriteBuf::new(out);
    let mut encoder = heartbeat_codec::HeartbeatEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder.heartbeat_seq(frame.heartbeat_seq);
    Ok(SBE_HEADER_LEN + block_len as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            EngineSeq, Price, ProtocolErrorEvent, RecvTs, RejectReason,
            control::{
                ControlEvent, ControlRequest, ControlResponse, ControlResult, HeartbeatSeq, HeartbeatStatus, RequestId, SnapshotBegin,
                SnapshotEnd, SnapshotOrder,
            },
            rd::InstrumentSpec,
            trading::order::{AccountId, OrderId, Side},
        },
        protocol::{
            SBE_HEADER_LEN,
            generated::codecs::{
                control_result, error_code, heartbeat_codec, kill_switch_action, kill_switch_codec, reject_reason, snapshot_codec,
            },
            wire::control::{decode_control_request, encode_control_event},
        },
    };

    fn instrument() -> InstrumentSpec {
        InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument")
    }

    fn price(raw: u64) -> Price {
        Price::new(raw, &instrument()).expect("price")
    }

    #[test]
    fn client_frames_decode_as_server_control_requests() {
        let mut bytes = [0_u8; SBE_HEADER_LEN + kill_switch_codec::SBE_BLOCK_LENGTH as usize];
        let written = encode_control_frame(
            ClientControlFrame::KillSwitch(ClientKillSwitch {
                request_id: 7,
                action: kill_switch_action::KillSwitchAction::HaltTrading,
            }),
            &mut bytes,
        )
        .expect("encode kill switch");
        let request = decode_control_request(&bytes[..written], RecvTs::new(55)).expect("decode kill switch");
        match request {
            ControlRequest::KillSwitch(request) => {
                assert_eq!(request.request_id.raw(), 7);
                assert_eq!(request.recv_ts.raw(), 55);
                assert_eq!(request.action, crate::domain::control::KillSwitchAction::HaltTrading);
            }
            other => panic!("unexpected request: {other:?}"),
        }

        let mut snapshot = [0_u8; SBE_HEADER_LEN + snapshot_codec::SBE_BLOCK_LENGTH as usize];
        let written =
            encode_control_frame(ClientControlFrame::Snapshot(ClientSnapshot { request_id: 8 }), &mut snapshot).expect("encode snapshot");
        let request = decode_control_request(&snapshot[..written], RecvTs::new(56)).expect("decode snapshot");
        match request {
            ControlRequest::Snapshot(request) => {
                assert_eq!(request.request_id.raw(), 8);
                assert_eq!(request.recv_ts.raw(), 56);
            }
            other => panic!("unexpected request: {other:?}"),
        }

        let mut heartbeat = [0_u8; SBE_HEADER_LEN + heartbeat_codec::SBE_BLOCK_LENGTH as usize];
        let written = encode_control_frame(ClientControlFrame::Heartbeat(ClientHeartbeat { heartbeat_seq: 9 }), &mut heartbeat)
            .expect("encode heartbeat");
        let request = decode_control_request(&heartbeat[..written], RecvTs::new(57)).expect("decode heartbeat");
        match request {
            ControlRequest::Heartbeat(request) => {
                assert_eq!(request.heartbeat_seq.raw(), 9);
                assert_eq!(request.recv_ts.raw(), 57);
            }
            other => panic!("unexpected request: {other:?}"),
        }
    }

    #[test]
    fn server_control_events_decode_for_client_display() {
        let mut bytes = [0_u8; 128];
        let written = encode_control_event(
            ControlEvent::ControlResponse(ControlResponse {
                engine_seq: EngineSeq::new(10),
                request_id: RequestId::new(1).expect("request"),
                recv_ts: RecvTs::new(100),
                result: ControlResult::Ok,
                reason: RejectReason::None,
            }),
            &mut bytes,
        )
        .expect("encode response");
        assert_eq!(
            decode_control_event(&bytes[..written]).expect("decode response"),
            ClientControlEvent::ControlResponse(ClientControlResponse {
                engine_seq: 10,
                request_id: 1,
                recv_ts: 100,
                result: control_result::ControlResult::Ok,
                reason: reject_reason::RejectReason::None,
            })
        );

        let written = encode_control_event(
            ControlEvent::HeartbeatStatus(HeartbeatStatus {
                engine_seq: EngineSeq::new(11),
                heartbeat_seq: HeartbeatSeq::new(2).expect("heartbeat"),
                recv_ts: RecvTs::new(101),
                result: ControlResult::Ok,
            }),
            &mut bytes,
        )
        .expect("encode heartbeat status");
        assert_eq!(
            decode_control_event(&bytes[..written]).expect("decode heartbeat"),
            ClientControlEvent::HeartbeatStatus(ClientHeartbeatStatus {
                engine_seq: 11,
                heartbeat_seq: 2,
                recv_ts: 101,
                result: control_result::ControlResult::Ok,
            })
        );
    }

    #[test]
    fn snapshot_events_decode_for_client_display() {
        let request_id = RequestId::new(3).expect("request");
        let mut bytes = [0_u8; 128];

        let written = encode_control_event(
            ControlEvent::SnapshotBegin(SnapshotBegin {
                engine_seq: EngineSeq::new(12),
                request_id,
                recv_ts: RecvTs::new(102),
                bid_price: Some(price(100)),
                bid_qty: 4,
                ask_price: Some(price(101)),
                ask_qty: 5,
                resting_order_count: 6,
                last_md_seq: 7,
                next_trade_id: 8,
                risk_reference_price: price(100),
            }),
            &mut bytes,
        )
        .expect("encode snapshot begin");
        assert_eq!(
            decode_control_event(&bytes[..written]).expect("decode begin"),
            ClientControlEvent::SnapshotBegin(ClientSnapshotBegin {
                engine_seq: 12,
                request_id: 3,
                recv_ts: 102,
                bid_price: 100,
                bid_qty: 4,
                ask_price: 101,
                ask_qty: 5,
                resting_order_count: 6,
                last_md_seq: 7,
                next_trade_id: 8,
                risk_reference_price: 100,
            })
        );

        let written = encode_control_event(
            ControlEvent::SnapshotOrder(SnapshotOrder {
                engine_seq: EngineSeq::new(13),
                request_id,
                row_index: 0,
                side: Side::Sell,
                client_order_id: OrderId::new(9).expect("order"),
                account_id: AccountId::new(10).expect("account"),
                price: price(101),
                remaining_qty: 5,
            }),
            &mut bytes,
        )
        .expect("encode snapshot order");
        assert_eq!(
            decode_control_event(&bytes[..written]).expect("decode row"),
            ClientControlEvent::SnapshotOrder(ClientSnapshotOrder {
                engine_seq: 13,
                request_id: 3,
                row_index: 0,
                side: crate::protocol::generated::codecs::side::Side::Sell,
                client_order_id: 9,
                account_id: 10,
                price: 101,
                remaining_qty: 5,
            })
        );

        let written = encode_control_event(
            ControlEvent::SnapshotEnd(SnapshotEnd {
                engine_seq: EngineSeq::new(14),
                request_id,
                recv_ts: RecvTs::new(103),
                row_count: 1,
                result: ControlResult::Rejected,
                reason: RejectReason::KillSwitchActive,
            }),
            &mut bytes,
        )
        .expect("encode snapshot end");
        assert_eq!(
            decode_control_event(&bytes[..written]).expect("decode end"),
            ClientControlEvent::SnapshotEnd(ClientSnapshotEnd {
                engine_seq: 14,
                request_id: 3,
                recv_ts: 103,
                row_count: 1,
                result: control_result::ControlResult::Rejected,
                reason: reject_reason::RejectReason::KillSwitchActive,
            })
        );
    }

    #[test]
    fn protocol_error_events_decode_for_client_display() {
        let mut bytes = [0_u8; 128];
        let written = encode_control_event(
            ControlEvent::ProtocolError(ProtocolErrorEvent {
                engine_seq: EngineSeq::new(15),
                template_id: 999,
                recv_ts: RecvTs::new(104),
                error_code: crate::domain::ErrorCode::UnsupportedTemplate,
            }),
            &mut bytes,
        )
        .expect("encode protocol error");

        assert_eq!(
            decode_control_event(&bytes[..written]).expect("decode protocol error"),
            ClientControlEvent::ProtocolError(ClientProtocolError {
                engine_seq: 15,
                template_id: 999,
                recv_ts: 104,
                error_code: error_code::ErrorCode::UnsupportedTemplate,
            })
        );
    }
}
