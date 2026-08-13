//! Thin protocol frame/header facade around generated SBE codecs.
//!
//! The facade stays buffer-oriented and side-effect-free. Runtime, gateway,
//! replay, and client tooling should depend on this module rather than on the
//! generated codec layout.

use crate::protocol::error::{ProtocolError, ProtocolResult};

/// SBE schema ID assigned to the Ignition protocol.
pub const SCHEMA_ID: u16 = 7001;

/// Current Ignition protocol schema version.
pub const SCHEMA_VERSION: u16 = 12;

/// Length of the standard SBE message header.
pub const SBE_HEADER_LEN: usize = 8;

/// Known protocol message families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    /// Order-entry NewOrder request.
    NewOrder,
    /// Order-entry CancelOrder request.
    CancelOrder,
    /// Order-entry CancelReplace request.
    CancelReplace,
    /// Per-account mass-cancel request.
    MassCancel,
    /// Kill-switch control request.
    KillSwitch,
    /// Snapshot control request.
    Snapshot,
    /// Control-plane heartbeat request.
    Heartbeat,
    /// Outbound execution report.
    ExecutionReport,
    /// Outbound trade print.
    TradePrint,
    /// Outbound top-of-book update.
    L1Update,
    /// Outbound absolute L2 price-level update.
    L2Update,
    /// Outbound control response.
    ControlResponse,
    /// Outbound heartbeat status.
    HeartbeatStatus,
    /// Outbound snapshot begin marker and metadata.
    SnapshotBegin,
    /// Outbound resting-order snapshot row.
    SnapshotOrder,
    /// Outbound snapshot end marker.
    SnapshotEnd,
    /// Outbound protocol error.
    ProtocolError,
}

impl MessageKind {
    /// Return the stable SBE template ID for this message kind.
    pub const fn template_id(self) -> u16 {
        match self {
            Self::KillSwitch => 1,
            Self::Snapshot => 2,
            Self::Heartbeat => 3,
            Self::ControlResponse => 50,
            Self::HeartbeatStatus => 51,
            Self::SnapshotBegin => 52,
            Self::SnapshotOrder => 53,
            Self::SnapshotEnd => 54,
            Self::ProtocolError => 99,
            Self::NewOrder => 100,
            Self::CancelOrder => 101,
            Self::CancelReplace => 102,
            Self::MassCancel => 103,
            Self::ExecutionReport => 150,
            Self::TradePrint => 200,
            Self::L1Update => 201,
            Self::L2Update => 202,
        }
    }

    /// Decode a stable SBE template ID.
    pub const fn from_template_id(template_id: u16) -> ProtocolResult<Self> {
        match template_id {
            1 => Ok(Self::KillSwitch),
            2 => Ok(Self::Snapshot),
            3 => Ok(Self::Heartbeat),
            50 => Ok(Self::ControlResponse),
            51 => Ok(Self::HeartbeatStatus),
            52 => Ok(Self::SnapshotBegin),
            53 => Ok(Self::SnapshotOrder),
            54 => Ok(Self::SnapshotEnd),
            99 => Ok(Self::ProtocolError),
            100 => Ok(Self::NewOrder),
            101 => Ok(Self::CancelOrder),
            102 => Ok(Self::CancelReplace),
            103 => Ok(Self::MassCancel),
            150 => Ok(Self::ExecutionReport),
            200 => Ok(Self::TradePrint),
            201 => Ok(Self::L1Update),
            202 => Ok(Self::L2Update),
            _ => Err(ProtocolError::UnknownTemplate { template_id }),
        }
    }

    /// True when this template is accepted on the control ingress endpoint.
    pub const fn is_control_request(self) -> bool {
        matches!(self, Self::KillSwitch | Self::Snapshot | Self::Heartbeat)
    }

    /// True when this template is accepted on the trading ingress endpoint.
    pub const fn is_order_entry(self) -> bool {
        matches!(self, Self::NewOrder | Self::CancelOrder | Self::CancelReplace | Self::MassCancel)
    }
}

/// Decoded SBE message header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolFrame {
    /// SBE block length for the message payload.
    pub block_length: u16,
    /// Logical message kind identified by the SBE template ID.
    pub kind: MessageKind,
    /// Protocol schema ID.
    pub schema_id: u16,
    /// Protocol schema version.
    pub version: u16,
}

/// Encode the standard SBE message header into `out`.
pub fn encode_header(out: &mut [u8], block_length: u16, kind: MessageKind) -> ProtocolResult<()> {
    if out.len() < SBE_HEADER_LEN {
        return Err(ProtocolError::BufferTooShort);
    }

    out[0..2].copy_from_slice(&block_length.to_le_bytes());
    out[2..4].copy_from_slice(&kind.template_id().to_le_bytes());
    out[4..6].copy_from_slice(&SCHEMA_ID.to_le_bytes());
    out[6..8].copy_from_slice(&SCHEMA_VERSION.to_le_bytes());
    Ok(())
}

/// Decode and validate the standard SBE message header from `input`.
pub fn decode_header(input: &[u8]) -> ProtocolResult<ProtocolFrame> {
    if input.len() < SBE_HEADER_LEN {
        return Err(ProtocolError::BufferTooShort);
    }

    let block_length = u16::from_le_bytes([input[0], input[1]]);
    let template_id = u16::from_le_bytes([input[2], input[3]]);
    let schema_id = u16::from_le_bytes([input[4], input[5]]);
    let version = u16::from_le_bytes([input[6], input[7]]);

    if schema_id != SCHEMA_ID {
        return Err(ProtocolError::UnsupportedSchema { schema_id });
    }

    if version != SCHEMA_VERSION {
        return Err(ProtocolError::UnsupportedVersion { version });
    }

    Ok(ProtocolFrame {
        block_length,
        kind: MessageKind::from_template_id(template_id)?,
        schema_id,
        version,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::generated::codecs::{
        ReadBuf, WriteBuf, cancel_order_codec, cancel_replace_codec, control_response_codec, execution_report_codec, heartbeat_codec,
        heartbeat_status_codec, kill_switch_codec, l1_update_codec, l2_update_codec, mass_cancel_codec, message_header_codec,
        new_order_codec, order_type, protocol_error_codec, side, snapshot_begin_codec, snapshot_codec, snapshot_end_codec,
        snapshot_order_codec, time_in_force, trade_print_codec,
    };

    #[test]
    fn header_round_trips_representative_message_kinds() {
        let cases = [
            MessageKind::NewOrder,
            MessageKind::ExecutionReport,
            MessageKind::L1Update,
            MessageKind::L2Update,
            MessageKind::Heartbeat,
        ];

        for kind in cases {
            let mut out = [0_u8; SBE_HEADER_LEN];
            encode_header(&mut out, 42, kind).expect("header should encode");
            let decoded = decode_header(&out).expect("header should decode");

            assert_eq!(decoded.block_length, 42);
            assert_eq!(decoded.kind, kind);
            assert_eq!(decoded.schema_id, SCHEMA_ID);
            assert_eq!(decoded.version, SCHEMA_VERSION);
        }
    }

    #[test]
    fn decode_rejects_unknown_template() {
        let mut input = [0_u8; SBE_HEADER_LEN];
        input[2..4].copy_from_slice(&999_u16.to_le_bytes());
        input[4..6].copy_from_slice(&SCHEMA_ID.to_le_bytes());
        input[6..8].copy_from_slice(&SCHEMA_VERSION.to_le_bytes());

        assert_eq!(decode_header(&input), Err(ProtocolError::UnknownTemplate { template_id: 999 }));
    }

    #[test]
    fn facade_template_ids_match_generated_sbe_codecs() {
        let cases = [
            (MessageKind::KillSwitch, kill_switch_codec::SBE_TEMPLATE_ID),
            (MessageKind::Snapshot, snapshot_codec::SBE_TEMPLATE_ID),
            (MessageKind::Heartbeat, heartbeat_codec::SBE_TEMPLATE_ID),
            (MessageKind::ControlResponse, control_response_codec::SBE_TEMPLATE_ID),
            (MessageKind::HeartbeatStatus, heartbeat_status_codec::SBE_TEMPLATE_ID),
            (MessageKind::SnapshotBegin, snapshot_begin_codec::SBE_TEMPLATE_ID),
            (MessageKind::SnapshotOrder, snapshot_order_codec::SBE_TEMPLATE_ID),
            (MessageKind::SnapshotEnd, snapshot_end_codec::SBE_TEMPLATE_ID),
            (MessageKind::ProtocolError, protocol_error_codec::SBE_TEMPLATE_ID),
            (MessageKind::NewOrder, new_order_codec::SBE_TEMPLATE_ID),
            (MessageKind::CancelOrder, cancel_order_codec::SBE_TEMPLATE_ID),
            (MessageKind::CancelReplace, cancel_replace_codec::SBE_TEMPLATE_ID),
            (MessageKind::MassCancel, mass_cancel_codec::SBE_TEMPLATE_ID),
            (MessageKind::ExecutionReport, execution_report_codec::SBE_TEMPLATE_ID),
            (MessageKind::TradePrint, trade_print_codec::SBE_TEMPLATE_ID),
            (MessageKind::L1Update, l1_update_codec::SBE_TEMPLATE_ID),
            (MessageKind::L2Update, l2_update_codec::SBE_TEMPLATE_ID),
        ];

        for (kind, template_id) in cases {
            assert_eq!(kind.template_id(), template_id);
            assert_eq!(MessageKind::from_template_id(template_id).expect("template ID should decode"), kind);
        }
    }

    #[test]
    fn generated_new_order_codec_round_trips_payload() {
        let mut bytes = [0_u8; SBE_HEADER_LEN + new_order_codec::SBE_BLOCK_LENGTH as usize];
        let write_buf = WriteBuf::new(&mut bytes);
        let encoder = new_order_codec::NewOrderEncoder::default().wrap(write_buf, SBE_HEADER_LEN);

        let mut header = encoder.header(0);
        let mut encoder = header.parent().expect("generated header parent");
        encoder
            .client_order_id(11)
            .account_id(22)
            .client_ts(33)
            .side(side::Side::Buy)
            .order_type(order_type::OrderType::Limit)
            .time_in_force(time_in_force::TimeInForce::Gtc)
            .price(44)
            .qty(55);

        let decoded_header = decode_header(&bytes[..SBE_HEADER_LEN]).expect("facade header");
        assert_eq!(decoded_header.kind, MessageKind::NewOrder);
        assert_eq!(decoded_header.block_length, new_order_codec::SBE_BLOCK_LENGTH);

        let read_buf = ReadBuf::new(&bytes);
        let header = message_header_codec::MessageHeaderDecoder::default().wrap(read_buf, 0);
        let decoder = new_order_codec::NewOrderDecoder::default().header(header, 0);

        assert_eq!(decoder.client_order_id(), 11);
        assert_eq!(decoder.account_id(), 22);
        assert_eq!(decoder.client_ts(), 33);
        assert_eq!(decoder.side(), side::Side::Buy);
        assert_eq!(decoder.order_type(), order_type::OrderType::Limit);
        assert_eq!(decoder.time_in_force(), time_in_force::TimeInForce::Gtc);
        assert_eq!(decoder.price(), 44);
        assert_eq!(decoder.qty(), 55);
    }
}
