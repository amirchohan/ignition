//! Thin protocol facade around generated SBE codecs.
//!
//! The facade stays buffer-oriented and side-effect-free. Runtime, gateway,
//! replay, and client tooling should depend on this module rather than on the
//! generated codec layout.

use crate::protocol::error::{ProtocolError, ProtocolResult};

/// SBE schema ID assigned to the Ignition protocol.
pub const SCHEMA_ID: u16 = 7001;

/// Current Ignition protocol schema version.
pub const SCHEMA_VERSION: u16 = 1;

/// Length of the standard SBE message header.
pub const SBE_HEADER_LEN: usize = 8;

/// Known protocol message families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageKind {
    /// Order-entry NewOrder command.
    NewOrder,
    /// Order-entry CancelOrder command.
    CancelOrder,
    /// Order-entry CancelReplace command.
    CancelReplace,
    /// Per-account mass-cancel command.
    MassCancel,
    /// Kill-switch control command.
    KillSwitch,
    /// Snapshot control command.
    Snapshot,
    /// Control-plane heartbeat command.
    Heartbeat,
    /// Outbound execution report.
    ExecutionReport,
    /// Outbound trade print.
    TradePrint,
    /// Outbound book update.
    BookUpdate,
    /// Outbound control response.
    ControlResponse,
    /// Outbound heartbeat status.
    HeartbeatStatus,
    /// Outbound protocol error.
    ProtocolError,
}

impl MessageKind {
    /// Return the stable SBE template ID for this message kind.
    pub const fn template_id(self) -> u16 {
        match self {
            Self::NewOrder => 1,
            Self::CancelOrder => 2,
            Self::CancelReplace => 3,
            Self::MassCancel => 4,
            Self::KillSwitch => 10,
            Self::Snapshot => 11,
            Self::Heartbeat => 12,
            Self::ExecutionReport => 100,
            Self::TradePrint => 101,
            Self::BookUpdate => 102,
            Self::ControlResponse => 110,
            Self::HeartbeatStatus => 111,
            Self::ProtocolError => 112,
        }
    }

    /// Decode a stable SBE template ID.
    pub const fn from_template_id(template_id: u16) -> ProtocolResult<Self> {
        match template_id {
            1 => Ok(Self::NewOrder),
            2 => Ok(Self::CancelOrder),
            3 => Ok(Self::CancelReplace),
            4 => Ok(Self::MassCancel),
            10 => Ok(Self::KillSwitch),
            11 => Ok(Self::Snapshot),
            12 => Ok(Self::Heartbeat),
            100 => Ok(Self::ExecutionReport),
            101 => Ok(Self::TradePrint),
            102 => Ok(Self::BookUpdate),
            110 => Ok(Self::ControlResponse),
            111 => Ok(Self::HeartbeatStatus),
            112 => Ok(Self::ProtocolError),
            _ => Err(ProtocolError::UnknownTemplate { template_id }),
        }
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
        ReadBuf, WriteBuf, execution_report_codec, kill_switch_codec, message_header_codec,
        new_order_codec, order_type, side, time_in_force,
    };

    #[test]
    fn header_round_trips_representative_message_kinds() {
        let cases = [
            MessageKind::NewOrder,
            MessageKind::ExecutionReport,
            MessageKind::BookUpdate,
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

        assert_eq!(
            decode_header(&input),
            Err(ProtocolError::UnknownTemplate { template_id: 999 })
        );
    }

    #[test]
    fn facade_template_ids_match_generated_sbe_codecs() {
        assert_eq!(
            MessageKind::NewOrder.template_id(),
            new_order_codec::SBE_TEMPLATE_ID
        );
        assert_eq!(
            MessageKind::ExecutionReport.template_id(),
            execution_report_codec::SBE_TEMPLATE_ID
        );
        assert_eq!(
            MessageKind::KillSwitch.template_id(),
            kill_switch_codec::SBE_TEMPLATE_ID
        );
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
        assert_eq!(
            decoded_header.block_length,
            new_order_codec::SBE_BLOCK_LENGTH
        );

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
