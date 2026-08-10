//! Pure SBE wire adapters for internal domain messages.
//!
//! This module is the only handwritten layer that imports generated SBE codecs.
//! Core modules consume validated domain types instead of generated protocol
//! enums or byte buffers.

pub mod control;
pub mod trading;

use crate::{
    domain::{
        DomainError, DomainResult, ErrorCode, ProtocolErrorEvent, RejectReason,
        control::ControlResult,
        trading::order::{ExecType, OrdStatus, OrderType, Side, TimeInForce},
    },
    protocol::{
        MessageKind, ProtocolError, ProtocolFrame, ProtocolResult, SBE_HEADER_LEN, encode_header,
        generated::codecs::{
            WriteBuf, control_result, error_code, exec_type, ord_status, protocol_error_codec,
            reject_reason, side, time_in_force,
        },
    },
};

/// Error returned by combined decode-and-normalize adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InboundDecodeError {
    Protocol(ProtocolError),
    Domain(DomainError),
}

pub(crate) fn ensure_complete(input: &[u8], frame: ProtocolFrame) -> ProtocolResult<()> {
    let required_len = SBE_HEADER_LEN + usize::from(frame.block_length);
    if input.len() < required_len {
        return Err(ProtocolError::BufferTooShort);
    }

    Ok(())
}

pub(crate) fn ensure_out(out: &[u8], payload_len: u16) -> ProtocolResult<()> {
    if out.len() < SBE_HEADER_LEN + usize::from(payload_len) {
        return Err(ProtocolError::BufferTooShort);
    }

    Ok(())
}

pub(crate) fn unexpected_template(kind: MessageKind) -> ProtocolError {
    ProtocolError::UnexpectedTemplate {
        template_id: kind.template_id(),
    }
}

pub(crate) fn encode_protocol_error(
    event: ProtocolErrorEvent,
    out: &mut [u8],
) -> ProtocolResult<usize> {
    let block_len = protocol_error_codec::SBE_BLOCK_LENGTH;
    ensure_out(out, block_len)?;
    encode_header(out, block_len, MessageKind::ProtocolError)?;

    let write_buf = WriteBuf::new(out);
    let mut encoder =
        protocol_error_codec::ProtocolErrorEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .engine_seq(event.engine_seq.raw())
        .template_id(event.template_id)
        .recv_ts(event.recv_ts.raw())
        .error_code(to_wire_error_code(event.error_code));
    Ok(SBE_HEADER_LEN + protocol_error_codec::SBE_BLOCK_LENGTH as usize)
}

pub(crate) fn normalize_side(value: side::Side) -> DomainResult<Side> {
    match value {
        side::Side::Buy => Ok(Side::Buy),
        side::Side::Sell => Ok(Side::Sell),
        side::Side::NullVal => Err(DomainError::Malformed),
    }
}

pub(crate) fn normalize_order_type(
    value: crate::protocol::generated::codecs::order_type::OrderType,
) -> DomainResult<OrderType> {
    match value {
        crate::protocol::generated::codecs::order_type::OrderType::Limit => Ok(OrderType::Limit),
        crate::protocol::generated::codecs::order_type::OrderType::Market => Ok(OrderType::Market),
        crate::protocol::generated::codecs::order_type::OrderType::NullVal => {
            Err(DomainError::Malformed)
        }
    }
}

pub(crate) fn normalize_tif(value: time_in_force::TimeInForce) -> DomainResult<TimeInForce> {
    match value {
        time_in_force::TimeInForce::Gtc => Ok(TimeInForce::Gtc),
        time_in_force::TimeInForce::Ioc => Ok(TimeInForce::Ioc),
        time_in_force::TimeInForce::PostOnly => Ok(TimeInForce::PostOnly),
        time_in_force::TimeInForce::NullVal => Err(DomainError::Malformed),
    }
}

pub(crate) fn to_wire_exec_type(value: ExecType) -> exec_type::ExecType {
    match value {
        ExecType::New => exec_type::ExecType::New,
        ExecType::Trade => exec_type::ExecType::Trade,
        ExecType::Canceled => exec_type::ExecType::Canceled,
        ExecType::Replaced => exec_type::ExecType::Replaced,
        ExecType::Rejected => exec_type::ExecType::Rejected,
        ExecType::Expired => exec_type::ExecType::Expired,
    }
}

pub(crate) fn to_wire_ord_status(value: OrdStatus) -> ord_status::OrdStatus {
    match value {
        OrdStatus::New => ord_status::OrdStatus::New,
        OrdStatus::PartiallyFilled => ord_status::OrdStatus::PartiallyFilled,
        OrdStatus::Filled => ord_status::OrdStatus::Filled,
        OrdStatus::Canceled => ord_status::OrdStatus::Canceled,
        OrdStatus::Replaced => ord_status::OrdStatus::Replaced,
        OrdStatus::Rejected => ord_status::OrdStatus::Rejected,
        OrdStatus::Expired => ord_status::OrdStatus::Expired,
    }
}

pub(crate) fn to_wire_reject_reason(value: RejectReason) -> reject_reason::RejectReason {
    match value {
        RejectReason::None => reject_reason::RejectReason::None,
        RejectReason::Malformed => reject_reason::RejectReason::Malformed,
        RejectReason::UnknownOrder => reject_reason::RejectReason::UnknownOrder,
        RejectReason::InvalidPrice => reject_reason::RejectReason::InvalidPrice,
        RejectReason::InvalidQuantity => reject_reason::RejectReason::InvalidQuantity,
        RejectReason::RiskOpenOrderLimit => reject_reason::RejectReason::RiskOpenOrderLimit,
        RejectReason::RiskNotionalLimit => reject_reason::RejectReason::RiskNotionalLimit,
        RejectReason::PriceBand => reject_reason::RejectReason::PriceBand,
        RejectReason::SelfTradePrevention => reject_reason::RejectReason::SelfTradePrevention,
        RejectReason::KillSwitchActive => reject_reason::RejectReason::KillSwitchActive,
        RejectReason::HeartbeatLost => reject_reason::RejectReason::HeartbeatLost,
    }
}

pub(crate) fn to_wire_control_result(value: ControlResult) -> control_result::ControlResult {
    match value {
        ControlResult::Ok => control_result::ControlResult::Ok,
        ControlResult::Rejected => control_result::ControlResult::Rejected,
    }
}

pub(crate) fn to_wire_error_code(value: ErrorCode) -> error_code::ErrorCode {
    match value {
        ErrorCode::DecodeError => error_code::ErrorCode::DecodeError,
        ErrorCode::UnsupportedTemplate => error_code::ErrorCode::UnsupportedTemplate,
        ErrorCode::UnsupportedVersion => error_code::ErrorCode::UnsupportedVersion,
        ErrorCode::InternalError => error_code::ErrorCode::InternalError,
    }
}
