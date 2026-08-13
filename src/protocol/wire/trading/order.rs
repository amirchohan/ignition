use crate::{
    domain::{
        DomainResult, Price, Qty, RecvTs,
        rd::InstrumentSpec,
        trading::order::{
            AccountId, CancelOrder, CancelReplace, ClientTs, ExecutionReport, MassCancel, NewOrder, OrderEntry, OrderId, OrderType,
        },
    },
    protocol::{
        MessageKind, ProtocolResult, SBE_HEADER_LEN, decode_header, encode_header,
        generated::codecs::{
            ReadBuf, WriteBuf, cancel_order_codec, cancel_replace_codec, execution_report_codec, mass_cancel_codec, new_order_codec,
            order_type, side, time_in_force,
        },
    },
};

use super::super::{
    InboundDecodeError, ensure_complete, ensure_out, normalize_order_type, normalize_side, normalize_tif, to_wire_exec_type,
    to_wire_ord_status, to_wire_reject_reason, unexpected_template,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireOrderEntry {
    NewOrder(WireNewOrder),
    CancelOrder(WireCancelOrder),
    CancelReplace(WireCancelReplace),
    MassCancel(WireMassCancel),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireNewOrder {
    pub client_order_id: u64,
    pub account_id: u32,
    pub client_ts: u64,
    pub side: side::Side,
    pub order_type: order_type::OrderType,
    pub time_in_force: time_in_force::TimeInForce,
    pub price: u64,
    pub qty: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireCancelOrder {
    pub client_order_id: u64,
    pub account_id: u32,
    pub orig_client_order_id: u64,
    pub client_ts: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireCancelReplace {
    pub client_order_id: u64,
    pub account_id: u32,
    pub orig_client_order_id: u64,
    pub client_ts: u64,
    pub price: u64,
    pub qty: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WireMassCancel {
    pub client_order_id: u64,
    pub account_id: u32,
    pub client_ts: u64,
}

pub fn decode_order_entry(input: &[u8]) -> ProtocolResult<WireOrderEntry> {
    let frame = decode_header(input)?;
    ensure_complete(input, frame)?;

    if !frame.kind.is_order_entry() {
        return Err(unexpected_template(frame.kind));
    }

    let read_buf = ReadBuf::new(input);
    match frame.kind {
        MessageKind::NewOrder => {
            let decoder = new_order_codec::NewOrderDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(WireOrderEntry::NewOrder(WireNewOrder {
                client_order_id: decoder.client_order_id(),
                account_id: decoder.account_id(),
                client_ts: decoder.client_ts(),
                side: decoder.side(),
                order_type: decoder.order_type(),
                time_in_force: decoder.time_in_force(),
                price: decoder.price(),
                qty: decoder.qty(),
            }))
        }
        MessageKind::CancelOrder => {
            let decoder =
                cancel_order_codec::CancelOrderDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(WireOrderEntry::CancelOrder(WireCancelOrder {
                client_order_id: decoder.client_order_id(),
                account_id: decoder.account_id(),
                orig_client_order_id: decoder.orig_client_order_id(),
                client_ts: decoder.client_ts(),
            }))
        }
        MessageKind::CancelReplace => {
            let decoder =
                cancel_replace_codec::CancelReplaceDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(WireOrderEntry::CancelReplace(WireCancelReplace {
                client_order_id: decoder.client_order_id(),
                account_id: decoder.account_id(),
                orig_client_order_id: decoder.orig_client_order_id(),
                client_ts: decoder.client_ts(),
                price: decoder.price(),
                qty: decoder.qty(),
            }))
        }
        MessageKind::MassCancel => {
            let decoder = mass_cancel_codec::MassCancelDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(WireOrderEntry::MassCancel(WireMassCancel {
                client_order_id: decoder.client_order_id(),
                account_id: decoder.account_id(),
                client_ts: decoder.client_ts(),
            }))
        }
        _ => unreachable!("order entry kind checked above"),
    }
}

pub fn normalize_order_entry(message: WireOrderEntry, recv_ts: RecvTs, instrument: &InstrumentSpec) -> DomainResult<OrderEntry> {
    match message {
        WireOrderEntry::NewOrder(message) => {
            let order_type = normalize_order_type(message.order_type)?;
            let price = match order_type {
                OrderType::Limit => Some(Price::new(message.price, instrument)?),
                OrderType::Market => None,
            };

            Ok(OrderEntry::NewOrder(NewOrder {
                client_order_id: OrderId::new(message.client_order_id)?,
                account_id: AccountId::new(message.account_id)?,
                client_ts: ClientTs::new(message.client_ts),
                recv_ts,
                side: normalize_side(message.side)?,
                order_type,
                time_in_force: normalize_tif(message.time_in_force)?,
                price,
                qty: Qty::new(message.qty, instrument)?,
            }))
        }
        WireOrderEntry::CancelOrder(message) => Ok(OrderEntry::CancelOrder(CancelOrder {
            client_order_id: OrderId::new(message.client_order_id)?,
            account_id: AccountId::new(message.account_id)?,
            orig_client_order_id: OrderId::new(message.orig_client_order_id)?,
            client_ts: ClientTs::new(message.client_ts),
            recv_ts,
        })),
        WireOrderEntry::CancelReplace(message) => Ok(OrderEntry::CancelReplace(CancelReplace {
            client_order_id: OrderId::new(message.client_order_id)?,
            account_id: AccountId::new(message.account_id)?,
            orig_client_order_id: OrderId::new(message.orig_client_order_id)?,
            client_ts: ClientTs::new(message.client_ts),
            recv_ts,
            price: Price::new(message.price, instrument)?,
            qty: Qty::new(message.qty, instrument)?,
        })),
        WireOrderEntry::MassCancel(message) => Ok(OrderEntry::MassCancel(MassCancel {
            client_order_id: OrderId::new(message.client_order_id)?,
            account_id: AccountId::new(message.account_id)?,
            client_ts: ClientTs::new(message.client_ts),
            recv_ts,
        })),
    }
}

pub fn decode_and_normalize_order_entry(
    input: &[u8],
    recv_ts: RecvTs,
    instrument: &InstrumentSpec,
) -> Result<OrderEntry, InboundDecodeError> {
    let message = decode_order_entry(input).map_err(InboundDecodeError::Protocol)?;
    normalize_order_entry(message, recv_ts, instrument).map_err(InboundDecodeError::Domain)
}

pub fn encode_execution_report(event: ExecutionReport, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = execution_report_codec::SBE_BLOCK_LENGTH;
    ensure_out(out, block_len)?;
    encode_header(out, block_len, MessageKind::ExecutionReport)?;

    let write_buf = WriteBuf::new(out);
    let mut encoder = execution_report_codec::ExecutionReportEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .engine_seq(event.engine_seq.raw())
        .client_order_id(event.client_order_id.raw())
        .account_id(event.account_id.raw())
        .exec_type(to_wire_exec_type(event.exec_type))
        .ord_status(to_wire_ord_status(event.ord_status))
        .reject_reason(to_wire_reject_reason(event.reject_reason))
        .fill_price(event.fill_price.map_or(0, Price::raw))
        .fill_qty(event.fill_qty)
        .remaining_qty(event.remaining_qty);
    Ok(SBE_HEADER_LEN + execution_report_codec::SBE_BLOCK_LENGTH as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            DomainError, EngineSeq, RejectReason,
            rd::InstrumentSpec,
            trading::order::{ExecType, OrdStatus, Side, TimeInForce},
        },
        protocol::{
            ProtocolError,
            generated::codecs::{kill_switch_action as wire_kill_switch_action, kill_switch_codec, message_header_codec},
        },
    };

    fn instrument() -> InstrumentSpec {
        InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("valid instrument")
    }

    fn encode_new_order(order_type: order_type::OrderType, price: u64, qty: u64) -> Vec<u8> {
        let len = SBE_HEADER_LEN + new_order_codec::SBE_BLOCK_LENGTH as usize;
        let mut bytes = vec![0_u8; len];
        encode_header(&mut bytes, new_order_codec::SBE_BLOCK_LENGTH, MessageKind::NewOrder).expect("header");

        let write_buf = WriteBuf::new(&mut bytes);
        let mut encoder = new_order_codec::NewOrderEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
        encoder
            .client_order_id(11)
            .account_id(22)
            .client_ts(33)
            .side(side::Side::Buy)
            .order_type(order_type)
            .time_in_force(time_in_force::TimeInForce::Gtc)
            .price(price)
            .qty(qty);
        bytes
    }

    fn encode_kill_switch() -> Vec<u8> {
        let len = SBE_HEADER_LEN + kill_switch_codec::SBE_BLOCK_LENGTH as usize;
        let mut bytes = vec![0_u8; len];
        encode_header(&mut bytes, kill_switch_codec::SBE_BLOCK_LENGTH, MessageKind::KillSwitch).expect("header");

        let write_buf = WriteBuf::new(&mut bytes);
        let mut encoder = kill_switch_codec::KillSwitchEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
        encoder
            .request_id(77)
            .action(wire_kill_switch_action::KillSwitchAction::HaltTrading);
        bytes
    }

    #[test]
    fn order_entry_normalizes_limit_order() {
        let bytes = encode_new_order(order_type::OrderType::Limit, 6_725_123, 125_000_000);
        let request = decode_and_normalize_order_entry(&bytes, RecvTs::new(900), &instrument()).expect("order entry");

        let OrderEntry::NewOrder(order) = request else {
            panic!("expected new order");
        };

        assert_eq!(order.client_order_id.raw(), 11);
        assert_eq!(order.account_id.raw(), 22);
        assert_eq!(order.client_ts.raw(), 33);
        assert_eq!(order.recv_ts.raw(), 900);
        assert_eq!(order.side, Side::Buy);
        assert_eq!(order.order_type, OrderType::Limit);
        assert_eq!(order.time_in_force, TimeInForce::Gtc);
        assert_eq!(order.price.expect("limit price").raw(), 6_725_123);
        assert_eq!(order.qty.raw(), 125_000_000);
    }

    #[test]
    fn order_entry_ignores_market_order_price() {
        let bytes = encode_new_order(order_type::OrderType::Market, 99, 100);
        let request = decode_and_normalize_order_entry(&bytes, RecvTs::new(1), &instrument()).expect("market order");

        let OrderEntry::NewOrder(order) = request else {
            panic!("expected new order");
        };

        assert_eq!(order.order_type, OrderType::Market);
        assert_eq!(order.price, None);
        assert_eq!(order.qty.raw(), 100);
    }

    #[test]
    fn order_entry_rejects_wrong_endpoint_template() {
        assert_eq!(
            decode_order_entry(&encode_kill_switch()),
            Err(ProtocolError::UnexpectedTemplate { template_id: 1 })
        );
    }

    #[test]
    fn order_entry_rejects_invalid_wire_and_scalars() {
        let null_type = encode_new_order(order_type::OrderType::NullVal, 100, 100);
        assert_eq!(
            decode_and_normalize_order_entry(&null_type, RecvTs::new(1), &instrument()),
            Err(InboundDecodeError::Domain(DomainError::Malformed))
        );

        let invalid_limit_price = encode_new_order(order_type::OrderType::Limit, 0, 100);
        assert_eq!(
            decode_and_normalize_order_entry(&invalid_limit_price, RecvTs::new(1), &instrument()),
            Err(InboundDecodeError::Domain(DomainError::InvalidPrice))
        );

        let invalid_qty = encode_new_order(order_type::OrderType::Market, 0, 0);
        assert_eq!(
            decode_and_normalize_order_entry(&invalid_qty, RecvTs::new(1), &instrument()),
            Err(InboundDecodeError::Domain(DomainError::InvalidQuantity))
        );
    }

    #[test]
    fn execution_report_encodes_zero_fill_sentinel() {
        let event = ExecutionReport {
            engine_seq: EngineSeq::new(7),
            client_order_id: OrderId::new(11).expect("order id"),
            account_id: AccountId::new(22).expect("account id"),
            exec_type: ExecType::Rejected,
            ord_status: OrdStatus::Rejected,
            reject_reason: RejectReason::InvalidPrice,
            fill_price: None,
            fill_qty: 0,
            remaining_qty: 0,
        };

        let mut bytes = [0_u8; SBE_HEADER_LEN + execution_report_codec::SBE_BLOCK_LENGTH as usize];
        let written = encode_execution_report(event, &mut bytes).expect("encode");
        assert_eq!(written, bytes.len());
        assert_eq!(decode_header(&bytes).expect("header").kind, MessageKind::ExecutionReport);

        let decoder = message_header_codec::MessageHeaderDecoder::default().wrap(ReadBuf::new(&bytes), 0);
        assert_eq!(decoder.template_id(), MessageKind::ExecutionReport.template_id());
    }
}
