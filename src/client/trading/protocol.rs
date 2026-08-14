use crate::{
    domain::trading::order::{OrderType, Side, TimeInForce},
    protocol::{
        MessageKind, ProtocolResult, SBE_HEADER_LEN, decode_header, encode_header,
        generated::codecs::{
            ReadBuf, WriteBuf, book_side, cancel_order_codec, cancel_replace_codec, error_code, exec_type, execution_report_codec,
            l1_update_codec, l2_update_codec, mass_cancel_codec, new_order_codec, ord_status, order_type, protocol_error_codec,
            reject_reason, side, time_in_force, trade_print_codec,
        },
    },
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientNewOrder {
    pub client_order_id: u64,
    pub account_id: u32,
    pub client_ts: u64,
    pub side: Side,
    pub order_type: OrderType,
    pub time_in_force: TimeInForce,
    pub price: u64,
    pub qty: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientCancelOrder {
    pub client_order_id: u64,
    pub account_id: u32,
    pub orig_client_order_id: u64,
    pub client_ts: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientCancelReplace {
    pub client_order_id: u64,
    pub account_id: u32,
    pub orig_client_order_id: u64,
    pub client_ts: u64,
    pub price: u64,
    pub qty: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientMassCancel {
    pub client_order_id: u64,
    pub account_id: u32,
    pub client_ts: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientOrderFrame {
    New(ClientNewOrder),
    Cancel(ClientCancelOrder),
    Replace(ClientCancelReplace),
    MassCancel(ClientMassCancel),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientInboundEvent {
    ExecutionReport(ClientExecutionReport),
    TradePrint(ClientTradePrint),
    L1Update(ClientL1Update),
    L2Update(ClientL2Update),
    ProtocolError(ClientProtocolError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientExecutionReport {
    pub engine_seq: u64,
    pub client_order_id: u64,
    pub account_id: u32,
    pub exec_type: exec_type::ExecType,
    pub ord_status: ord_status::OrdStatus,
    pub reject_reason: reject_reason::RejectReason,
    pub fill_price: u64,
    pub fill_qty: u64,
    pub remaining_qty: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientTradePrint {
    pub engine_seq: u64,
    pub md_seq: u64,
    pub trade_id: u64,
    pub maker_order_id: u64,
    pub taker_order_id: u64,
    pub price: u64,
    pub qty: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientL1Update {
    pub engine_seq: u64,
    pub md_seq: u64,
    pub prev_md_seq: u64,
    pub bid_price: u64,
    pub bid_qty: u64,
    pub ask_price: u64,
    pub ask_qty: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientL2Update {
    pub engine_seq: u64,
    pub md_seq: u64,
    pub prev_md_seq: u64,
    pub side: book_side::BookSide,
    pub price: u64,
    pub qty: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientProtocolError {
    pub engine_seq: u64,
    pub template_id: u16,
    pub recv_ts: u64,
    pub error_code: error_code::ErrorCode,
}

pub fn encode_order_frame(frame: ClientOrderFrame, out: &mut [u8]) -> ProtocolResult<usize> {
    match frame {
        ClientOrderFrame::New(order) => encode_new_order(order, out),
        ClientOrderFrame::Cancel(cancel) => encode_cancel_order(cancel, out),
        ClientOrderFrame::Replace(replace) => encode_cancel_replace(replace, out),
        ClientOrderFrame::MassCancel(cancel) => encode_mass_cancel(cancel, out),
    }
}

pub fn decode_inbound_event(input: &[u8]) -> ProtocolResult<ClientInboundEvent> {
    let frame = decode_header(input)?;
    if input.len() < SBE_HEADER_LEN + frame.block_length as usize {
        return Err(crate::protocol::ProtocolError::BufferTooShort);
    }
    let read_buf = ReadBuf::new(input);
    match frame.kind {
        MessageKind::ExecutionReport => {
            let decoder =
                execution_report_codec::ExecutionReportDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(ClientInboundEvent::ExecutionReport(ClientExecutionReport {
                engine_seq: decoder.engine_seq(),
                client_order_id: decoder.client_order_id(),
                account_id: decoder.account_id(),
                exec_type: decoder.exec_type(),
                ord_status: decoder.ord_status(),
                reject_reason: decoder.reject_reason(),
                fill_price: decoder.fill_price(),
                fill_qty: decoder.fill_qty(),
                remaining_qty: decoder.remaining_qty(),
            }))
        }
        MessageKind::TradePrint => {
            let decoder = trade_print_codec::TradePrintDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(ClientInboundEvent::TradePrint(ClientTradePrint {
                engine_seq: decoder.engine_seq(),
                md_seq: decoder.md_seq(),
                trade_id: decoder.trade_id(),
                maker_order_id: decoder.maker_order_id(),
                taker_order_id: decoder.taker_order_id(),
                price: decoder.price(),
                qty: decoder.qty(),
            }))
        }
        MessageKind::L1Update => {
            let decoder = l1_update_codec::L1UpdateDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(ClientInboundEvent::L1Update(ClientL1Update {
                engine_seq: decoder.engine_seq(),
                md_seq: decoder.md_seq(),
                prev_md_seq: decoder.prev_md_seq(),
                bid_price: decoder.bid_price(),
                bid_qty: decoder.bid_qty(),
                ask_price: decoder.ask_price(),
                ask_qty: decoder.ask_qty(),
            }))
        }
        MessageKind::L2Update => {
            let decoder = l2_update_codec::L2UpdateDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(ClientInboundEvent::L2Update(ClientL2Update {
                engine_seq: decoder.engine_seq(),
                md_seq: decoder.md_seq(),
                prev_md_seq: decoder.prev_md_seq(),
                side: decoder.side(),
                price: decoder.price(),
                qty: decoder.qty(),
            }))
        }
        MessageKind::ProtocolError => {
            let decoder =
                protocol_error_codec::ProtocolErrorDecoder::default().wrap(read_buf, SBE_HEADER_LEN, frame.block_length, frame.version);
            Ok(ClientInboundEvent::ProtocolError(ClientProtocolError {
                engine_seq: decoder.engine_seq(),
                template_id: decoder.template_id(),
                recv_ts: decoder.recv_ts(),
                error_code: decoder.error_code(),
            }))
        }
        _ => Err(crate::protocol::ProtocolError::UnexpectedTemplate {
            template_id: frame.kind.template_id(),
        }),
    }
}

fn encode_new_order(order: ClientNewOrder, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = new_order_codec::SBE_BLOCK_LENGTH;
    encode_header(out, block_len, MessageKind::NewOrder)?;
    let write_buf = WriteBuf::new(out);
    let mut encoder = new_order_codec::NewOrderEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .client_order_id(order.client_order_id)
        .account_id(order.account_id)
        .client_ts(order.client_ts)
        .side(to_wire_side(order.side))
        .order_type(to_wire_order_type(order.order_type))
        .time_in_force(to_wire_tif(order.time_in_force))
        .price(order.price)
        .qty(order.qty);
    Ok(SBE_HEADER_LEN + block_len as usize)
}

fn encode_cancel_order(cancel: ClientCancelOrder, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = cancel_order_codec::SBE_BLOCK_LENGTH;
    encode_header(out, block_len, MessageKind::CancelOrder)?;
    let write_buf = WriteBuf::new(out);
    let mut encoder = cancel_order_codec::CancelOrderEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .client_order_id(cancel.client_order_id)
        .account_id(cancel.account_id)
        .orig_client_order_id(cancel.orig_client_order_id)
        .client_ts(cancel.client_ts);
    Ok(SBE_HEADER_LEN + block_len as usize)
}

fn encode_cancel_replace(replace: ClientCancelReplace, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = cancel_replace_codec::SBE_BLOCK_LENGTH;
    encode_header(out, block_len, MessageKind::CancelReplace)?;
    let write_buf = WriteBuf::new(out);
    let mut encoder = cancel_replace_codec::CancelReplaceEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .client_order_id(replace.client_order_id)
        .account_id(replace.account_id)
        .orig_client_order_id(replace.orig_client_order_id)
        .client_ts(replace.client_ts)
        .price(replace.price)
        .qty(replace.qty);
    Ok(SBE_HEADER_LEN + block_len as usize)
}

fn encode_mass_cancel(cancel: ClientMassCancel, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = mass_cancel_codec::SBE_BLOCK_LENGTH;
    encode_header(out, block_len, MessageKind::MassCancel)?;
    let write_buf = WriteBuf::new(out);
    let mut encoder = mass_cancel_codec::MassCancelEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .client_order_id(cancel.client_order_id)
        .account_id(cancel.account_id)
        .client_ts(cancel.client_ts);
    Ok(SBE_HEADER_LEN + block_len as usize)
}

fn to_wire_side(value: Side) -> side::Side {
    match value {
        Side::Buy => side::Side::Buy,
        Side::Sell => side::Side::Sell,
    }
}

fn to_wire_order_type(value: OrderType) -> order_type::OrderType {
    match value {
        OrderType::Limit => order_type::OrderType::Limit,
        OrderType::Market => order_type::OrderType::Market,
    }
}

fn to_wire_tif(value: TimeInForce) -> time_in_force::TimeInForce {
    match value {
        TimeInForce::Gtc => time_in_force::TimeInForce::Gtc,
        TimeInForce::Ioc => time_in_force::TimeInForce::Ioc,
        TimeInForce::PostOnly => time_in_force::TimeInForce::PostOnly,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            EngineSeq, Price, Qty, RejectReason,
            rd::InstrumentSpec,
            trading::{
                md::{L1Update, MarketDataEvent, MdSeq, TradeId, TradePrint},
                order::{AccountId, ExecType, ExecutionReport, OrdStatus, OrderId},
            },
        },
        protocol::wire::trading::{
            encode_execution_report, encode_market_data_event,
            order::{WireOrderEntry, decode_order_entry},
        },
    };

    fn instrument() -> InstrumentSpec {
        InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument")
    }

    #[test]
    fn encodes_new_order_for_server_decoder() {
        let mut bytes = [0_u8; 128];
        let written = encode_order_frame(
            ClientOrderFrame::New(ClientNewOrder {
                client_order_id: 11,
                account_id: 22,
                client_ts: 33,
                side: Side::Buy,
                order_type: OrderType::Limit,
                time_in_force: TimeInForce::Gtc,
                price: 44,
                qty: 55,
            }),
            &mut bytes,
        )
        .expect("encode");

        let WireOrderEntry::NewOrder(order) = decode_order_entry(&bytes[..written]).expect("decode") else {
            panic!("expected new order");
        };
        assert_eq!(order.client_order_id, 11);
        assert_eq!(order.account_id, 22);
        assert_eq!(order.side, side::Side::Buy);
        assert_eq!(order.price, 44);
        assert_eq!(order.qty, 55);
    }

    #[test]
    fn decodes_execution_report() {
        let event = ExecutionReport {
            engine_seq: EngineSeq::new(1),
            client_order_id: OrderId::new(2).expect("order"),
            account_id: AccountId::new(3).expect("account"),
            exec_type: ExecType::Rejected,
            ord_status: OrdStatus::Rejected,
            reject_reason: RejectReason::PriceBand,
            fill_price: None,
            fill_qty: 0,
            remaining_qty: 10,
        };
        let mut bytes = [0_u8; 128];
        let written = encode_execution_report(event, &mut bytes).expect("encode");

        let ClientInboundEvent::ExecutionReport(report) = decode_inbound_event(&bytes[..written]).expect("decode") else {
            panic!("expected report");
        };
        assert_eq!(report.engine_seq, 1);
        assert_eq!(report.client_order_id, 2);
        assert_eq!(report.reject_reason, reject_reason::RejectReason::PriceBand);
        assert_eq!(report.remaining_qty, 10);
    }

    #[test]
    fn decodes_market_data_events() {
        let instrument = instrument();
        let mut bytes = [0_u8; 128];
        let written = encode_market_data_event(
            MarketDataEvent::TradePrint(TradePrint {
                engine_seq: EngineSeq::new(6),
                md_seq: MdSeq::new(7),
                trade_id: TradeId::new(8),
                maker_order_id: OrderId::new(9).expect("maker"),
                taker_order_id: OrderId::new(10).expect("taker"),
                price: Price::new(11, &instrument).expect("price"),
                qty: Qty::new(12, &instrument).expect("qty"),
            }),
            &mut bytes,
        )
        .expect("encode");
        let ClientInboundEvent::TradePrint(trade) = decode_inbound_event(&bytes[..written]).expect("decode") else {
            panic!("expected trade");
        };
        assert_eq!(trade.trade_id, 8);
        assert_eq!(trade.engine_seq, 6);
        assert_eq!(trade.price, 11);

        let written = encode_market_data_event(
            MarketDataEvent::L1Update(L1Update {
                engine_seq: EngineSeq::new(14),
                md_seq: MdSeq::new(13),
                prev_md_seq: MdSeq::new(12),
                bid_price: Some(Price::new(10, &instrument).expect("bid")),
                bid_qty: 20,
                ask_price: None,
                ask_qty: 0,
            }),
            &mut bytes,
        )
        .expect("encode");
        let ClientInboundEvent::L1Update(l1) = decode_inbound_event(&bytes[..written]).expect("decode") else {
            panic!("expected l1");
        };
        assert_eq!(l1.bid_price, 10);
        assert_eq!(l1.engine_seq, 14);
        assert_eq!(l1.ask_price, 0);
    }
}
