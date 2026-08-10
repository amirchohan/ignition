pub mod md;
pub mod order;

use crate::{
    domain::{
        ProtocolErrorEvent, RecvTs,
        rd::InstrumentSpec,
        trading::{TradingEvent, md::BookUpdate, order::ExecutionReport},
    },
    protocol::{ProtocolResult, SBE_HEADER_LEN, generated::codecs::protocol_error_codec},
};

use super::{InboundDecodeError, encode_protocol_error};

pub fn decode_and_normalize_trading_entry(
    input: &[u8],
    recv_ts: RecvTs,
    instrument: &InstrumentSpec,
) -> Result<crate::domain::trading::order::OrderEntry, InboundDecodeError> {
    order::decode_and_normalize_order_entry(input, recv_ts, instrument)
}

pub fn encode_trading_event(event: TradingEvent, out: &mut [u8]) -> ProtocolResult<usize> {
    match event {
        TradingEvent::ExecutionReport(event) => order::encode_execution_report(event, out),
        TradingEvent::TradePrint(event) => md::encode_trade_print(event, out),
        TradingEvent::BookUpdate(event) => md::encode_book_update(event, out),
        TradingEvent::ProtocolError(event) => encode_protocol_error(event, out),
    }
}

#[allow(dead_code)]
const _: fn(ExecutionReport, &mut [u8]) -> ProtocolResult<usize> = order::encode_execution_report;
#[allow(dead_code)]
const _: fn(BookUpdate, &mut [u8]) -> ProtocolResult<usize> = md::encode_book_update;
#[allow(dead_code)]
const _: fn(ProtocolErrorEvent, &mut [u8]) -> ProtocolResult<usize> = encode_protocol_error;
#[allow(dead_code)]
const _: usize = SBE_HEADER_LEN + protocol_error_codec::SBE_BLOCK_LENGTH as usize;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            EngineSeq, ErrorCode, Price, ProtocolErrorEvent, Qty, RecvTs, RejectReason,
            rd::InstrumentSpec,
            trading::{
                md::{BookUpdate, MdSeq, TradeId, TradePrint},
                order::{AccountId, ExecType, ExecutionReport, OrdStatus, OrderId},
            },
        },
        protocol::{MessageKind, decode_header},
    };

    fn instrument() -> InstrumentSpec {
        InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument")
    }

    #[test]
    fn trading_event_encodes_all_families() {
        let instrument = instrument();
        let price = Price::new(100, &instrument).expect("price");
        let qty = Qty::new(10, &instrument).expect("qty");

        let events = [
            (
                TradingEvent::ExecutionReport(ExecutionReport {
                    engine_seq: EngineSeq::new(1),
                    client_order_id: OrderId::new(11).expect("order"),
                    account_id: AccountId::new(22).expect("account"),
                    recv_ts: RecvTs::new(100),
                    exec_type: ExecType::New,
                    ord_status: OrdStatus::New,
                    reject_reason: RejectReason::None,
                    fill_price: None,
                    fill_qty: 0,
                    remaining_qty: 10,
                }),
                MessageKind::ExecutionReport,
            ),
            (
                TradingEvent::TradePrint(TradePrint {
                    md_seq: MdSeq::new(2),
                    trade_id: TradeId::new(3),
                    maker_order_id: OrderId::new(12).expect("maker"),
                    taker_order_id: OrderId::new(13).expect("taker"),
                    price,
                    qty,
                    recv_ts: RecvTs::new(101),
                }),
                MessageKind::TradePrint,
            ),
            (
                TradingEvent::BookUpdate(BookUpdate {
                    md_seq: MdSeq::new(4),
                    prev_md_seq: MdSeq::new(3),
                    bid_price: None,
                    bid_qty: 0,
                    ask_price: None,
                    ask_qty: 0,
                    recv_ts: RecvTs::new(102),
                }),
                MessageKind::BookUpdate,
            ),
            (
                TradingEvent::ProtocolError(ProtocolErrorEvent {
                    engine_seq: EngineSeq::new(5),
                    template_id: 222,
                    recv_ts: RecvTs::new(103),
                    error_code: ErrorCode::UnsupportedTemplate,
                }),
                MessageKind::ProtocolError,
            ),
        ];

        for (event, kind) in events {
            let mut bytes = [0_u8; 128];
            let written = encode_trading_event(event, &mut bytes).expect("encode");
            assert_eq!(decode_header(&bytes[..written]).expect("header").kind, kind);
        }
    }
}
