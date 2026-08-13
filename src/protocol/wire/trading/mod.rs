pub mod md;
pub mod order;

use crate::{
    domain::{
        ProtocolErrorEvent, RecvTs,
        rd::InstrumentSpec,
        trading::{
            md::{L1Update, L2Update, MarketDataEvent},
            order::ExecutionReport,
        },
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

pub fn encode_execution_report(event: ExecutionReport, out: &mut [u8]) -> ProtocolResult<usize> {
    order::encode_execution_report(event, out)
}

pub fn encode_market_data_event(event: MarketDataEvent, out: &mut [u8]) -> ProtocolResult<usize> {
    match event {
        MarketDataEvent::TradePrint(event) => md::encode_trade_print(event, out),
        MarketDataEvent::L1Update(event) => md::encode_l1_update(event, out),
        MarketDataEvent::L2Update(event) => md::encode_l2_update(event, out),
    }
}

#[allow(dead_code)]
const _: fn(ExecutionReport, &mut [u8]) -> ProtocolResult<usize> = order::encode_execution_report;
#[allow(dead_code)]
const _: fn(L1Update, &mut [u8]) -> ProtocolResult<usize> = md::encode_l1_update;
#[allow(dead_code)]
const _: fn(L2Update, &mut [u8]) -> ProtocolResult<usize> = md::encode_l2_update;
#[allow(dead_code)]
const _: fn(ProtocolErrorEvent, &mut [u8]) -> ProtocolResult<usize> = encode_protocol_error;
#[allow(dead_code)]
const _: usize = SBE_HEADER_LEN + protocol_error_codec::SBE_BLOCK_LENGTH as usize;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{
            EngineSeq, Price, Qty, RejectReason,
            rd::InstrumentSpec,
            trading::{
                md::{L1Update, L2Update, MdSeq, TradeId, TradePrint},
                order::{AccountId, ExecType, ExecutionReport, OrdStatus, OrderId},
            },
        },
        protocol::{MessageKind, decode_header},
    };

    fn instrument() -> InstrumentSpec {
        InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument")
    }

    #[test]
    fn order_entry_and_market_data_events_encode_all_families() {
        let instrument = instrument();
        let price = Price::new(100, &instrument).expect("price");
        let qty = Qty::new(10, &instrument).expect("qty");

        let report = ExecutionReport {
            engine_seq: EngineSeq::new(1),
            client_order_id: OrderId::new(11).expect("order"),
            account_id: AccountId::new(22).expect("account"),
            exec_type: ExecType::New,
            ord_status: OrdStatus::New,
            reject_reason: RejectReason::None,
            fill_price: None,
            fill_qty: 0,
            remaining_qty: 10,
        };
        let market_data = [
            (
                MarketDataEvent::TradePrint(TradePrint {
                    md_seq: MdSeq::new(2),
                    trade_id: TradeId::new(3),
                    maker_order_id: OrderId::new(12).expect("maker"),
                    taker_order_id: OrderId::new(13).expect("taker"),
                    price,
                    qty,
                }),
                MessageKind::TradePrint,
            ),
            (
                MarketDataEvent::L1Update(L1Update {
                    md_seq: MdSeq::new(4),
                    prev_md_seq: MdSeq::new(3),
                    bid_price: None,
                    bid_qty: 0,
                    ask_price: None,
                    ask_qty: 0,
                }),
                MessageKind::L1Update,
            ),
            (
                MarketDataEvent::L2Update(L2Update {
                    md_seq: MdSeq::new(5),
                    prev_md_seq: MdSeq::new(4),
                    side: crate::domain::trading::order::Side::Buy,
                    price,
                    qty: 20,
                }),
                MessageKind::L2Update,
            ),
        ];

        let mut bytes = [0_u8; 128];
        let written = encode_execution_report(report, &mut bytes).expect("encode");
        assert_eq!(decode_header(&bytes[..written]).expect("header").kind, MessageKind::ExecutionReport);

        for (event, kind) in market_data {
            let mut bytes = [0_u8; 128];
            let written = encode_market_data_event(event, &mut bytes).expect("encode");
            assert_eq!(decode_header(&bytes[..written]).expect("header").kind, kind);
        }
    }
}
