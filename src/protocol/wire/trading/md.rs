use crate::{
    domain::{
        Price,
        trading::{
            md::{L1Update, L2Update, TradePrint},
            order::Side,
        },
    },
    protocol::{
        MessageKind, ProtocolResult, SBE_HEADER_LEN, encode_header,
        generated::codecs::{WriteBuf, book_side, l1_update_codec, l2_update_codec, trade_print_codec},
    },
};

use super::super::ensure_out;

pub fn encode_trade_print(event: TradePrint, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = trade_print_codec::SBE_BLOCK_LENGTH;
    ensure_out(out, block_len)?;
    encode_header(out, block_len, MessageKind::TradePrint)?;

    let write_buf = WriteBuf::new(out);
    let mut encoder = trade_print_codec::TradePrintEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .engine_seq(event.engine_seq.raw())
        .md_seq(event.md_seq.raw())
        .trade_id(event.trade_id.raw())
        .maker_order_id(event.maker_order_id.raw())
        .taker_order_id(event.taker_order_id.raw())
        .price(event.price.raw())
        .qty(event.qty.raw());
    Ok(SBE_HEADER_LEN + trade_print_codec::SBE_BLOCK_LENGTH as usize)
}

pub fn encode_l1_update(event: L1Update, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = l1_update_codec::SBE_BLOCK_LENGTH;
    ensure_out(out, block_len)?;
    encode_header(out, block_len, MessageKind::L1Update)?;

    let write_buf = WriteBuf::new(out);
    let mut encoder = l1_update_codec::L1UpdateEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .engine_seq(event.engine_seq.raw())
        .md_seq(event.md_seq.raw())
        .prev_md_seq(event.prev_md_seq.raw())
        .bid_price(event.bid_price.map_or(0, Price::raw))
        .bid_qty(event.bid_qty)
        .ask_price(event.ask_price.map_or(0, Price::raw))
        .ask_qty(event.ask_qty);
    Ok(SBE_HEADER_LEN + l1_update_codec::SBE_BLOCK_LENGTH as usize)
}

pub fn encode_l2_update(event: L2Update, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = l2_update_codec::SBE_BLOCK_LENGTH;
    ensure_out(out, block_len)?;
    encode_header(out, block_len, MessageKind::L2Update)?;

    let write_buf = WriteBuf::new(out);
    let mut encoder = l2_update_codec::L2UpdateEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .engine_seq(event.engine_seq.raw())
        .md_seq(event.md_seq.raw())
        .prev_md_seq(event.prev_md_seq.raw())
        .side(match event.side {
            Side::Buy => book_side::BookSide::Bid,
            Side::Sell => book_side::BookSide::Ask,
        })
        .price(event.price.raw())
        .qty(event.qty);
    Ok(SBE_HEADER_LEN + l2_update_codec::SBE_BLOCK_LENGTH as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        EngineSeq,
        trading::md::{MdSeq, TradeId},
        trading::order::OrderId,
    };

    #[test]
    fn l1_update_encodes_empty_top_of_book_as_zero() {
        let event = L1Update {
            engine_seq: EngineSeq::new(8),
            md_seq: MdSeq::new(10),
            prev_md_seq: MdSeq::new(9),
            bid_price: None,
            bid_qty: 0,
            ask_price: None,
            ask_qty: 0,
        };

        let mut bytes = [0_u8; SBE_HEADER_LEN + l1_update_codec::SBE_BLOCK_LENGTH as usize];
        encode_l1_update(event, &mut bytes).expect("encode");

        let decoder = l1_update_codec::L1UpdateDecoder::default().wrap(
            crate::protocol::generated::codecs::ReadBuf::new(&bytes),
            SBE_HEADER_LEN,
            l1_update_codec::SBE_BLOCK_LENGTH,
            crate::protocol::SCHEMA_VERSION,
        );
        assert_eq!(decoder.md_seq(), 10);
        assert_eq!(decoder.prev_md_seq(), 9);
        assert_eq!(decoder.engine_seq(), 8);
        assert_eq!(decoder.bid_price(), 0);
        assert_eq!(decoder.bid_qty(), 0);
        assert_eq!(decoder.ask_price(), 0);
        assert_eq!(decoder.ask_qty(), 0);
    }

    #[test]
    fn trade_print_encodes_expected_template() {
        let instrument = crate::domain::rd::InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument");
        let event = TradePrint {
            engine_seq: EngineSeq::new(11),
            md_seq: MdSeq::new(12),
            trade_id: TradeId::new(13),
            maker_order_id: OrderId::new(14).expect("maker"),
            taker_order_id: OrderId::new(15).expect("taker"),
            price: crate::domain::Price::new(100, &instrument).expect("price"),
            qty: crate::domain::Qty::new(10, &instrument).expect("qty"),
        };

        let mut bytes = [0_u8; SBE_HEADER_LEN + trade_print_codec::SBE_BLOCK_LENGTH as usize];
        encode_trade_print(event, &mut bytes).expect("encode");

        assert_eq!(
            crate::protocol::decode_header(&bytes).expect("header").kind,
            MessageKind::TradePrint
        );
        let decoder = trade_print_codec::TradePrintDecoder::default().wrap(
            crate::protocol::generated::codecs::ReadBuf::new(&bytes),
            SBE_HEADER_LEN,
            trade_print_codec::SBE_BLOCK_LENGTH,
            crate::protocol::SCHEMA_VERSION,
        );
        assert_eq!(decoder.engine_seq(), 11);
    }

    #[test]
    fn l2_update_encodes_absolute_level() {
        let instrument = crate::domain::rd::InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument");
        let event = L2Update {
            engine_seq: EngineSeq::new(18),
            md_seq: MdSeq::new(20),
            prev_md_seq: MdSeq::new(19),
            side: Side::Buy,
            price: crate::domain::Price::new(100, &instrument).expect("price"),
            qty: 30,
        };

        let mut bytes = [0_u8; SBE_HEADER_LEN + l2_update_codec::SBE_BLOCK_LENGTH as usize];
        encode_l2_update(event, &mut bytes).expect("encode");

        let decoder = l2_update_codec::L2UpdateDecoder::default().wrap(
            crate::protocol::generated::codecs::ReadBuf::new(&bytes),
            SBE_HEADER_LEN,
            l2_update_codec::SBE_BLOCK_LENGTH,
            crate::protocol::SCHEMA_VERSION,
        );
        assert_eq!(decoder.md_seq(), 20);
        assert_eq!(decoder.prev_md_seq(), 19);
        assert_eq!(decoder.engine_seq(), 18);
        assert_eq!(decoder.side(), book_side::BookSide::Bid);
        assert_eq!(decoder.price(), 100);
        assert_eq!(decoder.qty(), 30);
    }
}
