use crate::{
    domain::{
        Price,
        trading::md::{BookUpdate, TradePrint},
    },
    protocol::{
        MessageKind, ProtocolResult, SBE_HEADER_LEN, encode_header,
        generated::codecs::{WriteBuf, book_update_codec, trade_print_codec},
    },
};

use super::super::ensure_out;

pub fn encode_trade_print(event: TradePrint, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = trade_print_codec::SBE_BLOCK_LENGTH;
    ensure_out(out, block_len)?;
    encode_header(out, block_len, MessageKind::TradePrint)?;

    let write_buf = WriteBuf::new(out);
    let mut encoder =
        trade_print_codec::TradePrintEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .md_seq(event.md_seq.raw())
        .trade_id(event.trade_id.raw())
        .maker_order_id(event.maker_order_id.raw())
        .taker_order_id(event.taker_order_id.raw())
        .price(event.price.raw())
        .qty(event.qty.raw())
        .recv_ts(event.recv_ts.raw());
    Ok(SBE_HEADER_LEN + trade_print_codec::SBE_BLOCK_LENGTH as usize)
}

pub fn encode_book_update(event: BookUpdate, out: &mut [u8]) -> ProtocolResult<usize> {
    let block_len = book_update_codec::SBE_BLOCK_LENGTH;
    ensure_out(out, block_len)?;
    encode_header(out, block_len, MessageKind::BookUpdate)?;

    let write_buf = WriteBuf::new(out);
    let mut encoder =
        book_update_codec::BookUpdateEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .md_seq(event.md_seq.raw())
        .prev_md_seq(event.prev_md_seq.raw())
        .bid_price(event.bid_price.map_or(0, Price::raw))
        .bid_qty(event.bid_qty)
        .ask_price(event.ask_price.map_or(0, Price::raw))
        .ask_qty(event.ask_qty)
        .recv_ts(event.recv_ts.raw());
    Ok(SBE_HEADER_LEN + book_update_codec::SBE_BLOCK_LENGTH as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        RecvTs,
        trading::md::{MdSeq, TradeId},
        trading::order::OrderId,
    };

    #[test]
    fn book_update_encodes_empty_top_of_book_as_zero() {
        let event = BookUpdate {
            md_seq: MdSeq::new(10),
            prev_md_seq: MdSeq::new(9),
            bid_price: None,
            bid_qty: 0,
            ask_price: None,
            ask_qty: 0,
            recv_ts: RecvTs::new(1000),
        };

        let mut bytes = [0_u8; SBE_HEADER_LEN + book_update_codec::SBE_BLOCK_LENGTH as usize];
        encode_book_update(event, &mut bytes).expect("encode");

        let decoder = book_update_codec::BookUpdateDecoder::default().wrap(
            crate::protocol::generated::codecs::ReadBuf::new(&bytes),
            SBE_HEADER_LEN,
            book_update_codec::SBE_BLOCK_LENGTH,
            crate::protocol::SCHEMA_VERSION,
        );
        assert_eq!(decoder.md_seq(), 10);
        assert_eq!(decoder.prev_md_seq(), 9);
        assert_eq!(decoder.bid_price(), 0);
        assert_eq!(decoder.bid_qty(), 0);
        assert_eq!(decoder.ask_price(), 0);
        assert_eq!(decoder.ask_qty(), 0);
    }

    #[test]
    fn trade_print_encodes_expected_template() {
        let instrument =
            crate::domain::rd::InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument");
        let event = TradePrint {
            md_seq: MdSeq::new(12),
            trade_id: TradeId::new(13),
            maker_order_id: OrderId::new(14).expect("maker"),
            taker_order_id: OrderId::new(15).expect("taker"),
            price: crate::domain::Price::new(100, &instrument).expect("price"),
            qty: crate::domain::Qty::new(10, &instrument).expect("qty"),
            recv_ts: RecvTs::new(1000),
        };

        let mut bytes = [0_u8; SBE_HEADER_LEN + trade_print_codec::SBE_BLOCK_LENGTH as usize];
        encode_trade_print(event, &mut bytes).expect("encode");

        assert_eq!(
            crate::protocol::decode_header(&bytes).expect("header").kind,
            MessageKind::TradePrint
        );
    }
}
