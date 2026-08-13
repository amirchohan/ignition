use std::{collections::BTreeMap, net::TcpStream, sync::Once, time::Duration};

use serde::Deserialize;
use tungstenite::{Message, WebSocket, connect, stream::MaybeTlsStream};

use crate::{
    domain::{rd::InstrumentSpec, trading::order::Side},
    protocol::generated::codecs::book_side,
};

use super::{
    commands::{ClientIdSource, scale_decimal_ceil, scale_decimal_floor},
    config::BinanceConfig,
    protocol::{ClientCancelOrder, ClientCancelReplace, ClientNewOrder, ClientOrderFrame},
};

#[derive(Debug)]
pub struct BinanceDepthSource {
    config: BinanceConfig,
    instrument: InstrumentSpec,
    buy_account: u32,
    sell_account: u32,
    ids: ClientIdSource,
    levels: BTreeMap<(book_side::BookSide, u64), SyntheticLevel>,
    socket: Option<WebSocket<MaybeTlsStream<TcpStream>>>,
    status: String,
}

impl BinanceDepthSource {
    pub fn new(config: BinanceConfig, instrument: InstrumentSpec, buy_account: u32, sell_account: u32) -> Self {
        Self {
            config,
            instrument,
            buy_account,
            sell_account,
            ids: ClientIdSource::new(10_000_000_000),
            levels: BTreeMap::new(),
            socket: None,
            status: "binance idle".to_owned(),
        }
    }

    pub fn status(&self) -> &str {
        &self.status
    }

    pub fn poll(&mut self, out: &mut Vec<ClientOrderFrame>) {
        if self.socket.is_none() && !self.connect() {
            return;
        }

        let Some(socket) = self.socket.as_mut() else {
            return;
        };
        match socket.read() {
            Ok(Message::Text(text)) => match serde_json::from_str::<PartialDepth>(&text) {
                Ok(depth) => self.apply_depth(depth, out),
                Err(_) => self.status = "binance decode failed".to_owned(),
            },
            Ok(_) => {}
            Err(_) => {
                self.socket = None;
                self.status = "binance read failed".to_owned();
            }
        }
    }

    fn connect(&mut self) -> bool {
        install_rustls_provider();
        let url = format!(
            "wss://stream.binance.com:9443/ws/{}@depth{}@{}ms",
            self.config.symbol.to_ascii_lowercase(),
            self.config.levels,
            self.config.speed_ms
        );
        match connect(&url) {
            Ok((socket, _)) => {
                self.socket = Some(socket);
                self.status = format!("binance {}", self.config.symbol);
                true
            }
            Err(_) => {
                self.status = "binance connect failed".to_owned();
                std::thread::sleep(Duration::from_millis(self.config.reconnect_backoff_ms));
                false
            }
        }
    }

    fn apply_depth(&mut self, depth: PartialDepth, out: &mut Vec<ClientOrderFrame>) {
        let mut seen = BTreeMap::new();
        for level in depth.bids {
            self.apply_level(book_side::BookSide::Bid, level, &mut seen, out);
        }
        for level in depth.asks {
            self.apply_level(book_side::BookSide::Ask, level, &mut seen, out);
        }

        let stale = self
            .levels
            .keys()
            .copied()
            .filter(|key| !seen.contains_key(key))
            .collect::<Vec<_>>();
        for key in stale {
            if let Some(level) = self.levels.remove(&key) {
                out.push(ClientOrderFrame::Cancel(ClientCancelOrder {
                    client_order_id: self.ids.next(),
                    account_id: level.account_id,
                    orig_client_order_id: level.order_id,
                    client_ts: self.ids.next(),
                }));
            }
        }
    }

    fn apply_level(
        &mut self,
        side: book_side::BookSide,
        raw: [String; 2],
        seen: &mut BTreeMap<(book_side::BookSide, u64), ()>,
        out: &mut Vec<ClientOrderFrame>,
    ) {
        let price = match side {
            book_side::BookSide::Bid => scale_decimal_floor(&raw[0], self.instrument.price_decimals, self.instrument.price_tick),
            book_side::BookSide::Ask => scale_decimal_ceil(&raw[0], self.instrument.price_decimals, self.instrument.price_tick),
            book_side::BookSide::NullVal => return,
        };
        let Ok(price) = price else {
            return;
        };
        let Ok(qty) = scale_decimal_floor(&raw[1], self.instrument.qty_decimals, self.instrument.qty_lot) else {
            return;
        };
        if price == 0 || qty == 0 {
            return;
        }

        let key = (side, price);
        seen.insert(key, ());
        let account_id = match side {
            book_side::BookSide::Bid => self.buy_account,
            book_side::BookSide::Ask => self.sell_account,
            book_side::BookSide::NullVal => return,
        };
        let order_side = match side {
            book_side::BookSide::Bid => Side::Buy,
            book_side::BookSide::Ask => Side::Sell,
            book_side::BookSide::NullVal => return,
        };

        if let Some(level) = self.levels.get_mut(&key) {
            if level.qty == qty {
                return;
            }
            level.qty = qty;
            let new_order_id = self.ids.next();
            out.push(ClientOrderFrame::Replace(ClientCancelReplace {
                client_order_id: new_order_id,
                account_id,
                orig_client_order_id: level.order_id,
                client_ts: self.ids.next(),
                price,
                qty,
            }));
            level.order_id = new_order_id;
            return;
        }

        let order_id = self.ids.next();
        out.push(ClientOrderFrame::New(ClientNewOrder {
            client_order_id: order_id,
            account_id,
            client_ts: self.ids.next(),
            side: order_side,
            order_type: crate::domain::trading::order::OrderType::Limit,
            time_in_force: crate::domain::trading::order::TimeInForce::Gtc,
            price,
            qty,
        }));
        self.levels.insert(key, SyntheticLevel { order_id, account_id, qty });
    }
}

fn install_rustls_provider() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SyntheticLevel {
    order_id: u64,
    account_id: u32,
    qty: u64,
}

#[derive(Debug, Deserialize)]
struct PartialDepth {
    #[serde(default)]
    bids: Vec<[String; 2]>,
    #[serde(default)]
    asks: Vec<[String; 2]>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instrument() -> InstrumentSpec {
        InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument")
    }

    fn source() -> BinanceDepthSource {
        BinanceDepthSource::new(BinanceConfig::default(), instrument(), 1, 2)
    }

    #[test]
    fn maps_top_levels_to_one_order_per_side_price() {
        let mut source = source();
        let mut frames = Vec::new();
        source.apply_depth(
            PartialDepth {
                bids: vec![["100000.00".to_owned(), "0.10".to_owned()]],
                asks: vec![["100001.001".to_owned(), "0.20".to_owned()]],
            },
            &mut frames,
        );

        assert_eq!(frames.len(), 2);
        let ClientOrderFrame::New(bid) = frames[0] else {
            panic!("expected bid new");
        };
        assert_eq!(bid.account_id, 1);
        assert_eq!(bid.price, 10_000_000);
        assert_eq!(bid.qty, 10_000_000);

        let ClientOrderFrame::New(ask) = frames[1] else {
            panic!("expected ask new");
        };
        assert_eq!(ask.account_id, 2);
        assert_eq!(ask.price, 10_000_101);
        assert_eq!(ask.qty, 20_000_000);
    }

    #[test]
    fn maps_changed_and_removed_levels_to_replace_and_cancel() {
        let mut source = source();
        let mut frames = Vec::new();
        source.apply_depth(
            PartialDepth {
                bids: vec![["100000.00".to_owned(), "0.10".to_owned()]],
                asks: vec![],
            },
            &mut frames,
        );
        frames.clear();

        source.apply_depth(
            PartialDepth {
                bids: vec![["100000.00".to_owned(), "0.11".to_owned()]],
                asks: vec![],
            },
            &mut frames,
        );
        assert!(matches!(frames[0], ClientOrderFrame::Replace(_)));
        frames.clear();

        source.apply_depth(
            PartialDepth {
                bids: vec![],
                asks: vec![],
            },
            &mut frames,
        );
        assert!(matches!(frames[0], ClientOrderFrame::Cancel(_)));
    }
}
