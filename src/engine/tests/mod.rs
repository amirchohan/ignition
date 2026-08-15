use super::*;
use crate::domain::{
    EngineSeq, Price, Qty, RecvTs, Sequencer,
    rd::InstrumentSpec,
    trading::order::{
        AccountId, ClientTs, ExecType, ExecutionReport, NewOrder, OrdStatus, OrderEntry, OrderId, OrderType, Side, TimeInForce,
    },
};
use crate::{
    io::IOConfig,
    market_data::{MarketDataFeedConfig, MarketDataRuntime},
    protocol::{
        MessageKind, SBE_HEADER_LEN, SCHEMA_VERSION, decode_header,
        generated::codecs::{ReadBuf, book_side, l1_update_codec, l2_update_codec, trade_print_codec},
    },
};
use proptest::prelude::*;
use std::{
    collections::{HashMap, HashSet},
    io::Read,
    path::{Path, PathBuf},
};

type TradingEvent = ExecutionReport;

fn instrument() -> InstrumentSpec {
    InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument")
}

fn config() -> EngineConfig {
    EngineConfig::new(instrument(), 100, 10_000, 1024, 128, 100, 16).expect("config")
}

fn price(raw: u64) -> Price {
    Price::new(raw, &instrument()).expect("price")
}

fn qty(raw: u64) -> Qty {
    Qty::new(raw, &instrument()).expect("qty")
}

#[allow(clippy::too_many_arguments)]
fn new_order(
    id: u64,
    account: u32,
    side: Side,
    order_type: OrderType,
    tif: TimeInForce,
    price_raw: Option<u64>,
    qty_raw: u64,
    recv_ts: u64,
) -> OrderEntry {
    OrderEntry::NewOrder(NewOrder {
        client_order_id: OrderId::new(id).expect("order"),
        account_id: AccountId::new(account).expect("account"),
        client_ts: ClientTs::new(recv_ts),
        recv_ts: RecvTs::new(recv_ts),
        side,
        order_type,
        time_in_force: tif,
        price: price_raw.map(price),
        qty: qty(qty_raw),
    })
}

fn cancel(id: u64, account: u32, orig: u64) -> OrderEntry {
    OrderEntry::CancelOrder(crate::domain::trading::order::CancelOrder {
        client_order_id: OrderId::new(id).expect("order"),
        account_id: AccountId::new(account).expect("account"),
        orig_client_order_id: OrderId::new(orig).expect("orig"),
        client_ts: ClientTs::new(1),
        recv_ts: RecvTs::new(1),
    })
}

fn replace(id: u64, account: u32, orig: u64, price_raw: u64, qty_raw: u64) -> OrderEntry {
    OrderEntry::CancelReplace(crate::domain::trading::order::CancelReplace {
        client_order_id: OrderId::new(id).expect("order"),
        account_id: AccountId::new(account).expect("account"),
        orig_client_order_id: OrderId::new(orig).expect("orig"),
        client_ts: ClientTs::new(1),
        recv_ts: RecvTs::new(1),
        price: price(price_raw),
        qty: qty(qty_raw),
    })
}

fn mass_cancel(id: u64, account: u32) -> OrderEntry {
    OrderEntry::MassCancel(crate::domain::trading::order::MassCancel {
        client_order_id: OrderId::new(id).expect("order"),
        account_id: AccountId::new(account).expect("account"),
        client_ts: ClientTs::new(1),
        recv_ts: RecvTs::new(1),
    })
}

fn exec_reports(events: &[TradingEvent]) -> Vec<crate::domain::trading::order::ExecutionReport> {
    events.to_vec()
}

fn reports_for_order(events: &[TradingEvent], order_id: u64) -> Vec<crate::domain::trading::order::ExecutionReport> {
    exec_reports(events)
        .into_iter()
        .filter(|report| report.client_order_id.raw() == order_id)
        .collect()
}

fn trade_report_makers(events: &[TradingEvent], taker_order_id: u64) -> Vec<u64> {
    exec_reports(events)
        .into_iter()
        .filter(|report| report.exec_type == ExecType::Trade && report.client_order_id.raw() != taker_order_id)
        .map(|report| report.client_order_id.raw())
        .collect()
}

fn market_data_runtime() -> Option<MarketDataRuntime> {
    match MarketDataRuntime::new(market_data_config()) {
        Ok(market_data) => Some(market_data),
        Err(crate::market_data::MarketDataError::Io(crate::io::IoError::Bind(error)))
            if error.kind() == std::io::ErrorKind::PermissionDenied =>
        {
            None
        }
        Err(error) => panic!("market-data: {error:?}"),
    }
}

fn market_data_config() -> MarketDataFeedConfig {
    MarketDataFeedConfig {
        io: IOConfig::Udp {
            bind_addr: "0.0.0.0:0".to_owned(),
            group_addr: "239.255.0.1:19002".to_owned(),
            multicast_ttl: 1,
            multicast_loop_v4: true,
            max_datagram_len: 4096,
            max_pending_datagram_bytes: 1 << 20,
        },
    }
}

fn market_data_file_config(path: PathBuf) -> MarketDataFeedConfig {
    MarketDataFeedConfig {
        io: IOConfig::File {
            input_path: None,
            output_path: Some(path),
            max_frame_len: 4096,
        },
    }
}

fn temp_path(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("ignition-engine-{name}-{}-{}.bin", std::process::id(), unique_suffix()))
}

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CapturedMarketData {
    Trade {
        engine_seq: u64,
        md_seq: u64,
        trade_id: u64,
        maker_order_id: u64,
        taker_order_id: u64,
        price: u64,
        qty: u64,
    },
    L1 {
        engine_seq: u64,
        md_seq: u64,
        prev_md_seq: u64,
        bid_price: u64,
        bid_qty: u64,
        ask_price: u64,
        ask_qty: u64,
    },
    L2 {
        engine_seq: u64,
        md_seq: u64,
        prev_md_seq: u64,
        side: Side,
        price: u64,
        qty: u64,
    },
}

impl CapturedMarketData {
    const fn md_seq(self) -> u64 {
        match self {
            Self::Trade { md_seq, .. } | Self::L1 { md_seq, .. } | Self::L2 { md_seq, .. } => md_seq,
        }
    }

    const fn engine_seq(self) -> u64 {
        match self {
            Self::Trade { engine_seq, .. } | Self::L1 { engine_seq, .. } | Self::L2 { engine_seq, .. } => engine_seq,
        }
    }
}

fn read_market_data(path: &Path) -> Vec<CapturedMarketData> {
    let mut file = std::fs::File::open(path).expect("market-data output");
    let mut events = Vec::new();
    loop {
        let mut len = [0_u8; 4];
        match file.read_exact(&mut len) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => panic!("read market-data frame len: {error:?}"),
        }
        let mut frame = vec![0_u8; u32::from_le_bytes(len) as usize];
        file.read_exact(&mut frame).expect("read market-data frame");
        let header = decode_header(&frame).expect("market-data header");
        let read_buf = ReadBuf::new(&frame);
        events.push(match header.kind {
            MessageKind::TradePrint => {
                let decoder = trade_print_codec::TradePrintDecoder::default().wrap(
                    read_buf,
                    SBE_HEADER_LEN,
                    trade_print_codec::SBE_BLOCK_LENGTH,
                    SCHEMA_VERSION,
                );
                CapturedMarketData::Trade {
                    engine_seq: decoder.engine_seq(),
                    md_seq: decoder.md_seq(),
                    trade_id: decoder.trade_id(),
                    maker_order_id: decoder.maker_order_id(),
                    taker_order_id: decoder.taker_order_id(),
                    price: decoder.price(),
                    qty: decoder.qty(),
                }
            }
            MessageKind::L1Update => {
                let decoder = l1_update_codec::L1UpdateDecoder::default().wrap(
                    read_buf,
                    SBE_HEADER_LEN,
                    l1_update_codec::SBE_BLOCK_LENGTH,
                    SCHEMA_VERSION,
                );
                CapturedMarketData::L1 {
                    engine_seq: decoder.engine_seq(),
                    md_seq: decoder.md_seq(),
                    prev_md_seq: decoder.prev_md_seq(),
                    bid_price: decoder.bid_price(),
                    bid_qty: decoder.bid_qty(),
                    ask_price: decoder.ask_price(),
                    ask_qty: decoder.ask_qty(),
                }
            }
            MessageKind::L2Update => {
                let decoder = l2_update_codec::L2UpdateDecoder::default().wrap(
                    read_buf,
                    SBE_HEADER_LEN,
                    l2_update_codec::SBE_BLOCK_LENGTH,
                    SCHEMA_VERSION,
                );
                CapturedMarketData::L2 {
                    engine_seq: decoder.engine_seq(),
                    md_seq: decoder.md_seq(),
                    prev_md_seq: decoder.prev_md_seq(),
                    side: match decoder.side() {
                        book_side::BookSide::Bid => Side::Buy,
                        book_side::BookSide::Ask => Side::Sell,
                        book_side::BookSide::NullVal => panic!("null book side"),
                    },
                    price: decoder.price(),
                    qty: decoder.qty(),
                }
            }
            kind => panic!("unexpected market-data frame: {kind:?}"),
        });
    }
    events
}

struct TestEngine {
    engine: Engine,
    sequencer: Sequencer,
    market_data: MarketDataRuntime,
}

impl TestEngine {
    fn new() -> Option<Self> {
        Some(Self {
            engine: Engine::new(config()).expect("engine"),
            sequencer: Sequencer::default(),
            market_data: market_data_runtime()?,
        })
    }

    fn new_noop() -> Self {
        Self::new_noop_with_config(config())
    }

    fn new_noop_with_config(engine_config: EngineConfig) -> Self {
        Self {
            engine: Engine::new(engine_config).expect("engine"),
            sequencer: Sequencer::default(),
            market_data: MarketDataRuntime::new(MarketDataFeedConfig { io: IOConfig::Noop }).expect("market data"),
        }
    }

    fn new_file_market_data(path: PathBuf) -> Self {
        Self {
            engine: Engine::new(config()).expect("engine"),
            sequencer: Sequencer::default(),
            market_data: MarketDataRuntime::new(market_data_file_config(path)).expect("market data"),
        }
    }

    fn best_bid(&self) -> Option<(Price, u64)> {
        self.engine.best_bid()
    }

    fn best_ask(&self) -> Option<(Price, u64)> {
        self.engine.best_ask()
    }

    fn resting_order_count(&self) -> usize {
        self.engine.resting_order_count()
    }

    fn account_open_order_count(&self, account: u32) -> Option<u32> {
        self.engine.account_open_order_count(AccountId::new(account).expect("account"))
    }

    fn account_open_notional(&self, account: u32) -> Option<u128> {
        self.engine.account_open_notional(AccountId::new(account).expect("account"))
    }

    fn execute(&mut self, entry: OrderEntry, events: &mut Vec<TradingEvent>, label: &str) {
        let _ = self.execute_outcome(entry, events, label);
    }

    fn execute_outcome(&mut self, entry: OrderEntry, events: &mut Vec<TradingEvent>, label: &str) -> EngineExecution {
        events.clear();
        let mut emit_report = |report| {
            events.push(report);
        };
        self.engine
            .execute(entry, &mut self.sequencer, &mut self.market_data, &mut emit_report)
            .expect(label)
    }

    fn flush_market_data(&mut self) {
        self.market_data.flush_pending().expect("flush market data");
    }
}

fn execute(engine: &mut TestEngine, entry: OrderEntry, events: &mut Vec<TradingEvent>, label: &str) {
    engine.execute(entry, events, label);
}

fn resting_rows(engine: &TestEngine) -> Vec<(u64, u32, Side, u64, u64)> {
    let mut rows = Vec::new();
    engine
        .engine
        .try_for_each_resting_order(|row| {
            rows.push((
                row.order_id.raw(),
                row.account_id.raw(),
                row.side,
                row.price.raw(),
                row.remaining_qty,
            ));
            Ok::<(), ()>(())
        })
        .expect("resting rows");
    rows
}

fn assert_account_aggregates_match_rows(engine: &TestEngine) {
    let rows = resting_rows(engine);
    let mut expected: HashMap<u32, (u32, u128)> = HashMap::new();
    for (_, account, _, price_raw, qty_raw) in rows {
        let entry = expected.entry(account).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += u128::from(price_raw) * u128::from(qty_raw);
    }
    for account in 1..=config().max_account_id() {
        let expected = expected.get(&account).copied().unwrap_or((0, 0));
        assert_eq!(
            engine.account_open_order_count(account),
            Some(expected.0),
            "account {account} open count"
        );
        assert_eq!(
            engine.account_open_notional(account),
            Some(expected.1),
            "account {account} notional"
        );
    }
}

fn side_order(side: Side, price_raw: u64) -> std::cmp::Reverse<u64> {
    match side {
        Side::Buy => std::cmp::Reverse(price_raw),
        Side::Sell => std::cmp::Reverse(u64::MAX - price_raw),
    }
}

#[derive(Debug, Clone, Copy)]
enum GeneratedCommand {
    New {
        order_id: u64,
        account: u32,
        buy: bool,
        market: bool,
        tif: u8,
        price_raw: u64,
        qty_raw: u64,
    },
    Cancel {
        order_id: u64,
        account: u32,
        orig_order_id: u64,
    },
    Replace {
        order_id: u64,
        account: u32,
        orig_order_id: u64,
        price_raw: u64,
        qty_raw: u64,
    },
    MassCancel {
        order_id: u64,
        account: u32,
    },
}

impl GeneratedCommand {
    fn into_entry(self, recv_ts: u64) -> OrderEntry {
        match self {
            Self::New {
                order_id,
                account,
                buy,
                market,
                tif,
                price_raw,
                qty_raw,
            } => new_order(
                order_id,
                account,
                if buy { Side::Buy } else { Side::Sell },
                if market { OrderType::Market } else { OrderType::Limit },
                match tif {
                    0 => TimeInForce::Gtc,
                    1 => TimeInForce::Ioc,
                    _ => TimeInForce::PostOnly,
                },
                (!market).then_some(price_raw),
                qty_raw,
                recv_ts,
            ),
            Self::Cancel {
                order_id,
                account,
                orig_order_id,
            } => cancel(order_id, account, orig_order_id),
            Self::Replace {
                order_id,
                account,
                orig_order_id,
                price_raw,
                qty_raw,
            } => replace(order_id, account, orig_order_id, price_raw, qty_raw),
            Self::MassCancel { order_id, account } => mass_cancel(order_id, account),
        }
    }
}

fn generated_command_strategy() -> impl Strategy<Value = GeneratedCommand> {
    prop_oneof![
        5 => (1_u64..40, 1_u32..10, any::<bool>(), any::<bool>(), 0_u8..3, 1_u64..11_000, 1_u64..25)
            .prop_map(|(order_id, account, buy, market, tif, price_raw, qty_raw)| GeneratedCommand::New {
                order_id,
                account,
                buy,
                market,
                tif,
                price_raw,
                qty_raw,
            }),
        2 => (100_u64..160, 1_u32..10, 1_u64..40)
            .prop_map(|(order_id, account, orig_order_id)| GeneratedCommand::Cancel {
                order_id,
                account,
                orig_order_id,
            }),
        3 => (1_u64..50, 1_u32..10, 1_u64..40, 1_u64..11_000, 1_u64..35)
            .prop_map(|(order_id, account, orig_order_id, price_raw, qty_raw)| GeneratedCommand::Replace {
                order_id,
                account,
                orig_order_id,
                price_raw,
                qty_raw,
            }),
        1 => (200_u64..240, 1_u32..10).prop_map(|(order_id, account)| GeneratedCommand::MassCancel {
            order_id,
            account,
        }),
    ]
}

fn assert_reports_are_strictly_sequenced(events: &[TradingEvent], last_engine_seq: &mut u64) {
    for report in events {
        assert!(
            report.engine_seq.raw() > *last_engine_seq,
            "engine seq {} did not advance after {}",
            report.engine_seq.raw(),
            *last_engine_seq
        );
        *last_engine_seq = report.engine_seq.raw();
    }
}

fn assert_trade_reports_are_paired(events: &[TradingEvent]) {
    let trades = events
        .iter()
        .filter(|report| report.exec_type == ExecType::Trade)
        .collect::<Vec<_>>();
    assert_eq!(trades.len() % 2, 0, "trade reports must be maker/taker pairs: {trades:?}");
    for pair in trades.chunks_exact(2) {
        assert_ne!(
            (pair[0].account_id, pair[0].client_order_id),
            (pair[1].account_id, pair[1].client_order_id)
        );
        assert_eq!(pair[0].fill_price, pair[1].fill_price);
        assert_eq!(pair[0].fill_qty, pair[1].fill_qty);
        assert!(pair[0].fill_price.is_some());
        assert!(pair[0].fill_qty > 0);
    }
}

fn assert_rows_are_in_book_order(rows: &[(u64, u32, Side, u64, u64)]) {
    let first_sell = rows.iter().position(|(_, _, side, _, _)| *side == Side::Sell).unwrap_or(rows.len());
    assert!(rows[..first_sell].iter().all(|(_, _, side, _, _)| *side == Side::Buy));
    assert!(rows[first_sell..].iter().all(|(_, _, side, _, _)| *side == Side::Sell));

    for pair in rows[..first_sell].windows(2) {
        assert!(pair[0].3 >= pair[1].3, "bids not descending: {rows:?}");
    }
    for pair in rows[first_sell..].windows(2) {
        assert!(pair[0].3 <= pair[1].3, "asks not ascending: {rows:?}");
    }
}

fn assert_best_quotes_match_rows(engine: &TestEngine, rows: &[(u64, u32, Side, u64, u64)]) {
    let best_bid = rows
        .iter()
        .filter(|(_, _, side, _, _)| *side == Side::Buy)
        .map(|(_, _, _, price_raw, _)| *price_raw)
        .max()
        .map(|best_price| {
            (
                price(best_price),
                rows.iter()
                    .filter(|(_, _, side, price_raw, _)| *side == Side::Buy && *price_raw == best_price)
                    .map(|(_, _, _, _, qty_raw)| *qty_raw)
                    .sum(),
            )
        });
    let best_ask = rows
        .iter()
        .filter(|(_, _, side, _, _)| *side == Side::Sell)
        .map(|(_, _, _, price_raw, _)| *price_raw)
        .min()
        .map(|best_price| {
            (
                price(best_price),
                rows.iter()
                    .filter(|(_, _, side, price_raw, _)| *side == Side::Sell && *price_raw == best_price)
                    .map(|(_, _, _, _, qty_raw)| *qty_raw)
                    .sum(),
            )
        });

    assert_eq!(engine.best_bid(), best_bid);
    assert_eq!(engine.best_ask(), best_ask);
    if let (Some((bid, _)), Some((ask, _))) = (best_bid, best_ask) {
        assert!(bid < ask, "book must not remain crossed");
    }
}

fn assert_core_book_invariants(engine: &TestEngine) {
    let rows = resting_rows(engine);
    assert_eq!(engine.resting_order_count(), rows.len());
    assert!(
        rows.iter()
            .all(|(_, _, _, price_raw, qty_raw)| (100..=10_000).contains(price_raw) && *qty_raw > 0)
    );
    assert_eq!(
        rows.iter()
            .map(|(order_id, account_id, _, _, _)| (*account_id, *order_id))
            .collect::<HashSet<_>>()
            .len(),
        rows.len(),
        "resting order IDs must be unique within each account"
    );
    assert_rows_are_in_book_order(&rows);
    assert_best_quotes_match_rows(engine, &rows);
    assert_account_aggregates_match_rows(engine);
}

fn capacity_config(max_resting_orders: u32, page_size: u32, max_active_pages: u32) -> EngineConfig {
    EngineConfig::new(instrument(), 100, 10_000, page_size, max_resting_orders, 100, max_active_pages).expect("capacity config")
}

mod capacity;
mod properties;
mod scenarios;
