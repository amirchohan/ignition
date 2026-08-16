use super::*;
use crate::control::ControlConfig;
use crate::domain::{
    Price, Qty, RecvTs, RejectReason,
    control::{ControlResult, KillSwitchAction},
    rd::InstrumentSpec,
    trading::order::{
        AccountId, CancelOrder, CancelReplace, ClientTs, ExecType, ExecutionReport, NewOrder, OrdStatus, OrderEntry, OrderId, OrderType,
        Side, TimeInForce,
    },
};
use crate::io::{Endpoint, FileFrameIo, IOConfig, InputRecord};
use crate::market_data::{MarketDataFeedConfig, MarketDataRuntime};
use crate::protocol::{
    MessageKind, SBE_HEADER_LEN, SCHEMA_VERSION, decode_header, encode_header,
    generated::codecs::{
        ReadBuf, WriteBuf, cancel_order_codec, cancel_replace_codec, control_result as wire_control_result, execution_report_codec,
        heartbeat_codec, kill_switch_action, kill_switch_codec, mass_cancel_codec, new_order_codec, order_type as wire_order_type,
        reject_reason as wire_reject_reason, side as wire_side, snapshot_begin_codec, snapshot_codec, snapshot_end_codec,
        snapshot_order_codec, time_in_force as wire_tif,
    },
};
use proptest::prelude::*;
use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    thread,
    time::Duration,
};

fn instrument() -> InstrumentSpec {
    InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1).expect("instrument")
}

fn price(raw: u64) -> Price {
    Price::new(raw, &instrument()).expect("price")
}

fn qty(raw: u64) -> Qty {
    Qty::new(raw, &instrument()).expect("qty")
}

fn engine_config() -> EngineConfig {
    EngineConfig::new(instrument(), 100, 10_000, 1024, 128, 100, 16).expect("config")
}

fn risk_config(max_orders: u32, max_notional: u128) -> RiskConfig {
    RiskConfig::new(max_orders, max_notional, price(1_000), 100).expect("risk config")
}

#[allow(clippy::too_many_arguments)]
fn new_order(id: u64, account: u32, side: Side, tif: TimeInForce, price_raw: u64, qty_raw: u64) -> OrderEntry {
    OrderEntry::NewOrder(NewOrder {
        client_order_id: OrderId::new(id).expect("order"),
        account_id: AccountId::new(account).expect("account"),
        client_ts: ClientTs::new(id),
        recv_ts: RecvTs::new(id),
        side,
        order_type: OrderType::Limit,
        time_in_force: tif,
        price: Some(price(price_raw)),
        qty: qty(qty_raw),
    })
}

fn replace(id: u64, account: u32, orig: u64, price_raw: u64, qty_raw: u64) -> OrderEntry {
    OrderEntry::CancelReplace(CancelReplace {
        client_order_id: OrderId::new(id).expect("order"),
        account_id: AccountId::new(account).expect("account"),
        orig_client_order_id: OrderId::new(orig).expect("orig"),
        client_ts: ClientTs::new(id),
        recv_ts: RecvTs::new(id),
        price: price(price_raw),
        qty: qty(qty_raw),
    })
}

fn cancel(id: u64, account: u32, orig: u64) -> OrderEntry {
    OrderEntry::CancelOrder(CancelOrder {
        client_order_id: OrderId::new(id).expect("order"),
        account_id: AccountId::new(account).expect("account"),
        orig_client_order_id: OrderId::new(orig).expect("orig"),
        client_ts: ClientTs::new(id),
        recv_ts: RecvTs::new(id),
    })
}

fn runtime_with_orders(orders: impl IntoIterator<Item = OrderEntry>, risk_config: RiskConfig) -> Option<(Runtime, TcpStream)> {
    let control = new_control()?;
    runtime_with_control_and_orders(control, orders, risk_config, engine_config())
}

fn runtime_without_gateway_client(risk_config: RiskConfig) -> Option<Runtime> {
    let control = new_control()?;
    let market_data = new_market_data()?;
    let gateway_addr = reserve_loopback_addr()?;
    let gateway = match GatewayRuntime::new(
        crate::gateway::GatewayConfig {
            io: IOConfig::Tcp {
                bind_addr: gateway_addr.to_string(),
                max_frame_len: 4096,
                max_pending_write_bytes: 1 << 20,
                record_path: None,
            },
            max_messages_per_poll: 1024,
        },
        instrument(),
    ) {
        Ok(gateway) => gateway,
        Err(crate::gateway::GatewayError::Io(crate::io::IoError::Bind(error))) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            return None;
        }
        Err(error) => panic!("gateway: {error:?}"),
    };
    Some(
        Runtime::new(
            Mode::default(),
            Yield::Noop,
            engine_config(),
            risk_config,
            control,
            gateway,
            market_data,
            MetricsRuntime::noop(),
        )
        .expect("runtime"),
    )
}

fn runtime_with_control_and_orders(
    control: ControlRuntime,
    orders: impl IntoIterator<Item = OrderEntry>,
    risk_config: RiskConfig,
    engine_config: EngineConfig,
) -> Option<(Runtime, TcpStream)> {
    let market_data = new_market_data()?;
    let gateway_addr = reserve_loopback_addr()?;
    let gateway_config = crate::gateway::GatewayConfig {
        io: IOConfig::Tcp {
            bind_addr: gateway_addr.to_string(),
            max_frame_len: 4096,
            max_pending_write_bytes: 1 << 20,
            record_path: None,
        },
        max_messages_per_poll: 1024,
    };
    let gateway = match GatewayRuntime::new(gateway_config, instrument()) {
        Ok(gateway) => gateway,
        Err(crate::gateway::GatewayError::Io(crate::io::IoError::Bind(error))) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            return None;
        }
        Err(error) => panic!("gateway: {error:?}"),
    };
    let mut client = TcpStream::connect(gateway_addr).expect("connect gateway");
    client.set_read_timeout(Some(Duration::from_secs(1))).expect("gateway timeout");
    write_gateway_orders(&mut client, orders);
    Some(
        Runtime::new(
            Mode::default(),
            Yield::Noop,
            engine_config,
            risk_config,
            control,
            gateway,
            market_data,
            MetricsRuntime::noop(),
        )
        .expect("runtime"),
    )
    .map(|runtime| (runtime, client))
}

fn control_config() -> ControlConfig {
    ControlConfig {
        heartbeat_timeout_ns: 0,
        max_messages_per_poll: 1024,
        io: IOConfig::Tcp {
            bind_addr: "127.0.0.1:0".to_owned(),
            max_frame_len: 4096,
            max_pending_write_bytes: 1 << 20,
            record_path: None,
        },
    }
}

fn new_market_data() -> Option<MarketDataRuntime> {
    match MarketDataRuntime::new(market_data_config()) {
        Ok(feed) => Some(feed),
        Err(crate::market_data::MarketDataError::Io(crate::io::IoError::Bind(error)))
            if error.kind() == std::io::ErrorKind::PermissionDenied =>
        {
            None
        }
        Err(error) => panic!("market-data feed: {error:?}"),
    }
}

fn market_data_config() -> MarketDataFeedConfig {
    MarketDataFeedConfig { io: IOConfig::Noop }
}

fn reserve_loopback_addr() -> Option<SocketAddr> {
    match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => Some(listener.local_addr().expect("addr")),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
        Err(error) => panic!("reserve control addr: {error:?}"),
    }
}

fn new_control() -> Option<ControlRuntime> {
    new_control_with_config(control_config())
}

fn new_control_with_config(config: ControlConfig) -> Option<ControlRuntime> {
    match ControlRuntime::new(config) {
        Ok(control) => Some(control),
        Err(crate::control::ControlError::Io(crate::io::IoError::Bind(error))) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            None
        }
        Err(error) => panic!("control: {error:?}"),
    }
}

fn temp_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("ignition-{name}-{}-{}.bin", std::process::id(), unique_suffix()))
}

fn unique_suffix() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_nanos()
}

fn runtime_with_control_client_and_orders(
    orders: impl IntoIterator<Item = OrderEntry>,
    risk_config: RiskConfig,
) -> Option<(Runtime, TcpStream, TcpStream)> {
    let bind_addr = reserve_loopback_addr()?;
    let control = new_control_with_config(ControlConfig {
        heartbeat_timeout_ns: 0,
        max_messages_per_poll: 1024,
        io: IOConfig::Tcp {
            bind_addr: bind_addr.to_string(),
            max_frame_len: 4096,
            max_pending_write_bytes: 1 << 20,
            record_path: None,
        },
    })?;
    let client = TcpStream::connect(bind_addr).expect("connect");
    client.set_read_timeout(Some(Duration::from_secs(1))).expect("timeout");
    let (runtime, gateway_client) = runtime_with_control_and_orders(control, orders, risk_config, engine_config())?;
    Some((runtime, client, gateway_client))
}

fn write_control(client: &mut TcpStream, frame: &[u8]) {
    client.write_all(frame).expect("write control");
}

fn write_gateway_orders(client: &mut TcpStream, orders: impl IntoIterator<Item = OrderEntry>) {
    for order in orders {
        client.write_all(&order_entry_frame(order)).expect("write gateway");
    }
}

fn order_entry_frame(entry: OrderEntry) -> Vec<u8> {
    match entry {
        OrderEntry::NewOrder(order) => {
            let mut bytes = vec![0_u8; SBE_HEADER_LEN + new_order_codec::SBE_BLOCK_LENGTH as usize];
            encode_header(&mut bytes, new_order_codec::SBE_BLOCK_LENGTH, MessageKind::NewOrder).expect("header");
            let write_buf = WriteBuf::new(&mut bytes);
            let mut encoder = new_order_codec::NewOrderEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
            encoder
                .client_order_id(order.client_order_id.raw())
                .account_id(order.account_id.raw())
                .client_ts(order.client_ts.raw())
                .side(to_wire_side(order.side))
                .order_type(match order.order_type {
                    OrderType::Limit => wire_order_type::OrderType::Limit,
                    OrderType::Market => wire_order_type::OrderType::Market,
                })
                .time_in_force(to_wire_tif(order.time_in_force))
                .price(order.price.map_or(0, Price::raw))
                .qty(order.qty.raw());
            bytes
        }
        OrderEntry::CancelOrder(cancel) => {
            let mut bytes = vec![0_u8; SBE_HEADER_LEN + cancel_order_codec::SBE_BLOCK_LENGTH as usize];
            encode_header(&mut bytes, cancel_order_codec::SBE_BLOCK_LENGTH, MessageKind::CancelOrder).expect("header");
            let write_buf = WriteBuf::new(&mut bytes);
            let mut encoder = cancel_order_codec::CancelOrderEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
            encoder
                .client_order_id(cancel.client_order_id.raw())
                .account_id(cancel.account_id.raw())
                .orig_client_order_id(cancel.orig_client_order_id.raw())
                .client_ts(cancel.client_ts.raw());
            bytes
        }
        OrderEntry::CancelReplace(replace) => {
            let mut bytes = vec![0_u8; SBE_HEADER_LEN + cancel_replace_codec::SBE_BLOCK_LENGTH as usize];
            encode_header(&mut bytes, cancel_replace_codec::SBE_BLOCK_LENGTH, MessageKind::CancelReplace).expect("header");
            let write_buf = WriteBuf::new(&mut bytes);
            let mut encoder = cancel_replace_codec::CancelReplaceEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
            encoder
                .client_order_id(replace.client_order_id.raw())
                .account_id(replace.account_id.raw())
                .orig_client_order_id(replace.orig_client_order_id.raw())
                .client_ts(replace.client_ts.raw())
                .price(replace.price.raw())
                .qty(replace.qty.raw());
            bytes
        }
        OrderEntry::MassCancel(cancel) => {
            let mut bytes = vec![0_u8; SBE_HEADER_LEN + mass_cancel_codec::SBE_BLOCK_LENGTH as usize];
            encode_header(&mut bytes, mass_cancel_codec::SBE_BLOCK_LENGTH, MessageKind::MassCancel).expect("header");
            let write_buf = WriteBuf::new(&mut bytes);
            let mut encoder = mass_cancel_codec::MassCancelEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
            encoder
                .client_order_id(cancel.client_order_id.raw())
                .account_id(cancel.account_id.raw())
                .client_ts(cancel.client_ts.raw());
            bytes
        }
    }
}

fn to_wire_side(side: Side) -> wire_side::Side {
    match side {
        Side::Buy => wire_side::Side::Buy,
        Side::Sell => wire_side::Side::Sell,
    }
}

fn to_wire_tif(tif: TimeInForce) -> wire_tif::TimeInForce {
    match tif {
        TimeInForce::Gtc => wire_tif::TimeInForce::Gtc,
        TimeInForce::Ioc => wire_tif::TimeInForce::Ioc,
        TimeInForce::PostOnly => wire_tif::TimeInForce::PostOnly,
    }
}

fn poll_until_control(runtime: &mut Runtime) {
    for _ in 0..100 {
        if runtime.poll_control().expect("poll control") {
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
    panic!("expected control request");
}

fn kill_switch(action: KillSwitchAction) -> Vec<u8> {
    let mut bytes = vec![0_u8; SBE_HEADER_LEN + kill_switch_codec::SBE_BLOCK_LENGTH as usize];
    encode_header(&mut bytes, kill_switch_codec::SBE_BLOCK_LENGTH, MessageKind::KillSwitch).expect("header");
    let write_buf = WriteBuf::new(&mut bytes);
    let mut encoder = kill_switch_codec::KillSwitchEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder.request_id(1).action(match action {
        KillSwitchAction::HaltTrading => kill_switch_action::KillSwitchAction::HaltTrading,
        KillSwitchAction::Shutdown => kill_switch_action::KillSwitchAction::Shutdown,
        KillSwitchAction::ResumeTrading => kill_switch_action::KillSwitchAction::ResumeTrading,
    });
    bytes
}

fn heartbeat(seq: u32) -> Vec<u8> {
    let mut bytes = vec![0_u8; SBE_HEADER_LEN + heartbeat_codec::SBE_BLOCK_LENGTH as usize];
    encode_header(&mut bytes, heartbeat_codec::SBE_BLOCK_LENGTH, MessageKind::Heartbeat).expect("header");
    let write_buf = WriteBuf::new(&mut bytes);
    let mut encoder = heartbeat_codec::HeartbeatEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder.heartbeat_seq(seq);
    bytes
}

fn snapshot(request_id: u32) -> Vec<u8> {
    let mut bytes = vec![0_u8; SBE_HEADER_LEN + snapshot_codec::SBE_BLOCK_LENGTH as usize];
    encode_header(&mut bytes, snapshot_codec::SBE_BLOCK_LENGTH, MessageKind::Snapshot).expect("header");
    let write_buf = WriteBuf::new(&mut bytes);
    let mut encoder = snapshot_codec::SnapshotEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder.request_id(request_id);
    bytes
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishedSnapshotEvent {
    Begin {
        bid_price: Option<Price>,
        bid_qty: u64,
        ask_price: Option<Price>,
        resting_order_count: u64,
        risk_reference_price: Price,
    },
    Order {
        row_index: u64,
        client_order_id: OrderId,
    },
    End {
        row_count: u64,
        result: ControlResult,
    },
}

fn read_control_frame(client: &mut TcpStream) -> Vec<u8> {
    let mut header = [0_u8; SBE_HEADER_LEN];
    client.read_exact(&mut header).expect("read control header");
    let frame_len = SBE_HEADER_LEN + u16::from_le_bytes([header[0], header[1]]) as usize;
    let mut frame = vec![0_u8; frame_len];
    frame[..SBE_HEADER_LEN].copy_from_slice(&header);
    client.read_exact(&mut frame[SBE_HEADER_LEN..]).expect("read control payload");
    frame
}

fn read_gateway_frame(client: &mut TcpStream) -> Vec<u8> {
    let mut header = [0_u8; SBE_HEADER_LEN];
    client.read_exact(&mut header).expect("read gateway header");
    let frame_len = SBE_HEADER_LEN + u16::from_le_bytes([header[0], header[1]]) as usize;
    let mut frame = vec![0_u8; frame_len];
    frame[..SBE_HEADER_LEN].copy_from_slice(&header);
    client.read_exact(&mut frame[SBE_HEADER_LEN..]).expect("read gateway payload");
    frame
}

fn read_execution_reports(client: &mut TcpStream, count: usize) -> Vec<ExecutionReport> {
    let mut reports = Vec::with_capacity(count);
    while reports.len() < count {
        let frame = read_gateway_frame(client);
        let header = decode_header(&frame).expect("gateway header");
        if header.kind != MessageKind::ExecutionReport {
            continue;
        }
        reports.push(decode_execution_report_frame(&frame));
    }
    reports
}

fn read_file_execution_reports(path: &Path) -> Vec<ExecutionReport> {
    let mut file = std::fs::File::open(path).expect("gateway output");
    let mut reports = Vec::new();
    loop {
        let mut len = [0_u8; 4];
        match file.read_exact(&mut len) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => panic!("read output len: {error:?}"),
        }
        let mut frame = vec![0_u8; u32::from_le_bytes(len) as usize];
        file.read_exact(&mut frame).expect("read output frame");
        if decode_header(&frame).expect("gateway header").kind == MessageKind::ExecutionReport {
            reports.push(decode_execution_report_frame(&frame));
        }
    }
    reports
}

fn decode_execution_report_frame(frame: &[u8]) -> ExecutionReport {
    let decoder = execution_report_codec::ExecutionReportDecoder::default().wrap(
        ReadBuf::new(frame),
        SBE_HEADER_LEN,
        execution_report_codec::SBE_BLOCK_LENGTH,
        SCHEMA_VERSION,
    );
    ExecutionReport {
        engine_seq: crate::domain::EngineSeq::new(decoder.engine_seq()),
        client_order_id: OrderId::new(decoder.client_order_id()).expect("order"),
        account_id: AccountId::new(decoder.account_id()).expect("account"),
        exec_type: match decoder.exec_type() {
            crate::protocol::generated::codecs::exec_type::ExecType::New => ExecType::New,
            crate::protocol::generated::codecs::exec_type::ExecType::Trade => ExecType::Trade,
            crate::protocol::generated::codecs::exec_type::ExecType::Canceled => ExecType::Canceled,
            crate::protocol::generated::codecs::exec_type::ExecType::Replaced => ExecType::Replaced,
            crate::protocol::generated::codecs::exec_type::ExecType::Rejected => ExecType::Rejected,
            crate::protocol::generated::codecs::exec_type::ExecType::Expired => ExecType::Expired,
            crate::protocol::generated::codecs::exec_type::ExecType::NullVal => panic!("null exec type"),
        },
        ord_status: match decoder.ord_status() {
            crate::protocol::generated::codecs::ord_status::OrdStatus::New => OrdStatus::New,
            crate::protocol::generated::codecs::ord_status::OrdStatus::PartiallyFilled => OrdStatus::PartiallyFilled,
            crate::protocol::generated::codecs::ord_status::OrdStatus::Filled => OrdStatus::Filled,
            crate::protocol::generated::codecs::ord_status::OrdStatus::Canceled => OrdStatus::Canceled,
            crate::protocol::generated::codecs::ord_status::OrdStatus::Replaced => OrdStatus::Replaced,
            crate::protocol::generated::codecs::ord_status::OrdStatus::Rejected => OrdStatus::Rejected,
            crate::protocol::generated::codecs::ord_status::OrdStatus::Expired => OrdStatus::Expired,
            crate::protocol::generated::codecs::ord_status::OrdStatus::NullVal => panic!("null order status"),
        },
        reject_reason: match decoder.reject_reason() {
            wire_reject_reason::RejectReason::None => RejectReason::None,
            wire_reject_reason::RejectReason::Malformed => RejectReason::Malformed,
            wire_reject_reason::RejectReason::UnknownOrder => RejectReason::UnknownOrder,
            wire_reject_reason::RejectReason::InvalidPrice => RejectReason::InvalidPrice,
            wire_reject_reason::RejectReason::InvalidQuantity => RejectReason::InvalidQuantity,
            wire_reject_reason::RejectReason::RiskOpenOrderLimit => RejectReason::RiskOpenOrderLimit,
            wire_reject_reason::RejectReason::RiskNotionalLimit => RejectReason::RiskNotionalLimit,
            wire_reject_reason::RejectReason::PriceBand => RejectReason::PriceBand,
            wire_reject_reason::RejectReason::SelfTradePrevention => RejectReason::SelfTradePrevention,
            wire_reject_reason::RejectReason::KillSwitchActive => RejectReason::KillSwitchActive,
            wire_reject_reason::RejectReason::HeartbeatLost => RejectReason::HeartbeatLost,
            wire_reject_reason::RejectReason::NullVal => panic!("null reject reason"),
        },
        fill_price: if decoder.fill_price() == 0 {
            None
        } else {
            Some(price(decoder.fill_price()))
        },
        fill_qty: decoder.fill_qty(),
        remaining_qty: decoder.remaining_qty(),
    }
}

fn assert_no_control_frame(client: &mut TcpStream) {
    let mut header = [0_u8; SBE_HEADER_LEN];
    let error = client.read_exact(&mut header).expect_err("unexpected control frame");
    assert!(
        matches!(error.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut),
        "unexpected read error: {error:?}"
    );
}

fn optional_price(raw: u64) -> Option<Price> {
    (raw != 0).then(|| price(raw))
}

fn read_snapshot_events(client: &mut TcpStream, count: usize) -> Vec<PublishedSnapshotEvent> {
    let mut events = Vec::with_capacity(count);
    for _ in 0..count {
        let frame = read_control_frame(client);
        let header = decode_header(&frame).expect("control header");
        let read_buf = ReadBuf::new(&frame);
        match header.kind {
            MessageKind::SnapshotBegin => {
                let decoder = snapshot_begin_codec::SnapshotBeginDecoder::default().wrap(
                    read_buf,
                    SBE_HEADER_LEN,
                    snapshot_begin_codec::SBE_BLOCK_LENGTH,
                    SCHEMA_VERSION,
                );
                events.push(PublishedSnapshotEvent::Begin {
                    bid_price: optional_price(decoder.bid_price()),
                    bid_qty: decoder.bid_qty(),
                    ask_price: optional_price(decoder.ask_price()),
                    resting_order_count: decoder.resting_order_count(),
                    risk_reference_price: price(decoder.risk_reference_price()),
                });
            }
            MessageKind::SnapshotOrder => {
                let decoder = snapshot_order_codec::SnapshotOrderDecoder::default().wrap(
                    read_buf,
                    SBE_HEADER_LEN,
                    snapshot_order_codec::SBE_BLOCK_LENGTH,
                    SCHEMA_VERSION,
                );
                assert_ne!(decoder.side(), wire_side::Side::NullVal);
                events.push(PublishedSnapshotEvent::Order {
                    row_index: decoder.row_index(),
                    client_order_id: OrderId::new(decoder.client_order_id()).expect("order"),
                });
            }
            MessageKind::SnapshotEnd => {
                let decoder = snapshot_end_codec::SnapshotEndDecoder::default().wrap(
                    read_buf,
                    SBE_HEADER_LEN,
                    snapshot_end_codec::SBE_BLOCK_LENGTH,
                    SCHEMA_VERSION,
                );
                assert_eq!(decoder.reason(), wire_reject_reason::RejectReason::None);
                events.push(PublishedSnapshotEvent::End {
                    row_count: decoder.row_count(),
                    result: match decoder.result() {
                        wire_control_result::ControlResult::Ok => ControlResult::Ok,
                        wire_control_result::ControlResult::Rejected => ControlResult::Rejected,
                        wire_control_result::ControlResult::NullVal => {
                            panic!("null snapshot result")
                        }
                    },
                });
            }
            kind => panic!("unexpected control event: {kind:?}"),
        }
    }
    events
}

#[test]
fn poll_gateway_processes_orders_fifo_through_engine() {
    let Some((mut runtime, mut gateway_client)) = runtime_with_orders(
        [
            new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
            new_order(2, 2, Side::Buy, TimeInForce::Gtc, 1_001, 5),
        ],
        risk_config(10, 1_000_000),
    ) else {
        return;
    };

    let made_progress = runtime.poll_gateway().expect("poll gateway");

    assert!(made_progress);
    assert_eq!(runtime.engine.best_bid(), Ok(Some((price(1_001), 5))));
    let reports = read_execution_reports(&mut gateway_client, 2);
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0].client_order_id.raw(), 1);
    assert_eq!(reports[1].client_order_id.raw(), 2);
}

#[test]
fn risk_reject_uses_shared_engine_sequence_and_publishes() {
    let Some((mut runtime, mut gateway_client)) = runtime_with_orders(
        [
            new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
            new_order(2, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
        ],
        risk_config(1, 1_000_000),
    ) else {
        return;
    };

    let made_progress = runtime.poll_gateway().expect("poll gateway");
    let reports = read_execution_reports(&mut gateway_client, 2);

    assert!(made_progress);
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0].engine_seq.raw(), 3);
    assert_eq!(reports[1].engine_seq.raw(), 4);
    assert_eq!(reports[1].reject_reason, RejectReason::RiskOpenOrderLimit);
    assert_eq!(runtime.engine.resting_order_count(), 1);
}

#[test]
fn risk_reject_does_not_mutate_book() {
    let Some((mut runtime, mut gateway_client)) =
        runtime_with_orders([new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10)], risk_config(10, 9_999))
    else {
        return;
    };

    let made_progress = runtime.poll_gateway().expect("poll gateway");

    assert!(made_progress);
    assert_eq!(runtime.engine.resting_order_count(), 0);
    assert_eq!(runtime.engine.account_open_notional(AccountId::new(1).expect("account")), Some(0));
    assert_eq!(
        read_execution_reports(&mut gateway_client, 1)[0].reject_reason,
        RejectReason::RiskNotionalLimit
    );
}

#[test]
fn duplicate_order_id_reaches_engine_structural_reject() {
    let Some((mut runtime, mut gateway_client)) = runtime_with_orders(
        [
            new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
            new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
        ],
        risk_config(10, 1_000_000),
    ) else {
        return;
    };

    let made_progress = runtime.poll_gateway().expect("poll gateway");
    let reports = read_execution_reports(&mut gateway_client, 2);

    assert!(made_progress);
    assert_eq!(reports.len(), 2);
    assert_eq!(reports[1].client_order_id.raw(), 1);
    assert_eq!(reports[1].reject_reason, RejectReason::Malformed);
    assert_eq!(runtime.engine.resting_order_count(), 1);
}

#[test]
fn binary_ingress_accepts_same_client_order_id_for_different_accounts() {
    let Some((mut runtime, mut gateway_client)) = runtime_with_orders(
        [
            new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
            new_order(1, 2, Side::Buy, TimeInForce::Gtc, 999, 10),
        ],
        risk_config(10, 1_000_000),
    ) else {
        return;
    };

    let made_progress = runtime.poll_gateway().expect("poll gateway");
    let reports = read_execution_reports(&mut gateway_client, 2);

    assert!(made_progress);
    assert_eq!(
        reports.iter().map(|report| report.client_order_id.raw()).collect::<Vec<_>>(),
        vec![1, 1]
    );
    assert_eq!(reports.iter().map(|report| report.account_id.raw()).collect::<Vec<_>>(), vec![1, 2]);
    assert!(reports.iter().all(|report| report.reject_reason == RejectReason::None));
    assert_eq!(runtime.engine.resting_order_count(), 2);
}

#[test]
fn unknown_replace_reaches_engine_unknown_order_reject() {
    let Some((mut runtime, mut gateway_client)) = runtime_with_orders([replace(2, 1, 404, 1_000, 15)], risk_config(10, 1_000_000)) else {
        return;
    };

    let made_progress = runtime.poll_gateway().expect("poll gateway");
    let reports = read_execution_reports(&mut gateway_client, 1);

    assert!(made_progress);
    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].client_order_id.raw(), 2);
    assert_eq!(reports[0].reject_reason, RejectReason::UnknownOrder);
    assert_eq!(runtime.engine.resting_order_count(), 0);
}

#[test]
fn poll_gateway_processes_gateway_without_closing_live_control() {
    let Some((mut runtime, mut gateway_client)) = runtime_with_orders(
        [new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10)],
        risk_config(10, 1_000_000),
    ) else {
        return;
    };

    let made_progress = runtime.poll_gateway().expect("poll gateway");

    assert!(made_progress);
    assert_eq!(runtime.engine.resting_order_count(), 1);
    let _ = read_execution_reports(&mut gateway_client, 1);
}

#[test]
fn empty_live_runtime_phases_are_open_without_progress() {
    let Some(mut runtime) = runtime_without_gateway_client(risk_config(10, 1_000_000)) else {
        return;
    };

    let control_progress = runtime.poll_control().expect("poll control");
    let gateway_progress = runtime.poll_gateway().expect("poll gateway");

    assert!(!control_progress);
    assert!(!gateway_progress);
}

#[test]
fn empty_replay_with_file_market_data_output_drains_without_progress() {
    let market_data_path = temp_path("replay-market-data-output");
    let control = ControlRuntime::new(ControlConfig {
        heartbeat_timeout_ns: 0,
        max_messages_per_poll: 1024,
        io: IOConfig::File {
            input_path: None,
            output_path: None,
            max_frame_len: 4096,
        },
    })
    .expect("control");
    let gateway = GatewayRuntime::new(
        crate::gateway::GatewayConfig {
            io: IOConfig::File {
                input_path: None,
                output_path: None,
                max_frame_len: 4096,
            },
            max_messages_per_poll: 1024,
        },
        instrument(),
    )
    .expect("gateway");
    let market_data = MarketDataRuntime::new(MarketDataFeedConfig {
        io: IOConfig::File {
            input_path: None,
            output_path: Some(market_data_path.clone()),
            max_frame_len: 4096,
        },
    })
    .expect("market data");
    let mut runtime = Runtime::new(
        Mode::Replay { expected_input_seq: 1 },
        Yield::Noop,
        engine_config(),
        risk_config(10, 1_000_000),
        control,
        gateway,
        market_data,
        MetricsRuntime::noop(),
    )
    .expect("runtime");

    let control_progress = runtime.poll_control().expect("poll control");
    let gateway_progress = runtime.poll_gateway().expect("poll gateway");

    assert!(!control_progress);
    assert!(!gateway_progress);
    assert!(!runtime.handle_idle_iteration(control_progress, gateway_progress).expect("idle"));

    let _ = std::fs::remove_file(market_data_path);
}

#[test]
fn halt_trading_rejects_new_and_replace_before_risk() {
    let Some((mut runtime, mut client, mut gateway_client)) = runtime_with_control_client_and_orders(
        [new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10), replace(2, 1, 1, 1_001, 10)],
        risk_config(10, 1_000_000),
    ) else {
        return;
    };
    write_control(&mut client, &kill_switch(KillSwitchAction::HaltTrading));

    poll_until_control(&mut runtime);
    runtime.poll_gateway().expect("poll gateway");
    let reports = read_execution_reports(&mut gateway_client, 2);

    assert_eq!(reports.len(), 2);
    assert_eq!(runtime.engine.resting_order_count(), 0);
    assert!(reports.iter().all(|report| report.reject_reason == RejectReason::KillSwitchActive));
}

#[test]
fn halt_trading_allows_cancel_flow_through_engine() {
    let Some((mut runtime, mut client, mut gateway_client)) = runtime_with_control_client_and_orders(
        [new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10), cancel(2, 1, 1)],
        risk_config(10, 1_000_000),
    ) else {
        return;
    };
    write_control(&mut client, &kill_switch(KillSwitchAction::HaltTrading));

    poll_until_control(&mut runtime);
    runtime.poll_gateway().expect("poll gateway");
    let reports = read_execution_reports(&mut gateway_client, 2);

    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0].reject_reason, RejectReason::KillSwitchActive);
    assert_eq!(reports[1].reject_reason, RejectReason::UnknownOrder);
    assert_eq!(runtime.engine.resting_order_count(), 0);
}

#[test]
fn live_heartbeat_timeout_ignores_endpoint_construction_skew() {
    let Some(gateway_addr) = reserve_loopback_addr() else {
        return;
    };
    let Some(control_addr) = reserve_loopback_addr() else {
        return;
    };
    let gateway = match GatewayRuntime::new(
        crate::gateway::GatewayConfig {
            io: IOConfig::Tcp {
                bind_addr: gateway_addr.to_string(),
                max_frame_len: 4096,
                max_pending_write_bytes: 1 << 20,
                record_path: None,
            },
            max_messages_per_poll: 1024,
        },
        instrument(),
    ) {
        Ok(gateway) => gateway,
        Err(crate::gateway::GatewayError::Io(crate::io::IoError::Bind(error))) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            return;
        }
        Err(error) => panic!("gateway: {error:?}"),
    };

    // Separate endpoint-local origins would turn this construction delay
    // into apparent heartbeat age on the gateway timestamp.
    thread::sleep(Duration::from_millis(200));

    let control = new_control_with_config(ControlConfig {
        heartbeat_timeout_ns: 100_000_000,
        max_messages_per_poll: 1024,
        io: IOConfig::Tcp {
            bind_addr: control_addr.to_string(),
            max_frame_len: 4096,
            max_pending_write_bytes: 1 << 20,
            record_path: None,
        },
    })
    .expect("control");
    let mut control_client = TcpStream::connect(control_addr).expect("connect control");
    let mut gateway_client = TcpStream::connect(gateway_addr).expect("connect gateway");
    gateway_client
        .set_read_timeout(Some(Duration::from_secs(1)))
        .expect("gateway timeout");
    write_control(&mut control_client, &heartbeat(1));
    write_gateway_orders(&mut gateway_client, [new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10)]);

    let mut runtime = Runtime::new(
        Mode::default(),
        Yield::Noop,
        engine_config(),
        risk_config(10, 1_000_000),
        control,
        gateway,
        new_market_data().expect("market data"),
        MetricsRuntime::noop(),
    )
    .expect("runtime");
    poll_until_control(&mut runtime);
    runtime.poll_gateway().expect("poll gateway");
    let reports = read_execution_reports(&mut gateway_client, 1);

    assert_eq!(reports[0].reject_reason, RejectReason::None);
    assert_eq!(runtime.engine.resting_order_count(), 1);
}

#[test]
fn heartbeat_timeout_starts_after_first_heartbeat() {
    let gateway_input = temp_path("heartbeat-gateway-input");
    let gateway_output = temp_path("heartbeat-gateway-output");
    {
        let mut input = FileFrameIo::open(None, Some(gateway_input.clone()), 4096).expect("gateway input writer");
        input
            .record_input(&InputRecord {
                input_seq: 2,
                endpoint: Endpoint::Gateway,
                recv_ts: RecvTs::new(1_000_000_000),
                frame: order_entry_frame(new_order(1_000_000_000, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10)),
            })
            .expect("gateway record");
        input.flush_pending().expect("flush gateway input");
    }

    let control = ControlRuntime::new(ControlConfig {
        heartbeat_timeout_ns: 10,
        max_messages_per_poll: 1024,
        io: IOConfig::InMemory {
            input: vec![InputRecord {
                input_seq: 1,
                endpoint: Endpoint::Control,
                recv_ts: RecvTs::new(1),
                frame: heartbeat(1),
            }],
            max_frame_len: 4096,
        },
    })
    .expect("control");
    let gateway = GatewayRuntime::new(
        crate::gateway::GatewayConfig {
            io: IOConfig::File {
                input_path: Some(gateway_input.clone()),
                output_path: Some(gateway_output.clone()),
                max_frame_len: 4096,
            },
            max_messages_per_poll: 1024,
        },
        instrument(),
    )
    .expect("gateway");
    let mut runtime = Runtime::new(
        Mode::Replay { expected_input_seq: 1 },
        Yield::Noop,
        engine_config(),
        risk_config(10, 1_000_000),
        control,
        gateway,
        new_market_data().expect("market data"),
        MetricsRuntime::noop(),
    )
    .expect("runtime");

    assert!(runtime.poll_control().expect("poll control"));
    runtime.poll_gateway().expect("poll gateway");
    runtime.gateway.flush_pending().expect("flush gateway");
    let reports = read_file_execution_reports(&gateway_output);

    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].reject_reason, RejectReason::HeartbeatLost);
    assert_eq!(runtime.engine.resting_order_count(), 0);

    let _ = std::fs::remove_file(gateway_input);
    let _ = std::fs::remove_file(gateway_output);
}

#[test]
fn snapshot_emits_price_time_rows_and_state_metadata() {
    let Some((mut runtime, mut client, mut gateway_client)) = runtime_with_control_client_and_orders(
        [
            new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
            new_order(2, 2, Side::Buy, TimeInForce::Gtc, 1_001, 5),
            new_order(3, 3, Side::Buy, TimeInForce::Gtc, 1_001, 7),
            new_order(4, 4, Side::Sell, TimeInForce::Gtc, 1_100, 9),
        ],
        RiskConfig::new(10, 10_000_000, price(1_000), 2_000).expect("risk config"),
    ) else {
        return;
    };
    runtime.poll_gateway().expect("seed book");
    let _ = read_execution_reports(&mut gateway_client, 4);
    write_control(&mut client, &snapshot(99));

    poll_until_control(&mut runtime);
    let events = read_snapshot_events(&mut client, 6);

    assert_eq!(events.len(), 6);
    let PublishedSnapshotEvent::Begin {
        bid_price,
        bid_qty,
        ask_price,
        resting_order_count,
        risk_reference_price,
    } = events[0]
    else {
        panic!("begin");
    };
    assert_eq!(bid_price, Some(price(1_001)));
    assert_eq!(bid_qty, 12);
    assert_eq!(ask_price, Some(price(1_100)));
    assert_eq!(resting_order_count, 4);
    assert_eq!(risk_reference_price, price(1_000));

    let rows: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            PublishedSnapshotEvent::Order {
                row_index,
                client_order_id,
            } => Some((*row_index, *client_order_id)),
            _ => None,
        })
        .collect();
    assert_eq!(
        rows.iter().map(|(_, client_order_id)| client_order_id.raw()).collect::<Vec<_>>(),
        vec![2, 3, 1, 4]
    );
    assert_eq!(rows[0].0, 0);
    assert_eq!(rows[1].0, 1);

    let PublishedSnapshotEvent::End { row_count, result } = events[5] else {
        panic!("end");
    };
    assert_eq!(row_count, 4);
    assert_eq!(result, ControlResult::Ok);
}

#[test]
fn shutdown_drains_book_and_closes_runtime() {
    let Some((mut runtime, mut client, mut gateway_client)) = runtime_with_control_client_and_orders(
        [new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10)],
        risk_config(10, 1_000_000),
    ) else {
        return;
    };
    runtime.poll_gateway().expect("seed book");
    write_control(&mut client, &kill_switch(KillSwitchAction::Shutdown));

    poll_until_control(&mut runtime);
    let reports = read_execution_reports(&mut gateway_client, 2);

    assert!(runtime.terminal);
    assert_eq!(runtime.engine.resting_order_count(), 0);
    assert_eq!(reports.last().expect("drain cancel").reject_reason, RejectReason::None);
    assert_eq!(reports.last().expect("drain cancel").ord_status, OrdStatus::Canceled);
}

#[test]
fn capacity_pressure_rejects_trigger_drains_and_stops_buffered_ingress() {
    let Some(control) = new_control() else {
        return;
    };
    let capacity_config = EngineConfig::new(instrument(), 100, 10_000, 1024, 1, 100, 1).expect("capacity config");
    let Some((mut runtime, mut gateway_client)) = runtime_with_control_and_orders(
        control,
        [
            new_order(1, 1, Side::Buy, TimeInForce::Gtc, 1_000, 10),
            new_order(2, 2, Side::Buy, TimeInForce::Gtc, 1_001, 10),
            cancel(3, 1, 1),
        ],
        risk_config(10, 1_000_000),
        capacity_config,
    ) else {
        return;
    };

    runtime.poll_gateway().expect("capacity shutdown");
    let reports = read_execution_reports(&mut gateway_client, 3);

    assert!(runtime.terminal);
    assert_eq!(runtime.engine.resting_order_count(), 0);
    assert_eq!(runtime.input, Mode::Live { next_input_seq: 3 });
    assert_eq!(
        reports.iter().map(|report| report.client_order_id.raw()).collect::<Vec<_>>(),
        vec![1, 2, 1]
    );
    assert_eq!(reports[1].exec_type, ExecType::Rejected);
    assert_eq!(reports[1].ord_status, OrdStatus::Rejected);
    assert_eq!(reports[1].reject_reason, RejectReason::KillSwitchActive);
    assert_eq!(reports[2].exec_type, ExecType::Canceled);
    assert!(reports.windows(2).all(|pair| pair[0].engine_seq.raw() < pair[1].engine_seq.raw()));
}

#[test]
fn shutdown_stops_same_poll_control_processing() {
    let Some((mut runtime, mut client, _gateway_client)) = runtime_with_control_client_and_orders([], risk_config(10, 1_000_000)) else {
        return;
    };
    let mut frames = kill_switch(KillSwitchAction::Shutdown);
    frames.extend_from_slice(&snapshot(99));
    write_control(&mut client, &frames);

    poll_until_control(&mut runtime);
    let frame = read_control_frame(&mut client);

    assert!(runtime.terminal);
    assert_eq!(decode_header(&frame).expect("control header").kind, MessageKind::ControlResponse);
    assert_no_control_frame(&mut client);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// Writes a generated batch of gateway orders, then delivers a halt
    /// control request before polling the gateway. The property checks that
    /// runtime orchestration observes control before matching: every
    /// risk-increasing order is rejected with KillSwitchActive, nothing
    /// rests on the book, and execution report sequences still increase.
    #[test]
    fn property_halt_control_is_observed_before_generated_gateway_batch(qtys in prop::collection::vec(1_u64..50, 1..8)) {
        let orders = qtys
            .iter()
            .enumerate()
            .map(|(index, qty_raw)| new_order(index as u64 + 1, index as u32 + 1, Side::Buy, TimeInForce::Gtc, 1_000, *qty_raw))
            .collect::<Vec<_>>();
        let Some((mut runtime, mut client, mut gateway_client)) =
            runtime_with_control_client_and_orders(orders, risk_config(100, 1_000_000))
        else {
            return Ok(());
        };

        write_control(&mut client, &kill_switch(KillSwitchAction::HaltTrading));
        poll_until_control(&mut runtime);
        runtime.poll_gateway().expect("poll gateway");
        let reports = read_execution_reports(&mut gateway_client, qtys.len());

        prop_assert_eq!(runtime.engine.resting_order_count(), 0);
        prop_assert!(reports.iter().all(|report| report.reject_reason == RejectReason::KillSwitchActive));
        prop_assert!(reports.windows(2).all(|pair| pair[0].engine_seq.raw() < pair[1].engine_seq.raw()));
    }

    /// Replays an already-serialized gateway stream that represents
    /// interleaved submitters. The property checks that same-price maker
    /// priority follows runtime ingress order, not client timestamps,
    /// account IDs, receive timestamps, or client order IDs.
    #[test]
    fn property_serialized_gateway_ingress_defines_same_price_priority(
        qtys in prop::collection::vec(1_u64..25, 2..8),
        lane_count in 2_u32..5,
        lane_offset in 0_u32..5,
    ) {
        let gateway_input = temp_path("serialized-ingress-gateway-input");
        let gateway_output = temp_path("serialized-ingress-gateway-output");
        let mut expected_maker_ids = Vec::with_capacity(qtys.len());
        {
            let mut input = FileFrameIo::open(None, Some(gateway_input.clone()), 4096).expect("gateway input writer");
            for (index, qty_raw) in qtys.iter().copied().enumerate() {
                let maker_id = 10_000 - index as u64;
                expected_maker_ids.push(maker_id);
                let account = ((index as u32 + lane_offset) % lane_count) + 1;
                input
                    .record_input(&InputRecord {
                        input_seq: index as u64 + 1,
                        endpoint: Endpoint::Gateway,
                        recv_ts: RecvTs::new(1_000_000 - index as u64),
                        frame: order_entry_frame(new_order(
                            maker_id,
                            account,
                            Side::Sell,
                            TimeInForce::Gtc,
                            1_000,
                            qty_raw,
                        )),
                    })
                    .expect("maker record");
            }
            input
                .record_input(&InputRecord {
                    input_seq: qtys.len() as u64 + 1,
                    endpoint: Endpoint::Gateway,
                    recv_ts: RecvTs::new(1),
                    frame: order_entry_frame(new_order(
                        20_000,
                        99,
                        Side::Buy,
                        TimeInForce::Ioc,
                        1_000,
                        qtys.iter().sum(),
                    )),
                })
                .expect("taker record");
            input.flush_pending().expect("flush gateway input");
        }

        let control = ControlRuntime::new(ControlConfig {
            heartbeat_timeout_ns: 0,
            max_messages_per_poll: 1024,
            io: IOConfig::Noop,
        })
        .expect("control");
        let gateway = GatewayRuntime::new(
            crate::gateway::GatewayConfig {
                io: IOConfig::File {
                    input_path: Some(gateway_input.clone()),
                    output_path: Some(gateway_output.clone()),
                    max_frame_len: 4096,
                },
                max_messages_per_poll: 1024,
            },
            instrument(),
        )
        .expect("gateway");
        let mut runtime = Runtime::new(
            Mode::Replay { expected_input_seq: 1 },
            Yield::Noop,
            engine_config(),
            risk_config(100, 10_000_000),
            control,
            gateway,
            new_market_data().expect("market data"),
            MetricsRuntime::noop(),
        )
        .expect("runtime");

        runtime.run().expect("runtime");
        runtime.gateway.flush_pending().expect("flush gateway");
        let reports = read_file_execution_reports(&gateway_output);
        let maker_trade_ids = reports
            .iter()
            .filter(|report| report.exec_type == ExecType::Trade && report.client_order_id.raw() != 20_000)
            .map(|report| report.client_order_id.raw())
            .collect::<Vec<_>>();

        prop_assert_eq!(maker_trade_ids, expected_maker_ids);
        prop_assert!(reports.windows(2).all(|pair| pair[0].engine_seq.raw() < pair[1].engine_seq.raw()));

        let _ = std::fs::remove_file(gateway_input);
        let _ = std::fs::remove_file(gateway_output);
    }
}
