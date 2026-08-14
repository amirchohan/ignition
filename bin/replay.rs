//! Replay entry point.

use ignition::{
    config::{RuntimeModeConfig, ServerConfig},
    runtime::{Mode, Runtime, RuntimeBuildError},
};
use std::{
    env,
    error::Error,
    fs, io,
    path::{Path, PathBuf},
};
use tracing::info;
use tracing_subscriber::EnvFilter;

type ReplayResult<T> = Result<T, Box<dyn Error>>;

/// Start tracing, parse CLI arguments, and execute the selected replay command.
fn main() {
    init_tracing();
    match run(env::args().skip(1)) {
        Ok(()) => {}
        Err(error) => {
            tracing::error!("replay failed: {error}");
            std::process::exit(1);
        }
    }
}

/// Initialise process-wide tracing from `RUST_LOG`, defaulting to info logs.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

/// Parse CLI arguments, prepare replay config, and run the runtime.
fn run(args: impl Iterator<Item = String>) -> ReplayResult<()> {
    let args = parse_args(args)?;

    // `--run` keeps the operator-facing config stable and derives file
    // inputs/outputs from mode.record_path_dir plus the requested id.
    let mut config = ServerConfig::load(&args.config_path).map_err(RuntimeBuildError::Config)?;
    prepare_config_for_replay_mode(&mut config, &args.input_id)?;
    let mode = runtime_mode_for_replay(&config)?;

    // Runtime::from_server_config is shared with live startup; replay only
    // changes file IO paths before this point.
    let mut runtime = Runtime::from_server_config(mode, config)?;
    runtime
        .run()
        .map_err(|error| io::Error::other(format!("replay runtime failed: {error:?}")))?;
    Ok(())
}

/// Resolve replay-mode sequencing state from config.
fn runtime_mode_for_replay(config: &ServerConfig) -> ReplayResult<Mode> {
    match &config.mode {
        RuntimeModeConfig::Replay { expected_input_seq, .. } if *expected_input_seq != 0 => Ok(Mode::Replay {
            expected_input_seq: *expected_input_seq,
        }),
        RuntimeModeConfig::Replay { expected_input_seq, .. } => {
            Err(io::Error::other(format!("invalid runtime mode sequence expected_input_seq={expected_input_seq}")).into())
        }
        RuntimeModeConfig::Live { .. } => Err(io::Error::other("--run requires config mode.type: replay").into()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReplayArgs {
    input_id: String,
    config_path: PathBuf,
}

/// Parse replay CLI arguments.
fn parse_args(args: impl Iterator<Item = String>) -> ReplayResult<ReplayArgs> {
    let mut config_path = PathBuf::from("configs/replay.yaml");
    let mut input_id = None;
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Err(io::Error::other(usage()).into()),
            "--config" => config_path = PathBuf::from(next_arg(&mut args, "--config")?),
            "--run" => input_id = Some(next_arg(&mut args, "--run")?),
            _ => return Err(io::Error::other(format!("unknown argument {arg}\n{}", usage())).into()),
        }
    }

    Ok(ReplayArgs {
        input_id: input_id.ok_or_else(|| io::Error::other(format!("missing value for --run\n{}", usage())))?,
        config_path,
    })
}

/// Read the next CLI argument.
fn next_arg(args: &mut impl Iterator<Item = String>, name: &'static str) -> ReplayResult<String> {
    args.next()
        .ok_or_else(|| io::Error::other(format!("missing value for {name}\n{}", usage())).into())
}

/// Prepare replay-mode runtime config for one recorded input id.
fn prepare_config_for_replay_mode(config: &mut ServerConfig, input_id: &str) -> ReplayResult<()> {
    if input_id.is_empty() || !input_id.as_bytes().iter().all(u8::is_ascii_digit) {
        return Err(io::Error::other(format!("invalid replay input_id: {input_id}")).into());
    }
    let record_path_dir = match &config.mode {
        RuntimeModeConfig::Replay { record_path_dir, .. } => record_path_dir.clone(),
        RuntimeModeConfig::Live { .. } => return Err(io::Error::other("--run requires config mode.type: replay").into()),
    };

    let run_dir = record_path_dir.join(input_id);
    let input_dir = run_dir.join("input");
    let output_root = run_dir.join("output");
    ensure_input_exists(&input_dir)?;
    let (output_id, output_dir) = create_next_output_dir(&output_root)?;

    config.mode = RuntimeModeConfig::Replay {
        expected_input_seq: 1,
        record_path_dir,
    };
    let control_input = input_dir.join("control.bin");
    let gateway_input = input_dir.join("gateway.bin");
    config.control.set_file_io(
        control_input.is_file().then_some(control_input),
        Some(output_dir.join("control.bin")),
    );
    config.gateway.set_file_io(
        gateway_input.is_file().then_some(gateway_input),
        Some(output_dir.join("gateway.bin")),
    );
    config.market_data.set_file_output(Some(output_dir.join("market_data.bin")));

    info!("replay input_id={input_id}");
    info!("output_id={output_id}");
    info!("output: {}", output_dir.display());
    Ok(())
}

/// Ensure this recorded run has at least one endpoint input file.
fn ensure_input_exists(input_dir: &Path) -> ReplayResult<()> {
    if !input_dir.is_dir() {
        return Err(io::Error::other(format!("missing replay input directory: {}", input_dir.display())).into());
    }
    if !input_dir.join("control.bin").is_file() && !input_dir.join("gateway.bin").is_file() {
        return Err(io::Error::other(format!(
            "missing control.bin or gateway.bin in replay input directory: {}",
            input_dir.display()
        ))
        .into());
    }
    Ok(())
}

/// Create the next numbered output directory.
fn create_next_output_dir(output_root: &Path) -> ReplayResult<(String, PathBuf)> {
    fs::create_dir_all(output_root)?;

    let mut max_id = 0_u32;
    for entry in fs::read_dir(output_root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.len() == 4
            && name.as_bytes().iter().all(u8::is_ascii_digit)
            && let Ok(id) = name.parse::<u32>()
        {
            max_id = max_id.max(id);
        }
    }

    let output_id = format!("{:04}", max_id + 1);
    let output_dir = output_root.join(&output_id);
    fs::create_dir(&output_dir)?;
    Ok((output_id, output_dir))
}

/// Return replay CLI usage text.
fn usage() -> &'static str {
    "usage: replay --run INPUT_ID [--config PATH]"
}

#[cfg(test)]
mod tests {
    use super::*;
    use ignition::{
        domain::{RecvTs, trading::order::Side},
        io::{Endpoint, FileFrameIo, InputRecord},
        protocol::{
            MessageKind, SBE_HEADER_LEN, encode_header,
            generated::codecs::{
                WriteBuf, cancel_order_codec, mass_cancel_codec, new_order_codec, order_type as wire_order_type, side as wire_side,
                time_in_force as wire_tif,
            },
        },
    };

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ignition-replay-{name}-{}-{}.bin", std::process::id(), unique_suffix()))
    }

    fn unique_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    }

    fn sample_records() -> Vec<InputRecord> {
        vec![
            record(
                1,
                10,
                new_order(1, 10, Side::Sell, 7_000_000, 200_000_000, wire_tif::TimeInForce::Gtc),
            ),
            record(
                2,
                20,
                new_order(2, 20, Side::Buy, 6_999_000, 100_000_000, wire_tif::TimeInForce::Gtc),
            ),
            record(
                3,
                30,
                new_order(3, 30, Side::Buy, 7_000_000, 150_000_000, wire_tif::TimeInForce::Ioc),
            ),
            record(4, 40, cancel_order(4, 20, 2)),
            record(5, 50, mass_cancel(5, 10)),
        ]
    }

    fn record(input_seq: u64, recv_ts: u64, frame: Vec<u8>) -> InputRecord {
        InputRecord {
            input_seq,
            endpoint: Endpoint::Gateway,
            recv_ts: RecvTs::new(recv_ts),
            frame,
        }
    }

    fn write_records(path: &Path, records: &[InputRecord]) {
        let mut io = FileFrameIo::open(None, Some(path.to_path_buf()), 4096).expect("writer");
        for record in records {
            io.record_input(record).expect("record");
        }
        io.flush_pending().expect("flush");
    }

    fn new_order(client_order_id: u64, account_id: u32, side: Side, price: u64, qty: u64, tif: wire_tif::TimeInForce) -> Vec<u8> {
        let block_len = new_order_codec::SBE_BLOCK_LENGTH;
        let mut bytes = vec![0_u8; SBE_HEADER_LEN + block_len as usize];
        encode_header(&mut bytes, block_len, MessageKind::NewOrder).expect("header");
        let write_buf = WriteBuf::new(&mut bytes);
        let mut encoder = new_order_codec::NewOrderEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
        encoder
            .client_order_id(client_order_id)
            .account_id(account_id)
            .client_ts(client_order_id * 100)
            .side(match side {
                Side::Buy => wire_side::Side::Buy,
                Side::Sell => wire_side::Side::Sell,
            })
            .order_type(wire_order_type::OrderType::Limit)
            .time_in_force(tif)
            .price(price)
            .qty(qty);
        bytes
    }

    fn cancel_order(client_order_id: u64, account_id: u32, orig_client_order_id: u64) -> Vec<u8> {
        let block_len = cancel_order_codec::SBE_BLOCK_LENGTH;
        let mut bytes = vec![0_u8; SBE_HEADER_LEN + block_len as usize];
        encode_header(&mut bytes, block_len, MessageKind::CancelOrder).expect("header");
        let write_buf = WriteBuf::new(&mut bytes);
        let mut encoder = cancel_order_codec::CancelOrderEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
        encoder
            .client_order_id(client_order_id)
            .account_id(account_id)
            .orig_client_order_id(orig_client_order_id)
            .client_ts(client_order_id * 100);
        bytes
    }

    fn mass_cancel(client_order_id: u64, account_id: u32) -> Vec<u8> {
        let block_len = mass_cancel_codec::SBE_BLOCK_LENGTH;
        let mut bytes = vec![0_u8; SBE_HEADER_LEN + block_len as usize];
        encode_header(&mut bytes, block_len, MessageKind::MassCancel).expect("header");
        let write_buf = WriteBuf::new(&mut bytes);
        let mut encoder = mass_cancel_codec::MassCancelEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
        encoder
            .client_order_id(client_order_id)
            .account_id(account_id)
            .client_ts(client_order_id * 100);
        bytes
    }

    fn run_sample(gateway_input: PathBuf, gateway_output: PathBuf, market_data_output: PathBuf) {
        let config_path = temp_path("sample-config");
        write_replay_config(&config_path, &gateway_input, &gateway_output, &market_data_output);
        let config = ServerConfig::load(&config_path).expect("load replay config");
        let mut runtime = Runtime::from_server_config(Mode::Replay { expected_input_seq: 1 }, config).expect("runtime");
        runtime.run().expect("replay");
        let _ = std::fs::remove_file(config_path);
    }

    fn write_replay_config(path: &Path, gateway_input: &Path, gateway_output: &Path, market_data_output: &Path) {
        std::fs::write(
            path,
            format!(
                r#"
mode:
  type: replay
  expected_input_seq: 1
  record_path_dir: "fixtures/replay/runs"
reference_data:
  instrument:
    instrument_id: 1001
    base_asset_id: 1
    quote_asset_id: 2
    price_decimals: 2
    qty_decimals: 8
    price_tick: 1
    qty_lot: 1
engine:
  storage_min_price: 1000000
  storage_max_price: 25000000
  page_size: 1024
  max_resting_orders: 1024
  max_account_id: 100
  max_active_pages: 128
risk:
  max_open_orders_per_account: 100
  max_open_notional_per_account: 100000000000000000
  startup_reference_price: 7000000
  price_band_bps: 1000
control:
  type: file
  heartbeat_timeout_ns: 0
  max_frame_len: 4096
gateway:
  type: file
  input_path: "{}"
  output_path: "{}"
  max_frame_len: 4096
  max_messages_per_poll: 1024
market_data:
  type: file
  output_path: "{}"
  max_frame_len: 4096
"#,
                gateway_input.display(),
                gateway_output.display(),
                market_data_output.display()
            ),
        )
        .expect("write replay config");
    }

    #[test]
    fn replay_produces_byte_identical_gateway_and_market_data_outputs() {
        let input = temp_path("input");
        let first_gateway = temp_path("first-gateway-output");
        let first_market_data = temp_path("first-market-data-output");
        let second_gateway = temp_path("second-gateway-output");
        let second_market_data = temp_path("second-market-data-output");
        write_records(&input, &sample_records());

        run_sample(input.clone(), first_gateway.clone(), first_market_data.clone());
        run_sample(input.clone(), second_gateway.clone(), second_market_data.clone());

        assert_eq!(
            std::fs::read(&first_gateway).expect("first gateway"),
            std::fs::read(&second_gateway).expect("second gateway")
        );
        assert_eq!(
            std::fs::read(&first_market_data).expect("first market data"),
            std::fs::read(&second_market_data).expect("second market data")
        );

        for path in [input, first_gateway, first_market_data, second_gateway, second_market_data] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    fn replay_rejects_stalled_sequence() {
        let input = temp_path("stalled-input");
        let gateway_output = temp_path("stalled-gateway-output");
        let market_data_output = temp_path("stalled-market-data-output");
        let config_path = temp_path("stalled-config");
        write_records(
            &input,
            &[record(
                2,
                10,
                new_order(1, 10, Side::Sell, 7_000_000, 100_000_000, wire_tif::TimeInForce::Gtc),
            )],
        );
        write_replay_config(&config_path, &input, &gateway_output, &market_data_output);

        let config = ServerConfig::load(&config_path).expect("load replay config");
        let mut runtime = Runtime::from_server_config(Mode::Replay { expected_input_seq: 1 }, config).expect("runtime");
        let error = runtime.run().expect_err("stalled sequence");

        let error = format!("{error:?}");
        assert!(error.contains("ReplayStalled"));
        assert!(error.contains("expected_input_seq: 1"));

        for path in [input, gateway_output, market_data_output, config_path] {
            let _ = std::fs::remove_file(path);
        }
    }

    #[test]
    #[ignore = "fixture generation is run manually when sample bytes need refreshing"]
    fn write_checked_in_sample_fixture() {
        let dir = PathBuf::from("fixtures/replay");
        std::fs::create_dir_all(&dir).expect("fixture dir");
        let input = dir.join("sample-gateway-input.bin");
        let expected_gateway = dir.join("sample-gateway-output.bin");
        let expected_market_data = dir.join("sample-market-data-output.bin");
        write_records(&input, &sample_records());
        run_sample(input, expected_gateway, expected_market_data);
    }
}
