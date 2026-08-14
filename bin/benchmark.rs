//! Hot-path benchmark entry point.
//!
//! This binary measures the order-entry path without TCP or disk IO in the
//! measured loop. It generates deterministic binary SBE order-entry frames,
//! wraps them as gateway `InputRecord`s, preloads them into `InMemoryFrameIo`,
//! and runs the normal single-writer `Runtime` in replay mode.
//!
//! Replay mode gives deterministic input sequencing; in-memory IO removes
//! transport and filesystem noise. The measured path is:
//!
//! gateway frame decode -> risk checks -> matching engine -> execution report
//! encoding/emission.
//!
//! The benchmark runs an optional warmup phase first, discards those samples,
//! then runs a measured phase and prints HDR-histogram latency percentiles,
//! sustained throughput, and global allocation counts after the runtime exits.

use std::{
    alloc::{GlobalAlloc, Layout, System},
    env, fs, io,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use ignition::{
    config::ServerConfig,
    control::{ControlConfig, ControlRuntime},
    domain::{RecvTs, rd::InstrumentSpec},
    gateway::{GatewayConfig, GatewayRuntime},
    io::{Endpoint, IOConfig, InputRecord},
    market_data::{MarketDataFeedConfig, MarketDataRuntime},
    metrics::{MetricsBackend, MetricsRuntime, MetricsSnapshot},
    protocol::{
        MessageKind, SBE_HEADER_LEN, encode_header,
        generated::codecs::{WriteBuf, cancel_order_codec, cancel_replace_codec, new_order_codec, order_type, side, time_in_force},
    },
    runtime::{Mode, Runtime, RuntimeBuildError},
};

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);

/// Global allocator wrapper used to count allocations during the measured run.
struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        // SAFETY: this wrapper delegates allocation to the standard system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: `ptr` and `layout` were provided by the global allocator contract.
        unsafe { System.dealloc(ptr, layout) }
    }
}

fn main() {
    if let Err(error) = run(env::args().skip(1)) {
        eprintln!("benchmark failed: {error}");
        std::process::exit(1);
    }
}

/// Parse CLI arguments, run optional warmup, run the measured workload, and print the report.
fn run(args: impl Iterator<Item = String>) -> Result<(), Box<dyn std::error::Error>> {
    let args = BenchArgs::parse(args)?;
    let environment = EnvironmentSnapshot::read();
    if args.warmup_orders != 0 {
        let warmup_records = generate_records(args.warmup_orders, args.workload, 1)?;
        let mut runtime = build_runtime(&args, warmup_records, Some(MetricsBackend::Noop))?;
        runtime.run().map_err(runtime_error)?;
    }

    let measured_records = generate_records(args.orders, args.workload, 1_000_000_001)?;
    let mut runtime = build_runtime(&args, measured_records, None)?;

    reset_allocations();
    runtime.run().map_err(runtime_error)?;
    let allocations = AllocationSnapshot::read();
    print_report(&args, runtime.metrics_snapshot(), allocations, &environment);
    Ok(())
}

/// Build a normal runtime with benchmark-specific endpoint IO.
///
/// Engine, risk, instrument, gateway capacity, and metrics backend still come
/// from config. Control and market data are disabled, and gateway input is
/// replaced with prebuilt in-memory `InputRecord`s so transport IO is outside
/// the measured path.
fn build_runtime(
    args: &BenchArgs,
    records: Vec<InputRecord>,
    metrics_override: Option<MetricsBackend>,
) -> Result<Runtime, RuntimeBuildError> {
    let config = ServerConfig::load(&args.config_path).map_err(RuntimeBuildError::Config)?;
    let instrument = config.validate().map_err(RuntimeBuildError::ReferenceData)?;
    let metrics = MetricsRuntime::new(metrics_override.unwrap_or(config.metrics), 0);
    let control = ControlRuntime::new(ControlConfig {
        heartbeat_timeout_ns: 0,
        io: IOConfig::Noop,
    })?;
    let gateway = GatewayRuntime::new(
        GatewayConfig {
            io: IOConfig::InMemory {
                input: records,
                max_frame_len: config.gateway.frame_capacity(),
            },
            max_messages_per_poll: config.gateway.max_messages_per_poll,
        },
        instrument,
    )?;
    let market_data = MarketDataRuntime::new(MarketDataFeedConfig { io: IOConfig::Noop })?;

    Runtime::new(
        Mode::Replay { expected_input_seq: 1 },
        config.validate_engine().map_err(RuntimeBuildError::EngineConfig)?,
        config.validate_risk().map_err(RuntimeBuildError::RiskConfig)?,
        control,
        gateway,
        market_data,
        metrics,
    )
    .map_err(RuntimeBuildError::Runtime)
}

/// Convert runtime errors into the binary's simple IO-style error surface.
fn runtime_error(error: ignition::runtime::RuntimeError) -> io::Error {
    io::Error::other(format!("runtime failed: {error:?}"))
}

/// Command-line options controlling benchmark size, warmup, workload, and config.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BenchArgs {
    orders: u64,
    warmup_orders: u64,
    workload: Workload,
    config_path: String,
}

impl BenchArgs {
    /// Parse benchmark CLI flags.
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, io::Error> {
        let mut parsed = Self {
            orders: 1_000_000,
            warmup_orders: 50_000,
            workload: Workload::Mixed,
            config_path: "configs/benchmark.yaml".to_owned(),
        };
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--orders" => parsed.orders = next_arg(&mut args, "--orders")?.parse().map_err(parse_error)?,
                "--warmup" => parsed.warmup_orders = next_arg(&mut args, "--warmup")?.parse().map_err(parse_error)?,
                "--workload" => parsed.workload = next_arg(&mut args, "--workload")?.parse()?,
                "--config" => parsed.config_path = next_arg(&mut args, "--config")?,
                "--help" | "-h" => return Err(io::Error::other(usage())),
                other => return Err(io::Error::other(format!("unknown argument {other}\n{}", usage()))),
            }
        }
        if parsed.orders == 0 {
            return Err(io::Error::other("--orders must be greater than zero"));
        }
        Ok(parsed)
    }
}

/// Read the value following a CLI flag.
fn next_arg(args: &mut impl Iterator<Item = String>, name: &'static str) -> Result<String, io::Error> {
    args.next()
        .ok_or_else(|| io::Error::other(format!("missing value for {name}\n{}", usage())))
}

/// Format an integer parse failure for CLI output.
fn parse_error(error: std::num::ParseIntError) -> io::Error {
    io::Error::other(format!("invalid integer: {error}"))
}

/// Return benchmark CLI usage.
fn usage() -> &'static str {
    "usage: benchmark [--orders N] [--warmup N] [--workload mixed|cancel|aggressive-ioc|passive-add] [--config PATH]"
}

/// Deterministic workload shapes supported by the benchmark generator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Workload {
    /// Five-message cycle: add, replace, cancel, passive sell, aggressive IOC buy.
    Mixed,
    /// Alternating add/cancel pairs against the same account and order id.
    Cancel,
    /// Alternating resting sell liquidity and crossing IOC buy orders.
    AggressiveIoc,
    /// Resting sell limit orders only.
    PassiveAdd,
}

impl std::str::FromStr for Workload {
    type Err = io::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "mixed" => Ok(Self::Mixed),
            "cancel" => Ok(Self::Cancel),
            "aggressive-ioc" => Ok(Self::AggressiveIoc),
            "passive-add" => Ok(Self::PassiveAdd),
            _ => Err(io::Error::other(format!("unknown workload {value}"))),
        }
    }
}

impl std::fmt::Display for Workload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Mixed => "mixed",
            Self::Cancel => "cancel",
            Self::AggressiveIoc => "aggressive-ioc",
            Self::PassiveAdd => "passive-add",
        })
    }
}

/// Generate gateway replay records containing real binary SBE order-entry frames.
///
/// `first_order_id` lets warmup and measured phases use disjoint client order
/// id ranges while keeping every run deterministic.
fn generate_records(count: u64, workload: Workload, first_order_id: u64) -> Result<Vec<InputRecord>, io::Error> {
    let instrument = InstrumentSpec::new(1001, 1, 2, 2, 8, 1, 1)
        .map_err(|error| io::Error::other(format!("invalid benchmark instrument: {error:?}")))?;
    let mut records = Vec::with_capacity(count as usize);
    for index in 0..count {
        let input_seq = index + 1;
        let frame = match workload {
            Workload::Mixed => mixed_frame(index, first_order_id, &instrument),
            Workload::Cancel => cancel_frame(index, first_order_id, &instrument),
            Workload::AggressiveIoc => aggressive_ioc_frame(index, first_order_id, &instrument),
            Workload::PassiveAdd => passive_add_frame(index, first_order_id, &instrument),
        }?;
        records.push(InputRecord {
            input_seq,
            endpoint: Endpoint::Gateway,
            recv_ts: RecvTs::new(input_seq),
            frame,
        });
    }
    Ok(records)
}

/// Emit one message from the five-message mixed workload cycle.
fn mixed_frame(index: u64, first_order_id: u64, instrument: &InstrumentSpec) -> Result<Vec<u8>, io::Error> {
    let group = index / 5;
    let case = index % 5;
    let base = first_order_id + group * 10;
    let account = 10 + (group % 90_000) as u32;
    match case {
        0 => encode_new_order(
            base,
            account,
            side::Side::Buy,
            order_type::OrderType::Limit,
            time_in_force::TimeInForce::Gtc,
            6_499_900,
            2,
        ),
        1 => encode_cancel_replace(base + 1, account, base, 6_499_901, 2),
        2 => encode_cancel_order(base + 2, account, base + 1),
        3 => encode_new_order(
            base + 3,
            account + 1,
            side::Side::Sell,
            order_type::OrderType::Limit,
            time_in_force::TimeInForce::Gtc,
            6_500_000 + price_offset(group, instrument),
            1,
        ),
        _ => encode_new_order(
            base + 4,
            account + 2,
            side::Side::Buy,
            order_type::OrderType::Limit,
            time_in_force::TimeInForce::Ioc,
            6_500_000 + price_offset(group, instrument),
            1,
        ),
    }
}

/// Emit alternating add and cancel messages.
fn cancel_frame(index: u64, first_order_id: u64, _instrument: &InstrumentSpec) -> Result<Vec<u8>, io::Error> {
    let pair = index / 2;
    let account = 10 + (pair % 90_000) as u32;
    let order_id = first_order_id + pair * 2;
    if index.is_multiple_of(2) {
        encode_new_order(
            order_id,
            account,
            side::Side::Buy,
            order_type::OrderType::Limit,
            time_in_force::TimeInForce::Gtc,
            6_499_900,
            1,
        )
    } else {
        encode_cancel_order(order_id + 1, account, order_id)
    }
}

/// Emit alternating passive sell liquidity and aggressive IOC buy messages.
fn aggressive_ioc_frame(index: u64, first_order_id: u64, instrument: &InstrumentSpec) -> Result<Vec<u8>, io::Error> {
    let pair = index / 2;
    let account = 10 + (pair % 90_000) as u32;
    let order_id = first_order_id + pair * 2;
    let price = 6_500_000 + price_offset(pair, instrument);
    if index.is_multiple_of(2) {
        encode_new_order(
            order_id,
            account,
            side::Side::Sell,
            order_type::OrderType::Limit,
            time_in_force::TimeInForce::Gtc,
            price,
            1,
        )
    } else {
        encode_new_order(
            order_id + 1,
            account + 1,
            side::Side::Buy,
            order_type::OrderType::Limit,
            time_in_force::TimeInForce::Ioc,
            price,
            1,
        )
    }
}

/// Emit one passive resting sell limit order.
fn passive_add_frame(index: u64, first_order_id: u64, instrument: &InstrumentSpec) -> Result<Vec<u8>, io::Error> {
    let account = 10 + (index % 90_000) as u32;
    encode_new_order(
        first_order_id + index,
        account,
        side::Side::Sell,
        order_type::OrderType::Limit,
        time_in_force::TimeInForce::Gtc,
        6_500_000 + price_offset(index, instrument),
        1,
    )
}

/// Cycle generated prices through a small deterministic ladder.
fn price_offset(index: u64, instrument: &InstrumentSpec) -> u64 {
    (index % 64) * instrument.price_tick
}

/// Encode a valid binary SBE `NewOrder` gateway frame.
fn encode_new_order(
    client_order_id: u64,
    account_id: u32,
    side_value: side::Side,
    order_type_value: order_type::OrderType,
    tif: time_in_force::TimeInForce,
    price: u64,
    qty: u64,
) -> Result<Vec<u8>, io::Error> {
    let mut bytes = vec![0_u8; SBE_HEADER_LEN + new_order_codec::SBE_BLOCK_LENGTH as usize];
    encode_header(&mut bytes, new_order_codec::SBE_BLOCK_LENGTH, MessageKind::NewOrder).map_err(protocol_error)?;
    let write_buf = WriteBuf::new(&mut bytes);
    let mut encoder = new_order_codec::NewOrderEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .client_order_id(client_order_id)
        .account_id(account_id)
        .client_ts(client_order_id)
        .side(side_value)
        .order_type(order_type_value)
        .time_in_force(tif)
        .price(price)
        .qty(qty);
    Ok(bytes)
}

/// Encode a valid binary SBE `CancelOrder` gateway frame.
fn encode_cancel_order(client_order_id: u64, account_id: u32, orig_client_order_id: u64) -> Result<Vec<u8>, io::Error> {
    let mut bytes = vec![0_u8; SBE_HEADER_LEN + cancel_order_codec::SBE_BLOCK_LENGTH as usize];
    encode_header(&mut bytes, cancel_order_codec::SBE_BLOCK_LENGTH, MessageKind::CancelOrder).map_err(protocol_error)?;
    let write_buf = WriteBuf::new(&mut bytes);
    let mut encoder = cancel_order_codec::CancelOrderEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .client_order_id(client_order_id)
        .account_id(account_id)
        .orig_client_order_id(orig_client_order_id)
        .client_ts(client_order_id);
    Ok(bytes)
}

/// Encode a valid binary SBE `CancelReplace` gateway frame.
fn encode_cancel_replace(
    client_order_id: u64,
    account_id: u32,
    orig_client_order_id: u64,
    price: u64,
    qty: u64,
) -> Result<Vec<u8>, io::Error> {
    let mut bytes = vec![0_u8; SBE_HEADER_LEN + cancel_replace_codec::SBE_BLOCK_LENGTH as usize];
    encode_header(&mut bytes, cancel_replace_codec::SBE_BLOCK_LENGTH, MessageKind::CancelReplace).map_err(protocol_error)?;
    let write_buf = WriteBuf::new(&mut bytes);
    let mut encoder = cancel_replace_codec::CancelReplaceEncoder::default().wrap(write_buf, SBE_HEADER_LEN);
    encoder
        .client_order_id(client_order_id)
        .account_id(account_id)
        .orig_client_order_id(orig_client_order_id)
        .client_ts(client_order_id)
        .price(price)
        .qty(qty);
    Ok(bytes)
}

/// Format protocol encoding failures for benchmark setup.
fn protocol_error(error: ignition::protocol::ProtocolError) -> io::Error {
    io::Error::other(format!("protocol encode failed: {error:?}"))
}

/// Reset allocation counters immediately before the measured run.
fn reset_allocations() {
    ALLOC_CALLS.store(0, Ordering::Relaxed);
    ALLOC_BYTES.store(0, Ordering::Relaxed);
}

/// Allocation totals captured after the measured run exits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AllocationSnapshot {
    calls: u64,
    bytes: u64,
}

impl AllocationSnapshot {
    /// Read the current global allocation counters.
    fn read() -> Self {
        Self {
            calls: ALLOC_CALLS.load(Ordering::Relaxed),
            bytes: ALLOC_BYTES.load(Ordering::Relaxed),
        }
    }
}

/// Hardware and OS context that materially affects local latency measurements.
#[derive(Debug, Clone, PartialEq, Eq)]
struct EnvironmentSnapshot {
    cpu_model: String,
    logical_cpus: String,
    physical_cores: String,
    cpu_governor: String,
    l1d_cache: String,
    l1i_cache: String,
    l2_cache: String,
    l3_cache: String,
    memory_total: String,
    numa_nodes: String,
    kernel: String,
    cpu_pinning: String,
}

impl EnvironmentSnapshot {
    /// Read Linux system metadata outside the measured path.
    fn read() -> Self {
        Self {
            cpu_model: read_cpu_model(),
            logical_cpus: std::thread::available_parallelism()
                .map(|cpus| cpus.get().to_string())
                .unwrap_or_else(|_| unknown()),
            physical_cores: read_physical_cores(),
            cpu_governor: read_trimmed("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor"),
            l1d_cache: read_cache_size(1, "Data"),
            l1i_cache: read_cache_size(1, "Instruction"),
            l2_cache: read_cache_size(2, "Unified"),
            l3_cache: read_cache_size(3, "Unified"),
            memory_total: read_mem_total(),
            numa_nodes: read_numa_nodes(),
            kernel: read_trimmed("/proc/sys/kernel/osrelease"),
            cpu_pinning: read_status_value("Cpus_allowed_list"),
        }
    }
}

/// Print the benchmark result in a simple stable text format.
fn print_report(args: &BenchArgs, metrics: MetricsSnapshot, allocations: AllocationSnapshot, environment: &EnvironmentSnapshot) {
    let throughput = if metrics.measured_elapsed_ns == 0 {
        0.0
    } else {
        metrics.measured_samples as f64 / (metrics.measured_elapsed_ns as f64 / 1_000_000_000.0)
    };
    let allocs_per_order = allocations.calls as f64 / metrics.measured_samples.max(1) as f64;

    println!("environment:");
    println!("  cpu_model: {}", environment.cpu_model);
    println!("  logical_cpus: {}", environment.logical_cpus);
    println!("  physical_cores: {}", environment.physical_cores);
    println!("  cpu_governor: {}", environment.cpu_governor);
    println!("  l1d_cache: {}", environment.l1d_cache);
    println!("  l1i_cache: {}", environment.l1i_cache);
    println!("  l2_cache: {}", environment.l2_cache);
    println!("  l3_cache: {}", environment.l3_cache);
    println!("  memory_total: {}", environment.memory_total);
    println!("  numa_nodes: {}", environment.numa_nodes);
    println!("  kernel: {}", environment.kernel);
    println!("  cpu_pinning: {}", environment.cpu_pinning);
    println!();
    println!("workload: {}", args.workload);
    println!("orders: {}", args.orders);
    println!("warmup_orders: {}", args.warmup_orders);
    println!("throughput_orders_per_sec: {:.0}", throughput);
    println!();
    println!("latency_ns:");
    println!("  p50: {}", metrics.p50_ns);
    println!("  p99: {}", metrics.p99_ns);
    println!("  p99.9: {}", metrics.p999_ns);
    println!("  p99.99: {}", metrics.p9999_ns);
    println!("  max: {}", metrics.max_ns);
    println!();
    println!("allocations:");
    println!("  calls: {}", allocations.calls);
    println!("  bytes: {}", allocations.bytes);
    println!("  calls_per_order: {:.6}", allocs_per_order);
}

/// Read the CPU model from `/proc/cpuinfo`.
fn read_cpu_model() -> String {
    read_colon_value("/proc/cpuinfo", "model name")
}

/// Estimate physical cores from unique `(physical id, core id)` pairs in `/proc/cpuinfo`.
fn read_physical_cores() -> String {
    let Ok(text) = fs::read_to_string("/proc/cpuinfo") else {
        return unknown();
    };
    let mut cores = Vec::<(String, String)>::new();
    let mut physical_id = None::<String>;
    let mut core_id = None::<String>;

    for line in text.lines().chain(std::iter::once("")) {
        if line.trim().is_empty() {
            if let (Some(physical), Some(core)) = (physical_id.take(), core_id.take()) {
                if !cores.iter().any(|seen| seen.0 == physical && seen.1 == core) {
                    cores.push((physical, core));
                }
            }
            physical_id = None;
            core_id = None;
            continue;
        }

        if let Some(value) = line.strip_prefix("physical id").and_then(colon_value) {
            physical_id = Some(value.to_owned());
        } else if let Some(value) = line.strip_prefix("core id").and_then(colon_value) {
            core_id = Some(value.to_owned());
        }
    }

    if cores.is_empty() { unknown() } else { cores.len().to_string() }
}

/// Read cache size from the first CPU cache index matching level and type.
fn read_cache_size(level: u32, cache_type: &str) -> String {
    let Ok(entries) = fs::read_dir("/sys/devices/system/cpu/cpu0/cache") else {
        return unknown();
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let found_level = read_trimmed(path.join("level"));
        let found_type = read_trimmed(path.join("type"));
        if found_level == level.to_string() && found_type == cache_type {
            return read_trimmed(path.join("size"));
        }
    }

    unknown()
}

/// Read total system memory from `/proc/meminfo`.
fn read_mem_total() -> String {
    read_colon_value("/proc/meminfo", "MemTotal")
}

/// Count visible NUMA nodes from sysfs.
fn read_numa_nodes() -> String {
    let Ok(entries) = fs::read_dir("/sys/devices/system/node") else {
        return unknown();
    };
    let count = entries
        .flatten()
        .filter(|entry| {
            entry.file_name().to_str().is_some_and(|name| {
                name.strip_prefix("node")
                    .is_some_and(|suffix| suffix.chars().all(|ch| ch.is_ascii_digit()))
            })
        })
        .count();
    count.max(1).to_string()
}

/// Read a `key: value` field from a procfs-style file.
fn read_colon_value(path: impl AsRef<Path>, key: &str) -> String {
    let Ok(text) = fs::read_to_string(path) else {
        return unknown();
    };
    text.lines()
        .find_map(|line| line.strip_prefix(key).and_then(colon_value).map(ToOwned::to_owned))
        .unwrap_or_else(unknown)
}

/// Read a field from `/proc/self/status`.
fn read_status_value(key: &str) -> String {
    read_colon_value("/proc/self/status", key)
}

/// Return the value after `:` in a procfs-style line.
fn colon_value(line_suffix: &str) -> Option<&str> {
    line_suffix.split_once(':').map(|(_, value)| value.trim())
}

/// Read a small system file and trim whitespace.
fn read_trimmed(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path)
        .map(|value| value.trim().to_owned())
        .unwrap_or_else(|_| unknown())
}

/// Placeholder for environment fields unavailable on this host.
fn unknown() -> String {
    "unknown".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ignition::protocol::decode_header;

    #[test]
    fn workload_generation_is_deterministic() {
        let first = generate_records(20, Workload::Mixed, 1).expect("first");
        let second = generate_records(20, Workload::Mixed, 1).expect("second");

        assert_eq!(first, second);
    }

    #[test]
    fn workload_generation_emits_gateway_order_frames() {
        let records = generate_records(8, Workload::AggressiveIoc, 1).expect("records");

        for (index, record) in records.iter().enumerate() {
            assert_eq!(record.input_seq, index as u64 + 1);
            assert_eq!(record.endpoint, Endpoint::Gateway);
            assert!(decode_header(&record.frame).expect("header").kind.is_order_entry());
        }
    }
}
