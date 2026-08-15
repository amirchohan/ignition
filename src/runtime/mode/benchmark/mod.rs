//! Benchmark execution, CLI parsing, and stable text reporting.
//!
//! This mode measures the order-entry path without TCP or disk IO in the
//! measured loop. It generates deterministic binary SBE order-entry frames,
//! wraps them as gateway `InputRecord`s, preloads them into in-memory IO, and
//! runs the normal single-writer `Runtime` with finite ordered input.
//!
//! Finite ordered input gives deterministic sequencing; in-memory IO removes
//! transport and filesystem noise. The measured path is:
//!
//! gateway frame decode -> risk checks -> matching engine -> execution report
//! encoding/emission.
//!
//! Burst mode releases input as fast as the runtime can consume it. Open-loop
//! mode releases input against absolute deadlines at a configured target rate
//! and measures every response from its scheduled arrival deadline.
//!
//! # Main components
//!
//! - [`run`] owns warmup, measured execution, allocation sampling, and final
//!   reporting.
//! - `BenchArgs`, `Schedule`, and `Workload` define the validated command-
//!   line experiment.
//! - `generate_records` and the frame encoders create deterministic binary input
//!   before timing begins.
//! - `EnvironmentSnapshot` captures host context outside the measured path.
//! - [`AllocationProbe`] bridges binary-owned allocator counters into the
//!   library benchmark mode.
//!
//! # Data flow and invariants
//!
//! Warmup and measurement use disjoint order-ID ranges and separate runtimes.
//! Both use the same resolved core configuration and common runtime builder as
//! live and replay modes. The measured section begins only after every input
//! frame is allocated and encoded, and it ends before environment inspection or
//! text formatting. The reported sample count must equal the requested order
//! count, preventing a partial finite run from producing plausible statistics.

mod report;
mod workload;

pub use report::{AllocationProbe, AllocationSnapshot};

use std::{fs, io, path::Path};

use crate::{
    config::{CoreConfig, ResolvedCoreConfig},
    domain::{RecvTs, rd::InstrumentSpec},
    io::{Endpoint, IOConfig, InputRecord},
    metrics::{MetricsBackend, MetricsSnapshot},
    protocol::{
        MAX_ACCEPTED_WIRE_FRAME_LEN, MessageKind, SBE_HEADER_LEN, encode_header,
        generated::codecs::{WriteBuf, cancel_order_codec, cancel_replace_codec, new_order_codec, order_type, side, time_in_force},
    },
    runtime::{
        InputLifecycle, Runtime, RuntimeBuildError,
        build::{RuntimeSpec, build_runtime},
    },
};

/// Parse CLI arguments, run optional warmup, run the measured workload, and print the report.
pub fn run(args: impl Iterator<Item = String>, probe: AllocationProbe) -> Result<(), Box<dyn std::error::Error>> {
    let args = BenchArgs::parse(args)?;
    let core = CoreConfig::load(&args.config_path)?;
    let resolved = core.resolve().map_err(crate::config::ConfigError::Validate)?;
    let environment = EnvironmentSnapshot::read();
    if args.warmup_orders != 0 {
        workload::validate(args.warmup_orders, args.workload == Workload::PassiveAdd, &resolved)?;
        let warmup_records = generate_records(args.warmup_orders, args.workload, 1, &resolved)?;
        let mut runtime = build_benchmark_runtime(resolved, args.schedule, warmup_records, MetricsBackend::Noop)?;
        runtime.run().map_err(runtime_error)?;
    }

    workload::validate(args.orders, args.workload == Workload::PassiveAdd, &resolved)?;
    let measured_records = generate_records(args.orders, args.workload, 1_000_000_001, &resolved)?;
    let mut runtime = build_benchmark_runtime(resolved, args.schedule, measured_records, MetricsBackend::InMemory)?;

    (probe.reset)();
    runtime.run().map_err(runtime_error)?;
    let allocations = (probe.read)();
    let metrics = runtime.metrics_snapshot();
    if metrics.measured_samples != args.orders {
        return Err(io::Error::other(format!(
            "benchmark processed {} of {} requested orders",
            metrics.measured_samples, args.orders
        ))
        .into());
    }
    print_report(&args, metrics, allocations, &environment);
    Ok(())
}

/// Build a normal runtime with benchmark-specific endpoint IO.
///
/// Engine, risk, instrument, gateway capacity, and metrics backend still come
/// from config. Control and market data are disabled, and gateway input is
/// replaced with prebuilt in-memory `InputRecord`s so transport IO is outside
/// the measured path.
fn build_benchmark_runtime(
    core: ResolvedCoreConfig,
    schedule: Schedule,
    records: Vec<InputRecord>,
    metrics: MetricsBackend,
) -> Result<Runtime, RuntimeBuildError> {
    let benchmark_io = match schedule {
        Schedule::Burst => IOConfig::InMemory {
            input: records,
            max_frame_len: MAX_ACCEPTED_WIRE_FRAME_LEN,
        },
        Schedule::OpenLoop { target_rate_per_sec } => IOConfig::ScheduledInMemory {
            input: records,
            max_frame_len: MAX_ACCEPTED_WIRE_FRAME_LEN,
            target_rate_per_sec,
        },
    };
    build_runtime(RuntimeSpec {
        input: InputLifecycle::Replay { expected_input_seq: 1 },
        core,
        control_io: IOConfig::Noop,
        gateway_io: benchmark_io,
        market_data_io: IOConfig::Noop,
        metrics,
    })
}

/// Convert runtime errors into the binary's simple IO-style error surface.
fn runtime_error(error: crate::runtime::RuntimeError) -> io::Error {
    io::Error::other(format!("runtime failed: {error:?}"))
}

/// Command-line options controlling benchmark size, warmup, workload, and config.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BenchArgs {
    /// Number of commands included in measured latency statistics.
    orders: u64,
    /// Number of unmeasured commands used to warm code and data paths.
    warmup_orders: u64,
    /// Deterministic command pattern generated for each phase.
    workload: Workload,
    /// Arrival policy used by the in-memory gateway adapter.
    schedule: Schedule,
    /// Path to deterministic core runtime configuration.
    config_path: String,
}

impl BenchArgs {
    /// Parse benchmark CLI flags.
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, io::Error> {
        let mut parsed = Self {
            orders: 1_000_000,
            warmup_orders: 50_000,
            workload: Workload::Mixed,
            schedule: Schedule::Burst,
            config_path: "configs/server.yaml".to_owned(),
        };
        let mut schedule_kind = ScheduleKind::Burst;
        let mut target_rate_per_sec = None;
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--orders" => parsed.orders = next_arg(&mut args, "--orders")?.parse().map_err(parse_error)?,
                "--warmup" => parsed.warmup_orders = next_arg(&mut args, "--warmup")?.parse().map_err(parse_error)?,
                "--workload" => parsed.workload = next_arg(&mut args, "--workload")?.parse()?,
                "--schedule" => schedule_kind = next_arg(&mut args, "--schedule")?.parse()?,
                "--target-rate" => target_rate_per_sec = Some(next_arg(&mut args, "--target-rate")?.parse().map_err(parse_error)?),
                "--core-config" => parsed.config_path = next_arg(&mut args, "--core-config")?,
                "--help" | "-h" => return Err(io::Error::other(usage())),
                other => return Err(io::Error::other(format!("unknown argument {other}\n{}", usage()))),
            }
        }
        if parsed.orders == 0 {
            return Err(io::Error::other("--orders must be greater than zero"));
        }
        parsed.schedule = Schedule::from_options(schedule_kind, target_rate_per_sec)?;
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
    "usage: benchmark [--orders N] [--warmup N] [--workload mixed|cancel|aggressive-ioc|passive-add] [--schedule burst|open-loop] [--target-rate ORDERS_PER_SEC] [--core-config PATH]"
}

/// Validated benchmark input schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Schedule {
    /// Release every prepared input as quickly as the runtime can consume it.
    Burst,
    /// Release inputs against absolute deadlines at a fixed offered rate.
    OpenLoop {
        /// Number of input commands scheduled per second.
        target_rate_per_sec: u64,
    },
}

impl Schedule {
    /// Combine a parsed schedule name with its optional required rate.
    fn from_options(kind: ScheduleKind, target_rate_per_sec: Option<u64>) -> Result<Self, io::Error> {
        match (kind, target_rate_per_sec) {
            (ScheduleKind::Burst, None) => Ok(Self::Burst),
            (ScheduleKind::Burst, Some(_)) => Err(io::Error::other("--target-rate requires --schedule open-loop")),
            (ScheduleKind::OpenLoop, None) => Err(io::Error::other("--schedule open-loop requires --target-rate")),
            (ScheduleKind::OpenLoop, Some(target_rate_per_sec)) if !(1..=1_000_000_000).contains(&target_rate_per_sec) => {
                Err(io::Error::other("--target-rate must be between 1 and 1000000000 orders per second"))
            }
            (ScheduleKind::OpenLoop, Some(target_rate_per_sec)) => Ok(Self::OpenLoop { target_rate_per_sec }),
        }
    }

    /// Return the integer nanosecond interval reported for open-loop input.
    const fn expected_interval_ns(self) -> Option<u64> {
        match self {
            Self::Burst => None,
            Self::OpenLoop { target_rate_per_sec } => Some(1_000_000_000 / target_rate_per_sec),
        }
    }

    /// Return the configured offered rate for open-loop input.
    const fn target_rate_per_sec(self) -> Option<u64> {
        match self {
            Self::Burst => None,
            Self::OpenLoop { target_rate_per_sec } => Some(target_rate_per_sec),
        }
    }
}

impl std::fmt::Display for Schedule {
    /// Write the stable schedule name used in benchmark reports.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Burst => f.write_str("burst"),
            Self::OpenLoop { .. } => f.write_str("open-loop"),
        }
    }
}

/// Schedule name parsed before its required rate is validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScheduleKind {
    /// Parsed burst schedule, which must not have a target rate.
    Burst,
    /// Parsed open-loop schedule, which requires a target rate.
    OpenLoop,
}

impl std::str::FromStr for ScheduleKind {
    type Err = io::Error;

    /// Parse a schedule name accepted by the benchmark CLI.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "burst" => Ok(Self::Burst),
            "open-loop" => Ok(Self::OpenLoop),
            _ => Err(io::Error::other(format!("unknown schedule {value}"))),
        }
    }
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

    /// Parse a workload name accepted by the benchmark CLI.
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
    /// Write the stable workload name used in benchmark reports.
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
fn generate_records(count: u64, workload: Workload, first_order_id: u64, core: &ResolvedCoreConfig) -> Result<Vec<InputRecord>, io::Error> {
    let instrument = core.instrument;
    let reference_price = core.risk.startup_reference_price.raw();
    let mut records = Vec::with_capacity(count as usize);
    for index in 0..count {
        let input_seq = index + 1;
        let frame = match workload {
            Workload::Mixed => mixed_frame(index, first_order_id, &instrument, reference_price),
            Workload::Cancel => cancel_frame(index, first_order_id, &instrument, reference_price),
            Workload::AggressiveIoc => aggressive_ioc_frame(index, first_order_id, &instrument, reference_price),
            Workload::PassiveAdd => passive_add_frame(index, first_order_id, &instrument, reference_price),
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
fn mixed_frame(index: u64, first_order_id: u64, instrument: &InstrumentSpec, reference_price: u64) -> Result<Vec<u8>, io::Error> {
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
            reference_price.saturating_sub(100 * instrument.price_tick),
            2 * instrument.qty_lot,
        ),
        1 => encode_cancel_replace(
            base + 1,
            account,
            base,
            reference_price.saturating_sub(99 * instrument.price_tick),
            2 * instrument.qty_lot,
        ),
        2 => encode_cancel_order(base + 2, account, base + 1),
        3 => encode_new_order(
            base + 3,
            account + 1,
            side::Side::Sell,
            order_type::OrderType::Limit,
            time_in_force::TimeInForce::Gtc,
            reference_price + price_offset(group, instrument),
            instrument.qty_lot,
        ),
        _ => encode_new_order(
            base + 4,
            account + 2,
            side::Side::Buy,
            order_type::OrderType::Limit,
            time_in_force::TimeInForce::Ioc,
            reference_price + price_offset(group, instrument),
            instrument.qty_lot,
        ),
    }
}

/// Emit alternating add and cancel messages.
fn cancel_frame(index: u64, first_order_id: u64, instrument: &InstrumentSpec, reference_price: u64) -> Result<Vec<u8>, io::Error> {
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
            reference_price.saturating_sub(100 * instrument.price_tick),
            instrument.qty_lot,
        )
    } else {
        encode_cancel_order(order_id + 1, account, order_id)
    }
}

/// Emit alternating passive sell liquidity and aggressive IOC buy messages.
fn aggressive_ioc_frame(index: u64, first_order_id: u64, instrument: &InstrumentSpec, reference_price: u64) -> Result<Vec<u8>, io::Error> {
    let pair = index / 2;
    let account = 10 + (pair % 90_000) as u32;
    let order_id = first_order_id + pair * 2;
    let price = reference_price + price_offset(pair, instrument);
    if index.is_multiple_of(2) {
        encode_new_order(
            order_id,
            account,
            side::Side::Sell,
            order_type::OrderType::Limit,
            time_in_force::TimeInForce::Gtc,
            price,
            instrument.qty_lot,
        )
    } else {
        encode_new_order(
            order_id + 1,
            account + 1,
            side::Side::Buy,
            order_type::OrderType::Limit,
            time_in_force::TimeInForce::Ioc,
            price,
            instrument.qty_lot,
        )
    }
}

/// Emit one passive resting sell limit order.
fn passive_add_frame(index: u64, first_order_id: u64, instrument: &InstrumentSpec, reference_price: u64) -> Result<Vec<u8>, io::Error> {
    let account = 10 + (index % 90_000) as u32;
    encode_new_order(
        first_order_id + index,
        account,
        side::Side::Sell,
        order_type::OrderType::Limit,
        time_in_force::TimeInForce::Gtc,
        reference_price + price_offset(index, instrument),
        instrument.qty_lot,
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
fn protocol_error(error: crate::protocol::ProtocolError) -> io::Error {
    io::Error::other(format!("protocol encode failed: {error:?}"))
}

/// Hardware and OS context that materially affects local latency measurements.
#[derive(Debug, Clone, PartialEq, Eq)]
struct EnvironmentSnapshot {
    /// Processor model reported by the host kernel.
    cpu_model: String,
    /// Logical processors visible to the benchmark process.
    logical_cpus: String,
    /// Unique physical package/core pairs reported by the host kernel.
    physical_cores: String,
    /// Frequency governor reported for the first logical processor.
    cpu_governor: String,
    /// Level-one data-cache size reported by sysfs.
    l1d_cache: String,
    /// Level-one instruction-cache size reported by sysfs.
    l1i_cache: String,
    /// Level-two unified-cache size reported by sysfs.
    l2_cache: String,
    /// Level-three unified-cache size reported by sysfs.
    l3_cache: String,
    /// Total physical memory reported by procfs.
    memory_total: String,
    /// Number of NUMA nodes visible through sysfs.
    numa_nodes: String,
    /// Host kernel release string.
    kernel: String,
    /// Logical CPU set available to the benchmark process.
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
    println!("schedule: {}", args.schedule);
    if let Some(target_rate_per_sec) = args.schedule.target_rate_per_sec() {
        println!("target_rate_orders_per_sec: {target_rate_per_sec}");
    }
    if let Some(expected_interval_ns) = args.schedule.expected_interval_ns() {
        println!("expected_interval_ns: {expected_interval_ns}");
    }
    println!("orders: {}", args.orders);
    println!("warmup_orders: {}", args.warmup_orders);
    println!("throughput_orders_per_sec: {:.0}", throughput);
    println!();
    let latency_label = match args.schedule {
        Schedule::Burst => "latency_ns_service_time",
        Schedule::OpenLoop { .. } => "latency_ns_scheduled_arrival_to_report",
    };
    print_latency(latency_label, metrics);
    println!();
    println!("allocations:");
    println!("  calls: {}", allocations.calls);
    println!("  bytes: {}", allocations.bytes);
    println!("  calls_per_order: {:.6}", allocs_per_order);
}

/// Print tail-latency percentiles and the observed maximum.
fn print_latency(label: &str, metrics: MetricsSnapshot) {
    println!("{label}:");
    println!("  p50: {}", metrics.p50_ns);
    println!("  p99: {}", metrics.p99_ns);
    println!("  p99.9: {}", metrics.p999_ns);
    println!("  p99.99: {}", metrics.p9999_ns);
    println!("  max: {}", metrics.max_ns);
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
            if let (Some(physical), Some(core)) = (physical_id.take(), core_id.take())
                && !cores.iter().any(|seen| seen.0 == physical && seen.1 == core)
            {
                cores.push((physical, core));
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
    use crate::protocol::decode_header;

    #[test]
    fn workload_generation_is_deterministic() {
        let core = CoreConfig::load("configs/server.yaml").expect("core").resolve().expect("resolved");
        let first = generate_records(20, Workload::Mixed, 1, &core).expect("first");
        let second = generate_records(20, Workload::Mixed, 1, &core).expect("second");

        assert_eq!(first, second);
    }

    #[test]
    fn workload_generation_emits_gateway_order_frames() {
        let core = CoreConfig::load("configs/server.yaml").expect("core").resolve().expect("resolved");
        let records = generate_records(8, Workload::AggressiveIoc, 1, &core).expect("records");

        for (index, record) in records.iter().enumerate() {
            assert_eq!(record.input_seq, index as u64 + 1);
            assert_eq!(record.endpoint, Endpoint::Gateway);
            assert!(decode_header(&record.frame).expect("header").kind.is_order_entry());
        }
    }

    #[test]
    fn benchmark_defaults_to_burst_without_a_target_rate() {
        let args = BenchArgs::parse(std::iter::empty()).expect("args");

        assert_eq!(args.schedule, Schedule::Burst);
    }

    #[test]
    fn open_loop_requires_and_validates_a_target_rate() {
        let args = BenchArgs::parse(
            ["--schedule", "open-loop", "--target-rate", "100000"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect("args");

        assert_eq!(
            args.schedule,
            Schedule::OpenLoop {
                target_rate_per_sec: 100_000
            }
        );
        assert_eq!(args.schedule.expected_interval_ns(), Some(10_000));

        let missing = BenchArgs::parse(["--schedule", "open-loop"].into_iter().map(str::to_owned)).expect_err("missing rate");
        assert!(missing.to_string().contains("requires --target-rate"));

        let zero =
            BenchArgs::parse(["--schedule", "open-loop", "--target-rate", "0"].into_iter().map(str::to_owned)).expect_err("zero rate");
        assert!(zero.to_string().contains("between 1 and 1000000000"));

        let too_high = BenchArgs::parse(
            ["--schedule", "open-loop", "--target-rate", "1000000001"]
                .into_iter()
                .map(str::to_owned),
        )
        .expect_err("sub-nanosecond interval");
        assert!(too_high.to_string().contains("between 1 and 1000000000"));
    }

    #[test]
    fn burst_rejects_a_target_rate() {
        let error = BenchArgs::parse(["--target-rate", "100000"].into_iter().map(str::to_owned)).expect_err("rate without open loop");

        assert!(error.to_string().contains("requires --schedule open-loop"));
    }
}
