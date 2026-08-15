//! Live network composition and optional deterministic input recording.
//!
//! This module turns live-mode CLI and YAML configuration into the common
//! runtime graph. It owns socket adapter selection and the recording directory
//! created for a live process; matching, risk, sequencing, and polling remain
//! owned by the shared runtime and its neighboring subsystems.
//!
//! # Main components
//!
//! - [`run`] loads core and live configuration, prepares optional recording
//!   paths, builds the shared runtime, and runs it.
//! - `LiveArgs` owns the two configuration paths accepted by the server CLI.
//! - `LiveRun` identifies the recording input directory created for one live
//!   process.
//! - `LiveRunError` reports recording setup failures before the hot loop.
//!
//! # Data flow and invariants
//!
//! Control and gateway frames share one live [`InputLifecycle`]. When recording
//! is enabled, both streams are written beneath the same run directory and the
//! manifest captures the exact core configuration before any frame is accepted.
//! Network backpressure and multicast settings come only from `LiveConfig`;
//! protocol frame bounds come from the protocol module rather than YAML.

use std::{
    fs, io,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    config::{CoreConfig, LiveConfig, RecordingConfig},
    io::IOConfig,
    metrics::MetricsBackend,
    protocol::MAX_ACCEPTED_WIRE_FRAME_LEN,
    runtime::{
        InputLifecycle,
        build::{RuntimeSpec, build_runtime},
    },
};

use super::{RUN_MANIFEST_VERSION, RunManifest};

/// Build and run the live server from command-line configuration paths.
pub fn run(args: impl Iterator<Item = String>) -> Result<(), Box<dyn std::error::Error>> {
    let args = LiveArgs::parse(args)?;
    let core = CoreConfig::load(&args.core_config)?;
    let resolved = core.resolve().map_err(crate::config::ConfigError::Validate)?;
    let live = LiveConfig::load(&args.live_config)?;

    let (control_record, gateway_record) = match &live.recording {
        RecordingConfig::Noop => (None, None),
        RecordingConfig::File { root_dir } => {
            let run = create_live_run(root_dir, &core)?;
            tracing::info!("recording input_id={}", run.input_id);
            tracing::info!("input: {}", run.input_dir.display());
            (Some(run.input_dir.join("control.bin")), Some(run.input_dir.join("gateway.bin")))
        }
    };

    let mut runtime = build_runtime(RuntimeSpec {
        input: InputLifecycle::Live { next_input_seq: 1 },
        core: resolved,
        control_io: IOConfig::Tcp {
            bind_addr: live.control.listen_addr,
            max_frame_len: MAX_ACCEPTED_WIRE_FRAME_LEN,
            max_pending_write_bytes: live.control.max_pending_write_bytes,
            record_path: control_record,
        },
        gateway_io: IOConfig::Tcp {
            bind_addr: live.gateway.listen_addr,
            max_frame_len: MAX_ACCEPTED_WIRE_FRAME_LEN,
            max_pending_write_bytes: live.gateway.max_pending_write_bytes,
            record_path: gateway_record,
        },
        market_data_io: IOConfig::Udp {
            bind_addr: live.market_data.bind_addr,
            group_addr: live.market_data.group_addr,
            multicast_ttl: live.market_data.multicast_ttl,
            multicast_loop_v4: live.market_data.multicast_loop_v4,
            max_datagram_len: MAX_ACCEPTED_WIRE_FRAME_LEN,
            max_pending_datagram_bytes: live.market_data.max_pending_datagram_bytes,
        },
        metrics: MetricsBackend::Noop,
    })?;
    runtime
        .run()
        .map_err(|error| io::Error::other(format!("runtime failed: {error:?}")))?;
    Ok(())
}

/// Configuration paths accepted by the live server entry point.
struct LiveArgs {
    /// Path to deterministic instrument, engine, risk, and poll configuration.
    core_config: PathBuf,
    /// Path to live network, backpressure, and recording configuration.
    live_config: PathBuf,
}

impl LiveArgs {
    /// Parse live-mode flags and apply repository-local defaults.
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, io::Error> {
        let mut parsed = Self {
            core_config: PathBuf::from("configs/server.yaml"),
            live_config: PathBuf::from("configs/live.yaml"),
        };
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--core-config" => parsed.core_config = PathBuf::from(next_arg(&mut args, "--core-config")?),
                "--live-config" => parsed.live_config = PathBuf::from(next_arg(&mut args, "--live-config")?),
                "--help" | "-h" => return Err(io::Error::other(usage())),
                _ => return Err(io::Error::other(format!("unknown argument {arg}\n{}", usage()))),
            }
        }
        Ok(parsed)
    }
}

/// Read the value following a live-mode CLI flag.
fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, io::Error> {
    args.next()
        .ok_or_else(|| io::Error::other(format!("missing value for {name}\n{}", usage())))
}

/// Return live-mode CLI usage.
fn usage() -> &'static str {
    "usage: server [--core-config PATH] [--live-config PATH]"
}

/// Recording paths allocated for one live process.
struct LiveRun {
    /// Epoch-second identifier used as the run directory name.
    input_id: u64,
    /// Directory receiving control and gateway input records.
    input_dir: PathBuf,
}

/// Create a new recording directory and persist its manifest.
fn create_live_run(root: &Path, core: &CoreConfig) -> Result<LiveRun, LiveRunError> {
    core.resolve().map_err(|error| LiveRunError::InvalidConfig(format!("{error:?}")))?;
    // the wall clock names an operational artifact only; it never enters
    // matching decisions or recorded input sequencing.
    let input_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| LiveRunError::Clock(error.to_string()))?
        .as_secs();
    fs::create_dir_all(root).map_err(LiveRunError::Io)?;
    let run_dir = root.join(input_id.to_string());
    // refusing to reuse an existing run directory prevents two processes from
    // interleaving input files under one manifest.
    fs::create_dir(&run_dir).map_err(LiveRunError::Io)?;
    let input_dir = run_dir.join("input");
    fs::create_dir(&input_dir).map_err(LiveRunError::Io)?;
    write_manifest(&run_dir, core, 1)?;
    Ok(LiveRun { input_id, input_dir })
}

/// Persist the versioned replay contract for a newly created live run.
fn write_manifest(run_dir: &Path, core: &CoreConfig, first_input_sequence: u64) -> Result<(), LiveRunError> {
    let manifest = RunManifest {
        format_version: RUN_MANIFEST_VERSION,
        first_input_sequence,
        core: core.clone(),
    };
    let text = serde_yaml::to_string(&manifest).map_err(LiveRunError::Serialize)?;
    fs::write(run_dir.join("run.yaml"), text).map_err(LiveRunError::Io)
}

/// Failure while preparing live recording artifacts.
#[derive(Debug)]
enum LiveRunError {
    /// A recording directory or manifest file operation failed.
    Io(std::io::Error),
    /// The run manifest could not be encoded as YAML.
    Serialize(serde_yaml::Error),
    /// The core configuration was not valid enough to record.
    InvalidConfig(String),
    /// The operating-system clock could not produce a run identifier.
    Clock(String),
}

impl std::fmt::Display for LiveRunError {
    /// Format a caller-facing recording setup failure.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "run recording I/O failed: {error}"),
            Self::Serialize(error) => write!(f, "failed to serialize run manifest: {error}"),
            Self::InvalidConfig(error) => write!(f, "invalid recorded core config: {error}"),
            Self::Clock(error) => write!(f, "failed to generate run id: {error}"),
        }
    }
}

impl std::error::Error for LiveRunError {}
