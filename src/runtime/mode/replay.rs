//! Deterministic reconstruction of a recorded live run.
//!
//! This module validates a recorded run directory, discovers its finite input
//! streams, allocates a fresh output directory, and composes file-backed IO for
//! the common runtime. It owns replay artifact policy; decoding, sequencing,
//! matching, risk, and publication remain in their normal runtime components.
//!
//! # Main components
//!
//! - [`run`] resolves the requested run, builds file adapters, and executes the
//!   shared runtime with finite ordered input.
//! - `ReplayArgs` owns the run-directory path accepted by the replay CLI.
//! - `ReplayRun` contains the validated manifest state and concrete input and
//!   output paths needed for composition.
//! - `ReplayRunError` distinguishes malformed artifacts from filesystem and
//!   manifest compatibility failures.
//!
//! # Data flow and invariants
//!
//! Replay loads core configuration only from `run.yaml`, so later operator
//! configuration edits cannot alter historical results. Control and gateway
//! files are consumed through one global input sequence, at least one input
//! stream must exist, and every execution writes to a new monotonically named
//! output directory without replacing earlier evidence.

use std::{
    fs, io,
    path::{Path, PathBuf},
};

use crate::{
    io::IOConfig,
    metrics::MetricsBackend,
    protocol::MAX_ACCEPTED_WIRE_FRAME_LEN,
    runtime::{
        InputLifecycle, Yield,
        build::{RuntimeSpec, build_runtime},
    },
};

use super::{RUN_MANIFEST_VERSION, RunManifest};

/// Replay one recorded run into a newly allocated output directory.
pub fn run(args: impl Iterator<Item = String>) -> Result<(), Box<dyn std::error::Error>> {
    let run_path = ReplayArgs::parse(args)?.run_path;
    let run = open_replay_run(&run_path)?;
    let resolved = run.core.resolve().map_err(crate::config::ConfigError::Validate)?;
    tracing::info!("replay run={}", run_path.display());
    tracing::info!("output_id={}", run.output_id);
    tracing::info!("output: {}", run.output_dir.display());

    let mut runtime = build_runtime(RuntimeSpec {
        input: InputLifecycle::Replay {
            expected_input_seq: run.first_input_sequence,
        },
        idle_yield: Yield::Noop,
        core: resolved,
        control_io: IOConfig::File {
            input_path: run.control_input,
            output_path: Some(run.output_dir.join("control.bin")),
            max_frame_len: MAX_ACCEPTED_WIRE_FRAME_LEN,
        },
        gateway_io: IOConfig::File {
            input_path: run.gateway_input,
            output_path: Some(run.output_dir.join("gateway.bin")),
            max_frame_len: MAX_ACCEPTED_WIRE_FRAME_LEN,
        },
        market_data_io: IOConfig::File {
            input_path: None,
            output_path: Some(run.output_dir.join("market_data.bin")),
            max_frame_len: MAX_ACCEPTED_WIRE_FRAME_LEN,
        },
        metrics: MetricsBackend::Noop,
    })?;
    runtime
        .run()
        .map_err(|error| io::Error::other(format!("replay runtime failed: {error:?}")))?;
    Ok(())
}

/// Run-directory path accepted by the replay entry point.
struct ReplayArgs {
    /// Directory containing `run.yaml`, `input/`, and replay outputs.
    run_path: PathBuf,
}

impl ReplayArgs {
    /// Parse the required replay run-directory flag.
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, io::Error> {
        let mut run_path = None;
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--run" => run_path = Some(PathBuf::from(next_arg(&mut args, "--run")?)),
                "--help" | "-h" => return Err(io::Error::other(usage())),
                _ => return Err(io::Error::other(format!("unknown argument {arg}\n{}", usage()))),
            }
        }
        Ok(Self {
            run_path: run_path.ok_or_else(|| io::Error::other(format!("missing value for --run\n{}", usage())))?,
        })
    }
}

/// Read the value following a replay CLI flag.
fn next_arg(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, io::Error> {
    args.next()
        .ok_or_else(|| io::Error::other(format!("missing value for {name}\n{}", usage())))
}

/// Return replay CLI usage.
fn usage() -> &'static str {
    "usage: replay --run RUN_DIR"
}

/// Validated artifacts required to compose one replay execution.
struct ReplayRun {
    /// Exact deterministic core configuration captured by the live run.
    core: crate::config::CoreConfig,
    /// First global sequence expected from the merged input streams.
    first_input_sequence: u64,
    /// Recorded control input when that endpoint received frames.
    control_input: Option<PathBuf>,
    /// Recorded gateway input when that endpoint received frames.
    gateway_input: Option<PathBuf>,
    /// Four-digit identifier assigned to this replay output.
    output_id: String,
    /// Fresh directory receiving control, gateway, and market-data output.
    output_dir: PathBuf,
}

/// Validate a run directory and reserve its next replay output directory.
fn open_replay_run(run_dir: &Path) -> Result<ReplayRun, ReplayRunError> {
    let manifest = read_manifest(run_dir)?;
    let input_dir = run_dir.join("input");
    if !input_dir.is_dir() {
        return Err(ReplayRunError::InvalidLayout(format!(
            "missing input directory: {}",
            input_dir.display()
        )));
    }
    let control_path = input_dir.join("control.bin");
    let gateway_path = input_dir.join("gateway.bin");
    // an absent endpoint file means that endpoint had no recorded frames; the
    // file backend still participates as an already exhausted input.
    let control_input = control_path.is_file().then_some(control_path);
    let gateway_input = gateway_path.is_file().then_some(gateway_path);
    if control_input.is_none() && gateway_input.is_none() {
        return Err(ReplayRunError::InvalidLayout(format!(
            "missing control.bin or gateway.bin: {}",
            input_dir.display()
        )));
    }
    let (output_id, output_dir) = create_next_output_dir(&run_dir.join("output"))?;
    Ok(ReplayRun {
        core: manifest.core,
        first_input_sequence: manifest.first_input_sequence,
        control_input,
        gateway_input,
        output_id,
        output_dir,
    })
}

/// Read and validate the versioned manifest in a run directory.
fn read_manifest(run_dir: &Path) -> Result<RunManifest, ReplayRunError> {
    let path = run_dir.join("run.yaml");
    let text = fs::read_to_string(&path).map_err(ReplayRunError::Io)?;
    let manifest = serde_yaml::from_str::<RunManifest>(&text).map_err(ReplayRunError::Parse)?;
    if manifest.format_version != RUN_MANIFEST_VERSION {
        return Err(ReplayRunError::UnsupportedVersion(manifest.format_version));
    }
    if manifest.first_input_sequence == 0 {
        return Err(ReplayRunError::InvalidLayout(
            "first_input_sequence must be greater than zero".to_owned(),
        ));
    }
    manifest
        .core
        .resolve()
        .map_err(|error| ReplayRunError::InvalidConfig(format!("{error:?}")))?;
    Ok(manifest)
}

/// Reserve the next four-digit replay output directory without overwriting one.
fn create_next_output_dir(output_root: &Path) -> Result<(String, PathBuf), ReplayRunError> {
    fs::create_dir_all(output_root).map_err(ReplayRunError::Io)?;
    let mut max_id = 0_u32;
    for entry in fs::read_dir(output_root).map_err(ReplayRunError::Io)? {
        let entry = entry.map_err(ReplayRunError::Io)?;
        if !entry.file_type().map_err(ReplayRunError::Io)?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        // only directories written by this naming scheme influence the next
        // identifier; unrelated artifacts do not make replay unusable.
        if name.len() == 4
            && name.bytes().all(|byte| byte.is_ascii_digit())
            && let Ok(id) = name.parse::<u32>()
        {
            max_id = max_id.max(id);
        }
    }
    let output_id = format!("{:04}", max_id.checked_add(1).ok_or(ReplayRunError::OutputSequenceOverflow)?);
    let output_dir = output_root.join(&output_id);
    fs::create_dir(&output_dir).map_err(ReplayRunError::Io)?;
    Ok((output_id, output_dir))
}

/// Failure while validating or preparing replay artifacts.
#[derive(Debug)]
enum ReplayRunError {
    /// A run-directory or output-directory operation failed.
    Io(std::io::Error),
    /// The manifest could not be decoded as the expected YAML shape.
    Parse(serde_yaml::Error),
    /// The manifest uses an unsupported persisted schema version.
    UnsupportedVersion(u16),
    /// The recorded core configuration is no longer valid.
    InvalidConfig(String),
    /// Required directories, inputs, or sequence metadata are invalid.
    InvalidLayout(String),
    /// No further four-digit output identifier can be represented.
    OutputSequenceOverflow,
}

impl std::fmt::Display for ReplayRunError {
    /// Format a caller-facing replay artifact failure.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(f, "replay run I/O failed: {error}"),
            Self::Parse(error) => write!(f, "failed to parse run manifest: {error}"),
            Self::UnsupportedVersion(version) => write!(f, "unsupported run manifest version: {version}"),
            Self::InvalidConfig(error) => write!(f, "invalid recorded core config: {error}"),
            Self::InvalidLayout(error) => f.write_str(error),
            Self::OutputSequenceOverflow => f.write_str("replay output sequence overflow"),
        }
    }
}

impl std::error::Error for ReplayRunError {}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::config::CoreConfig;

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "ignition-replay-{name}-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).expect("time").as_nanos()
        ))
    }

    #[test]
    fn manifest_round_trip_preserves_resolved_core() {
        let root = temp_dir("manifest");
        fs::create_dir(&root).expect("root");
        fs::create_dir(root.join("input")).expect("input");
        fs::write(root.join("input/gateway.bin"), []).expect("gateway");
        let core = CoreConfig::load("configs/server.yaml").expect("core");
        let manifest = RunManifest {
            format_version: RUN_MANIFEST_VERSION,
            first_input_sequence: 7,
            core: core.clone(),
        };
        fs::write(root.join("run.yaml"), serde_yaml::to_string(&manifest).expect("serialize")).expect("write");

        let manifest = read_manifest(&root).expect("read manifest");

        assert_eq!(manifest.first_input_sequence, 7);
        assert_eq!(manifest.core, core);
        fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn unsupported_manifest_version_is_rejected() {
        let root = temp_dir("version");
        fs::create_dir(&root).expect("root");
        let core = CoreConfig::load("configs/server.yaml").expect("core");
        let manifest = RunManifest {
            format_version: 99,
            first_input_sequence: 1,
            core,
        };
        fs::write(root.join("run.yaml"), serde_yaml::to_string(&manifest).expect("serialize")).expect("write");

        assert!(matches!(read_manifest(&root), Err(ReplayRunError::UnsupportedVersion(99))));
        fs::remove_dir_all(root).expect("cleanup");
    }
}
