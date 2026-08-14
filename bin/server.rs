//! Server entry point.

use std::{
    env,
    error::Error,
    fs, io,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use ignition::{
    config::{RuntimeModeConfig, ServerConfig},
    runtime::{Mode, Runtime, RuntimeBuildError},
};
use tracing::info;
use tracing_subscriber::EnvFilter;

type ServerResult<T> = Result<T, Box<dyn Error>>;

/// Start tracing and run the configured server process.
fn main() {
    init_tracing();
    if let Err(error) = run() {
        tracing::error!("server failed: {error}");
        std::process::exit(1);
    };
}

/// Initialise process-wide tracing from `RUST_LOG`, defaulting to info logs.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

/// Load server config, apply optional live replay recording, and run the runtime.
fn run() -> ServerResult<()> {
    let config_path = parse_config_path(env::args().skip(1), "configs/server.yaml")?;
    let mut config = ServerConfig::load(config_path).map_err(RuntimeBuildError::Config)?;
    prepare_config_for_live_mode(&mut config)?;
    let mode = runtime_mode_for_live(&config)?;

    // runtime construction is shared with replay so both entry points validate
    // and open endpoints through the same path.
    let mut runtime = Runtime::from_server_config(mode, config)?;
    runtime
        .run()
        .map_err(|error| io::Error::other(format!("runtime failed: {error:?}")))?;
    Ok(())
}

/// Parse the optional config path flag.
fn parse_config_path(args: impl Iterator<Item = String>, default_path: &'static str) -> ServerResult<String> {
    let mut config_path = default_path.to_owned();
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Err(io::Error::other(usage()).into()),
            "--config" => {
                config_path = args
                    .next()
                    .ok_or_else(|| io::Error::other(format!("missing value for --config\n{}", usage())))?;
            }
            other => return Err(io::Error::other(format!("unknown argument {other}\n{}", usage())).into()),
        }
    }
    Ok(config_path)
}

/// Return server CLI usage text.
fn usage() -> &'static str {
    "usage: server [--config PATH]"
}

/// Resolve live-mode sequencing state from config.
fn runtime_mode_for_live(config: &ServerConfig) -> ServerResult<Mode> {
    match &config.mode {
        RuntimeModeConfig::Live { next_input_seq, .. } if *next_input_seq != 0 => Ok(Mode::Live {
            next_input_seq: *next_input_seq,
        }),
        RuntimeModeConfig::Live { next_input_seq, .. } => {
            Err(io::Error::other(format!("invalid runtime mode sequence next_input_seq={next_input_seq}")).into())
        }
        RuntimeModeConfig::Replay { .. } => Err(io::Error::other("server requires config mode.type: live").into()),
    }
}

/// Prepare live-mode runtime config before startup.
fn prepare_config_for_live_mode(config: &mut ServerConfig) -> ServerResult<()> {
    let record_path_dir = match &config.mode {
        RuntimeModeConfig::Live {
            record_path_dir: Some(path),
            ..
        } => path.clone(),
        RuntimeModeConfig::Live { .. } | RuntimeModeConfig::Replay { .. } => return Ok(()),
    };

    let input_id = epoch_seconds()?;
    let input_dir = create_recording_input_dir(&record_path_dir, input_id)?;

    if !config.control.set_tcp_record_path(input_dir.join("control.bin")) {
        return Err(io::Error::other("control endpoint must use tcp backend for live input recording").into());
    }
    if !config.gateway.set_tcp_record_path(input_dir.join("gateway.bin")) {
        return Err(io::Error::other("gateway endpoint must use tcp backend for live input recording").into());
    }

    info!("recording input_id={input_id}");
    info!("input: {}", input_dir.display());
    Ok(())
}

/// Create the input directory for one live recording run.
fn create_recording_input_dir(record_path_dir: &Path, input_id: u64) -> ServerResult<PathBuf> {
    // The root can pre-exist across many server runs, but a specific run id
    // must be unique so replay inputs are never silently appended.
    fs::create_dir_all(record_path_dir)?;

    let run_dir = record_path_dir.join(input_id.to_string());
    match fs::create_dir(&run_dir) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("replay recording run already exists: {}", run_dir.display()),
            )
            .into());
        }
        Err(error) => return Err(error.into()),
    }

    // TCP endpoints record into the input directory expected by `just replay`.
    let input_dir = run_dir.join("input");
    fs::create_dir(&input_dir)?;
    Ok(input_dir)
}

/// Return the current Unix epoch seconds used as a live recording input id.
fn epoch_seconds() -> ServerResult<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| io::Error::other(format!("failed to generate replay input id: {error}")))?
        .as_secs())
}
