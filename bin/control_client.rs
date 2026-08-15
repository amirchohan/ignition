#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unreachable))]

//! Control client entry point.

use std::{env, io};

use ignition::client::control::{ControlClientConfig, run_control_client};
use tracing_subscriber::EnvFilter;

fn main() {
    init_tracing();
    let config_path = match parse_config_path(env::args().skip(1), "configs/control-client.yaml") {
        Ok(path) => path,
        Err(error) => {
            tracing::error!("control client failed: {error}");
            std::process::exit(1);
        }
    };
    match ControlClientConfig::load(config_path)
        .map_err(Into::into)
        .and_then(run_control_client)
    {
        Ok(()) => {}
        Err(error) => {
            tracing::error!("control client failed: {error}");
            std::process::exit(1);
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

fn parse_config_path(args: impl Iterator<Item = String>, default_path: &'static str) -> Result<String, io::Error> {
    let mut config_path = default_path.to_owned();
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => return Err(io::Error::other(usage())),
            "--config" => {
                config_path = args
                    .next()
                    .ok_or_else(|| io::Error::other(format!("missing value for --config\n{}", usage())))?;
            }
            other => return Err(io::Error::other(format!("unknown argument {other}\n{}", usage()))),
        }
    }
    Ok(config_path)
}

fn usage() -> &'static str {
    "usage: control-client [--config PATH]"
}
