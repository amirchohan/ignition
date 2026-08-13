use std::{fs, path::Path};

use serde::Deserialize;

use crate::config::{ConfigError, ServerConfig};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ControlClientConfig {
    pub server_config_path: String,
    pub control_addr: String,
    pub heartbeat_interval_ms: u64,
    pub max_snapshot_rows: usize,
}

impl ControlClientConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ControlClientConfigError> {
        let text = fs::read_to_string(path).map_err(ControlClientConfigError::Read)?;
        let config = serde_yaml::from_str::<Self>(&text).map_err(ControlClientConfigError::Parse)?;
        ServerConfig::load(&config.server_config_path)
            .and_then(|config| config.validate().map(|_| ()).map_err(ConfigError::Validate))
            .map_err(ControlClientConfigError::Server)?;
        Ok(config)
    }
}

#[derive(Debug)]
pub enum ControlClientConfigError {
    Read(std::io::Error),
    Parse(serde_yaml::Error),
    Server(ConfigError),
}

impl std::fmt::Display for ControlClientConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(error) => write!(f, "failed to read control client config: {error}"),
            Self::Parse(error) => write!(f, "failed to parse control client config: {error}"),
            Self::Server(error) => write!(f, "failed to load server reference data: {error:?}"),
        }
    }
}

impl std::error::Error for ControlClientConfigError {}
