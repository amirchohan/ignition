//! Startup configuration loading and validation.
//!
//! Config parsing is intentionally outside hot paths. Runtime code should pass
//! the validated numeric `InstrumentSpec` into protocol normalization and core
//! components rather than reading YAML inside matching/risk logic.

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;

use crate::{
    control::ControlConfig,
    domain::{DomainError, DomainResult, Price, rd::InstrumentSpec},
    engine::EngineConfig,
    gateway::GatewayConfig,
    io::IOConfig,
    market_data::MarketDataFeedConfig,
    metrics::MetricsBackend,
    risk::RiskConfig,
};

/// Server configuration loaded from `configs/server.yaml`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ServerConfig {
    pub mode: RuntimeModeConfig,
    pub metrics: MetricsBackend,
    pub reference_data: ReferenceDataConfig,
    pub engine: EngineRuntimeConfig,
    pub risk: RiskRuntimeConfig,
    pub control: ControlRuntimeConfig,
    pub gateway: GatewayRuntimeConfig,
    pub market_data: MarketDataRuntimeConfig,
}

impl ServerConfig {
    /// Load and validate server configuration from disk.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path).map_err(ConfigError::Read)?;
        let config = serde_yaml::from_str::<Self>(&text).map_err(ConfigError::Parse)?;
        config.validate().map_err(ConfigError::Validate)?;
        Ok(config)
    }

    /// Validate this parsed config into core-ready reference data.
    pub fn validate(&self) -> DomainResult<InstrumentSpec> {
        self.reference_data.instrument.validate()
    }

    /// Validate matching-engine capacity and storage-envelope settings.
    pub fn validate_engine(&self) -> DomainResult<EngineConfig> {
        let instrument = self.validate()?;
        self.engine.validate(&instrument)
    }

    /// Validate risk settings against the configured instrument scale.
    pub fn validate_risk(&self) -> DomainResult<RiskConfig> {
        let instrument = self.validate()?;
        self.risk.validate(&instrument)
    }

    /// Validate control-plane runtime settings.
    pub fn validate_control(&self) -> ControlConfig {
        self.control.validate()
    }

    /// Validate trading gateway runtime settings.
    pub fn validate_gateway(&self) -> GatewayConfig {
        self.gateway.validate()
    }

    /// Validate market-data multicast feed runtime settings.
    pub fn validate_market_data(&self) -> MarketDataFeedConfig {
        self.market_data.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ReferenceDataConfig {
    pub instrument: InstrumentConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct InstrumentConfig {
    pub instrument_id: u16,
    pub base_asset_id: u16,
    pub quote_asset_id: u16,
    pub price_decimals: u8,
    pub qty_decimals: u8,
    pub price_tick: u64,
    pub qty_lot: u64,
}

impl InstrumentConfig {
    /// Validate instrument reference data into core domain types.
    pub fn validate(self) -> DomainResult<InstrumentSpec> {
        InstrumentSpec::new(
            self.instrument_id,
            self.base_asset_id,
            self.quote_asset_id,
            self.price_decimals,
            self.qty_decimals,
            self.price_tick,
            self.qty_lot,
        )
    }
}

/// Matching-engine storage and capacity configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct EngineRuntimeConfig {
    pub storage_min_price: u64,
    pub storage_max_price: u64,
    pub page_size: u32,
    pub max_resting_orders: u32,
    pub max_account_id: u32,
    pub max_active_pages: u32,
}

impl EngineRuntimeConfig {
    /// Validate storage bounds and engine capacity settings.
    pub fn validate(self, instrument: &InstrumentSpec) -> DomainResult<EngineConfig> {
        EngineConfig::new(
            *instrument,
            self.storage_min_price,
            self.storage_max_price,
            self.page_size,
            self.max_resting_orders,
            self.max_account_id,
            self.max_active_pages,
        )
    }
}

/// Risk policy configuration loaded from YAML.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct RiskRuntimeConfig {
    pub max_open_orders_per_account: u32,
    pub max_open_notional_per_account: u128,
    pub startup_reference_price: u64,
    pub price_band_bps: u32,
}

impl RiskRuntimeConfig {
    /// Validate risk settings against the configured instrument scale.
    pub fn validate(self, instrument: &InstrumentSpec) -> DomainResult<RiskConfig> {
        RiskConfig::new(
            self.max_open_orders_per_account,
            self.max_open_notional_per_account,
            Price::new(self.startup_reference_price, instrument)?,
            self.price_band_bps,
        )
    }
}

/// Control-plane runtime configuration loaded from YAML.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ControlRuntimeConfig {
    pub heartbeat_timeout_ns: u64,
    #[serde(flatten)]
    pub backend: ControlBackendConfig,
}

impl ControlRuntimeConfig {
    /// Convert control-plane config into endpoint runtime config.
    pub fn validate(&self) -> ControlConfig {
        ControlConfig {
            heartbeat_timeout_ns: self.heartbeat_timeout_ns,
            io: self.backend.validate(),
        }
    }

    /// Return the configured control frame capacity.
    pub fn frame_capacity(&self) -> usize {
        self.backend.frame_capacity()
    }

    /// Set TCP live-input recording path, returning whether the backend was TCP.
    pub fn set_tcp_record_path(&mut self, path: PathBuf) -> bool {
        self.backend.set_tcp_record_path(path)
    }

    /// Switch the control endpoint to file input/output while preserving frame capacity.
    pub fn set_file_io(&mut self, input_path: Option<PathBuf>, output_path: Option<PathBuf>) {
        self.backend = ControlBackendConfig::File {
            input_path,
            output_path,
            max_frame_len: self.frame_capacity(),
        };
    }

    /// Return the configured control file output path, if any.
    pub fn file_output_path(&self) -> Option<&Path> {
        self.backend.file_output_path()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlBackendConfig {
    Noop,
    File {
        #[serde(default)]
        input_path: Option<PathBuf>,
        #[serde(default)]
        output_path: Option<PathBuf>,
        max_frame_len: usize,
    },
    Tcp {
        bind_addr: String,
        max_frame_len: usize,
        max_pending_write_bytes: usize,
        #[serde(default)]
        record_path: Option<PathBuf>,
    },
}

impl ControlBackendConfig {
    /// Convert the control backend config into generic IO config.
    fn validate(&self) -> IOConfig {
        match self {
            Self::Noop => IOConfig::Noop,
            Self::File {
                input_path,
                output_path,
                max_frame_len,
            } => IOConfig::File {
                input_path: input_path.clone(),
                output_path: output_path.clone(),
                max_frame_len: *max_frame_len,
            },
            Self::Tcp {
                bind_addr,
                max_frame_len,
                max_pending_write_bytes,
                record_path,
            } => IOConfig::Tcp {
                bind_addr: bind_addr.clone(),
                max_frame_len: *max_frame_len,
                max_pending_write_bytes: *max_pending_write_bytes,
                record_path: record_path.clone(),
            },
        }
    }

    /// Return the maximum frame length for this backend.
    fn frame_capacity(&self) -> usize {
        match self {
            Self::Noop => 4096,
            Self::File { max_frame_len, .. } | Self::Tcp { max_frame_len, .. } => *max_frame_len,
        }
    }

    /// Set TCP live-input recording path, returning whether this backend is TCP.
    fn set_tcp_record_path(&mut self, path: PathBuf) -> bool {
        match self {
            Self::Tcp { record_path, .. } => {
                *record_path = Some(path);
                true
            }
            Self::Noop | Self::File { .. } => false,
        }
    }

    /// Return the file output path when this backend is file based.
    fn file_output_path(&self) -> Option<&Path> {
        match self {
            Self::File {
                output_path: Some(path), ..
            } => Some(path.as_path()),
            Self::File { .. } | Self::Noop | Self::Tcp { .. } => None,
        }
    }
}

/// Trading gateway runtime configuration loaded from YAML.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct GatewayRuntimeConfig {
    pub max_messages_per_poll: usize,
    #[serde(flatten)]
    pub backend: GatewayBackendConfig,
}

impl GatewayRuntimeConfig {
    /// Convert gateway config into endpoint runtime config.
    pub fn validate(&self) -> GatewayConfig {
        GatewayConfig {
            io: self.backend.validate(),
            max_messages_per_poll: self.max_messages_per_poll,
        }
    }

    /// Return the configured gateway frame capacity.
    pub fn frame_capacity(&self) -> usize {
        self.backend.frame_capacity()
    }

    /// Set TCP live-input recording path, returning whether the backend was TCP.
    pub fn set_tcp_record_path(&mut self, path: PathBuf) -> bool {
        self.backend.set_tcp_record_path(path)
    }

    /// Switch the gateway endpoint to file input/output while preserving frame capacity.
    pub fn set_file_io(&mut self, input_path: Option<PathBuf>, output_path: Option<PathBuf>) {
        self.backend = GatewayBackendConfig::File {
            input_path,
            output_path,
            max_frame_len: self.frame_capacity(),
        };
    }

    /// Return the configured gateway file output path, if any.
    pub fn file_output_path(&self) -> Option<&Path> {
        self.backend.file_output_path()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GatewayBackendConfig {
    Noop,
    File {
        #[serde(default)]
        input_path: Option<PathBuf>,
        #[serde(default)]
        output_path: Option<PathBuf>,
        max_frame_len: usize,
    },
    Tcp {
        bind_addr: String,
        max_frame_len: usize,
        max_pending_write_bytes: usize,
        #[serde(default)]
        record_path: Option<PathBuf>,
    },
}

impl GatewayBackendConfig {
    /// Convert the gateway backend config into generic IO config.
    fn validate(&self) -> IOConfig {
        match self {
            Self::Noop => IOConfig::Noop,
            Self::File {
                input_path,
                output_path,
                max_frame_len,
            } => IOConfig::File {
                input_path: input_path.clone(),
                output_path: output_path.clone(),
                max_frame_len: *max_frame_len,
            },
            Self::Tcp {
                bind_addr,
                max_frame_len,
                max_pending_write_bytes,
                record_path,
            } => IOConfig::Tcp {
                bind_addr: bind_addr.clone(),
                max_frame_len: *max_frame_len,
                max_pending_write_bytes: *max_pending_write_bytes,
                record_path: record_path.clone(),
            },
        }
    }

    /// Return the maximum frame length for this backend.
    fn frame_capacity(&self) -> usize {
        match self {
            Self::Noop => 4096,
            Self::File { max_frame_len, .. } | Self::Tcp { max_frame_len, .. } => *max_frame_len,
        }
    }

    /// Set TCP live-input recording path, returning whether this backend is TCP.
    fn set_tcp_record_path(&mut self, path: PathBuf) -> bool {
        match self {
            Self::Tcp { record_path, .. } => {
                *record_path = Some(path);
                true
            }
            Self::Noop | Self::File { .. } => false,
        }
    }

    /// Return the file output path when this backend is file based.
    fn file_output_path(&self) -> Option<&Path> {
        match self {
            Self::File {
                output_path: Some(path), ..
            } => Some(path.as_path()),
            Self::File { .. } | Self::Noop | Self::Tcp { .. } => None,
        }
    }
}

/// Market-data multicast feed runtime configuration loaded from YAML.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MarketDataRuntimeConfig {
    Noop,
    File {
        #[serde(default)]
        output_path: Option<PathBuf>,
        max_frame_len: usize,
    },
    Udp {
        bind_addr: String,
        group_addr: String,
        multicast_ttl: u32,
        multicast_loop_v4: bool,
        max_datagram_len: usize,
        max_pending_datagram_bytes: usize,
    },
}

impl MarketDataRuntimeConfig {
    /// Return the configured market-data frame/datagram capacity.
    pub fn frame_capacity(&self) -> usize {
        match self {
            Self::Noop => 4096,
            Self::File { max_frame_len, .. } => *max_frame_len,
            Self::Udp { max_datagram_len, .. } => *max_datagram_len,
        }
    }

    /// Switch market data to file output while preserving frame capacity.
    pub fn set_file_output(&mut self, output_path: Option<PathBuf>) {
        *self = Self::File {
            output_path,
            max_frame_len: self.frame_capacity(),
        };
    }

    /// Return the configured market-data file output path, if any.
    pub fn file_output_path(&self) -> Option<&Path> {
        match self {
            Self::File {
                output_path: Some(path), ..
            } => Some(path.as_path()),
            Self::File { .. } | Self::Noop | Self::Udp { .. } => None,
        }
    }

    /// Convert market-data config into endpoint runtime config.
    pub fn validate(&self) -> MarketDataFeedConfig {
        MarketDataFeedConfig {
            io: match self {
                Self::Noop => IOConfig::Noop,
                Self::File {
                    output_path,
                    max_frame_len,
                } => IOConfig::File {
                    input_path: None,
                    output_path: output_path.clone(),
                    max_frame_len: *max_frame_len,
                },
                Self::Udp {
                    bind_addr,
                    group_addr,
                    multicast_ttl,
                    multicast_loop_v4,
                    max_datagram_len,
                    max_pending_datagram_bytes,
                } => IOConfig::Udp {
                    bind_addr: bind_addr.clone(),
                    group_addr: group_addr.clone(),
                    multicast_ttl: *multicast_ttl,
                    multicast_loop_v4: *multicast_loop_v4,
                    max_datagram_len: *max_datagram_len,
                    max_pending_datagram_bytes: *max_pending_datagram_bytes,
                },
            },
        }
    }
}

/// Runtime input sequencing mode loaded from YAML.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RuntimeModeConfig {
    Live {
        next_input_seq: u64,
        #[serde(default)]
        record_path_dir: Option<PathBuf>,
    },
    Replay {
        expected_input_seq: u64,
        record_path_dir: PathBuf,
    },
}

/// Config load/parse/validation errors.
#[derive(Debug)]
pub enum ConfigError {
    Read(std::io::Error),
    Parse(serde_yaml::Error),
    Validate(DomainError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_config_parses_in_memory_metrics_backend() {
        let config = serde_yaml::from_str::<ServerConfig>(config_yaml()).expect("config");

        assert_eq!(config.metrics, MetricsBackend::InMemory);
    }

    fn config_yaml() -> &'static str {
        r#"
mode:
  type: live
  next_input_seq: 1
metrics:
  type: in_memory
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
  max_resting_orders: 100000
  max_account_id: 100000
  max_active_pages: 4096
risk:
  max_open_orders_per_account: 1000
  max_open_notional_per_account: 100000000000000000
  startup_reference_price: 6500000
  price_band_bps: 1000
control:
  type: noop
  heartbeat_timeout_ns: 0
gateway:
  type: noop
  max_messages_per_poll: 1024
market_data:
  type: noop
"#
    }
}
