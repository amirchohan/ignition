//! Cold-path configuration loading and validation.
//!
//! `CoreConfig` contains settings that can affect deterministic engine output.
//! `LiveConfig` contains deployment-only transport and recording settings.

use std::{
    env, fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{
    domain::{DomainError, DomainResult, Price, rd::InstrumentSpec},
    engine::EngineConfig,
    risk::RiskConfig,
};

/// Canonical deterministic engine configuration shared by every runtime mode.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct CoreConfig {
    pub reference_data: ReferenceDataConfig,
    pub engine: EngineRuntimeConfig,
    pub risk: RiskRuntimeConfig,
    pub runtime: RuntimeTuningConfig,
}

impl CoreConfig {
    /// Load and fully validate a canonical core configuration.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path).map_err(ConfigError::Read)?;
        let config = serde_yaml::from_str::<Self>(&text).map_err(ConfigError::Parse)?;
        config.resolve().map_err(ConfigError::Validate)?;
        Ok(config)
    }

    /// Resolve parsed scalar settings into domain-ready runtime configuration.
    pub fn resolve(&self) -> DomainResult<ResolvedCoreConfig> {
        let instrument = self.reference_data.instrument.validate()?;
        Ok(ResolvedCoreConfig {
            instrument,
            engine: self.engine.validate(&instrument)?,
            risk: self.risk.validate(&instrument)?,
            control: self.runtime.control,
            gateway: self.runtime.gateway,
        })
    }

    /// Return validated instrument reference data for client normalization.
    pub fn instrument(&self) -> DomainResult<InstrumentSpec> {
        self.reference_data.instrument.validate()
    }
}

/// Fully validated core settings consumed by the shared runtime builder.
#[derive(Debug, Clone, Copy)]
pub struct ResolvedCoreConfig {
    pub instrument: InstrumentSpec,
    pub engine: EngineConfig,
    pub risk: RiskConfig,
    pub control: ControlRuntimeConfig,
    pub gateway: GatewayRuntimeConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ReferenceDataConfig {
    pub instrument: InstrumentConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
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
    fn validate(self) -> DomainResult<InstrumentSpec> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct EngineRuntimeConfig {
    pub storage_min_price: u64,
    pub storage_max_price: u64,
    pub page_size: u32,
    pub max_resting_orders: u32,
    pub max_account_id: u32,
    pub max_active_pages: u32,
}

impl EngineRuntimeConfig {
    fn validate(self, instrument: &InstrumentSpec) -> DomainResult<EngineConfig> {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct RiskRuntimeConfig {
    pub max_open_orders_per_account: u32,
    pub max_open_notional_per_account: u128,
    pub startup_reference_price: u64,
    pub price_band_bps: u32,
}

impl RiskRuntimeConfig {
    fn validate(self, instrument: &InstrumentSpec) -> DomainResult<RiskConfig> {
        RiskConfig::new(
            self.max_open_orders_per_account,
            self.max_open_notional_per_account,
            Price::new(self.startup_reference_price, instrument)?,
            self.price_band_bps,
        )
    }
}

/// Polling and control policy shared by every runtime mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct RuntimeTuningConfig {
    pub control: ControlRuntimeConfig,
    pub gateway: GatewayRuntimeConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct ControlRuntimeConfig {
    pub heartbeat_timeout_ns: u64,
    pub max_messages_per_poll: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub struct GatewayRuntimeConfig {
    pub max_messages_per_poll: usize,
}

/// Live-only transport and recording configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LiveConfig {
    pub control: LiveTcpEndpointConfig,
    pub gateway: LiveTcpEndpointConfig,
    pub market_data: LiveMarketDataConfig,
    pub recording: RecordingConfig,
}

impl LiveConfig {
    /// Load live deployment settings and apply explicit environment overrides.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path).map_err(ConfigError::Read)?;
        let mut config = serde_yaml::from_str::<Self>(&text).map_err(ConfigError::Parse)?;
        if let Ok(value) = env::var("IGNITION_CONTROL_LISTEN_ADDR") {
            config.control.listen_addr = value;
        }
        if let Ok(value) = env::var("IGNITION_GATEWAY_LISTEN_ADDR") {
            config.gateway.listen_addr = value;
        }
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.control.listen_addr.is_empty() || self.gateway.listen_addr.is_empty() {
            return Err(ConfigError::Invalid("live listen addresses must not be empty"));
        }
        if self.control.max_pending_write_bytes == 0 || self.gateway.max_pending_write_bytes == 0 {
            return Err(ConfigError::Invalid("TCP pending-write capacities must be greater than zero"));
        }
        if self.market_data.max_pending_datagram_bytes == 0 {
            return Err(ConfigError::Invalid("market-data pending capacity must be greater than zero"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LiveTcpEndpointConfig {
    pub listen_addr: String,
    pub max_pending_write_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LiveMarketDataConfig {
    pub bind_addr: String,
    pub group_addr: String,
    pub multicast_ttl: u32,
    pub multicast_loop_v4: bool,
    pub max_pending_datagram_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RecordingConfig {
    Noop,
    File { root_dir: PathBuf },
}

/// Configuration load, parse, or validation failure.
#[derive(Debug)]
pub enum ConfigError {
    Read(std::io::Error),
    Parse(serde_yaml::Error),
    Validate(DomainError),
    Invalid(&'static str),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(error) => write!(f, "failed to read config: {error}"),
            Self::Parse(error) => write!(f, "failed to parse config: {error}"),
            Self::Validate(error) => write!(f, "failed to validate config: {error:?}"),
            Self::Invalid(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<DomainError> for ConfigError {
    fn from(error: DomainError) -> Self {
        Self::Validate(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_core_config_is_fully_valid() {
        let config = CoreConfig::load("configs/server.yaml").expect("core config");
        let resolved = config.resolve().expect("resolved config");

        assert_eq!(resolved.instrument.instrument_id.raw(), 1001);
        assert_eq!(resolved.risk.startup_reference_price.raw(), 6_500_000);
    }
}
