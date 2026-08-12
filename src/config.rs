//! Startup configuration loading and validation.
//!
//! Config parsing is intentionally outside hot paths. Runtime code should pass
//! the validated numeric `InstrumentSpec` into protocol normalization and core
//! components rather than reading YAML inside matching/risk logic.

use std::{fs, path::Path};

use serde::Deserialize;

use crate::{
    control::ControlConfig,
    domain::{DomainError, DomainResult, Price, rd::InstrumentSpec},
    engine::EngineConfig,
    gateway::GatewayConfig,
    market_data::MarketDataFeedConfig,
    risk::RiskConfig,
};

/// Server configuration loaded from `configs/server.yaml`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ServerConfig {
    pub reference_data: ReferenceDataConfig,
    #[serde(default)]
    pub engine: EngineRuntimeConfig,
    #[serde(default)]
    pub risk: RiskRuntimeConfig,
    #[serde(default)]
    pub control: ControlRuntimeConfig,
    #[serde(default)]
    pub gateway: GatewayRuntimeConfig,
    #[serde(default)]
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

impl Default for EngineRuntimeConfig {
    fn default() -> Self {
        Self {
            storage_min_price: 1,
            storage_max_price: 1_000_000,
            page_size: 1024,
            max_resting_orders: 16_384,
            max_account_id: 65_535,
            max_active_pages: 1024,
        }
    }
}

impl EngineRuntimeConfig {
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

impl Default for RiskRuntimeConfig {
    fn default() -> Self {
        Self {
            max_open_orders_per_account: 1_000,
            max_open_notional_per_account: 10_000_000_000_000_000,
            startup_reference_price: 10_000_000,
            price_band_bps: 1_000,
        }
    }
}

impl RiskRuntimeConfig {
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
    #[serde(default)]
    pub heartbeat_timeout_ns: u64,
    #[serde(default = "default_control_bind_addr")]
    pub bind_addr: String,
    #[serde(default = "default_control_max_frame_len")]
    pub max_frame_len: usize,
    #[serde(default = "default_control_max_pending_write_bytes")]
    pub max_pending_write_bytes: usize,
}

impl Default for ControlRuntimeConfig {
    fn default() -> Self {
        Self {
            heartbeat_timeout_ns: 0,
            bind_addr: default_control_bind_addr(),
            max_frame_len: default_control_max_frame_len(),
            max_pending_write_bytes: default_control_max_pending_write_bytes(),
        }
    }
}

impl ControlRuntimeConfig {
    pub fn validate(&self) -> ControlConfig {
        ControlConfig {
            heartbeat_timeout_ns: self.heartbeat_timeout_ns,
            bind_addr: self.bind_addr.clone(),
            max_frame_len: self.max_frame_len,
            max_pending_write_bytes: self.max_pending_write_bytes,
        }
    }
}

fn default_control_bind_addr() -> String {
    "127.0.0.1:9001".to_owned()
}

const fn default_control_max_frame_len() -> usize {
    4096
}

const fn default_control_max_pending_write_bytes() -> usize {
    1 << 20
}

/// Trading gateway runtime configuration loaded from YAML.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct GatewayRuntimeConfig {
    #[serde(default = "default_gateway_bind_addr")]
    pub bind_addr: String,
    #[serde(default = "default_gateway_max_frame_len")]
    pub max_frame_len: usize,
    #[serde(default = "default_gateway_max_pending_write_bytes")]
    pub max_pending_write_bytes: usize,
    #[serde(default = "default_gateway_max_messages_per_poll")]
    pub max_messages_per_poll: usize,
}

impl Default for GatewayRuntimeConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_gateway_bind_addr(),
            max_frame_len: default_gateway_max_frame_len(),
            max_pending_write_bytes: default_gateway_max_pending_write_bytes(),
            max_messages_per_poll: default_gateway_max_messages_per_poll(),
        }
    }
}

impl GatewayRuntimeConfig {
    pub fn validate(&self) -> GatewayConfig {
        GatewayConfig {
            bind_addr: self.bind_addr.clone(),
            max_frame_len: self.max_frame_len,
            max_pending_write_bytes: self.max_pending_write_bytes,
            max_messages_per_poll: self.max_messages_per_poll,
        }
    }
}

fn default_gateway_bind_addr() -> String {
    "127.0.0.1:9000".to_owned()
}

const fn default_gateway_max_frame_len() -> usize {
    4096
}

const fn default_gateway_max_pending_write_bytes() -> usize {
    1 << 20
}

const fn default_gateway_max_messages_per_poll() -> usize {
    1024
}

/// Market-data multicast feed runtime configuration loaded from YAML.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct MarketDataRuntimeConfig {
    #[serde(default = "default_market_data_bind_addr")]
    pub bind_addr: String,
    #[serde(default = "default_market_data_group_addr")]
    pub group_addr: String,
    #[serde(default = "default_market_data_multicast_ttl")]
    pub multicast_ttl: u32,
    #[serde(default = "default_market_data_multicast_loop_v4")]
    pub multicast_loop_v4: bool,
    #[serde(default = "default_market_data_max_datagram_len")]
    pub max_datagram_len: usize,
    #[serde(default = "default_market_data_max_pending_datagram_bytes")]
    pub max_pending_datagram_bytes: usize,
}

impl Default for MarketDataRuntimeConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_market_data_bind_addr(),
            group_addr: default_market_data_group_addr(),
            multicast_ttl: default_market_data_multicast_ttl(),
            multicast_loop_v4: default_market_data_multicast_loop_v4(),
            max_datagram_len: default_market_data_max_datagram_len(),
            max_pending_datagram_bytes: default_market_data_max_pending_datagram_bytes(),
        }
    }
}

impl MarketDataRuntimeConfig {
    pub fn validate(&self) -> MarketDataFeedConfig {
        MarketDataFeedConfig {
            bind_addr: self.bind_addr.clone(),
            group_addr: self.group_addr.clone(),
            multicast_ttl: self.multicast_ttl,
            multicast_loop_v4: self.multicast_loop_v4,
            max_datagram_len: self.max_datagram_len,
            max_pending_datagram_bytes: self.max_pending_datagram_bytes,
        }
    }
}

fn default_market_data_bind_addr() -> String {
    "0.0.0.0:0".to_owned()
}

fn default_market_data_group_addr() -> String {
    "239.255.0.1:9002".to_owned()
}

const fn default_market_data_multicast_ttl() -> u32 {
    1
}

const fn default_market_data_multicast_loop_v4() -> bool {
    true
}

const fn default_market_data_max_datagram_len() -> usize {
    4096
}

const fn default_market_data_max_pending_datagram_bytes() -> usize {
    1 << 20
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

    fn parse(input: &str) -> ServerConfig {
        serde_yaml::from_str(input).expect("config should parse")
    }

    #[test]
    fn parses_and_validates_reference_data() {
        let config = parse(
            r#"
reference_data:
  instrument:
    instrument_id: 1001
    base_asset_id: 1
    quote_asset_id: 2
    price_decimals: 2
    qty_decimals: 8
    price_tick: 1
    qty_lot: 1
"#,
        );

        let instrument = config.validate().expect("valid instrument");
        assert_eq!(instrument.instrument_id.raw(), 1001);
        assert_eq!(instrument.base_asset_id.raw(), 1);
        assert_eq!(instrument.quote_asset_id.raw(), 2);
        assert_eq!(instrument.price_decimals, 2);
        assert_eq!(instrument.qty_decimals, 8);
        assert_eq!(instrument.price_tick, 1);
        assert_eq!(instrument.qty_lot, 1);
        let engine = config.validate_engine().expect("valid engine config");
        assert_eq!(engine.page_size(), 1024);
    }

    #[test]
    fn rejects_invalid_reference_data() {
        let config = parse(
            r#"
reference_data:
  instrument:
    instrument_id: 1001
    base_asset_id: 1
    quote_asset_id: 1
    price_decimals: 2
    qty_decimals: 8
    price_tick: 1
    qty_lot: 1
"#,
        );

        assert_eq!(config.validate(), Err(DomainError::InvalidInstrumentSpec));
    }

    #[test]
    fn rejects_reference_data_ids_that_exceed_u16() {
        let error = serde_yaml::from_str::<ServerConfig>(
            r#"
reference_data:
  instrument:
    instrument_id: 65536
    base_asset_id: 1
    quote_asset_id: 2
    price_decimals: 2
    qty_decimals: 8
    price_tick: 1
    qty_lot: 1
"#,
        )
        .expect_err("instrument id should exceed u16");

        assert!(error.to_string().contains("invalid value"));
    }

    #[test]
    fn parses_and_validates_engine_config() {
        let config = parse(
            r#"
reference_data:
  instrument:
    instrument_id: 1001
    base_asset_id: 1
    quote_asset_id: 2
    price_decimals: 2
    qty_decimals: 8
    price_tick: 5
    qty_lot: 1
engine:
  storage_min_price: 10
  storage_max_price: 1000
  page_size: 1024
  max_resting_orders: 100
  max_account_id: 1000
  max_active_pages: 10
"#,
        );

        let engine = config.validate_engine().expect("valid engine config");
        assert_eq!(engine.storage_min_price().raw(), 10);
        assert_eq!(engine.storage_max_price().raw(), 1000);
        assert_eq!(engine.max_resting_orders(), 100);
        assert_eq!(engine.max_account_id(), 1000);
    }

    #[test]
    fn parses_and_validates_control_config() {
        let config = parse(
            r#"
reference_data:
  instrument:
    instrument_id: 1001
    base_asset_id: 1
    quote_asset_id: 2
    price_decimals: 2
    qty_decimals: 8
    price_tick: 1
    qty_lot: 1
control:
  bind_addr: "127.0.0.1:9011"
  heartbeat_timeout_ns: 500
  max_frame_len: 8192
  max_pending_write_bytes: 16384
"#,
        );

        let control = config.validate_control();
        assert_eq!(control.heartbeat_timeout_ns, 500);
        assert_eq!(control.bind_addr, "127.0.0.1:9011");
        assert_eq!(control.max_frame_len, 8192);
        assert_eq!(control.max_pending_write_bytes, 16384);
    }

    #[test]
    fn parses_and_validates_gateway_config() {
        let config = parse(
            r#"
reference_data:
  instrument:
    instrument_id: 1001
    base_asset_id: 1
    quote_asset_id: 2
    price_decimals: 2
    qty_decimals: 8
    price_tick: 1
    qty_lot: 1
gateway:
  bind_addr: "127.0.0.1:9010"
  max_frame_len: 8192
  max_pending_write_bytes: 32768
  max_messages_per_poll: 64
"#,
        );

        let gateway = config.validate_gateway();
        assert_eq!(gateway.bind_addr, "127.0.0.1:9010");
        assert_eq!(gateway.max_frame_len, 8192);
        assert_eq!(gateway.max_pending_write_bytes, 32768);
        assert_eq!(gateway.max_messages_per_poll, 64);
    }
}
