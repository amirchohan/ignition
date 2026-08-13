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
    io::IOConfig,
    market_data::MarketDataFeedConfig,
    risk::RiskConfig,
};

/// Server configuration loaded from `configs/server.yaml`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ServerConfig {
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
    pub fn validate(&self) -> ControlConfig {
        ControlConfig {
            heartbeat_timeout_ns: self.heartbeat_timeout_ns,
            io: self.backend.validate(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ControlBackendConfig {
    Noop,
    Tcp {
        bind_addr: String,
        max_frame_len: usize,
        max_pending_write_bytes: usize,
    },
}

impl ControlBackendConfig {
    fn validate(&self) -> IOConfig {
        match self {
            Self::Noop => IOConfig::Noop,
            Self::Tcp {
                bind_addr,
                max_frame_len,
                max_pending_write_bytes,
            } => IOConfig::Tcp {
                bind_addr: bind_addr.clone(),
                max_frame_len: *max_frame_len,
                max_pending_write_bytes: *max_pending_write_bytes,
                record_path: None,
            },
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
    pub fn validate(&self) -> GatewayConfig {
        GatewayConfig {
            io: self.backend.validate(),
            max_messages_per_poll: self.max_messages_per_poll,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GatewayBackendConfig {
    Noop,
    Tcp {
        bind_addr: String,
        max_frame_len: usize,
        max_pending_write_bytes: usize,
    },
}

impl GatewayBackendConfig {
    fn validate(&self) -> IOConfig {
        match self {
            Self::Noop => IOConfig::Noop,
            Self::Tcp {
                bind_addr,
                max_frame_len,
                max_pending_write_bytes,
            } => IOConfig::Tcp {
                bind_addr: bind_addr.clone(),
                max_frame_len: *max_frame_len,
                max_pending_write_bytes: *max_pending_write_bytes,
                record_path: None,
            },
        }
    }
}

/// Market-data multicast feed runtime configuration loaded from YAML.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MarketDataRuntimeConfig {
    Noop,
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
    pub fn validate(&self) -> MarketDataFeedConfig {
        MarketDataFeedConfig {
            io: match self {
                Self::Noop => IOConfig::Noop,
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

    fn parse_with_required_defaults(input: &str) -> ServerConfig {
        parse(&format!("{input}{}", required_non_target_config()))
    }

    fn required_non_target_config() -> &'static str {
        r#"
engine:
  storage_min_price: 1
  storage_max_price: 1000000
  page_size: 1024
  max_resting_orders: 16384
  max_account_id: 65535
  max_active_pages: 1024
risk:
  max_open_orders_per_account: 1000
  max_open_notional_per_account: 10000000000000000
  startup_reference_price: 10000000
  price_band_bps: 1000
control:
  type: tcp
  bind_addr: "127.0.0.1:19001"
  heartbeat_timeout_ns: 0
  max_frame_len: 4096
  max_pending_write_bytes: 1048576
gateway:
  type: tcp
  bind_addr: "127.0.0.1:19000"
  max_frame_len: 4096
  max_pending_write_bytes: 1048576
  max_messages_per_poll: 1024
market_data:
  type: udp
  bind_addr: "0.0.0.0:0"
  group_addr: "239.255.0.1:19002"
  multicast_ttl: 1
  multicast_loop_v4: true
  max_datagram_len: 4096
  max_pending_datagram_bytes: 1048576
"#
    }

    #[test]
    fn parses_and_validates_reference_data() {
        let config = parse_with_required_defaults(
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
        let config = parse_with_required_defaults(
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
        let mut yaml = r#"
reference_data:
  instrument:
    instrument_id: 65536
    base_asset_id: 1
    quote_asset_id: 2
    price_decimals: 2
    qty_decimals: 8
    price_tick: 1
    qty_lot: 1
"#
        .to_owned();
        yaml.push_str(required_non_target_config());
        let error = serde_yaml::from_str::<ServerConfig>(&yaml).expect_err("instrument id should exceed u16");

        assert!(error.to_string().contains("invalid value"));
    }

    #[test]
    fn rejects_missing_runtime_config_sections() {
        let error = serde_yaml::from_str::<ServerConfig>(
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
        )
        .expect_err("runtime config sections should be required");

        assert!(error.to_string().contains("missing field"));
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
risk:
  max_open_orders_per_account: 1000
  max_open_notional_per_account: 10000000000000000
  startup_reference_price: 100
  price_band_bps: 1000
control:
  type: tcp
  bind_addr: "127.0.0.1:19001"
  heartbeat_timeout_ns: 0
  max_frame_len: 4096
  max_pending_write_bytes: 1048576
gateway:
  type: tcp
  bind_addr: "127.0.0.1:19000"
  max_frame_len: 4096
  max_pending_write_bytes: 1048576
  max_messages_per_poll: 1024
market_data:
  type: udp
  bind_addr: "0.0.0.0:0"
  group_addr: "239.255.0.1:19002"
  multicast_ttl: 1
  multicast_loop_v4: true
  max_datagram_len: 4096
  max_pending_datagram_bytes: 1048576
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
engine:
  storage_min_price: 1
  storage_max_price: 1000000
  page_size: 1024
  max_resting_orders: 16384
  max_account_id: 65535
  max_active_pages: 1024
risk:
  max_open_orders_per_account: 1000
  max_open_notional_per_account: 10000000000000000
  startup_reference_price: 10000000
  price_band_bps: 1000
control:
  type: tcp
  bind_addr: "127.0.0.1:9011"
  heartbeat_timeout_ns: 500
  max_frame_len: 8192
  max_pending_write_bytes: 16384
gateway:
  type: tcp
  bind_addr: "127.0.0.1:19000"
  max_frame_len: 4096
  max_pending_write_bytes: 1048576
  max_messages_per_poll: 1024
market_data:
  type: udp
  bind_addr: "0.0.0.0:0"
  group_addr: "239.255.0.1:19002"
  multicast_ttl: 1
  multicast_loop_v4: true
  max_datagram_len: 4096
  max_pending_datagram_bytes: 1048576
"#,
        );

        let control = config.validate_control();
        assert_eq!(control.heartbeat_timeout_ns, 500);
        let IOConfig::Tcp {
            bind_addr,
            max_frame_len,
            max_pending_write_bytes,
            record_path,
        } = control.io
        else {
            panic!("control io");
        };
        assert_eq!(bind_addr, "127.0.0.1:9011");
        assert_eq!(max_frame_len, 8192);
        assert_eq!(max_pending_write_bytes, 16384);
        assert_eq!(record_path, None);
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
engine:
  storage_min_price: 1
  storage_max_price: 1000000
  page_size: 1024
  max_resting_orders: 16384
  max_account_id: 65535
  max_active_pages: 1024
risk:
  max_open_orders_per_account: 1000
  max_open_notional_per_account: 10000000000000000
  startup_reference_price: 10000000
  price_band_bps: 1000
control:
  type: tcp
  bind_addr: "127.0.0.1:19001"
  heartbeat_timeout_ns: 0
  max_frame_len: 4096
  max_pending_write_bytes: 1048576
gateway:
  type: tcp
  bind_addr: "127.0.0.1:9010"
  max_frame_len: 8192
  max_pending_write_bytes: 32768
  max_messages_per_poll: 64
market_data:
  type: udp
  bind_addr: "0.0.0.0:0"
  group_addr: "239.255.0.1:19002"
  multicast_ttl: 1
  multicast_loop_v4: true
  max_datagram_len: 4096
  max_pending_datagram_bytes: 1048576
"#,
        );

        let gateway = config.validate_gateway();
        let IOConfig::Tcp {
            bind_addr,
            max_frame_len,
            max_pending_write_bytes,
            record_path,
        } = gateway.io
        else {
            panic!("gateway io");
        };
        assert_eq!(bind_addr, "127.0.0.1:9010");
        assert_eq!(max_frame_len, 8192);
        assert_eq!(max_pending_write_bytes, 32768);
        assert_eq!(record_path, None);
        assert_eq!(gateway.max_messages_per_poll, 64);
    }

    #[test]
    fn parses_noop_endpoint_backend_types() {
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
engine:
  storage_min_price: 1
  storage_max_price: 1000000
  page_size: 1024
  max_resting_orders: 16384
  max_account_id: 65535
  max_active_pages: 1024
risk:
  max_open_orders_per_account: 1000
  max_open_notional_per_account: 10000000000000000
  startup_reference_price: 10000000
  price_band_bps: 1000
control:
  type: noop
  heartbeat_timeout_ns: 0
gateway:
  type: noop
  max_messages_per_poll: 1024
market_data:
  type: noop
"#,
        );

        assert!(matches!(config.validate_control().io, IOConfig::Noop));
        assert!(matches!(config.validate_gateway().io, IOConfig::Noop));
        assert!(matches!(config.validate_market_data().io, IOConfig::Noop));
    }
}
