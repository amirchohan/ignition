use std::{env, fs, path::Path};

use serde::Deserialize;

use crate::{
    config::{ConfigError, CoreConfig},
    domain::rd::InstrumentSpec,
};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TradingClientConfig {
    pub core_config_path: String,
    pub gateway_addr: String,
    pub market_data: MarketDataClientConfig,
    pub synthetic_traffic: SyntheticTrafficConfig,
}

impl TradingClientConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, TradingClientConfigError> {
        let text = fs::read_to_string(path).map_err(TradingClientConfigError::Read)?;
        let mut config = serde_yaml::from_str::<Self>(&text).map_err(TradingClientConfigError::Parse)?;
        if let Ok(value) = env::var("IGNITION_GATEWAY_ADDR") {
            config.gateway_addr = value;
        }
        if let Ok(value) = env::var("IGNITION_SYNTHETIC_TRAFFIC") {
            match value.as_str() {
                "noop" => config.synthetic_traffic = SyntheticTrafficConfig::Noop,
                "configured" => {}
                _ => return Err(TradingClientConfigError::InvalidEnvironment(value)),
            }
        }
        config.instrument().map_err(TradingClientConfigError::Server)?;
        Ok(config)
    }

    pub fn instrument(&self) -> Result<InstrumentSpec, ConfigError> {
        CoreConfig::load(&self.core_config_path).and_then(|config| config.instrument().map_err(ConfigError::Validate))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct MarketDataClientConfig {
    pub bind_addr: String,
    pub group_addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
#[serde(tag = "type")]
pub enum SyntheticTrafficConfig {
    Noop,
    Binance {
        symbol: String,
        levels: usize,
        speed_ms: u64,
        reconnect_backoff_ms: u64,
        buys_account_id: u32,
        sells_account_id: u32,
    },
}

#[derive(Debug)]
pub enum TradingClientConfigError {
    Read(std::io::Error),
    Parse(serde_yaml::Error),
    Server(ConfigError),
    InvalidEnvironment(String),
}

impl std::fmt::Display for TradingClientConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(error) => write!(f, "failed to read trading client config: {error}"),
            Self::Parse(error) => write!(f, "failed to parse trading client config: {error}"),
            Self::Server(error) => write!(f, "failed to load server reference data: {error:?}"),
            Self::InvalidEnvironment(value) => write!(f, "invalid IGNITION_SYNTHETIC_TRAFFIC value: {value}"),
        }
    }
}

impl std::error::Error for TradingClientConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_binance_synthetic_traffic_config() {
        let config = serde_yaml::from_str::<TradingClientConfig>(
            r#"
core_config_path: "configs/server.yaml"
gateway_addr: "127.0.0.1:19000"
market_data:
  bind_addr: "0.0.0.0:19002"
  group_addr: "239.255.0.1:19002"
synthetic_traffic:
  type: binance
  symbol: "btcusdt"
  levels: 20
  speed_ms: 100
  reconnect_backoff_ms: 1000
  buys_account_id: 1
  sells_account_id: 2
"#,
        )
        .expect("config should parse");

        assert!(matches!(
            config.synthetic_traffic,
            SyntheticTrafficConfig::Binance {
                buys_account_id: 1,
                sells_account_id: 2,
                ..
            }
        ));
    }

    #[test]
    fn parses_noop_synthetic_traffic_config() {
        let config = serde_yaml::from_str::<TradingClientConfig>(
            r#"
core_config_path: "configs/server.yaml"
gateway_addr: "127.0.0.1:19000"
market_data:
  bind_addr: "0.0.0.0:19002"
  group_addr: "239.255.0.1:19002"
synthetic_traffic:
  type: noop
"#,
        )
        .expect("config should parse");

        assert!(matches!(config.synthetic_traffic, SyntheticTrafficConfig::Noop));
    }
}
