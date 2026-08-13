use std::{fs, path::Path};

use serde::Deserialize;

use crate::{
    config::{ConfigError, ServerConfig},
    domain::rd::InstrumentSpec,
};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TradingClientConfig {
    #[serde(default = "default_server_config_path")]
    pub server_config_path: String,
    #[serde(default = "default_gateway_addr")]
    pub gateway_addr: String,
    #[serde(default)]
    pub market_data: MarketDataClientConfig,
    #[serde(default = "default_buy_account")]
    pub default_buy_account: u32,
    #[serde(default = "default_sell_account")]
    pub default_sell_account: u32,
    #[serde(default)]
    pub traffic_source: TrafficSourceConfig,
    #[serde(default)]
    pub binance: BinanceConfig,
}

impl TradingClientConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, TradingClientConfigError> {
        let text = fs::read_to_string(path).map_err(TradingClientConfigError::Read)?;
        let config = serde_yaml::from_str::<Self>(&text).map_err(TradingClientConfigError::Parse)?;
        config.instrument().map_err(TradingClientConfigError::Server)?;
        Ok(config)
    }

    pub fn instrument(&self) -> Result<InstrumentSpec, ConfigError> {
        ServerConfig::load(&self.server_config_path).and_then(|config| config.validate().map_err(ConfigError::Validate))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct MarketDataClientConfig {
    #[serde(default = "default_md_bind_addr")]
    pub bind_addr: String,
    #[serde(default = "default_md_group_addr")]
    pub group_addr: String,
}

impl Default for MarketDataClientConfig {
    fn default() -> Self {
        Self {
            bind_addr: default_md_bind_addr(),
            group_addr: default_md_group_addr(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TrafficSourceConfig {
    #[serde(default)]
    pub kind: TrafficSourceKind,
}

impl Default for TrafficSourceConfig {
    fn default() -> Self {
        Self {
            kind: TrafficSourceKind::Noop,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrafficSourceKind {
    #[default]
    Noop,
    Binance,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct BinanceConfig {
    #[serde(default = "default_binance_symbol")]
    pub symbol: String,
    #[serde(default = "default_binance_levels")]
    pub levels: usize,
    #[serde(default = "default_binance_speed_ms")]
    pub speed_ms: u64,
    #[serde(default = "default_binance_reconnect_backoff_ms")]
    pub reconnect_backoff_ms: u64,
}

impl Default for BinanceConfig {
    fn default() -> Self {
        Self {
            symbol: default_binance_symbol(),
            levels: default_binance_levels(),
            speed_ms: default_binance_speed_ms(),
            reconnect_backoff_ms: default_binance_reconnect_backoff_ms(),
        }
    }
}

#[derive(Debug)]
pub enum TradingClientConfigError {
    Read(std::io::Error),
    Parse(serde_yaml::Error),
    Server(ConfigError),
}

impl std::fmt::Display for TradingClientConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read(error) => write!(f, "failed to read trading client config: {error}"),
            Self::Parse(error) => write!(f, "failed to parse trading client config: {error}"),
            Self::Server(error) => write!(f, "failed to load server reference data: {error:?}"),
        }
    }
}

impl std::error::Error for TradingClientConfigError {}

fn default_server_config_path() -> String {
    "configs/server.yaml".to_owned()
}

fn default_gateway_addr() -> String {
    "127.0.0.1:19000".to_owned()
}

fn default_md_bind_addr() -> String {
    "0.0.0.0:19002".to_owned()
}

fn default_md_group_addr() -> String {
    "239.255.0.1:19002".to_owned()
}

const fn default_buy_account() -> u32 {
    1
}

const fn default_sell_account() -> u32 {
    2
}

fn default_binance_symbol() -> String {
    "btcusdt".to_owned()
}

const fn default_binance_levels() -> usize {
    20
}

const fn default_binance_speed_ms() -> u64 {
    100
}

const fn default_binance_reconnect_backoff_ms() -> u64 {
    1000
}
