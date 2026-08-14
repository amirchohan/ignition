//! Runtime construction from server config.
//!
//! Startup is intentionally outside the poll loop. It loads and validates
//! config, opens endpoint runtimes, and then hands fully constructed components
//! to `Runtime::new`.

use crate::{
    config::{ConfigError, ServerConfig},
    control::{ControlError, ControlRuntime},
    domain::{DomainError, rd::InstrumentSpec},
    engine::{EngineConfig, EngineError},
    gateway::{GatewayError, GatewayRuntime},
    market_data::{MarketDataError, MarketDataRuntime},
    metrics::{MetricsBackend, MetricsRuntime},
    risk::RiskConfig,
};

use super::{Mode, Runtime, RuntimeError};

impl Runtime {
    /// Open endpoints from an already parsed server config and construct the runtime.
    pub fn from_server_config(mode: Mode, server_config: ServerConfig) -> Result<Self, RuntimeBuildError> {
        let resolved = ResolvedRuntimeConfig::from_server_config(mode, &server_config)?;
        resolved.log();
        let endpoints = RuntimeEndpoints::open(&server_config, resolved.instrument)?;

        Runtime::new(
            resolved.mode,
            resolved.engine,
            resolved.risk,
            endpoints.control,
            endpoints.gateway,
            endpoints.market_data,
            MetricsRuntime::new(resolved.metrics, 0),
        )
        .map_err(RuntimeBuildError::Runtime)
    }
}

struct ResolvedRuntimeConfig {
    mode: Mode,
    instrument: InstrumentSpec,
    engine: EngineConfig,
    risk: RiskConfig,
    metrics: MetricsBackend,
}

impl ResolvedRuntimeConfig {
    /// Validate static runtime configuration into strongly typed components.
    fn from_server_config(mode: Mode, server_config: &ServerConfig) -> Result<Self, RuntimeBuildError> {
        Ok(Self {
            mode,
            instrument: server_config.validate().map_err(RuntimeBuildError::ReferenceData)?,
            engine: server_config.validate_engine().map_err(RuntimeBuildError::EngineConfig)?,
            risk: server_config.validate_risk().map_err(RuntimeBuildError::RiskConfig)?,
            metrics: server_config.metrics,
        })
    }

    /// Emit startup configuration details before endpoints are opened.
    fn log(&self) {
        tracing::info!(
            "reference data config: instrument_id={}|base_asset_id={}|quote_asset_id={}|price_decimals={}|qty_decimals={}|price_tick={}|qty_lot={}",
            self.instrument.instrument_id.raw(),
            self.instrument.base_asset_id.raw(),
            self.instrument.quote_asset_id.raw(),
            self.instrument.price_decimals,
            self.instrument.qty_decimals,
            self.instrument.price_tick,
            self.instrument.qty_lot
        );
        tracing::info!(
            "engine config: storage_min_price={}|storage_max_price={}|page_size={}|max_resting_orders={}|max_account_id={}|max_active_pages={}",
            self.engine.storage_min_price().raw(),
            self.engine.storage_max_price().raw(),
            self.engine.page_size(),
            self.engine.max_resting_orders(),
            self.engine.max_account_id(),
            self.engine.max_active_pages()
        );
        tracing::info!(
            "risk config: max_open_orders_per_account={}|max_open_notional_per_account={}|startup_reference_price={}|price_band_bps={}",
            self.risk.max_open_orders_per_account,
            self.risk.max_open_notional_per_account,
            self.risk.startup_reference_price.raw(),
            self.risk.price_band_bps
        );
        tracing::info!("metrics config: backend={:?}", self.metrics);
    }
}

struct RuntimeEndpoints {
    control: ControlRuntime,
    gateway: GatewayRuntime,
    market_data: MarketDataRuntime,
}

impl RuntimeEndpoints {
    /// Open control, gateway, and market-data endpoint runtimes.
    fn open(server_config: &ServerConfig, instrument: InstrumentSpec) -> Result<Self, RuntimeBuildError> {
        Ok(Self {
            control: ControlRuntime::new(server_config.validate_control())?,
            gateway: GatewayRuntime::new(server_config.validate_gateway(), instrument)?,
            market_data: MarketDataRuntime::new(server_config.validate_market_data())?,
        })
    }
}

/// Runtime construction failure.
#[derive(Debug)]
pub enum RuntimeBuildError {
    Config(ConfigError),
    ReferenceData(DomainError),
    EngineConfig(DomainError),
    RiskConfig(DomainError),
    Control(ControlError),
    Gateway(GatewayError),
    MarketData(MarketDataError),
    Engine(EngineError),
    Runtime(RuntimeError),
}

impl std::fmt::Display for RuntimeBuildError {
    /// Format runtime construction errors for entry-point diagnostics.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(error) => write!(f, "failed to load config: {error:?}"),
            Self::ReferenceData(error) => write!(f, "failed to validate reference data: {error:?}"),
            Self::EngineConfig(error) => write!(f, "failed to validate engine config: {error:?}"),
            Self::RiskConfig(error) => write!(f, "failed to validate risk config: {error:?}"),
            Self::Control(error) => write!(f, "failed to open control endpoint: {error:?}"),
            Self::Gateway(error) => write!(f, "failed to open gateway endpoint: {error:?}"),
            Self::MarketData(error) => write!(f, "failed to open market-data endpoint: {error:?}"),
            Self::Engine(error) => write!(f, "failed to construct engine: {error:?}"),
            Self::Runtime(error) => write!(f, "failed to construct runtime: {error:?}"),
        }
    }
}

impl std::error::Error for RuntimeBuildError {}

impl From<ConfigError> for RuntimeBuildError {
    /// Convert config loading failures into runtime construction errors.
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<ControlError> for RuntimeBuildError {
    /// Convert control endpoint failures into runtime construction errors.
    fn from(error: ControlError) -> Self {
        Self::Control(error)
    }
}

impl From<GatewayError> for RuntimeBuildError {
    /// Convert gateway endpoint failures into runtime construction errors.
    fn from(error: GatewayError) -> Self {
        Self::Gateway(error)
    }
}

impl From<MarketDataError> for RuntimeBuildError {
    /// Convert market-data endpoint failures into runtime construction errors.
    fn from(error: MarketDataError) -> Self {
        Self::MarketData(error)
    }
}

impl From<EngineError> for RuntimeBuildError {
    /// Convert engine construction failures into runtime construction errors.
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}
