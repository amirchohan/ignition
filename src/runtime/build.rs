//! Shared construction of a fully wired matching-engine runtime.
//!
//! Mode modules decide where input and output come from, then pass those
//! choices to this module as a [`RuntimeSpec`]. This module owns the common
//! assembly of control, gateway, market-data, metrics, risk, and matching
//! components so live, replay, and benchmark execution cannot drift.
//!
//! # Main components
//!
//! - [`RuntimeSpec`] carries validated core configuration plus mode-selected IO
//!   and metrics backends.
//! - [`build_runtime`] consumes that specification and constructs the single
//!   [`Runtime`] used by every mode.
//! - [`RuntimeBuildError`] preserves the subsystem that rejected construction.
//!
//! # Boundaries and invariants
//!
//! Configuration parsing and mode-specific filesystem or socket choices happen
//! before this boundary. The resulting adapters are opened here, before the
//! hot run loop begins. Every mode therefore receives the same component graph,
//! initialization order, and validated engine and risk settings.

use crate::{
    config::{ConfigError, ResolvedCoreConfig},
    control::{ControlConfig, ControlError, ControlRuntime},
    domain::DomainError,
    engine::EngineError,
    gateway::{GatewayConfig, GatewayError, GatewayRuntime},
    io::IOConfig,
    market_data::{MarketDataError, MarketDataFeedConfig, MarketDataRuntime},
    metrics::{MetricsBackend, MetricsError, MetricsRuntime},
};

use super::{InputLifecycle, Runtime, RuntimeError};

/// Fully resolved cold-path inputs required to construct one runtime.
pub(crate) struct RuntimeSpec {
    /// Sequencing and completion policy for the selected input source.
    pub input: InputLifecycle,
    /// Validated instrument, engine, risk, and poll-budget configuration.
    pub core: ResolvedCoreConfig,
    /// Control-plane transport selected by the mode.
    pub control_io: IOConfig,
    /// Order-entry and execution-report transport selected by the mode.
    pub gateway_io: IOConfig,
    /// Market-data transport selected by the mode.
    pub market_data_io: IOConfig,
    /// Metrics storage used for this run.
    pub metrics: MetricsBackend,
}

/// Construct the shared runtime from a mode-resolved specification.
pub(crate) fn build_runtime(spec: RuntimeSpec) -> Result<Runtime, RuntimeBuildError> {
    log_core(&spec.core);
    let control = ControlRuntime::new(ControlConfig {
        heartbeat_timeout_ns: spec.core.control.heartbeat_timeout_ns,
        max_messages_per_poll: spec.core.control.max_messages_per_poll,
        io: spec.control_io,
    })?;
    let gateway = GatewayRuntime::new(
        GatewayConfig {
            io: spec.gateway_io,
            max_messages_per_poll: spec.core.gateway.max_messages_per_poll,
        },
        spec.core.instrument,
    )?;
    let market_data = MarketDataRuntime::new(MarketDataFeedConfig { io: spec.market_data_io })?;
    let metrics = MetricsRuntime::new(spec.metrics, 0)?;

    Runtime::new(spec.input, spec.core.engine, spec.core.risk, control, gateway, market_data, metrics).map_err(RuntimeBuildError::Runtime)
}

/// Log resolved reference data and fixed-capacity limits before execution.
fn log_core(core: &ResolvedCoreConfig) {
    tracing::info!(
        "reference data: instrument_id={}|base_asset_id={}|quote_asset_id={}|price_decimals={}|qty_decimals={}|price_tick={}|qty_lot={}",
        core.instrument.instrument_id.raw(),
        core.instrument.base_asset_id.raw(),
        core.instrument.quote_asset_id.raw(),
        core.instrument.price_decimals,
        core.instrument.qty_decimals,
        core.instrument.price_tick,
        core.instrument.qty_lot
    );
    tracing::info!(
        "engine capacity: storage_min_price={}|storage_max_price={}|page_size={}|max_resting_orders={}|max_account_id={}|max_active_pages={}",
        core.engine.storage_min_price().raw(),
        core.engine.storage_max_price().raw(),
        core.engine.page_size(),
        core.engine.max_resting_orders(),
        core.engine.max_account_id(),
        core.engine.max_active_pages()
    );
}

/// Failure while constructing the shared runtime component graph.
#[derive(Debug)]
pub enum RuntimeBuildError {
    /// Core or mode configuration could not be loaded or validated.
    Config(ConfigError),
    /// Instrument reference data violated domain constraints.
    ReferenceData(DomainError),
    /// Engine capacity or storage configuration was invalid.
    EngineConfig(DomainError),
    /// Risk configuration was invalid.
    RiskConfig(DomainError),
    /// The control runtime or its transport could not be opened.
    Control(ControlError),
    /// The gateway runtime or its transport could not be opened.
    Gateway(GatewayError),
    /// The market-data runtime or its transport could not be opened.
    MarketData(MarketDataError),
    /// The matching engine could not be constructed.
    Engine(EngineError),
    /// The selected metrics backend could not be constructed.
    Metrics(MetricsError),
    /// Final runtime assembly failed.
    Runtime(RuntimeError),
}

impl std::fmt::Display for RuntimeBuildError {
    /// Format the construction stage together with its source error.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(error) => write!(f, "failed to load config: {error}"),
            Self::ReferenceData(error) => write!(f, "failed to validate reference data: {error:?}"),
            Self::EngineConfig(error) => write!(f, "failed to validate engine config: {error:?}"),
            Self::RiskConfig(error) => write!(f, "failed to validate risk config: {error:?}"),
            Self::Control(error) => write!(f, "failed to open control endpoint: {error:?}"),
            Self::Gateway(error) => write!(f, "failed to open gateway endpoint: {error:?}"),
            Self::MarketData(error) => write!(f, "failed to open market-data endpoint: {error:?}"),
            Self::Engine(error) => write!(f, "failed to construct engine: {error:?}"),
            Self::Metrics(error) => write!(f, "failed to construct metrics: {error:?}"),
            Self::Runtime(error) => write!(f, "failed to construct runtime: {error:?}"),
        }
    }
}

impl std::error::Error for RuntimeBuildError {}

impl From<ConfigError> for RuntimeBuildError {
    /// Preserve configuration failures at the shared build boundary.
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}
impl From<ControlError> for RuntimeBuildError {
    /// Preserve control construction failures at the shared build boundary.
    fn from(error: ControlError) -> Self {
        Self::Control(error)
    }
}
impl From<GatewayError> for RuntimeBuildError {
    /// Preserve gateway construction failures at the shared build boundary.
    fn from(error: GatewayError) -> Self {
        Self::Gateway(error)
    }
}
impl From<MarketDataError> for RuntimeBuildError {
    /// Preserve market-data construction failures at the shared build boundary.
    fn from(error: MarketDataError) -> Self {
        Self::MarketData(error)
    }
}
impl From<EngineError> for RuntimeBuildError {
    /// Preserve engine construction failures at the shared build boundary.
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}
impl From<MetricsError> for RuntimeBuildError {
    /// Preserve metrics construction failures at the shared build boundary.
    fn from(error: MetricsError) -> Self {
        Self::Metrics(error)
    }
}
