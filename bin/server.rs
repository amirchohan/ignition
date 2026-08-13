//! Server entry point.

use ignition::{
    config::{ConfigError, ServerConfig},
    control::{ControlError, ControlRuntime},
    domain::DomainError,
    engine::EngineError,
    gateway::{GatewayError, GatewayRuntime},
    market_data::{MarketDataError, MarketDataRuntime},
    runtime::{Mode, Runtime, RuntimeError},
};
use tracing_subscriber::EnvFilter;

fn main() {
    init_tracing();
    if let Err(error) = run() {
        tracing::error!("server failed: {error}");
        std::process::exit(1);
    };
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

fn run() -> Result<(), ServerError> {
    let config = ServerConfig::load("configs/server.yaml")?;
    let instrument = config.validate().map_err(ServerError::ReferenceData)?;
    let engine_config = config.validate_engine().map_err(ServerError::EngineConfig)?;
    let risk_config = config.validate_risk().map_err(ServerError::RiskConfig)?;
    tracing::info!(
        "reference data config: instrument_id={}|base_asset_id={}|quote_asset_id={}|price_decimals={}|qty_decimals={}|price_tick={}|qty_lot={}",
        instrument.instrument_id.raw(),
        instrument.base_asset_id.raw(),
        instrument.quote_asset_id.raw(),
        instrument.price_decimals,
        instrument.qty_decimals,
        instrument.price_tick,
        instrument.qty_lot
    );
    tracing::info!(
        "engine config: storage_min_price={}|storage_max_price={}|page_size={}|max_resting_orders={}|max_account_id={}|max_active_pages={}",
        engine_config.storage_min_price().raw(),
        engine_config.storage_max_price().raw(),
        engine_config.page_size(),
        engine_config.max_resting_orders(),
        engine_config.max_account_id(),
        engine_config.max_active_pages()
    );
    tracing::info!(
        "risk config: max_open_orders_per_account={}|max_open_notional_per_account={}|startup_reference_price={}|price_band_bps={}",
        risk_config.max_open_orders_per_account,
        risk_config.max_open_notional_per_account,
        risk_config.startup_reference_price.raw(),
        risk_config.price_band_bps
    );

    let control = ControlRuntime::new(config.validate_control())?;
    let gateway = GatewayRuntime::new(config.validate_gateway(), instrument)?;
    let market_data = MarketDataRuntime::new(config.validate_market_data())?;
    let mut runtime = Runtime::new(Mode::default(), engine_config, risk_config, control, gateway, market_data)?;

    runtime.run()?;
    Ok(())
}

#[derive(Debug)]
enum ServerError {
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

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Config(error) => write!(f, "failed to load config: {error:?}"),
            Self::ReferenceData(error) => write!(f, "failed to validate reference data: {error:?}"),
            Self::EngineConfig(error) => write!(f, "failed to validate engine config: {error:?}"),
            Self::RiskConfig(error) => write!(f, "failed to validate risk config: {error:?}"),
            Self::Control(error) => write!(f, "failed to open control listener: {error:?}"),
            Self::Gateway(error) => write!(f, "failed to open trading gateway listener: {error:?}"),
            Self::MarketData(error) => write!(f, "failed to open market-data multicast feed: {error:?}"),
            Self::Engine(error) => write!(f, "failed to construct runtime engine: {error:?}"),
            Self::Runtime(error) => write!(f, "runtime failed: {error:?}"),
        }
    }
}

impl std::error::Error for ServerError {}

impl From<ConfigError> for ServerError {
    fn from(error: ConfigError) -> Self {
        Self::Config(error)
    }
}

impl From<ControlError> for ServerError {
    fn from(error: ControlError) -> Self {
        Self::Control(error)
    }
}

impl From<GatewayError> for ServerError {
    fn from(error: GatewayError) -> Self {
        Self::Gateway(error)
    }
}

impl From<MarketDataError> for ServerError {
    fn from(error: MarketDataError) -> Self {
        Self::MarketData(error)
    }
}

impl From<EngineError> for ServerError {
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}

impl From<RuntimeError> for ServerError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}
