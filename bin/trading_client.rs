//! Trading client entry point.

use ignition::client::trading::{TradingClientConfig, run_trading_client};
use tracing_subscriber::EnvFilter;

fn main() {
    init_tracing();
    match TradingClientConfig::load("configs/trading-client.yaml")
        .map_err(Into::into)
        .and_then(run_trading_client)
    {
        Ok(()) => {}
        Err(error) => {
            tracing::error!("trading client failed: {error}");
            std::process::exit(1);
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
