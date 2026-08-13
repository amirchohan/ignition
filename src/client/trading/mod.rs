//! Interactive trading client.

mod binance;
mod commands;
mod config;
mod io;
mod protocol;
mod source;
mod tui;

pub use config::TradingClientConfig;
pub use tui::run_trading_client;
