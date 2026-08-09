//! Minimal server entry point.
//!
//! The executable is intentionally small in Milestone 1. Runtime wiring lands
//! behind the library boundary so future clients and replay tooling can share
//! the same protocol path.

use ignition::config::ServerConfig;

fn main() {
    match ServerConfig::load("configs/server.yaml") {
        Ok(config) => match config.validate() {
            Ok(instrument) => {
                println!(
                    "ignition server: configured instrument {} with price_tick {} and qty_lot {}",
                    instrument.instrument_id.raw(),
                    instrument.price_tick,
                    instrument.qty_lot
                );
            }
            Err(error) => {
                eprintln!("failed to validate server config: {error:?}");
                std::process::exit(1);
            }
        },
        Err(error) => {
            eprintln!("failed to load server config: {error:?}");
            std::process::exit(1);
        }
    }
}
