//! Server entry point.

use ignition::{config::ServerConfig, control::ControlRuntime, gateway::GatewayRuntime, market_data::MarketDataRuntime, runtime::Runtime};

fn main() {
    match ServerConfig::load("configs/server.yaml") {
        Ok(config) => run(config),
        Err(error) => {
            eprintln!("failed to load server config: {error:?}");
            std::process::exit(1);
        }
    }
}

fn run(config: ServerConfig) {
    let instrument = match config.validate() {
        Ok(instrument) => instrument,
        Err(error) => {
            eprintln!("failed to validate reference data: {error:?}");
            std::process::exit(1);
        }
    };
    let engine_config = match config.validate_engine() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("failed to validate engine config: {error:?}");
            std::process::exit(1);
        }
    };
    let risk_config = match config.validate_risk() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("failed to validate risk config: {error:?}");
            std::process::exit(1);
        }
    };
    let control_config = config.validate_control();
    let control = match ControlRuntime::new(control_config.clone()) {
        Ok(control) => control,
        Err(error) => {
            eprintln!("failed to open control listener {}: {error:?}", control_config.bind_addr);
            std::process::exit(1);
        }
    };
    let gateway_config = config.validate_gateway();
    let gateway = match GatewayRuntime::new(gateway_config.clone(), instrument) {
        Ok(gateway) => gateway,
        Err(error) => {
            eprintln!("failed to open trading gateway listener {}: {error:?}", gateway_config.bind_addr);
            std::process::exit(1);
        }
    };
    let market_data_config = config.validate_market_data();
    let market_data = match MarketDataRuntime::new(market_data_config.clone()) {
        Ok(feed) => feed,
        Err(error) => {
            eprintln!(
                "failed to open market-data multicast feed {}: {error:?}",
                market_data_config.group_addr
            );
            std::process::exit(1);
        }
    };

    let mut runtime = match Runtime::new(engine_config, risk_config, control, gateway, market_data) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("failed to construct runtime: {error:?}");
            std::process::exit(1);
        }
    };

    println!(
        "ignition server: gateway listening on {}, control listening on {}, market-data multicast on {}",
        gateway_config.bind_addr, control_config.bind_addr, market_data_config.group_addr
    );
    if let Err(error) = runtime.run() {
        eprintln!("runtime failed: {error:?}");
        std::process::exit(1);
    }
}
