//! Server entry point.

use ignition::{
    config::ServerConfig, control::ControlRuntime, gateway::GatewayRuntime, runtime::Runtime,
};

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
            eprintln!(
                "failed to open control listener {}: {error:?}",
                control_config.bind_addr
            );
            std::process::exit(1);
        }
    };

    let mut runtime = match Runtime::new(
        engine_config,
        risk_config,
        control,
        GatewayRuntime::default(),
    ) {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("failed to construct runtime: {error:?}");
            std::process::exit(1);
        }
    };

    println!(
        "ignition server: control listening on {}",
        control_config.bind_addr
    );
    if let Err(error) = runtime.run() {
        eprintln!("runtime failed: {error:?}");
        std::process::exit(1);
    }
}
