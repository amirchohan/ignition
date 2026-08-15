#![cfg_attr(not(test), deny(clippy::unwrap_used, clippy::expect_used, clippy::panic, clippy::unreachable))]

fn main() {
    ignition::runtime::mode::init_tracing();
    if let Err(error) = ignition::runtime::mode::replay::run(std::env::args().skip(1)) {
        tracing::error!("replay failed: {error}");
        std::process::exit(1);
    }
}
