//! Ignition is a single-symbol, deterministic CLOB matching engine.
//!
//! The crate keeps protocol, I/O, runtime orchestration, risk, matching,
//! market-data, replay, benchmark, and client concerns behind explicit typed
//! boundaries.

pub mod client;
pub mod config;
pub mod control;
pub mod domain;
pub mod engine;
pub mod gateway;
pub mod io;
pub mod market_data;
pub mod metrics;
pub mod protocol;
pub mod risk;
pub mod runtime;
