//! Ignition is a single-symbol, deterministic CLOB matching engine.
//!
//! Milestone 1 establishes the architecture and protocol contract. Later
//! milestones fill in matching, risk, market-data, replay, and client behavior.

pub mod config;
pub mod control;
pub mod domain;
pub mod engine;
pub mod gateway;
pub mod market_data;
pub mod protocol;
pub mod risk;
pub mod runtime;
