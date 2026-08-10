//! Runtime orchestration boundary.
//!
//! The runtime will connect gateway input, risk checks, engine execution,
//! market-data emission, and control-plane state in an explicit poll-driven
//! loop. Milestone 1 only defines ownership.

use crate::{
    domain::trading::{TradingEvent, order::OrderEntry},
    engine::{Engine, EngineConfig, EngineError},
    market_data::MarketDataRuntime,
};

/// Runtime shell for orchestration-owned state.
#[derive(Debug)]
pub struct Runtime {
    /// Matching engine state and execution-report sequencer.
    engine: Engine,
    /// Market-data shaper and market-data sequencer.
    market_data: MarketDataRuntime,
}

impl Runtime {
    /// Create a runtime with separate matching and market-data state.
    pub fn new(config: EngineConfig) -> Result<Self, EngineError> {
        Ok(Self {
            engine: Engine::new(config)?,
            market_data: MarketDataRuntime::default(),
        })
    }

    /// Execute one normalized order-entry command through the runtime-owned
    /// matching and market-data components.
    pub fn execute(
        &mut self,
        entry: OrderEntry,
        events: &mut Vec<TradingEvent>,
    ) -> Result<(), EngineError> {
        self.engine.execute(entry, &mut self.market_data, events)
    }

    /// Return the runtime-owned matching engine.
    pub fn engine(&self) -> &Engine {
        &self.engine
    }
}
