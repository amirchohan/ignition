//! Deterministic single-writer orchestration for the matching engine.
//!
//! This module owns the run loop that moves typed work through control, gateway,
//! risk, matching, market-data, and output components. It deliberately does not
//! parse configuration, select transports, decode wire layouts, or own order-
//! book data structures; neighboring modules retain those responsibilities.
//!
//! # Main components
//!
//! - [`Runtime`] owns every mutable subsystem and executes the fixed
//!   control-before-gateway phase order.
//! - [`RuntimeError`] identifies the subsystem that failed during execution.
//! - [`InputLifecycle`] tracks either live input numbering or finite replay
//!   progress and tells the runtime when an idle iteration may terminate.
//! - `OrderProcessor` coordinates control gating, risk, matching, and report
//!   publication for one normalized order command.
//! - `SnapshotProcessor` streams a sequenced control snapshot without building
//!   a snapshot-sized intermediate collection.
//!
//! # Data flow and invariants
//!
//! `mode` selects adapters and `build` constructs the component graph. During a
//! run, control is always polled before order entry, all execution and control
//! output shares one monotonic sequencer, and queued market data is flushed
//! before the next control phase. Shutdown drains the book and flushes every
//! sink before `terminal` is set, preserving deterministic output order.

mod build;
pub mod mode;
mod order;
mod snapshot;

pub use build::RuntimeBuildError;
pub use mode::InputLifecycle;
/// Compatibility name for the runtime input lifecycle policy.
#[doc(hidden)]
pub type Mode = InputLifecycle;

use self::order::{OrderProcessOutcome, OrderProcessor};
use self::snapshot::SnapshotProcessor;
use crate::{
    clock::Clock,
    control::{ControlError, ControlRuntime},
    domain::{
        Sequencer,
        control::{ControlRequest, KillSwitchAction},
    },
    engine::{CapacityResource, Engine, EngineConfig, EngineError},
    gateway::{GatewayError, GatewayPollControl, GatewayPollError, GatewayRuntime},
    io::IoError,
    market_data::{MarketDataError, MarketDataRuntime},
    metrics::{MetricsRuntime, MetricsSnapshot},
    risk::{RiskConfig, RiskRuntime},
};

/// Poll-driven runtime orchestrator.
#[derive(Debug)]
pub struct Runtime {
    /// Process-local monotonic clock shared by runtime-adjacent timing paths.
    clock: Clock,
    /// Online or finite-ordered input lifecycle.
    input: InputLifecycle,
    /// Control-plane state and ingress.
    control: ControlRuntime,
    /// Pre-match risk gate and reference-price state.
    risk: RiskRuntime,
    /// Order-entry ingress and trading egress.
    gateway: GatewayRuntime,
    /// Market-data multicast egress.
    market_data: MarketDataRuntime,
    /// Matching engine state.
    engine: Engine,
    /// Runtime-adjacent hot-path metrics.
    metrics: MetricsRuntime,
    /// Shared deterministic execution/control sequencer.
    sequencer: Sequencer,
    /// Set after deterministic drain-and-shutdown completes.
    terminal: bool,
}

impl Runtime {
    /// Build the single-writer runtime from pre-opened adapters and validated configs.
    pub fn new(
        input: InputLifecycle,
        engine_config: EngineConfig,
        risk_config: RiskConfig,
        control: ControlRuntime,
        gateway: GatewayRuntime,
        market_data: MarketDataRuntime,
        metrics: MetricsRuntime,
    ) -> Result<Self, RuntimeError> {
        tracing::info!("runtime setup with {} input", input.label());
        Ok(Self {
            clock: Clock::new(),
            input,
            control,
            gateway,
            market_data,
            risk: RiskRuntime::new(risk_config),
            engine: Engine::new(engine_config)?,
            metrics,
            sequencer: Sequencer::default(),
            terminal: false,
        })
    }

    /// Return a post-run metrics snapshot.
    pub fn metrics_snapshot(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Run the deterministic control-then-gateway phase loop until shutdown completes.
    pub fn run(&mut self) -> Result<(), RuntimeError> {
        tracing::info!("starting matching engine loop");
        loop {
            if self.terminal {
                return Ok(());
            }

            // check admin commands first. A halt or shutdown must be seen before we read more orders.
            let control_progress = self.poll_control()?;
            if self.terminal {
                return Ok(());
            }

            // then process order entry and flush any market data caused by it.
            let gateway_progress = self.poll_gateway()?;

            if !self.handle_idle_iteration(control_progress, gateway_progress)? {
                return Ok(());
            }
        }
    }

    /// Poll control ingress and apply any control-owned runtime consequences.
    fn poll_control(&mut self) -> Result<bool, RuntimeError> {
        let mut shutdown_requested = false;
        let mut handled_control_request = false;

        let control_progress = {
            let Runtime {
                clock,
                control,
                market_data,
                risk,
                engine,
                sequencer,
                input,
                ..
            } = self;
            // poll every ready control frame before touching order entry. Normal
            // control state is updated inside `ControlRuntime`; this closure only
            // handles work that needs runtime-owned engine or market-data state.
            control.poll(clock, input, sequencer, |request, publisher, sequencer| {
                handled_control_request = true;
                match request {
                    ControlRequest::KillSwitch(request) => {
                        // a shutdown request means "drain after this control batch."
                        if request.action == KillSwitchAction::Shutdown {
                            shutdown_requested = true;
                        }
                    }
                    ControlRequest::Heartbeat(_) => {}
                    ControlRequest::Snapshot(snapshot) => {
                        SnapshotProcessor::process(engine, risk, market_data, sequencer, publisher, snapshot)?;
                    }
                }
                Ok(())
            })?
        };

        if shutdown_requested {
            self.complete_shutdown()?;
            return Ok(true);
        }

        Ok(control_progress || handled_control_request)
    }

    /// Poll order-entry ingress, matching output, and market-data egress.
    fn poll_gateway(&mut self) -> Result<bool, RuntimeError> {
        let mut handled_order = false;
        let mut capacity_shutdown = None;
        let gateway_progress = {
            let Runtime {
                clock,
                input,
                control,
                gateway,
                market_data,
                risk,
                engine,
                sequencer,
                metrics,
                ..
            } = self;
            // process frames in ingress order and stop the batch immediately
            // after a capacity-triggering command has been fully accepted.
            gateway
                .poll_controlled(clock, input, sequencer, metrics, |entry, publisher, sequencer| {
                    handled_order = true;
                    let outcome = OrderProcessor::process(control, risk, engine, sequencer, publisher, market_data, entry)?;
                    Ok::<GatewayPollControl, RuntimeError>(match outcome {
                        OrderProcessOutcome::Continue => GatewayPollControl::Continue,
                        OrderProcessOutcome::GracefulShutdown(resource) => {
                            capacity_shutdown = Some(resource);
                            GatewayPollControl::Stop
                        }
                    })
                })
                .map_err(|error| match error {
                    GatewayPollError::Gateway(error) => RuntimeError::Gateway(error),
                    GatewayPollError::Handler(error) => error,
                })?
        };

        if let Some(resource) = capacity_shutdown {
            tracing::error!(
                resource = capacity_resource_label(resource),
                "book capacity exhausted; initiating graceful shutdown"
            );
            self.complete_shutdown()?;
            return Ok(true);
        }

        // matching queues market-data packets while orders are processed. flush
        // them before leaving the gateway phase so the next control poll sees a
        // fully published view of the work just completed.
        let market_data_progress = self.market_data.flush_pending()?;
        Ok(gateway_progress || market_data_progress || handled_order)
    }

    /// Drain book state and flush every resulting output before termination.
    fn complete_shutdown(&mut self) -> Result<(), RuntimeError> {
        {
            let mut emit_report = |report| self.gateway.emit_execution_report(report);
            self.engine
                .drain_all(&mut self.sequencer, &mut self.market_data, &mut emit_report)?;
            if let Some(price) = self.market_data.take_reference_price_update() {
                self.risk.observe_trade_price(price);
            }
        }
        // deferred sink errors are surfaced before the final flush so a
        // failed drain cannot be hidden by marking the runtime terminal.
        self.gateway.check_deferred_error()?;
        self.market_data.check_deferred_error()?;
        self.gateway.flush_pending()?;
        self.market_data.flush_pending()?;
        self.terminal = true;
        Ok(())
    }

    /// Handle a run-loop iteration that may have made no IO/runtime progress.
    ///
    /// Returns `false` when replay input is fully drained and the runtime should
    /// exit normally.
    fn handle_idle_iteration(&mut self, control_progress: bool, gateway_progress: bool) -> Result<bool, RuntimeError> {
        if control_progress || gateway_progress {
            return Ok(true);
        }

        if self.input.is_finite() {
            if self.control.replay_input_exhausted()? && self.gateway.replay_input_exhausted()? {
                return Ok(false);
            }
            return Err(RuntimeError::Io(IoError::ReplayStalled {
                expected_input_seq: self.input.current_sequence(),
            }));
        }

        // live mode yields when both ingress endpoints are idle.
        std::thread::yield_now();
        Ok(true)
    }
}

/// Return the stable log label for an exhausted fixed-capacity resource.
const fn capacity_resource_label(resource: CapacityResource) -> &'static str {
    match resource {
        CapacityResource::RestingOrders => "resting_orders",
        CapacityResource::ActivePages => "active_pages",
    }
}

/// Runtime orchestration failure.
#[derive(Debug)]
pub enum RuntimeError {
    /// Matching or deterministic sequencing failed.
    Engine(EngineError),
    /// Control ingress, publication, or control handling failed.
    Control(ControlError),
    /// Gateway ingress or execution-report publication failed.
    Gateway(GatewayError),
    /// Market-data publication failed.
    MarketData(MarketDataError),
    /// Input lifecycle or transport progress failed.
    Io(IoError),
}

impl From<EngineError> for RuntimeError {
    /// Preserve an engine failure as a runtime failure.
    fn from(error: EngineError) -> Self {
        Self::Engine(error)
    }
}

impl From<ControlError> for RuntimeError {
    /// Preserve a control failure as a runtime failure.
    fn from(error: ControlError) -> Self {
        Self::Control(error)
    }
}

impl From<GatewayError> for RuntimeError {
    /// Preserve a gateway failure as a runtime failure.
    fn from(error: GatewayError) -> Self {
        Self::Gateway(error)
    }
}

impl From<MarketDataError> for RuntimeError {
    /// Preserve a market-data failure as a runtime failure.
    fn from(error: MarketDataError) -> Self {
        Self::MarketData(error)
    }
}

impl From<IoError> for RuntimeError {
    /// Preserve an input lifecycle or transport failure as a runtime failure.
    fn from(error: IoError) -> Self {
        Self::Io(error)
    }
}

#[cfg(test)]
mod tests;
