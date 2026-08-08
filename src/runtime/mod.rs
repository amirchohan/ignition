//! Runtime orchestration boundary.
//!
//! The runtime will connect gateway input, risk checks, engine execution,
//! market-data emission, and control-plane state in an explicit poll-driven
//! loop. Milestone 1 only defines ownership.

/// Runtime shell for future orchestration.
#[derive(Debug, Default)]
pub struct Runtime;
