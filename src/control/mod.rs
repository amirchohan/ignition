//! Control-plane boundary.
//!
//! Control owns operational commands such as kill switch, snapshot, and
//! heartbeat state. Exact heartbeat fail-fast policy is deferred to the control
//! milestone.

/// Control-plane runtime shell for future milestones.
#[derive(Debug, Default)]
pub struct ControlRuntime;
