//! Control-plane boundary.
//!
//! Control owns operational commands such as kill switch, snapshot, and
//! heartbeat state. Exact control semantics are deferred, but the runtime polls
//! control first so later kill-switch behavior has the correct insertion point.

/// Result of one control poll phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ControlPoll {
    /// Whether this poll made forward progress.
    pub made_progress: bool,
    /// Whether this control adapter has no more work.
    pub closed: bool,
}

/// Control-plane runtime shell.
#[derive(Debug, Default)]
pub struct ControlRuntime;

impl ControlRuntime {
    /// Poll control-plane input. Milestone 5 keeps this as a closed no-op.
    pub const fn poll(&mut self) -> ControlPoll {
        ControlPoll {
            made_progress: false,
            closed: true,
        }
    }
}
