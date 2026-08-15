//! Operating-mode selection and shared input lifecycle policy.
//!
//! This module is the boundary between thin binaries and the common runtime.
//! Live, replay, and benchmark modules choose configuration and IO adapters;
//! [`InputLifecycle`] supplies the sequencing rules used by those adapters and
//! by the runtime's idle/termination logic.
//!
//! # Main components
//!
//! - [`InputLifecycle`] owns the next accepted global input sequence and
//!   distinguishes unbounded live input from finite ordered input.
//! - `RunManifest` is the private serialized contract written by `live` and
//!   validated by `replay`.
//! - [`live`], [`replay`], and [`benchmark`] compose their mode-specific edges
//!   before calling the shared runtime builder.
//! - [`init_tracing`] installs the process-wide subscriber used by thin binary
//!   entry points.
//!
//! # Boundaries and invariants
//!
//! The lifecycle is passed through control and gateway polling so both inputs
//! share one strictly monotonic sequence. Live input assigns sequence numbers;
//! replay input must present the exact expected number. Mode composition may
//! change transports, but it never changes matching, risk, or run-loop policy.

use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;

use crate::{config::CoreConfig, io::IoError};

pub mod benchmark;
pub mod live;
pub mod replay;

/// Input sequencing and completion policy shared by every runtime mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputLifecycle {
    /// Unbounded network input that assigns a sequence as each frame is accepted.
    Live {
        /// Sequence number to assign to the next accepted live frame.
        next_input_seq: u64,
    },
    /// Finite recorded input that must arrive in its persisted global order.
    Replay {
        /// Sequence number required from the next replay record.
        expected_input_seq: u64,
    },
}

impl Default for InputLifecycle {
    /// Start a live lifecycle at the first valid global input sequence.
    fn default() -> Self {
        Self::Live { next_input_seq: 1 }
    }
}

impl InputLifecycle {
    /// Return the stable lifecycle label used in startup logs.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Live { .. } => "online",
            Self::Replay { .. } => "finite-ordered",
        }
    }

    /// Return whether idle, exhausted inputs may terminate the runtime.
    pub const fn is_finite(self) -> bool {
        matches!(self, Self::Replay { .. })
    }

    /// Assign and advance the next live input sequence.
    pub fn accept_live_frame(&mut self) -> Result<u64, IoError> {
        match self {
            Self::Live { next_input_seq } => {
                let current = *next_input_seq;
                // sequence zero is reserved, and wrapping would make recorded
                // input ambiguous, so exhaustion is reported before mutation.
                *next_input_seq = next_input_seq.checked_add(1).ok_or(IoError::ReplaySequenceMismatch {
                    expected: current,
                    actual: u64::MAX,
                })?;
                Ok(current)
            }
            Self::Replay { .. } => Err(IoError::LiveInputInReplay),
        }
    }

    /// Return the sequence required from the next finite input record.
    pub fn expected_replay_input_seq(&self) -> Result<u64, IoError> {
        match *self {
            Self::Replay { expected_input_seq } => Ok(expected_input_seq),
            Self::Live { .. } => Err(IoError::ReplayExpectedInLive),
        }
    }

    /// Validate and consume one finite input sequence.
    pub fn accept_replay_frame(&mut self, input_seq: u64) -> Result<(), IoError> {
        match self {
            Self::Replay { expected_input_seq } => {
                if *expected_input_seq != input_seq {
                    return Err(IoError::ReplaySequenceMismatch {
                        expected: *expected_input_seq,
                        actual: input_seq,
                    });
                }
                // advance only after equality is established so a rejected
                // record cannot move the shared replay cursor.
                *expected_input_seq = expected_input_seq.checked_add(1).ok_or(IoError::ReplaySequenceMismatch {
                    expected: input_seq,
                    actual: u64::MAX,
                })?;
                Ok(())
            }
            Self::Live { .. } => Err(IoError::ReplayExpectedInLive),
        }
    }

    /// Return the sequence that must be assigned or accepted next.
    pub(crate) const fn current_sequence(self) -> u64 {
        match self {
            Self::Live { next_input_seq } => next_input_seq,
            Self::Replay { expected_input_seq } => expected_input_seq,
        }
    }
}

/// Current on-disk run-manifest schema version.
const RUN_MANIFEST_VERSION: u16 = 1;

/// Shared persisted contract between live recording and deterministic replay.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
struct RunManifest {
    /// Version used to reject incompatible manifest layouts.
    format_version: u16,
    /// First global sequence expected in the recorded input streams.
    first_input_sequence: u64,
    /// Exact core configuration used when the input was recorded.
    core: CoreConfig,
}

/// Install environment-filtered process tracing when no subscriber exists.
pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}
