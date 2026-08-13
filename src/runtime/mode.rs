//! Runtime live/replay input sequencing mode.

use crate::io::IoError;

/// Runtime input mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Live { next_input_seq: u64 },
    Replay { expected_input_seq: u64 },
}

impl Default for Mode {
    fn default() -> Self {
        Self::Live { next_input_seq: 1 }
    }
}

impl Mode {
    pub const fn is_replay(self) -> bool {
        matches!(self, Self::Replay { .. })
    }

    pub fn accept_live_frame(&mut self) -> Result<u64, IoError> {
        match self {
            Self::Live { next_input_seq } => {
                let current = *next_input_seq;
                *next_input_seq = next_input_seq.checked_add(1).ok_or(IoError::ReplaySequenceMismatch {
                    expected: current,
                    actual: u64::MAX,
                })?;
                Ok(current)
            }
            Self::Replay { .. } => Err(IoError::LiveInputInReplay),
        }
    }

    pub fn expected_replay_input_seq(&self) -> Result<u64, IoError> {
        match *self {
            Self::Replay { expected_input_seq } => Ok(expected_input_seq),
            Self::Live { .. } => Err(IoError::ReplayExpectedInLive),
        }
    }

    pub fn accept_replay_frame(&mut self, input_seq: u64) -> Result<(), IoError> {
        match self {
            Self::Replay { expected_input_seq } => {
                if *expected_input_seq != input_seq {
                    return Err(IoError::ReplaySequenceMismatch {
                        expected: *expected_input_seq,
                        actual: input_seq,
                    });
                }
                *expected_input_seq = expected_input_seq.checked_add(1).ok_or(IoError::ReplaySequenceMismatch {
                    expected: input_seq,
                    actual: u64::MAX,
                })?;
                Ok(())
            }
            Self::Live { .. } => Err(IoError::ReplayExpectedInLive),
        }
    }
}
