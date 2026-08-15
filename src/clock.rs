//! Process-local monotonic clock owned by the server runtime.

use std::time::Instant;

/// Monotonic nanosecond clock shared by runtime-adjacent live-time consumers.
///
/// The value has no wall-clock or Unix-epoch meaning. `Runtime` owns one clock
/// and lends it to live ingress, benchmark scheduling, and metrics so every
/// sampled timestamp has the same origin.
#[derive(Debug)]
pub struct Clock {
    started_at: Instant,
}

impl Default for Clock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock {
    /// Capture the monotonic origin for one runtime instance.
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
        }
    }

    /// Return saturated nanoseconds elapsed since this clock was created.
    pub fn now_ns(&self) -> u64 {
        self.started_at.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_is_monotonic() {
        let clock = Clock::new();
        let first = clock.now_ns();
        let second = clock.now_ns();

        assert!(second >= first);
    }
}
