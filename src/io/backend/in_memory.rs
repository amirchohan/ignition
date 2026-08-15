//! Allocation-free frame replay and output accounting for benchmarks.
//!
//! [`InMemoryFrameIo`] owns a fixed vector of [`InputRecord`] values, a cursor
//! into that vector, output counters, and an [`InputSchedule`]. It delivers the
//! same borrowed [`InboundFrame`] and publisher interface as live or file I/O,
//! while excluding socket and disk costs from measured hot-path latency.
//!
//! Burst input becomes available immediately. Open-loop input derives every
//! absolute due time from one start timestamp and the record index, avoiding
//! cumulative rounding or handler-time drift. [`InMemoryFramePublisher`] owns
//! no storage; it borrows the backend's counters and validates output sizes.
//!
//! The parent backend and [`InputLifecycle`] still enforce endpoint identity and global
//! replay sequence. Records remain in their original order, the cursor advances
//! only after successful handling, and output counting uses saturating totals so
//! benchmark traffic cannot wrap the counters.

use std::time::Duration;

#[cfg(test)]
use crate::runtime::Mode;
use crate::{clock::Clock, runtime::InputLifecycle};

use super::{
    Endpoint, FrameLatencyStart, FramePollControl, FramePollError, FramePollStatus, IOPublisher, InboundFrame, InputRecord, IoError,
};

/// Owner of benchmark replay records, scheduling state, and output counters.
#[derive(Debug)]
pub struct InMemoryFrameIo {
    /// Replay records retained in deterministic delivery order.
    input: Vec<InputRecord>,
    /// Index of the next record eligible for delivery.
    cursor: usize,
    /// Number of output frames accepted by this backend.
    output_frames: u64,
    /// Total encoded bytes accepted by this backend.
    output_bytes: u64,
    /// Maximum accepted input and output frame length.
    max_frame_len: usize,
    /// Release policy applied before inspecting the next record.
    schedule: InputSchedule,
}

impl InMemoryFrameIo {
    /// Create a replay backend whose records are available as a burst.
    pub fn new(input: Vec<InputRecord>, max_frame_len: usize) -> Self {
        Self {
            input,
            cursor: 0,
            output_frames: 0,
            output_bytes: 0,
            max_frame_len,
            schedule: InputSchedule::Burst,
        }
    }

    /// Create an open-loop replay backend at the requested frames-per-second rate.
    pub fn new_scheduled(input: Vec<InputRecord>, max_frame_len: usize, target_rate_per_sec: u64) -> Result<Self, IoError> {
        if !(1..=1_000_000_000).contains(&target_rate_per_sec) {
            return Err(IoError::Configure(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("target rate must be between 1 and 1000000000 orders per second, got {target_rate_per_sec}"),
            )));
        }
        Ok(Self {
            input,
            cursor: 0,
            output_frames: 0,
            output_bytes: 0,
            max_frame_len,
            schedule: InputSchedule::OpenLoop {
                target_rate_per_sec,
                started_at_ns: None,
            },
        })
    }

    /// Deliver up to `max_frames` globally due records for this endpoint.
    pub(crate) fn poll<F, E>(
        &mut self,
        clock: &Clock,
        endpoint: Endpoint,
        mode: &mut InputLifecycle,
        max_frames: usize,
        mut on_frame: F,
    ) -> Result<FramePollStatus, FramePollError<E>>
    where
        F: FnMut(&mut IOPublisher<'_>, InboundFrame<'_>) -> Result<FramePollControl, E>,
    {
        let mut frames = 0_usize;
        let mut made_progress = false;
        loop {
            if frames >= max_frames {
                return Ok(FramePollStatus {
                    progress: made_progress,
                    stopped: false,
                });
            }

            if self.input.get(self.cursor).is_none() {
                return Ok(FramePollStatus {
                    progress: made_progress,
                    stopped: false,
                });
            }

            let latency_start = self.schedule.wait_until_due(clock, self.cursor);
            let record = &self.input[self.cursor];

            // checking the shared replay sequence before advancing preserves global ordering across endpoint runtimes.
            if record.input_seq != mode.expected_replay_input_seq().map_err(FramePollError::Io)? {
                return Ok(FramePollStatus {
                    progress: made_progress,
                    stopped: false,
                });
            }

            if record.endpoint != endpoint {
                return Err(FramePollError::Io(IoError::EndpointMismatch {
                    configured: endpoint,
                    record: record.endpoint,
                }));
            }

            let mut publisher = IOPublisher::InMemory(InMemoryFramePublisher::new(
                &mut self.output_frames,
                &mut self.output_bytes,
                self.max_frame_len,
            ));
            let control = on_frame(
                &mut publisher,
                InboundFrame {
                    recv_ts: record.recv_ts,
                    latency_start,
                    bytes: &record.frame,
                },
            )
            .map_err(FramePollError::Handler)?;
            // the cursor advances only after both endpoint handling and replay acceptance succeed.
            mode.accept_replay_frame(record.input_seq).map_err(FramePollError::Io)?;
            self.cursor += 1;
            frames += 1;
            made_progress = true;
            if control == FramePollControl::Stop {
                return Ok(FramePollStatus {
                    progress: true,
                    stopped: true,
                });
            }
        }
    }

    /// Return whether every replay record has been accepted.
    pub fn input_exhausted(&self) -> bool {
        self.cursor >= self.input.len()
    }

    /// Validate and count one output frame without retaining its bytes.
    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        if frame.len() > self.max_frame_len {
            return Err(IoError::FrameTooLarge {
                frame_len: frame.len(),
                max_frame_len: self.max_frame_len,
            });
        }
        self.output_frames = self.output_frames.saturating_add(1);
        self.output_bytes = self.output_bytes.saturating_add(frame.len() as u64);
        Ok(())
    }

    /// Report no flush progress because output is counted synchronously.
    pub const fn flush_pending(&self) -> Result<bool, IoError> {
        Ok(false)
    }
}

/// Benchmark input release policy.
#[derive(Debug)]
enum InputSchedule {
    /// Make every record immediately eligible.
    Burst,
    /// Release records against absolute deadlines at a fixed open-loop rate.
    OpenLoop {
        /// Requested frames per second, constrained to one nanosecond or slower intervals.
        target_rate_per_sec: u64,
        /// Runtime-clock origin captured when the first record is polled.
        started_at_ns: Option<u64>,
    },
}

impl InputSchedule {
    /// Wait until one record is due and return its latency start policy.
    fn wait_until_due(&mut self, clock: &Clock, input_index: usize) -> FrameLatencyStart {
        /// Final interval reserved for active waiting near a scheduled deadline.
        const SPIN_WINDOW: Duration = Duration::from_micros(50);

        let Self::OpenLoop {
            target_rate_per_sec,
            started_at_ns,
        } = self
        else {
            return FrameLatencyStart::HandlerEntry;
        };
        let start_ns = *started_at_ns.get_or_insert_with(|| clock.now_ns());
        // absolute offsets prevent handler duration and fractional intervals from accumulating schedule drift.
        let due_offset_ns = scheduled_offset_ns(input_index as u64, *target_rate_per_sec);
        let due_ns = start_ns.saturating_add(due_offset_ns);

        loop {
            let now_ns = clock.now_ns();
            if now_ns >= due_ns {
                return FrameLatencyStart::Scheduled(due_ns);
            }
            let remaining = Duration::from_nanos(due_ns - now_ns);
            // sleeping most of a long wait limits CPU use while the final spin protects sub-millisecond precision.
            if remaining > SPIN_WINDOW {
                std::thread::sleep(remaining - SPIN_WINDOW);
            } else {
                std::hint::spin_loop();
            }
        }
    }
}

/// Return an index-based nanosecond offset without cumulative rounding error.
fn scheduled_offset_ns(input_index: u64, target_rate_per_sec: u64) -> u64 {
    let offset = u128::from(input_index) * 1_000_000_000_u128 / u128::from(target_rate_per_sec);
    offset.min(u128::from(u64::MAX)) as u64
}

/// In-memory frame publisher borrowed during benchmark replay.
#[derive(Debug)]
pub struct InMemoryFramePublisher<'a> {
    /// Borrowed count of accepted output frames.
    output_frames: &'a mut u64,
    /// Borrowed total of accepted encoded output bytes.
    output_bytes: &'a mut u64,
    /// Maximum frame length accepted by the owning backend.
    max_frame_len: usize,
}

impl<'a> InMemoryFramePublisher<'a> {
    /// Borrow an in-memory backend's output counters for one callback.
    pub fn new(output_frames: &'a mut u64, output_bytes: &'a mut u64, max_frame_len: usize) -> Self {
        Self {
            output_frames,
            output_bytes,
            max_frame_len,
        }
    }

    /// Validate and count one output frame immediately.
    pub fn publish(&mut self, frame: &[u8]) -> Result<(), IoError> {
        self.queue_frame(frame)
    }

    /// Validate and count one output frame without retaining its bytes.
    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        if frame.len() > self.max_frame_len {
            return Err(IoError::FrameTooLarge {
                frame_len: frame.len(),
                max_frame_len: self.max_frame_len,
            });
        }
        *self.output_frames = (*self.output_frames).saturating_add(1);
        *self.output_bytes = (*self.output_bytes).saturating_add(frame.len() as u64);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::RecvTs;

    #[test]
    fn scheduled_offsets_start_at_zero_and_preserve_fractional_intervals() {
        assert_eq!(scheduled_offset_ns(0, 3), 0);
        assert_eq!(scheduled_offset_ns(1, 3), 333_333_333);
        assert_eq!(scheduled_offset_ns(2, 3), 666_666_666);
        assert_eq!(scheduled_offset_ns(3, 3), 1_000_000_000);
    }

    #[test]
    fn scheduled_constructor_rejects_invalid_rates() {
        assert!(matches!(
            InMemoryFrameIo::new_scheduled(Vec::new(), 128, 0),
            Err(IoError::Configure(error)) if error.kind() == std::io::ErrorKind::InvalidInput
        ));
        assert!(matches!(
            InMemoryFrameIo::new_scheduled(Vec::new(), 128, 1_000_000_001),
            Err(IoError::Configure(error)) if error.kind() == std::io::ErrorKind::InvalidInput
        ));
    }

    #[test]
    fn burst_input_uses_handler_entry_latency_start() {
        let clock = Clock::new();
        let records = vec![InputRecord {
            input_seq: 1,
            endpoint: Endpoint::Gateway,
            recv_ts: RecvTs::new(1),
            frame: vec![1],
        }];
        let mut io = InMemoryFrameIo::new(records, 128);
        let mut mode = Mode::Replay { expected_input_seq: 1 };
        let mut latency_start = None;

        io.poll(&clock, Endpoint::Gateway, &mut mode, 1, |_, frame| {
            latency_start = Some(frame.latency_start);
            Ok::<FramePollControl, std::convert::Infallible>(FramePollControl::Continue)
        })
        .expect("poll");

        assert_eq!(latency_start, Some(FrameLatencyStart::HandlerEntry));
    }

    #[test]
    fn controlled_poll_stops_after_accepting_the_triggering_record() {
        let clock = Clock::new();
        let records = (1..=2)
            .map(|input_seq| InputRecord {
                input_seq,
                endpoint: Endpoint::Gateway,
                recv_ts: RecvTs::new(input_seq),
                frame: vec![input_seq as u8],
            })
            .collect();
        let mut io = InMemoryFrameIo::new(records, 128);
        let mut mode = Mode::Replay { expected_input_seq: 1 };
        let mut handled = 0;

        let status = io
            .poll(&clock, Endpoint::Gateway, &mut mode, 2, |_, _| {
                handled += 1;
                Ok::<FramePollControl, std::convert::Infallible>(FramePollControl::Stop)
            })
            .expect("controlled poll");

        assert_eq!(handled, 1);
        assert_eq!(
            status,
            FramePollStatus {
                progress: true,
                stopped: true
            }
        );
        assert_eq!(mode, Mode::Replay { expected_input_seq: 2 });
        assert!(!io.input_exhausted());
    }

    #[test]
    fn scheduled_input_waits_for_absolute_arrival_deadlines() {
        let clock = Clock::new();
        let records = vec![
            InputRecord {
                input_seq: 1,
                endpoint: Endpoint::Gateway,
                recv_ts: RecvTs::new(1),
                frame: vec![1],
            },
            InputRecord {
                input_seq: 2,
                endpoint: Endpoint::Gateway,
                recv_ts: RecvTs::new(2),
                frame: vec![2],
            },
        ];
        let mut io = InMemoryFrameIo::new_scheduled(records, 128, 200).expect("scheduled io");
        let mut mode = Mode::Replay { expected_input_seq: 1 };
        let started_ns = clock.now_ns();

        let status = io
            .poll(&clock, Endpoint::Gateway, &mut mode, 2, |_, _| {
                Ok::<FramePollControl, std::convert::Infallible>(FramePollControl::Continue)
            })
            .expect("poll");

        assert!(status.progress);

        assert!(clock.now_ns().saturating_sub(started_ns) >= 5_000_000);
        assert_eq!(mode, Mode::Replay { expected_input_seq: 3 });
    }

    #[test]
    fn overdue_input_preserves_its_original_scheduled_arrival() {
        let clock = Clock::new();
        let records = vec![
            InputRecord {
                input_seq: 1,
                endpoint: Endpoint::Gateway,
                recv_ts: RecvTs::new(1),
                frame: vec![1],
            },
            InputRecord {
                input_seq: 2,
                endpoint: Endpoint::Gateway,
                recv_ts: RecvTs::new(2),
                frame: vec![2],
            },
        ];
        let mut io = InMemoryFrameIo::new_scheduled(records, 128, 1_000).expect("scheduled io");
        let mut mode = Mode::Replay { expected_input_seq: 1 };
        let mut arrivals = Vec::new();

        io.poll(&clock, Endpoint::Gateway, &mut mode, 2, |_, frame| {
            arrivals.push((frame.latency_start, clock.now_ns()));
            if arrivals.len() == 1 {
                std::thread::sleep(Duration::from_millis(8));
            }
            Ok::<FramePollControl, std::convert::Infallible>(FramePollControl::Continue)
        })
        .expect("poll");

        let FrameLatencyStart::Scheduled(first_due_ns) = arrivals[0].0 else {
            panic!("first scheduled arrival");
        };
        let FrameLatencyStart::Scheduled(second_due_ns) = arrivals[1].0 else {
            panic!("second scheduled arrival");
        };
        assert_eq!(second_due_ns - first_due_ns, 1_000_000);
        assert!(arrivals[1].1.saturating_sub(second_due_ns) >= 5_000_000);
    }
}
