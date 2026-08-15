//! In-memory frame input/output for benchmark replay.
//!
//! This backend removes TCP and disk from measured hot-path latency while still
//! delivering raw protocol frames through the normal endpoint callback path.

use std::time::Duration;

use crate::{clock::Clock, domain::RecvTs, runtime::Mode};

use super::{Endpoint, FrameLatencyStart, FramePollControl, FramePollError, FramePollStatus, IOPublisher, InputRecord, IoError};

/// In-memory frame IO used by benchmarks to remove TCP and disk from measured latency.
#[derive(Debug)]
pub struct InMemoryFrameIo {
    input: Vec<InputRecord>,
    cursor: usize,
    output_frames: u64,
    output_bytes: u64,
    max_frame_len: usize,
    schedule: InputSchedule,
}

impl InMemoryFrameIo {
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

    pub fn poll<F, E>(
        &mut self,
        clock: &Clock,
        endpoint: Endpoint,
        mode: &mut Mode,
        mut on_frame: F,
        max_frames: Option<usize>,
    ) -> Result<bool, FramePollError<E>>
    where
        F: FnMut(&mut IOPublisher<'_>, RecvTs, &[u8]) -> Result<(), E>,
    {
        self.poll_with_timing(
            clock,
            endpoint,
            mode,
            |publisher, recv_ts, _, frame| on_frame(publisher, recv_ts, frame),
            max_frames,
        )
    }

    pub(crate) fn poll_with_timing<F, E>(
        &mut self,
        clock: &Clock,
        endpoint: Endpoint,
        mode: &mut Mode,
        mut on_frame: F,
        max_frames: Option<usize>,
    ) -> Result<bool, FramePollError<E>>
    where
        F: FnMut(&mut IOPublisher<'_>, RecvTs, FrameLatencyStart, &[u8]) -> Result<(), E>,
    {
        self.poll_controlled_with_timing(
            clock,
            endpoint,
            mode,
            |publisher, recv_ts, latency_start, frame| {
                on_frame(publisher, recv_ts, latency_start, frame)?;
                Ok(FramePollControl::Continue)
            },
            max_frames,
        )
        .map(|status| status.progress)
    }

    pub(crate) fn poll_controlled_with_timing<F, E>(
        &mut self,
        clock: &Clock,
        endpoint: Endpoint,
        mode: &mut Mode,
        mut on_frame: F,
        max_frames: Option<usize>,
    ) -> Result<FramePollStatus, FramePollError<E>>
    where
        F: FnMut(&mut IOPublisher<'_>, RecvTs, FrameLatencyStart, &[u8]) -> Result<FramePollControl, E>,
    {
        let mut frames = 0_usize;
        let mut made_progress = false;
        loop {
            if max_frames.is_some_and(|max_frames| frames >= max_frames) {
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
            let control = on_frame(&mut publisher, record.recv_ts, latency_start, &record.frame).map_err(FramePollError::Handler)?;
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

    pub fn peek_record(&self) -> Option<&InputRecord> {
        self.input.get(self.cursor)
    }

    pub fn take_peeked(&mut self) -> Option<InputRecord> {
        let record = self.input.get(self.cursor).cloned()?;
        self.cursor += 1;
        Some(record)
    }

    pub fn input_exhausted(&self) -> bool {
        self.cursor >= self.input.len()
    }

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

    pub const fn flush_pending(&self) -> bool {
        false
    }

    pub const fn output_frames(&self) -> u64 {
        self.output_frames
    }

    pub const fn output_bytes(&self) -> u64 {
        self.output_bytes
    }
}

/// Benchmark input release policy.
#[derive(Debug)]
enum InputSchedule {
    Burst,
    OpenLoop {
        target_rate_per_sec: u64,
        started_at_ns: Option<u64>,
    },
}

impl InputSchedule {
    fn wait_until_due(&mut self, clock: &Clock, input_index: usize) -> FrameLatencyStart {
        const SPIN_WINDOW: Duration = Duration::from_micros(50);

        let Self::OpenLoop {
            target_rate_per_sec,
            started_at_ns,
        } = self
        else {
            return FrameLatencyStart::HandlerEntry;
        };
        let start_ns = *started_at_ns.get_or_insert_with(|| clock.now_ns());
        let due_offset_ns = scheduled_offset_ns(input_index as u64, *target_rate_per_sec);
        let due_ns = start_ns.saturating_add(due_offset_ns);

        loop {
            let now_ns = clock.now_ns();
            if now_ns >= due_ns {
                return FrameLatencyStart::Scheduled(due_ns);
            }
            let remaining = Duration::from_nanos(due_ns - now_ns);
            if remaining > SPIN_WINDOW {
                std::thread::sleep(remaining - SPIN_WINDOW);
            } else {
                std::hint::spin_loop();
            }
        }
    }
}

fn scheduled_offset_ns(input_index: u64, target_rate_per_sec: u64) -> u64 {
    let offset = u128::from(input_index) * 1_000_000_000_u128 / u128::from(target_rate_per_sec);
    offset.min(u128::from(u64::MAX)) as u64
}

/// In-memory frame publisher borrowed during benchmark replay.
#[derive(Debug)]
pub struct InMemoryFramePublisher<'a> {
    output_frames: &'a mut u64,
    output_bytes: &'a mut u64,
    max_frame_len: usize,
}

impl<'a> InMemoryFramePublisher<'a> {
    pub fn new(output_frames: &'a mut u64, output_bytes: &'a mut u64, max_frame_len: usize) -> Self {
        Self {
            output_frames,
            output_bytes,
            max_frame_len,
        }
    }

    pub fn publish(&mut self, frame: &[u8]) -> Result<(), IoError> {
        self.queue_frame(frame)
    }

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

        io.poll_with_timing(
            &clock,
            Endpoint::Gateway,
            &mut mode,
            |_, _, observed, _| {
                latency_start = Some(observed);
                Ok::<(), std::convert::Infallible>(())
            },
            Some(1),
        )
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
            .poll_controlled_with_timing(
                &clock,
                Endpoint::Gateway,
                &mut mode,
                |_, _, _, _| {
                    handled += 1;
                    Ok::<FramePollControl, std::convert::Infallible>(FramePollControl::Stop)
                },
                Some(2),
            )
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

        assert!(
            io.poll(
                &clock,
                Endpoint::Gateway,
                &mut mode,
                |_, _, _| Ok::<(), std::convert::Infallible>(()),
                Some(2),
            )
            .expect("poll")
        );

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

        io.poll_with_timing(
            &clock,
            Endpoint::Gateway,
            &mut mode,
            |_, _, latency_start, _| {
                arrivals.push((latency_start, clock.now_ns()));
                if arrivals.len() == 1 {
                    std::thread::sleep(Duration::from_millis(8));
                }
                Ok::<(), std::convert::Infallible>(())
            },
            Some(2),
        )
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
