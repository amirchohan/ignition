//! In-memory frame input/output for benchmark replay.
//!
//! This backend removes TCP and disk from measured hot-path latency while still
//! delivering raw protocol frames through the normal endpoint callback path.

use std::time::{Duration, Instant};

use crate::{domain::RecvTs, runtime::Mode};

use super::{Endpoint, FramePollError, IOPublisher, InputRecord, IoError};

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
                started_at: None,
            },
        })
    }

    pub fn poll<F, E>(
        &mut self,
        endpoint: Endpoint,
        mode: &mut Mode,
        mut on_frame: F,
        max_frames: Option<usize>,
    ) -> Result<bool, FramePollError<E>>
    where
        F: FnMut(&mut IOPublisher<'_>, RecvTs, &[u8]) -> Result<(), E>,
    {
        let mut frames = 0_usize;
        let mut made_progress = false;
        loop {
            if max_frames.is_some_and(|max_frames| frames >= max_frames) {
                return Ok(made_progress);
            }

            if self.input.get(self.cursor).is_none() {
                return Ok(made_progress);
            }

            self.schedule.wait_until_due(self.cursor);
            let record = &self.input[self.cursor];

            if record.input_seq != mode.expected_replay_input_seq().map_err(FramePollError::Io)? {
                return Ok(made_progress);
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
            on_frame(&mut publisher, record.recv_ts, &record.frame).map_err(FramePollError::Handler)?;
            mode.accept_replay_frame(record.input_seq).map_err(FramePollError::Io)?;
            self.cursor += 1;
            frames += 1;
            made_progress = true;
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
        started_at: Option<Instant>,
    },
}

impl InputSchedule {
    fn wait_until_due(&mut self, input_index: usize) {
        const SPIN_WINDOW: Duration = Duration::from_micros(50);

        let Self::OpenLoop {
            target_rate_per_sec,
            started_at,
        } = self
        else {
            return;
        };
        let start = *started_at.get_or_insert_with(Instant::now);
        let due_offset = Duration::from_nanos(scheduled_offset_ns(input_index as u64, *target_rate_per_sec));

        loop {
            let elapsed = start.elapsed();
            if elapsed >= due_offset {
                return;
            }
            let remaining = due_offset - elapsed;
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
    fn scheduled_input_waits_for_absolute_arrival_deadlines() {
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
        let started = Instant::now();

        assert!(
            io.poll(
                Endpoint::Gateway,
                &mut mode,
                |_, _, _| Ok::<(), std::convert::Infallible>(()),
                Some(2),
            )
            .expect("poll")
        );

        assert!(started.elapsed() >= Duration::from_millis(5));
        assert_eq!(mode, Mode::Replay { expected_input_seq: 3 });
    }
}
