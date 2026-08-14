//! In-memory frame input/output for benchmark replay.
//!
//! This backend removes TCP and disk from measured hot-path latency while still
//! delivering raw protocol frames through the normal endpoint callback path.

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
}

impl InMemoryFrameIo {
    pub fn new(input: Vec<InputRecord>, max_frame_len: usize) -> Self {
        Self {
            input,
            cursor: 0,
            output_frames: 0,
            output_bytes: 0,
            max_frame_len,
        }
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
        let Self {
            input,
            cursor,
            output_frames,
            output_bytes,
            max_frame_len,
        } = self;
        let mut frames = 0_usize;
        let mut made_progress = false;
        loop {
            if max_frames.is_some_and(|max_frames| frames >= max_frames) {
                return Ok(made_progress);
            }

            let Some(record) = input.get(*cursor) else {
                return Ok(made_progress);
            };

            if record.input_seq != mode.expected_replay_input_seq().map_err(FramePollError::Io)? {
                return Ok(made_progress);
            }

            if record.endpoint != endpoint {
                return Err(FramePollError::Io(IoError::EndpointMismatch {
                    configured: endpoint,
                    record: record.endpoint,
                }));
            }

            let mut publisher = IOPublisher::InMemory(InMemoryFramePublisher::new(output_frames, output_bytes, *max_frame_len));
            on_frame(&mut publisher, record.recv_ts, &record.frame).map_err(FramePollError::Handler)?;
            mode.accept_replay_frame(record.input_seq).map_err(FramePollError::Io)?;
            *cursor += 1;
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
