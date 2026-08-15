//! Deterministic replay input, live-input recording, and output capture files.
//!
//! [`FileFrameIo`] owns optional buffered input and output files. Its one-record
//! lookahead keeps the next [`InputRecord`] available until [`Mode`] says that
//! record's global sequence is due for this endpoint. [`FileFramePublisher`]
//! lends the same output writer to a frame handler, so replies use the ordinary
//! publisher interface without acquiring another file or buffer.
//!
//! Input records wrap raw protocol frames with an input sequence, endpoint tag,
//! receive timestamp, and length. This wrapper belongs only to capture and
//! replay; it does not change the binary order-entry protocol. Output captures
//! use a length prefix followed by the untouched encoded frame.
//!
//! The parent backend coordinates endpoint validation and replay sequencing
//! with `Mode`. A record is consumed only when its sequence is due, and replay
//! advances only after its handler succeeds. File replay deliberately delivers
//! at most one record per poll so separately owned endpoint files can alternate
//! according to the single global sequence.

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::PathBuf,
};

use crate::{domain::RecvTs, runtime::Mode};

use super::{
    Endpoint, FrameLatencyStart, FramePollControl, FramePollError, FramePollStatus, IOPublisher, InboundFrame, InputRecord, IoError,
};

/// Bytes preceding a frame in one deterministic input record.
const RECORD_HEADER_LEN: usize = 21;

/// Owner of optional replay input and capture output files.
#[derive(Debug)]
pub struct FileFrameIo {
    /// Buffered replay source, or `None` for an output-only instance.
    input: Option<BufReader<File>>,
    /// Buffered capture sink, or `None` for an input-only instance.
    output: Option<BufWriter<File>>,
    /// One record retained until global replay order permits delivery.
    peeked: Option<InputRecord>,
    /// Maximum frame payload accepted from the replay file.
    max_frame_len: usize,
    /// Whether buffered output has changed since the last successful flush.
    output_dirty: bool,
}

impl FileFrameIo {
    /// Open the configured replay source and capture sink.
    pub fn open(input_path: Option<PathBuf>, output_path: Option<PathBuf>, max_frame_len: usize) -> Result<Self, IoError> {
        let input = match input_path {
            Some(path) => Some(BufReader::new(File::open(path).map_err(IoError::Open)?)),
            None => None,
        };
        let output = match output_path {
            Some(path) => Some(BufWriter::new(File::create(path).map_err(IoError::Create)?)),
            None => None,
        };
        Ok(Self {
            input,
            output,
            peeked: None,
            max_frame_len,
            output_dirty: false,
        })
    }

    /// Deliver the next globally due record for this endpoint, if one exists.
    pub(crate) fn poll<F, E>(
        &mut self,
        endpoint: Endpoint,
        mode: &mut Mode,
        _max_frames: usize,
        mut on_frame: F,
    ) -> Result<FramePollStatus, FramePollError<E>>
    where
        F: FnMut(&mut IOPublisher<'_>, InboundFrame<'_>) -> Result<FramePollControl, E>,
    {
        // file replay intentionally accepts at most one record per call. the
        // main runtime uses that behavior to alternate globally sequenced
        // records between endpoint-specific files.
        let Some(record) = self.peek_record().map_err(FramePollError::Io)? else {
            return Ok(FramePollStatus::IDLE);
        };

        if record.input_seq != mode.expected_replay_input_seq().map_err(FramePollError::Io)? {
            return Ok(FramePollStatus::IDLE);
        }
        if record.endpoint != endpoint {
            return Err(FramePollError::Io(IoError::EndpointMismatch {
                configured: endpoint,
                record: record.endpoint,
            }));
        }

        let Some(record) = self.peeked.take() else {
            return Ok(FramePollStatus::IDLE);
        };
        let mut publisher = IOPublisher::File(FileFramePublisher::new(self));
        let control = on_frame(
            &mut publisher,
            InboundFrame {
                recv_ts: record.recv_ts,
                latency_start: FrameLatencyStart::HandlerEntry,
                bytes: &record.frame,
            },
        )
        .map_err(FramePollError::Handler)?;
        self.flush_pending().map_err(FramePollError::Io)?;
        mode.accept_replay_frame(record.input_seq).map_err(FramePollError::Io)?;

        Ok(FramePollStatus {
            progress: true,
            stopped: control == FramePollControl::Stop,
        })
    }

    /// Return whether no buffered or unread replay record remains.
    pub fn input_exhausted(&mut self) -> Result<bool, IoError> {
        Ok(self.peek_record()?.is_none())
    }

    /// Append one length-prefixed output frame when a capture sink is configured.
    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        let Some(output) = self.output.as_mut() else {
            return Ok(());
        };
        write_output_frame(output, frame).map_err(IoError::Write)?;
        self.output_dirty = true;
        Ok(())
    }

    /// Append one metadata-wrapped live input record to the configured sink.
    pub fn record_input(&mut self, record: &InputRecord) -> Result<(), IoError> {
        let Some(output) = self.output.as_mut() else {
            return Ok(());
        };
        write_input_record(output, record).map_err(IoError::Write)?;
        self.output_dirty = true;
        Ok(())
    }

    /// Flush dirty capture output, reporting whether bytes were pending.
    pub fn flush_pending(&mut self) -> Result<bool, IoError> {
        let Some(output) = self.output.as_mut() else {
            return Ok(false);
        };
        if !self.output_dirty {
            return Ok(false);
        }
        output.flush().map_err(IoError::Write)?;
        self.output_dirty = false;
        Ok(true)
    }

    /// Return the next replay record without consuming it.
    pub(crate) fn peek_record(&mut self) -> Result<Option<&InputRecord>, IoError> {
        if self.peeked.is_none() {
            self.peeked = self.read_record()?;
        }
        Ok(self.peeked.as_ref())
    }

    /// Read and decode the next complete replay record from the input file.
    fn read_record(&mut self) -> Result<Option<InputRecord>, IoError> {
        let Some(input) = self.input.as_mut() else {
            return Ok(None);
        };
        let mut header = [0_u8; RECORD_HEADER_LEN];
        match input.read_exact(&mut header) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(IoError::Read(error)),
        }

        let input_seq = u64::from_le_bytes(header[0..8].try_into().expect("input seq bytes"));
        let endpoint = Endpoint::from_u8(header[8])?;
        let recv_ts = RecvTs::new(u64::from_le_bytes(header[9..17].try_into().expect("recv ts bytes")));
        let frame_len = u32::from_le_bytes(header[17..21].try_into().expect("frame len bytes")) as usize;
        if frame_len > self.max_frame_len {
            return Err(IoError::FrameTooLarge {
                frame_len,
                max_frame_len: self.max_frame_len,
            });
        }

        let mut frame = vec![0_u8; frame_len];
        input.read_exact(&mut frame).map_err(IoError::Read)?;
        Ok(Some(InputRecord {
            input_seq,
            endpoint,
            recv_ts,
            frame,
        }))
    }
}

/// File-backed publisher borrowed while replaying one frame.
#[derive(Debug)]
pub struct FileFramePublisher<'a> {
    /// File backend whose output writer remains owned by the runtime.
    io: &'a mut FileFrameIo,
}

impl<'a> FileFramePublisher<'a> {
    /// Borrow a file backend for output during one frame callback.
    pub fn new(io: &'a mut FileFrameIo) -> Self {
        Self { io }
    }

    /// Queue and flush one output frame before returning.
    pub fn publish(&mut self, frame: &[u8]) -> Result<(), IoError> {
        self.io.queue_frame(frame)?;
        self.io.flush_pending()?;
        Ok(())
    }

    /// Queue one output frame for the runtime's next flush.
    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        self.io.queue_frame(frame)
    }
}

/// Write one metadata-wrapped input record in the stable replay-file layout.
fn write_input_record(output: &mut BufWriter<File>, record: &InputRecord) -> Result<(), std::io::Error> {
    output.write_all(&record.input_seq.to_le_bytes())?;
    output.write_all(&[record.endpoint.as_u8()])?;
    output.write_all(&record.recv_ts.raw().to_le_bytes())?;
    output.write_all(&(record.frame.len() as u32).to_le_bytes())?;
    output.write_all(&record.frame)?;
    Ok(())
}

/// Write one length-prefixed encoded frame to an output capture.
fn write_output_frame(output: &mut BufWriter<File>, frame: &[u8]) -> Result<(), std::io::Error> {
    output.write_all(&(frame.len() as u32).to_le_bytes())?;
    output.write_all(frame)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_records_round_trip() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("ignition-file-record-{}.bin", std::process::id()));
        let record = InputRecord {
            input_seq: 7,
            endpoint: Endpoint::Gateway,
            recv_ts: RecvTs::new(9),
            frame: vec![1, 2, 3],
        };

        {
            let mut io = FileFrameIo::open(None, Some(path.clone()), 128).expect("writer");
            io.record_input(&record).expect("record");
            io.flush_pending().expect("flush");
        }
        {
            let mut io = FileFrameIo::open(Some(path.clone()), None, 128).expect("reader");
            assert_eq!(io.peek_record().expect("peek"), Some(&record));
            assert_eq!(io.peeked.take(), Some(record));
            assert!(io.input_exhausted().expect("exhausted"));
        }

        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn file_output_flush_reports_progress_only_after_writes() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("ignition-file-output-progress-{}.bin", std::process::id()));
        let mut io = FileFrameIo::open(None, Some(path.clone()), 128).expect("writer");

        assert!(!io.flush_pending().expect("initial flush"));
        io.queue_frame(&[1, 2, 3]).expect("queue");
        assert!(io.flush_pending().expect("flush write"));
        assert!(!io.flush_pending().expect("clean flush"));
        io.record_input(&InputRecord {
            input_seq: 1,
            endpoint: Endpoint::Gateway,
            recv_ts: RecvTs::new(10),
            frame: vec![4, 5, 6],
        })
        .expect("record");
        assert!(io.flush_pending().expect("flush record"));
        assert!(!io.flush_pending().expect("clean record flush"));

        let _ = std::fs::remove_file(path);
    }
}
