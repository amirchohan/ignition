//! Deterministic file-backed frame input/output.
//!
//! File records wrap raw protocol frames with replay metadata. The wrapped
//! metadata is not part of the wire protocol; it exists so replay can preserve
//! global input ordering and injected receive timestamps.

use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    path::PathBuf,
};

use crate::domain::RecvTs;

use super::{Endpoint, IoError};

const RECORD_HEADER_LEN: usize = 21;

/// One recorded input frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputRecord {
    pub input_seq: u64,
    pub endpoint: Endpoint,
    pub recv_ts: RecvTs,
    pub frame: Vec<u8>,
}

/// File-backed frame IO.
#[derive(Debug)]
pub struct FileFrameIo {
    input: Option<BufReader<File>>,
    output: Option<BufWriter<File>>,
    peeked: Option<InputRecord>,
    max_frame_len: usize,
    output_dirty: bool,
}

impl FileFrameIo {
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

    pub fn peek_record(&mut self) -> Result<Option<&InputRecord>, IoError> {
        if self.peeked.is_none() {
            self.peeked = self.read_record()?;
        }
        Ok(self.peeked.as_ref())
    }

    pub fn take_peeked(&mut self) -> Option<InputRecord> {
        self.peeked.take()
    }

    pub fn input_exhausted(&mut self) -> Result<bool, IoError> {
        Ok(self.peek_record()?.is_none())
    }

    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        let Some(output) = self.output.as_mut() else {
            return Ok(());
        };
        write_output_frame(output, frame).map_err(IoError::Write)?;
        self.output_dirty = true;
        Ok(())
    }

    pub fn record_input(&mut self, record: &InputRecord) -> Result<(), IoError> {
        let Some(output) = self.output.as_mut() else {
            return Ok(());
        };
        write_input_record(output, record).map_err(IoError::Write)?;
        self.output_dirty = true;
        Ok(())
    }

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
    io: &'a mut FileFrameIo,
}

impl<'a> FileFramePublisher<'a> {
    pub fn new(io: &'a mut FileFrameIo) -> Self {
        Self { io }
    }

    pub fn publish(&mut self, frame: &[u8]) -> Result<(), IoError> {
        self.io.queue_frame(frame)?;
        self.io.flush_pending()?;
        Ok(())
    }

    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        self.io.queue_frame(frame)
    }
}

fn write_input_record(output: &mut BufWriter<File>, record: &InputRecord) -> Result<(), std::io::Error> {
    output.write_all(&record.input_seq.to_le_bytes())?;
    output.write_all(&[record.endpoint.as_u8()])?;
    output.write_all(&record.recv_ts.raw().to_le_bytes())?;
    output.write_all(&(record.frame.len() as u32).to_le_bytes())?;
    output.write_all(&record.frame)?;
    Ok(())
}

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
            assert_eq!(io.take_peeked(), Some(record));
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
