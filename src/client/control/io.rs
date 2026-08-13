use std::{
    io::{Read, Write},
    net::TcpStream,
};

use crate::protocol::SBE_HEADER_LEN;

use super::protocol::{ClientControlEvent, ClientControlFrame, decode_control_event, encode_control_frame};

#[derive(Debug)]
pub struct ControlEndpointClient {
    stream: TcpStream,
    read_buf: Vec<u8>,
    read_start: usize,
    encode_buf: Vec<u8>,
}

impl ControlEndpointClient {
    pub fn connect(addr: &str) -> Result<Self, ControlClientIoError> {
        let stream = TcpStream::connect(addr).map_err(ControlClientIoError::Connect)?;
        stream.set_nonblocking(true).map_err(ControlClientIoError::Configure)?;
        stream.set_nodelay(true).map_err(ControlClientIoError::Configure)?;
        Ok(Self {
            stream,
            read_buf: Vec::with_capacity(4096),
            read_start: 0,
            encode_buf: vec![0_u8; 4096],
        })
    }

    pub fn send(&mut self, frame: ClientControlFrame) -> Result<(), ControlClientIoError> {
        let written = encode_control_frame(frame, &mut self.encode_buf).map_err(ControlClientIoError::Protocol)?;
        self.stream
            .write_all(&self.encode_buf[..written])
            .map_err(ControlClientIoError::Write)
    }

    pub fn poll(&mut self, events: &mut Vec<ClientControlEvent>) -> Result<bool, ControlClientIoError> {
        let mut made_progress = false;
        let mut buf = [0_u8; 4096];
        loop {
            match self.stream.read(&mut buf) {
                Ok(0) => return Err(ControlClientIoError::Closed),
                Ok(read) => {
                    made_progress = true;
                    self.read_buf.extend_from_slice(&buf[..read]);
                    self.drain_frames(events)?;
                    self.compact_read_buf();
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    self.compact_read_buf();
                    return Ok(made_progress);
                }
                Err(error) => return Err(ControlClientIoError::Read(error)),
            }
        }
    }

    fn drain_frames(&mut self, events: &mut Vec<ClientControlEvent>) -> Result<(), ControlClientIoError> {
        loop {
            if self.read_buf.len().saturating_sub(self.read_start) < SBE_HEADER_LEN {
                return Ok(());
            }
            let block_len = u16::from_le_bytes([self.read_buf[self.read_start], self.read_buf[self.read_start + 1]]) as usize;
            let frame_len = SBE_HEADER_LEN + block_len;
            if self.read_buf.len().saturating_sub(self.read_start) < frame_len {
                return Ok(());
            }
            let frame_start = self.read_start;
            let frame_end = frame_start + frame_len;
            let event = decode_control_event(&self.read_buf[frame_start..frame_end]).map_err(ControlClientIoError::Protocol)?;
            events.push(event);
            self.read_start = frame_end;
        }
    }

    fn compact_read_buf(&mut self) {
        if self.read_start == 0 {
            return;
        }
        if self.read_start == self.read_buf.len() {
            self.read_buf.clear();
            self.read_start = 0;
            return;
        }
        let remaining = self.read_buf.len() - self.read_start;
        self.read_buf.copy_within(self.read_start.., 0);
        self.read_buf.truncate(remaining);
        self.read_start = 0;
    }
}

#[derive(Debug)]
pub enum ControlClientIoError {
    Connect(std::io::Error),
    Configure(std::io::Error),
    Write(std::io::Error),
    Read(std::io::Error),
    Closed,
    Protocol(crate::protocol::ProtocolError),
}

impl std::fmt::Display for ControlClientIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Connect(error) => write!(f, "failed to connect control endpoint: {error}"),
            Self::Configure(error) => write!(f, "failed to configure control socket: {error}"),
            Self::Write(error) => write!(f, "failed to write control frame: {error}"),
            Self::Read(error) => write!(f, "failed to read control frame: {error}"),
            Self::Closed => write!(f, "control connection closed"),
            Self::Protocol(error) => write!(f, "protocol error: {error:?}"),
        }
    }
}

impl std::error::Error for ControlClientIoError {}
