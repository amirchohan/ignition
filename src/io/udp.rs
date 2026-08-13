use std::net::{SocketAddr, SocketAddrV4, UdpSocket};

use super::IoError;

/// Multicast UDP publisher configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UdpMulticastConfig {
    pub bind_addr: String,
    pub group_addr: String,
    pub multicast_ttl: u32,
    pub multicast_loop_v4: bool,
    pub max_datagram_len: usize,
    pub max_pending_datagram_bytes: usize,
}

/// Nonblocking UDP publisher with a bounded pending datagram queue.
#[derive(Debug)]
pub struct UdpFramePublisher {
    socket: UdpSocket,
    pending: Vec<u8>,
    read_start: usize,
    max_datagram_len: usize,
    max_pending_datagram_bytes: usize,
}

impl UdpFramePublisher {
    pub fn bind_multicast(config: UdpMulticastConfig) -> Result<Self, IoError> {
        let group_addr = parse_ipv4_multicast(&config.group_addr)?;
        let socket = UdpSocket::bind(&config.bind_addr).map_err(IoError::Bind)?;
        socket.set_nonblocking(true).map_err(IoError::Configure)?;
        socket.set_multicast_ttl_v4(config.multicast_ttl).map_err(IoError::Configure)?;
        socket.set_multicast_loop_v4(config.multicast_loop_v4).map_err(IoError::Configure)?;
        socket.connect(group_addr).map_err(IoError::Configure)?;
        Ok(Self {
            socket,
            pending: Vec::with_capacity(config.max_pending_datagram_bytes),
            read_start: 0,
            max_datagram_len: config.max_datagram_len,
            max_pending_datagram_bytes: config.max_pending_datagram_bytes,
        })
    }

    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        if frame.len() > self.max_datagram_len {
            return Err(IoError::FrameTooLarge {
                frame_len: frame.len(),
                max_frame_len: self.max_datagram_len,
            });
        }
        if frame.len() > u16::MAX as usize {
            return Err(IoError::FrameTooLarge {
                frame_len: frame.len(),
                max_frame_len: u16::MAX as usize,
            });
        }

        self.compact_pending();
        let pending_bytes = self
            .pending
            .len()
            .saturating_sub(self.read_start)
            .saturating_add(2)
            .saturating_add(frame.len());
        if pending_bytes > self.max_pending_datagram_bytes {
            return Err(IoError::PendingWriteLimit {
                pending_bytes,
                max_pending_write_bytes: self.max_pending_datagram_bytes,
            });
        }

        self.pending.extend_from_slice(&(frame.len() as u16).to_le_bytes());
        self.pending.extend_from_slice(frame);
        Ok(())
    }

    pub fn flush_pending(&mut self) -> Result<bool, IoError> {
        let mut made_progress = false;
        while self.pending.len().saturating_sub(self.read_start) >= 2 {
            let len = u16::from_le_bytes([self.pending[self.read_start], self.pending[self.read_start + 1]]) as usize;
            let frame_start = self.read_start + 2;
            let frame_end = frame_start + len;
            if self.pending.len() < frame_end {
                break;
            }

            match self.socket.send(&self.pending[frame_start..frame_end]) {
                Ok(written) if written == len => {
                    made_progress = true;
                    self.read_start = frame_end;
                }
                Ok(written) => {
                    return Err(IoError::Write(std::io::Error::new(
                        std::io::ErrorKind::WriteZero,
                        format!("short udp datagram write: {written}/{len}"),
                    )));
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Ok(made_progress);
                }
                Err(error) => return Err(IoError::Write(error)),
            }
        }

        if self.read_start == self.pending.len() {
            self.pending.clear();
            self.read_start = 0;
        }
        Ok(made_progress)
    }

    fn compact_pending(&mut self) {
        if self.read_start == 0 {
            return;
        }
        if self.read_start == self.pending.len() {
            self.pending.clear();
            self.read_start = 0;
            return;
        }
        let remaining = self.pending.len() - self.read_start;
        self.pending.copy_within(self.read_start.., 0);
        self.pending.truncate(remaining);
        self.read_start = 0;
    }
}

fn parse_ipv4_multicast(addr: &str) -> Result<SocketAddrV4, IoError> {
    match addr.parse::<SocketAddr>() {
        Ok(SocketAddr::V4(addr)) if addr.ip().is_multicast() => Ok(addr),
        Ok(SocketAddr::V4(_)) => Err(invalid_multicast("udp destination is not multicast")),
        Ok(SocketAddr::V6(_)) => Err(invalid_multicast("udp multicast feed requires ipv4")),
        Err(error) => Err(IoError::Configure(std::io::Error::new(std::io::ErrorKind::InvalidInput, error))),
    }
}

fn invalid_multicast(message: &'static str) -> IoError {
    IoError::Configure(std::io::Error::new(std::io::ErrorKind::InvalidInput, message))
}
