//! Reusable nonblocking framed I/O.
//!
//! This module owns transport mechanics only. Endpoint modules decide how to
//! decode and encode the SBE payload carried by each complete frame.

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
};

use crate::{clock::Clock, domain::RecvTs, protocol::SBE_HEADER_LEN};

use super::{FramePollControl, FramePollError, FramePollStatus, IoError};

/// Raw frame publisher borrowed during transport polling.
#[derive(Debug)]
pub struct TcpFramePublisher<'a> {
    active: &'a mut Option<TcpStream>,
    write_buf: &'a mut Vec<u8>,
    write_head: &'a mut usize,
    write_len: &'a mut usize,
    max_pending_write_bytes: usize,
}

impl TcpFramePublisher<'_> {
    pub fn reborrow(&mut self) -> TcpFramePublisher<'_> {
        TcpFramePublisher {
            active: self.active,
            write_buf: self.write_buf,
            write_head: self.write_head,
            write_len: self.write_len,
            max_pending_write_bytes: self.max_pending_write_bytes,
        }
    }

    pub fn publish(&mut self, frame: &[u8]) -> Result<(), IoError> {
        self.flush_pending()?;
        self.queue_frame(frame)?;
        self.flush_pending()?;
        Ok(())
    }

    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        let pending_bytes = self.write_len.saturating_add(frame.len());
        if pending_bytes > self.max_pending_write_bytes {
            self.disconnect_active();
            return Err(IoError::PendingWriteLimit {
                pending_bytes,
                max_pending_write_bytes: self.max_pending_write_bytes,
            });
        }
        self.push_write_bytes(frame);
        Ok(())
    }

    pub fn flush_pending(&mut self) -> Result<bool, IoError> {
        let mut made_progress = false;
        let Some(stream) = self.active.as_mut() else {
            *self.write_head = 0;
            *self.write_len = 0;
            return Ok(false);
        };

        while *self.write_len != 0 {
            let write_cap = self.write_buf.len();
            let contiguous = if *self.write_head + *self.write_len <= write_cap {
                *self.write_len
            } else {
                write_cap - *self.write_head
            };
            match stream.write(&self.write_buf[*self.write_head..*self.write_head + contiguous]) {
                Ok(0) => return Ok(made_progress),
                Ok(written) => {
                    made_progress = true;
                    *self.write_head = (*self.write_head + written) % write_cap;
                    *self.write_len -= written;
                    if *self.write_len == 0 {
                        *self.write_head = 0;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Ok(made_progress);
                }
                Err(error) => {
                    self.disconnect_active();
                    return Err(IoError::Write(error));
                }
            }
        }

        Ok(made_progress)
    }

    fn disconnect_active(&mut self) {
        *self.active = None;
        *self.write_head = 0;
        *self.write_len = 0;
    }

    fn push_write_bytes(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }

        let write_cap = self.write_buf.len();
        debug_assert!(write_cap != 0);
        let write_tail = (*self.write_head + *self.write_len) % write_cap;
        let first = bytes.len().min(write_cap - write_tail);
        self.write_buf[write_tail..write_tail + first].copy_from_slice(&bytes[..first]);
        if first < bytes.len() {
            self.write_buf[..bytes.len() - first].copy_from_slice(&bytes[first..]);
        }
        *self.write_len += bytes.len();
    }
}

/// Nonblocking TCP listener with one active client.
#[derive(Debug)]
pub struct TcpFrameIo {
    listener: TcpListener,
    active: Option<TcpStream>,
    read_buf: Vec<u8>,
    read_start: usize,
    write_buf: Vec<u8>,
    write_head: usize,
    write_len: usize,
    max_frame_len: usize,
    max_pending_write_bytes: usize,
}

impl TcpFrameIo {
    pub fn bind(bind_addr: &str, max_frame_len: usize, max_pending_write_bytes: usize) -> Result<Self, IoError> {
        let listener = TcpListener::bind(bind_addr).map_err(IoError::Bind)?;
        listener.set_nonblocking(true).map_err(IoError::Configure)?;
        Ok(Self {
            listener,
            active: None,
            read_buf: Vec::with_capacity(max_frame_len),
            read_start: 0,
            write_buf: vec![0_u8; max_pending_write_bytes],
            write_head: 0,
            write_len: 0,
            max_frame_len,
            max_pending_write_bytes,
        })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, IoError> {
        self.listener.local_addr().map_err(IoError::LocalAddr)
    }

    fn accept_available(&mut self) -> Result<bool, IoError> {
        let mut made_progress = false;
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true).map_err(IoError::Configure)?;
                    made_progress = true;
                    if self.active.is_none() {
                        self.active = Some(stream);
                    }
                    // Dropping additional accepted streams enforces a single
                    // active control client without blocking the listener.
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Ok(made_progress);
                }
                Err(error) => return Err(IoError::Read(error)),
            }
        }
    }

    fn read_available_controlled<F, E>(
        &mut self,
        clock: &Clock,
        mut on_frame: F,
        max_frames: Option<usize>,
    ) -> Result<FramePollStatus, FramePollError<E>>
    where
        F: FnMut(&mut TcpFramePublisher<'_>, RecvTs, &[u8]) -> Result<FramePollControl, E>,
    {
        if self.active.is_none() {
            return Ok(FramePollStatus {
                progress: false,
                stopped: false,
            });
        }

        let mut made_progress = false;
        let mut buf = [0_u8; 4096];
        loop {
            let read_result = match self.active.as_mut() {
                Some(stream) => stream.read(&mut buf),
                None => {
                    return Ok(FramePollStatus {
                        progress: made_progress,
                        stopped: false,
                    });
                }
            };
            match read_result {
                Ok(0) => {
                    self.disconnect_active();
                    return Ok(FramePollStatus {
                        progress: true,
                        stopped: false,
                    });
                }
                Ok(read) => {
                    made_progress = true;
                    self.read_buf.extend_from_slice(&buf[..read]);
                    let (return_from_poll, stopped) = self.drain_complete_frames_controlled(clock, &mut on_frame, max_frames)?;
                    if return_from_poll {
                        self.compact_read_buf();
                        return Ok(FramePollStatus { progress: true, stopped });
                    }
                    self.compact_read_buf();
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    self.compact_read_buf();
                    return Ok(FramePollStatus {
                        progress: made_progress,
                        stopped: false,
                    });
                }
                Err(error) => {
                    self.disconnect_active();
                    return Err(FramePollError::Io(IoError::Read(error)));
                }
            }
        }
    }

    fn drain_complete_frames_controlled<F, E>(
        &mut self,
        clock: &Clock,
        on_frame: &mut F,
        max_frames: Option<usize>,
    ) -> Result<(bool, bool), FramePollError<E>>
    where
        F: FnMut(&mut TcpFramePublisher<'_>, RecvTs, &[u8]) -> Result<FramePollControl, E>,
    {
        let Self {
            active,
            read_buf,
            read_start,
            write_buf,
            write_head,
            write_len,
            max_frame_len,
            max_pending_write_bytes,
            ..
        } = self;
        let mut frames = 0_usize;
        loop {
            if max_frames.is_some_and(|max_frames| frames >= max_frames) {
                return Ok((true, false));
            }
            if read_buf.len() - *read_start < SBE_HEADER_LEN {
                return Ok((false, false));
            }
            let block_len = u16::from_le_bytes([read_buf[*read_start], read_buf[*read_start + 1]]) as usize;
            let frame_len = SBE_HEADER_LEN + block_len;
            if frame_len > *max_frame_len {
                *active = None;
                read_buf.clear();
                *read_start = 0;
                *write_head = 0;
                *write_len = 0;
                return Err(FramePollError::Io(IoError::FrameTooLarge {
                    frame_len,
                    max_frame_len: *max_frame_len,
                }));
            }
            if read_buf.len() - *read_start < frame_len {
                return Ok((false, false));
            }

            let frame_start = *read_start;
            let frame_end = frame_start + frame_len;
            let recv_ts = RecvTs::new(clock.now_ns());
            *read_start = frame_end;
            let mut publisher = TcpFramePublisher {
                active,
                write_buf,
                write_head,
                write_len,
                max_pending_write_bytes: *max_pending_write_bytes,
            };
            let control = on_frame(&mut publisher, recv_ts, &read_buf[frame_start..frame_end]).map_err(FramePollError::Handler)?;
            frames += 1;
            if control == FramePollControl::Stop {
                return Ok((true, true));
            }
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

    fn flush_writes(&mut self) -> Result<bool, IoError> {
        let mut made_progress = false;
        let Some(stream) = self.active.as_mut() else {
            self.write_head = 0;
            self.write_len = 0;
            return Ok(false);
        };

        while self.write_len != 0 {
            let write_cap = self.write_buf.len();
            let contiguous = if self.write_head + self.write_len <= write_cap {
                self.write_len
            } else {
                write_cap - self.write_head
            };
            match stream.write(&self.write_buf[self.write_head..self.write_head + contiguous]) {
                Ok(0) => return Ok(made_progress),
                Ok(written) => {
                    made_progress = true;
                    self.write_head = (self.write_head + written) % write_cap;
                    self.write_len -= written;
                    if self.write_len == 0 {
                        self.write_head = 0;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Ok(made_progress);
                }
                Err(error) => {
                    self.disconnect_active();
                    return Err(IoError::Write(error));
                }
            }
        }

        Ok(made_progress)
    }

    fn disconnect_active(&mut self) {
        self.active = None;
        self.read_buf.clear();
        self.read_start = 0;
        self.write_head = 0;
        self.write_len = 0;
    }

    pub fn poll<F, E>(&mut self, clock: &Clock, on_frame: F) -> Result<bool, FramePollError<E>>
    where
        F: FnMut(&mut TcpFramePublisher<'_>, RecvTs, &[u8]) -> Result<(), E>,
    {
        self.poll_controlled(clock, adapt_controlled_callback(on_frame))
            .map(|status| status.progress)
    }

    pub fn poll_bounded<F, E>(&mut self, clock: &Clock, on_frame: F, max_frames: usize) -> Result<bool, FramePollError<E>>
    where
        F: FnMut(&mut TcpFramePublisher<'_>, RecvTs, &[u8]) -> Result<(), E>,
    {
        self.poll_bounded_controlled(clock, adapt_controlled_callback(on_frame), max_frames)
            .map(|status| status.progress)
    }

    pub(crate) fn poll_controlled<F, E>(&mut self, clock: &Clock, on_frame: F) -> Result<FramePollStatus, FramePollError<E>>
    where
        F: FnMut(&mut TcpFramePublisher<'_>, RecvTs, &[u8]) -> Result<FramePollControl, E>,
    {
        self.poll_inner_controlled(clock, on_frame, None)
    }

    pub(crate) fn poll_bounded_controlled<F, E>(
        &mut self,
        clock: &Clock,
        on_frame: F,
        max_frames: usize,
    ) -> Result<FramePollStatus, FramePollError<E>>
    where
        F: FnMut(&mut TcpFramePublisher<'_>, RecvTs, &[u8]) -> Result<FramePollControl, E>,
    {
        self.poll_inner_controlled(clock, on_frame, Some(max_frames))
    }

    fn poll_inner_controlled<F, E>(
        &mut self,
        clock: &Clock,
        on_frame: F,
        max_frames: Option<usize>,
    ) -> Result<FramePollStatus, FramePollError<E>>
    where
        F: FnMut(&mut TcpFramePublisher<'_>, RecvTs, &[u8]) -> Result<FramePollControl, E>,
    {
        // First accept any waiting client so new input can be read this tick.
        let accepted = self.accept_available().map_err(FramePollError::Io)?;

        // Then read whatever bytes are available and call the handler for each
        // complete frame. Partial frames stay buffered for a later poll.
        let read = self.read_available_controlled(clock, on_frame, max_frames)?;

        // Finally push out anything endpoint code queued while handling input.
        let flushed = self.flush_writes().map_err(FramePollError::Io)?;

        // The scheduler only needs to know whether this poll did useful work.
        Ok(FramePollStatus {
            progress: accepted || read.progress || flushed,
            stopped: read.stopped,
        })
    }

    pub fn publish(&mut self, frame: &[u8]) -> Result<(), IoError> {
        self.publisher().publish(frame)
    }

    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        self.publisher().queue_frame(frame)
    }

    pub fn flush_pending(&mut self) -> Result<bool, IoError> {
        self.flush_writes()
    }

    fn publisher(&mut self) -> TcpFramePublisher<'_> {
        TcpFramePublisher {
            active: &mut self.active,
            write_buf: &mut self.write_buf,
            write_head: &mut self.write_head,
            write_len: &mut self.write_len,
            max_pending_write_bytes: self.max_pending_write_bytes,
        }
    }
}

fn adapt_controlled_callback<F, E>(mut on_frame: F) -> impl FnMut(&mut TcpFramePublisher<'_>, RecvTs, &[u8]) -> Result<FramePollControl, E>
where
    F: FnMut(&mut TcpFramePublisher<'_>, RecvTs, &[u8]) -> Result<(), E>,
{
    move |publisher, recv_ts, frame| {
        on_frame(publisher, recv_ts, frame)?;
        Ok(FramePollControl::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{MessageKind, SBE_HEADER_LEN, encode_header};
    use std::{io::Read, net::TcpStream, thread, time::Duration};

    #[derive(Debug)]
    struct CapturedFrame {
        recv_ts: RecvTs,
        bytes: Vec<u8>,
    }

    fn frame(kind: MessageKind, payload: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0_u8; SBE_HEADER_LEN + payload.len()];
        encode_header(&mut bytes, payload.len() as u16, kind).expect("header");
        bytes[SBE_HEADER_LEN..].copy_from_slice(payload);
        bytes
    }

    fn tcp_io() -> Option<TcpFrameIo> {
        match TcpFrameIo::bind("127.0.0.1:0", 128, 1024) {
            Ok(io) => Some(io),
            Err(IoError::Bind(error)) if error.kind() == std::io::ErrorKind::PermissionDenied => None,
            Err(error) => panic!("tcp io: {error:?}"),
        }
    }

    fn poll_once(io: &mut TcpFrameIo, clock: &Clock, out: &mut Vec<CapturedFrame>) -> bool {
        io.poll(clock, |_, recv_ts, bytes| {
            out.push(CapturedFrame {
                recv_ts,
                bytes: bytes.to_vec(),
            });
            Ok::<(), std::convert::Infallible>(())
        })
        .expect("poll")
    }

    fn poll_until_frame(io: &mut TcpFrameIo, clock: &Clock, out: &mut Vec<CapturedFrame>) {
        for _ in 0..100 {
            poll_once(io, clock, out);
            if !out.is_empty() {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("expected frame");
    }

    #[test]
    fn tcp_frame_io_buffers_fragmented_frames() {
        let Some(mut io) = tcp_io() else {
            return;
        };
        let clock = Clock::new();
        let addr = io.local_addr().expect("addr");
        let mut client = TcpStream::connect(addr).expect("connect");
        let input = frame(MessageKind::Heartbeat, &[1, 2, 3, 4]);

        client.write_all(&input[..3]).expect("write partial");
        let mut out = Vec::new();
        for _ in 0..10 {
            poll_once(&mut io, &clock, &mut out);
        }
        assert!(out.is_empty());

        client.write_all(&input[3..]).expect("write rest");
        poll_until_frame(&mut io, &clock, &mut out);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].bytes, input);
    }

    #[test]
    fn tcp_frame_io_extracts_multiple_frames_from_one_read() {
        let Some(mut io) = tcp_io() else {
            return;
        };
        let clock = Clock::new();
        let addr = io.local_addr().expect("addr");
        let mut client = TcpStream::connect(addr).expect("connect");
        let first = frame(MessageKind::Heartbeat, &[1, 2, 3, 4]);
        let second = frame(MessageKind::Snapshot, &[5, 6, 7, 8]);
        client.write_all(&first).expect("write first");
        client.write_all(&second).expect("write second");

        let mut out = Vec::new();
        poll_until_frame(&mut io, &clock, &mut out);
        if out.len() < 2 {
            poll_until_frame(&mut io, &clock, &mut out);
        }

        assert_eq!(out.len(), 2);
        assert_eq!(out[0].bytes, first);
        assert_eq!(out[1].bytes, second);
        assert!(out[1].recv_ts >= out[0].recv_ts);
    }

    #[test]
    fn tcp_frame_io_publishes_to_active_client() {
        let Some(mut io) = tcp_io() else {
            return;
        };
        let clock = Clock::new();
        let addr = io.local_addr().expect("addr");
        let mut client = TcpStream::connect(addr).expect("connect");
        client.set_read_timeout(Some(Duration::from_secs(1))).expect("timeout");
        let output = frame(MessageKind::HeartbeatStatus, &[9, 8, 7, 6]);

        for _ in 0..100 {
            io.poll(&clock, |_, _, _| Ok::<(), std::convert::Infallible>(())).expect("accept");
            if io.active.is_some() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        io.publish(&output).expect("publish");

        let mut read = vec![0_u8; output.len()];
        client.read_exact(&mut read).expect("read output");
        assert_eq!(read, output);
    }

    #[test]
    fn tcp_frame_io_keeps_single_active_client() {
        let Some(mut io) = tcp_io() else {
            return;
        };
        let clock = Clock::new();
        let addr = io.local_addr().expect("addr");
        let mut first = TcpStream::connect(addr).expect("first");
        let mut second = TcpStream::connect(addr).expect("second");
        let first_frame = frame(MessageKind::Heartbeat, &[1, 0, 0, 0]);
        let second_frame = frame(MessageKind::Heartbeat, &[2, 0, 0, 0]);

        first.write_all(&first_frame).expect("write first");
        second.write_all(&second_frame).expect("write second");

        let mut out = Vec::new();
        poll_until_frame(&mut io, &clock, &mut out);

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].bytes, first_frame);
    }
}
