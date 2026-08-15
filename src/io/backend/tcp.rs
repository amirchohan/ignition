//! Nonblocking framed TCP input and output for one active client.
//!
//! [`TcpFrameIo`] owns the listener, the accepted client, an accumulated read
//! buffer, and a fixed-capacity write ring. It accepts at most one active client
//! while continuing to drain the listener backlog. [`TcpFramePublisher`] is a
//! borrowed view of that client's write state, allowing an endpoint callback to
//! respond while the input frame remains borrowed from the read buffer.
//!
//! Reads may contain partial or multiple SBE frames. The SBE block length in
//! each header determines the complete frame boundary; incomplete bytes remain
//! buffered for the next poll. Polling stops at its frame budget or when the
//! callback requests it. Output preserves byte order in a bounded ring and a
//! client is disconnected on overflow or terminal transport failure so stale
//! bytes cannot leak into a later connection.
//!
//! The parent backend assigns global input sequences and optionally records
//! accepted live frames. Endpoint modules above this transport decode and
//! encode the SBE messages; this module treats every complete frame as bytes.

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
};

/// Stack scratch space used by each nonblocking read syscall. This is a
/// transport batching choice, not the maximum legal wire-frame length.
const TCP_READ_CHUNK_LEN: usize = 4 * 1024;

use crate::{clock::Clock, domain::RecvTs, protocol::SBE_HEADER_LEN};

use super::{FrameLatencyStart, FramePollControl, FramePollError, FramePollStatus, InboundFrame, IoError};

/// Owner of a nonblocking listener, one active client, and framed byte buffers.
#[derive(Debug)]
pub struct TcpFrameIo {
    /// Nonblocking listener kept open across client sessions.
    listener: TcpListener,
    /// Sole client allowed to exchange frames with this runtime.
    active: Option<TcpStream>,
    /// Accumulated input bytes containing zero or more complete frames.
    read_buf: Vec<u8>,
    /// Index of the first input byte not yet delivered.
    read_start: usize,
    /// Fixed-size circular buffer for unsent output bytes.
    write_buf: Vec<u8>,
    /// Index of the first unsent output byte.
    write_head: usize,
    /// Number of unsent bytes in the output ring.
    write_len: usize,
    /// Maximum accepted complete SBE frame length.
    max_frame_len: usize,
    /// Maximum number of output bytes retained between flushes.
    max_pending_write_bytes: usize,
}

impl TcpFrameIo {
    /// Bind a nonblocking listener with bounded input frames and pending output.
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

    /// Return the listener's resolved local address.
    pub fn local_addr(&self) -> Result<SocketAddr, IoError> {
        self.listener.local_addr().map_err(IoError::LocalAddr)
    }

    /// Accept, read, dispatch up to `max_frames`, and flush queued responses.
    pub(crate) fn poll<F, E>(&mut self, clock: &Clock, max_frames: usize, on_frame: F) -> Result<FramePollStatus, FramePollError<E>>
    where
        F: FnMut(&mut TcpFramePublisher<'_>, InboundFrame<'_>) -> Result<FramePollControl, E>,
    {
        // accepting first lets a newly connected client contribute input during the same runtime tick.
        let accepted = self.accept_available().map_err(FramePollError::Io)?;

        // partial frames must stay buffered because TCP does not preserve application frame boundaries.
        let read = self.read_available(clock, on_frame, max_frames)?;

        // flushing after dispatch gives callback-generated responses a send opportunity in the same tick.
        let flushed = self.flush_pending().map_err(FramePollError::Io)?;

        Ok(FramePollStatus {
            progress: accepted || read.progress || flushed,
            stopped: read.stopped,
        })
    }

    /// Queue one frame on the active client's output ring.
    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        self.publisher().queue_frame(frame)
    }

    /// Flush the active client's pending output bytes.
    pub fn flush_pending(&mut self) -> Result<bool, IoError> {
        self.publisher().flush_pending()
    }

    /// Drain pending accepts while retaining only the first active client.
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
                    // dropping additional accepted streams enforces a single active client without blocking the listener.
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return Ok(made_progress);
                }
                Err(error) => return Err(IoError::Read(error)),
            }
        }
    }

    /// Read available bytes and dispatch complete frames within the poll budget.
    fn read_available<F, E>(&mut self, clock: &Clock, mut on_frame: F, max_frames: usize) -> Result<FramePollStatus, FramePollError<E>>
    where
        F: FnMut(&mut TcpFramePublisher<'_>, InboundFrame<'_>) -> Result<FramePollControl, E>,
    {
        if self.active.is_none() {
            return Ok(FramePollStatus {
                progress: false,
                stopped: false,
            });
        }

        let mut made_progress = false;
        let mut buf = [0_u8; TCP_READ_CHUNK_LEN];
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
                    // eof ends the session and invalidates both partial input and queued output.
                    self.disconnect_active();
                    return Ok(FramePollStatus {
                        progress: true,
                        stopped: false,
                    });
                }
                Ok(read) => {
                    made_progress = true;
                    self.read_buf.extend_from_slice(&buf[..read]);
                    let (return_from_poll, stopped) = self.dispatch_complete_frames(clock, &mut on_frame, max_frames)?;
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

    /// Deliver complete buffered frames and report whether polling should return.
    fn dispatch_complete_frames<F, E>(
        &mut self,
        clock: &Clock,
        on_frame: &mut F,
        max_frames: usize,
    ) -> Result<(bool, bool), FramePollError<E>>
    where
        F: FnMut(&mut TcpFramePublisher<'_>, InboundFrame<'_>) -> Result<FramePollControl, E>,
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
            if frames >= max_frames {
                return Ok((true, false));
            }
            if read_buf.len() - *read_start < SBE_HEADER_LEN {
                return Ok((false, false));
            }
            let block_len = u16::from_le_bytes([read_buf[*read_start], read_buf[*read_start + 1]]) as usize;
            // the protocol header is part of the delivered frame but its block length describes only the following body.
            let frame_len = SBE_HEADER_LEN + block_len;
            if frame_len > *max_frame_len {
                // clearing all session state prevents bytes following an invalid frame from being interpreted as a new frame.
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
            // advancing before the callback allows the frame slice and mutable output state to be borrowed independently.
            *read_start = frame_end;
            let mut publisher = TcpFramePublisher {
                active,
                write_buf,
                write_head,
                write_len,
                max_pending_write_bytes: *max_pending_write_bytes,
            };
            let control = on_frame(
                &mut publisher,
                InboundFrame {
                    recv_ts,
                    latency_start: FrameLatencyStart::HandlerEntry,
                    bytes: &read_buf[frame_start..frame_end],
                },
            )
            .map_err(FramePollError::Handler)?;
            frames += 1;
            if control == FramePollControl::Stop {
                return Ok((true, true));
            }
        }
    }

    /// Move undelivered input to the front without changing its byte order.
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

    /// Drop the active client and reset all session-owned buffered state.
    fn disconnect_active(&mut self) {
        self.active = None;
        self.read_buf.clear();
        self.read_start = 0;
        self.write_head = 0;
        self.write_len = 0;
    }

    /// Borrow the active client's output state through the publisher interface.
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

/// Raw frame publisher borrowed during transport polling.
#[derive(Debug)]
pub struct TcpFramePublisher<'a> {
    /// Borrowed active client receiving queued bytes.
    active: &'a mut Option<TcpStream>,
    /// Fixed-size circular buffer owned by the TCP runtime.
    write_buf: &'a mut Vec<u8>,
    /// Index of the first unsent byte in the circular buffer.
    write_head: &'a mut usize,
    /// Number of unsent bytes currently in the circular buffer.
    write_len: &'a mut usize,
    /// Configured upper bound for pending output bytes.
    max_pending_write_bytes: usize,
}

impl TcpFramePublisher<'_> {
    /// Create a shorter-lived publisher over the same TCP output state.
    pub fn reborrow(&mut self) -> TcpFramePublisher<'_> {
        TcpFramePublisher {
            active: self.active,
            write_buf: self.write_buf,
            write_head: self.write_head,
            write_len: self.write_len,
            max_pending_write_bytes: self.max_pending_write_bytes,
        }
    }

    /// Flush prior output, queue one frame, and attempt to flush it immediately.
    pub fn publish(&mut self, frame: &[u8]) -> Result<(), IoError> {
        self.flush_pending()?;
        self.queue_frame(frame)?;
        self.flush_pending()?;
        Ok(())
    }

    /// Append one frame to the active client's bounded output ring.
    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        let pending_bytes = self.write_len.saturating_add(frame.len());
        if pending_bytes > self.max_pending_write_bytes {
            // disconnecting also discards queued bytes so a future client cannot receive another session's output.
            self.disconnect_active();
            return Err(IoError::PendingWriteLimit {
                pending_bytes,
                max_pending_write_bytes: self.max_pending_write_bytes,
            });
        }
        self.push_write_bytes(frame);
        Ok(())
    }

    /// Write pending bytes until the socket blocks or the ring is empty.
    pub fn flush_pending(&mut self) -> Result<bool, IoError> {
        let mut made_progress = false;
        let Some(stream) = self.active.as_mut() else {
            // output has no valid recipient once the active session is absent.
            *self.write_head = 0;
            *self.write_len = 0;
            return Ok(false);
        };

        while *self.write_len != 0 {
            let write_cap = self.write_buf.len();
            // only the contiguous head segment can be passed to one socket write when the ring wraps.
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

    /// Drop the active session and all output owned by that session.
    fn disconnect_active(&mut self) {
        *self.active = None;
        *self.write_head = 0;
        *self.write_len = 0;
    }

    /// Copy bytes into the free tail of the circular output buffer.
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
        io.poll(clock, 128, |_, frame| {
            out.push(CapturedFrame {
                recv_ts: frame.recv_ts,
                bytes: frame.bytes.to_vec(),
            });
            Ok::<FramePollControl, std::convert::Infallible>(FramePollControl::Continue)
        })
        .expect("poll")
        .progress
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
            io.poll(&clock, 128, |_, _| {
                Ok::<FramePollControl, std::convert::Infallible>(FramePollControl::Continue)
            })
            .expect("accept");
            if io.active.is_some() {
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        io.queue_frame(&output).expect("queue");
        io.flush_pending().expect("publish");

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
