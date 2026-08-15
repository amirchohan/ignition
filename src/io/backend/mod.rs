//! Concrete framed-I/O backends behind the endpoint runtime facade.
//!
//! [`IOBackend`] owns exactly one transport or replay implementation and gives
//! [`super::IORuntime`] a uniform operation for each runtime action: open,
//! bounded poll, queue, flush, and replay-exhaustion inspection. The leaf
//! modules own their transport-specific buffers and resources.
//!
//! [`IOPublisher`] is the corresponding borrowed output facade supplied to an
//! endpoint handler. It borrows backend state so a handler can emit responses
//! without allocating a second queue or escaping the inbound frame's lifetime.
//!
//! TCP is the live bidirectional backend and may be paired with a file recorder.
//! File and in-memory backends drive deterministic replay. UDP is output-only,
//! while `Noop` intentionally accepts and discards output. Replay sequencing
//! remains coordinated by [`Mode`], not by individual leaf backends.

mod file;
mod in_memory;
mod tcp;
mod udp;

#[cfg(test)]
use std::net::SocketAddr;

use crate::{clock::Clock, runtime::Mode};

use super::{Endpoint, FrameLatencyStart, FramePollControl, FramePollError, FramePollStatus, IOConfig, InboundFrame, InputRecord, IoError};

pub use file::{FileFrameIo, FileFramePublisher};
pub use in_memory::{InMemoryFrameIo, InMemoryFramePublisher};
pub use tcp::{TcpFrameIo, TcpFramePublisher};
pub use udp::{UdpFramePublisher, UdpMulticastConfig};

/// Enum-gated endpoint I/O backend.
#[derive(Debug)]
pub enum IOBackend {
    /// Disabled backend.
    Noop,
    /// TCP framed input/output backend.
    Tcp(#[doc = "Owned TCP listener, client, and frame buffers."] TcpFrameIo),
    /// UDP multicast output backend.
    Udp(#[doc = "Owned multicast socket and pending datagrams."] UdpFramePublisher),
    /// File-backed replay/capture backend.
    File(#[doc = "Owned replay reader and optional capture writer."] FileFrameIo),
    /// In-memory replay/capture backend for benchmarks.
    InMemory(#[doc = "Owned replay records and output counters."] InMemoryFrameIo),
}

impl IOBackend {
    /// Open the configured leaf backend and its optional live-input recorder.
    pub(crate) fn open(config: IOConfig) -> Result<(Self, Option<FileFrameIo>), IoError> {
        match config {
            IOConfig::Noop => Ok((Self::Noop, None)),
            IOConfig::Tcp {
                bind_addr,
                max_frame_len,
                max_pending_write_bytes,
                record_path,
            } => {
                let backend = Self::Tcp(TcpFrameIo::bind(&bind_addr, max_frame_len, max_pending_write_bytes)?);
                let recorder = match record_path {
                    Some(path) => Some(FileFrameIo::open(None, Some(path), max_frame_len)?),
                    None => None,
                };
                Ok((backend, recorder))
            }
            IOConfig::Udp {
                bind_addr,
                group_addr,
                multicast_ttl,
                multicast_loop_v4,
                max_datagram_len,
                max_pending_datagram_bytes,
            } => Ok((
                Self::Udp(UdpFramePublisher::bind_multicast(UdpMulticastConfig {
                    bind_addr,
                    group_addr,
                    multicast_ttl,
                    multicast_loop_v4,
                    max_datagram_len,
                    max_pending_datagram_bytes,
                })?),
                None,
            )),
            IOConfig::File {
                input_path,
                output_path,
                max_frame_len,
            } => Ok((Self::File(FileFrameIo::open(input_path, output_path, max_frame_len)?), None)),
            IOConfig::InMemory { input, max_frame_len } => Ok((Self::InMemory(InMemoryFrameIo::new(input, max_frame_len)), None)),
            IOConfig::ScheduledInMemory {
                input,
                max_frame_len,
                target_rate_per_sec,
            } => Ok((
                Self::InMemory(InMemoryFrameIo::new_scheduled(input, max_frame_len, target_rate_per_sec)?),
                None,
            )),
        }
    }

    /// Poll bounded ingress through the selected backend.
    pub(crate) fn poll<F, E>(
        &mut self,
        endpoint: Endpoint,
        clock: &Clock,
        mode: &mut Mode,
        recorder: &mut Option<FileFrameIo>,
        max_frames: usize,
        mut on_frame: F,
    ) -> Result<FramePollStatus, FramePollError<E>>
    where
        F: FnMut(&mut IOPublisher<'_>, InboundFrame<'_>) -> Result<FramePollControl, E>,
    {
        match self {
            Self::Tcp(io) => {
                let result = io.poll(clock, max_frames, |publisher, frame| {
                    // assigning and recording before handling makes accepted live input replayable even if handling fails.
                    let input_seq = mode.accept_live_frame().map_err(BackendFrameError::Io)?;
                    if let Some(recorder) = recorder.as_mut() {
                        recorder
                            .record_input(&InputRecord {
                                input_seq,
                                endpoint,
                                recv_ts: frame.recv_ts,
                                frame: frame.bytes.to_vec(),
                            })
                            .map_err(BackendFrameError::Io)?;
                    }

                    let mut publisher = IOPublisher::Tcp(publisher.reborrow());
                    on_frame(&mut publisher, frame).map_err(BackendFrameError::Handler)
                });
                let mut status = flatten_backend_poll_error(result)?;
                if let Some(recorder) = recorder.as_mut() {
                    status.progress |= recorder.flush_pending().map_err(FramePollError::Io)?;
                }
                Ok(status)
            }
            Self::File(io) => io.poll(endpoint, mode, max_frames, on_frame),
            Self::InMemory(io) => io.poll(clock, endpoint, mode, max_frames, on_frame),
            Self::Noop | Self::Udp(_) => Ok(FramePollStatus::IDLE),
        }
    }

    /// Queue one outbound frame through the selected backend.
    pub(crate) fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        match self {
            Self::Noop => Ok(()),
            Self::Tcp(io) => io.queue_frame(frame),
            Self::Udp(io) => io.queue_frame(frame),
            Self::File(io) => io.queue_frame(frame),
            Self::InMemory(io) => io.queue_frame(frame),
        }
    }

    /// Flush pending output through the selected backend.
    pub(crate) fn flush_pending(&mut self) -> Result<bool, IoError> {
        match self {
            Self::Noop => Ok(false),
            Self::Tcp(io) => io.flush_pending(),
            Self::Udp(io) => io.flush_pending(),
            Self::File(io) => io.flush_pending(),
            Self::InMemory(io) => io.flush_pending(),
        }
    }

    /// Report whether replay input is exhausted for replay-capable backends.
    pub(crate) fn replay_input_exhausted(&mut self) -> Result<bool, IoError> {
        match self {
            Self::File(io) => io.input_exhausted(),
            Self::InMemory(io) => Ok(io.input_exhausted()),
            Self::Noop => Ok(true),
            Self::Tcp(_) | Self::Udp(_) => Err(IoError::ReplayExpectedInLive),
        }
    }

    #[cfg(test)]
    pub(crate) fn tcp_local_addr(&self) -> Result<SocketAddr, IoError> {
        match self {
            Self::Tcp(io) => io.local_addr(),
            Self::Noop | Self::Udp(_) | Self::File(_) | Self::InMemory(_) => Err(IoError::ReplayExpectedInLive),
        }
    }
}

/// Borrowed frame publisher passed to endpoint frame handlers.
#[derive(Debug)]
pub enum IOPublisher<'a> {
    /// Publisher that drops all frames successfully.
    Noop,
    /// TCP publisher borrowed from the active framed TCP session.
    Tcp(#[doc = "Borrowed access to the TCP backend's active client and write ring."] TcpFramePublisher<'a>),
    /// File publisher borrowed from a file-backed I/O runtime.
    File(#[doc = "Borrowed access to the file backend's output writer."] FileFramePublisher<'a>),
    /// In-memory publisher borrowed from a benchmark I/O runtime.
    InMemory(#[doc = "Borrowed access to the benchmark backend's output counters."] InMemoryFramePublisher<'a>),
}

impl IOPublisher<'_> {
    /// Publish one frame immediately when the backend supports immediate flush.
    pub fn publish(&mut self, frame: &[u8]) -> Result<(), IoError> {
        match self {
            Self::Noop => Ok(()),
            Self::Tcp(publisher) => publisher.publish(frame),
            Self::File(publisher) => publisher.publish(frame),
            Self::InMemory(publisher) => publisher.publish(frame),
        }
    }

    /// Queue one frame for later flush.
    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        match self {
            Self::Noop => Ok(()),
            Self::Tcp(publisher) => publisher.queue_frame(frame),
            Self::File(publisher) => publisher.queue_frame(frame),
            Self::InMemory(publisher) => publisher.queue_frame(frame),
        }
    }
}

/// Internal adapter error used while TCP polling invokes an endpoint callback.
#[derive(Debug)]
enum BackendFrameError<E> {
    /// I/O failure raised while sequencing or recording a live frame.
    Io(#[doc = "I/O-layer failure."] IoError),
    /// Failure returned by the endpoint callback.
    Handler(#[doc = "Endpoint-defined handler failure."] E),
}

/// Restore the public polling error shape after the TCP callback adapter runs.
fn flatten_backend_poll_error<E>(
    result: Result<FramePollStatus, FramePollError<BackendFrameError<E>>>,
) -> Result<FramePollStatus, FramePollError<E>> {
    match result {
        Ok(status) => Ok(status),
        Err(FramePollError::Io(error)) => Err(FramePollError::Io(error)),
        Err(FramePollError::Handler(BackendFrameError::Io(error))) => Err(FramePollError::Io(error)),
        Err(FramePollError::Handler(BackendFrameError::Handler(error))) => Err(FramePollError::Handler(error)),
    }
}
