//! Endpoint I/O for live traffic, replay traffic, and captured output.
//!
//! Each endpoint runtime owns one `IORuntime`. The endpoint runtime asks this
//! module for complete frames, then the endpoint runtime decodes those bytes
//! into control, gateway, or market-data messages.
//!
//! In live mode, TCP reads bytes from a client connection and turns them into
//! complete frames. When a frame is ready, `Mode` assigns the next input
//! sequence number. If recording is enabled, the frame, endpoint, receive
//! timestamp, and input sequence are written to a file before the endpoint
//! handles the frame.
//!
//! In replay mode, file I/O looks at the next recorded frame without consuming
//! it. The frame is only delivered when its input sequence is the next sequence
//! expected by `Mode` and the recorded endpoint matches this `IORuntime`. After
//! the endpoint handles the frame successfully, the replay input sequence moves
//! forward.
//!
//! Endpoint code sends responses through `IOPublisher`. This module only moves
//! framed bytes in and out; it does not decode business messages or update the
//! matching engine.

use std::path::PathBuf;

use crate::{domain::RecvTs, runtime::Mode};

pub mod file;
pub mod tcp;
pub mod udp;

pub use file::{FileFrameIo, FileFramePublisher, InputRecord};
pub use tcp::{TcpFrameIo, TcpFramePublisher};
pub use udp::{UdpFramePublisher, UdpMulticastConfig};

/// Runtime-owned endpoint IO adapter.
#[derive(Debug)]
pub struct IORuntime {
    endpoint: Endpoint,
    backend: IOBackend,
    recorder: Option<FileFrameIo>,
}

impl IORuntime {
    /// Build an endpoint IO adapter from a mode, endpoint, and backend config.
    pub fn new(endpoint: Endpoint, config: IOConfig) -> Result<Self, IoError> {
        let (backend, recorder) = match config {
            IOConfig::Noop => (IOBackend::Noop, None),
            IOConfig::Tcp {
                bind_addr,
                max_frame_len,
                max_pending_write_bytes,
                record_path,
            } => {
                let io = TcpFrameIo::bind(&bind_addr, max_frame_len, max_pending_write_bytes)?;
                let recorder = match record_path {
                    Some(path) => Some(FileFrameIo::open(None, Some(path), max_frame_len)?),
                    None => None,
                };
                (IOBackend::Tcp(io), recorder)
            }
            IOConfig::Udp {
                bind_addr,
                group_addr,
                multicast_ttl,
                multicast_loop_v4,
                max_datagram_len,
                max_pending_datagram_bytes,
            } => (
                IOBackend::Udp(UdpFramePublisher::bind_multicast(UdpMulticastConfig {
                    bind_addr,
                    group_addr,
                    multicast_ttl,
                    multicast_loop_v4,
                    max_datagram_len,
                    max_pending_datagram_bytes,
                })?),
                None,
            ),
            IOConfig::File {
                input_path,
                output_path,
                max_frame_len,
            } => (IOBackend::File(FileFrameIo::open(input_path, output_path, max_frame_len)?), None),
        };

        Ok(Self {
            endpoint,
            backend,
            recorder,
        })
    }

    /// Poll inbound frames without an explicit frame budget.
    pub fn poll<F, E>(&mut self, mode: &mut Mode, on_frame: F) -> Result<bool, FramePollError<E>>
    where
        F: FnMut(&mut IOPublisher<'_>, RecvTs, &[u8]) -> Result<(), E>,
    {
        self.poll_inner(mode, on_frame, None)
    }

    /// Poll inbound frames with a maximum number of frames accepted this call.
    pub fn poll_bounded<F, E>(&mut self, mode: &mut Mode, on_frame: F, max_frames: usize) -> Result<bool, FramePollError<E>>
    where
        F: FnMut(&mut IOPublisher<'_>, RecvTs, &[u8]) -> Result<(), E>,
    {
        self.poll_inner(mode, on_frame, Some(max_frames))
    }

    fn poll_inner<F, E>(&mut self, mode: &mut Mode, on_frame: F, max_frames: Option<usize>) -> Result<bool, FramePollError<E>>
    where
        F: FnMut(&mut IOPublisher<'_>, RecvTs, &[u8]) -> Result<(), E>,
    {
        if matches!(&self.backend, IOBackend::Tcp(_)) {
            self.poll_tcp(mode, on_frame, max_frames)
        } else if matches!(&self.backend, IOBackend::File(_)) {
            self.poll_file(mode, on_frame)
        } else {
            // These backends do not produce inbound frames here.
            Ok(false)
        }
    }

    fn poll_tcp<F, E>(&mut self, mode: &mut Mode, mut on_frame: F, max_frames: Option<usize>) -> Result<bool, FramePollError<E>>
    where
        F: FnMut(&mut IOPublisher<'_>, RecvTs, &[u8]) -> Result<(), E>,
    {
        let IOBackend::Tcp(io) = &mut self.backend else {
            return Ok(false);
        };

        let endpoint = self.endpoint;
        let recorder = &mut self.recorder;
        let mut callback = |publisher: &mut TcpFramePublisher<'_>, recv_ts, bytes: &[u8]| {
            // Live input becomes deterministic at this boundary.
            // Every accepted frame gets one global input sequence.
            let input_seq = mode.accept_live_frame().map_err(RuntimeFrameError::Io)?;

            // If this endpoint is recording, write exactly what replay
            // will need later: sequence, endpoint, timestamp, and bytes.
            if let Some(recorder) = recorder.as_mut() {
                recorder
                    .record_input(&InputRecord {
                        input_seq,
                        endpoint,
                        recv_ts,
                        frame: bytes.to_vec(),
                    })
                    .map_err(RuntimeFrameError::Io)?;
            }

            // Endpoint code sees an endpoint-neutral publisher, not the
            // concrete TCP transport object.
            let mut publisher = IOPublisher::Tcp(publisher.reborrow());
            on_frame(&mut publisher, recv_ts, bytes).map_err(RuntimeFrameError::Handler)
        };

        // TCP owns connection handling and frame assembly. This layer adds
        // sequencing, optional recording, and endpoint dispatch.
        let result = match max_frames {
            Some(max_frames) => io.poll_bounded(&mut callback, max_frames),
            None => io.poll(&mut callback),
        };
        flatten_poll_error(result)
    }

    fn poll_file<F, E>(&mut self, mode: &mut Mode, mut on_frame: F) -> Result<bool, FramePollError<E>>
    where
        F: FnMut(&mut IOPublisher<'_>, RecvTs, &[u8]) -> Result<(), E>,
    {
        let IOBackend::File(io) = &mut self.backend else {
            return Ok(false);
        };

        // Replay peeks first so this endpoint does not consume a frame that
        // belongs to another endpoint or a later input sequence.
        let Some(record) = io.peek_record().map_err(FramePollError::Io)?.cloned() else {
            return Ok(false);
        };

        // If this is not the next global input, another endpoint must run
        // first. Returning no progress lets the runtime poll it.
        if record.input_seq != mode.expected_replay_input_seq().map_err(FramePollError::Io)? {
            return Ok(false);
        }

        // A matching sequence with the wrong endpoint means the replay file was
        // wired to the wrong runtime.
        if record.endpoint != self.endpoint {
            return Err(FramePollError::Io(IoError::EndpointMismatch {
                configured: self.endpoint,
                record: record.endpoint,
            }));
        }

        // Now the frame is known to be this endpoint's next input, so it is
        // safe to consume and deliver to endpoint code.
        let record = io.take_peeked().expect("peeked record");
        let mut publisher = IOPublisher::File(FileFramePublisher::new(io));
        on_frame(&mut publisher, record.recv_ts, &record.frame).map_err(FramePollError::Handler)?;

        // Replay advances only after the endpoint handled the frame.
        mode.accept_replay_frame(record.input_seq).map_err(FramePollError::Io)?;
        Ok(true)
    }

    /// Queue one outbound frame through this endpoint backend.
    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        match &mut self.backend {
            IOBackend::Noop => Ok(()),
            IOBackend::Tcp(io) => io.queue_frame(frame),
            IOBackend::Udp(io) => io.queue_frame(frame),
            IOBackend::File(io) => io.queue_frame(frame),
        }
    }

    /// Flush pending outbound frames.
    pub fn flush_pending(&mut self) -> Result<bool, IoError> {
        match &mut self.backend {
            IOBackend::Noop => Ok(false),
            IOBackend::Tcp(io) => io.flush_pending(),
            IOBackend::Udp(io) => io.flush_pending(),
            IOBackend::File(io) => io.flush_pending(),
        }
    }

    /// True when this endpoint's file-backed replay input is fully drained.
    pub fn replay_input_exhausted(&mut self) -> Result<bool, IoError> {
        match &mut self.backend {
            IOBackend::File(io) => io.input_exhausted(),
            IOBackend::Noop => Ok(true),
            IOBackend::Tcp(_) | IOBackend::Udp(_) => Err(IoError::ReplayExpectedInLive),
        }
    }
}

fn flatten_poll_error<E>(result: Result<bool, FramePollError<RuntimeFrameError<E>>>) -> Result<bool, FramePollError<E>> {
    match result {
        Ok(progress) => Ok(progress),
        Err(FramePollError::Io(error)) => Err(FramePollError::Io(error)),
        Err(FramePollError::Handler(RuntimeFrameError::Io(error))) => Err(FramePollError::Io(error)),
        Err(FramePollError::Handler(RuntimeFrameError::Handler(error))) => Err(FramePollError::Handler(error)),
    }
}

/// Enum-gated endpoint IO backend.
#[derive(Debug)]
pub enum IOBackend {
    /// Disabled backend.
    Noop,
    /// TCP framed input/output backend.
    Tcp(TcpFrameIo),
    /// UDP multicast output backend.
    Udp(UdpFramePublisher),
    /// File-backed replay/capture backend.
    File(FileFrameIo),
}

/// Borrowed frame publisher passed to endpoint frame handlers.
#[derive(Debug)]
pub enum IOPublisher<'a> {
    /// Publisher that drops all frames successfully.
    Noop,
    /// TCP publisher borrowed from the active framed TCP session.
    Tcp(TcpFramePublisher<'a>),
    /// File publisher borrowed from a file-backed IO runtime.
    File(FileFramePublisher<'a>),
}

impl IOPublisher<'_> {
    /// Publish one frame immediately when the backend supports immediate flush.
    pub fn publish(&mut self, frame: &[u8]) -> Result<(), IoError> {
        match self {
            Self::Noop => Ok(()),
            Self::Tcp(publisher) => publisher.publish(frame),
            Self::File(publisher) => publisher.publish(frame),
        }
    }

    /// Queue one frame for later flush.
    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        match self {
            Self::Noop => Ok(()),
            Self::Tcp(publisher) => publisher.queue_frame(frame),
            Self::File(publisher) => publisher.queue_frame(frame),
        }
    }
}

/// Transport and shared IO-layer failure.
#[derive(Debug)]
pub enum IoError {
    Bind(std::io::Error),
    Open(std::io::Error),
    Create(std::io::Error),
    LocalAddr(std::io::Error),
    Configure(std::io::Error),
    Read(std::io::Error),
    Write(std::io::Error),
    FrameTooLarge {
        frame_len: usize,
        max_frame_len: usize,
    },
    PendingWriteLimit {
        pending_bytes: usize,
        max_pending_write_bytes: usize,
    },
    InvalidEndpoint(u8),
    EndpointMismatch {
        configured: Endpoint,
        record: Endpoint,
    },
    ReplayExpectedInLive,
    LiveInputInReplay,
    ReplaySequenceMismatch {
        expected: u64,
        actual: u64,
    },
    ReplayStalled {
        expected_input_seq: u64,
    },
}

/// Error returned by framed IO polling when either transport IO or frame handling fails.
#[derive(Debug)]
pub enum FramePollError<E> {
    Io(IoError),
    Handler(E),
}

/// Logical endpoint used for routing, live recording, and deterministic replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    /// Administrative/control-plane endpoint.
    Control,
    /// Order-entry gateway endpoint.
    Gateway,
    /// Outbound market-data endpoint.
    MarketData,
}

impl Endpoint {
    /// Return the stable byte tag used in file-backed replay records.
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Control => 1,
            Self::Gateway => 2,
            Self::MarketData => 3,
        }
    }

    /// Decode the stable file-record endpoint tag.
    pub fn from_u8(value: u8) -> Result<Self, IoError> {
        match value {
            1 => Ok(Self::Control),
            2 => Ok(Self::Gateway),
            3 => Ok(Self::MarketData),
            _ => Err(IoError::InvalidEndpoint(value)),
        }
    }
}

#[derive(Debug)]
enum RuntimeFrameError<E> {
    Io(IoError),
    Handler(E),
}

/// Endpoint IO backend configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IOConfig {
    /// Disabled IO backend.
    Noop,
    /// Nonblocking TCP framed input/output.
    Tcp {
        /// Listener bind address.
        bind_addr: String,
        /// Maximum accepted SBE frame length including the SBE header.
        max_frame_len: usize,
        /// Maximum queued outbound bytes before disconnecting the active client.
        max_pending_write_bytes: usize,
        /// Optional file path for recording observed live input frames.
        record_path: Option<PathBuf>,
    },
    /// Nonblocking UDP multicast output.
    Udp {
        /// Local UDP bind address.
        bind_addr: String,
        /// IPv4 multicast destination address.
        group_addr: String,
        /// IPv4 multicast TTL.
        multicast_ttl: u32,
        /// Whether multicast loopback is enabled.
        multicast_loop_v4: bool,
        /// Maximum encoded datagram length.
        max_datagram_len: usize,
        /// Maximum queued datagram bytes before reporting backpressure.
        max_pending_datagram_bytes: usize,
    },
    /// File-backed replay input and/or output capture.
    File {
        /// Optional replay input file containing wrapped input records.
        input_path: Option<PathBuf>,
        /// Optional output/capture file path.
        output_path: Option<PathBuf>,
        /// Maximum accepted frame length in file records.
        max_frame_len: usize,
    },
}

impl IOConfig {
    /// Return the buffer size endpoint runtimes should use when encoding frames
    /// for this backend.
    pub const fn frame_capacity(&self) -> usize {
        match self {
            Self::Noop => 4096,
            Self::Tcp { max_frame_len, .. } | Self::File { max_frame_len, .. } => *max_frame_len,
            Self::Udp { max_datagram_len, .. } => *max_datagram_len,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    use crate::{domain::RecvTs, runtime::Mode};

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("ignition-{name}-{}-{}.bin", std::process::id(), unique_suffix()))
    }

    fn unique_suffix() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    }

    fn write_record(path: &Path, record: InputRecord) {
        let mut io = FileFrameIo::open(None, Some(path.to_path_buf()), 128).expect("writer");
        io.record_input(&record).expect("record");
        io.flush_pending().expect("flush");
    }

    #[test]
    fn replay_file_backend_releases_only_expected_global_input_sequence() {
        let gateway_path = temp_path("gateway-replay");
        let control_path = temp_path("control-replay");
        write_record(
            &gateway_path,
            InputRecord {
                input_seq: 1,
                endpoint: Endpoint::Gateway,
                recv_ts: RecvTs::new(10),
                frame: vec![1, 2, 3],
            },
        );
        write_record(
            &control_path,
            InputRecord {
                input_seq: 2,
                endpoint: Endpoint::Control,
                recv_ts: RecvTs::new(11),
                frame: vec![4, 5, 6],
            },
        );

        let mut mode = Mode::Replay { expected_input_seq: 1 };
        let mut control = IORuntime::new(
            Endpoint::Control,
            IOConfig::File {
                input_path: Some(control_path.clone()),
                output_path: None,
                max_frame_len: 128,
            },
        )
        .expect("control");
        let mut gateway = IORuntime::new(
            Endpoint::Gateway,
            IOConfig::File {
                input_path: Some(gateway_path.clone()),
                output_path: None,
                max_frame_len: 128,
            },
        )
        .expect("gateway");

        let mut seen = Vec::new();
        {
            assert!(
                !control
                    .poll(&mut mode, |_, recv_ts, frame| {
                        seen.push((Endpoint::Control, recv_ts.raw(), frame.to_vec()));
                        Ok::<(), std::convert::Infallible>(())
                    })
                    .expect("control poll")
            );
        }
        {
            assert!(
                gateway
                    .poll(&mut mode, |_, recv_ts, frame| {
                        seen.push((Endpoint::Gateway, recv_ts.raw(), frame.to_vec()));
                        Ok::<(), std::convert::Infallible>(())
                    })
                    .expect("gateway poll")
            );
        }
        {
            assert!(
                control
                    .poll(&mut mode, |_, recv_ts, frame| {
                        seen.push((Endpoint::Control, recv_ts.raw(), frame.to_vec()));
                        Ok::<(), std::convert::Infallible>(())
                    })
                    .expect("control poll")
            );
        }

        assert_eq!(
            seen,
            vec![(Endpoint::Gateway, 10, vec![1, 2, 3]), (Endpoint::Control, 11, vec![4, 5, 6])]
        );
        assert_eq!(mode, Mode::Replay { expected_input_seq: 3 });

        let _ = std::fs::remove_file(gateway_path);
        let _ = std::fs::remove_file(control_path);
    }
}
