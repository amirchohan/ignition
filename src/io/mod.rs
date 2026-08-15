//! Framed endpoint I/O for live traffic, deterministic replay, and benchmarks.
//!
//! This module is the transport boundary between the control, gateway, and
//! market-data runtimes and the byte-oriented mechanisms that carry their
//! frames. It owns framing, bounded polling, output buffering, live-input
//! recording, and replay ordering. It does not decode business messages or
//! mutate matching-engine state.
//!
//! # Main components
//!
//! - [`IORuntime`] is the endpoint-facing facade. It owns exactly one
//!   [`IOBackend`], its logical [`Endpoint`], and an optional file recorder.
//! - [`IOConfig`] selects and sizes the concrete backend at startup.
//! - [`InputRecord`] carries the metadata required to reproduce one inbound
//!   frame deterministically.
//! - [`IOPublisher`] is a short-lived borrowed view of backend-owned output
//!   state, passed to an endpoint handler while an inbound frame is borrowed.
//!
//! # Data flow and invariants
//!
//! Live TCP input is framed first, assigned a global input sequence by
//! [`Mode`], optionally recorded, and only then handed to its endpoint. Replay
//! backends peek without consuming until the record's sequence is globally due
//! and its endpoint matches the runtime. The replay sequence advances only
//! after the handler succeeds. This keeps live recordings and replay behavior
//! aligned across independently polled endpoint runtimes.
//!
//! Polling is bounded so one endpoint cannot monopolize a runtime tick. Output
//! is queued into storage owned by the selected backend and flushed explicitly.
//! The clock supplies receive or scheduled-arrival timestamps; protocol modules
//! above this layer remain responsible for interpreting the frame bytes.

use std::path::PathBuf;

use crate::{clock::Clock, domain::RecvTs, runtime::Mode};

mod backend;

pub use backend::{
    FileFrameIo, FileFramePublisher, IOBackend, IOPublisher, InMemoryFrameIo, InMemoryFramePublisher, TcpFrameIo, TcpFramePublisher,
    UdpFramePublisher, UdpMulticastConfig,
};

/// Endpoint-facing owner of one configured I/O backend and optional recorder.
#[derive(Debug)]
pub struct IORuntime {
    /// Logical destination used to validate and record inbound frames.
    endpoint: Endpoint,
    /// Concrete transport or replay mechanism selected at startup.
    backend: IOBackend,
    /// Optional file sink that records accepted live input before handling.
    recorder: Option<FileFrameIo>,
}

impl IORuntime {
    /// Build an endpoint IO adapter from a mode, endpoint, and backend config.
    pub fn new(endpoint: Endpoint, config: IOConfig) -> Result<Self, IoError> {
        let startup_log = IOStartupLog::from_config(&config);
        let (backend, recorder) = IOBackend::open(config)?;

        tracing::info!(
            "{}Runtime started. ingress: {} | egress: {}. Recorder: {}",
            endpoint.runtime_name(),
            startup_log.ingress,
            startup_log.egress,
            startup_log.recorder
        );

        Ok(Self {
            endpoint,
            backend,
            recorder,
        })
    }

    /// Poll bounded ingress until the budget is reached or the handler stops the batch.
    pub(crate) fn poll<F, E>(
        &mut self,
        clock: &Clock,
        mode: &mut Mode,
        max_frames: usize,
        on_frame: F,
    ) -> Result<FramePollStatus, FramePollError<E>>
    where
        F: FnMut(&mut IOPublisher<'_>, InboundFrame<'_>) -> Result<FramePollControl, E>,
    {
        self.backend
            .poll(self.endpoint, clock, mode, &mut self.recorder, max_frames.max(1), on_frame)
    }

    /// Queue one outbound frame through this endpoint backend.
    pub fn queue_frame(&mut self, frame: &[u8]) -> Result<(), IoError> {
        self.backend.queue_frame(frame)
    }

    /// Flush pending backend and recorder output, reporting whether either advanced.
    pub fn flush_pending(&mut self) -> Result<bool, IoError> {
        let backend_progress = self.backend.flush_pending()?;
        let recorder_progress = match &mut self.recorder {
            Some(recorder) => recorder.flush_pending()?,
            None => false,
        };
        Ok(backend_progress || recorder_progress)
    }

    /// Return whether this endpoint's replay-capable backend is fully drained.
    pub fn replay_input_exhausted(&mut self) -> Result<bool, IoError> {
        self.backend.replay_input_exhausted()
    }

    #[cfg(test)]
    fn tcp_local_addr(&self) -> Result<std::net::SocketAddr, IoError> {
        self.backend.tcp_local_addr()
    }
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
    /// In-memory replay input and output counting for benchmarks.
    InMemory {
        /// Replay records retained in deterministic delivery order.
        input: Vec<InputRecord>,
        /// Maximum accepted input and counted output frame length.
        max_frame_len: usize,
    },
    /// Rate-scheduled in-memory replay input and output counting for benchmarks.
    ScheduledInMemory {
        /// Replay records retained in deterministic delivery order.
        input: Vec<InputRecord>,
        /// Maximum accepted input and counted output frame length.
        max_frame_len: usize,
        /// Open-loop release rate used to derive absolute arrival times.
        target_rate_per_sec: u64,
    },
}

impl IOConfig {
    /// Return the buffer size endpoint runtimes should use when encoding frames
    /// for this backend.
    pub const fn frame_capacity(&self) -> usize {
        match self {
            Self::Noop => 4096,
            Self::Tcp { max_frame_len, .. }
            | Self::File { max_frame_len, .. }
            | Self::InMemory { max_frame_len, .. }
            | Self::ScheduledInMemory { max_frame_len, .. } => *max_frame_len,
            Self::Udp { max_datagram_len, .. } => *max_datagram_len,
        }
    }
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

    /// Return the stable endpoint label used in startup diagnostics.
    const fn runtime_name(self) -> &'static str {
        match self {
            Self::Control => "Control",
            Self::Gateway => "Gateway",
            Self::MarketData => "MarketData",
        }
    }
}

/// One recorded input frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputRecord {
    /// Global input sequence assigned in live order and enforced during replay.
    pub input_seq: u64,
    /// Endpoint that originally accepted the frame.
    pub endpoint: Endpoint,
    /// Receive timestamp captured for deterministic replay.
    pub recv_ts: RecvTs,
    /// Complete encoded protocol frame.
    pub frame: Vec<u8>,
}

/// Transport and shared IO-layer failure.
#[derive(Debug)]
pub enum IoError {
    /// A socket could not bind to its configured local address.
    Bind(#[doc = "Underlying socket error."] std::io::Error),
    /// A replay input file could not be opened.
    Open(#[doc = "Underlying file error."] std::io::Error),
    /// An output or recording file could not be created.
    Create(#[doc = "Underlying file error."] std::io::Error),
    /// A socket's bound local address could not be read.
    LocalAddr(#[doc = "Underlying socket error."] std::io::Error),
    /// A transport or schedule configuration could not be applied.
    Configure(#[doc = "Underlying configuration error."] std::io::Error),
    /// An input source failed while reading bytes.
    Read(#[doc = "Underlying read error."] std::io::Error),
    /// An output sink failed while writing or flushing bytes.
    Write(#[doc = "Underlying write error."] std::io::Error),
    /// A frame exceeds the configured transport or replay limit.
    FrameTooLarge {
        /// Observed frame length in bytes.
        frame_len: usize,
        /// Maximum accepted frame length in bytes.
        max_frame_len: usize,
    },
    /// Queuing a frame would exceed the configured pending-output bound.
    PendingWriteLimit {
        /// Pending byte count that the requested write would produce.
        pending_bytes: usize,
        /// Maximum permitted pending byte count.
        max_pending_write_bytes: usize,
    },
    /// A replay record contains an unknown endpoint tag.
    InvalidEndpoint(#[doc = "Unrecognized on-disk endpoint tag."] u8),
    /// A replay record belongs to a different endpoint runtime.
    EndpointMismatch {
        /// Endpoint owned by the runtime reading the record.
        configured: Endpoint,
        /// Endpoint encoded in the replay record.
        record: Endpoint,
    },
    /// Replay exhaustion was queried on a live-only backend.
    ReplayExpectedInLive,
    /// A live frame was offered while the runtime is in replay mode.
    LiveInputInReplay,
    /// A replay record is not the globally expected next input.
    ReplaySequenceMismatch {
        /// Next input sequence required by replay mode.
        expected: u64,
        /// Input sequence carried by the accepted record.
        actual: u64,
    },
    /// Replay cannot find the record for the globally expected sequence.
    ReplayStalled {
        /// Input sequence whose record is required to continue.
        expected_input_seq: u64,
    },
}

/// Error returned by framed IO polling when either transport IO or frame handling fails.
#[derive(Debug)]
pub enum FramePollError<E> {
    /// The transport, recorder, or replay state failed.
    Io(#[doc = "I/O-layer failure."] IoError),
    /// The endpoint callback rejected a delivered frame.
    Handler(#[doc = "Endpoint-defined handler failure."] E),
}

/// Metric start policy attached to one inbound frame inside the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameLatencyStart {
    /// Start timing when the gateway handler receives the frame.
    HandlerEntry,
    /// Start timing at this benchmark-scheduled arrival on the runtime clock.
    Scheduled(
        /// Absolute scheduled arrival time in runtime-clock nanoseconds.
        u64,
    ),
}

/// Per-frame control returned by an endpoint handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FramePollControl {
    /// Continue delivering frames while the poll budget permits.
    Continue,
    /// Return control to the endpoint after accepting the current frame.
    Stop,
}

/// Aggregate result of one controlled ingress poll.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FramePollStatus {
    /// Whether ingress, connection, or egress state advanced during the poll.
    pub(crate) progress: bool,
    /// Whether the frame handler requested an early return.
    pub(crate) stopped: bool,
}

impl FramePollStatus {
    /// Poll result for a backend with no ingress work.
    const IDLE: Self = Self {
        progress: false,
        stopped: false,
    };
}

/// Complete inbound frame and runtime-adjacent timing metadata.
#[derive(Debug, Clone, Copy)]
pub(crate) struct InboundFrame<'a> {
    /// Receive timestamp associated with the frame.
    pub(crate) recv_ts: RecvTs,
    /// Timestamp policy used by latency measurement.
    pub(crate) latency_start: FrameLatencyStart,
    /// Complete protocol frame borrowed from backend-owned storage.
    pub(crate) bytes: &'a [u8],
}

/// Preformatted backend descriptions emitted when an endpoint starts.
#[derive(Debug)]
struct IOStartupLog {
    /// Description of the configured inbound source.
    ingress: String,
    /// Description of the configured outbound sink.
    egress: String,
    /// Description of the optional live-input recorder.
    recorder: String,
}

impl IOStartupLog {
    /// Describe a configuration without opening or mutating its resources.
    fn from_config(config: &IOConfig) -> Self {
        match config {
            IOConfig::Noop => Self {
                ingress: "None".to_owned(),
                egress: "None".to_owned(),
                recorder: "disabled".to_owned(),
            },
            IOConfig::Tcp {
                bind_addr, record_path, ..
            } => {
                let backend = format!("TCP({bind_addr})");
                Self {
                    ingress: backend.clone(),
                    egress: backend,
                    recorder: record_path
                        .as_ref()
                        .map_or_else(|| "disabled".to_owned(), |path| format!("File({})", path.display())),
                }
            }
            IOConfig::Udp { group_addr, .. } => Self {
                ingress: "None".to_owned(),
                egress: format!("UDP({group_addr})"),
                recorder: "disabled".to_owned(),
            },
            IOConfig::File {
                input_path, output_path, ..
            } => Self {
                ingress: optional_file_descriptor(input_path.as_ref()),
                egress: optional_file_descriptor(output_path.as_ref()),
                recorder: "disabled".to_owned(),
            },
            IOConfig::InMemory { input, .. } => Self {
                ingress: format!("InMemory({} records)", input.len()),
                egress: "InMemory(counting)".to_owned(),
                recorder: "disabled".to_owned(),
            },
            IOConfig::ScheduledInMemory {
                input,
                target_rate_per_sec,
                ..
            } => Self {
                ingress: format!("ScheduledInMemory({} records at {target_rate_per_sec}/s)", input.len()),
                egress: "InMemory(counting)".to_owned(),
                recorder: "disabled".to_owned(),
            },
        }
    }
}

/// Format an optional file path for startup diagnostics.
fn optional_file_descriptor(path: Option<&PathBuf>) -> String {
    path.map_or_else(|| "None".to_owned(), |path| format!("File({})", path.display()))
}

#[cfg(test)]
mod tests;
