//! Reusable nonblocking framed I/O.
//!
//! This module owns transport mechanics only. Endpoint modules decide how to
//! decode and encode the SBE payload carried by each complete frame.

pub mod tcp;
pub mod udp;

pub use tcp::{FramePollError, IoError, TcpFrameIo, TcpFramePublisher};
pub use udp::{UdpFramePublisher, UdpMulticastConfig};
