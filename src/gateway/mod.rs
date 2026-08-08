//! Gateway boundary.
//!
//! The gateway owns frame/session ingress and egress. It may decode protocol
//! frames and forward typed commands, but it must not make matching, risk, or
//! market-data decisions.

use crate::protocol::{ProtocolFrame, ProtocolResult};

/// Buffer-oriented frame decoder used by future transports and clients.
pub trait FrameDecoder {
    /// Decode a complete input frame header from `input`.
    fn decode_frame(&self, input: &[u8]) -> ProtocolResult<ProtocolFrame>;
}
