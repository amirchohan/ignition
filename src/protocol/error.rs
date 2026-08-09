//! Protocol error types.

/// Result type used by protocol encode/decode operations.
pub type ProtocolResult<T> = Result<T, ProtocolError>;

/// Errors returned while encoding or decoding protocol frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolError {
    /// The provided input buffer is too short for the requested operation.
    BufferTooShort,
    /// The frame references a schema ID this crate does not support.
    UnsupportedSchema { schema_id: u16 },
    /// The frame references a schema version this crate does not support.
    UnsupportedVersion { version: u16 },
    /// The frame references an unknown SBE template ID.
    UnknownTemplate { template_id: u16 },
    /// The frame references a known template that is not accepted by this endpoint.
    UnexpectedTemplate { template_id: u16 },
}
