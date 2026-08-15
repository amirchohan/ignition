//! Protocol ownership boundary.
//!
//! This module owns the SBE schema integration, generated codecs, and
//! hand-written facade used by the rest of the crate. Business logic must not
//! import generated codecs directly.

pub mod error;
pub mod frame;
pub mod wire;

/// Generated SBE codecs.
///
/// Files in this module are generated from `protocol/ignition.xml` and must not
/// be edited by hand. The handwritten facade in `crate::protocol::frame` is the
/// public integration point for the rest of the crate.
pub mod generated {
    pub use ignition_protocol_generated as codecs;
}

pub use error::{ProtocolError, ProtocolResult};
pub use frame::{
    MAX_ACCEPTED_WIRE_FRAME_LEN, MessageKind, ProtocolFrame, SBE_HEADER_LEN, SCHEMA_ID, SCHEMA_VERSION, decode_header, encode_header,
};
