//! Protocol ownership boundary.
//!
//! This module owns the SBE schema integration, generated codecs, and
//! hand-written facade used by the rest of the crate. Business logic must not
//! import generated codecs directly.

pub mod codec;
pub mod error;
pub mod generated;

pub use codec::{
    MessageKind, ProtocolFrame, SBE_HEADER_LEN, SCHEMA_ID, SCHEMA_VERSION, decode_header,
    encode_header,
};
pub use error::{ProtocolError, ProtocolResult};
