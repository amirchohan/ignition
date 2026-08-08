//! Matching-engine boundary.
//!
//! The engine will own the order book, matching state, and price-time priority
//! rules. It must stay protocol-agnostic: gateway/protocol code converts bytes
//! into internal commands before engine interaction.

/// Matching engine shell for future milestones.
#[derive(Debug, Default)]
pub struct Engine;
