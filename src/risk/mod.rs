//! Risk boundary.
//!
//! Risk owns pre-match gating such as account limits, price bands, kill-switch
//! checks, and self-trade-prevention policy evaluation. It must not mutate the
//! order book directly.

/// Risk runtime shell for future milestones.
#[derive(Debug, Default)]
pub struct RiskRuntime;
