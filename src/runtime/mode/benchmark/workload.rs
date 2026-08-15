//! Preflight validation for deterministic benchmark workloads.
//!
//! This module checks that the selected core configuration can contain the
//! generated accounts, resting orders, and 64-level price ladder before timing
//! begins. The generator remains in the parent benchmark module; this boundary
//! keeps configuration failures out of the measured runtime path.
//!
//! # Main components
//!
//! - [`validate`] rejects workload/configuration combinations that would test
//!   capacity shutdown or invalid prices instead of the intended hot path.

use std::io;

use crate::config::ResolvedCoreConfig;

/// Validate configuration-dependent workload bounds before measurement.
pub(super) fn validate(count: u64, passive_add: bool, core: &ResolvedCoreConfig) -> Result<(), io::Error> {
    if core.engine.max_account_id() < 3 {
        return Err(io::Error::other("benchmark workloads require max_account_id >= 3"));
    }
    if passive_add && count > u64::from(core.engine.max_resting_orders()) {
        return Err(io::Error::other(format!(
            "passive-add count {count} exceeds max_resting_orders {}",
            core.engine.max_resting_orders()
        )));
    }
    let highest = core
        .risk
        .startup_reference_price
        .raw()
        .checked_add(63_u64.saturating_mul(core.instrument.price_tick))
        .ok_or_else(|| io::Error::other("benchmark price ladder overflow"))?;
    if highest > core.engine.storage_max_price().raw() {
        return Err(io::Error::other("benchmark price ladder exceeds engine storage maximum"));
    }
    Ok(())
}
