//! Allocation measurements supplied by the benchmark binary.
//!
//! The runtime library cannot install a process-global allocator, so the thin
//! benchmark binary owns the counters and passes their operations through an
//! [`AllocationProbe`]. [`AllocationSnapshot`] is the value returned after the
//! measured run. Both types are copy-only handles/data and add no measured-path
//! allocation of their own.
//!
//! # Main components
//!
//! - [`AllocationProbe`] holds binary-owned reset and read callbacks.
//! - [`AllocationSnapshot`] carries the resulting allocation count and bytes.

/// Binary-owned allocation instrumentation invoked only around the measured run.
#[derive(Clone, Copy)]
pub struct AllocationProbe {
    /// Reset allocation counters immediately before measured execution.
    pub reset: fn(),
    /// Read allocation counters immediately after measured execution.
    pub read: fn() -> AllocationSnapshot,
}

/// Allocation totals captured immediately after the measured run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllocationSnapshot {
    /// Number of allocator calls observed in the measured interval.
    pub calls: u64,
    /// Total bytes requested from the allocator in the measured interval.
    pub bytes: u64,
}
