//! Runtime metrics collection.
//!
//! Metrics are enum-gated so the normal live/replay path pays only a tiny
//! branch for the disabled backend. The runtime owns the metric state; the
//! backend decides whether samples are recorded.

use hdrhistogram::Histogram;
use serde::Deserialize;

use crate::clock::Clock;

/// Runtime metrics collector.
#[derive(Debug)]
pub struct MetricsRuntime {
    backend: MetricsBackend,
    metrics: Metrics,
}

impl MetricsRuntime {
    /// Build a metrics runtime for the selected backend.
    pub fn new(backend: MetricsBackend, warmup_samples: u64) -> Self {
        Self {
            backend,
            metrics: Metrics::new(warmup_samples),
        }
    }

    /// Build a no-op metrics runtime.
    pub fn noop() -> Self {
        Self::new(MetricsBackend::Noop, 0)
    }

    /// Build an in-memory metrics runtime with a warmup sample budget.
    pub fn in_memory(warmup_samples: u64) -> Self {
        Self::new(MetricsBackend::InMemory, warmup_samples)
    }

    /// Start one timing sample.
    pub fn start_clock(&self, clock: &Clock) -> MetricsTime {
        match self.backend {
            MetricsBackend::Noop => MetricsTime::Noop,
            MetricsBackend::InMemory => MetricsTime::Started(clock.now_ns()),
        }
    }

    /// Stop one timing sample and record it when metrics are enabled.
    pub fn stop_clock(&mut self, clock: &Clock, time: MetricsTime) {
        match (self.backend, time) {
            (MetricsBackend::InMemory, MetricsTime::Started(start_ns)) => self.metrics.record(start_ns, clock.now_ns()),
            (MetricsBackend::Noop, _) | (_, MetricsTime::Noop) => {}
        }
    }

    /// Return a post-run metrics snapshot.
    pub fn snapshot(&self) -> MetricsSnapshot {
        match self.backend {
            MetricsBackend::Noop => MetricsSnapshot::default(),
            MetricsBackend::InMemory => self.metrics.snapshot(),
        }
    }

    /// Return a post-run snapshot corrected for coordinated omission.
    pub fn snapshot_corrected(&self, expected_interval_ns: u64) -> MetricsSnapshot {
        match self.backend {
            MetricsBackend::Noop => MetricsSnapshot::default(),
            MetricsBackend::InMemory => self.metrics.snapshot_corrected(expected_interval_ns),
        }
    }
}

/// Metrics backend selected at runtime construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MetricsBackend {
    /// Metrics disabled.
    Noop,
    /// Store latency samples in process memory for post-run reporting.
    InMemory,
}

/// Timer token returned by `MetricsRuntime::start_clock`.
#[derive(Debug)]
pub enum MetricsTime {
    Noop,
    Started(u64),
}

/// Runtime-owned metric state.
#[derive(Debug)]
struct Metrics {
    warmup_samples: u64,
    seen_samples: u64,
    measured_samples: u64,
    first_measured_start_ns: Option<u64>,
    last_measured_end_ns: Option<u64>,
    histogram: Histogram<u64>,
}

impl Metrics {
    fn new(warmup_samples: u64) -> Self {
        Self {
            warmup_samples,
            seen_samples: 0,
            measured_samples: 0,
            first_measured_start_ns: None,
            last_measured_end_ns: None,
            histogram: Histogram::new_with_bounds(1, 60_000_000_000, 3).expect("valid histogram bounds"),
        }
    }

    fn record(&mut self, start_ns: u64, end_ns: u64) {
        self.seen_samples = self.seen_samples.saturating_add(1);
        if self.seen_samples <= self.warmup_samples {
            return;
        }
        if self.first_measured_start_ns.is_none() {
            self.first_measured_start_ns = Some(start_ns);
        }
        self.last_measured_end_ns = Some(end_ns);

        let nanos = end_ns.saturating_sub(start_ns);
        if self.histogram.record(nanos.max(1)).is_ok() {
            self.measured_samples = self.measured_samples.saturating_add(1);
        }
    }

    fn snapshot(&self) -> MetricsSnapshot {
        self.snapshot_from(&self.histogram)
    }

    fn snapshot_corrected(&self, expected_interval_ns: u64) -> MetricsSnapshot {
        let histogram = self.histogram.clone_correct(expected_interval_ns);
        self.snapshot_from(&histogram)
    }

    fn snapshot_from(&self, histogram: &Histogram<u64>) -> MetricsSnapshot {
        MetricsSnapshot {
            warmup_samples: self.warmup_samples,
            measured_samples: self.measured_samples,
            p50_ns: percentile(histogram, 50.0),
            p99_ns: percentile(histogram, 99.0),
            p999_ns: percentile(histogram, 99.9),
            p9999_ns: percentile(histogram, 99.99),
            max_ns: histogram.max(),
            measured_elapsed_ns: match (self.first_measured_start_ns, self.last_measured_end_ns) {
                (Some(start_ns), Some(end_ns)) => end_ns.saturating_sub(start_ns),
                _ => 0,
            },
        }
    }
}

fn percentile(histogram: &Histogram<u64>, quantile: f64) -> u64 {
    if histogram.is_empty() {
        0
    } else {
        histogram.value_at_percentile(quantile)
    }
}

/// Immutable metrics summary safe to print after the runtime exits.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub warmup_samples: u64,
    pub measured_samples: u64,
    pub p50_ns: u64,
    pub p99_ns: u64,
    pub p999_ns: u64,
    pub p9999_ns: u64,
    pub max_ns: u64,
    pub measured_elapsed_ns: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_records_no_samples() {
        let clock = Clock::new();
        let mut metrics = MetricsRuntime::noop();
        let time = metrics.start_clock(&clock);
        metrics.stop_clock(&clock, time);

        assert_eq!(metrics.snapshot(), MetricsSnapshot::default());
    }

    #[test]
    fn in_memory_skips_warmup_and_records_percentiles() {
        let clock = Clock::new();
        let mut metrics = MetricsRuntime::in_memory(1);
        let first = metrics.start_clock(&clock);
        metrics.stop_clock(&clock, first);
        let second = metrics.start_clock(&clock);
        metrics.stop_clock(&clock, second);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.warmup_samples, 1);
        assert_eq!(snapshot.measured_samples, 1);
        assert!(snapshot.p50_ns > 0);
        assert!(snapshot.max_ns >= snapshot.p50_ns);
    }

    #[test]
    fn corrected_snapshot_adds_expected_interval_samples() {
        let mut metrics = MetricsRuntime::in_memory(0);
        metrics.metrics.record(0, 100);

        let raw = metrics.snapshot();
        let corrected = metrics.snapshot_corrected(10);

        assert_eq!(raw.measured_samples, 1);
        assert_eq!(raw.measured_elapsed_ns, 100);
        assert_eq!(corrected.measured_samples, 1);
        assert!(corrected.p50_ns < raw.p50_ns);
        assert_eq!(corrected.max_ns, raw.max_ns);
    }
}
