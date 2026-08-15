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
    state: MetricsState,
}

#[derive(Debug)]
enum MetricsState {
    Noop,
    InMemory(Metrics),
}

impl MetricsRuntime {
    /// Build a metrics runtime for the selected backend.
    pub fn new(backend: MetricsBackend, warmup_samples: u64) -> Result<Self, MetricsError> {
        let state = match backend {
            MetricsBackend::Noop => MetricsState::Noop,
            MetricsBackend::InMemory => MetricsState::InMemory(Metrics::new(warmup_samples)?),
        };
        Ok(Self { state })
    }

    /// Build a no-op metrics runtime.
    pub fn noop() -> Self {
        Self { state: MetricsState::Noop }
    }

    /// Build an in-memory metrics runtime with a warmup sample budget.
    pub fn in_memory(warmup_samples: u64) -> Result<Self, MetricsError> {
        Self::new(MetricsBackend::InMemory, warmup_samples)
    }

    /// Start one timing sample.
    pub fn start_clock(&self, clock: &Clock) -> MetricsTime {
        self.start_clock_at(clock.now_ns())
    }

    /// Start one timing sample at a timestamp from the runtime clock.
    pub(crate) fn start_clock_at(&self, start_ns: u64) -> MetricsTime {
        match &self.state {
            MetricsState::Noop => MetricsTime::Noop,
            MetricsState::InMemory(_) => MetricsTime::Started(start_ns),
        }
    }

    /// Stop one timing sample and record it when metrics are enabled.
    pub fn stop_clock(&mut self, clock: &Clock, time: MetricsTime) {
        match (&mut self.state, time) {
            (MetricsState::InMemory(metrics), MetricsTime::Started(start_ns)) => metrics.record(start_ns, clock.now_ns()),
            (MetricsState::Noop, _) | (_, MetricsTime::Noop) => {}
        }
    }

    /// Return a post-run metrics snapshot.
    pub fn snapshot(&self) -> MetricsSnapshot {
        match &self.state {
            MetricsState::Noop => MetricsSnapshot::default(),
            MetricsState::InMemory(metrics) => metrics.snapshot(),
        }
    }
}

/// Metrics construction failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsError {
    /// The in-memory latency histogram could not be constructed.
    InvalidHistogramConfig,
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
    fn new(warmup_samples: u64) -> Result<Self, MetricsError> {
        Ok(Self {
            warmup_samples,
            seen_samples: 0,
            measured_samples: 0,
            first_measured_start_ns: None,
            last_measured_end_ns: None,
            histogram: Histogram::new_with_bounds(1, 60_000_000_000, 3).map_err(|_| MetricsError::InvalidHistogramConfig)?,
        })
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
        MetricsSnapshot {
            warmup_samples: self.warmup_samples,
            measured_samples: self.measured_samples,
            p50_ns: percentile(&self.histogram, 50.0),
            p99_ns: percentile(&self.histogram, 99.0),
            p999_ns: percentile(&self.histogram, 99.9),
            p9999_ns: percentile(&self.histogram, 99.99),
            max_ns: self.histogram.max(),
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
        let mut metrics = MetricsRuntime::in_memory(1).expect("metrics");
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
    fn scheduled_start_records_queue_delay_directly() {
        let mut metrics = MetricsRuntime::in_memory(0).expect("metrics");
        let MetricsState::InMemory(state) = &mut metrics.state else {
            panic!("in-memory metrics state");
        };
        state.record(1_000, 4_000);

        let snapshot = metrics.snapshot();

        assert_eq!(snapshot.measured_samples, 1);
        assert_eq!(snapshot.measured_elapsed_ns, 3_000);
        assert!(snapshot.p50_ns >= 3_000);
        assert!(snapshot.max_ns >= snapshot.p50_ns);
    }
}
