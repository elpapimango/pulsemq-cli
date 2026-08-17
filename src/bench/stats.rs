//! Counters, latency samples and the run report.

/// Latency samples in nanoseconds, collected by one task.
///
/// Kept as a plain vector rather than a histogram: memory is 8 bytes per
/// message and the run is bounded by `--count`, so exact percentiles cost
/// little and remove the "is this bucket wide enough" question entirely.
#[derive(Debug, Default)]
pub struct Samples {
    values: Vec<u64>,
    sorted: bool,
}

impl Samples {
    pub fn new() -> Self {
        Samples::default()
    }

    pub fn record(&mut self, nanos: u64) {
        self.values.push(nanos);
        self.sorted = false;
    }

    /// Absorb another task's samples. Used when the per-task vectors join.
    pub fn merge(&mut self, other: Samples) {
        self.values.extend(other.values);
        self.sorted = false;
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Sort once, then read every statistic off the sorted vector. `None` when
    /// nothing was recorded — a QoS 0 run has no acknowledgement samples, and
    /// the report must say so rather than print zeros.
    pub fn summary(&mut self) -> Option<Summary> {
        if self.values.is_empty() {
            return None;
        }
        if !self.sorted {
            self.values.sort_unstable();
            self.sorted = true;
        }
        let sum: u128 = self.values.iter().map(|v| *v as u128).sum();
        Some(Summary {
            count: self.values.len(),
            min_ns: self.values[0],
            p50_ns: percentile(&self.values, 0.50),
            p95_ns: percentile(&self.values, 0.95),
            p99_ns: percentile(&self.values, 0.99),
            max_ns: self.values[self.values.len() - 1],
            mean_ns: (sum / self.values.len() as u128) as u64,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Summary {
    pub count: usize,
    pub min_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
    pub mean_ns: u64,
}

/// Nearest-rank percentile: the smallest value at or below which at least
/// `p` of the samples fall. Requires `sorted` to be sorted ascending and
/// non-empty.
pub fn percentile(sorted: &[u64], p: f64) -> u64 {
    debug_assert!(!sorted.is_empty());
    let rank = (p * sorted.len() as f64).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted.len() - 1);
    sorted[index]
}

#[cfg(test)]
mod tests {
    use super::{percentile, Samples};

    #[test]
    fn percentile_uses_nearest_rank() {
        // 1..=100 makes the expected ranks obvious: the p-th percentile is the
        // ceil(p * n)-th value, one-indexed.
        let sorted: Vec<u64> = (1..=100).collect();
        assert_eq!(percentile(&sorted, 0.50), 50);
        assert_eq!(percentile(&sorted, 0.95), 95);
        assert_eq!(percentile(&sorted, 0.99), 99);
        assert_eq!(percentile(&sorted, 1.0), 100);
    }

    #[test]
    fn percentile_of_one_sample_is_that_sample() {
        assert_eq!(percentile(&[42], 0.50), 42);
        assert_eq!(percentile(&[42], 0.99), 42);
    }

    #[test]
    fn summary_of_empty_samples_is_none() {
        let mut samples = Samples::new();
        assert!(samples.is_empty());
        assert!(samples.summary().is_none());
    }

    #[test]
    fn summary_reports_the_whole_distribution() {
        let mut samples = Samples::new();
        for v in [30u64, 10, 20, 50, 40] {
            samples.record(v);
        }
        let summary = samples.summary().expect("five samples");
        assert_eq!(summary.count, 5);
        assert_eq!(summary.min_ns, 10);
        assert_eq!(summary.max_ns, 50);
        assert_eq!(summary.mean_ns, 30);
        assert_eq!(summary.p50_ns, 30);
    }

    #[test]
    fn merge_combines_two_task_local_sets() {
        let mut a = Samples::new();
        a.record(1);
        a.record(3);
        let mut b = Samples::new();
        b.record(2);
        a.merge(b);
        assert_eq!(a.len(), 3);
        let summary = a.summary().expect("three samples");
        assert_eq!(summary.min_ns, 1);
        assert_eq!(summary.max_ns, 3);
    }
}
