//! Counters, latency samples and the run report.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use pulsemq::types::ReasonCode;

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

/// Run-wide counters, incremented from every task.
///
/// Relaxed ordering throughout: these are statistics, never used to coordinate
/// between tasks, and the report reads them after every task has joined.
#[derive(Debug, Default)]
pub struct Counters {
    pub published: AtomicU64,
    pub acknowledged: AtomicU64,
    pub received: AtomicU64,
    pub bytes_sent: AtomicU64,
    pub bytes_received: AtomicU64,
    pub publish_errors: AtomicU64,
    refused: Mutex<BTreeMap<u8, u64>>,
}

impl Counters {
    /// A message the broker declined, tallied by Reason Code so a refusal
    /// partway through a run is visible instead of looking like slowness.
    pub fn record_refusal(&self, code: ReasonCode) {
        let mut refused = self.refused.lock().expect("counters mutex poisoned");
        *refused.entry(code.as_u8()).or_insert(0) += 1;
    }

    pub fn snapshot(&self) -> CounterTotals {
        CounterTotals {
            published: self.published.load(Ordering::Relaxed),
            acknowledged: self.acknowledged.load(Ordering::Relaxed),
            received: self.received.load(Ordering::Relaxed),
            bytes_sent: self.bytes_sent.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            publish_errors: self.publish_errors.load(Ordering::Relaxed),
            refused: self
                .refused
                .lock()
                .expect("counters mutex poisoned")
                .clone(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct CounterTotals {
    pub published: u64,
    pub acknowledged: u64,
    pub received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub publish_errors: u64,
    pub refused: BTreeMap<u8, u64>,
}

/// The resolved configuration, carried into the report so a JSON run record
/// says what produced it.
#[derive(Debug, Clone)]
pub struct ReportConfig {
    pub publishers: usize,
    pub subscribers: usize,
    pub qos: u8,
    pub payload_size: usize,
    pub inflight: usize,
    pub rate: Option<f64>,
    pub topic_prefix: String,
}

#[derive(Debug)]
pub struct Report {
    pub config: ReportConfig,
    /// Measured wall time of the run, never the requested duration: a run cut
    /// short must not report the rate it was aiming for.
    pub elapsed: Duration,
    pub totals: CounterTotals,
    pub task_failures: Vec<String>,
    pub ack: Option<Summary>,
    pub end_to_end: Option<Summary>,
    pub end_to_end_disabled: bool,
}

impl Report {
    pub fn publish_rate(&self) -> f64 {
        per_second(self.totals.published, self.elapsed)
    }

    pub fn receive_rate(&self) -> f64 {
        per_second(self.totals.received, self.elapsed)
    }

    /// A run is a success only when nothing failed and nothing was refused.
    pub fn success(&self) -> bool {
        self.task_failures.is_empty()
            && self.totals.refused.is_empty()
            && self.totals.publish_errors == 0
    }

    pub fn to_table(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();

        let rate = match self.config.rate {
            Some(r) => format!("{r} msg/s offered"),
            None => "unthrottled".to_string(),
        };
        let _ = writeln!(
            out,
            "config: {} publishers, {} subscribers, QoS {}, {} B payload, \
             inflight {}, {}, prefix {}",
            self.config.publishers,
            self.config.subscribers,
            self.config.qos,
            self.config.payload_size,
            self.config.inflight,
            rate,
            self.config.topic_prefix,
        );
        let _ = writeln!(out, "elapsed:  {:.3} s", self.elapsed.as_secs_f64());
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "  published       {:>12}  ({:.0}/s)",
            self.totals.published,
            self.publish_rate()
        );
        let _ = writeln!(out, "  acknowledged    {:>12}", self.totals.acknowledged);
        let _ = writeln!(
            out,
            "  received        {:>12}  ({:.0}/s)",
            self.totals.received,
            self.receive_rate()
        );
        let _ = writeln!(out, "  bytes sent      {:>12}", self.totals.bytes_sent);
        let _ = writeln!(out, "  bytes received  {:>12}", self.totals.bytes_received);
        let _ = writeln!(out, "  publish errors  {:>12}", self.totals.publish_errors);
        for (code, count) in &self.totals.refused {
            let _ = writeln!(out, "  refused 0x{code:02X}     {count:>12}");
        }
        for failure in &self.task_failures {
            let _ = writeln!(out, "  task failed: {failure}");
        }

        let _ = writeln!(out);
        write_latency(&mut out, "ack latency", self.ack.as_ref(), None);
        write_latency(
            &mut out,
            "end-to-end latency",
            self.end_to_end.as_ref(),
            self.end_to_end_disabled
                .then_some("payload too small for the measurement header"),
        );
        out
    }

    pub fn to_json(&self) -> String {
        let refused: serde_json::Map<String, serde_json::Value> = self
            .totals
            .refused
            .iter()
            .map(|(code, count)| (format!("0x{code:02X}"), (*count).into()))
            .collect();

        let value = serde_json::json!({
            "config": {
                "publishers": self.config.publishers,
                "subscribers": self.config.subscribers,
                "qos": self.config.qos,
                "payload_size": self.config.payload_size,
                "inflight": self.config.inflight,
                "rate": self.config.rate,
                "topic_prefix": self.config.topic_prefix,
            },
            "elapsed_secs": self.elapsed.as_secs_f64(),
            "counters": {
                "published": self.totals.published,
                "acknowledged": self.totals.acknowledged,
                "received": self.totals.received,
                "bytes_sent": self.totals.bytes_sent,
                "bytes_received": self.totals.bytes_received,
                "publish_errors": self.totals.publish_errors,
                "refused": refused,
            },
            "throughput": {
                "published_per_sec": self.publish_rate(),
                "received_per_sec": self.receive_rate(),
            },
            "latency": {
                "ack": summary_json(self.ack.as_ref()),
                "end_to_end": summary_json(self.end_to_end.as_ref()),
                "end_to_end_disabled": self.end_to_end_disabled,
            },
            "task_failures": self.task_failures,
            "success": self.success(),
        });
        serde_json::to_string_pretty(&value).expect("report serialises")
    }
}

fn per_second(count: u64, elapsed: Duration) -> f64 {
    let secs = elapsed.as_secs_f64();
    if secs <= 0.0 {
        return 0.0;
    }
    count as f64 / secs
}

fn write_latency(out: &mut String, label: &str, summary: Option<&Summary>, why: Option<&str>) {
    use std::fmt::Write;
    match summary {
        Some(s) => {
            let _ = writeln!(out, "{label} ({} samples)", s.count);
            for (name, value) in [
                ("min", s.min_ns),
                ("p50", s.p50_ns),
                ("p95", s.p95_ns),
                ("p99", s.p99_ns),
                ("max", s.max_ns),
                ("mean", s.mean_ns),
            ] {
                let _ = writeln!(out, "  {name:<4} {:>10.3} ms", value as f64 / 1e6);
            }
        }
        None => {
            let reason = why.unwrap_or("no samples");
            let _ = writeln!(out, "{label}: not measured ({reason})");
        }
    }
}

fn summary_json(summary: Option<&Summary>) -> serde_json::Value {
    match summary {
        Some(s) => serde_json::json!({
            "count": s.count,
            "min_ns": s.min_ns,
            "p50_ns": s.p50_ns,
            "p95_ns": s.p95_ns,
            "p99_ns": s.p99_ns,
            "max_ns": s.max_ns,
            "mean_ns": s.mean_ns,
        }),
        None => serde_json::Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::{percentile, CounterTotals, Counters, Report, ReportConfig, Samples, Summary};
    use std::sync::atomic::Ordering;

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

    use pulsemq::types::ReasonCode;
    use std::time::Duration;

    fn totals() -> CounterTotals {
        let counters = Counters::default();
        counters.published.fetch_add(1000, Ordering::Relaxed);
        counters.acknowledged.fetch_add(1000, Ordering::Relaxed);
        counters.received.fetch_add(2000, Ordering::Relaxed);
        counters.bytes_sent.fetch_add(64_000, Ordering::Relaxed);
        counters
            .bytes_received
            .fetch_add(128_000, Ordering::Relaxed);
        counters.snapshot()
    }

    fn report_config() -> ReportConfig {
        ReportConfig {
            publishers: 1,
            subscribers: 2,
            qos: 1,
            payload_size: 64,
            inflight: 100,
            rate: None,
            topic_prefix: "bench".into(),
        }
    }

    #[test]
    fn refusals_are_tallied_per_reason_code() {
        let counters = Counters::default();
        counters.record_refusal(ReasonCode::NotAuthorized);
        counters.record_refusal(ReasonCode::NotAuthorized);
        counters.record_refusal(ReasonCode::QuotaExceeded);
        let snapshot = counters.snapshot();
        assert_eq!(snapshot.refused.get(&0x87), Some(&2));
        assert_eq!(snapshot.refused.get(&0x97), Some(&1));
    }

    #[test]
    fn rates_come_from_measured_time_not_requested_time() {
        let report = Report {
            config: report_config(),
            elapsed: Duration::from_secs(2),
            totals: totals(),
            task_failures: Vec::new(),
            ack: None,
            end_to_end: None,
            end_to_end_disabled: false,
        };
        assert_eq!(report.publish_rate(), 500.0);
        assert_eq!(report.receive_rate(), 1000.0);
    }

    #[test]
    fn a_run_with_a_refusal_or_a_failed_task_is_not_a_success() {
        let mut report = Report {
            config: report_config(),
            elapsed: Duration::from_secs(1),
            totals: totals(),
            task_failures: Vec::new(),
            ack: None,
            end_to_end: None,
            end_to_end_disabled: false,
        };
        assert!(report.success());

        report.task_failures.push("publisher 0: io error".into());
        assert!(!report.success());

        report.task_failures.clear();
        report.totals.refused.insert(0x87, 1);
        assert!(!report.success());
    }

    #[test]
    fn the_table_names_what_was_measured() {
        let report = Report {
            config: report_config(),
            elapsed: Duration::from_secs(1),
            totals: totals(),
            task_failures: Vec::new(),
            ack: Some(Summary {
                count: 1000,
                min_ns: 1_000,
                p50_ns: 2_000,
                p95_ns: 3_000,
                p99_ns: 4_000,
                max_ns: 5_000,
                mean_ns: 2_500,
            }),
            end_to_end: None,
            end_to_end_disabled: true,
        };
        let table = report.to_table();
        assert!(table.contains("published"));
        assert!(table.contains("ack latency"));
        assert!(table.contains("p99"));
        // A disabled measurement is stated, never printed as zeros.
        assert!(table.contains("end-to-end latency: not measured"));
        assert!(!table.contains("0.000 ms"));
    }

    #[test]
    fn json_carries_the_configuration_and_the_numbers() {
        let report = Report {
            config: report_config(),
            elapsed: Duration::from_millis(1500),
            totals: totals(),
            task_failures: vec!["subscriber 1: connection reset".into()],
            ack: None,
            end_to_end: None,
            end_to_end_disabled: false,
        };
        let value: serde_json::Value = serde_json::from_str(&report.to_json()).expect("valid JSON");
        assert_eq!(value["config"]["publishers"], 1);
        assert_eq!(value["config"]["qos"], 1);
        assert_eq!(value["counters"]["published"], 1000);
        assert_eq!(value["elapsed_secs"], 1.5);
        assert_eq!(value["task_failures"][0], "subscriber 1: connection reset");
        assert!(value["latency"]["ack"].is_null());
    }
}
