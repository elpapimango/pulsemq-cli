//! Broker performance testing: `wispmq-cli bench`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;

use wispmq_protocol::types::QoS;

use crate::bench::stats::{Counters, Report, ReportConfig, Samples};
use crate::cli::BenchArgs;
use crate::error::{Error, Result};

pub mod payload;
pub mod publisher;
pub mod schedule;
pub mod stats;
pub mod subscriber;

/// The largest offered rate accepted by `--rate`. Above this, per-message
/// scheduling stops being meaningful; `Duration::from_secs_f64(1.0 / rate)`
/// also loses useful precision long before this bound.
const MAX_RATE: f64 = 10_000_000.0;

/// What ends the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stop {
    /// Send exactly this many messages, however long it takes.
    Count(u64),
    /// Send for this long, however many that turns out to be.
    Duration(Duration),
    /// Send this many, giving up if the duration expires first.
    CountWithin(u64, Duration),
}

/// The resolved run configuration. Every "which option wins" decision is made
/// here, so the tasks never re-derive one.
#[derive(Debug, Clone)]
pub struct Config {
    pub publishers: usize,
    pub subscribers: usize,
    pub topic_prefix: String,
    pub qos: QoS,
    pub payload_size: usize,
    pub stop: Stop,
    pub rate: Option<f64>,
    pub inflight: usize,
    pub warmup: Duration,
    pub drain: Duration,
    pub json: bool,
}

/// Messages to send when neither `--count` nor `--duration` is given: enough
/// to be meaningful, few enough to finish on a laptop.
const DEFAULT_COUNT: u64 = 10_000;

impl Config {
    pub fn from_args(args: &BenchArgs) -> Result<Config> {
        if args.publishers == 0 {
            return Err(Error::Usage("--publishers must be at least 1".into()));
        }
        if let Some(rate) = args.rate {
            if !rate.is_finite() || rate <= 0.0 || rate > MAX_RATE {
                return Err(Error::Usage(format!(
                    "--rate must be a finite number greater than 0 and at most {MAX_RATE}, got {rate}"
                )));
            }
            // A rate can sit inside (0, MAX_RATE] and still be pathologically
            // small once split across publishers: `Schedule::new` in
            // schedule.rs receives `per_publisher_rate()` (rate / publishers),
            // not the raw rate, and its reciprocal period must fit in a
            // `Duration` or `Duration::from_secs_f64` panics. Validate the
            // quantity that actually reaches schedule.rs.
            let per_publisher_rate = rate / args.publishers as f64;
            let period_secs = 1.0 / per_publisher_rate;
            if !period_secs.is_finite() || period_secs > Duration::MAX.as_secs_f64() {
                return Err(Error::Usage(format!(
                    "--rate {rate} split across {} publisher(s) is too small: the resulting period does not fit",
                    args.publishers
                )));
            }
        }
        let stop = match (args.count, args.duration) {
            (Some(count), Some(secs)) => Stop::CountWithin(count, Duration::from_secs(secs)),
            (Some(count), None) => Stop::Count(count),
            (None, Some(secs)) => Stop::Duration(Duration::from_secs(secs)),
            (None, None) => Stop::Count(DEFAULT_COUNT),
        };
        Ok(Config {
            publishers: args.publishers,
            subscribers: args.subscribers,
            topic_prefix: args.topic_prefix.clone(),
            qos: QoS::from_u8(args.qos)?,
            payload_size: args.payload_size,
            stop,
            rate: args.rate,
            inflight: args.inflight,
            warmup: Duration::from_secs(args.warmup),
            drain: Duration::from_secs(args.drain),
            json: args.json,
        })
    }

    /// End-to-end latency needs the measurement header to fit in the payload.
    pub fn measures_end_to_end(&self) -> bool {
        self.subscribers > 0 && self.payload_size >= payload::HEADER_LEN
    }

    /// The offered load is stated for the run; each publisher takes a share.
    pub fn per_publisher_rate(&self) -> Option<f64> {
        self.rate.map(|r| r / self.publishers as f64)
    }

    pub fn topic_for(&self, publisher: u32) -> String {
        format!("{}/{publisher}", self.topic_prefix)
    }

    pub fn subscribe_filter(&self) -> String {
        format!("{}/#", self.topic_prefix)
    }

    /// Messages this publisher is responsible for, when the run is
    /// count-limited. The remainder goes to the first publishers.
    pub fn quota_for(&self, publisher: usize) -> Option<u64> {
        let total = match self.stop {
            Stop::Count(n) | Stop::CountWithin(n, _) => n,
            Stop::Duration(_) => return None,
        };
        let base = total / self.publishers as u64;
        let remainder = total % self.publishers as u64;
        Some(base + if (publisher as u64) < remainder { 1 } else { 0 })
    }

    /// The wall-clock limit on the sending phase, if any.
    pub fn deadline(&self) -> Option<Duration> {
        match self.stop {
            Stop::Duration(d) | Stop::CountWithin(_, d) => Some(d),
            Stop::Count(_) => None,
        }
    }
}

/// The window a publisher may keep in flight: what was asked for, capped by
/// what the broker said it will accept in CONNACK, and never zero.
pub fn effective_inflight(requested: usize, broker_receive_maximum: Option<u16>) -> usize {
    let cap = broker_receive_maximum
        .map(usize::from)
        .unwrap_or(usize::MAX);
    requested.min(cap).max(1)
}

/// How long to let subscribers settle before the first publish.
///
/// Each subscriber task awaits its own SUBACK, but a SUBACK only says the
/// broker recorded the subscription — it does not say every *other*
/// subscriber has one too. Publishing the instant the last SUBACK lands still
/// races the slowest subscriber's routing state. This is a pragmatic margin,
/// not a correctness guarantee: the drain window at the other end is what
/// actually keeps late deliveries from being counted as lost.
const SUBSCRIBER_SETTLE: Duration = Duration::from_millis(200);

/// Turn per-task results into the report's failure list, labelled so a reader
/// knows which connection died.
fn collect_failures(results: Vec<Result<()>>, role: &str) -> Vec<String> {
    results
        .into_iter()
        .enumerate()
        .filter_map(|(index, result)| result.err().map(|e| format!("{role} {index}: {e}")))
        .collect()
}

/// Await a set of task handles, merging the samples of those that finished and
/// recording a result per task so the report can name what failed.
///
/// A panicking task is a failure of that connection, not of the run: the
/// remaining handles are still awaited and the report still prints.
async fn join_tasks(
    handles: Vec<tokio::task::JoinHandle<Result<Samples>>>,
) -> (Samples, Vec<Result<()>>) {
    let mut merged = Samples::new();
    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        match handle.await {
            Ok(Ok(samples)) => {
                merged.merge(samples);
                results.push(Ok(()));
            }
            Ok(Err(e)) => results.push(Err(e)),
            Err(e) => results.push(Err(Error::Usage(format!("task panicked: {e}")))),
        }
    }
    (merged, results)
}

/// Run the benchmark and print the report.
pub async fn run(args: BenchArgs) -> Result<()> {
    let config = Arc::new(Config::from_args(&args)?);
    let counters = Arc::new(Counters::default());
    let baseline = Instant::now();
    let warmup_until = baseline + config.warmup;
    let (stop_tx, stop_rx) = watch::channel(false);

    // Subscribers first: a message published before a subscription exists is
    // delivered to nobody, and the short receive count that follows looks like
    // a broker fault rather than a test that raced itself.
    let subscriber_handles: Vec<_> = (0..config.subscribers)
        .map(|index| {
            tokio::spawn(subscriber::run(
                index as u32,
                config.clone(),
                args.conn.clone(),
                counters.clone(),
                baseline,
                warmup_until,
                stop_rx.clone(),
            ))
        })
        .collect();
    if config.subscribers > 0 {
        tokio::time::sleep(SUBSCRIBER_SETTLE).await;
    }

    let started = Instant::now();
    let publisher_handles: Vec<_> = (0..config.publishers)
        .map(|index| {
            tokio::spawn(publisher::run(
                index as u32,
                config.clone(),
                args.conn.clone(),
                counters.clone(),
                baseline,
                warmup_until,
                stop_rx.clone(),
            ))
        })
        .collect();

    // Stop on Ctrl-C, or when the run's own deadline expires. Either way the
    // report still prints for whatever completed, which is the point: a run
    // interrupted at second 20 of 60 is still 20 seconds of measurement.
    let deadline_stop = {
        let stop_tx = stop_tx.clone();
        let deadline = config.deadline();
        tokio::spawn(async move {
            match deadline {
                Some(d) => {
                    tokio::select! {
                        _ = tokio::time::sleep(d) => {}
                        _ = tokio::signal::ctrl_c() => {}
                    }
                }
                None => {
                    let _ = tokio::signal::ctrl_c().await;
                }
            }
            let _ = stop_tx.send(true);
        })
    };

    let (mut ack_samples, publisher_results) = join_tasks(publisher_handles).await;
    // Measured from the first publish to the last, so a run cut short reports
    // the rate it achieved rather than the one it was aiming for.
    let elapsed = started.elapsed();

    // Publishing is done; let deliveries still in flight arrive before the
    // subscribers are told to stop. Those messages are late, not lost, and
    // counting them as lost would blame the broker for the client's impatience.
    tokio::time::sleep(config.drain).await;
    let _ = stop_tx.send(true);
    deadline_stop.abort();

    let (mut e2e_samples, subscriber_results) = join_tasks(subscriber_handles).await;

    let mut task_failures = collect_failures(publisher_results, "publisher");
    task_failures.extend(collect_failures(subscriber_results, "subscriber"));

    let report = Report {
        config: ReportConfig {
            publishers: config.publishers,
            subscribers: config.subscribers,
            qos: config.qos.as_u8(),
            payload_size: config.payload_size,
            inflight: config.inflight,
            rate: config.rate,
            topic_prefix: config.topic_prefix.clone(),
        },
        elapsed,
        totals: counters.snapshot(),
        task_failures,
        ack: ack_samples.summary(),
        end_to_end: e2e_samples.summary(),
        end_to_end_disabled: config.subscribers > 0 && !config.measures_end_to_end(),
    };

    if config.json {
        println!("{}", report.to_json());
    } else {
        print!("{}", report.to_table());
    }

    if report.success() {
        Ok(())
    } else {
        Err(Error::Usage(
            "the run completed with failures or refused messages; see the report above".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::{Cli, Command};
    use clap::Parser;

    fn args_from(argv: &[&str]) -> crate::cli::BenchArgs {
        let cli = Cli::parse_from(argv);
        match cli.command {
            Command::Bench(args) => args,
            _ => panic!("expected the bench subcommand"),
        }
    }

    #[test]
    fn count_defaults_when_neither_stop_condition_is_given() {
        let config = Config::from_args(&args_from(&["wispmq-cli", "bench"])).unwrap();
        assert_eq!(config.stop, Stop::Count(10_000));
    }

    #[test]
    fn count_wins_over_duration_but_duration_still_bounds_the_run() {
        let args = args_from(&["wispmq-cli", "bench", "--count", "50", "--duration", "30"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.stop, Stop::CountWithin(50, Duration::from_secs(30)));
    }

    #[test]
    fn duration_alone_is_a_timed_run() {
        let args = args_from(&["wispmq-cli", "bench", "--duration", "5"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.stop, Stop::Duration(Duration::from_secs(5)));
    }

    #[test]
    fn a_payload_below_the_header_length_disables_end_to_end_latency() {
        // `measures_end_to_end` also requires a subscriber to observe the
        // header, so both runs ask for one.
        let args = args_from(&[
            "wispmq-cli",
            "bench",
            "--subscribers",
            "1",
            "--payload-size",
            "8",
        ]);
        let config = Config::from_args(&args).unwrap();
        assert!(!config.measures_end_to_end());

        let args = args_from(&[
            "wispmq-cli",
            "bench",
            "--subscribers",
            "1",
            "--payload-size",
            "16",
        ]);
        let config = Config::from_args(&args).unwrap();
        assert!(config.measures_end_to_end());
    }

    #[test]
    fn the_offered_rate_is_split_across_publishers() {
        let args = args_from(&["wispmq-cli", "bench", "--publishers", "4", "--rate", "1000"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.per_publisher_rate(), Some(250.0));

        let args = args_from(&["wispmq-cli", "bench", "--publishers", "4"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.per_publisher_rate(), None);
    }

    #[test]
    fn topics_follow_the_prefix() {
        let args = args_from(&["wispmq-cli", "bench", "--topic-prefix", "load"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.topic_for(3), "load/3");
        assert_eq!(config.subscribe_filter(), "load/#");
    }

    #[test]
    fn zero_publishers_is_rejected() {
        let args = args_from(&["wispmq-cli", "bench", "--publishers", "0"]);
        assert!(Config::from_args(&args).is_err());
    }

    #[test]
    fn the_broker_receive_maximum_caps_the_window() {
        assert_eq!(effective_inflight(100, Some(20)), 20);
        assert_eq!(effective_inflight(100, Some(500)), 100);
        assert_eq!(effective_inflight(100, None), 100);
        // Never zero: a zero-permit window would deadlock the publisher.
        assert_eq!(effective_inflight(0, None), 1);
    }

    #[test]
    fn a_count_run_splits_the_quota_and_hands_the_remainder_to_the_first() {
        let args = args_from(&["wispmq-cli", "bench", "--publishers", "3", "--count", "10"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.quota_for(0), Some(4));
        assert_eq!(config.quota_for(1), Some(3));
        assert_eq!(config.quota_for(2), Some(3));
        assert_eq!(config.deadline(), None);
    }

    #[test]
    fn a_timed_run_has_no_quota_but_has_a_deadline() {
        let args = args_from(&["wispmq-cli", "bench", "--duration", "7"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.quota_for(0), None);
        assert_eq!(config.deadline(), Some(Duration::from_secs(7)));
    }

    #[test]
    fn qos_2_is_accepted() {
        let args = args_from(&["wispmq-cli", "bench", "--qos", "2"]);
        assert_eq!(Config::from_args(&args).unwrap().qos, QoS::ExactlyOnce);
    }

    /// `Duration::from_secs_f64(1.0 / rate)` in `schedule.rs` panics for a
    /// pathologically small positive rate; `--rate` is the only path that
    /// reaches it, so it is validated here.
    #[test]
    fn a_pathologically_small_rate_is_rejected() {
        let args = args_from(&["wispmq-cli", "bench", "--rate", "1e-300"]);
        assert!(Config::from_args(&args).is_err());
    }

    #[test]
    fn an_ordinary_rate_resolves() {
        let args = args_from(&["wispmq-cli", "bench", "--rate", "500"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.rate, Some(500.0));
    }

    /// `Schedule::new` receives `per_publisher_rate()` (rate / publishers),
    /// not the raw rate, so a rate that is safe alone can still make the
    /// per-publisher share pathologically small once split. `--rate 2e-19`
    /// alone has a reciprocal period (~5e18s) under `Duration::MAX`
    /// (~1.84e19s), but split across 4 publishers the per-publisher rate is
    /// 5e-20, whose reciprocal (~2e19s) overflows it.
    #[test]
    fn task_failures_are_collected_with_their_role_and_index() {
        let failures = collect_failures(
            vec![Ok(()), Err(crate::error::Error::Usage("boom".into()))],
            "publisher",
        );
        assert_eq!(failures, vec!["publisher 1: boom".to_string()]);
    }

    #[test]
    fn a_clean_sweep_of_results_produces_no_failures() {
        assert!(collect_failures(vec![Ok(()), Ok(())], "subscriber").is_empty());
    }

    /// End to end through the real code path, against the broker in the
    /// sibling repo when it is available. Ignored by default because it needs
    /// that broker binary built and a free port.
    ///
    /// Run with:
    ///   cargo build --manifest-path ../wispmq/Cargo.toml --bin wispmq
    ///   cargo test bench::tests::a_small_run_publishes_and_receives -- --ignored
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn a_small_run_publishes_and_receives() {
        use std::process::{Command as ProcessCommand, Stdio};

        let broker_bin = "../wispmq/target/debug/wispmq";
        let mut broker = ProcessCommand::new(broker_bin)
            .args([
                "--listen-addr",
                "127.0.0.1:18841",
                "--admin-addr",
                "127.0.0.1:19041",
                "--db-path",
                "/tmp/wispmq-bench-test.db",
                "--sys-interval",
                "0",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("broker binary built; see the doc comment");
        tokio::time::sleep(Duration::from_millis(500)).await;

        let args = args_from(&[
            "wispmq-cli",
            "bench",
            "-b",
            "127.0.0.1",
            "-p",
            "18841",
            "--publishers",
            "2",
            "--subscribers",
            "1",
            "--count",
            "200",
            "--qos",
            "1",
        ]);
        let result = run(args).await;
        // Kill then reap: `kill` only signals, and an unreaped child stays a
        // zombie for the lifetime of the test binary.
        let _ = broker.kill();
        let _ = broker.wait();
        result.expect("the run completes");
    }

    #[test]
    fn a_rate_safe_alone_is_rejected_once_split_across_publishers() {
        let args = args_from(&["wispmq-cli", "bench", "--rate", "2e-19"]);
        assert!(Config::from_args(&args).is_ok());

        let args = args_from(&[
            "wispmq-cli",
            "bench",
            "--rate",
            "2e-19",
            "--publishers",
            "4",
        ]);
        assert!(Config::from_args(&args).is_err());
    }
}
