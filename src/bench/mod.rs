//! Broker performance testing: `pulsemq-cli bench`.

use std::time::Duration;

use crate::mqtt::types::QoS;

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
        // Bench mode supports QoS 0 and 1 only: the QoS 2 publisher path
        // needs a PUBREL written from the send side while the ack side owns
        // the read half, which is not implemented yet. A half-built QoS 2
        // path would corrupt the throughput and latency numbers this
        // feature exists to produce.
        if args.qos == 2 {
            return Err(Error::Usage(
                "--qos 2: bench mode does not yet support QoS 2".into(),
            ));
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
        let config = Config::from_args(&args_from(&["pulsemq-cli", "bench"])).unwrap();
        assert_eq!(config.stop, Stop::Count(10_000));
    }

    #[test]
    fn count_wins_over_duration_but_duration_still_bounds_the_run() {
        let args = args_from(&["pulsemq-cli", "bench", "--count", "50", "--duration", "30"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.stop, Stop::CountWithin(50, Duration::from_secs(30)));
    }

    #[test]
    fn duration_alone_is_a_timed_run() {
        let args = args_from(&["pulsemq-cli", "bench", "--duration", "5"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.stop, Stop::Duration(Duration::from_secs(5)));
    }

    #[test]
    fn a_payload_below_the_header_length_disables_end_to_end_latency() {
        // `measures_end_to_end` also requires a subscriber to observe the
        // header, so both runs ask for one.
        let args = args_from(&[
            "pulsemq-cli",
            "bench",
            "--subscribers",
            "1",
            "--payload-size",
            "8",
        ]);
        let config = Config::from_args(&args).unwrap();
        assert!(!config.measures_end_to_end());

        let args = args_from(&[
            "pulsemq-cli",
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
        let args = args_from(&[
            "pulsemq-cli",
            "bench",
            "--publishers",
            "4",
            "--rate",
            "1000",
        ]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.per_publisher_rate(), Some(250.0));

        let args = args_from(&["pulsemq-cli", "bench", "--publishers", "4"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.per_publisher_rate(), None);
    }

    #[test]
    fn topics_follow_the_prefix() {
        let args = args_from(&["pulsemq-cli", "bench", "--topic-prefix", "load"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.topic_for(3), "load/3");
        assert_eq!(config.subscribe_filter(), "load/#");
    }

    #[test]
    fn zero_publishers_is_rejected() {
        let args = args_from(&["pulsemq-cli", "bench", "--publishers", "0"]);
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
        let args = args_from(&["pulsemq-cli", "bench", "--publishers", "3", "--count", "10"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.quota_for(0), Some(4));
        assert_eq!(config.quota_for(1), Some(3));
        assert_eq!(config.quota_for(2), Some(3));
        assert_eq!(config.deadline(), None);
    }

    #[test]
    fn a_timed_run_has_no_quota_but_has_a_deadline() {
        let args = args_from(&["pulsemq-cli", "bench", "--duration", "7"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.quota_for(0), None);
        assert_eq!(config.deadline(), Some(Duration::from_secs(7)));
    }

    /// The QoS 2 publisher path needs a PUBREL written from the send side
    /// while the ack side owns the read half; bench mode does not support
    /// it yet, so `--qos 2` is rejected here rather than half-implemented.
    #[test]
    fn qos_2_is_rejected() {
        let args = args_from(&["pulsemq-cli", "bench", "--qos", "2"]);
        assert!(Config::from_args(&args).is_err());
    }

    /// `Duration::from_secs_f64(1.0 / rate)` in `schedule.rs` panics for a
    /// pathologically small positive rate; `--rate` is the only path that
    /// reaches it, so it is validated here.
    #[test]
    fn a_pathologically_small_rate_is_rejected() {
        let args = args_from(&["pulsemq-cli", "bench", "--rate", "1e-300"]);
        assert!(Config::from_args(&args).is_err());
    }

    #[test]
    fn an_ordinary_rate_resolves() {
        let args = args_from(&["pulsemq-cli", "bench", "--rate", "500"]);
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
    fn a_rate_safe_alone_is_rejected_once_split_across_publishers() {
        let args = args_from(&["pulsemq-cli", "bench", "--rate", "2e-19"]);
        assert!(Config::from_args(&args).is_ok());

        let args = args_from(&[
            "pulsemq-cli",
            "bench",
            "--rate",
            "2e-19",
            "--publishers",
            "4",
        ]);
        assert!(Config::from_args(&args).is_err());
    }
}
