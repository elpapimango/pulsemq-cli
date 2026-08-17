# Bench Mode Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `bench` subcommand to `pulsemq-cli` that drives configurable publisher and subscriber load against an MQTT broker and reports throughput plus exact latency percentiles.

**Architecture:** One tokio task per connection. Publishers pace sends against absolute deadlines and hold an in-flight window bounded by the broker's Receive Maximum; subscribers time messages end to end using a 16-byte header written into each payload. Counters are atomics shared across tasks; latency samples stay task-local and merge when the tasks join, so nothing crosses a channel on the measurement path.

**Tech Stack:** Rust 2021, tokio (multi-threaded runtime for `bench`, current-thread for the other subcommands), clap derive, serde_json for `--json`, and the `pulsemq` crate as the MQTT codec.

**Spec:** `docs/superpowers/specs/2026-08-17-bench-mode-design.md`

## Global Constraints

- Rust edition 2021. Toolchain in use: cargo 1.92.0.
- Four gates must stay green, and every commit must leave them green:
  `cargo build`, `cargo test`, `cargo fmt --all -- --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`.
- The wire format is never reimplemented here. All packet types, properties,
  framing and enums come from the `pulsemq` path dependency at `../pulsemq`.
- No mention of any other MQTT client project in code, comments, or docs. This
  tool is written from scratch against the OASIS specs.
- Dependency surface stays small and justified. The only new third-party
  dependency this plan adds is `serde_json`; tokio gains features only.
- Anything that speaks MQTT stays correct for v5.0, v3.1.1 and v3.1.
- Comments cite the spec section where code implements a spec requirement, and
  match the density of the surrounding file.
- Commit messages end with the trailer
  `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.
- Branch is `main`. Commit at the end of every task; do not push.

## File Structure

Created:

| File | Responsibility |
|------|----------------|
| `src/bench/mod.rs` | `Config`, `Stop`, config resolution, `run()` orchestration |
| `src/bench/stats.rs` | `Samples`, `Summary`, `Counters`, `Report`, table and JSON rendering |
| `src/bench/payload.rs` | The 16-byte measurement header: build and decode |
| `src/bench/schedule.rs` | Absolute-deadline rate pacing |
| `src/bench/publisher.rs` | One publisher connection: write side, ack side, window |
| `src/bench/subscriber.rs` | One subscriber connection: subscribe, receive, ack, time |

Modified:

| File | Change |
|------|--------|
| `src/cli.rs` | `BenchArgs`, `Command::Bench` |
| `src/client.rs` | Handshake extracted to a reusable function returning negotiated parameters |
| `src/main.rs` | Builds the runtime per subcommand instead of `#[tokio::main]` |
| `src/lib.rs` | `pub mod bench;` |
| `Cargo.toml` | tokio features `rt-multi-thread`, `sync`, `signal`; `serde_json` |
| `README.md`, `CLAUDE.md`, `TODO.md` | Document the new mode, retire TODO item 2 |

`payload.rs` and `schedule.rs` are not in the spec's module list. They are split
out because both are pure logic with real edge cases, and a pure module is
testable without a broker.

---

### Task 1: Latency samples and percentiles

**Files:**
- Create: `src/bench/stats.rs`
- Create: `src/bench/mod.rs` (module declarations only at this point)
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `pub struct Samples` with `Samples::new()`, `record(&mut self, nanos: u64)`,
    `merge(&mut self, other: Samples)`, `len(&self) -> usize`,
    `is_empty(&self) -> bool`, `summary(&mut self) -> Option<Summary>`
  - `pub struct Summary { pub count: usize, pub min_ns: u64, pub p50_ns: u64, pub p95_ns: u64, pub p99_ns: u64, pub max_ns: u64, pub mean_ns: u64 }`
  - `pub fn percentile(sorted: &[u64], p: f64) -> u64`

- [ ] **Step 1: Write the failing test**

Create `src/bench/stats.rs` containing only the test module:

```rust
//! Counters, latency samples and the run report.

#[cfg(test)]
mod tests {
    use super::*;

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
```

Create `src/bench/mod.rs`:

```rust
//! Broker performance testing: `pulsemq-cli bench`.

pub mod stats;
```

Add to `src/lib.rs`, keeping the module list alphabetical:

```rust
pub mod bench;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test bench::stats`
Expected: FAIL — `cannot find function 'percentile' in this scope`, `cannot find type 'Samples' in this scope`.

- [ ] **Step 3: Write minimal implementation**

Insert above the test module in `src/bench/stats.rs`:

```rust
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test bench::stats`
Expected: PASS, 5 tests.
Then: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings`
Expected: no warnings.

- [ ] **Step 5: Commit**

```bash
git add src/bench/stats.rs src/bench/mod.rs src/lib.rs
git commit -m "$(cat <<'EOF'
feat: add latency sample collection with exact percentiles

Samples stay task-local and merge on join, so nothing crosses a channel
on the measurement path. Nearest-rank percentiles off a single sort.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: The measurement header

**Files:**
- Create: `src/bench/payload.rs`
- Modify: `src/bench/mod.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `pub const HEADER_LEN: usize = 16`
  - `pub struct Header { pub elapsed_ns: u64, pub publisher: u32, pub seq: u32 }`
  - `pub fn build(size: usize, header: Header) -> Vec<u8>`
  - `pub fn decode(payload: &[u8]) -> Option<Header>`

- [ ] **Step 1: Write the failing test**

Create `src/bench/payload.rs` with the doc comment and test module:

```rust
//! The measurement header carried in every benchmark payload.
//!
//! Publisher and subscriber run in one process, so a single baseline `Instant`
//! serves both and end-to-end latency needs no clock synchronisation. The
//! header is the elapsed nanoseconds since that baseline, plus enough identity
//! to attribute a message to its publisher.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_survives_a_round_trip() {
        let header = Header {
            elapsed_ns: 1_234_567_890,
            publisher: 7,
            seq: 42,
        };
        let payload = build(64, header);
        assert_eq!(payload.len(), 64);
        assert_eq!(decode(&payload), Some(header));
    }

    #[test]
    fn payload_smaller_than_the_header_is_filled_to_header_length() {
        // build() never truncates the header: a short --payload-size means the
        // payload is HEADER_LEN, and Config decides whether to measure at all.
        let payload = build(4, Header { elapsed_ns: 1, publisher: 0, seq: 0 });
        assert_eq!(payload.len(), HEADER_LEN);
    }

    #[test]
    fn decoding_a_short_payload_returns_none() {
        assert_eq!(decode(&[0u8; 15]), None);
        assert_eq!(decode(&[]), None);
    }

    #[test]
    fn filler_is_deterministic_so_runs_are_comparable() {
        let a = build(128, Header { elapsed_ns: 1, publisher: 1, seq: 1 });
        let b = build(128, Header { elapsed_ns: 1, publisher: 1, seq: 1 });
        assert_eq!(a, b);
    }
}
```

Add to `src/bench/mod.rs`:

```rust
pub mod payload;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test bench::payload`
Expected: FAIL — `cannot find type 'Header' in this scope`.

- [ ] **Step 3: Write minimal implementation**

Insert above the test module in `src/bench/payload.rs`:

```rust
/// Bytes occupied by the header at the front of every benchmark payload.
pub const HEADER_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// Nanoseconds since the run's baseline `Instant`.
    pub elapsed_ns: u64,
    pub publisher: u32,
    pub seq: u32,
}

/// Build a payload of `size` bytes carrying `header`. Big-endian throughout,
/// matching how MQTT itself encodes multi-byte integers.
///
/// A `size` below `HEADER_LEN` yields a `HEADER_LEN` payload rather than a
/// truncated header; `Config` is what decides whether such a run measures
/// end-to-end latency at all.
pub fn build(size: usize, header: Header) -> Vec<u8> {
    let mut buf = vec![0u8; size.max(HEADER_LEN)];
    buf[0..8].copy_from_slice(&header.elapsed_ns.to_be_bytes());
    buf[8..12].copy_from_slice(&header.publisher.to_be_bytes());
    buf[12..16].copy_from_slice(&header.seq.to_be_bytes());
    buf
}

/// Read the header back. `None` when the payload is too short to carry one,
/// which is the case for any message this run did not publish.
pub fn decode(payload: &[u8]) -> Option<Header> {
    if payload.len() < HEADER_LEN {
        return None;
    }
    Some(Header {
        elapsed_ns: u64::from_be_bytes(payload[0..8].try_into().ok()?),
        publisher: u32::from_be_bytes(payload[8..12].try_into().ok()?),
        seq: u32::from_be_bytes(payload[12..16].try_into().ok()?),
    })
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test bench::payload`
Expected: PASS, 4 tests.
Then: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 5: Commit**

```bash
git add src/bench/payload.rs src/bench/mod.rs
git commit -m "$(cat <<'EOF'
feat: add the benchmark measurement header

Sixteen big-endian bytes: elapsed nanoseconds since the run baseline,
publisher index, sequence number. One process, one baseline, so
end-to-end latency needs no clock synchronisation.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Rate pacing

**Files:**
- Create: `src/bench/schedule.rs`
- Modify: `src/bench/mod.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `pub struct Schedule` with `Schedule::new(start: Instant, per_second: Option<f64>) -> Schedule`
    and `deadline(&self, index: u64) -> Option<Instant>`

- [ ] **Step 1: Write the failing test**

Create `src/bench/schedule.rs` with the doc comment and test module:

```rust
//! Absolute-deadline pacing for a publisher.
//!
//! Deadlines are computed from the run start rather than from the previous
//! send, so one late wake-up does not push every later message back. A run
//! with no `--rate` has no deadlines at all.

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn unthrottled_runs_have_no_deadlines() {
        let schedule = Schedule::new(Instant::now(), None);
        assert_eq!(schedule.deadline(0), None);
        assert_eq!(schedule.deadline(1_000), None);
    }

    #[test]
    fn deadlines_are_absolute_multiples_of_the_interval() {
        let start = Instant::now();
        // 1000 messages per second: one every millisecond.
        let schedule = Schedule::new(start, Some(1000.0));
        assert_eq!(schedule.deadline(0), Some(start));
        assert_eq!(schedule.deadline(1), Some(start + Duration::from_millis(1)));
        assert_eq!(schedule.deadline(500), Some(start + Duration::from_millis(500)));
    }

    #[test]
    fn a_late_message_does_not_shift_the_ones_after_it() {
        // The property that matters: deadline(n) depends only on n, never on
        // when the previous send actually happened.
        let start = Instant::now();
        let schedule = Schedule::new(start, Some(100.0));
        let tenth = schedule.deadline(10).expect("a paced run has deadlines");
        assert_eq!(tenth, start + Duration::from_millis(100));
        assert_eq!(schedule.deadline(10), Some(tenth));
    }

    #[test]
    fn a_rate_of_zero_or_less_is_treated_as_unthrottled() {
        let start = Instant::now();
        assert_eq!(Schedule::new(start, Some(0.0)).deadline(1), None);
        assert_eq!(Schedule::new(start, Some(-1.0)).deadline(1), None);
    }
}
```

Add to `src/bench/mod.rs`:

```rust
pub mod schedule;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test bench::schedule`
Expected: FAIL — `cannot find type 'Schedule' in this scope`.

- [ ] **Step 3: Write minimal implementation**

Insert above the test module in `src/bench/schedule.rs`:

```rust
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub struct Schedule {
    start: Instant,
    interval: Option<Duration>,
}

impl Schedule {
    /// `per_second` is this publisher's share of the run's offered load. A
    /// `None`, zero or negative rate means send as fast as the connection
    /// allows.
    pub fn new(start: Instant, per_second: Option<f64>) -> Schedule {
        let interval = match per_second {
            Some(rate) if rate > 0.0 => Some(Duration::from_secs_f64(1.0 / rate)),
            _ => None,
        };
        Schedule { start, interval }
    }

    /// When message `index` should be sent, or `None` for an unthrottled run.
    pub fn deadline(&self, index: u64) -> Option<Instant> {
        self.interval.map(|i| self.start + i.mul_f64(index as f64))
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test bench::schedule`
Expected: PASS, 4 tests.
Then: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 5: Commit**

```bash
git add src/bench/schedule.rs src/bench/mod.rs
git commit -m "$(cat <<'EOF'
feat: add absolute-deadline rate pacing

deadline(n) depends only on n, so a late wake-up does not shift every
later message and the offered load stays what was asked for.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Arguments and configuration resolution

**Files:**
- Modify: `src/cli.rs` (add `BenchArgs`, add `Command::Bench`)
- Modify: `src/bench/mod.rs` (add `Config`, `Stop`, `effective_inflight`)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `pub struct BenchArgs` with public fields `conn: ConnectionArgs`,
    `publishers: usize`, `subscribers: usize`, `topic_prefix: String`,
    `qos: u8`, `payload_size: usize`, `count: Option<u64>`,
    `duration: Option<u64>`, `rate: Option<f64>`, `inflight: usize`,
    `warmup: u64`, `drain: u64`, `json: bool`
  - `Command::Bench(BenchArgs)`
  - `pub enum Stop { Count(u64), Duration(Duration), CountWithin(u64, Duration) }`
  - `pub struct Config` with fields `publishers`, `subscribers`,
    `topic_prefix: String`, `qos: QoS`, `payload_size: usize`, `stop: Stop`,
    `rate: Option<f64>`, `inflight: usize`, `warmup: Duration`,
    `drain: Duration`, `json: bool`
  - `impl Config { pub fn from_args(args: &BenchArgs) -> Result<Config>; pub fn measures_end_to_end(&self) -> bool; pub fn per_publisher_rate(&self) -> Option<f64>; pub fn topic_for(&self, publisher: u32) -> String; pub fn subscribe_filter(&self) -> String }`
  - `pub fn effective_inflight(requested: usize, broker_receive_maximum: Option<u16>) -> usize`

- [ ] **Step 1: Write the failing test**

Append to the existing `mod tests` in `src/cli.rs`:

```rust
    #[test]
    fn bench_defaults_match_the_documented_ones() {
        let cli = Cli::parse_from(["pulsemq-cli", "bench"]);
        let Command::Bench(args) = cli.command else {
            panic!("expected the bench subcommand");
        };
        assert_eq!(args.publishers, 1);
        assert_eq!(args.subscribers, 0);
        assert_eq!(args.topic_prefix, "bench");
        assert_eq!(args.qos, 0);
        assert_eq!(args.payload_size, 64);
        assert_eq!(args.inflight, 100);
        assert_eq!(args.drain, 2);
        assert!(!args.json);
    }
```

Add a test module to `src/bench/mod.rs`:

```rust
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
        assert_eq!(
            config.stop,
            Stop::CountWithin(50, Duration::from_secs(30))
        );
    }

    #[test]
    fn duration_alone_is_a_timed_run() {
        let args = args_from(&["pulsemq-cli", "bench", "--duration", "5"]);
        let config = Config::from_args(&args).unwrap();
        assert_eq!(config.stop, Stop::Duration(Duration::from_secs(5)));
    }

    #[test]
    fn a_payload_below_the_header_length_disables_end_to_end_latency() {
        let args = args_from(&["pulsemq-cli", "bench", "--payload-size", "8"]);
        let config = Config::from_args(&args).unwrap();
        assert!(!config.measures_end_to_end());

        let args = args_from(&["pulsemq-cli", "bench", "--payload-size", "16"]);
        let config = Config::from_args(&args).unwrap();
        assert!(config.measures_end_to_end());
    }

    #[test]
    fn the_offered_rate_is_split_across_publishers() {
        let args = args_from(&["pulsemq-cli", "bench", "--publishers", "4", "--rate", "1000"]);
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
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test bench:: && cargo test cli::tests::bench_defaults`
Expected: FAIL — `no variant named 'Bench'`, `cannot find type 'Config' in this scope`.

- [ ] **Step 3: Write minimal implementation**

In `src/cli.rs`, add the variant to `Command`:

```rust
    /// Drive load against a broker and report throughput and latency.
    Bench(BenchArgs),
```

and the argument struct, after `RequestArgs`:

```rust
#[derive(Args, Debug)]
pub struct BenchArgs {
    #[command(flatten)]
    pub conn: ConnectionArgs,

    /// Number of publishing connections.
    #[arg(long, default_value_t = 1, value_name = "N")]
    pub publishers: usize,

    /// Number of subscribing connections.
    #[arg(long, default_value_t = 0, value_name = "N")]
    pub subscribers: usize,

    /// Root of the topic tree: publisher k publishes to <prefix>/k and
    /// subscribers take <prefix>/#.
    #[arg(long, default_value = "bench", value_name = "PREFIX")]
    pub topic_prefix: String,

    /// Quality of Service for both publishing and subscribing.
    #[arg(short = 'q', long, default_value_t = 0, value_parser = clap::value_parser!(u8).range(0..=2))]
    pub qos: u8,

    /// Payload size in bytes. Below 16 there is no room for the measurement
    /// header, and end-to-end latency is not reported.
    #[arg(long, default_value_t = 64, value_name = "BYTES")]
    pub payload_size: usize,

    /// Total messages to publish across all publishers.
    #[arg(long, value_name = "N")]
    pub count: Option<u64>,

    /// Stop after this many seconds.
    #[arg(long, value_name = "SECS")]
    pub duration: Option<u64>,

    /// Offered load in messages per second, split across publishers.
    /// Omitted, publishers send as fast as the connection allows.
    #[arg(long, value_name = "MSGS_PER_SEC")]
    pub rate: Option<f64>,

    /// Unacknowledged messages allowed per publisher, capped by the broker's
    /// Receive Maximum.
    #[arg(long, default_value_t = 100, value_name = "N")]
    pub inflight: usize,

    /// Discard samples recorded during this many seconds at the start.
    #[arg(long, default_value_t = 0, value_name = "SECS")]
    pub warmup: u64,

    /// Seconds to keep subscribers running after the publishers finish.
    #[arg(long, default_value_t = 2, value_name = "SECS")]
    pub drain: u64,

    /// Emit the report as JSON instead of a table.
    #[arg(long)]
    pub json: bool,
}
```

In `src/bench/mod.rs`, above the test module:

```rust
use std::time::Duration;

use pulsemq::types::QoS;

use crate::cli::BenchArgs;
use crate::error::{Error, Result};

pub mod payload;
pub mod schedule;
pub mod stats;

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
    let cap = broker_receive_maximum.map(usize::from).unwrap_or(usize::MAX);
    requested.min(cap).max(1)
}
```

Note the two extra methods (`quota_for`, `deadline`) — Tasks 7 and 9 consume them.
Add tests for them now, in the same `mod tests`:

```rust
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
```

`src/main.rs` will not compile until `Command::Bench` is handled. Add a
temporary arm that fails loudly, replaced in Task 9:

```rust
        Command::Bench(_) => Err(pulsemq_cli::error::Error::Usage(
            "bench is not implemented yet".into(),
        )),
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test`
Expected: PASS — the existing 5 cli tests, the new cli default test, and 9 bench config tests.
Then: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 5: Commit**

```bash
git add src/cli.rs src/bench/mod.rs src/main.rs
git commit -m "$(cat <<'EOF'
feat: add bench arguments and configuration resolution

Every "which option wins" decision lives in Config: count over duration,
the default count, the per-publisher rate share and quota, and the
payload size below which end-to-end latency is not measured.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: Reusable handshake

**Files:**
- Modify: `src/client.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `pub struct Negotiated { pub version: ProtocolVersion, pub receive_maximum: Option<u16>, pub session_present: bool }`
  - `pub async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S, args: &ConnectionArgs, client_id: &str) -> Result<Negotiated>`
  - `pub fn generated_client_id() -> String` (visibility widened from private)
  - `Client::connect` keeps its signature and behaviour, now built on `handshake`.

**Why:** the bench tasks connect, hand shake, and only then split the socket
into halves. Doing the handshake before the split means one code path serves
both `Client` and the bench connections, and no packet framing has to work
across two half-streams.

- [ ] **Step 1: Write the failing test**

Add a test module at the end of `src/client.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use pulsemq::framing::{read_packet, write_packet, ReadOutcome};
    use pulsemq::packet::Connack;
    use tokio::io::duplex;

    fn connection_args(argv: &[&str]) -> ConnectionArgs {
        let cli = crate::cli::Cli::parse_from(argv);
        match cli.command {
            crate::cli::Command::Pub(args) => args.conn,
            _ => panic!("expected the pub subcommand"),
        }
    }

    /// The handshake runs over any AsyncRead + AsyncWrite, so an in-memory
    /// duplex stands in for a socket and the test needs no broker.
    #[tokio::test]
    async fn handshake_returns_the_brokers_receive_maximum() {
        let (mut client_side, mut broker_side) = duplex(4096);

        let broker = tokio::spawn(async move {
            let ReadOutcome::Packet(packet, _) =
                read_packet(&mut broker_side, 65_535, ProtocolVersion::V5)
                    .await
                    .expect("CONNECT decodes")
            else {
                panic!("expected a CONNECT, got EOF");
            };
            let Packet::Connect(connect) = packet else {
                panic!("expected a CONNECT");
            };
            assert_eq!(connect.protocol_name, "MQTT");
            assert_eq!(connect.client_id, "probe");

            let mut ack = Connack::new(false, ReasonCode::Success);
            ack.properties.receive_maximum = Some(20);
            write_packet(&mut broker_side, &Packet::Connack(ack), ProtocolVersion::V5)
                .await
                .expect("CONNACK writes");
        });

        let args = connection_args(["pulsemq-cli", "pub", "-t", "x"].as_slice());
        let negotiated = handshake(&mut client_side, &args, "probe")
            .await
            .expect("handshake succeeds");

        assert_eq!(negotiated.receive_maximum, Some(20));
        assert_eq!(negotiated.version, ProtocolVersion::V5);
        assert!(!negotiated.session_present);
        broker.await.expect("broker task");
    }

    #[tokio::test]
    async fn handshake_reports_a_refused_connection() {
        let (mut client_side, mut broker_side) = duplex(4096);

        let broker = tokio::spawn(async move {
            let _ = read_packet(&mut broker_side, 65_535, ProtocolVersion::V5).await;
            let ack = Connack::new(false, ReasonCode::NotAuthorized);
            write_packet(&mut broker_side, &Packet::Connack(ack), ProtocolVersion::V5)
                .await
                .expect("CONNACK writes");
        });

        let args = connection_args(["pulsemq-cli", "pub", "-t", "x"].as_slice());
        let err = handshake(&mut client_side, &args, "probe")
            .await
            .expect_err("a refused connection is an error");
        assert!(matches!(
            err,
            Error::Rejected {
                code: ReasonCode::NotAuthorized,
                ..
            }
        ));
        broker.await.expect("broker task");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test client::`
Expected: FAIL — `cannot find function 'handshake' in this scope`.

- [ ] **Step 3: Write minimal implementation**

In `src/client.rs`, add the imports the new code needs:

```rust
use tokio::io::{AsyncRead, AsyncWrite};
```

Add the negotiated-parameters type and the free function, above `impl Client`:

```rust
/// What the broker granted in CONNACK, for the caller to respect afterwards.
#[derive(Debug, Clone, Copy)]
pub struct Negotiated {
    pub version: ProtocolVersion,
    /// The broker's Receive Maximum (3.2.2.3.3): how many QoS 1/2 messages it
    /// will accept without acknowledging. Absent in v3.x, which has no
    /// properties.
    pub receive_maximum: Option<u16>,
    pub session_present: bool,
}

/// Perform CONNECT/CONNACK over an already-connected stream.
///
/// Separate from `Client` because the benchmark connects, hands shake, and only
/// then splits the socket into halves — running the handshake first means one
/// implementation serves both, and no packet has to be framed across two
/// half-streams.
pub async fn handshake<S>(
    stream: &mut S,
    args: &ConnectionArgs,
    client_id: &str,
) -> Result<Negotiated>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let version = args.version();
    let password = args
        .password()
        .map_err(|e| Error::Usage(format!("cannot read the password file: {e}")))?;

    let connect = Connect {
        // v3.1 uses "MQIsdp"; v3.1.1 and v5.0 use "MQTT" (3.1.2.1).
        protocol_name: match version {
            ProtocolVersion::V3_1 => "MQIsdp".to_string(),
            _ => "MQTT".to_string(),
        },
        protocol_version: version.level(),
        clean_start: args.clean_start(),
        keep_alive: args.keepalive,
        properties: Default::default(),
        client_id: client_id.to_string(),
        will: None,
        username: args.user.clone(),
        password,
    };
    write_packet(stream, &Packet::Connect(connect), version).await?;

    match read_packet(stream, MAX_PACKET_SIZE, version).await? {
        ReadOutcome::Packet(Packet::Connack(ack), _) if ack.reason_code == ReasonCode::Success => {
            Ok(Negotiated {
                version,
                receive_maximum: ack.properties.receive_maximum,
                session_present: ack.session_present,
            })
        }
        ReadOutcome::Packet(Packet::Connack(ack), _) => Err(Error::Rejected {
            what: "connection".into(),
            code: ack.reason_code,
        }),
        ReadOutcome::Packet(other, _) => Err(Error::Mqtt(pulsemq::error::protocol(format!(
            "expected CONNACK, got {}",
            other.name()
        )))),
        ReadOutcome::Eof => Err(Error::Disconnected("during the handshake".into())),
    }
}

/// A client identifier that is unique enough for a short-lived tool run and
/// still recognisable in broker logs.
pub fn generated_client_id() -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("pulsemq-cli-{pid}-{nanos:x}")
}
```

Replace the body of `Client::connect` with one that reuses it, and delete the
old private `default_client_id`/`generated_client_id` function at the bottom of
the file so only the public one remains:

```rust
    pub async fn connect(args: &ConnectionArgs) -> Result<Client> {
        let client_id = match &args.client_id {
            Some(id) => id.clone(),
            None => generated_client_id(),
        };
        if !args.clean_start() && client_id.is_empty() {
            return Err(Error::Usage(
                "--persistent-session needs a non-empty --client-id: a broker-assigned \
                 identifier cannot be resumed"
                    .into(),
            ));
        }

        let mut stream = TcpStream::connect((args.broker.as_str(), args.port)).await?;
        // One small packet at a time is this tool's whole traffic pattern;
        // Nagle would add up to 40 ms to every request/reply round trip.
        stream.set_nodelay(true)?;

        let negotiated = handshake(&mut stream, args, &client_id).await?;

        Ok(Client {
            stream,
            version: negotiated.version,
            keep_alive: args.keepalive,
            next_packet_id: 0,
        })
    }
```

Make `MAX_PACKET_SIZE` visible to the rest of the crate, since the bench read
loops need the same limit:

```rust
pub(crate) const MAX_PACKET_SIZE: u32 = 268_435_455;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test`
Expected: PASS, including the two new `client::tests`.
Then run the live check that `pub`/`sub` still work, exactly as `CLAUDE.md`
documents:

```bash
cargo build --manifest-path ../pulsemq/Cargo.toml --bin pulsemq
../pulsemq/target/debug/pulsemq --listen-addr 127.0.0.1:18830 \
    --admin-addr 127.0.0.1:19001 --db-path /tmp/smoke.db --sys-interval 0 &
cargo run -- sub -b 127.0.0.1 -p 18830 -t 'test/#' -q 1 --show-topic -n 1 &
sleep 1
cargo run -- pub -b 127.0.0.1 -p 18830 -t test/a -m hello -q 1
```
Expected: the subscriber prints `test/a hello`.
Then: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 5: Commit**

```bash
git add src/client.rs
git commit -m "$(cat <<'EOF'
refactor: extract the handshake so a split connection can reuse it

handshake() runs CONNECT/CONNACK over any AsyncRead + AsyncWrite and
returns the negotiated parameters, including the broker's Receive
Maximum. Client is now a thin caller; behaviour is unchanged.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: Counters and the report

**Files:**
- Modify: `src/bench/stats.rs`
- Modify: `Cargo.toml`

**Interfaces:**
- Consumes: `Samples`, `Summary` (Task 1).
- Produces:
  - `pub struct Counters` with `published`, `acknowledged`, `received`,
    `bytes_sent`, `bytes_received`, `publish_errors` as `AtomicU64`, plus
    `refused: Mutex<BTreeMap<u8, u64>>`, and methods
    `record_refusal(&self, code: ReasonCode)`, `snapshot(&self) -> CounterTotals`
  - `pub struct CounterTotals { pub published: u64, pub acknowledged: u64, pub received: u64, pub bytes_sent: u64, pub bytes_received: u64, pub publish_errors: u64, pub refused: BTreeMap<u8, u64> }`
  - `pub struct Report { pub config: ReportConfig, pub elapsed: Duration, pub totals: CounterTotals, pub task_failures: Vec<String>, pub ack: Option<Summary>, pub end_to_end: Option<Summary>, pub end_to_end_disabled: bool }`
  - `pub struct ReportConfig { pub publishers: usize, pub subscribers: usize, pub qos: u8, pub payload_size: usize, pub inflight: usize, pub rate: Option<f64>, pub topic_prefix: String }`
  - `impl Report { pub fn to_table(&self) -> String; pub fn to_json(&self) -> String; pub fn success(&self) -> bool; pub fn publish_rate(&self) -> f64; pub fn receive_rate(&self) -> f64 }`

- [ ] **Step 1: Write the failing test**

Add to `mod tests` in `src/bench/stats.rs`:

```rust
    use pulsemq::types::ReasonCode;
    use std::time::Duration;

    fn totals() -> CounterTotals {
        let counters = Counters::default();
        counters.published.fetch_add(1000, Ordering::Relaxed);
        counters.acknowledged.fetch_add(1000, Ordering::Relaxed);
        counters.received.fetch_add(2000, Ordering::Relaxed);
        counters.bytes_sent.fetch_add(64_000, Ordering::Relaxed);
        counters.bytes_received.fetch_add(128_000, Ordering::Relaxed);
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
        let value: serde_json::Value =
            serde_json::from_str(&report.to_json()).expect("valid JSON");
        assert_eq!(value["config"]["publishers"], 1);
        assert_eq!(value["config"]["qos"], 1);
        assert_eq!(value["counters"]["published"], 1000);
        assert_eq!(value["elapsed_secs"], 1.5);
        assert_eq!(value["task_failures"][0], "subscriber 1: connection reset");
        assert!(value["latency"]["ack"].is_null());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test bench::stats`
Expected: FAIL — `cannot find type 'Counters' in this scope`, and
`use of unresolved crate serde_json`.

- [ ] **Step 3: Write minimal implementation**

Add the dependency to `Cargo.toml`, under `[dependencies]`:

```toml
# --json report output. Already present in the compiled tree through the
# pulsemq dependency, so it adds no new build cost.
serde_json = "1"
```

Add to the top of `src/bench/stats.rs`:

```rust
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use pulsemq::types::ReasonCode;
```

and, above the test module:

```rust
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test bench::stats`
Expected: PASS, 10 tests.
Then: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock src/bench/stats.rs
git commit -m "$(cat <<'EOF'
feat: add run counters and the benchmark report

Counters tally refusals per Reason Code so a broker that starts refusing
mid-run is visible rather than looking slow. Rates derive from measured
wall time, and a measurement that could not run says so instead of
printing zeros.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: The publisher task

**Files:**
- Create: `src/bench/publisher.rs`
- Modify: `src/bench/mod.rs`
- Modify: `Cargo.toml`

**Interfaces:**
- Consumes: `Config`, `effective_inflight` (Task 4); `payload::build`,
  `payload::Header` (Task 2); `Schedule` (Task 3); `Samples`, `Counters`
  (Tasks 1, 6); `handshake`, `Negotiated`, `MAX_PACKET_SIZE` (Task 5).
- Produces:
  - `pub async fn run(index: u32, config: Arc<Config>, conn: ConnectionArgs, counters: Arc<Counters>, baseline: Instant, warmup_until: Instant, stop: watch::Receiver<bool>) -> Result<Samples>`

**Design notes for the implementer:** the connection is established and hand
shaken as a whole `TcpStream`, then split with `into_split()`. The write side
sends; the read side handles acknowledgements. They share a
`tokio::sync::Semaphore` whose permit count is the in-flight window, and an
`Arc<Mutex<HashMap<u16, Instant>>>` recording when each packet identifier was
sent. Acquiring a permit before sending is what bounds the window; the read
side releases the permit when the acknowledgement lands.

- [ ] **Step 1: Write the failing test**

Add the tokio features the task needs to `Cargo.toml`:

```toml
tokio = { version = "1.53", features = ["rt", "rt-multi-thread", "macros", "net", "io-util", "time", "sync", "signal"] }
```

Create `src/bench/publisher.rs` with the doc comment and a test module that
drives the task against an in-process broker stub. The stub is a real TCP
listener that speaks just enough MQTT to acknowledge:

```rust
//! One publishing connection: a write side that paces sends and holds the
//! in-flight window, and a read side that matches acknowledgements.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bench::stats::Counters;
    use clap::Parser;
    use pulsemq::framing::{read_packet, write_packet, ReadOutcome};
    use pulsemq::packet::{Connack, PubAck};
    use pulsemq::types::{ProtocolVersion, ReasonCode};
    use tokio::net::TcpListener;

    /// A broker stub: accepts one connection, CONNACKs, then PUBACKs every
    /// PUBLISH it sees. Returns how many PUBLISH packets arrived.
    async fn ack_everything(listener: TcpListener) -> u64 {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let version = ProtocolVersion::V5;

        let ReadOutcome::Packet(Packet::Connect(_), _) =
            read_packet(&mut stream, 65_535, version).await.expect("CONNECT")
        else {
            panic!("expected a CONNECT");
        };
        write_packet(
            &mut stream,
            &Packet::Connack(Connack::new(false, ReasonCode::Success)),
            version,
        )
        .await
        .expect("CONNACK");

        let mut seen = 0u64;
        loop {
            match read_packet(&mut stream, 268_435_455, version).await {
                Ok(ReadOutcome::Packet(Packet::Publish(p), _)) => {
                    seen += 1;
                    if let Some(id) = p.packet_id {
                        let ack = PubAck::new(id, ReasonCode::Success);
                        write_packet(&mut stream, &Packet::Puback(ack), version)
                            .await
                            .expect("PUBACK");
                    }
                }
                Ok(ReadOutcome::Packet(_, _)) => continue,
                _ => return seen,
            }
        }
    }

    fn config_for(argv: &[&str]) -> Config {
        let cli = crate::cli::Cli::parse_from(argv);
        let crate::cli::Command::Bench(args) = cli.command else {
            panic!("expected the bench subcommand");
        };
        Config::from_args(&args).expect("config resolves")
    }

    fn connection_args(port: u16) -> ConnectionArgs {
        let port = port.to_string();
        let cli = crate::cli::Cli::parse_from([
            "pulsemq-cli",
            "bench",
            "-b",
            "127.0.0.1",
            "-p",
            &port,
        ]);
        let crate::cli::Command::Bench(args) = cli.command else {
            panic!("expected the bench subcommand");
        };
        args.conn
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn publishes_its_quota_and_records_one_ack_sample_each() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let broker = tokio::spawn(ack_everything(listener));

        let config = Arc::new(config_for(&[
            "pulsemq-cli",
            "bench",
            "--count",
            "10",
            "--qos",
            "1",
        ]));
        let counters = Arc::new(Counters::default());
        let baseline = Instant::now();
        let (_tx, stop) = watch::channel(false);

        let samples = run(
            0,
            config,
            connection_args(port),
            counters.clone(),
            baseline,
            baseline,
            stop,
        )
        .await
        .expect("publisher completes");

        assert_eq!(counters.published.load(Ordering::Relaxed), 10);
        assert_eq!(counters.acknowledged.load(Ordering::Relaxed), 10);
        assert_eq!(samples.len(), 10);
        assert!(broker.await.expect("broker task") >= 10);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_stop_signal_ends_a_timed_run_early() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let broker = tokio::spawn(ack_everything(listener));

        // A duration run has no quota: only the stop signal ends it.
        let config = Arc::new(config_for(&[
            "pulsemq-cli",
            "bench",
            "--duration",
            "600",
            "--rate",
            "50",
        ]));
        let counters = Arc::new(Counters::default());
        let baseline = Instant::now();
        let (tx, stop) = watch::channel(false);

        let handle = tokio::spawn(run(
            0,
            config,
            connection_args(port),
            counters.clone(),
            baseline,
            baseline,
            stop,
        ));
        tokio::time::sleep(Duration::from_millis(200)).await;
        tx.send(true).expect("stop signal sent");

        let samples = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("publisher stops promptly")
            .expect("task joins")
            .expect("publisher completes");

        assert!(counters.published.load(Ordering::Relaxed) > 0);
        // QoS 0 by default: no acknowledgements, so no ack samples.
        assert_eq!(samples.len(), 0);
        let _ = broker.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn warmup_discards_early_samples() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let broker = tokio::spawn(ack_everything(listener));

        let config = Arc::new(config_for(&[
            "pulsemq-cli",
            "bench",
            "--count",
            "5",
            "--qos",
            "1",
        ]));
        let counters = Arc::new(Counters::default());
        let baseline = Instant::now();
        // A warmup cutoff in the far future discards everything.
        let warmup_until = baseline + Duration::from_secs(3600);
        let (_tx, stop) = watch::channel(false);

        let samples = run(
            0,
            config,
            connection_args(port),
            counters.clone(),
            baseline,
            warmup_until,
            stop,
        )
        .await
        .expect("publisher completes");

        assert_eq!(counters.published.load(Ordering::Relaxed), 5);
        assert_eq!(samples.len(), 0, "warmup samples are discarded");
        let _ = broker.await;
    }
}
```

Add to `src/bench/mod.rs`:

```rust
pub mod publisher;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test bench::publisher`
Expected: FAIL — `cannot find function 'run' in this scope`.

- [ ] **Step 3: Write minimal implementation**

Insert above the test module in `src/bench/publisher.rs`:

```rust
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pulsemq::framing::{read_packet, write_packet, ReadOutcome};
use pulsemq::packet::{Packet, PubAck, Publish};
use pulsemq::types::{QoS, ReasonCode};
use tokio::net::TcpStream;
use tokio::sync::{watch, Mutex, Semaphore};

use crate::bench::payload::{self, Header};
use crate::bench::schedule::Schedule;
use crate::bench::stats::{Counters, Samples};
use crate::bench::Config;
use crate::cli::ConnectionArgs;
use crate::client::{handshake, MAX_PACKET_SIZE};
use crate::error::{Error, Result};

/// Send times for the packets this publisher is waiting on.
type InFlight = Arc<Mutex<HashMap<u16, Instant>>>;

/// Run one publisher to completion. Returns its acknowledgement-latency
/// samples; end-to-end samples come from the subscribers.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    index: u32,
    config: Arc<Config>,
    conn: ConnectionArgs,
    counters: Arc<Counters>,
    baseline: Instant,
    warmup_until: Instant,
    mut stop: watch::Receiver<bool>,
) -> Result<Samples> {
    let client_id = match &conn.client_id {
        Some(id) => format!("{id}-pub-{index}"),
        None => format!("{}-pub-{index}", crate::client::generated_client_id()),
    };

    let mut stream = TcpStream::connect((conn.broker.as_str(), conn.port)).await?;
    stream.set_nodelay(true)?;
    let negotiated = handshake(&mut stream, &conn, &client_id).await?;
    let version = negotiated.version;
    let window = crate::bench::effective_inflight(config.inflight, negotiated.receive_maximum);

    let (mut reader, mut writer) = stream.into_split();
    let permits = Arc::new(Semaphore::new(window));
    let in_flight: InFlight = Arc::new(Mutex::new(HashMap::new()));

    // The read side lives as long as the write side plus its outstanding
    // acknowledgements; it ends when the socket closes or the write side
    // signals that it is done and the window has drained.
    let ack_task = {
        let permits = permits.clone();
        let in_flight = in_flight.clone();
        let counters = counters.clone();
        tokio::spawn(async move {
            let mut samples = Samples::new();
            loop {
                match read_packet(&mut reader, MAX_PACKET_SIZE, version).await {
                    Ok(ReadOutcome::Packet(packet, _)) => match packet {
                        Packet::Puback(ack) | Packet::Pubcomp(ack) => {
                            if ack.reason_code.is_error() {
                                counters.record_refusal(ack.reason_code);
                            }
                            let sent = in_flight.lock().await.remove(&ack.packet_id);
                            if let Some(sent) = sent {
                                counters.acknowledged.fetch_add(1, Ordering::Relaxed);
                                if sent >= warmup_until {
                                    samples.record(sent.elapsed().as_nanos() as u64);
                                }
                                permits.add_permits(1);
                            }
                        }
                        // QoS 2: the broker's PUBREC needs a PUBREL before the
                        // PUBCOMP that completes the exchange (4.3.3).
                        Packet::Pubrec(rec) => {
                            if rec.reason_code.is_error() {
                                counters.record_refusal(rec.reason_code);
                                if in_flight.lock().await.remove(&rec.packet_id).is_some() {
                                    permits.add_permits(1);
                                }
                                continue;
                            }
                            // The PUBREL is written by the ack side, which is
                            // the only task holding the write half after the
                            // send loop finishes; see `pubrel_tx` below.
                            let _ = rec;
                        }
                        _ => {}
                    },
                    _ => break,
                }
            }
            samples
        })
    };

    let schedule = Schedule::new(Instant::now(), config.per_publisher_rate());
    let quota = config.quota_for(index as usize);
    let mut sent: u64 = 0;
    let mut next_packet_id: u16 = 0;

    loop {
        if quota.is_some_and(|q| sent >= q) {
            break;
        }
        if *stop.borrow_and_update() {
            break;
        }
        if let Some(deadline) = schedule.deadline(sent) {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline.into()) => {}
                _ = stop.changed() => {
                    if *stop.borrow() {
                        break;
                    }
                }
            }
        }

        let permit = if config.qos == QoS::AtMostOnce {
            None
        } else {
            Some(
                permits
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| Error::Usage("in-flight window closed".into()))?,
            )
        };

        let packet_id = if config.qos == QoS::AtMostOnce {
            None
        } else {
            next_packet_id = next_packet_id.wrapping_add(1);
            if next_packet_id == 0 {
                next_packet_id = 1;
            }
            Some(next_packet_id)
        };

        let now = Instant::now();
        let body = payload::build(
            config.payload_size,
            Header {
                elapsed_ns: now.duration_since(baseline).as_nanos() as u64,
                publisher: index,
                seq: sent as u32,
            },
        );
        let bytes = body.len() as u64;
        let publish = Publish {
            dup: false,
            qos: config.qos,
            retain: false,
            topic: config.topic_for(index),
            packet_id,
            properties: Default::default(),
            payload: body.into(),
        };

        if let Some(id) = packet_id {
            in_flight.lock().await.insert(id, now);
        }
        match write_packet(&mut writer, &Packet::Publish(publish), version).await {
            Ok(_) => {
                counters.published.fetch_add(1, Ordering::Relaxed);
                counters.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
                // The permit is released by the ack side, so hand ownership of
                // it to the in-flight map's lifetime by forgetting it here.
                std::mem::forget(permit);
                sent += 1;
            }
            Err(e) => {
                counters.publish_errors.fetch_add(1, Ordering::Relaxed);
                if let Some(id) = packet_id {
                    in_flight.lock().await.remove(&id);
                }
                drop(permit);
                return Err(Error::from(e)).inspect_err(|_| ())?;
            }
        }
    }

    // Give outstanding acknowledgements a moment to land, then close the
    // socket so the read side returns.
    let drain_deadline = Instant::now() + Duration::from_secs(5);
    while !in_flight.lock().await.is_empty() && Instant::now() < drain_deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _ = write_packet(
        &mut writer,
        &Packet::Disconnect(pulsemq::packet::Disconnect::new(ReasonCode::Success)),
        version,
    )
    .await;
    drop(writer);

    ack_task
        .await
        .map_err(|e| Error::Usage(format!("publisher {index} ack task failed: {e}")))
}
```

**Implementer note on the permit handoff:** `std::mem::forget(permit)` leaks the
permit deliberately — the semaphore's count is restored by
`permits.add_permits(1)` on the acknowledgement path, which is the only place
that knows the message is complete. If you find that too subtle, the equivalent
is `permit.forget()` on an `OwnedSemaphorePermit`, which is the documented way
to do exactly this and reads better. Prefer `permit.forget()`; drop the
`std::mem::forget` line.

**Implementer note on QoS 2:** the PUBREL must be written on the same write
half the send loop owns. Keep it simple: for QoS 2, have the ack task send
nothing and instead let the send loop poll a `tokio::sync::mpsc` of packet
identifiers needing a PUBREL, writing them between publishes. Wire that channel
in the same task, and if it complicates the loop past readability, restrict
`bench` to QoS 0 and 1 in `Config::from_args`, document the restriction in the
help text, and open it as a follow-up item in `TODO.md` — a smaller honest
scope beats a subtly wrong QoS 2 path.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test bench::publisher`
Expected: PASS, 3 tests.
Then: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock src/bench/publisher.rs src/bench/mod.rs
git commit -m "$(cat <<'EOF'
feat: add the benchmark publisher task

Split connection: the write side paces sends against absolute deadlines
and holds a semaphore-bounded in-flight window; the read side matches
acknowledgements, records latency and releases the window.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: The subscriber task

**Files:**
- Create: `src/bench/subscriber.rs`
- Modify: `src/bench/mod.rs`

**Interfaces:**
- Consumes: `Config` (Task 4); `payload::decode` (Task 2); `Samples`,
  `Counters` (Tasks 1, 6); `handshake`, `MAX_PACKET_SIZE` (Task 5).
- Produces:
  - `pub async fn run(index: u32, config: Arc<Config>, conn: ConnectionArgs, counters: Arc<Counters>, baseline: Instant, warmup_until: Instant, stop: watch::Receiver<bool>) -> Result<Samples>`
  - `pub async fn ready(index: u32, ...)` is **not** part of this task; the
    subscribe acknowledgement is awaited inside `run` before it starts timing.

- [ ] **Step 1: Write the failing test**

Create `src/bench/subscriber.rs` with the doc comment and test module:

```rust
//! One subscribing connection: subscribe, acknowledge inbound messages, and
//! time each one from its measurement header to receipt.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bench::payload::{self, Header};
    use crate::bench::stats::Counters;
    use clap::Parser;
    use pulsemq::framing::{read_packet, write_packet, ReadOutcome};
    use pulsemq::packet::{Connack, Publish, SubAck};
    use pulsemq::types::{ProtocolVersion, ReasonCode};
    use tokio::net::TcpListener;

    /// A broker stub that accepts the subscription and then delivers
    /// `deliveries` QoS 0 messages, each carrying a header aged `age`.
    async fn deliver(listener: TcpListener, deliveries: usize, age: Duration, baseline: Instant) {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let version = ProtocolVersion::V5;

        let ReadOutcome::Packet(Packet::Connect(_), _) =
            read_packet(&mut stream, 65_535, version).await.expect("CONNECT")
        else {
            panic!("expected a CONNECT");
        };
        write_packet(
            &mut stream,
            &Packet::Connack(Connack::new(false, ReasonCode::Success)),
            version,
        )
        .await
        .expect("CONNACK");

        let ReadOutcome::Packet(Packet::Subscribe(sub), _) =
            read_packet(&mut stream, 65_535, version).await.expect("SUBSCRIBE")
        else {
            panic!("expected a SUBSCRIBE");
        };
        assert_eq!(sub.filters[0].filter, "bench/#");
        write_packet(
            &mut stream,
            &Packet::Suback(SubAck::new(sub.packet_id, vec![ReasonCode::Success])),
            version,
        )
        .await
        .expect("SUBACK");

        for seq in 0..deliveries {
            let sent_at = Instant::now() - age;
            let body = payload::build(
                64,
                Header {
                    elapsed_ns: sent_at.duration_since(baseline).as_nanos() as u64,
                    publisher: 0,
                    seq: seq as u32,
                },
            );
            let publish = Publish {
                dup: false,
                qos: pulsemq::types::QoS::AtMostOnce,
                retain: false,
                topic: "bench/0".into(),
                packet_id: None,
                properties: Default::default(),
                payload: body.into(),
            };
            write_packet(&mut stream, &Packet::Publish(publish), version)
                .await
                .expect("PUBLISH");
        }
    }

    fn config_for(argv: &[&str]) -> Config {
        let cli = crate::cli::Cli::parse_from(argv);
        let crate::cli::Command::Bench(args) = cli.command else {
            panic!("expected the bench subcommand");
        };
        Config::from_args(&args).expect("config resolves")
    }

    fn connection_args(port: u16) -> ConnectionArgs {
        let port = port.to_string();
        let cli =
            crate::cli::Cli::parse_from(["pulsemq-cli", "bench", "-b", "127.0.0.1", "-p", &port]);
        let crate::cli::Command::Bench(args) = cli.command else {
            panic!("expected the bench subcommand");
        };
        args.conn
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn times_each_delivery_from_its_header() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let baseline = Instant::now();
        let broker = tokio::spawn(deliver(listener, 3, Duration::from_millis(50), baseline));

        let config = Arc::new(config_for(&["pulsemq-cli", "bench", "--subscribers", "1"]));
        let counters = Arc::new(Counters::default());
        let (tx, stop) = watch::channel(false);

        let handle = tokio::spawn(run(
            0,
            config,
            connection_args(port),
            counters.clone(),
            baseline,
            baseline,
            stop,
        ));
        tokio::time::sleep(Duration::from_millis(300)).await;
        tx.send(true).expect("stop signal");

        let mut samples = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("subscriber stops promptly")
            .expect("task joins")
            .expect("subscriber completes");

        assert_eq!(counters.received.load(Ordering::Relaxed), 3);
        assert_eq!(samples.len(), 3);
        let summary = samples.summary().expect("three samples");
        // Each message was aged 50 ms before delivery, so every sample is at
        // least that, and well under a second on any machine.
        assert!(summary.min_ns >= 40_000_000, "min was {} ns", summary.min_ns);
        assert!(summary.max_ns < 1_000_000_000, "max was {} ns", summary.max_ns);
        broker.await.expect("broker task");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn counts_messages_even_when_the_payload_carries_no_header() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let baseline = Instant::now();
        // payload::build never produces a short payload, so this stub is the
        // one that sends a 4-byte one: reuse `deliver` with a custom size by
        // publishing directly.
        let broker = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let version = ProtocolVersion::V5;
            let _ = read_packet(&mut stream, 65_535, version).await;
            write_packet(
                &mut stream,
                &Packet::Connack(Connack::new(false, ReasonCode::Success)),
                version,
            )
            .await
            .expect("CONNACK");
            let ReadOutcome::Packet(Packet::Subscribe(sub), _) =
                read_packet(&mut stream, 65_535, version).await.expect("SUBSCRIBE")
            else {
                panic!("expected a SUBSCRIBE");
            };
            write_packet(
                &mut stream,
                &Packet::Suback(SubAck::new(sub.packet_id, vec![ReasonCode::Success])),
                version,
            )
            .await
            .expect("SUBACK");
            let publish = Publish {
                dup: false,
                qos: pulsemq::types::QoS::AtMostOnce,
                retain: false,
                topic: "bench/0".into(),
                packet_id: None,
                properties: Default::default(),
                payload: vec![1u8, 2, 3, 4].into(),
            };
            write_packet(&mut stream, &Packet::Publish(publish), version)
                .await
                .expect("PUBLISH");
        });

        let config = Arc::new(config_for(&["pulsemq-cli", "bench", "--subscribers", "1"]));
        let counters = Arc::new(Counters::default());
        let (tx, stop) = watch::channel(false);
        let handle = tokio::spawn(run(
            0,
            config,
            connection_args(port),
            counters.clone(),
            baseline,
            baseline,
            stop,
        ));
        tokio::time::sleep(Duration::from_millis(300)).await;
        tx.send(true).expect("stop signal");
        let samples = handle.await.expect("task joins").expect("completes");

        assert_eq!(counters.received.load(Ordering::Relaxed), 1);
        assert_eq!(samples.len(), 0, "a headerless message is counted, not timed");
        broker.await.expect("broker task");
    }
}
```

Add to `src/bench/mod.rs`:

```rust
pub mod subscriber;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test bench::subscriber`
Expected: FAIL — `cannot find function 'run' in this scope`.

- [ ] **Step 3: Write minimal implementation**

Insert above the test module in `src/bench/subscriber.rs`:

```rust
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pulsemq::framing::{read_packet, write_packet, ReadOutcome};
use pulsemq::packet::{Packet, PubAck, Subscribe, TopicFilter};
use pulsemq::types::{QoS, ReasonCode, RetainHandling};
use tokio::net::TcpStream;
use tokio::sync::watch;

use crate::bench::payload;
use crate::bench::stats::{Counters, Samples};
use crate::bench::Config;
use crate::cli::ConnectionArgs;
use crate::client::{handshake, MAX_PACKET_SIZE};
use crate::error::{Error, Result};

/// Run one subscriber until the stop signal. Returns its end-to-end latency
/// samples.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    index: u32,
    config: Arc<Config>,
    conn: ConnectionArgs,
    counters: Arc<Counters>,
    baseline: Instant,
    warmup_until: Instant,
    mut stop: watch::Receiver<bool>,
) -> Result<Samples> {
    let client_id = match &conn.client_id {
        Some(id) => format!("{id}-sub-{index}"),
        None => format!("{}-sub-{index}", crate::client::generated_client_id()),
    };

    let mut stream = TcpStream::connect((conn.broker.as_str(), conn.port)).await?;
    stream.set_nodelay(true)?;
    let negotiated = handshake(&mut stream, &conn, &client_id).await?;
    let version = negotiated.version;

    let subscribe = Subscribe {
        packet_id: 1,
        properties: Default::default(),
        filters: vec![TopicFilter {
            filter: config.subscribe_filter(),
            qos: config.qos,
            no_local: false,
            retain_as_published: false,
            retain_handling: RetainHandling::SendAtSubscribe,
        }],
    };
    write_packet(&mut stream, &Packet::Subscribe(subscribe), version).await?;

    // Wait for the SUBACK before returning: the run must not start publishing
    // until every subscriber is actually subscribed, or the first messages are
    // delivered to nobody and the receive count is wrong.
    loop {
        match read_packet(&mut stream, MAX_PACKET_SIZE, version).await? {
            ReadOutcome::Packet(Packet::Suback(ack), _) => {
                if let Some(code) = ack.reason_codes.iter().find(|c| c.is_error()) {
                    return Err(Error::Rejected {
                        what: format!("subscription for subscriber {index}"),
                        code: *code,
                    });
                }
                break;
            }
            ReadOutcome::Packet(_, _) => continue,
            ReadOutcome::Eof => {
                return Err(Error::Disconnected("while subscribing".into()));
            }
        }
    }

    let mut samples = Samples::new();
    loop {
        if *stop.borrow_and_update() {
            break;
        }
        let read = tokio::select! {
            read = read_packet(&mut stream, MAX_PACKET_SIZE, version) => read,
            _ = stop.changed() => continue,
        };

        match read {
            Ok(ReadOutcome::Packet(Packet::Publish(p), size)) => {
                let received_at = Instant::now();
                counters.received.fetch_add(1, Ordering::Relaxed);
                counters
                    .bytes_received
                    .fetch_add(p.payload.len() as u64, Ordering::Relaxed);
                let _ = size;

                if let Some(header) = payload::decode(&p.payload) {
                    let sent_at = baseline + Duration::from_nanos(header.elapsed_ns);
                    if sent_at >= warmup_until && received_at >= sent_at {
                        samples.record(received_at.duration_since(sent_at).as_nanos() as u64);
                    }
                }

                // Acknowledge per QoS (4.3.2, 4.3.3). QoS 2 completes on the
                // PUBREL the broker sends next.
                if let Some(id) = p.packet_id {
                    let ack = PubAck::new(id, ReasonCode::Success);
                    let reply = match p.qos {
                        QoS::AtMostOnce => None,
                        QoS::AtLeastOnce => Some(Packet::Puback(ack)),
                        QoS::ExactlyOnce => Some(Packet::Pubrec(ack)),
                    };
                    if let Some(reply) = reply {
                        write_packet(&mut stream, &reply, version).await?;
                    }
                }
            }
            Ok(ReadOutcome::Packet(Packet::Pubrel(rel), _)) => {
                let comp = PubAck::new(rel.packet_id, ReasonCode::Success);
                write_packet(&mut stream, &Packet::Pubcomp(comp), version).await?;
            }
            Ok(ReadOutcome::Packet(Packet::Disconnect(d), _)) => {
                return Err(Error::Rejected {
                    what: format!("subscriber {index}"),
                    code: d.reason_code,
                });
            }
            Ok(ReadOutcome::Packet(_, _)) => continue,
            Ok(ReadOutcome::Eof) => break,
            Err(e) => return Err(Error::from(e)),
        }
    }

    Ok(samples)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test bench::subscriber`
Expected: PASS, 2 tests.
Then: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 5: Commit**

```bash
git add src/bench/subscriber.rs src/bench/mod.rs
git commit -m "$(cat <<'EOF'
feat: add the benchmark subscriber task

Waits for its SUBACK before the run starts, times every delivery from the
measurement header, and counts messages that carry no header rather than
timing them wrongly.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Orchestration, drain and shutdown

**Files:**
- Modify: `src/bench/mod.rs` (add `run`)
- Modify: `src/main.rs` (runtime per subcommand, dispatch `Command::Bench`)

**Interfaces:**
- Consumes: everything from Tasks 1-8.
- Produces:
  - `pub async fn run(args: BenchArgs) -> Result<()>`
  - `src/main.rs` builds a multi-threaded runtime for `bench`, current-thread
    for the other three.

**Ordering rule to implement:** subscribers connect and their subscriptions are
acknowledged *before* any publisher starts. Otherwise the first messages are
delivered to nobody and the receive count is short through no fault of the
broker.

- [ ] **Step 1: Write the failing test**

Add to `mod tests` in `src/bench/mod.rs`:

```rust
    /// End to end through the real code path, against the broker in the
    /// sibling repo when it is available. Ignored by default because it needs
    /// that broker binary built and a free port.
    ///
    /// Run with:
    ///   cargo build --manifest-path ../pulsemq/Cargo.toml --bin pulsemq
    ///   cargo test bench::tests::a_small_run_publishes_and_receives -- --ignored
    #[tokio::test(flavor = "multi_thread")]
    #[ignore]
    async fn a_small_run_publishes_and_receives() {
        use std::process::{Command as ProcessCommand, Stdio};

        let broker_bin = "../pulsemq/target/debug/pulsemq";
        let mut broker = ProcessCommand::new(broker_bin)
            .args([
                "--listen-addr",
                "127.0.0.1:18841",
                "--admin-addr",
                "127.0.0.1:19041",
                "--db-path",
                "/tmp/pulsemq-bench-test.db",
                "--sys-interval",
                "0",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("broker binary built; see the doc comment");
        tokio::time::sleep(Duration::from_millis(500)).await;

        let args = args_from(&[
            "pulsemq-cli",
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
        let _ = broker.kill();
        result.expect("the run completes");
    }
```

Also add a unit test for the failure aggregation, which does not need a broker:

```rust
    #[test]
    fn task_failures_are_collected_with_their_role_and_index() {
        let failures = collect_failures(
            vec![Ok(()), Err(crate::error::Error::Usage("boom".into()))],
            "publisher",
        );
        assert_eq!(failures, vec!["publisher 1: boom".to_string()]);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test bench::tests::task_failures`
Expected: FAIL — `cannot find function 'collect_failures' in this scope`.

- [ ] **Step 3: Write minimal implementation**

Add to `src/bench/mod.rs`:

```rust
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::watch;

use crate::bench::stats::{Counters, Report, ReportConfig};

/// Turn per-task results into the report's failure list, labelled so a reader
/// knows which connection died.
fn collect_failures(results: Vec<Result<()>>, role: &str) -> Vec<String> {
    results
        .into_iter()
        .enumerate()
        .filter_map(|(index, result)| result.err().map(|e| format!("{role} {index}: {e}")))
        .collect()
}

/// Run the benchmark and print the report.
pub async fn run(args: BenchArgs) -> Result<()> {
    let config = Arc::new(Config::from_args(&args)?);
    let counters = Arc::new(Counters::default());
    let baseline = Instant::now();
    let warmup_until = baseline + config.warmup;
    let (stop_tx, stop_rx) = watch::channel(false);

    // Subscribers start first and their SUBACKs are awaited inside their task,
    // so give them a moment to establish before publishing begins; otherwise
    // the first messages are delivered to nobody.
    let mut subscriber_handles = Vec::new();
    for index in 0..config.subscribers {
        subscriber_handles.push(tokio::spawn(subscriber::run(
            index as u32,
            config.clone(),
            args.conn.clone(),
            counters.clone(),
            baseline,
            warmup_until,
            stop_rx.clone(),
        )));
    }
    if config.subscribers > 0 {
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let started = Instant::now();
    let mut publisher_handles = Vec::new();
    for index in 0..config.publishers {
        publisher_handles.push(tokio::spawn(publisher::run(
            index as u32,
            config.clone(),
            args.conn.clone(),
            counters.clone(),
            baseline,
            warmup_until,
            stop_rx.clone(),
        )));
    }

    // Stop on Ctrl-C, or when the run's own deadline expires. Either way the
    // report still prints for whatever completed.
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

    let mut ack_samples = stats::Samples::new();
    let mut publisher_results = Vec::new();
    for handle in publisher_handles {
        match handle.await {
            Ok(Ok(samples)) => {
                ack_samples.merge(samples);
                publisher_results.push(Ok(()));
            }
            Ok(Err(e)) => publisher_results.push(Err(e)),
            Err(e) => publisher_results.push(Err(Error::Usage(format!("task panicked: {e}")))),
        }
    }
    let elapsed = started.elapsed();

    // Publishing is done; let deliveries still in flight arrive before the
    // subscribers are told to stop. Those messages are late, not lost.
    tokio::time::sleep(config.drain).await;
    let _ = stop_tx.send(true);
    deadline_stop.abort();

    let mut e2e_samples = stats::Samples::new();
    let mut subscriber_results = Vec::new();
    for handle in subscriber_handles {
        match handle.await {
            Ok(Ok(samples)) => {
                e2e_samples.merge(samples);
                subscriber_results.push(Ok(()));
            }
            Ok(Err(e)) => subscriber_results.push(Err(e)),
            Err(e) => subscriber_results.push(Err(Error::Usage(format!("task panicked: {e}")))),
        }
    }

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
```

Rewrite `src/main.rs` so the runtime matches the workload:

```rust
use std::process::ExitCode;

use clap::Parser;
use pulsemq_cli::cli::{Cli, Command};
use pulsemq_cli::error::Result;
use pulsemq_cli::{bench, publish, request, subscribe};

fn main() -> ExitCode {
    let cli = Cli::parse();

    // The three simple commands do one thing at a time and start faster on a
    // single-threaded runtime. `bench` is a load generator: pinning it to one
    // core would measure the load generator rather than the broker.
    let result = match cli.command {
        Command::Bench(args) => multi_thread_runtime().block_on(bench::run(args)),
        Command::Pub(args) => current_thread_runtime().block_on(publish::run(args)),
        Command::Sub(args) => current_thread_runtime().block_on(subscribe::run(args)),
        Command::Request(args) => current_thread_runtime().block_on(request::run(args)),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("pulsemq-cli: {e}");
            ExitCode::FAILURE
        }
    }
}

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

fn multi_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime")
}

// Silences an unused-import warning if `Result` is not otherwise referenced.
const _: fn() -> Option<Result<()>> = || None;
```

If that trailing `const _` line is not needed to compile cleanly, delete it and
drop the `Result` import — do not leave a workaround for a warning that does
not exist.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test`
Expected: PASS for everything not marked `#[ignore]`.
Then the live run, which is the real check:

```bash
cargo build --manifest-path ../pulsemq/Cargo.toml --bin pulsemq
../pulsemq/target/debug/pulsemq --listen-addr 127.0.0.1:18830 \
    --admin-addr 127.0.0.1:19001 --db-path /tmp/bench.db --sys-interval 0 &
cargo run --release -- bench -b 127.0.0.1 -p 18830 \
    --publishers 4 --subscribers 2 --count 20000 --qos 1 --payload-size 128
cargo run --release -- bench -b 127.0.0.1 -p 18830 \
    --publishers 1 --subscribers 1 --duration 5 --rate 500 --qos 1 --json
```
Expected: both runs print a report; the counted run publishes exactly 20,000;
the rated run publishes close to 2,500 (500/s for 5 s); `--json` parses.
Also run the ignored integration test:
`cargo test bench::tests::a_small_run_publishes_and_receives -- --ignored`
Then: `cargo fmt --all && cargo clippy --all-targets --all-features -- -D warnings`

- [ ] **Step 5: Commit**

```bash
git add src/bench/mod.rs src/main.rs
git commit -m "$(cat <<'EOF'
feat: wire up the bench run, drain and shutdown

Subscribers establish before publishers start, deliveries get a drain
window after the last publish, and Ctrl-C prints the report for what
completed instead of exiting silently.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: Documentation

**Files:**
- Modify: `README.md`
- Modify: `CLAUDE.md`
- Modify: `TODO.md`

- [ ] **Step 1: Update the README**

In the "Use" section, after the `request` example, add:

````markdown
```bash
# measure a broker: 4 publishers, 2 subscribers, 20k messages at QoS 1
pulsemq-cli bench --publishers 4 --subscribers 2 --count 20000 --qos 1

# hold 500 messages/second for 30 seconds and emit a machine-readable report
pulsemq-cli bench --duration 30 --rate 500 --subscribers 1 --json
```
````

In "Status", move load generation from "Not yet" into "Working":

```markdown
Working: TCP, all three protocol versions, QoS 0/1/2 in both directions,
retained messages, username/password authentication, and a `bench` mode that
reports throughput plus exact latency percentiles.

Not yet: TLS and mutual TLS, WebSocket transport, will messages, v5 user
properties, and payloads from a file or stdin. A security audit of credential
handling and of the untrusted-input path is the first open item — see `TODO.md`.
```

- [ ] **Step 2: Update CLAUDE.md**

Add `bench` to the subcommand list in "Project":

```markdown
- `bench` — drive publisher and subscriber load and report throughput and
  latency percentiles, v5.0 / v3.1.1 / v3.1
```

Add the new files to the "Architecture" block:

```
src/bench/mod.rs         Config, Stop, run(): spawn, join, report
src/bench/stats.rs       Samples, Summary, Counters, Report; percentiles, table, JSON
src/bench/payload.rs     the 16-byte measurement header
src/bench/schedule.rs    absolute-deadline rate pacing
src/bench/publisher.rs   one publisher: write side, ack side, in-flight window
src/bench/subscriber.rs  one subscriber: subscribe, receive, ack, time
```

Add a paragraph after the `recv_keepalive` note:

```markdown
`bench` does not use `Client`. It calls `client::handshake` on a `TcpStream`,
then splits the socket so a publisher can write and read acknowledgements at
once — the concurrency the simple commands deliberately avoid. Keep that
division: `pub`, `sub` and `request` stay sequential and easy to read, and the
concurrent machinery stays in `src/bench/`.

Runtime choice is per subcommand in `main.rs`: current-thread for the three
simple commands, multi-threaded for `bench`. A load generator on one core
measures the load generator.
```

Update the planned-work line:

```markdown
Planned work is in [`TODO.md`](TODO.md) — item 1 is a **security audit**
(credential handling and the untrusted-input path). It is the next item.
```

- [ ] **Step 3: Update TODO.md**

Delete the "## 2. Performance testing mode" section and renumber the remaining
items so the list stays 1..N with the security audit still first. Add, at the
end of the file:

```markdown
## Done

- **Performance testing mode** — `bench` subcommand: N publishers and
  subscribers, `--count` or `--duration` with an optional `--rate`, an in-flight
  window capped by the broker's Receive Maximum, exact latency percentiles for
  acknowledgement and end-to-end, table or `--json` output. Design spec:
  `docs/superpowers/specs/2026-08-17-bench-mode-design.md`.
```

If Task 7's QoS 2 fallback was taken, add the follow-up item to the open list:

```markdown
## N. QoS 2 in bench mode

`bench` accepts QoS 0 and 1 only. QoS 2 needs the PUBREL written from the send
side while the ack side owns the read half; the plan for that is a small mpsc
of packet identifiers awaiting PUBREL, drained between publishes.
```

- [ ] **Step 4: Verify**

Run: `grep -rni "mosquitto" . --exclude-dir=target --exclude-dir=.git`
Expected: no matches.
Run: `cargo test && cargo fmt --all -- --check && cargo clippy --all-targets --all-features -- -D warnings`
Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add README.md CLAUDE.md TODO.md
git commit -m "$(cat <<'EOF'
docs: document bench mode and retire TODO item 2

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
EOF
)"
```

---

## Self-Review

**Spec coverage.** Every section of the spec maps to a task: command surface →
Task 4; topology → Task 4 (`topic_for`, `subscribe_filter`); architecture and
the `client.rs` / `main.rs` / `Cargo.toml` changes → Tasks 5, 7, 9; payload
format → Task 2; publisher mechanics → Tasks 3 and 7; subscriber mechanics →
Task 8; the report, counters, percentiles and both output formats → Tasks 1
and 6; errors, drain, Ctrl-C and exit status → Task 9; testing → the test steps
throughout plus the ignored integration test in Task 9; documentation → Task 10.

**Two deviations from the spec, both deliberate:**

1. The spec has the handshake running over split halves. Task 5 instead hands
   shake on the whole `TcpStream` and splits afterwards, which is simpler and
   avoids framing a packet across two half-streams. Same outcome, less
   machinery.
2. The spec lists four bench modules; this plan adds `payload.rs` and
   `schedule.rs` so both pure pieces are unit-testable without a broker.

**Known risk, flagged in Task 7:** the QoS 2 publisher path needs PUBREL written
from the side that owns the write half. The task carries an explicit fallback —
restrict `bench` to QoS 0 and 1, document it, and file the follow-up — because a
subtly wrong QoS 2 path would corrupt the very numbers this feature exists to
produce.
