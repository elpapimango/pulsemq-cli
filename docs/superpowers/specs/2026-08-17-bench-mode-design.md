# pulsemq-cli `bench` — broker performance testing mode

Date: 2026-08-17
Status: approved, not implemented
Source: `TODO.md` item 2

## Purpose

Measure an MQTT broker with the same tool used to talk to it. One `bench`
subcommand runs publishers and subscribers together in one process, holds a
chosen offered load, and reports throughput plus latency distributions.

Two questions the run must answer:

1. How many messages per second does the broker sustain at a given QoS and
   payload size?
2. What does the latency tail look like — publish to acknowledgement, and
   publish to subscriber receipt?

Success: a run against a local broker produces stable numbers across repeats,
the report distinguishes a broker that is slow from one that is refusing
messages, and `--json` output from two runs can be diffed.

## Command surface

```
pulsemq-cli bench [connection options]
  --publishers N        default 1
  --subscribers N       default 0
  --topic-prefix S      default "bench"
  --qos 0|1|2           default 0
  --payload-size BYTES  default 64, minimum 16 for end-to-end latency
  --count N             total messages across all publishers
  --duration SECS       alternative stop condition
  --rate MSGS_PER_SEC   offered load; unthrottled when omitted
  --inflight N          default 100, capped by the broker's Receive Maximum
  --warmup SECS         default 0
  --drain SECS          default 2
  --json                machine-readable report instead of the table
```

The existing `ConnectionArgs` is flattened in as it is for the other
subcommands, so `--broker`, `--port`, `--protocol`, `--user`, `--password`,
`--password-file`, `--keepalive` all work unchanged. `--client-id` becomes a
prefix here: publisher *k* connects as `<id>-pub-k`, subscriber *k* as
`<id>-sub-k`, because every task needs its own session.

Configuration resolution rules, all decided in `cli.rs` or `bench::Config`:

- `--count` and `--duration` may both be given; `--count` wins and `--duration`
  becomes an upper bound on the run.
- Neither given: `--count` defaults to 10,000.
- `--payload-size` below 16 is accepted but disables end-to-end latency; the
  report states that rather than printing zeros.
- `--inflight` is capped by the Receive Maximum the broker sends in CONNACK.
- `--subscribers 0` is valid and yields a publish-only run.

## Topology

Publisher *k* publishes to `<topic-prefix>/k`. Every subscriber subscribes to
`<topic-prefix>/#`. This exercises routing across many topics and fan-out to
every subscriber, which is the shape that stresses a broker's matching and
delivery paths at once.

## Architecture

```
src/bench/mod.rs         Config, run(): spawn tasks, join, hand results to stats
src/bench/publisher.rs   one publisher connection: write side, ack side, window
src/bench/subscriber.rs  one subscriber connection: subscribe, receive, ack, time
src/bench/stats.rs       Samples, Counters, Report; percentiles; table and JSON
```

Each publisher and each subscriber is one tokio task owning one connection with
the `TcpStream` split into halves. Counters are atomics shared across tasks;
latency samples stay in per-task `Vec<u64>` and merge when the tasks join.
No collector task and no channel on the measurement path.

### Changes to existing code

- **`client.rs`** — the CONNECT/CONNACK handshake becomes a free function
  generic over `AsyncRead + AsyncWrite`, returning the negotiated parameters,
  including the broker's Receive Maximum. `Client` keeps its current shape and
  calls it. `pub`, `sub` and `request` behaviour does not change.
- **`main.rs`** — stops being `#[tokio::main(flavor = "current_thread")]` and
  builds the runtime itself: current-thread for the three simple commands,
  multi-threaded for `bench`. A load generator pinned to one core measures the
  load generator.
- **`Cargo.toml`** — tokio gains `rt-multi-thread`, `sync` and `signal` (the
  last for the Ctrl-C path); `serde_json` is added for `--json`. serde_json is
  already in the compiled tree through the broker dependency, so it costs no new
  build time.

### Payload format

The first 16 bytes carry, big-endian:

| offset | size | field                                        |
|--------|------|----------------------------------------------|
| 0      | 8    | nanoseconds since the run's baseline `Instant` |
| 8      | 4    | publisher index                              |
| 12     | 4    | sequence number within that publisher        |

The rest is filler to `--payload-size`. One process means one baseline
`Instant`, so end-to-end latency needs no clock synchronisation and no skew
correction.

### Publisher

The write side sends on a schedule derived from `--rate / --publishers`, using
`sleep_until` against absolute deadlines so a late tick does not shift every
later one. Unthrottled runs skip the sleep entirely.

At QoS 1 and 2 each send records the packet identifier and its send time in an
in-flight map, and the write side blocks once the map reaches
`min(--inflight, broker Receive Maximum)`. The read side matches PUBACK, or
drives PUBREC → PUBREL → PUBCOMP, records the ack latency, and frees the slot.
QoS 0 has no acknowledgement, so such a run reports throughput and end-to-end
latency only.

### Subscriber

Subscribes to `<topic-prefix>/#` at `--qos`, acknowledges inbound QoS 1 and 2,
and for each message reads the header, subtracts from the current elapsed time,
and stores the end-to-end sample.

## The report

Counted per run:

- messages published, acknowledged, and received across all subscribers
- payload bytes sent and received
- publish errors
- messages refused by the broker, tallied per Reason Code, so a `0x87` at
  message 40,000 is visible rather than silent
- tasks that died, and why

Throughput derives from the measured wall time, never the requested duration: a
run cut short by a disconnect must not report the rate it was aiming for.

Two latency series, kept separate because they answer different questions:

- **ack latency** — publish to PUBACK/PUBCOMP, QoS 1 and 2 only
- **end-to-end latency** — publish to subscriber receipt

Each reports count, min, p50, p95, p99, max and mean. Percentiles come from the
merged sample vector, sorted once at the end, nearest-rank — exact, not a bucket
estimate. Memory is 8 bytes per sample, bounded by `--count`.

`--warmup` discards samples recorded before its cutoff, so connection setup and
the broker's cold start stay out of the tail.

Human output is an aligned table: a configuration line, a counters block, then a
row per percentile for each latency series. `--json` emits one object carrying
the same fields plus the resolved configuration, so two runs diff cleanly.

## Errors and shutdown

A task that fails records its error and exits; the run continues with the rest
and the report names how many died and why. The run ends when every publisher
has sent its quota or the duration expires, after which subscribers get
`--drain` seconds to collect deliveries still in flight — those messages are
late, not lost.

Ctrl-C stops the run and prints the report for what completed, rather than
exiting silently.

Exit status is non-zero if any task failed or any message was refused.

## Testing

Unit tests cover the parts that are pure:

- percentile math against a known vector, including empty and single-sample
- payload header round trip, and rejection of a short payload
- the rate scheduler's deadline sequence
- configuration resolution: `--count` beating `--duration`, `--inflight` capped
  by Receive Maximum, payload under 16 bytes disabling end-to-end latency

The full path needs a live broker. That belongs with `TODO.md` item 8
(end-to-end tests against `../pulsemq`) rather than being faked in a unit test;
until then it is verified by hand the way the earlier smoke tests were.

## Out of scope

Live progress output during a run, multi-host coordination, TLS (`TODO.md`
item 3 covers the transport, and `bench` inherits it), and histogram-based
approximate percentiles for unbounded runs.
