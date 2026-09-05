//! One subscribing connection: subscribe, acknowledge inbound messages, and
//! time each one from its measurement header to receipt.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::watch;
use wispmq_protocol::framing::{read_packet, write_packet, ReadOutcome};
use wispmq_protocol::packet::{Packet, PubAck, RetainHandling, Subscribe, TopicFilter};
use wispmq_protocol::types::{QoS, ReasonCode};

use crate::bench::payload;
use crate::bench::stats::{Counters, Samples};
use crate::bench::Config;
use crate::cli::ConnectionArgs;
use crate::client::handshake;
use crate::error::{Error, Result};
use crate::transport;

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

    let mut stream = transport::connect(&conn).await?;
    let negotiated = handshake(&mut stream, &conn, &client_id).await?;
    let version = negotiated.version;
    // Same ceiling the handshake advertised, so every read here agrees with
    // what the broker was told.
    let max_packet_size = conn.max_packet_size().map_err(Error::Usage)?;

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
        match read_packet(&mut stream, max_packet_size, version).await? {
            ReadOutcome::Packet(Packet::Suback(ack), _) => {
                // 3.9.3: any Reason Code >= 0x80 is a failed subscription.
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
            read = read_packet(&mut stream, max_packet_size, version) => read,
            // `wait_for` resolves once the predicate holds (a real stop
            // request) or the sender is dropped (`Err`). Both cases end the
            // run: breaking here (rather than `continue`) matters because a
            // dropped sender makes `wait_for` resolve immediately and
            // forever, and looping back into another `select!` that
            // re-creates the same already-resolved future would spin this
            // arm instead of blocking on the read.
            _ = stop.wait_for(|v| *v) => break,
        };

        match read {
            Ok(ReadOutcome::Packet(Packet::Publish(p), _)) => {
                let received_at = Instant::now();
                counters.received.fetch_add(1, Ordering::Relaxed);
                counters
                    .bytes_received
                    .fetch_add(p.payload.len() as u64, Ordering::Relaxed);

                // A message whose payload carries no header is counted, not
                // timed: `payload::decode` returns `None` for any message
                // this run did not publish with a header. `elapsed_ns` is a
                // `u64` read straight from that payload — the broker's word,
                // or cross-talk from another run sharing this topic prefix —
                // so `checked_add` stands in for `+`: `Instant` addition
                // panics on overflow, and an implausible header must be
                // treated the same as no header rather than crash the task.
                if let Some(header) = payload::decode(&p.payload) {
                    if let Some(sent_at) =
                        baseline.checked_add(Duration::from_nanos(header.elapsed_ns))
                    {
                        if sent_at >= warmup_until && received_at >= sent_at {
                            samples.record(received_at.duration_since(sent_at).as_nanos() as u64);
                        }
                    }
                }

                // Acknowledge per QoS (4.3.2, 4.3.3). QoS 2 completes on the
                // PUBREL the broker sends next, handled by the `Pubrel` arm
                // below.
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
            Ok(ReadOutcome::Eof) => {
                // The broker closed its half of the connection: nothing
                // further will ever arrive on this socket. Reading it again
                // would just report EOF forever, spinning the loop, so wait
                // here for the stop signal instead — the run is not over
                // until Task 9 says so, even if this connection already is.
                let _ = stop.wait_for(|v| *v).await;
                break;
            }
            Err(e) => return Err(e.into()),
        }
    }

    Ok(samples)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bench::payload::{self, Header};
    use crate::bench::stats::Counters;
    use clap::Parser;
    use tokio::net::TcpListener;
    use wispmq_protocol::framing::{read_packet, write_packet, ReadOutcome};
    use wispmq_protocol::packet::{Connack, Publish, SubAck};
    use wispmq_protocol::types::{ProtocolVersion, ReasonCode};

    /// Bound on how long any test may wait on a task that a bug elsewhere in
    /// this file could hang forever: a wedged subscriber or broker stub must
    /// fail the test, not the whole suite.
    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    /// A broker stub that accepts the subscription and then delivers
    /// `deliveries` QoS 0 messages, each carrying a header aged `age`.
    async fn deliver(listener: TcpListener, deliveries: usize, age: Duration, baseline: Instant) {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let version = ProtocolVersion::V5;

        let ReadOutcome::Packet(Packet::Connect(_), _) = read_packet(&mut stream, 65_535, version)
            .await
            .expect("CONNECT")
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
            read_packet(&mut stream, 65_535, version)
                .await
                .expect("SUBSCRIBE")
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

        // `sent_at` below is computed as `Instant::now() - age`; without this
        // wait, the handshake and subscribe round trips above may not yet
        // have consumed `age` of real time since `baseline`, so `sent_at`
        // could land before `baseline` and `duration_since` would saturate
        // to zero (Instant subtraction never goes negative) instead of
        // encoding a real "50 ms ago" header.
        tokio::time::sleep(age).await;

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
                qos: wispmq_protocol::types::QoS::AtMostOnce,
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
            crate::cli::Cli::parse_from(["wispmq-cli", "bench", "-b", "127.0.0.1", "-p", &port]);
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

        let config = Arc::new(config_for(&["wispmq-cli", "bench", "--subscribers", "1"]));
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

        let mut samples = tokio::time::timeout(TEST_TIMEOUT, handle)
            .await
            .expect("subscriber stops promptly")
            .expect("task joins")
            .expect("subscriber completes");

        assert_eq!(counters.received.load(Ordering::Relaxed), 3);
        assert_eq!(samples.len(), 3);
        let summary = samples.summary().expect("three samples");
        // Each message was aged 50 ms before delivery, so every sample is at
        // least that, and well under a second on any machine.
        assert!(
            summary.min_ns >= 40_000_000,
            "min was {} ns",
            summary.min_ns
        );
        assert!(
            summary.max_ns < 1_000_000_000,
            "max was {} ns",
            summary.max_ns
        );
        tokio::time::timeout(TEST_TIMEOUT, broker)
            .await
            .expect("broker task does not hang")
            .expect("broker task");
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
                read_packet(&mut stream, 65_535, version)
                    .await
                    .expect("SUBSCRIBE")
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
                qos: wispmq_protocol::types::QoS::AtMostOnce,
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

        let config = Arc::new(config_for(&["wispmq-cli", "bench", "--subscribers", "1"]));
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
        let samples = tokio::time::timeout(TEST_TIMEOUT, handle)
            .await
            .expect("subscriber stops promptly")
            .expect("task joins")
            .expect("completes");

        assert_eq!(counters.received.load(Ordering::Relaxed), 1);
        assert_eq!(
            samples.len(),
            0,
            "a headerless message is counted, not timed"
        );
        tokio::time::timeout(TEST_TIMEOUT, broker)
            .await
            .expect("broker task does not hang")
            .expect("broker task");
    }

    /// Regression: `elapsed_ns` comes straight from the payload, which a
    /// hostile broker (or cross-talk from another run on the same topic
    /// prefix) fully controls. `u64::MAX` nanoseconds pushed `baseline +
    /// Duration::from_nanos(..)` past what `Instant` can represent and
    /// panicked; the message must instead be counted and left untimed, the
    /// same as a payload with no header at all.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_header_with_an_unrepresentable_elapsed_time_does_not_panic() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let baseline = Instant::now();
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
                read_packet(&mut stream, 65_535, version)
                    .await
                    .expect("SUBSCRIBE")
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
            let body = payload::build(
                64,
                Header {
                    elapsed_ns: u64::MAX,
                    publisher: 0,
                    seq: 0,
                },
            );
            let publish = Publish {
                dup: false,
                qos: wispmq_protocol::types::QoS::AtMostOnce,
                retain: false,
                topic: "bench/0".into(),
                packet_id: None,
                properties: Default::default(),
                payload: body.into(),
            };
            write_packet(&mut stream, &Packet::Publish(publish), version)
                .await
                .expect("PUBLISH");
        });

        let config = Arc::new(config_for(&["wispmq-cli", "bench", "--subscribers", "1"]));
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

        let samples = tokio::time::timeout(TEST_TIMEOUT, handle)
            .await
            .expect("subscriber stops promptly")
            .expect("task joins, i.e. did not panic")
            .expect("subscriber completes");

        assert_eq!(counters.received.load(Ordering::Relaxed), 1);
        assert_eq!(
            samples.len(),
            0,
            "an implausible header is counted, not timed"
        );
        tokio::time::timeout(TEST_TIMEOUT, broker)
            .await
            .expect("broker task does not hang")
            .expect("broker task");
    }

    /// A broker stub that accepts the subscription and then goes silent
    /// without closing the connection, keeping the subscriber parked in
    /// `read_packet` — the scenario the stop signal must interrupt promptly
    /// rather than waiting for a message that will never arrive.
    async fn subscribe_then_go_silent(listener: TcpListener) {
        let (mut stream, _) = listener.accept().await.expect("accept");
        let version = ProtocolVersion::V5;

        let ReadOutcome::Packet(Packet::Connect(_), _) = read_packet(&mut stream, 65_535, version)
            .await
            .expect("CONNECT")
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
            read_packet(&mut stream, 65_535, version)
                .await
                .expect("SUBSCRIBE")
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

        // Stay connected but silent until the test aborts this task: the
        // subscriber has nothing to read and must rely on the stop signal,
        // not on EOF, to end its run.
        std::future::pending::<()>().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_stop_signal_ends_a_run_parked_in_read_packet() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let broker = tokio::spawn(subscribe_then_go_silent(listener));

        let config = Arc::new(config_for(&["wispmq-cli", "bench", "--subscribers", "1"]));
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
        // Give the subscriber time to connect, subscribe, and settle into
        // `read_packet` waiting for a message that will never come.
        tokio::time::sleep(Duration::from_millis(100)).await;
        tx.send(true).expect("stop signal");

        let samples = tokio::time::timeout(TEST_TIMEOUT, handle)
            .await
            .expect("subscriber stops promptly while parked in read_packet")
            .expect("task joins")
            .expect("subscriber completes");

        assert_eq!(samples.len(), 0);
        broker.abort();
    }

    /// Regression guard: if the `Sender` half of the stop channel is ever
    /// dropped without sending `true` (the orchestrator panicking, say),
    /// `watch::Receiver::changed()` returns `Err` immediately and forever.
    /// An unchecked `changed()` arm in the `select!` would spin on that
    /// forever; `run` must still end promptly rather than hang or burn CPU.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_dropped_stop_sender_ends_the_run_promptly_instead_of_spinning() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let broker = tokio::spawn(subscribe_then_go_silent(listener));

        let config = Arc::new(config_for(&["wispmq-cli", "bench", "--subscribers", "1"]));
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
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Drop the sender without ever signalling a stop.
        drop(tx);

        let samples = tokio::time::timeout(TEST_TIMEOUT, handle)
            .await
            .expect("subscriber does not hang or spin once the stop sender is gone")
            .expect("task joins")
            .expect("subscriber completes");

        assert_eq!(samples.len(), 0);
        broker.abort();
    }
}
