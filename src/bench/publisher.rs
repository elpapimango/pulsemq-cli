//! One publishing connection: a write side that paces sends and holds the
//! in-flight window, and a read side that matches acknowledgements.
//!
//! QoS 2's PUBREC -> PUBREL -> PUBCOMP exchange (3.4 - 3.7) splits across
//! both halves: the ack side (which owns the read half) is what sees a
//! PUBREC, but only the write side may write to the socket. It hands the
//! packet identifier back over `pubrel_tx`/`pubrel_rx`, and the write side
//! drains that channel between publishes, writing one PUBREL per id.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::mqtt::framing::{read_packet, write_packet, ReadOutcome};
use crate::mqtt::packet::{Packet, PubAck, Publish};
use crate::mqtt::types::{ProtocolVersion, QoS, ReasonCode};
use tokio::sync::{mpsc, watch, Mutex, Semaphore};

use crate::bench::payload::{self, Header};
use crate::bench::schedule::Schedule;
use crate::bench::stats::{Counters, Samples};
use crate::bench::Config;
use crate::cli::ConnectionArgs;
use crate::client::handshake;
use crate::error::{Error, Result};
use crate::transport;

/// Send times for the packets this publisher is waiting on.
type InFlight = Arc<Mutex<HashMap<u16, Instant>>>;

/// Complete an in-flight id on its terminal acknowledgement — PUBACK for
/// QoS 1, PUBCOMP (or an erroring PUBREC, which ends the flow early per
/// 4.3.3) for QoS 2. Both mean the same thing to a publisher: the window
/// slot is free, and unless the broker refused it, a latency sample.
#[allow(clippy::too_many_arguments)]
async fn complete_ack(
    id: u16,
    reason_code: ReasonCode,
    in_flight: &InFlight,
    permits: &Semaphore,
    counters: &Counters,
    samples: &mut Samples,
    warmup_until: Instant,
) {
    if reason_code.is_error() {
        counters.record_refusal(reason_code);
    }
    let sent = in_flight.lock().await.remove(&id);
    if let Some(sent) = sent {
        // The window slot is free either way; only a successful ack
        // contributes to the ack count and its latency distribution —
        // mixing in a refused message's (typically much faster) round trip
        // would skew the ack-latency percentiles low exactly when the run
        // is going badly.
        permits.add_permits(1);
        if !reason_code.is_error() {
            counters.acknowledged.fetch_add(1, Ordering::Relaxed);
            if sent >= warmup_until {
                samples.record(sent.elapsed().as_nanos() as u64);
            }
        }
    }
}

/// Write a PUBREL for every QoS 2 id the ack side has handed back since the
/// last drain.
async fn drain_pubrels<W: tokio::io::AsyncWrite + Unpin>(
    rx: &mut mpsc::UnboundedReceiver<u16>,
    writer: &mut W,
    version: ProtocolVersion,
) -> Result<()> {
    while let Ok(id) = rx.try_recv() {
        let pubrel = PubAck::new(id, ReasonCode::Success);
        write_packet(writer, &Packet::Pubrel(pubrel), version).await?;
    }
    Ok(())
}

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

    let mut stream = transport::connect(&conn).await?;
    let negotiated = handshake(&mut stream, &conn, &client_id).await?;
    let version = negotiated.version;
    // Same ceiling the handshake advertised, so every read here agrees with
    // what the broker was told.
    let max_packet_size = conn.max_packet_size().map_err(Error::Usage)?;
    // Packet Identifiers are 16-bit, 1..=65535 (2.2.1), so a window wider
    // than that would let two outstanding messages collide on the same id:
    // `in_flight.insert` would silently overwrite the older entry, losing
    // its send time and leaving its permit forgotten forever.
    let window = crate::bench::effective_inflight(config.inflight, negotiated.receive_maximum)
        .min(u16::MAX as usize);

    let (mut reader, mut writer) = stream.into_split();
    let permits = Arc::new(Semaphore::new(window));
    let in_flight: InFlight = Arc::new(Mutex::new(HashMap::new()));
    // QoS 2 PUBRECs the ack side has seen but this task (which owns the
    // write half) has not yet turned into a PUBREL.
    let (pubrel_tx, mut pubrel_rx) = mpsc::unbounded_channel::<u16>();

    // The read side lives as long as the write side plus its outstanding
    // acknowledgements; it ends when the socket closes or the write side
    // signals that it is done and the window has drained.
    let ack_task = {
        let permits = permits.clone();
        let in_flight = in_flight.clone();
        let counters = counters.clone();
        tokio::spawn(async move {
            let mut samples = Samples::new();
            // Why the read error is carried out rather than dropped: when a
            // broker kills the connection mid-run, the only thing the write
            // side observes is its window closing. Reporting that alone names
            // the symptom furthest from the cause — "in-flight window closed"
            // is what the operator sees when the broker actually said
            // "malformed packet". The cause lives here, so it leaves here.
            let mut read_error: Option<Error> = None;
            loop {
                let packet = match read_packet(&mut reader, max_packet_size, version).await {
                    Ok(ReadOutcome::Packet(packet, _)) => packet,
                    // The broker closed its half. Expected once this publisher
                    // has sent DISCONNECT (3.14.4); premature closure shows up
                    // as a short acknowledged count rather than an error,
                    // because a clean close is not itself a protocol failure.
                    Ok(ReadOutcome::Eof) => break,
                    Err(e) => {
                        read_error = Some(e.into());
                        break;
                    }
                };
                match packet {
                    // QoS 1's terminal acknowledgement.
                    Packet::Puback(ack) => {
                        complete_ack(
                            ack.packet_id,
                            ack.reason_code,
                            &in_flight,
                            &permits,
                            &counters,
                            &mut samples,
                            warmup_until,
                        )
                        .await;
                    }
                    Packet::Pubrec(rec) => {
                        if rec.reason_code.is_error() {
                            // 4.3.3: a Reason Code >= 0x80 on PUBREC means the
                            // QoS 2 flow is already complete — no PUBREL
                            // follows, and none should be sent.
                            complete_ack(
                                rec.packet_id,
                                rec.reason_code,
                                &in_flight,
                                &permits,
                                &counters,
                                &mut samples,
                                warmup_until,
                            )
                            .await;
                        } else {
                            // Only the write side may write to the socket
                            // (module doc comment); hand the id back so it
                            // can send the PUBREL.
                            let _ = pubrel_tx.send(rec.packet_id);
                        }
                    }
                    // QoS 2's terminal acknowledgement.
                    Packet::Pubcomp(comp) => {
                        complete_ack(
                            comp.packet_id,
                            comp.reason_code,
                            &in_flight,
                            &permits,
                            &counters,
                            &mut samples,
                            warmup_until,
                        )
                        .await;
                    }
                    _ => {}
                }
            }
            // No further acknowledgement will ever arrive once the read
            // side has ended (socket closed, or a protocol error): close the
            // semaphore so a write side blocked in `acquire_owned` on a full
            // window gets an error back instead of hanging forever.
            permits.close();
            (samples, read_error)
        })
    };

    let schedule = Schedule::new(Instant::now(), config.per_publisher_rate());
    let quota = config.quota_for(index as usize);
    let mut sent: u64 = 0;
    let mut next_packet_id: u16 = 0;
    // Set when the window closes under us. The ack task holds the reason it
    // closed, so this only records *that* it happened; the tail below joins
    // that task and reports what it saw.
    let mut window_closed = false;

    loop {
        // A QoS 2 PUBREL owed to the broker for a PUBREC the ack side saw
        // since the last iteration — only this side may write to the
        // socket, so this is where it actually gets sent.
        drain_pubrels(&mut pubrel_rx, &mut writer, version).await?;
        if quota.is_some_and(|q| sent >= q) {
            break;
        }
        if *stop.borrow_and_update() {
            break;
        }
        if let Some(deadline) = schedule.deadline(sent) {
            tokio::select! {
                _ = tokio::time::sleep_until(deadline.into()) => {}
                // `wait_for` only resolves once the predicate holds (a real
                // stop request) or the sender is dropped (`Err`, which can
                // never be followed by a stop request either) — unlike
                // `changed()`, it does not wake spuriously for a `send(false)`
                // and silently disable pacing for that iteration.
                _ = stop.wait_for(|v| *v) => break,
            }
        }

        let permit = if config.qos == QoS::AtMostOnce {
            None
        } else {
            let acquire = permits.clone().acquire_owned();
            tokio::select! {
                res = acquire => match res {
                    Ok(permit) => Some(permit),
                    // The ack side closed the semaphore, which it only does
                    // once no further acknowledgement can arrive. Stop sending
                    // and fall through to the tail, which reports the reason
                    // rather than this symptom.
                    Err(_) => {
                        window_closed = true;
                        break;
                    }
                },
                // A full window blocks `acquire` indefinitely if nothing
                // ever acknowledges; without this arm a stop signal (or
                // Ctrl-C via Task 9's duration deadline) could not break in
                // until the ack side closed the semaphore on its own.
                _ = stop.wait_for(|v| *v) => break,
            }
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
                // The read side releases this permit when the acknowledgement
                // lands (`permits.add_permits(1)` above); forgetting it here
                // hands that accounting to the ack side instead of returning
                // the slot when `permit` goes out of scope.
                if let Some(permit) = permit {
                    permit.forget();
                }
                sent += 1;
            }
            Err(e) => {
                counters.publish_errors.fetch_add(1, Ordering::Relaxed);
                if let Some(id) = packet_id {
                    in_flight.lock().await.remove(&id);
                }
                // `permit`, left unforgotten, releases its slot back to the
                // semaphore when it drops here.
                return Err(e.into());
            }
        }
    }

    // Give outstanding acknowledgements a moment to land, then close the
    // socket so the read side returns.
    //
    // Skipped entirely when the window closed under us: that only happens once
    // the ack task has ended, so nothing is left to do the acknowledging and
    // every one of these five seconds would be spent waiting for a reply that
    // cannot come.
    if !window_closed {
        let drain_deadline = Instant::now() + Duration::from_secs(5);
        while !in_flight.lock().await.is_empty() && Instant::now() < drain_deadline {
            // A PUBREC can still land while this loop waits; without this,
            // a QoS 2 run's trailing messages would never see their PUBREL
            // and the in-flight map would never empty before the deadline.
            let _ = drain_pubrels(&mut pubrel_rx, &mut writer, version).await;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = drain_pubrels(&mut pubrel_rx, &mut writer, version).await;
    }
    let _ = write_packet(
        &mut writer,
        &Packet::Disconnect(crate::mqtt::packet::Disconnect::new(ReasonCode::Success)),
        version,
    )
    .await;
    drop(writer);

    // A compliant broker closes its half of the socket once it has read our
    // DISCONNECT (3.14.4), which is what lets the read side above return.
    // A broker that does not is not this crate's problem to hang on: cap the
    // wait and abort rather than let `run` block forever. This is a real
    // run failure, not a quiet "nothing to report": returning `Err` here
    // (rather than `Ok(Samples::new())`) is what lands this publisher in
    // Task 9's `task_failures` and makes `Report::success()` false, so a
    // wedged broker shows up in the report instead of looking like a
    // publisher that simply had no samples. `counters.published` and
    // `counters.bytes_sent` still reflect everything this publisher sent —
    // they live on the shared `Counters`, not in the `Samples` this
    // function returns, so they are not lost on this path; only the
    // ack-latency samples the aborted ack task was holding are.
    let ack_task_abort = ack_task.abort_handle();
    match tokio::time::timeout(config.drain.max(Duration::from_secs(5)), ack_task).await {
        Ok(joined) => {
            let (samples, read_error) = joined
                .map_err(|e| Error::Usage(format!("publisher {index} ack task failed: {e}")))?;
            // A read error outranks everything else this publisher could
            // report: it is why the acknowledgements stopped, and the window
            // closing was only the consequence.
            match read_error {
                Some(e) => Err(Error::Usage(format!(
                    "publisher {index}: the broker's reply stream failed: {e}"
                ))),
                None if window_closed => Err(Error::Usage(format!(
                    "publisher {index}: the broker closed the connection after \
                     {sent} message(s), before acknowledging the rest"
                ))),
                None => Ok(samples),
            }
        }
        Err(_) => {
            ack_task_abort.abort();
            Err(Error::Usage(format!(
                "publisher {index}: acknowledgement task did not finish within the drain \
                 window; the broker likely never closed its half of the socket after \
                 DISCONNECT (3.14.4)"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bench::stats::Counters;
    use crate::mqtt::framing::{read_packet, write_packet, ReadOutcome};
    use crate::mqtt::packet::{Connack, PubAck};
    use crate::mqtt::types::{ProtocolVersion, ReasonCode};
    use clap::Parser;
    use std::sync::atomic::AtomicU64;
    use tokio::net::TcpListener;

    /// Bound on how long any test may wait on a task that a bug elsewhere in
    /// this file could hang forever: a wedged publisher or ack task must
    /// fail the test, not the whole suite.
    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    /// A broker stub: accepts one connection, CONNACKs, then PUBACKs every
    /// PUBLISH it sees. Returns how many PUBLISH packets arrived.
    async fn ack_everything(listener: TcpListener) -> u64 {
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

    /// A broker stub that runs the full QoS 2 handshake: PUBREC for every
    /// PUBLISH, then PUBCOMP for every PUBREL it gets back. Returns how many
    /// of each it saw, so a test can confirm the PUBREL side actually ran.
    async fn qos2_handshake(listener: TcpListener) -> (u64, u64) {
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

        let (mut publishes, mut pubrels) = (0u64, 0u64);
        loop {
            match read_packet(&mut stream, 268_435_455, version).await {
                Ok(ReadOutcome::Packet(Packet::Publish(p), _)) => {
                    publishes += 1;
                    let id = p.packet_id.expect("QoS 2 PUBLISH carries a packet id");
                    let rec = PubAck::new(id, ReasonCode::Success);
                    write_packet(&mut stream, &Packet::Pubrec(rec), version)
                        .await
                        .expect("PUBREC");
                }
                Ok(ReadOutcome::Packet(Packet::Pubrel(rel), _)) => {
                    pubrels += 1;
                    let comp = PubAck::new(rel.packet_id, ReasonCode::Success);
                    write_packet(&mut stream, &Packet::Pubcomp(comp), version)
                        .await
                        .expect("PUBCOMP");
                }
                Ok(ReadOutcome::Packet(_, _)) => continue,
                _ => return (publishes, pubrels),
            }
        }
    }

    /// A broker stub that CONNACKs but never acknowledges a PUBLISH, so the
    /// in-flight window is the only thing that can stop a publisher from
    /// racing ahead of it. Tallies how many PUBLISHes arrived in `received`.
    async fn withhold_acks(listener: TcpListener, received: Arc<AtomicU64>) {
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

        loop {
            match read_packet(&mut stream, 268_435_455, version).await {
                Ok(ReadOutcome::Packet(Packet::Publish(_), _)) => {
                    received.fetch_add(1, Ordering::Relaxed);
                }
                Ok(ReadOutcome::Packet(_, _)) => continue,
                _ => return,
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
        let cli =
            crate::cli::Cli::parse_from(["pulsemq-cli", "bench", "-b", "127.0.0.1", "-p", &port]);
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

        let samples = tokio::time::timeout(
            TEST_TIMEOUT,
            run(
                0,
                config,
                connection_args(port),
                counters.clone(),
                baseline,
                baseline,
                stop,
            ),
        )
        .await
        .expect("publisher does not hang")
        .expect("publisher completes");

        assert_eq!(counters.published.load(Ordering::Relaxed), 10);
        assert_eq!(counters.acknowledged.load(Ordering::Relaxed), 10);
        assert_eq!(samples.len(), 10);
        assert!(
            tokio::time::timeout(TEST_TIMEOUT, broker)
                .await
                .expect("broker task does not hang")
                .expect("broker task")
                >= 10
        );
    }

    /// Regression: QoS 2's ack sample must be recorded once, on PUBCOMP —
    /// not on PUBREC, and not twice. The broker stub only completes the
    /// handshake when it actually receives a PUBREL, so this also proves
    /// the write side (which owns the socket) sends the PUBRELs the ack
    /// side hands it over the channel, rather than the QoS 2 arm being a
    /// no-op that happens not to crash.
    #[tokio::test(flavor = "multi_thread")]
    async fn qos_2_records_one_sample_per_message_on_pubcomp() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let broker = tokio::spawn(qos2_handshake(listener));

        let config = Arc::new(config_for(&[
            "pulsemq-cli",
            "bench",
            "--count",
            "10",
            "--qos",
            "2",
        ]));
        let counters = Arc::new(Counters::default());
        let baseline = Instant::now();
        let (_tx, stop) = watch::channel(false);

        let samples = tokio::time::timeout(
            TEST_TIMEOUT,
            run(
                0,
                config,
                connection_args(port),
                counters.clone(),
                baseline,
                baseline,
                stop,
            ),
        )
        .await
        .expect("publisher does not hang")
        .expect("publisher completes");

        assert_eq!(counters.published.load(Ordering::Relaxed), 10);
        assert_eq!(counters.acknowledged.load(Ordering::Relaxed), 10);
        assert_eq!(samples.len(), 10, "one sample per message, not per PUBREC");

        let (publishes, pubrels) = tokio::time::timeout(TEST_TIMEOUT, broker)
            .await
            .expect("broker task does not hang")
            .expect("broker task");
        assert_eq!(publishes, 10);
        assert_eq!(pubrels, 10, "every PUBREC must be answered with a PUBREL");
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

        let samples = tokio::time::timeout(TEST_TIMEOUT, handle)
            .await
            .expect("publisher stops promptly")
            .expect("task joins")
            .expect("publisher completes");

        assert!(counters.published.load(Ordering::Relaxed) > 0);
        // QoS 0 by default: no acknowledgements, so no ack samples.
        assert_eq!(samples.len(), 0);
        let _ = tokio::time::timeout(TEST_TIMEOUT, broker).await;
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

        let samples = tokio::time::timeout(
            TEST_TIMEOUT,
            run(
                0,
                config,
                connection_args(port),
                counters.clone(),
                baseline,
                warmup_until,
                stop,
            ),
        )
        .await
        .expect("publisher does not hang")
        .expect("publisher completes");

        assert_eq!(counters.published.load(Ordering::Relaxed), 5);
        assert_eq!(samples.len(), 0, "warmup samples are discarded");
        let _ = tokio::time::timeout(TEST_TIMEOUT, broker).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn the_inflight_window_bounds_outstanding_sends() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let received = Arc::new(AtomicU64::new(0));
        let broker = tokio::spawn(withhold_acks(listener, received.clone()));

        // A huge quota and no rate cap: nothing but the in-flight window
        // itself can stop this publisher from racing ahead.
        let config = Arc::new(config_for(&[
            "pulsemq-cli",
            "bench",
            "--count",
            "1000",
            "--qos",
            "1",
            "--inflight",
            "5",
        ]));
        let counters = Arc::new(Counters::default());
        let baseline = Instant::now();
        let (_tx, stop) = watch::channel(false);

        let handle = tokio::spawn(run(
            0,
            config,
            connection_args(port),
            counters.clone(),
            baseline,
            baseline,
            stop,
        ));

        // Give the publisher time to fill the window. Nothing ever
        // acknowledges a message, so the count must plateau at the window
        // size rather than racing ahead of it.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            received.load(Ordering::Relaxed),
            5,
            "the publisher must stop at the in-flight window"
        );
        assert_eq!(counters.published.load(Ordering::Relaxed), 5);

        // Simulate the broker disappearing while the write side is parked
        // waiting for a permit: the ack task's read fails, closes the
        // semaphore, and the blocked acquire must return an error rather
        // than hang forever.
        broker.abort();

        let result = tokio::time::timeout(TEST_TIMEOUT, handle)
            .await
            .expect("publisher does not hang once the broker disappears")
            .expect("task joins");
        assert!(
            result.is_err(),
            "an acquire on a closed window must error, not hang"
        );
        assert_eq!(counters.published.load(Ordering::Relaxed), 5);
        let _ = tokio::time::timeout(TEST_TIMEOUT, broker).await;
    }
}
