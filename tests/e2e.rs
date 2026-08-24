//! End-to-end tests against a real broker.
//!
//! Not a build dependency: `../pulsemq`, if a sibling checkout exists, is
//! discovered and built here, at test run time, via a `Command` — nothing
//! in `cargo build`, `cargo test` (without this file), `fmt` or `clippy`
//! reads it. Every test checks the fixture is available before doing
//! anything else and skips with a printed reason when it isn't, so `cargo
//! test` in a fresh clone with no sibling checkout stays green and fast —
//! matching the manual smoke test CLAUDE.md documents today.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::OnceLock;
use std::time::Duration;

use clap::Parser;
use tokio::net::TcpStream;

use pulsemq_cli::cli::{Cli, Command as Subcommand, ConnectionArgs, PubArgs, RequestArgs, SubArgs};
use pulsemq_cli::client::Client;
use pulsemq_cli::mqtt::packet::Packet;
use pulsemq_cli::mqtt::types::QoS;
use pulsemq_cli::{publish, request, subscribe};

const READY_TIMEOUT: Duration = Duration::from_secs(10);
const ROUND_TRIP_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a subscriber gets to finish its SUBACK before a publish races
/// it — the same margin `bench::SUBSCRIBER_SETTLE` uses for the identical
/// problem, reused here rather than inventing a second fudge factor.
const SUBSCRIBE_SETTLE: Duration = Duration::from_millis(300);

/// `../pulsemq`, built once and cached for every test in this file. `None`
/// means "skip" — no sibling checkout, or it failed to build — logged once,
/// not per test.
fn pulsemq_binary() -> Option<&'static Path> {
    static BINARY: OnceLock<Option<PathBuf>> = OnceLock::new();
    BINARY
        .get_or_init(|| {
            let manifest = Path::new("../pulsemq/Cargo.toml");
            if !manifest.exists() {
                eprintln!(
                    "e2e: skipping — no sibling checkout at {} (see CLAUDE.md's smoke test)",
                    manifest.display()
                );
                return None;
            }
            let status = Command::new("cargo")
                .args([
                    "build",
                    "--manifest-path",
                    manifest.to_str().expect("utf8 path"),
                    "--bin",
                    "pulsemq",
                ])
                .status();
            match status {
                Ok(s) if s.success() => {
                    let bin = Path::new("../pulsemq/target/debug/pulsemq");
                    if bin.exists() {
                        Some(bin.to_path_buf())
                    } else {
                        eprintln!(
                            "e2e: skipping — build succeeded but {} is missing",
                            bin.display()
                        );
                        None
                    }
                }
                Ok(s) => {
                    eprintln!("e2e: skipping — building ../pulsemq exited with {s}");
                    None
                }
                Err(e) => {
                    eprintln!("e2e: skipping — could not run cargo to build ../pulsemq: {e}");
                    None
                }
            }
        })
        .as_deref()
}

/// A running broker on a scratch port, killed and reaped on drop.
struct Broker {
    child: Child,
    port: u16,
}

impl Broker {
    /// Spawn a broker on a free port and wait for it to accept MQTT
    /// connections. `None` when `../pulsemq` isn't available.
    async fn spawn() -> Option<Broker> {
        let bin = pulsemq_binary()?;

        // Reserve a port, then release it immediately before the broker
        // binds it — a small TOCTOU window, acceptable for a test fixture
        // that only needs to not collide with itself.
        let scratch = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a scratch port");
        let port = scratch.local_addr().expect("addr").port();
        drop(scratch);

        let db_path =
            std::env::temp_dir().join(format!("pulsemq-cli-e2e-{}-{port}.db", std::process::id()));
        let _ = std::fs::remove_file(&db_path);

        let child = Command::new(bin)
            .args([
                "--listen-addr",
                &format!("127.0.0.1:{port}"),
                // 0 lets the OS pick a free admin port too — this file
                // never talks to it, so which one doesn't matter.
                "--admin-addr",
                "127.0.0.1:0",
                "--db-path",
                db_path.to_str().expect("utf8 path"),
                "--sys-interval",
                "0",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn pulsemq");

        let mut broker = Broker { child, port };
        if !broker.wait_ready().await {
            eprintln!("e2e: skipping — broker did not start accepting connections in time");
            return None;
        }
        Some(broker)
    }

    async fn wait_ready(&mut self) -> bool {
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            if TcpStream::connect(("127.0.0.1", self.port)).await.is_ok() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    }
}

impl Drop for Broker {
    fn drop(&mut self) {
        // Kill then reap: `kill` only signals, and an unreaped child stays
        // a zombie process until something calls `wait`.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn conn_args(port: u16) -> ConnectionArgs {
    let port = port.to_string();
    match Cli::parse_from([
        "pulsemq-cli",
        "pub",
        "-t",
        "x",
        "-b",
        "127.0.0.1",
        "-p",
        &port,
    ])
    .command
    {
        Subcommand::Pub(args) => args.conn,
        _ => unreachable!(),
    }
}

fn pub_args(port: u16, topic: &str, message: &str) -> PubArgs {
    let port = port.to_string();
    match Cli::parse_from([
        "pulsemq-cli",
        "pub",
        "-b",
        "127.0.0.1",
        "-p",
        &port,
        "-t",
        topic,
        "-m",
        message,
        "-q",
        "1",
    ])
    .command
    {
        Subcommand::Pub(args) => args,
        _ => unreachable!(),
    }
}

fn sub_args(port: u16, topic: &str, count: u64) -> SubArgs {
    let port = port.to_string();
    let count = count.to_string();
    match Cli::parse_from([
        "pulsemq-cli",
        "sub",
        "-b",
        "127.0.0.1",
        "-p",
        &port,
        "-t",
        topic,
        "-q",
        "1",
        "-n",
        &count,
    ])
    .command
    {
        Subcommand::Sub(args) => args,
        _ => unreachable!(),
    }
}

fn request_args(port: u16, topic: &str, reply_topic: &str, message: &str) -> RequestArgs {
    let port = port.to_string();
    match Cli::parse_from([
        "pulsemq-cli",
        "request",
        "-b",
        "127.0.0.1",
        "-p",
        &port,
        "-t",
        topic,
        "--reply-topic",
        reply_topic,
        "-m",
        message,
        "-q",
        "1",
    ])
    .command
    {
        Subcommand::Request(args) => args,
        _ => unreachable!(),
    }
}

/// The publish/subscribe round trip: `sub` must actually receive what
/// `pub` sends, over a real broker and a real TCP connection — the two
/// halves are otherwise only ever tested against a duplex-backed stub.
#[tokio::test]
async fn publish_reaches_a_waiting_subscriber() {
    let Some(broker) = Broker::spawn().await else {
        return;
    };

    // Subscribe before publishing: an unsubscribed topic delivers to
    // nobody, and a test that races this way looks like a client bug
    // (CLAUDE.md).
    let sub = tokio::spawn(subscribe::run(sub_args(broker.port, "e2e/pubsub", 1)));
    tokio::time::sleep(SUBSCRIBE_SETTLE).await;

    publish::run(pub_args(broker.port, "e2e/pubsub", "hello e2e"))
        .await
        .expect("pub completes");

    tokio::time::timeout(ROUND_TRIP_TIMEOUT, sub)
        .await
        .expect("sub does not hang")
        .expect("sub task joins")
        .expect("sub completes");
}

/// The request/reply round trip: `request` subscribes to the reply topic,
/// publishes the request, and must see the reply a responder publishes
/// back — end to end, over a real broker.
#[tokio::test]
async fn request_receives_a_responders_reply() {
    let Some(broker) = Broker::spawn().await else {
        return;
    };

    let request_topic = "e2e/request";
    let reply_topic = "e2e/reply";

    let responder_conn = conn_args(broker.port);
    let responder = tokio::spawn(async move {
        let mut client = Client::connect(&responder_conn)
            .await
            .expect("responder connects");
        subscribe::subscribe(
            &mut client,
            std::slice::from_ref(&request_topic.to_string()),
            QoS::AtLeastOnce,
        )
        .await
        .expect("responder subscribes");

        loop {
            match client.recv_keepalive().await.expect("responder recv") {
                Packet::Publish(p) => {
                    subscribe::acknowledge(&mut client, &p)
                        .await
                        .expect("responder acks the request");
                    let reply = pulsemq_cli::mqtt::packet::Publish {
                        dup: false,
                        qos: QoS::AtLeastOnce,
                        retain: false,
                        topic: reply_topic.to_string(),
                        packet_id: Some(client.next_packet_id()),
                        properties: Default::default(),
                        payload: format!("echo:{}", String::from_utf8_lossy(&p.payload))
                            .into_bytes()
                            .into(),
                    };
                    client
                        .send(&Packet::Publish(reply))
                        .await
                        .expect("responder sends the reply");
                    break;
                }
                _ => continue,
            }
        }
        client.disconnect().await.expect("responder disconnects");
    });
    tokio::time::sleep(SUBSCRIBE_SETTLE).await;

    tokio::time::timeout(
        ROUND_TRIP_TIMEOUT,
        request::run(request_args(
            broker.port,
            request_topic,
            reply_topic,
            "ping",
        )),
    )
    .await
    .expect("request does not hang")
    .expect("request completes");

    tokio::time::timeout(ROUND_TRIP_TIMEOUT, responder)
        .await
        .expect("responder does not hang")
        .expect("responder task joins");
}
