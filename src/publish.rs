//! `wispmq-cli pub` — publish one message, complete its QoS handshake, exit.
//! With `--file` or `--stdin-lines`, the payload comes from somewhere other
//! than `--message`; `--stdin-lines` publishes once per line, all over one
//! connection.

use std::io::{BufRead, Read};

use crate::mqtt::codec::Properties;
use crate::mqtt::packet::{Packet, Publish};
use crate::mqtt::types::{QoS, ReasonCode};

use crate::cli::PubArgs;
use crate::client::Client;
use crate::error::{Error, Result};

/// Where the payload (or payloads) to publish come from, resolved once from
/// `PubArgs` before any connection is opened.
enum PayloadSource {
    /// One payload, one PUBLISH.
    Once(Vec<u8>),
    /// One PUBLISH per line of stdin, over the one connection `run` opens.
    /// Reading stdin synchronously between publishes is fine on the
    /// current-thread runtime `pub` runs on (CLAUDE.md: `pub`, `sub` and
    /// `request` do one thing at a time) — nothing else needs to run on
    /// this thread while a line is awaited. A caveat that follows from the
    /// same design: unlike `sub`, this loop never sends PINGREQ, so a
    /// producer slow enough to idle past the broker's keep-alive interval
    /// between lines can still see the connection dropped.
    Lines,
}

fn resolve_payload(args: &PubArgs) -> Result<PayloadSource> {
    if args.stdin_lines {
        return Ok(PayloadSource::Lines);
    }
    if let Some(path) = &args.file {
        let bytes = if path.as_os_str() == "-" {
            let mut buf = Vec::new();
            std::io::stdin()
                .read_to_end(&mut buf)
                .map_err(|e| Error::Usage(format!("reading stdin: {e}")))?;
            buf
        } else {
            std::fs::read(path).map_err(|e| Error::Usage(format!("{}: {e}", path.display())))?
        };
        return Ok(PayloadSource::Once(bytes));
    }
    Ok(PayloadSource::Once(
        args.message.as_deref().unwrap_or("").as_bytes().to_vec(),
    ))
}

/// v5 Properties (2.2.2) for the PUBLISH(es), from whichever `--user-property`
/// / `--message-expiry-interval` / `--content-type` /
/// `--payload-format-indicator` flags were given. Rejects those flags
/// outright on a v3.x connection, which has no Properties to carry them in
/// — the same "fail before dialling" style `request` uses for its own
/// version restriction, so a doomed run doesn't get as far as a socket.
fn resolve_properties(args: &PubArgs) -> Result<Properties> {
    let used_on_v3 = !args.conn.version().has_properties()
        && (!args.user_properties.is_empty()
            || args.message_expiry_interval.is_some()
            || args.content_type.is_some()
            || args.payload_format_indicator);
    if used_on_v3 {
        return Err(Error::Usage(
            "--user-property/--message-expiry-interval/--content-type/\
             --payload-format-indicator need v5 properties (--protocol 5)"
                .into(),
        ));
    }
    let mut properties = Properties::new();
    if args.conn.version().has_properties() {
        properties.user_properties = args.user_properties.clone();
        properties.message_expiry_interval = args.message_expiry_interval;
        properties.content_type = args.content_type.clone();
        if args.payload_format_indicator {
            properties.payload_format_indicator = Some(1);
        }
    }
    Ok(properties)
}

pub async fn run(args: PubArgs) -> Result<()> {
    let qos = QoS::from_u8(args.qos)?;
    let source = resolve_payload(&args)?;
    let properties = resolve_properties(&args)?;

    let mut client = Client::connect(&args.conn).await?;

    match source {
        PayloadSource::Once(payload) => {
            publish_one(
                &mut client,
                &args.topic,
                payload,
                qos,
                args.retain,
                properties,
            )
            .await?;
        }
        PayloadSource::Lines => {
            for line in std::io::stdin().lock().lines() {
                let line = line.map_err(|e| Error::Usage(format!("reading stdin: {e}")))?;
                publish_one(
                    &mut client,
                    &args.topic,
                    line.into_bytes(),
                    qos,
                    args.retain,
                    properties.clone(),
                )
                .await?;
            }
        }
    }

    client.disconnect().await
}

/// Publish one message and, for QoS 1/2, complete the acknowledgement
/// handshake before returning — the same sequence `run` used to perform
/// inline, factored out so `--stdin-lines` can repeat it per line.
async fn publish_one(
    client: &mut Client,
    topic: &str,
    payload: Vec<u8>,
    qos: QoS,
    retain: bool,
    properties: Properties,
) -> Result<()> {
    let packet_id = match qos {
        QoS::AtMostOnce => None,
        _ => Some(client.next_packet_id()),
    };
    let publish = Publish {
        dup: false,
        qos,
        retain,
        topic: topic.to_string(),
        packet_id,
        properties,
        payload: payload.into(),
    };
    client.send(&Packet::Publish(publish)).await?;

    // QoS 0 is fire-and-forget; 1 and 2 must finish their handshake before
    // the next publish (or the process exits), or the broker sees an
    // incomplete delivery.
    match qos {
        QoS::AtMostOnce => {}
        QoS::AtLeastOnce => {
            let ack = expect_ack(client, "publish").await?;
            check(ack, "publish")?;
        }
        QoS::ExactlyOnce => {
            let rec = expect_ack(client, "publish").await?;
            check(rec, "publish")?;
            let id = packet_id.expect("QoS 2 always allocates a packet identifier");
            let pubrel = crate::mqtt::packet::PubAck::new(id, ReasonCode::Success);
            client.send(&Packet::Pubrel(pubrel)).await?;
            let comp = expect_ack(client, "publish").await?;
            check(comp, "publish")?;
        }
    }
    Ok(())
}

/// Read until the next acknowledgement packet, ignoring anything the broker
/// sends in the meantime.
async fn expect_ack(client: &mut Client, what: &str) -> Result<ReasonCode> {
    loop {
        match client.recv().await? {
            Packet::Puback(a) | Packet::Pubrec(a) | Packet::Pubcomp(a) => return Ok(a.reason_code),
            Packet::Pingresp => continue,
            Packet::Disconnect(d) => {
                return Err(Error::Rejected {
                    what: what.into(),
                    code: d.reason_code,
                })
            }
            other => {
                return Err(Error::Mqtt(crate::mqtt::error::protocol(format!(
                    "expected an acknowledgement, got {}",
                    other.name()
                ))))
            }
        }
    }
}

fn check(code: ReasonCode, what: &str) -> Result<()> {
    // NoMatchingSubscribers means the message was accepted and had nowhere to
    // go — not a failure (3.4.2.1).
    if code.is_error() {
        Err(Error::Rejected {
            what: what.into(),
            code,
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::framing::{read_packet, write_packet, ReadOutcome};
    use crate::mqtt::packet::Connack;
    use crate::mqtt::types::ProtocolVersion;
    use clap::Parser;
    use tokio::net::TcpListener;
    use tokio::time::Duration;

    fn pub_args(argv: &[&str]) -> PubArgs {
        match crate::cli::Cli::parse_from(argv).command {
            crate::cli::Command::Pub(args) => args,
            _ => panic!("expected the pub subcommand"),
        }
    }

    fn conn_args(port: u16) -> crate::cli::ConnectionArgs {
        let port = port.to_string();
        pub_args(&[
            "wispmq-cli",
            "pub",
            "-t",
            "x",
            "-b",
            "127.0.0.1",
            "-p",
            &port,
        ])
        .conn
    }

    const TEST_TIMEOUT: Duration = Duration::from_secs(10);

    /// A broker stub: accepts one connection, replies CONNACK and a
    /// Success PUBACK to every PUBLISH it sees, and returns every PUBLISH
    /// it collected once the client disconnects.
    async fn collect_publishes(listener: TcpListener) -> Vec<Publish> {
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

        let mut received = Vec::new();
        loop {
            match read_packet(&mut stream, 65_535, version).await {
                Ok(ReadOutcome::Packet(Packet::Publish(p), _)) => {
                    if let Some(id) = p.packet_id {
                        let ack = crate::mqtt::packet::PubAck::new(id, ReasonCode::Success);
                        write_packet(&mut stream, &Packet::Puback(ack), version)
                            .await
                            .expect("PUBACK");
                    }
                    received.push(p);
                }
                Ok(ReadOutcome::Packet(Packet::Disconnect(_), _)) | Ok(ReadOutcome::Eof) => break,
                Ok(ReadOutcome::Packet(_, _)) => continue,
                Err(_) => break,
            }
        }
        received
    }

    /// Regression: `--stdin-lines` publishes multiple messages over the one
    /// connection `run` opens, rather than reconnecting per line. This
    /// exercises that behavior directly through `publish_one` — the
    /// function the per-line loop calls — without needing real stdin.
    #[tokio::test]
    async fn multiple_publishes_reuse_one_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let broker = tokio::spawn(collect_publishes(listener));

        let mut client = Client::connect(&conn_args(port))
            .await
            .expect("client connects");
        for payload in [b"one".to_vec(), b"two".to_vec(), b"three".to_vec()] {
            publish_one(
                &mut client,
                "x",
                payload,
                QoS::AtLeastOnce,
                false,
                Properties::new(),
            )
            .await
            .expect("publish_one succeeds");
        }
        client.disconnect().await.expect("disconnect");

        let received = tokio::time::timeout(TEST_TIMEOUT, broker)
            .await
            .expect("broker task does not hang")
            .expect("broker task");
        assert_eq!(received.len(), 3);
        assert_eq!(received[0].payload.as_ref(), b"one");
        assert_eq!(received[1].payload.as_ref(), b"two");
        assert_eq!(received[2].payload.as_ref(), b"three");
    }

    #[test]
    fn file_reads_the_payload_from_disk() {
        let path = std::env::temp_dir().join(format!("wispmq-cli-pub-file-{}", std::process::id()));
        std::fs::write(&path, b"from a file").unwrap();
        let args = pub_args(&[
            "wispmq-cli",
            "pub",
            "-t",
            "x",
            "--file",
            path.to_str().unwrap(),
        ]);
        let PayloadSource::Once(payload) = resolve_payload(&args).unwrap() else {
            panic!("expected a single payload");
        };
        assert_eq!(payload, b"from a file");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn message_and_file_conflict_at_the_clap_layer() {
        assert!(crate::cli::Cli::try_parse_from([
            "wispmq-cli",
            "pub",
            "-t",
            "x",
            "-m",
            "hi",
            "--file",
            "/tmp/whatever",
        ])
        .is_err());
    }

    #[test]
    fn no_source_means_an_empty_payload() {
        let args = pub_args(&["wispmq-cli", "pub", "-t", "x"]);
        let PayloadSource::Once(payload) = resolve_payload(&args).unwrap() else {
            panic!("expected a single payload");
        };
        assert!(payload.is_empty());
    }

    #[test]
    fn v5_properties_populate_from_the_flags() {
        let args = pub_args(&[
            "wispmq-cli",
            "pub",
            "-t",
            "x",
            "--user-property",
            "room=kitchen",
            "--message-expiry-interval",
            "60",
            "--content-type",
            "application/json",
            "--payload-format-indicator",
        ]);
        let properties = resolve_properties(&args).expect("v5 properties resolve");
        assert_eq!(
            properties.user_properties,
            vec![("room".to_string(), "kitchen".to_string())]
        );
        assert_eq!(properties.message_expiry_interval, Some(60));
        assert_eq!(properties.content_type.as_deref(), Some("application/json"));
        assert_eq!(properties.payload_format_indicator, Some(1));
    }

    #[test]
    fn a_v5_only_flag_on_v3_is_a_usage_error() {
        let args = pub_args(&[
            "wispmq-cli",
            "pub",
            "-t",
            "x",
            "--protocol",
            "3.1.1",
            "--content-type",
            "text/plain",
        ]);
        let err = resolve_properties(&args).expect_err("v5-only flag on v3.1.1");
        assert!(matches!(err, Error::Usage(_)));
    }

    /// v3.x with none of the v5-only flags set must not error — the flags
    /// are optional, not a blanket version requirement the way `request`'s
    /// v3.1 rejection is.
    #[test]
    fn v3_without_any_v5_only_flag_is_fine() {
        let args = pub_args(&["wispmq-cli", "pub", "-t", "x", "--protocol", "3.1.1"]);
        let properties = resolve_properties(&args).expect("no v5-only flags used");
        assert!(properties.is_empty());
    }

    #[test]
    fn user_property_requires_an_equals_sign() {
        assert!(crate::cli::Cli::try_parse_from([
            "wispmq-cli",
            "pub",
            "-t",
            "x",
            "--user-property",
            "no-equals-sign",
        ])
        .is_err());
    }
}
