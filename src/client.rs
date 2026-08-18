//! Connection handling shared by every subcommand.
//!
//! Dials the broker, performs the CONNECT/CONNACK handshake for the negotiated
//! protocol version, and exposes `send`/`recv` plus packet-identifier
//! allocation. Everything version-specific lives here or in the `pulsemq`
//! codec, so the subcommands hold only the part that differs between them.

use std::time::Duration;

use crate::mqtt::framing::{read_packet, write_packet, ReadOutcome};
use crate::mqtt::packet::{Connect, Packet};
use crate::mqtt::types::{ProtocolVersion, ReasonCode};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::cli::ConnectionArgs;
use crate::error::{Error, Result};

/// Client-side limit on an inbound packet, passed to `framing::read_packet`.
/// 268,435,455 is the protocol maximum; a client has no reason to be stricter
/// than the broker it is testing.
pub(crate) const MAX_PACKET_SIZE: u32 = 268_435_455;

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
        ReadOutcome::Packet(other, _) => Err(Error::Mqtt(crate::mqtt::error::protocol(format!(
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

pub struct Client {
    stream: TcpStream,
    version: ProtocolVersion,
    keep_alive: u16,
    next_packet_id: u16,
}

impl Client {
    /// Connect, handshake, and return a client ready to exchange packets.
    /// Fails on a non-success CONNACK rather than leaving a half-open session.
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

        // Read before dialling so a bad --password-file fails instantly
        // instead of after paying for a TCP handshake (or a connect timeout).
        args.password()
            .map_err(|e| Error::Usage(format!("cannot read the password file: {e}")))?;

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

    pub fn version(&self) -> ProtocolVersion {
        self.version
    }

    pub async fn send(&mut self, packet: &Packet) -> Result<()> {
        write_packet(&mut self.stream, packet, self.version).await?;
        Ok(())
    }

    /// Read one packet, treating a clean close as an error: every caller is
    /// waiting for something specific, so EOF is never the expected outcome.
    pub async fn recv(&mut self) -> Result<Packet> {
        match read_packet(&mut self.stream, MAX_PACKET_SIZE, self.version).await? {
            ReadOutcome::Packet(p, _) => Ok(p),
            ReadOutcome::Eof => Err(Error::Disconnected("while waiting for a reply".into())),
        }
    }

    /// Read one packet, sending PINGREQ whenever the keep-alive interval
    /// elapses first. Used by the long-running commands; the short ones are
    /// done well inside one interval.
    ///
    /// Serialising the ping with the read (rather than running a concurrent
    /// timer task) is enough because a client that is only waiting has nothing
    /// else to write, and PINGRESP arrives on this same path.
    pub async fn recv_keepalive(&mut self) -> Result<Packet> {
        if self.keep_alive == 0 {
            return self.recv().await;
        }
        // Ping at half the negotiated interval so a single lost PINGREQ does
        // not cost the session.
        let every = Duration::from_secs(self.keep_alive as u64).div_f32(2.0);
        loop {
            match timeout(every, self.recv()).await {
                Ok(packet) => return packet,
                Err(_) => self.send(&Packet::Pingreq).await?,
            }
        }
    }

    /// Packet Identifiers are 1..=65535; 0 is reserved (2.2.1).
    pub fn next_packet_id(&mut self) -> u16 {
        self.next_packet_id = self.next_packet_id.wrapping_add(1);
        if self.next_packet_id == 0 {
            self.next_packet_id = 1;
        }
        self.next_packet_id
    }

    /// Send DISCONNECT and close. v3.x DISCONNECT carries no reason code or
    /// properties; the codec handles that difference.
    pub async fn disconnect(mut self) -> Result<()> {
        let packet = Packet::Disconnect(crate::mqtt::packet::Disconnect::new(ReasonCode::Success));
        self.send(&packet).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mqtt::framing::{read_packet, write_packet, ReadOutcome};
    use crate::mqtt::packet::Connack;
    use clap::Parser;
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

    /// Regression: a bad --password-file must fail before any network I/O,
    /// not after paying for a TCP connect (or a connect timeout). Port 1 on
    /// loopback is not listening, so if the ordering regressed this would
    /// surface a connection error instead of the usage error.
    #[tokio::test]
    async fn connect_validates_the_password_file_before_dialling() {
        let args = connection_args(
            [
                "pulsemq-cli",
                "pub",
                "-t",
                "x",
                "-b",
                "127.0.0.1",
                "-p",
                "1",
                "--password-file",
                "/nonexistent/pulsemq-cli-password-file-that-does-not-exist",
            ]
            .as_slice(),
        );
        let err = Client::connect(&args)
            .await
            .err()
            .expect("an unreadable password file is a usage error");
        assert!(matches!(err, Error::Usage(_)));
    }
}
