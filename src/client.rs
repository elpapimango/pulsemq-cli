//! Connection handling shared by every subcommand.
//!
//! Dials the broker, performs the CONNECT/CONNACK handshake for the negotiated
//! protocol version, and exposes `send`/`recv` plus packet-identifier
//! allocation. Everything version-specific lives here or in the `pulsemq`
//! codec, so the subcommands hold only the part that differs between them.

use std::time::Duration;

use pulsemq::framing::{read_packet, write_packet, ReadOutcome};
use pulsemq::packet::{Connect, Packet};
use pulsemq::types::{ProtocolVersion, ReasonCode};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::cli::ConnectionArgs;
use crate::error::{Error, Result};

/// Client-side limit on an inbound packet, passed to `framing::read_packet`.
/// 268,435,455 is the protocol maximum; a client has no reason to be stricter
/// than the broker it is testing.
const MAX_PACKET_SIZE: u32 = 268_435_455;

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
        let version = args.version();
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
        let password = args
            .password()
            .map_err(|e| Error::Usage(format!("cannot read the password file: {e}")))?;

        let stream = TcpStream::connect((args.broker.as_str(), args.port)).await?;
        // One small packet at a time is this tool's whole traffic pattern;
        // Nagle would add up to 40 ms to every request/reply round trip.
        stream.set_nodelay(true)?;

        let mut client = Client {
            stream,
            version,
            keep_alive: args.keepalive,
            next_packet_id: 0,
        };

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
            client_id,
            will: None,
            username: args.user.clone(),
            password,
        };
        client.send(&Packet::Connect(connect)).await?;

        match client.recv().await? {
            Packet::Connack(ack) if ack.reason_code == ReasonCode::Success => Ok(client),
            Packet::Connack(ack) => Err(Error::Rejected {
                what: "connection".into(),
                code: ack.reason_code,
            }),
            other => Err(Error::Mqtt(pulsemq::error::protocol(format!(
                "expected CONNACK, got {}",
                other.name()
            )))),
        }
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
        let packet = Packet::Disconnect(pulsemq::packet::Disconnect::new(ReasonCode::Success));
        self.send(&packet).await?;
        Ok(())
    }
}

/// A client identifier that is unique enough for a short-lived tool run and
/// still recognisable in broker logs.
fn generated_client_id() -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("pulsemq-cli-{pid}-{nanos:x}")
}
