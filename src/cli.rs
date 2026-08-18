//! The `clap` command-line surface.
//!
//! Options are this tool's own design, not a clone of anyone else's: long
//! names say what they do, short letters exist only where the option is typed
//! often, and clap keeps its own `-h` / `-V`. Connection options are one
//! `ConnectionArgs` flattened into every subcommand, so `--broker` means the
//! same thing and appears in the same place everywhere.

use crate::mqtt::types::ProtocolVersion;
use clap::builder::PossibleValue;
use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(
    name = "pulsemq-cli",
    version,
    about = "Command-line MQTT client: publish, subscribe, request/response"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Publish a single message.
    Pub(PubArgs),
    /// Subscribe to topic filters and print what arrives.
    Sub(SubArgs),
    /// Publish a request, wait for the reply, exit.
    ///
    /// MQTT v5.0 and v3.1.1 only.
    Request(RequestArgs),
    /// Drive load against a broker and report throughput and latency.
    Bench(BenchArgs),
}

/// How to reach the broker. Flattened into every subcommand.
#[derive(Args, Debug, Clone)]
pub struct ConnectionArgs {
    /// Broker hostname or address.
    #[arg(short = 'b', long, default_value = "localhost", value_name = "HOST")]
    pub broker: String,

    /// Broker port.
    #[arg(short = 'p', long, default_value_t = 1883)]
    pub port: u16,

    /// Client identifier. Omitted, the tool generates one and the broker
    /// treats the session as disposable.
    #[arg(short = 'i', long, value_name = "ID")]
    pub client_id: Option<String>,

    /// MQTT protocol version to speak.
    #[arg(long, value_enum, default_value_t = Protocol::V5, value_name = "VERSION")]
    pub protocol: Protocol,

    /// Username to authenticate with.
    #[arg(short = 'u', long, value_name = "NAME")]
    pub user: Option<String>,

    /// Password to authenticate with. Visible in the process list and in shell
    /// history — prefer `--password-file`.
    #[arg(long, value_name = "PASSWORD", conflicts_with = "password_file")]
    pub password: Option<String>,

    /// Read the password from this file, trailing newline stripped.
    #[arg(long, value_name = "FILE")]
    pub password_file: Option<std::path::PathBuf>,

    /// Keep-alive interval in seconds. 0 disables it.
    #[arg(short = 'k', long, default_value_t = 60, value_name = "SECS")]
    pub keepalive: u16,

    /// Resume the broker-side session instead of starting clean. Needs
    /// `--client-id`, since a generated identifier cannot be resumed.
    #[arg(long, requires = "client_id")]
    pub persistent_session: bool,
}

impl ConnectionArgs {
    pub fn clean_start(&self) -> bool {
        !self.persistent_session
    }

    pub fn version(&self) -> ProtocolVersion {
        self.protocol.into()
    }

    /// The password from whichever source was given. Reading the file here
    /// keeps the "which source wins" question in one place.
    pub fn password(&self) -> std::io::Result<Option<Vec<u8>>> {
        if let Some(path) = &self.password_file {
            let mut bytes = std::fs::read(path)?;
            while matches!(bytes.last(), Some(b'\n' | b'\r')) {
                bytes.pop();
            }
            return Ok(Some(bytes));
        }
        Ok(self.password.as_ref().map(|p| p.as_bytes().to_vec()))
    }
}

#[derive(Args, Debug)]
pub struct PubArgs {
    #[command(flatten)]
    pub conn: ConnectionArgs,

    /// Topic to publish to.
    #[arg(short = 't', long)]
    pub topic: String,

    /// Message payload. Omitted, the payload is empty.
    #[arg(short = 'm', long, value_name = "PAYLOAD")]
    pub message: Option<String>,

    /// Quality of Service: 0, 1 or 2.
    #[arg(short = 'q', long, default_value_t = 0, value_parser = clap::value_parser!(u8).range(0..=2))]
    pub qos: u8,

    /// Ask the broker to retain the message.
    #[arg(short = 'r', long)]
    pub retain: bool,
}

#[derive(Args, Debug)]
pub struct SubArgs {
    #[command(flatten)]
    pub conn: ConnectionArgs,

    /// Topic filter to subscribe to. Repeatable.
    #[arg(short = 't', long, value_name = "FILTER")]
    pub topic: Vec<String>,

    /// Maximum Quality of Service to receive: 0, 1 or 2.
    #[arg(short = 'q', long, default_value_t = 0, value_parser = clap::value_parser!(u8).range(0..=2))]
    pub qos: u8,

    /// Print the topic name before each payload.
    #[arg(long)]
    pub show_topic: bool,

    /// Exit after this many messages. Omitted, run until interrupted.
    #[arg(short = 'n', long, value_name = "N")]
    pub count: Option<u64>,
}

#[derive(Args, Debug)]
pub struct RequestArgs {
    #[command(flatten)]
    pub conn: ConnectionArgs,

    /// Topic to publish the request to.
    #[arg(short = 't', long)]
    pub topic: String,

    /// Topic to receive the reply on.
    #[arg(long, value_name = "TOPIC")]
    pub reply_topic: String,

    /// Request payload.
    #[arg(short = 'm', long, value_name = "PAYLOAD")]
    pub message: Option<String>,

    /// Quality of Service for both the request and the reply subscription.
    #[arg(short = 'q', long, default_value_t = 0, value_parser = clap::value_parser!(u8).range(0..=2))]
    pub qos: u8,

    /// Print the topic name before the reply payload.
    #[arg(long)]
    pub show_topic: bool,

    /// Exit after this many replies.
    #[arg(short = 'n', long, default_value_t = 1, value_name = "N")]
    pub count: u64,
}

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

    /// Quality of Service for both publishing and subscribing: 0 or 1.
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

/// The protocol versions, spelled the way the specs are numbered. A local enum
/// rather than `ProtocolVersion` directly, because `ValueEnum` has to be
/// implemented on a type this crate owns.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Protocol {
    V5,
    V311,
    V31,
}

impl ValueEnum for Protocol {
    fn value_variants<'a>() -> &'a [Self] {
        &[Protocol::V5, Protocol::V311, Protocol::V31]
    }

    fn to_possible_value(&self) -> Option<PossibleValue> {
        Some(match self {
            Protocol::V5 => PossibleValue::new("5").alias("5.0").help("MQTT v5.0"),
            Protocol::V311 => PossibleValue::new("3.1.1").alias("311").help("MQTT v3.1.1"),
            Protocol::V31 => PossibleValue::new("3.1").alias("31").help("MQTT v3.1"),
        })
    }
}

impl From<Protocol> for ProtocolVersion {
    fn from(p: Protocol) -> Self {
        match p {
            Protocol::V5 => ProtocolVersion::V5,
            Protocol::V311 => ProtocolVersion::V3_1_1,
            Protocol::V31 => ProtocolVersion::V3_1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    /// clap validates the whole command tree here rather than at first run,
    /// where a conflicting flag definition would only show as a panic.
    #[test]
    fn command_tree_is_valid() {
        Cli::command().debug_assert();
    }

    #[test]
    fn protocol_accepts_both_spellings() {
        for (input, want) in [
            ("5", ProtocolVersion::V5),
            ("5.0", ProtocolVersion::V5),
            ("3.1.1", ProtocolVersion::V3_1_1),
            ("311", ProtocolVersion::V3_1_1),
            ("3.1", ProtocolVersion::V3_1),
            ("31", ProtocolVersion::V3_1),
        ] {
            let cli = Cli::parse_from(["pulsemq-cli", "pub", "-t", "x", "--protocol", input]);
            let Command::Pub(args) = cli.command else {
                panic!("expected the pub subcommand");
            };
            assert_eq!(args.conn.version(), want, "for --protocol {input}");
        }
        assert!(Cli::try_parse_from(["pulsemq-cli", "pub", "-t", "x", "--protocol", "4"]).is_err());
    }

    #[test]
    fn publish_options_parse() {
        let cli = Cli::parse_from([
            "pulsemq-cli",
            "pub",
            "-b",
            "broker.example",
            "-p",
            "8883",
            "-t",
            "sensors/temp",
            "-m",
            "21.5",
            "-q",
            "2",
            "-r",
        ]);
        let Command::Pub(args) = cli.command else {
            panic!("expected the pub subcommand");
        };
        assert_eq!(args.conn.broker, "broker.example");
        assert_eq!(args.conn.port, 8883);
        assert_eq!(args.topic, "sensors/temp");
        assert_eq!(args.message.as_deref(), Some("21.5"));
        assert_eq!(args.qos, 2);
        assert!(args.retain);
        assert!(args.conn.clean_start());
    }

    /// A resumed session needs a stable identifier, so clap rejects the
    /// combination rather than the broker rejecting the CONNECT.
    #[test]
    fn persistent_session_requires_a_client_id() {
        assert!(
            Cli::try_parse_from(["pulsemq-cli", "sub", "-t", "#", "--persistent-session"]).is_err()
        );

        let cli = Cli::parse_from([
            "pulsemq-cli",
            "sub",
            "-t",
            "#",
            "--persistent-session",
            "--client-id",
            "durable",
        ]);
        let Command::Sub(args) = cli.command else {
            panic!("expected the sub subcommand");
        };
        assert!(!args.conn.clean_start());
    }

    #[test]
    fn password_and_password_file_conflict() {
        assert!(Cli::try_parse_from([
            "pulsemq-cli",
            "pub",
            "-t",
            "x",
            "--password",
            "s3cret",
            "--password-file",
            "/tmp/pw",
        ])
        .is_err());
    }

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
}
