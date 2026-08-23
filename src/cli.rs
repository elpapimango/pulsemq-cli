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

/// Largest inbound packet accepted unless `--max-packet-size` says otherwise.
///
/// The protocol maximum is 268,435,455 bytes, and defaulting to it means a
/// broker can make this tool allocate 256 MB by declaring a Remaining Length
/// and then sending nothing: `framing::read_packet` sizes its buffer from the
/// declared length before any payload byte arrives. Everything the broker
/// sends is untrusted input, so the ceiling is set here rather than taken on
/// the broker's word. 1 MiB matches PulseMQ's own default; a run that needs
/// more says so explicitly.
pub const DEFAULT_MAX_PACKET_SIZE: u32 = 1024 * 1024;

/// The protocol's own ceiling on a packet (2.1.4): a Remaining Length is a
/// four-byte Variable Byte Integer, so nothing larger can be framed.
pub const PROTOCOL_MAX_PACKET_SIZE: u32 = 268_435_455;

/// Environment variable holding the password, used when neither
/// `--password-file` nor `--password` is given.
///
/// It exists because the two flags each leak: `--password` shows up in `ps`
/// output and in shell history, and `--password-file` needs a file to exist
/// somewhere. An environment variable does neither, though it is still visible
/// to anything that can read this process's environment.
pub const PASSWORD_ENV_VAR: &str = "PULSEMQ_PASSWORD";

/// Warn when a password file is readable by anyone but its owner.
///
/// A warning rather than a refusal: the credential is the operator's to
/// manage, and a tool that stops mid-task over file bits is a tool people work
/// around. Unix-only — Windows permissions do not map onto these bits, and
/// pretending otherwise would produce a warning that means nothing.
///
/// Said once per process, because `password()` is deliberately called more
/// than once — `Client::connect` validates the file before dialling and the
/// handshake reads it again — and `bench` calls it once per connection. The
/// same sentence repeated a dozen times reads like a fault in the tool rather
/// than advice about the file.
#[cfg(unix)]
fn warn_if_readable_by_others(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    static WARNED: std::sync::Once = std::sync::Once::new();

    let Ok(metadata) = std::fs::metadata(path) else {
        // Unreadable or missing: `std::fs::read` reports that properly a
        // moment later, with the real error. Nothing to say here.
        return;
    };
    let mode = metadata.permissions().mode();
    if mode & 0o077 != 0 {
        WARNED.call_once(|| {
            eprintln!(
                "pulsemq-cli: warning: {} is readable by others (mode {:04o}); \
                 consider chmod 600",
                path.display(),
                mode & 0o7777
            );
        });
    }
}

#[cfg(not(unix))]
fn warn_if_readable_by_others(_path: &std::path::Path) {}

/// How to reach the broker. Flattened into every subcommand.
#[derive(Args, Clone)]
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

    /// Largest inbound packet to accept, in bytes. Also sent to the broker as
    /// the v5 Maximum Packet Size so it never offers a larger one.
    #[arg(long, default_value_t = DEFAULT_MAX_PACKET_SIZE, value_name = "BYTES")]
    pub max_packet_size: u32,

    /// Resume the broker-side session instead of starting clean. Needs
    /// `--client-id`, since a generated identifier cannot be resumed.
    #[arg(long, requires = "client_id")]
    pub persistent_session: bool,
}

/// Hand-written rather than derived so a future `{:?}` on parsed args — a
/// `--verbose` dump, a debug-formatted error — can never put the cleartext
/// password in a log or a terminal scrollback.
impl std::fmt::Debug for ConnectionArgs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionArgs")
            .field("broker", &self.broker)
            .field("port", &self.port)
            .field("client_id", &self.client_id)
            .field("protocol", &self.protocol)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("password_file", &self.password_file)
            .field("keepalive", &self.keepalive)
            .field("max_packet_size", &self.max_packet_size)
            .field("persistent_session", &self.persistent_session)
            .finish()
    }
}

impl ConnectionArgs {
    pub fn clean_start(&self) -> bool {
        !self.persistent_session
    }

    pub fn version(&self) -> ProtocolVersion {
        self.protocol.into()
    }

    /// The inbound packet ceiling, validated.
    ///
    /// Zero is not merely useless here: as a v5 Maximum Packet Size it is a
    /// Protocol Error (3.1.2.11.4), so it must not reach the wire. The upper
    /// bound is the protocol's own.
    pub fn max_packet_size(&self) -> Result<u32, String> {
        match self.max_packet_size {
            0 => Err("--max-packet-size must be at least 1".into()),
            n if n > PROTOCOL_MAX_PACKET_SIZE => Err(format!(
                "--max-packet-size {n} exceeds the protocol maximum {PROTOCOL_MAX_PACKET_SIZE}"
            )),
            n => Ok(n),
        }
    }

    /// The password from whichever source was given. Reading the file here
    /// keeps the "which source wins" question in one place.
    ///
    /// Explicit beats ambient: `--password-file`, then `--password` (clap
    /// rejects the two together), then `PULSEMQ_PASSWORD`. The environment
    /// comes last so that a variable left in a shell profile cannot quietly
    /// override the credential someone just typed.
    pub fn password(&self) -> std::io::Result<Option<Vec<u8>>> {
        if let Some(path) = &self.password_file {
            warn_if_readable_by_others(path);
            let mut bytes = std::fs::read(path)?;
            while matches!(bytes.last(), Some(b'\n' | b'\r')) {
                bytes.pop();
            }
            return Ok(Some(bytes));
        }
        if let Some(password) = &self.password {
            return Ok(Some(password.as_bytes().to_vec()));
        }
        Ok(std::env::var_os(PASSWORD_ENV_VAR).map(|v| {
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStrExt;
                v.as_bytes().to_vec()
            }
            #[cfg(not(unix))]
            {
                v.to_string_lossy().into_owned().into_bytes()
            }
        }))
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

    fn conn_from(argv: &[&str]) -> ConnectionArgs {
        match Cli::parse_from(argv).command {
            Command::Pub(args) => args.conn,
            _ => panic!("expected the pub subcommand"),
        }
    }

    /// The default is the point of the whole option: left at the protocol
    /// maximum, a broker can declare a 256 MB Remaining Length and make this
    /// tool allocate that much before sending a single payload byte.
    #[test]
    fn the_packet_ceiling_defaults_to_one_mebibyte_not_the_protocol_maximum() {
        let conn = conn_from(&["pulsemq-cli", "pub", "-t", "x"]);
        assert_eq!(conn.max_packet_size().unwrap(), 1024 * 1024);
        assert!(conn.max_packet_size().unwrap() < PROTOCOL_MAX_PACKET_SIZE);
    }

    #[test]
    fn the_packet_ceiling_can_be_raised_to_the_protocol_maximum() {
        let argv_max = PROTOCOL_MAX_PACKET_SIZE.to_string();
        let conn = conn_from(&[
            "pulsemq-cli",
            "pub",
            "-t",
            "x",
            "--max-packet-size",
            &argv_max,
        ]);
        assert_eq!(conn.max_packet_size().unwrap(), PROTOCOL_MAX_PACKET_SIZE);
    }

    /// Zero is a Protocol Error as a v5 Maximum Packet Size (3.1.2.11.4), so
    /// it must be refused here rather than encoded onto the wire.
    #[test]
    fn a_zero_packet_ceiling_is_refused() {
        let conn = conn_from(&["pulsemq-cli", "pub", "-t", "x", "--max-packet-size", "0"]);
        assert!(conn.max_packet_size().is_err());
    }

    #[test]
    fn a_packet_ceiling_above_the_protocol_maximum_is_refused() {
        let over = (PROTOCOL_MAX_PACKET_SIZE + 1).to_string();
        let conn = conn_from(&["pulsemq-cli", "pub", "-t", "x", "--max-packet-size", &over]);
        assert!(conn.max_packet_size().is_err());
    }

    /// Explicit beats ambient: a variable left in a shell profile must not
    /// override the credential someone just typed.
    #[test]
    fn an_explicit_password_wins_over_the_environment() {
        temp_env_password("from-the-environment", || {
            let conn = conn_from(&["pulsemq-cli", "pub", "-t", "x", "--password", "typed"]);
            assert_eq!(conn.password().unwrap().unwrap(), b"typed");
        });
    }

    #[test]
    fn the_environment_supplies_the_password_when_no_flag_does() {
        temp_env_password("from-the-environment", || {
            let conn = conn_from(&["pulsemq-cli", "pub", "-t", "x"]);
            assert_eq!(conn.password().unwrap().unwrap(), b"from-the-environment");
        });
    }

    #[test]
    fn no_flag_and_no_environment_means_no_password() {
        temp_env_password_unset(|| {
            let conn = conn_from(&["pulsemq-cli", "pub", "-t", "x"]);
            assert!(conn.password().unwrap().is_none());
        });
    }

    /// Regression: `Debug` is hand-written specifically so a future
    /// `{:?}`-dumped `ConnectionArgs` (a `--verbose` flag, a debug-formatted
    /// error) can never put the cleartext password in a log.
    #[test]
    fn debug_formatting_never_shows_the_password() {
        let conn = conn_from(&["pulsemq-cli", "pub", "-t", "x", "--password", "s3cret"]);
        let debug = format!("{conn:?}");
        assert!(
            !debug.contains("s3cret"),
            "password leaked into Debug output: {debug}"
        );
        assert!(debug.contains("<redacted>"), "debug output was: {debug}");
    }

    /// `PULSEMQ_PASSWORD` is process-wide state, so these tests must not run
    /// concurrently with each other. A mutex is enough: they are the only
    /// readers and writers of it.
    fn env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    fn temp_env_password(value: &str, body: impl FnOnce()) {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        // SAFETY: the lock above serialises every test that touches this
        // variable, and nothing else in this binary reads the environment.
        unsafe { std::env::set_var(PASSWORD_ENV_VAR, value) };
        body();
        unsafe { std::env::remove_var(PASSWORD_ENV_VAR) };
    }

    fn temp_env_password_unset(body: impl FnOnce()) {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        unsafe { std::env::remove_var(PASSWORD_ENV_VAR) };
        body();
    }

    /// A file still wins over both, and its trailing newline is stripped.
    #[test]
    fn a_password_file_wins_over_the_environment() {
        let path = std::env::temp_dir().join(format!("pulsemq-cli-pw-{}", std::process::id()));
        std::fs::write(&path, b"from-the-file\n").unwrap();
        temp_env_password("from-the-environment", || {
            let conn = conn_from(&[
                "pulsemq-cli",
                "pub",
                "-t",
                "x",
                "--password-file",
                path.to_str().unwrap(),
            ]);
            assert_eq!(conn.password().unwrap().unwrap(), b"from-the-file");
        });
        let _ = std::fs::remove_file(&path);
    }
}
