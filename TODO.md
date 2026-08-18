# TODO

Ordered. Pick the top item.

## 1. Security audit

Before any feature work. This tool handles credentials and parses whatever a
broker sends it, so both directions need review.

- **Credential handling** — `--password` is visible in `ps` output and lands in
  shell history; `--password-file` exists but nothing checks the file's
  permissions. Decide what to do about a world-readable password file, whether
  to support an environment variable, and whether the password should be zeroed
  in memory after the CONNECT is encoded rather than living in a `Vec<u8>` until
  drop.
- **Untrusted input from the broker** — everything after CONNECT is attacker
  input if the broker is hostile or in the path. The codec is this crate's own
  code, in `src/mqtt`, and `tests/malformed.rs` proves `Packet::decode` does not
  panic on hostile bytes; confirm that guarantee actually covers the client
  direction (CONNACK, SUBACK, PUBLISH properties) and that the layers above it
  add no panic of their own — no `unwrap`, no indexing, no unchecked cast — on
  that path.
- **Resource limits** — `client::MAX_PACKET_SIZE` is the protocol maximum, so a
  broker can make the tool allocate 256 MB. Decide a sane default and expose it
  as a flag. Same question for an unbounded `sub` run.
- **Terminal output** — payloads go to stdout as raw bytes, so a broker can
  emit ANSI escape sequences into the user's terminal. Decide whether to escape
  non-printable bytes when stdout is a TTY.
- **Transport** — connections are plaintext today and there is no warning that
  credentials cross the wire in the clear. At minimum say so; see item 3.

Write findings into this file, fix what the audit finds, and add a regression
test per fix.

## 2. Performance testing mode

Turn the client into a load generator, so a broker can be measured with the same
tool used to talk to it.

- A `bench` (or `load`) subcommand: N concurrent connections, a publish rate or
  a fixed message count, configurable payload size, QoS and topic pattern.
- Report throughput and latency: messages and bytes per second, and the
  publish-to-acknowledge round trip as a distribution (median, p95, p99, max),
  not just a mean.
- End-to-end latency needs a subscriber side too: timestamp in the payload,
  measure at receipt, and keep the clock question honest — same host or
  documented skew.
- Structural prerequisites: `Client` currently owns one `TcpStream` and does one
  thing at a time, and `recv_keepalive` serialises the ping with the read. A
  publisher that must sustain a rate while reading acknowledgements needs the
  stream split, plus an in-flight window (Receive Maximum) rather than one
  round trip at a time.
- The runtime is `current_thread`; a load generator wants the multi-threaded
  one, which changes the `#[tokio::main]` flavour and the tokio feature set.
- Machine-readable output (JSON) so a run can be diffed against a previous one.

## 3. TLS and mutual TLS

`--cafile`, `--cert`, `--key`, `--insecure`, and a default port of 8883 when TLS
is on. All of it — root store, SNI, client certificate — is new code here, on
`tokio-rustls`, behind a non-default Cargo feature so the plain-TCP build keeps
its current dependency tree.

## 4. Payload sources

`--file FILE` and a stdin mode (whole input, or a message per line) for `pub`,
so a script can pipe into it. Today only `--message` exists.

## 5. Will message

`--will-topic`, `--will-payload`, `--will-qos`, `--will-retain`. `packet::Will`
already carries them; only the flags and the wiring into `Client::connect` are
missing.

## 6. v5 properties

User properties, message expiry, content type and payload format indicator on
`pub`, plus printing the received ones under `sub --show-topic`.

## 7. WebSocket transport

`ws://` and `wss://`. The handshake must offer the `mqtt` subprotocol or the
server rejects it at the HTTP layer, before any MQTT packet is exchanged.
`Client` holds a concrete `TcpStream` and has to become generic over the stream
first — the same prerequisite item 2 needs.

## 8. End-to-end tests

Spawn a broker — `../pulsemq` if one is checked out, otherwise any
spec-conformant one — then assert the publish/subscribe and request/reply round
trips. The smoke test in `CLAUDE.md` covers this by hand today. The broker stays
a test fixture, discovered at run time; it must not become a build dependency
again.
