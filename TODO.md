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
  credentials cross the wire in the clear. At minimum say so; see item 2.

Write findings into this file, fix what the audit finds, and add a regression
test per fix.

## 2. TLS and mutual TLS

`--cafile`, `--cert`, `--key`, `--insecure`, and a default port of 8883 when TLS
is on. All of it — root store, SNI, client certificate — is new code here, on
`tokio-rustls`, behind a non-default Cargo feature so the plain-TCP build keeps
its current dependency tree.

## 3. Payload sources

`--file FILE` and a stdin mode (whole input, or a message per line) for `pub`,
so a script can pipe into it. Today only `--message` exists.

## 4. Will message

`--will-topic`, `--will-payload`, `--will-qos`, `--will-retain`. `packet::Will`
already carries them; only the flags and the wiring into `Client::connect` are
missing.

## 5. v5 properties

User properties, message expiry, content type and payload format indicator on
`pub`, plus printing the received ones under `sub --show-topic`.

## 6. WebSocket transport

`ws://` and `wss://`. The handshake must offer the `mqtt` subprotocol or the
server rejects it at the HTTP layer, before any MQTT packet is exchanged.
`Client` holds a concrete `TcpStream` and has to become generic over the stream
first. `bench` already sidesteps this by calling `client::handshake` on the
socket directly, so the abstraction that lands here should serve both.

## 7. End-to-end tests

Spawn a broker — `../pulsemq` if one is checked out, otherwise any
spec-conformant one — then assert the publish/subscribe and request/reply round
trips. The smoke test in `CLAUDE.md` covers this by hand today. The broker stays
a test fixture, discovered at run time; it must not become a build dependency
again.

## 8. QoS 2 in bench mode

`bench` accepts QoS 0 and 1 only. QoS 2 needs the PUBREL written from the send
side while the ack side owns the read half; the plan for that is a small mpsc
of packet identifiers awaiting PUBREL, drained between publishes.

## Done

- **Performance testing mode** — `bench` subcommand: N publishers and
  subscribers, `--count` or `--duration` with an optional `--rate`, an in-flight
  window capped by the broker's Receive Maximum, exact latency percentiles for
  acknowledgement and end-to-end, table or `--json` output. Design spec:
  `docs/superpowers/specs/2026-08-17-bench-mode-design.md`.
- **Decoupling from the broker crate** — the MQTT wire format lives in
  `src/mqtt`; the crate builds with no PulseMQ checkout beside it.
