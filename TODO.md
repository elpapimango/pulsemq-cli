# TODO

Ordered. Pick the top item.

## 1. End-to-end tests

Spawn a broker — `../pulsemq` if one is checked out, otherwise any
spec-conformant one — then assert the publish/subscribe and request/reply round
trips. The smoke test in `CLAUDE.md` covers this by hand today. The broker stays
a test fixture, discovered at run time; it must not become a build dependency
again.

## Done

- **Performance testing mode** — `bench` subcommand: N publishers and
  subscribers, `--count` or `--duration` with an optional `--rate`, an in-flight
  window capped by the broker's Receive Maximum, exact latency percentiles for
  acknowledgement and end-to-end, table or `--json` output. Design spec:
  `docs/superpowers/specs/2026-08-17-bench-mode-design.md`.
- **Decoupling from the broker crate** — the MQTT wire format lives in
  `src/mqtt`; the crate builds with no PulseMQ checkout beside it.
- **Security audit, first pass** — `--max-packet-size` (1 MiB default, also
  advertised as the v5 Maximum Packet Size) closes a 256 MB allocation a broker
  could demand; control characters are escaped when stdout is a terminal, while
  pipes and redirects stay byte-exact; `PULSEMQ_PASSWORD` joins the credential
  sources and a world-readable password file warns. The decoder was audited and
  found clean: every `Reader` access is bounds-checked and no `unwrap`, index
  or unchecked cast sits on the broker-input path.
- **Security audit, second pass** — two more untrusted-input gaps closed:
  `ConnectionArgs` now has a hand-written `Debug` impl that redacts the
  password, so a future `{:?}` on parsed args can't leak it into a log; and
  `bench::subscriber` uses `Instant::checked_add` instead of `+` on the
  broker-controlled `elapsed_ns` header field, so an implausible or hostile
  value is treated as no header rather than panicking the task.
- **Security audit, third pass** — credential lifetime: a hand-rolled
  `SecretBytes` (`src/mqtt/secret.rs`) zeroes the CONNECT password on drop
  via `std::ptr::write_volatile` plus a compiler fence, no new dependency.
  Unbounded `sub`: a new `--max-messages` ceiling (default 1,000,000, `0`
  disables it) closes the "hostile broker holds the terminal forever"
  default; combined with `--count` via the smaller of the two. Fixed
  `sub -n 0` blocking for one message before exiting — the effective limit
  is now checked before the first `recv`, not only after one arrives. (On
  inspection, `request -n 0` never had this bug: its loop already checks the
  count before each `recv`.)
- **TLS and mutual TLS** — `--tls`, `--cafile` (falls back to the OS trust
  store via `rustls-native-certs` when omitted), `--cert`/`--key` for mutual
  TLS, `--insecure` (warns every connection, unlike the one-shot plaintext
  warning). `--port` now defaults to 8883 with `--tls`, 1883 without. All of
  it — `tokio-rustls`, `rustls-pemfile`, `rustls-native-certs` — sits behind
  a non-default `tls` Cargo feature; the plain build's dependency tree is
  unchanged (`cargo tree -e normal` confirmed). Also closes the last
  security-audit bullet: a one-time warning fires when a password is
  supplied without `--tls`.

  Landed alongside it: `src/transport.rs`, a new `Stream` abstraction (plain
  TCP or TLS behind one `AsyncRead`/`AsyncWrite`) that both `Client` and
  `bench` now dial through, replacing each one's own direct `TcpStream`
  handling — the refactor the WebSocket item below needed, done once so
  WebSocket could reuse it rather than repeating it. `bench`'s split keeps
  `TcpStream::into_split()`'s zero-cost path for the plain case and only
  pays `tokio::io::split`'s generic (mutex-backed) path when TLS is active,
  so the harness doesn't add overhead to what it measures.
- **WebSocket transport** — `--websocket` (`ws://`, or `wss://` combined
  with `--tls`). The Upgrade request offers the `mqtt` subprotocol via
  `tungstenite::ClientRequestBuilder`; a broker that doesn't echo it back
  fails the handshake before any MQTT packet exists (verified by test
  against a real client/server handshake, not just by inspecting the
  request). `src/transport/ws.rs`'s `WsByteStream<S>` adapts
  `tokio_tungstenite`'s `WebSocketStream` (a `Stream`/`Sink` of discrete
  messages) to `AsyncRead`/`AsyncWrite`, on the same one-packet-per-flush
  assumption `framing::write_packet` already makes: one flushed write
  becomes one WebSocket Binary frame. `wss://` composes by TLS-wrapping the
  socket first (reusing the TLS support above's `transport::tls::wrap`) and
  handing that already-connected stream to the WebSocket upgrade — `tokio-tungstenite`
  never dials or verifies a certificate itself. `tokio-tungstenite` and
  `futures-util` sit behind a non-default `websocket` Cargo feature, with no
  bundled TLS connector (`default-features = false`), so `--all-features`
  pulls in exactly one TLS stack, not two.
- **Payload sources** — `pub --file FILE` (`-` reads stdin as one payload)
  and `pub --stdin-lines` (one PUBLISH per line, all over the one connection
  `run` opens, not a reconnect per line). The per-message send-and-ack logic
  is now `publish_one`, factored out of `run` so both the single-payload
  path and the line loop share it.
- **Will message** — `--will-topic`, `--will-payload`, `--will-qos`,
  `--will-retain`. `ConnectionArgs::will()` builds the `packet::Will` (which
  already existed) from the flags — `--will-payload`/`--will-qos`/
  `--will-retain` all `requires = "will_topic"` at the clap layer, so
  there's nothing left to validate at connect time. Wired into
  `client::handshake`, so `pub`, `sub`, `request` and `bench` all get it for
  free.
- **v5 properties** — `pub --user-property KEY=VALUE` (repeatable),
  `--message-expiry-interval`, `--content-type`, `--payload-format-indicator`;
  rejected as a usage error on a v3.x connection (which has no Properties to
  carry them in) if any is actually set, but not merely for existing on a
  v3.x run — the same "fail before dialling" style `request`'s own version
  restriction uses. `sub --show-topic` prints whichever of the four a
  received PUBLISH carries, `key=value` space-separated, through the same
  terminal-escaping path as the topic and payload (broker-controlled
  strings, same risk).
- **Stale comment in `src/mqtt/packet/publish.rs`** — the doc comment on
  `Publish::payload` described broker-only behaviour that never existed in
  this crate (routing fan-out, a `tests/bench_routing.rs` that only exists
  in the PulseMQ broker's copy of this codec), leftover from the shared
  ancestor. On closer look, `Arc<[u8]>`'s benefit here isn't the
  retransmission path either — nothing in this crate actually clones a
  `Publish` today. Replaced with the honest reason: it keeps `Publish`
  (and `Packet`, which wraps it) an O(1) `Clone` instead of an O(n) payload
  copy, for whatever future call site duplicates a decoded message.
- **QoS 2 in bench mode** — implemented per the plan this bullet used to
  describe: a `pubrel_tx`/`pubrel_rx` `mpsc` channel in
  `bench/publisher.rs` lets the ack task (which owns the read half, and
  sees the PUBREC) hand a packet id back to the write side (which owns the
  socket) to actually send the PUBREL. `complete_ack` — the "free the
  permit, record a sample" logic — is now shared between PUBACK (QoS 1)
  and PUBCOMP (QoS 2, or an erroring PUBREC, which 4.3.3 says ends the flow
  without a PUBREL). `Config::from_args` no longer rejects `--qos 2`.
- **CI: dependency audit and non-Linux coverage** — `.github/workflows/ci.yml`
  gained an `audit` job (`rustsec/audit-check`, gates on the RustSec advisory
  database), `--all-features` variants of the build/test steps (so the
  `tls`/`websocket` feature code is actually compiled and tested in CI, not
  only locally), and a `windows-latest` job (fmt/clippy/test, default
  features) so `cli.rs`'s `#[cfg(not(unix))]` branch runs somewhere.
