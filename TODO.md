# TODO

Ordered. Pick the top item.

## 1. Security audit — remaining item

The first two passes are done; see "Done" below for what they changed. What
they did not close:

- **Plaintext warning** — nothing says credentials are crossing the wire in
  the clear. Deliberately left out until TLS exists (item 2), so the warning
  has an alternative to point at rather than firing on every authenticated
  connection and being trained away. Lands alongside item 2.

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

## 9. CI: dependency audit and non-Linux coverage

`.github/workflows/ci.yml` runs fmt/clippy/build/test/offline-build, all on
`ubuntu-latest`. Two gaps:

- No RustSec check (`cargo audit` or `cargo deny`) on the dependency tree, so
  a known-vulnerable transitive dependency (tokio, clap, serde_json today;
  `tokio-rustls` once item 2 lands) is not gated in CI, only caught by hand.
- The `#[cfg(unix)]` / `#[cfg(not(unix))]` split in `cli.rs` (world-readable
  password-file warning, environment variable byte handling) has no CI runner
  that exercises the `not(unix)` branch, so it can silently rot.

## 10. Stale comment in `src/mqtt/packet/publish.rs`

The doc comment on `Publish::payload` (lines 21–26) describes broker-only
behaviour that does not exist in this crate — "routing builds one `Publish`
per recipient", "fan-out to 100 subscribers", and a citation of
`tests/bench_routing.rs`, a file that exists only in the PulseMQ broker's copy
of this codec, not here. Leftover from the shared ancestor mentioned in
`CLAUDE.md`. Replace with a comment about why this crate's own `Publish` uses
`Arc<[u8]>` (cheap `Clone` across the QoS 1/2 retransmission path and
`bench`'s per-publisher payload reuse) or drop the justification if it no
longer applies here.

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
