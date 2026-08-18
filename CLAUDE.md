# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`pulsemq-cli` is **one self-contained command-line MQTT client binary** with
four subcommands:

- `pub` — publish a message, MQTT v5.0 / v3.1.1 / v3.1
- `sub` — subscribe to topic filters and print what arrives, v5.0 / v3.1.1 / v3.1
- `request` — publish a request, then wait on the reply topic and exit,
  **v5.0 / v3.1.1 only** — v3.1 has no Response Topic or Correlation Data
  property
- `bench` — drive publisher and subscriber load and report throughput and
  latency percentiles, v5.0 / v3.1.1 / v3.1

Written from scratch against the OASIS MQTT specifications. This is not a port
or a reimplementation of any other client: no other project's source, option
layout, or output format is a constraint here, and none should be cited as a
reason for a design decision. Where a question is genuinely open, answer it from
the spec.

One binary, not four. The option surface is `clap` derive: long names that say
what they do, short letters only for what is typed often (`-b`, `-p`, `-t`,
`-m`, `-q`, `-r`, `-i`, `-u`, `-k`, `-n`), and clap keeps its own `-h` and `-V`.

**This crate stands alone.** It has no dependency on the PulseMQ broker — not a
path dependency, not a git one. `cargo build` in a fresh clone works with
nothing checked out beside it. The wire format lives in-tree under `src/mqtt`.

`pulsemq` is a separate broker in Rust (https://github.com/elpapimango/pulsemq),
useful as a local server to test against and nothing more. Do not reintroduce a
dependency on it, and do not reach into `../pulsemq` for source: if the client
needs something the codec does not have, add it to `src/mqtt`. Nothing here
should require a PulseMQ-specific server — against any spec-conformant broker
the client must still work.

This is a client — it speaks MQTT over the wire, it does not link a broker or
share its state.

Name things after what they do, and keep protocol-derived identifiers as the
spec spells them (packet and property names) rather than renaming them to match
the product.

Planned work is in [`TODO.md`](TODO.md) — item 1 is a **security audit**
(credential handling and the untrusted-input path). It is the next item, and
comes before new transports or flags.

## Commands

```bash
cargo build                                             # debug build
cargo test                                              # unit + malformed-input tests
cargo fmt --all -- --check                              # formatting
cargo clippy --all-targets --all-features -- -D warnings # lints
```

Keep all four green. No step needs a broker checkout.

`.github/workflows/ci.yml` runs exactly these four on every push and pull
request to `main`, with `RUSTFLAGS: -D warnings`, plus one more: an
`--offline` build. That last step is the standing guard on this crate's
independence — if a broker path dependency ever comes back, it fails there
rather than in someone's fresh clone.

There are no automated end-to-end tests yet (`TODO.md` item 7). To exercise the
real path you need some broker running — any spec-conformant one will do. With
a PulseMQ checked out at `../pulsemq`:

```bash
cargo build --manifest-path ../pulsemq/Cargo.toml --bin pulsemq
../pulsemq/target/debug/pulsemq --listen-addr 127.0.0.1:18830 \
    --admin-addr 127.0.0.1:19001 --db-path /tmp/smoke.db --sys-interval 0 &
cargo run -- sub -b 127.0.0.1 -p 18830 -t 'test/#' -q 1 --show-topic -n 1 &
cargo run -- pub -b 127.0.0.1 -p 18830 -t test/a -m hello -q 1
```

That is a manual smoke test, not a build step: nothing in `cargo build`,
`cargo test`, `fmt` or `clippy` reads `../pulsemq`.

Subscribe before publishing: without a retained message, a subscriber that
arrives late gets nothing, and a test that races this way looks like a client
bug. Same trap in `request` — it subscribes to the reply topic before sending
the request for exactly this reason.

## Architecture

```
src/mqtt/         the wire format, and nothing above it:
                    codec/    Section 1.5 primitives + Section 2.2.2 properties
                    packet/   all 15 control packets, all three versions
                    framing   read_packet / write_packet over AsyncRead/Write
                    types     ProtocolVersion, QoS, ReasonCode
                    error     MqttError: Malformed / Protocol / Reason / Io
src/cli.rs        clap surface: ConnectionArgs (flattened into every subcommand)
                  plus PubArgs / SubArgs / RequestArgs / BenchArgs, and Protocol
src/client.rs     dial + CONNECT handshake + send/recv + packet-id allocation
src/publish.rs    pub: PUBLISH, then finish the QoS 1/2 handshake
src/subscribe.rs  sub: SUBSCRIBE, print, acknowledge — reused by request
src/request.rs    request: subscribe, publish request, wait for the reply
src/bench/        bench, the only concurrent subcommand:
                    mod.rs      Config, Stop, run(): spawn, join, report
                    stats.rs    Samples, Summary, Counters, Report; percentiles
                    payload.rs  the 16-byte measurement header
                    schedule.rs absolute-deadline rate pacing
                    publisher.rs one publisher: write side, ack side, window
                    subscriber.rs one subscriber: subscribe, receive, ack, time
src/error.rs      Usage / Rejected{code} / Disconnected / Unsupported / Mqtt
tests/malformed.rs  the decoder must never panic on hostile bytes
```

`src/mqtt` is the layering boundary: it may not know anything about `cli`,
`client` or the subcommands, and everything above it goes through it rather
than touching bytes. It shares a common ancestor with the PulseMQ broker's
codec, so a fix here is often worth carrying to that repo by hand — but the two
have separate release cycles now and are free to diverge. Do not re-couple them
to keep them in sync.

Everything version-specific belongs in `client.rs` or in `src/mqtt`.
`request` calls `subscribe::subscribe`, `subscribe::acknowledge` and
`subscribe::print_message` rather than repeating them; keep new shared behaviour
factored the same way instead of copying it into a third subcommand.

Argument decisions live in `cli.rs`, including the ones that could be deferred
to the broker: `--persistent-session` `requires` `--client-id`, and `--password`
conflicts with `--password-file`, so clap rejects the combination before a
connection is opened. `ConnectionArgs::password()` is the single place that
decides which password source wins.

`Client::recv_keepalive` reads with a timeout and sends PINGREQ when it expires,
rather than running a concurrent timer task — a waiting client has nothing else
to write, and PINGRESP arrives on the same path. Adding a second writer means
splitting the stream first.

Payloads are arbitrary bytes: `print_message` writes them to stdout unmodified.
Do not route output through a lossy UTF-8 conversion. The single exception is a
terminal, where `write_payload` escapes control characters — a payload reaching
a TTY is not just data, it can carry ANSI sequences. A pipe or a redirect still
gets the exact bytes, so that exception must stay keyed on `IsTerminal` and
never widen into a general transformation.

Broker-supplied limits are not taken on trust. `ConnectionArgs::max_packet_size`
is the one ceiling on an inbound packet, enforced locally *and* advertised in
CONNECT, because `read_packet` sizes its buffer from the length the broker
declares. Anything new that reads packets takes that value rather than reaching
for a constant.

`bench` does not use `Client`. It calls `client::handshake` on a `TcpStream`,
then splits the socket so a publisher can write and read acknowledgements at
once — the concurrency the simple commands deliberately avoid. Keep that
division: `pub`, `sub` and `request` stay sequential and easy to read, and the
concurrent machinery stays in `src/bench/`.

Runtime choice is per subcommand in `main.rs`: current-thread for the three
simple commands, multi-threaded for `bench`. A load generator on one core
measures the load generator.

A `bench` task that fails must say why in terms of what the broker did. The
publisher's ack task carries its read error out rather than dropping it,
because the write side can only observe its in-flight window closing — and
reporting that alone names the symptom furthest from the cause.

## What a client faces

The connection surface these tools have to cover.

- **Transports (five)**: plain TCP, TLS, and mutual TLS on the broker's MQTT
  port; WebSockets and WebSockets-over-TLS on a second port. Both ports can run
  at once against the same sessions. The WebSocket handshake requires the `mqtt`
  subprotocol — a client that omits it is rejected at the HTTP layer, before any
  MQTT packet is exchanged.
- **Authentication**, in the broker's order of precedence: username/password
  (PBKDF2-HMAC-SHA256) if the broker has a password file, else the mutual-TLS
  client-cert CN, else `anonymous` — which the broker may refuse outright. A
  tool needs credentials, a client cert, or neither, so all three paths belong
  in the CLI surface.
- **Authorization** is per-identity, per-topic. A denied publish or subscribe
  comes back as a reason code, not a transport error, and the ACL can change
  underneath a live connection: the broker revokes affected subscriptions and
  disconnects the client with `0x87` (Not authorized). Long-running `sub` must
  report that as an authorization event, not as an unexplained drop.

Everything the broker sends is untrusted input — a hostile or man-in-the-middle
broker reaches the codec, the packet size limit and the terminal. That is why
the security audit is `TODO.md` item 1, why the decoder is now this repo's own
code to audit, and why new parsing code needs a case in `tests/malformed.rs`
alongside it.

## Conventions

- Keep the dependency surface small and justified; prefer std plus what is
  already in use. A dependency not every user needs goes behind a non-default
  Cargo feature, and `cargo tree` must show none of it in the default build.
- Where code implements a spec requirement, cite the section in a comment. The
  OASIS MQTT specs are in [`spec/`](spec/).
- Anything that speaks MQTT must stay correct across all three protocol
  versions. v3.x has no Properties, uses 1-byte CONNACK return codes, omits
  reason codes in (un)subscribe acks, and has no server DISCONNECT.
- Commit and push only when asked. Branch is `main`. End commit messages with
  the `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>` trailer.
