# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`pulsemq_tools` builds **one command-line MQTT client binary**, `pulsemq-cli`,
with three subcommands:

- `pub` — publish a message, MQTT v5.0 / v3.1.1 / v3.1
- `sub` — subscribe to topic filters and print what arrives, v5.0 / v3.1.1 / v3.1
- `request` — publish a request, then wait on the reply topic and exit,
  **v5.0 / v3.1.1 only** — v3.1 has no Response Topic or Correlation Data
  property

Written from scratch against the OASIS MQTT specifications. This is not a port
or a reimplementation of any other client: no other project's source, option
layout, or output format is a constraint here, and none should be cited as a
reason for a design decision. Where a question is genuinely open, answer it from
the spec.

One binary, not three — `pulsemq-cli` is the client; `pulsemq` is the broker in
the other repo. The option surface is `clap` derive: long names that say what
they do, short letters only for what is typed often (`-b`, `-p`, `-t`, `-m`,
`-q`, `-r`, `-i`, `-u`, `-k`, `-n`), and clap keeps its own `-h` and `-V`.

These are clients — they speak MQTT over the wire, they do not link the broker
or share its state.

The tools target **PulseMQ**, an MQTT broker in Rust (protocols v5.0, v3.1.1,
v3.1), which lives in a separate repo: https://github.com/elpapimango/pulsemq,
checked out locally at `../pulsemq`. Read its `CLAUDE.md` before writing
anything that depends on broker internals. Nothing here should require a
PulseMQ-specific server: against any spec-conformant broker the tools must still
work.

Name things after what they do, and keep protocol-derived identifiers as the
spec spells them (packet and property names) rather than renaming them to match
the product.

Planned work is in [`TODO.md`](TODO.md) — item 1 is a **security audit**
(credential handling and the untrusted-input path) and item 2 is a
**performance-testing mode**. Both come before new transports or flags.

## Commands

```bash
cargo build                                             # debug build
cargo test                                              # unit tests
cargo fmt --all -- --check                              # formatting
cargo clippy --all-targets --all-features -- -D warnings # lints
```

Keep all four green — the broker repo's CI enforces the same set and this crate
is held to it.

There are no automated end-to-end tests yet (`TODO.md` item 8). To exercise the
real path, run the broker from the sibling repo and drive it:

```bash
cargo build --manifest-path ../pulsemq/Cargo.toml --bin pulsemq
../pulsemq/target/debug/pulsemq --listen-addr 127.0.0.1:18830 \
    --admin-addr 127.0.0.1:19001 --db-path /tmp/smoke.db --sys-interval 0 &
cargo run -- sub -b 127.0.0.1 -p 18830 -t 'test/#' -q 1 --show-topic -n 1 &
cargo run -- pub -b 127.0.0.1 -p 18830 -t test/a -m hello -q 1
```

Subscribe before publishing: without a retained message, a subscriber that
arrives late gets nothing, and a test that races this way looks like a client
bug. Same trap in `request` — it subscribes to the reply topic before sending
the request for exactly this reason.

## Architecture

```
src/cli.rs        clap surface: ConnectionArgs (flattened into every subcommand)
                  plus PubArgs / SubArgs / RequestArgs, and the Protocol enum
src/client.rs     dial + CONNECT handshake + send/recv + packet-id allocation
src/publish.rs    pub: PUBLISH, then finish the QoS 1/2 handshake
src/subscribe.rs  sub: SUBSCRIBE, print, acknowledge — reused by request
src/request.rs    request: subscribe, publish request, wait for the reply
src/error.rs      Usage / Rejected{code} / Disconnected / Unsupported / Mqtt
```

The wire format is **not** implemented here. `pulsemq` is a path dependency and
supplies `packet` (all 15 control packets), `codec` (properties and primitives),
`framing` (`read_packet`/`write_packet`) and `types` (`ProtocolVersion`, `QoS`,
`ReasonCode`). One codec means client and broker cannot disagree about the wire;
a bug found here often belongs in the broker repo. The cost is that a client
build compiles the broker's whole dependency tree, bundled SQLite included —
fix that by adding a `client` feature to the broker crate, not by forking the
codec.

Everything version-specific belongs in `client.rs` or in the `pulsemq` codec.
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
Do not route output through a lossy UTF-8 conversion.

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
the security audit is `TODO.md` item 1, and why new parsing code needs a
malformed-input test alongside it.

## Conventions

- Keep the dependency surface small and justified; prefer std plus what is
  already in use. A dependency not every user needs goes behind a non-default
  Cargo feature, and `cargo tree` must show none of it in the default build.
- Where code implements a spec requirement, cite the section in a comment. The
  OASIS MQTT specs are in `../pulsemq/spec/`.
- Anything that speaks MQTT must stay correct across all three protocol
  versions. v3.x has no Properties, uses 1-byte CONNACK return codes, omits
  reason codes in (un)subscribe acks, and has no server DISCONNECT.
- Commit and push only when asked. Branch is `main`. End commit messages with
  the `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>` trailer.
