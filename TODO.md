# TODO

Ordered. Pick the top item.

## 1. Security audit — remaining items

The first pass is done; see "Done" below for what it changed. What it did not
close:

- **Credential lifetime** — the password lives in a `Vec<u8>` until drop rather
  than being zeroed once the CONNECT is encoded. Doing it properly means either
  the `zeroize` dependency or a hand-written `Drop`, and neither stops the
  compiler from having left a copy behind during a move or a realloc. Decide
  whether the guarantee is worth the dependency.
- **Unbounded `sub`** — `sub` without `--count` runs forever and buffers
  nothing, so there is no leak, but there is also no ceiling on how long a
  hostile broker can hold the terminal. Decide whether a `--max-messages` or a
  time limit belongs here.
- **Plaintext warning** — nothing says credentials are crossing the wire in the
  clear. Deliberately left out for now: until TLS exists (item 2) the warning
  would fire on every authenticated connection and be trained away. Revisit
  when there is an alternative to point at.

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
- **Security audit, first pass** — `--max-packet-size` (1 MiB default, also
  advertised as the v5 Maximum Packet Size) closes a 256 MB allocation a broker
  could demand; control characters are escaped when stdout is a terminal, while
  pipes and redirects stay byte-exact; `PULSEMQ_PASSWORD` joins the credential
  sources and a world-readable password file warns. The decoder was audited and
  found clean: every `Reader` access is bounds-checked and no `unwrap`, index
  or unchecked cast sits on the broker-input path.
