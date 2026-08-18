# pulsemq-cli

A command-line MQTT client for **v5.0**, **v3.1.1** and **v3.1**: publish a
message, subscribe to topics, or do one request/reply round trip. One binary
with three subcommands.

Written from scratch in Rust against the OASIS MQTT specifications, including
the wire format: the codec, the 15 control packets and the framing all live in
this crate under `src/mqtt`. It speaks to any spec-conformant broker.

## Build

```
cargo build --release
```

No other checkout, no C toolchain, no system libraries. The dependencies are
`tokio`, `clap` and `serde_json`.

## Use

```bash
# publish
pulsemq-cli pub --broker localhost --topic sensors/temp --message 21.5 --qos 1

# subscribe, printing the topic before each payload, exit after 10 messages
pulsemq-cli sub --topic 'sensors/#' --qos 1 --show-topic --count 10

# request/reply: subscribes to the reply topic before sending the request
pulsemq-cli request --topic service/request --reply-topic service/reply -m ping

# speak an older protocol version
pulsemq-cli pub --topic test --message hello --protocol 3.1.1
```

Short forms exist for what gets typed often: `-b` broker, `-p` port, `-t` topic,
`-m` message, `-q` QoS, `-r` retain, `-i` client id, `-u` user, `-k` keepalive,
`-n` count. `pulsemq-cli <command> --help` lists the rest.

Payloads are written to stdout as received, without a UTF-8 conversion. Exit
status is 0 on success and 1 on any error, with the reason on stderr —
including the broker's Reason Code when a connection, publish or subscription is
refused.

## Status

Working: TCP, all three protocol versions, QoS 0/1/2 in both directions,
retained messages, username/password authentication.

Not yet: TLS and mutual TLS, WebSocket transport, will messages, v5 user
properties, payloads from a file or stdin, and a load-generating mode for
broker performance testing. A security audit of credential handling and of the
untrusted-input path is the first open item — see `TODO.md`.

## License

MIT OR Apache-2.0.
