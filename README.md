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

# measure a broker: 4 publishers, 2 subscribers, 20k messages at QoS 1
pulsemq-cli bench --publishers 4 --subscribers 2 --count 20000 --qos 1

# hold 500 messages/second for 30 seconds and emit a machine-readable report
pulsemq-cli bench --duration 30 --rate 500 --subscribers 1 --json
```

Short forms exist for what gets typed often: `-b` broker, `-p` port, `-t` topic,
`-m` message, `-q` QoS, `-r` retain, `-i` client id, `-u` user, `-k` keepalive,
`-n` count. `pulsemq-cli <command> --help` lists the rest.

Payloads are written to stdout as received, without a UTF-8 conversion. Exit
status is 0 on success and 1 on any error, with the reason on stderr —
including the broker's Reason Code when a connection, publish or subscription is
refused.

## Measuring a broker

`bench` opens N publishing and M subscribing connections and reports what the
broker did with them: messages and bytes per second, and latency as a
distribution rather than a mean — publish-to-PUBACK from the publishers, and
end-to-end from a timestamp written into each payload and read back by the
subscribers.

The run ends after `--count` messages, after `--duration` seconds, or on Ctrl-C;
whichever happens, the report still prints for what completed. `--rate` paces
the offered load and is split across the publishers. `--json` emits the same
numbers as a single object, so one run can be diffed against another.

Two knobs matter for accuracy. `--warmup` discards samples from the first
seconds, so connection setup does not land in the percentiles. `--inflight`
bounds the unacknowledged messages a publisher keeps outstanding; the broker's
Receive Maximum caps it further, and a window of 1 turns the publisher into a
strict round-trip loop.

Subscribers connect and subscribe before any publisher starts, and deliveries
still in flight get a `--drain` window after the last publish, so a message that
arrives late is counted rather than reported as lost.

`bench` accepts QoS 0 and 1. QoS 2 is rejected rather than half-measured — see
`TODO.md`.

## Status

Working: TCP, all three protocol versions, QoS 0/1/2 in both directions,
retained messages, username/password authentication, and a `bench` mode that
reports throughput plus exact latency percentiles.

Not yet: TLS and mutual TLS, WebSocket transport, will messages, v5 user
properties, and payloads from a file or stdin. A security audit of credential
handling and of the untrusted-input path is the first open item — see
`TODO.md`.

## License

MIT OR Apache-2.0.
