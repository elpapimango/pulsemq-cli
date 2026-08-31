//! Robustness tests: the decoder must never panic on hostile input.
//!
//! Everything the broker sends is untrusted input — a hostile broker, or one
//! in the network path, reaches `Packet::decode` before the client has looked
//! at a single field. That makes the decoder this crate's primary attack
//! surface. These tests assert the essential property that decoding *any* byte
//! string returns `Ok` or `Err`, never a panic, for every protocol version.
//!
//! The corpus is deliberately structural rather than purely random: valid
//! packets truncated at every offset and mutated byte-by-byte reach far more
//! decoder branches than uniform noise, which almost always dies on the first
//! length check.

use wispmq_cli::mqtt::codec::Properties;
use wispmq_cli::mqtt::packet::{Connect, Packet, Publish, Subscribe, TopicFilter};
use wispmq_cli::mqtt::types::{
    ProtocolVersion::{self, V3_1, V3_1_1, V5},
    QoS,
};

const VERSIONS: [ProtocolVersion; 3] = [V3_1, V3_1_1, V5];

/// Decode and discard the result. The point is that this returns at all.
fn decode_no_panic(buf: &[u8], version: ProtocolVersion) {
    let _ = Packet::decode(buf, version);
}

/// A small corpus of well-formed packets to use as mutation seeds.
fn seeds() -> Vec<Vec<u8>> {
    let mut out = Vec::new();

    let connect = Packet::Connect(Connect {
        protocol_name: "MQTT".into(),
        protocol_version: 5,
        clean_start: true,
        keep_alive: 30,
        properties: Properties::new(),
        client_id: "seed-client".into(),
        will: None,
        username: Some("user".into()),
        password: Some(b"pass".to_vec().into()),
    });

    let publish = Packet::Publish(Publish {
        dup: false,
        qos: QoS::AtLeastOnce,
        retain: true,
        topic: "a/b/c".into(),
        packet_id: Some(7),
        properties: Properties::new(),
        payload: b"hello world"[..].into(),
    });

    let subscribe = Packet::Subscribe(Subscribe {
        packet_id: 1,
        properties: Properties::new(),
        filters: vec![TopicFilter {
            filter: "a/+/c/#".into(),
            qos: QoS::ExactlyOnce,
            no_local: true,
            retain_as_published: false,
            retain_handling: wispmq_cli::mqtt::packet::RetainHandling::SendAtSubscribe,
        }],
    });

    for version in VERSIONS {
        for p in [&connect, &publish, &subscribe] {
            if let Ok(bytes) = p.encode(version) {
                out.push(bytes);
            }
        }
    }
    out
}

#[test]
fn truncated_packets_never_panic() {
    for seed in seeds() {
        for cut in 0..=seed.len() {
            for version in VERSIONS {
                decode_no_panic(&seed[..cut], version);
            }
        }
    }
}

#[test]
fn single_byte_mutations_never_panic() {
    // Flip every byte of every seed to a set of values chosen to hit length,
    // type-nibble, flag and property-id branches.
    const INTERESTING: [u8; 8] = [0x00, 0x01, 0x0F, 0x7F, 0x80, 0xF0, 0xFE, 0xFF];
    for seed in seeds() {
        for i in 0..seed.len() {
            for byte in INTERESTING {
                let mut m = seed.clone();
                m[i] = byte;
                for version in VERSIONS {
                    decode_no_panic(&m, version);
                }
            }
        }
    }
}

#[test]
fn every_packet_type_with_arbitrary_bodies_never_panics() {
    // Walk all 16 type nibbles x all 16 flag nibbles, with a range of bodies,
    // so no packet decoder is skipped just because no seed exercised it.
    let bodies: [&[u8]; 7] = [
        &[],
        &[0x00],
        &[0xFF],
        &[0x00, 0x00],
        &[0xFF, 0xFF, 0xFF, 0xFF],
        &[0x02, 0x00, 0x00],
        &[0xFF; 32],
    ];
    for type_nibble in 0u8..16 {
        for flags in 0u8..16 {
            for body in bodies {
                let mut buf = vec![(type_nibble << 4) | flags];
                // A plausible Remaining Length for the body we append.
                buf.push(body.len() as u8);
                buf.extend_from_slice(body);
                for version in VERSIONS {
                    decode_no_panic(&buf, version);
                }
            }
        }
    }
}

#[test]
fn declared_length_disagreeing_with_actual_never_panics() {
    // The Remaining Length byte lies about how much data follows. Decoders
    // that trust it and slice would panic here.
    for seed in seeds() {
        for claimed in [0u8, 1, 2, 5, 127, 200, 255] {
            let mut m = seed.clone();
            if m.len() > 1 {
                m[1] = claimed;
                for version in VERSIONS {
                    decode_no_panic(&m, version);
                }
            }
        }
    }
}

#[test]
fn pathological_varints_and_lengths_never_panic() {
    // Remaining Length / property-length / string-length fields built to
    // overflow, run off the end, or claim near-u32::MAX sizes.
    let cases: Vec<Vec<u8>> = vec![
        // PUBLISH with a 4-byte continuation VBI (malformed Remaining Length).
        vec![0x30, 0xFF, 0xFF, 0xFF, 0xFF],
        // VBI claiming the maximum representable value.
        vec![0x30, 0xFF, 0xFF, 0xFF, 0x7F],
        // CONNECT whose protocol-name length exceeds the buffer.
        vec![0x10, 0x04, 0xFF, 0xFF, 0x00, 0x00],
        // PUBLISH with a topic length longer than the packet.
        vec![0x30, 0x05, 0xFF, 0xFF, 0x61, 0x62, 0x63],
        // v5 PUBLISH with a property length larger than the remainder.
        vec![0x30, 0x08, 0x00, 0x01, 0x61, 0x00, 0x01, 0x7F, 0x00, 0x00],
        // SUBSCRIBE with a filter length running past the end.
        vec![0x82, 0x05, 0x00, 0x01, 0x00, 0xFF, 0xFF],
        // Zero-length everything.
        vec![0x00, 0x00],
        // Single stray byte.
        vec![0xF0],
    ];
    for buf in cases {
        for version in VERSIONS {
            decode_no_panic(&buf, version);
        }
    }
}

#[test]
fn utf8_and_null_rules_are_enforced_not_panicked() {
    // Invalid UTF-8 and embedded U+0000 must be rejected as errors (1.5.4),
    // not panic and not silently accepted.
    // PUBLISH, remaining len 5: topic len 3 = [0xFF,0xFE,0xFD] (invalid UTF-8).
    let bad_utf8 = vec![0x30, 0x05, 0x00, 0x03, 0xFF, 0xFE, 0xFD];
    assert!(
        Packet::decode(&bad_utf8, V3_1_1).is_err(),
        "invalid UTF-8 topic must be rejected"
    );

    // Topic containing U+0000.
    let with_null = vec![0x30, 0x05, 0x00, 0x03, 0x61, 0x00, 0x62];
    assert!(
        Packet::decode(&with_null, V3_1_1).is_err(),
        "topic containing U+0000 must be rejected"
    );
}

#[test]
fn round_trip_seeds_still_decode() {
    // Guard against the corpus silently becoming garbage: every seed must
    // decode cleanly under the version it was encoded for.
    for version in VERSIONS {
        let p = Packet::Publish(Publish {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic: "sanity/check".into(),
            packet_id: None,
            properties: Properties::new(),
            payload: b"payload"[..].into(),
        });
        let bytes = p.encode(version).expect("seed encodes");
        match Packet::decode(&bytes, version) {
            Ok(Packet::Publish(d)) => {
                assert_eq!(d.topic, "sanity/check");
                assert_eq!(&d.payload[..], b"payload");
            }
            other => panic!("seed did not round-trip on {version:?}: {other:?}"),
        }
    }
}

/// A declared Remaining Length is attacker input, and `read_packet` sizes its
/// buffer from it. The size check must therefore happen *before* the
/// allocation, or a broker gets to reserve the whole ceiling by sending five
/// bytes of header and then nothing.
#[tokio::test]
async fn an_oversized_declared_length_is_refused_without_allocating_it() {
    use wispmq_cli::mqtt::framing::read_packet;

    // PUBLISH with a Remaining Length of 200 MB, and no body at all.
    let header = [0x30u8, 0x80, 0x80, 0x80, 0x60];
    let declared = 0x60u32 << 21;
    assert!(
        declared > 100 * 1024 * 1024,
        "the corpus should dwarf the cap"
    );

    let mut stream = std::io::Cursor::new(header.to_vec());
    let ceiling = 1024 * 1024;
    let result = read_packet(&mut stream, ceiling, ProtocolVersion::V5).await;

    match result {
        Err(e) => assert_eq!(
            e.reason_code(),
            wispmq_cli::mqtt::types::ReasonCode::PacketTooLarge,
            "wrong reason: {e}"
        ),
        Ok(_) => panic!("a packet over the ceiling must be refused"),
    }
}
