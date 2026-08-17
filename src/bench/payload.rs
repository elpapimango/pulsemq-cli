//! The measurement header carried in every benchmark payload.
//!
//! Publisher and subscriber run in one process, so a single baseline `Instant`
//! serves both and end-to-end latency needs no clock synchronisation. The
//! header is the elapsed nanoseconds since that baseline, plus enough identity
//! to attribute a message to its publisher.

/// Bytes occupied by the header at the front of every benchmark payload.
pub const HEADER_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    /// Nanoseconds since the run's baseline `Instant`.
    pub elapsed_ns: u64,
    pub publisher: u32,
    pub seq: u32,
}

/// Build a payload of `size` bytes carrying `header`. Big-endian throughout,
/// matching how MQTT itself encodes multi-byte integers.
///
/// A `size` below `HEADER_LEN` yields a `HEADER_LEN` payload rather than a
/// truncated header; `Config` is what decides whether such a run measures
/// end-to-end latency at all.
pub fn build(size: usize, header: Header) -> Vec<u8> {
    let mut buf = vec![0u8; size.max(HEADER_LEN)];
    buf[0..8].copy_from_slice(&header.elapsed_ns.to_be_bytes());
    buf[8..12].copy_from_slice(&header.publisher.to_be_bytes());
    buf[12..16].copy_from_slice(&header.seq.to_be_bytes());
    buf
}

/// Read the header back. `None` when the payload is too short to carry one,
/// which is the case for any message this run did not publish.
pub fn decode(payload: &[u8]) -> Option<Header> {
    if payload.len() < HEADER_LEN {
        return None;
    }
    Some(Header {
        elapsed_ns: u64::from_be_bytes(payload[0..8].try_into().ok()?),
        publisher: u32::from_be_bytes(payload[8..12].try_into().ok()?),
        seq: u32::from_be_bytes(payload[12..16].try_into().ok()?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_survives_a_round_trip() {
        let header = Header {
            elapsed_ns: 1_234_567_890,
            publisher: 7,
            seq: 42,
        };
        let payload = build(64, header);
        assert_eq!(payload.len(), 64);
        assert_eq!(decode(&payload), Some(header));
    }

    #[test]
    fn payload_smaller_than_the_header_is_filled_to_header_length() {
        // build() never truncates the header: a short --payload-size means the
        // payload is HEADER_LEN, and Config decides whether to measure at all.
        let payload = build(
            4,
            Header {
                elapsed_ns: 1,
                publisher: 0,
                seq: 0,
            },
        );
        assert_eq!(payload.len(), HEADER_LEN);
    }

    #[test]
    fn decoding_a_short_payload_returns_none() {
        assert_eq!(decode(&[0u8; 15]), None);
        assert_eq!(decode(&[]), None);
    }

    #[test]
    fn filler_is_deterministic_so_runs_are_comparable() {
        let a = build(
            128,
            Header {
                elapsed_ns: 1,
                publisher: 1,
                seq: 1,
            },
        );
        let b = build(
            128,
            Header {
                elapsed_ns: 1,
                publisher: 1,
                seq: 1,
            },
        );
        assert_eq!(a, b);
    }
}
