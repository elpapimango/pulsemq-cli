//! PUBLISH (3.3) and the acknowledgement family PUBACK/PUBREC/PUBREL/PUBCOMP
//! (3.4 – 3.7), which share an identical structure.

use std::sync::Arc;

use crate::mqtt::codec::{Properties, Reader, Writer};
use crate::mqtt::error::{malformed, Result};
use crate::mqtt::types::{ProtocolVersion, QoS, ReasonCode};

#[derive(Debug, Clone)]
pub struct Publish {
    pub dup: bool,
    pub qos: QoS,
    pub retain: bool,
    pub topic: String,
    /// Present iff QoS > 0.
    pub packet_id: Option<u16>,
    pub properties: Properties,
    /// Application Message payload (3.3.3).
    ///
    /// `Arc<[u8]>` rather than `Vec<u8>` because routing builds one `Publish`
    /// per recipient from the same message, and QoS>0 clones it again into the
    /// retransmission buffer. With a `Vec` each of those copied the payload;
    /// fan-out to 100 subscribers of a 64 KiB message cost ~51 us per recipient
    /// (see `tests/bench_routing.rs`). Payloads are never mutated after decode,
    /// so sharing is free.
    pub payload: Arc<[u8]>,
}

impl Publish {
    pub fn decode(flags: u8, r: &mut Reader, version: ProtocolVersion) -> Result<Publish> {
        let dup = flags & 0x08 != 0;
        let qos = QoS::from_u8((flags & 0x06) >> 1)?;
        let retain = flags & 0x01 != 0;

        // [MQTT-3.3.1-2] DUP MUST be 0 for QoS 0.
        if qos == QoS::AtMostOnce && dup {
            return Err(malformed("DUP set on a QoS 0 PUBLISH"));
        }

        let topic = r.utf8()?;
        let packet_id = if qos == QoS::AtMostOnce {
            None
        } else {
            let id = r.u16()?;
            if id == 0 {
                return Err(malformed("Packet Identifier 0 on a QoS>0 PUBLISH"));
            }
            Some(id)
        };
        let properties = if version.has_properties() {
            Properties::decode(r)?
        } else {
            Properties::new()
        };
        let payload: Arc<[u8]> = Arc::from(r.rest());

        Ok(Publish {
            dup,
            qos,
            retain,
            topic,
            packet_id,
            properties,
            payload,
        })
    }

    pub fn fixed_flags(&self) -> u8 {
        let mut f = 0u8;
        if self.dup {
            f |= 0x08;
        }
        f |= self.qos.as_u8() << 1;
        if self.retain {
            f |= 0x01;
        }
        f
    }

    pub fn encode_body(&self, version: ProtocolVersion) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        w.put_utf8(&self.topic);
        if self.qos != QoS::AtMostOnce {
            let id = self
                .packet_id
                .ok_or_else(|| malformed("QoS>0 PUBLISH missing packet id"))?;
            w.put_u16(id);
        }
        if version.has_properties() {
            self.properties.encode(&mut w)?;
        }
        w.put_bytes(&self.payload);
        Ok(w.into_vec())
    }
}

/// Shared representation for PUBACK / PUBREC / PUBREL / PUBCOMP.
///
/// Per 3.4.2.1, the Reason Code and Property Length can be omitted when the
/// Reason Code is 0x00 and there are no properties. This is preserved on
/// decode and applied on encode for compactness.
#[derive(Debug, Clone)]
pub struct PubAck {
    pub packet_id: u16,
    pub reason_code: ReasonCode,
    pub properties: Properties,
}

impl PubAck {
    pub fn new(packet_id: u16, reason_code: ReasonCode) -> Self {
        PubAck {
            packet_id,
            reason_code,
            properties: Properties::new(),
        }
    }

    pub fn decode(r: &mut Reader, remaining_len: usize) -> Result<PubAck> {
        let packet_id = r.u16()?;
        // Remaining Length of 2 => Reason Code 0x00, no properties.
        if remaining_len == 2 {
            return Ok(PubAck::new(packet_id, ReasonCode::Success));
        }
        let reason_code = ReasonCode::from_u8(r.u8()?)?;
        // Remaining Length of 3 => Reason Code present, no property length.
        if remaining_len == 3 {
            return Ok(PubAck {
                packet_id,
                reason_code,
                properties: Properties::new(),
            });
        }
        let properties = Properties::decode(r)?;
        Ok(PubAck {
            packet_id,
            reason_code,
            properties,
        })
    }

    pub fn encode_body(&self, version: ProtocolVersion) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        w.put_u16(self.packet_id);
        // v3.x carries only the Packet Identifier (no Reason Code / properties).
        if !version.is_v5() {
            return Ok(w.into_vec());
        }
        // v5: omit trailing fields when Reason Code is Success and no properties.
        if self.reason_code == ReasonCode::Success && self.properties.is_empty() {
            return Ok(w.into_vec());
        }
        w.put_u8(self.reason_code.as_u8());
        self.properties.encode(&mut w)?;
        Ok(w.into_vec())
    }
}
