//! SUBSCRIBE (3.8), SUBACK (3.9), UNSUBSCRIBE (3.10), UNSUBACK (3.11).

use crate::mqtt::codec::{Properties, Reader, Writer};
use crate::mqtt::error::{malformed, Result};
use crate::mqtt::types::{ProtocolVersion, QoS, ReasonCode};

/// Retain Handling option in a Subscription Options byte (3.8.3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainHandling {
    /// Send retained messages at the time of the subscribe.
    SendAtSubscribe = 0,
    /// Send retained messages only if the subscription did not already exist.
    SendIfNewSubscription = 1,
    /// Do not send retained messages at subscribe time.
    DoNotSend = 2,
}

impl RetainHandling {
    fn from_u8(v: u8) -> Result<Self> {
        Ok(match v {
            0 => RetainHandling::SendAtSubscribe,
            1 => RetainHandling::SendIfNewSubscription,
            2 => RetainHandling::DoNotSend,
            _ => return Err(malformed("invalid Retain Handling value 3")),
        })
    }
}

/// One entry in a SUBSCRIBE payload: a Topic Filter plus its options.
#[derive(Debug, Clone)]
pub struct TopicFilter {
    pub filter: String,
    pub qos: QoS,
    pub no_local: bool,
    pub retain_as_published: bool,
    pub retain_handling: RetainHandling,
}

#[derive(Debug, Clone)]
pub struct Subscribe {
    pub packet_id: u16,
    pub properties: Properties,
    pub filters: Vec<TopicFilter>,
}

impl Subscribe {
    pub fn decode(r: &mut Reader, version: ProtocolVersion) -> Result<Subscribe> {
        let packet_id = r.u16()?;
        if packet_id == 0 {
            return Err(malformed("SUBSCRIBE with Packet Identifier 0"));
        }
        let properties = if version.has_properties() {
            Properties::decode(r)?
        } else {
            Properties::new()
        };
        let mut filters = Vec::new();
        while r.has_remaining() {
            let filter = r.utf8()?;
            let opts = r.u8()?;
            // In v3.x only bits 0-1 (QoS) are defined; bits 2-7 are reserved.
            // In v5, bits 2-5 carry No Local / Retain As Published / Retain
            // Handling, and bits 6-7 are reserved and MUST be 0 [MQTT-3.8.3-5].
            let (no_local, retain_as_published, retain_handling) = if version.has_properties() {
                if opts & 0xC0 != 0 {
                    return Err(malformed("reserved bits set in Subscription Options"));
                }
                (
                    opts & 0x04 != 0,
                    opts & 0x08 != 0,
                    RetainHandling::from_u8((opts & 0x30) >> 4)?,
                )
            } else {
                (false, false, RetainHandling::SendAtSubscribe)
            };
            let qos = QoS::from_u8(opts & 0x03)?;
            filters.push(TopicFilter {
                filter,
                qos,
                no_local,
                retain_as_published,
                retain_handling,
            });
        }
        // [MQTT-3.8.3-2] MUST contain at least one Topic Filter.
        if filters.is_empty() {
            return Err(malformed("SUBSCRIBE with no Topic Filters"));
        }
        Ok(Subscribe {
            packet_id,
            properties,
            filters,
        })
    }

    pub fn encode_body(&self, version: ProtocolVersion) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        w.put_u16(self.packet_id);
        if version.has_properties() {
            self.properties.encode(&mut w)?;
        }
        for f in &self.filters {
            w.put_utf8(&f.filter);
            let mut opts = f.qos.as_u8();
            if version.has_properties() {
                if f.no_local {
                    opts |= 0x04;
                }
                if f.retain_as_published {
                    opts |= 0x08;
                }
                opts |= (f.retain_handling as u8) << 4;
            }
            w.put_u8(opts);
        }
        Ok(w.into_vec())
    }
}

/// SUBACK and UNSUBACK share this shape: a packet id, properties, and a list
/// of Reason Codes (one per requested filter).
#[derive(Debug, Clone)]
pub struct SubAck {
    pub packet_id: u16,
    pub properties: Properties,
    pub reason_codes: Vec<ReasonCode>,
}

impl SubAck {
    pub fn new(packet_id: u16, reason_codes: Vec<ReasonCode>) -> Self {
        SubAck {
            packet_id,
            properties: Properties::new(),
            reason_codes,
        }
    }

    /// Decode a SUBACK (`unsuback == false`) or UNSUBACK (`true`). In v3.x,
    /// UNSUBACK carries no reason codes at all — just the Packet Identifier.
    pub fn decode(r: &mut Reader, version: ProtocolVersion, unsuback: bool) -> Result<SubAck> {
        let packet_id = r.u16()?;
        let properties = if version.has_properties() {
            Properties::decode(r)?
        } else {
            Properties::new()
        };
        let mut reason_codes = Vec::new();
        // v3.x UNSUBACK has no payload after the packet id.
        if !unsuback || version.has_properties() {
            while r.has_remaining() {
                reason_codes.push(ReasonCode::from_u8(r.u8()?)?);
            }
        }
        Ok(SubAck {
            packet_id,
            properties,
            reason_codes,
        })
    }

    pub fn encode_body(&self, version: ProtocolVersion, unsuback: bool) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        w.put_u16(self.packet_id);
        if version.has_properties() {
            self.properties.encode(&mut w)?;
        }
        // v3.x UNSUBACK is just the Packet Identifier (no reason codes).
        if unsuback && !version.has_properties() {
            return Ok(w.into_vec());
        }
        for rc in &self.reason_codes {
            // v3.x SUBACK return codes: 0/1/2 for granted QoS, 0x80 for failure.
            let byte = if version.has_properties() {
                rc.as_u8()
            } else if rc.is_error() {
                0x80
            } else {
                rc.as_u8()
            };
            w.put_u8(byte);
        }
        Ok(w.into_vec())
    }
}

#[derive(Debug, Clone)]
pub struct Unsubscribe {
    pub packet_id: u16,
    pub properties: Properties,
    pub filters: Vec<String>,
}

impl Unsubscribe {
    pub fn decode(r: &mut Reader, version: ProtocolVersion) -> Result<Unsubscribe> {
        let packet_id = r.u16()?;
        if packet_id == 0 {
            return Err(malformed("UNSUBSCRIBE with Packet Identifier 0"));
        }
        let properties = if version.has_properties() {
            Properties::decode(r)?
        } else {
            Properties::new()
        };
        let mut filters = Vec::new();
        while r.has_remaining() {
            filters.push(r.utf8()?);
        }
        if filters.is_empty() {
            return Err(malformed("UNSUBSCRIBE with no Topic Filters"));
        }
        Ok(Unsubscribe {
            packet_id,
            properties,
            filters,
        })
    }

    pub fn encode_body(&self, version: ProtocolVersion) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        w.put_u16(self.packet_id);
        if version.has_properties() {
            self.properties.encode(&mut w)?;
        }
        for f in &self.filters {
            w.put_utf8(f);
        }
        Ok(w.into_vec())
    }
}
