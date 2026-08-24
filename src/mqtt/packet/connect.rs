//! CONNECT (3.1) and CONNACK (3.2).

use crate::mqtt::codec::{Properties, Reader, Writer};
use crate::mqtt::error::{malformed, protocol, Result};
use crate::mqtt::secret::SecretBytes;
use crate::mqtt::types::{ProtocolVersion, QoS, ReasonCode};

/// The Will Message carried in a CONNECT payload (3.1.3.2 – 3.1.3.4).
#[derive(Debug, Clone)]
pub struct Will {
    pub qos: QoS,
    pub retain: bool,
    pub properties: Properties,
    pub topic: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct Connect {
    pub protocol_name: String,
    pub protocol_version: u8,
    pub clean_start: bool,
    pub keep_alive: u16,
    pub properties: Properties,
    pub client_id: String,
    pub will: Option<Will>,
    pub username: Option<String>,
    pub password: Option<SecretBytes>,
}

impl Connect {
    pub fn decode(r: &mut Reader) -> Result<Connect> {
        // Variable header
        let protocol_name = r.utf8()?;
        let protocol_version = r.u8()?;
        let connect_flags = r.u8()?;

        // [MQTT-3.1.2-3] reserved bit 0 MUST be 0.
        if connect_flags & 0x01 != 0 {
            return Err(malformed("CONNECT reserved flag is set"));
        }
        let clean_start = connect_flags & 0x02 != 0;
        let will_flag = connect_flags & 0x04 != 0;
        let will_qos = QoS::from_u8((connect_flags & 0x18) >> 3)?;
        let will_retain = connect_flags & 0x20 != 0;
        let password_flag = connect_flags & 0x40 != 0;
        let username_flag = connect_flags & 0x80 != 0;

        if !will_flag && (will_qos != QoS::AtMostOnce || will_retain) {
            // [MQTT-3.1.2-11 / -13/-15]
            return Err(malformed("Will QoS/Retain set but Will Flag is 0"));
        }

        let keep_alive = r.u16()?;
        // Properties exist only in MQTT v5 (protocol level 5). v3.1/v3.1.1 have
        // none. The level is self-describing, so we branch on it here.
        let has_properties = protocol_version == 5;
        let properties = if has_properties {
            Properties::decode(r)?
        } else {
            Properties::new()
        };

        // Payload
        let client_id = r.utf8()?;

        let will = if will_flag {
            let will_properties = if has_properties {
                Properties::decode(r)?
            } else {
                Properties::new()
            };
            let topic = r.utf8()?;
            let payload = r.binary()?;
            Some(Will {
                qos: will_qos,
                retain: will_retain,
                properties: will_properties,
                topic,
                payload,
            })
        } else {
            None
        };

        let username = if username_flag { Some(r.utf8()?) } else { None };
        let password = if password_flag {
            Some(SecretBytes::from(r.binary()?))
        } else {
            None
        };

        Ok(Connect {
            protocol_name,
            protocol_version,
            clean_start,
            keep_alive,
            properties,
            client_id,
            will,
            username,
            password,
        })
    }

    pub fn encode_body(&self) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        w.put_utf8(&self.protocol_name);
        w.put_u8(self.protocol_version);

        let mut flags = 0u8;
        if self.clean_start {
            flags |= 0x02;
        }
        if let Some(will) = &self.will {
            flags |= 0x04;
            flags |= will.qos.as_u8() << 3;
            if will.retain {
                flags |= 0x20;
            }
        }
        if self.password.is_some() {
            flags |= 0x40;
        }
        if self.username.is_some() {
            flags |= 0x80;
        }
        w.put_u8(flags);
        w.put_u16(self.keep_alive);
        // Properties only exist in v5 (level 5).
        let has_properties = self.protocol_version == 5;
        if has_properties {
            self.properties.encode(&mut w)?;
        }

        w.put_utf8(&self.client_id);
        if let Some(will) = &self.will {
            if has_properties {
                will.properties.encode(&mut w)?;
            }
            w.put_utf8(&will.topic);
            w.put_binary(&will.payload);
        }
        if let Some(u) = &self.username {
            w.put_utf8(u);
        }
        if let Some(p) = &self.password {
            w.put_binary(p);
        }
        Ok(w.into_vec())
    }

    /// Validate protocol name/version, accepting MQTT v3.1 ("MQIsdp", level 3),
    /// v3.1.1 ("MQTT", level 4) and v5.0 ("MQTT", level 5). Returns the negotiated
    /// version, or an error carrying the reason code to send in CONNACK.
    pub fn validate_protocol(&self) -> Result<ProtocolVersion> {
        let expected_name = if self.protocol_version == 3 {
            "MQIsdp"
        } else {
            "MQTT"
        };
        if self.protocol_name != expected_name {
            // [MQTT-3.1.2-1] Server MAY disconnect for an unacceptable name.
            return Err(protocol(format!(
                "unsupported protocol name {:?}",
                self.protocol_name
            )));
        }
        ProtocolVersion::from_level(self.protocol_version).ok_or_else(|| {
            crate::mqtt::error::MqttError::Reason(
                ReasonCode::UnsupportedProtocolVersion,
                format!("unsupported protocol version {}", self.protocol_version),
            )
        })
    }
}

#[derive(Debug, Clone)]
pub struct Connack {
    pub session_present: bool,
    pub reason_code: ReasonCode,
    pub properties: Properties,
}

impl Connack {
    pub fn new(session_present: bool, reason_code: ReasonCode) -> Self {
        Connack {
            session_present,
            reason_code,
            properties: Properties::new(),
        }
    }

    pub fn decode(r: &mut Reader, version: ProtocolVersion) -> Result<Connack> {
        let ack_flags = r.u8()?;
        if ack_flags & 0xFE != 0 {
            return Err(malformed("CONNACK reserved acknowledge flags set"));
        }
        let session_present = ack_flags & 0x01 != 0;
        let code = r.u8()?;
        // v3.x uses a 1-byte return code (0-5); v5 uses a Reason Code + properties.
        let reason_code = if version.is_v5() {
            ReasonCode::from_u8(code)?
        } else {
            connack_return_to_reason(code)
        };
        let properties = if version.is_v5() {
            Properties::decode(r)?
        } else {
            Properties::new()
        };
        Ok(Connack {
            session_present,
            reason_code,
            properties,
        })
    }

    pub fn encode_body(&self, version: ProtocolVersion) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        // Session Present is only defined from v3.1.1 onward; in v3.1 the byte
        // is reserved and MUST be 0.
        let session_present = version != ProtocolVersion::V3_1 && self.session_present;
        w.put_u8(if session_present { 0x01 } else { 0x00 });
        if version.is_v5() {
            w.put_u8(self.reason_code.as_u8());
            self.properties.encode(&mut w)?;
        } else {
            w.put_u8(reason_to_connack_return(self.reason_code));
        }
        Ok(w.into_vec())
    }
}

/// Map a v5 Reason Code to a v3.x CONNACK return code (0-5).
fn reason_to_connack_return(rc: ReasonCode) -> u8 {
    match rc {
        ReasonCode::Success => 0,
        ReasonCode::UnsupportedProtocolVersion => 1,
        ReasonCode::ClientIdentifierNotValid => 2,
        ReasonCode::ServerUnavailable | ReasonCode::ServerBusy => 3,
        ReasonCode::BadUserNameOrPassword => 4,
        ReasonCode::NotAuthorized => 5,
        // v3.x has no code for other failures; report "not authorized".
        _ => 5,
    }
}

/// Map a v3.x CONNACK return code to the closest v5 Reason Code.
fn connack_return_to_reason(code: u8) -> ReasonCode {
    match code {
        0 => ReasonCode::Success,
        1 => ReasonCode::UnsupportedProtocolVersion,
        2 => ReasonCode::ClientIdentifierNotValid,
        3 => ReasonCode::ServerUnavailable,
        4 => ReasonCode::BadUserNameOrPassword,
        _ => ReasonCode::NotAuthorized,
    }
}
