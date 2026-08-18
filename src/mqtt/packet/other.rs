//! DISCONNECT (3.14) and AUTH (3.15).

use crate::mqtt::codec::{Properties, Reader, Writer};
use crate::mqtt::error::Result;
use crate::mqtt::types::{ProtocolVersion, ReasonCode};

#[derive(Debug, Clone)]
pub struct Disconnect {
    pub reason_code: ReasonCode,
    pub properties: Properties,
}

impl Disconnect {
    pub fn new(reason_code: ReasonCode) -> Self {
        Disconnect {
            reason_code,
            properties: Properties::new(),
        }
    }

    pub fn decode(r: &mut Reader, remaining_len: usize) -> Result<Disconnect> {
        // 3.14.2.1: a Remaining Length of 0 means Normal disconnection (0x00)
        // and no properties.
        if remaining_len == 0 {
            return Ok(Disconnect::new(ReasonCode::Success));
        }
        let reason_code = ReasonCode::from_u8(r.u8()?)?;
        // If only the Reason Code is present, Property Length is omitted.
        if remaining_len == 1 {
            return Ok(Disconnect {
                reason_code,
                properties: Properties::new(),
            });
        }
        let properties = Properties::decode(r)?;
        Ok(Disconnect {
            reason_code,
            properties,
        })
    }

    pub fn encode_body(&self, version: ProtocolVersion) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        // v3.x DISCONNECT has no variable header (and only the client ever
        // sends it). Omit everything for v3.x, and for a v5 normal disconnect
        // with no properties.
        if !version.is_v5()
            || (self.reason_code == ReasonCode::Success && self.properties.is_empty())
        {
            return Ok(w.into_vec());
        }
        w.put_u8(self.reason_code.as_u8());
        self.properties.encode(&mut w)?;
        Ok(w.into_vec())
    }
}

#[derive(Debug, Clone)]
pub struct Auth {
    pub reason_code: ReasonCode,
    pub properties: Properties,
}

impl Auth {
    pub fn decode(r: &mut Reader, remaining_len: usize) -> Result<Auth> {
        // 3.15.2.1: Remaining Length 0 means Success and no properties.
        if remaining_len == 0 {
            return Ok(Auth {
                reason_code: ReasonCode::Success,
                properties: Properties::new(),
            });
        }
        let reason_code = ReasonCode::from_u8(r.u8()?)?;
        if remaining_len == 1 {
            return Ok(Auth {
                reason_code,
                properties: Properties::new(),
            });
        }
        let properties = Properties::decode(r)?;
        Ok(Auth {
            reason_code,
            properties,
        })
    }

    pub fn encode_body(&self) -> Result<Vec<u8>> {
        let mut w = Writer::new();
        if self.reason_code == ReasonCode::Success && self.properties.is_empty() {
            return Ok(w.into_vec());
        }
        w.put_u8(self.reason_code.as_u8());
        self.properties.encode(&mut w)?;
        Ok(w.into_vec())
    }
}
