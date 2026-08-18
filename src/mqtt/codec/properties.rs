//! MQTT v5.0 Properties (Section 2.2.2).
//!
//! Rather than a distinct type per packet, all properties are collected into
//! one `Properties` struct. Each packet encoder writes only the properties
//! valid for it; each decoder reads whatever identifiers are present and the
//! packet layer validates legality. Decoding is bounded by the Property
//! Length prefix.

use crate::mqtt::codec::{varint_len, Reader, Writer};
use crate::mqtt::error::{malformed, Result};
use crate::mqtt::types::{PropertyId, QoS};

/// All MQTT v5.0 properties. Fields are `Option` for at-most-once properties;
/// `user_properties` and `subscription_identifiers` may repeat.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Properties {
    pub payload_format_indicator: Option<u8>,
    pub message_expiry_interval: Option<u32>,
    pub content_type: Option<String>,
    pub response_topic: Option<String>,
    pub correlation_data: Option<Vec<u8>>,
    pub subscription_identifiers: Vec<u32>,
    pub session_expiry_interval: Option<u32>,
    pub assigned_client_identifier: Option<String>,
    pub server_keep_alive: Option<u16>,
    pub authentication_method: Option<String>,
    pub authentication_data: Option<Vec<u8>>,
    pub request_problem_information: Option<u8>,
    pub will_delay_interval: Option<u32>,
    pub request_response_information: Option<u8>,
    pub response_information: Option<String>,
    pub server_reference: Option<String>,
    pub reason_string: Option<String>,
    pub receive_maximum: Option<u16>,
    pub topic_alias_maximum: Option<u16>,
    pub topic_alias: Option<u16>,
    pub maximum_qos: Option<u8>,
    pub retain_available: Option<u8>,
    pub user_properties: Vec<(String, String)>,
    pub maximum_packet_size: Option<u32>,
    pub wildcard_subscription_available: Option<u8>,
    pub subscription_identifier_available: Option<u8>,
    pub shared_subscription_available: Option<u8>,
}

impl Properties {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        *self == Properties::default()
    }

    /// Decode a property set given the outer reader. Reads the Property Length
    /// VBI, then all properties inside it.
    pub fn decode(r: &mut Reader) -> Result<Properties> {
        let len = r.varint()? as usize;
        let mut sub = r.sub(len)?;
        let mut p = Properties::new();
        while sub.has_remaining() {
            let id = PropertyId::from_u8(sub.u8()?)?;
            use PropertyId::*;
            match id {
                PayloadFormatIndicator => set_once(&mut p.payload_format_indicator, sub.u8()?, id)?,
                MessageExpiryInterval => set_once(&mut p.message_expiry_interval, sub.u32()?, id)?,
                ContentType => set_once(&mut p.content_type, sub.utf8()?, id)?,
                ResponseTopic => set_once(&mut p.response_topic, sub.utf8()?, id)?,
                CorrelationData => set_once(&mut p.correlation_data, sub.binary()?, id)?,
                SubscriptionIdentifier => {
                    let v = sub.varint()?;
                    if v == 0 {
                        return Err(malformed(
                            "Subscription Identifier of 0 is a Protocol Error",
                        ));
                    }
                    p.subscription_identifiers.push(v);
                }
                SessionExpiryInterval => set_once(&mut p.session_expiry_interval, sub.u32()?, id)?,
                AssignedClientIdentifier => {
                    set_once(&mut p.assigned_client_identifier, sub.utf8()?, id)?
                }
                ServerKeepAlive => set_once(&mut p.server_keep_alive, sub.u16()?, id)?,
                AuthenticationMethod => set_once(&mut p.authentication_method, sub.utf8()?, id)?,
                AuthenticationData => set_once(&mut p.authentication_data, sub.binary()?, id)?,
                RequestProblemInformation => {
                    set_once(&mut p.request_problem_information, sub.u8()?, id)?
                }
                WillDelayInterval => set_once(&mut p.will_delay_interval, sub.u32()?, id)?,
                RequestResponseInformation => {
                    set_once(&mut p.request_response_information, sub.u8()?, id)?
                }
                ResponseInformation => set_once(&mut p.response_information, sub.utf8()?, id)?,
                ServerReference => set_once(&mut p.server_reference, sub.utf8()?, id)?,
                ReasonString => set_once(&mut p.reason_string, sub.utf8()?, id)?,
                ReceiveMaximum => {
                    let v = sub.u16()?;
                    if v == 0 {
                        return Err(malformed("Receive Maximum of 0 is a Protocol Error"));
                    }
                    set_once(&mut p.receive_maximum, v, id)?
                }
                TopicAliasMaximum => set_once(&mut p.topic_alias_maximum, sub.u16()?, id)?,
                TopicAlias => {
                    let v = sub.u16()?;
                    if v == 0 {
                        return Err(malformed("Topic Alias of 0 is a Protocol Error"));
                    }
                    set_once(&mut p.topic_alias, v, id)?
                }
                MaximumQoS => set_once(&mut p.maximum_qos, sub.u8()?, id)?,
                RetainAvailable => set_once(&mut p.retain_available, sub.u8()?, id)?,
                UserProperty => p.user_properties.push(sub.utf8_pair()?),
                MaximumPacketSize => {
                    let v = sub.u32()?;
                    if v == 0 {
                        return Err(malformed("Maximum Packet Size of 0 is a Protocol Error"));
                    }
                    set_once(&mut p.maximum_packet_size, v, id)?
                }
                WildcardSubscriptionAvailable => {
                    set_once(&mut p.wildcard_subscription_available, sub.u8()?, id)?
                }
                SubscriptionIdentifierAvailable => {
                    set_once(&mut p.subscription_identifier_available, sub.u8()?, id)?
                }
                SharedSubscriptionAvailable => {
                    set_once(&mut p.shared_subscription_available, sub.u8()?, id)?
                }
            }
        }
        Ok(p)
    }

    /// Serialize the properties body (without the length prefix) into a Writer.
    fn encode_body(&self, w: &mut Writer) -> Result<()> {
        use PropertyId::*;
        if let Some(v) = self.payload_format_indicator {
            w.put_u8(PayloadFormatIndicator as u8);
            w.put_u8(v);
        }
        if let Some(v) = self.message_expiry_interval {
            w.put_u8(MessageExpiryInterval as u8);
            w.put_u32(v);
        }
        if let Some(v) = &self.content_type {
            w.put_u8(ContentType as u8);
            w.put_utf8(v);
        }
        if let Some(v) = &self.response_topic {
            w.put_u8(ResponseTopic as u8);
            w.put_utf8(v);
        }
        if let Some(v) = &self.correlation_data {
            w.put_u8(CorrelationData as u8);
            w.put_binary(v);
        }
        for v in &self.subscription_identifiers {
            w.put_u8(SubscriptionIdentifier as u8);
            w.put_varint(*v)?;
        }
        if let Some(v) = self.session_expiry_interval {
            w.put_u8(SessionExpiryInterval as u8);
            w.put_u32(v);
        }
        if let Some(v) = &self.assigned_client_identifier {
            w.put_u8(AssignedClientIdentifier as u8);
            w.put_utf8(v);
        }
        if let Some(v) = self.server_keep_alive {
            w.put_u8(ServerKeepAlive as u8);
            w.put_u16(v);
        }
        if let Some(v) = &self.authentication_method {
            w.put_u8(AuthenticationMethod as u8);
            w.put_utf8(v);
        }
        if let Some(v) = &self.authentication_data {
            w.put_u8(AuthenticationData as u8);
            w.put_binary(v);
        }
        if let Some(v) = self.request_problem_information {
            w.put_u8(RequestProblemInformation as u8);
            w.put_u8(v);
        }
        if let Some(v) = self.will_delay_interval {
            w.put_u8(WillDelayInterval as u8);
            w.put_u32(v);
        }
        if let Some(v) = self.request_response_information {
            w.put_u8(RequestResponseInformation as u8);
            w.put_u8(v);
        }
        if let Some(v) = &self.response_information {
            w.put_u8(ResponseInformation as u8);
            w.put_utf8(v);
        }
        if let Some(v) = &self.server_reference {
            w.put_u8(ServerReference as u8);
            w.put_utf8(v);
        }
        if let Some(v) = &self.reason_string {
            w.put_u8(ReasonString as u8);
            w.put_utf8(v);
        }
        if let Some(v) = self.receive_maximum {
            w.put_u8(ReceiveMaximum as u8);
            w.put_u16(v);
        }
        if let Some(v) = self.topic_alias_maximum {
            w.put_u8(TopicAliasMaximum as u8);
            w.put_u16(v);
        }
        if let Some(v) = self.topic_alias {
            w.put_u8(TopicAlias as u8);
            w.put_u16(v);
        }
        if let Some(v) = self.maximum_qos {
            w.put_u8(MaximumQoS as u8);
            w.put_u8(v);
        }
        if let Some(v) = self.retain_available {
            w.put_u8(RetainAvailable as u8);
            w.put_u8(v);
        }
        for (k, val) in &self.user_properties {
            w.put_u8(UserProperty as u8);
            w.put_utf8_pair(k, val);
        }
        if let Some(v) = self.maximum_packet_size {
            w.put_u8(MaximumPacketSize as u8);
            w.put_u32(v);
        }
        if let Some(v) = self.wildcard_subscription_available {
            w.put_u8(WildcardSubscriptionAvailable as u8);
            w.put_u8(v);
        }
        if let Some(v) = self.subscription_identifier_available {
            w.put_u8(SubscriptionIdentifierAvailable as u8);
            w.put_u8(v);
        }
        if let Some(v) = self.shared_subscription_available {
            w.put_u8(SharedSubscriptionAvailable as u8);
            w.put_u8(v);
        }
        Ok(())
    }

    /// Encode the property set: Property Length VBI followed by the body.
    pub fn encode(&self, w: &mut Writer) -> Result<()> {
        // Encode the body to a scratch buffer to learn its length.
        let mut body = Writer::new();
        self.encode_body(&mut body)?;
        w.put_varint(body.len() as u32)?;
        w.put_bytes(body.as_slice());
        Ok(())
    }

    /// The encoded length including the Property Length prefix.
    pub fn encoded_len(&self) -> usize {
        let mut body = Writer::new();
        // encode_body only errors on VBI overflow of a subscription id, which
        // cannot occur for lengths; unwrap is safe here.
        self.encode_body(&mut body).expect("property body encode");
        varint_len(body.len() as u32) + body.len()
    }

    /// Helper for the common `maximum_qos` byte -> QoS mapping in CONNACK.
    pub fn max_qos_level(&self) -> Option<QoS> {
        self.maximum_qos.and_then(|b| QoS::from_u8(b).ok())
    }
}

/// Set an `Option` property, erroring if it was already present (most
/// properties MUST appear at most once — including it twice is a Protocol
/// Error, Section 2.2.2.2).
fn set_once<T>(slot: &mut Option<T>, value: T, id: PropertyId) -> Result<()> {
    if slot.is_some() {
        return Err(malformed(format!(
            "property {id:?} included more than once"
        )));
    }
    *slot = Some(value);
    Ok(())
}
