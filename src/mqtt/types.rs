//! Fundamental MQTT v5.0 enumerations: control packet types, QoS levels,
//! Reason Codes and property identifiers. Values come directly from the
//! OASIS MQTT Version 5.0 specification (07 March 2019).

use crate::mqtt::error::{malformed, MqttError};

/// MQTT Control Packet type — the high nibble of the fixed header byte 1.
/// Spec Table 2-1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    Connect = 1,
    Connack = 2,
    Publish = 3,
    Puback = 4,
    Pubrec = 5,
    Pubrel = 6,
    Pubcomp = 7,
    Subscribe = 8,
    Suback = 9,
    Unsubscribe = 10,
    Unsuback = 11,
    Pingreq = 12,
    Pingresp = 13,
    Disconnect = 14,
    Auth = 15,
}

impl PacketType {
    pub fn from_u8(v: u8) -> Result<Self, MqttError> {
        Ok(match v {
            1 => PacketType::Connect,
            2 => PacketType::Connack,
            3 => PacketType::Publish,
            4 => PacketType::Puback,
            5 => PacketType::Pubrec,
            6 => PacketType::Pubrel,
            7 => PacketType::Pubcomp,
            8 => PacketType::Subscribe,
            9 => PacketType::Suback,
            10 => PacketType::Unsubscribe,
            11 => PacketType::Unsuback,
            12 => PacketType::Pingreq,
            13 => PacketType::Pingresp,
            14 => PacketType::Disconnect,
            15 => PacketType::Auth,
            other => return Err(malformed(format!("invalid packet type {other}"))),
        })
    }
}

/// MQTT protocol version negotiated in CONNECT. The wire "protocol level" is
/// 3 for v3.1 (name "MQIsdp"), 4 for v3.1.1 and 5 for v5.0 (name "MQTT").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolVersion {
    V3_1,
    V3_1_1,
    V5,
}

impl ProtocolVersion {
    pub fn level(self) -> u8 {
        match self {
            ProtocolVersion::V3_1 => 3,
            ProtocolVersion::V3_1_1 => 4,
            ProtocolVersion::V5 => 5,
        }
    }

    pub fn from_level(level: u8) -> Option<Self> {
        match level {
            3 => Some(ProtocolVersion::V3_1),
            4 => Some(ProtocolVersion::V3_1_1),
            5 => Some(ProtocolVersion::V5),
            _ => None,
        }
    }

    pub fn is_v5(self) -> bool {
        matches!(self, ProtocolVersion::V5)
    }

    /// True when this version carries MQTT v5 Properties.
    pub fn has_properties(self) -> bool {
        self.is_v5()
    }
}

/// Quality of Service level (2 bits). Spec 4.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl QoS {
    pub fn from_u8(v: u8) -> Result<Self, MqttError> {
        Ok(match v {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => return Err(malformed(format!("invalid QoS {v}"))),
        })
    }

    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// The lower of two QoS levels — used when a message is delivered to a
    /// subscription whose Maximum QoS is below the publication QoS (3.8.4).
    pub fn min(self, other: QoS) -> QoS {
        if (self as u8) <= (other as u8) {
            self
        } else {
            other
        }
    }
}

/// Property identifiers (Section 2.2.2, Table 2-4). The discriminant is the
/// identifier byte transmitted on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PropertyId {
    PayloadFormatIndicator = 0x01,
    MessageExpiryInterval = 0x02,
    ContentType = 0x03,
    ResponseTopic = 0x08,
    CorrelationData = 0x09,
    SubscriptionIdentifier = 0x0B,
    SessionExpiryInterval = 0x11,
    AssignedClientIdentifier = 0x12,
    ServerKeepAlive = 0x13,
    AuthenticationMethod = 0x15,
    AuthenticationData = 0x16,
    RequestProblemInformation = 0x17,
    WillDelayInterval = 0x18,
    RequestResponseInformation = 0x19,
    ResponseInformation = 0x1A,
    ServerReference = 0x1C,
    ReasonString = 0x1F,
    ReceiveMaximum = 0x21,
    TopicAliasMaximum = 0x22,
    TopicAlias = 0x23,
    MaximumQoS = 0x24,
    RetainAvailable = 0x25,
    UserProperty = 0x26,
    MaximumPacketSize = 0x27,
    WildcardSubscriptionAvailable = 0x28,
    SubscriptionIdentifierAvailable = 0x29,
    SharedSubscriptionAvailable = 0x2A,
}

impl PropertyId {
    pub fn from_u8(v: u8) -> Result<Self, MqttError> {
        use PropertyId::*;
        Ok(match v {
            0x01 => PayloadFormatIndicator,
            0x02 => MessageExpiryInterval,
            0x03 => ContentType,
            0x08 => ResponseTopic,
            0x09 => CorrelationData,
            0x0B => SubscriptionIdentifier,
            0x11 => SessionExpiryInterval,
            0x12 => AssignedClientIdentifier,
            0x13 => ServerKeepAlive,
            0x15 => AuthenticationMethod,
            0x16 => AuthenticationData,
            0x17 => RequestProblemInformation,
            0x18 => WillDelayInterval,
            0x19 => RequestResponseInformation,
            0x1A => ResponseInformation,
            0x1C => ServerReference,
            0x1F => ReasonString,
            0x21 => ReceiveMaximum,
            0x22 => TopicAliasMaximum,
            0x23 => TopicAlias,
            0x24 => MaximumQoS,
            0x25 => RetainAvailable,
            0x26 => UserProperty,
            0x27 => MaximumPacketSize,
            0x28 => WildcardSubscriptionAvailable,
            0x29 => SubscriptionIdentifierAvailable,
            0x2A => SharedSubscriptionAvailable,
            other => {
                return Err(malformed(format!(
                    "unknown property identifier 0x{other:02X}"
                )))
            }
        })
    }
}

/// Reason Codes (Section 2.4, Table 2-3). A single byte; values >= 0x80 are
/// failures. The same numeric value can carry different meaning per packet,
/// so this is a flat catalogue of every code the broker emits or accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ReasonCode {
    Success = 0x00, // also: Normal disconnection / Granted QoS 0
    GrantedQoS1 = 0x01,
    GrantedQoS2 = 0x02,
    DisconnectWithWillMessage = 0x04,
    NoMatchingSubscribers = 0x10,
    NoSubscriptionExisted = 0x11,
    ContinueAuthentication = 0x18,
    ReAuthenticate = 0x19,
    UnspecifiedError = 0x80,
    MalformedPacket = 0x81,
    ProtocolError = 0x82,
    ImplementationSpecificError = 0x83,
    UnsupportedProtocolVersion = 0x84,
    ClientIdentifierNotValid = 0x85,
    BadUserNameOrPassword = 0x86,
    NotAuthorized = 0x87,
    ServerUnavailable = 0x88,
    ServerBusy = 0x89,
    Banned = 0x8A,
    ServerShuttingDown = 0x8B,
    BadAuthenticationMethod = 0x8C,
    KeepAliveTimeout = 0x8D,
    SessionTakenOver = 0x8E,
    TopicFilterInvalid = 0x8F,
    TopicNameInvalid = 0x90,
    PacketIdentifierInUse = 0x91,
    PacketIdentifierNotFound = 0x92,
    ReceiveMaximumExceeded = 0x93,
    TopicAliasInvalid = 0x94,
    PacketTooLarge = 0x95,
    MessageRateTooHigh = 0x96,
    QuotaExceeded = 0x97,
    AdministrativeAction = 0x98,
    PayloadFormatInvalid = 0x99,
    RetainNotSupported = 0x9A,
    QoSNotSupported = 0x9B,
    UseAnotherServer = 0x9C,
    ServerMoved = 0x9D,
    SharedSubscriptionsNotSupported = 0x9E,
    ConnectionRateExceeded = 0x9F,
    MaximumConnectTime = 0xA0,
    SubscriptionIdentifiersNotSupported = 0xA1,
    WildcardSubscriptionsNotSupported = 0xA2,
}

impl ReasonCode {
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// True for codes >= 0x80, which the spec defines as errors.
    pub fn is_error(self) -> bool {
        (self as u8) >= 0x80
    }

    /// Decode a Reason Code byte. Unknown values are rejected as malformed.
    pub fn from_u8(v: u8) -> Result<Self, MqttError> {
        use ReasonCode::*;
        Ok(match v {
            0x00 => Success,
            0x01 => GrantedQoS1,
            0x02 => GrantedQoS2,
            0x04 => DisconnectWithWillMessage,
            0x10 => NoMatchingSubscribers,
            0x11 => NoSubscriptionExisted,
            0x18 => ContinueAuthentication,
            0x19 => ReAuthenticate,
            0x80 => UnspecifiedError,
            0x81 => MalformedPacket,
            0x82 => ProtocolError,
            0x83 => ImplementationSpecificError,
            0x84 => UnsupportedProtocolVersion,
            0x85 => ClientIdentifierNotValid,
            0x86 => BadUserNameOrPassword,
            0x87 => NotAuthorized,
            0x88 => ServerUnavailable,
            0x89 => ServerBusy,
            0x8A => Banned,
            0x8B => ServerShuttingDown,
            0x8C => BadAuthenticationMethod,
            0x8D => KeepAliveTimeout,
            0x8E => SessionTakenOver,
            0x8F => TopicFilterInvalid,
            0x90 => TopicNameInvalid,
            0x91 => PacketIdentifierInUse,
            0x92 => PacketIdentifierNotFound,
            0x93 => ReceiveMaximumExceeded,
            0x94 => TopicAliasInvalid,
            0x95 => PacketTooLarge,
            0x96 => MessageRateTooHigh,
            0x97 => QuotaExceeded,
            0x98 => AdministrativeAction,
            0x99 => PayloadFormatInvalid,
            0x9A => RetainNotSupported,
            0x9B => QoSNotSupported,
            0x9C => UseAnotherServer,
            0x9D => ServerMoved,
            0x9E => SharedSubscriptionsNotSupported,
            0x9F => ConnectionRateExceeded,
            0xA0 => MaximumConnectTime,
            0xA1 => SubscriptionIdentifiersNotSupported,
            0xA2 => WildcardSubscriptionsNotSupported,
            other => return Err(malformed(format!("unknown reason code 0x{other:02X}"))),
        })
    }
}
