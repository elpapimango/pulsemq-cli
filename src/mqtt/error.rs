//! Error type for the MQTT protocol layer.
//!
//! `MqttError` distinguishes protocol-level failures (which map to an MQTT
//! Reason Code and typically cause a DISCONNECT / connection close per the
//! spec) from lower-level I/O failures.

use std::fmt;

use crate::mqtt::types::ReasonCode;

/// Result alias used throughout the protocol layer.
pub type Result<T> = std::result::Result<T, MqttError>;

#[derive(Debug)]
pub enum MqttError {
    /// A Malformed Packet: the bytes on the wire could not be decoded per
    /// the MQTT v5.0 grammar. Section 1.2 / 4.13.
    Malformed(String),
    /// A Protocol Error: the packet decoded but violates a MUST rule.
    Protocol(String),
    /// A protocol violation carrying a specific Reason Code to return to the
    /// peer (e.g. in CONNACK or DISCONNECT) before closing the connection.
    Reason(ReasonCode, String),
    /// Underlying transport error.
    Io(std::io::Error),
}

impl MqttError {
    /// The Reason Code that best describes this error, for inclusion in a
    /// CONNACK or DISCONNECT packet.
    pub fn reason_code(&self) -> ReasonCode {
        match self {
            MqttError::Malformed(_) => ReasonCode::MalformedPacket,
            MqttError::Protocol(_) => ReasonCode::ProtocolError,
            MqttError::Reason(rc, _) => *rc,
            MqttError::Io(_) => ReasonCode::UnspecifiedError,
        }
    }
}

impl fmt::Display for MqttError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MqttError::Malformed(m) => write!(f, "malformed packet: {m}"),
            MqttError::Protocol(m) => write!(f, "protocol error: {m}"),
            MqttError::Reason(rc, m) => write!(f, "protocol error ({rc:?}): {m}"),
            MqttError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for MqttError {}

impl From<std::io::Error> for MqttError {
    fn from(e: std::io::Error) -> Self {
        MqttError::Io(e)
    }
}

/// Convenience constructors.
pub fn malformed(msg: impl Into<String>) -> MqttError {
    MqttError::Malformed(msg.into())
}

pub fn protocol(msg: impl Into<String>) -> MqttError {
    MqttError::Protocol(msg.into())
}
