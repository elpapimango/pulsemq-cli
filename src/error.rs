//! Error type for the client tools.
//!
//! Distinguishes the three things a user needs to tell apart: bad arguments,
//! a broker that answered with a Reason Code, and everything underneath
//! (transport and codec errors from the `mqtt` layer).

use std::fmt;

use wispmq_protocol::error::MqttError;
use wispmq_protocol::types::ReasonCode;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    /// Bad or contradictory command-line arguments.
    Usage(String),
    /// The broker answered with a non-success Reason Code. `what` names the
    /// operation ("connection", "publish", "subscription to sensors/#").
    Rejected { what: String, code: ReasonCode },
    /// The broker closed the connection where a reply was expected.
    Disconnected(String),
    /// The requested feature does not exist in the negotiated protocol version.
    Unsupported(String),
    /// Transport or codec failure.
    Mqtt(MqttError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Usage(m) => write!(f, "{m}"),
            Error::Rejected { what, code } => {
                write!(
                    f,
                    "{what} rejected by broker: {code:?} (0x{:02X})",
                    code.as_u8()
                )
            }
            Error::Disconnected(m) => write!(f, "broker closed the connection {m}"),
            Error::Unsupported(m) => write!(f, "{m}"),
            Error::Mqtt(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<MqttError> for Error {
    fn from(e: MqttError) -> Self {
        Error::Mqtt(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Mqtt(MqttError::Io(e))
    }
}
