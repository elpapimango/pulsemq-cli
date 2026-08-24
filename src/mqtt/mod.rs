//! The MQTT wire format: control packets, the property codec, framing, and the
//! shared protocol enums.
//!
//! Written from scratch against the OASIS MQTT specifications, and covering
//! v5.0, v3.1.1 and v3.1 in one decoder — `ProtocolVersion` selects the
//! grammar rather than there being one module per version.
//!
//! This layer knows nothing about the client above it: `codec` holds the
//! Section 1.5 data representations and the Section 2.2.2 properties,
//! `packet` all 15 control packets, `framing` reads and writes whole frames
//! over any `AsyncRead`/`AsyncWrite`, and `types` the enums (`ProtocolVersion`,
//! `QoS`, `ReasonCode`) that the other three share.
//!
//! Two clippy lints are allowed for the whole layer by design: the
//! control-packet and frame enums intentionally have variants of very
//! different sizes (`large_enum_variant`), and the CONNECT path returns a
//! rejection CONNACK by value on the error path (`result_large_err`).
#![allow(clippy::large_enum_variant, clippy::result_large_err)]

pub mod codec;
pub mod error;
pub mod framing;
pub mod packet;
pub mod secret;
pub mod types;
