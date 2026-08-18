//! Async framing: read and write whole MQTT packets over a byte stream.
//!
//! MQTT frames are self-delimiting via the Remaining Length field, so a reader
//! must consume the fixed header, decode the length, then read exactly that
//! many further bytes before decoding.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::mqtt::codec::MAX_VARIABLE_BYTE_INTEGER;
use crate::mqtt::error::{malformed, MqttError, Result};
use crate::mqtt::packet::Packet;
use crate::mqtt::types::ProtocolVersion;

/// Outcome of attempting to read a packet.
pub enum ReadOutcome {
    /// A decoded packet plus the number of bytes the whole frame occupied.
    Packet(Packet, usize),
    /// The peer closed the connection cleanly at a frame boundary.
    Eof,
}

/// Read one complete control packet. Enforces `max_packet_size` on the total
/// encoded length (fixed header included), returning a Packet too large error
/// when exceeded.
pub async fn read_packet<R>(
    stream: &mut R,
    max_packet_size: u32,
    version: ProtocolVersion,
) -> Result<ReadOutcome>
where
    R: AsyncRead + Unpin,
{
    // First byte of the fixed header.
    let first = match stream.read_u8().await {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(ReadOutcome::Eof),
        Err(e) => return Err(MqttError::Io(e)),
    };

    // Decode Remaining Length (Variable Byte Integer) directly from the wire.
    let mut multiplier: u32 = 1;
    let mut remaining: u32 = 0;
    let mut header = vec![first];
    for _ in 0..4 {
        let byte = stream.read_u8().await?;
        header.push(byte);
        remaining += (byte & 0x7F) as u32 * multiplier;
        if byte & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
        if multiplier > 128 * 128 * 128 {
            return Err(malformed("malformed Remaining Length"));
        }
    }
    if remaining > MAX_VARIABLE_BYTE_INTEGER {
        return Err(malformed("Remaining Length exceeds maximum"));
    }

    let total = header.len() as u32 + remaining;
    if total > max_packet_size {
        return Err(MqttError::Reason(
            crate::mqtt::types::ReasonCode::PacketTooLarge,
            format!("packet of {total} bytes exceeds maximum {max_packet_size}"),
        ));
    }

    // Read the remainder and decode the whole frame.
    let mut buf = header;
    let start = buf.len();
    buf.resize(start + remaining as usize, 0);
    stream.read_exact(&mut buf[start..]).await?;

    let size = buf.len();
    let packet = Packet::decode(&buf, version)?;
    Ok(ReadOutcome::Packet(packet, size))
}

/// Encode and write a single packet, flushing the stream. Returns the number
/// of bytes written.
pub async fn write_packet<W>(
    stream: &mut W,
    packet: &Packet,
    version: ProtocolVersion,
) -> Result<usize>
where
    W: AsyncWrite + Unpin,
{
    let bytes = packet.encode(version)?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(bytes.len())
}
