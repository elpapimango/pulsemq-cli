//! Append-only writer producing MQTT v5.0 wire representations.

use crate::mqtt::codec::MAX_VARIABLE_BYTE_INTEGER;
use crate::mqtt::error::{protocol, Result};

/// Clamp a slice to the 65535 bytes a Two Byte length prefix can describe.
fn clamp_16bit<'a>(v: &'a [u8], what: &str) -> &'a [u8] {
    if v.len() > u16::MAX as usize {
        eprintln!(
            "wispmq-cli: {what} is {} bytes, exceeds the 65535-byte MQTT limit, truncated",
            v.len()
        );
        return &v[..u16::MAX as usize];
    }
    v
}

#[derive(Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Writer { buf: Vec::new() }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Writer {
            buf: Vec::with_capacity(cap),
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn put_u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    pub fn put_u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn put_u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    pub fn put_bytes(&mut self, v: &[u8]) {
        self.buf.extend_from_slice(v);
    }

    /// Encode a Variable Byte Integer (Section 1.5.5).
    pub fn put_varint(&mut self, mut value: u32) -> Result<()> {
        if value > MAX_VARIABLE_BYTE_INTEGER {
            return Err(protocol("variable byte integer overflow"));
        }
        loop {
            let mut byte = (value % 128) as u8;
            value /= 128;
            if value > 0 {
                byte |= 0x80;
            }
            self.buf.push(byte);
            if value == 0 {
                break;
            }
        }
        Ok(())
    }

    /// Encode Binary Data with a Two Byte length prefix (Section 1.5.6).
    ///
    /// Values longer than 65535 bytes cannot be represented. They are truncated
    /// to fit rather than silently corrupting the length prefix (`len as u16`
    /// wraps modulo 65536, which would desynchronise the receiver's framing for
    /// every subsequent packet). Every such field arrives from the wire behind
    /// its own Two Byte length, so this is unreachable today; the clamp exists
    /// so a future internally-generated value cannot corrupt the stream.
    pub fn put_binary(&mut self, v: &[u8]) {
        let v = clamp_16bit(v, "binary data");
        self.put_u16(v.len() as u16);
        self.buf.extend_from_slice(v);
    }

    /// Encode a UTF-8 Encoded String (Section 1.5.4).
    ///
    /// Over-long strings are truncated on a character boundary; see
    /// [`Writer::put_binary`] for why truncation beats a wrapping length.
    pub fn put_utf8(&mut self, v: &str) {
        let mut end = v.len().min(u16::MAX as usize);
        if end < v.len() {
            eprintln!(
                "wispmq-cli: string is {} bytes, exceeds the 65535-byte MQTT limit, truncated",
                v.len()
            );
            // Do not split a multi-byte character; back off to a boundary.
            while end > 0 && !v.is_char_boundary(end) {
                end -= 1;
            }
        }
        let v = &v[..end];
        self.put_u16(v.len() as u16);
        self.buf.extend_from_slice(v.as_bytes());
    }

    pub fn put_utf8_pair(&mut self, k: &str, v: &str) {
        self.put_utf8(k);
        self.put_utf8(v);
    }
}
