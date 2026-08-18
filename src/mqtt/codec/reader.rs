//! Sequential reader over a byte slice implementing the MQTT v5.0 data types.

use crate::mqtt::codec::MAX_VARIABLE_BYTE_INTEGER;
use crate::mqtt::error::{malformed, Result};

pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn has_remaining(&self) -> bool {
        self.remaining() > 0
    }

    fn ensure(&self, n: usize) -> Result<()> {
        if self.remaining() < n {
            return Err(malformed(format!(
                "unexpected end of packet: need {n} bytes, have {}",
                self.remaining()
            )));
        }
        Ok(())
    }

    /// Read a single byte (Section 1.5.1).
    pub fn u8(&mut self) -> Result<u8> {
        self.ensure(1)?;
        let b = self.buf[self.pos];
        self.pos += 1;
        Ok(b)
    }

    /// Read a Two Byte Integer, big-endian (Section 1.5.2).
    pub fn u16(&mut self) -> Result<u16> {
        self.ensure(2)?;
        let v = u16::from_be_bytes([self.buf[self.pos], self.buf[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    /// Read a Four Byte Integer, big-endian (Section 1.5.3).
    pub fn u32(&mut self) -> Result<u32> {
        self.ensure(4)?;
        let v = u32::from_be_bytes([
            self.buf[self.pos],
            self.buf[self.pos + 1],
            self.buf[self.pos + 2],
            self.buf[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    /// Read a Variable Byte Integer (Section 1.5.5). At most 4 bytes.
    pub fn varint(&mut self) -> Result<u32> {
        let mut multiplier: u32 = 1;
        let mut value: u32 = 0;
        for _ in 0..4 {
            let byte = self.u8()?;
            value += (byte & 0x7F) as u32 * multiplier;
            if byte & 0x80 == 0 {
                if value > MAX_VARIABLE_BYTE_INTEGER {
                    return Err(malformed("variable byte integer too large"));
                }
                return Ok(value);
            }
            multiplier *= 128;
        }
        Err(malformed("malformed variable byte integer (>4 bytes)"))
    }

    /// Read `n` raw bytes, returning a borrowed slice.
    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        self.ensure(n)?;
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// Read Binary Data: a Two Byte length prefix followed by that many bytes
    /// (Section 1.5.6).
    pub fn binary(&mut self) -> Result<Vec<u8>> {
        let len = self.u16()? as usize;
        Ok(self.bytes(len)?.to_vec())
    }

    /// Read a UTF-8 Encoded String (Section 1.5.4). Rejects invalid UTF-8 and
    /// the U+0000 null character and other disallowed code points.
    pub fn utf8(&mut self) -> Result<String> {
        let len = self.u16()? as usize;
        let raw = self.bytes(len)?;
        let s =
            std::str::from_utf8(raw).map_err(|_| malformed("string is not well-formed UTF-8"))?;
        // [MQTT-1.5.4-2] MUST NOT include U+0000.
        if s.contains('\u{0000}') {
            return Err(malformed("string contains U+0000"));
        }
        Ok(s.to_string())
    }

    /// Read a UTF-8 String Pair (Section 1.5.7).
    pub fn utf8_pair(&mut self) -> Result<(String, String)> {
        let k = self.utf8()?;
        let v = self.utf8()?;
        Ok((k, v))
    }

    /// Consume and return all remaining bytes.
    pub fn rest(&mut self) -> &'a [u8] {
        let s = &self.buf[self.pos..];
        self.pos = self.buf.len();
        s
    }

    /// Create a sub-reader over the next `n` bytes, advancing self past them.
    /// Used to bound property/payload sections precisely.
    pub fn sub(&mut self, n: usize) -> Result<Reader<'a>> {
        let s = self.bytes(n)?;
        Ok(Reader::new(s))
    }
}
