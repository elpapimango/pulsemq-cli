//! Low-level MQTT v5.0 data-representation primitives (Section 1.5) and the
//! property codec (Section 2.2.2).
//!
//! `Reader` walks a byte slice; `Writer` accumulates into a `Vec<u8>`. Both
//! operate purely in-memory — framing (reading a whole packet off the socket)
//! is handled separately in `packet::framing`.

mod properties;
mod reader;
mod writer;

pub use properties::Properties;
pub use reader::Reader;
pub use writer::Writer;

/// Maximum value of a Variable Byte Integer (Section 1.5.5): 268,435,455.
pub const MAX_VARIABLE_BYTE_INTEGER: u32 = 268_435_455;

/// Number of bytes needed to encode `value` as a Variable Byte Integer.
pub fn varint_len(value: u32) -> usize {
    match value {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    }
}
