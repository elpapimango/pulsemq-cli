//! A byte buffer that overwrites itself on drop.
//!
//! Defends the CONNECT password specifically (Section 3.1.3.5): once the
//! handshake has encoded it onto the wire, nothing needs the plaintext bytes
//! again, and leaving them sitting in freed memory until the allocator
//! reuses that page is needless residue. A plain `for b in &mut buf { *b = 0
//! }` loop is a dead store the optimizer is free to elide once it can prove
//! nothing reads the buffer afterward — which is exactly the situation here.
//! `std::ptr::write_volatile` plus a compiler fence is what actually
//! survives optimization; it is the same technique the `zeroize` crate uses
//! internally, kept in-tree instead of taken as a dependency.
//!
//! This does not defend against every copy of the password that ever
//! existed: a `Vec` reallocation or a move before this type took ownership
//! can leave a copy elsewhere in memory that this `Drop` never touches, and
//! neither `zeroize` nor a hand-written `Drop` changes that. It closes the
//! specific gap the security audit named — one `Vec<u8>` sitting around
//! until process exit rather than being zeroed once it is no longer needed
//! — not the general problem.

/// An owned byte buffer, zeroed on drop.
pub struct SecretBytes(Vec<u8>);

impl SecretBytes {
    pub fn new(bytes: Vec<u8>) -> Self {
        SecretBytes(bytes)
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl std::ops::Deref for SecretBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for SecretBytes {
    fn from(bytes: Vec<u8>) -> Self {
        SecretBytes(bytes)
    }
}

impl Clone for SecretBytes {
    fn clone(&self) -> Self {
        SecretBytes(self.0.clone())
    }
}

/// Redacted: the whole point of this type is that the bytes never appear in
/// a log or a terminal by accident, including through a derived `Debug` on
/// whatever struct holds one.
impl std::fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretBytes(<redacted, {} bytes>)", self.0.len())
    }
}

impl PartialEq for SecretBytes {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Drop for SecretBytes {
    fn drop(&mut self) {
        // SAFETY: `self.0` is a `Vec<u8>` this type uniquely owns for the
        // duration of this call, so every index in `0..len` is a valid,
        // initialized, properly aligned `u8` to write through. The volatile
        // write stops the optimizer from treating the store as dead (the
        // buffer is about to be deallocated, so a plain store has no
        // observable effect it can prove matters); the fence stops it from
        // reordering these writes past the deallocation that follows.
        for byte in self.0.iter_mut() {
            unsafe { std::ptr::write_volatile(byte, 0) };
        }
        std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deref_exposes_the_bytes() {
        let secret = SecretBytes::new(vec![1, 2, 3]);
        assert_eq!(&*secret, &[1, 2, 3]);
        assert_eq!(secret.as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn debug_never_shows_the_bytes() {
        let secret = SecretBytes::new(b"s3cret".to_vec());
        let debug = format!("{secret:?}");
        assert!(!debug.contains("s3cret"), "leaked into Debug: {debug}");
        assert!(debug.contains("6 bytes"), "debug output was: {debug}");
    }

    #[test]
    fn equality_compares_the_bytes() {
        assert_eq!(SecretBytes::new(vec![1, 2]), SecretBytes::new(vec![1, 2]));
        assert_ne!(SecretBytes::new(vec![1, 2]), SecretBytes::new(vec![1, 3]));
    }

    /// Not a proof the memory was overwritten (that needs process-inspection
    /// tooling this test suite doesn't have), just that `Drop` runs without
    /// panicking or miscounting on the boundary cases: empty and non-empty.
    #[test]
    fn drop_runs_cleanly_on_empty_and_nonempty_buffers() {
        drop(SecretBytes::new(Vec::new()));
        drop(SecretBytes::new(vec![0xffu8; 64]));
    }
}
