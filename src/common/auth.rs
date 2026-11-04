//! Cryptographic authentication and constant-time token comparison utilities.
//! All comments and docstrings must be in English.

use ring::digest::{SHA256, digest};
use subtle::ConstantTimeEq;

/// Compares two token byte sequences in constant time without leaking length information.
///
/// Standard slice `ConstantTimeEq` returns immediately if slice lengths differ,
/// leaking token length through side channels. This function pre-hashes both slices
/// with SHA-256 to ensure comparison is strictly constant time across all inputs.
pub fn constant_time_token_eq(candidate: &[u8], expected: &[u8]) -> bool {
    let hash_cand = digest(&SHA256, candidate);
    let hash_exp = digest(&SHA256, expected);
    bool::from(hash_cand.as_ref().ct_eq(hash_exp.as_ref()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constant_time_token_eq() {
        let expected = b"my_secret_token_12345";

        assert!(constant_time_token_eq(expected, expected));
        assert!(constant_time_token_eq(b"my_secret_token_12345", expected));

        // Different content, same length
        assert!(!constant_time_token_eq(b"my_secret_token_54321", expected));

        // Different length (shorter / longer)
        assert!(!constant_time_token_eq(b"short", expected));
        assert!(!constant_time_token_eq(b"my_secret_token_12345_extra_long_payload", expected));
        assert!(!constant_time_token_eq(b"", expected));
    }
}
