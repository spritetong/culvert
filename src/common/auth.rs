//! Cryptographic authentication and constant-time token comparison utilities.
//! All comments and docstrings must be in English.

use ring::digest::{SHA256, digest};
use subtle::ConstantTimeEq;

const TOKEN_MAX_LEN: usize = 256;

fn pad_to_fixed_len(input: &[u8]) -> Option<[u8; TOKEN_MAX_LEN]> {
    if input.len() > TOKEN_MAX_LEN {
        return None;
    }
    let mut buf = [0u8; TOKEN_MAX_LEN];
    buf[..input.len()].copy_from_slice(input);
    Some(buf)
}

/// Compares two token byte sequences in constant time without leaking length information.
///
/// Standard slice `ConstantTimeEq` returns immediately if slice lengths differ,
/// leaking token length through side channels. This function pads inputs up to 256 bytes
/// to enforce constant SHA-256 block compression counts before constant-time comparison,
/// preventing length-based timing side channels.
pub fn constant_time_token_eq(candidate: &[u8], expected: &[u8]) -> bool {
    let eq_len = candidate.len().ct_eq(&expected.len());
    match (pad_to_fixed_len(candidate), pad_to_fixed_len(expected)) {
        (Some(c), Some(e)) => {
            let hash_cand = digest(&SHA256, &c);
            let hash_exp = digest(&SHA256, &e);
            bool::from(hash_cand.as_ref().ct_eq(hash_exp.as_ref()) & eq_len)
        }
        (None, None) => {
            let hash_cand = digest(&SHA256, candidate);
            let hash_exp = digest(&SHA256, expected);
            bool::from(hash_cand.as_ref().ct_eq(hash_exp.as_ref()) & eq_len)
        }
        _ => false,
    }
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
