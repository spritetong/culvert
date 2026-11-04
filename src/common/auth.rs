//! Cryptographic authentication and constant-time token comparison utilities.
//! All comments and docstrings must be in English.

use ring::digest::{SHA256, digest};
use subtle::ConstantTimeEq;

/// Maximum supported token length in bytes for constant-time comparison.
pub const TOKEN_MAX_LEN: usize = 256;

/// Compares two token byte sequences in constant time without leaking length information.
///
/// Standard slice `ConstantTimeEq` returns immediately if slice lengths differ,
/// leaking token length through side channels. This function copies up to 256 bytes
/// into fixed-size buffers, enforcing constant SHA-256 block compression counts before
/// constant-time comparison. Inputs exceeding 256 bytes are rejected in constant time
/// without timing leakages.
pub fn constant_time_token_eq(candidate: &[u8], expected: &[u8]) -> bool {
    let eq_len = candidate.len().ct_eq(&expected.len());
    let mut cand_buf = [0u8; TOKEN_MAX_LEN];
    let mut exp_buf = [0u8; TOKEN_MAX_LEN];

    let cand_len = candidate.len().min(TOKEN_MAX_LEN);
    let exp_len = expected.len().min(TOKEN_MAX_LEN);
    cand_buf[..cand_len].copy_from_slice(&candidate[..cand_len]);
    exp_buf[..exp_len].copy_from_slice(&expected[..exp_len]);

    let hash_cand = digest(&SHA256, &cand_buf);
    let hash_exp = digest(&SHA256, &exp_buf);
    let overflow = (candidate.len() > TOKEN_MAX_LEN) | (expected.len() > TOKEN_MAX_LEN);
    bool::from(
        hash_cand.as_ref().ct_eq(hash_exp.as_ref())
            & eq_len
            & !subtle::Choice::from(overflow as u8),
    )
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
        assert!(!constant_time_token_eq(
            b"my_secret_token_12345_extra_long_payload",
            expected
        ));
        assert!(!constant_time_token_eq(b"", expected));

        // Exactly TOKEN_MAX_LEN (256 bytes)
        let token_256 = vec![b'a'; TOKEN_MAX_LEN];
        assert!(constant_time_token_eq(&token_256, &token_256));

        // Tokens exceeding TOKEN_MAX_LEN (256 bytes) must be rejected in constant time
        let token_300 = vec![b'a'; 300];
        assert!(
            !constant_time_token_eq(&token_300, &token_300),
            "Tokens > 256 bytes must be rejected"
        );
        assert!(!constant_time_token_eq(&token_300, &token_256));
        assert!(!constant_time_token_eq(&token_256, &token_300));

        let token_257 = vec![b'a'; 257];
        assert!(!constant_time_token_eq(&token_257, &token_256));
    }
}
