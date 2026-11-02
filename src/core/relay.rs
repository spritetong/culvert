//! High-performance bidirectional byte relay engine.
//! All comments and docstrings must be in English.

use crate::constants::RELAY_BUFFER_SIZE;
use crate::core::error::TunnelError;
use crate::core::stream::TunnelStream;
use tokio::io::copy_bidirectional_with_sizes;

/// Relays bytes bidirectionally between two dynamic stream trait objects.
///
/// Uses an optimized buffer size (64 KB) to minimize context switching
/// and maximize throughput across high-bandwidth networks.
pub struct RelayEngine;

impl RelayEngine {
    /// Copies data bidirectionally between stream_a and stream_b until EOF or error.
    /// Returns the number of bytes transferred in both directions (a_to_b, b_to_a).
    pub async fn relay(
        mut stream_a: Box<dyn TunnelStream>,
        mut stream_b: Box<dyn TunnelStream>,
    ) -> Result<(u64, u64), TunnelError> {
        let (from_a_to_b, from_b_to_a) = copy_bidirectional_with_sizes(
            &mut stream_a,
            &mut stream_b,
            RELAY_BUFFER_SIZE,
            RELAY_BUFFER_SIZE,
        )
        .await
        .map_err(TunnelError::Io)?;

        Ok((from_a_to_b, from_b_to_a))
    }
}
