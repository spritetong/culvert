// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Pure data-plane relay implementations decoupled from external cancellation tokens.
//! All comments and docstrings must be in English.

pub mod channel;
pub mod datagram_datagram;
pub mod datagram_queue;
pub mod datagram_stream;
pub mod direction;
pub mod duplex;

#[cfg(test)]
mod tests;

pub use channel::{ChannelClosing, ChannelRelay};
pub use datagram_datagram::{DatagramDatagramClosing, DatagramDatagramRelay};
pub use datagram_queue::DatagramQueue;
pub use datagram_stream::{DatagramStreamClosing, DatagramStreamRelay};
pub use direction::TransferDirection;
pub use duplex::{DuplexRelay, RelayClosing, RelayCounters};

/// Drives bounded graceful-drain cleanup for a relay `ClosingStage`.
///
/// Reaches completion on clean drain or when `drain_timeout` expires, while
/// propagating genuine underlying I/O errors so failed drains are not masked as
/// `TaskOutcome::GracefullyDrained`.
pub(super) async fn run_bounded_closing<R>(
    mut relay: Option<R>,
    reason: crate::v2::CloseReason,
    drain_timeout: std::time::Duration,
    mut poll_drain: impl FnMut(
        &mut R,
        &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), crate::v2::TaskError>>,
    sync_counters: impl FnOnce(&R),
) -> Result<(), crate::v2::TaskError> {
    if matches!(reason, crate::v2::CloseReason::GracefulDrain)
        && let Some(ref mut r) = relay
    {
        let drain_res =
            tokio::time::timeout(drain_timeout, std::future::poll_fn(|cx| poll_drain(r, cx))).await;
        sync_counters(r);
        match drain_res {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err),
            Err(_) => {
                // Drain deadline reached; continue bounded cleanup.
            }
        }
    }
    drop(relay);
    Ok(())
}
