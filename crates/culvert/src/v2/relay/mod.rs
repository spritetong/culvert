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
