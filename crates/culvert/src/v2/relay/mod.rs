// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Pure data-plane relay implementations decoupled from external cancellation tokens.
//! All comments and docstrings must be in English.

pub mod direction;
pub mod duplex;

#[cfg(test)]
mod tests;

pub use direction::TransferDirection;
pub use duplex::{DuplexRelay, RelayClosing, RelayCounters};
