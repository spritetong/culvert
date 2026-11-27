// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Reverse tunnel (`-R`) client and server workflows built on `Coordinator`,
//! `TransportPool`, and `ChannelRelay`.
//! All comments and docstrings must be in English.

pub mod client;
pub mod runner;
pub mod visitor;

#[cfg(test)]
mod tests;

pub use client::{
    PrewarmSignal, ReverseClientConfig, ReverseClientInput, ReverseClientRunner,
    ReverseClientWorkflow, ReverseReadyNotifier,
};
pub use runner::ReverseServerRunner;
pub use visitor::{ReverseSignaler, ReverseVisitorInput, ReverseVisitorWorkflow};
