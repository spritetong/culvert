// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Server ingress runtime built on v2 `Coordinator`, `IoTask`, `TransportPool`, and `ChannelRelay`.
//! All comments and docstrings must be in English.

pub mod forward;
pub mod reverse;
pub mod runner;

#[cfg(test)]
mod tests;

pub use forward::{ServerForwardInput, ServerForwardWorkflow};
pub use reverse::{ReverseControlSession, ReverseSessionHub, RouterReverseSignaler};
pub use runner::ServerRunner;
