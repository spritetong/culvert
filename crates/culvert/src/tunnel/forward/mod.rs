// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! TCP/stream Forward workflow (`TaskWorkflow`) and runner integrating `Frontend`,
//! `TransportConnector`, `Coordinator` (with embedded `Idle` + `DialDriver`), and `ChannelRelay`.
//! All comments and docstrings must be in English.

pub mod runner;
pub mod workflow;

pub use runner::ForwardRunner;
pub use workflow::{ForwardInput, ForwardWorkflow};
