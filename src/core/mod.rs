// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Core module containing foundational abstractions, streams, error types, and the relay engine.
//! All comments and docstrings must be in English.

pub mod error;
pub mod failover;
pub mod relay;
pub mod stream;

pub use error::TunnelError;
pub use relay::RelayEngine;
pub use stream::{BoxFuture, TunnelStream};
