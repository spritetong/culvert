// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Core session lifecycle and pump dispatch. Core never moves bytes itself.
//! All comments and docstrings must be in English.

pub mod error;
pub mod failover;
pub mod serve;
pub mod stream;

pub use error::TunnelError;
pub use serve::serve;
pub use stream::{BoxFuture, TunnelStream};
