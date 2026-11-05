// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Core module containing foundational abstractions, streams, error types, and the relay engine.
//! All comments and docstrings must be in English.

pub mod error;
pub mod failover;
pub mod relay;
pub mod stream;

pub use error::TunnelError;
pub use relay::RelayEngine;
pub use stream::{BoxFuture, TunnelStream};
