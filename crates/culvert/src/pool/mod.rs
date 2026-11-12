// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Connection pool module for pre-warmed tunnel connections.
//! All comments and docstrings must be in English.

pub mod activation;
pub mod connection_pool;

pub use activation::{ActivationHeader, decode_activation, encode_activation_with_proto};
pub use connection_pool::{InMemoryStreamPool, PoolStats, ReplenishingStreamPool, StreamPool};
