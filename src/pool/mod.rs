// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Connection pool module for pre-warmed tunnel connections.
//! All comments and docstrings must be in English.

pub mod connection_pool;

pub use connection_pool::{InMemoryStreamPool, PoolStats, ReplenishingStreamPool, StreamPool};
