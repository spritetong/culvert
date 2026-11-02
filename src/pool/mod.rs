//! Connection pool module for pre-warmed tunnel connections.
//! All comments and docstrings must be in English.

pub mod connection_pool;

pub use connection_pool::{InMemoryStreamPool, PoolStats, ReplenishingStreamPool, StreamPool};
