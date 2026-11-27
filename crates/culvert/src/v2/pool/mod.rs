// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Unified v2 `TransportPool` supporting both Forward active pre-warming and
//! Reverse passive inbound queueing.
//! All comments and docstrings must be in English.

pub mod actor;
pub mod config;
pub mod inventory;
pub mod lease;

#[cfg(test)]
mod tests;

pub use actor::{PoolHandle, TransportPool};
pub use config::{PoolConfig, PoolDialConfig, PoolRole, PoolStats, PoolSummary};
pub use inventory::{PoolInventory, SweepOutcome};
pub use lease::{LeaseGuard, PoolReleaseSignal, StreamLease};
