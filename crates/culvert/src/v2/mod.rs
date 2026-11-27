// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Experimental v2 Coordinator + IO Task state-machine prototype.
//! Isolated from existing `core`, `tunnel`, `pool`, and `io/pump` implementations.
//! All comments and docstrings must be in English.

pub mod cluster;
pub mod coordinator;
pub mod engine;
pub mod error;
pub mod forward;
pub mod pool;
pub mod relay;
pub mod reverse;
pub mod server;
pub mod stage;
pub mod state;
pub mod supervisor;
pub mod task;

#[cfg(test)]
mod tests;

pub use cluster::TunnelCluster;
pub use coordinator::{
    CompletedJob, Coordinator, CoordinatorHandle, CoordinatorSummary, Microtask,
};
pub use engine::StateEngine;
pub use error::TaskError;
pub use forward::{ForwardInput, ForwardRunner, ForwardWorkflow};
pub use pool::{
    LeaseGuard, PoolConfig, PoolDialConfig, PoolHandle, PoolInventory, PoolRole, PoolStats,
    PoolSummary, StreamLease, SweepOutcome, TransportPool,
};
pub use relay::{
    ChannelClosing, ChannelRelay, DatagramDatagramClosing, DatagramDatagramRelay,
    DatagramStreamClosing, DatagramStreamRelay, DuplexRelay, RelayClosing, RelayCounters,
    TransferDirection,
};
pub use reverse::{
    ReverseClientConfig, ReverseClientInput, ReverseClientRunner, ReverseClientWorkflow,
    ReverseReadyNotifier, ReverseServerRunner, ReverseSignaler, ReverseVisitorInput,
    ReverseVisitorWorkflow,
};
pub use server::{
    ReverseControlSession, ReverseSessionHub, RouterReverseSignaler, ServerForwardInput,
    ServerForwardWorkflow, ServerRunner,
};
pub use stage::{ActiveStage, ClosingStage, ConnectingContext, TaskWorkflow};
pub use state::{
    CloseReason, ExternalState, JobId, TaskDirective, TaskId, TaskOutcome, TunnelPhase, WaitResult,
};
pub use supervisor::{ClientRunner, TunnelSupervisor};
pub use task::IoTask;
