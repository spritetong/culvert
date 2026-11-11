// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Concrete backend drivers (WS/WSS, H3/QUIC, UDS) and re-exports of the
//! backend traits they implement. No interface is defined here.
//! All comments and docstrings must be in English.

pub use crate::backend::registry::TransportRegistry;
pub use crate::backend::{
    AcceptOutput, StreamMetadata, TransportConnector, TransportDriver, TransportListener,
    accept_one,
};
pub use crate::consts::ACCEPTED_STREAM_QUEUE_SIZE;
pub use crate::io::addr::TargetAddr;

/// Runs a blocking address operation (DNS resolution plus socket bind) off the
/// async worker when the runtime has a blocking pool. `getaddrinfo` has no
/// timeout, so pinning a Tokio worker with it can stall unrelated tasks. On a
/// current-thread runtime there is no blocking pool to hand off to, so the
/// operation has to run inline.
pub(crate) fn run_blocking_addr_op<R>(f: impl FnOnce() -> R) -> R {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        _ => f(),
    }
}

pub mod h3;
pub mod uds;
pub mod ws;
