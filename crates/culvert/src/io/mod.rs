// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Neutral I/O vocabulary shared by frontend, core, and backend.
//! Contains no business logic and no reference to a concrete transport driver.
//! All comments and docstrings must be in English.

pub mod addr;
pub mod channel;
pub mod error;
pub mod pump;
pub mod stream;

pub use addr::{L4Protocol, MappingSpec, PeerAddr, TargetAddr, parse_target_addr};
pub use channel::{ChannelKind, Datagram, DatagramIo, FlowKey, IoChannel};
pub use error::TunnelError;
pub use pump::{Pump, PumpCtx, PumpRegistry, PumpRequest, StreamStreamPump, pump_registry};
pub use stream::{
    BoxFuture, BufferedTlsStream, HeartbeatStatus, StreamCaps, TunnelStream, activity_now_ms,
    heartbeat_timing,
};
