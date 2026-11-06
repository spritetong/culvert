// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Common utilities and shared foundation components.
//! All comments and docstrings must be in English.

pub mod auth;
pub mod cancel;
pub mod net;
pub mod task;

pub use auth::constant_time_token_eq;
pub use cancel::{CancelFutureExt, CancellationTokenExt, TimeoutOrCancel};
pub use net::SocketTuner;
pub use task::TaskTracker;
