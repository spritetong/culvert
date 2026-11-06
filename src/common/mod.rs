// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
