//! Common utilities and shared foundation components.
//! All comments and docstrings must be in English.

pub mod auth;
pub mod net;
pub mod task;

pub use auth::constant_time_token_eq;
pub use net::SocketTuner;
pub use task::TaskTracker;
