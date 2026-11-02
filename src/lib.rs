//! micro-tunnel core library.
//! All comments and docstrings must be in English.

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

pub mod common;
pub mod config;
pub mod constants;
pub mod control;
pub mod core;
pub mod dialer;
pub mod mux;
pub mod pool;
pub mod server;
pub mod session;
pub mod supervisor;
pub mod transport;
pub mod tunnel;
