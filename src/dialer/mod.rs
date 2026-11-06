// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Dialer module responsible for URL parsing, strategy-based DNS resolution, TLS, and dialing.
//! All comments and docstrings must be in English.

pub mod dns;
pub mod endpoint;
pub mod tls;
pub mod unified;

pub use dns::{CachedDnsResolver, DnsResolver};
pub use endpoint::{Endpoint, EndpointContext};
pub use tls::TlsConfigBuilder;
pub use unified::{DefaultDialer, Dialer};
