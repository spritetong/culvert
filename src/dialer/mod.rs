// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
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
