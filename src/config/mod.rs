// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.
//! Configuration module providing declarative YAML models and file loaders.
//! All comments and docstrings must be in English.

pub mod loader;
pub mod model;

pub use loader::ConfigLoader;
pub use model::{
    AppConfig, DialerConfig, DnsConfig, DnsStrategy, EndpointConfig, FailoverConfig, LogConfig,
    ServerConfig, TlsConfig, TunnelConfig, TunnelMode,
};
