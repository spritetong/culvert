// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Configuration module providing declarative YAML models and file loaders.
//! All comments and docstrings must be in English.

pub mod loader;
pub mod model;

pub use loader::ConfigLoader;
pub use model::{
    AppConfig, DialerConfig, DnsConfig, DnsResolverConfig, DnsServerConfig, DnsStrategy,
    DnsTlsConfig, EndpointConfig, FailoverConfig, LogConfig, MappingGroupConfig, MappingRuleConfig,
    PortMappingConfig, ServerConfig, TlsConfig, TunnelConfig, TunnelMappingsConfig, TunnelMode,
    TunnelRetryConfig,
};
