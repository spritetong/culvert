// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! HTTP/3 (QUIC) transport driver implementation utilizing `quinn` and `h3`.
//! All comments and docstrings must be in English.

pub mod connector;
pub mod listener;
pub mod stream;

pub use connector::H3Connector;
pub use listener::H3Listener;
pub use stream::{H3TunnelStream, QuinnTunnelStream};

use std::sync::Arc;

use crate::config::model::{QuicConfig, QuicCongestion, TunnelMode};
use crate::consts::{
    ALPN_H3, ALPN_QUIC, DEFAULT_QUIC_ACK_ELICITING_THRESHOLD,
    DEFAULT_QUIC_ACK_REORDERING_THRESHOLD, DEFAULT_QUIC_MTU_UPPER_BOUND, SCHEME_H3, SCHEME_HTTPS,
    SCHEME_QUIC,
};
use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;
use crate::transport::{TransportConnector, TransportDriver, TransportListener};
use tokio_util::sync::CancellationToken;

fn effective_mtu(quic: &QuicConfig) -> u16 {
    quic.mtu_upper_bound.unwrap_or(DEFAULT_QUIC_MTU_UPPER_BOUND)
}

/// Endpoint-level QUIC socket configuration. `max_udp_payload_size` is also
/// advertised to the peer, so it must be set on listeners and connectors alike.
pub(crate) fn quic_endpoint_config(quic: &QuicConfig) -> Result<quinn::EndpointConfig, TunnelError> {
    let mut config = quinn::EndpointConfig::default();
    let mtu = effective_mtu(quic);
    config
        .max_udp_payload_size(mtu)
        .map_err(|e| TunnelError::Config(format!("Invalid quic.mtu_upper_bound {}: {}", mtu, e)))?;
    Ok(config)
}

/// Applies the exposed QUIC transport knobs. Everything else stays at quinn's
/// defaults: initial MTU, loss thresholds, and RTT estimation are not config
/// surface because lowering them trades WAN stability for tail latency.
pub(crate) fn apply_quic_config(config: &mut quinn::TransportConfig, quic: &QuicConfig) {
    let mut discovery = quinn::MtuDiscoveryConfig::default();
    discovery.upper_bound(effective_mtu(quic));
    config.mtu_discovery_config(Some(discovery));

    if quic.immediate_ack.unwrap_or(true) {
        let mut ack = quinn::AckFrequencyConfig::default();
        ack.ack_eliciting_threshold(quinn::VarInt::from_u32(
            DEFAULT_QUIC_ACK_ELICITING_THRESHOLD,
        ));
        ack.max_ack_delay(Some(std::time::Duration::ZERO));
        ack.reordering_threshold(quinn::VarInt::from_u32(
            DEFAULT_QUIC_ACK_REORDERING_THRESHOLD,
        ));
        config.ack_frequency_config(Some(ack));
    } else {
        config.ack_frequency_config(None);
    }

    match quic.congestion.unwrap_or_default() {
        QuicCongestion::Cubic => {
            config
                .congestion_controller_factory(Arc::new(quinn::congestion::CubicConfig::default()));
        }
        QuicCongestion::Bbr => {
            config.congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default()));
        }
        QuicCongestion::NewReno => {
            config.congestion_controller_factory(Arc::new(
                quinn::congestion::NewRenoConfig::default(),
            ));
        }
    }
}

/// HTTP/3 transport driver handling `h3`, `https`, and `quic` schemes.
pub struct H3TransportDriver;

impl TransportDriver for H3TransportDriver {
    fn schemes(&self) -> &[&'static str] {
        &[SCHEME_H3, SCHEME_HTTPS, SCHEME_QUIC]
    }

    fn build_connector(
        &self,
        endpoint: &EndpointContext,
        auth_token: &str,
        mode: TunnelMode,
    ) -> Result<Box<dyn TransportConnector>, TunnelError> {
        let connector = H3Connector::new(endpoint, auth_token, mode)?;
        Ok(Box::new(connector))
    }

    fn build_listener(
        &self,
        listen_addr: &str,
        auth_token: Option<&str>,
        path: Option<&str>,
        tls: Option<&crate::config::model::TlsConfig>,
        quic: &QuicConfig,
        cancel: CancellationToken,
    ) -> Result<Box<dyn TransportListener>, TunnelError> {
        let mut alpn = tls
            .and_then(|t| t.alpn.as_ref())
            .map(|list| {
                list.iter()
                    .map(|s| s.as_bytes().to_vec())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !alpn.iter().any(|p| p == ALPN_H3) {
            alpn.push(ALPN_H3.to_vec());
        }
        if !alpn.iter().any(|p| p == ALPN_QUIC) {
            alpn.push(ALPN_QUIC.to_vec());
        }
        let cert_str = tls
            .and_then(|t| t.certificate.as_ref())
            .and_then(|p| p.to_str());
        let key_str = tls.and_then(|t| t.key.as_ref()).and_then(|p| p.to_str());
        let (min_v, max_v) = match tls {
            Some(t) => (t.min_version.as_deref(), t.max_version.as_deref()),
            None => (None, None),
        };

        let server_rustls = crate::dialer::tls::TlsConfigBuilder::build_server_config(
            cert_str,
            key_str,
            Some(alpn),
            min_v,
            max_v,
        )?;

        let listener =
            H3Listener::bind_with_quic(listen_addr, auth_token, path, server_rustls, quic, cancel)?;
        Ok(Box::new(listener))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quic_endpoint_config_valid() {
        let quic = QuicConfig {
            mtu_upper_bound: Some(8192),
            ..Default::default()
        };
        let cfg = quic_endpoint_config(&quic);
        assert!(cfg.is_ok());
    }

    #[test]
    fn test_quic_endpoint_config_invalid_returns_err() {
        let quic = QuicConfig {
            mtu_upper_bound: Some(100),
            ..Default::default()
        };
        let cfg = quic_endpoint_config(&quic);
        assert!(cfg.is_err());
    }
}
