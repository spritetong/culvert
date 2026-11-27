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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use parking_lot::Mutex;

use crate::config::model::{QuicConfig, QuicCongestion, TunnelMode};
use crate::consts::{
    ALPN_H3, ALPN_QUIC, DEFAULT_QUIC_ACK_ELICITING_THRESHOLD,
    DEFAULT_QUIC_ACK_REORDERING_THRESHOLD, DEFAULT_QUIC_MTU_UPPER_BOUND,
    DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD, SCHEME_H3, SCHEME_HTTPS, SCHEME_QUIC,
};
use crate::core::error::TunnelError;
use crate::dialer::endpoint::EndpointContext;
use crate::transport::{TransportConnector, TransportDriver, TransportListener};
use tokio_util::sync::CancellationToken;

/// Shared health state for all streams on one QUIC connection.
///
/// Connector caches, relay streams and listener handlers all consult this handle,
/// so "dead" has one meaning and one close path regardless of tunnel direction.
#[derive(Clone)]
pub(crate) struct QuicSessionHandle {
    conn: quinn::Connection,
    dead: Arc<AtomicBool>,
    liveness: Arc<Mutex<QuicLiveness>>,
}

struct QuicLiveness {
    last_rx_dgrams: u64,
    last_tx_dgrams: u64,
    stalled_since: Option<Instant>,
    stall_start_tx: u64,
}

impl QuicLiveness {
    fn new(conn: &quinn::Connection) -> Self {
        let stats = conn.stats();
        Self {
            last_rx_dgrams: stats.udp_rx.datagrams,
            last_tx_dgrams: stats.udp_tx.datagrams,
            stalled_since: None,
            stall_start_tx: 0,
        }
    }

    /// Returns true only when subsequent TX attempts confirm an unbroken TX-without-RX
    /// window beyond `DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD`.
    ///
    /// The first TX without RX starts the clock and anchors `stall_start_tx`.
    /// Subsequent calls without RX remain healthy if no new TX was attempted
    /// (`cur_tx == stall_start_tx`, e.g. a single non-ack-eliciting pure ACK frame
    /// followed by routine idle).
    /// Once subsequent transmissions are observed (`cur_tx > stall_start_tx`) and
    /// silence persists beyond the threshold, the session is declared dead without
    /// requiring a new TX right at the exact instant of the liveness check.
    fn observe(&mut self, cur_rx: u64, cur_tx: u64, now: Instant) -> bool {
        if cur_rx > self.last_rx_dgrams {
            self.last_rx_dgrams = cur_rx;
            self.last_tx_dgrams = cur_tx;
            self.stalled_since = None;
            self.stall_start_tx = 0;
            return false;
        }

        if cur_tx > self.last_tx_dgrams {
            self.last_tx_dgrams = cur_tx;
            if self.stalled_since.is_none() {
                self.stalled_since = Some(now);
                self.stall_start_tx = cur_tx;
                return false;
            }
        }

        if let Some(since) = self.stalled_since
            && cur_tx > self.stall_start_tx
            && now.duration_since(since) >= DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD
        {
            return true;
        }

        false
    }
}

impl QuicSessionHandle {
    pub(crate) fn new(conn: quinn::Connection) -> Self {
        let liveness = QuicLiveness::new(&conn);
        Self {
            conn,
            dead: Arc::new(AtomicBool::new(false)),
            liveness: Arc::new(Mutex::new(liveness)),
        }
    }

    pub(crate) fn conn(&self) -> &quinn::Connection {
        &self.conn
    }

    pub(crate) fn session_id(&self) -> u64 {
        self.conn.stable_id() as u64
    }

    pub(crate) fn is_alive(&self) -> bool {
        !self.dead.load(Ordering::Acquire) && self.conn.close_reason().is_none()
    }

    pub(crate) fn mark_dead(&self, reason: &'static [u8]) {
        if !self.dead.swap(true, Ordering::AcqRel) {
            self.conn.close(quinn::VarInt::from_u32(0), reason);
        }
    }

    /// Returns false only after two TX-without-RX samples confirm that the
    /// whole connection is still transmitting while receiving nothing. The
    /// state is shared by every stream, so one stream's observation applies to
    /// the session rather than a copy.
    pub(crate) fn check_liveness(&self) -> bool {
        if !self.is_alive() {
            return false;
        }

        let stats = self.conn.stats();
        let cur_rx = stats.udp_rx.datagrams;
        let cur_tx = stats.udp_tx.datagrams;
        let stalled = {
            let mut liveness = self.liveness.lock();
            liveness.observe(cur_rx, cur_tx, Instant::now())
        };

        if stalled {
            tracing::warn!(
                cur_tx,
                cur_rx,
                conn = ?self.conn,
                "QUIC session unresponsive: confirmed transmitting without receiving datagrams"
            );
            self.mark_dead(b"quic session unresponsive");
            false
        } else {
            true
        }
    }
}

fn effective_mtu(quic: &QuicConfig) -> u16 {
    quic.mtu_upper_bound.unwrap_or(DEFAULT_QUIC_MTU_UPPER_BOUND)
}

/// Endpoint-level QUIC socket configuration. `max_udp_payload_size` is also
/// advertised to the peer, so it must be set on listeners and connectors alike.
pub(crate) fn quic_endpoint_config(
    quic: &QuicConfig,
) -> Result<quinn::EndpointConfig, TunnelError> {
    let mut config = quinn::EndpointConfig::default();
    let mtu = effective_mtu(quic);
    config
        .max_udp_payload_size(mtu)
        .map_err(|e| TunnelError::Config(format!("Invalid quic.mtu_upper_bound {}: {}", mtu, e)))?;
    Ok(config)
}

/// Applies the exposed QUIC transport knobs and tunes parameters for resilient
/// WAN and weak network operation.
pub(crate) fn apply_quic_config(config: &mut quinn::TransportConfig, quic: &QuicConfig) {
    let mut discovery = quinn::MtuDiscoveryConfig::default();
    discovery.upper_bound(effective_mtu(quic));
    config.mtu_discovery_config(Some(discovery));

    // Immediate ACK is opt-in (defaults to false) to adhere to RFC 9000 delayed ACK
    // behavior, protecting asymmetric and weak network uplinks from ACK saturation.
    if quic.immediate_ack.unwrap_or(false) {
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

    // Default to BBR congestion control: model-based rate/RTT tracking is vastly
    // superior to loss-based Cubic on jittery, lossy, and bufferbloated weak networks.
    match quic.congestion.unwrap_or_default() {
        QuicCongestion::Bbr => {
            config.congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default()));
        }
        QuicCongestion::Cubic => {
            config
                .congestion_controller_factory(Arc::new(quinn::congestion::CubicConfig::default()));
        }
        QuicCongestion::NewReno => {
            config.congestion_controller_factory(Arc::new(
                quinn::congestion::NewRenoConfig::default(),
            ));
        }
    }

    // Weak network enhancements:
    // 1. Enable fair queuing across concurrent send streams so bulk transfers do not
    //    starve control messages or heartbeat probes on bandwidth-constrained links.
    config.send_fairness(true);

    // 2. Expand crypto buffer to 64 KiB so multi-flight handshakes and reordered
    //    crypto frames under high packet loss do not exhaust buffer space.
    config.crypto_buffer_size(64 * 1024);
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
        if !alpn.iter().any(|p| p == ALPN_H3.as_bytes()) {
            alpn.push(ALPN_H3.as_bytes().to_vec());
        }
        if !alpn.iter().any(|p| p == ALPN_QUIC.as_bytes()) {
            alpn.push(ALPN_QUIC.as_bytes().to_vec());
        }
        let cert_str = tls
            .and_then(|t| t.certificate.as_ref())
            .and_then(|p| p.to_str());
        let key_str = tls.and_then(|t| t.key.as_ref()).and_then(|p| p.to_str());
        let client_ca_str = tls
            .and_then(|t| t.effective_client_ca_certificate())
            .and_then(|p| p.to_str());
        let client_auth = tls.map(|t| t.effective_client_auth());
        let (min_v, max_v) = match tls {
            Some(t) => (t.min_version.as_deref(), t.max_version.as_deref()),
            None => (None, None),
        };

        let server_rustls = crate::dialer::tls::TlsConfigBuilder::build_dynamic_server_config(
            &crate::cert::CertManager::new(),
            cert_str,
            key_str,
            client_ca_str,
            client_auth,
            Some(alpn),
            min_v,
            max_v,
            cancel.clone(),
        )?;

        let listener =
            H3Listener::bind_with_quic(listen_addr, auth_token, path, server_rustls, quic, cancel)?;
        Ok(Box::new(listener))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn stalled_tracker() -> QuicLiveness {
        QuicLiveness {
            last_rx_dgrams: 10,
            last_tx_dgrams: 20,
            stalled_since: None,
            stall_start_tx: 0,
        }
    }

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

    #[test]
    fn test_quic_liveness_single_tx_then_idle_is_healthy() {
        let now = Instant::now();
        let mut liveness = stalled_tracker();

        assert!(!liveness.observe(10, 21, now));
        assert!(!liveness.observe(
            10,
            21,
            now + DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD + Duration::from_secs(1)
        ));
    }

    #[test]
    fn test_quic_liveness_continued_tx_without_rx_is_dead() {
        let now = Instant::now();
        let mut liveness = stalled_tracker();

        assert!(!liveness.observe(10, 21, now));
        assert!(!liveness.observe(10, 22, now + Duration::from_secs(5)));
        assert!(liveness.observe(10, 23, now + DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD));
    }

    #[test]
    fn test_quic_liveness_no_tx_polls_do_not_reset_stall_clock() {
        let now = Instant::now();
        let mut liveness = stalled_tracker();

        assert!(!liveness.observe(10, 21, now));
        for sec in 1..DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD.as_secs() {
            assert!(!liveness.observe(10, 21, now + Duration::from_secs(sec)));
        }
        assert!(liveness.observe(10, 22, now + DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD));
    }

    #[test]
    fn test_quic_liveness_rx_resets_stall_clock() {
        let now = Instant::now();
        let mut liveness = stalled_tracker();

        assert!(!liveness.observe(10, 21, now));
        assert!(!liveness.observe(11, 22, now + DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD));
        assert!(!liveness.observe(
            11,
            23,
            now + DEFAULT_QUIC_UNRESPONSIVE_THRESHOLD + Duration::from_secs(1)
        ));
    }

    #[test]
    fn test_quic_liveness_dead_session_detected_without_tx_on_eval_instant() {
        let now = Instant::now();
        let mut liveness = stalled_tracker();

        // 1. Initial TX starts the stall clock with stall_start_tx = 21
        assert!(!liveness.observe(10, 21, now));

        // 2. Transmissions continue without RX at T = 5s and T = 9s
        assert!(!liveness.observe(10, 22, now + Duration::from_secs(5)));
        assert!(!liveness.observe(10, 23, now + Duration::from_secs(9)));

        // 3. At T = 11s, a check occurs where cur_tx has NOT increased between 9s and 11s (still 23).
        // Because cur_tx (23) > stall_start_tx (21) and elapsed (11s) >= threshold (10s),
        // it must reliably detect the dead session instead of missing the stall.
        assert!(liveness.observe(10, 23, now + Duration::from_secs(11)));
    }
}
