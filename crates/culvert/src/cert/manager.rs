// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Central certificate manager coordinating file reading, watchers, and dynamic resolvers.
//! All comments and docstrings must be in English.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rustls::server::WebPkiClientVerifier;
use rustls::sign::CertifiedKey;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::model::ClientAuthMode;
use crate::core::error::TunnelError;

use super::events::{CertEvent, CertEventBus};
use super::reader::CertReader;
use super::resolver::{DynamicCertResolver, DynamicClientCertVerifier};
use super::watcher::{CertFileWatcher, DEFAULT_DEBOUNCE_INTERVAL};

/// Coordinates TLS certificate lifecycle, unified reading, file watching, and dynamic reloading.
#[derive(Clone)]
pub struct CertManager {
    event_bus: CertEventBus,
    debounce_interval: Duration,
}

impl CertManager {
    /// Creates a new `CertManager` with default debounce settings.
    pub fn new() -> Self {
        Self {
            event_bus: CertEventBus::default(),
            debounce_interval: DEFAULT_DEBOUNCE_INTERVAL,
        }
    }

    /// Creates a new `CertManager` with a custom debounce duration.
    pub fn with_debounce_interval(debounce_interval: Duration) -> Self {
        Self {
            event_bus: CertEventBus::default(),
            debounce_interval,
        }
    }

    /// Returns a reference to the event bus.
    pub fn event_bus(&self) -> &CertEventBus {
        &self.event_bus
    }

    /// Subscribes to certificate lifecycle events.
    pub fn subscribe_events(&self) -> tokio::sync::broadcast::Receiver<CertEvent> {
        self.event_bus.subscribe()
    }

    /// Resolves or registers server certificate credentials into a `DynamicCertResolver`.
    /// - When paths are provided: loads and validates from disk, spawns a debounced file watcher.
    /// - When paths are omitted: generates an ephemeral self-signed certificate (zero-config fallback).
    pub fn resolve_server_cert(
        &self,
        cert_path: Option<&str>,
        key_path: Option<&str>,
        cancel: CancellationToken,
    ) -> Result<Arc<DynamicCertResolver>, TunnelError> {
        match (cert_path, key_path) {
            (Some(cp), Some(kp)) => {
                let cert_p = PathBuf::from(cp);
                let key_p = PathBuf::from(kp);
                self.register_server_cert(cert_p, key_p, cancel)
            }
            (None, None) => {
                let certified_key = Self::generate_self_signed()?;
                Ok(Arc::new(DynamicCertResolver::new(certified_key)))
            }
            _ => Err(TunnelError::Tls(
                "Both server certificate and private key must be provided together".to_string(),
            )),
        }
    }

    /// Registers a server certificate and private key pair, validating initial credentials
    /// and starting a debounced background watcher that updates the resolver upon file changes.
    pub fn register_server_cert(
        &self,
        cert_path: PathBuf,
        key_path: PathBuf,
        cancel: CancellationToken,
    ) -> Result<Arc<DynamicCertResolver>, TunnelError> {
        // Initial load and validation
        let initial_key = CertReader::read_server_credentials(&cert_path, &key_path)?;
        let resolver = Arc::new(DynamicCertResolver::new(initial_key));

        let resolver_clone = Arc::clone(&resolver);
        let event_bus = self.event_bus.clone();
        let cp = cert_path.clone();
        let kp = key_path.clone();

        // Spawn watcher
        let watcher = CertFileWatcher::spawn(
            vec![cert_path.clone(), key_path.clone()],
            self.debounce_interval,
            move || {
                let resolver = Arc::clone(&resolver_clone);
                let bus = event_bus.clone();
                let c_path = cp.clone();
                let k_path = kp.clone();

                async move {
                    match CertReader::read_server_credentials(&c_path, &k_path) {
                        Ok(new_key) => {
                            resolver.update(new_key);
                            bus.emit(CertEvent::ServerCertReloaded {
                                cert_path: c_path.clone(),
                                key_path: k_path.clone(),
                            });
                            info!(
                                "Successfully reloaded server TLS certificates from {:?} and {:?}",
                                c_path, k_path
                            );
                        }
                        Err(err) => {
                            let err_msg = err.to_string();
                            bus.emit(CertEvent::ServerCertReloadFailed {
                                cert_path: c_path.clone(),
                                key_path: k_path.clone(),
                                error: err_msg.clone(),
                            });
                            warn!(
                                "Failed to reload server TLS certificates from {:?} / {:?}: {}; retaining currently active certificate",
                                c_path, k_path, err_msg
                            );
                        }
                    }
                }
            },
            cancel,
        )?;

        resolver.set_watcher(watcher);
        Ok(resolver)
    }

    /// Registers a client CA certificate for mTLS server authentication, validating initial
    /// roots and starting a debounced background watcher that updates the verifier upon file changes.
    pub fn register_client_ca(
        &self,
        ca_path: PathBuf,
        client_auth_mode: ClientAuthMode,
        cancel: CancellationToken,
    ) -> Result<Arc<DynamicClientCertVerifier>, TunnelError> {
        let initial_verifier = Self::build_client_verifier(&ca_path, client_auth_mode)?;
        let verifier = Arc::new(DynamicClientCertVerifier::new(initial_verifier));

        let verifier_clone = Arc::clone(&verifier);
        let event_bus = self.event_bus.clone();
        let p = ca_path.clone();

        let watcher = CertFileWatcher::spawn(
            vec![ca_path.clone()],
            self.debounce_interval,
            move || {
                let verifier = Arc::clone(&verifier_clone);
                let bus = event_bus.clone();
                let path = p.clone();

                async move {
                    match Self::build_client_verifier(&path, client_auth_mode) {
                        Ok(new_verifier) => {
                            verifier.update(new_verifier);
                            bus.emit(CertEvent::ClientCaReloaded {
                                ca_path: path.clone(),
                            });
                            info!(
                                "Successfully reloaded client CA certificate from {:?}",
                                path
                            );
                        }
                        Err(err) => {
                            let err_msg = err.to_string();
                            bus.emit(CertEvent::ClientCaReloadFailed {
                                ca_path: path.clone(),
                                error: err_msg.clone(),
                            });
                            warn!(
                                "Failed to reload client CA certificate from {:?}: {}; retaining currently active CA trust store",
                                path, err_msg
                            );
                        }
                    }
                }
            },
            cancel,
        )?;

        verifier.set_watcher(watcher);
        Ok(verifier)
    }

    /// Helper to build a `WebPkiClientVerifier` from a CA certificate path.
    fn build_client_verifier(
        ca_path: &Path,
        auth_mode: ClientAuthMode,
    ) -> Result<Arc<dyn rustls::server::danger::ClientCertVerifier>, TunnelError> {
        let roots = CertReader::read_root_store(ca_path)?;
        let builder = WebPkiClientVerifier::builder(Arc::new(roots));
        let verifier = match auth_mode {
            ClientAuthMode::None => {
                return Err(TunnelError::Tls(
                    "Client CA registration called with ClientAuthMode::None".to_string(),
                ));
            }
            ClientAuthMode::Required => builder.build().map_err(|e| {
                TunnelError::Tls(format!("Failed to build client cert verifier: {}", e))
            })?,
            ClientAuthMode::Optional => builder.allow_unauthenticated().build().map_err(|e| {
                TunnelError::Tls(format!(
                    "Failed to build optional client cert verifier: {}",
                    e
                ))
            })?,
        };
        Ok(verifier)
    }

    /// Generates an ephemeral self-signed `CertifiedKey` for zero-configuration fallback.
    pub fn generate_self_signed() -> Result<Arc<CertifiedKey>, TunnelError> {
        let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
        let cert_key = rcgen::generate_simple_self_signed(subject_alt_names)
            .map_err(|e| TunnelError::Tls(format!("Failed to generate self-signed cert: {}", e)))?;

        let cert_der = cert_key.cert.der().to_vec();
        let key_der = cert_key.signing_key.serialize_der();

        let cert_chain = vec![rustls::pki_types::CertificateDer::from(cert_der)];
        let private_key = rustls::pki_types::PrivateKeyDer::Pkcs8(
            rustls::pki_types::PrivatePkcs8KeyDer::from(key_der),
        );

        let signing_key = rustls::crypto::ring::sign::any_supported_type(&private_key)
            .map_err(|e| TunnelError::Tls(format!("Failed to build signing key: {:?}", e)))?;

        let certified_key = CertifiedKey::new(cert_chain, signing_key);
        Ok(Arc::new(certified_key))
    }
}

impl Default for CertManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generate_cert_and_key() -> (String, String) {
        let cert_key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_pem = cert_key.cert.pem();
        let key_pem = cert_key.signing_key.serialize_pem();
        (cert_pem, key_pem)
    }

    #[tokio::test]
    async fn test_cert_manager_self_signed() {
        let manager = CertManager::new();
        let cancel = CancellationToken::new();
        let resolver = manager.resolve_server_cert(None, None, cancel).unwrap();
        assert_eq!(resolver.current().cert.len(), 1);
    }

    #[tokio::test]
    async fn test_cert_manager_hot_reload_success() {
        let dir = tempfile::tempdir().unwrap();
        let (cert1, key1) = generate_cert_and_key();

        let cert_path = dir.path().join("server.crt");
        let key_path = dir.path().join("server.key");
        std::fs::write(&cert_path, &cert1).unwrap();
        std::fs::write(&key_path, &key1).unwrap();

        let manager = CertManager::with_debounce_interval(Duration::from_millis(100));
        let mut rx = manager.subscribe_events();
        let cancel = CancellationToken::new();

        let resolver = manager
            .register_server_cert(cert_path.clone(), key_path.clone(), cancel.clone())
            .unwrap();

        let initial_der = resolver.current().cert[0].to_vec();

        // Write new cert and key
        let (cert2, key2) = generate_cert_and_key();
        std::fs::write(&cert_path, &cert2).unwrap();
        std::fs::write(&key_path, &key2).unwrap();

        // Wait for debounce and event
        let event = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(CertEvent::ServerCertReloaded { .. }) = rx.recv().await {
                    break;
                }
            }
        })
        .await;

        assert!(
            event.is_ok(),
            "Expected ServerCertReloaded event within timeout"
        );
        let updated_der = resolver.current().cert[0].to_vec();
        assert_ne!(
            initial_der, updated_der,
            "Certificate must be updated after reload"
        );

        cancel.cancel();
    }

    #[tokio::test]
    async fn test_cert_manager_hot_reload_mismatch_preserves_old() {
        let dir = tempfile::tempdir().unwrap();
        let (cert1, key1) = generate_cert_and_key();

        let cert_path = dir.path().join("server.crt");
        let key_path = dir.path().join("server.key");
        std::fs::write(&cert_path, &cert1).unwrap();
        std::fs::write(&key_path, &key1).unwrap();

        let manager = CertManager::with_debounce_interval(Duration::from_millis(100));
        let mut rx = manager.subscribe_events();
        let cancel = CancellationToken::new();

        let resolver = manager
            .register_server_cert(cert_path.clone(), key_path.clone(), cancel.clone())
            .unwrap();

        let initial_der = resolver.current().cert[0].to_vec();

        // Write mismatched key (corrupt pairing)
        let (_, mismatched_key) = generate_cert_and_key();
        std::fs::write(&key_path, &mismatched_key).unwrap();

        // Wait for debounce and failure event
        let event = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(CertEvent::ServerCertReloadFailed { error, .. }) = rx.recv().await {
                    return error;
                }
            }
        })
        .await;

        assert!(
            event.is_ok(),
            "Expected ServerCertReloadFailed event within timeout"
        );
        let current_der = resolver.current().cert[0].to_vec();
        assert_eq!(
            initial_der, current_der,
            "Original certificate must be preserved on reload failure"
        );

        cancel.cancel();
    }
}
