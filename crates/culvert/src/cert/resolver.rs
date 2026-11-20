// Copyright (c) 2026 Sprite Tong <spritetong@gmail.com>
// SPDX-License-Identifier: MPL-2.0
// culvert is licensed under the Mozilla Public License Version 2.0.

//! Dynamic certificate resolver and client certificate verifier for runtime hot-reloading.
//! All comments and docstrings must be in English.

use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use rustls::pki_types::{CertificateDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls::{DigitallySignedStruct, DistinguishedName, SignatureScheme};

use super::watcher::CertFileWatcher;

/// Dynamic certificate resolver implementing `rustls::server::ResolvesServerCert`.
/// Allows zero-downtime atomic updates of active TLS certificates during runtime.
pub struct DynamicCertResolver {
    current: RwLock<Arc<CertifiedKey>>,
    watcher: Mutex<Option<CertFileWatcher>>,
}

impl DynamicCertResolver {
    /// Creates a new dynamic resolver with an initial `CertifiedKey`.
    pub fn new(initial: Arc<CertifiedKey>) -> Self {
        Self {
            current: RwLock::new(initial),
            watcher: Mutex::new(None),
        }
    }

    /// Attaches an active background file watcher to be retained for the lifetime of this resolver.
    pub fn set_watcher(&self, watcher: CertFileWatcher) {
        *self.watcher.lock() = Some(watcher);
    }

    /// Atomically updates the active certificate key pair.
    pub fn update(&self, new_key: Arc<CertifiedKey>) {
        *self.current.write() = new_key;
    }

    /// Returns a clone of the currently active `CertifiedKey`.
    pub fn current(&self) -> Arc<CertifiedKey> {
        self.current.read().clone()
    }
}

impl std::fmt::Debug for DynamicCertResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicCertResolver").finish()
    }
}

impl ResolvesServerCert for DynamicCertResolver {
    fn resolve(&self, _client_hello: ClientHello) -> Option<Arc<CertifiedKey>> {
        Some(self.current())
    }
}

/// Dynamic client certificate verifier delegating to an inner `ClientCertVerifier`.
/// Allows hot-reloading client CA trust roots without restarting listeners.
pub struct DynamicClientCertVerifier {
    inner: RwLock<Arc<dyn ClientCertVerifier>>,
    watcher: Mutex<Option<CertFileWatcher>>,
}

impl DynamicClientCertVerifier {
    /// Creates a new dynamic verifier with an initial verifier.
    pub fn new(initial: Arc<dyn ClientCertVerifier>) -> Self {
        Self {
            inner: RwLock::new(initial),
            watcher: Mutex::new(None),
        }
    }

    /// Attaches an active background file watcher to be retained for the lifetime of this verifier.
    pub fn set_watcher(&self, watcher: CertFileWatcher) {
        *self.watcher.lock() = Some(watcher);
    }

    /// Atomically updates the active client certificate verifier.
    pub fn update(&self, new_verifier: Arc<dyn ClientCertVerifier>) {
        *self.inner.write() = new_verifier;
    }

    /// Returns a clone of the currently active verifier.
    pub fn current(&self) -> Arc<dyn ClientCertVerifier> {
        self.inner.read().clone()
    }
}

impl std::fmt::Debug for DynamicClientCertVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicClientCertVerifier").finish()
    }
}

impl ClientCertVerifier for DynamicClientCertVerifier {
    fn offer_client_auth(&self) -> bool {
        self.current().offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        // Must delegate: the trait default derives this from `offer_client_auth()`
        // and would force `ClientAuthMode::Optional` into mandatory mTLS.
        self.current().client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        // An empty CA hint list is valid: rustls clients still present their
        // configured client certificate. The hot-swappable inner verifier lives
        // behind a read guard, so a borrow into it cannot be returned here.
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.current()
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.current().verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.current().verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.current().supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cert::CertManager;
    use crate::config::model::ClientAuthMode;
    use tokio_util::sync::CancellationToken;

    #[test]
    fn test_dynamic_resolver_quic_compatibility() {
        let key = CertManager::generate_self_signed().unwrap();
        let resolver = Arc::new(DynamicCertResolver::new(key));

        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(resolver.clone());

        let quic_crypto = quinn::crypto::rustls::QuicServerConfig::try_from(server_config);
        assert!(
            quic_crypto.is_ok(),
            "QuicServerConfig must support ResolvesServerCert"
        );

        // Verify resolver update works
        let new_key = CertManager::generate_self_signed().unwrap();
        resolver.update(new_key.clone());
        assert_eq!(resolver.current().cert, new_key.cert);
    }

    #[test]
    fn test_dynamic_client_verifier_preserves_auth_mode() {
        let dir = tempfile::tempdir().unwrap();
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca = rcgen::CertifiedIssuer::self_signed(rcgen::CertificateParams::default(), &ca_key)
            .unwrap();
        let ca_path = dir.path().join("ca.crt");
        std::fs::write(&ca_path, ca.as_ref().pem()).unwrap();

        let manager = CertManager::new();
        let optional = manager
            .register_client_ca(
                ca_path.clone(),
                ClientAuthMode::Optional,
                CancellationToken::new(),
            )
            .unwrap();
        let required = manager
            .register_client_ca(ca_path, ClientAuthMode::Required, CancellationToken::new())
            .unwrap();

        assert!(optional.offer_client_auth());
        assert!(
            !optional.client_auth_mandatory(),
            "Optional must not require a client cert"
        );
        assert!(required.offer_client_auth());
        assert!(
            required.client_auth_mandatory(),
            "Required must require a client cert"
        );
    }
}
