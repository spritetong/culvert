//! TLS connector builder based on Rustls.
//! All comments and docstrings must be in English.

use rustls::client::danger::{ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;
use tokio_rustls::TlsConnector;

use crate::config::model::TlsConfig;
use crate::core::error::TunnelError;

/// Dangerous certificate verifier that skips all validation checks.
#[derive(Debug)]
struct NoVerifyVerifier;

impl ServerCertVerifier for NoVerifyVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
        ]
    }
}

/// Helper for constructing client TLS configurations from `TlsConfig`.
pub struct TlsConfigBuilder;

impl TlsConfigBuilder {
    /// Builds a `rustls::ClientConfig` from the given configuration.
    pub fn build_client_config(config: &TlsConfig) -> Result<ClientConfig, TunnelError> {
        let mut root_store = RootCertStore {
            roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
        };

        // Load custom CA certificate if configured
        if let Some(ref ca_path) = config.ca_certificate {
            let file = File::open(ca_path).map_err(|e| {
                TunnelError::Tls(format!(
                    "Failed to open CA certificate {:?}: {}",
                    ca_path, e
                ))
            })?;
            let mut reader = BufReader::new(file);
            for cert in rustls_pemfile::certs(&mut reader) {
                let cert = cert.map_err(|e| TunnelError::Tls(format!("Invalid CA cert: {}", e)))?;
                root_store
                    .add(cert)
                    .map_err(|e| TunnelError::Tls(format!("Failed to add CA cert: {}", e)))?;
            }
        }

        let builder = ClientConfig::builder();

        let builder = if config.insecure {
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifyVerifier))
        } else {
            builder.with_root_certificates(root_store)
        };

        // Configure client authentication (mTLS) if client certificate & key are provided
        let mut client_config = if let (Some(cert_path), Some(key_path)) =
            (&config.client_certificate, &config.client_key)
        {
            let cert_file = File::open(cert_path).map_err(|e| {
                TunnelError::Tls(format!("Failed to open client cert {:?}: {}", cert_path, e))
            })?;
            let mut cert_reader = BufReader::new(cert_file);
            let client_certs: Vec<CertificateDer<'static>> =
                rustls_pemfile::certs(&mut cert_reader)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| TunnelError::Tls(format!("Invalid client cert: {}", e)))?;

            let key_file = File::open(key_path).map_err(|e| {
                TunnelError::Tls(format!("Failed to open client key {:?}: {}", key_path, e))
            })?;
            let mut key_reader = BufReader::new(key_file);
            let key = rustls_pemfile::private_key(&mut key_reader)
                .map_err(|e| TunnelError::Tls(format!("Failed to read client key: {}", e)))?
                .ok_or_else(|| {
                    TunnelError::Tls("No private key found in client key file".to_string())
                })?;

            builder
                .with_client_auth_cert(client_certs, key)
                .map_err(|e| TunnelError::Tls(format!("Failed to configure mTLS: {}", e)))?
        } else {
            builder.with_no_client_auth()
        };

        if let Some(alpn_list) = &config.alpn {
            client_config.alpn_protocols =
                alpn_list.iter().map(|s| s.as_bytes().to_vec()).collect();
        }

        Ok(client_config)
    }

    /// Builds a `tokio_rustls::TlsConnector` from the given configuration.
    pub fn build_connector(config: &TlsConfig) -> Result<TlsConnector, TunnelError> {
        let client_config = Self::build_client_config(config)?;
        Ok(TlsConnector::from(Arc::new(client_config)))
    }
}
